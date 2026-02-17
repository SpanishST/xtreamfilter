"""Stream service — async stream proxy with live passthrough and VOD adaptive buffering."""
from __future__ import annotations

import logging
import time
from collections import deque

import httpx
from fastapi import Request
from fastapi.responses import Response, StreamingResponse

logger = logging.getLogger(__name__)

# Live stream: immediate passthrough — no proxy-side buffering.
# The IPTV player handles its own buffering; adding a second buffer here
# causes bursty delivery that makes players stutter or replay ~2 s of video.
LIVE_CHUNK_SIZE = 32 * 1024  # 32 KB — small chunks for low-latency forwarding

# VOD / series: adaptive pre-buffering is still beneficial for downloads / seeking.
VOD_CHUNK_SIZE = 64 * 1024  # 64 KB chunks
VOD_PRE_BUFFER_SIZE = 256 * 1024  # 256 KB pre-buffer
VOD_MIN_BUFFER_SIZE = 1024 * 1024  # 1 MB minimum adaptive buffer
VOD_MAX_BUFFER_SIZE = 16 * 1024 * 1024  # 16 MB maximum adaptive buffer

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}


async def proxy_stream(upstream_url: str, request: Request, stream_type: str = "live") -> Response:
    """Proxy a stream from upstream server.

    • **live** → immediate chunk-by-chunk passthrough (no proxy buffering)
      so the player's own buffer is the only one in the chain.
    • **vod / series** → adaptive pre-buffering for smoother downloads.
    """
    upstream_headers = dict(HEADERS)
    if "range" in request.headers:
        upstream_headers["Range"] = request.headers["range"]
    if "accept" in request.headers:
        upstream_headers["Accept"] = request.headers["accept"]

    try:
        if stream_type == "live":
            timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0)
        else:
            timeout = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)

        client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        req = client.build_request("GET", upstream_url, headers=upstream_headers)
        upstream_response = await client.send(req, stream=True)

        response_headers: dict[str, str] = {}
        for header in ("content-type", "content-length", "content-range", "accept-ranges", "content-disposition"):
            if header in upstream_response.headers:
                response_headers[header] = upstream_response.headers[header]
        if "content-type" not in response_headers:
            response_headers["content-type"] = "video/mp2t" if stream_type == "live" else "video/mp4"
        response_headers["X-Accel-Buffering"] = "no"
        response_headers["Cache-Control"] = "no-cache, no-store"

        # ----- live: zero-copy passthrough -----
        if stream_type == "live":
            async def generate_live():
                try:
                    async for chunk in upstream_response.aiter_bytes(chunk_size=LIVE_CHUNK_SIZE):
                        if chunk:
                            yield chunk
                except httpx.ReadError:
                    logger.debug("Live upstream read interrupted")
                except Exception:
                    pass
                finally:
                    await upstream_response.aclose()
                    await client.aclose()

            return StreamingResponse(
                generate_live(),
                status_code=upstream_response.status_code,
                headers=response_headers,
                media_type=response_headers.get("content-type", "application/octet-stream"),
            )

        # ----- vod / series: adaptive pre-buffering -----
        async def generate_with_prebuffer():
            buffer: deque = deque()
            buffer_size = 0
            prebuffer_filled = False
            bytes_sent = 0
            start_time = time.time()
            current_min_buffer = VOD_MIN_BUFFER_SIZE
            last_throughput_check = time.time()
            bytes_since_check = 0
            throughput_samples: deque = deque(maxlen=10)
            last_buffer_adjustment = time.time()
            consecutive_slowdowns = 0
            stable_periods = 0

            try:
                async for chunk in upstream_response.aiter_bytes(chunk_size=VOD_CHUNK_SIZE):
                    if not chunk:
                        continue
                    buffer.append(chunk)
                    buffer_size += len(chunk)
                    bytes_since_check += len(chunk)

                    if not prebuffer_filled:
                        if buffer_size >= VOD_PRE_BUFFER_SIZE:
                            prebuffer_filled = True
                            elapsed = time.time() - start_time
                            initial_throughput = buffer_size / elapsed if elapsed > 0 else 0
                            throughput_samples.append(initial_throughput)
                            last_throughput_check = time.time()
                            bytes_since_check = 0
                        continue

                    now = time.time()
                    check_interval = now - last_throughput_check
                    if check_interval >= 2.0 and bytes_since_check > 0:
                        current_throughput = bytes_since_check / check_interval
                        throughput_samples.append(current_throughput)
                        avg_throughput = sum(throughput_samples) / len(throughput_samples)
                        if now - last_buffer_adjustment >= 3.0 and len(throughput_samples) >= 3:
                            if current_throughput < avg_throughput * 0.6:
                                consecutive_slowdowns += 1
                                stable_periods = 0
                                multiplier = 4 if consecutive_slowdowns >= 2 else 2
                                current_min_buffer = min(current_min_buffer * multiplier, VOD_MAX_BUFFER_SIZE)
                            elif current_throughput > avg_throughput * 0.9:
                                consecutive_slowdowns = 0
                                stable_periods += 1
                                if stable_periods >= 4 and current_min_buffer > VOD_MIN_BUFFER_SIZE:
                                    current_min_buffer = max(int(current_min_buffer * 0.75), VOD_MIN_BUFFER_SIZE)
                                    stable_periods = 0
                            else:
                                consecutive_slowdowns = 0
                            last_buffer_adjustment = now
                        last_throughput_check = now
                        bytes_since_check = 0

                    while buffer and buffer_size > current_min_buffer:
                        out_chunk = buffer.popleft()
                        buffer_size -= len(out_chunk)
                        bytes_sent += len(out_chunk)
                        yield out_chunk

                while buffer:
                    out_chunk = buffer.popleft()
                    bytes_sent += len(out_chunk)
                    yield out_chunk

            except httpx.ReadError:
                while buffer:
                    yield buffer.popleft()
            except Exception:
                pass
            finally:
                await upstream_response.aclose()
                await client.aclose()

        return StreamingResponse(
            generate_with_prebuffer(),
            status_code=upstream_response.status_code,
            headers=response_headers,
            media_type=response_headers.get("content-type", "application/octet-stream"),
        )

    except httpx.TimeoutException:
        return Response(content="Upstream timeout", status_code=504)
    except httpx.ConnectError:
        return Response(content="Upstream connection error", status_code=502)
    except Exception as e:
        return Response(content=f"Proxy error: {e}", status_code=500)

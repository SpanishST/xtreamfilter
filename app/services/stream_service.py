"""Stream service — async stream proxy with live passthrough and VOD adaptive buffering."""
from __future__ import annotations

import asyncio
import logging
import shutil
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


# ---------------------------------------------------------------------------
# Remux — ffmpeg remux to fragmented MP4 for browser playback
# ---------------------------------------------------------------------------

REMUX_CHUNK_SIZE = 64 * 1024  # 64 KB


async def remux_stream(upstream_url: str, request: Request) -> Response:
    """Remux a stream via ffmpeg to fragmented MP4 for in-browser playback.

    This performs a lossless remux (no transcoding): the audio/video codecs pass
    through unchanged, only the container is changed to fragmented MP4 which
    browsers can play natively.

    ffmpeg reads from the upstream URL directly (avoids double-piping).
    """
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return Response(content="ffmpeg not available on server", status_code=501)

    cmd = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel", "error",
        "-user_agent", HEADERS["User-Agent"],
        "-headers", "Accept: */*\r\nAccept-Encoding: identity\r\nConnection: keep-alive\r\n",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+discardcorrupt",
        "-i", upstream_url,
        "-c", "copy",                    # No transcoding — lossless remux
        "-movflags", "frag_keyframe+empty_moov+faststart",
        "-f", "mp4",
        "-"                              # Output to stdout
    ]

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def generate_remux():
            try:
                while True:
                    chunk = await process.stdout.read(REMUX_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk
            except Exception as e:
                logger.error(f"Remux stream error: {e}")
            finally:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
                # Log any ffmpeg errors
                stderr = await process.stderr.read()
                if stderr:
                    logger.warning(f"ffmpeg remux stderr: {stderr.decode(errors='replace')[:500]}")

        return StreamingResponse(
            generate_remux(),
            media_type="video/mp4",
            headers={
                "Content-Type": "video/mp4",
                "X-Accel-Buffering": "no",
                "Cache-Control": "no-cache, no-store",
            },
        )

    except FileNotFoundError:
        return Response(content="ffmpeg not found", status_code=501)
    except Exception as e:
        logger.error(f"Remux error: {e}")
        return Response(content=f"Remux error: {e}", status_code=500)


# ---------------------------------------------------------------------------
# Transcode — ffmpeg H.265→H.264 for browsers that don't support HEVC
# ---------------------------------------------------------------------------

TRANSCODE_CHUNK_SIZE = 128 * 1024  # 128 KB — larger chunks reduce overhead


async def transcode_stream(
    upstream_url: str,
    request: Request,
    is_live: bool = True,
    audio_only: bool = False,
    start_seconds: float = 0,
    audio_track: int = -1,
) -> Response:
    """Transcode a stream via ffmpeg for browser playback.

    For **live** streams this always performs a full transcode to
    H.264 + AAC at 720p.  The yadif filter is always present
    (``deint=interlaced`` makes it a no-op on progressive sources).
    This guarantees every source — HEVC, interlaced 1080i,
    E-AC3/DTS audio — plays in the browser without client-side
    codec sniffing or fallback logic.

    For **VOD/series** streams ``audio_only=True`` can be used to
    copy the video track untouched and only re-encode audio to AAC
    (lighter on CPU).

    ``start_seconds`` enables seeking for VOD: ffmpeg will seek to
    this position before decoding.

    ``audio_track`` selects a specific audio stream (0-based audio
    index).  -1 means ffmpeg's default (first audio).

    Output is MPEG-TS (live, low-latency) or fMP4 (VOD).
    """
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        return Response(content="ffmpeg not available on server", status_code=501)

    # Video codec settings
    if audio_only:
        video_opts = ["-c:v", "copy"]                    # Pass-through video (no re-encode)
    else:
        video_opts = [
            "-c:v", "libx264",
            "-preset", "ultrafast",      # Fastest encoding — lowest CPU
            "-tune", "zerolatency",      # Minimize latency + bframes=0
            "-crf", "26",                # Slightly lower quality → faster encoding
            "-flags", "+low_delay",      # Reduce encoder output buffering
        ]

    # Video filter — always deinterlace (no-op on progressive) + scale to
    # 720p to keep encoding faster-than-realtime on modest hardware.
    # When audio_only the video is copied so filters cannot be applied.
    video_filter = []
    if not audio_only:
        video_filter = ["-vf", "yadif=mode=send_frame:parity=auto:deint=interlaced,scale=1280:720"]

    # Audio codec settings — force stereo + 48kHz to handle streams with
    # missing channel layout or sample rate (common with E-AC3/DTS).
    audio_opts = ["-c:a", "aac", "-ac", "2", "-ar", "48000", "-b:a", "128k"]

    # Audio filter differs between live and VOD:
    # - Live: aresample=async=1:first_pts=0 smooths PTS discontinuities
    #   (IPTV sources often have huge base PTS or jumps).
    # - VOD: aresample=async=1000 gently corrects drift without forcing
    #   first_pts=0, which would desync audio from video after seeking.
    if is_live:
        audio_filter = ["-af", "aresample=async=1:first_pts=0"]
    else:
        audio_filter = ["-af", "aresample=async=1000"]

    # Common input options for resilience with broken IPTV streams.
    # +nobuffer reduces initial input buffering for faster startup.
    # -headers sends the same browser-like headers that httpx uses —
    # many IPTV CDNs reject requests missing Accept / Connection headers.
    input_opts = [
        "-user_agent", HEADERS["User-Agent"],
        "-headers", "Accept: */*\r\nAccept-Encoding: identity\r\nConnection: keep-alive\r\n",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-fflags", "+genpts+discardcorrupt+nobuffer",
        "-analyzeduration", "3000000",    # 3s analysis — enough for IPTV
        "-probesize", "5000000",          # 5MB probe — faster startup
        "-thread_queue_size", "512",      # larger input queue
    ]

    # Timestamp normalisation differs between live and VOD:
    # - Live: IPTV sources often have huge initial PTS (e.g. 34 000 s)
    #   → force start_at_zero + output_ts_offset 0 to normalise.
    # - VOD: let ffmpeg handle timestamps naturally; only add
    #   avoid_negative_ts after seeking to prevent negative PTS.
    if is_live:
        ts_opts = ["-start_at_zero", "-output_ts_offset", "0",
                   "-max_muxing_queue_size", "2048"]
    else:
        ts_opts = ["-max_muxing_queue_size", "2048",
                   "-avoid_negative_ts", "make_zero"]

    # Seek option — placed before -i for fast input seeking (VOD only).
    seek_opts = []
    if start_seconds and start_seconds > 0 and not is_live:
        seek_opts = ["-ss", str(start_seconds)]

    # Audio track selection — -map 0:v:0 -map 0:a:<idx>
    map_opts = []
    if audio_track >= 0:
        map_opts = ["-map", "0:v:0", "-map", f"0:a:{audio_track}"]
    # When no specific track is selected, ffmpeg picks the default

    if is_live:
        # Live: output MPEG-TS for low-latency streaming
        cmd = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel", "error",
            "-threads", "0",              # Use all CPU cores
            *input_opts,
            "-i", upstream_url,
            *map_opts,
            *video_opts,
            *video_filter,
            *audio_opts,
            *audio_filter,
            *ts_opts,
            "-flush_packets", "1",        # Flush output immediately
            "-f", "mpegts",
            "-"                           # Output to stdout
        ]
        media_type = "video/mp2t"
    else:
        # VOD: output fragmented MP4
        cmd = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel", "error",
            "-threads", "0",              # Use all CPU cores
            *seek_opts,                   # -ss before -i for fast seek
            *input_opts,
            "-i", upstream_url,
            *map_opts,
            *video_opts,
            *video_filter,
            *audio_opts,
            *audio_filter,
            *ts_opts,
            "-movflags", "frag_keyframe+empty_moov+faststart",
            "-f", "mp4",
            "-"                           # Output to stdout
        ]
        media_type = "video/mp4"

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        async def generate_transcode():
            try:
                while True:
                    chunk = await process.stdout.read(TRANSCODE_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk
            except Exception as e:
                logger.error(f"Transcode stream error: {e}")
            finally:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
                stderr = await process.stderr.read()
                if stderr:
                    logger.warning(f"ffmpeg transcode stderr: {stderr.decode(errors='replace')[:500]}")

        return StreamingResponse(
            generate_transcode(),
            media_type=media_type,
            headers={
                "Content-Type": media_type,
                "X-Accel-Buffering": "no",
                "Cache-Control": "no-cache, no-store",
            },
        )

    except FileNotFoundError:
        return Response(content="ffmpeg not found", status_code=501)
    except Exception as e:
        logger.error(f"Transcode error: {e}")
        return Response(content=f"Transcode error: {e}", status_code=500)

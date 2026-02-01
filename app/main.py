import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import httpx
from fastapi import FastAPI, Form, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

# Data directory - use environment variable or default to /data (Docker) or ./data (local)
DATA_DIR = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "./data")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
CACHE_FILE = os.path.join(DATA_DIR, "playlist_cache.m3u")
API_CACHE_FILE = os.path.join(DATA_DIR, "api_cache.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "refresh_progress.json")

# Templates
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Headers to mimic a browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Stream chunk size for proxying
# 1MB chunks work better for high-bitrate 4K streams
STREAM_CHUNK_SIZE = 1024 * 1024  # 1MB

# Adaptive buffer configuration for dynamic buffering
# These values are tuned for 4K streams at ~25 Mbps
PRE_BUFFER_SIZE = 4 * 1024 * 1024  # 4MB pre-buffer before starting (~1.3s at 25Mbps)
MIN_BUFFER_SIZE = 1 * 1024 * 1024  # 1MB minimum buffer to maintain
MAX_BUFFER_SIZE = 16 * 1024 * 1024  # 16MB max buffer (~5s at 25Mbps for bad connections)
BUFFER_CHUNK_SIZE = 512 * 1024  # 512KB chunks for reading (larger = fewer syscalls)

# Virtual ID offset for merged playlists (10 million per source)
# This allows up to 10 million streams per source before collision
VIRTUAL_ID_OFFSET = 10_000_000

# ============================================
# HTTP CLIENT SETUP
# ============================================

# Global httpx client with connection pooling
_http_client: Optional[httpx.AsyncClient] = None


async def get_http_client() -> httpx.AsyncClient:
    """Get or create the global httpx async client"""
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(connect=30.0, read=600.0, write=30.0, pool=30.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            follow_redirects=True,
        )
    return _http_client


async def close_http_client():
    """Close the global httpx client"""
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


# ============================================
# VIRTUAL ID ENCODING/DECODING
# ============================================


def encode_virtual_id(source_index, original_id):
    """
    Encode a source index and original stream ID into a virtual ID.
    This creates unique IDs across all sources for merged playlists.
    """
    try:
        return source_index * VIRTUAL_ID_OFFSET + int(original_id)
    except (ValueError, TypeError):
        return original_id


def decode_virtual_id(virtual_id):
    """
    Decode a virtual ID back to source index and original ID.
    """
    try:
        vid = int(virtual_id)
        source_index = vid // VIRTUAL_ID_OFFSET
        original_id = vid % VIRTUAL_ID_OFFSET
        return source_index, original_id
    except (ValueError, TypeError):
        return 0, virtual_id


def get_source_by_index(index):
    """Get source configuration by its index in the enabled sources list"""
    config = load_config()
    enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    if 0 <= index < len(enabled_sources):
        return enabled_sources[index]
    return None


def get_source_index(source_id):
    """Get the index of a source by its ID"""
    config = load_config()
    enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    for i, source in enumerate(enabled_sources):
        if source.get("id") == source_id:
            return i
    return 0


def get_proxy_enabled():
    """Check if proxy mode is enabled in config"""
    config = load_config()
    return config.get("options", {}).get("proxy_streams", True)


# ============================================
# ASYNC STREAM PROXY WITH DYNAMIC PRE-BUFFERING
# ============================================


async def proxy_stream(upstream_url: str, request: Request, stream_type: str = "live") -> Response:
    """
    Proxy a stream from upstream server to the client with dynamic pre-buffering.
    
    Features:
    - Pre-buffers data before starting to stream to handle upstream hiccups
    - Adaptive buffering based on upstream throughput
    - Handles Range headers for seeking in VOD content
    - Streaming response to avoid loading entire file in memory
    
    Args:
        upstream_url: The full upstream URL to fetch from
        request: The FastAPI request object
        stream_type: Type of stream ("live", "vod", "series") - affects timeout behavior
    
    Returns:
        StreamingResponse with pre-buffered content
    """
    # Build headers for upstream request
    upstream_headers = dict(HEADERS)
    
    # Forward Range header if present (for seeking in VOD)
    if "range" in request.headers:
        upstream_headers["Range"] = request.headers["range"]
    
    # Forward other relevant headers
    if "accept" in request.headers:
        upstream_headers["Accept"] = request.headers["accept"]
    
    try:
        # Set timeout based on stream type
        if stream_type == "live":
            # Live streams: short connect timeout, long read timeout for continuous streaming
            timeout = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=10.0)
        else:
            # VOD/Series: reasonable timeouts
            timeout = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)
        
        # Create a streaming client
        client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        
        # Make the initial request
        req = client.build_request("GET", upstream_url, headers=upstream_headers)
        upstream_response = await client.send(req, stream=True)
        
        # Build response headers
        response_headers = {}
        
        # Forward content-related headers
        headers_to_forward = [
            "content-type",
            "content-length",
            "content-range",
            "accept-ranges",
            "content-disposition",
        ]
        
        for header in headers_to_forward:
            if header in upstream_response.headers:
                response_headers[header] = upstream_response.headers[header]
        
        # Default content type if not provided
        if "content-type" not in response_headers:
            if stream_type == "live":
                response_headers["content-type"] = "video/mp2t"
            else:
                response_headers["content-type"] = "video/mp4"
        
        # Add headers to prevent buffering issues
        response_headers["X-Accel-Buffering"] = "no"
        response_headers["Cache-Control"] = "no-cache, no-store"
        
        async def generate_with_prebuffer():
            """Generator with dynamic adaptive buffering to handle upstream issues"""
            buffer = deque()
            buffer_size = 0
            prebuffer_filled = False
            bytes_sent = 0
            start_time = time.time()
            
            # Adaptive buffer settings
            current_min_buffer = MIN_BUFFER_SIZE  # Start with 1MB minimum
            last_throughput_check = time.time()
            bytes_since_check = 0
            throughput_samples = deque(maxlen=10)  # Keep last 10 throughput measurements for stability
            last_buffer_adjustment = time.time()
            consecutive_slowdowns = 0  # Track consecutive slowdowns for faster response
            stable_periods = 0  # Track stable periods before reducing buffer
            
            try:
                async for chunk in upstream_response.aiter_bytes(chunk_size=BUFFER_CHUNK_SIZE):
                    if not chunk:
                        continue
                    
                    # Add to buffer
                    buffer.append(chunk)
                    buffer_size += len(chunk)
                    bytes_since_check += len(chunk)
                    
                    # Pre-buffering phase: wait until we have enough data
                    if not prebuffer_filled:
                        if buffer_size >= PRE_BUFFER_SIZE:
                            prebuffer_filled = True
                            elapsed = time.time() - start_time
                            initial_throughput = buffer_size / elapsed if elapsed > 0 else 0
                            throughput_samples.append(initial_throughput)
                            logger.info(
                                f"Pre-buffer filled: {buffer_size / 1024:.1f}KB in {elapsed:.2f}s "
                                f"({initial_throughput / 1024:.1f} KB/s)"
                            )
                            last_throughput_check = time.time()
                            bytes_since_check = 0
                        continue
                    
                    # Measure throughput every 2 seconds and adapt buffer size
                    now = time.time()
                    check_interval = now - last_throughput_check
                    if check_interval >= 2.0 and bytes_since_check > 0:
                        current_throughput = bytes_since_check / check_interval
                        throughput_samples.append(current_throughput)
                        
                        # Calculate average throughput from samples
                        avg_throughput = sum(throughput_samples) / len(throughput_samples)
                        
                        # Adapt buffer based on throughput changes (check every 3 seconds)
                        if now - last_buffer_adjustment >= 3.0:
                            if len(throughput_samples) >= 3:
                                # Compare current to average - if dropping significantly, increase buffer
                                if current_throughput < avg_throughput * 0.6:
                                    # Throughput dropped by more than 40% - increase buffer
                                    consecutive_slowdowns += 1
                                    stable_periods = 0
                                    
                                    # Increase more aggressively with consecutive slowdowns
                                    if consecutive_slowdowns >= 2:
                                        multiplier = 4  # Quadruple on persistent slowdown
                                    else:
                                        multiplier = 2  # Double on first slowdown
                                    
                                    old_buffer = current_min_buffer
                                    current_min_buffer = min(current_min_buffer * multiplier, MAX_BUFFER_SIZE)
                                    if current_min_buffer > old_buffer:
                                        logger.info(
                                            f"Upstream slowdown detected ({current_throughput/1024:.0f} KB/s < "
                                            f"{avg_throughput/1024:.0f} KB/s avg), "
                                            f"increasing buffer: {old_buffer/1024:.0f}KB -> {current_min_buffer/1024:.0f}KB"
                                        )
                                elif current_throughput > avg_throughput * 0.9:
                                    # Throughput is stable/good
                                    consecutive_slowdowns = 0
                                    stable_periods += 1
                                    
                                    # Only reduce buffer after 4 consecutive stable periods (~12+ seconds)
                                    if stable_periods >= 4 and current_min_buffer > MIN_BUFFER_SIZE:
                                        old_buffer = current_min_buffer
                                        # Reduce gradually (by 25% instead of 50%)
                                        current_min_buffer = max(int(current_min_buffer * 0.75), MIN_BUFFER_SIZE)
                                        if current_min_buffer < old_buffer:
                                            stable_periods = 0  # Reset after reduction
                                            logger.info(
                                                f"Upstream stable ({current_throughput/1024:.0f} KB/s), "
                                                f"reducing buffer: {old_buffer/1024:.0f}KB -> {current_min_buffer/1024:.0f}KB"
                                            )
                                else:
                                    # Moderate throughput - reset slowdown counter but don't reduce
                                    consecutive_slowdowns = 0
                                    
                            last_buffer_adjustment = now
                        
                        last_throughput_check = now
                        bytes_since_check = 0
                    
                    # Streaming phase: yield chunks while maintaining adaptive minimum buffer
                    while buffer and buffer_size > current_min_buffer:
                        out_chunk = buffer.popleft()
                        buffer_size -= len(out_chunk)
                        bytes_sent += len(out_chunk)
                        yield out_chunk
                
                # Drain remaining buffer at end of stream
                while buffer:
                    out_chunk = buffer.popleft()
                    bytes_sent += len(out_chunk)
                    yield out_chunk
                
                elapsed = time.time() - start_time
                if bytes_sent > 1024 * 1024:  # Only log if > 1MB
                    avg_throughput = bytes_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Stream complete: {bytes_sent / 1024 / 1024:.2f}MB in {elapsed:.1f}s "
                        f"(avg {avg_throughput / 1024:.0f} KB/s, final buffer: {current_min_buffer/1024:.0f}KB)"
                    )
                
            except httpx.ReadError as e:
                logger.warning(f"Upstream read error: {e}")
                # Yield remaining buffer if any
                while buffer:
                    yield buffer.popleft()
            except Exception as e:
                logger.warning(f"Stream proxy error: {e}")
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
        logger.error(f"Timeout connecting to upstream: {upstream_url}")
        return Response(content="Upstream timeout", status_code=504)
    except httpx.ConnectError as e:
        logger.error(f"Connection error to upstream: {e}")
        return Response(content="Upstream connection error", status_code=502)
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return Response(content=f"Proxy error: {e}", status_code=500)


# ============================================
# CACHING SYSTEM
# ============================================

# In-memory cache - now supports multiple sources
_api_cache = {
    "sources": {},
    "last_refresh": None,
    "refresh_in_progress": False,
    "refresh_progress": {
        "current_source": 0,
        "total_sources": 0,
        "current_source_name": "",
        "current_step": "",
        "percent": 0
    },
}
_cache_lock = asyncio.Lock()

# Stream ID to source mapping for routing
_stream_source_map = {
    "live": {},
    "vod": {},
    "series": {},
}
_stream_map_lock = asyncio.Lock()

# Background task reference
_background_task: Optional[asyncio.Task] = None


def save_refresh_progress(progress_data):
    """Save refresh progress to file for cross-worker visibility"""
    try:
        os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
        with open(PROGRESS_FILE, "w") as f:
            json.dump(progress_data, f)
    except Exception as e:
        logger.warning(f"Failed to save progress: {e}")


def load_refresh_progress():
    """Load refresh progress from file"""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {
        "in_progress": False,
        "current_source": 0,
        "total_sources": 0,
        "current_source_name": "",
        "current_step": "",
        "percent": 0,
        "started_at": None
    }


def clear_refresh_progress():
    """Clear the progress file when refresh is complete"""
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    except Exception:
        pass


def get_cache_ttl():
    """Get cache TTL from config (in seconds)"""
    config = load_config()
    return config.get("options", {}).get("cache_ttl", 3600)


def load_cache_from_disk():
    """Load cache from disk on startup"""
    global _api_cache, _stream_source_map
    if os.path.exists(API_CACHE_FILE):
        try:
            with open(API_CACHE_FILE) as f:
                data = json.load(f)
                # Handle migration from old single-source cache format
                if "sources" not in data and "live_streams" in data:
                    logger.info("Migrating old cache format to multi-source format")
                    _api_cache = {
                        "sources": {},
                        "last_refresh": data.get("last_refresh"),
                        "refresh_in_progress": False,
                    }
                else:
                    _api_cache.update(data)
                    _api_cache["refresh_in_progress"] = False
                # Rebuild stream-to-source mapping synchronously on startup
                _rebuild_stream_source_map_sync()
                logger.info(f"Loaded cache from disk. Last refresh: {_api_cache.get('last_refresh', 'Never')}")
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")


def _rebuild_stream_source_map_sync():
    """Rebuild the stream ID to source mapping (synchronous version for startup)"""
    global _stream_source_map
    sources = _api_cache.get("sources", {})
    
    new_map = {"live": {}, "vod": {}, "series": {}}
    
    for source_id, source_cache in sources.items():
        for stream in source_cache.get("live_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["live"][stream_id] = source_id
        
        for stream in source_cache.get("vod_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["vod"][stream_id] = source_id
        
        for series in source_cache.get("series", []):
            series_id = str(series.get("series_id", ""))
            if series_id:
                new_map["series"][series_id] = source_id
    
    _stream_source_map = new_map
    logger.info(f"Rebuilt stream-source map: {len(new_map['live'])} live, {len(new_map['vod'])} vod, {len(new_map['series'])} series")


async def rebuild_stream_source_map():
    """Rebuild the stream ID to source mapping (async version)"""
    global _stream_source_map
    async with _cache_lock:
        sources = _api_cache.get("sources", {})
    
    new_map = {"live": {}, "vod": {}, "series": {}}
    
    for source_id, source_cache in sources.items():
        for stream in source_cache.get("live_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["live"][stream_id] = source_id
        
        for stream in source_cache.get("vod_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["vod"][stream_id] = source_id
        
        for series in source_cache.get("series", []):
            series_id = str(series.get("series_id", ""))
            if series_id:
                new_map["series"][series_id] = source_id
    
    async with _stream_map_lock:
        _stream_source_map = new_map
    
    logger.info(f"Rebuilt stream-source map: {len(new_map['live'])} live, {len(new_map['vod'])} vod, {len(new_map['series'])} series")


def save_cache_to_disk():
    """Save cache to disk"""
    try:
        data = {k: v for k, v in _api_cache.items() if k != "refresh_in_progress"}
        os.makedirs(os.path.dirname(API_CACHE_FILE), exist_ok=True)
        with open(API_CACHE_FILE, "w") as f:
            json.dump(data, f)
        logger.info(f"Cache saved to disk at {datetime.now().isoformat()}")
    except Exception as e:
        logger.error(f"Failed to save cache to disk: {e}")


def is_cache_valid():
    """Check if cache is still valid based on TTL"""
    last_refresh = _api_cache.get("last_refresh")

    if not last_refresh:
        return False

    try:
        last_time = datetime.fromisoformat(last_refresh)
        age = (datetime.now() - last_time).total_seconds()
        return age < get_cache_ttl()
    except (ValueError, TypeError):
        return False


def get_cached(key, source_id=None):
    """Get cached data for a key, optionally for a specific source or merged from all sources"""
    sources = _api_cache.get("sources", {})
    
    if source_id is not None:
        source_cache = sources.get(source_id, {})
        return source_cache.get(key, [])
    
    # Merge from all sources
    result = []
    for src_id, src_cache in sources.items():
        result.extend(src_cache.get(key, []))
    return result


def get_source_for_stream(stream_id, stream_type="live"):
    """Get the source ID for a given stream ID"""
    return _stream_source_map.get(stream_type, {}).get(str(stream_id))


def get_source_by_id(source_id):
    """Get source configuration by ID"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("id") == source_id:
            return source
    # Fallback to legacy xtream config
    if config.get("xtream", {}).get("host"):
        return {
            "id": "default",
            "host": config["xtream"]["host"],
            "username": config["xtream"]["username"],
            "password": config["xtream"]["password"],
        }
    return None


def get_source_credentials_for_stream(stream_id, stream_type="live"):
    """Get the upstream host/user/pass for a given stream ID"""
    source_id = get_source_for_stream(stream_id, stream_type)
    if source_id:
        source = get_source_by_id(source_id)
        if source:
            return source.get("host", "").rstrip("/"), source.get("username", ""), source.get("password", "")
    
    # Fallback: try first available source or legacy config
    config = load_config()
    sources = config.get("sources", [])
    if sources:
        source = sources[0]
        return source.get("host", "").rstrip("/"), source.get("username", ""), source.get("password", "")
    
    # Legacy fallback
    xtream = config.get("xtream", {})
    return xtream.get("host", "").rstrip("/"), xtream.get("username", ""), xtream.get("password", "")


async def fetch_from_upstream(host: str, username: str, password: str, action: str, retries: int = 2):
    """Fetch data from upstream Xtream server with retry support"""
    url = f"{host}/player_api.php"
    params = {"username": username, "password": password, "action": action}

    for attempt in range(retries + 1):
        try:
            # Create a fresh client for each large request to avoid connection issues
            async with httpx.AsyncClient(
                headers=HEADERS,
                timeout=httpx.Timeout(connect=30.0, read=600.0, write=30.0, pool=30.0),
                follow_redirects=True,
            ) as client:
                start_time = time.time()
                response = await client.get(url, params=params)
                elapsed = time.time() - start_time
                
                if response.status_code == 200:
                    data = response.json()
                    logger.debug(f"Fetched {action}: {len(data) if isinstance(data, list) else 'ok'} items in {elapsed:.1f}s")
                    return data
                else:
                    logger.warning(f"Fetch {action} failed with status {response.status_code} in {elapsed:.1f}s")
                    
        except httpx.TimeoutException:
            logger.error(f"Timeout fetching {action} (attempt {attempt + 1}/{retries + 1})")
        except httpx.RemoteProtocolError as e:
            logger.error(f"Protocol error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
        except httpx.ReadError as e:
            logger.error(f"Read error fetching {action}: {e} (attempt {attempt + 1}/{retries + 1})")
        except httpx.ConnectError as e:
            logger.error(f"Connection error fetching {action}: {e}")
            break  # Don't retry connection errors
        except Exception as e:
            logger.error(f"Error fetching {action}: {e}")
            break
        
        # Wait before retry
        if attempt < retries:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
    
    return None


async def refresh_cache():
    """Refresh all cached data from all configured sources"""
    global _api_cache

    # Check if already in progress (using file-based check for cross-worker visibility)
    existing_progress = load_refresh_progress()
    if existing_progress.get("in_progress"):
        started_at = existing_progress.get("started_at")
        if started_at:
            try:
                started_time = datetime.fromisoformat(started_at)
                if (datetime.now() - started_time).total_seconds() < 600:
                    logger.info("Refresh already in progress, skipping")
                    return False
            except (ValueError, TypeError):
                pass

    # Mark as in progress
    progress = {
        "in_progress": True,
        "current_source": 0,
        "total_sources": 0,
        "current_source_name": "",
        "current_step": "Initializing...",
        "percent": 0,
        "started_at": datetime.now().isoformat()
    }
    save_refresh_progress(progress)

    async with _cache_lock:
        _api_cache["refresh_in_progress"] = True

    config = load_config()
    sources = config.get("sources", [])

    # Backward compatibility
    if not sources and config.get("xtream", {}).get("host"):
        sources = [{
            "id": "default",
            "name": "Default",
            "host": config["xtream"]["host"],
            "username": config["xtream"]["username"],
            "password": config["xtream"]["password"],
            "enabled": True,
            "prefix": "",
            "filters": config.get("filters", {}),
        }]

    enabled_sources = [s for s in sources if s.get("enabled", True) and s.get("host") and s.get("username") and s.get("password")]
    
    if not enabled_sources:
        logger.info("Cannot refresh - no valid sources configured")
        async with _cache_lock:
            _api_cache["refresh_in_progress"] = False
        clear_refresh_progress()
        return False

    total_sources = len(enabled_sources)
    progress["total_sources"] = total_sources
    save_refresh_progress(progress)

    logger.info(f"Starting full refresh at {datetime.now().isoformat()} for {total_sources} source(s)")

    new_sources_cache = {}
    total_stats = {"live_cats": 0, "live_streams": 0, "vod_cats": 0, "vod_streams": 0, "series_cats": 0, "series": 0}

    try:
        for source_idx, source in enumerate(enabled_sources):
            source_id = source.get("id", "default")
            source_name = source.get("name", source_id)
            host = source.get("host", "").rstrip("/")
            username = source.get("username", "")
            password = source.get("password", "")

            progress = load_refresh_progress()
            progress["current_source"] = source_idx + 1
            progress["current_source_name"] = source_name
            save_refresh_progress(progress)

            logger.info(f"Refreshing source: {source_name}")

            try:
                def update_step(step_name, step_num):
                    p = load_refresh_progress()
                    p["current_step"] = f"{source_name}: {step_name}"
                    base_percent = (source_idx / total_sources) * 100
                    step_percent = (step_num / 6) * (100 / total_sources)
                    p["percent"] = int(base_percent + step_percent)
                    save_refresh_progress(p)
                    logger.info(f"[{source_name}] Step {step_num}/6: {step_name} (progress: {p['percent']}%)")

                update_step("Live categories", 0)
                live_cats = await fetch_from_upstream(host, username, password, "get_live_categories") or []
                logger.info(f"[{source_name}] Fetched {len(live_cats)} live categories")
                
                update_step("VOD categories", 1)
                vod_cats = await fetch_from_upstream(host, username, password, "get_vod_categories") or []
                logger.info(f"[{source_name}] Fetched {len(vod_cats)} VOD categories")
                
                update_step("Series categories", 2)
                series_cats = await fetch_from_upstream(host, username, password, "get_series_categories") or []
                logger.info(f"[{source_name}] Fetched {len(series_cats)} series categories")
                
                update_step("Live streams", 3)
                live_streams = await fetch_from_upstream(host, username, password, "get_live_streams") or []
                logger.info(f"[{source_name}] Fetched {len(live_streams)} live streams")
                
                update_step("VOD streams", 4)
                vod_streams = await fetch_from_upstream(host, username, password, "get_vod_streams") or []
                logger.info(f"[{source_name}] Fetched {len(vod_streams)} VOD streams")
                
                update_step("Series", 5)
                series = await fetch_from_upstream(host, username, password, "get_series") or []
                logger.info(f"[{source_name}] Fetched {len(series)} series")

                new_sources_cache[source_id] = {
                    "live_categories": live_cats,
                    "vod_categories": vod_cats,
                    "series_categories": series_cats,
                    "live_streams": live_streams,
                    "vod_streams": vod_streams,
                    "series": series,
                    "last_refresh": datetime.now().isoformat(),
                }

                total_stats["live_cats"] += len(live_cats)
                total_stats["live_streams"] += len(live_streams)
                total_stats["vod_cats"] += len(vod_cats)
                total_stats["vod_streams"] += len(vod_streams)
                total_stats["series_cats"] += len(series_cats)
                total_stats["series"] += len(series)

                logger.info(f"Source {source_id}: {len(live_streams)} live, {len(vod_streams)} vod, {len(series)} series")

            except Exception as e:
                logger.error(f"Failed to refresh source {source_id}: {e}")

        # Update cache only if we got some data
        if new_sources_cache:
            async with _cache_lock:
                _api_cache["sources"] = new_sources_cache
                _api_cache["last_refresh"] = datetime.now().isoformat()
                _api_cache["refresh_in_progress"] = False

            await rebuild_stream_source_map()
            save_cache_to_disk()

            logger.info(
                f"Refresh complete. Total: {total_stats['live_streams']} live, "
                f"{total_stats['vod_streams']} vod, {total_stats['series']} series"
            )
        else:
            logger.warning("Refresh completed but no data was fetched from any source")
            async with _cache_lock:
                _api_cache["refresh_in_progress"] = False

    finally:
        # Always clear progress on completion or error
        save_refresh_progress({
            "in_progress": False,
            "current_source": total_sources,
            "total_sources": total_sources,
            "current_source_name": "",
            "current_step": "Complete",
            "percent": 100
        })

    return True


async def background_refresh_loop():
    """Background task that periodically refreshes the cache"""
    logger.info("Background refresh task started")

    # Initial delay
    await asyncio.sleep(10)

    while True:
        try:
            ttl = get_cache_ttl()

            if not is_cache_valid():
                logger.info("Cache expired, triggering refresh...")
                await refresh_cache()
            else:
                last_refresh = _api_cache.get("last_refresh", "Never")
                logger.info(f"Cache still valid. Last refresh: {last_refresh}")

            sleep_time = max(ttl // 4, 300)
            await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("Background refresh task cancelled")
            break
        except Exception as e:
            logger.info(f"Background refresh error: {e}")
            await asyncio.sleep(60)


# ============================================
# CONFIGURATION
# ============================================


def load_config():
    """Load configuration from file"""
    default_filters = {
        "live": {"groups": [], "channels": []},
        "vod": {"groups": [], "channels": []},
        "series": {"groups": [], "channels": []},
    }
    
    default_config = {
        "sources": [],
        "xtream": {"host": "", "username": "", "password": ""},
        "filters": default_filters.copy(),
        "content_types": {
            "live": True,
            "vod": True,
            "series": True,
        },
        "options": {"cache_enabled": True, "cache_ttl": 3600, "proxy_streams": True},
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                config = json.load(f)
                
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                
                # Migrate old single-source config to multi-source if needed
                if not config.get("sources") and config.get("xtream", {}).get("host"):
                    filters = config.get("filters", {})
                    if "groups" in filters and isinstance(filters["groups"], list):
                        old_groups = filters.get("groups", [])
                        old_channels = filters.get("channels", [])
                        filters = {
                            "live": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "vod": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "series": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                        }
                    
                    config["sources"] = [{
                        "id": str(uuid.uuid4())[:8],
                        "name": "Default",
                        "host": config["xtream"]["host"],
                        "username": config["xtream"]["username"],
                        "password": config["xtream"]["password"],
                        "enabled": True,
                        "prefix": "",
                        "filters": filters,
                    }]
                    logger.info("Migrated legacy single-source config to multi-source format")
                
                for source in config.get("sources", []):
                    if "filters" not in source:
                        source["filters"] = default_filters.copy()
                    if "enabled" not in source:
                        source["enabled"] = True
                    if "prefix" not in source:
                        source["prefix"] = ""
                    for cat in ["live", "vod", "series"]:
                        if cat not in source["filters"]:
                            source["filters"][cat] = {"groups": [], "channels": []}
                        if "groups" not in source["filters"][cat]:
                            source["filters"][cat]["groups"] = []
                        if "channels" not in source["filters"][cat]:
                            source["filters"][cat]["channels"] = []
                
                if "content_types" not in config:
                    config["content_types"] = {"live": True, "vod": True, "series": True}
                
                return config
        except (OSError, json.JSONDecodeError, KeyError) as e:
            logger.error(f"Error loading config: {e}")
    return default_config


def save_config(config):
    """Save configuration to file"""
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


# ============================================
# FILTER FUNCTIONS
# ============================================


def matches_filter(value, filter_rule):
    """Check if a value matches a filter rule."""
    match_type = filter_rule.get("match", "contains")
    
    if match_type == "all":
        return True
    
    pattern = filter_rule.get("value", "")
    case_sensitive = filter_rule.get("case_sensitive", False)

    if not pattern:
        return False

    test_value = value if case_sensitive else value.lower()
    test_pattern = pattern if case_sensitive else pattern.lower()

    if match_type == "exact":
        return test_value == test_pattern
    elif match_type == "starts_with":
        return test_value.startswith(test_pattern)
    elif match_type == "ends_with":
        return test_value.endswith(test_pattern)
    elif match_type == "contains":
        return test_pattern in test_value
    elif match_type == "not_contains":
        return test_pattern not in test_value
    elif match_type == "regex":
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            return bool(re.search(pattern, value, flags))
        except re.error:
            return False

    return False


def should_include(value, filter_rules):
    """Determine if a value should be included based on filter rules."""
    include_rules = [r for r in filter_rules if r.get("type") == "include"]
    exclude_rules = [r for r in filter_rules if r.get("type") == "exclude"]

    for rule in exclude_rules:
        if matches_filter(value, rule):
            return False

    if include_rules:
        for rule in include_rules:
            if matches_filter(value, rule):
                return True
        return False

    return True


def build_category_map(categories):
    """Build a category ID to name map."""
    cat_map = {}
    for cat in categories:
        if isinstance(cat, dict):
            cat_id = str(cat.get("category_id", ""))
            cat_name = cat.get("category_name", "")
            cat_map[cat_id] = cat_name
        elif isinstance(cat, str):
            cat_map[cat] = cat
    return cat_map


def safe_get_category_name(cat):
    """Safely get category_name from a category."""
    if isinstance(cat, dict):
        return cat.get("category_name", "")
    elif isinstance(cat, str):
        return cat
    return ""


def safe_get_category_id(cat):
    """Safely get category_id from a category."""
    if isinstance(cat, dict):
        return cat.get("category_id", "")
    elif isinstance(cat, str):
        return cat
    return ""


def safe_copy_category(cat):
    """Safely copy a category."""
    if isinstance(cat, dict):
        return cat.copy()
    elif isinstance(cat, str):
        return {"category_id": cat, "category_name": cat}
    return {"category_id": "", "category_name": ""}


# ============================================
# M3U GENERATION
# ============================================


def generate_m3u(config, request: Request, use_virtual_ids=True):
    """Generate filtered M3U playlist from cached data, merging all sources."""
    sources = config.get("sources", [])
    content_types = config.get("content_types", {"live": True, "vod": True, "series": True})
    
    if not sources and config.get("xtream", {}).get("host"):
        sources = [{
            "id": "default",
            "name": "Default",
            "host": config["xtream"]["host"],
            "username": config["xtream"]["username"],
            "password": config["xtream"]["password"],
            "enabled": True,
            "prefix": "",
            "filters": config.get("filters", {}),
        }]
    
    if not sources:
        return "#EXTM3U\n# Error: No sources configured\n"

    server_url = str(request.base_url).rstrip("/")
    stream_base = "/merged" if use_virtual_ids else ""

    lines = ["#EXTM3U"]
    stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0, "sources": 0}

    enabled_sources = [s for s in sources if s.get("enabled", True)]

    for source_index, source in enumerate(enabled_sources):
        source_id = source.get("id", "default")
        prefix = source.get("prefix", "")
        filters = source.get("filters", {})
        
        if not source.get("host") or not source.get("username") or not source.get("password"):
            continue
        
        stats["sources"] += 1

        # Live TV
        if content_types.get("live", True):
            live_filters = filters.get("live", {"groups": [], "channels": []})
            live_group_filters = live_filters.get("groups", [])
            live_channel_filters = live_filters.get("channels", [])

            categories = get_cached("live_categories", source_id)
            cat_map = build_category_map(categories)
            streams = get_cached("live_streams", source_id)

            for stream in streams:
                stream_id = stream.get("stream_id")
                name = stream.get("name", "Unknown")
                icon = stream.get("stream_icon", "")
                cat_id = str(stream.get("category_id", ""))
                group = cat_map.get(cat_id, "Unknown")
                epg_id = stream.get("epg_channel_id", "")

                if not should_include(group, live_group_filters):
                    stats["excluded"] += 1
                    continue

                if not should_include(name, live_channel_filters):
                    stats["excluded"] += 1
                    continue

                display_group = f"{prefix}{group}" if prefix else group
                url_id = encode_virtual_id(source_index, stream_id) if use_virtual_ids else stream_id
                stream_url = f"{server_url}{stream_base}/user/pass/{url_id}.ts"
                lines.append(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-logo="{icon}" group-title="{display_group}",{name}')
                lines.append(stream_url)
                stats["live"] += 1

        # VOD
        if content_types.get("vod", True):
            vod_filters = filters.get("vod", {"groups": [], "channels": []})
            vod_group_filters = vod_filters.get("groups", [])
            vod_channel_filters = vod_filters.get("channels", [])

            vod_categories = get_cached("vod_categories", source_id)
            vod_cat_map = build_category_map(vod_categories)
            vod_streams = get_cached("vod_streams", source_id)

            for vod in vod_streams:
                stream_id = vod.get("stream_id")
                name = vod.get("name", "Unknown")
                icon = vod.get("stream_icon", "")
                cat_id = str(vod.get("category_id", ""))
                group = vod_cat_map.get(cat_id, "Movies")
                extension = vod.get("container_extension", "mp4")

                if not should_include(group, vod_group_filters):
                    stats["excluded"] += 1
                    continue

                if not should_include(name, vod_channel_filters):
                    stats["excluded"] += 1
                    continue

                display_group = f"{prefix}{group}" if prefix else group
                url_id = encode_virtual_id(source_index, stream_id) if use_virtual_ids else stream_id
                stream_url = f"{server_url}{stream_base}/movie/user/pass/{url_id}.{extension}"
                lines.append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{display_group}",{name}')
                lines.append(stream_url)
                stats["vod"] += 1

        # Series
        if content_types.get("series", True):
            series_filters = filters.get("series", {"groups": [], "channels": []})
            series_group_filters = series_filters.get("groups", [])
            series_channel_filters = series_filters.get("channels", [])

            series_categories = get_cached("series_categories", source_id)
            series_cat_map = build_category_map(series_categories)
            all_series = get_cached("series", source_id)

            for series in all_series:
                series_name = series.get("name", "Unknown")
                series_icon = series.get("cover", "")
                cat_id = str(series.get("category_id", ""))
                group = series_cat_map.get(cat_id, "Series")

                if not should_include(group, series_group_filters):
                    stats["excluded"] += 1
                    continue

                if not should_include(series_name, series_channel_filters):
                    stats["excluded"] += 1
                    continue

                display_group = f"{prefix}{group}" if prefix else group
                lines.append(f'#EXTINF:-1 tvg-logo="{series_icon}" group-title="{display_group}",{series_name}')
                lines.append("# Series - use Xtream API for episodes")
                stats["series"] += 1

    stats_line = f"# Content: {stats['live']} live, {stats['vod']} movies, {stats['series']} series | {stats['excluded']} excluded | {stats['sources']} source(s)"
    lines.insert(1, stats_line)

    return "\n".join(lines)


# ============================================
# FASTAPI APPLICATION
# ============================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown"""
    global _background_task
    
    # Startup
    os.makedirs(DATA_DIR, exist_ok=True)
    load_cache_from_disk()
    
    # Start background refresh task
    _background_task = asyncio.create_task(background_refresh_loop())
    
    # If cache is empty or invalid, trigger immediate refresh
    if not is_cache_valid():
        logger.info("Cache is empty or invalid, triggering initial refresh...")
        asyncio.create_task(refresh_cache())
    
    yield
    
    # Shutdown
    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass
    
    await close_http_client()
    logger.info("Application shutdown complete")


app = FastAPI(title="XtreamFilter", lifespan=lifespan)


# ============================================
# WEB UI ROUTES
# ============================================


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main configuration page"""
    config = load_config()
    return templates.TemplateResponse("index.html", {"request": request, "config": config})


@app.post("/save")
async def save(
    request: Request,
    host: str = Form(""),
    username: str = Form(""),
    password: str = Form(""),
):
    """Save configuration"""
    config = load_config()

    config["xtream"]["host"] = host.strip()
    config["xtream"]["username"] = username.strip()
    config["xtream"]["password"] = password.strip()

    form = await request.form()
    config["content_types"] = {
        "live": "content_live" in form,
        "vod": "content_vod" in form,
        "series": "content_series" in form,
    }

    save_config(config)

    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

    return RedirectResponse(url="/", status_code=303)


# ============================================
# FILTER API ROUTES
# ============================================


@app.get("/api/filters")
async def get_filters():
    """Get all filter rules"""
    config = load_config()
    return config["filters"]


@app.post("/api/filters")
async def save_filters(request: Request):
    """Save filter rules"""
    config = load_config()
    data = await request.json()

    if "live" in data or "vod" in data or "series" in data:
        for cat in ["live", "vod", "series"]:
            if cat in data:
                config["filters"][cat] = data[cat]
    else:
        for cat in ["live", "vod", "series"]:
            config["filters"][cat]["groups"] = data.get("groups", [])
            config["filters"][cat]["channels"] = data.get("channels", [])

    save_config(config)

    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

    return {"status": "ok"}


@app.post("/api/filters/add")
async def add_filter(request: Request):
    """Add a single filter rule"""
    config = load_config()
    data = await request.json()

    category = data.get("category", "live")
    target = data.get("target", "groups")
    rule = {
        "type": data.get("type", "exclude"),
        "match": data.get("match", "contains"),
        "value": data.get("value", ""),
        "case_sensitive": data.get("case_sensitive", False),
    }

    if category in config["filters"] and target in config["filters"][category]:
        config["filters"][category][target].append(rule)
        save_config(config)

    return {"status": "ok", "filters": config["filters"]}


@app.post("/api/filters/delete")
async def delete_filter(request: Request):
    """Delete a filter rule by index"""
    config = load_config()
    data = await request.json()

    category = data.get("category", "live")
    target = data.get("target", "groups")
    index = data.get("index", -1)

    if category in config["filters"] and target in config["filters"][category]:
        filter_list = config["filters"][category][target]
        if 0 <= index < len(filter_list):
            filter_list.pop(index)
            save_config(config)

    return {"status": "ok", "filters": config["filters"]}


# ============================================
# SOURCE MANAGEMENT API
# ============================================


@app.get("/api/sources")
async def get_sources():
    """Get all configured sources"""
    config = load_config()
    return {"sources": config.get("sources", [])}


@app.post("/api/sources")
async def add_source(request: Request):
    """Add a new source"""
    config = load_config()
    data = await request.json()
    
    new_source = {
        "id": str(uuid.uuid4())[:8],
        "name": data.get("name", "New Source"),
        "host": data.get("host", ""),
        "username": data.get("username", ""),
        "password": data.get("password", ""),
        "enabled": data.get("enabled", True),
        "prefix": data.get("prefix", ""),
        "filters": data.get("filters", {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        }),
    }
    
    if "sources" not in config:
        config["sources"] = []
    config["sources"].append(new_source)
    save_config(config)
    
    return {"status": "ok", "source": new_source, "sources": config["sources"]}


@app.get("/api/sources/{source_id}")
async def get_source(source_id: str):
    """Get a specific source by ID"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("id") == source_id:
            return {"source": source}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.put("/api/sources/{source_id}")
async def update_source(source_id: str, request: Request):
    """Update a source"""
    config = load_config()
    data = await request.json()
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            if "name" in data:
                source["name"] = data["name"]
            if "host" in data:
                source["host"] = data["host"]
            if "username" in data:
                source["username"] = data["username"]
            if "password" in data:
                source["password"] = data["password"]
            if "enabled" in data:
                source["enabled"] = data["enabled"]
            if "prefix" in data:
                source["prefix"] = data["prefix"] if data["prefix"] else ""
            if "route" in data:
                source["route"] = data["route"]
            if "filters" in data:
                source["filters"] = data["filters"]
            
            config["sources"][i] = source
            save_config(config)
            return {"status": "ok", "source": source, "sources": config["sources"]}
    
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.delete("/api/sources/{source_id}")
async def delete_source(source_id: str):
    """Delete a source"""
    config = load_config()
    
    sources = config.get("sources", [])
    config["sources"] = [s for s in sources if s.get("id") != source_id]
    
    if len(config["sources"]) < len(sources):
        save_config(config)
        return {"status": "ok", "sources": config["sources"]}
    
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.get("/api/sources/{source_id}/filters")
async def get_source_filters(source_id: str):
    """Get filters for a specific source"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("id") == source_id:
            return {"filters": source.get("filters", {})}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.post("/api/sources/{source_id}/filters")
async def save_source_filters(source_id: str, request: Request):
    """Save filters for a specific source"""
    config = load_config()
    data = await request.json()
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            source["filters"] = data
            config["sources"][i] = source
            save_config(config)
            return {"status": "ok", "filters": source["filters"]}
    
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.post("/api/sources/{source_id}/filters/add")
async def add_source_filter(source_id: str, request: Request):
    """Add a filter rule to a specific source"""
    config = load_config()
    data = await request.json()
    
    category = data.get("category", "live")
    target = data.get("target", "groups")
    rule = {
        "type": data.get("type", "exclude"),
        "match": data.get("match", "contains"),
        "value": data.get("value", ""),
        "case_sensitive": data.get("case_sensitive", False),
    }
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            if "filters" not in source:
                source["filters"] = {"live": {"groups": [], "channels": []}, "vod": {"groups": [], "channels": []}, "series": {"groups": [], "channels": []}}
            if category not in source["filters"]:
                source["filters"][category] = {"groups": [], "channels": []}
            if target not in source["filters"][category]:
                source["filters"][category][target] = []
            
            source["filters"][category][target].append(rule)
            config["sources"][i] = source
            save_config(config)
            return {"status": "ok", "filters": source["filters"]}
    
    return JSONResponse({"error": "Source not found"}, status_code=404)


@app.post("/api/sources/{source_id}/filters/delete")
async def delete_source_filter(source_id: str, request: Request):
    """Delete a filter rule from a specific source"""
    config = load_config()
    data = await request.json()
    
    category = data.get("category", "live")
    target = data.get("target", "groups")
    index = data.get("index", -1)
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            if category in source.get("filters", {}) and target in source["filters"][category]:
                filter_list = source["filters"][category][target]
                if 0 <= index < len(filter_list):
                    filter_list.pop(index)
                    config["sources"][i] = source
                    save_config(config)
            return {"status": "ok", "filters": source.get("filters", {})}
    
    return JSONResponse({"error": "Source not found"}, status_code=404)


# ============================================
# PLAYLIST ROUTES
# ============================================


@app.get("/playlist.m3u")
@app.get("/get.php")
async def playlist(request: Request):
    """Serve the filtered M3U playlist"""
    config = load_config()
    m3u_content = generate_m3u(config, request)

    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": 'attachment; filename="playlist.m3u"', "Cache-Control": "no-cache"},
    )


@app.get("/playlist_full.m3u")
@app.get("/full.m3u")
async def playlist_full(request: Request):
    """Serve the unfiltered M3U playlist"""
    config = load_config()
    unfiltered_config = {
        "xtream": config["xtream"],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": config.get("content_types", {"live": True, "vod": True, "series": True}),
    }
    m3u_content = generate_m3u(unfiltered_config, request)

    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": 'attachment; filename="playlist_full.m3u"', "Cache-Control": "no-cache"},
    )


@app.get("/groups")
async def groups(
    type: str = Query("live"),
    source_id: Optional[str] = Query(None),
):
    """API endpoint to get all available groups for a content type"""
    config = load_config()
    sources = config.get("sources", [])
    
    if not sources and not config.get("xtream", {}).get("host"):
        return {"error": "Not configured", "groups": []}
    
    if type == "live":
        categories = get_cached("live_categories", source_id)
    elif type == "vod":
        categories = get_cached("vod_categories", source_id)
    elif type == "series":
        categories = get_cached("series_categories", source_id)
    else:
        categories = []
    
    groups_list = sorted(set(safe_get_category_name(cat) for cat in categories if safe_get_category_name(cat)))
    return {"groups": groups_list}


@app.get("/channels")
async def channels(
    type: str = Query("live"),
    source_id: Optional[str] = Query(None),
    search: str = Query(""),
    group: str = Query(""),
    page: int = Query(1),
    per_page: int = Query(100),
):
    """API endpoint to get channel/item names with search and filtering"""
    config = load_config()
    sources = config.get("sources", [])

    if not sources and not config.get("xtream", {}).get("host"):
        return {"error": "Not configured", "channels": []}

    search_lower = search.lower()

    if type == "live":
        streams = get_cached("live_streams", source_id)
        categories = get_cached("live_categories", source_id)
    elif type == "vod":
        streams = get_cached("vod_streams", source_id)
        categories = get_cached("vod_categories", source_id)
    elif type == "series":
        streams = get_cached("series", source_id)
        categories = get_cached("series_categories", source_id)
    else:
        streams = []
        categories = []

    cat_map = build_category_map(categories)

    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")

        if search_lower and search_lower not in name.lower():
            continue
        if group and grp != group:
            continue

        items.append({"name": name, "group": grp})

    total = len(items)

    start = (page - 1) * per_page
    end = start + per_page
    paginated = items[start:end]

    return {
        "channels": [item["name"] for item in paginated],
        "items": paginated,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
    }


@app.get("/api/browse")
async def api_browse(
    type: str = Query("live"),
    search: str = Query(""),
    group: str = Query(""),
    page: int = Query(1),
    per_page: int = Query(50),
):
    """Enhanced browsing API for channels/items with full metadata."""
    per_page = min(per_page, 200)
    search_lower = search.lower()

    if type == "live":
        streams = get_cached("live_streams")
        categories = get_cached("live_categories")
    elif type == "vod":
        streams = get_cached("vod_streams")
        categories = get_cached("vod_categories")
    elif type == "series":
        streams = get_cached("series")
        categories = get_cached("series_categories")
    else:
        return {"error": "Invalid content type", "items": []}

    cat_map = build_category_map(categories)

    group_counts = {}
    for s in streams:
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")
        group_counts[grp] = group_counts.get(grp, 0) + 1

    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")
        icon = s.get("stream_icon", "") or s.get("cover", "")

        if search_lower and search_lower not in name.lower() and search_lower not in grp.lower():
            continue

        if group and grp != group:
            continue

        items.append({
            "name": name,
            "group": grp,
            "icon": icon,
            "id": s.get("stream_id") or s.get("series_id"),
        })

    total = len(items)
    items.sort(key=lambda x: (x["group"].lower(), x["name"].lower()))

    start = (page - 1) * per_page
    end = start + per_page
    paginated = items[start:end]

    groups_list = [{"name": g, "count": c} for g, c in sorted(group_counts.items())]

    return {
        "items": paginated,
        "groups": groups_list,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "content_type": type,
    }


@app.get("/stats")
async def stats():
    """API endpoint to get playlist statistics"""
    config = load_config()
    xtream = config["xtream"]

    if not xtream["host"]:
        return {"error": "Not configured"}

    host = xtream["host"].rstrip("/")
    client = await get_http_client()

    try:
        live_cats_resp = await client.get(
            f"{host}/player_api.php",
            params={"username": xtream["username"], "password": xtream["password"], "action": "get_live_categories"}
        )
        live_categories = live_cats_resp.json() if live_cats_resp.status_code == 200 else []
        
        vod_cats_resp = await client.get(
            f"{host}/player_api.php",
            params={"username": xtream["username"], "password": xtream["password"], "action": "get_vod_categories"}
        )
        vod_categories = vod_cats_resp.json() if vod_cats_resp.status_code == 200 else []
        
        series_cats_resp = await client.get(
            f"{host}/player_api.php",
            params={"username": xtream["username"], "password": xtream["password"], "action": "get_series_categories"}
        )
        series_categories = series_cats_resp.json() if series_cats_resp.status_code == 200 else []
        
        streams_resp = await client.get(
            f"{host}/player_api.php",
            params={"username": xtream["username"], "password": xtream["password"], "action": "get_live_streams"}
        )
        streams = streams_resp.json() if streams_resp.status_code == 200 else []
    except Exception:
        return {"error": "Failed to fetch stats"}

    filters = config["filters"]
    total_group_filters = 0
    total_channel_filters = 0
    for cat in ["live", "vod", "series"]:
        if cat in filters:
            total_group_filters += len(filters[cat].get("groups", []))
            total_channel_filters += len(filters[cat].get("channels", []))

    return {
        "total_categories": len(live_categories) + len(vod_categories) + len(series_categories),
        "total_channels": len(streams),
        "live_categories": len(live_categories),
        "vod_categories": len(vod_categories),
        "series_categories": len(series_categories),
        "group_filters": total_group_filters,
        "channel_filters": total_channel_filters,
    }


@app.get("/preview")
async def preview(request: Request):
    """Preview filtered playlist stats"""
    config = load_config()
    m3u_content = generate_m3u(config, request)

    lines = m3u_content.split("\n")
    stats_line = lines[1] if len(lines) > 1 else ""

    sample = []
    for i in range(2, min(len(lines), 22), 2):
        if lines[i].startswith("#EXTINF"):
            name = lines[i].split(",", 1)[-1] if "," in lines[i] else lines[i]
            sample.append(name)

    return {"stats": stats_line, "sample_channels": sample}


# ============================================
# XTREAM CODES API PROXY
# ============================================


@app.get("/player_api.php")
async def player_api(request: Request):
    """Root Xtream Codes API endpoint - redirects to merged API."""
    return await player_api_merged(request)


@app.get("/full/player_api.php")
async def player_api_full(request: Request):
    """Root full/unfiltered endpoint."""
    config = load_config()
    sources = config.get("sources", [])
    
    for source in sources:
        if source.get("enabled", True) and source.get("route"):
            return await player_api_source_full(source.get("route"), request)
    
    available_routes = [s.get("route") for s in sources if s.get("enabled", True) and s.get("route")]
    if available_routes:
        return JSONResponse({
            "error": "Please use a dedicated source route",
            "available_routes": [f"/{r}/full/player_api.php" for r in available_routes]
        }, status_code=400)
    
    return JSONResponse({
        "error": "No sources configured with dedicated routes."
    }, status_code=400)


# ============================================
# MERGED XTREAM API
# ============================================


@app.get("/merged/player_api.php")
async def player_api_merged(request: Request):
    """Merged Xtream API - combines all sources with virtual IDs."""
    config = load_config()
    enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    action = request.query_params.get("action", "")

    if not enabled_sources:
        return JSONResponse({"error": "No sources configured"}, status_code=400)

    client = await get_http_client()

    try:
        if not action:
            source = enabled_sources[0]
            host = source["host"].rstrip("/")
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + "/merged"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action == "get_live_categories":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                prefix = source.get("prefix", "")
                filters = source.get("filters", {}).get("live", {}).get("groups", [])
                for cat in get_cached("live_categories", source_id):
                    cat_name = safe_get_category_name(cat)
                    if should_include(cat_name, filters):
                        display_name = f"{prefix}{cat_name}" if prefix else cat_name
                        cat_copy = safe_copy_category(cat)
                        cat_copy["category_name"] = display_name
                        cat_copy["category_id"] = encode_virtual_id(idx, safe_get_category_id(cat))
                        result.append(cat_copy)
            return result

        elif action == "get_vod_categories":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                prefix = source.get("prefix", "")
                filters = source.get("filters", {}).get("vod", {}).get("groups", [])
                for cat in get_cached("vod_categories", source_id):
                    cat_name = safe_get_category_name(cat)
                    if should_include(cat_name, filters):
                        display_name = f"{prefix}{cat_name}" if prefix else cat_name
                        cat_copy = safe_copy_category(cat)
                        cat_copy["category_name"] = display_name
                        cat_copy["category_id"] = encode_virtual_id(idx, safe_get_category_id(cat))
                        result.append(cat_copy)
            return result

        elif action == "get_series_categories":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                prefix = source.get("prefix", "")
                filters = source.get("filters", {}).get("series", {}).get("groups", [])
                for cat in get_cached("series_categories", source_id):
                    cat_name = safe_get_category_name(cat)
                    if should_include(cat_name, filters):
                        display_name = f"{prefix}{cat_name}" if prefix else cat_name
                        cat_copy = safe_copy_category(cat)
                        cat_copy["category_name"] = display_name
                        cat_copy["category_id"] = encode_virtual_id(idx, safe_get_category_id(cat))
                        result.append(cat_copy)
            return result

        elif action == "get_live_streams":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                filters = source.get("filters", {})
                group_filters = filters.get("live", {}).get("groups", [])
                channel_filters = filters.get("live", {}).get("channels", [])
                categories = get_cached("live_categories", source_id)
                cat_map = build_category_map(categories)
                
                for stream in get_cached("live_streams", source_id):
                    cat_id = str(stream.get("category_id", ""))
                    group_name = cat_map.get(cat_id, "")
                    channel_name = stream.get("name", "")
                    if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                        stream_copy = stream.copy()
                        stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                        stream_copy["category_id"] = encode_virtual_id(idx, stream.get("category_id", 0))
                        result.append(stream_copy)
            return result

        elif action == "get_vod_streams":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                filters = source.get("filters", {})
                group_filters = filters.get("vod", {}).get("groups", [])
                channel_filters = filters.get("vod", {}).get("channels", [])
                categories = get_cached("vod_categories", source_id)
                cat_map = build_category_map(categories)
                
                for stream in get_cached("vod_streams", source_id):
                    cat_id = str(stream.get("category_id", ""))
                    group_name = cat_map.get(cat_id, "")
                    channel_name = stream.get("name", "")
                    if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                        stream_copy = stream.copy()
                        stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                        stream_copy["category_id"] = encode_virtual_id(idx, stream.get("category_id", 0))
                        result.append(stream_copy)
            return result

        elif action == "get_series":
            result = []
            for idx, source in enumerate(enabled_sources):
                source_id = source.get("id")
                filters = source.get("filters", {})
                group_filters = filters.get("series", {}).get("groups", [])
                channel_filters = filters.get("series", {}).get("channels", [])
                categories = get_cached("series_categories", source_id)
                cat_map = build_category_map(categories)
                
                for series in get_cached("series", source_id):
                    cat_id = str(series.get("category_id", ""))
                    group_name = cat_map.get(cat_id, "")
                    series_name = series.get("name", "")
                    if should_include(group_name, group_filters) and should_include(series_name, channel_filters):
                        series_copy = series.copy()
                        series_copy["series_id"] = encode_virtual_id(idx, series.get("series_id", 0))
                        series_copy["category_id"] = encode_virtual_id(idx, series.get("category_id", 0))
                        result.append(series_copy)
            return result

        elif action == "get_series_info":
            virtual_series_id = request.query_params.get("series_id", "")
            source_idx, original_id = decode_virtual_id(virtual_series_id)
            source = get_source_by_index(source_idx)
            if not source:
                return JSONResponse({"error": "Source not found"}, status_code=404)
            
            host = source["host"].rstrip("/")
            params = {
                "username": source["username"],
                "password": source["password"],
                "action": "get_series_info",
                "series_id": original_id
            }
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            
            if response.status_code == 200:
                data = response.json()
                if "episodes" in data:
                    for season, episodes in data["episodes"].items():
                        for ep in episodes:
                            if "id" in ep:
                                ep["id"] = encode_virtual_id(source_idx, ep["id"])
                return data
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        elif action == "get_vod_info":
            virtual_vod_id = request.query_params.get("vod_id", "")
            source_idx, original_id = decode_virtual_id(virtual_vod_id)
            source = get_source_by_index(source_idx)
            if not source:
                return JSONResponse({"error": "Source not found"}, status_code=404)
            
            host = source["host"].rstrip("/")
            params = {
                "username": source["username"],
                "password": source["password"],
                "action": "get_vod_info",
                "vod_id": original_id
            }
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        else:
            return JSONResponse({"error": f"Unknown action: {action}"}, status_code=400)

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        logger.error(f"Merged API error: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)


# ============================================
# MERGED STREAM PROXY ROUTES
# ============================================


@app.get("/merged/live/{username}/{password}/{stream_id}")
@app.get("/merged/live/{username}/{password}/{stream_id}.{ext}")
@app.get("/merged/{username}/{password}/{stream_id}")
@app.get("/merged/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_merged(request: Request, username: str, password: str, stream_id: str, ext: str = "ts"):
    """Proxy live stream using virtual ID to find correct source."""
    source_idx, original_id = decode_virtual_id(stream_id)
    source = get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{original_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/merged/movie/{username}/{password}/{stream_id}")
@app.get("/merged/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_merged(request: Request, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy VOD/movie stream using virtual ID."""
    source_idx, original_id = decode_virtual_id(stream_id)
    source = get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{original_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/merged/series/{username}/{password}/{stream_id}")
@app.get("/merged/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_merged(request: Request, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy series stream using virtual ID."""
    source_idx, original_id = decode_virtual_id(stream_id)
    source = get_source_by_index(source_idx)
    if not source:
        return Response(content="Source not found", status_code=404)
    
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{original_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ============================================
# PER-SOURCE DEDICATED ROUTES
# ============================================


def get_source_by_route(route_name):
    """Get source configuration by its route name"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("route") == route_name and source.get("enabled", True):
            return source
    return None


@app.get("/{source_route}/player_api.php")
async def player_api_source(source_route: str, request: Request):
    """Per-source Xtream Codes API endpoint."""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)
    
    source_id = source.get("id")
    source_filters = source.get("filters", {})
    prefix = source.get("prefix", "")
    action = request.query_params.get("action", "")
    client = await get_http_client()

    try:
        host = source["host"].rstrip("/")
        
        if not action:
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + f"/{source_route}"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action == "get_live_categories":
            group_filters = source_filters.get("live", {}).get("groups", [])
            categories = get_cached("live_categories", source_id)
            result = []
            for cat in categories:
                cat_name = safe_get_category_name(cat)
                if should_include(cat_name, group_filters):
                    cat_copy = safe_copy_category(cat)
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return result

        elif action == "get_vod_categories":
            group_filters = source_filters.get("vod", {}).get("groups", [])
            categories = get_cached("vod_categories", source_id)
            result = []
            for cat in categories:
                cat_name = safe_get_category_name(cat)
                if should_include(cat_name, group_filters):
                    cat_copy = safe_copy_category(cat)
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return result

        elif action == "get_series_categories":
            group_filters = source_filters.get("series", {}).get("groups", [])
            categories = get_cached("series_categories", source_id)
            result = []
            for cat in categories:
                cat_name = safe_get_category_name(cat)
                if should_include(cat_name, group_filters):
                    cat_copy = safe_copy_category(cat)
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return result

        elif action == "get_live_streams":
            group_filters = source_filters.get("live", {}).get("groups", [])
            channel_filters = source_filters.get("live", {}).get("channels", [])
            streams = get_cached("live_streams", source_id)
            categories = get_cached("live_categories", source_id)
            cat_map = build_category_map(categories)
            
            result = []
            for stream in streams:
                cat_id = str(stream.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                channel_name = stream.get("name", "")
                if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                    result.append(stream)
            return result

        elif action == "get_vod_streams":
            group_filters = source_filters.get("vod", {}).get("groups", [])
            channel_filters = source_filters.get("vod", {}).get("channels", [])
            streams = get_cached("vod_streams", source_id)
            categories = get_cached("vod_categories", source_id)
            cat_map = build_category_map(categories)
            
            result = []
            for stream in streams:
                cat_id = str(stream.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                channel_name = stream.get("name", "")
                if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                    result.append(stream)
            return result

        elif action == "get_series":
            group_filters = source_filters.get("series", {}).get("groups", [])
            channel_filters = source_filters.get("series", {}).get("channels", [])
            series_list = get_cached("series", source_id)
            categories = get_cached("series_categories", source_id)
            cat_map = build_category_map(categories)
            
            result = []
            for s in series_list:
                cat_id = str(s.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                series_name = s.get("name", "")
                if should_include(group_name, group_filters) and should_include(series_name, channel_filters):
                    result.append(s)
            return result

        elif action == "get_series_info":
            series_id = request.query_params.get("series_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "series_id": series_id}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        elif action == "get_vod_info":
            vod_id = request.query_params.get("vod_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "vod_id": vod_id}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        else:
            params = dict(request.query_params)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# Per-source stream proxy routes
@app.get("/{source_route}/live/{username}/{password}/{stream_id}")
@app.get("/{source_route}/live/{username}/{password}/{stream_id}.{ext}")
@app.get("/{source_route}/{username}/{password}/{stream_id}")
@app.get("/{source_route}/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_source(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "ts"):
    """Proxy live stream requests from the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/{source_route}/movie/{username}/{password}/{stream_id}")
@app.get("/{source_route}/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_source(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy VOD/movie stream requests from the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/{source_route}/series/{username}/{password}/{stream_id}")
@app.get("/{source_route}/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_source(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy series stream requests from the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ============================================
# PER-SOURCE FULL (UNFILTERED) ROUTES
# ============================================


@app.get("/{source_route}/full/player_api.php")
async def player_api_source_full(source_route: str, request: Request):
    """Per-source UNFILTERED Xtream Codes API endpoint."""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)
    
    source_id = source.get("id")
    prefix = source.get("prefix", "")
    action = request.query_params.get("action", "")
    client = await get_http_client()

    try:
        host = source["host"].rstrip("/")
        
        if not action:
            response = await client.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                timeout=30.0,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = str(request.base_url).rstrip("/") + f"/{source_route}/full"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return data
            return Response(content=response.content, status_code=response.status_code)

        elif action == "get_live_categories":
            categories = get_cached("live_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = safe_copy_category(cat)
                    cat_copy["category_name"] = f"{prefix}{safe_get_category_name(cat)}"
                    result.append(cat_copy)
                return result
            return categories

        elif action == "get_vod_categories":
            categories = get_cached("vod_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = safe_copy_category(cat)
                    cat_copy["category_name"] = f"{prefix}{safe_get_category_name(cat)}"
                    result.append(cat_copy)
                return result
            return categories

        elif action == "get_series_categories":
            categories = get_cached("series_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = safe_copy_category(cat)
                    cat_copy["category_name"] = f"{prefix}{safe_get_category_name(cat)}"
                    result.append(cat_copy)
                return result
            return categories

        elif action == "get_live_streams":
            return get_cached("live_streams", source_id)

        elif action == "get_vod_streams":
            return get_cached("vod_streams", source_id)

        elif action == "get_series":
            return get_cached("series", source_id)

        elif action == "get_series_info":
            series_id = request.query_params.get("series_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "series_id": series_id}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        elif action == "get_vod_info":
            vod_id = request.query_params.get("vod_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "vod_id": vod_id}
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

        else:
            params = dict(request.query_params)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
            return Response(content=response.content, status_code=response.status_code, media_type="application/json")

    except httpx.TimeoutException:
        return JSONResponse({"error": "Upstream server timeout"}, status_code=504)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# Per-source FULL stream proxy routes
@app.get("/{source_route}/full/live/{username}/{password}/{stream_id}")
@app.get("/{source_route}/full/live/{username}/{password}/{stream_id}.{ext}")
@app.get("/{source_route}/full/{username}/{password}/{stream_id}")
@app.get("/{source_route}/full/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream_source_full(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "ts"):
    """Proxy live stream requests from the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/{source_route}/full/movie/{username}/{password}/{stream_id}")
@app.get("/{source_route}/full/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream_source_full(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy VOD/movie stream requests from the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/{source_route}/full/series/{username}/{password}/{stream_id}")
@app.get("/{source_route}/full/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream_source_full(request: Request, source_route: str, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy series stream requests from the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(content=f"Source route '{source_route}' not found", status_code=404)
    host = source["host"].rstrip("/")
    upstream_url = f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ============================================
# LEGACY STREAM PROXY ROUTES
# ============================================


@app.get("/live/{username}/{password}/{stream_id}")
@app.get("/live/{username}/{password}/{stream_id}.{ext}")
async def proxy_live_stream(request: Request, username: str, password: str, stream_id: str, ext: str = "ts"):
    """Proxy live stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "live")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="live")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/movie/{username}/{password}/{stream_id}")
@app.get("/movie/{username}/{password}/{stream_id}.{ext}")
async def proxy_movie_stream(request: Request, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy VOD/movie stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "vod")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/movie/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="vod")
    return RedirectResponse(url=upstream_url, status_code=302)


@app.get("/series/{username}/{password}/{stream_id}")
@app.get("/series/{username}/{password}/{stream_id}.{ext}")
async def proxy_series_stream(request: Request, username: str, password: str, stream_id: str, ext: str = "mp4"):
    """Proxy series stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "series")
    if not host:
        return Response(content="Source not found", status_code=404)
    upstream_url = f"{host}/series/{upstream_user}/{upstream_pass}/{stream_id}.{ext}"
    
    if get_proxy_enabled():
        return await proxy_stream(upstream_url, request, stream_type="series")
    return RedirectResponse(url=upstream_url, status_code=302)


# ============================================
# XMLTV PROXY
# ============================================


@app.get("/xmltv.php")
@app.get("/merged/xmltv.php")
async def proxy_xmltv():
    """Proxy EPG/XMLTV requests"""
    config = load_config()
    enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
    
    # Fallback to legacy config
    if not enabled_sources and config.get("xtream", {}).get("host"):
        xtream = config["xtream"]
        host = xtream["host"].rstrip("/")
        
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as stream_client:
                async with stream_client.stream(
                    "GET",
                    f"{host}/xmltv.php",
                    params={"username": xtream["username"], "password": xtream["password"]},
                    headers=HEADERS,
                ) as response:
                    async def generate():
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            yield chunk
                    
                    return StreamingResponse(
                        generate(),
                        status_code=response.status_code,
                        media_type=response.headers.get("content-type", "application/xml"),
                    )
        except Exception as e:
            return Response(content=f"<!-- Error: {e} -->", media_type="application/xml")
    
    # Use first source for EPG
    if enabled_sources:
        source = enabled_sources[0]
        host = source["host"].rstrip("/")
        
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as stream_client:
                async with stream_client.stream(
                    "GET",
                    f"{host}/xmltv.php",
                    params={"username": source["username"], "password": source["password"]},
                    headers=HEADERS,
                ) as response:
                    async def generate():
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            yield chunk
                    
                    return StreamingResponse(
                        generate(),
                        status_code=response.status_code,
                        media_type=response.headers.get("content-type", "application/xml"),
                    )
        except Exception as e:
            return Response(content=f"<!-- Error: {e} -->", media_type="application/xml")
    
    return Response(content="<!-- No sources configured -->", media_type="application/xml")


# ============================================
# CACHE MANAGEMENT API
# ============================================


@app.get("/api/options")
async def get_options():
    """Get current options"""
    config = load_config()
    return config.get("options", {})


@app.post("/api/options")
async def update_options(request: Request):
    """Update options"""
    config = load_config()
    data = await request.json()
    
    if "options" not in config:
        config["options"] = {}
    
    for key, value in data.items():
        config["options"][key] = value
    
    save_config(config)
    return {"status": "ok", "options": config["options"]}


@app.get("/api/options/proxy")
async def get_proxy_status():
    """Get proxy streaming status"""
    return {"proxy_enabled": get_proxy_enabled()}


@app.post("/api/options/proxy")
async def set_proxy_status(request: Request):
    """Enable or disable proxy streaming"""
    config = load_config()
    data = await request.json()
    
    if "options" not in config:
        config["options"] = {}
    
    config["options"]["proxy_streams"] = data.get("enabled", True)
    save_config(config)
    
    return {"status": "ok", "proxy_enabled": config["options"]["proxy_streams"]}


@app.get("/api/cache/status")
async def cache_status():
    """Get cache status and statistics"""
    refresh_progress = load_refresh_progress()
    refresh_in_progress = refresh_progress.get("in_progress", False)
    
    async with _cache_lock:
        last_refresh = _api_cache.get("last_refresh")
        sources_cache = _api_cache.get("sources", {})
    
    live_streams = sum(len(s.get("live_streams", [])) for s in sources_cache.values())
    vod_streams = sum(len(s.get("vod_streams", [])) for s in sources_cache.values())
    series_count = sum(len(s.get("series", [])) for s in sources_cache.values())
    live_cats = sum(len(s.get("live_categories", [])) for s in sources_cache.values())
    vod_cats = sum(len(s.get("vod_categories", [])) for s in sources_cache.values())
    series_cats = sum(len(s.get("series_categories", [])) for s in sources_cache.values())

    cache_valid = is_cache_valid()
    ttl = get_cache_ttl()

    next_refresh = None
    if last_refresh:
        try:
            last_time = datetime.fromisoformat(last_refresh)
            next_time = last_time + timedelta(seconds=ttl)
            next_refresh = next_time.isoformat()
        except (ValueError, TypeError):
            pass
    
    sources_info = {}
    for source_id, source_cache in sources_cache.items():
        sources_info[source_id] = {
            "live_streams": len(source_cache.get("live_streams", [])),
            "vod_streams": len(source_cache.get("vod_streams", [])),
            "series": len(source_cache.get("series", [])),
            "last_refresh": source_cache.get("last_refresh"),
        }

    return {
        "last_refresh": last_refresh,
        "next_refresh": next_refresh,
        "cache_valid": cache_valid,
        "refresh_in_progress": refresh_in_progress,
        "refresh_progress": refresh_progress,
        "ttl_seconds": ttl,
        "sources_count": len(sources_cache),
        "counts": {
            "live_categories": live_cats,
            "vod_categories": vod_cats,
            "series_categories": series_cats,
            "live_streams": live_streams,
            "vod_streams": vod_streams,
            "series": series_count,
        },
        "sources": sources_info,
    }


@app.post("/api/cache/refresh")
async def trigger_cache_refresh():
    """Manually trigger a cache refresh"""
    asyncio.create_task(refresh_cache())
    return {"status": "refresh_started", "message": "Cache refresh has been triggered in the background"}


@app.post("/api/cache/cancel-refresh")
async def cancel_cache_refresh():
    """Cancel/clear a stuck refresh state"""
    clear_refresh_progress()
    async with _cache_lock:
        _api_cache["refresh_in_progress"] = False
    return {"status": "ok", "message": "Refresh state cleared"}


@app.post("/api/cache/clear")
async def clear_cache():
    """Clear the cache"""
    global _api_cache, _stream_source_map
    async with _cache_lock:
        _api_cache = {
            "sources": {},
            "last_refresh": None,
            "refresh_in_progress": False,
        }
    
    async with _stream_map_lock:
        _stream_source_map = {"live": {}, "vod": {}, "series": {}}

    if os.path.exists(API_CACHE_FILE):
        os.remove(API_CACHE_FILE)

    return {"status": "ok", "message": "Cache cleared"}


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok"}


# ============================================
# MAIN ENTRY POINT
# ============================================


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)

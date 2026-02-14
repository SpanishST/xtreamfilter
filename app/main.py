import asyncio
import hashlib
import io
import json
import logging
import os
import re
import shutil
import time
import unicodedata
import uuid
from pathlib import Path

from lxml import etree
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import httpx
from rapidfuzz import fuzz
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

# Application version
APP_VERSION = "0.3.0"
GITHUB_REPO = "SpanishST/xtreamfilter"

# Data directory - use environment variable or default to /data (Docker) or ./data (local)
DATA_DIR = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "./data")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
CACHE_FILE = os.path.join(DATA_DIR, "playlist_cache.m3u")
API_CACHE_FILE = os.path.join(DATA_DIR, "api_cache.json")
PROGRESS_FILE = os.path.join(DATA_DIR, "refresh_progress.json")
CATEGORIES_FILE = os.path.join(DATA_DIR, "categories.json")
CART_FILE = os.path.join(DATA_DIR, "cart.json")
EPG_CACHE_FILE = os.path.join(DATA_DIR, "epg_cache.xml")

# Templates
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Headers to mimic a browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Cache for version check (avoid hitting GitHub API too often)
_version_cache = {"latest": None, "checked_at": 0}

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

# EPG cache - stores merged XMLTV data
_epg_cache = {
    "data": None,  # bytes of merged XMLTV
    "last_refresh": None,
    "refresh_in_progress": False,
}
_epg_cache_lock = asyncio.Lock()


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


def get_refresh_interval():
    """Get cache refresh interval from config (in seconds).
    
    This controls how often the background task checks and refreshes the cache.
    Defaults to 3600 seconds (1 hour). Minimum is 300 seconds (5 minutes).
    """
    config = load_config()
    interval = config.get("options", {}).get("refresh_interval", 3600)
    return max(interval, 300)  # Minimum 5 minutes to avoid API abuse


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


# ============================================
# EPG CACHING SYSTEM
# ============================================


def get_epg_cache_ttl():
    """Get EPG cache TTL from config (in seconds). Defaults to 6 hours."""
    config = load_config()
    return config.get("options", {}).get("epg_cache_ttl", 21600)  # Default: 6 hours


def is_epg_cache_valid():
    """Check if EPG cache is still valid based on TTL"""
    last_refresh = _epg_cache.get("last_refresh")

    if not last_refresh or _epg_cache.get("data") is None:
        return False

    try:
        last_time = datetime.fromisoformat(last_refresh)
        age = (datetime.now() - last_time).total_seconds()
        return age < get_epg_cache_ttl()
    except (ValueError, TypeError):
        return False


def load_epg_cache_from_disk():
    """Load EPG cache from disk on startup"""
    global _epg_cache
    if os.path.exists(EPG_CACHE_FILE):
        try:
            with open(EPG_CACHE_FILE, "rb") as f:
                _epg_cache["data"] = f.read()
            # Load metadata from a companion JSON file
            meta_file = EPG_CACHE_FILE + ".meta"
            if os.path.exists(meta_file):
                with open(meta_file) as f:
                    meta = json.load(f)
                    _epg_cache["last_refresh"] = meta.get("last_refresh")
            logger.info(f"Loaded EPG cache from disk. Last refresh: {_epg_cache.get('last_refresh', 'Unknown')}")
        except Exception as e:
            logger.error(f"Failed to load EPG cache from disk: {e}")


def save_epg_cache_to_disk():
    """Save EPG cache to disk"""
    try:
        if _epg_cache.get("data"):
            os.makedirs(os.path.dirname(EPG_CACHE_FILE), exist_ok=True)
            with open(EPG_CACHE_FILE, "wb") as f:
                f.write(_epg_cache["data"])
            # Save metadata to a companion JSON file
            meta_file = EPG_CACHE_FILE + ".meta"
            with open(meta_file, "w") as f:
                json.dump({"last_refresh": _epg_cache.get("last_refresh")}, f)
            logger.info(f"EPG cache saved to disk at {datetime.now().isoformat()}")
    except Exception as e:
        logger.error(f"Failed to save EPG cache to disk: {e}")


async def fetch_epg_from_source(source: dict) -> bytes | None:
    """Fetch XMLTV data from a single source."""
    host = source["host"].rstrip("/")
    url = f"{host}/xmltv.php"
    params = {"username": source["username"], "password": source["password"]}
    
    try:
        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0),
            follow_redirects=True,
        ) as client:
            response = await client.get(url, params=params)
            if response.status_code == 200:
                logger.info(f"Fetched EPG from source '{source.get('name', source['id'])}': {len(response.content)} bytes")
                return response.content
            else:
                logger.warning(f"Failed to fetch EPG from source '{source.get('name', source['id'])}': HTTP {response.status_code}")
    except Exception as e:
        logger.error(f"Error fetching EPG from source '{source.get('name', source['id'])}': {e}")
    
    return None


def get_filtered_epg_ids_for_source(source: dict) -> set:
    """
    Get the set of valid EPG channel IDs for a source based on its live channel filters.
    Returns prefixed IDs in lowercase format (e.g., 'source_id_channel.epg').
    """
    source_id = source.get("id", "unknown")
    filters = source.get("filters", {})
    live_filters = filters.get("live", {"groups": [], "channels": []})
    group_filters = live_filters.get("groups", [])
    channel_filters = live_filters.get("channels", [])
    
    # Get cached data for this source
    categories = _api_cache.get("sources", {}).get(source_id, {}).get("live_categories", [])
    streams = _api_cache.get("sources", {}).get(source_id, {}).get("live_streams", [])
    
    if not streams:
        logger.debug(f"No cached streams for source '{source_id}', including all EPG channels")
        return None  # None means include all (no filter applied)
    
    # Build category map
    cat_map = {}
    for cat in categories:
        if isinstance(cat, dict):
            cat_id = str(cat.get("category_id", ""))
            cat_name = cat.get("category_name", "")
            cat_map[cat_id] = cat_name
    
    valid_epg_ids = set()
    
    for stream in streams:
        name = stream.get("name", "Unknown")
        cat_id = str(stream.get("category_id", ""))
        group = cat_map.get(cat_id, "Unknown")
        epg_id = stream.get("epg_channel_id", "")
        
        if not epg_id:
            continue
        
        # Apply group filters
        if group_filters:
            include_rules = [r for r in group_filters if r.get("type") == "include"]
            exclude_rules = [r for r in group_filters if r.get("type") == "exclude"]
            
            excluded = False
            for rule in exclude_rules:
                if matches_filter(group, rule):
                    excluded = True
                    break
            if excluded:
                continue
            
            if include_rules:
                included = False
                for rule in include_rules:
                    if matches_filter(group, rule):
                        included = True
                        break
                if not included:
                    continue
        
        # Apply channel filters
        if channel_filters:
            include_rules = [r for r in channel_filters if r.get("type") == "include"]
            exclude_rules = [r for r in channel_filters if r.get("type") == "exclude"]
            
            excluded = False
            for rule in exclude_rules:
                if matches_filter(name, rule):
                    excluded = True
                    break
            if excluded:
                continue
            
            if include_rules:
                included = False
                for rule in include_rules:
                    if matches_filter(name, rule):
                        included = True
                        break
                if not included:
                    continue
        
        # This channel passed all filters - add its prefixed EPG ID
        prefixed_epg_id = f"{source_id}_{epg_id}".lower()
        valid_epg_ids.add(prefixed_epg_id)
    
    logger.debug(f"Source '{source_id}': {len(valid_epg_ids)} EPG IDs pass filters (from {len(streams)} streams)")
    return valid_epg_ids


async def refresh_epg_cache():
    """Fetch and merge EPG from all enabled sources, filtered to match channel filters."""
    global _epg_cache
    
    async with _epg_cache_lock:
        if _epg_cache.get("refresh_in_progress"):
            logger.info("EPG refresh already in progress, skipping")
            return
        _epg_cache["refresh_in_progress"] = True
    
    try:
        config = load_config()
        enabled_sources = [s for s in config.get("sources", []) if s.get("enabled", True)]
        
        if not enabled_sources:
            logger.warning("No enabled sources for EPG refresh")
            return
        
        logger.info(f"Starting EPG refresh from {len(enabled_sources)} source(s)")
        
        # Build set of valid EPG IDs for each source based on filters
        source_epg_filters = {}
        for source in enabled_sources:
            source_id = source.get("id", "unknown")
            valid_ids = get_filtered_epg_ids_for_source(source)
            source_epg_filters[source_id] = valid_ids
            if valid_ids is not None:
                logger.info(f"Source '{source.get('name', source_id)}': filtering EPG to {len(valid_ids)} channels")
            else:
                logger.info(f"Source '{source.get('name', source_id)}': no cache data, including all EPG channels")
        
        # Fetch EPG from all sources in parallel
        tasks = [fetch_epg_from_source(source) for source in enabled_sources]
        results = await asyncio.gather(*tasks)
        
        # Create merged XMLTV document
        merged_root = etree.Element("tv", attrib={"generator-info-name": "XtreamFilter Merged EPG"})
        
        total_stats = {"channels_included": 0, "channels_excluded": 0, "programmes_included": 0, "programmes_excluded": 0}
        
        for source, epg_data in zip(enabled_sources, results):
            if epg_data is None:
                continue
            
            source_id = source.get("id", "unknown")
            valid_epg_ids = source_epg_filters.get(source_id)
            
            try:
                # Parse the XMLTV data
                parser = etree.XMLParser(recover=True)  # Recover from malformed XML
                tree = etree.parse(io.BytesIO(epg_data), parser)
                root = tree.getroot()
                
                source_stats = {"channels": 0, "channels_excluded": 0, "programmes": 0, "programmes_excluded": 0}
                
                # Process channel elements - prefix the ID with source_id and normalize to lowercase
                for channel in root.findall("channel"):
                    original_id = channel.get("id", "")
                    if original_id:
                        # Prefix the channel ID with source_id and normalize to lowercase for consistent matching
                        new_id = f"{source_id}_{original_id}".lower()
                        
                        # Filter: only include if valid_epg_ids is None (no filter) or ID is in the set
                        if valid_epg_ids is not None and new_id not in valid_epg_ids:
                            source_stats["channels_excluded"] += 1
                            continue
                        
                        channel.set("id", new_id)
                    merged_root.append(channel)
                    source_stats["channels"] += 1
                
                # Process programme elements - prefix the channel reference and normalize to lowercase
                for programme in root.findall("programme"):
                    original_channel = programme.get("channel", "")
                    if original_channel:
                        # Prefix the channel reference with source_id and normalize to lowercase
                        new_channel = f"{source_id}_{original_channel}".lower()
                        
                        # Filter: only include if valid_epg_ids is None (no filter) or channel is in the set
                        if valid_epg_ids is not None and new_channel not in valid_epg_ids:
                            source_stats["programmes_excluded"] += 1
                            continue
                        
                        programme.set("channel", new_channel)
                    merged_root.append(programme)
                    source_stats["programmes"] += 1
                
                total_stats["channels_included"] += source_stats["channels"]
                total_stats["channels_excluded"] += source_stats["channels_excluded"]
                total_stats["programmes_included"] += source_stats["programmes"]
                total_stats["programmes_excluded"] += source_stats["programmes_excluded"]
                
                logger.info(f"Merged EPG from source '{source.get('name', source_id)}': "
                           f"{source_stats['channels']} channels (+{source_stats['channels_excluded']} filtered), "
                           f"{source_stats['programmes']} programmes (+{source_stats['programmes_excluded']} filtered)")
                
            except Exception as e:
                logger.error(f"Error parsing EPG from source '{source.get('name', source_id)}': {e}")
                continue
        
        # Serialize the merged document
        merged_data = etree.tostring(
            merged_root,
            encoding="UTF-8",
            xml_declaration=True,
            pretty_print=False
        )
        
        # Update cache
        async with _epg_cache_lock:
            _epg_cache["data"] = merged_data
            _epg_cache["last_refresh"] = datetime.now().isoformat()
        
        # Save to disk
        save_epg_cache_to_disk()
        
        logger.info(f"EPG refresh complete. Total: {total_stats['channels_included']} channels, "
                   f"{total_stats['programmes_included']} programmes. "
                   f"Filtered out: {total_stats['channels_excluded']} channels, {total_stats['programmes_excluded']} programmes. "
                   f"Size: {len(merged_data)} bytes ({len(merged_data) / 1024 / 1024:.1f} MB)")
        
    except Exception as e:
        logger.error(f"EPG refresh failed: {e}")
    finally:
        async with _epg_cache_lock:
            _epg_cache["refresh_in_progress"] = False


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


def get_cached_with_source_info(key, category_key):
    """Get cached data with source information attached to each item.
    
    Returns a list of items where each item has source_id and source_name added.
    Also returns a merged category map from all sources.
    """
    sources = _api_cache.get("sources", {})
    config = load_config()
    
    # Build source name lookup
    source_names = {}
    for src in config.get("sources", []):
        source_names[src.get("id")] = src.get("name", "Unknown")
    
    result = []
    all_categories = []
    
    for src_id, src_cache in sources.items():
        src_name = source_names.get(src_id, "Unknown")
        items = src_cache.get(key, [])
        categories = src_cache.get(category_key, [])
        all_categories.extend(categories)
        
        for item in items:
            item_copy = dict(item)
            item_copy["_source_id"] = src_id
            item_copy["_source_name"] = src_name
            result.append(item_copy)
    
    return result, all_categories


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
            
            # Refresh pattern-based categories
            save_refresh_progress({
                "in_progress": True,
                "current_source": total_sources,
                "total_sources": total_sources,
                "current_source_name": "Categories",
                "current_step": "Refreshing automatic categories...",
                "percent": 95
            })
            await refresh_pattern_categories_async()

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
            refresh_interval = get_refresh_interval()

            if not is_cache_valid():
                logger.info("Cache expired, triggering refresh...")
                await refresh_cache()
            else:
                last_refresh = _api_cache.get("last_refresh", "Never")
                logger.info(f"Cache still valid. Last refresh: {last_refresh}")

            logger.debug(f"Next cache check in {refresh_interval} seconds")
            await asyncio.sleep(refresh_interval)

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
        "options": {
            "cache_enabled": True,
            "cache_ttl": 3600,
            "refresh_interval": 3600,
            "proxy_streams": True,
            "telegram": {
                "enabled": False,
                "bot_token": "",
                "chat_id": ""
            }
        },
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
# FAVORITES FUNCTIONS
# ============================================
# CUSTOM CATEGORIES FUNCTIONS
# ============================================


def load_categories():
    """Load categories from file, with migration from old favorites.json"""
    default_categories = {
        "categories": [{
            "id": "favorites",
            "name": "Favorite streams",
            "icon": "❤️",
            "mode": "manual",
            "content_types": ["live", "vod", "series"],
            "items": [],
            "patterns": [],
            "pattern_logic": "or",
            "use_source_filters": False,
            "cached_items": [],
            "last_refresh": None
        }]
    }
    
    if os.path.exists(CATEGORIES_FILE):
        try:
            with open(CATEGORIES_FILE) as f:
                data = json.load(f)
                if "categories" in data:
                    return data
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error loading categories: {e}")
    
    # Check for old favorites.json to migrate
    old_favorites_file = os.path.join(DATA_DIR, "favorites.json")
    if os.path.exists(old_favorites_file):
        try:
            with open(old_favorites_file) as f:
                old_favorites = json.load(f)
            
            # Migrate old favorites to new category structure
            migrated_items = []
            for content_type in ["live", "vod", "series"]:
                for fav in old_favorites.get(content_type, []):
                    migrated_items.append({
                        "id": fav.get("id"),
                        "source_id": fav.get("source_id"),
                        "content_type": content_type,
                        "added_at": fav.get("added_at", datetime.now().isoformat())
                    })
            
            if migrated_items:
                default_categories["categories"][0]["items"] = migrated_items
                logger.info(f"Migrated {len(migrated_items)} items from favorites.json")
            
            # Save migrated data
            save_categories(default_categories)
            
            # Rename old file
            os.rename(old_favorites_file, old_favorites_file + ".bak")
            logger.info("Renamed old favorites.json to favorites.json.bak")
            
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error migrating favorites: {e}")
    
    return default_categories


def save_categories(data):
    """Save categories to file"""
    os.makedirs(os.path.dirname(CATEGORIES_FILE), exist_ok=True)
    with open(CATEGORIES_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ============================================
# DOWNLOAD CART FUNCTIONS
# ============================================

# In-memory download state
_download_cart: list = []
_download_task: Optional[asyncio.Task] = None
_download_cancel_event: Optional[asyncio.Event] = None
_download_current_item: Optional[dict] = None
_download_progress: dict = {"bytes_downloaded": 0, "total_bytes": 0, "speed": 0, "paused": False, "pause_remaining": 0}


def load_cart() -> list:
    """Load download cart from file"""
    global _download_cart
    if os.path.exists(CART_FILE):
        try:
            with open(CART_FILE) as f:
                data = json.load(f)
                _download_cart = data.get("items", [])
                return _download_cart
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error loading cart: {e}")
    _download_cart = []
    return _download_cart


def save_cart(items: list = None):
    """Save download cart to file"""
    global _download_cart
    if items is not None:
        _download_cart = items
    os.makedirs(os.path.dirname(CART_FILE), exist_ok=True)
    with open(CART_FILE, "w") as f:
        json.dump({"items": _download_cart}, f, indent=2)


def get_download_path() -> str:
    """Get the configured download path"""
    config = load_config()
    return config.get("options", {}).get("download_path", "/data/downloads")


def get_download_temp_path() -> str:
    """Get the configured temporary download path.
    Downloads go here first, then are moved to the final destination on completion."""
    config = load_config()
    return config.get("options", {}).get("download_temp_path", "/data/downloads/.tmp")


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename"""
    # Normalize unicode
    name = unicodedata.normalize("NFKD", name)
    # Remove invalid filesystem characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    # Replace multiple spaces/dots
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.strip('.')
    return name or "untitled"


def build_download_filepath(item: dict, ext_override: str = None) -> str:
    """Build the destination file path for a download item.
    If ext_override is provided, use that instead of the item's container_extension."""
    base_path = get_download_path()
    ext = ext_override or item.get("container_extension", "mp4")
    name = sanitize_filename(item.get("name", "untitled"))

    if item.get("content_type") == "vod":
        # Films/<name>.<ext>
        return os.path.join(base_path, "Films", f"{name}.{ext}")
    elif item.get("content_type") == "series":
        series_name = sanitize_filename(item.get("series_name", name))
        season = item.get("season", "1")
        episode = item.get("episode_num", 1)
        ep_title = item.get("episode_title", "")
        season_str = f"S{int(season):02d}"
        episode_str = f"E{int(episode):02d}"
        if ep_title and ep_title != name:
            ep_title_clean = sanitize_filename(ep_title)
            filename = f"{series_name} {season_str}{episode_str} - {ep_title_clean}.{ext}"
        else:
            filename = f"{series_name} {season_str}{episode_str}.{ext}"
        return os.path.join(base_path, "Series", series_name, season_str, filename)
    else:
        return os.path.join(base_path, f"{name}.{ext}")


def _resolve_output_format(item_ext: str, allowed_formats: list) -> str:
    """Resolve the correct file extension based on provider's allowed output formats.
    
    If the item's extension (e.g. mp4, mkv) is in the allowed list, use it as-is.
    Otherwise, pick the best alternative from the allowed list.
    Priority: ts > mp4 > mkv > m3u8 (ts is widely supported for Xtream downloads).
    """
    if not allowed_formats:
        # No info from provider, keep original
        return item_ext
    
    # Normalize
    item_ext_lower = item_ext.lower()
    allowed_lower = [f.lower().strip() for f in allowed_formats]
    
    if item_ext_lower in allowed_lower:
        return item_ext  # Original extension is allowed
    
    # Pick the best alternative
    preferred_order = ["ts", "mp4", "mkv", "m3u8"]
    for pref in preferred_order:
        if pref in allowed_lower:
            logger.info(f"Format '{item_ext}' not in allowed formats {allowed_formats}, using '{pref}' instead")
            return pref
    
    # Last resort: use the first allowed format
    if allowed_lower:
        fallback = allowed_lower[0]
        logger.info(f"Format '{item_ext}' not in allowed formats {allowed_formats}, falling back to '{fallback}'")
        return fallback
    
    return item_ext


def build_upstream_url(item: dict) -> Optional[str]:
    """Build the upstream URL for downloading an item.
    Always uses the item's original container_extension — VOD/series files have
    a fixed format on the server. allowed_output_formats only applies to live transcoding."""
    source = get_source_by_id(item.get("source_id", ""))
    if not source:
        return None
    host = source["host"].rstrip("/")
    username = source["username"]
    password = source["password"]
    stream_id = item.get("stream_id", "")
    ext = item.get("container_extension", "mp4")

    content_type = item.get("content_type", "vod")
    if content_type == "vod":
        return f"{host}/movie/{username}/{password}/{stream_id}.{ext}"
    elif content_type == "series":
        return f"{host}/series/{username}/{password}/{stream_id}.{ext}"
    return None


def get_download_throttle_settings() -> dict:
    """Get download throttle settings from config"""
    config = load_config()
    opts = config.get("options", {})
    return {
        "bandwidth_limit": opts.get("download_bandwidth_limit", 0),       # KB/s, 0 = unlimited
        "pause_interval": opts.get("download_pause_interval", 0),         # seconds between pauses, 0 = disabled
        "pause_duration": opts.get("download_pause_duration", 0),         # seconds to pause
        "player_profile": opts.get("download_player_profile", "tivimate"),  # IPTV player to emulate
    }


# Predefined IPTV player header profiles
# These mimic real players to avoid provider blocks (HTTP 551, etc.)
PLAYER_PROFILES = {
    "tivimate": {
        "name": "TiviMate",
        "headers": {
            "User-Agent": "TiviMate/4.7.0 (Linux; Android 14) OkHttp/4.12.0",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "smarters": {
        "name": "IPTV Smarters Pro",
        "headers": {
            "User-Agent": "IPTVSmartersPro",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "vlc": {
        "name": "VLC Media Player",
        "headers": {
            "User-Agent": "VLC/3.0.20 LibVLC/3.0.20",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "kodi": {
        "name": "Kodi",
        "headers": {
            "User-Agent": "Kodi/21.0 (Linux; Android 14) Kodi_Home/21.0 ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "xciptv": {
        "name": "XCIPTV Player",
        "headers": {
            "User-Agent": "xciptv/6.0.0 (Linux; Android 14) ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "ott_navigator": {
        "name": "OTT Navigator",
        "headers": {
            "User-Agent": "OTT Navigator/1.7.1.3 (Linux; Android 14) ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "ffmpeg": {
        "name": "FFmpeg / Lavf",
        "headers": {
            "User-Agent": "Lavf/60.16.100",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
            "Icy-MetaData": "1",
        },
    },
}


def get_player_headers(profile_key: str = "tivimate") -> dict:
    """Get the HTTP headers for a given IPTV player profile"""
    profile = PLAYER_PROFILES.get(profile_key, PLAYER_PROFILES["tivimate"])
    return dict(profile["headers"])


async def _get_provider_info(source: dict) -> dict:
    """Fetch provider info including connection slots and allowed output formats.
    Returns dict with 'has_free_slot' (bool) and 'allowed_output_formats' (list)."""
    result = {"has_free_slot": True, "allowed_output_formats": []}
    try:
        host = source["host"].rstrip("/")
        params = {
            "username": source["username"],
            "password": source["password"],
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0), follow_redirects=True, headers={"Connection": "close"}) as client:
            resp = await client.get(f"{host}/player_api.php", params=params)
            if resp.status_code == 200:
                data = resp.json()
                ui = data.get("user_info", {})
                active = int(ui.get("active_cons", 0))
                max_cons = int(ui.get("max_connections", 1))
                allowed = ui.get("allowed_output_formats", [])
                if isinstance(allowed, str):
                    allowed = [allowed]
                logger.info(f"Provider {source.get('name', '')}: {active}/{max_cons} connections, allowed formats: {allowed}")
                result["has_free_slot"] = active < max_cons
                result["allowed_output_formats"] = allowed
    except Exception as e:
        logger.warning(f"Could not check provider info: {e}")
    return result


async def _check_provider_connections(source: dict) -> bool:
    """Check if a provider has free connection slots (convenience wrapper)."""
    info = await _get_provider_info(source)
    return info["has_free_slot"]


async def download_worker():
    """Background worker that processes the download cart sequentially.
    Supports bandwidth throttling and periodic pauses to mimic a media player."""
    global _download_current_item, _download_progress, _download_cancel_event
    _download_cancel_event = asyncio.Event()

    # Close the global keepalive HTTP client to free any persistent connections
    # to the provider (Xtream-Codes counts keepalive connections as active)
    await close_http_client()
    logger.info("Closed global HTTP client to free provider connection slots")

    while True:
        # Find next queued item
        queued = [item for item in _download_cart if item.get("status") == "queued"]
        if not queued:
            logger.info("Download queue empty, worker stopping")
            break

        item = queued[0]
        item_id = item["id"]
        _download_current_item = item
        _download_progress = {"bytes_downloaded": 0, "total_bytes": 0, "speed": 0, "paused": False, "pause_remaining": 0}

        # Update status
        item["status"] = "downloading"
        item["progress"] = 0
        save_cart()

        source = get_source_by_id(item.get("source_id", ""))

        upstream_url = build_upstream_url(item)
        if not upstream_url:
            item["status"] = "failed"
            item["error"] = "Could not build upstream URL (source not found)"
            save_cart()
            continue

        file_path = build_download_filepath(item)
        item["file_path"] = file_path

        # Build temp file path — download here first, move to final path on completion
        temp_dir = get_download_temp_path()
        temp_filename = f"{item['id']}_{os.path.basename(file_path)}"
        temp_path = os.path.join(temp_dir, temp_filename)

        logger.info(f"Download URL: {upstream_url} -> {temp_path} (final: {file_path})")

        try:
            # Create directories
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            os.makedirs(temp_dir, exist_ok=True)

            # Read throttle settings at start of each file
            throttle = get_download_throttle_settings()
            bw_limit = throttle["bandwidth_limit"] * 1024  # convert KB/s to B/s
            pause_interval = throttle["pause_interval"]
            pause_duration = throttle["pause_duration"]
            player_profile = throttle["player_profile"]

            # Use a smaller chunk size when bandwidth is limited for finer control
            chunk_size = min(64 * 1024, bw_limit) if bw_limit > 0 else 512 * 1024

            # Use the selected IPTV player profile headers
            player_headers = get_player_headers(player_profile)
            # Use Connection: close to prevent keepalive from counting as active connection
            player_headers["Connection"] = "close"
            logger.info(f"Downloading with player profile '{player_profile}': {player_headers.get('User-Agent', '')}")

            timeout = httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0)

            # Auto-retry logic for connection-limit errors (458, 429, 503)
            max_retries = 5
            retry_base_delay = 30  # seconds — start with 30s, then 60s, 90s, 120s, 150s
            RETRYABLE_CODES = {429, 458, 503, 551}

            for attempt in range(1, max_retries + 1):
                # On retries, just wait with backoff then try again
                # (Don't pre-check connections — with max_connections=1 providers,
                #  the check itself counts as an active connection and creates a deadlock)
                if attempt > 1:
                    delay = retry_base_delay * attempt
                    logger.info(f"Retry {attempt}/{max_retries}: waiting {delay}s before next attempt")
                    _download_progress["paused"] = True
                    _download_progress["pause_remaining"] = delay
                    item["error"] = f"Retrying in {delay}s ({attempt}/{max_retries})"
                    save_cart()
                    remaining = delay
                    while remaining > 0:
                        if _download_cancel_event.is_set():
                            item["status"] = "cancelled"
                            item["error"] = "Cancelled by user"
                            save_cart()
                            _download_current_item = None
                            _download_cancel_event.clear()
                            return
                        sleep_step = min(1.0, remaining)
                        await asyncio.sleep(sleep_step)
                        remaining -= sleep_step
                        _download_progress["pause_remaining"] = round(remaining, 1)
                    _download_progress["paused"] = False
                    _download_progress["pause_remaining"] = 0
                    item["error"] = None

                async with httpx.AsyncClient(headers=player_headers, timeout=timeout, follow_redirects=True) as client:
                    async with client.stream("GET", upstream_url) as response:
                        if response.status_code in RETRYABLE_CODES and attempt < max_retries:
                            delay = retry_base_delay * attempt
                            error_msg = {
                                458: "Max connections reached",
                                429: "Rate limited",
                                503: "Service unavailable",
                                551: "Access denied by provider",
                            }.get(response.status_code, f"HTTP {response.status_code}")
                            logger.warning(f"Download blocked: HTTP {response.status_code} ({error_msg}) — retry {attempt}/{max_retries} in {delay}s")
                            _download_progress["paused"] = True
                            _download_progress["pause_remaining"] = delay
                            item["error"] = f"{error_msg} — retrying in {delay}s ({attempt}/{max_retries})"
                            save_cart()
                            remaining = delay
                            while remaining > 0:
                                if _download_cancel_event.is_set():
                                    item["status"] = "cancelled"
                                    item["error"] = "Cancelled by user"
                                    save_cart()
                                    _download_current_item = None
                                    _download_cancel_event.clear()
                                    return
                                sleep_step = min(1.0, remaining)
                                await asyncio.sleep(sleep_step)
                                remaining -= sleep_step
                                _download_progress["pause_remaining"] = round(remaining, 1)
                            _download_progress["paused"] = False
                            _download_progress["pause_remaining"] = 0
                            item["error"] = None
                            continue  # retry the request

                        if response.status_code >= 400:
                            error_msg = {
                                458: "Max connections reached (all retries exhausted)",
                                429: "Rate limited (all retries exhausted)",
                                503: "Service unavailable (all retries exhausted)",
                                551: "Access denied by provider (possible IP temp-ban from too many attempts)",
                            }.get(response.status_code, f"HTTP {response.status_code}")
                            item["status"] = "failed"
                            item["error"] = error_msg
                            save_cart()
                            # Clean up partial temp file
                            try:
                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                            except OSError:
                                pass
                            break  # break out of retry loop, continue to next item

                        # Success — proceed with download
                        item["error"] = None

                        # Content-Range header may contain total size for 206 responses
                        total = 0
                        content_range = response.headers.get("content-range", "")
                        if content_range and "/" in content_range:
                            try:
                                total = int(content_range.split("/")[-1])
                            except (ValueError, IndexError):
                                pass
                        if not total:
                            total = int(response.headers.get("content-length", 0))
                        _download_progress["total_bytes"] = total
                        downloaded = 0
                        start_time = time.time()
                        last_save = time.time()
                        # Bandwidth throttle: track bytes in the current 1-second window
                        window_start = time.time()
                        window_bytes = 0
                        # Pause timer: track when we last paused
                        last_pause_time = time.time()

                        with open(temp_path, "wb") as f:
                            async for chunk in response.aiter_bytes(chunk_size=chunk_size):
                                if _download_cancel_event.is_set():
                                    item["status"] = "cancelled"
                                    item["error"] = "Cancelled by user"
                                    save_cart()
                                    # Clean up partial temp file
                                    try:
                                        os.remove(temp_path)
                                    except OSError:
                                        pass
                                    _download_current_item = None
                                    _download_cancel_event.clear()
                                    return  # Stop the worker entirely

                                f.write(chunk)
                                downloaded += len(chunk)
                                window_bytes += len(chunk)
                                elapsed = time.time() - start_time
                                speed = downloaded / elapsed if elapsed > 0 else 0

                                _download_progress["bytes_downloaded"] = downloaded
                                _download_progress["speed"] = speed

                                if total > 0:
                                    item["progress"] = round((downloaded / total) * 100, 1)
                                else:
                                    item["progress"] = 0

                                # Persist progress every 5 seconds
                                if time.time() - last_save > 5:
                                    save_cart()
                                    last_save = time.time()

                                # --- Bandwidth throttling ---
                                if bw_limit > 0:
                                    now = time.time()
                                    window_elapsed = now - window_start
                                    if window_bytes >= bw_limit:
                                        # We've used up this second's budget, sleep the rest
                                        sleep_time = max(0, 1.0 - window_elapsed)
                                        if sleep_time > 0:
                                            await asyncio.sleep(sleep_time)
                                        window_start = time.time()
                                        window_bytes = 0
                                    elif window_elapsed >= 1.0:
                                        # Window expired, reset
                                        window_start = now
                                        window_bytes = 0

                                # --- Periodic pause (simulate player buffering) ---
                                if pause_interval > 0 and pause_duration > 0:
                                    now = time.time()
                                    if now - last_pause_time >= pause_interval:
                                        _download_progress["paused"] = True
                                        _download_progress["pause_remaining"] = pause_duration
                                        logger.info(f"Download throttle: pausing for {pause_duration}s (simulating player buffer)")
                                        # Sleep in 1-second increments so cancel is responsive
                                        remaining = pause_duration
                                        while remaining > 0:
                                            if _download_cancel_event.is_set():
                                                break
                                            sleep_step = min(1.0, remaining)
                                            await asyncio.sleep(sleep_step)
                                            remaining -= sleep_step
                                            _download_progress["pause_remaining"] = round(remaining, 1)
                                        _download_progress["paused"] = False
                                        _download_progress["pause_remaining"] = 0
                                        last_pause_time = time.time()
                                        # Reset bandwidth window after pause
                                        window_start = time.time()
                                        window_bytes = 0

                        # Download succeeded, break out of retry loop
                        break

            # Only mark completed if the download actually succeeded
            if item.get("status") == "downloading":
                # Move from temp to final destination
                try:
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    shutil.move(temp_path, file_path)
                    logger.info(f"Moved completed download: {temp_path} -> {file_path}")
                except Exception as e:
                    logger.error(f"Failed to move download to final path: {e}")
                    # If move fails (e.g. cross-device), try copy+delete
                    try:
                        shutil.copy2(temp_path, file_path)
                        os.remove(temp_path)
                        logger.info(f"Copied completed download: {temp_path} -> {file_path}")
                    except Exception as e2:
                        logger.error(f"Failed to copy download to final path: {e2}")
                        item["status"] = "failed"
                        item["error"] = f"Download OK but failed to move file: {e2}"
                        save_cart()
                        continue

                item["status"] = "completed"
                item["progress"] = 100
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                item["file_size"] = file_size
                save_cart()
                logger.info(f"Download completed: {item.get('name')} -> {file_path} ({file_size / 1024 / 1024:.1f} MB)")

            # Pause between files if throttle is active
            if pause_interval > 0 and pause_duration > 0:
                logger.info(f"Download throttle: pausing {pause_duration}s between files")
                _download_progress["paused"] = True
                _download_progress["pause_remaining"] = pause_duration
                remaining = pause_duration
                while remaining > 0:
                    if _download_cancel_event.is_set():
                        break
                    sleep_step = min(1.0, remaining)
                    await asyncio.sleep(sleep_step)
                    remaining -= sleep_step
                    _download_progress["pause_remaining"] = round(remaining, 1)
                _download_progress["paused"] = False
                _download_progress["pause_remaining"] = 0

        except Exception as e:
            logger.error(f"Download error for {item.get('name')}: {e}")
            item["status"] = "failed"
            item["error"] = str(e)
            save_cart()
            # Clean up partial temp file on error
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError:
                pass

    _download_current_item = None
    _download_progress = {"bytes_downloaded": 0, "total_bytes": 0, "speed": 0, "paused": False, "pause_remaining": 0}


async def fetch_series_episodes(source_id: str, series_id: str) -> list:
    """Fetch all episodes for a series from the upstream provider"""
    source = get_source_by_id(source_id)
    if not source:
        return []

    host = source["host"].rstrip("/")
    params = {
        "username": source["username"],
        "password": source["password"],
        "action": "get_series_info",
        "series_id": series_id
    }

    try:
        client = await get_http_client()
        response = await client.get(f"{host}/player_api.php", params=params, timeout=60.0)
        if response.status_code != 200:
            return []

        data = response.json()
        episodes = []
        series_info = data.get("info", {})
        series_name = series_info.get("name", "") or series_info.get("title", "")

        for season_num, season_episodes in data.get("episodes", {}).items():
            for ep in season_episodes:
                episodes.append({
                    "stream_id": str(ep.get("id", "")),
                    "season": str(season_num),
                    "episode_num": ep.get("episode_num", 0),
                    "title": ep.get("title", ""),
                    "container_extension": ep.get("container_extension", "mp4"),
                    "info": ep.get("info", {}),
                    "series_name": series_name,
                })
        return episodes

    except Exception as e:
        logger.error(f"Error fetching series episodes: {e}")
        return []


def get_category_by_id(category_id: str):
    """Get a category by its ID"""
    data = load_categories()
    for cat in data.get("categories", []):
        if cat.get("id") == category_id:
            return cat
    return None


def is_in_category(category_id: str, content_type: str, item_id: str, source_id: str) -> bool:
    """Check if an item is in a specific category (manual mode only)"""
    cat = get_category_by_id(category_id)
    if not cat:
        return False
    
    if cat.get("mode") == "manual":
        for item in cat.get("items", []):
            if (item.get("id") == item_id and 
                item.get("source_id") == source_id and
                item.get("content_type") == content_type):
                return True
    return False


def get_item_categories(content_type: str, item_id: str, source_id: str) -> list:
    """Get all manual categories that contain this item"""
    data = load_categories()
    result = []
    for cat in data.get("categories", []):
        if cat.get("mode") == "manual":
            for item in cat.get("items", []):
                if (item.get("id") == item_id and 
                    item.get("source_id") == source_id and
                    item.get("content_type") == content_type):
                    result.append(cat.get("id"))
                    break
    return result


async def send_telegram_notification(category_name: str, new_items: list):
    """Send a Telegram notification for new items in a category.
    
    Args:
        category_name: Name of the category that was updated
        new_items: List of dicts with 'name' and optionally 'cover' keys
    """
    config = load_config()
    telegram_config = config.get("options", {}).get("telegram", {})
    
    if not telegram_config.get("enabled"):
        return
    
    bot_token = telegram_config.get("bot_token", "")
    chat_id = telegram_config.get("chat_id", "")
    
    if not bot_token or not chat_id:
        logger.warning("Telegram notification skipped: missing bot_token or chat_id")
        return
    
    if not new_items:
        return
    
    # Group similar items to avoid duplicate listings
    grouped = _group_new_items_for_notification(new_items)
    unique_count = len(grouped)
    
    try:
        client = await get_http_client()
        
        # Collect unique grouped items with valid covers (one per group)
        groups_with_covers = [g for g in grouped if g.get("cover")]
        
        # If we have multiple items with covers, use sendMediaGroup (album)
        if len(groups_with_covers) >= 2:
            # Telegram allows max 10 media items per group
            media_items = groups_with_covers[:10]
            
            media = []
            for i, g in enumerate(media_items):
                media_obj = {
                    "type": "photo",
                    "media": g["cover"],
                }
                # Only first item gets the caption
                if i == 0:
                    # Telegram caption limit is 1024 chars - fit as many as possible
                    caption = f"🆕 <b>{category_name}</b> - {unique_count} new unique title(s)\n\n"
                    for gr in grouped:
                        line = _format_grouped_item_line(gr)
                        if len(caption) + len(line) < 1000:
                            caption += line
                        else:
                            break
                    caption = caption.rstrip()
                    media_obj["caption"] = caption
                    media_obj["parse_mode"] = "HTML"
                media.append(media_obj)
            
            url = f"https://api.telegram.org/bot{bot_token}/sendMediaGroup"
            payload = {
                "chat_id": chat_id,
                "media": media
            }
            logger.info(f"Sending Telegram album with {len(media)} photos")
            response = await client.post(url, json=payload)
            logger.info(f"Telegram sendMediaGroup response: {response.status_code} - {response.text[:200]}")
            
            # If media group fails, fallback to text message with all items
            if response.status_code != 200:
                logger.warning(f"Telegram sendMediaGroup failed, falling back to text: {response.text}")
                await _send_telegram_text_message(client, bot_token, chat_id, category_name, grouped)
            else:
                # If album succeeded but we couldn't fit all items in caption, send text message with full list
                items_in_caption = caption.count("• ")
                if items_in_caption < unique_count:
                    await _send_telegram_text_message(client, bot_token, chat_id, category_name, grouped)
        
        # Single item with cover: use sendPhoto
        elif len(groups_with_covers) == 1:
            g = groups_with_covers[0]
            caption = f"🆕 <b>{category_name}</b>\n\n" + _format_grouped_item_line(g).rstrip()
            
            url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            payload = {
                "chat_id": chat_id,
                "photo": g["cover"],
                "caption": caption,
                "parse_mode": "HTML"
            }
            response = await client.post(url, json=payload)
            
            # If photo fails (invalid URL), fallback to text message
            if response.status_code != 200:
                logger.warning(f"Telegram sendPhoto failed, falling back to text: {response.text}")
                await _send_telegram_text_message(client, bot_token, chat_id, category_name, grouped)
        
        # No covers: send text message
        else:
            await _send_telegram_text_message(client, bot_token, chat_id, category_name, grouped)
        
        logger.info(f"Telegram notification sent for category '{category_name}': {len(new_items)} new items ({unique_count} unique)")
        
    except Exception as e:
        logger.error(f"Failed to send Telegram notification: {e}")


async def _send_telegram_text_message(client, bot_token: str, chat_id: str, category_name: str, grouped_items: list):
    """Send a text-only Telegram message with grouped item list, split into multiple messages if needed.
    
    Args:
        grouped_items: List of grouped dicts with 'name', 'sources' (list of source names), and optionally 'cover'.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    # Telegram message limit is 4096 characters
    MAX_MESSAGE_LENGTH = 4096
    
    unique_count = len(grouped_items)
    
    # Build header for first message
    header = f"🆕 <b>{category_name}</b> - {unique_count} new unique title(s)\n\n"
    
    # Build all items
    all_lines = [_format_grouped_item_line(g) for g in grouped_items]
    
    # Split into messages respecting the character limit
    messages = []
    current_message = header
    
    for item_line in all_lines:
        # Check if adding this item would exceed the limit
        if len(current_message) + len(item_line) > MAX_MESSAGE_LENGTH - 50:  # Leave some margin
            # Save current message and start a new one
            messages.append(current_message.rstrip())
            # For continuation messages, add a shorter header
            part_num = len(messages) + 1
            current_message = f"📋 <b>{category_name}</b> (continued - part {part_num})\n\n{item_line}"
        else:
            current_message += item_line
    
    # Don't forget the last message
    if current_message.strip():
        messages.append(current_message.rstrip())
    
    # Send all messages
    for message in messages:
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        await client.post(url, json=payload)


def _group_new_items_for_notification(new_items: list) -> list:
    """Group new items by name similarity for notification deduplication.
    
    Args:
        new_items: List of dicts with 'name' and optionally 'cover' keys
    
    Returns:
        List of grouped dicts: {'name': str, 'cover': str, 'sources': [str], 'count': int}
    """
    if not new_items:
        return []
    
    groups = []  # Each: {'normalized': str, 'name': str, 'cover': str, 'sources': []}
    
    # Load source configs to resolve source names
    config = load_config()
    sources_by_id = {s.get("id"): s.get("name", s.get("id", "?")) for s in config.get("sources", [])}
    
    for item in new_items:
        item_name = item.get('name', '')
        item_normalized = normalize_name(item_name)
        source_name = sources_by_id.get(item.get('source_id', ''), '')
        
        best_group = None
        best_score = 0
        
        for group in groups:
            score = fuzz.token_sort_ratio(item_normalized, group['normalized'])
            if score >= 85 and score > best_score:
                best_score = score
                best_group = group
        
        if best_group is not None:
            if source_name and source_name not in best_group['sources']:
                best_group['sources'].append(source_name)
            if not best_group['cover'] and item.get('cover'):
                best_group['cover'] = item['cover']
            # Use shortest name as canonical
            if len(item_name) < len(best_group['name']):
                best_group['name'] = item_name
        else:
            groups.append({
                'normalized': item_normalized,
                'name': item_name,
                'cover': item.get('cover', ''),
                'sources': [source_name] if source_name else [],
            })
    
    return [{'name': g['name'], 'cover': g['cover'], 'sources': g['sources'], 'count': len(g['sources'])} for g in groups]


def _format_grouped_item_line(grouped_item: dict) -> str:
    """Format a single grouped item as a text line for Telegram.
    
    If the item has multiple sources, shows: • Movie Name (Source A, Source B)
    Otherwise just: • Movie Name
    """
    name = grouped_item.get('name', '')
    sources = grouped_item.get('sources', [])
    if len(sources) > 1:
        return f"• {name} ({', '.join(sources)})\n"
    elif len(sources) == 1:
        return f"• {name} ({sources[0]})\n"
    return f"• {name}\n"


def get_item_details_from_cache(item_id: str, source_id: str, content_type: str) -> dict:
    """Get full item details (name, cover) from cache by ID and source."""
    if content_type == "live":
        streams, _ = get_cached_with_source_info("live_streams", "live_categories")
        id_field = "stream_id"
    elif content_type == "vod":
        streams, _ = get_cached_with_source_info("vod_streams", "vod_categories")
        id_field = "stream_id"
    elif content_type == "series":
        streams, _ = get_cached_with_source_info("series", "series_categories")
        id_field = "series_id"
    else:
        return {}
    
    for stream in streams:
        stream_id = str(stream.get(id_field, ""))
        stream_source = stream.get("_source_id", "")
        if stream_id == item_id and stream_source == source_id:
            return {
                "name": stream.get("name", "Unknown"),
                "cover": stream.get("stream_icon", "") or stream.get("cover", "")
            }
    return {}


def refresh_pattern_categories_sync():
    """Synchronous wrapper that collects notifications to send."""
    return _refresh_pattern_categories_internal()


def _refresh_pattern_categories_internal():
    """Internal refresh logic that returns notifications to send.
    
    Returns:
        List of tuples: (category_name, new_items_list)
    """
    data = load_categories()
    config = load_config()
    sources_config = {s.get("id"): s for s in config.get("sources", [])}
    current_time = int(time.time())
    
    updated = False
    notifications_to_send = []
    
    for cat in data.get("categories", []):
        if cat.get("mode") != "automatic":
            continue
        
        patterns = cat.get("patterns", [])
        recently_added_days = cat.get("recently_added_days", 0)
        
        # Skip if no patterns AND no recently_added_days filter
        if not patterns and recently_added_days <= 0:
            continue
        
        content_types = cat.get("content_types", ["live", "vod", "series"])
        use_source_filters = cat.get("use_source_filters", False)
        pattern_logic = cat.get("pattern_logic", "or")  # 'or' or 'and'
        
        # Calculate cutoff timestamp for recently added filter
        recently_added_cutoff = current_time - (recently_added_days * 86400) if recently_added_days > 0 else 0
        
        matched_items = []
        
        for content_type in content_types:
            if content_type == "live":
                streams, categories = get_cached_with_source_info("live_streams", "live_categories")
            elif content_type == "vod":
                streams, categories = get_cached_with_source_info("vod_streams", "vod_categories")
            elif content_type == "series":
                streams, categories = get_cached_with_source_info("series", "series_categories")
            else:
                continue
            
            cat_map = build_category_map(categories)
            
            for stream in streams:
                name = stream.get("name", "")
                item_id = str(stream.get("stream_id") or stream.get("series_id") or "")
                src_id = stream.get("_source_id", "")
                
                # Apply recently added filter
                if recently_added_days > 0:
                    # Use 'added' field, fallback to 'last_modified' for series
                    added = stream.get("added") or stream.get("last_modified", 0)
                    try:
                        added_ts = int(added) if added else 0
                    except (ValueError, TypeError):
                        added_ts = 0
                    if added_ts < recently_added_cutoff:
                        continue
                
                # Apply source filters if enabled
                if use_source_filters and src_id in sources_config:
                    source_cfg = sources_config[src_id]
                    filters = source_cfg.get("filters", {})
                    content_filters = filters.get(content_type, {})
                    group_filters = content_filters.get("groups", [])
                    channel_filters = content_filters.get("channels", [])
                    
                    cat_id = str(stream.get("category_id", ""))
                    group_name = cat_map.get(cat_id, "")
                    
                    if group_filters and not should_include(group_name, group_filters):
                        continue
                    if channel_filters and not should_include(name, channel_filters):
                        continue
                
                # Check name against patterns (only if patterns exist)
                if patterns:
                    if pattern_logic == "and":
                        # All patterns must match
                        all_match = True
                        for pattern in patterns:
                            if not matches_filter(name, pattern):
                                all_match = False
                                break
                        if not all_match:
                            continue
                    else:
                        # At least one pattern must match (OR logic)
                        any_match = False
                        for pattern in patterns:
                            if matches_filter(name, pattern):
                                any_match = True
                                break
                        if not any_match:
                            continue
                
                matched_items.append({
                    "id": item_id,
                    "source_id": src_id,
                    "content_type": content_type,
                    "name": name,
                    "cover": stream.get("stream_icon", "") or stream.get("cover", "")
                })
        
        # Detect new items (diff between old and new)
        old_items = cat.get("cached_items", [])
        old_item_keys = {(item.get("id"), item.get("source_id"), item.get("content_type")) for item in old_items}
        
        new_items = []
        for item in matched_items:
            key = (item["id"], item["source_id"], item["content_type"])
            if key not in old_item_keys:
                new_items.append({"name": item["name"], "cover": item["cover"], "source_id": item["source_id"]})
        
        # Check if notifications are enabled for this category
        if cat.get("notify_telegram", False) and new_items:
            notifications_to_send.append((cat.get("name", "Unknown Category"), new_items))
        
        # Store cached_items without name/cover to save space
        cat["cached_items"] = [
            {"id": item["id"], "source_id": item["source_id"], "content_type": item["content_type"]}
            for item in matched_items
        ]
        cat["last_refresh"] = datetime.now().isoformat()
        updated = True
        logger.info(f"Category '{cat.get('name')}' refreshed: {len(matched_items)} items matched, {len(new_items)} new")
    
    if updated:
        save_categories(data)
    
    return notifications_to_send


async def refresh_pattern_categories_async():
    """Async version that refreshes categories and sends Telegram notifications."""
    notifications = _refresh_pattern_categories_internal()
    
    for category_name, new_items in notifications:
        await send_telegram_notification(category_name, new_items)


def refresh_pattern_categories():
    """Refresh all automatic (pattern-based) categories.
    
    This is the sync entry point that just refreshes without sending notifications.
    For notification support, use refresh_pattern_categories_async().
    """
    _refresh_pattern_categories_internal()


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


def normalize_name(name: str) -> str:
    """Normalize a title for fuzzy comparison.
    
    Strips common channel/source prefixes, language suffixes/tags, quality markers,
    and accents so that e.g. 'A+ - Hijack (2023) (GB)', 'FR - Hijack (2023) (GB)',
    and 'Hijack_fr' are all compared on their core title.
    """
    n = name.strip().lower()
    # Strip accents for cross-language matching (e.g. Téhéran vs Tehran)
    n = ''.join(c for c in unicodedata.normalize('NFD', n) if unicodedata.category(c) != 'Mn')
    # Remove common channel/source prefixes like "FR - ", "A+ - ", "NF - ", "4K-FR - ", "4K-FR-HDR - ",
    # "FR -4KL ", etc. These are typically short tags before the actual title.
    # Strategy: repeatedly strip a leading prefix followed by " - " or "- "
    # Prefixes are either short (1-4 chars) or longer codes containing digits/+/non-alpha (up to 10 chars)
    for _ in range(3):  # Handle up to 3 chained prefixes
        # Short prefix (any 1-4 chars): "FR - ", "NF - ", "A+ - ", "D+ - "
        m = re.match(r'^(\S{1,4})\s*-\s+', n)
        if not m:
            # Longer prefix containing at least one digit or '+' (e.g. "4K-FR-HDR - ", "4K-A+ - ")
            m = re.match(r'^(\S*[\d+]\S*)\s*-\s+', n)
            # Safety: only accept if prefix is <= 10 chars to avoid stripping title words
            if m and len(m.group(1)) > 10:
                m = None
        if not m:
            m = re.match(r'^(\S{1,4})\s+-\S+\s+', n)
        if not m:
            # Handle "XX: title" prefix (e.g. "FR: Industry")
            m = re.match(r'^([A-Za-z0-9+]{1,4}):\s+', n)
        if not m:
            # Handle "XX: title" prefix (e.g. "FR: Industry")
            m = re.match(r'^([A-Za-z0-9+]{1,4}):\s+', n)
        if m:
            n = n[m.end():]
        else:
            break
    # Strip trailing language suffixes like "_fr", "-fr", "_en", "_vost"
    n = re.sub(r'[_-](fr|en|de|es|it|pt|nl|pl|ar|tr|jp|kr|gb|us|br|as|vost|vostfr|multi)\s*$', '', n)
    # Remove common suffixes/tags in parentheses or brackets
    n = re.sub(r'\s*[\(\[][^)\]]*[\)\]]\s*', ' ', n)
    # Remove trailing standalone language/region codes and compound codes (e.g. "Title FR", "Title FR-EN")
    n = re.sub(r'\s+(?:(?:fr|en|de|es|it|pt|nl|pl)(?:-(?:fr|en|de|es|it|pt|nl|pl))?)\s*$', '', n)
    # Remove common quality / codec / language tags
    n = re.sub(r'\b(4k|uhd|fhd|hd|sd|hdr|hdr10|dolby|atmos|hevc|h\.?265|h\.?264|x264|x265|bluray|blu-ray|webrip|web-dl|remux|multi|vf|vo|vost|vostfr|french|english|truefrench|cam|ts|md)\b', '', n, flags=re.IGNORECASE)
    # Remove trailing "- YYYY" or standalone trailing YYYY year pattern
    n = re.sub(r'\s*-\s*\d{4}\s*$', '', n)
    n = re.sub(r'\s+\d{4}\s*$', '', n)
    # Collapse whitespace
    n = re.sub(r'\s+', ' ', n).strip()
    # Remove leading/trailing punctuation like " -" or "| "
    n = re.sub(r'^[\s\-\|\.:]+', '', n)
    n = re.sub(r'[\s\-\|\.:]+$', '', n)
    return n


def group_similar_items(items: list, threshold: int = 85) -> list:
    """Group items with similar names using fuzzy matching.
    
    Args:
        items: List of item dicts (must have 'name' key)
        threshold: Minimum similarity ratio (0-100) for grouping
    
    Returns:
        List of group dicts: {
            'name': canonical name (shortest variant),
            'icon': best cover (first non-empty),
            'items': [original items],
            'count': number of items in group
        }
    """
    if not items:
        return []
    
    groups = []  # Each group: {'normalized': str, 'name': str, 'icon': str, 'items': []}
    
    for item in items:
        item_name = item.get('name', '')
        item_normalized = normalize_name(item_name)
        
        best_group = None
        best_score = 0
        
        # Compare against existing groups
        for group in groups:
            score = fuzz.token_sort_ratio(item_normalized, group['normalized'])
            if score >= threshold and score > best_score:
                best_score = score
                best_group = group
        
        if best_group is not None:
            best_group['items'].append(item)
            # Use shortest name as canonical (usually the cleanest)
            if len(item_name) < len(best_group['name']):
                best_group['name'] = item_name
            # Use first non-empty icon
            if not best_group['icon'] and item.get('icon'):
                best_group['icon'] = item['icon']
        else:
            groups.append({
                'normalized': item_normalized,
                'name': item_name,
                'icon': item.get('icon', ''),
                'items': [item],
            })
    
    # Build result without the internal 'normalized' key
    result = []
    for g in groups:
        result.append({
            'name': g['name'],
            'icon': g['icon'],
            'items': g['items'],
            'count': len(g['items']),
        })
    
    return result


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


# Icon name to emoji mapping for custom categories
ICON_EMOJI_MAP = {
    "folder": "📁",
    "heart": "❤️",
    "star": "⭐",
    "fire": "🔥",
    "clock": "🕐",
    "film": "🎬",
    "tv": "📺",
    "music": "🎵",
    "sports": "⚽",
    "news": "📰",
    "kids": "👶",
    "bookmark": "🔖",
    "tag": "🏷️",
    "check": "✅",
    "play": "▶️",
    "list": "📋"
}

def get_icon_emoji(icon_name: str) -> str:
    """Convert icon name to emoji, or return empty string if not found."""
    return ICON_EMOJI_MAP.get(icon_name, "")

# Base offset for custom category IDs - using low numbers that fit normal Xtream range
# Starting at 99001 to avoid conflicts with source categories (typically under 10000)
CUSTOM_CAT_ID_BASE = 99001

def get_custom_cat_id_map() -> dict:
    """Build a mapping of custom category string IDs to sequential numeric IDs.
    
    Returns dict: {cat_id_string: numeric_id}
    """
    data = load_categories()
    cat_id_map = {}
    for idx, cat in enumerate(data.get("categories", [])):
        cat_id_map[cat.get("id")] = CUSTOM_CAT_ID_BASE + idx
    return cat_id_map

def custom_cat_id_to_numeric(cat_id: str) -> str:
    """Convert a custom category string ID to a unique positive numeric ID string.
    
    Uses sequential IDs starting from CUSTOM_CAT_ID_BASE (50,000,001).
    Returns as string to match Xtream API format.
    """
    cat_id_map = get_custom_cat_id_map()
    return str(cat_id_map.get(cat_id, CUSTOM_CAT_ID_BASE))

def get_custom_categories_for_content_type(content_type: str) -> list:
    """Get all custom categories that have items for the specified content type.
    
    Args:
        content_type: 'live', 'vod', or 'series'
        
    Returns:
        List of category dicts with 'category_id', 'category_name', and 'items' 
    """
    data = load_categories()
    result = []
    
    # Map browse content types to internal types
    type_map = {"live": "live", "vod": "vod", "series": "series"}
    internal_type = type_map.get(content_type, content_type)
    
    for cat in data.get("categories", []):
        # Skip if this content type is not in the category's allowed types
        if internal_type not in cat.get("content_types", []):
            continue
        
        # Get items based on mode
        if cat.get("mode") == "manual":
            items = cat.get("items", [])
        else:
            items = cat.get("cached_items", [])
        
        # Filter items to only those of the requested content type
        filtered_items = [
            item for item in items 
            if item.get("content_type") == internal_type
        ]
        
        # Only include category if it has items for this content type
        if filtered_items:
            result.append({
                "id": cat.get("id"),
                "name": cat.get("name"),
                "icon": cat.get("icon", "📁"),
                "items": filtered_items
            })
    
    return result


def get_custom_category_streams(content_type: str, custom_cat_id: str) -> list:
    """Get all streams that belong to a custom category for the specified content type.
    
    Returns list of (source_id, item_id) tuples.
    """
    data = load_categories()
    
    for cat in data.get("categories", []):
        if cat.get("id") != custom_cat_id:
            continue
        
        if cat.get("mode") == "manual":
            items = cat.get("items", [])
        else:
            items = cat.get("cached_items", [])
        
        return [
            (item.get("source_id"), str(item.get("id")))
            for item in items 
            if item.get("content_type") == content_type
        ]
    
    return []


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
                
                # Prefix EPG ID with source_id to match merged XMLTV (lowercase for consistent matching)
                if epg_id:
                    epg_id = f"{source_id}_{epg_id}".lower()

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


def generate_m3u_for_source(source: dict, request: Request, source_route: str):
    """Generate filtered M3U playlist for a single source with direct stream URLs."""
    source_id = source.get("id", "default")
    prefix = source.get("prefix", "")
    filters = source.get("filters", {})
    
    server_url = str(request.base_url).rstrip("/")
    stream_base = f"/{source_route}"

    lines = ["#EXTM3U"]
    stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0}

    # Live TV
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
        
        # Prefix EPG ID with source_id to match XMLTV (lowercase for consistent matching)
        if epg_id:
            epg_id = f"{source_id}_{epg_id}".lower()

        if not should_include(group, live_group_filters):
            stats["excluded"] += 1
            continue

        if not should_include(name, live_channel_filters):
            stats["excluded"] += 1
            continue

        display_group = f"{prefix}{group}" if prefix else group
        stream_url = f"{server_url}{stream_base}/user/pass/{stream_id}.ts"
        lines.append(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-logo="{icon}" group-title="{display_group}",{name}')
        lines.append(stream_url)
        stats["live"] += 1

    # VOD
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
        stream_url = f"{server_url}{stream_base}/movie/user/pass/{stream_id}.{extension}"
        lines.append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{display_group}",{name}')
        lines.append(stream_url)
        stats["vod"] += 1

    # Series
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

    stats_line = f"# Content: {stats['live']} live, {stats['vod']} movies, {stats['series']} series | {stats['excluded']} excluded | Source: {source.get('name', source_id)}"
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
    load_epg_cache_from_disk()
    load_cart()
    
    # Recovery: reset any items stuck in "downloading" from a previous run/crash
    recovered = 0
    for item in _download_cart:
        if item.get("status") == "downloading":
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            recovered += 1
    if recovered:
        save_cart()
        logger.info(f"Recovered {recovered} stuck download(s) back to queued")
    
    # Start background refresh task
    _background_task = asyncio.create_task(background_refresh_loop())
    
    # If cache is empty or invalid, trigger immediate refresh
    if not is_cache_valid():
        logger.info("Cache is empty or invalid, triggering initial refresh...")
        asyncio.create_task(refresh_cache())
    
    # If EPG cache is empty or invalid, trigger EPG refresh
    if not is_epg_cache_valid():
        logger.info("EPG cache is empty or invalid, triggering initial EPG refresh...")
        asyncio.create_task(refresh_epg_cache())
    
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


# Middleware to ensure UTF-8 charset in JSON responses
@app.middleware("http")
async def add_utf8_charset(request: Request, call_next):
    response = await call_next(request)
    content_type = response.headers.get("content-type", "")
    if "application/json" in content_type and "charset" not in content_type:
        response.headers["content-type"] = "application/json; charset=utf-8"
    return response


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
    source: str = Query(""),
    news_days: int = Query(0),
    category_id: str = Query(""),
    use_source_filters: bool = Query(False),
    page: int = Query(1),
    per_page: int = Query(50),
):
    """Enhanced browsing API for channels/items with full metadata.
    
    Args:
        type: Content type - 'live', 'vod', or 'series'
        search: Search filter for name/group
        group: Filter by specific group name
        source: Filter by source_id (empty = all sources)
        news_days: If > 0, only show items added within the last X days
        category_id: If set, only show items in this category
        use_source_filters: If True, apply source filter rules
        page: Page number (1-indexed)
        per_page: Items per page (max 200)
    """
    per_page = min(per_page, 200)
    search_lower = search.lower()
    
    # Load source config for filter rules if needed
    sources_config = {}
    if use_source_filters:
        config = load_config()
        sources_config = {s.get("id"): s for s in config.get("sources", [])}
    
    # Get current timestamp for news filtering
    current_time = int(time.time())
    news_cutoff = current_time - (news_days * 86400) if news_days > 0 else 0

    # Get data with source info
    if type == "live":
        streams, categories = get_cached_with_source_info("live_streams", "live_categories")
    elif type == "vod":
        streams, categories = get_cached_with_source_info("vod_streams", "vod_categories")
    elif type == "series":
        streams, categories = get_cached_with_source_info("series", "series_categories")
    else:
        return {"error": "Invalid content type", "items": []}

    cat_map = build_category_map(categories)
    
    # Build reverse lookup map for category membership (item -> list of category IDs)
    # This is done ONCE instead of calling get_item_categories for each item
    categories_data = load_categories()
    category_membership = {}  # (content_type, item_id, source_id) -> [cat_id, ...]
    for cat in categories_data.get("categories", []):
        if cat.get("mode") == "manual":
            for item in cat.get("items", []):
                key = (item.get("content_type"), str(item.get("id")), item.get("source_id"))
                if key not in category_membership:
                    category_membership[key] = []
                category_membership[key].append(cat.get("id"))
    
    # Build category item set if filtering by category
    category_item_set = set()
    if category_id:
        cat_data = get_category_by_id(category_id)
        if cat_data:
            # For manual categories, use items; for automatic, use cached_items
            if cat_data.get("mode") == "manual":
                cat_items = cat_data.get("items", [])
            else:
                cat_items = cat_data.get("cached_items", [])
            
            for item in cat_items:
                if item.get("content_type") == type:
                    category_item_set.add((str(item.get("id")), item.get("source_id")))

    # Collect unique sources for filter dropdown
    source_set = {}
    group_counts = {}
    
    for s in streams:
        src_id = s.get("_source_id", "")
        src_name = s.get("_source_name", "Unknown")
        if src_id:
            source_set[src_id] = src_name
        
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")
        group_counts[grp] = group_counts.get(grp, 0) + 1

    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        grp = cat_map.get(cat_id, "Unknown")
        icon = s.get("stream_icon", "") or s.get("cover", "")
        item_id = str(s.get("stream_id") or s.get("series_id") or "")
        src_id = s.get("_source_id", "")
        src_name = s.get("_source_name", "Unknown")
        # For series, use last_modified as fallback since they don't have 'added' field
        added = s.get("added") or s.get("last_modified", 0)
        
        # Try to parse added timestamp
        try:
            added_ts = int(added) if added else 0
        except (ValueError, TypeError):
            added_ts = 0

        # Filter by source
        if source and src_id != source:
            continue
        
        # Apply source filter rules if enabled
        if use_source_filters and src_id in sources_config:
            source_cfg = sources_config[src_id]
            filters = source_cfg.get("filters", {})
            content_filters = filters.get(type, {})
            group_filters = content_filters.get("groups", [])
            channel_filters = content_filters.get("channels", [])
            
            if group_filters and not should_include(grp, group_filters):
                continue
            if channel_filters and not should_include(name, channel_filters):
                continue
        
        # Filter by news_days
        if news_days > 0 and added_ts < news_cutoff:
            continue
        
        # Filter by category
        if category_id and (item_id, src_id) not in category_item_set:
            continue

        # Filter by search
        if search_lower and search_lower not in name.lower() and search_lower not in grp.lower():
            continue

        # Filter by group
        if group and grp != group:
            continue

        item_data = {
            "name": name,
            "group": grp,
            "icon": icon,
            "id": item_id,
            "source_id": src_id,
            "source_name": src_name,
            "added": added_ts,
        }
        # Include container_extension for VOD items (needed for correct download URLs)
        if type == "vod":
            item_data["container_extension"] = s.get("container_extension", "mp4")
        items.append(item_data)

    total = len(items)
    
    # Sort: if news mode, sort by added desc; otherwise by group/name
    if news_days > 0:
        items.sort(key=lambda x: x["added"], reverse=True)
    else:
        items.sort(key=lambda x: (x["group"].lower(), x["name"].lower()))

    # Group similar items when browsing a category (duplicates across sources/groups)
    grouped = False
    if category_id:
        grouped_items = group_similar_items(items, threshold=85)
        # Enrich each sub-item with category membership
        for gi in grouped_items:
            for sub in gi["items"]:
                key = (type, sub["id"], sub["source_id"])
                sub["categories"] = category_membership.get(key, [])
        total = len(grouped_items)
        grouped = True
        
        start = (page - 1) * per_page
        end = start + per_page
        paginated = grouped_items[start:end]
    else:
        start = (page - 1) * per_page
        end = start + per_page
        paginated = items[start:end]
        
        # Add category membership only to paginated items (fast lookup)
        for item in paginated:
            key = (type, item["id"], item["source_id"])
            item["categories"] = category_membership.get(key, [])

    groups_list = [{"name": g, "count": c} for g, c in sorted(group_counts.items())]
    sources_list = [{"id": sid, "name": sname} for sid, sname in sorted(source_set.items(), key=lambda x: x[1])]

    return {
        "items": paginated,
        "grouped": grouped,
        "groups": groups_list,
        "sources": sources_list,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page,
        "content_type": type,
    }


# ============================================
# CATEGORIES API ROUTES
# ============================================


@app.get("/api/categories")
async def get_categories():
    """Get all categories."""
    data = load_categories()
    return {"categories": data.get("categories", [])}


@app.post("/api/categories")
async def create_category(request: Request):
    """Create a new category.
    
    Request body: {
        "name": "Category Name",
        "icon": "🎬",
        "mode": "manual" | "automatic",
        "content_types": ["live", "vod", "series"],
        "patterns": [{"match": "contains", "value": "4K", "case_sensitive": false}],
        "pattern_logic": "or" | "and",
        "use_source_filters": false
    }
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    
    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"error": "Category name is required"}, status_code=400)
    
    mode = body.get("mode", "manual")
    if mode not in ["manual", "automatic"]:
        return JSONResponse({"error": "Invalid mode"}, status_code=400)
    
    content_types = body.get("content_types", ["live", "vod", "series"])
    valid_types = {"live", "vod", "series"}
    content_types = [t for t in content_types if t in valid_types]
    if not content_types:
        content_types = ["live", "vod", "series"]
    
    new_category = {
        "id": str(uuid.uuid4()),
        "name": name,
        "icon": body.get("icon", "📁"),
        "mode": mode,
        "content_types": content_types,
        "items": [],
        "patterns": body.get("patterns", []),
        "pattern_logic": body.get("pattern_logic", "or"),
        "use_source_filters": body.get("use_source_filters", False),
        "notify_telegram": body.get("notify_telegram", False),
        "recently_added_days": body.get("recently_added_days", 0),
        "cached_items": [],
        "last_refresh": None
    }
    
    data = load_categories()
    data["categories"].append(new_category)
    save_categories(data)
    
    # If automatic mode, refresh immediately
    if mode == "automatic":
        refresh_pattern_categories()
        # Reload to get updated cached_items
        data = load_categories()
        for cat in data["categories"]:
            if cat["id"] == new_category["id"]:
                new_category = cat
                break
    
    return {"status": "created", "category": new_category}


@app.put("/api/categories/{category_id}")
async def update_category(category_id: str, request: Request):
    """Update an existing category."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    
    data = load_categories()
    category = None
    cat_index = -1
    
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break
    
    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    
    # Update fields
    if "name" in body:
        category["name"] = body["name"].strip()
    if "icon" in body:
        category["icon"] = body["icon"]
    if "mode" in body and body["mode"] in ["manual", "automatic"]:
        old_mode = category.get("mode")
        category["mode"] = body["mode"]
        # Clear items/cached_items when switching modes
        if old_mode != body["mode"]:
            if body["mode"] == "manual":
                category["cached_items"] = []
            else:
                category["items"] = []
    if "content_types" in body:
        valid_types = {"live", "vod", "series"}
        category["content_types"] = [t for t in body["content_types"] if t in valid_types]
    if "patterns" in body:
        category["patterns"] = body["patterns"]
    if "pattern_logic" in body:
        category["pattern_logic"] = body["pattern_logic"]
    if "use_source_filters" in body:
        category["use_source_filters"] = body["use_source_filters"]
    if "notify_telegram" in body:
        category["notify_telegram"] = body["notify_telegram"]
    if "recently_added_days" in body:
        category["recently_added_days"] = body["recently_added_days"]
    
    data["categories"][cat_index] = category
    save_categories(data)
    
    # If automatic mode, refresh
    if category.get("mode") == "automatic":
        refresh_pattern_categories()
        data = load_categories()
        category = data["categories"][cat_index]
    
    return {"status": "updated", "category": category}


@app.delete("/api/categories/{category_id}")
async def delete_category(category_id: str):
    """Delete a category."""
    data = load_categories()
    
    original_count = len(data.get("categories", []))
    data["categories"] = [
        cat for cat in data.get("categories", [])
        if cat.get("id") != category_id
    ]
    
    if len(data["categories"]) < original_count:
        save_categories(data)
        return {"status": "deleted"}
    
    return JSONResponse({"error": "Category not found"}, status_code=404)


@app.post("/api/categories/{category_id}/items")
async def add_item_to_category(category_id: str, request: Request):
    """Add an item to a manual category.
    
    Request body: {"content_type": "live|vod|series", "id": "123", "source_id": "abc"}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    
    content_type = body.get("content_type")
    item_id = str(body.get("id", ""))
    source_id = body.get("source_id", "")
    
    if content_type not in ["vod", "series", "live"]:
        return JSONResponse({"error": "Invalid content type"}, status_code=400)
    
    if not item_id:
        return JSONResponse({"error": "Missing item id"}, status_code=400)
    
    data = load_categories()
    category = None
    cat_index = -1
    
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break
    
    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    
    if category.get("mode") != "manual":
        return JSONResponse({"error": "Cannot add items to automatic categories"}, status_code=400)
    
    # Check content type is allowed
    if content_type not in category.get("content_types", []):
        return JSONResponse({"error": f"Category does not accept {content_type} content"}, status_code=400)
    
    # Check if already exists
    for item in category.get("items", []):
        if (item.get("id") == item_id and 
            item.get("source_id") == source_id and
            item.get("content_type") == content_type):
            return {"status": "already_exists", "message": "Item already in category"}
    
    # Add item
    category["items"].append({
        "id": item_id,
        "source_id": source_id,
        "content_type": content_type,
        "added_at": datetime.now().isoformat()
    })
    
    data["categories"][cat_index] = category
    save_categories(data)
    
    return {"status": "added", "message": f"Added to {category['name']}"}


@app.delete("/api/categories/{category_id}/items/{content_type}/{source_id}/{item_id}")
async def remove_item_from_category(category_id: str, content_type: str, source_id: str, item_id: str):
    """Remove an item from a manual category."""
    if content_type not in ["vod", "series", "live"]:
        return JSONResponse({"error": "Invalid content type"}, status_code=400)
    
    data = load_categories()
    category = None
    cat_index = -1
    
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break
    
    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    
    if category.get("mode") != "manual":
        return JSONResponse({"error": "Cannot remove items from automatic categories"}, status_code=400)
    
    original_count = len(category.get("items", []))
    category["items"] = [
        item for item in category.get("items", [])
        if not (item.get("id") == item_id and 
                item.get("source_id") == source_id and
                item.get("content_type") == content_type)
    ]
    
    if len(category["items"]) < original_count:
        data["categories"][cat_index] = category
        save_categories(data)
        return {"status": "removed", "message": f"Removed from {category['name']}"}
    
    return {"status": "not_found", "message": "Item not found in category"}


@app.post("/api/categories/refresh")
async def refresh_categories():
    """Manually trigger refresh of all automatic categories."""
    refresh_pattern_categories()
    return {"status": "refreshed"}


# ============================================
# BROWSE PAGE ROUTE
# ============================================


@app.get("/browse", response_class=HTMLResponse)
async def browse_page(request: Request):
    """Browse page for searching and filtering cached content."""
    config = load_config()
    sources = [{"id": s.get("id"), "name": s.get("name")} for s in config.get("sources", []) if s.get("enabled", True)]
    return templates.TemplateResponse("browse.html", {
        "request": request,
        "sources": sources
    })


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
            # First, add custom categories at the beginning
            custom_cats = get_custom_categories_for_content_type("live")
            for custom_cat in custom_cats:
                result.append({
                    "category_id": custom_cat_id_to_numeric(custom_cat['id']),
                    "category_name": custom_cat['name'],
                    "parent_id": 0
                })
            # Then add source categories
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
                        cat_copy["category_id"] = str(encode_virtual_id(idx, safe_get_category_id(cat)))
                        result.append(cat_copy)
            return result

        elif action == "get_vod_categories":
            result = []
            # First, add custom categories at the beginning
            custom_cats = get_custom_categories_for_content_type("vod")
            for custom_cat in custom_cats:
                result.append({
                    "category_id": custom_cat_id_to_numeric(custom_cat['id']),
                    "category_name": custom_cat['name'],
                    "parent_id": 0
                })
            # Then add source categories
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
                        cat_copy["category_id"] = str(encode_virtual_id(idx, safe_get_category_id(cat)))
                        result.append(cat_copy)
            return result

        elif action == "get_series_categories":
            result = []
            # First, add custom categories at the beginning
            custom_cats = get_custom_categories_for_content_type("series")
            for custom_cat in custom_cats:
                result.append({
                    "category_id": custom_cat_id_to_numeric(custom_cat['id']),
                    "category_name": custom_cat['name'],
                    "parent_id": 0
                })
            # Then add source categories
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
                        cat_copy["category_id"] = str(encode_virtual_id(idx, safe_get_category_id(cat)))
                        result.append(cat_copy)
            return result

        elif action == "get_live_streams":
            result = []
            requested_cat_id = request.query_params.get("category_id")
            
            # Build lookup of source index by source_id
            source_idx_map = {source.get("id"): idx for idx, source in enumerate(enabled_sources)}
            
            # Get custom categories and build a lookup: (source_id, item_id) -> list of custom_cat_ids (strings)
            custom_cats = get_custom_categories_for_content_type("live")
            custom_item_cats = {}  # (source_id, item_id) -> [cat_id_str, ...]
            for custom_cat in custom_cats:
                cat_id_str = custom_cat_id_to_numeric(custom_cat['id'])
                for item in custom_cat["items"]:
                    key = (item.get("source_id"), str(item.get("id")))
                    if key not in custom_item_cats:
                        custom_item_cats[key] = []
                    custom_item_cats[key].append(cat_id_str)
            
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
                    stream_id = str(stream.get("stream_id", ""))
                    
                    if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                        virtual_cat_id = str(encode_virtual_id(idx, stream.get("category_id", 0)))
                        # Only include if no category filter or matches
                        if requested_cat_id is None or requested_cat_id == virtual_cat_id:
                            stream_copy = stream.copy()
                            stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                            stream_copy["category_id"] = virtual_cat_id
                            stream_copy["category_ids"] = [int(virtual_cat_id)]
                            # Prefix epg_channel_id to match XMLTV channel IDs (lowercase for consistent matching)
                            epg_id = stream.get("epg_channel_id", "")
                            if epg_id:
                                stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                            result.append(stream_copy)
                    
                    # Also add to custom categories if applicable
                    custom_cat_ids = custom_item_cats.get((source_id, stream_id), [])
                    for custom_cat_id in custom_cat_ids:
                        # Only include if no category filter or matches this custom category
                        if requested_cat_id is None or requested_cat_id == custom_cat_id:
                            stream_copy = stream.copy()
                            stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                            stream_copy["category_id"] = custom_cat_id
                            stream_copy["category_ids"] = [int(custom_cat_id)]
                            # Prefix epg_channel_id to match XMLTV channel IDs (lowercase for consistent matching)
                            epg_id = stream.get("epg_channel_id", "")
                            if epg_id:
                                stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                            result.append(stream_copy)
            return result

        elif action == "get_vod_streams":
            result = []
            requested_cat_id = request.query_params.get("category_id")
            
            # Build lookup of source index by source_id
            source_idx_map = {source.get("id"): idx for idx, source in enumerate(enabled_sources)}
            
            # Get custom categories and build a lookup: (source_id, item_id) -> list of custom_cat_ids (strings)
            custom_cats = get_custom_categories_for_content_type("vod")
            custom_item_cats = {}
            for custom_cat in custom_cats:
                cat_id_str = custom_cat_id_to_numeric(custom_cat['id'])
                for item in custom_cat["items"]:
                    key = (item.get("source_id"), str(item.get("id")))
                    if key not in custom_item_cats:
                        custom_item_cats[key] = []
                    custom_item_cats[key].append(cat_id_str)
            
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
                    stream_id = str(stream.get("stream_id", ""))
                    
                    if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                        virtual_cat_id = str(encode_virtual_id(idx, stream.get("category_id", 0)))
                        # Only include if no category filter or matches
                        if requested_cat_id is None or requested_cat_id == virtual_cat_id:
                            stream_copy = stream.copy()
                            stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                            stream_copy["category_id"] = virtual_cat_id
                            stream_copy["category_ids"] = [int(virtual_cat_id)]
                            result.append(stream_copy)
                    
                    # Also add to custom categories if applicable
                    custom_cat_ids = custom_item_cats.get((source_id, stream_id), [])
                    for custom_cat_id in custom_cat_ids:
                        # Only include if no category filter or matches this custom category
                        if requested_cat_id is None or requested_cat_id == custom_cat_id:
                            stream_copy = stream.copy()
                            stream_copy["stream_id"] = encode_virtual_id(idx, stream.get("stream_id", 0))
                            stream_copy["category_id"] = custom_cat_id
                            stream_copy["category_ids"] = [int(custom_cat_id)]
                            result.append(stream_copy)
            return result

        elif action == "get_series":
            result = []
            requested_cat_id = request.query_params.get("category_id")
            
            # Build lookup of source index by source_id
            source_idx_map = {source.get("id"): idx for idx, source in enumerate(enabled_sources)}
            
            # Get custom categories and build a lookup: (source_id, item_id) -> list of custom_cat_ids (strings)
            custom_cats = get_custom_categories_for_content_type("series")
            custom_item_cats = {}
            for custom_cat in custom_cats:
                cat_id_str = custom_cat_id_to_numeric(custom_cat['id'])
                for item in custom_cat["items"]:
                    key = (item.get("source_id"), str(item.get("id")))
                    if key not in custom_item_cats:
                        custom_item_cats[key] = []
                    custom_item_cats[key].append(cat_id_str)
            
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
                    series_id = str(series.get("series_id", ""))
                    
                    if should_include(group_name, group_filters) and should_include(series_name, channel_filters):
                        virtual_cat_id = str(encode_virtual_id(idx, series.get("category_id", 0)))
                        # Only include if no category filter or matches
                        if requested_cat_id is None or requested_cat_id == virtual_cat_id:
                            series_copy = series.copy()
                            series_copy["series_id"] = encode_virtual_id(idx, series.get("series_id", 0))
                            series_copy["category_id"] = virtual_cat_id
                            series_copy["category_ids"] = [int(virtual_cat_id)]
                            result.append(series_copy)
                    
                    # Also add to custom categories if applicable
                    custom_cat_ids = custom_item_cats.get((source_id, series_id), [])
                    for custom_cat_id in custom_cat_ids:
                        # Only include if no category filter or matches this custom category
                        if requested_cat_id is None or requested_cat_id == custom_cat_id:
                            series_copy = series.copy()
                            series_copy["series_id"] = encode_virtual_id(idx, series.get("series_id", 0))
                            series_copy["category_id"] = custom_cat_id
                            series_copy["category_ids"] = [int(custom_cat_id)]
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
                    stream_copy = stream.copy()
                    # Prefix epg_channel_id to match XMLTV channel IDs (lowercase for consistent matching)
                    epg_id = stream.get("epg_channel_id", "")
                    if epg_id:
                        stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                    result.append(stream_copy)
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


# Per-source M3U playlist routes
@app.get("/{source_route}/playlist.m3u")
@app.get("/{source_route}/get.php")
async def playlist_source(source_route: str, request: Request):
    """Serve filtered M3U playlist for a specific source."""
    if source_route in ("full", "live", "movie", "series", "api", "static", "merged"):
        return Response(content="Not found", status_code=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)
    
    # Build a config with only this source
    source_id = source.get("id")
    single_source_config = {
        "sources": [source],
        "content_types": {"live": True, "vod": True, "series": True},
    }
    
    # Generate M3U with source-relative URLs (no virtual IDs needed for single source)
    m3u_content = generate_m3u_for_source(source, request, source_route)
    
    return Response(
        content=m3u_content,
        media_type="audio/x-mpegurl",
        headers={"Content-Disposition": f'attachment; filename="{source_route}_playlist.m3u"', "Cache-Control": "no-cache"},
    )


# Per-source XMLTV/EPG routes
@app.get("/{source_route}/xmltv.php")
@app.get("/{source_route}/epg.xml")
async def xmltv_source(source_route: str, request: Request):
    """Serve XMLTV EPG for a specific source from the cached EPG file.
    
    Reads from the merged EPG cache and filters only the channels/programmes 
    belonging to the specific source (identified by their source_id prefix).
    """
    # Handle merged route - serve full merged EPG
    if source_route == "merged":
        return await get_merged_xmltv()
    
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response(content="Not found", status_code=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return JSONResponse({"error": f"Source route '{source_route}' not found"}, status_code=404)
    
    source_id = source.get("id")
    source_prefix = f"{source_id}_"
    
    # Read from the cached EPG file
    cache_path = Path("/data/epg_cache.xml")
    if not cache_path.exists():
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- No EPG cache available --></tv>',
            media_type="application/xml"
        )
    
    try:
        # Parse the cached EPG file
        parser = etree.XMLParser(recover=True)
        tree = etree.parse(str(cache_path), parser)
        root = tree.getroot()
        
        # Create a new root for this source's EPG
        new_root = etree.Element("tv", attrib={"generator-info-name": f"XtreamFilter - {source.get('name', source_id)}"})
        
        # Filter channel elements - only include those belonging to this source
        for channel in root.findall("channel"):
            channel_id = channel.get("id", "")
            if channel_id.startswith(source_prefix):
                # Create a copy of the channel element
                new_channel = etree.Element("channel", id=channel_id)
                for child in channel:
                    new_channel.append(child)
                new_root.append(new_channel)
        
        # Filter programme elements - only include those belonging to this source
        for programme in root.findall("programme"):
            programme_channel = programme.get("channel", "")
            if programme_channel.startswith(source_prefix):
                new_root.append(programme)
        
        # Log the count
        channel_count = len(new_root.findall("channel"))
        programme_count = len(new_root.findall("programme"))
        logger.info(f"Serving EPG for source '{source_route}': {channel_count} channels, {programme_count} programmes")
        
        # Serialize
        result = etree.tostring(new_root, encoding="UTF-8", xml_declaration=True, pretty_print=False)
        
        return Response(
            content=result,
            media_type="application/xml",
            headers={"Content-Type": "application/xml; charset=utf-8"}
        )
        
    except Exception as e:
        logger.error(f"Error processing EPG for source '{source_route}': {e}")
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- EPG processing error --></tv>',
            media_type="application/xml"
        )


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
            streams = get_cached("live_streams", source_id)
            result = []
            for stream in streams:
                stream_copy = stream.copy()
                # Prefix epg_channel_id to match XMLTV channel IDs (lowercase for consistent matching)
                epg_id = stream.get("epg_channel_id", "")
                if epg_id:
                    stream_copy["epg_channel_id"] = f"{source_id}_{epg_id}".lower()
                result.append(stream_copy)
            return result

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
# XMLTV MERGED EPG
# ============================================


@app.get("/xmltv.php")
@app.get("/merged/xmltv.php")
async def get_merged_xmltv():
    """Return merged EPG/XMLTV from all enabled sources with cached support."""
    # Check if cache is valid
    if is_epg_cache_valid() and _epg_cache.get("data"):
        logger.debug("Serving EPG from cache")
        return Response(
            content=_epg_cache["data"],
            media_type="application/xml",
            headers={"Content-Type": "application/xml; charset=utf-8"}
        )
    
    # Check if refresh is already in progress
    if _epg_cache.get("refresh_in_progress"):
        # Return stale cache if available while refresh is in progress
        if _epg_cache.get("data"):
            logger.info("EPG refresh in progress, serving stale cache")
            return Response(
                content=_epg_cache["data"],
                media_type="application/xml",
                headers={"Content-Type": "application/xml; charset=utf-8"}
            )
        else:
            return Response(
                content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- EPG refresh in progress --></tv>',
                media_type="application/xml"
            )
    
    # Trigger refresh and wait for it
    await refresh_epg_cache()
    
    # Return the freshly cached data
    if _epg_cache.get("data"):
        return Response(
            content=_epg_cache["data"],
            media_type="application/xml",
            headers={"Content-Type": "application/xml; charset=utf-8"}
        )
    
    return Response(
        content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- No EPG data available --></tv>',
        media_type="application/xml"
    )


@app.post("/api/epg/refresh")
async def trigger_epg_refresh():
    """Manually trigger EPG cache refresh."""
    if _epg_cache.get("refresh_in_progress"):
        return {"status": "already_in_progress", "message": "EPG refresh is already in progress"}
    
    # Trigger refresh in background
    asyncio.create_task(refresh_epg_cache())
    
    return {"status": "started", "message": "EPG refresh started"}


@app.get("/api/epg/status")
async def get_epg_status():
    """Get EPG cache status."""
    return {
        "cached": _epg_cache.get("data") is not None,
        "last_refresh": _epg_cache.get("last_refresh"),
        "refresh_in_progress": _epg_cache.get("refresh_in_progress", False),
        "cache_valid": is_epg_cache_valid(),
        "cache_ttl_seconds": get_epg_cache_ttl(),
        "cache_size_bytes": len(_epg_cache.get("data", b"")) if _epg_cache.get("data") else 0
    }


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


@app.get("/api/options/refresh_interval")
async def get_refresh_interval_api():
    """Get cache refresh interval"""
    return {"refresh_interval": get_refresh_interval()}


@app.post("/api/options/refresh_interval")
async def set_refresh_interval_api(request: Request):
    """Set cache refresh interval (in seconds)"""
    config = load_config()
    data = await request.json()
    
    if "options" not in config:
        config["options"] = {}
    
    # Minimum 300 seconds (5 minutes)
    interval = max(int(data.get("refresh_interval", 3600)), 300)
    config["options"]["refresh_interval"] = interval
    save_config(config)
    
    return {"status": "ok", "refresh_interval": interval}


@app.get("/api/config/telegram")
async def get_telegram_config():
    """Get Telegram notification settings"""
    config = load_config()
    telegram_config = config.get("options", {}).get("telegram", {
        "enabled": False,
        "bot_token": "",
        "chat_id": ""
    })
    # Mask the bot token for security (show only last 4 chars)
    masked_config = telegram_config.copy()
    if masked_config.get("bot_token"):
        token = masked_config["bot_token"]
        masked_config["bot_token_masked"] = f"***{token[-4:]}" if len(token) > 4 else "***"
    return masked_config


@app.post("/api/config/telegram")
async def update_telegram_config(request: Request):
    """Update Telegram notification settings"""
    config = load_config()
    data = await request.json()
    
    if "options" not in config:
        config["options"] = {}
    
    if "telegram" not in config["options"]:
        config["options"]["telegram"] = {
            "enabled": False,
            "bot_token": "",
            "chat_id": ""
        }
    
    # Update only provided fields
    if "enabled" in data:
        config["options"]["telegram"]["enabled"] = data["enabled"]
    if "bot_token" in data:
        config["options"]["telegram"]["bot_token"] = data["bot_token"]
    if "chat_id" in data:
        config["options"]["telegram"]["chat_id"] = data["chat_id"]
    
    save_config(config)
    
    return {"status": "ok", "message": "Telegram settings updated"}


@app.post("/api/config/telegram/test")
async def test_telegram_notification():
    """Send a test Telegram notification"""
    config = load_config()
    telegram_config = config.get("options", {}).get("telegram", {})
    
    bot_token = telegram_config.get("bot_token", "")
    chat_id = telegram_config.get("chat_id", "")
    
    if not bot_token or not chat_id:
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Bot token and chat ID are required"}
        )
    
    try:
        client = await get_http_client()
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": "🔔 <b>Test Notification</b>\n\nYour Telegram integration is working correctly!",
            "parse_mode": "HTML"
        }
        response = await client.post(url, json=payload)
        result = response.json()
        
        if response.status_code == 200 and result.get("ok"):
            return {"status": "ok", "message": "Test notification sent successfully"}
        else:
            error_msg = result.get("description", "Unknown error")
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": f"Telegram API error: {error_msg}"}
            )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to send test: {str(e)}"}
        )


@app.post("/api/config/telegram/test-diff")
async def test_telegram_diff_notification(request: Request):
    """Send a test diff notification (simulates new items found in a category)
    
    Body params:
    - single: sends single item with cover (uses sendPhoto)
    - album: sends multiple items with covers (uses sendMediaGroup)
    - (default): sends multiple items without covers (uses sendMessage)
    """
    try:
        body = await request.json()
    except:
        body = {}
    
    single = body.get("single", False)
    album = body.get("album", False)
    
    if single:
        # Single item with cover - will use sendPhoto
        sample_items = [
            {
                "name": "Avatar: The Way of Water (2022) 4K HDR",
                "cover": "https://image.tmdb.org/t/p/w500/t6HIqrRAclMCA60NsSmeqe9RmNV.jpg"
            }
        ]
        msg = "Test notification sent with 1 item (photo)"
    elif album:
        # Multiple items with covers - will use sendMediaGroup (album)
        # Using Wikipedia/Wikimedia images which are more reliably accessible
        sample_items = [
            {
                "name": "Inception (2010)",
                "cover": "https://upload.wikimedia.org/wikipedia/en/2/2e/Inception_%282010%29_theatrical_poster.jpg"
            },
            {
                "name": "The Dark Knight (2008)",
                "cover": "https://upload.wikimedia.org/wikipedia/en/1/1c/The_Dark_Knight_%282008_film%29.jpg"
            },
            {
                "name": "Interstellar (2014)",
                "cover": "https://upload.wikimedia.org/wikipedia/en/b/bc/Interstellar_film_poster.jpg"
            }
        ]
        msg = "Test notification sent with 3 items (album with covers)"
    else:
        # Multiple items without covers - will use sendMessage
        sample_items = [
            {"name": "Test Movie 1 - 4K HDR", "cover": ""},
            {"name": "Test Movie 2 - Dolby Vision", "cover": ""},
            {"name": "Test Series S01E01", "cover": ""}
        ]
        msg = "Test notification sent with 3 items (text list)"
    
    try:
        await send_telegram_notification("🧪 Test Category", sample_items)
        return {"status": "ok", "message": msg}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": f"Failed to send: {str(e)}"}
        )


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


# ============================================
# DOWNLOAD CART API
# ============================================


@app.get("/cart", response_class=HTMLResponse)
async def cart_page(request: Request):
    """Download cart page"""
    return templates.TemplateResponse("cart.html", {"request": request})


@app.get("/api/cart")
async def get_cart():
    """Get all cart items"""
    return {"items": _download_cart}


@app.post("/api/cart")
async def add_to_cart(request: Request):
    """Add item(s) to the download cart.
    
    Body: {
        source_id, stream_id, content_type, name, icon, group,
        container_extension,
        // For series: add_mode ("episode"|"season"|"series"), season_num (optional),
        //   series_id (the parent series ID), series_name
    }
    """
    data = await request.json()
    content_type = data.get("content_type", "vod")
    add_mode = data.get("add_mode", "episode")
    added_items = []

    if content_type == "series" and add_mode in ("series", "season"):
        # Fetch episodes from upstream and expand
        series_id = data.get("series_id", data.get("stream_id", ""))
        source_id = data.get("source_id", "")
        series_name = data.get("series_name", data.get("name", ""))
        season_filter = data.get("season_num") if add_mode == "season" else None

        episodes = await fetch_series_episodes(source_id, series_id)
        if not episodes:
            return JSONResponse({"error": "Could not fetch series episodes"}, status_code=400)

        for ep in episodes:
            if season_filter and str(ep["season"]) != str(season_filter):
                continue
            # Check for duplicates
            if any(
                i.get("source_id") == source_id
                and i.get("stream_id") == ep["stream_id"]
                and i.get("status") in ("queued", "downloading")
                for i in _download_cart
            ):
                continue

            item = {
                "id": str(uuid.uuid4()),
                "stream_id": ep["stream_id"],
                "source_id": source_id,
                "content_type": "series",
                "name": ep.get("title", "") or f"Episode {ep['episode_num']}",
                "series_name": ep.get("series_name", series_name),
                "season": ep["season"],
                "episode_num": ep.get("episode_num", 0),
                "episode_title": ep.get("title", ""),
                "icon": data.get("icon", ""),
                "group": data.get("group", ""),
                "container_extension": ep.get("container_extension", "mp4"),
                "added_at": datetime.now().isoformat(),
                "status": "queued",
                "progress": 0,
                "error": None,
                "file_path": None,
                "file_size": None,
            }
            _download_cart.append(item)
            added_items.append(item)
    else:
        # Single item (VOD or single series episode)
        source_id = data.get("source_id", "")
        stream_id = data.get("stream_id", "")

        # Check for duplicates
        if any(
            i.get("source_id") == source_id
            and i.get("stream_id") == stream_id
            and i.get("status") in ("queued", "downloading")
            for i in _download_cart
        ):
            return JSONResponse({"error": "Item already in cart"}, status_code=409)

        item = {
            "id": str(uuid.uuid4()),
            "stream_id": stream_id,
            "source_id": source_id,
            "content_type": content_type,
            "name": data.get("name", ""),
            "series_name": data.get("series_name"),
            "season": data.get("season"),
            "episode_num": data.get("episode_num"),
            "episode_title": data.get("episode_title"),
            "icon": data.get("icon", ""),
            "group": data.get("group", ""),
            "container_extension": data.get("container_extension", "mp4"),
            "added_at": datetime.now().isoformat(),
            "status": "queued",
            "progress": 0,
            "error": None,
            "file_path": None,
            "file_size": None,
        }
        _download_cart.append(item)
        added_items.append(item)

    save_cart()
    return {"status": "ok", "added": len(added_items), "items": added_items}


@app.delete("/api/cart/{item_id}")
async def remove_from_cart(item_id: str):
    """Remove an item from the cart"""
    global _download_cart
    original_len = len(_download_cart)
    _download_cart = [i for i in _download_cart if i.get("id") != item_id]
    if len(_download_cart) == original_len:
        return JSONResponse({"error": "Item not found"}, status_code=404)
    save_cart()
    return {"status": "ok"}


@app.post("/api/cart/{item_id}/retry")
async def retry_cart_item(item_id: str):
    """Retry a failed or cancelled cart item by resetting it to queued"""
    for item in _download_cart:
        if item.get("id") == item_id:
            if item.get("status") not in ("failed", "cancelled"):
                return JSONResponse({"error": "Item is not in a retryable state"}, status_code=400)
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            save_cart()
            return {"status": "ok", "message": f"Re-queued: {item.get('name', '')}"}
    return JSONResponse({"error": "Item not found"}, status_code=404)


@app.post("/api/cart/retry-all")
async def retry_all_failed():
    """Retry all failed/cancelled items"""
    count = 0
    for item in _download_cart:
        if item.get("status") in ("failed", "cancelled"):
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            count += 1
    save_cart()
    return {"status": "ok", "retried": count}


@app.post("/api/cart/clear")
async def clear_cart(request: Request):
    """Clear cart items by status. Body: { mode: 'completed'|'failed'|'all' }"""
    global _download_cart
    data = await request.json()
    mode = data.get("mode", "completed")

    if mode == "all":
        # Don't clear currently-downloading items
        _download_cart = [i for i in _download_cart if i.get("status") == "downloading"]
    elif mode == "completed":
        _download_cart = [i for i in _download_cart if i.get("status") != "completed"]
    elif mode == "failed":
        _download_cart = [i for i in _download_cart if i.get("status") not in ("failed", "cancelled")]
    elif mode == "finished":
        _download_cart = [i for i in _download_cart if i.get("status") not in ("completed", "failed", "cancelled")]

    save_cart()
    return {"status": "ok", "remaining": len(_download_cart)}


@app.post("/api/cart/start")
async def start_downloads():
    """Start processing the download queue"""
    global _download_task

    queued = [i for i in _download_cart if i.get("status") == "queued"]
    if not queued:
        return JSONResponse({"error": "No queued items to download"}, status_code=400)

    # Check if already running
    if _download_task and not _download_task.done():
        return JSONResponse({"error": "Downloads already in progress"}, status_code=409)

    # Ensure download directory exists
    download_path = get_download_path()
    try:
        os.makedirs(download_path, exist_ok=True)
    except OSError as e:
        return JSONResponse({"error": f"Cannot create download directory: {e}"}, status_code=500)

    _download_task = asyncio.create_task(download_worker())
    return {"status": "ok", "message": f"Started downloading {len(queued)} items"}


@app.post("/api/cart/cancel")
async def cancel_download():
    """Cancel the current download"""
    global _download_cancel_event
    if _download_cancel_event:
        _download_cancel_event.set()
        return {"status": "ok", "message": "Download cancellation requested"}
    return JSONResponse({"error": "No active download"}, status_code=400)


@app.get("/api/cart/status")
async def cart_status():
    """Get download queue status"""
    queued = len([i for i in _download_cart if i.get("status") == "queued"])
    downloading = len([i for i in _download_cart if i.get("status") == "downloading"])
    completed = len([i for i in _download_cart if i.get("status") == "completed"])
    failed = len([i for i in _download_cart if i.get("status") in ("failed", "cancelled")])

    current = None
    if _download_current_item:
        current = {
            "name": _download_current_item.get("name", ""),
            "series_name": _download_current_item.get("series_name"),
            "season": _download_current_item.get("season"),
            "episode_num": _download_current_item.get("episode_num"),
            "progress": _download_current_item.get("progress", 0),
            "bytes_downloaded": _download_progress.get("bytes_downloaded", 0),
            "total_bytes": _download_progress.get("total_bytes", 0),
            "speed": _download_progress.get("speed", 0),
            "paused": _download_progress.get("paused", False),
            "pause_remaining": _download_progress.get("pause_remaining", 0),
        }

    is_running = _download_task is not None and not _download_task.done()

    return {
        "is_running": is_running,
        "queued": queued,
        "downloading": downloading,
        "completed": completed,
        "failed": failed,
        "total": len(_download_cart),
        "current": current,
    }


@app.get("/api/options/download_path")
async def get_download_path_api():
    """Get configured download path"""
    return {"download_path": get_download_path()}


@app.post("/api/options/download_path")
async def set_download_path_api(request: Request):
    """Set download path"""
    config = load_config()
    data = await request.json()
    path = data.get("download_path", "/data/downloads").strip()

    if not path:
        return JSONResponse({"error": "Path cannot be empty"}, status_code=400)

    # Try to create the directory
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        return JSONResponse({"error": f"Invalid path: {e}"}, status_code=400)

    if "options" not in config:
        config["options"] = {}
    config["options"]["download_path"] = path
    save_config(config)

    return {"status": "ok", "download_path": path}


@app.get("/api/options/download_temp_path")
async def get_download_temp_path_api():
    """Get configured temporary download path"""
    return {"download_temp_path": get_download_temp_path()}


@app.post("/api/options/download_temp_path")
async def set_download_temp_path_api(request: Request):
    """Set temporary download path"""
    config = load_config()
    data = await request.json()
    path = data.get("download_temp_path", "/data/downloads/.tmp").strip()

    if not path:
        return JSONResponse({"error": "Path cannot be empty"}, status_code=400)

    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        return JSONResponse({"error": f"Invalid path: {e}"}, status_code=400)

    if "options" not in config:
        config["options"] = {}
    config["options"]["download_temp_path"] = path
    save_config(config)

    return {"status": "ok", "download_temp_path": path}


@app.get("/api/options/download_throttle")
async def get_download_throttle_api():
    """Get download throttle settings"""
    return get_download_throttle_settings()


@app.post("/api/options/download_throttle")
async def set_download_throttle_api(request: Request):
    """Set download throttle settings (bandwidth limit, periodic pause, player profile)"""
    config = load_config()
    data = await request.json()

    bw = data.get("bandwidth_limit", 0)
    interval = data.get("pause_interval", 0)
    duration = data.get("pause_duration", 0)
    profile = data.get("player_profile", "tivimate")

    # Validate
    try:
        bw = max(0, int(bw))
        interval = max(0, int(interval))
        duration = max(0, int(duration))
    except (ValueError, TypeError):
        return JSONResponse({"error": "Invalid numeric values"}, status_code=400)

    if profile not in PLAYER_PROFILES:
        profile = "tivimate"

    if "options" not in config:
        config["options"] = {}
    config["options"]["download_bandwidth_limit"] = bw
    config["options"]["download_pause_interval"] = interval
    config["options"]["download_pause_duration"] = duration
    config["options"]["download_player_profile"] = profile
    save_config(config)

    logger.info(f"Download throttle updated: bandwidth={bw} KB/s, pause every {interval}s for {duration}s, profile={profile}")
    return {"status": "ok", "bandwidth_limit": bw, "pause_interval": interval, "pause_duration": duration, "player_profile": profile}


@app.get("/api/options/player_profiles")
async def get_player_profiles_api():
    """Get available IPTV player profiles"""
    return {k: {"name": v["name"], "user_agent": v["headers"]["User-Agent"]} for k, v in PLAYER_PROFILES.items()}


@app.get("/api/cart/series-episodes/{source_id}/{series_id}")
async def get_series_episodes_api(source_id: str, series_id: str):
    """Fetch series episodes for the cart episode selector"""
    episodes = await fetch_series_episodes(source_id, series_id)
    if not episodes:
        return JSONResponse({"error": "Could not fetch episodes"}, status_code=400)

    # Group by season
    seasons = {}
    for ep in episodes:
        s = ep["season"]
        if s not in seasons:
            seasons[s] = []
        seasons[s].append(ep)

    # Sort seasons and episodes
    sorted_seasons = {}
    for s in sorted(seasons.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        sorted_seasons[s] = sorted(seasons[s], key=lambda e: int(e.get("episode_num", 0)))

    return {
        "series_name": episodes[0].get("series_name", "") if episodes else "",
        "seasons": sorted_seasons,
        "total_episodes": len(episodes),
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok"}


@app.get("/api/version")
async def check_version():
    """Check current version and whether a newer version is available on GitHub."""
    import time as _time
    now = _time.time()
    # Cache for 1 hour to avoid hitting GitHub API too often
    if _version_cache["latest"] and now - _version_cache["checked_at"] < 3600:
        latest = _version_cache["latest"]
        release_url = _version_cache.get("release_url", "")
    else:
        latest = None
        release_url = ""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
                    headers={"Accept": "application/vnd.github.v3+json", "User-Agent": "XtreamFilter"}
                )
                if resp.status_code == 200:
                    data = resp.json()
                    latest = data.get("tag_name", "").lstrip("v")
                    release_url = data.get("html_url", "")
                    _version_cache["latest"] = latest
                    _version_cache["release_url"] = release_url
                    _version_cache["checked_at"] = now
        except Exception as e:
            logger.warning(f"Failed to check for updates: {e}")

    update_available = False
    if latest:
        try:
            from packaging.version import Version
            update_available = Version(latest) > Version(APP_VERSION)
        except Exception:
            update_available = latest != APP_VERSION

    return {
        "current": APP_VERSION,
        "latest": latest,
        "update_available": update_available,
        "release_url": release_url,
    }


# ============================================
# MAIN ENTRY POINT
# ============================================


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)

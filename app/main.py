import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timedelta

import requests
from flask import Flask, Response, jsonify, redirect, render_template, request, url_for

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Data directory - use environment variable or default to /data (Docker) or ./data (local)
DATA_DIR = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "./data")
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
CACHE_FILE = os.path.join(DATA_DIR, "playlist_cache.m3u")
API_CACHE_FILE = os.path.join(DATA_DIR, "api_cache.json")

# Headers to mimic a browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ============================================
# CACHING SYSTEM
# ============================================

# In-memory cache - now supports multiple sources
# Structure: { "sources": { source_id: { categories, streams, last_refresh }, ... }, "refresh_in_progress": False }
_api_cache = {
    "sources": {},  # Per-source cached data
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
_cache_lock = threading.Lock()

# Stream ID to source mapping for routing
_stream_source_map = {
    "live": {},    # stream_id -> source_id
    "vod": {},     # stream_id -> source_id
    "series": {},  # series_id -> source_id
}
_stream_map_lock = threading.Lock()


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
                with _cache_lock:
                    # Handle migration from old single-source cache format
                    if "sources" not in data and "live_streams" in data:
                        # Old format - migrate to new multi-source format
                        logger.info("Migrating old cache format to multi-source format")
                        _api_cache = {
                            "sources": {},
                            "last_refresh": data.get("last_refresh"),
                            "refresh_in_progress": False,
                        }
                    else:
                        _api_cache.update(data)
                        _api_cache["refresh_in_progress"] = False
                # Rebuild stream-to-source mapping
                rebuild_stream_source_map()
                logger.info(f"Loaded cache from disk. Last refresh: {_api_cache.get('last_refresh', 'Never')}")
        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")


def save_cache_to_disk():
    """Save cache to disk"""
    try:
        with _cache_lock:
            data = {k: v for k, v in _api_cache.items() if k != "refresh_in_progress"}
        os.makedirs(os.path.dirname(API_CACHE_FILE), exist_ok=True)
        with open(API_CACHE_FILE, "w") as f:
            json.dump(data, f)
        logger.info(f"Cache saved to disk at {datetime.now().isoformat()}")
    except Exception as e:
        logger.error(f"Failed to save cache to disk: {e}")


def is_cache_valid():
    """Check if cache is still valid based on TTL"""
    with _cache_lock:
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
    with _cache_lock:
        sources = _api_cache.get("sources", {})
        
        if source_id is not None:
            # Get from specific source
            source_cache = sources.get(source_id, {})
            return source_cache.get(key, [])
        
        # Merge from all sources
        result = []
        for src_id, src_cache in sources.items():
            result.extend(src_cache.get(key, []))
        return result


def rebuild_stream_source_map():
    """Rebuild the stream ID to source mapping for routing"""
    global _stream_source_map
    with _cache_lock:
        sources = _api_cache.get("sources", {})
    
    new_map = {"live": {}, "vod": {}, "series": {}}
    
    for source_id, source_cache in sources.items():
        # Map live streams
        for stream in source_cache.get("live_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["live"][stream_id] = source_id
        
        # Map VOD streams
        for stream in source_cache.get("vod_streams", []):
            stream_id = str(stream.get("stream_id", ""))
            if stream_id:
                new_map["vod"][stream_id] = source_id
        
        # Map series
        for series in source_cache.get("series", []):
            series_id = str(series.get("series_id", ""))
            if series_id:
                new_map["series"][series_id] = source_id
    
    with _stream_map_lock:
        _stream_source_map = new_map
    
    logger.info(f"Rebuilt stream-source map: {len(new_map['live'])} live, {len(new_map['vod'])} vod, {len(new_map['series'])} series")


def get_source_for_stream(stream_id, stream_type="live"):
    """Get the source ID for a given stream ID"""
    with _stream_map_lock:
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


def refresh_cache():
    """Refresh all cached data from all configured sources"""
    global _api_cache

    with _cache_lock:
        if _api_cache.get("refresh_in_progress"):
            logger.info("Refresh already in progress, skipping")
            return False
        _api_cache["refresh_in_progress"] = True
        _api_cache["refresh_progress"] = {
            "current_source": 0,
            "total_sources": 0,
            "current_source_name": "",
            "current_step": "Initializing...",
            "percent": 0
        }

    config = load_config()
    sources = config.get("sources", [])

    # Backward compatibility: if no sources but xtream config exists, treat as single source
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

    # Filter enabled sources
    enabled_sources = [s for s in sources if s.get("enabled", True) and s.get("host") and s.get("username") and s.get("password")]
    
    if not enabled_sources:
        logger.info("Cannot refresh - no valid sources configured")
        with _cache_lock:
            _api_cache["refresh_in_progress"] = False
            _api_cache["refresh_progress"]["percent"] = 0
            _api_cache["refresh_progress"]["current_step"] = "No sources"
        return False

    total_sources = len(enabled_sources)
    with _cache_lock:
        _api_cache["refresh_progress"]["total_sources"] = total_sources

    logger.info(f"Starting full refresh at {datetime.now().isoformat()} for {total_sources} source(s)")

    new_sources_cache = {}
    total_stats = {"live_cats": 0, "live_streams": 0, "vod_cats": 0, "vod_streams": 0, "series_cats": 0, "series": 0}

    for source_idx, source in enumerate(enabled_sources):
        source_id = source.get("id", "default")
        source_name = source.get("name", source_id)
        host = source.get("host", "").rstrip("/")
        username = source.get("username", "")
        password = source.get("password", "")

        # Update progress
        with _cache_lock:
            _api_cache["refresh_progress"]["current_source"] = source_idx + 1
            _api_cache["refresh_progress"]["current_source_name"] = source_name

        logger.info(f"Refreshing source: {source_name}")

        try:
            # Fetch all data for this source with progress updates
            def update_step(step_name, step_num):
                with _cache_lock:
                    _api_cache["refresh_progress"]["current_step"] = f"{source_name}: {step_name}"
                    # Each source has 6 steps, calculate overall percent
                    base_percent = (source_idx / total_sources) * 100
                    step_percent = (step_num / 6) * (100 / total_sources)
                    _api_cache["refresh_progress"]["percent"] = int(base_percent + step_percent)

            update_step("Live categories", 0)
            live_cats = fetch_from_upstream(host, username, password, "get_live_categories") or []
            
            update_step("VOD categories", 1)
            vod_cats = fetch_from_upstream(host, username, password, "get_vod_categories") or []
            
            update_step("Series categories", 2)
            series_cats = fetch_from_upstream(host, username, password, "get_series_categories") or []
            
            update_step("Live streams", 3)
            live_streams = fetch_from_upstream(host, username, password, "get_live_streams") or []
            
            update_step("VOD streams", 4)
            vod_streams = fetch_from_upstream(host, username, password, "get_vod_streams") or []
            
            update_step("Series", 5)
            series = fetch_from_upstream(host, username, password, "get_series") or []

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

    # Update cache
    with _cache_lock:
        _api_cache["sources"] = new_sources_cache
        _api_cache["last_refresh"] = datetime.now().isoformat()
        _api_cache["refresh_in_progress"] = False
        _api_cache["refresh_progress"] = {
            "current_source": total_sources,
            "total_sources": total_sources,
            "current_source_name": "",
            "current_step": "Complete",
            "percent": 100
        }

    # Rebuild stream-to-source mapping
    rebuild_stream_source_map()

    save_cache_to_disk()

    logger.info(
        f"Refresh complete. Total: {total_stats['live_streams']} live, "
        f"{total_stats['vod_streams']} vod, {total_stats['series']} series"
    )

    return True


def fetch_from_upstream(host, username, password, action):
    """Fetch data from upstream Xtream server"""
    url = f"{host}/player_api.php"
    params = {"username": username, "password": password, "action": action}

    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=120)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.info(f"Error fetching {action}: {e}")
    return None


def background_refresh_loop():
    """Background thread that periodically refreshes the cache"""
    logger.info("Background refresh thread started")

    # Initial delay to let the app start
    time.sleep(10)

    while True:
        try:
            ttl = get_cache_ttl()

            if not is_cache_valid():
                logger.info("Cache expired, triggering refresh...")
                refresh_cache()
            else:
                with _cache_lock:
                    last_refresh = _api_cache.get("last_refresh", "Never")
                logger.info(f"Cache still valid. Last refresh: {last_refresh}")

            # Sleep for 1/4 of TTL or minimum 5 minutes
            sleep_time = max(ttl // 4, 300)
            time.sleep(sleep_time)

        except Exception as e:
            logger.info(f"Background refresh error: {e}")
            time.sleep(60)


# Start background refresh thread
_refresh_thread = None


def start_background_refresh():
    """Start the background refresh thread"""
    global _refresh_thread
    if _refresh_thread is None or not _refresh_thread.is_alive():
        _refresh_thread = threading.Thread(target=background_refresh_loop, daemon=True)
        _refresh_thread.start()
        logger.info("Background refresh thread initialized")


def load_config():
    """Load configuration from file"""
    default_filters = {
        "live": {"groups": [], "channels": []},
        "vod": {"groups": [], "channels": []},
        "series": {"groups": [], "channels": []},
    }
    
    default_config = {
        "sources": [],  # List of source configurations
        "xtream": {"host": "", "username": "", "password": ""},  # Legacy single source (for backward compat)
        "filters": default_filters.copy(),  # Legacy global filters (for backward compat)
        "content_types": {
            "live": True,
            "vod": True,
            "series": True,
        },
        "options": {"cache_enabled": True, "cache_ttl": 3600},
    }

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                config = json.load(f)
                
                # Merge with defaults to ensure all keys exist
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                
                # Migrate old single-source config to multi-source if needed
                if not config.get("sources") and config.get("xtream", {}).get("host"):
                    # Migrate old filters structure first
                    filters = config.get("filters", {})
                    if "groups" in filters and isinstance(filters["groups"], list):
                        old_groups = filters.get("groups", [])
                        old_channels = filters.get("channels", [])
                        filters = {
                            "live": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "vod": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "series": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                        }
                    
                    # Create source from legacy config
                    config["sources"] = [{
                        "id": str(uuid.uuid4())[:8],
                        "name": "Default",
                        "host": config["xtream"]["host"],
                        "username": config["xtream"]["username"],
                        "password": config["xtream"]["password"],
                        "enabled": True,
                        "prefix": "",  # No prefix for seamless merge
                        "filters": filters,
                    }]
                    logger.info("Migrated legacy single-source config to multi-source format")
                
                # Ensure each source has all required fields
                for source in config.get("sources", []):
                    if "filters" not in source:
                        source["filters"] = default_filters.copy()
                    if "enabled" not in source:
                        source["enabled"] = True
                    if "prefix" not in source:
                        source["prefix"] = ""
                    # Ensure filter structure is complete
                    for cat in ["live", "vod", "series"]:
                        if cat not in source["filters"]:
                            source["filters"][cat] = {"groups": [], "channels": []}
                        if "groups" not in source["filters"][cat]:
                            source["filters"][cat]["groups"] = []
                        if "channels" not in source["filters"][cat]:
                            source["filters"][cat]["channels"] = []
                
                # Ensure content_types exists
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


def get_categories(host, username, password, content_type="live"):
    """Get categories from Xtream API for a specific content type"""
    actions = {"live": "get_live_categories", "vod": "get_vod_categories", "series": "get_series_categories"}
    action = actions.get(content_type, "get_live_categories")
    url = f"{host}/player_api.php?username={username}&password={password}&action={action}"
    try:
        response = requests.get(url, timeout=60, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except (requests.RequestException, json.JSONDecodeError):
        return []


def get_live_streams(host, username, password):
    """Get all live streams from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_live_streams"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except (requests.RequestException, json.JSONDecodeError):
        return []


def get_vod_streams(host, username, password):
    """Get all VOD (movies) from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_vod_streams"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except (requests.RequestException, json.JSONDecodeError):
        return []


def get_series(host, username, password):
    """Get all series from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_series"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except (requests.RequestException, json.JSONDecodeError):
        return []


def get_series_info(host, username, password, series_id):
    """Get series info including episodes from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_series_info&series_id={series_id}"
    try:
        response = requests.get(url, timeout=60, headers=HEADERS)
        return response.json() if response.status_code == 200 else {}
    except (requests.RequestException, json.JSONDecodeError):
        return {}


def matches_filter(value, filter_rule):
    """
    Check if a value matches a filter rule.

    Filter rule structure:
    {
        "type": "include" or "exclude",
        "match": "exact" | "starts_with" | "ends_with" | "contains" | "not_contains" | "regex" | "all",
        "value": "the pattern",
        "case_sensitive": true/false
    }
    """
    match_type = filter_rule.get("match", "contains")
    
    # "all" match type matches everything
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
    """
    Determine if a value should be included based on filter rules.

    Logic:
    1. If there are include rules, item must match at least one include rule
    2. If item matches any exclude rule, it's excluded
    3. If no include rules exist, items are included by default (unless excluded)
    """
    include_rules = [r for r in filter_rules if r.get("type") == "include"]
    exclude_rules = [r for r in filter_rules if r.get("type") == "exclude"]

    # Check exclude rules first - if any match, exclude the item
    for rule in exclude_rules:
        if matches_filter(value, rule):
            return False

    # If there are include rules, item must match at least one
    if include_rules:
        for rule in include_rules:
            if matches_filter(value, rule):
                return True
        return False  # No include rule matched

    # No include rules and no exclude rules matched - include by default
    return True


def generate_m3u(config):
    """Generate filtered M3U playlist from cached data, merging all sources"""
    sources = config.get("sources", [])
    content_types = config.get("content_types", {"live": True, "vod": True, "series": True})
    
    # Backward compatibility: if no sources but xtream config exists
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

    # Use this server's URL for stream proxying
    server_url = request.host_url.rstrip("/")

    lines = ["#EXTM3U"]
    stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0, "sources": 0}

    for source in sources:
        if not source.get("enabled", True):
            continue
        
        source_id = source.get("id", "default")
        prefix = source.get("prefix", "")
        filters = source.get("filters", {})
        
        # Check if source has credentials
        if not source.get("host") or not source.get("username") or not source.get("password"):
            continue
        
        stats["sources"] += 1

        # ===== LIVE TV =====
        if content_types.get("live", True):
            live_filters = filters.get("live", {"groups": [], "channels": []})
            live_group_filters = live_filters.get("groups", [])
            live_channel_filters = live_filters.get("channels", [])

            categories = get_cached("live_categories", source_id)
            cat_map = {str(cat.get("category_id", "")): cat.get("category_name", "") for cat in categories}
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

                # Apply prefix to group if set
                display_group = f"{prefix}{group}" if prefix else group
                
                # Use local proxy URL (stream routing will find the right source)
                stream_url = f"{server_url}/user/pass/{stream_id}.ts"
                lines.append(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-logo="{icon}" group-title="{display_group}",{name}')
                lines.append(stream_url)
                stats["live"] += 1

        # ===== VOD (MOVIES) =====
        if content_types.get("vod", True):
            vod_filters = filters.get("vod", {"groups": [], "channels": []})
            vod_group_filters = vod_filters.get("groups", [])
            vod_channel_filters = vod_filters.get("channels", [])

            vod_categories = get_cached("vod_categories", source_id)
            vod_cat_map = {str(cat.get("category_id", "")): cat.get("category_name", "") for cat in vod_categories}
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
                stream_url = f"{server_url}/movie/user/pass/{stream_id}.{extension}"
                lines.append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{display_group}",{name}')
                lines.append(stream_url)
                stats["vod"] += 1

        # ===== SERIES =====
        if content_types.get("series", True):
            series_filters = filters.get("series", {"groups": [], "channels": []})
            series_group_filters = series_filters.get("groups", [])
            series_channel_filters = series_filters.get("channels", [])

            series_categories = get_cached("series_categories", source_id)
            series_cat_map = {str(cat.get("category_id", "")): cat.get("category_name", "") for cat in series_categories}
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

    # Add stats as comment
    stats_line = f"# Content: {stats['live']} live, {stats['vod']} movies, {stats['series']} series | {stats['excluded']} excluded | {stats['sources']} source(s)"
    lines.insert(1, stats_line)

    return "\n".join(lines)


@app.route("/")
def index():
    """Main configuration page"""
    config = load_config()
    return render_template("index.html", config=config)


@app.route("/save", methods=["POST"])
def save():
    """Save configuration"""
    config = load_config()

    # Update Xtream settings
    config["xtream"]["host"] = request.form.get("host", "").strip()
    config["xtream"]["username"] = request.form.get("username", "").strip()
    config["xtream"]["password"] = request.form.get("password", "").strip()

    # Update content type settings (checkbox: present = True, absent = False)
    config["content_types"] = {
        "live": "content_live" in request.form,
        "vod": "content_vod" in request.form,
        "series": "content_series" in request.form,
    }

    save_config(config)

    # Clear cache
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

    return redirect(url_for("index"))


@app.route("/api/filters", methods=["GET"])
def get_filters():
    """Get all filter rules"""
    config = load_config()
    return jsonify(config["filters"])


@app.route("/api/filters", methods=["POST"])
def save_filters():
    """Save filter rules"""
    config = load_config()
    data = request.get_json()

    # Support both old structure and new per-category structure
    if "live" in data or "vod" in data or "series" in data:
        # New structure
        for cat in ["live", "vod", "series"]:
            if cat in data:
                config["filters"][cat] = data[cat]
    else:
        # Old structure - apply to all categories for backwards compat
        for cat in ["live", "vod", "series"]:
            config["filters"][cat]["groups"] = data.get("groups", [])
            config["filters"][cat]["channels"] = data.get("channels", [])

    save_config(config)

    # Clear cache
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)

    return jsonify({"status": "ok"})


@app.route("/api/filters/add", methods=["POST"])
def add_filter():
    """Add a single filter rule"""
    config = load_config()
    data = request.get_json()

    category = data.get("category", "live")  # "live", "vod", or "series"
    target = data.get("target", "groups")  # "groups" or "channels"
    rule = {
        "type": data.get("type", "exclude"),
        "match": data.get("match", "contains"),
        "value": data.get("value", ""),
        "case_sensitive": data.get("case_sensitive", False),
    }

    if category in config["filters"] and target in config["filters"][category]:
        config["filters"][category][target].append(rule)
        save_config(config)

    return jsonify({"status": "ok", "filters": config["filters"]})


@app.route("/api/filters/delete", methods=["POST"])
def delete_filter():
    """Delete a filter rule by index"""
    config = load_config()
    data = request.get_json()

    category = data.get("category", "live")  # "live", "vod", or "series"
    target = data.get("target", "groups")
    index = data.get("index", -1)

    if category in config["filters"] and target in config["filters"][category]:
        filter_list = config["filters"][category][target]
        if 0 <= index < len(filter_list):
            filter_list.pop(index)
            save_config(config)

    return jsonify({"status": "ok", "filters": config["filters"]})


# ============================================
# SOURCE MANAGEMENT API
# ============================================

@app.route("/api/sources", methods=["GET"])
def get_sources():
    """Get all configured sources"""
    config = load_config()
    return jsonify({"sources": config.get("sources", [])})


@app.route("/api/sources", methods=["POST"])
def add_source():
    """Add a new source"""
    config = load_config()
    data = request.get_json()
    
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
    
    return jsonify({"status": "ok", "source": new_source, "sources": config["sources"]})


@app.route("/api/sources/<source_id>", methods=["GET"])
def get_source(source_id):
    """Get a specific source by ID"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("id") == source_id:
            return jsonify({"source": source})
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>", methods=["PUT"])
def update_source(source_id):
    """Update a source"""
    config = load_config()
    data = request.get_json()
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            # Update fields
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
            return jsonify({"status": "ok", "source": source, "sources": config["sources"]})
    
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>", methods=["DELETE"])
def delete_source(source_id):
    """Delete a source"""
    config = load_config()
    
    sources = config.get("sources", [])
    config["sources"] = [s for s in sources if s.get("id") != source_id]
    
    if len(config["sources"]) < len(sources):
        save_config(config)
        return jsonify({"status": "ok", "sources": config["sources"]})
    
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>/filters", methods=["GET"])
def get_source_filters(source_id):
    """Get filters for a specific source"""
    config = load_config()
    for source in config.get("sources", []):
        if source.get("id") == source_id:
            return jsonify({"filters": source.get("filters", {})})
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>/filters", methods=["POST"])
def save_source_filters(source_id):
    """Save filters for a specific source"""
    config = load_config()
    data = request.get_json()
    
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            source["filters"] = data
            config["sources"][i] = source
            save_config(config)
            return jsonify({"status": "ok", "filters": source["filters"]})
    
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>/filters/add", methods=["POST"])
def add_source_filter(source_id):
    """Add a filter rule to a specific source"""
    config = load_config()
    data = request.get_json()
    
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
            return jsonify({"status": "ok", "filters": source["filters"]})
    
    return jsonify({"error": "Source not found"}), 404


@app.route("/api/sources/<source_id>/filters/delete", methods=["POST"])
def delete_source_filter(source_id):
    """Delete a filter rule from a specific source"""
    config = load_config()
    data = request.get_json()
    
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
            return jsonify({"status": "ok", "filters": source.get("filters", {})})
    
    return jsonify({"error": "Source not found"}), 404


@app.route("/playlist.m3u")
@app.route("/get.php")
def playlist():
    """Serve the filtered M3U playlist"""
    config = load_config()
    m3u_content = generate_m3u(config)

    return Response(
        m3u_content,
        mimetype="audio/x-mpegurl",
        headers={"Content-Disposition": 'inline; filename="playlist.m3u"', "Cache-Control": "no-cache"},
    )


@app.route("/playlist_full.m3u")
@app.route("/full.m3u")
def playlist_full():
    """Serve the unfiltered M3U playlist (full catalog)"""
    config = load_config()
    # Create a config copy with empty filters
    unfiltered_config = {
        "xtream": config["xtream"],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": config.get("content_types", {"live": True, "vod": True, "series": True}),
    }
    m3u_content = generate_m3u(unfiltered_config)

    return Response(
        m3u_content,
        mimetype="audio/x-mpegurl",
        headers={"Content-Disposition": 'inline; filename="playlist_full.m3u"', "Cache-Control": "no-cache"},
    )


@app.route("/groups")
def groups():
    """API endpoint to get all available groups for a content type"""
    config = load_config()
    sources = config.get("sources", [])
    content_type = request.args.get("type", "live")
    source_id = request.args.get("source_id")  # Optional: filter by source
    
    # Check if any sources configured
    if not sources and not config.get("xtream", {}).get("host"):
        return jsonify({"error": "Not configured", "groups": []})
    
    # Get categories from cache (merged from all sources or specific source)
    if content_type == "live":
        categories = get_cached("live_categories", source_id)
    elif content_type == "vod":
        categories = get_cached("vod_categories", source_id)
    elif content_type == "series":
        categories = get_cached("series_categories", source_id)
    else:
        categories = []
    
    groups_list = sorted(set(cat.get("category_name", "") for cat in categories if cat.get("category_name")))
    return jsonify({"groups": groups_list})


@app.route("/channels")
def channels():
    """API endpoint to get channel/item names with search and filtering"""
    config = load_config()
    sources = config.get("sources", [])
    content_type = request.args.get("type", "live")
    source_id = request.args.get("source_id")  # Optional: filter by source

    if not sources and not config.get("xtream", {}).get("host"):
        return jsonify({"error": "Not configured", "channels": []})

    search = request.args.get("search", "").lower()
    group_filter = request.args.get("group", "")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 100))

    # Get data from cache based on content type (merged from all sources or specific)
    if content_type == "live":
        streams = get_cached("live_streams", source_id)
        categories = get_cached("live_categories", source_id)
    elif content_type == "vod":
        streams = get_cached("vod_streams", source_id)
        categories = get_cached("vod_categories", source_id)
    elif content_type == "series":
        streams = get_cached("series", source_id)
        categories = get_cached("series_categories", source_id)
    else:
        streams = []
        categories = []

    # Build category mapping
    cat_map = {str(c.get("category_id", "")): c.get("category_name", "") for c in categories}

    # Extract channel info with group
    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        group = cat_map.get(cat_id, "Unknown")

        # Apply filters
        if search and search not in name.lower():
            continue
        if group_filter and group != group_filter:
            continue

        items.append({"name": name, "group": group})

    total = len(items)

    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    paginated = items[start:end]

    return jsonify(
        {
            "channels": [item["name"] for item in paginated],
            "items": paginated,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
        }
    )


@app.route("/api/browse")
def api_browse():
    """
    Enhanced browsing API for channels/items with full metadata.
    Supports filtering by content type, group, search, and pagination.
    """
    content_type = request.args.get("type", "live")
    search = request.args.get("search", "").lower()
    group_filter = request.args.get("group", "")
    page = int(request.args.get("page", 1))
    per_page = min(int(request.args.get("per_page", 50)), 200)

    # Get cached data
    if content_type == "live":
        streams = get_cached("live_streams")
        categories = get_cached("live_categories")
    elif content_type == "vod":
        streams = get_cached("vod_streams")
        categories = get_cached("vod_categories")
    elif content_type == "series":
        streams = get_cached("series")
        categories = get_cached("series_categories")
    else:
        return jsonify({"error": "Invalid content type", "items": []})

    # Build category mapping with counts
    cat_map = {str(c.get("category_id", "")): c.get("category_name", "") for c in categories}

    # Count items per group
    group_counts = {}
    for s in streams:
        cat_id = str(s.get("category_id", ""))
        group = cat_map.get(cat_id, "Unknown")
        group_counts[group] = group_counts.get(group, 0) + 1

    # Filter and build items list
    items = []
    for s in streams:
        name = s.get("name", "")
        cat_id = str(s.get("category_id", ""))
        group = cat_map.get(cat_id, "Unknown")
        icon = s.get("stream_icon", "") or s.get("cover", "")

        # Apply search filter
        if search and search not in name.lower() and search not in group.lower():
            continue

        # Apply group filter
        if group_filter and group != group_filter:
            continue

        items.append(
            {
                "name": name,
                "group": group,
                "icon": icon,
                "id": s.get("stream_id") or s.get("series_id"),
            }
        )

    total = len(items)

    # Sort by group then name
    items.sort(key=lambda x: (x["group"].lower(), x["name"].lower()))

    # Paginate
    start = (page - 1) * per_page
    end = start + per_page
    paginated = items[start:end]

    # Build groups list with counts for the dropdown
    groups_list = [{"name": g, "count": c} for g, c in sorted(group_counts.items())]

    return jsonify(
        {
            "items": paginated,
            "groups": groups_list,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
            "content_type": content_type,
        }
    )


@app.route("/stats")
def stats():
    """API endpoint to get playlist statistics"""
    config = load_config()
    xtream = config["xtream"]

    if not xtream["host"]:
        return jsonify({"error": "Not configured"})

    host = xtream["host"].rstrip("/")

    # Get counts for all content types
    live_categories = get_categories(host, xtream["username"], xtream["password"], "live")
    vod_categories = get_categories(host, xtream["username"], xtream["password"], "vod")
    series_categories = get_categories(host, xtream["username"], xtream["password"], "series")

    streams = get_live_streams(host, xtream["username"], xtream["password"])

    # Count all filters across categories
    filters = config["filters"]
    total_group_filters = 0
    total_channel_filters = 0
    for cat in ["live", "vod", "series"]:
        if cat in filters:
            total_group_filters += len(filters[cat].get("groups", []))
            total_channel_filters += len(filters[cat].get("channels", []))

    return jsonify(
        {
            "total_categories": len(live_categories) + len(vod_categories) + len(series_categories),
            "total_channels": len(streams),
            "live_categories": len(live_categories),
            "vod_categories": len(vod_categories),
            "series_categories": len(series_categories),
            "group_filters": total_group_filters,
            "channel_filters": total_channel_filters,
        }
    )


@app.route("/preview")
def preview():
    """Preview filtered playlist stats"""
    config = load_config()
    m3u_content = generate_m3u(config)

    # Parse stats from first comment
    lines = m3u_content.split("\n")
    stats_line = lines[1] if len(lines) > 1 else ""

    # Get sample channels
    sample = []
    for i in range(2, min(len(lines), 22), 2):
        if lines[i].startswith("#EXTINF"):
            name = lines[i].split(",", 1)[-1] if "," in lines[i] else lines[i]
            sample.append(name)

    return jsonify({"stats": stats_line, "sample_channels": sample})


# ============================================
# XTREAM CODES API PROXY
# ============================================
# These endpoints mimic a real Xtream Codes server so IPTV apps can connect


@app.route("/player_api.php")
def player_api():
    """
    Root Xtream Codes API endpoint - redirects to first source with a route,
    or returns an error if no routes are configured.
    """
    config = load_config()
    sources = config.get("sources", [])
    
    # Find first enabled source with a route
    for source in sources:
        if source.get("enabled", True) and source.get("route"):
            # Redirect to the dedicated route
            return player_api_source(source.get("route"))
    
    # No source with route found - return helpful error
    available_routes = [s.get("route") for s in sources if s.get("enabled", True) and s.get("route")]
    if available_routes:
        return jsonify({
            "error": "Please use a dedicated source route",
            "available_routes": [f"/{r}/player_api.php" for r in available_routes]
        }), 400
    
    return jsonify({
        "error": "No sources configured with dedicated routes. Please configure a route for each source in the web UI."
    }), 400


@app.route("/full/player_api.php")
def player_api_full():
    """
    Root full/unfiltered endpoint - redirects to first source with a route.
    """
    config = load_config()
    sources = config.get("sources", [])
    
    # Find first enabled source with a route
    for source in sources:
        if source.get("enabled", True) and source.get("route"):
            return player_api_source_full(source.get("route"))
    
    # No source with route found
    available_routes = [s.get("route") for s in sources if s.get("enabled", True) and s.get("route")]
    if available_routes:
        return jsonify({
            "error": "Please use a dedicated source route",
            "available_routes": [f"/{r}/full/player_api.php" for r in available_routes]
        }), 400
    
    return jsonify({
        "error": "No sources configured with dedicated routes. Please configure a route for each source in the web UI."
    }), 400


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


@app.route("/<source_route>/player_api.php")
def player_api_source(source_route):
    """
    Per-source Xtream Codes API endpoint - serves data from a single source only.
    This avoids ID conflicts between sources.
    """
    # Skip reserved routes
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return jsonify({"error": f"Source route '{source_route}' not found"}), 404
    
    source_id = source.get("id")
    source_filters = source.get("filters", {})
    prefix = source.get("prefix", "")
    action = request.args.get("action", "")

    try:
        host = source["host"].rstrip("/")
        
        # No action = authentication request
        if not action:
            response = requests.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                headers=HEADERS,
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                # Modify server_info to point to our proxy (source-specific endpoint)
                if "server_info" in data:
                    data["server_info"]["url"] = request.host_url.rstrip("/") + f"/{source_route}"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return jsonify(data)
            return Response(response.content, status=response.status_code)

        # Get Live Categories - filtered for this source only
        elif action == "get_live_categories":
            group_filters = source_filters.get("live", {}).get("groups", [])
            categories = get_cached("live_categories", source_id)
            result = []
            for cat in categories:
                cat_name = cat.get("category_name", "")
                if should_include(cat_name, group_filters):
                    cat_copy = cat.copy()
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return jsonify(result)

        # Get VOD Categories
        elif action == "get_vod_categories":
            group_filters = source_filters.get("vod", {}).get("groups", [])
            categories = get_cached("vod_categories", source_id)
            result = []
            for cat in categories:
                cat_name = cat.get("category_name", "")
                if should_include(cat_name, group_filters):
                    cat_copy = cat.copy()
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return jsonify(result)

        # Get Series Categories
        elif action == "get_series_categories":
            group_filters = source_filters.get("series", {}).get("groups", [])
            categories = get_cached("series_categories", source_id)
            result = []
            for cat in categories:
                cat_name = cat.get("category_name", "")
                if should_include(cat_name, group_filters):
                    cat_copy = cat.copy()
                    if prefix:
                        cat_copy["category_name"] = f"{prefix}{cat_name}"
                    result.append(cat_copy)
            return jsonify(result)

        # Get Live Streams
        elif action == "get_live_streams":
            group_filters = source_filters.get("live", {}).get("groups", [])
            channel_filters = source_filters.get("live", {}).get("channels", [])
            streams = get_cached("live_streams", source_id)
            categories = get_cached("live_categories", source_id)
            cat_map = {str(c.get("category_id", "")): c.get("category_name", "") for c in categories}
            
            result = []
            for stream in streams:
                cat_id = str(stream.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                channel_name = stream.get("name", "")
                if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                    result.append(stream)
            return jsonify(result)

        # Get VOD Streams
        elif action == "get_vod_streams":
            group_filters = source_filters.get("vod", {}).get("groups", [])
            channel_filters = source_filters.get("vod", {}).get("channels", [])
            streams = get_cached("vod_streams", source_id)
            categories = get_cached("vod_categories", source_id)
            cat_map = {str(c.get("category_id", "")): c.get("category_name", "") for c in categories}
            
            result = []
            for stream in streams:
                cat_id = str(stream.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                channel_name = stream.get("name", "")
                if should_include(group_name, group_filters) and should_include(channel_name, channel_filters):
                    result.append(stream)
            return jsonify(result)

        # Get Series
        elif action == "get_series":
            group_filters = source_filters.get("series", {}).get("groups", [])
            channel_filters = source_filters.get("series", {}).get("channels", [])
            series_list = get_cached("series", source_id)
            categories = get_cached("series_categories", source_id)
            cat_map = {str(c.get("category_id", "")): c.get("category_name", "") for c in categories}
            
            result = []
            for s in series_list:
                cat_id = str(s.get("category_id", ""))
                group_name = cat_map.get(cat_id, "")
                series_name = s.get("name", "")
                if should_include(group_name, group_filters) and should_include(series_name, channel_filters):
                    result.append(s)
            return jsonify(result)

        # Get Series Info - direct pass-through to this source
        elif action == "get_series_info":
            series_id = request.args.get("series_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "series_id": series_id}
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

        # Get VOD Info - direct pass-through to this source
        elif action == "get_vod_info":
            vod_id = request.args.get("vod_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "vod_id": vod_id}
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

        # Any other action - pass through to this source
        else:
            params = dict(request.args)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

    except requests.exceptions.Timeout:
        return jsonify({"error": "Upstream server timeout"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Per-source stream proxy routes
@app.route("/<source_route>/live/<username>/<password>/<stream_id>")
@app.route("/<source_route>/live/<username>/<password>/<stream_id>.<ext>")
@app.route("/<source_route>/<username>/<password>/<stream_id>")
@app.route("/<source_route>/<username>/<password>/<stream_id>.<ext>")
def proxy_live_stream_source(source_route, username, password, stream_id, ext="ts"):
    """Redirect live stream requests to the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


@app.route("/<source_route>/movie/<username>/<password>/<stream_id>")
@app.route("/<source_route>/movie/<username>/<password>/<stream_id>.<ext>")
def proxy_movie_stream_source(source_route, username, password, stream_id, ext="mp4"):
    """Redirect VOD/movie stream requests to the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


@app.route("/<source_route>/series/<username>/<password>/<stream_id>")
@app.route("/<source_route>/series/<username>/<password>/<stream_id>.<ext>")
def proxy_series_stream_source(source_route, username, password, stream_id, ext="mp4"):
    """Redirect series stream requests to the source specified by route"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


# ============================================
# PER-SOURCE FULL (UNFILTERED) ROUTES
# ============================================

@app.route("/<source_route>/full/player_api.php")
def player_api_source_full(source_route):
    """
    Per-source UNFILTERED Xtream Codes API endpoint.
    Returns all content from this source without applying filters.
    """
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    
    source = get_source_by_route(source_route)
    if not source:
        return jsonify({"error": f"Source route '{source_route}' not found"}), 404
    
    source_id = source.get("id")
    prefix = source.get("prefix", "")
    action = request.args.get("action", "")

    try:
        host = source["host"].rstrip("/")
        
        # No action = authentication request
        if not action:
            response = requests.get(
                f"{host}/player_api.php",
                params={"username": source["username"], "password": source["password"]},
                headers=HEADERS,
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                if "server_info" in data:
                    data["server_info"]["url"] = request.host_url.rstrip("/") + f"/{source_route}/full"
                    data["server_info"]["port"] = "80"
                    data["server_info"]["https_port"] = "443"
                    data["server_info"]["rtmp_port"] = "80"
                return jsonify(data)
            return Response(response.content, status=response.status_code)

        # Get Live Categories - unfiltered
        elif action == "get_live_categories":
            categories = get_cached("live_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = cat.copy()
                    cat_copy["category_name"] = f"{prefix}{cat.get('category_name', '')}"
                    result.append(cat_copy)
                return jsonify(result)
            return jsonify(categories)

        # Get VOD Categories - unfiltered
        elif action == "get_vod_categories":
            categories = get_cached("vod_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = cat.copy()
                    cat_copy["category_name"] = f"{prefix}{cat.get('category_name', '')}"
                    result.append(cat_copy)
                return jsonify(result)
            return jsonify(categories)

        # Get Series Categories - unfiltered
        elif action == "get_series_categories":
            categories = get_cached("series_categories", source_id)
            if prefix:
                result = []
                for cat in categories:
                    cat_copy = cat.copy()
                    cat_copy["category_name"] = f"{prefix}{cat.get('category_name', '')}"
                    result.append(cat_copy)
                return jsonify(result)
            return jsonify(categories)

        # Get Live Streams - unfiltered
        elif action == "get_live_streams":
            return jsonify(get_cached("live_streams", source_id))

        # Get VOD Streams - unfiltered
        elif action == "get_vod_streams":
            return jsonify(get_cached("vod_streams", source_id))

        # Get Series - unfiltered
        elif action == "get_series":
            return jsonify(get_cached("series", source_id))

        # Get Series Info - direct pass-through
        elif action == "get_series_info":
            series_id = request.args.get("series_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "series_id": series_id}
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

        # Get VOD Info - direct pass-through
        elif action == "get_vod_info":
            vod_id = request.args.get("vod_id", "")
            params = {"username": source["username"], "password": source["password"], "action": action, "vod_id": vod_id}
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

        # Any other action - pass through
        else:
            params = dict(request.args)
            params["username"] = source["username"]
            params["password"] = source["password"]
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type="application/json")

    except requests.exceptions.Timeout:
        return jsonify({"error": "Upstream server timeout"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Per-source FULL stream proxy routes
@app.route("/<source_route>/full/live/<username>/<password>/<stream_id>")
@app.route("/<source_route>/full/live/<username>/<password>/<stream_id>.<ext>")
@app.route("/<source_route>/full/<username>/<password>/<stream_id>")
@app.route("/<source_route>/full/<username>/<password>/<stream_id>.<ext>")
def proxy_live_stream_source_full(source_route, username, password, stream_id, ext="ts"):
    """Redirect live stream requests to the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


@app.route("/<source_route>/full/movie/<username>/<password>/<stream_id>")
@app.route("/<source_route>/full/movie/<username>/<password>/<stream_id>.<ext>")
def proxy_movie_stream_source_full(source_route, username, password, stream_id, ext="mp4"):
    """Redirect VOD/movie stream requests to the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/movie/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


@app.route("/<source_route>/full/series/<username>/<password>/<stream_id>")
@app.route("/<source_route>/full/series/<username>/<password>/<stream_id>.<ext>")
def proxy_series_stream_source_full(source_route, username, password, stream_id, ext="mp4"):
    """Redirect series stream requests to the source (unfiltered path)"""
    if source_route in ("full", "live", "movie", "series", "api", "static"):
        return Response("Not found", status=404)
    source = get_source_by_route(source_route)
    if not source:
        return Response(f"Source route '{source_route}' not found", status=404)
    host = source["host"].rstrip("/")
    return redirect(f"{host}/series/{source['username']}/{source['password']}/{stream_id}.{ext}", code=302)


# Stream proxy routes - redirect to upstream server
@app.route("/live/<username>/<password>/<stream_id>")
@app.route("/live/<username>/<password>/<stream_id>.<ext>")
@app.route("/<username>/<password>/<stream_id>")
@app.route("/<username>/<password>/<stream_id>.<ext>")
def proxy_live_stream(username, password, stream_id, ext="ts"):
    """Redirect live stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "live")
    if not host:
        return Response("Source not found", status=404)
    return redirect(f"{host}/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route("/movie/<username>/<password>/<stream_id>")
@app.route("/movie/<username>/<password>/<stream_id>.<ext>")
def proxy_movie_stream(username, password, stream_id, ext="mp4"):
    """Redirect VOD/movie stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "vod")
    if not host:
        return Response("Source not found", status=404)
    return redirect(f"{host}/movie/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route("/series/<username>/<password>/<stream_id>")
@app.route("/series/<username>/<password>/<stream_id>.<ext>")
def proxy_series_stream(username, password, stream_id, ext="mp4"):
    """Redirect series stream requests to upstream server (finds correct source)"""
    host, upstream_user, upstream_pass = get_source_credentials_for_stream(stream_id, "series")
    if not host:
        return Response("Source not found", status=404)
    return redirect(f"{host}/series/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route("/xmltv.php")
def proxy_xmltv():
    """Proxy EPG/XMLTV requests"""
    config = load_config()
    xtream = config["xtream"]
    host = xtream["host"].rstrip("/")
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]

    try:
        response = requests.get(
            f"{host}/xmltv.php",
            params={"username": upstream_user, "password": upstream_pass},
            headers=HEADERS,
            timeout=120,
            stream=True,
        )
        return Response(
            response.iter_content(chunk_size=8192),
            status=response.status_code,
            content_type=response.headers.get("content-type", "application/xml"),
        )
    except Exception as e:
        return Response(f"<!-- Error: {e} -->", content_type="application/xml")


# ============================================
# CACHE MANAGEMENT API
# ============================================


@app.route("/api/cache/status")
def cache_status():
    """Get cache status and statistics"""
    with _cache_lock:
        last_refresh = _api_cache.get("last_refresh")
        refresh_in_progress = _api_cache.get("refresh_in_progress", False)
        refresh_progress = _api_cache.get("refresh_progress", {})
        sources_cache = _api_cache.get("sources", {})
    
    # Aggregate counts from all sources
    live_streams = sum(len(s.get("live_streams", [])) for s in sources_cache.values())
    vod_streams = sum(len(s.get("vod_streams", [])) for s in sources_cache.values())
    series_count = sum(len(s.get("series", [])) for s in sources_cache.values())
    live_cats = sum(len(s.get("live_categories", [])) for s in sources_cache.values())
    vod_cats = sum(len(s.get("vod_categories", [])) for s in sources_cache.values())
    series_cats = sum(len(s.get("series_categories", [])) for s in sources_cache.values())

    cache_valid = is_cache_valid()
    ttl = get_cache_ttl()

    # Calculate next refresh time
    next_refresh = None
    if last_refresh:
        try:
            last_time = datetime.fromisoformat(last_refresh)
            next_time = last_time + timedelta(seconds=ttl)
            next_refresh = next_time.isoformat()
        except (ValueError, TypeError):
            pass
    
    # Per-source details
    sources_info = {}
    for source_id, source_cache in sources_cache.items():
        sources_info[source_id] = {
            "live_streams": len(source_cache.get("live_streams", [])),
            "vod_streams": len(source_cache.get("vod_streams", [])),
            "series": len(source_cache.get("series", [])),
            "last_refresh": source_cache.get("last_refresh"),
        }

    return jsonify(
        {
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
    )


@app.route("/api/cache/refresh", methods=["POST"])
def trigger_cache_refresh():
    """Manually trigger a cache refresh"""

    def refresh_async():
        refresh_cache()

    # Start refresh in background thread
    thread = threading.Thread(target=refresh_async)
    thread.start()

    return jsonify({"status": "refresh_started", "message": "Cache refresh has been triggered in the background"})


@app.route("/api/cache/clear", methods=["POST"])
def clear_cache():
    """Clear the cache"""
    global _api_cache, _stream_source_map
    with _cache_lock:
        _api_cache = {
            "sources": {},
            "last_refresh": None,
            "refresh_in_progress": False,
        }
    
    with _stream_map_lock:
        _stream_source_map = {"live": {}, "vod": {}, "series": {}}

    # Also remove disk cache
    if os.path.exists(API_CACHE_FILE):
        os.remove(API_CACHE_FILE)

    return jsonify({"status": "ok", "message": "Cache cleared"})


@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})


# ============================================
# APPLICATION STARTUP
# ============================================


def initialize_app():
    """Initialize the application - load cache and start background refresh"""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load cache from disk
    load_cache_from_disk()

    # Start background refresh thread
    start_background_refresh()

    # If cache is empty or invalid, trigger immediate refresh
    if not is_cache_valid():
        logger.info("Cache is empty or invalid, triggering initial refresh...")
        threading.Thread(target=refresh_cache, daemon=True).start()


# Initialize on module load (for gunicorn), skip during tests
if not os.environ.get("TESTING"):
    initialize_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

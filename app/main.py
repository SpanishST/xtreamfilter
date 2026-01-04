from flask import Flask, Response, render_template, request, redirect, url_for, jsonify
import requests
import json
import os
import re
import time
import threading
from datetime import datetime

app = Flask(__name__)

CONFIG_FILE = "/data/config.json"
CACHE_FILE = "/data/playlist_cache.m3u"
API_CACHE_FILE = "/data/api_cache.json"

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

# In-memory cache
_api_cache = {
    "live_categories": [],
    "vod_categories": [],
    "series_categories": [],
    "live_streams": [],
    "vod_streams": [],
    "series": [],
    "last_refresh": None,
    "refresh_in_progress": False
}
_cache_lock = threading.Lock()

def get_cache_ttl():
    """Get cache TTL from config (in seconds)"""
    config = load_config()
    return config.get("options", {}).get("cache_ttl", 3600)

def load_cache_from_disk():
    """Load cache from disk on startup"""
    global _api_cache
    if os.path.exists(API_CACHE_FILE):
        try:
            with open(API_CACHE_FILE, 'r') as f:
                data = json.load(f)
                with _cache_lock:
                    _api_cache.update(data)
                    _api_cache["refresh_in_progress"] = False
                print(f"[Cache] Loaded from disk. Last refresh: {_api_cache.get('last_refresh', 'Never')}")
        except Exception as e:
            print(f"[Cache] Failed to load from disk: {e}")

def save_cache_to_disk():
    """Save cache to disk"""
    try:
        with _cache_lock:
            data = {k: v for k, v in _api_cache.items() if k != "refresh_in_progress"}
        os.makedirs(os.path.dirname(API_CACHE_FILE), exist_ok=True)
        with open(API_CACHE_FILE, 'w') as f:
            json.dump(data, f)
        print(f"[Cache] Saved to disk at {datetime.now().isoformat()}")
    except Exception as e:
        print(f"[Cache] Failed to save to disk: {e}")

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
    except:
        return False

def get_cached(key):
    """Get cached data for a key"""
    with _cache_lock:
        return _api_cache.get(key, [])

def refresh_cache():
    """Refresh all cached data from upstream server"""
    global _api_cache
    
    with _cache_lock:
        if _api_cache.get("refresh_in_progress"):
            print("[Cache] Refresh already in progress, skipping")
            return False
        _api_cache["refresh_in_progress"] = True
    
    config = load_config()
    xtream = config["xtream"]
    
    if not xtream.get("host") or not xtream.get("username") or not xtream.get("password"):
        print("[Cache] Cannot refresh - credentials not configured")
        with _cache_lock:
            _api_cache["refresh_in_progress"] = False
        return False
    
    host = xtream["host"].rstrip('/')
    username = xtream["username"]
    password = xtream["password"]
    
    print(f"[Cache] Starting full refresh at {datetime.now().isoformat()}")
    
    try:
        # Fetch all categories
        print("[Cache] Fetching live categories...")
        live_cats = fetch_from_upstream(host, username, password, "get_live_categories")
        
        print("[Cache] Fetching VOD categories...")
        vod_cats = fetch_from_upstream(host, username, password, "get_vod_categories")
        
        print("[Cache] Fetching series categories...")
        series_cats = fetch_from_upstream(host, username, password, "get_series_categories")
        
        # Fetch all streams
        print("[Cache] Fetching live streams...")
        live_streams = fetch_from_upstream(host, username, password, "get_live_streams")
        
        print("[Cache] Fetching VOD streams...")
        vod_streams = fetch_from_upstream(host, username, password, "get_vod_streams")
        
        print("[Cache] Fetching series...")
        series = fetch_from_upstream(host, username, password, "get_series")
        
        # Update cache
        with _cache_lock:
            _api_cache["live_categories"] = live_cats or []
            _api_cache["vod_categories"] = vod_cats or []
            _api_cache["series_categories"] = series_cats or []
            _api_cache["live_streams"] = live_streams or []
            _api_cache["vod_streams"] = vod_streams or []
            _api_cache["series"] = series or []
            _api_cache["last_refresh"] = datetime.now().isoformat()
            _api_cache["refresh_in_progress"] = False
        
        save_cache_to_disk()
        
        print(f"[Cache] Refresh complete. Live: {len(live_cats or [])} cats, {len(live_streams or [])} streams | "
              f"VOD: {len(vod_cats or [])} cats, {len(vod_streams or [])} streams | "
              f"Series: {len(series_cats or [])} cats, {len(series or [])} items")
        
        return True
        
    except Exception as e:
        print(f"[Cache] Refresh failed: {e}")
        with _cache_lock:
            _api_cache["refresh_in_progress"] = False
        return False

def fetch_from_upstream(host, username, password, action):
    """Fetch data from upstream Xtream server"""
    url = f"{host}/player_api.php"
    params = {"username": username, "password": password, "action": action}
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=120)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[Cache] Error fetching {action}: {e}")
    return None

def background_refresh_loop():
    """Background thread that periodically refreshes the cache"""
    print("[Cache] Background refresh thread started")
    
    # Initial delay to let the app start
    time.sleep(10)
    
    while True:
        try:
            ttl = get_cache_ttl()
            
            if not is_cache_valid():
                print("[Cache] Cache expired, triggering refresh...")
                refresh_cache()
            else:
                with _cache_lock:
                    last_refresh = _api_cache.get("last_refresh", "Never")
                print(f"[Cache] Cache still valid. Last refresh: {last_refresh}")
            
            # Sleep for 1/4 of TTL or minimum 5 minutes
            sleep_time = max(ttl // 4, 300)
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"[Cache] Background refresh error: {e}")
            time.sleep(60)

# Start background refresh thread
_refresh_thread = None

def start_background_refresh():
    """Start the background refresh thread"""
    global _refresh_thread
    if _refresh_thread is None or not _refresh_thread.is_alive():
        _refresh_thread = threading.Thread(target=background_refresh_loop, daemon=True)
        _refresh_thread.start()
        print("[Cache] Background refresh thread initialized")


def load_config():
    """Load configuration from file"""
    default_config = {
        "xtream": {
            "host": "",
            "username": "",
            "password": ""
        },
        "filters": {
            "live": {
                "groups": [],      # Filter rules for live TV groups
                "channels": []     # Filter rules for live TV channels
            },
            "vod": {
                "groups": [],      # Filter rules for VOD groups
                "channels": []     # Filter rules for VOD items (movies)
            },
            "series": {
                "groups": [],      # Filter rules for series groups
                "channels": []     # Filter rules for series names
            }
        },
        "content_types": {
            "live": True,      # Live TV channels
            "vod": True,       # Movies (VOD)
            "series": True     # TV Series
        },
        "options": {
            "cache_enabled": True,
            "cache_ttl": 3600
        }
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                # Merge with defaults to ensure all keys exist
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                # Migrate old filter structure to new per-category structure
                if "filters" in config:
                    filters = config["filters"]
                    # Check if using old structure (direct groups/channels arrays)
                    if "groups" in filters and isinstance(filters["groups"], list):
                        # Old structure - migrate to new
                        old_groups = filters.get("groups", [])
                        old_channels = filters.get("channels", [])
                        config["filters"] = {
                            "live": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "vod": {"groups": old_groups.copy(), "channels": old_channels.copy()},
                            "series": {"groups": old_groups.copy(), "channels": old_channels.copy()}
                        }
                    else:
                        # New structure - ensure all categories exist
                        for cat in ["live", "vod", "series"]:
                            if cat not in filters:
                                filters[cat] = {"groups": [], "channels": []}
                            elif "groups" not in filters[cat]:
                                filters[cat]["groups"] = []
                            elif "channels" not in filters[cat]:
                                filters[cat]["channels"] = []
                # Ensure content_types exists
                if "content_types" not in config:
                    config["content_types"] = {"live": True, "vod": True, "series": True}
                return config
        except:
            pass
    return default_config

def save_config(config):
    """Save configuration to file"""
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

def get_categories(host, username, password, content_type="live"):
    """Get categories from Xtream API for a specific content type"""
    actions = {
        "live": "get_live_categories",
        "vod": "get_vod_categories",
        "series": "get_series_categories"
    }
    action = actions.get(content_type, "get_live_categories")
    url = f"{host}/player_api.php?username={username}&password={password}&action={action}"
    try:
        response = requests.get(url, timeout=60, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_live_streams(host, username, password):
    """Get all live streams from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_live_streams"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_vod_streams(host, username, password):
    """Get all VOD (movies) from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_vod_streams"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_series(host, username, password):
    """Get all series from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_series"
    try:
        response = requests.get(url, timeout=120, headers=HEADERS)
        return response.json() if response.status_code == 200 else []
    except:
        return []

def get_series_info(host, username, password, series_id):
    """Get series info including episodes from Xtream API"""
    url = f"{host}/player_api.php?username={username}&password={password}&action=get_series_info&series_id={series_id}"
    try:
        response = requests.get(url, timeout=60, headers=HEADERS)
        return response.json() if response.status_code == 200 else {}
    except:
        return {}

def matches_filter(value, filter_rule):
    """
    Check if a value matches a filter rule.
    
    Filter rule structure:
    {
        "type": "include" or "exclude",
        "match": "exact" | "starts_with" | "ends_with" | "contains" | "not_contains" | "regex",
        "value": "the pattern",
        "case_sensitive": true/false
    }
    """
    pattern = filter_rule.get("value", "")
    match_type = filter_rule.get("match", "contains")
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
        except:
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
    """Generate filtered M3U playlist from cached data"""
    xtream = config["xtream"]
    filters = config["filters"]
    content_types = config.get("content_types", {"live": True, "vod": True, "series": True})
    
    if not xtream["host"] or not xtream["username"] or not xtream["password"]:
        return "#EXTM3U\n# Error: Xtream credentials not configured\n"
    
    host = xtream["host"].rstrip('/')
    username = xtream["username"]
    password = xtream["password"]
    
    # Generate M3U from cache
    lines = ['#EXTM3U']
    stats = {"live": 0, "vod": 0, "series": 0, "excluded": 0}
    
    # ===== LIVE TV =====
    if content_types.get("live", True):
        live_filters = filters.get("live", {"groups": [], "channels": []})
        live_group_filters = live_filters.get("groups", [])
        live_channel_filters = live_filters.get("channels", [])
        
        # Use cached data
        categories = get_cached("live_categories")
        cat_map = {str(cat["category_id"]): cat["category_name"] for cat in categories}
        streams = get_cached("live_streams")
        
        for stream in streams:
            stream_id = stream.get('stream_id')
            name = stream.get('name', 'Unknown')
            icon = stream.get('stream_icon', '')
            cat_id = str(stream.get('category_id', ''))
            group = cat_map.get(cat_id, 'Unknown')
            epg_id = stream.get('epg_channel_id', '')
            
            if not should_include(group, live_group_filters):
                stats["excluded"] += 1
                continue
            
            if not should_include(name, live_channel_filters):
                stats["excluded"] += 1
                continue
            
            stream_url = f"{host}/{username}/{password}/{stream_id}.ts"
            lines.append(f'#EXTINF:-1 tvg-id="{epg_id}" tvg-logo="{icon}" group-title="{group}",{name}')
            lines.append(stream_url)
            stats["live"] += 1
    
    # ===== VOD (MOVIES) =====
    if content_types.get("vod", True):
        vod_filters = filters.get("vod", {"groups": [], "channels": []})
        vod_group_filters = vod_filters.get("groups", [])
        vod_channel_filters = vod_filters.get("channels", [])
        
        # Use cached data
        vod_categories = get_cached("vod_categories")
        vod_cat_map = {str(cat["category_id"]): cat["category_name"] for cat in vod_categories}
        vod_streams = get_cached("vod_streams")
        
        for vod in vod_streams:
            stream_id = vod.get('stream_id')
            name = vod.get('name', 'Unknown')
            icon = vod.get('stream_icon', '')
            cat_id = str(vod.get('category_id', ''))
            group = vod_cat_map.get(cat_id, 'Movies')
            extension = vod.get('container_extension', 'mp4')
            
            if not should_include(group, vod_group_filters):
                stats["excluded"] += 1
                continue
            
            if not should_include(name, vod_channel_filters):
                stats["excluded"] += 1
                continue
            
            stream_url = f"{host}/movie/{username}/{password}/{stream_id}.{extension}"
            lines.append(f'#EXTINF:-1 tvg-logo="{icon}" group-title="{group}",{name}')
            lines.append(stream_url)
            stats["vod"] += 1
    
    # ===== SERIES (without episodes - just series entries) =====
    # Note: Full episode listing would require caching all series_info which is too heavy
    # IPTV apps should use Xtream API for series, not M3U
    if content_types.get("series", True):
        series_filters = filters.get("series", {"groups": [], "channels": []})
        series_group_filters = series_filters.get("groups", [])
        series_channel_filters = series_filters.get("channels", [])
        
        # Use cached data
        series_categories = get_cached("series_categories")
        series_cat_map = {str(cat["category_id"]): cat["category_name"] for cat in series_categories}
        all_series = get_cached("series")
        
        for series in all_series:
            series_name = series.get('name', 'Unknown')
            series_icon = series.get('cover', '')
            cat_id = str(series.get('category_id', ''))
            group = series_cat_map.get(cat_id, 'Series')
            
            if not should_include(group, series_group_filters):
                stats["excluded"] += 1
                continue
            
            if not should_include(series_name, series_channel_filters):
                stats["excluded"] += 1
                continue
            
            # Add series as a single entry (episodes available via Xtream API)
            lines.append(f'#EXTINF:-1 tvg-logo="{series_icon}" group-title="{group}",{series_name}')
            lines.append(f'# Series - use Xtream API for episodes')
            stats["series"] += 1
    
    # Add stats as comment
    stats_line = f'# Content: {stats["live"]} live, {stats["vod"]} movies, {stats["series"]} series | {stats["excluded"]} excluded'
    lines.insert(1, stats_line)
    
    return '\n'.join(lines)
    
    return '\n'.join(lines)

@app.route('/')
def index():
    """Main configuration page"""
    config = load_config()
    return render_template('index.html', config=config)

@app.route('/save', methods=['POST'])
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
        "series": "content_series" in request.form
    }
    
    save_config(config)
    
    # Clear cache
    if os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    
    return redirect(url_for('index'))

@app.route('/api/filters', methods=['GET'])
def get_filters():
    """Get all filter rules"""
    config = load_config()
    return jsonify(config["filters"])

@app.route('/api/filters', methods=['POST'])
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

@app.route('/api/filters/add', methods=['POST'])
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
        "case_sensitive": data.get("case_sensitive", False)
    }
    
    if category in config["filters"] and target in config["filters"][category]:
        config["filters"][category][target].append(rule)
        save_config(config)
    
    return jsonify({"status": "ok", "filters": config["filters"]})

@app.route('/api/filters/delete', methods=['POST'])
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

@app.route('/playlist.m3u')
@app.route('/get.php')
def playlist():
    """Serve the filtered M3U playlist"""
    config = load_config()
    m3u_content = generate_m3u(config)
    
    return Response(
        m3u_content,
        mimetype='audio/x-mpegurl',
        headers={
            'Content-Disposition': 'inline; filename="playlist.m3u"',
            'Cache-Control': 'no-cache'
        }
    )

@app.route('/groups')
def groups():
    """API endpoint to get all available groups for a content type"""
    config = load_config()
    xtream = config["xtream"]
    content_type = request.args.get("type", "live")  # live, vod, or series
    
    if not xtream["host"]:
        return jsonify({"error": "Not configured", "groups": []})
    
    categories = get_categories(
        xtream["host"].rstrip('/'),
        xtream["username"],
        xtream["password"],
        content_type
    )
    
    groups = sorted([cat["category_name"] for cat in categories])
    return jsonify({"groups": groups})

@app.route('/channels')
def channels():
    """API endpoint to get channel names (paginated)"""
    config = load_config()
    xtream = config["xtream"]
    
    if not xtream["host"]:
        return jsonify({"error": "Not configured", "channels": []})
    
    search = request.args.get('search', '').lower()
    
    streams = get_live_streams(
        xtream["host"].rstrip('/'),
        xtream["username"],
        xtream["password"]
    )
    
    channel_names = [s.get("name", "") for s in streams]
    
    if search:
        channel_names = [c for c in channel_names if search in c.lower()]
    
    return jsonify({"channels": channel_names[:200], "total": len(channel_names)})

@app.route('/stats')
def stats():
    """API endpoint to get playlist statistics"""
    config = load_config()
    xtream = config["xtream"]
    
    if not xtream["host"]:
        return jsonify({"error": "Not configured"})
    
    host = xtream["host"].rstrip('/')
    
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
    
    return jsonify({
        "total_categories": len(live_categories) + len(vod_categories) + len(series_categories),
        "total_channels": len(streams),
        "live_categories": len(live_categories),
        "vod_categories": len(vod_categories),
        "series_categories": len(series_categories),
        "group_filters": total_group_filters,
        "channel_filters": total_channel_filters
    })

@app.route('/preview')
def preview():
    """Preview filtered playlist stats"""
    config = load_config()
    m3u_content = generate_m3u(config)
    
    # Parse stats from first comment
    lines = m3u_content.split('\n')
    stats_line = lines[1] if len(lines) > 1 else ""
    
    # Get sample channels
    sample = []
    for i in range(2, min(len(lines), 22), 2):
        if lines[i].startswith('#EXTINF'):
            name = lines[i].split(',', 1)[-1] if ',' in lines[i] else lines[i]
            sample.append(name)
    
    return jsonify({
        "stats": stats_line,
        "sample_channels": sample
    })

# ============================================
# XTREAM CODES API PROXY
# ============================================
# These endpoints mimic a real Xtream Codes server so IPTV apps can connect

@app.route('/player_api.php')
def player_api():
    """
    Main Xtream Codes API endpoint - serves cached data with filtering applied
    """
    config = load_config()
    xtream = config["xtream"]
    filters = config["filters"]
    
    if not xtream["host"]:
        return jsonify({"user_info": {"auth": 0, "status": "Disabled"}}), 401
    
    host = xtream["host"].rstrip('/')
    username = request.args.get('username', '')
    password = request.args.get('password', '')
    action = request.args.get('action', '')
    
    # For authentication, use our proxy credentials
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]
    
    # Build upstream params for pass-through requests
    params = dict(request.args)
    params['username'] = upstream_user
    params['password'] = upstream_pass
    
    try:
        # No action = authentication request (always fetch fresh)
        if not action:
            response = requests.get(
                f"{host}/player_api.php",
                params={'username': upstream_user, 'password': upstream_pass},
                headers=HEADERS,
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                # Modify server_info to point to our proxy
                if 'server_info' in data:
                    data['server_info']['url'] = request.host_url.rstrip('/')
                    data['server_info']['port'] = '80'
                    data['server_info']['https_port'] = '443'
                    data['server_info']['rtmp_port'] = '80'
                return jsonify(data)
            return Response(response.content, status=response.status_code)
        
        # Get Live Categories - from cache with filters
        elif action == 'get_live_categories':
            categories = get_cached("live_categories")
            live_filters = filters.get("live", {}).get("groups", [])
            filtered = [cat for cat in categories if should_include(cat.get('category_name', ''), live_filters)]
            return jsonify(filtered)
        
        # Get VOD Categories - from cache with filters
        elif action == 'get_vod_categories':
            categories = get_cached("vod_categories")
            vod_filters = filters.get("vod", {}).get("groups", [])
            filtered = [cat for cat in categories if should_include(cat.get('category_name', ''), vod_filters)]
            return jsonify(filtered)
        
        # Get Series Categories - from cache with filters
        elif action == 'get_series_categories':
            categories = get_cached("series_categories")
            series_filters = filters.get("series", {}).get("groups", [])
            filtered = [cat for cat in categories if should_include(cat.get('category_name', ''), series_filters)]
            return jsonify(filtered)
        
        # Get Live Streams - from cache with filters
        elif action == 'get_live_streams':
            streams = get_cached("live_streams")
            categories = get_cached("live_categories")
            live_group_filters = filters.get("live", {}).get("groups", [])
            live_channel_filters = filters.get("live", {}).get("channels", [])
            
            # Build category mapping
            cat_map = {str(c.get('category_id', '')): c.get('category_name', '') for c in categories}
            
            filtered = []
            for stream in streams:
                cat_id = str(stream.get('category_id', ''))
                group_name = cat_map.get(cat_id, '')
                channel_name = stream.get('name', '')
                
                if should_include(group_name, live_group_filters) and should_include(channel_name, live_channel_filters):
                    filtered.append(stream)
            
            return jsonify(filtered)
        
        # Get VOD Streams - from cache with filters
        elif action == 'get_vod_streams':
            streams = get_cached("vod_streams")
            categories = get_cached("vod_categories")
            vod_group_filters = filters.get("vod", {}).get("groups", [])
            vod_channel_filters = filters.get("vod", {}).get("channels", [])
            
            # Build category mapping
            cat_map = {str(c.get('category_id', '')): c.get('category_name', '') for c in categories}
            
            filtered = []
            for stream in streams:
                cat_id = str(stream.get('category_id', ''))
                group_name = cat_map.get(cat_id, '')
                channel_name = stream.get('name', '')
                
                if should_include(group_name, vod_group_filters) and should_include(channel_name, vod_channel_filters):
                    filtered.append(stream)
            
            return jsonify(filtered)
        
        # Get Series - from cache with filters
        elif action == 'get_series':
            series_list = get_cached("series")
            categories = get_cached("series_categories")
            series_group_filters = filters.get("series", {}).get("groups", [])
            series_channel_filters = filters.get("series", {}).get("channels", [])
            
            # Build category mapping
            cat_map = {str(c.get('category_id', '')): c.get('category_name', '') for c in categories}
            
            filtered = []
            for series in series_list:
                cat_id = str(series.get('category_id', ''))
                group_name = cat_map.get(cat_id, '')
                series_name = series.get('name', '')
                
                if should_include(group_name, series_group_filters) and should_include(series_name, series_channel_filters):
                    filtered.append(series)
            
            return jsonify(filtered)
        
        # Get Series Info - pass through (episodes for a specific series)
        elif action == 'get_series_info':
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type='application/json')
        
        # Get VOD Info - pass through
        elif action == 'get_vod_info':
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type='application/json')
        
        # Get Short EPG - pass through
        elif action == 'get_short_epg':
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type='application/json')
        
        # Get Simple Data Table - pass through
        elif action == 'get_simple_data_table':
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type='application/json')
        
        # Any other action - pass through to upstream
        else:
            response = requests.get(f"{host}/player_api.php", params=params, headers=HEADERS, timeout=60)
            return Response(response.content, status=response.status_code, content_type='application/json')
    
    except requests.exceptions.Timeout:
        return jsonify({"error": "Upstream server timeout"}), 504
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Stream proxy routes - redirect to upstream server
@app.route('/live/<username>/<password>/<stream_id>')
@app.route('/live/<username>/<password>/<stream_id>.<ext>')
@app.route('/<username>/<password>/<stream_id>')
@app.route('/<username>/<password>/<stream_id>.<ext>')
def proxy_live_stream(username, password, stream_id, ext='ts'):
    """Redirect live stream requests to upstream server"""
    config = load_config()
    xtream = config["xtream"]
    host = xtream["host"].rstrip('/')
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]
    
    # Redirect to upstream with real credentials
    return redirect(f"{host}/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route('/movie/<username>/<password>/<stream_id>')
@app.route('/movie/<username>/<password>/<stream_id>.<ext>')
def proxy_movie_stream(username, password, stream_id, ext='mp4'):
    """Redirect VOD/movie stream requests to upstream server"""
    config = load_config()
    xtream = config["xtream"]
    host = xtream["host"].rstrip('/')
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]
    
    return redirect(f"{host}/movie/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route('/series/<username>/<password>/<stream_id>')
@app.route('/series/<username>/<password>/<stream_id>.<ext>')
def proxy_series_stream(username, password, stream_id, ext='mp4'):
    """Redirect series stream requests to upstream server"""
    config = load_config()
    xtream = config["xtream"]
    host = xtream["host"].rstrip('/')
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]
    
    return redirect(f"{host}/series/{upstream_user}/{upstream_pass}/{stream_id}.{ext}", code=302)


@app.route('/xmltv.php')
def proxy_xmltv():
    """Proxy EPG/XMLTV requests"""
    config = load_config()
    xtream = config["xtream"]
    host = xtream["host"].rstrip('/')
    upstream_user = xtream["username"]
    upstream_pass = xtream["password"]
    
    try:
        response = requests.get(
            f"{host}/xmltv.php",
            params={'username': upstream_user, 'password': upstream_pass},
            headers=HEADERS,
            timeout=120,
            stream=True
        )
        return Response(
            response.iter_content(chunk_size=8192),
            status=response.status_code,
            content_type=response.headers.get('content-type', 'application/xml')
        )
    except Exception as e:
        return Response(f"<!-- Error: {e} -->", content_type='application/xml')


# ============================================
# CACHE MANAGEMENT API
# ============================================

@app.route('/api/cache/status')
def cache_status():
    """Get cache status and statistics"""
    with _cache_lock:
        last_refresh = _api_cache.get("last_refresh")
        refresh_in_progress = _api_cache.get("refresh_in_progress", False)
        live_cats = len(_api_cache.get("live_categories", []))
        vod_cats = len(_api_cache.get("vod_categories", []))
        series_cats = len(_api_cache.get("series_categories", []))
        live_streams = len(_api_cache.get("live_streams", []))
        vod_streams = len(_api_cache.get("vod_streams", []))
        series_count = len(_api_cache.get("series", []))
    
    cache_valid = is_cache_valid()
    ttl = get_cache_ttl()
    
    # Calculate next refresh time
    next_refresh = None
    if last_refresh:
        try:
            last_time = datetime.fromisoformat(last_refresh)
            from datetime import timedelta
            next_time = last_time + timedelta(seconds=ttl)
            next_refresh = next_time.isoformat()
        except:
            pass
    
    return jsonify({
        "last_refresh": last_refresh,
        "next_refresh": next_refresh,
        "cache_valid": cache_valid,
        "refresh_in_progress": refresh_in_progress,
        "ttl_seconds": ttl,
        "counts": {
            "live_categories": live_cats,
            "vod_categories": vod_cats,
            "series_categories": series_cats,
            "live_streams": live_streams,
            "vod_streams": vod_streams,
            "series": series_count
        }
    })


@app.route('/api/cache/refresh', methods=['POST'])
def trigger_cache_refresh():
    """Manually trigger a cache refresh"""
    def refresh_async():
        refresh_cache()
    
    # Start refresh in background thread
    thread = threading.Thread(target=refresh_async)
    thread.start()
    
    return jsonify({
        "status": "refresh_started",
        "message": "Cache refresh has been triggered in the background"
    })


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """Clear the cache"""
    global _api_cache
    with _cache_lock:
        _api_cache = {
            "live_categories": [],
            "vod_categories": [],
            "series_categories": [],
            "live_streams": [],
            "vod_streams": [],
            "series": [],
            "last_refresh": None,
            "refresh_in_progress": False
        }
    
    # Also remove disk cache
    if os.path.exists(API_CACHE_FILE):
        os.remove(API_CACHE_FILE)
    
    return jsonify({"status": "ok", "message": "Cache cleared"})


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok"})


# ============================================
# APPLICATION STARTUP
# ============================================

def initialize_app():
    """Initialize the application - load cache and start background refresh"""
    os.makedirs('/data', exist_ok=True)
    
    # Load cache from disk
    load_cache_from_disk()
    
    # Start background refresh thread
    start_background_refresh()
    
    # If cache is empty or invalid, trigger immediate refresh
    if not is_cache_valid():
        print("[Startup] Cache is empty or invalid, triggering initial refresh...")
        threading.Thread(target=refresh_cache, daemon=True).start()

# Initialize on module load (for gunicorn)
initialize_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)

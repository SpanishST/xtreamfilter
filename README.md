# XtreamFilter

[![Docker Hub](https://img.shields.io/docker/pulls/spanishst/xtreamfilter.svg)](https://hub.docker.com/r/spanishst/xtreamfilter)
[![Docker Image Size](https://img.shields.io/docker/image-size/spanishst/xtreamfilter/latest)](https://hub.docker.com/r/spanishst/xtreamfilter)

A Docker-based Xtream Codes proxy that filters IPTV content (Live TV, Movies, Series) from multiple sources with per-source dedicated routes, advanced filtering, and caching.

## Screenshots

![Configuration Interface](app1.png)

![Filter Management](app2.png)

## Features

- 📺 **Full Xtream Codes API Proxy** - Works with any Xtream-compatible player (TiviMate, XCIPTV, etc.)
- 🔗 **Multi-Source Support** - Configure multiple Xtream providers with dedicated routes per source
- 🛣️ **Per-Source Routing** - Each source has its own URL path to avoid ID conflicts
- 🎬 **Live TV, Movies & Series** - Filter all content types independently
- 🔧 **Web-based Configuration** - Easy UI to manage sources, settings and filters
- 🎯 **Advanced Filtering** - Include/exclude filters with multiple match types per source
- 🚫 **Exclude All** - Start with empty and whitelist only what you want
- 🏷️ **Source Prefixing** - Optionally prefix group names to identify content origin
- 🔄 **Smart Caching** - Background refresh with configurable TTL and progress bar
- 💾 **Persistent Cache** - Survives container restarts
- 🐳 **Docker Ready** - Easy deployment with docker-compose

## Quick Start

### Using Docker Hub (Recommended)

```bash
docker run -d \
  --name xtreamfilter \
  -p 8080:5000 \
  -v ./data:/data \
  --restart unless-stopped \
  spanishst/xtreamfilter:latest
```

Or with docker-compose, create a `docker-compose.yml`:

```yaml
version: '3'
services:
  xtreamfilter:
    image: spanishst/xtreamfilter:latest
    container_name: xtreamfilter
    ports:
      - "8080:5000"
    volumes:
      - ./data:/data
    restart: unless-stopped
```

Then run:

```bash
docker-compose up -d
```

### Building from Source

1. **Clone the repository:**

```bash
git clone https://github.com/spanishst/xtreamfilter.git
cd xtreamfilter
```

2. **Build and run:**

```bash
docker-compose up --build -d
```

### Next Steps

1. **Open the web UI:**

```
http://localhost:8080
```

2. **Add your Xtream source(s)** in the Sources section:
   - Name, Host, Username, Password
   - **Dedicated Route** (required): URL path for this source (e.g., `myprovider`)

3. **Configure your filters** for each source - Live TV, VOD, and Series

4. **Connect your IPTV player** using the dedicated route:
   ```
   Server: http://YOUR_SERVER_IP:8080/<route>
   Username: (from your provider)
   Password: (from your provider)
   ```

## Multi-Source Support

XtreamFilter allows you to manage multiple Xtream Codes providers, each with its own dedicated endpoint.

### Adding Sources

1. Open the web UI and go to the **Sources** section
2. Click **Add Source** to add a new provider
3. Enter the source details:
   - **Name**: Friendly name for the source (e.g., "Provider A")
   - **Host**: The Xtream server URL (e.g., `http://provider.example.com`)
   - **Username/Password**: Your credentials for this provider
   - **Prefix** (optional): Text to prepend to group names (e.g., `[ProvA] `)
   - **Dedicated Route** (required): URL path for this source (e.g., `providera`)
   - **Enabled**: Toggle to enable/disable this source

### Dedicated Source Routes

Each source **must have** a dedicated route. This ensures:
- No ID conflicts between providers (two sources may have series with the same ID)
- Clean separation of content per source
- Correct playback for all content types

**Configure in the web UI:**
1. Edit a source
2. Set the **Dedicated Route** field (e.g., `smarters` or `strong`)
3. Use the source-specific endpoint in your IPTV player

**Filtered endpoint (with your filter rules applied):**
```
Server: http://YOUR_SERVER_IP:8080/<route>
Username: (from your provider)
Password: (from your provider)
```

**Unfiltered endpoint (full catalog from this source):**
```
Server: http://YOUR_SERVER_IP:8080/<route>/full
Username: (from your provider)
Password: (from your provider)
```

**Example with two sources:**
- Source "Smarters" with route `smarters`:
  - Filtered: `http://YOUR_SERVER_IP:8080/smarters`
  - Unfiltered: `http://YOUR_SERVER_IP:8080/smarters/full`
- Source "Strong" with route `strong`:
  - Filtered: `http://YOUR_SERVER_IP:8080/strong`
  - Unfiltered: `http://YOUR_SERVER_IP:8080/strong/full`

> **Single Source Mode:** If you only have one source configured, the root `/player_api.php` will automatically serve that source, so you can use either the root or the dedicated route.

### Source Prefixing

The **Prefix** option prepends text to all group names from a source, helping identify content origin when viewing in your player.

**Example:**
- Source with prefix `[US] ` → Groups become `[US] Sports`, `[US] Movies`, etc.

### Per-Source Filtering

Each source has its own independent filter configuration:
- Filters are applied per-source
- You can have different include/exclude rules per provider
- Select a source in the filter dropdown to edit its specific filters

## Filter System

### Content Categories
- **Live TV** - Television channels
- **VOD** - Movies/Films
- **Series** - TV series

### Filter Types
- **Include** - Only keep matching items (whitelist mode)
- **Exclude** - Remove matching items (blacklist mode)

### Match Modes
| Mode | Description | Example |
|------|-------------|---------|
| `starts_with` | Matches if name starts with value | `FR\|` matches "FR\| TF1" but NOT "ABC FR\| News" |
| `ends_with` | Matches if name ends with value | `HD` matches "Canal+ HD" |
| `contains` | Matches if name contains value anywhere | `Sports` matches "beIN Sports 1" |
| `not_contains` | Matches if name does NOT contain value | `XXX` excludes adult content |
| `exact` | Exact match only (case insensitive by default) | `TF1` matches only "TF1" |
| `regex` | Regular expression pattern | `^FR\|.*` for regex patterns |
| `all` | Matches everything | Use with "Exclude All" to start fresh |

> **Note:** `starts_with` only matches at the **beginning** of the name. If you want to match anywhere, use `contains`.

### Exclude All Feature

The **Exclude All** option lets you start with a clean slate by excluding everything, then adding include rules to whitelist specific content:

1. Click "Exclude All Groups" or "Exclude All Channels"
2. Add "Include" filters for the specific content you want to keep

This is useful when you only want a small subset of content from a large catalog.

### Filter Examples

**Include only French content (Live TV):**
- Type: `include`, Match: `starts_with`, Value: `FR|`

**Exclude adult content (all categories):**
- Type: `exclude`, Match: `contains`, Value: `XXX`

**Include specific streaming services:**
- Type: `include`, Match: `exact`, Value: `NETFLIX SERIES`
- Type: `include`, Match: `exact`, Value: `DISNEY+ MOVIES`

**Start fresh and whitelist:**
1. Add: Type: `exclude`, Match: `all`, Value: `*`
2. Add: Type: `include`, Match: `starts_with`, Value: `FR|`

## Cache System

The proxy caches all data from upstream servers for fast responses:

- **Default TTL:** 1 hour (3600 seconds)
- **Background Refresh:** Automatic refresh before cache expires
- **Progress Bar:** Visual progress indicator during refresh
- **Disk Persistence:** Cache survives container restarts
- **Per-Source Caching:** Each source is cached independently
- **Manual Control:** Refresh or clear cache via UI button

### Cache Status

The web UI shows:
- Total Live Streams, Movies, and Series counts
- Cache validity status (✅ valid, ⚠️ expired, 🔄 refreshing)
- Last refresh time
- Progress bar during refresh with current step

## API Endpoints

### Per-Source Dedicated Routes

Each source with a dedicated route exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `/<route>/player_api.php` | Filtered Xtream API for this source |
| `/<route>/full/player_api.php` | Unfiltered Xtream API for this source |
| `/<route>/live/{user}/{pass}/{id}` | Live stream redirect |
| `/<route>/movie/{user}/{pass}/{id}` | Movie stream redirect |
| `/<route>/series/{user}/{pass}/{id}` | Series stream redirect |
| `/<route>/full/live/{user}/{pass}/{id}` | Live stream (unfiltered path) |
| `/<route>/full/movie/{user}/{pass}/{id}` | Movie stream (unfiltered path) |
| `/<route>/full/series/{user}/{pass}/{id}` | Series stream (unfiltered path) |

### Root Endpoints

| Endpoint | Description |
|----------|-------------|
| `/player_api.php` | Serves first configured source (or redirects) |
| `/full/player_api.php` | Serves first source unfiltered |
| `/live/{user}/{pass}/{id}` | Live stream (auto-routes to correct source) |
| `/movie/{user}/{pass}/{id}` | Movie stream (auto-routes to correct source) |
| `/series/{user}/{pass}/{id}` | Series stream (auto-routes to correct source) |

### Web Interface & Management

| Endpoint | Description |
|----------|-------------|
| `/` | Web configuration UI |
| `/playlist.m3u` | Filtered M3U playlist |
| `/health` | Health check |

### Source Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/sources` | GET | List all sources |
| `/api/sources` | POST | Add a new source |
| `/api/sources/<id>` | PUT | Update a source |
| `/api/sources/<id>` | DELETE | Delete a source |
| `/api/sources/<id>/filters` | GET | Get filters for a source |
| `/api/sources/<id>/filters` | POST | Update all filters for a source |

### Cache Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cache/status` | GET | Cache status, stats, and refresh progress |
| `/api/cache/refresh` | POST | Trigger background cache refresh |
| `/api/cache/clear` | POST | Clear all cached data |

## Configuration

Configuration is stored in `data/config.json`:

```json
{
  "sources": [
    {
      "id": "abc123",
      "name": "My Provider",
      "host": "http://provider.example.com",
      "username": "myuser",
      "password": "mypass",
      "enabled": true,
      "prefix": "",
      "route": "myprovider",
      "filters": {
        "live": { "groups": [], "channels": [] },
        "vod": { "groups": [], "channels": [] },
        "series": { "groups": [], "channels": [] }
      }
    }
  ],
  "content_types": {
    "live": true,
    "vod": true,
    "series": true
  }
}
```

Cache is stored in `data/api_cache.json` and automatically rebuilt on startup.

## Docker Compose

```yaml
version: '3'
services:
  xtreamfilter:
    build: .
    container_name: xtreamfilter
    ports:
      - "8080:5000"
    volumes:
      - ./data:/data
    restart: unless-stopped
```

## Development

Run locally without Docker:

```bash
cd app
pip install flask requests gunicorn
python main.py
```

The app will be available at `http://localhost:5000`

## Running Tests

```bash
uv run pytest tests/ -v
```

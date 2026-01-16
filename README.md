# XtreamFilter

[![Docker Hub](https://img.shields.io/docker/pulls/spanishst/xtreamfilter.svg)](https://hub.docker.com/r/spanishst/xtreamfilter)
[![Docker Image Size](https://img.shields.io/docker/image-size/spanishst/xtreamfilter/latest)](https://hub.docker.com/r/spanishst/xtreamfilter)

A Docker-based Xtream Codes proxy that filters IPTV content (Live TV, Movies, Series) from multiple sources with advanced per-category rules and caching.

## Screenshots

![Configuration Interface](app1.png)

![Filter Management](app2.png)

## Features

- 📺 **Full Xtream Codes API Proxy** - Works with any Xtream-compatible player (TiviMate, XCIPTV, etc.)
- 🔗 **Multi-Source Support** - Combine multiple Xtream providers into a single unified playlist
- 🎬 **Live TV, Movies & Series** - Filter all content types independently
- 🔧 **Web-based Configuration** - Easy UI to manage sources, settings and filters
- 🎯 **Advanced Filtering** - Include/exclude filters with multiple match types per source
- 🏷️ **Source Prefixing** - Optionally prefix group names to identify content origin
- 🔄 **Smart Caching** - Background refresh with configurable TTL
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

2. **Configure your Xtream sources** in the web interface (you can add multiple providers)

4. **Add your filters** for each source - Live TV, VOD, and Series categories

5. **Connect your IPTV player:**

   **Option A - Xtream Codes API (Recommended):**
   ```
   Server: http://YOUR_SERVER_IP:8080
   Username: (from your provider)
   Password: (from your provider)
   ```

   **Option B - M3U Playlist:**
   ```
   http://YOUR_SERVER_IP:8080/playlist.m3u
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
   - **Prefix** (optional): Text to prepend to group names (e.g., `[ProvA]`)
   - **Dedicated Route** (recommended): URL path for this source (e.g., `providera`)
   - **Enabled**: Toggle to enable/disable this source

### Dedicated Source Routes

Each source must have its own **dedicated route** to avoid ID conflicts between providers. When two sources have content with the same ID (common with series), using separate routes ensures correct playback.

1. Edit a source in the web UI
2. Set the **Dedicated Route** field (e.g., `smarters` or `strong`)
3. Use the source-specific endpoint in your IPTV player:

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

**Example setup:**
- Source "Smarters" with route `smarters`:
  - Filtered: `http://YOUR_SERVER_IP:8080/smarters`
  - Unfiltered: `http://YOUR_SERVER_IP:8080/smarters/full`
- Source "Strong" with route `strong`:
  - Filtered: `http://YOUR_SERVER_IP:8080/strong`
  - Unfiltered: `http://YOUR_SERVER_IP:8080/strong/full`

> **Note:** The root `/player_api.php` redirects to the first configured source. Always use dedicated routes for clarity.

### Source Prefixing

The **Prefix** option prepends text to all group names from a source, helping identify content origin.

**Example:**
- Source with prefix `[US]` → Groups become `[US] Sports`, `[US] Movies`, etc.

### Per-Source Filtering

Each source has its own independent filter configuration:
- Filters are applied per-source
- You can have different include/exclude rules per provider
- Select a source in the UI to edit its specific filters

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
| `starts_with` | Matches if name starts with value | `FR\|` matches "FR\| TF1" |
| `ends_with` | Matches if name ends with value | `HD` matches "Canal+ HD" |
| `contains` | Matches if name contains value | `Sports` matches "beIN Sports 1" |
| `not_contains` | Matches if name does NOT contain value | `XXX` excludes adult content |
| `exact` | Exact match only | `TF1` matches only "TF1" |
| `regex` | Regular expression pattern | `.*\|FR\|.*` for regex patterns |
| `all` | Matches everything | Use with "Exclude All" to start fresh |

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

**Include French movies:**
- Type: `include`, Match: `starts_with`, Value: `FR -`

## API Endpoints

### Per-Source Dedicated Routes (Required)

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

**Filtered usage (with filter rules applied):**
```
Server: http://YOUR_SERVER_IP:8080/smarters
Username: (from your provider)
Password: (from your provider)
```

**Unfiltered usage (full catalog):**
```
Server: http://YOUR_SERVER_IP:8080/smarters/full
Username: (from your provider)
Password: (from your provider)
```

### Legacy Root Endpoints

These endpoints redirect to the first configured source:

| Endpoint | Description |
|----------|-------------|
| `/player_api.php` | Redirects to first source |
| `/full/player_api.php` | Redirects to first source (full) |

### Web Interface & Management

| Endpoint | Description |
|----------|-------------|
| `/` | Web configuration UI |
| `/playlist.m3u` | Filtered M3U playlist |
| `/get.php` | Alternative M3U URL |
| `/health` | Health check |

### Source Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/sources` | GET | List all sources |
| `/api/sources` | POST | Add a new source |
| `/api/sources/<id>` | GET | Get a specific source |
| `/api/sources/<id>` | PUT | Update a source |
| `/api/sources/<id>` | DELETE | Delete a source |
| `/api/sources/<id>/filters` | GET | Get filters for a source |
| `/api/sources/<id>/filters` | POST | Update all filters for a source |
| `/api/sources/<id>/filters/add` | POST | Add a filter to a source |
| `/api/sources/<id>/filters/delete` | POST | Delete a filter from a source |

### Filter Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/filters` | GET | Get all filters (legacy, first source) |
| `/api/filters` | POST | Update all filters (legacy, first source) |
| `/api/filters/add` | POST | Add a single filter (legacy) |
| `/api/filters/delete` | POST | Delete a filter (legacy) |

### Cache Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/cache/status` | GET | Cache status and stats |
| `/api/cache/refresh` | POST | Trigger cache refresh |
| `/api/cache/clear` | POST | Clear all cached data |

### Data Endpoints

| Endpoint | Description |
|----------|-------------|
| `/groups?type=live\|vod\|series` | List available groups |
| `/channels?type=live&search=query` | Search channels |
| `/stats` | Playlist statistics |
| `/preview` | Preview filtered content |

## Caching System

The proxy caches all data from the upstream server to provide fast responses:

- **Default TTL:** 1 hour (3600 seconds)
- **Background Refresh:** Automatic refresh before cache expires
- **Disk Persistence:** Cache survives container restarts
- **Manual Control:** Refresh or clear cache via UI or API

## Configuration

Configuration is stored in `data/config.json` and includes:

```json
{
  "sources": [
    {
      "id": "source_1",
      "name": "Provider A",
      "host": "http://provider-a.example.com",
      "username": "user1",
      "password": "pass1",
      "enabled": true,
      "prefix": "[A] ",
      "route": "providera",
      "filters": {
        "live": { "groups": [], "channels": [] },
        "vod": { "groups": [], "channels": [] },
        "series": { "groups": [], "channels": [] }
      }
    },
    {
      "id": "source_2",
      "name": "Provider B",
      "host": "http://provider-b.example.com",
      "username": "user2",
      "password": "pass2",
      "enabled": true,
      "prefix": "",
      "route": "",
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

> **Note:** Legacy single-source configurations (using `xtream` key) are automatically migrated to the new multi-source format on startup.

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


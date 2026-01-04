# XtreamFilter

[![Docker Hub](https://img.shields.io/docker/pulls/spanishst/xtreamfilter.svg)](https://hub.docker.com/r/spanishst/xtreamfilter)
[![Docker Image Size](https://img.shields.io/docker/image-size/spanishst/xtreamfilter/latest)](https://hub.docker.com/r/spanishst/xtreamfilter)

A Docker-based Xtream Codes proxy that filters IPTV content (Live TV, Movies, Series) with advanced per-category rules and caching.

## Screenshots

![Configuration Interface](app1.png)

![Filter Management](app2.png)

## Features

- 📺 **Full Xtream Codes API Proxy** - Works with any Xtream-compatible player (TiviMate, XCIPTV, etc.)
- 🎬 **Live TV, Movies & Series** - Filter all content types independently
- 🔧 **Web-based Configuration** - Easy UI to manage settings and filters
- 🎯 **Advanced Filtering** - Include/exclude filters with multiple match types
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

2. **Configure your Xtream credentials** in the web interface

4. **Add your filters** for Live TV, VOD, and Series categories

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

### Filter Examples

**Include only French content (Live TV):**
- Type: `include`, Match: `starts_with`, Value: `FR|`

**Exclude adult content (all categories):**
- Type: `exclude`, Match: `contains`, Value: `XXX`

**Include French movies:**
- Type: `include`, Match: `starts_with`, Value: `FR -`

## API Endpoints

### Xtream Codes API (for IPTV players)

| Endpoint | Description |
|----------|-------------|
| `/player_api.php` | Full Xtream API with filtering |
| `/live/{user}/{pass}/{id}` | Live stream redirect |
| `/movie/{user}/{pass}/{id}` | Movie stream redirect |
| `/series/{user}/{pass}/{id}` | Series stream redirect |

### Web Interface & Management

| Endpoint | Description |
|----------|-------------|
| `/` | Web configuration UI |
| `/playlist.m3u` | Filtered M3U playlist |
| `/get.php` | Alternative M3U URL |
| `/health` | Health check |

### Filter Management API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/filters` | GET | Get all filters |
| `/api/filters` | POST | Update all filters |
| `/api/filters/add` | POST | Add a single filter |
| `/api/filters/delete` | POST | Delete a filter |

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
  "server": "http://provider.example.com",
  "username": "your_username",
  "password": "your_password",
  "content_types": {
    "live": true,
    "vod": true,
    "series": true
  },
  "filters": {
    "live": { "groups": [], "channels": [] },
    "vod": { "groups": [], "channels": [] },
    "series": { "groups": [], "channels": [] }
  }
}
```

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


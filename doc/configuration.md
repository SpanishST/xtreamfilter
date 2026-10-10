# Configuration, Sources, and Playlists

## Source Model

Each provider is configured as a separate source with:

- Name
- Host
- Username and password
- Dedicated route
- Optional group prefix
- Maximum connections
- Per-source filters

Dedicated routes are important because upstream providers can reuse IDs for streams, VOD items, or series.

## Merged and Dedicated Access

XtreamFilter exposes content in two ways:

- **Merged access:** all enabled sources appear under one virtualized endpoint.
- **Dedicated access:** each source keeps its own Xtream-compatible endpoint.

## Connection URLs

### Merged Xtream Endpoint

Use this when you want one combined playlist across all enabled sources:

```text
Server: http://YOUR_SERVER_IP:8080/merged
Username: proxy
Password: proxy
```

### Per-Source Xtream Endpoints

Filtered endpoint:

```text
Server: http://YOUR_SERVER_IP:8080/<route>
Username: <provider username>
Password: <provider password>
```

Unfiltered endpoint:

```text
Server: http://YOUR_SERVER_IP:8080/<route>/full
Username: <provider username>
Password: <provider password>
```

### M3U Playlists

| URL | Description |
| --- | --- |
| `/playlist.m3u` | Merged playlist using virtual IDs |
| `/<route>/playlist.m3u` | Filtered playlist for one source |
| `/<route>/full/playlist.m3u` | Unfiltered playlist for one source |

## Stream Proxy Mode

When proxy mode is enabled, clients stream through XtreamFilter instead of receiving upstream redirect URLs directly. This can hide upstream server URLs from clients, centralize stream access, improve playback stability on problematic sources, and keep client configuration simple when upstream URLs change.

Read or change proxy mode through the API:

```bash
curl http://localhost:8080/api/options/proxy

curl -X POST http://localhost:8080/api/options/proxy \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}'
```

## Filtering

Filters are configured per source and per content type.

Content types: `live`, `vod`, and `series`.

Filter targets: `groups` and `channels`.

| Match mode | Description |
| --- | --- |
| `contains` | Value appears anywhere |
| `not_contains` | Value must not appear |
| `starts_with` | Name begins with value |
| `ends_with` | Name ends with value |
| `exact` | Exact match |
| `regex` | Regular expression |
| `all` | Match everything |

Typical uses include excluding all content and then whitelisting selected items, keeping only specific streaming-service groups, excluding adult content globally, or applying separate rules to live, VOD, and series.

## Cache

The application builds and refreshes a local cache of upstream categories and content.

- Default cache TTL: `3600` seconds
- Refresh runs in the background and progress is visible in the UI.
- Cache survives restarts.
- Automatic categories refresh after a cache refresh.
- The cache UI shows the last refresh time, next refresh estimate, validity, per-source item counts, and current refresh progress/step.

## Data and Configuration Storage

Runtime data is stored under `/data`, including:

- `config.json` for source and option settings
- `app.db` for cache indexes, categories, monitoring state, and related persisted data
- Cache and download metadata used by the UI and background jobs

Configuration is primarily stored in `/data/config.json`. Important areas include:

- `sources`: provider definitions and per-source filters
- `content_types`: global enablement of live, VOD, and series
- `options.proxy_streams`: stream proxy toggle
- `options.telegram`: Telegram credentials and enablement
- `options.jellyfin`: Jellyfin base URL, API key, and refresh triggers
- `options.download_path` and `options.download_temp_path`: file-system destinations
- `options.download_movie_destination` and `options.download_series_destination`: relative folders below `download_path`
- `options.download_*`: throttling, pause, profile, notifications, and scheduling

Example configuration shape:

```json
{
  "sources": [
    {
      "id": "abc12345",
      "name": "Provider A",
      "host": "http://provider.example.com",
      "username": "user",
      "password": "pass",
      "enabled": true,
      "prefix": "[A] ",
      "route": "providera",
      "max_connections": 1,
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
  },
  "options": {
    "proxy_streams": true,
    "refresh_interval": 3600,
    "telegram": {
      "enabled": false,
      "bot_token": "",
      "chat_id": ""
    },
    "jellyfin": {
      "enabled": false,
      "base_url": "http://jellyfin:8096",
      "api_key": "",
      "trigger_file": true,
      "trigger_queue": true
    },
    "download_path": "/downloads",
    "download_temp_path": "/downloads/.tmp",
    "download_movie_destination": "Films",
    "download_series_destination": "Series"
  }
}
```

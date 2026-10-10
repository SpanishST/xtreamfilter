# Routes and API Reference

## Xtream and Playlist Routes

| Route | Description |
| --- | --- |
| `/merged/player_api.php` | Merged Xtream API for all enabled sources |
| `/merged/live/{username}/{password}/{stream_id}` | Merged live stream route |
| `/merged/movie/{username}/{password}/{stream_id}` | Merged movie stream route |
| `/merged/series/{username}/{password}/{stream_id}` | Merged series stream route |
| `/player_api.php` | Root Xtream API helper route |
| `/full/player_api.php` | Root unfiltered Xtream helper route |
| `/{source_route}/player_api.php` | Filtered Xtream API for one dedicated source |
| `/{source_route}/full/player_api.php` | Unfiltered Xtream API for one dedicated source |
| `/{source_route}/playlist.m3u` | Filtered M3U playlist for one source |
| `/playlist.m3u` | Merged M3U playlist |
| `/merged/xmltv.php` | Merged XMLTV/EPG output |

## Web UI Routes

| Route | Description |
| --- | --- |
| `/` | Main configuration UI |
| `/browse` | Catalog browser |
| `/cart` | Download cart and queue UI |
| `/monitor` | Series and movie monitoring UI |

## Health and Version

| Route | Method | Description |
| --- | --- | --- |
| `/health` | `GET` | Liveness check |
| `/api/version` | `GET` | Current version, latest release, and update availability |

## Source Management

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/sources` | `GET` | List sources |
| `/api/sources` | `POST` | Create a source |
| `/api/sources/{source_id}` | `GET` | Get one source |
| `/api/sources/{source_id}` | `PUT` | Update one source |
| `/api/sources/{source_id}` | `DELETE` | Delete one source |
| `/api/sources/{source_id}/filters` | `GET` | Get source filters |
| `/api/sources/{source_id}/filters` | `POST` | Replace source filters |
| `/api/sources/{source_id}/filters/add` | `POST` | Add one filter rule |
| `/api/sources/{source_id}/filters/delete` | `POST` | Delete one filter rule |

## Cache Management

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/cache/status` | `GET` | Cache status and counts |
| `/api/cache/refresh` | `POST` | Trigger a background refresh |
| `/api/cache/cancel-refresh` | `POST` | Clear refresh state |
| `/api/cache/clear` | `POST` | Clear the cached data |

## Browse APIs

| Endpoint | Method | Description |
| --- | --- | --- |
| `/groups` | `GET` | Lightweight group list for a content type and source |
| `/channels` | `GET` | Lightweight channel/item list |
| `/api/browse` | `GET` | Main browse/search endpoint |
| `/api/browse/groups` | `GET` | Group list with counts for current source/type |

## Categories API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/categories` | `GET` | List categories with full data |
| `/api/categories` | `POST` | Create a category |
| `/api/categories/summary` | `GET` | Lightweight category list for nav and quick state |
| `/api/categories/{category_id}` | `GET` | Get one category |
| `/api/categories/{category_id}` | `PUT` | Update one category |
| `/api/categories/{category_id}` | `DELETE` | Delete one category |
| `/api/categories/{category_id}/items` | `POST` | Add an item to a manual category |
| `/api/categories/{category_id}/items/{content_type}/{source_id}/{item_id}` | `DELETE` | Remove an item from a manual category |
| `/api/categories/refresh` | `POST` | Refresh automatic categories |

## General Options APIs

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/options` | `GET`, `POST` | Get or update the options object |
| `/api/options/proxy` | `GET`, `POST` | Get or set proxy mode |
| `/api/options/refresh_interval` | `GET`, `POST` | Get or set background refresh interval |

## Integration APIs

### Telegram

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/config/telegram` | `GET` | Get Telegram settings with masked token |
| `/api/config/telegram` | `POST` | Update Telegram settings |
| `/api/config/telegram/test` | `POST` | Send a basic test notification |
| `/api/config/telegram/test-diff` | `POST` | Send a sample category-style notification |

### Jellyfin

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/config/jellyfin` | `GET` | Get Jellyfin settings with masked API key |
| `/api/config/jellyfin` | `POST` | Update Jellyfin base URL, API key, and trigger settings |
| `/api/config/jellyfin/test` | `POST` | Validate the configured Jellyfin connection |

## Webhook API

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/config/webhooks` | `GET` | List configured endpoints (secrets are never returned) |
| `/api/config/webhooks` | `POST` | Create an endpoint |
| `/api/config/webhooks/{endpoint_id}` | `PUT` | Update an endpoint; omit `secret` to keep its current secret |
| `/api/config/webhooks/{endpoint_id}` | `DELETE` | Remove an endpoint and cancel its pending deliveries |
| `/api/config/webhooks/{endpoint_id}/test` | `POST` | Send a signed test event |
| `/api/config/webhooks/deliveries` | `GET` | List recent delivery attempts; supports `limit` and `endpoint_id` |

## Download APIs

### Queue

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/cart` | `GET` | List cart items |
| `/api/cart` | `POST` | Add movie, episode, season, or full-series items |
| `/api/cart/{item_id}` | `DELETE` | Remove one item |
| `/api/cart/{item_id}/retry` | `POST` | Retry a failed, cancelled, or move-failed item |
| `/api/cart/{item_id}/move` | `POST` | Retry only the final move step for a `move_failed` item |
| `/api/cart/retry-all` | `POST` | Retry all failed/cancelled/move-failed items |
| `/api/cart/clear` | `POST` | Clear items by mode |
| `/api/cart/start` | `POST` | Start the worker manually |
| `/api/cart/cancel` | `POST` | Request cancellation of the active download |
| `/api/cart/pause` | `POST` | Pause the queue without cancelling the active download |
| `/api/cart/resume` | `POST` | Resume a paused queue |
| `/api/cart/status` | `GET` | Queue and active-download status |
| `/api/cart/active-source-downloads` | `GET` | Count active downloads by source |
| `/api/cart/series-episodes/{source_id}/{series_id}` | `GET` | Fetch season/episode structure for a series |

### Options

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/options/download_path` | `GET`, `POST` | Get or set the final download directory |
| `/api/options/download_temp_path` | `GET`, `POST` | Get or set the temp directory |
| `/api/options/download_destinations` | `GET`, `POST` | Get or set relative movie and series destinations |
| `/api/options/download_folders` | `GET`, `POST` | Browse or create folders below the download root |
| `/api/options/test_path` | `POST` | Validate write access to a path |
| `/api/options/download_throttle` | `GET`, `POST` | Get or set throttling, pause, and profile options |
| `/api/options/player_profiles` | `GET` | List supported player profiles |
| `/api/options/download_notifications` | `GET`, `POST` | Get or set Telegram download notification options |
| `/api/options/download_schedule` | `GET`, `POST` | Get or set the day-by-day download schedule |

Jellyfin refresh settings are available from the main Settings page and use Jellyfin's server-wide `/Library/Refresh` API.

## Monitoring APIs

| Endpoint | Method | Description |
| --- | --- | --- |
| `/api/monitor` | `GET` | List monitored series |
| `/api/monitor` | `POST` | Add a monitored series |
| `/api/monitor/{id}` | `PUT` | Update a monitored series |
| `/api/monitor/{id}` | `DELETE` | Delete a monitored series |
| `/api/monitor/{id}/episodes` | `GET` | Preview detected episodes and their status |
| `/api/monitor/series-meta/{source_id}/{series_id}` | `GET` | Fetch series metadata used by the UI |
| `/api/monitor/check` | `POST` | Trigger a manual monitoring run |
| `/api/monitor/movies` | `GET` | List monitored movies |
| `/api/monitor/movies` | `POST` | Add a monitored movie |
| `/api/monitor/movies/{movie_id}` | `PUT` | Update a monitored movie |
| `/api/monitor/movies/{movie_id}` | `DELETE` | Delete a monitored movie |
| `/api/monitor/movie-lookup` | `GET` | Search the VOD cache by title or TMDB ID for movie setup |
| `/api/monitor/custom-categories` | `GET` | List custom categories usable as monitoring channels |
| `/api/monitor/vod-categories` | `GET` | List VOD categories grouped by enabled source |

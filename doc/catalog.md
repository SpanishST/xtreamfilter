# Catalog, Categories, and Filtering

## Browse UI

The Browse page is available at `/browse` and supports:

- Search by title, including TMDB-prefixed queries such as `tmdb:12345`
- Filters by content type, source, group, and custom category
- Rating and recency filters for VOD and series
- Sorting for catalog exploration
- Group dropdown populated dynamically from the current source/type selection
- Built-in playback preview for live, VOD, and playable episodes
- Add-to-cart and add-to-category actions directly from result cards
- Browse-from-monitor links for series and movies

The built-in player supports MPEG-TS and HLS playback, audio track switching when available, keyboard shortcuts, and episode playback from the series browser.

## Custom Categories

Custom categories help organize content across all sources.

### Manual Categories

Manual categories are curated item by item:

1. Create a category in manual mode.
2. Select accepted content types.
3. Browse the catalog and use the `+` button to link items.
4. Use the same control again to unlink them later.

### Automatic Categories

Automatic categories are built from pattern rules. Configure the category name and icon, accepted content types, pattern list, `and`/`or` pattern logic, recently-added window, whether source filters also apply, and optional Telegram notification.

Automatic categories refresh when a cache refresh runs and can also be refreshed manually.

Use cases include new movies from the last 7 days, 4K content, provider-specific highlights, hand-picked favorites, and monitoring-only custom channels used to restrict movie or series checks.

See the [API reference](api-reference.md#categories-api) for category endpoints.

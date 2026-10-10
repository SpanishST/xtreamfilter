# Downloads and Monitoring

## Download Manager

The download workflow is exposed through the Browse page and the Cart page.

### Supported Downloads

- Single VOD movie
- Single series episode
- Full season
- Entire series

Downloads are persisted and processed sequentially. Item states include `queued`, `downloading`, `completed`, `failed`, `cancelled`, and `move_failed`.

### Queue Features

- Retry failed or cancelled items, or retry all failed items
- Resume the move step when the temp-to-final move failed
- Pause and resume the entire queue without cancelling the active item
- Track current speed, ETA-related speed, and pause state
- Optional Telegram notifications when queueing or completing downloads
- Optional Jellyfin full-library refresh after completed downloads
- Duplicate prevention for active queued/downloading entries
- Crash recovery for interrupted download state

Pausing is session-only. The active stream stops after its current chunk, its partial temporary file is preserved, and the worker resumes from that point when the queue is resumed. A process restart clears the pause state.

### Throttling and Scheduling

Download options are configurable from the UI and API:

- Bandwidth limit
- Periodic pause interval and pause duration
- Player-profile emulation
- Burst reconnect behavior
- Per-day download schedule windows

If a download schedule is enabled, automatic monitoring downloads wait until the configured window is open.

### Output Layout

Downloads are organized into media-library-friendly folders and include metadata. The library root is the configured `download_path`. Movie and series destinations are configurable subfolders below that root; the cart also supports a per-item folder override through the in-app folder picker.

```text
<download_path>/
├── Films/
│   └── Movie Name/
│       ├── Movie Name.mp4
│       ├── Movie Name.nfo
│       └── poster.jpg
└── Series/
    └── Show Name/
        ├── tvshow.nfo
        ├── poster.jpg
        ├── S01/
        │   ├── Show Name S01E01 - Episode Title.mkv
        │   └── Show Name S01E01 - Episode Title.nfo
        └── S02/
            └── Show Name S02E01 - Episode Title.mp4
```

Download queue and option endpoints are listed in the [API reference](api-reference.md#download-apis).

## Monitoring

The Monitor page is available at `/monitor` and includes separate tabs for series and movies.

### Series Monitoring

Series modes:

- `new_only`: snapshot the current set as known and only react to future episodes
- `season`: watch one season only
- `all`: treat all discovered episodes as candidates

Series actions are `download`, `notify`, or `both`. Features include multi-source matching, optional restrictions to selected sources or custom categories used as monitoring channels, episode previews grouped by season, enable/disable without deleting an entry, and backfill support when editing an existing entry.

Backfill can queue all known episodes, all known episodes from one season, or no backfill.

### Movie Monitoring

Movie monitoring watches for a VOD title to become available:

1. Add a movie from the Monitor page.
2. Search by title or `tmdb:<id>`.
3. Optionally rename the local canonical title used for downloads.
4. Restrict the search to selected sources.
5. Optionally restrict those sources to selected VOD categories.
6. Optionally restrict matching further through custom categories.
7. Choose whether the result should notify, download, or do both.

When a matching movie is found, it is marked as found or downloaded depending on the configured action and whether it was queued or already present on disk.

### Monitoring Checks

Monitoring checks run after cache refresh in the background loop and can be manually triggered through `/api/monitor/check`. Monitoring endpoints are listed in the [API reference](api-reference.md#monitoring-apis).

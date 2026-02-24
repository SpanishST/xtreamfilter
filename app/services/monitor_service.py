"""Monitor service — series monitoring and new-episode detection."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from rapidfuzz import fuzz

from app.database import DB_NAME, db_connect
from app.services.filter_service import normalize_name

if TYPE_CHECKING:
    from app.services.cache_service import CacheService
    from app.services.cart_service import CartService
    from app.services.config_service import ConfigService
    from app.services.notification_service import NotificationService
    from app.services.xtream_service import XtreamService

logger = logging.getLogger(__name__)


def _safe_episode_num(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _normalize_tmdb_id(value) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("tmdb:"):
        raw = raw[5:].strip()
    if raw.isdigit():
        return raw
    return None


def _normalize_imdb_id(value) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw.startswith("imdb:"):
        raw = raw[5:].strip()
    if raw.startswith("tt") and raw[2:].isdigit():
        return raw
    if raw.isdigit():
        return f"tt{raw}"
    return None


class MonitorService:
    """Manages monitored series and checks for new episodes."""

    def __init__(
        self,
        config_service: "ConfigService",
        cache_service: "CacheService",
        xtream_service: "XtreamService",
        notification_service: "NotificationService",
        cart_service: "CartService",
    ):
        self._cfg = config_service
        self.cache_service = cache_service
        self.cart_service = cart_service
        self.notification_service = notification_service
        self.xtream_service = xtream_service
        self.db_path = os.path.join(config_service.data_dir, DB_NAME)
        self._monitored_series: list[dict] = []
        self._monitored_movies: list[dict] = []
        self._check_in_progress: bool = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def series(self) -> list[dict]:
        return self._monitored_series

    @series.setter
    def series(self, value: list[dict]):
        self._monitored_series = value

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_monitored(self) -> list:
        conn = db_connect(self.db_path)
        try:
            series_rows = conn.execute(
                "SELECT id, series_name, canonical_name, series_id, source_id, source_name, "
                "source_category, cover, scope, season_filter, action, enabled, "
                "created_at, last_checked, last_new_count, tmdb_id, imdb_id "
                "FROM monitored_series ORDER BY created_at"
            ).fetchall()

            result: list[dict] = []
            for sr in series_rows:
                sid = sr["id"]

                known = conn.execute(
                    "SELECT stream_id, source_id, season, episode_num "
                    "FROM known_episodes WHERE series_id = ?",
                    (sid,),
                ).fetchall()

                downloaded = conn.execute(
                    "SELECT stream_id, source_id, season, episode_num "
                    "FROM downloaded_episodes WHERE series_id = ?",
                    (sid,),
                ).fetchall()

                # Load multi-source slots (new table; may not exist on old DBs yet)
                try:
                    msources = conn.execute(
                        "SELECT source_id, series_ref, source_name, category, series_name "
                        "FROM monitor_sources WHERE series_id = ? ORDER BY id",
                        (sid,),
                    ).fetchall()
                    monitor_sources = [
                        {
                            "source_id": ms["source_id"],
                            "series_ref": ms["series_ref"],
                            "source_name": ms["source_name"],
                            "category": ms["category"],
                            "series_name": ms["series_name"],
                        }
                        for ms in msources
                    ]
                except Exception:
                    monitor_sources = []

                result.append({
                    "id": sid,
                    "series_name": sr["series_name"],
                    "canonical_name": sr["canonical_name"],
                    "series_id": sr["series_id"],
                    "source_id": sr["source_id"],
                    "source_name": sr["source_name"],
                    "source_category": sr["source_category"],
                    "monitor_sources": monitor_sources,
                    "cover": sr["cover"],
                    "scope": sr["scope"],
                    "season_filter": sr["season_filter"],
                    "action": sr["action"],
                    "enabled": bool(sr["enabled"]),
                    "created_at": sr["created_at"],
                    "last_checked": sr["last_checked"],
                    "last_new_count": sr["last_new_count"],
                    "tmdb_id": sr["tmdb_id"],
                    "imdb_id": sr["imdb_id"],
                    "known_episodes": [
                        {"stream_id": k["stream_id"], "source_id": k["source_id"],
                         "season": k["season"], "episode_num": k["episode_num"]}
                        for k in known
                    ],
                    "downloaded_episodes": [
                        {"stream_id": d["stream_id"], "source_id": d["source_id"],
                         "season": d["season"], "episode_num": d["episode_num"]}
                        for d in downloaded
                    ],
                })

            self._monitored_series = result
            self._backfill_external_ids()
            return self._monitored_series
        except Exception as e:
            logger.error(f"Error loading monitored series from DB: {e}")
            self._monitored_series = []
            return self._monitored_series
        finally:
            conn.close()

    def save_monitored(self, items: list | None = None) -> None:
        if items is not None:
            self._monitored_series = items
        conn = db_connect(self.db_path)
        try:
            for entry in self._monitored_series:
                entry_id = entry.get("id")
                if not entry_id:
                    continue

                conn.execute(
                    """INSERT OR REPLACE INTO monitored_series
                       (id, series_name, canonical_name, series_id, source_id, source_name,
                        source_category, cover, scope, season_filter, action,
                        enabled, created_at, last_checked, last_new_count,
                        tmdb_id, imdb_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        entry_id,
                        entry.get("series_name", ""),
                        entry.get("canonical_name") or None,
                        entry.get("series_id", ""),
                        entry.get("source_id"),
                        entry.get("source_name"),
                        entry.get("source_category"),
                        entry.get("cover"),
                        entry.get("scope", "all"),
                        entry.get("season_filter"),
                        entry.get("action", "notify"),
                        int(entry.get("enabled", True)),
                        entry.get("created_at"),
                        entry.get("last_checked"),
                        int(entry.get("last_new_count", 0)),
                        entry.get("tmdb_id"),
                        entry.get("imdb_id"),
                    ),
                )

                # Replace episodes (simpler than diffing)
                conn.execute(
                    "DELETE FROM known_episodes WHERE series_id = ?", (entry_id,)
                )
                conn.execute(
                    "DELETE FROM downloaded_episodes WHERE series_id = ?", (entry_id,)
                )

                known_rows = [
                    (entry_id, ep.get("stream_id", ""), ep.get("source_id", ""),
                     ep.get("season"), ep.get("episode_num"))
                    for ep in entry.get("known_episodes", [])
                ]
                if known_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO known_episodes "
                        "(series_id, stream_id, source_id, season, episode_num) "
                        "VALUES (?,?,?,?,?)",
                        known_rows,
                    )

                dl_rows = [
                    (entry_id, ep.get("stream_id", ""), ep.get("source_id", ""),
                     ep.get("season"), ep.get("episode_num"))
                    for ep in entry.get("downloaded_episodes", [])
                ]
                if dl_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO downloaded_episodes "
                        "(series_id, stream_id, source_id, season, episode_num) "
                        "VALUES (?,?,?,?,?)",
                        dl_rows,
                    )

                # Persist multi-source slots
                try:
                    conn.execute(
                        "DELETE FROM monitor_sources WHERE series_id = ?", (entry_id,)
                    )
                    ms_rows = [
                        (entry_id,
                         ms.get("source_id", ""),
                         ms.get("series_ref", ""),
                         ms.get("source_name"),
                         ms.get("category"),
                         ms.get("series_name"))
                        for ms in entry.get("monitor_sources", [])
                        if ms.get("source_id") and ms.get("series_ref")
                    ]
                    if ms_rows:
                        conn.executemany(
                            "INSERT OR IGNORE INTO monitor_sources "
                            "(series_id, source_id, series_ref, source_name, category, series_name) "
                            "VALUES (?,?,?,?,?,?)",
                            ms_rows,
                        )
                except Exception as ms_err:
                    logger.warning(f"Could not persist monitor_sources: {ms_err}")

            # Remove deleted entries
            current_ids = [e["id"] for e in self._monitored_series if e.get("id")]
            if current_ids:
                placeholders = ",".join("?" * len(current_ids))
                conn.execute(
                    f"DELETE FROM monitored_series WHERE id NOT IN ({placeholders})",
                    current_ids,
                )
            else:
                conn.execute("DELETE FROM monitored_series")

            conn.commit()
        except Exception as e:
            logger.error(f"Error saving monitored series to DB: {e}")
        finally:
            conn.close()

    def _backfill_external_ids(self) -> None:
        """Backfill tmdb/imdb ids for legacy monitored entries when possible."""
        if not self._monitored_series:
            return
        if not hasattr(self, "cache_service") or self.cache_service is None:
            return
        series_list, _ = self.cache_service.get_cached_with_source_info("series", "series_categories")
        if not series_list:
            return

        index_by_source_series: dict[tuple[str, str], dict] = {}
        for item in series_list:
            key = (str(item.get("_source_id", "")), str(item.get("series_id", "")))
            if key[0] and key[1]:
                index_by_source_series[key] = item

        changed = False
        for entry in self._monitored_series:
            has_tmdb = _normalize_tmdb_id(entry.get("tmdb_id") or entry.get("tmdb"))
            has_imdb = _normalize_imdb_id(entry.get("imdb_id") or entry.get("imdb"))
            if has_tmdb or has_imdb:
                continue
            key = (str(entry.get("source_id") or ""), str(entry.get("series_id") or ""))
            candidate = index_by_source_series.get(key)
            if not candidate:
                continue
            tmdb_id = _normalize_tmdb_id(candidate.get("tmdb_id") or candidate.get("tmdb"))
            imdb_id = _normalize_imdb_id(candidate.get("imdb_id") or candidate.get("imdb"))
            if tmdb_id:
                entry["tmdb_id"] = tmdb_id
                changed = True
            if imdb_id:
                entry["imdb_id"] = imdb_id
                changed = True
        if changed:
            self.save_monitored()

    # ------------------------------------------------------------------
    # Canonical-name resolution (TMDB/IMDB cross-source key)
    # ------------------------------------------------------------------

    def resolve_canonical_name(self, tmdb_id: str | None, imdb_id: str | None) -> str | None:
        """Return the user-set canonical_name for a monitored series matching the given
        TMDB or IMDB id, or None if no match is found."""
        if not tmdb_id and not imdb_id:
            return None
        for entry in self._monitored_series:
            canon = entry.get("canonical_name")
            if not canon:
                continue
            if tmdb_id and _normalize_tmdb_id(entry.get("tmdb_id")) == tmdb_id:
                return canon
            if imdb_id and _normalize_imdb_id(entry.get("imdb_id")) == imdb_id:
                return canon
        return None

    # ------------------------------------------------------------------
    # Cross-source lookup
    # ------------------------------------------------------------------

    def find_series_across_sources(
        self,
        series_name: str,
        exclude_source_id: str | None = None,
        tmdb_id=None,
        imdb_id=None,
    ) -> list[dict]:
        """Find a series across enabled sources using IDs first, fuzzy fallback."""
        config = self._cfg.config
        enabled_sources = {s["id"]: s for s in config.get("sources", []) if s.get("enabled", True)}
        target_tmdb_id = _normalize_tmdb_id(tmdb_id)
        target_imdb_id = _normalize_imdb_id(imdb_id)
        target_normalized = normalize_name(series_name)
        if not target_tmdb_id and not target_imdb_id and not target_normalized:
            return []

        matches: list[dict] = []
        series_list, _ = self.cache_service.get_cached_with_source_info("series", "series_categories")

        for s in series_list:
            src_id = s.get("_source_id", "")
            if src_id not in enabled_sources:
                continue
            if exclude_source_id and src_id == exclude_source_id:
                continue
            s_name = s.get("name", "")
            s_normalized = normalize_name(s_name)
            s_tmdb_id = _normalize_tmdb_id(s.get("tmdb_id") or s.get("tmdb"))
            s_imdb_id = _normalize_imdb_id(s.get("imdb_id") or s.get("imdb"))

            matched_by_id = False
            if target_tmdb_id and s_tmdb_id == target_tmdb_id:
                matched_by_id = True
            if target_imdb_id and s_imdb_id == target_imdb_id:
                matched_by_id = True

            if matched_by_id:
                matches.append({
                    "source_id": src_id,
                    "series_id": str(s.get("series_id", "")),
                    "name": s_name,
                    "cover": s.get("cover", "") or s.get("stream_icon", ""),
                    "tmdb_id": s_tmdb_id,
                    "imdb_id": s_imdb_id,
                    "score": 100,
                    "matched_by": "id",
                })
                continue

            if target_normalized and s_normalized:
                score = fuzz.token_sort_ratio(target_normalized, s_normalized)
                if score >= 90:
                    matches.append({
                        "source_id": src_id,
                        "series_id": str(s.get("series_id", "")),
                        "name": s_name,
                        "cover": s.get("cover", "") or s.get("stream_icon", ""),
                        "tmdb_id": s_tmdb_id,
                        "imdb_id": s_imdb_id,
                        "score": score,
                        "matched_by": "fuzzy",
                    })

        source_order = {s["id"]: i for i, s in enumerate(config.get("sources", []))}
        matches.sort(
            key=lambda m: (
                0 if m.get("matched_by") == "id" else 1,
                -m["score"],
                source_order.get(m["source_id"], 999),
            )
        )
        return matches

    # ------------------------------------------------------------------
    # Check logic
    # ------------------------------------------------------------------

    async def check_monitored_series(self) -> None:
        """Check all enabled monitored series for new episodes (called after cache refresh)."""
        if self._check_in_progress:
            logger.info("Series monitoring check already in progress, skipping")
            return

        if not self._monitored_series:
            return

        enabled = [m for m in self._monitored_series if m.get("enabled", True)]
        if not enabled:
            return

        self._check_in_progress = True
        logger.info(f"Series monitoring: checking {len(enabled)} monitored series...")
        all_notifications: list[tuple] = []
        any_queued = False

        try:
            for entry in enabled:
                try:
                    new_eps = await self._check_single_monitored(entry)
                    if new_eps:
                        action = entry.get("action", "both")
                        all_notifications.append((
                            entry.get("series_name", "Unknown"),
                            new_eps,
                            entry.get("cover", ""),
                            action,
                        ))
                        if action in ("download", "both"):
                            any_queued = True
                except Exception as e:
                    logger.error(f"Error checking monitored series '{entry.get('series_name', '?')}': {e}")
                await asyncio.sleep(1)

            self.save_monitored()

            for series_name, new_eps, cover, action in all_notifications:
                if action in ("notify", "both"):
                    await self.notification_service.send_monitor_notification(series_name, new_eps, cover, action)

            if any_queued:
                queued = [i for i in self.cart_service.cart if i.get("status") == "queued"]
                if queued:
                    if self.cart_service.is_in_download_window():
                        if self.cart_service._try_start_worker():
                            logger.info(f"Series monitoring: auto-started download worker for {len(queued)} queued items")
                    else:
                        logger.info(f"Series monitoring: {len(queued)} items queued but outside download window, waiting for schedule")

            total_new = sum(len(eps) for _, eps, _, _ in all_notifications)
            logger.info(f"Series monitoring: check complete. {total_new} new episodes found.")
        finally:
            self._check_in_progress = False

    async def _check_single_monitored(self, entry: dict) -> list[dict]:
        """Check one monitored series entry. Returns new episode dicts."""
        series_name = entry.get("canonical_name") or entry.get("series_name", "")
        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")
        scope = entry.get("scope", "new_only")
        season_filter = entry.get("season_filter")
        monitor_sources: list[dict] = entry.get("monitor_sources") or []

        # Build list of (source_id, series_id) pairs to fetch
        sources_to_try: list[dict] = []
        if monitor_sources:
            # Explicit multi-source list set by the user
            for ms in monitor_sources:
                if ms.get("source_id") and ms.get("series_ref"):
                    sources_to_try.append({
                        "source_id": ms["source_id"],
                        "series_id": ms["series_ref"],
                    })
        elif source_id:
            # Legacy: single explicit source
            sources_to_try = [{"source_id": source_id, "series_id": series_id}]
        else:
            # Fuzzy cross-source fallback (pre-multi-source behaviour)
            matches = self.find_series_across_sources(
                series_name,
                tmdb_id=entry.get("tmdb_id") or entry.get("tmdb"),
                imdb_id=entry.get("imdb_id") or entry.get("imdb"),
            )
            if matches:
                sources_to_try = [{"source_id": m["source_id"], "series_id": m["series_id"]} for m in matches]
            else:
                logger.warning(f"Series monitoring: '{series_name}' not found in any source")
                entry["last_checked"] = datetime.now().isoformat()
                entry["last_new_count"] = 0
                return []

        # When multiple sources are configured, check ALL of them so we don't
        # miss an episode that appears on one source before the others.
        # We de-duplicate new episodes by (season, episode_num).
        all_new_episodes: list[dict] = []
        seen_ep_keys_global: set[tuple] = set()

        known_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("known_episodes", [])
        }
        downloaded_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("downloaded_episodes", [])
        }
        already_seen = known_keys | downloaded_keys
        action = entry.get("action", "both")
        should_download = action in ("download", "both")

        any_source_had_episodes = False

        for src in sources_to_try:
            src_source_id = src["source_id"]
            src_series_id = str(src["series_id"])

            episodes = await self.xtream_service.fetch_series_episodes(src_source_id, src_series_id)
            if not episodes:
                await asyncio.sleep(0.5)
                continue

            any_source_had_episodes = True

            if scope == "season" and season_filter:
                episodes = [ep for ep in episodes if str(ep.get("season")) == str(season_filter)]

            for ep in episodes:
                ep_key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
                if ep_key in already_seen or ep_key in seen_ep_keys_global:
                    continue

                stream_id = str(ep.get("stream_id", ""))
                if should_download and any(
                    i.get("source_id") == src_source_id
                    and i.get("stream_id") == stream_id
                    and i.get("status") in ("queued", "downloading")
                    for i in self.cart_service.cart
                ):
                    continue

                _sname = series_name or ep.get("series_name", "")
                cart_item = {
                    "id": str(uuid.uuid4()),
                    "stream_id": stream_id,
                    "source_id": src_source_id,
                    "content_type": "series",
                    "name": ep.get("title", "") or f"Episode {ep.get('episode_num', '')}",
                    "series_name": _sname,
                    # Preserved so _enrich_item_name_from_metadata never overwrites it
                    "_monitor_canonical": _sname if series_name else None,
                    "series_id": src_series_id or series_id,
                    "season": ep.get("season"),
                    "episode_num": ep.get("episode_num", 0),
                    "episode_title": ep.get("title", ""),
                    "episode_info": ep.get("info", {}),
                    "icon": entry.get("cover", ""),
                    "group": "",
                    "container_extension": ep.get("container_extension", "mp4"),
                    "added_at": datetime.now().isoformat(),
                    "status": "queued",
                    "progress": 0,
                    "error": None,
                    "file_path": None,
                    "file_size": None,
                }

                if should_download:
                    filepath = self.cart_service.build_download_filepath(cart_item)
                    if os.path.exists(filepath):
                        entry.setdefault("downloaded_episodes", []).append({
                            "stream_id": stream_id,
                            "source_id": src_source_id,
                            "season": ep.get("season"),
                            "episode_num": ep.get("episode_num", 0),
                        })
                        seen_ep_keys_global.add(ep_key)
                        continue
                    self.cart_service.cart.append(cart_item)

                all_new_episodes.append(cart_item)
                seen_ep_keys_global.add(ep_key)
                entry.setdefault("downloaded_episodes", []).append({
                    "stream_id": stream_id,
                    "source_id": src_source_id,
                    "season": ep.get("season"),
                    "episode_num": ep.get("episode_num", 0),
                })

            await asyncio.sleep(0.5)

        if not any_source_had_episodes:
            entry["last_checked"] = datetime.now().isoformat()
            entry["last_new_count"] = 0
            return []

        new_episodes = all_new_episodes

        if new_episodes and should_download:
            self.cart_service.save_cart()
            logger.info(f"Series monitoring: '{series_name}' — {len(new_episodes)} new episodes queued")
        elif new_episodes:
            logger.info(f"Series monitoring: '{series_name}' — {len(new_episodes)} new episodes detected (notify only)")

        entry["last_checked"] = datetime.now().isoformat()
        entry["last_new_count"] = len(new_episodes)
        return new_episodes

    # ------------------------------------------------------------------
    # Backfill – immediate download of existing episodes on monitor add
    # ------------------------------------------------------------------

    async def backfill_episodes(
        self,
        entry: dict,
        backfill: str,
        backfill_season: str | None = None,
    ) -> int:
        """Queue existing episodes for immediate download when adding a monitor entry.

        Args:
            entry: The newly-created monitor entry dict (already saved).
            backfill: ``"all"`` → every episode;
                      ``"season"`` → only ``backfill_season``.
            backfill_season: Season number string (required when backfill='season').

        Returns:
            Number of episodes queued.
        """
        if backfill not in ("all", "season"):
            return 0

        action = entry.get("action", "both")
        if action not in ("download", "both"):
            logger.info(
                f"Series monitoring backfill: skipped for '{entry.get('series_name')}' "
                f"(action={action})"
            )
            return 0

        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")
        series_name = entry.get("canonical_name") or entry.get("series_name", "")
        monitor_sources: list[dict] = entry.get("monitor_sources") or []

        # --- Build list of source/series pairs to fetch --------------------
        sources_to_try: list[dict] = []
        if monitor_sources:
            for ms in monitor_sources:
                if ms.get("source_id") and ms.get("series_ref"):
                    sources_to_try.append({
                        "source_id": ms["source_id"],
                        "series_id": ms["series_ref"],
                    })
        elif source_id:
            sources_to_try = [{"source_id": source_id, "series_id": series_id}]
        else:
            matches = self.find_series_across_sources(
                series_name,
                tmdb_id=entry.get("tmdb_id") or entry.get("tmdb"),
                imdb_id=entry.get("imdb_id") or entry.get("imdb"),
            )
            for m in matches:
                sources_to_try.append({"source_id": m["source_id"], "series_id": m["series_id"]})

        if not sources_to_try:
            logger.warning(
                f"Series monitoring backfill: no sources found for '{series_name}'"
            )
            return 0

        # --- Collect episodes from all applicable sources ------------------
        seen_ep_keys: set[tuple] = set()
        queued_count = 0

        for src in sources_to_try:
            used_source_id = src["source_id"]
            used_series_id = str(src["series_id"])

            episodes = await self.xtream_service.fetch_series_episodes(used_source_id, used_series_id)
            if not episodes:
                await asyncio.sleep(0.5)
                continue

            # Filter by season if requested
            if backfill == "season" and backfill_season:
                episodes = [
                    ep for ep in episodes
                    if str(ep.get("season")) == str(backfill_season)
                ]

            for ep in episodes:
                ep_key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
                if ep_key in seen_ep_keys:
                    continue

                stream_id = str(ep.get("stream_id", ""))

                # Skip if already in cart (queued/downloading)
                if any(
                    i.get("source_id") == used_source_id
                    and i.get("stream_id") == stream_id
                    and i.get("status") in ("queued", "downloading")
                    for i in self.cart_service.cart
                ):
                    continue

                _sname_bf = series_name or ep.get("series_name", "")
                cart_item = {
                    "id": str(uuid.uuid4()),
                    "stream_id": stream_id,
                    "source_id": used_source_id,
                    "content_type": "series",
                    "name": ep.get("title", "") or f"Episode {ep.get('episode_num', '')}",
                    "series_name": _sname_bf,
                    # Preserved so _enrich_item_name_from_metadata never overwrites it
                    "_monitor_canonical": _sname_bf if series_name else None,
                    "series_id": used_series_id or series_id,
                    "season": ep.get("season"),
                    "episode_num": ep.get("episode_num", 0),
                    "episode_title": ep.get("title", ""),
                    "episode_info": ep.get("info", {}),
                    "icon": entry.get("cover", ""),
                    "group": "",
                    "container_extension": ep.get("container_extension", "mp4"),
                    "added_at": datetime.now().isoformat(),
                    "status": "queued",
                    "progress": 0,
                    "error": None,
                    "file_path": None,
                    "file_size": None,
                }

                filepath = self.cart_service.build_download_filepath(cart_item)
                if os.path.exists(filepath):
                    entry.setdefault("downloaded_episodes", []).append({
                        "stream_id": stream_id,
                        "source_id": used_source_id,
                        "season": ep.get("season"),
                        "episode_num": ep.get("episode_num", 0),
                    })
                    seen_ep_keys.add(ep_key)
                    continue

                self.cart_service.cart.append(cart_item)
                entry.setdefault("downloaded_episodes", []).append({
                    "stream_id": stream_id,
                    "source_id": used_source_id,
                    "season": ep.get("season"),
                    "episode_num": ep.get("episode_num", 0),
                })
                seen_ep_keys.add(ep_key)
                queued_count += 1

            await asyncio.sleep(0.5)

        if queued_count > 0:
            self.cart_service.save_cart()
            logger.info(
                f"Series monitoring backfill: '{series_name}' — "
                f"{queued_count} episodes queued for download"
            )
            # Auto-start download worker when inside the download window
            queued = [i for i in self.cart_service.cart if i.get("status") == "queued"]
            if queued:
                if self.cart_service.is_in_download_window():
                    if self.cart_service._try_start_worker():
                        logger.info("Series monitoring backfill: auto-started download worker")
                else:
                    logger.info(
                        f"Series monitoring backfill: {len(queued)} items queued "
                        "but outside download window, waiting for schedule"
                    )

        return queued_count

    # ------------------------------------------------------------------
    # Preview
    # ------------------------------------------------------------------

    async def preview_episodes(self, monitor_id: str) -> dict | None:
        """Get current vs known episode comparison for a monitored entry."""
        entry = None
        for m in self._monitored_series:
            if m.get("id") == monitor_id:
                entry = m
                break
        if not entry:
            return None

        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")

        if source_id:
            episodes = await self.xtream_service.fetch_series_episodes(source_id, series_id)
        else:
            matches = self.find_series_across_sources(
                entry.get("series_name", ""),
                tmdb_id=entry.get("tmdb_id") or entry.get("tmdb"),
                imdb_id=entry.get("imdb_id") or entry.get("imdb"),
            )
            episodes = []
            for m in matches:
                eps = await self.xtream_service.fetch_series_episodes(m["source_id"], m["series_id"])
                if eps:
                    episodes = eps
                    break

        known_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("known_episodes", [])
        }
        downloaded_keys = {
            (str(k.get("season", "")), _safe_episode_num(k.get("episode_num")))
            for k in entry.get("downloaded_episodes", [])
        }

        for ep in episodes:
            key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
            ep["is_known"] = key in known_keys
            ep["is_downloaded"] = key in downloaded_keys
            ep["is_new"] = key not in (known_keys | downloaded_keys)

        return {
            "entry": entry,
            "episodes": episodes,
            "known_count": len(known_keys),
            "downloaded_count": len(downloaded_keys),
            "new_count": sum(1 for ep in episodes if ep.get("is_new")),
        }

    # ======================================================================
    # Movie monitoring
    # ======================================================================

    # ------------------------------------------------------------------
    # Movie persistence
    # ------------------------------------------------------------------

    def load_monitored_movies(self) -> list:
        """Load monitored movie rules from the database."""
        conn = db_connect(self.db_path)
        try:
            movie_rows = conn.execute(
                "SELECT id, movie_name, canonical_name, tmdb_id, cover, "
                "action, enabled, status, created_at, last_checked "
                "FROM monitored_movies ORDER BY created_at"
            ).fetchall()

            result: list[dict] = []
            for mr in movie_rows:
                mid = mr["id"]
                try:
                    msources = conn.execute(
                        "SELECT source_id, source_name, category_filter "
                        "FROM movie_monitor_sources WHERE movie_id = ? ORDER BY id",
                        (mid,),
                    ).fetchall()
                    monitor_sources = []
                    for ms in msources:
                        raw_cf = ms["category_filter"]
                        if raw_cf is None:
                            cats: list[str] = []
                        elif isinstance(raw_cf, str) and raw_cf.startswith("["):
                            try:
                                cats = json.loads(raw_cf)
                            except Exception:
                                cats = [raw_cf] if raw_cf else []
                        elif isinstance(raw_cf, str) and raw_cf:
                            cats = [raw_cf]   # backward compat: plain single string
                        else:
                            cats = []
                        monitor_sources.append({
                            "source_id": ms["source_id"],
                            "source_name": ms["source_name"],
                            "category_filter": cats,
                        })
                except Exception:
                    monitor_sources = []

                result.append({
                    "id": mid,
                    "movie_name": mr["movie_name"],
                    "canonical_name": mr["canonical_name"],
                    "tmdb_id": mr["tmdb_id"],
                    "cover": mr["cover"],
                    "action": mr["action"],
                    "enabled": bool(mr["enabled"]),
                    "status": mr["status"],
                    "monitor_sources": monitor_sources,
                    "created_at": mr["created_at"],
                    "last_checked": mr["last_checked"],
                })

            self._monitored_movies = result
            return self._monitored_movies
        except Exception as e:
            logger.error(f"Error loading monitored movies from DB: {e}")
            self._monitored_movies = []
            return self._monitored_movies
        finally:
            conn.close()

    def save_monitored_movies(self, items: list | None = None) -> None:
        """Persist the current monitored-movies list to the database."""
        if items is not None:
            self._monitored_movies = items
        conn = db_connect(self.db_path)
        try:
            for entry in self._monitored_movies:
                entry_id = entry.get("id")
                if not entry_id:
                    continue
                conn.execute(
                    """INSERT OR REPLACE INTO monitored_movies
                       (id, movie_name, canonical_name, tmdb_id, cover,
                        action, enabled, status, created_at, last_checked)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (
                        entry_id,
                        entry.get("movie_name", ""),
                        entry.get("canonical_name") or None,
                        entry.get("tmdb_id"),
                        entry.get("cover"),
                        entry.get("action", "notify"),
                        int(entry.get("enabled", True)),
                        entry.get("status", "watching"),
                        entry.get("created_at"),
                        entry.get("last_checked"),
                    ),
                )
                conn.execute(
                    "DELETE FROM movie_monitor_sources WHERE movie_id = ?", (entry_id,)
                )
                ms_rows = []
                for ms in entry.get("monitor_sources", []):
                    if not ms.get("source_id"):
                        continue
                    cf = ms.get("category_filter") or []
                    if isinstance(cf, str):
                        cf = [cf] if cf else []
                    cf_json = json.dumps(cf) if cf else None
                    ms_rows.append((entry_id, ms.get("source_id", ""), ms.get("source_name"), cf_json))
                if ms_rows:
                    conn.executemany(
                        "INSERT OR IGNORE INTO movie_monitor_sources "
                        "(movie_id, source_id, source_name, category_filter) VALUES (?,?,?,?)",
                        ms_rows,
                    )

            current_ids = [e["id"] for e in self._monitored_movies if e.get("id")]
            if current_ids:
                placeholders = ",".join("?" * len(current_ids))
                conn.execute(
                    f"DELETE FROM monitored_movies WHERE id NOT IN ({placeholders})", current_ids
                )
            else:
                conn.execute("DELETE FROM monitored_movies")

            conn.commit()
        except Exception as e:
            logger.error(f"Error saving monitored movies to DB: {e}")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Movie cross-source lookup
    # ------------------------------------------------------------------

    def find_movie_across_sources(
        self,
        movie_name: str,
        tmdb_id: str | None = None,
        source_ids: list[str] | None = None,
        category_filters: dict[str, list[str]] | None = None,
        limit: int = 0,
    ) -> list[dict]:
        """Find VOD movies across enabled sources.

        Args:
            movie_name: Title to fuzzy-match against (used when IDs are absent).
            tmdb_id: TMDB ID for exact matching (higher priority).
            source_ids: Restrict search to these source IDs (None = all enabled sources).
            category_filters: Optional per-source category_id list filter
                ({source_id: [category_id, ...]}). Empty list or missing key = no filter.
            limit: If > 0, return at most this many results.

        Returns:
            List of match dicts sorted by score (id-match first, then fuzzy).
        """
        config = self._cfg.config
        enabled_sources = {s["id"]: s for s in config.get("sources", []) if s.get("enabled", True)}
        target_source_ids = [
            sid for sid in (source_ids or list(enabled_sources.keys()))
            if sid in enabled_sources
        ]
        if not target_source_ids:
            return []

        target_tmdb_id = _normalize_tmdb_id(tmdb_id)
        target_normalized = normalize_name(movie_name)
        if not target_tmdb_id and not target_normalized:
            return []

        conn = db_connect(self.db_path)
        try:
            placeholders = ",".join("?" * len(target_source_ids))
            rows = conn.execute(
                f"SELECT source_id, stream_id, name, category_id, data "
                f"FROM streams WHERE content_type = 'vod' AND source_id IN ({placeholders})",
                target_source_ids,
            ).fetchall()
        finally:
            conn.close()

        matches: list[dict] = []
        for row in rows:
            src_id = row["source_id"]

            # Per-source category filter (list of allowed category IDs)
            if category_filters:
                allowed_cats = category_filters.get(src_id) or []
                if allowed_cats and str(row["category_id"] or "") not in [str(c) for c in allowed_cats]:
                    continue

            try:
                data = json.loads(row["data"]) if row["data"] else {}
            except Exception:
                data = {}

            s_tmdb_id = _normalize_tmdb_id(data.get("tmdb_id") or data.get("tmdb"))
            s_name = row["name"] or data.get("name", "")
            s_normalized = normalize_name(s_name)

            matched_by_id = bool(target_tmdb_id and s_tmdb_id and s_tmdb_id == target_tmdb_id)

            cover = (
                data.get("stream_icon") or data.get("movie_image")
                or data.get("cover") or data.get("backdrop_path") or ""
            )

            if matched_by_id:
                matches.append({
                    "source_id": src_id,
                    "stream_id": str(row["stream_id"]),
                    "name": s_name,
                    "cover": cover,
                    "container_extension": data.get("container_extension", "mkv"),
                    "tmdb_id": s_tmdb_id,
                    "category_id": row["category_id"],
                    "score": 100,
                    "matched_by": "id",
                })
                continue

            if target_normalized and s_normalized:
                score = fuzz.token_sort_ratio(target_normalized, s_normalized)
                if score >= 85:
                    matches.append({
                        "source_id": src_id,
                        "stream_id": str(row["stream_id"]),
                        "name": s_name,
                        "cover": cover,
                        "container_extension": data.get("container_extension", "mkv"),
                        "tmdb_id": s_tmdb_id,
                        "category_id": row["category_id"],
                        "score": score,
                        "matched_by": "fuzzy",
                    })

        source_order = {s["id"]: i for i, s in enumerate(config.get("sources", []))}
        matches.sort(key=lambda m: (
            0 if m.get("matched_by") == "id" else 1,
            -m["score"],
            source_order.get(m["source_id"], 999),
        ))
        if limit > 0:
            matches = matches[:limit]
        return matches

    # ------------------------------------------------------------------
    # Movie check logic
    # ------------------------------------------------------------------

    async def check_monitored_movies(self) -> None:
        """Check all enabled watched movies for availability (called after cache refresh)."""
        watching = [
            m for m in self._monitored_movies
            if m.get("enabled", True) and m.get("status") == "watching"
        ]
        if not watching:
            return

        logger.info(f"Movie monitoring: checking {len(watching)} watched movie(s)...")
        all_notifications: list[tuple] = []
        any_queued = False

        for entry in watching:
            try:
                found_item = await self._check_single_movie(entry)
                if found_item:
                    action = entry.get("action", "notify")
                    all_notifications.append((
                        entry.get("canonical_name") or entry.get("movie_name", "Unknown"),
                        found_item,
                        entry.get("cover", ""),
                        action,
                    ))
                    if action in ("download", "both"):
                        any_queued = True
            except Exception as e:
                logger.error(f"Error checking monitored movie '{entry.get('movie_name', '?')}': {e}")
            await asyncio.sleep(0.5)

        self.save_monitored_movies()

        for movie_name, found_item, cover, action in all_notifications:
            if action in ("notify", "both"):
                await self.notification_service.send_movie_monitor_notification(movie_name, found_item, cover, action)

        if any_queued:
            queued = [i for i in self.cart_service.cart if i.get("status") == "queued"]
            if queued:
                if self.cart_service.is_in_download_window():
                    if self.cart_service._try_start_worker():
                        logger.info(f"Movie monitoring: auto-started download worker for {len(queued)} queued item(s)")
                else:
                    logger.info(f"Movie monitoring: {len(queued)} item(s) queued but outside download window")

        logger.info(f"Movie monitoring: check complete. {len(all_notifications)} movie(s) newly found.")

    async def _check_single_movie(self, entry: dict) -> dict | None:
        """Check one monitored movie. Returns a cart-item dict if the movie is now found."""
        movie_name = entry.get("canonical_name") or entry.get("movie_name", "")
        tmdb_id = entry.get("tmdb_id")
        imdb_id = entry.get("imdb_id")
        monitor_sources: list[dict] = entry.get("monitor_sources") or []

        source_ids = [ms["source_id"] for ms in monitor_sources if ms.get("source_id")] or None
        category_filters: dict[str, list[str]] = {}
        for ms in monitor_sources:
            if ms.get("source_id"):
                cats = ms.get("category_filter") or []
                if isinstance(cats, str):
                    cats = [cats] if cats else []
                if cats:
                    category_filters[ms["source_id"]] = cats

        matches = self.find_movie_across_sources(
            movie_name,
            tmdb_id=tmdb_id,
            imdb_id=imdb_id,
            source_ids=source_ids,
            category_filters=category_filters if category_filters else None,
        )

        entry["last_checked"] = datetime.now().isoformat()

        if not matches:
            return None

        best = matches[0]
        action = entry.get("action", "notify")
        should_download = action in ("download", "both")

        cart_item = {
            "id": str(uuid.uuid4()),
            "stream_id": best["stream_id"],
            "source_id": best["source_id"],
            "content_type": "vod",
            "name": entry.get("canonical_name") or entry.get("movie_name") or best["name"],
            "icon": entry.get("cover") or best.get("cover", ""),
            "grp": "",
            "container_extension": best.get("container_extension", "mkv"),
            "added_at": datetime.now().isoformat(),
            "status": "queued",
            "progress": 0,
            "error": None,
            "file_path": None,
            "file_size": None,
        }

        if should_download:
            already_in_cart = any(
                i.get("source_id") == best["source_id"]
                and i.get("stream_id") == best["stream_id"]
                and i.get("status") in ("queued", "downloading", "completed")
                for i in self.cart_service.cart
            )
            if not already_in_cart:
                filepath = self.cart_service.build_download_filepath(cart_item)
                if os.path.exists(filepath):
                    entry["status"] = "downloaded"
                    logger.info(f"Movie monitoring: '{movie_name}' already on disk, marking downloaded")
                    return cart_item
                self.cart_service.cart.append(cart_item)
                self.cart_service.save_cart()
            entry["status"] = "downloaded"
            logger.info(f"Movie monitoring: '{movie_name}' found and queued for download")
        else:
            entry["status"] = "found"
            logger.info(f"Movie monitoring: '{movie_name}' found (notify only)")

        return cart_item

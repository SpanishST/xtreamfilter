"""Monitor service — series monitoring and new-episode detection."""
from __future__ import annotations

import asyncio
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
                "SELECT id, series_name, series_id, source_id, source_name, "
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

                result.append({
                    "id": sid,
                    "series_name": sr["series_name"],
                    "series_id": sr["series_id"],
                    "source_id": sr["source_id"],
                    "source_name": sr["source_name"],
                    "source_category": sr["source_category"],
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
                       (id, series_name, series_id, source_id, source_name,
                        source_category, cover, scope, season_filter, action,
                        enabled, created_at, last_checked, last_new_count,
                        tmdb_id, imdb_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        entry_id,
                        entry.get("series_name", ""),
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
        if not self._monitored_series:
            return

        enabled = [m for m in self._monitored_series if m.get("enabled", True)]
        if not enabled:
            return

        logger.info(f"Series monitoring: checking {len(enabled)} monitored series...")
        all_notifications: list[tuple] = []
        any_queued = False

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
            if queued and (self.cart_service.download_task is None or self.cart_service.download_task.done()):
                if self.cart_service.is_in_download_window():
                    download_path = self._cfg.download_path
                    os.makedirs(download_path, exist_ok=True)
                    self.cart_service.download_task = asyncio.create_task(self.cart_service.download_worker())
                    logger.info(f"Series monitoring: auto-started download worker for {len(queued)} queued items")
                else:
                    logger.info(f"Series monitoring: {len(queued)} items queued but outside download window, waiting for schedule")

        total_new = sum(len(eps) for _, eps, _, _ in all_notifications)
        logger.info(f"Series monitoring: check complete. {total_new} new episodes found.")

    async def _check_single_monitored(self, entry: dict) -> list[dict]:
        """Check one monitored series entry. Returns new episode dicts."""
        series_name = entry.get("series_name", "")
        source_id = entry.get("source_id")
        series_id = entry.get("series_id", "")
        scope = entry.get("scope", "new_only")
        season_filter = entry.get("season_filter")

        sources_to_try: list[dict] = []
        if source_id:
            sources_to_try = [{"source_id": source_id, "series_id": series_id}]
        else:
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

        episodes: list = []
        used_source_id: Optional[str] = None
        used_series_id: Optional[str] = None
        for src in sources_to_try:
            eps = await self.xtream_service.fetch_series_episodes(src["source_id"], src["series_id"])
            if eps:
                episodes = eps
                used_source_id = src["source_id"]
                used_series_id = str(src.get("series_id", ""))
                break
            await asyncio.sleep(0.5)

        if not episodes:
            entry["last_checked"] = datetime.now().isoformat()
            entry["last_new_count"] = 0
            return []

        if scope == "season" and season_filter:
            episodes = [ep for ep in episodes if str(ep.get("season")) == str(season_filter)]

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

        new_episodes: list[dict] = []
        for ep in episodes:
            ep_key = (str(ep.get("season", "")), _safe_episode_num(ep.get("episode_num")))
            if ep_key in already_seen:
                continue

            stream_id = str(ep.get("stream_id", ""))
            if should_download and any(
                i.get("source_id") == used_source_id
                and i.get("stream_id") == stream_id
                and i.get("status") in ("queued", "downloading")
                for i in self.cart_service.cart
            ):
                continue

            cart_item = {
                "id": str(uuid.uuid4()),
                "stream_id": stream_id,
                "source_id": used_source_id,
                "content_type": "series",
                "name": ep.get("title", "") or f"Episode {ep.get('episode_num', '')}",
                "series_name": ep.get("series_name", series_name),
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

            if should_download:
                filepath = self.cart_service.build_download_filepath(cart_item)
                if os.path.exists(filepath):
                    entry.setdefault("downloaded_episodes", []).append({
                        "stream_id": stream_id,
                        "source_id": used_source_id,
                        "season": ep.get("season"),
                        "episode_num": ep.get("episode_num", 0),
                    })
                    continue
                self.cart_service.cart.append(cart_item)

            new_episodes.append(cart_item)
            entry.setdefault("downloaded_episodes", []).append({
                "stream_id": stream_id,
                "source_id": used_source_id,
                "season": ep.get("season"),
                "episode_num": ep.get("episode_num", 0),
            })

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
        series_name = entry.get("series_name", "")

        # --- Fetch episodes ------------------------------------------------
        episodes: list = []
        used_source_id: Optional[str] = None
        used_series_id: Optional[str] = None

        if source_id:
            episodes = await self.xtream_service.fetch_series_episodes(source_id, series_id)
            used_source_id = source_id
            used_series_id = series_id
        else:
            matches = self.find_series_across_sources(
                series_name,
                tmdb_id=entry.get("tmdb_id") or entry.get("tmdb"),
                imdb_id=entry.get("imdb_id") or entry.get("imdb"),
            )
            for m in matches:
                eps = await self.xtream_service.fetch_series_episodes(
                    m["source_id"], m["series_id"]
                )
                if eps:
                    episodes = eps
                    used_source_id = m["source_id"]
                    used_series_id = str(m.get("series_id", ""))
                    break
                await asyncio.sleep(0.5)

        if not episodes:
            logger.warning(
                f"Series monitoring backfill: no episodes found for '{series_name}'"
            )
            return 0

        # --- Filter by season if requested ---------------------------------
        if backfill == "season" and backfill_season:
            episodes = [
                ep for ep in episodes
                if str(ep.get("season")) == str(backfill_season)
            ]

        # --- Queue each episode --------------------------------------------
        queued_count = 0
        for ep in episodes:
            stream_id = str(ep.get("stream_id", ""))

            # Skip if already in cart (queued/downloading)
            if any(
                i.get("source_id") == used_source_id
                and i.get("stream_id") == stream_id
                and i.get("status") in ("queued", "downloading")
                for i in self.cart_service.cart
            ):
                continue

            cart_item = {
                "id": str(uuid.uuid4()),
                "stream_id": stream_id,
                "source_id": used_source_id,
                "content_type": "series",
                "name": ep.get("title", "") or f"Episode {ep.get('episode_num', '')}",
                "series_name": ep.get("series_name", series_name),
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
                continue

            self.cart_service.cart.append(cart_item)
            entry.setdefault("downloaded_episodes", []).append({
                "stream_id": stream_id,
                "source_id": used_source_id,
                "season": ep.get("season"),
                "episode_num": ep.get("episode_num", 0),
            })
            queued_count += 1

        if queued_count > 0:
            self.cart_service.save_cart()
            logger.info(
                f"Series monitoring backfill: '{series_name}' — "
                f"{queued_count} episodes queued for download"
            )
            # Auto-start download worker when inside the download window
            queued = [i for i in self.cart_service.cart if i.get("status") == "queued"]
            if queued and (
                self.cart_service.download_task is None
                or self.cart_service.download_task.done()
            ):
                if self.cart_service.is_in_download_window():
                    download_path = self._cfg.download_path
                    os.makedirs(download_path, exist_ok=True)
                    self.cart_service.download_task = asyncio.create_task(
                        self.cart_service.download_worker()
                    )
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

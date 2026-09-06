"""Category-driven automatic download reconciliation."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime

from app.database import DB_NAME, db_connect
from app.services.media_identity import build_media_identity, history_match_sql

logger = logging.getLogger(__name__)


class AutodownloadService:
    """Find new category content and feed resolved items to the download cart."""

    def __init__(self, config_service, category_service, cache_service, xtream_service, cart_service):
        self.config_service = config_service
        self.category_service = category_service
        self.cache_service = cache_service
        self.xtream_service = xtream_service
        self.cart_service = cart_service
        self.db_path = os.path.join(config_service.data_dir, DB_NAME)
        self._run_lock = asyncio.Lock()

    async def reconcile_all(self) -> None:
        async with self._run_lock:
            categories = self.category_service.load_categories().get("categories", [])
            for category in categories:
                if (category.get("autodownload") or {}).get("enabled"):
                    await self._reconcile_category(category)

    async def reconcile_category(self, category_id: str) -> None:
        async with self._run_lock:
            category = self.category_service.get_category_by_id(category_id)
            if category and (category.get("autodownload") or {}).get("enabled"):
                await self._reconcile_category(category)

    async def backfill_category(self, category_id: str) -> dict:
        """Queue current category content that has not been downloaded yet."""
        async with self._run_lock:
            category = self.category_service.get_category_by_id(category_id)
            if not category:
                return {"error": "Category not found"}
            if not (category.get("autodownload") or {}).get("enabled"):
                return {"error": "Enable autodownload on this category first"}
            return await self._reconcile_category(category, force_backfill=True)

    def mark_cart_item_completed(self, cart_item_id: str) -> None:
        """Mark an automatic target complete using its durable cart link."""
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                "UPDATE category_autodownload_targets SET status='completed', "
                "completed_at=?, last_error=NULL WHERE cart_item_id=?",
                (datetime.now(UTC).isoformat(), cart_item_id),
            )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _media_key(content_type: str, identity: dict) -> str:
        if identity.get("media_tmdb_id"):
            return f"{content_type}:tmdb:{identity['media_tmdb_id']}"
        if identity.get("media_imdb_id"):
            return f"{content_type}:imdb:{identity['media_imdb_id']}"
        title = identity.get("media_title_key") or "unknown"
        year = identity.get("media_year") or ""
        return f"{content_type}:title:{title}:{year}"

    def _load_category_candidates(self, category: dict) -> list[dict]:
        table = "category_cached_items" if category.get("mode") == "automatic" else "category_manual_items"
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                f"""SELECT s.source_id, s.content_type, s.stream_id, s.name, s.icon,
                           s.group_name, s.tmdb_id, s.imdb_id, s.title_key,
                           s.release_year, s.container_ext, s.data
                    FROM {table} ci
                    JOIN streams s ON s.source_id = ci.source_id
                       AND s.content_type = ci.content_type
                       AND s.stream_id = ci.stream_id
                    WHERE ci.category_id=?
                      AND ci.content_type IN ('vod', 'series')""",
                (category["id"],),
            ).fetchall()
            candidates = []
            for row in rows:
                try:
                    data = json.loads(row["data"] or "{}")
                except (TypeError, ValueError):
                    data = {}
                identity = build_media_identity(
                    row["content_type"], data, name=row["name"] or ""
                )
                identity["media_tmdb_id"] = identity["media_tmdb_id"] or row["tmdb_id"]
                identity["media_imdb_id"] = identity["media_imdb_id"] or row["imdb_id"]
                identity["media_title_key"] = identity["media_title_key"] or row["title_key"]
                identity["media_year"] = identity["media_year"] or row["release_year"]
                candidates.append({
                    "source_id": row["source_id"],
                    "content_type": row["content_type"],
                    "stream_id": str(row["stream_id"]),
                    "series_id": str(row["stream_id"]) if row["content_type"] == "series" else None,
                    "name": row["name"] or "",
                    "series_name": row["name"] or "",
                    "icon": row["icon"] or "",
                    "group": row["group_name"] or "",
                    "container_extension": row["container_ext"] or "mp4",
                    **identity,
                })
            return candidates
        finally:
            conn.close()

    @staticmethod
    def _group_candidates(candidates: list[dict]) -> list[dict]:
        groups: list[dict] = []
        for candidate in candidates:
            key = AutodownloadService._media_key(candidate["content_type"], candidate)
            match = None
            for group in groups:
                same_key = group["key"] == key
                same_title = (
                    candidate.get("media_title_key")
                    and candidate.get("media_title_key") == group.get("media_title_key")
                    and candidate.get("media_year") == group.get("media_year")
                )
                if same_key or same_title:
                    match = group
                    break
            if match is None:
                match = {
                    "key": key,
                    "media_title_key": candidate.get("media_title_key"),
                    "media_year": candidate.get("media_year"),
                    "candidates": [],
                }
                groups.append(match)
            match["candidates"].append(candidate)
        return groups

    def _source_rank(self, policy: dict) -> dict[str, int]:
        configured = [
            str(source.get("id"))
            for source in self.config_service.get_enabled_sources()
            if source.get("id")
        ]
        priority = [str(source_id) for source_id in policy.get("source_priority", [])]
        ordered = list(dict.fromkeys(priority + configured))
        return {source_id: index for index, source_id in enumerate(ordered)}

    def _ordered_candidates(self, group: dict, policy: dict) -> list[dict]:
        ranks = self._source_rank(policy)
        return sorted(
            group["candidates"],
            key=lambda item: (ranks.get(str(item["source_id"]), 9999), str(item["source_id"])),
        )

    async def _build_targets(self, category: dict, policy: dict) -> list[dict]:
        candidates = self._load_category_candidates(category)
        groups = self._group_candidates(candidates)
        targets: list[dict] = []

        if policy.get("movies_enabled", True):
            for group in groups:
                if group["candidates"][0]["content_type"] != "vod":
                    continue
                candidate = self._ordered_candidates(group, policy)[0]
                targets.append({
                    "target_key": group["key"],
                    "content_type": "vod",
                    "title": candidate["name"],
                    "source_id": candidate["source_id"],
                    "stream_id": candidate["stream_id"],
                    **{field: candidate.get(field) for field in (
                        "media_tmdb_id", "media_imdb_id", "media_title_key", "media_year"
                    )},
                    "item": candidate,
                })

        if policy.get("series_enabled", True):
            seasons = {str(value) for value in policy.get("series_seasons", [])}
            for group in groups:
                if group["candidates"][0]["content_type"] != "series":
                    continue
                selected_episodes: dict[tuple[str, int], tuple[dict, dict]] = {}
                for candidate in self._ordered_candidates(group, policy):
                    episodes = await self.xtream_service.fetch_series_episodes(
                        candidate["source_id"], candidate["series_id"]
                    )
                    for episode in episodes:
                        season = str(episode.get("season", ""))
                        episode_num = int(episode.get("episode_num", 0) or 0)
                        if seasons and season not in seasons:
                            continue
                        selected_episodes.setdefault((season, episode_num), (candidate, episode))

                for (season, episode_num), (candidate, episode) in selected_episodes.items():
                    target_key = f"{group['key']}:s{season}:e{episode_num}"
                    targets.append({
                        "target_key": target_key,
                        "content_type": "series",
                        "title": candidate["series_name"],
                        "series_id": candidate["series_id"],
                        "season": season,
                        "episode_num": episode_num,
                        "source_id": candidate["source_id"],
                        "stream_id": str(episode.get("stream_id", "")),
                        **{field: candidate.get(field) for field in (
                            "media_tmdb_id", "media_imdb_id", "media_title_key", "media_year"
                        )},
                        "item": {
                            **candidate,
                            "stream_id": str(episode.get("stream_id", "")),
                            "name": episode.get("title", "") or f"Episode {episode_num}",
                            "episode_title": episode.get("title", ""),
                            "episode_info": episode.get("info"),
                            "season": season,
                            "episode_num": episode_num,
                            "container_extension": episode.get("container_extension", "mp4"),
                        },
                    })
        return targets

    def _load_targets(self, category_id: str) -> dict[str, dict]:
        conn = db_connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM category_autodownload_targets WHERE category_id=?",
                (category_id,),
            ).fetchall()
            return {row["target_key"]: dict(row) for row in rows}
        finally:
            conn.close()

    def _history_exists(self, target: dict) -> bool:
        conn = db_connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT 1 FROM streams s JOIN download_history dh "
                f"ON dh.content_type=s.content_type AND {history_match_sql('s', 'dh')} "
                "WHERE s.source_id=? AND s.content_type=? AND s.stream_id=? "
                "AND (? IS NULL OR dh.season=?) AND (? IS NULL OR dh.episode_num=?) LIMIT 1",
                (
                    target["source_id"], target["content_type"],
                    target["item"].get("series_id") if target["content_type"] == "series" else target["stream_id"],
                    target.get("season"), target.get("season"),
                    target.get("episode_num"), target.get("episode_num"),
                ),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def _upsert_target(self, category_id: str, target: dict, status: str, *, error: str | None = None, cart_item_id: str | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                """INSERT INTO category_autodownload_targets
                   (category_id, target_key, content_type, title, media_tmdb_id,
                    media_imdb_id, media_title_key, media_year, series_id, season,
                    episode_num, source_id, stream_id, status, cart_item_id,
                    first_seen, last_seen, completed_at, last_error)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(category_id, target_key) DO UPDATE SET
                    title=excluded.title, media_tmdb_id=excluded.media_tmdb_id,
                    media_imdb_id=excluded.media_imdb_id, media_title_key=excluded.media_title_key,
                    media_year=excluded.media_year, source_id=excluded.source_id,
                    stream_id=excluded.stream_id, status=excluded.status,
                    cart_item_id=COALESCE(excluded.cart_item_id, category_autodownload_targets.cart_item_id),
                    last_seen=excluded.last_seen, completed_at=excluded.completed_at,
                    last_error=excluded.last_error""",
                (
                    category_id, target["target_key"], target["content_type"], target.get("title"),
                    target.get("media_tmdb_id"), target.get("media_imdb_id"),
                    target.get("media_title_key"), target.get("media_year"), target.get("series_id"),
                    target.get("season"), target.get("episode_num"), target.get("source_id"),
                    target.get("stream_id"), status, cart_item_id, now, now,
                    now if status == "completed" else None, error,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _update_policy(self, category_id: str, *, baseline: bool, queued: int, error: str | None = None) -> None:
        conn = db_connect(self.db_path)
        try:
            conn.execute(
                "UPDATE category_autodownload SET baseline_initialized=?, last_run=?, "
                "last_queued=?, last_error=? WHERE category_id=?",
                (int(baseline), datetime.now(UTC).isoformat(), queued, error, category_id),
            )
            conn.commit()
        finally:
            conn.close()

    async def _reconcile_category(self, category: dict, force_backfill: bool = False) -> dict:
        category_id = category["id"]
        policy = category.get("autodownload") or {}
        try:
            targets = await self._build_targets(category, policy)
            existing = self._load_targets(category_id)
            if not force_backfill and not policy.get("baseline_initialized", False):
                for target in targets:
                    self._upsert_target(category_id, target, "seen")
                self._update_policy(category_id, baseline=True, queued=0)
                logger.info("Autodownload baseline initialized for category '%s' (%d targets)", category.get("name"), len(targets))
                return {"queued": 0, "skipped": len(targets), "baseline_initialized": True}

            queue: list[dict] = []
            for target in targets:
                existing_target = existing.get(target["target_key"])
                if existing_target and (
                    not force_backfill
                    or existing_target.get("status") in ("queued", "downloading", "completed")
                ):
                    continue
                if self._history_exists(target):
                    self._upsert_target(category_id, target, "completed")
                    continue
                item = dict(target["item"])
                item.update({
                    "destination": self.config_service.get_download_destination(target["content_type"]),
                    "autodownload_category_id": category_id,
                    "autodownload_target_key": target["target_key"],
                    "media_tmdb_id": target.get("media_tmdb_id"),
                    "media_imdb_id": target.get("media_imdb_id"),
                    "media_title_key": target.get("media_title_key"),
                    "media_year": target.get("media_year"),
                })
                queue.append(item)

            result = await self.cart_service.add_prebuilt_items(queue) if queue else {"items": [], "added": 0}
            for item in result.get("items", []):
                self._upsert_target(
                    category_id,
                    next(target for target in targets if target["target_key"] == item.get("autodownload_target_key")),
                    "queued",
                    cart_item_id=item.get("id"),
                )
            self._update_policy(category_id, baseline=True, queued=result.get("added", 0))
            if result.get("added") and self.cart_service.is_in_download_window():
                self.cart_service._try_start_worker()
            logger.info("Autodownload category '%s': queued %d item(s)", category.get("name"), result.get("added", 0))
            return {
                "queued": result.get("added", 0),
                "skipped": len(result.get("skipped", [])),
                "baseline_initialized": True,
            }
        except Exception as exc:
            logger.error("Autodownload failed for category '%s': %s", category.get("name"), exc, exc_info=True)
            self._update_policy(category_id, baseline=bool(policy.get("baseline_initialized")), queued=0, error=str(exc))
            return {"error": str(exc), "queued": 0}

"""Unit tests for movie monitoring feature (MonitorService movie methods)."""

import asyncio
import json
import os
import tempfile
import uuid

from app.database import DB_NAME, db_connect, init_db


def _run(coro):
    """Run a coroutine synchronously (helper for sync test methods)."""
    return asyncio.run(coro)


# ---------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------

def _make_ms(d, sources=None):
    """Build a minimal MonitorService pointing at a temp DB directory.

    Args:
        d: Temporary directory path used for both config and DB.
        sources: Optional list of source dicts to put in config. Defaults to
                 two sources: srcA and srcB (both enabled).
    """
    from app.services.config_service import ConfigService
    from app.services.monitor_service import MonitorService

    if sources is None:
        sources = [
            {"id": "srcA", "name": "Provider A", "enabled": True},
            {"id": "srcB", "name": "Provider B", "enabled": True},
        ]

    cfg = ConfigService(d)
    cfg.load()
    cfg.config["sources"] = sources

    db_path = os.path.join(d, DB_NAME)
    init_db(db_path)

    ms = MonitorService.__new__(MonitorService)
    ms._cfg = cfg
    ms.db_path = db_path
    ms._monitored_series = []
    ms._monitored_movies = []
    return ms


def _insert_vod_stream(db_path, *, source_id, stream_id, name, category_id="10",
                        tmdb_id=None, cover=""):
    """Insert a VOD row into the streams table for testing."""
    data = {
        "tmdb_id": tmdb_id,
        "stream_icon": cover,
        "container_extension": "mkv",
    }
    conn = db_connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO streams "
            "(source_id, stream_id, content_type, name, category_id, data) "
            "VALUES (?,?,?,?,?,?)",
            (source_id, stream_id, "vod", name, category_id, json.dumps(data)),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------
# Monitored-movie persistence (save / load round-trips)
# ---------------------------------------------------------------

class TestMovieMonitorPersistence:

    def _make_entry(self, *, eid=None, movie_name="Dune", tmdb_id="438631",
                    status="watching", action="notify", sources=None):
        return {
            "id": eid or str(uuid.uuid4()),
            "movie_name": movie_name,
            "canonical_name": movie_name,
            "tmdb_id": tmdb_id,
            "cover": "",
            "action": action,
            "enabled": True,
            "status": status,
            "monitor_sources": sources or [],
            "created_at": "2026-01-01T00:00:00",
            "last_checked": None,
        }

    def test_load_empty(self):
        """Loading from an empty table returns an empty list."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            result = ms.load_monitored_movies()
            assert result == []
            assert ms._monitored_movies == []

    def test_save_load_single_entry_no_sources(self):
        """A movie with no monitor_sources survives a save/load cycle."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            entry = self._make_entry(movie_name="Inception", tmdb_id="27205")
            ms._monitored_movies = [entry]
            ms.save_monitored_movies()

            ms._monitored_movies = []
            ms.load_monitored_movies()

            assert len(ms._monitored_movies) == 1
            loaded = ms._monitored_movies[0]
            assert loaded["movie_name"] == "Inception"
            assert loaded["tmdb_id"] == "27205"
            assert loaded["status"] == "watching"
            assert loaded["monitor_sources"] == []

    def test_save_load_single_entry_with_one_source(self):
        """A movie with a monitor source survives a save/load cycle."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            entry = self._make_entry(
                movie_name="Dune",
                tmdb_id="438631",
                sources=[{"source_id": "srcA", "source_name": "Provider A", "category_filter": ["15"]}],
            )
            ms._monitored_movies = [entry]
            ms.save_monitored_movies()

            ms._monitored_movies = []
            ms.load_monitored_movies()

            loaded = ms._monitored_movies[0]
            assert len(loaded["monitor_sources"]) == 1
            src = loaded["monitor_sources"][0]
            assert src["source_id"] == "srcA"
            assert src["source_name"] == "Provider A"
            assert src["category_filter"] == ["15"]

    def test_save_load_multi_source(self):
        """A movie tracked in two sources stores and reloads both rows."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            entry = self._make_entry(
                movie_name="Interstellar",
                tmdb_id="157336",
                sources=[
                    {"source_id": "srcA", "source_name": "Provider A", "category_filter": []},
                    {"source_id": "srcB", "source_name": "Provider B", "category_filter": ["22"]},
                ],
            )
            ms._monitored_movies = [entry]
            ms.save_monitored_movies()

            ms._monitored_movies = []
            ms.load_monitored_movies()

            srcs = ms._monitored_movies[0]["monitor_sources"]
            assert len(srcs) == 2
            src_ids = {s["source_id"] for s in srcs}
            assert src_ids == {"srcA", "srcB"}

    def test_save_load_multiple_movies(self):
        """Multiple movies are all reloaded correctly."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            ms._monitored_movies = [
                self._make_entry(movie_name="Dune", tmdb_id="438631"),
                self._make_entry(movie_name="Arrival", tmdb_id="329865"),
                self._make_entry(movie_name="Tenet", tmdb_id="577922"),
            ]
            ms.save_monitored_movies()

            ms._monitored_movies = []
            ms.load_monitored_movies()

            assert len(ms._monitored_movies) == 3
            names = {m["movie_name"] for m in ms._monitored_movies}
            assert names == {"Dune", "Arrival", "Tenet"}

    def test_update_status_persists(self):
        """Changing status to 'found' is persisted on next save."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            entry = self._make_entry(status="watching")
            ms._monitored_movies = [entry]
            ms.save_monitored_movies()

            ms._monitored_movies[0]["status"] = "found"
            ms.save_monitored_movies()

            ms._monitored_movies = []
            ms.load_monitored_movies()

            assert ms._monitored_movies[0]["status"] == "found"

    def test_delete_movie_removes_sources_cascade(self):
        """Removing a movie from the list also removes its source rows (cascade)."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            entry = self._make_entry(
                eid="movie-del",
                sources=[{"source_id": "srcA", "source_name": "P A", "category_filter": None}],
            )
            ms._monitored_movies = [entry]
            ms.save_monitored_movies()

            # Now remove the entry (simulates DELETE)
            ms._monitored_movies = []
            ms.save_monitored_movies()

            conn = db_connect(ms.db_path)
            try:
                rows = conn.execute(
                    "SELECT * FROM movie_monitor_sources WHERE movie_id = 'movie-del'"
                ).fetchall()
            finally:
                conn.close()
            assert rows == []

    def test_save_via_items_argument(self):
        """save_monitored_movies(items=...) updates _monitored_movies and persists."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            items = [self._make_entry(movie_name="Blade Runner", tmdb_id="78")]
            ms.save_monitored_movies(items=items)
            assert ms._monitored_movies == items

            ms._monitored_movies = []
            ms.load_monitored_movies()
            assert ms._monitored_movies[0]["movie_name"] == "Blade Runner"


# ---------------------------------------------------------------
# find_movie_across_sources DB-backed matching
# ---------------------------------------------------------------

class TestFindMovieAcrossSourcesDB:

    def test_exact_tmdb_id_match(self):
        """A stream with a matching TMDB ID is returned as an id-match."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="1001",
                                name="FR - Dune: Part One", tmdb_id="438631")

            results = ms.find_movie_across_sources("Dune", tmdb_id="438631")
            assert len(results) == 1
            assert results[0]["matched_by"] == "id"
            assert results[0]["stream_id"] == "1001"
            assert results[0]["source_id"] == "srcA"

    def test_fuzzy_name_match_above_threshold(self):
        """A stream matching the title with ≥85 fuzzy score is returned."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="3003",
                                name="Blade Runner 2049")

            results = ms.find_movie_across_sources("Blade Runner 2049")
            assert results
            assert results[0]["matched_by"] == "fuzzy"
            assert results[0]["stream_id"] == "3003"

    def test_fuzzy_name_match_below_threshold_not_returned(self):
        """A stream whose fuzzy score is below 85 is excluded."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            # "Batman Begins" vs "Blade Runner 2049" → low score
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="9999",
                                name="Completely Unrelated Title XYZ")

            results = ms.find_movie_across_sources("Blade Runner 2049")
            assert results == []

    def test_id_match_preferred_over_fuzzy(self):
        """When both an id-match and fuzzy match exist, id-match sorts first."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="1001",
                                name="FR - Dune 2021", tmdb_id="438631")
            _insert_vod_stream(ms.db_path, source_id="srcB", stream_id="2001",
                                name="Dune 2021")  # fuzzy match only

            results = ms.find_movie_across_sources("Dune", tmdb_id="438631")
            assert results[0]["matched_by"] == "id"
            assert results[0]["stream_id"] == "1001"

    def test_source_id_filter_restricts_results(self):
        """Passing source_ids limits search to those sources only."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="1001",
                                name="Arrival", tmdb_id="329865")
            _insert_vod_stream(ms.db_path, source_id="srcB", stream_id="2001",
                                name="Arrival", tmdb_id="329865")

            results = ms.find_movie_across_sources("Arrival", tmdb_id="329865",
                                                    source_ids=["srcB"])
            assert len(results) == 1
            assert results[0]["source_id"] == "srcB"

    def test_category_filter_restricts_within_source(self):
        """category_filters excludes VoD entries not in the specified category."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="1001",
                                name="Tenet", tmdb_id="577922", category_id="10")
            _insert_vod_stream(ms.db_path, source_id="srcA", stream_id="1002",
                                name="Tenet", tmdb_id="577922", category_id="20")

            # Only look in category 10
            results = ms.find_movie_across_sources(
                "Tenet", tmdb_id="577922",
                category_filters={"srcA": ["10"]},
            )
            assert len(results) == 1
            assert results[0]["stream_id"] == "1001"

    def test_no_streams_in_db_returns_empty(self):
        """When no streams match, an empty list is returned."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            results = ms.find_movie_across_sources("Ghost Movie", tmdb_id="999999")
            assert results == []

    def test_limit_parameter_is_respected(self):
        """The limit parameter caps the number of returned results."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d)
            for i in range(5):
                _insert_vod_stream(ms.db_path, source_id="srcA", stream_id=str(1000 + i),
                                    name="Dune", tmdb_id="438631")

            results = ms.find_movie_across_sources("Dune", tmdb_id="438631", limit=2)
            assert len(results) <= 2

    def test_disabled_source_excluded(self):
        """Streams from disabled sources are not returned."""
        with tempfile.TemporaryDirectory() as d:
            ms = _make_ms(d, sources=[
                {"id": "srcA", "name": "Provider A", "enabled": True},
                {"id": "srcDisabled", "name": "Disabled", "enabled": False},
            ])
            _insert_vod_stream(ms.db_path, source_id="srcDisabled", stream_id="9001",
                                name="Inception", tmdb_id="27205")

            results = ms.find_movie_across_sources("Inception", tmdb_id="27205")
            assert results == []


# ---------------------------------------------------------------
# _check_single_movie logic
# ---------------------------------------------------------------

class TestCheckSingleMovieLogic:
    """Tests for _check_single_movie with mocked cart and notification services."""

    def _make_ms_with_mocks(self, d, found_matches=None):
        """Build MonitorService with overridden find_movie_across_sources."""
        from app.services.monitor_service import MonitorService

        ms = _make_ms(d)

        # Override find_movie_across_sources so we don't need DB rows
        _matches = found_matches if found_matches is not None else []
        ms.find_movie_across_sources = lambda *a, **kw: _matches

        class _Cart:
            cart = []
            _started = False

            def build_download_filepath(self, item):
                return "/tmp/nonexistent_" + item["stream_id"] + ".mkv"

            def save_cart(self):
                pass

            def is_in_download_window(self):
                return True

            def _try_start_worker(self):
                self._started = True
                return True

        class _Notif:
            notifications = []

            async def send_movie_monitor_notification(self, movie_name, item, cover, action):
                self.notifications.append((movie_name, item, action))

        cart = _Cart()
        notif = _Notif()
        ms.cart_service = cart  # type: ignore[assignment]
        ms.notification_service = notif  # type: ignore[assignment]
        return ms, cart, notif

    def _make_entry(self, *, status="watching", action="notify", sources=None):
        return {
            "id": str(uuid.uuid4()),
            "movie_name": "Dune",
            "canonical_name": "Dune: Part One",
            "tmdb_id": "438631",
            "cover": "http://example.com/dune.jpg",
            "action": action,
            "enabled": True,
            "status": status,
            "monitor_sources": sources or [],
            "created_at": "2026-01-01T00:00:00",
            "last_checked": None,
        }

    def _make_match(self):
        return {
            "source_id": "srcA",
            "stream_id": "1001",
            "name": "FR - Dune: Part One",
            "cover": "http://example.com/dune.jpg",
            "container_extension": "mkv",
            "tmdb_id": "438631",
            "score": 100,
            "matched_by": "id",
        }

    def test_not_found_returns_none_and_status_unchanged(self):
        """When no matches, _check_single_movie returns None and status stays 'watching'."""
        with tempfile.TemporaryDirectory() as d:
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=[])
            entry = self._make_entry()
            result = _run(ms._check_single_movie(entry))
            assert result is None
            assert entry["status"] == "watching"
            assert entry["last_checked"] is not None

    def test_notify_action_sets_status_found(self):
        """When action='notify', status becomes 'found' and cart is not touched."""
        with tempfile.TemporaryDirectory() as d:
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=[self._make_match()])
            entry = self._make_entry(action="notify")
            result = _run(ms._check_single_movie(entry))
            assert result is not None
            assert entry["status"] == "found"
            assert cart.cart == []

    def test_download_action_adds_to_cart_and_sets_status_downloaded(self):
        """When action='download', item is added to cart and status becomes 'downloaded'."""
        with tempfile.TemporaryDirectory() as d:
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=[self._make_match()])
            entry = self._make_entry(action="download")
            result = _run(ms._check_single_movie(entry))
            assert result is not None
            assert entry["status"] == "downloaded"
            assert len(cart.cart) == 1
            assert cart.cart[0]["stream_id"] == "1001"
            assert cart.cart[0]["content_type"] == "vod"

    def test_both_action_adds_to_cart_and_sets_downloaded(self):
        """When action='both', item is downloaded and status is 'downloaded'."""
        with tempfile.TemporaryDirectory() as d:
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=[self._make_match()])
            entry = self._make_entry(action="both")
            result = _run(ms._check_single_movie(entry))
            assert result is not None
            assert entry["status"] == "downloaded"
            assert len(cart.cart) == 1

    def test_already_in_cart_not_duplicated(self):
        """A stream already queued in cart is not added a second time."""
        with tempfile.TemporaryDirectory() as d:
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=[self._make_match()])
            # Pre-populate cart with a completed item for the same stream
            cart.cart.append({
                "source_id": "srcA",
                "stream_id": "1001",
                "status": "completed",
            })
            entry = self._make_entry(action="download")
            _run(ms._check_single_movie(entry))
            # cart length should still be 1 (no duplicate)
            assert len(cart.cart) == 1

    def test_best_match_used_when_multiple_found(self):
        """The first (highest-scored) match is used for the cart item."""
        with tempfile.TemporaryDirectory() as d:
            matches = [
                {**self._make_match(), "stream_id": "9001", "score": 100, "matched_by": "id"},
                {**self._make_match(), "stream_id": "9002", "score": 88, "matched_by": "fuzzy"},
            ]
            ms, cart, notif = self._make_ms_with_mocks(d, found_matches=matches)
            entry = self._make_entry(action="download")
            _run(ms._check_single_movie(entry))
            assert cart.cart[0]["stream_id"] == "9001"


# ---------------------------------------------------------------
# check_monitored_movies (batch) logic
# ---------------------------------------------------------------

class TestCheckMonitoredMoviesBatch:
    """Tests for the public check_monitored_movies coroutine."""

    def _make_ms_batch(self, d, entries, found_matches_by_entry=None):
        """Build ms where _check_single_movie is mocked per-entry."""
        from app.services.monitor_service import MonitorService
        import asyncio

        ms = _make_ms(d)
        ms._monitored_movies = entries

        # found_matches_by_entry: dict[movie_name -> match or None]
        _fmbe = found_matches_by_entry or {}

        async def _check_single(entry):
            match = _fmbe.get(entry["movie_name"])
            if match:
                action = entry.get("action", "notify")
                entry["status"] = "downloaded" if action in ("download", "both") else "found"
                entry["last_checked"] = "2026-01-01T12:00:00"
                return match
            entry["last_checked"] = "2026-01-01T12:00:00"
            return None

        ms._check_single_movie = _check_single

        class _Cart:
            cart = []

            def is_in_download_window(self):
                return True

            def _try_start_worker(self):
                return True

        class _Notif:
            notifications = []

            async def send_movie_monitor_notification(self, movie_name, item, cover, action):
                self.notifications.append((movie_name, action))

        ms.cart_service = _Cart()  # type: ignore[assignment]
        ms.notification_service = _Notif()  # type: ignore[assignment]
        return ms, ms.cart_service, ms.notification_service

    def _make_movie_entry(self, movie_name, *, action="notify", status="watching"):
        return {
            "id": str(uuid.uuid4()),
            "movie_name": movie_name,
            "canonical_name": movie_name,
            "tmdb_id": "12345",
            "cover": "",
            "action": action,
            "enabled": True,
            "status": status,
            "monitor_sources": [],
            "created_at": "2026-01-01T00:00:00",
            "last_checked": None,
        }

    def _dummy_match(self, stream_id="5001"):
        return {
            "source_id": "srcA",
            "stream_id": stream_id,
            "name": "Movie",
            "cover": "",
            "container_extension": "mkv",
            "tmdb_id": "12345",
            "score": 100,
            "matched_by": "id",
        }

    def test_no_watching_movies_does_nothing(self):
        """check_monitored_movies is a no-op when nothing has status='watching'."""
        with tempfile.TemporaryDirectory() as d:
            entries = [self._make_movie_entry("Dune", status="found")]
            ms, cart, notif = self._make_ms_batch(d, entries)
            _run(ms.check_monitored_movies())
            assert notif.notifications == []  # type: ignore[attr-defined]

    def test_found_movie_triggers_notification_for_notify_action(self):
        """When a watched movie is found with action='notify', a notification is sent."""
        with tempfile.TemporaryDirectory() as d:
            entry = self._make_movie_entry("Dune", action="notify")
            ms, cart, notif = self._make_ms_batch(
                d, [entry], {"Dune": self._dummy_match()}
            )
            _run(ms.check_monitored_movies())
            assert len(notif.notifications) == 1  # type: ignore[attr-defined]
            assert notif.notifications[0][0] == "Dune"  # type: ignore[attr-defined]
            assert notif.notifications[0][1] == "notify"  # type: ignore[attr-defined]

    def test_not_found_movie_no_notification(self):
        """When a watched movie is not found, no notification is sent."""
        with tempfile.TemporaryDirectory() as d:
            entry = self._make_movie_entry("Ghost Film", action="notify")
            ms, cart, notif = self._make_ms_batch(d, [entry], {})  # no match
            _run(ms.check_monitored_movies())
            assert notif.notifications == []  # type: ignore[attr-defined]

    def test_multiple_movies_mixed_results(self):
        """Only found movies produce notifications."""
        with tempfile.TemporaryDirectory() as d:
            entries = [
                self._make_movie_entry("Found Film", action="notify"),
                self._make_movie_entry("Missing Film", action="notify"),
            ]
            ms, cart, notif = self._make_ms_batch(
                d, entries, {"Found Film": self._dummy_match("5001")}
            )
            _run(ms.check_monitored_movies())
            assert len(notif.notifications) == 1  # type: ignore[attr-defined]
            assert notif.notifications[0][0] == "Found Film"  # type: ignore[attr-defined]

    def test_disabled_movies_are_skipped(self):
        """Movies with enabled=False are not checked."""
        with tempfile.TemporaryDirectory() as d:
            entry = self._make_movie_entry("Disabled Film", action="notify")
            entry["enabled"] = False
            ms, cart, notif = self._make_ms_batch(
                d, [entry], {"Disabled Film": self._dummy_match()}
            )
            _run(ms.check_monitored_movies())
            assert notif.notifications == []  # type: ignore[attr-defined]

# ---------------------------------------------------------------
# Models — MonitoredMovie and MovieMonitorSource
# ---------------------------------------------------------------

class TestMovieMonitorModels:

    def test_monitored_movie_model(self):
        """MonitoredMovie model accepts a valid payload with dict monitor_sources."""
        from app.models.monitor import MonitoredMovie

        movie = MonitoredMovie(
            id="uuid-123",
            movie_name="Dune",
            canonical_name="Dune: Part One",
            tmdb_id="438631",
            action="notify",
            enabled=True,
            status="watching",
            monitor_sources=[
                {"source_id": "srcA", "source_name": "Provider A", "category_filter": []}
            ],
            created_at="2026-01-01T00:00:00",
        )
        assert movie.movie_name == "Dune"
        assert movie.status == "watching"
        assert len(movie.monitor_sources) == 1
        assert movie.monitor_sources[0]["source_id"] == "srcA"

    def test_movie_monitor_source_model_defaults(self):
        """MovieMonitorSource category_filter defaults to empty list."""
        from app.models.monitor import MovieMonitorSource

        src = MovieMonitorSource(source_id="x", source_name="X")
        assert src.category_filter == []

    def test_monitored_movie_defaults(self):
        """Optional fields have sensible defaults."""
        from app.models.monitor import MonitoredMovie

        movie = MonitoredMovie(
            id="u1",
            movie_name="Test",
            action="notify",
            enabled=True,
            status="watching",
            created_at="2026-01-01T00:00:00",
        )
        assert movie.monitor_sources == []
        assert movie.tmdb_id is None
        assert movie.last_checked is None

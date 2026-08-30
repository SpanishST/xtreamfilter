"""Regression tests for download memory and file-cache retention."""

from __future__ import annotations

import asyncio
import os

from app.services.cart_service import CartService, release_file_cache
from app.services.xtream_service import compact_episode_info


def test_compact_episode_info_discards_nested_provider_payloads():
    info = {
        "plot": "An episode plot",
        "rating": "8.5",
        "air_date": "2026-08-28",
        "nested": {"large_payload": "x" * 100_000},
        "cast": ["Actor 1", "Actor 2"],
    }

    compact = compact_episode_info(info)

    assert compact == {
        "plot": "An episode plot",
        "rating": "8.5",
        "air_date": "2026-08-28",
    }


def test_cart_items_compact_episode_metadata():
    item = CartService._build_cart_item(
        content_type="series",
        stream_id="episode-1",
        episode_info={
            "description": "Description",
            "info": {"large": "x" * 100_000},
        },
    )

    assert item["episode_info"] == {"description": "Description"}


def test_finalize_removes_transient_metadata_and_releases_file_cache(monkeypatch):
    cart = CartService.__new__(CartService)
    cart.jellyfin_service = None

    async def write_metadata(_item, _file_path):
        return None

    async def embed_metadata(_item, _file_path):
        return None

    class FakeNotificationService:
        async def send_download_file_notification(self, _item):
            return None

    released: list[str] = []

    def fake_release(path: str):
        released.append(path)

    cart._write_metadata = write_metadata
    cart._embed_container_metadata = embed_metadata
    cart.notification_service = FakeNotificationService()
    monkeypatch.setattr("app.services.cart_service.release_file_cache", fake_release)

    item = {
        "id": "episode-1",
        "file_path": "/downloads/episode-1.mp4",
        "episode_info": {"description": "Description"},
        "_series_info": {"name": "Series", "episodes": ["large"]},
        "_vod_info": {"plot": "Movie"},
    }

    asyncio.run(cart._finalize_completed_download(item))

    assert released == ["/downloads/episode-1.mp4"]
    assert "episode_info" not in item
    assert "_series_info" not in item
    assert "_vod_info" not in item


def test_release_file_cache_advises_kernel_for_entire_file(tmp_path, monkeypatch):
    path = tmp_path / "download.mp4"
    path.write_bytes(b"video")
    calls: list[tuple[int, int, int, int]] = []

    def fake_posix_fadvise(fd: int, offset: int, length: int, advice: int):
        calls.append((fd, offset, length, advice))

    monkeypatch.setattr(os, "posix_fadvise", fake_posix_fadvise, raising=False)

    release_file_cache(str(path))

    assert len(calls) == 1
    _, offset, length, advice = calls[0]
    assert offset == 0
    assert length == 0
    assert advice == os.POSIX_FADV_DONTNEED

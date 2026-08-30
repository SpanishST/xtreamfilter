"""Download queue pause/resume behavior."""

import asyncio
import os

import pytest

from app.services import cart_service as cart_module
from app.services.cart_service import CartService


class _Config:
    def __init__(self, data_dir):
        self.data_dir = str(data_dir)

    def get_download_temp_path(self):
        return self.data_dir

    def get_download_throttle_settings(self):
        return {
            "bandwidth_limit": 0,
            "pause_interval": 0,
            "pause_duration": 0,
            "player_profile": "tivimate",
            "burst_reconnect": 0,
        }

    def get_download_schedule(self):
        return {"enabled": False}


class _Response:
    def __init__(self, headers, request_number, first_chunk_seen, allow_second_chunk):
        self.status_code = 200 if request_number == 0 else 206
        self.headers = {"content-length": "6"}
        if request_number > 0:
            self.headers = {"content-range": "bytes 3-5/6", "content-length": "3"}
        self._request_number = request_number
        self._first_chunk_seen = first_chunk_seen
        self._allow_second_chunk = allow_second_chunk
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self.closed = True

    async def aiter_bytes(self, chunk_size):
        if self._request_number == 0:
            self._first_chunk_seen.set()
            yield b"abc"
            await self._allow_second_chunk.wait()
            yield b"def"
        else:
            yield b"def"


class _AsyncClient:
    requests = []
    first_chunk_seen = None
    allow_second_chunk = None

    def __init__(self, *, headers, **_):
        self.headers = headers
        self.request_number = len(type(self).requests)
        type(self).requests.append(headers)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    def stream(self, *_):
        return _Response(
            self.headers,
            self.request_number,
            type(self).first_chunk_seen,
            type(self).allow_second_chunk,
        )


@pytest.mark.asyncio
async def test_pause_active_download_resumes_from_partial_file(tmp_path, monkeypatch):
    _AsyncClient.requests = []
    _AsyncClient.first_chunk_seen = asyncio.Event()
    _AsyncClient.allow_second_chunk = asyncio.Event()
    monkeypatch.setattr(cart_module.httpx, "AsyncClient", _AsyncClient)

    config = _Config(tmp_path)
    cart = CartService(config, None, None, None)
    item = {
        "id": "item-1",
        "stream_id": "stream-1",
        "source_id": "source-1",
        "content_type": "vod",
        "name": "Test video",
        "container_extension": "mp4",
        "status": "queued",
        "progress": 0,
        "error": None,
    }
    cart.cart.append(item)
    cart.save_cart = lambda: None
    cart.build_upstream_url = lambda _: "https://provider.example/video"
    cart.build_download_filepath = lambda _: str(tmp_path / "destination.mp4")

    async def enrich(_):
        pass

    async def move(item_to_move, temp_path, file_path):
        os.replace(temp_path, file_path)
        item_to_move["status"] = "completed"
        item_to_move["file_size"] = os.path.getsize(file_path)
        return True

    async def finalize(_):
        pass

    async def queue_complete():
        pass

    cart._enrich_item_name_from_metadata = enrich
    cart._move_temp_to_destination = move
    cart._finalize_completed_download = finalize
    cart._handle_download_queue_complete = queue_complete

    assert cart._try_start_worker() is True
    task = cart.download_task
    assert task is not None

    await asyncio.wait_for(_AsyncClient.first_chunk_seen.wait(), timeout=1)
    cart.pause_downloads()
    _AsyncClient.allow_second_chunk.set()

    temp_path = tmp_path / "item-1_destination.mp4"
    for _ in range(20):
        if temp_path.exists() and temp_path.stat().st_size == 3 and len(_AsyncClient.requests) == 1:
            break
        await asyncio.sleep(0.01)

    assert temp_path.stat().st_size == 3
    assert cart.queue_paused is True
    assert cart.current_item is item
    assert not task.done()

    cart.resume_downloads()
    await asyncio.wait_for(asyncio.shield(task), timeout=1)

    assert _AsyncClient.requests[0].get("Range") is None
    assert _AsyncClient.requests[1]["Range"] == "bytes=3-"
    assert (tmp_path / "destination.mp4").read_bytes() == b"abcdef"
    assert item["status"] == "completed"

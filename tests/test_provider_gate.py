"""ProviderGate claim/exclusion behavior."""

import asyncio

import pytest

from app.services.provider_gate import ProviderGate, maybe_background_fetch


@pytest.mark.asyncio
async def test_background_fetch_defers_to_active_transfer():
    gate = ProviderGate(poll_interval=0.01)
    order = []

    await gate.begin_transfer()

    async def fetcher():
        async with gate.background_fetch():
            order.append("fetch")

    task = asyncio.create_task(fetcher())
    await asyncio.sleep(0.05)
    assert order == []  # still deferred
    assert gate.is_transferring()

    gate.end_transfer()
    await asyncio.wait_for(task, timeout=1)
    assert order == ["fetch"]


@pytest.mark.asyncio
async def test_begin_transfer_waits_for_inflight_fetch():
    gate = ProviderGate(poll_interval=0.01)
    order = []
    fetch_started = asyncio.Event()
    release_fetch = asyncio.Event()

    async def fetcher():
        async with gate.background_fetch():
            order.append("fetch:start")
            fetch_started.set()
            await release_fetch.wait()
            order.append("fetch:end")

    task = asyncio.create_task(fetcher())
    await fetch_started.wait()

    transfer_task = asyncio.create_task(gate.begin_transfer())
    await asyncio.sleep(0.05)
    assert not gate.is_transferring()  # handshake: fetch still in flight
    assert not transfer_task.done()

    release_fetch.set()
    await asyncio.wait_for(transfer_task, timeout=1)
    assert gate.is_transferring()
    assert order == ["fetch:start", "fetch:end"]
    await task
    gate.end_transfer()


@pytest.mark.asyncio
async def test_waiting_fetcher_gets_gap_between_transfers():
    """Refresh waiting during a transfer runs before the next transfer starts."""
    gate = ProviderGate(poll_interval=0.01)
    order = []

    await gate.begin_transfer()

    async def fetcher():
        async with gate.background_fetch():
            order.append("fetch")

    task = asyncio.create_task(fetcher())
    await asyncio.sleep(0.05)
    assert order == []

    gate.end_transfer()
    await gate.begin_transfer()  # must serve the waiting fetcher first
    assert order == ["fetch"]
    assert gate.is_transferring()
    gate.end_transfer()
    await task


@pytest.mark.asyncio
async def test_transfer_owner_nested_fetch_does_not_deadlock():
    gate = ProviderGate(poll_interval=0.01)
    await gate.begin_transfer()
    async with gate.background_fetch():
        pass  # metadata enrichment during a download
    gate.end_transfer()


@pytest.mark.asyncio
async def test_end_transfer_is_idempotent():
    gate = ProviderGate(poll_interval=0.01)
    gate.end_transfer()  # no claim held
    await gate.begin_transfer()
    gate.end_transfer()
    gate.end_transfer()
    assert not gate.is_transferring()
    assert not gate.has_background_fetches()


@pytest.mark.asyncio
async def test_transfer_never_overlaps_background_fetch():
    gate = ProviderGate(poll_interval=0.01)
    transfer_running = 0
    overlap = False

    async def fetcher():
        nonlocal overlap
        async with gate.background_fetch():
            if transfer_running:
                overlap = True
            await asyncio.sleep(0.02)

    async def transfer_holder():
        nonlocal transfer_running, overlap
        await gate.begin_transfer()
        transfer_running += 1
        if gate.has_background_fetches():
            overlap = True
        await asyncio.sleep(0.02)
        transfer_running -= 1
        gate.end_transfer()

    await asyncio.gather(*(fetcher() for _ in range(3)), transfer_holder())
    assert overlap is False


@pytest.mark.asyncio
async def test_cancelled_background_fetch_releases_waiter_slot():
    gate = ProviderGate(poll_interval=0.01)
    await gate.begin_transfer()

    async def fetcher():
        async with gate.background_fetch():
            pass

    task = asyncio.create_task(fetcher())
    await asyncio.sleep(0.05)
    assert gate.has_background_fetches()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not gate.has_background_fetches()
    gate.end_transfer()

    # Transfer can still be claimed afterwards — no leaked waiter.
    await asyncio.wait_for(gate.begin_transfer(), timeout=1)
    gate.end_transfer()


@pytest.mark.asyncio
async def test_maybe_background_fetch_noop_without_gate():
    async with maybe_background_fetch(None):
        pass
    gate = ProviderGate(poll_interval=0.01)
    await gate.begin_transfer()
    done = asyncio.Event()

    async def fetcher():
        async with maybe_background_fetch(gate):
            done.set()

    task = asyncio.create_task(fetcher())
    await asyncio.sleep(0.05)
    assert not done.is_set()
    gate.end_transfer()
    await asyncio.wait_for(task, timeout=1)
    assert done.is_set()


@pytest.mark.asyncio
async def test_on_wait_callback_fires_while_deferring():
    gate = ProviderGate(poll_interval=0.01)
    ticks = []

    await gate.begin_transfer()

    async def fetcher():
        async with gate.background_fetch(on_wait=ticks.append, wait_interval=0.03):
            pass

    task = asyncio.create_task(fetcher())
    await asyncio.sleep(0.15)
    gate.end_transfer()
    await asyncio.wait_for(task, timeout=1)
    assert ticks, "expected on_wait ticks while the fetch was deferred"
    assert all(t > 0 for t in ticks)


class _EpgResponse:
    status_code = 200
    content = b"<tv/>"


class _EpgClient:
    calls = 0

    def __init__(self, **_):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get(self, *_, **__):
        type(self).calls += 1
        return _EpgResponse()


def test_epg_fetch_defers_to_active_transfer(tmp_path, monkeypatch):
    from app.services import epg_service as epg_module
    from app.services.epg_service import EpgService

    class _Cfg:
        data_dir = str(tmp_path)

        def get_epg_cache_ttl(self):
            return 21600

    monkeypatch.setattr(epg_module.httpx, "AsyncClient", _EpgClient)
    _EpgClient.calls = 0

    gate = ProviderGate(poll_interval=0.01)
    epg = EpgService(_Cfg())
    epg.provider_gate = gate
    source = {"id": "src-1", "host": "http://provider.test", "username": "u", "password": "p"}

    async def exercise():
        await gate.begin_transfer()
        task = asyncio.create_task(epg._fetch_epg_from_source(source))
        await asyncio.sleep(0.05)
        assert _EpgClient.calls == 0  # parked while the download streams
        assert not task.done()
        gate.end_transfer()
        return await asyncio.wait_for(task, timeout=1)

    content = asyncio.run(exercise())
    assert content == b"<tv/>"
    assert _EpgClient.calls == 1


def test_monitor_check_defers_to_active_transfer():
    from app.services.monitor_service import MonitorService

    gate = ProviderGate(poll_interval=0.01)
    fetch_calls = []

    class _Xtream:
        async def fetch_series_episodes(self, source_id, series_id):
            fetch_calls.append(source_id)
            return [{"stream_id": "1", "season": "1", "episode_num": 1, "title": "Ep1"}]

    class _Cart:
        cart = []

        def build_download_filepath(self, item):
            return "/nonexistent/ep1.mp4"

        def save_cart(self):
            pass

    ms = MonitorService.__new__(MonitorService)
    ms._cfg = type("_Cfg", (), {"config": {"sources": []}})()
    ms.xtream_service = _Xtream()
    ms.cart_service = _Cart()
    ms.provider_gate = gate
    entry = {
        "id": "m-1",
        "series_name": "The Show",
        "series_id": "22",
        "source_id": "srcA",
        "monitor_sources": [],
        "known_episodes": [],
        "downloaded_episodes": [],
        "action": "notify",
        "scope": "new_only",
    }

    async def exercise():
        await gate.begin_transfer()
        task = asyncio.create_task(ms._check_single_monitored(entry))
        await asyncio.sleep(0.05)
        assert fetch_calls == []  # monitor check parked while the download streams
        assert not task.done()
        gate.end_transfer()
        return await asyncio.wait_for(task, timeout=1)

    new_eps = asyncio.run(exercise())
    assert fetch_calls == ["srcA"]
    assert new_eps and new_eps[0]["episode_num"] == 1

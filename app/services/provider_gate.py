"""Coordination between background provider fetches and active downloads.

Single-connection providers allow only one concurrent request per account, so
background refresh traffic (cache, EPG, monitor metadata) and the download
stream must never overlap on the same upstream. This module provides a small
claim-based gate:

* ``background_fetch()`` — claim used by short-lived metadata/API fetches.
  Registers as a waiter, holds a fetch slot while the HTTP call runs, and
  defers to any active (or pending) download transfer.
* ``begin_transfer()`` / ``end_transfer()`` — claim used by the download worker
  around each item. ``begin_transfer`` first yields the gap to fetchers already
  waiting (so refresh work slips in *between* download items), then waits for
  in-flight fetches only (a download start is delayed by at most one fetch),
  then holds the claim so later fetches defer to the stream.

Claims are re-entrant per task: a task that already holds the transfer may open
nested background fetches (e.g. the download worker enriching metadata) without
deadlocking on its own claim.

All state updates are synchronous (single-threaded event loop), so
``end_transfer`` is safe to call from ``finally`` blocks and task callbacks.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class ProviderGate:
    """Serializes upstream access between background fetches and downloads."""

    def __init__(self, poll_interval: float = 0.1, max_serve_wait: float = 60.0):
        self._poll_interval = poll_interval
        self._max_serve_wait = max_serve_wait
        self._fetch_count = 0
        self._fetch_waiters = 0
        self._transfer_active = False
        self._transfer_pending = False
        self._transfer_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def is_transferring(self) -> bool:
        """True while a download transfer claim is held."""
        return self._transfer_active

    def has_background_fetches(self) -> bool:
        """True while a background fetch is in flight or waiting to start."""
        return self._fetch_count > 0 or self._fetch_waiters > 0

    def _holds_transfer(self) -> bool:
        return self._transfer_task is not None and asyncio.current_task() is self._transfer_task

    # ------------------------------------------------------------------
    # Background fetch claim
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def background_fetch(
        self,
        on_wait: Callable[[float], None] | None = None,
        wait_interval: float = 30.0,
    ) -> AsyncIterator[None]:
        """Hold a background fetch slot, deferring to any active transfer.

        If the calling task already owns the transfer claim the fetch is let
        through immediately: it belongs to the download itself (e.g. metadata
        enrichment) and cannot deadlock against its own claim.

        *on_wait* is called every *wait_interval* seconds with the elapsed wait
        time while the fetch is deferred (used for progress heartbeats).
        """
        if self._holds_transfer():
            yield
            return

        self._fetch_waiters += 1
        acquired = False
        try:
            loop = asyncio.get_running_loop()
            started = loop.time()
            last_tick = started
            while self._transfer_active or self._transfer_pending:
                await asyncio.sleep(self._poll_interval)
                if on_wait is not None:
                    now = loop.time()
                    if now - last_tick >= wait_interval:
                        last_tick = now
                        on_wait(now - started)
            self._fetch_count += 1
            acquired = True
        finally:
            self._fetch_waiters -= 1

        try:
            yield
        finally:
            if acquired:
                self._fetch_count -= 1

    # ------------------------------------------------------------------
    # Transfer claim
    # ------------------------------------------------------------------

    async def _serve_waiters(self) -> None:
        """Yield the current gap to fetchers already waiting for one.

        Only fetchers that registered before this call are served — new
        arrivals acquire synchronously when the gap is open anyway. Bounded by
        *max_serve_wait* so a pathological fetch stream cannot block a download
        forever.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._max_serve_wait
        while self._fetch_waiters > 0 and not self._transfer_active and not self._transfer_pending:
            if loop.time() >= deadline:
                logger.warning("Provider gate: gave up serving background fetchers before transfer")
                return
            await asyncio.sleep(self._poll_interval)

    async def begin_transfer(self) -> None:
        """Claim the upstream for a download item.

        Fetchers already waiting for a gap are served first (refresh never
        starves across long queues), then only in-flight fetches are awaited
        (a download about to start waits at most one fetch). New fetchers block
        on the pending flag instead of racing the claim.
        """
        if self._holds_transfer():
            return
        while self._transfer_active:
            await asyncio.sleep(self._poll_interval)
        await self._serve_waiters()
        self._transfer_pending = True
        try:
            while self._fetch_count > 0:
                await asyncio.sleep(self._poll_interval)
            self._transfer_active = True
            self._transfer_task = asyncio.current_task()
            logger.debug("Provider gate: transfer claimed")
        finally:
            self._transfer_pending = False

    def end_transfer(self) -> None:
        """Release the download transfer claim (idempotent)."""
        if not self._transfer_active:
            return
        self._transfer_active = False
        self._transfer_task = None
        logger.debug("Provider gate: transfer released")


@asynccontextmanager
async def maybe_background_fetch(
    gate: ProviderGate | None,
    on_wait: Callable[[float], None] | None = None,
) -> AsyncIterator[None]:
    """``gate.background_fetch()`` when a gate is bound, otherwise a no-op."""
    if gate is None:
        yield
    else:
        async with gate.background_fetch(on_wait=on_wait):
            yield

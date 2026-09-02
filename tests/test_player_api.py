"""Player subprocess lifecycle tests."""

from __future__ import annotations

import asyncio

from app.routes import player_api


class _FakeProbeProcess:
    def __init__(self):
        self.returncode = None
        self.kill_calls = 0
        self.communicate_calls = 0

    async def communicate(self):
        self.communicate_calls += 1
        if self.communicate_calls == 1:
            raise TimeoutError
        self.returncode = -9
        return b"", b""

    async def wait(self):
        self.returncode = -9
        return self.returncode

    def kill(self):
        self.kill_calls += 1


def test_media_probe_reaps_timed_out_process(monkeypatch):
    process = _FakeProbeProcess()

    async def create_process(*args, **kwargs):
        return process

    monkeypatch.setattr(player_api.shutil, "which", lambda _: "/usr/bin/ffprobe")
    monkeypatch.setattr(player_api.asyncio, "create_subprocess_exec", create_process)

    result = asyncio.run(player_api._probe_media_info("http://provider.test/movie"))

    assert result == {"duration": None, "video_codec": None}
    assert process.kill_calls == 1
    assert process.returncode == -9
    assert process.communicate_calls == 2

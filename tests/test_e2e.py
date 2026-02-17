"""End-to-end browser tests using Playwright.

These tests start the real FastAPI server (with a temp data dir) in a
background thread, then use Playwright to exercise the UI.
"""
from __future__ import annotations

import json
import threading
import time

import pytest
import uvicorn
from playwright.sync_api import Page, expect


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def _server(tmp_path_factory):
    """Start uvicorn in a daemon thread with a temp data dir."""
    import os

    data_dir = str(tmp_path_factory.mktemp("e2e_data"))
    # Write a minimal config
    config = {
        "sources": [],
        "filters": {
            "live": {"groups": [], "channels": []},
            "vod": {"groups": [], "channels": []},
            "series": {"groups": [], "channels": []},
        },
        "content_types": {"live": True, "vod": True, "series": True},
        "options": {},
    }
    with open(os.path.join(data_dir, "config.json"), "w") as f:
        json.dump(config, f)

    os.environ["DATA_DIR"] = data_dir

    # Force reimport with our DATA_DIR
    from app.main import app  # noqa: E402

    host, port = "127.0.0.1", 5199
    cfg = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(cfg)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for server
    import httpx

    for _ in range(40):
        try:
            r = httpx.get(f"http://{host}:{port}/health", timeout=2)
            if r.status_code == 200:
                break
        except Exception:
            time.sleep(0.25)
    else:
        pytest.fail("Server did not start in time")

    yield f"http://{host}:{port}"

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture(scope="session")
def base_url(_server):
    return _server


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_index_page_loads(page: Page, base_url: str):
    """Main config page loads and has expected elements."""
    page.goto(base_url + "/")
    expect(page).to_have_title("XtreamFilter")


def test_browse_page_loads(page: Page, base_url: str):
    """Browse page renders."""
    page.goto(base_url + "/browse")
    expect(page.locator("body")).to_be_visible()


def test_cart_page_loads(page: Page, base_url: str):
    """Cart page renders."""
    page.goto(base_url + "/cart")
    expect(page.locator("body")).to_be_visible()


def test_monitor_page_loads(page: Page, base_url: str):
    """Monitor page renders."""
    page.goto(base_url + "/monitor")
    expect(page.locator("body")).to_be_visible()


def test_health_json(page: Page, base_url: str):
    """Health endpoint returns OK JSON."""
    page.goto(base_url + "/health")
    content = page.text_content("body")
    assert content is not None
    data = json.loads(content)
    assert data["status"] == "ok"


def test_api_version(page: Page, base_url: str):
    """Version endpoint returns current version."""
    page.goto(base_url + "/api/version")
    content = page.text_content("body")
    assert content is not None
    data = json.loads(content)
    assert "current" in data


def test_add_source_via_api(page: Page, base_url: str):
    """Add a source through the API and verify it appears."""
    page.goto(base_url + "/")
    # Use page.evaluate to send a POST request
    result = page.evaluate("""async () => {
        const resp = await fetch('/api/sources', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: 'E2E Source',
                host: 'http://example.com',
                username: 'testuser',
                password: 'testpass'
            })
        });
        return await resp.json();
    }""")
    assert result["status"] == "ok"
    assert result["source"]["name"] == "E2E Source"


def test_filter_add_and_list(page: Page, base_url: str):
    """Add a filter rule via API and verify it's listed."""
    page.goto(base_url + "/")
    result = page.evaluate("""async () => {
        await fetch('/api/filters/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                content_type: 'live',
                filter_type: 'groups',
                rule: { type: 'include', match: 'contains', value: 'sports', case_sensitive: false }
            })
        });
        const resp = await fetch('/api/filters');
        return await resp.json();
    }""")
    assert len(result["live"]["groups"]) >= 1


def test_cache_status_api(page: Page, base_url: str):
    """Cache status returns counts structure."""
    page.goto(base_url + "/")
    result = page.evaluate("""async () => {
        const resp = await fetch('/api/cache/status');
        return await resp.json();
    }""")
    assert "counts" in result
    assert "sources" in result


def test_playlist_endpoint(page: Page, base_url: str):
    """Playlist endpoint returns M3U content."""
    # Use page.request.get instead of page.goto since .m3u triggers download
    response = page.request.get(base_url + "/playlist.m3u")
    assert response.ok
    assert "#EXTM3U" in response.text()

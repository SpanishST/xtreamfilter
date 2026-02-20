"""Health and version check routes."""
from __future__ import annotations

import time

import httpx
from fastapi import APIRouter

router = APIRouter(tags=["health"])

APP_VERSION = "0.5.1"
GITHUB_REPO = "SpanishST/xtreamfilter"

_version_cache: dict = {"latest": None, "release_url": "", "checked_at": 0.0}


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/api/version")
async def check_version():
    now = time.time()
    if _version_cache["latest"] and now - _version_cache["checked_at"] < 3600:
        latest = _version_cache["latest"]
        release_url = _version_cache.get("release_url", "")
    else:
        latest = None
        release_url = ""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
                    headers={"Accept": "application/vnd.github.v3+json", "User-Agent": "XtreamFilter"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    latest = data.get("tag_name", "").lstrip("v")
                    release_url = data.get("html_url", "")
                    _version_cache["latest"] = latest
                    _version_cache["release_url"] = release_url
                    _version_cache["checked_at"] = now
        except Exception:
            pass

    update_available = False
    if latest:
        try:
            from packaging.version import Version
            update_available = Version(latest) > Version(APP_VERSION)
        except Exception:
            update_available = latest != APP_VERSION

    return {
        "current": APP_VERSION,
        "latest": latest,
        "update_available": update_available,
        "release_url": release_url,
    }

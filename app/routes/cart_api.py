"""Download cart API routes."""
from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_cart_service, get_config_service, get_xtream_service
from app.services.cart_service import CartService
from app.services.config_service import ConfigService
from app.services.xtream_service import XtreamService

router = APIRouter(tags=["cart"])


@router.get("/api/cart")
async def get_cart(cart: CartService = Depends(get_cart_service)):
    return {"items": cart._download_cart}


@router.get("/api/cart/active-source-downloads")
async def active_source_downloads(cart: CartService = Depends(get_cart_service)):
    """Return the number of active (downloading) items per source_id."""
    result: dict[str, int] = {}
    for item in cart._download_cart:
        if item.get("status") == "downloading":
            sid = item.get("source_id", "")
            if sid:
                result[sid] = result.get(sid, 0) + 1
    return result


@router.post("/api/cart")
async def add_to_cart(
    request: Request,
    cart: CartService = Depends(get_cart_service),
    xtream: XtreamService = Depends(get_xtream_service),
):
    data = await request.json()
    content_type = data.get("content_type", "vod")
    add_mode = data.get("add_mode", "episode")
    added_items: list[dict] = []

    if content_type == "series" and add_mode in ("series", "season"):
        series_id = data.get("series_id", data.get("stream_id", ""))
        source_id = data.get("source_id", "")
        series_name = data.get("series_name", data.get("name", ""))
        season_filter = data.get("season_num") if add_mode == "season" else None

        episodes = await xtream.fetch_series_episodes(source_id, series_id)
        if not episodes:
            return JSONResponse(status_code=400, content={"error": "Could not fetch series episodes"})

        for ep in episodes:
            if season_filter and str(ep["season"]) != str(season_filter):
                continue
            if any(
                i.get("source_id") == source_id
                and i.get("stream_id") == ep["stream_id"]
                and i.get("status") in ("queued", "downloading")
                for i in cart._download_cart
            ):
                continue
            item = {
                "id": str(uuid.uuid4()),
                "stream_id": ep["stream_id"],
                "source_id": source_id,
                "content_type": "series",
                "name": ep.get("title", "") or f"Episode {ep['episode_num']}",
                "series_name": ep.get("series_name", series_name),
                "season": ep["season"],
                "episode_num": ep.get("episode_num", 0),
                "episode_title": ep.get("title", ""),
                "icon": data.get("icon", ""),
                "group": data.get("group", ""),
                "container_extension": ep.get("container_extension", "mp4"),
                "added_at": datetime.now().isoformat(),
                "status": "queued",
                "progress": 0,
                "error": None,
                "file_path": None,
                "file_size": None,
            }
            cart._download_cart.append(item)
            added_items.append(item)
    else:
        source_id = data.get("source_id", "")
        stream_id = data.get("stream_id", "")
        if any(
            i.get("source_id") == source_id
            and i.get("stream_id") == stream_id
            and i.get("status") in ("queued", "downloading")
            for i in cart._download_cart
        ):
            return JSONResponse(status_code=409, content={"error": "Item already in cart"})
        item = {
            "id": str(uuid.uuid4()),
            "stream_id": stream_id,
            "source_id": source_id,
            "content_type": content_type,
            "name": data.get("name", ""),
            "series_name": data.get("series_name"),
            "season": data.get("season"),
            "episode_num": data.get("episode_num"),
            "episode_title": data.get("episode_title"),
            "icon": data.get("icon", ""),
            "group": data.get("group", ""),
            "container_extension": data.get("container_extension", "mp4"),
            "added_at": datetime.now().isoformat(),
            "status": "queued",
            "progress": 0,
            "error": None,
            "file_path": None,
            "file_size": None,
        }
        cart._download_cart.append(item)
        added_items.append(item)

    cart.save_cart()
    return {"status": "ok", "added": len(added_items), "items": added_items}


@router.delete("/api/cart/{item_id}")
async def remove_from_cart(item_id: str, cart: CartService = Depends(get_cart_service)):
    original_len = len(cart._download_cart)
    cart._download_cart[:] = [i for i in cart._download_cart if i.get("id") != item_id]
    if len(cart._download_cart) == original_len:
        return JSONResponse(status_code=404, content={"error": "Item not found"})
    cart.save_cart()
    return {"status": "ok"}


@router.post("/api/cart/{item_id}/retry")
async def retry_cart_item(item_id: str, cart: CartService = Depends(get_cart_service)):
    for item in cart._download_cart:
        if item.get("id") == item_id:
            if item.get("status") not in ("failed", "cancelled", "move_failed"):
                return JSONResponse(status_code=400, content={"error": "Item is not in a retryable state"})
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            item.pop("temp_path", None)
            cart.save_cart()
            return {"status": "ok", "message": f"Re-queued: {item.get('name', '')}"}
    return JSONResponse(status_code=404, content={"error": "Item not found"})


@router.post("/api/cart/{item_id}/move")
async def move_cart_item(item_id: str, cart: CartService = Depends(get_cart_service)):
    for item in cart._download_cart:
        if item.get("id") == item_id:
            if item.get("status") != "move_failed":
                return JSONResponse(status_code=400, content={"error": "Item is not in move_failed state"})
            temp_path = item.get("temp_path")
            if not temp_path or not os.path.exists(temp_path):
                return JSONResponse(status_code=400, content={"error": f"Temp file not found: {temp_path}"})
            file_path = cart.build_download_filepath(item)
            item["status"] = "downloading"
            move_ok = await cart._move_temp_to_destination(item, temp_path, file_path)
            if move_ok:
                await cart._send_download_file_notification(item)
                return {"status": "ok", "message": f"Moved successfully: {item.get('name', '')}"}
            return JSONResponse(status_code=500, content={"error": item.get("error", "Move failed")})
    return JSONResponse(status_code=404, content={"error": "Item not found"})


@router.post("/api/cart/retry-all")
async def retry_all_failed(cart: CartService = Depends(get_cart_service)):
    count = 0
    for item in cart._download_cart:
        if item.get("status") in ("failed", "cancelled", "move_failed"):
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            item.pop("temp_path", None)
            count += 1
    cart.save_cart()
    return {"status": "ok", "retried": count}


@router.post("/api/cart/clear")
async def clear_cart(request: Request, cart: CartService = Depends(get_cart_service)):
    data = await request.json()
    mode = data.get("mode", "completed")
    dl = cart._download_cart
    if mode == "all":
        dl[:] = [i for i in dl if i.get("status") == "downloading"]
    elif mode == "completed":
        dl[:] = [i for i in dl if i.get("status") != "completed"]
    elif mode == "failed":
        dl[:] = [i for i in dl if i.get("status") not in ("failed", "cancelled", "move_failed")]
    elif mode == "finished":
        dl[:] = [i for i in dl if i.get("status") not in ("completed", "failed", "cancelled", "move_failed")]
    cart.save_cart()
    return {"status": "ok", "remaining": len(dl)}


@router.post("/api/cart/start")
async def start_downloads(
    cart: CartService = Depends(get_cart_service),
    cfg: ConfigService = Depends(get_config_service),
):
    queued = [i for i in cart._download_cart if i.get("status") == "queued"]
    if not queued:
        return JSONResponse(status_code=400, content={"error": "No queued items to download"})
    if cart._download_task and not cart._download_task.done():
        return JSONResponse(status_code=409, content={"error": "Downloads already in progress"})
    download_path = cfg.download_path
    try:
        os.makedirs(download_path, exist_ok=True)
    except OSError as e:
        return JSONResponse(status_code=500, content={"error": f"Cannot create download directory: {e}"})
    cart._force_started = True
    cart._download_task = asyncio.create_task(cart.download_worker())
    return {"status": "ok", "message": f"Started downloading {len(queued)} items"}


@router.post("/api/cart/cancel")
async def cancel_download(cart: CartService = Depends(get_cart_service)):
    if cart._download_cancel_event:
        cart._download_cancel_event.set()
        return {"status": "ok", "message": "Download cancellation requested"}
    return JSONResponse(status_code=400, content={"error": "No active download"})


@router.get("/api/cart/status")
async def cart_status(cart: CartService = Depends(get_cart_service)):
    dl = cart._download_cart
    queued = len([i for i in dl if i.get("status") == "queued"])
    downloading = len([i for i in dl if i.get("status") == "downloading"])
    completed = len([i for i in dl if i.get("status") == "completed"])
    failed = len([i for i in dl if i.get("status") in ("failed", "cancelled", "move_failed")])

    current = None
    if cart._download_current_item:
        ci = cart._download_current_item
        current = {
            "name": ci.get("name", ""),
            "series_name": ci.get("series_name"),
            "season": ci.get("season"),
            "episode_num": ci.get("episode_num"),
            "progress": ci.get("progress", 0),
            "bytes_downloaded": cart._download_progress.get("bytes_downloaded", 0),
            "total_bytes": cart._download_progress.get("total_bytes", 0),
            "speed": cart._download_progress.get("speed", 0),
            "eta_speed": cart._download_progress.get("eta_speed", 0),
            "paused": cart._download_progress.get("paused", False),
            "pause_remaining": cart._download_progress.get("pause_remaining", 0),
        }

    is_running = cart._download_task is not None and not cart._download_task.done()
    schedule = cart.config_service.get_download_schedule()
    schedule_active = schedule.get("enabled", False)
    in_window = cart.is_in_download_window() if schedule_active else True
    return {
        "is_running": is_running,
        "queued": queued,
        "downloading": downloading,
        "completed": completed,
        "failed": failed,
        "total": len(dl),
        "current": current,
        "schedule_enabled": schedule_active,
        "in_download_window": in_window,
    }


@router.get("/api/cart/series-episodes/{source_id}/{series_id}")
async def get_series_episodes_api(
    source_id: str,
    series_id: str,
    xtream: XtreamService = Depends(get_xtream_service),
):
    episodes = await xtream.fetch_series_episodes(source_id, series_id)
    if not episodes:
        return JSONResponse(status_code=400, content={"error": "Could not fetch episodes"})
    seasons: dict[str, list] = {}
    for ep in episodes:
        s = ep["season"]
        seasons.setdefault(s, []).append(ep)
    sorted_seasons = {}
    for s in sorted(seasons.keys(), key=lambda x: int(x) if x.isdigit() else 0):
        sorted_seasons[s] = sorted(seasons[s], key=lambda e: int(e.get("episode_num", 0)))
    return {
        "series_name": episodes[0].get("series_name", "") if episodes else "",
        "seasons": sorted_seasons,
        "total_episodes": len(episodes),
    }

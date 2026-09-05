"""Download cart API routes."""
from __future__ import annotations

import asyncio
import json
import os

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_cart_service, get_config_service, get_log_service, get_xtream_service
from app.services.cart_service import CartService
from app.services.config_service import ConfigService
from app.services.log_service import LogService
from app.services.xtream_service import XtreamService

router = APIRouter(tags=["cart"])


@router.get("/api/cart")
async def get_cart(cart: CartService = Depends(get_cart_service)):
    return {"items": cart._download_cart}


@router.get("/api/download-history")
async def get_download_history(
    type: str = Query("", pattern="^(|vod|series)$"),
    source: str = Query(""),
    search: str = Query(""),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cart: CartService = Depends(get_cart_service),
    cfg: ConfigService = Depends(get_config_service),
):
    result = await asyncio.to_thread(
        cart.get_download_history,
        content_type=type,
        source_id=source,
        search=search.strip(),
        limit=limit,
        offset=offset,
    )
    source_names = {
        str(item.get("id")): item.get("name", str(item.get("id")))
        for item in cfg.config.get("sources", [])
        if item.get("id")
    }
    for item in result["items"]:
        item["source_name"] = source_names.get(item["source_id"], "Unknown")
    return result


@router.get("/api/download-history/item")
async def get_item_download_history(
    keys: str = Query(""),
    limit: int = Query(3, ge=1, le=3),
    cart: CartService = Depends(get_cart_service),
    cfg: ConfigService = Depends(get_config_service),
):
    """Return the newest history events for one browse card or group."""
    try:
        raw_keys = json.loads(keys) if keys else []
    except (TypeError, json.JSONDecodeError):
        return JSONResponse(status_code=400, content={"error": "Invalid history keys"})
    if not isinstance(raw_keys, list) or len(raw_keys) > 500:
        return JSONResponse(status_code=400, content={"error": "Invalid history keys"})

    browse_keys: list[tuple[str, str, str]] = []
    for item in raw_keys:
        if not isinstance(item, dict):
            return JSONResponse(status_code=400, content={"error": "Invalid history key"})
        source_id = str(item.get("source_id", ""))
        content_type = str(item.get("content_type", ""))
        stream_id = str(item.get("stream_id", ""))
        if not source_id or content_type not in ("vod", "series") or not stream_id:
            return JSONResponse(status_code=400, content={"error": "Invalid history key"})
        key = (source_id, content_type, stream_id)
        if key not in browse_keys:
            browse_keys.append(key)

    items = await asyncio.to_thread(cart.get_download_history_for_keys, browse_keys, limit)
    source_names = {
        str(item.get("id")): item.get("name", str(item.get("id")))
        for item in cfg.config.get("sources", [])
        if item.get("id")
    }
    for item in items:
        item["source_name"] = source_names.get(item["source_id"], "Unknown")
    return {"items": items}


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
):
    data = await request.json()
    result = await cart.add_to_cart(data)
    if result.get("error"):
        status_code = 409 if result["error"] == "Item already in cart" else 400
        return JSONResponse(status_code=status_code, content=result)
    return {"status": "ok", **result}


@router.post("/api/cart/batch")
async def add_to_cart_batch(
    request: Request,
    cart: CartService = Depends(get_cart_service),
):
    """Add selected Browse titles and series scopes in one cart operation."""
    data = await request.json()
    selections = data.get("selections") if isinstance(data, dict) else None
    if (
        not isinstance(selections, list)
        or not selections
        or len(selections) > 200
        or any(not isinstance(selection, dict) for selection in selections)
    ):
        return JSONResponse(status_code=400, content={"error": "Selections must contain 1 to 200 items"})
    result = await cart.add_to_cart_batch(selections)
    return {"status": "ok", **result}


@router.post("/api/cart/reorder")
async def reorder_cart(
    request: Request,
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    data = await request.json()
    item_ids = data.get("item_ids") if isinstance(data, dict) else None
    if not isinstance(item_ids, list) or any(not isinstance(item_id, str) for item_id in item_ids):
        return JSONResponse(status_code=400, content={"error": "item_ids must be a list of strings"})

    result = await cart.reorder_queued_items(item_ids)
    if result.get("error"):
        return JSONResponse(status_code=409, content=result)
    await log_service.log("cart", "info", "Reordered queued downloads", {"item_ids": item_ids})
    return result


@router.delete("/api/cart/{item_id}")
async def remove_from_cart(
    item_id: str,
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    removed_name = "Unknown"
    original_len = len(cart._download_cart)
    for i in cart._download_cart:
        if i.get("id") == item_id:
            removed_name = i.get("name", "Unknown")
            break
    cart._download_cart[:] = [i for i in cart._download_cart if i.get("id") != item_id]
    if len(cart._download_cart) == original_len:
        return JSONResponse(status_code=404, content={"error": "Item not found"})
    cart.save_cart()
    await log_service.log("cart", "info", f"Removed from cart: {removed_name}", {"item_id": item_id})
    return {"status": "ok"}


@router.post("/api/cart/{item_id}/retry")
async def retry_cart_item(
    item_id: str,
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    for item in cart._download_cart:
        if item.get("id") == item_id:
            if item.get("status") not in ("failed", "cancelled", "move_failed"):
                return JSONResponse(status_code=400, content={"error": "Item is not in a retryable state"})
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            item["retried_once"] = False
            item.pop("temp_path", None)
            cart.save_cart()
            await log_service.log("cart", "info", f"Manual retry: {item.get('name', '')}", {"item_id": item_id})
            return {"status": "ok", "message": f"Re-queued: {item.get('name', '')}"}
    return JSONResponse(status_code=404, content={"error": "Item not found"})


@router.post("/api/cart/{item_id}/move")
async def move_cart_item(
    item_id: str,
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
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
                await cart._finalize_completed_download(item)
                await log_service.log("cart", "info", f"Move retry succeeded: {item.get('name', '')}", {"item_id": item_id})
                return {"status": "ok", "message": f"Moved successfully: {item.get('name', '')}"}
            await log_service.log("cart", "error", f"Move retry failed: {item.get('name', '')}", {"item_id": item_id, "error": item.get("error", "")})
            return JSONResponse(status_code=500, content={"error": item.get("error", "Move failed")})
    return JSONResponse(status_code=404, content={"error": "Item not found"})


@router.post("/api/cart/retry-all")
async def retry_all_failed(
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    count = 0
    for item in cart._download_cart:
        if item.get("status") in ("failed", "cancelled", "move_failed"):
            item["status"] = "queued"
            item["progress"] = 0
            item["error"] = None
            item["file_path"] = None
            item["file_size"] = None
            item["retried_once"] = False
            item.pop("temp_path", None)
            count += 1
    cart.save_cart()
    await log_service.log("cart", "info", f"Retry all: {count} item(s) re-queued", {"count": count})
    return {"status": "ok", "retried": count}


@router.post("/api/cart/clear")
async def clear_cart(
    request: Request,
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
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
    await log_service.log("cart", "info", f"Cart cleared (mode={mode})", {"mode": mode, "remaining": len(dl)})
    return {"status": "ok", "remaining": len(dl)}


@router.post("/api/cart/start")
async def start_downloads(
    cart: CartService = Depends(get_cart_service),
    cfg: ConfigService = Depends(get_config_service),
    log_service: LogService = Depends(get_log_service),
):
    queued = [i for i in cart._download_cart if i.get("status") == "queued"]
    if not queued:
        return JSONResponse(status_code=400, content={"error": "No queued items to download"})
    if cart.queue_paused:
        return JSONResponse(status_code=409, content={"error": "Download queue is paused; resume it first"})
    if cart._download_task and not cart._download_task.done():
        return JSONResponse(status_code=409, content={"error": "Downloads already in progress"})
    download_path = cfg.download_path
    try:
        os.makedirs(download_path, exist_ok=True)
    except OSError as e:
        return JSONResponse(status_code=500, content={"error": f"Cannot create download directory: {e}"})
    cart._force_started = True
    cart._try_start_worker()
    await log_service.log("cart", "info", f"Download started manually: {len(queued)} item(s)", {"count": len(queued)})
    return {"status": "ok", "message": f"Started downloading {len(queued)} items"}


@router.post("/api/cart/cancel")
async def cancel_download(
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    if cart.cancel_download():
        await log_service.log("cart", "warning", "Download cancellation requested")
        return {"status": "ok", "message": "Download cancellation requested"}
    return JSONResponse(status_code=400, content={"error": "No active download"})


@router.post("/api/cart/pause")
async def pause_downloads(
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    has_work = any(item.get("status") in ("queued", "downloading") for item in cart._download_cart)
    if not has_work:
        return JSONResponse(status_code=400, content={"error": "No queued or active downloads"})
    changed = cart.pause_downloads()
    if changed:
        await log_service.log("cart", "warning", "Download queue paused")
    return {"status": "ok", "message": "Download queue paused"}


@router.post("/api/cart/resume")
async def resume_downloads(
    cart: CartService = Depends(get_cart_service),
    log_service: LogService = Depends(get_log_service),
):
    changed = cart.resume_downloads()
    if changed:
        await log_service.log("cart", "info", "Download queue resumed")
    return {"status": "ok", "message": "Download queue resumed"}


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
        "queue_paused": cart.queue_paused,
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

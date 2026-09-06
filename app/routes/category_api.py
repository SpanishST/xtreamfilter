"""Category API routes."""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_category_service
from app.services.category_service import CategoryService

router = APIRouter(prefix="/api/categories", tags=["categories"])


def _normalize_autodownload(value: dict | None, current: dict | None = None) -> dict:
    result = {
        "enabled": False,
        "movies_enabled": True,
        "series_enabled": True,
        "source_priority": [],
        "series_seasons": [],
        "baseline_initialized": False,
        "last_run": None,
        "last_queued": 0,
        "last_error": None,
    }
    if current:
        result.update(current)
    if isinstance(value, dict):
        for key in ("enabled", "movies_enabled", "series_enabled"):
            if key in value:
                result[key] = bool(value[key])
        for key in ("source_priority", "series_seasons"):
            if key in value and isinstance(value[key], list):
                result[key] = [str(item) for item in value[key] if item]
    return result


@router.get("")
async def get_categories(cat_svc: CategoryService = Depends(get_category_service)):
    data = cat_svc.load_categories()
    return {"categories": data.get("categories", [])}


@router.get("/summary")
async def get_categories_summary(cat_svc: CategoryService = Depends(get_category_service)):
    """Lightweight endpoint returning only id/name/icon/mode/item_count.

    Used by the browse nav pills — avoids loading full item arrays.
    """
    return {"categories": cat_svc.load_categories_summary()}


@router.post("")
async def create_category(request: Request, cat_svc: CategoryService = Depends(get_category_service)):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    name = body.get("name", "").strip()
    if not name:
        return JSONResponse({"error": "Category name is required"}, status_code=400)

    mode = body.get("mode", "manual")
    if mode not in ("manual", "automatic"):
        return JSONResponse({"error": "Invalid mode"}, status_code=400)

    content_types = [t for t in body.get("content_types", ["live", "vod", "series"]) if t in {"live", "vod", "series"}]
    if not content_types:
        content_types = ["live", "vod", "series"]

    new_category = {
        "id": str(uuid.uuid4()),
        "name": name,
        "icon": body.get("icon", "📁"),
        "mode": mode,
        "content_types": content_types,
        "items": [],
        "patterns": body.get("patterns", []),
        "pattern_logic": body.get("pattern_logic", "or"),
        "use_source_filters": body.get("use_source_filters", False),
        "notify_telegram": body.get("notify_telegram", False),
        "recently_added_days": body.get("recently_added_days", 0),
        "cached_items": [],
        "last_refresh": None,
        "autodownload": _normalize_autodownload(body.get("autodownload")),
    }

    data = cat_svc.load_categories()
    data["categories"].append(new_category)
    cat_svc.save_categories(data)

    if mode == "automatic":
        cat_svc.refresh_single_pattern_category(new_category["id"])
        data = cat_svc.load_categories()
        for cat in data["categories"]:
            if cat["id"] == new_category["id"]:
                new_category = cat
                break
    if cat_svc.autodownload_service is not None:
        await cat_svc.autodownload_service.reconcile_category(new_category["id"])

    return {"status": "created", "category": new_category}


@router.get("/{category_id}")
async def get_category(category_id: str, cat_svc: CategoryService = Depends(get_category_service)):
    cat = cat_svc.get_category_by_id(category_id)
    if cat is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    return {"category": cat}


@router.post("/{category_id}/autodownload/backfill")
async def backfill_category_autodownload(
    category_id: str,
    cat_svc: CategoryService = Depends(get_category_service),
):
    if cat_svc.autodownload_service is None:
        return JSONResponse({"error": "Autodownload service is not available"}, status_code=503)
    result = await cat_svc.autodownload_service.backfill_category(category_id)
    if result.get("error"):
        status_code = 404 if result["error"] == "Category not found" else 400
        return JSONResponse(result, status_code=status_code)
    return {"status": "ok", **result}


@router.put("/{category_id}")
async def update_category(category_id: str, request: Request, cat_svc: CategoryService = Depends(get_category_service)):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    data = cat_svc.load_categories()
    category = None
    cat_index = -1
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break

    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)

    if "name" in body:
        category["name"] = body["name"].strip()
    if "icon" in body:
        category["icon"] = body["icon"]
    if "mode" in body and body["mode"] in ("manual", "automatic"):
        old_mode = category.get("mode")
        category["mode"] = body["mode"]
        if old_mode != body["mode"]:
            if body["mode"] == "manual":
                category["cached_items"] = []
            else:
                category["items"] = []
    if "content_types" in body:
        category["content_types"] = [t for t in body["content_types"] if t in {"live", "vod", "series"}]
    for k in ("patterns", "pattern_logic", "use_source_filters", "notify_telegram", "recently_added_days"):
        if k in body:
            category[k] = body[k]
    if "autodownload" in body:
        category["autodownload"] = _normalize_autodownload(
            body.get("autodownload"), category.get("autodownload")
        )

    data["categories"][cat_index] = category
    cat_svc.save_categories(data)

    if category.get("mode") == "automatic":
        cat_svc.refresh_single_pattern_category(category_id)
        data = cat_svc.load_categories()
        category = data["categories"][cat_index]

    if cat_svc.autodownload_service is not None:
        await cat_svc.autodownload_service.reconcile_category(category_id)

    return {"status": "updated", "category": category}


@router.delete("/{category_id}")
async def delete_category(category_id: str, cat_svc: CategoryService = Depends(get_category_service)):
    data = cat_svc.load_categories()
    original_count = len(data.get("categories", []))
    data["categories"] = [c for c in data.get("categories", []) if c.get("id") != category_id]
    if len(data["categories"]) < original_count:
        cat_svc.save_categories(data)
        return {"status": "deleted"}
    return JSONResponse({"error": "Category not found"}, status_code=404)


@router.post("/{category_id}/items")
async def add_item_to_category(category_id: str, request: Request, cat_svc: CategoryService = Depends(get_category_service)):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    content_type = body.get("content_type")
    item_id = str(body.get("id", ""))
    source_id = body.get("source_id", "")
    if content_type not in ("vod", "series", "live"):
        return JSONResponse({"error": "Invalid content type"}, status_code=400)
    if not item_id:
        return JSONResponse({"error": "Missing item id"}, status_code=400)

    data = cat_svc.load_categories()
    category = None
    cat_index = -1
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break
    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    if category.get("mode") != "manual":
        return JSONResponse({"error": "Cannot add items to automatic categories"}, status_code=400)
    if content_type not in category.get("content_types", []):
        return JSONResponse({"error": f"Category does not accept {content_type} content"}, status_code=400)

    for item in category.get("items", []):
        if item.get("id") == item_id and item.get("source_id") == source_id and item.get("content_type") == content_type:
            return {"status": "already_exists", "message": "Item already in category"}

    category["items"].append({
        "id": item_id,
        "source_id": source_id,
        "content_type": content_type,
        "added_at": datetime.now().isoformat(),
    })
    data["categories"][cat_index] = category
    cat_svc.save_categories(data)
    if cat_svc.autodownload_service is not None:
        await cat_svc.autodownload_service.reconcile_category(category_id)
    return {"status": "added", "message": f"Added to {category['name']}"}


@router.delete("/{category_id}/items/{content_type}/{source_id}/{item_id}")
async def remove_item_from_category(
    category_id: str, content_type: str, source_id: str, item_id: str,
    cat_svc: CategoryService = Depends(get_category_service),
):
    if content_type not in ("vod", "series", "live"):
        return JSONResponse({"error": "Invalid content type"}, status_code=400)

    data = cat_svc.load_categories()
    category = None
    cat_index = -1
    for i, cat in enumerate(data.get("categories", [])):
        if cat.get("id") == category_id:
            category = cat
            cat_index = i
            break
    if category is None:
        return JSONResponse({"error": "Category not found"}, status_code=404)
    if category.get("mode") != "manual":
        return JSONResponse({"error": "Cannot remove items from automatic categories"}, status_code=400)

    original_count = len(category.get("items", []))
    category["items"] = [
        item for item in category.get("items", [])
        if not (item.get("id") == item_id and item.get("source_id") == source_id and item.get("content_type") == content_type)
    ]
    if len(category["items"]) < original_count:
        data["categories"][cat_index] = category
        cat_svc.save_categories(data)
        return {"status": "removed", "message": f"Removed from {category['name']}"}
    return {"status": "not_found", "message": "Item not found in category"}


@router.post("/refresh")
async def refresh_categories(cat_svc: CategoryService = Depends(get_category_service)):
    await cat_svc.refresh_pattern_categories_async()
    return {"status": "refreshed"}

"""Source management API routes."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_config_service
from app.services.config_service import ConfigService

router = APIRouter(prefix="/api/sources", tags=["sources"])

_DEFAULT_FILTERS = {
    "live": {"groups": [], "channels": []},
    "vod": {"groups": [], "channels": []},
    "series": {"groups": [], "channels": []},
}


@router.get("")
async def get_sources(cfg: ConfigService = Depends(get_config_service)):
    return {"sources": cfg.config.get("sources", [])}


@router.post("")
async def add_source(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    new_source = {
        "id": str(uuid.uuid4())[:8],
        "name": data.get("name", "New Source"),
        "host": data.get("host", ""),
        "username": data.get("username", ""),
        "password": data.get("password", ""),
        "enabled": data.get("enabled", True),
        "prefix": data.get("prefix", ""),
        "filters": data.get("filters", dict(_DEFAULT_FILTERS)),
    }
    config.setdefault("sources", []).append(new_source)
    cfg.save()
    return {"status": "ok", "source": new_source, "sources": config["sources"]}


@router.get("/{source_id}")
async def get_source(source_id: str, cfg: ConfigService = Depends(get_config_service)):
    for source in cfg.config.get("sources", []):
        if source.get("id") == source_id:
            return {"source": source}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.put("/{source_id}")
async def update_source(source_id: str, request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            for key in ("name", "host", "username", "password", "enabled", "route", "filters"):
                if key in data:
                    source[key] = data[key]
            if "prefix" in data:
                source["prefix"] = data["prefix"] if data["prefix"] else ""
            config["sources"][i] = source
            cfg.save()
            return {"status": "ok", "source": source, "sources": config["sources"]}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.delete("/{source_id}")
async def delete_source(source_id: str, cfg: ConfigService = Depends(get_config_service)):
    config = cfg.config
    sources = config.get("sources", [])
    config["sources"] = [s for s in sources if s.get("id") != source_id]
    if len(config["sources"]) < len(sources):
        cfg.save()
        return {"status": "ok", "sources": config["sources"]}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.get("/{source_id}/filters")
async def get_source_filters(source_id: str, cfg: ConfigService = Depends(get_config_service)):
    for source in cfg.config.get("sources", []):
        if source.get("id") == source_id:
            return {"filters": source.get("filters", {})}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.post("/{source_id}/filters")
async def save_source_filters(source_id: str, request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            source["filters"] = data
            config["sources"][i] = source
            cfg.save()
            return {"status": "ok", "filters": source["filters"]}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.post("/{source_id}/filters/add")
async def add_source_filter(source_id: str, request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    category = data.get("category", "live")
    target = data.get("target", "groups")
    rule = {
        "type": data.get("type", "exclude"),
        "match": data.get("match", "contains"),
        "value": data.get("value", ""),
        "case_sensitive": data.get("case_sensitive", False),
    }
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            source.setdefault("filters", dict(_DEFAULT_FILTERS))
            source["filters"].setdefault(category, {"groups": [], "channels": []})
            source["filters"][category].setdefault(target, [])
            source["filters"][category][target].append(rule)
            config["sources"][i] = source
            cfg.save()
            return {"status": "ok", "filters": source["filters"]}
    return JSONResponse({"error": "Source not found"}, status_code=404)


@router.post("/{source_id}/filters/delete")
async def delete_source_filter(source_id: str, request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    category = data.get("category", "live")
    target = data.get("target", "groups")
    index = data.get("index", -1)
    for i, source in enumerate(config.get("sources", [])):
        if source.get("id") == source_id:
            if category in source.get("filters", {}) and target in source["filters"][category]:
                filter_list = source["filters"][category][target]
                if 0 <= index < len(filter_list):
                    filter_list.pop(index)
                    config["sources"][i] = source
                    cfg.save()
            return {"status": "ok", "filters": source.get("filters", {})}
    return JSONResponse({"error": "Source not found"}, status_code=404)

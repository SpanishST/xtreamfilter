"""Filter API routes — global filter rule CRUD."""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends, Request

from app.dependencies import get_config_service
from app.services.config_service import ConfigService

router = APIRouter(prefix="/api/filters", tags=["filters"])


@router.get("")
async def get_filters(cfg: ConfigService = Depends(get_config_service)):
    return cfg.config["filters"]


@router.post("")
async def save_filters(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config

    if "live" in data or "vod" in data or "series" in data:
        for cat in ["live", "vod", "series"]:
            if cat in data:
                config["filters"][cat] = data[cat]
    else:
        for cat in ["live", "vod", "series"]:
            config["filters"][cat]["groups"] = data.get("groups", [])
            config["filters"][cat]["channels"] = data.get("channels", [])

    cfg.save()
    cache_file = os.path.join(cfg.data_dir, "api_cache.json")
    if os.path.exists(cache_file):
        os.remove(cache_file)
    return {"status": "ok"}


@router.post("/add")
async def add_filter(request: Request, cfg: ConfigService = Depends(get_config_service)):
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
    if category in config["filters"] and target in config["filters"][category]:
        config["filters"][category][target].append(rule)
        cfg.save()
    return {"status": "ok", "filters": config["filters"]}


@router.post("/delete")
async def delete_filter(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    config = cfg.config
    category = data.get("category", "live")
    target = data.get("target", "groups")
    index = data.get("index", -1)
    if category in config["filters"] and target in config["filters"][category]:
        filter_list = config["filters"][category][target]
        if 0 <= index < len(filter_list):
            filter_list.pop(index)
            cfg.save()
    return {"status": "ok", "filters": config["filters"]}

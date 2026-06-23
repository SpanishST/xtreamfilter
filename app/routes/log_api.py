"""Activity log API routes."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.dependencies import get_log_service
from app.services.log_service import LogService

router = APIRouter(tags=["logs"])


@router.get("/api/logs")
async def get_logs(
    category: str | None = Query(None, pattern="^(cache|cart|monitor)$"),
    level: str | None = Query(None, pattern="^(info|warning|error)$"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    log_svc: LogService = Depends(get_log_service),
):
    logs = await log_svc.get_logs(category=category, level=level, limit=limit, offset=offset)
    for entry in logs:
        if entry.get("details"):
            try:
                import json
                entry["details"] = json.loads(entry["details"])
            except (json.JSONDecodeError, TypeError):
                pass
    return logs


@router.get("/api/logs/counts")
async def get_log_counts(log_svc: LogService = Depends(get_log_service)):
    return await log_svc.get_log_counts()


@router.delete("/api/logs")
async def clear_logs(
    category: str | None = Query(None, pattern="^(cache|cart|monitor)$"),
    log_svc: LogService = Depends(get_log_service),
):
    deleted = await log_svc.clear_logs(category=category)
    return {"status": "ok", "deleted": deleted}

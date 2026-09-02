"""Database maintenance API routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.dependencies import get_database_maintenance_service
from app.services.database_maintenance_service import DatabaseMaintenanceService

router = APIRouter(tags=["database"])


@router.post("/api/database/cleanup")
async def start_database_cleanup(
    maintenance: DatabaseMaintenanceService = Depends(get_database_maintenance_service),  # noqa: B008
):
    result = maintenance.start()
    if not result.get("started"):
        return JSONResponse(status_code=409, content=result)
    return result


@router.get("/api/database/cleanup/status")
async def database_cleanup_status(
    maintenance: DatabaseMaintenanceService = Depends(get_database_maintenance_service),  # noqa: B008
):
    return maintenance.get_status()


@router.post("/api/database/cleanup/cancel")
async def cancel_database_cleanup(
    maintenance: DatabaseMaintenanceService = Depends(get_database_maintenance_service),  # noqa: B008
):
    return await maintenance.request_stop()

"""EPG / XMLTV routes."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends
from fastapi.responses import Response

from app.dependencies import get_epg_service
from app.services.epg_service import EpgService

router = APIRouter(tags=["epg"])


@router.get("/xmltv.php")
@router.get("/merged/xmltv.php")
async def get_merged_xmltv(epg: EpgService = Depends(get_epg_service)):
    data = epg.get_epg_data()
    if data:
        return Response(content=data, media_type="application/xml", headers={"Content-Type": "application/xml; charset=utf-8"})

    if epg._epg_cache.get("refresh_in_progress"):
        return Response(
            content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- EPG refresh in progress --></tv>',
            media_type="application/xml",
        )

    await epg.refresh_epg_cache()
    data = epg.get_epg_data()
    if data:
        return Response(content=data, media_type="application/xml", headers={"Content-Type": "application/xml; charset=utf-8"})

    return Response(
        content='<?xml version="1.0" encoding="UTF-8"?><tv generator-info-name="XtreamFilter"><!-- No EPG data available --></tv>',
        media_type="application/xml",
    )


@router.post("/api/epg/refresh")
async def trigger_epg_refresh(epg: EpgService = Depends(get_epg_service)):
    if epg._epg_cache.get("refresh_in_progress"):
        return {"status": "already_in_progress", "message": "EPG refresh is already in progress"}
    asyncio.create_task(epg.refresh_epg_cache())
    return {"status": "started", "message": "EPG refresh started"}


@router.get("/api/epg/status")
async def get_epg_status_route(epg: EpgService = Depends(get_epg_service)):
    return epg.get_epg_status()

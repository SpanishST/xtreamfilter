"""UI page routes — HTML template rendering."""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_config_service
from app.services.config_service import ConfigService

router = APIRouter(tags=["ui"])
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request, cfg: ConfigService = Depends(get_config_service)):
    config = cfg.config
    return templates.TemplateResponse("index.html", {"request": request, "config": config})


@router.post("/save")
async def save(
    request: Request,
    host: str = Form(""),
    username: str = Form(""),
    password: str = Form(""),
    cfg: ConfigService = Depends(get_config_service),
):
    config = cfg.config
    config["xtream"]["host"] = host.strip()
    config["xtream"]["username"] = username.strip()
    config["xtream"]["password"] = password.strip()

    form = await request.form()
    config["content_types"] = {
        "live": "content_live" in form,
        "vod": "content_vod" in form,
        "series": "content_series" in form,
    }
    cfg.save()

    cache_file = os.path.join(cfg.data_dir, "api_cache.json")
    if os.path.exists(cache_file):
        os.remove(cache_file)

    return RedirectResponse(url="/", status_code=303)


@router.get("/browse", response_class=HTMLResponse)
async def browse_page(request: Request):
    return templates.TemplateResponse("browse.html", {"request": request})


@router.get("/cart", response_class=HTMLResponse)
async def cart_page(request: Request):
    return templates.TemplateResponse("cart.html", {"request": request})


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_page(request: Request):
    return templates.TemplateResponse("monitor.html", {"request": request})

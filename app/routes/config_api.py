"""Configuration and options API routes."""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import get_config_service, get_http_client, get_notification_service
from app.models.xtream import PLAYER_PROFILES
from app.services.config_service import ConfigService
from app.services.http_client import HttpClientService
from app.services.notification_service import NotificationService

router = APIRouter(tags=["config"])


# ---- Generic options ----

@router.get("/api/options")
async def get_options(cfg: ConfigService = Depends(get_config_service)):
    return cfg.config.get("options", {})


@router.post("/api/options")
async def update_options(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    for key, value in data.items():
        cfg.config["options"][key] = value
    cfg.save()
    return {"status": "ok", "options": cfg.config["options"]}


# ---- Proxy streaming ----

@router.get("/api/options/proxy")
async def get_proxy_status(cfg: ConfigService = Depends(get_config_service)):
    return {"proxy_enabled": cfg.proxy_enabled}


@router.post("/api/options/proxy")
async def set_proxy_status(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    cfg.config["options"]["proxy_streams"] = data.get("enabled", True)
    cfg.save()
    return {"status": "ok", "proxy_enabled": cfg.config["options"]["proxy_streams"]}


# ---- Refresh interval ----

@router.get("/api/options/refresh_interval")
async def get_refresh_interval_api(cfg: ConfigService = Depends(get_config_service)):
    return {"refresh_interval": cfg.refresh_interval}


@router.post("/api/options/refresh_interval")
async def set_refresh_interval_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    interval = max(int(data.get("refresh_interval", 3600)), 300)
    cfg.config["options"]["refresh_interval"] = interval
    cfg.save()
    return {"status": "ok", "refresh_interval": interval}


# ---- Telegram ----

@router.get("/api/config/telegram")
async def get_telegram_config(cfg: ConfigService = Depends(get_config_service)):
    telegram_config = cfg.config.get("options", {}).get("telegram", {
        "enabled": False, "bot_token": "", "chat_id": ""
    })
    masked = telegram_config.copy()
    if masked.get("bot_token"):
        token = masked["bot_token"]
        masked["bot_token_masked"] = f"***{token[-4:]}" if len(token) > 4 else "***"
    return masked


@router.post("/api/config/telegram")
async def update_telegram_config(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    if "telegram" not in cfg.config["options"]:
        cfg.config["options"]["telegram"] = {"enabled": False, "bot_token": "", "chat_id": ""}
    tg = cfg.config["options"]["telegram"]
    if "enabled" in data:
        tg["enabled"] = data["enabled"]
    if "bot_token" in data:
        tg["bot_token"] = data["bot_token"]
    if "chat_id" in data:
        tg["chat_id"] = data["chat_id"]
    cfg.save()
    return {"status": "ok", "message": "Telegram settings updated"}


@router.post("/api/config/telegram/test")
async def test_telegram_notification(
    cfg: ConfigService = Depends(get_config_service),
    http: HttpClientService = Depends(get_http_client),
):
    telegram_config = cfg.config.get("options", {}).get("telegram", {})
    bot_token = telegram_config.get("bot_token", "")
    chat_id = telegram_config.get("chat_id", "")
    if not bot_token or not chat_id:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Bot token and chat ID are required"})
    try:
        client = await http.get_client()
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": "🔔 <b>Test Notification</b>\n\nYour Telegram integration is working correctly!",
            "parse_mode": "HTML",
        }
        response = await client.post(url, json=payload)
        result = response.json()
        if response.status_code == 200 and result.get("ok"):
            return {"status": "ok", "message": "Test notification sent successfully"}
        return JSONResponse(status_code=400, content={"status": "error", "message": f"Telegram API error: {result.get('description', 'Unknown error')}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Failed to send test: {e}"})


@router.post("/api/config/telegram/test-diff")
async def test_telegram_diff_notification(
    request: Request,
    notif: NotificationService = Depends(get_notification_service),
):
    try:
        body = await request.json()
    except Exception:
        body = {}

    single = body.get("single", False)
    album = body.get("album", False)

    if single:
        sample_items = [{"name": "Avatar: The Way of Water (2022) 4K HDR", "cover": "https://image.tmdb.org/t/p/w500/t6HIqrRAclMCA60NsSmeqe9RmNV.jpg"}]
        msg = "Test notification sent with 1 item (photo)"
    elif album:
        sample_items = [
            {"name": "Inception (2010)", "cover": "https://upload.wikimedia.org/wikipedia/en/2/2e/Inception_%282010%29_theatrical_poster.jpg"},
            {"name": "The Dark Knight (2008)", "cover": "https://upload.wikimedia.org/wikipedia/en/1/1c/The_Dark_Knight_%282008_film%29.jpg"},
            {"name": "Interstellar (2014)", "cover": "https://upload.wikimedia.org/wikipedia/en/b/bc/Interstellar_film_poster.jpg"},
        ]
        msg = "Test notification sent with 3 items (album with covers)"
    else:
        sample_items = [
            {"name": "Test Movie 1 - 4K HDR", "cover": ""},
            {"name": "Test Movie 2 - Dolby Vision", "cover": ""},
            {"name": "Test Series S01E01", "cover": ""},
        ]
        msg = "Test notification sent with 3 items (text list)"

    try:
        await notif.send_telegram_notification("🧪 Test Category", sample_items)
        return {"status": "ok", "message": msg}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Failed to send: {e}"})


# ---- Download paths ----

@router.get("/api/options/download_path")
async def get_download_path_api(cfg: ConfigService = Depends(get_config_service)):
    return {"download_path": cfg.download_path}


@router.post("/api/options/download_path")
async def set_download_path_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    path = data.get("download_path", "/data/downloads").strip()
    if not path:
        return JSONResponse(status_code=400, content={"error": "Path cannot be empty"})
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid path: {e}"})
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    cfg.config["options"]["download_path"] = path
    cfg.save()
    return {"status": "ok", "download_path": path}


@router.get("/api/options/download_temp_path")
async def get_download_temp_path_api(cfg: ConfigService = Depends(get_config_service)):
    return {"download_temp_path": cfg.download_temp_path}


@router.post("/api/options/download_temp_path")
async def set_download_temp_path_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    path = data.get("download_temp_path", "/data/downloads/.tmp").strip()
    if not path:
        return JSONResponse(status_code=400, content={"error": "Path cannot be empty"})
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid path: {e}"})
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    cfg.config["options"]["download_temp_path"] = path
    cfg.save()
    return {"status": "ok", "download_temp_path": path}


@router.post("/api/options/test_path")
async def test_path_write(request: Request):
    data = await request.json()
    path = data.get("path", "").strip()
    if not path:
        return JSONResponse(status_code=400, content={"error": "Path cannot be empty"})
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        return JSONResponse(status_code=400, content={"error": f"Cannot create directory: {e}", "writable": False})
    if not os.path.isdir(path):
        return JSONResponse(status_code=400, content={"error": f"Path is not a directory: {path}", "writable": False})
    test_file = os.path.join(path, ".write_test_xtreamfilter")
    try:
        with open(test_file, "w") as f:
            f.write("write test")
        with open(test_file, "r") as f:
            content = f.read()
        if content != "write test":
            return JSONResponse(status_code=500, content={"error": "Write verification failed", "writable": False})
        os.remove(test_file)
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"error": f"Permission denied: {e}", "writable": False})
    except OSError as e:
        return JSONResponse(status_code=500, content={"error": f"Write test failed: {e}", "writable": False})
    try:
        stat = os.statvfs(path)
        free_gb = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
        msg = f"Writable, {free_gb:.1f} GB free"
    except Exception:
        msg = "Writable"
    return {"writable": True, "message": msg}


# ---- Download throttle ----

@router.get("/api/options/download_throttle")
async def get_download_throttle_api(cfg: ConfigService = Depends(get_config_service)):
    return cfg.download_throttle


@router.post("/api/options/download_throttle")
async def set_download_throttle_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    try:
        bw = max(0, int(data.get("bandwidth_limit", 0)))
        interval = max(0, int(data.get("pause_interval", 0)))
        duration = max(0, int(data.get("pause_duration", 0)))
        burst = max(0, int(data.get("burst_reconnect", 0)))
    except (ValueError, TypeError):
        return JSONResponse(status_code=400, content={"error": "Invalid numeric values"})
    profile = data.get("player_profile", "tivimate")
    if profile not in PLAYER_PROFILES:
        profile = "tivimate"
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    cfg.config["options"]["download_bandwidth_limit"] = bw
    cfg.config["options"]["download_pause_interval"] = interval
    cfg.config["options"]["download_pause_duration"] = duration
    cfg.config["options"]["download_player_profile"] = profile
    cfg.config["options"]["download_burst_reconnect"] = burst
    cfg.save()
    return {"status": "ok", "bandwidth_limit": bw, "pause_interval": interval, "pause_duration": duration, "player_profile": profile, "burst_reconnect": burst}


@router.get("/api/options/player_profiles")
async def get_player_profiles_api():
    return {k: {"name": v["name"], "user_agent": v["headers"]["User-Agent"]} for k, v in PLAYER_PROFILES.items()}


# ---- Download notifications ----

@router.get("/api/options/download_notifications")
async def get_download_notifications_api(cfg: ConfigService = Depends(get_config_service)):
    opts = cfg.config.get("options", {})
    tg = opts.get("telegram", {})
    return {
        "telegram_configured": bool(tg.get("enabled") and tg.get("bot_token") and tg.get("chat_id")),
        "notify_file": opts.get("download_notify_file", False),
        "notify_queue": opts.get("download_notify_queue", False),
    }


@router.post("/api/options/download_notifications")
async def set_download_notifications_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    cfg.config["options"]["download_notify_file"] = bool(data.get("notify_file", False))
    cfg.config["options"]["download_notify_queue"] = bool(data.get("notify_queue", False))
    cfg.save()
    return {
        "status": "ok",
        "notify_file": cfg.config["options"]["download_notify_file"],
        "notify_queue": cfg.config["options"]["download_notify_queue"],
    }


# ---- Download schedule ----

VALID_DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


@router.get("/api/options/download_schedule")
async def get_download_schedule_api(cfg: ConfigService = Depends(get_config_service)):
    return cfg.download_schedule


@router.post("/api/options/download_schedule")
async def set_download_schedule_api(request: Request, cfg: ConfigService = Depends(get_config_service)):
    data = await request.json()
    if "options" not in cfg.config:
        cfg.config["options"] = {}
    schedule = cfg.config["options"].get("download_schedule", {})
    if "enabled" in data:
        schedule["enabled"] = bool(data["enabled"])
    for day in VALID_DAYS:
        if day in data:
            day_data = data[day]
            if day not in schedule:
                schedule[day] = {"enabled": False, "start": "01:00", "end": "07:00"}
            if "enabled" in day_data:
                schedule[day]["enabled"] = bool(day_data["enabled"])
            if "start" in day_data:
                schedule[day]["start"] = str(day_data["start"]).strip()[:5]
            if "end" in day_data:
                schedule[day]["end"] = str(day_data["end"]).strip()[:5]
    cfg.config["options"]["download_schedule"] = schedule
    cfg.save()
    return {"status": "ok", "schedule": schedule}

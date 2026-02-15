"""FastAPI dependency injection — provides services via Depends()."""
from __future__ import annotations

from fastapi import Request

from app.services.cache_service import CacheService
from app.services.cart_service import CartService
from app.services.category_service import CategoryService
from app.services.config_service import ConfigService
from app.services.epg_service import EpgService
from app.services.http_client import HttpClientService
from app.services.m3u_service import M3uService
from app.services.monitor_service import MonitorService
from app.services.notification_service import NotificationService
from app.services.xtream_service import XtreamService


def get_config_service(request: Request) -> ConfigService:
    return request.app.state.config_service


def get_http_client(request: Request) -> HttpClientService:
    return request.app.state.http_client


def get_cache_service(request: Request) -> CacheService:
    return request.app.state.cache_service


def get_epg_service(request: Request) -> EpgService:
    return request.app.state.epg_service


def get_xtream_service(request: Request) -> XtreamService:
    return request.app.state.xtream_service


def get_notification_service(request: Request) -> NotificationService:
    return request.app.state.notification_service


def get_category_service(request: Request) -> CategoryService:
    return request.app.state.category_service


def get_cart_service(request: Request) -> CartService:
    return request.app.state.cart_service


def get_monitor_service(request: Request) -> MonitorService:
    return request.app.state.monitor_service


def get_m3u_service(request: Request) -> M3uService:
    return request.app.state.m3u_service

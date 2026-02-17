"""Pydantic models for application configuration."""
from __future__ import annotations

import uuid
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class FilterRule(BaseModel):
    """A single filter rule (include/exclude)."""
    model_config = ConfigDict(extra="allow")

    type: str = "exclude"  # "include" or "exclude"
    match: str = "contains"  # contains, exact, starts_with, ends_with, not_contains, regex, all
    value: str = ""
    case_sensitive: bool = False


class ContentFilters(BaseModel):
    """Filter rules for a content type (groups + channels)."""
    model_config = ConfigDict(extra="allow")

    groups: list[FilterRule] = Field(default_factory=list)
    channels: list[FilterRule] = Field(default_factory=list)


class SourceFilters(BaseModel):
    """Per-source filter configuration across all content types."""
    model_config = ConfigDict(extra="allow")

    live: ContentFilters = Field(default_factory=ContentFilters)
    vod: ContentFilters = Field(default_factory=ContentFilters)
    series: ContentFilters = Field(default_factory=ContentFilters)


class Source(BaseModel):
    """An IPTV source (Xtream Codes provider)."""
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = "New Source"
    host: str = ""
    username: str = ""
    password: str = ""
    enabled: bool = True
    prefix: str = ""
    route: Optional[str] = None
    filters: SourceFilters = Field(default_factory=SourceFilters)


class XtreamLegacy(BaseModel):
    """Legacy single-source Xtream config (for migration)."""
    model_config = ConfigDict(extra="allow")

    host: str = ""
    username: str = ""
    password: str = ""


class TelegramConfig(BaseModel):
    """Telegram notification settings."""
    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""


class DaySchedule(BaseModel):
    """Download schedule for a single day of the week."""
    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    start: str = "01:00"
    end: str = "07:00"


class DownloadSchedule(BaseModel):
    """Per-day-of-week download time-slot schedule."""
    model_config = ConfigDict(extra="allow")

    enabled: bool = False
    monday: DaySchedule = Field(default_factory=DaySchedule)
    tuesday: DaySchedule = Field(default_factory=DaySchedule)
    wednesday: DaySchedule = Field(default_factory=DaySchedule)
    thursday: DaySchedule = Field(default_factory=DaySchedule)
    friday: DaySchedule = Field(default_factory=DaySchedule)
    saturday: DaySchedule = Field(default_factory=DaySchedule)
    sunday: DaySchedule = Field(default_factory=DaySchedule)


class Options(BaseModel):
    """Application options."""
    model_config = ConfigDict(extra="allow")

    cache_enabled: bool = True
    cache_ttl: int = 3600
    epg_cache_ttl: int = 21600  # 6 hours
    refresh_interval: int = 3600
    proxy_streams: bool = True
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    download_path: str = "/data/downloads"
    download_temp_path: str = "/data/downloads/.tmp"
    download_bandwidth_limit: int = 0
    download_pause_interval: int = 0
    download_pause_duration: int = 0
    download_player_profile: str = "tivimate"
    download_burst_reconnect: int = 0
    download_notify_file: bool = False
    download_notify_queue: bool = False
    download_schedule: DownloadSchedule = Field(default_factory=DownloadSchedule)


class ContentTypes(BaseModel):
    """Which content types are enabled."""
    model_config = ConfigDict(extra="allow")

    live: bool = True
    vod: bool = True
    series: bool = True


class AppConfig(BaseModel):
    """Root application configuration."""
    model_config = ConfigDict(extra="allow")

    sources: list[Source] = Field(default_factory=list)
    xtream: XtreamLegacy = Field(default_factory=XtreamLegacy)
    filters: SourceFilters = Field(default_factory=SourceFilters)
    content_types: ContentTypes = Field(default_factory=ContentTypes)
    options: Options = Field(default_factory=Options)

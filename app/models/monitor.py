"""Pydantic models for series monitoring."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class KnownEpisode(BaseModel):
    """A known/downloaded episode reference."""
    model_config = ConfigDict(extra="allow")

    stream_id: str = ""
    source_id: str = ""
    season: str = ""
    episode_num: int = 0


class MonitorSource(BaseModel):
    """A (source, series_ref, category) slot inside a MonitoredSeries."""
    model_config = ConfigDict(extra="allow")

    source_id: str = ""
    series_ref: str = ""        # the series_id on that specific source
    source_name: Optional[str] = None
    category: Optional[str] = None


class MonitoredSeries(BaseModel):
    """A monitored series entry."""
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    series_name: str = ""
    series_id: str = ""
    # Legacy single-source fields (kept for backward compatibility)
    source_id: Optional[str] = None
    source_name: Optional[str] = None
    source_category: Optional[str] = None
    # Multi-source list (new)
    monitor_sources: list[dict] = Field(default_factory=list)
    cover: str = ""
    scope: str = "new_only"  # "all", "season", "new_only"
    season_filter: Optional[str] = None
    action: str = "both"  # "notify", "download", "both"
    enabled: bool = True
    known_episodes: list[dict] = Field(default_factory=list)
    downloaded_episodes: list[dict] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    last_checked: Optional[str] = None
    last_new_count: int = 0


class MonitorData(BaseModel):
    """Root monitored series file structure."""
    model_config = ConfigDict(extra="allow")

    series: list[dict] = Field(default_factory=list)

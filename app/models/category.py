"""Pydantic models for custom categories."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class PatternRule(BaseModel):
    """A pattern matching rule for automatic categories."""
    model_config = ConfigDict(extra="allow")

    match: str = "contains"
    value: str = ""
    case_sensitive: bool = False


class CachedItem(BaseModel):
    """A reference to a cached item in a category."""
    model_config = ConfigDict(extra="allow")

    id: str
    source_id: str
    content_type: str  # "live", "vod", "series"


class ManualItem(CachedItem):
    """An item manually added to a category."""
    added_at: str = Field(default_factory=lambda: datetime.now().isoformat())


class Category(BaseModel):
    """A custom category (manual or automatic)."""
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    icon: str = "📁"
    mode: str = "manual"  # "manual" or "automatic"
    content_types: list[str] = Field(default_factory=lambda: ["live", "vod", "series"])
    items: list[dict] = Field(default_factory=list)  # manual items (raw dicts for flexibility)
    patterns: list[PatternRule] = Field(default_factory=list)
    pattern_logic: str = "or"  # "or" or "and"
    use_source_filters: bool = False
    notify_telegram: bool = False
    recently_added_days: int = 0
    cached_items: list[dict] = Field(default_factory=list)  # auto-matched items
    last_refresh: Optional[str] = None


class CategoriesData(BaseModel):
    """Root categories file structure."""
    model_config = ConfigDict(extra="allow")

    categories: list[Category] = Field(default_factory=list)

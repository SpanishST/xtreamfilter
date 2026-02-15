"""Pydantic models for download cart items."""
from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class CartItemStatus(str, enum.Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    MOVE_FAILED = "move_failed"


class CartItem(BaseModel):
    """A single download cart item."""
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    stream_id: str = ""
    source_id: str = ""
    content_type: str = "vod"  # "vod" or "series"
    name: str = ""
    series_name: Optional[str] = None
    season: Optional[str] = None
    episode_num: Optional[int] = None
    episode_title: Optional[str] = None
    icon: str = ""
    group: str = ""
    container_extension: str = "mp4"
    added_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    status: str = "queued"
    progress: float = 0
    error: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    temp_path: Optional[str] = None


class CartData(BaseModel):
    """Root cart file structure."""
    model_config = ConfigDict(extra="allow")

    items: list[dict] = Field(default_factory=list)

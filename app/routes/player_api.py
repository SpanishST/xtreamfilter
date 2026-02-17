"""Player API routes — stream info, remux proxy, EPG now/next for in-browser playback."""
from __future__ import annotations

import asyncio
import logging
import shutil

from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response

from app.dependencies import (
    get_cache_service,
    get_config_service,
    get_epg_service,
)
from app.services.cache_service import CacheService
from app.services.config_service import ConfigService
from app.services.epg_service import EpgService
from app.services.stream_service import proxy_stream, remux_stream, transcode_stream
from app.services.xtream_service import decode_virtual_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/player", tags=["player"])

# Browser-native playable formats (container+codec combos that work in <video>)
_NATIVE_EXTENSIONS = {"mp4", "m4v", "webm", "ogg", "mp3", "aac"}

HEADERS_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _build_upstream_url(source: dict, content_type: str, original_id: int, ext: str) -> str:
    """Build the upstream Xtream URL for a given stream."""
    host = source["host"].rstrip("/")
    username = source["username"]
    password = source["password"]
    if content_type == "live":
        return f"{host}/{username}/{password}/{original_id}.{ext}"
    elif content_type == "vod":
        return f"{host}/movie/{username}/{password}/{original_id}.{ext}"
    elif content_type == "series":
        return f"{host}/series/{username}/{password}/{original_id}.{ext}"
    return f"{host}/{username}/{password}/{original_id}.{ext}"


async def _probe_media_info(upstream_url: str) -> dict:
    """Run a single ffprobe to get duration and video codec.

    Returns {"duration": float|None, "video_codec": str|None}.
    """
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return {"duration": None, "video_codec": None}
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe_path,
            "-v", "error",
            "-user_agent", HEADERS_UA,
            "-show_entries", "format=duration:stream=codec_name,codec_type",
            "-of", "json",
            upstream_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        import json as _json
        data = _json.loads(stdout.decode())
        # Duration
        dur_str = data.get("format", {}).get("duration")
        duration = float(dur_str) if dur_str and dur_str != "N/A" else None
        # Video codec (first video stream)
        video_codec = None
        for s in data.get("streams", []):
            if s.get("codec_type") == "video":
                video_codec = s.get("codec_name")
                break
        return {"duration": duration, "video_codec": video_codec}
    except Exception as e:
        logger.debug(f"ffprobe media info failed: {e}")
        return {"duration": None, "video_codec": None}


async def _probe_audio_tracks(upstream_url: str) -> list[dict]:
    """Run ffprobe to list audio streams with index, codec and language.

    Returns a list of dicts like:
      [{"index": 0, "stream_index": 1, "codec": "aac", "language": "fre", "title": "French", "channels": 6}, ...]
    Audio-only index (0-based among audio streams) is what ffmpeg -map 0:a:<idx> expects.
    """
    ffprobe_path = shutil.which("ffprobe")
    if not ffprobe_path:
        return []
    try:
        proc = await asyncio.create_subprocess_exec(
            ffprobe_path,
            "-v", "error",
            "-user_agent", HEADERS_UA,
            "-select_streams", "a",
            "-show_entries", "stream=index,codec_name,channels:stream_tags=language,title",
            "-of", "json",
            upstream_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        import json as _json
        data = _json.loads(stdout.decode())
        tracks = []
        for i, s in enumerate(data.get("streams", [])):
            tags = s.get("tags", {})
            tracks.append({
                "index": i,  # audio-stream index for -map 0:a:i
                "stream_index": s.get("index"),  # global stream index
                "codec": s.get("codec_name", "unknown"),
                "language": tags.get("language", ""),
                "title": tags.get("title", ""),
                "channels": s.get("channels", 0),
            })
        return tracks
    except Exception as e:
        logger.debug(f"ffprobe audio tracks failed: {e}")
        return []


@router.get("/info/{content_type}/{source_index}/{stream_id}")
async def get_player_info(
    content_type: str,
    source_index: int,
    stream_id: int,
    ext: str = "ts",
    cfg: ConfigService = Depends(get_config_service),
    cache: CacheService = Depends(get_cache_service),
):
    """Return stream metadata and playback strategy for the in-browser player.

    Response:
        stream_url: proxied stream URL (always via proxy, never redirect)
        remux_url: remux endpoint URL (for non-native formats)
        format: detected format (ts, mp4, mkv, m3u8, ...)
        needs_remux: whether the browser needs the remux endpoint
        content_type: live / vod / series
        epg_channel_id: EPG channel ID for live streams (if available)
    """
    source = cfg.get_source_by_index(source_index)
    if not source:
        return Response(content="Source not found", status_code=404)

    # Determine the actual extension
    fmt = ext.lower() if ext else ("ts" if content_type == "live" else "mp4")

    # Build the proxy stream URL (always proxied for in-browser playback / CORS)
    if content_type == "live":
        stream_url = f"/merged/live/user/pass/{source_index * 10_000_000 + stream_id}.{fmt}"
    elif content_type == "vod":
        stream_url = f"/merged/movie/user/pass/{source_index * 10_000_000 + stream_id}.{fmt}"
    elif content_type == "series":
        stream_url = f"/merged/series/user/pass/{source_index * 10_000_000 + stream_id}.{fmt}"
    else:
        return Response(content="Invalid content type", status_code=400)

    needs_remux = False  # deprecated — always transcode non-native formats
    remux_url = None

    # Find EPG channel ID for live streams
    epg_channel_id = None
    if content_type == "live":
        source_id = source.get("id", "unknown")
        streams = cache.get_cached("live_streams", source_id)
        if streams:
            for s in streams:
                if str(s.get("stream_id", "")) == str(stream_id):
                    raw_epg = s.get("epg_channel_id", "")
                    if raw_epg:
                        epg_channel_id = f"{source_id}_{raw_epg}".lower()
                    break

    # Probe media info & audio tracks for VOD/series
    duration = None
    video_codec = None
    audio_tracks = []
    upstream_url = _build_upstream_url(source, content_type, stream_id, fmt)
    if content_type in ("vod", "series"):
        media_info, audio_tracks = await asyncio.gather(
            _probe_media_info(upstream_url),
            _probe_audio_tracks(upstream_url),
        )
        duration = media_info["duration"]
        video_codec = media_info["video_codec"]
    else:
        # For live, still try to probe audio tracks (useful for multi-language channels)
        audio_tracks = await _probe_audio_tracks(upstream_url)

    return {
        "stream_url": stream_url,
        "remux_url": remux_url,
        "format": fmt,
        "needs_remux": needs_remux,
        "content_type": content_type,
        "epg_channel_id": epg_channel_id,
        "duration": duration,
        "video_codec": video_codec,
        "audio_tracks": audio_tracks,
    }


# ------------------------------------------------------------------
# Remux endpoint — ffmpeg remux to fMP4 for non-native containers
# ------------------------------------------------------------------

@router.get("/remux/{content_type}/{source_index}/{stream_id}")
async def player_remux_stream(
    request: Request,
    content_type: str,
    source_index: int,
    stream_id: int,
    ext: str = "mkv",
    cfg: ConfigService = Depends(get_config_service),
):
    """Remux a stream through ffmpeg to fragmented MP4 for browser playback."""
    source = cfg.get_source_by_index(source_index)
    if not source:
        return Response(content="Source not found", status_code=404)

    upstream_url = _build_upstream_url(source, content_type, stream_id, ext)
    return await remux_stream(upstream_url, request)


# ------------------------------------------------------------------
# Transcode endpoint — ffmpeg H.265→H.264 for HEVC streams
# ------------------------------------------------------------------

@router.get("/transcode/{content_type}/{source_index}/{stream_id}")
async def player_transcode_stream(
    request: Request,
    content_type: str,
    source_index: int,
    stream_id: int,
    ext: str = "ts",
    audio_only: bool = False,
    start: float = 0,
    audio_track: int = -1,
    cfg: ConfigService = Depends(get_config_service),
):
    """Transcode a stream through ffmpeg for browser playback.

    For live streams this always produces H.264 + AAC at 720p with
    automatic deinterlacing — handles HEVC, 1080i, E-AC3, DTS, etc.

    Query params:
        ext: source format extension (default: ts)
        audio_only: if true, copy video and only transcode audio to AAC.
                    Useful for VOD with H.264 video + unsupported audio.
        start: seek offset in seconds (VOD/series only). ffmpeg will
               start decoding from this position.
        audio_track: 0-based audio stream index to select (via -map 0:a:<idx>).
                     -1 means use ffmpeg default (first audio).
    """
    source = cfg.get_source_by_index(source_index)
    if not source:
        return Response(content="Source not found", status_code=404)

    upstream_url = _build_upstream_url(source, content_type, stream_id, ext)
    is_live = content_type == "live"
    return await transcode_stream(
        upstream_url, request, is_live=is_live,
        audio_only=audio_only, start_seconds=start if not is_live else 0,
        audio_track=audio_track,
    )


# ------------------------------------------------------------------
# Force-proxied stream for the player (always proxy, even if global proxy is off)
# ------------------------------------------------------------------

@router.get("/stream/{content_type}/{source_index}/{stream_id}")
async def player_proxy_stream(
    request: Request,
    content_type: str,
    source_index: int,
    stream_id: int,
    ext: str = "",
    cfg: ConfigService = Depends(get_config_service),
):
    """Always-proxied stream endpoint for in-browser playback (bypasses global proxy toggle)."""
    source = cfg.get_source_by_index(source_index)
    if not source:
        return Response(content="Source not found", status_code=404)

    if not ext:
        ext = "ts" if content_type == "live" else "mp4"

    upstream_url = _build_upstream_url(source, content_type, stream_id, ext)
    stream_type = "live" if content_type == "live" else content_type
    return await proxy_stream(upstream_url, request, stream_type=stream_type)


# ------------------------------------------------------------------
# EPG now / next for a channel
# ------------------------------------------------------------------

@router.get("/epg/now/{channel_id:path}")
async def get_epg_now_next(
    channel_id: str,
    epg: EpgService = Depends(get_epg_service),
):
    """Return the current and next programme for a given EPG channel ID."""
    result = epg.get_now_next(channel_id)
    return result

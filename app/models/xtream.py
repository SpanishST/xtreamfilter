"""Xtream-related constants and player profiles."""
from __future__ import annotations

# Virtual ID offset for merged playlists (10 million per source)
VIRTUAL_ID_OFFSET = 10_000_000

# Base offset for custom category IDs
CUSTOM_CAT_ID_BASE = 99001

# Predefined IPTV player header profiles
PLAYER_PROFILES = {
    "tivimate": {
        "name": "TiviMate",
        "headers": {
            "User-Agent": "TiviMate/4.7.0 (Linux; Android 14) OkHttp/4.12.0",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "smarters": {
        "name": "IPTV Smarters Pro",
        "headers": {
            "User-Agent": "IPTVSmartersPro",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "vlc": {
        "name": "VLC Media Player",
        "headers": {
            "User-Agent": "VLC/3.0.20 LibVLC/3.0.20",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "kodi": {
        "name": "Kodi",
        "headers": {
            "User-Agent": "Kodi/21.0 (Linux; Android 14) Kodi_Home/21.0 ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "xciptv": {
        "name": "XCIPTV Player",
        "headers": {
            "User-Agent": "xciptv/6.0.0 (Linux; Android 14) ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "ott_navigator": {
        "name": "OTT Navigator",
        "headers": {
            "User-Agent": "OTT Navigator/1.7.1.3 (Linux; Android 14) ExoPlayerLib/2.19.1",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "keep-alive",
        },
    },
    "ffmpeg": {
        "name": "FFmpeg / Lavf",
        "headers": {
            "User-Agent": "Lavf/60.16.100",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Connection": "close",
            "Icy-MetaData": "1",
        },
    },
}

# Icon name to emoji mapping for custom categories
ICON_EMOJI_MAP = {
    "folder": "📁",
    "heart": "❤️",
    "star": "⭐",
    "fire": "🔥",
    "clock": "🕐",
    "film": "🎬",
    "tv": "📺",
    "music": "🎵",
    "sports": "⚽",
    "news": "📰",
    "kids": "👶",
    "bookmark": "🔖",
    "tag": "🏷️",
    "check": "✅",
    "play": "▶️",
    "list": "📋",
}

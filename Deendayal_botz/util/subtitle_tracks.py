# Deendayal_botz/util/subtitle_tracks.py
# Pure Reaper-style: extract entire subtitle track once and cache it

import os
import json
import asyncio
import logging
from pathlib import Path

from info import LOG_CHANNEL, PORT as LOCAL_PORT
from Deendayal_botz.Bot import DeendayalBot
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

LOCAL_PORT = int(LOCAL_PORT)

FULL_SUB_DIR = Path("/tmp/subtitle_full_cache")
FULL_SUB_DIR.mkdir(parents=True, exist_ok=True)

_file_cache = {}          # msg_id -> {"tracks": [...]}
SUB_WINDOW_SECONDS = 600  # match your audio window size

async def extract_subtitle_window(msg_id: int, secure_hash: str, stream_index: int, start_time: float) -> bytes:
    """
    Extract only a WINDOW of subtitles starting at start_time.
    Uses -ss BEFORE -i so ffmpeg seeks fast instead of reading from byte 0.
    Returns raw WebVTT bytes for just this window (timestamps start at 0
    inside this window — the frontend must add start_time back on).
    """
    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
        "-ss", str(max(0.0, start_time)),
        "-t", str(SUB_WINDOW_SECONDS),
        "-i", internal_url,
        "-map", f"0:{stream_index}",
        "-c:s", "webvtt",
        "-f", "webvtt",
        "pipe:1",
    ]

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        logging.error(f"[SubWindow] ffmpeg failed: {stderr[:400]!r}")
        raise RuntimeError(f"subtitle window extraction failed for stream {stream_index}")

    return stdout
def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None})



def _internal_stream_url(msg_id: int, secure_hash: str) -> str:
    return f"http://127.0.0.1:{LOCAL_PORT}/{secure_hash}{msg_id}"


async def get_subtitle_tracks(msg_id: int, secure_hash: str) -> list:
    """Return list of subtitle streams (index, codec, language, title)."""
    entry = _meta(msg_id)
    if entry["tracks"] is not None:
        return entry["tracks"]

    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "s",
        "-timeout", "15000000",
        "-i", internal_url,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    tracks = []
    try:
        data = json.loads(stdout)
        for i, stream in enumerate(data.get("streams", [])):
            tags = stream.get("tags", {}) or {}
            tracks.append({
                "index": stream.get("index", i),
                "codec_name": stream.get("codec_name", "unknown"),
                "tags": {
                    "language": tags.get("language", tags.get("LANGUAGE", "")),
                    "title": tags.get("title", tags.get("TITLE", "")),
                    "handler_name": tags.get("handler_name", ""),
                }
            })
    except Exception as e:
        logging.error(f"[SubtitleTracks] ffprobe parse failed: {e} | stderr={stderr[:300]!r}")
        tracks = []

    entry["tracks"] = tracks
    return tracks

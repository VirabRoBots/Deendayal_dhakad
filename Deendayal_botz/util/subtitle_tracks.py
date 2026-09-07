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
_full_sub_locks = {}      # (msg_id, stream_index) -> asyncio.Lock


def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None})


async def _get_file_id(msg_id: int, secure_hash: str):
    file_id = await get_file_ids(DeendayalBot, LOG_CHANNEL, msg_id)
    if not file_id:
        raise FIleNotFound
    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash
    return file_id


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


def _full_sub_path(msg_id: int, stream_index: int) -> Path:
    return FULL_SUB_DIR / f"{msg_id}_full_t{stream_index}.vtt"


async def extract_full_subtitle(msg_id: int, secure_hash: str, stream_index: int) -> Path:
    """
    Extract the ENTIRE subtitle stream once and cache it on disk.
    This is the pure Reaper-style method.
    """
    out_path = _full_sub_path(msg_id, stream_index)

    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    lock = _full_sub_locks.setdefault((msg_id, stream_index), asyncio.Lock())
    async with lock:
        # Double-check after acquiring lock
        if out_path.exists() and out_path.stat().st_size > 0:
            return out_path

        await _get_file_id(msg_id, secure_hash)
        internal_url = _internal_stream_url(msg_id, secure_hash)

        # Make sure we have track list (optional but useful for logging)
        entry = _meta(msg_id)
        if entry.get("tracks") is None:
            try:
                await get_subtitle_tracks(msg_id, secure_hash)
            except Exception as e:
                logging.warning(f"[FullSubtitle] get_subtitle_tracks failed: {e}")

        logging.info(
            f"[FullSubtitle] extracting full track msg={msg_id} stream={stream_index}"
        )

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
            "-timeout", "15000000",
            "-i", internal_url,
            "-map", f"0:{stream_index}",
            "-c:s", "webvtt",
            "-f", "webvtt",
            str(out_path),
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()

        ok = process.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0
        if not ok:
            if out_path.exists():
                try:
                    out_path.unlink()
                except Exception:
                    pass
            logging.error(f"[FullSubtitle] ffmpeg failed: {stderr[:400]!r}")
            raise RuntimeError(f"subtitle extraction failed for stream {stream_index}")

        return out_path

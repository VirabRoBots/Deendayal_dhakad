# Deendayal_botz/util/audio_tracks.py

import os
import json
import asyncio
import logging
import time
from pathlib import Path

from aiohttp import web

from info import LOG_CHANNEL, PORT
from Deendayal_botz.Bot import DeendayalBot
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

LOCAL_PORT = int(PORT)
TEMP_DIR = Path("/tmp/audio_cache")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

_file_cache = {}
CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
CACHE_MIN_FREE_BYTES = 1 * 1024 ** 3
COPYABLE_AUDIO = {"aac", "mp4a"}


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


def _codec_for_stream(msg_id: int, stream_index: int) -> str:
    tracks = _meta(msg_id).get("tracks") or []
    for t in tracks:
        if t.get("index") == stream_index:
            return (t.get("codec_name") or "").lower()
    return ""


async def get_tracks(msg_id: int, secure_hash: str) -> list:
    entry = _meta(msg_id)
    if entry["tracks"] is not None:
        return entry["tracks"]

    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "a",
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
                },
            })
    except Exception as e:
        logging.error(f"[AudioTracks] ffprobe parse failed: {e} | stderr={stderr[:300]!r}")
        tracks = []

    entry["tracks"] = tracks
    return tracks


async def mux_av_stream(
    request: web.Request,
    msg_id: int,
    secure_hash: str,
    stream_index: int,
    start_time: float = 0.0,
):
    """Video + selected audio as one fragmented MP4 stream (play while muxing)."""
    await _get_file_id(msg_id, secure_hash)

    entry = _meta(msg_id)
    if entry.get("tracks") is None:
        try:
            await get_tracks(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[Mux] get_tracks failed: {e}")

    codec = _codec_for_stream(msg_id, stream_index)
    use_copy = codec in COPYABLE_AUDIO
    internal_url = _internal_stream_url(msg_id, secure_hash)

    logging.info(
        f"[Mux] msg={msg_id} audio={stream_index} codec={codec or '?'} "
        f"mode={'copy' if use_copy else 'encode'} start={start_time}s"
    )

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
        "-seekable", "1",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-timeout", "30000000",
    ]
    # -ss BEFORE -i for fast input seek
    if start_time > 0:
        cmd += ["-ss", str(start_time)]

    cmd += [
        "-i", internal_url,
        "-map", "0:v:0",
        "-map", f"0:{stream_index}",
        "-c:v", "copy",
    ]
    if use_copy:
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-c:a", "aac", "-b:a", "96k", "-ac", "2"]

    cmd += [
        "-f", "mp4",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof+frag_every_frame",
        "pipe:1",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "video/mp4",
            "Cache-Control": "no-store",
            "Accept-Ranges": "none",
        },
    )
    await resp.prepare(request)

    try:
        while True:
            chunk = await proc.stdout.read(64 * 1024)
            if not chunk:
                break
            await resp.write(chunk)
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        try:
            proc.kill()
        except Exception:
            pass
    finally:
        try:
            if proc.returncode is None:
                proc.kill()
        except Exception:
            pass
        try:
            _, err = await proc.communicate()
            if proc.returncode not in (0, None, -9):
                logging.error(f"[Mux] ffmpeg exit={proc.returncode} err={err[:300]!r}")
        except Exception:
            pass

    return resp


def _dir_free_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


async def cleanup_audio_cache():
    now = time.time()
    files = sorted(TEMP_DIR.glob("*"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    for f in list(files):
        try:
            if f.exists() and now - f.stat().st_mtime > CACHE_MAX_AGE_SECONDS:
                f.unlink()
        except FileNotFoundError:
            pass
    files = sorted(TEMP_DIR.glob("*"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    while files and _dir_free_bytes(TEMP_DIR) < CACHE_MIN_FREE_BYTES:
        oldest = files.pop(0)
        try:
            if oldest.exists():
                oldest.unlink()
        except FileNotFoundError:
            pass


async def start_cache_cleanup_loop(interval_seconds: int = 3600):
    while True:
        try:
            await cleanup_audio_cache()
        except Exception as e:
            logging.error(f"[AudioTracks] cleanup error: {e}")
        await asyncio.sleep(interval_seconds)

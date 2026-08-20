# Deendayal_botz/util/audio_tracks.py
# Full-track audio from 0 + cache. Player: audio follows video.

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
_extract_locks = {}
COPYABLE_AUDIO = {"aac", "mp4a", "mp4a.40.2"}
CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
CACHE_MIN_FREE_BYTES = 1 * 1024 ** 3
PROBE_TIMEOUT_SECONDS = 40


def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None, "duration": 0.0})


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
    for t in _meta(msg_id).get("tracks") or []:
        if t.get("index") == stream_index:
            return (t.get("codec_name") or "").lower()
    return ""


def _cache_path(msg_id: int, stream_index: int) -> Path:
    return TEMP_DIR / f"{msg_id}_track{stream_index}.aac"


async def get_track_info(msg_id: int, secure_hash: str) -> dict:
    entry = _meta(msg_id)
    if entry.get("tracks"):
        return {"tracks": entry["tracks"], "duration": entry.get("duration") or 0.0}

    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    proc = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-select_streams", "a",
        "-rw_timeout", "20000000",
        "-i", internal_url,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=PROBE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except Exception:
            pass
        return {"tracks": [], "duration": 0.0}

    tracks = []
    duration = 0.0
    try:
        data = json.loads(stdout or b"{}")
        for i, stream in enumerate(data.get("streams", [])):
            tags = stream.get("tags") or {}
            tracks.append({
                "index": stream.get("index", i),
                "codec_name": stream.get("codec_name", "unknown"),
                "tags": {
                    "language": tags.get("language", tags.get("LANGUAGE", "")),
                    "title": tags.get("title", tags.get("TITLE", "")),
                    "handler_name": tags.get("handler_name", ""),
                },
            })
        raw = (data.get("format") or {}).get("duration")
        if raw:
            duration = max(0.0, float(raw))
    except Exception as e:
        logging.error(f"[AudioTracks] ffprobe parse failed: {e} | {stderr[:200]!r}")
        return {"tracks": [], "duration": 0.0}

    if tracks:
        entry["tracks"] = tracks
        entry["duration"] = duration
    return {"tracks": tracks, "duration": duration}


async def get_tracks(msg_id: int, secure_hash: str) -> list:
    info = await get_track_info(msg_id, secure_hash)
    return info.get("tracks", [])


async def _ensure_audio_file(msg_id: int, secure_hash: str, stream_index: int) -> Path:
    path = _cache_path(msg_id, stream_index)
    if path.exists() and path.stat().st_size > 0:
        return path

    await _get_file_id(msg_id, secure_hash)
    entry = _meta(msg_id)
    if not entry.get("tracks"):
        await get_track_info(msg_id, secure_hash)

    codec = _codec_for_stream(msg_id, stream_index)
    use_copy = codec in COPYABLE_AUDIO
    internal_url = _internal_stream_url(msg_id, secure_hash)
    lock = _extract_locks.setdefault((msg_id, stream_index), asyncio.Lock())

    async with lock:
        if path.exists() and path.stat().st_size > 0:
            return path

        tmp = path.with_suffix(".part.aac")
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass

        logging.info(
            f"[AudioTracks] extract full msg={msg_id} stream={stream_index} "
            f"codec={codec or '?'} mode={'copy' if use_copy else 'encode'}"
        )

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
            "-seekable", "1",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
            "-rw_timeout", "30000000",
            "-i", internal_url,
            "-map", f"0:{stream_index}",
            "-vn",
        ]
        if use_copy:
            cmd += ["-c:a", "copy", "-f", "adts"]
        else:
            cmd += ["-c:a", "aac", "-b:a", "96k", "-ac", "2", "-f", "adts"]
        cmd += [str(tmp)]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()

        if proc.returncode != 0 or not tmp.exists() or tmp.stat().st_size <= 0:
            logging.error(f"[AudioTracks] extract failed: {err[:300]!r}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass
            raise RuntimeError("Audio extract failed")

        tmp.rename(path)
        return path


async def stream_audio_file(
    request: web.Request, msg_id: int, secure_hash: str, stream_index: int
):
    try:
        path = await _ensure_audio_file(msg_id, secure_hash, stream_index)
    except Exception as e:
        logging.exception("[AudioTracks] ensure failed")
        raise web.HTTPInternalServerError(text=str(e))

    file_size = path.stat().st_size
    range_header = request.headers.get("Range")

    if range_header:
        try:
            _, rng = range_header.split("=", 1)
            start_s, end_s = (rng + "-").split("-")[:2]
            start = int(start_s) if start_s else 0
            end = int(end_s) if end_s else file_size - 1
            end = min(end, file_size - 1)
            if start < 0 or start > end:
                raise ValueError("bad range")
        except Exception:
            return web.Response(
                status=416,
                headers={"Content-Range": f"bytes */{file_size}"},
            )
        length = end - start + 1
        with open(path, "rb") as f:
            f.seek(start)
            data = f.read(length)
        return web.Response(
            status=206,
            body=data,
            headers={
                "Content-Type": "audio/aac",
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Content-Length": str(length),
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=3600",
            },
        )

    return web.FileResponse(
        path,
        headers={
            "Content-Type": "audio/aac",
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",
        },
    )


def _dir_free_bytes(path: Path) -> int:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


async def cleanup_audio_cache():
    now = time.time()
    files = sorted(
        TEMP_DIR.glob("*.aac"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
    )
    for f in list(files):
        try:
            if f.exists() and now - f.stat().st_mtime > CACHE_MAX_AGE_SECONDS:
                f.unlink()
        except FileNotFoundError:
            pass
    files = sorted(
        TEMP_DIR.glob("*.aac"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
    )
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
            logging.error(f"[AudioTracks] cleanup: {e}")
        await asyncio.sleep(interval_seconds)

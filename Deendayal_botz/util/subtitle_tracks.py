# Deendayal_botz/util/subtitle_tracks.py

import os
import json
import asyncio
import logging
import time
from pathlib import Path

from info import LOG_CHANNEL
from Deendayal_botz.Bot import DeendayalBot
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

from info import PORT as LOCAL_PORT
LOCAL_PORT = int(LOCAL_PORT)

TEMP_DIR = Path("/tmp/subtitle_cache")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

_file_cache = {}
_extract_locks = {}
_inflight = {}

CACHE_MAX_AGE_SECONDS = 60 * 60          # 1 hour (subs are small)
CACHE_MIN_FREE_BYTES = 512 * 1024 ** 2   # 512 MB


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


def _temp_sub_path(msg_id: int, stream_index: int) -> Path:
    return TEMP_DIR / f"{msg_id}_s{stream_index}.vtt"


def _build_ffmpeg_sub_cmd(internal_url: str, stream_index: int, out_path: Path) -> list:
    """Extract one subtitle stream to WebVTT (works for srt, ass, ssa, mov_text, etc.)."""
    return [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
        "-seekable", "1",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-timeout", "15000000",
        "-probesize", "10M", "-analyzeduration", "10M",
        "-i", internal_url,
        "-map", f"0:{stream_index}",
        "-c:s", "webvtt",
        "-f", "webvtt",
        str(out_path),
    ]


def _kill_job(job: dict, reason: str):
    try:
        if job["proc"].returncode is None:
            job["proc"].kill()
            logging.info(
                f"[SubCleanup] Killed msg={job['msg_id']} stream={job['stream_index']} reason={reason}"
            )
    except Exception as e:
        logging.warning(f"[SubCleanup] kill failed: {e}")


def _delete_file(path: Path, reason: str):
    try:
        if path.exists():
            size = path.stat().st_size
            path.unlink()
            logging.info(f"[SubCleanup] Deleted {path.name} size={size}bytes reason={reason}")
    except Exception as e:
        logging.warning(f"[SubCleanup] delete failed for {path}: {e}")


async def extract_subtitle(msg_id: int, secure_hash: str, stream_index: int):
    """
    Extract subtitle stream → WebVTT and return a streaming generator
    (or the finished file if already cached).
    """
    key = (msg_id, stream_index)

    existing = _inflight.get(key)
    if existing is not None:
        return _stream_from_inflight(existing)

    # Ensure we know the tracks
    entry = _meta(msg_id)
    if entry.get("tracks") is None:
        try:
            await get_subtitle_tracks(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[SubtitleTracks] get_subtitle_tracks failed: {e}")

    await _get_file_id(msg_id, secure_hash)
    lock = _extract_locks.setdefault(key, asyncio.Lock())

    async with lock:
        existing = _inflight.get(key)
        if existing is not None:
            return _stream_from_inflight(existing)

        # Already on disk?
        tmp_out = _temp_sub_path(msg_id, stream_index)
        if tmp_out.exists() and tmp_out.stat().st_size > 0:
            job = {
                "msg_id": msg_id,
                "stream_index": stream_index,
                "tmp_out": tmp_out,
                "proc": None,
                "done": asyncio.Event(),
                "ok": True,
            }
            job["done"].set()
            _inflight[key] = job
            return _stream_from_inflight(job)

        internal_url = _internal_stream_url(msg_id, secure_hash)
        cmd = _build_ffmpeg_sub_cmd(internal_url, stream_index, tmp_out)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )

        job = {
            "msg_id": msg_id,
            "stream_index": stream_index,
            "tmp_out": tmp_out,
            "proc": proc,
            "done": asyncio.Event(),
            "ok": False,
        }
        _inflight[key] = job

        async def finalize():
            _, stderr = await proc.communicate()
            if key not in _inflight:
                job["done"].set()
                return
            ok = proc.returncode == 0 and tmp_out.exists() and tmp_out.stat().st_size > 0
            job["ok"] = ok
            if not ok:
                logging.error(f"[SubtitleTracks] ffmpeg failed: {stderr[:400]!r}")
                if tmp_out.exists():
                    tmp_out.unlink(missing_ok=True)
            job["done"].set()

        asyncio.create_task(finalize())

    return _stream_from_inflight(job)


def _stream_from_inflight(job: dict):
    key = (job["msg_id"], job["stream_index"])

    async def generator():
        tmp_out = job["tmp_out"]
        try:
            # Wait until ffmpeg fully finishes – browsers need a complete VTT
            await job["done"].wait()

            if not job.get("ok") or not tmp_out.exists() or tmp_out.stat().st_size == 0:
                logging.error("[SubtitleTracks] extraction failed or empty file")
                return

            with open(tmp_out, "rb") as f:
                while True:
                    chunk = f.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            logging.info("[SubtitleTracks] client disconnected")
        except Exception as e:
            logging.error(f"[SubtitleTracks] stream error: {e}")
        finally:
            # Keep the VTT on disk for reuse
            _inflight.pop(key, None)

    return generator()


def cancel_subtitle(msg_id: int, stream_index: int = None):
    for key, job in list(_inflight.items()):
        m, s = key
        if m != msg_id:
            continue
        if stream_index is not None and s != stream_index:
            continue
        _kill_job(job, reason="cancel")
        _delete_file(job["tmp_out"], reason="cancel")
        _inflight.pop(key, None)


def _dir_free_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


async def cleanup_subtitle_cache():
    now = time.time()
    files = sorted(TEMP_DIR.glob("*.vtt"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    for f in list(files):
        try:
            if f.exists() and now - f.stat().st_mtime > CACHE_MAX_AGE_SECONDS:
                _delete_file(f, reason="stale")
        except FileNotFoundError:
            pass

    files = sorted(TEMP_DIR.glob("*.vtt"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    while files and _dir_free_bytes(TEMP_DIR) < CACHE_MIN_FREE_BYTES:
        oldest = files.pop(0)
        try:
            if oldest.exists():
                _delete_file(oldest, reason="low-disk")
        except FileNotFoundError:
            pass


async def start_subtitle_cleanup_loop(interval_seconds: int = 1800):
    while True:
        try:
            await cleanup_subtitle_cache()
        except Exception as e:
            logging.error(f"[SubtitleTracks] cleanup error: {e}")
        await asyncio.sleep(interval_seconds)

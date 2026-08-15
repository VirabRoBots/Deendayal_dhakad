# Deendayal_botz/util/audio_tracks.py

import os
import json
import asyncio
import logging
import time
from pathlib import Path

from info import LOG_CHANNEL
from Deendayal_botz.Bot import DeendayalBot, multi_clients, work_loads
from Deendayal_botz.util.custom_dl import ByteStreamer
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

TEMP_DIR = Path("/tmp/audio_cache")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

_file_cache = {}
_download_locks = {}
_extract_locks = {}

TG_CHUNK_LIMIT = 1024 * 1024
PROBE_BYTES = 4 * 1024 * 1024
DOWNLOAD_RETRIES = 2
CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
CACHE_MIN_FREE_BYTES = 1 * 1024 ** 3


def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None, "path": None})


async def _get_file_id(msg_id: int, secure_hash: str):
    file_id = await get_file_ids(DeendayalBot, LOG_CHANNEL, msg_id)
    if not file_id:
        raise FIleNotFound
    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash
    return file_id


def _pick_client():
    index = min(work_loads, key=work_loads.get)
    return index, multi_clients[index]


async def get_tracks(msg_id: int, secure_hash: str) -> list:
    entry = _meta(msg_id)
    if entry["tracks"] is not None:
        return entry["tracks"]

    file_id = await _get_file_id(msg_id, secure_hash)
    index, client = _pick_client()
    streamer = ByteStreamer(client)
    part_count = max(1, PROBE_BYTES // TG_CHUNK_LIMIT)

    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "a",
        "-i", "pipe:0",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def feed():
        try:
            async for chunk in streamer.yield_file(
                file_id, index, 0, 0, TG_CHUNK_LIMIT, part_count, TG_CHUNK_LIMIT
            ):
                if chunk:
                    process.stdin.write(chunk)
        except Exception as e:
            logging.error(f"[AudioTracks] probe feed error: {e}")
        finally:
            try:
                process.stdin.close()
            except Exception:
                pass

    await feed()
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
        logging.error(f"[AudioTracks] ffprobe parse failed: {e} | stderr={stderr[:300]!r}")
        tracks = []

    entry["tracks"] = tracks
    return tracks


async def _download_file_to_temp(msg_id: int, secure_hash: str, file_id) -> Path:
    lock = _download_locks.setdefault(msg_id, asyncio.Lock())
    async with lock:
        entry = _meta(msg_id)
        if entry["path"] and os.path.exists(entry["path"]):
            return Path(entry["path"])

        file_name = file_id.file_name or f"{msg_id}.mkv"
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in file_name)[:80]
        temp_path = TEMP_DIR / f"{msg_id}_{secure_hash}_{safe_name}"

        last_err = None
        for attempt in range(1, DOWNLOAD_RETRIES + 2):
            try:
                logging.info(f"[AudioTracks] Downloading message {msg_id} (attempt {attempt}) -> {temp_path}")
                message = await DeendayalBot.get_messages(LOG_CHANNEL, msg_id)
                await DeendayalBot.download_media(message, file_name=str(temp_path))

                if not temp_path.exists() or temp_path.stat().st_size == 0:
                    raise IOError("downloaded file missing or empty")
                if file_id.file_size and temp_path.stat().st_size < file_id.file_size:
                    raise IOError(
                        f"incomplete download: got {temp_path.stat().st_size} of {file_id.file_size} bytes"
                    )

                entry["path"] = str(temp_path)
                return temp_path
            except Exception as e:
                last_err = e
                logging.warning(f"[AudioTracks] download attempt {attempt} failed: {e}")
                try:
                    if temp_path.exists():
                        temp_path.unlink()
                except Exception:
                    pass
                await asyncio.sleep(1.5 * attempt)

        logging.error(f"[AudioTracks] all download attempts failed for {msg_id}: {last_err}")
        raise FIleNotFound


def _cached_audio_path(msg_id: int, stream_index: int) -> Path:
    return TEMP_DIR / f"{msg_id}_track{stream_index}.aac"


async def extract_audio_stream(msg_id: int, secure_hash: str, stream_index: int, start_time: float = 0.0):
    cache_path = _cached_audio_path(msg_id, stream_index)
    if cache_path.exists():
        return _stream_from_cached_file(cache_path)

    file_id = await _get_file_id(msg_id, secure_hash)
    lock = _extract_locks.setdefault((msg_id, stream_index), asyncio.Lock())

    async with lock:
        if cache_path.exists():
            return _stream_from_cached_file(cache_path)

        src_path = await _download_file_to_temp(msg_id, secure_hash, file_id)

        tmp_out = cache_path.with_suffix(".part.aac")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-i", str(src_path),
            "-map", f"0:{stream_index}",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "adts",
            str(tmp_out),
        ]
        proc = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.PIPE)
        _, stderr = await proc.communicate()

        if proc.returncode == 0 and tmp_out.exists() and tmp_out.stat().st_size > 0:
            tmp_out.rename(cache_path)
        else:
            logging.error(f"[AudioTracks] ffmpeg extraction failed: {stderr[:300]!r}")
            try:
                if tmp_out.exists():
                    tmp_out.unlink()
            except Exception:
                pass
            raise FIleNotFound

        try:
            if src_path.exists():
                src_path.unlink()
            _meta(msg_id)["path"] = None
        except Exception as e:
            logging.warning(f"[AudioTracks] source cleanup failed: {e}")

    return _stream_from_cached_file(cache_path)


def _stream_from_cached_file(path: Path):
    async def generator():
        try:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(64 * 1024)
                    if not chunk:
                        break
                    yield chunk
                    await asyncio.sleep(0)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            logging.info(f"[AudioTracks] client disconnected mid-stream: {path.name}")
        except Exception as e:
            logging.error(f"[AudioTracks] read error on {path.name}: {e}")
    return generator()


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
            logging.error(f"[AudioTracks] cleanup loop error: {e}")
        await asyncio.sleep(interval_seconds)

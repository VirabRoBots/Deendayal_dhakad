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
_extract_locks = {}
# Tracks an in-progress streamed extraction so concurrent requests for the
# SAME track can attach to it instead of starting a second ffmpeg process.
_inflight = {}

TG_CHUNK_LIMIT = 1024 * 1024
PROBE_BYTES = 4 * 1024 * 1024
CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
CACHE_MIN_FREE_BYTES = 1 * 1024 ** 3


def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None})


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


def _cached_audio_path(msg_id: int, stream_index: int) -> Path:
    return TEMP_DIR / f"{msg_id}_track{stream_index}.aac"


async def extract_audio_stream(msg_id: int, secure_hash: str, stream_index: int, start_time: float = 0.0):
    """
    Returns an async generator of audio bytes.

    - If already cached on disk, serves straight from the cache file (fast path).
    - Otherwise, starts ffmpeg, streams the source video into it, and yields
      audio bytes to the CALLER as ffmpeg produces them (so playback can start
      before extraction finishes), while ffmpeg simultaneously writes those
      same bytes to a temp file on disk. When done, the temp file is renamed
      into the cache so the NEXT request for this track is instant.
    - If a request for this exact track arrives while one is already being
      extracted, it attaches to the same in-progress job instead of starting
      a second ffmpeg process.
    """
    cache_path = _cached_audio_path(msg_id, stream_index)
    if cache_path.exists():
        return _stream_from_cached_file(cache_path)

    key = (msg_id, stream_index)

    # Already being extracted by another concurrent request? Attach to it.
    existing = _inflight.get(key)
    if existing is not None:
        return _stream_from_inflight(existing)

    file_id = await _get_file_id(msg_id, secure_hash)
    lock = _extract_locks.setdefault(key, asyncio.Lock())

    async with lock:
        # Re-check after acquiring the lock — someone may have just finished.
        if cache_path.exists():
            return _stream_from_cached_file(cache_path)
        existing = _inflight.get(key)
        if existing is not None:
            return _stream_from_inflight(existing)

        index, client = _pick_client()
        streamer = ByteStreamer(client)
        file_size = file_id.file_size or 0
        part_count = (
            max(1, (file_size + TG_CHUNK_LIMIT - 1) // TG_CHUNK_LIMIT)
            if file_size else 100000
        )

        tmp_out = cache_path.with_suffix(".part.aac")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-probesize", "2M", "-analyzeduration", "2M",
            "-i", "pipe:0",
            "-map", f"0:{stream_index}",
            "-c:a", "aac", "-b:a", "128k",
            "-f", "adts",
            str(tmp_out),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        # ffmpeg writes the ADTS/AAC stream directly to tmp_out (not stdout) —
        # a valid ADTS stream that's simultaneously readable mid-write via a
        # stdout pipe is unreliable across ffmpeg builds. Instead we tail-read
        # tmp_out as ffmpeg writes it, which still lets playback start before
        # extraction fully finishes.

        async def feed_source():
            try:
                async for chunk in streamer.yield_file(
                    file_id, index, 0, 0, TG_CHUNK_LIMIT, part_count, TG_CHUNK_LIMIT
                ):
                    if not chunk:
                        continue
                    try:
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
                    except (ConnectionResetError, BrokenPipeError):
                        break
            except Exception as e:
                logging.error(f"[AudioTracks] extract feed error: {e}")
            finally:
                try:
                    proc.stdin.close()
                except Exception:
                    pass

        feed_task = asyncio.create_task(feed_source())

        job = {
            "tmp_out": tmp_out,
            "cache_path": cache_path,
            "proc": proc,
            "feed_task": feed_task,
            "done": asyncio.Event(),
            "ok": False,
        }
        _inflight[key] = job

        async def finalize():
            _, stderr = await proc.communicate()
            await feed_task
            ok = proc.returncode == 0 and tmp_out.exists() and tmp_out.stat().st_size > 0
            if ok:
                try:
                    tmp_out.rename(cache_path)
                    job["ok"] = True
                except Exception as e:
                    logging.error(f"[AudioTracks] cache rename failed: {e}")
                    job["ok"] = False
            else:
                logging.error(f"[AudioTracks] ffmpeg extraction failed: {stderr[:300]!r}")
                try:
                    if tmp_out.exists():
                        tmp_out.unlink()
                except Exception:
                    pass
                job["ok"] = False
            job["done"].set()
            _inflight.pop(key, None)

        asyncio.create_task(finalize())

    return _stream_from_inflight(job)


def _stream_from_inflight(job: dict):
    """Tail-reads the partial file as ffmpeg writes it, so the client can
    start playing before extraction is fully done."""

    async def generator():
        tmp_out = job["tmp_out"]
        sent = 0
        try:
            while True:
                if tmp_out.exists():
                    with open(tmp_out, "rb") as f:
                        f.seek(sent)
                        chunk = f.read(64 * 1024)
                    if chunk:
                        sent += len(chunk)
                        yield chunk
                        continue
                if job["done"].is_set():
                    # Drain whatever is left, then stop.
                    if job["ok"] and job["cache_path"].exists():
                        with open(job["cache_path"], "rb") as f:
                            f.seek(sent)
                            rest = f.read()
                        if rest:
                            yield rest
                    break
                await asyncio.sleep(0.15)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            logging.info("[AudioTracks] client disconnected mid-stream (live extraction)")
        except Exception as e:
            logging.error(f"[AudioTracks] live-stream read error: {e}")

    return generator()


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

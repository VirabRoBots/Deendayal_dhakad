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

CACHE_MAX_AGE_SECONDS = 30 * 60
CACHE_MIN_FREE_BYTES = 512 * 1024 ** 2

# MUST match AUDIO_WINDOW_SECONDS / the player page's window size.
# Subtitles are windowed the same way audio is, so the two stay in sync
# and neither one has to pull/convert the whole file up front.
SUBTITLE_WINDOW_SECONDS = 600

SEEK_PAD_SECONDS = 10


def _round_start(start_time: float) -> float:
    if not start_time or start_time < 0:
        return 0.0
    return round(float(start_time), 3)


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


def _codec_for_stream(msg_id: int, stream_index: int) -> str:
    tracks = _meta(msg_id).get("tracks") or []
    for t in tracks:
        if t.get("index") == stream_index:
            return (t.get("codec_name") or "").lower()
    return ""


def _temp_sub_path(msg_id: int, stream_index: int, bucket_start: float) -> Path:
    return TEMP_DIR / f"{msg_id}_t{stream_index}_s{bucket_start:.3f}_w{SUBTITLE_WINDOW_SECONDS}.part.vtt"


def _build_ffmpeg_sub_cmd(internal_url: str, stream_index: int, bucket_start: float,
                           out_path: Path) -> list:
    """
    Windowed subtitle extraction, mirroring the audio path: seek near the
    bucket with input-side -ss (fast, no full-file decode), then trim exactly
    to the bucket with a small output-side -ss, and cap with -t so we only
    ever touch one window's worth of the file — never the whole thing.
    """
    pad = min(bucket_start, float(SEEK_PAD_SECONDS))
    input_ss = bucket_start - pad

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
        "-seekable", "1",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-timeout", "15000000",
    ]
    if input_ss > 0:
        cmd += ["-ss", f"{input_ss:.3f}"]

    cmd += [
        "-probesize", "2M", "-analyzeduration", "2M",
        "-i", internal_url,
        "-map", f"0:{stream_index}",
    ]
    if pad > 0:
        cmd += ["-ss", f"{pad:.3f}"]

    cmd += ["-t", str(SUBTITLE_WINDOW_SECONDS)]
    cmd += ["-c:s", "webvtt", "-f", "webvtt", str(out_path)]
    return cmd


def _kill_job(job: dict, reason: str):
    try:
        if job["proc"] is not None and job["proc"].returncode is None:
            job["proc"].kill()
            logging.info(
                f"[SubCleanup] Killed in-progress extraction "
                f"msg={job['msg_id']} stream={job['stream_index']} "
                f"start={job['bucket_start']:.3f}s reason={reason}"
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


def _destroy_previous(msg_id: int, stream_index: int, keep_bucket: float):
    for key, job in list(_inflight.items()):
        m, s, b = key
        if m == msg_id and s == stream_index and b != keep_bucket:
            _kill_job(job, reason="superseded")
            _delete_file(job["tmp_out"], reason="superseded")
            _inflight.pop(key, None)

    prefix = f"{msg_id}_t{stream_index}_s"
    for f in TEMP_DIR.glob(f"{prefix}*_w{SUBTITLE_WINDOW_SECONDS}*.vtt"):
        if f"_s{keep_bucket:.3f}_" not in f.name:
            _delete_file(f, reason="orphaned")


async def extract_subtitle(msg_id: int, secure_hash: str, stream_index: int, start_time: float = 0.0):
    """
    Extract one windowed chunk of a subtitle stream and return a live
    streaming generator — same contract as extract_audio_stream. Playback
    is never blocked waiting for the whole file, and switching windows
    (seeking) kills/cleans up the previous in-flight job instead of piling up.
    """
    bucket_start = _round_start(start_time)
    key = (msg_id, stream_index, bucket_start)

    _destroy_previous(msg_id, stream_index, bucket_start)

    existing = _inflight.get(key)
    if existing is not None:
        return _stream_from_inflight(existing)

    entry = _meta(msg_id)
    if entry.get("tracks") is None:
        try:
            await get_subtitle_tracks(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[SubtitleTracks] get_subtitle_tracks before extract failed: {e}")

    codec = _codec_for_stream(msg_id, stream_index)
    logging.info(
        f"[SubtitleTracks] extract msg={msg_id} stream={stream_index} "
        f"codec={codec or 'unknown'} start={bucket_start:.3f}s window={SUBTITLE_WINDOW_SECONDS}s"
    )

    await _get_file_id(msg_id, secure_hash)
    lock = _extract_locks.setdefault(key, asyncio.Lock())

    async with lock:
        existing = _inflight.get(key)
        if existing is not None:
            return _stream_from_inflight(existing)

        internal_url = _internal_stream_url(msg_id, secure_hash)
        tmp_out = _temp_sub_path(msg_id, stream_index, bucket_start)
        cmd = _build_ffmpeg_sub_cmd(internal_url, stream_index, bucket_start, tmp_out)

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )

        job = {
            "msg_id": msg_id,
            "stream_index": stream_index,
            "bucket_start": bucket_start,
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
                logging.error(f"[SubtitleTracks] ffmpeg extraction failed: {stderr[:400]!r}")
            job["done"].set()

        asyncio.create_task(finalize())

    return _stream_from_inflight(job)


def cancel_inflight(msg_id: int, stream_index: int, start_time: float = None):
    for key, job in list(_inflight.items()):
        m, s, b = key
        if m != msg_id or s != stream_index:
            continue
        if start_time is not None and b != _round_start(start_time):
            continue
        _kill_job(job, reason="cancel_inflight")
        _delete_file(job["tmp_out"], reason="cancel_inflight")
        _inflight.pop(key, None)


def _stream_from_inflight(job: dict):
    """
    Live-tail the .vtt file as ffmpeg writes it — exactly like the audio
    generator. The player gets the WEBVTT header and early cues as soon as
    they land on disk instead of waiting for the whole window to finish.
    """
    key = (job["msg_id"], job["stream_index"], job["bucket_start"])

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
                    if tmp_out.exists():
                        with open(tmp_out, "rb") as f:
                            f.seek(sent)
                            remaining = f.read()
                        if remaining:
                            yield remaining
                    break

                await asyncio.sleep(0.12)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            logging.info("[SubtitleTracks] client disconnected mid-stream")
        except Exception as e:
            logging.error(f"[SubtitleTracks] live-stream read error: {e}")
        finally:
            _inflight.pop(key, None)
            _delete_file(job["tmp_out"], reason="sent")

    return generator()


def _dir_free_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


async def cleanup_subtitle_cache():
    now = time.time()
    files = sorted(TEMP_DIR.glob("*"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    for f in list(files):
        try:
            if f.exists() and now - f.stat().st_mtime > CACHE_MAX_AGE_SECONDS:
                _delete_file(f, reason="stale-safety-net")
        except FileNotFoundError:
            pass

    files = sorted(TEMP_DIR.glob("*"), key=lambda p: p.stat().st_mtime if p.exists() else 0)
    while files and _dir_free_bytes(TEMP_DIR) < CACHE_MIN_FREE_BYTES:
        oldest = files.pop(0)
        try:
            if oldest.exists():
                _delete_file(oldest, reason="low-disk-safety-net")
        except FileNotFoundError:
            pass


async def start_subtitle_cleanup_loop(interval_seconds: int = 900):
    while True:
        try:
            await cleanup_subtitle_cache()
        except Exception as e:
            logging.error(f"[SubtitleTracks] cleanup loop error: {e}")
        await asyncio.sleep(interval_seconds)

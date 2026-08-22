# Deendayal_botz/util/audio_tracks.py

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

# Port your aiohttp server is actually listening on. If your info.py
# exposes a different name for this (e.g. WEB_SERVER_PORT), change the
# import below to match — this MUST be the same port stream_handler
# is bound to, since ffmpeg will hit http://127.0.0.1:<this>/... directly.
from info import PORT as LOCAL_PORT
LOCAL_PORT = int(LOCAL_PORT)

TEMP_DIR = Path("/tmp/audio_cache")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

_file_cache = {}
_extract_locks = {}
_inflight = {}

CACHE_MAX_AGE_SECONDS = 12 * 60 * 60
CACHE_MIN_FREE_BYTES = 1 * 1024 ** 3
SEEK_BUCKET_SECONDS = 10

# Codecs ffmpeg can stream-copy without re-encoding
COPYABLE_AUDIO = {"aac", "mp4a"}
COPYABLE_VIDEO = {"h264", "hevc", "h265", "vp9", "av1"}


def _round_start(start_time: float) -> int:
    if not start_time or start_time < 0:
        return 0
    return int(round(start_time / SEEK_BUCKET_SECONDS) * SEEK_BUCKET_SECONDS)


def _meta(msg_id: int) -> dict:
    return _file_cache.setdefault(msg_id, {"tracks": None, "video_codec": None})


async def _get_file_id(msg_id: int, secure_hash: str):
    """Validates the hash and confirms the file exists before we ask
    ffmpeg/ffprobe to go fetch it — lets us fail fast with the right
    HTTP error (403/404) instead of an opaque ffmpeg failure."""
    file_id = await get_file_ids(DeendayalBot, LOG_CHANNEL, msg_id)
    if not file_id:
        raise FIleNotFound
    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash
    return file_id


def _internal_stream_url(msg_id: int, secure_hash: str) -> str:
    """Points at your own stream_handler route (the one that already
    serves Range requests). ffmpeg reads from this like a browser would —
    it can jump straight to a byte offset instead of reading from the
    start. Must match the path shape parse_id_hash() expects:
    <6-char hash><numeric id>, no separator."""
    return f"http://127.0.0.1:{LOCAL_PORT}/{secure_hash}{msg_id}"


def _codec_for_stream(msg_id: int, stream_index: int) -> str:
    tracks = _meta(msg_id).get("tracks") or []
    for t in tracks:
        if t.get("index") == stream_index:
            return (t.get("codec_name") or "").lower()
    return ""


async def get_tracks(msg_id: int, secure_hash: str) -> list:
    """Lists audio tracks (index/codec/language/title) so the frontend
    can build the language picker."""
    entry = _meta(msg_id)
    if entry["tracks"] is not None:
        return entry["tracks"]

    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "a",
        "-timeout", "15000000",  # 15s, in microseconds
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
        logging.error(f"[Tracks] ffprobe parse failed: {e} | stderr={stderr[:300]!r}")
        tracks = []

    entry["tracks"] = tracks
    return tracks


async def get_video_codec(msg_id: int, secure_hash: str) -> str:
    """Cheap ffprobe for just the video stream's codec, cached per file.
    Used to decide whether the video can be stream-copied (fast) or
    needs re-encoding when muxing a non-default audio track."""
    entry = _meta(msg_id)
    if entry.get("video_codec") is not None:
        return entry["video_codec"]

    await _get_file_id(msg_id, secure_hash)
    internal_url = _internal_stream_url(msg_id, secure_hash)

    process = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-select_streams", "v",
        "-timeout", "15000000",
        "-i", internal_url,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()

    codec = ""
    try:
        data = json.loads(stdout)
        streams = data.get("streams", [])
        if streams:
            codec = (streams[0].get("codec_name") or "").lower()
    except Exception as e:
        logging.error(f"[VideoTrack] ffprobe video codec failed: {e} | stderr={stderr[:300]!r}")

    entry["video_codec"] = codec
    return codec


def _cached_mux_path(msg_id: int, stream_index: int, bucket_start: int) -> Path:
    if bucket_start <= 0:
        return TEMP_DIR / f"{msg_id}_mux{stream_index}.mp4"
    return TEMP_DIR / f"{msg_id}_mux{stream_index}_seek{bucket_start}.mp4"


async def extract_muxed_stream(msg_id: int, secure_hash: str, stream_index: int, start_time: float = 0.0):
    """
    Muxes VIDEO + the selected audio track together into one fragmented-MP4
    file starting at start_time (bucketed to SEEK_BUCKET_SECONDS). Both
    tracks come from the same ffmpeg process and share one output timeline,
    so there's no audio/video drift the way there would be with two
    separately-synced elements.

    If both video and the chosen audio track are stream-copyable, this is
    cheap (no re-encode) — same cost class as your old audio-only extraction.
    """
    bucket_start = _round_start(start_time)
    cache_path = _cached_mux_path(msg_id, stream_index, bucket_start)
    if cache_path.exists():
        return _stream_from_cached_file(cache_path)

    key = ("mux", msg_id, stream_index, bucket_start)

    existing = _inflight.get(key)
    if existing is not None:
        return _stream_from_inflight(existing)

    entry = _meta(msg_id)
    if entry.get("tracks") is None:
        try:
            await get_tracks(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[VideoTrack] get_tracks before mux failed: {e}")

    audio_codec = _codec_for_stream(msg_id, stream_index)
    video_codec = await get_video_codec(msg_id, secure_hash)

    audio_copy = audio_codec in COPYABLE_AUDIO
    video_copy = video_codec in COPYABLE_VIDEO

    logging.info(
        f"[VideoTrack] mux msg={msg_id} stream={stream_index} "
        f"vcodec={video_codec or 'unknown'}({'copy' if video_copy else 'encode'}) "
        f"acodec={audio_codec or 'unknown'}({'copy' if audio_copy else 'encode'}) "
        f"start={bucket_start}s"
    )

    await _get_file_id(msg_id, secure_hash)
    lock = _extract_locks.setdefault(key, asyncio.Lock())

    async with lock:
        if cache_path.exists():
            return _stream_from_cached_file(cache_path)
        existing = _inflight.get(key)
        if existing is not None:
            return _stream_from_inflight(existing)

        internal_url = _internal_stream_url(msg_id, secure_hash)
        tmp_out = cache_path.with_suffix(".part.mp4")

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
            "-seekable", "1",
            "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
            "-timeout", "15000000",
        ]
        if bucket_start > 0:
            # -ss BEFORE -i: ffmpeg seeks in the source before decoding,
            # letting it jump near this byte offset instead of decoding
            # everything from the start.
            cmd += ["-ss", str(bucket_start)]

        cmd += [
            "-probesize", "2M", "-analyzeduration", "2M",
            "-i", internal_url,
            "-map", "0:v:0",
            "-map", f"0:{stream_index}",
        ]

        cmd += ["-c:v", "copy" if video_copy else "libx264"]
        if not video_copy:
            cmd += ["-preset", "veryfast", "-crf", "23"]

        cmd += ["-c:a", "copy" if audio_copy else "aac"]
        if not audio_copy:
            cmd += ["-b:a", "128k"]

        cmd += [
            "-f", "mp4",
            "-movflags", "frag_keyframe+empty_moov+faststart",
            str(tmp_out),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )

        job = {
            "tmp_out": tmp_out,
            "cache_path": cache_path,
            "proc": proc,
            "done": asyncio.Event(),
            "ok": False,
        }
        _inflight[key] = job

        async def finalize():
            _, stderr = await proc.communicate()
            ok = proc.returncode == 0 and tmp_out.exists() and tmp_out.stat().st_size > 0
            if ok:
                try:
                    tmp_out.rename(cache_path)
                    job["ok"] = True
                except Exception as e:
                    logging.error(f"[VideoTrack] cache rename failed: {e}")
                    job["ok"] = False
            else:
                logging.error(f"[VideoTrack] ffmpeg mux failed: {stderr[:300]!r}")
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


def cancel_inflight(msg_id: int, stream_index: int, start_time: float = None):
    for k, job in list(_inflight.items()):
        if len(k) == 4:
            _, m, s, b = k  # mux key
        else:
            m, s, b = k
        if m != msg_id or s != stream_index:
            continue
        if start_time is not None and b != _round_start(start_time):
            continue
        try:
            if job["proc"].returncode is None:
                job["proc"].kill()
        except Exception:
            pass


def _stream_from_inflight(job: dict):
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
                    if job["ok"] and job["cache_path"].exists():
                        with open(job["cache_path"], "rb") as f:
                            f.seek(sent)
                            rest = f.read()
                        if rest:
                            yield rest
                    break
                await asyncio.sleep(0.15)
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            logging.info("[VideoTrack] client disconnected mid-stream (live extraction)")
        except Exception as e:
            logging.error(f"[VideoTrack] live-stream read error: {e}")

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
            logging.info(f"[VideoTrack] client disconnected mid-stream: {path.name}")
        except Exception as e:
            logging.error(f"[VideoTrack] read error on {path.name}: {e}")
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
            logging.error(f"[VideoTrack] cleanup loop error: {e}")
        await asyncio.sleep(interval_seconds)

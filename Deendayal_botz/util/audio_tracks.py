# Live mux from T: video copy + audio (copy if AAC). No disk cache.

import json
import asyncio
import logging

from aiohttp import web

from info import LOG_CHANNEL, PORT
from Deendayal_botz.Bot import DeendayalBot
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

LOCAL_PORT = int(PORT)
COPYABLE_AUDIO = {"aac", "mp4a", "mp4a.40.2"}
PROBE_TIMEOUT_SECONDS = 40
MAX_CONCURRENT_MUX = 3
_mux_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MUX)
_file_cache = {}
CACHE_MAX_ENTRIES = 500


def _meta(msg_id: int) -> dict:
    entry = _file_cache.get(msg_id)
    if entry is None:
        if len(_file_cache) >= CACHE_MAX_ENTRIES:
            for key in list(_file_cache.keys())[: CACHE_MAX_ENTRIES // 4]:
                _file_cache.pop(key, None)
        entry = {"tracks": None, "duration": 0.0}
        _file_cache[msg_id] = entry
    return entry


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
    return (await get_track_info(msg_id, secure_hash)).get("tracks", [])


async def mux_av_stream(
    request: web.Request,
    msg_id: int,
    secure_hash: str,
    stream_index: int,
    start_time: float = 0.0,
):
    await _get_file_id(msg_id, secure_hash)

    entry = _meta(msg_id)
    if not entry.get("tracks"):
        try:
            await get_track_info(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[Mux] probe failed: {e}")

    codec = _codec_for_stream(msg_id, stream_index)
    use_copy = codec in COPYABLE_AUDIO
    internal_url = _internal_stream_url(msg_id, secure_hash)

    logging.info(
        f"[Mux] LIVE msg={msg_id} audio={stream_index} codec={codec or '?'} "
        f"mode={'copy' if use_copy else 'encode'} start={start_time:.2f}s"
    )

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
        "-seekable", "1",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-rw_timeout", "30000000",
    ]
    if start_time > 0:
        cmd += ["-ss", f"{start_time:.3f}"]

    cmd += [
        "-i", internal_url,
        "-map", "0:v:0",
        "-map", f"0:{stream_index}",
        "-c:v", "copy",
    ]
    if use_copy:
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-c:a", "aac", "-b:a", "128k", "-ac", "2"]

    cmd += [
        "-avoid_negative_ts", "make_zero",
        "-f", "mp4",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "pipe:1",
    ]

    async with _mux_semaphore:
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
                "X-Accel-Buffering": "no",
            },
        )
        try:
            await resp.prepare(request)
            while True:
                chunk = await proc.stdout.read(64 * 1024)
                if not chunk:
                    break
                try:
                    await resp.write(chunk)
                except (ConnectionResetError, ConnectionError, BrokenPipeError, asyncio.CancelledError):
                    break
                if request.transport is None or request.transport.is_closing():
                    break
        finally:
            if proc.returncode is None:
                try:
                    proc.kill()
                except Exception:
                    pass
            try:
                _, err = await asyncio.wait_for(proc.communicate(), timeout=5)
                if proc.returncode not in (0, None, -9, 255):
                    logging.error(f"[Mux] ffmpeg exit={proc.returncode} err={err[:300]!r}")
            except Exception:
                pass

    return resp


async def cleanup_audio_cache():
    return None


async def start_cache_cleanup_loop(interval_seconds: int = 3600):
    while True:
        await asyncio.sleep(interval_seconds)

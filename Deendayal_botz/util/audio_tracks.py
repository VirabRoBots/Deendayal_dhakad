# Deendayal_botz/util/audio_tracks.py
#
# Fast, cancellable audio-track muxing for the Deendayal stream player.
#
# Key ideas:
#   * /api/tracks  -> audio stream list + real container duration (cached)
#   * /mux/<i>/... -> fragmented MP4 (video copy + chosen audio) starting at ?t=
#   * the browser switching source drops the socket; we detect that and kill
#     ffmpeg immediately so bandwidth goes to the new segment, not the old one.

import json
import asyncio
import logging

from aiohttp import web

from info import LOG_CHANNEL, PORT
from Deendayal_botz.Bot import DeendayalBot
from Deendayal_botz.util.file_properties import get_file_ids
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash

LOCAL_PORT = int(PORT)

# Audio codecs that browsers can play inside MP4 without re-encoding.
COPYABLE_AUDIO = {"aac", "mp4a", "mp4a.40.2"}

# Cap simultaneous ffmpeg jobs so rapid seeking can't spawn a dozen transcodes.
MAX_CONCURRENT_MUX = 4
_mux_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MUX)

# In-memory metadata cache: msg_id -> {"tracks": [...] | None, "duration": float}
_file_cache: dict = {}
CACHE_MAX_ENTRIES = 500

# How long ffprobe may spend opening the remote stream.
PROBE_TIMEOUT_SECONDS = 40


def _meta(msg_id: int) -> dict:
    entry = _file_cache.get(msg_id)
    if entry is None:
        # Cheap bound so the cache can't grow forever on a long-running bot.
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
    tracks = _meta(msg_id).get("tracks") or []
    for track in tracks:
        if track.get("index") == stream_index:
            return (track.get("codec_name") or "").lower()
    return ""


async def _probe(msg_id: int, secure_hash: str) -> dict:
    """Run ffprobe once for audio streams + container duration."""
    internal_url = _internal_stream_url(msg_id, secure_hash)

    process = await asyncio.create_subprocess_exec(
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
            process.communicate(), timeout=PROBE_TIMEOUT_SECONDS
        )
    except asyncio.TimeoutError:
        try:
            process.kill()
        except Exception:
            pass
        logging.warning(f"[AudioTracks] ffprobe timed out for msg={msg_id}")
        return {"tracks": [], "duration": 0.0}

    tracks = []
    duration = 0.0
    try:
        data = json.loads(stdout or b"{}")

        for i, stream in enumerate(data.get("streams", [])):
            tags = stream.get("tags", {}) or {}
            tracks.append(
                {
                    "index": stream.get("index", i),
                    "codec_name": stream.get("codec_name", "unknown"),
                    "tags": {
                        "language": tags.get("language", tags.get("LANGUAGE", "")),
                        "title": tags.get("title", tags.get("TITLE", "")),
                        "handler_name": tags.get("handler_name", ""),
                    },
                }
            )

        raw_duration = (data.get("format") or {}).get("duration")
        if raw_duration:
            try:
                duration = max(0.0, float(raw_duration))
            except (TypeError, ValueError):
                duration = 0.0
    except Exception as e:
        logging.error(
            f"[AudioTracks] ffprobe parse failed: {e} | stderr={stderr[:300]!r}"
        )
        return {"tracks": [], "duration": 0.0}

    return {"tracks": tracks, "duration": duration}


async def get_track_info(msg_id: int, secure_hash: str) -> dict:
    """Return {"tracks": [...], "duration": seconds}. Only caches useful results."""
    entry = _meta(msg_id)
    if entry["tracks"]:
        return {"tracks": entry["tracks"], "duration": entry.get("duration", 0.0)}

    await _get_file_id(msg_id, secure_hash)
    probed = await _probe(msg_id, secure_hash)

    # A failed probe must NOT be cached, otherwise one transient error hides the
    # language chips for this file forever.
    if probed["tracks"]:
        entry["tracks"] = probed["tracks"]
        entry["duration"] = probed["duration"]

    return probed


async def get_tracks(msg_id: int, secure_hash: str) -> list:
    """Backwards-compatible helper: audio stream list only."""
    info = await get_track_info(msg_id, secure_hash)
    return info.get("tracks", [])


def _build_mux_cmd(internal_url: str, stream_index: int, start_time: float, use_copy: bool):
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-nostdin",
        "-seekable", "1",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
        "-rw_timeout", "30000000",
    ]

    # -ss BEFORE -i => fast input seek, ffmpeg only range-fetches from there.
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
        "-fflags", "+nobuffer+genpts",
        "-flush_packets", "1",
        "-max_delay", "0",
        "-f", "mp4",
        # frag_every_frame removed on purpose: one fragment per frame is huge
        # overhead and delays the first playable bytes.
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        "pipe:1",
    ]
    return cmd


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
    if not entry.get("tracks"):
        try:
            await get_track_info(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[Mux] track probe failed: {e}")

    codec = _codec_for_stream(msg_id, stream_index)
    use_copy = codec in COPYABLE_AUDIO
    internal_url = _internal_stream_url(msg_id, secure_hash)

    logging.info(
        f"[Mux] msg={msg_id} audio={stream_index} codec={codec or '?'} "
        f"mode={'copy' if use_copy else 'encode'} start={start_time:.2f}s"
    )

    async with _mux_semaphore:
        proc = await asyncio.create_subprocess_exec(
            *_build_mux_cmd(internal_url, stream_index, start_time, use_copy),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=1024 * 1024,
        )

        resp = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "video/mp4",
                "Cache-Control": "no-store",
                "Accept-Ranges": "none",
                # Hint to any proxy in front: do not buffer, we want first bytes out.
                "X-Accel-Buffering": "no",
                "X-Segment-Start": f"{start_time:.3f}",
            },
        )

        try:
            await resp.prepare(request)

            while True:
                chunk = await proc.stdout.read(32 * 1024)
                if not chunk:
                    break
                await resp.write(chunk)
                # Client vanished (source switch / seek) -> stop immediately.
                if request.transport is None or request.transport.is_closing():
                    break
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            # Normal when the player switches track or seeks.
            pass
        except Exception as e:
            logging.warning(f"[Mux] stream aborted: {e}")
        finally:
            await _terminate(proc)

    return resp


async def _terminate(proc) -> None:
    """Kill ffmpeg right away and drain its stderr without blocking."""
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


# --------------------------------------------------------------------------
# Compatibility shims.
#
# The old implementation kept a /tmp/audio_cache directory and an hourly
# cleanup loop, but nothing ever wrote files into it (muxing is fully
# streamed through a pipe). These remain as harmless no-ops so existing
# imports / startup tasks keep working.
# --------------------------------------------------------------------------


async def cleanup_audio_cache():
    return None


async def start_cache_cleanup_loop(interval_seconds: int = 3600):
    while True:
        await asyncio.sleep(interval_seconds)

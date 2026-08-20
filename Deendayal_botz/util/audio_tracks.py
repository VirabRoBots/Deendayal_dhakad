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

# How long to wait, at most, for new bytes to appear in a growing file before
# giving up and telling the client there's nothing more right now.
GROW_WAIT_TIMEOUT = 20.0
GROW_POLL_INTERVAL = 0.2
# How many bytes of "safety margin" behind the writer we insist exist before
# serving them (avoids serving a half-written moof/frame boundary).
GROW_SAFETY_MARGIN = 32 * 1024


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


# ---------------------------------------------------------------------------
# Build-session tracking
# ---------------------------------------------------------------------------
# One "session" = one ffmpeg process currently writing (or having written) a
# file for a given (msg_id, stream_index, start_time). Kept in memory so
# concurrent requests for the same in-progress build can share it instead of
# spawning duplicate ffmpeg processes.

_sessions = {}  # key -> BuildSession
_sessions_lock = asyncio.Lock()


class BuildSession:
    def __init__(self, msg_id, stream_index, start_time, path, is_full_build):
        self.msg_id = msg_id
        self.stream_index = stream_index
        self.start_time = start_time
        self.path = path
        self.is_full_build = is_full_build  # True only if start_time == 0
        self.process = None
        self.done = False
        self.failed = False
        self.written_bytes = 0
        self.lock = asyncio.Lock()

    def key(self):
        return (self.msg_id, self.stream_index, self.start_time)


def _cache_key(msg_id: int, stream_index: int) -> str:
    return f"{msg_id}_{stream_index}.mp4"


def _cache_path(msg_id: int, stream_index: int) -> Path:
    return TEMP_DIR / _cache_key(msg_id, stream_index)


def _ephemeral_path(msg_id: int, stream_index: int, start_time: float) -> Path:
    return TEMP_DIR / f"{msg_id}_{stream_index}_{int(start_time)}.part.mp4"


async def _start_build(msg_id, secure_hash, stream_index, start_time) -> "BuildSession":
    """Kick off ffmpeg writing to a file. Returns the BuildSession (already
    registered) whether newly created or reused from a concurrent request."""
    is_full_build = start_time <= 0.01
    target_path = _cache_path(msg_id, stream_index) if is_full_build else _ephemeral_path(
        msg_id, stream_index, start_time
    )

    session_key = (msg_id, stream_index, round(start_time, 1))

    async with _sessions_lock:
        existing = _sessions.get(session_key)
        if existing is not None and not existing.failed:
            return existing

        session = BuildSession(msg_id, stream_index, start_time, target_path, is_full_build)
        _sessions[session_key] = session

    codec = _codec_for_stream(msg_id, stream_index)
    use_copy = codec in COPYABLE_AUDIO
    internal_url = _internal_stream_url(msg_id, secure_hash)

    logging.info(
        f"[Mux] BUILD msg={msg_id} track={stream_index} start={start_time}s "
        f"full={is_full_build} mode={'copy' if use_copy else 'encode'} -> {target_path}"
    )

    if target_path.exists():
        try:
            target_path.unlink()
        except FileNotFoundError:
            pass

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
        "-seekable", "1",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-timeout", "30000000",
    ]
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
        "-movflags", "frag_keyframe+empty_moov+default_base_moof",
        str(target_path),
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    session.process = proc

    asyncio.create_task(_watch_build(session))
    return session


async def _watch_build(session: "BuildSession"):
    """Waits for ffmpeg to exit, updates session state, cleans up on failure."""
    try:
        _, err = await session.process.communicate()
        rc = session.process.returncode
        if rc not in (0, None):
            session.failed = True
            logging.error(f"[Mux] build failed rc={rc} err={err[:300]!r} path={session.path}")
            try:
                if session.path.exists():
                    session.path.unlink()
            except FileNotFoundError:
                pass
        else:
            session.done = True
            logging.info(f"[Mux] build complete: {session.path}")
    except Exception as e:
        session.failed = True
        logging.error(f"[Mux] build watcher error: {e}")
    finally:
        async with _sessions_lock:
            key = session.key()
            # Only drop from the in-progress registry; the file itself
            # (if it's a completed full build) stays on disk as the cache.
            if _sessions.get((session.msg_id, session.stream_index, round(session.start_time, 1))) is session:
                del _sessions[(session.msg_id, session.stream_index, round(session.start_time, 1))]


def cached_full_file_exists(msg_id: int, stream_index: int) -> Path | None:
    p = _cache_path(msg_id, stream_index)
    return p if p.exists() else None


async def _wait_for_bytes(path: Path, needed: int, session: "BuildSession" = None) -> int:
    """Poll file size until it has at least `needed` bytes, the build finished,
    the build failed, or we time out. Returns the current file size."""
    waited = 0.0
    while True:
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            size = 0

        if size >= needed:
            return size
        if session is not None and (session.done or session.failed):
            return size
        if waited >= GROW_WAIT_TIMEOUT:
            return size

        await asyncio.sleep(GROW_POLL_INTERVAL)
        waited += GROW_POLL_INTERVAL


async def serve_track(request: web.Request, msg_id: int, secure_hash: str,
                       stream_index: int, start_time: float = 0.0) -> web.StreamResponse:
    """Main entry point used by the /mux route. Decides whether to serve an
    already-cached complete file (with Range support, fully seekable) or to
    start/reuse a build and stream from the growing file."""
    await _get_file_id(msg_id, secure_hash)

    entry = _meta(msg_id)
    if entry.get("tracks") is None:
        try:
            await get_tracks(msg_id, secure_hash)
        except Exception as e:
            logging.warning(f"[Mux] get_tracks failed: {e}")

    # 1) Fully-built cache already exists for this language -> serve it like
    #    a normal seekable file (same behavior as the default track).
    cached = cached_full_file_exists(msg_id, stream_index)
    if cached is not None:
        return await _serve_complete_file(request, cached)

    # 2) Otherwise start (or join) a build and stream from the growing file.
    session = await _start_build(msg_id, secure_hash, stream_index, start_time)

    return await _serve_growing_file(request, session)


async def _serve_complete_file(request: web.Request, path: Path) -> web.Response:
    file_size = path.stat().st_size
    range_header = request.headers.get("Range", 0)

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    until_bytes = min(until_bytes, file_size - 1)
    if from_bytes < 0 or until_bytes < from_bytes:
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    length = until_bytes - from_bytes + 1

    async def _reader():
        with open(path, "rb") as f:
            f.seek(from_bytes)
            remaining = length
            chunk = 1024 * 1024
            while remaining > 0:
                data = f.read(min(chunk, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

    return web.Response(
        status=206 if range_header else 200,
        body=_reader(),
        headers={
            "Content-Type": "video/mp4",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(length),
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-store",
        },
    )


async def _serve_growing_file(request: web.Request, session: "BuildSession") -> web.StreamResponse:
    """Streams a file that ffmpeg is actively writing. No hard Content-Length
    (we don't know the final size yet) -- served as a plain chunked stream,
    similar in spirit to the old live-pipe behavior, but reading from disk so
    a second concurrent viewer of the same session doesn't need a second
    ffmpeg process."""
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "video/mp4",
            "Cache-Control": "no-store",
            "Accept-Ranges": "none",
        },
    )
    await resp.prepare(request)

    sent = 0
    try:
        with open(session.path, "rb") as f:
            while True:
                target = sent + (256 * 1024)
                available = await _wait_for_bytes(session.path, target, session)
                readable_to = max(0, available - GROW_SAFETY_MARGIN) if not session.done else available

                if readable_to <= sent:
                    if session.done or session.failed:
                        break
                    # Timed out waiting for more data; end this response,
                    # client (Plyr) will treat it like the mux ended.
                    break

                f.seek(sent)
                data = f.read(readable_to - sent)
                if not data:
                    break
                await resp.write(data)
                sent += len(data)
    except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
        pass
    except Exception as e:
        logging.exception(f"[Mux] serve_growing_file error: {e}")

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
    # Also always clear ephemeral (.part.mp4) files older than 1 hour --
    # they were never meant to be kept long-term.
    for f in TEMP_DIR.glob("*.part.mp4"):
        try:
            if f.exists() and now - f.stat().st_mtime > 3600:
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

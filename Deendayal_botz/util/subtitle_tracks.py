# Add this to Deendayal_botz/util/subtitle_tracks.py
# This is a SEPARATE, simpler code path from the existing windowed
# extract_subtitle() — it fetches the ENTIRE subtitle track in one ffmpeg
# call, no -ss/-t windowing. Subtitle files are plain text and small
# (a full 2hr movie's subs are typically 40-150KB), so there's no real
# benefit to chunking them the way audio/video streams are chunked.
#
# This removes the entire class of bugs the windowed approach had:
# - no relative-vs-absolute timestamp ambiguity
# - no reshifting on every seek
# - no window-boundary refetching
# - works correctly from t=0 all the way to the end, always

FULL_SUB_DIR = Path("/tmp/subtitle_full_cache")
FULL_SUB_DIR.mkdir(parents=True, exist_ok=True)

_full_sub_locks = {}


def _full_sub_path(msg_id: int, stream_index: int) -> Path:
    return FULL_SUB_DIR / f"{msg_id}_full_t{stream_index}.vtt"


async def extract_full_subtitle(msg_id: int, secure_hash: str, stream_index: int) -> Path:
    """
    Extract an ENTIRE subtitle stream in a single ffmpeg pass and return
    the path to the finished .vtt file. Cached on disk — repeat requests
    for the same (msg_id, stream_index) reuse the file instead of
    re-running ffmpeg.
    """
    out_path = _full_sub_path(msg_id, stream_index)

    # Cache hit — reuse existing extracted file
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path

    lock = _full_sub_locks.setdefault((msg_id, stream_index), asyncio.Lock())
    async with lock:
        # Re-check after acquiring lock in case another request just finished it
        if out_path.exists() and out_path.stat().st_size > 0:
            return out_path

        await _get_file_id(msg_id, secure_hash)
        internal_url = _internal_stream_url(msg_id, secure_hash)

        codec = _codec_for_stream(msg_id, stream_index)
        logging.info(
            f"[FullSubtitle] extracting full track msg={msg_id} "
            f"stream={stream_index} codec={codec or 'unknown'}"
        )

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-nostdin",
            "-timeout", "15000000",
            "-i", internal_url,
            "-map", f"0:{stream_index}",
            "-c:s", "webvtt", "-f", "webvtt", str(out_path),
        ]

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()

        ok = process.returncode == 0 and out_path.exists() and out_path.stat().st_size > 0
        if not ok:
            if out_path.exists():
                _delete_file(out_path, reason="failed-extraction")
            logging.error(f"[FullSubtitle] ffmpeg extraction failed: {stderr[:400]!r}")
            raise RuntimeError(f"subtitle extraction failed for stream {stream_index}")

        return out_path


# ---- Wire this into your route file (e.g. Deendayal_botz/server/routes.py) ----
#
# from Deendayal_botz.util.subtitle_tracks import extract_full_subtitle
#
# @routes.get("/subs/{stream_index}/{msg_id}/{filename}")
# async def full_subtitle_route(request):
#     msg_id = int(request.match_info["msg_id"])
#     stream_index = int(request.match_info["stream_index"])
#     secure_hash = request.query.get("hash")
#     try:
#         path = await extract_full_subtitle(msg_id, secure_hash, stream_index)
#     except Exception as e:
#         return web.json_response({"error": str(e)}, status=500)
#     return web.FileResponse(
#         path,
#         headers={"Content-Type": "text/vtt; charset=utf-8"},
#     )
#
# Adjust the decorator/response types to match whatever web framework
# your server actually uses (aiohttp shown above as an example — swap
# for FastAPI/Starlette/Flask equivalents if that's what you're on).

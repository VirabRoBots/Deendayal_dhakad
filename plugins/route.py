from aiohttp import web
import re
import math
import logging
import secrets
import mimetypes
from aiohttp.http_exceptions import BadStatusLine
from Deendayal_botz.Bot import multi_clients, work_loads
from Deendayal_botz.server.exceptions import FIleNotFound, InvalidHash
from Deendayal_botz.util.custom_dl import ByteStreamer
from Deendayal_botz.util.render_template import render_page
from Deendayal_botz.util.audio_tracks import get_track_info, mux_av_stream
from info import *

routes = web.RouteTableDef()

_COMBINED_RE = re.compile(r"^([a-zA-Z0-9_-]{6})(\d+)")
_ID_FIRST_SEGMENT_RE = re.compile(r"^(\d+)")


def parse_id_hash(path: str, request: web.Request):
    first_segment = path.split("/", 1)[0]
    match = _COMBINED_RE.match(first_segment)
    if match:
        return int(match.group(2)), match.group(1)
    match = _ID_FIRST_SEGMENT_RE.match(first_segment)
    if not match:
        raise web.HTTPBadRequest(text="Invalid path")
    secure_hash = request.rel_url.query.get("hash")
    if not secure_hash:
        raise web.HTTPBadRequest(text="Missing hash")
    return int(match.group(1)), secure_hash


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response("Deendayal_Botz")


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def watch_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        id, secure_hash = parse_id_hash(path, request)
        return web.Response(
            text=await render_page(id, secure_hash),
            content_type="text/html",
        )
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=499, text="")
    except web.HTTPException:
        raise
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/api/tracks/{path:\S+}", allow_head=True)
async def tracks_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        id, secure_hash = parse_id_hash(path, request)
        info = await get_track_info(id, secure_hash)
        return web.json_response(
            {
                "tracks": info.get("tracks", []),
                "duration": info.get("duration", 0.0),
            },
            headers={"Cache-Control": "no-store"},
        )
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except web.HTTPException:
        raise
    except Exception as e:
        logging.exception("tracks_handler error")
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/mux/{stream_index:\d+}/{path:\S+}", allow_head=True)
async def mux_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        stream_index = int(request.match_info["stream_index"])
        id, secure_hash = parse_id_hash(path, request)
        start_time = 0.0
        raw_t = request.rel_url.query.get("t")
        if raw_t:
            try:
                start_time = max(0.0, float(raw_t))
            except ValueError:
                start_time = 0.0
        return await mux_av_stream(request, id, secure_hash, stream_index, start_time)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (ConnectionResetError, BrokenPipeError, ConnectionError):
        return web.Response(status=499, text="")
    except web.HTTPException:
        raise
    except Exception as e:
        logging.exception("mux_handler error")
        raise web.HTTPInternalServerError(text=str(e))


@routes.get(r"/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        id, secure_hash = parse_id_hash(path, request)
        return await media_streamer(request, id, secure_hash)
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        return web.Response(status=499, text="")
    except web.HTTPException:
        raise
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))


class_cache = {}


def _parse_range(range_header: str, file_size: int):
    if not range_header:
        return None
    match = re.match(r"^\s*bytes\s*=\s*(\d*)\s*-\s*(\d*)\s*$", range_header)
    if not match:
        return None
    start_raw, end_raw = match.group(1), match.group(2)
    if not start_raw and not end_raw:
        return None
    if not start_raw:
        length = int(end_raw)
        if length <= 0:
            return None
        from_bytes = max(0, file_size - length)
        until_bytes = file_size - 1
    else:
        from_bytes = int(start_raw)
        until_bytes = int(end_raw) if end_raw else file_size - 1
    return from_bytes, until_bytes


async def media_streamer(request: web.Request, id: int, secure_hash: str):
    range_header = request.headers.get("Range")
    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    if MULTI_CLIENT:
        logging.info(f"Client {index} is now serving {request.remote}")

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    file_id = await tg_connect.get_file_properties(id)
    if file_id.unique_id[:6] != secure_hash:
        raise InvalidHash

    file_size = file_id.file_size
    parsed = _parse_range(range_header, file_size) if range_header else None
    if range_header and parsed is None:
        return web.Response(
            status=416,
            text="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    if parsed:
        from_bytes, until_bytes = parsed
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    if (until_bytes >= file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            text="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)
    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1
    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    mime_type = file_id.mime_type
    file_name = file_id.file_name
    disposition = "inline"

    if mime_type:
        if not file_name:
            try:
                file_name = f"{secrets.token_hex(2)}.{mime_type.split('/')[1]}"
            except (IndexError, AttributeError):
                file_name = f"{secrets.token_hex(2)}.unknown"
    else:
        if file_name:
            mime_type = mimetypes.guess_type(file_id.file_name)[0] or "application/octet-stream"
        else:
            mime_type = "application/octet-stream"
            file_name = f"{secrets.token_hex(2)}.unknown"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )

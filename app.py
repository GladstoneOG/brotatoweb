import asyncio
import json
import os
from typing import Dict, Optional
import gradio as gr
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

FRAME_DATA = 1
FRAME_PEER_CONNECTED = 2
FRAME_PEER_DISCONNECTED = 3
FRAME_KICK_PEER = 4
FRAME_ROOM_INFO = 5
FRAME_HOST_ASSIGNED = 6
FRAME_REJECTED = 7

MAX_CLIENTS_PER_ROOM = 3  # 1 Host + 3 Clients = 4 Players

rooms: Dict[int, dict] = {}


def allocate_room_port(requested_port: int) -> int:
    p = requested_port if 1024 <= requested_port <= 65535 else 27462
    while p in rooms:
        p += 1
        if p > 65535:
            p = 27000
    return p


def make_frame(frame_type: int, peer_id: int, channel: int, payload: bytes = b"") -> bytes:
    header = bytes([
        frame_type & 0xFF,
        (peer_id >> 8) & 0xFF,
        peer_id & 0xFF,
        channel & 0xFF,
    ])
    return header + (payload or b"")


def get_room_list_snapshot() -> list:
    out = []
    for port, room in list(rooms.items()):
        if room.get("host_ws") is None:
            continue
        info = room.get("info") or {}
        clients: Dict[int, WebSocket] = room.get("clients") or {}
        member_count = int(info.get("member_count") or (1 + len(clients)))
        member_limit = int(info.get("member_limit") or 4)
        host_name = info.get("host_name") or "Player"
        out.append({
            "protocol": "brotato_online_lan",
            "protocol_version": 2,
            "host_name": host_name,
            "room_name": info.get("room_name") or f"{host_name}'s Room (#{port})",
            "game_port": port,
            "address": "relay",
            "member_count": member_count,
            "member_limit": member_limit,
            "state": info.get("state") or "character_selection",
            "mod_version": info.get("mod_version") or "6.3.0",
            "game_version": info.get("game_version") or "1.2.0.1",
            "host_mods_format": info.get("host_mods_format") or "1",
            "host_mods_payload": info.get("host_mods_payload") or "",
            "host_mods_too_large": bool(info.get("host_mods_too_large", False)),
            "joinable": bool(info["joinable"]) if "joinable" in info else (member_count < member_limit),
        })
    return out


app = FastAPI(title="Brotato Web Multiplayer Relay")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/info")
async def api_info():
    return JSONResponse(
        {
            "ok": True,
            "port": 443,
            "lanIps": [],
            "roomCount": len(rooms),
            "rooms": get_room_list_snapshot(),
        },
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/rooms")
async def api_rooms():
    return JSONResponse(
        {"rooms": get_room_list_snapshot()},
        headers={"Cache-Control": "no-store"},
    )


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    role = (ws.query_params.get("role") or "client").lower()
    try:
        requested_room = int(ws.query_params.get("room") or "27462")
    except ValueError:
        requested_room = 27462

    if role == "discover":
        try:
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                for entry in get_room_list_snapshot():
                    await ws.send_text(json.dumps(entry))
        except Exception:
            pass
        return

    if role == "host":
        room = rooms.get(requested_room)
        room_port = requested_room
        if room and room.get("host_ws") is not None:
            room_port = allocate_room_port(requested_room)
            room = None

        if room and room.get("cleanup_task"):
            room["cleanup_task"].cancel()
            room["cleanup_task"] = None
            room["host_ws"] = ws
        else:
            room = {
                "port": room_port,
                "host_ws": ws,
                "next_peer_id": 2,
                "clients": {},
                "info": {
                    "host_name": "Player",
                    "room_name": f"Room #{room_port}",
                    "game_port": room_port,
                    "member_count": 1,
                    "member_limit": 4,
                    "state": "character_selection",
                    "joinable": True,
                },
                "cleanup_task": None,
            }
            rooms[room_port] = room

        await ws.send_bytes(make_frame(FRAME_HOST_ASSIGNED, room_port, 0))

        try:
            while True:
                msg = await ws.receive()
                if msg.get("type") == "websocket.disconnect":
                    break
                if "text" in msg and msg["text"] is not None:
                    try:
                        parsed = json.loads(msg["text"])
                        if isinstance(parsed, dict):
                            room["info"].update(parsed)
                            room["info"]["game_port"] = room_port
                    except Exception:
                        pass
                    continue

                data = msg.get("bytes")
                if not data or len(data) < 4:
                    continue
                frame_type = data[0]
                target_peer = (data[1] << 8) | data[2]
                channel = data[3]
                payload = data[4:]

                if frame_type == FRAME_ROOM_INFO:
                    try:
                        parsed = json.loads(payload.decode("utf-8"))
                        if isinstance(parsed, dict):
                            room["info"].update(parsed)
                            room["info"]["game_port"] = room_port
                    except Exception:
                        pass
                    continue

                if frame_type == FRAME_KICK_PEER:
                    client_ws: Optional[WebSocket] = room["clients"].pop(target_peer, None)
                    if client_ws:
                        try:
                            await client_ws.close(code=4001)
                        except Exception:
                            pass
                    continue

                if frame_type == FRAME_DATA:
                    out_frame = make_frame(FRAME_DATA, 1, channel, payload)
                    if target_peer == 0:
                        for cws in list(room["clients"].values()):
                            try:
                                await cws.send_bytes(out_frame)
                            except Exception:
                                pass
                    else:
                        cws = room["clients"].get(target_peer)
                        if cws:
                            try:
                                await cws.send_bytes(out_frame)
                            except Exception:
                                pass
        except Exception:
            pass
        finally:
            if room.get("host_ws") is ws:
                room["host_ws"] = None

                async def delayed_cleanup():
                    await asyncio.sleep(6.0)
                    if room.get("host_ws") is None:
                        for cws in list(room["clients"].values()):
                            try:
                                await cws.close(code=4000)
                            except Exception:
                                pass
                        room["clients"].clear()
                        rooms.pop(room_port, None)

                room["cleanup_task"] = asyncio.create_task(delayed_cleanup())
        return

    # Client role
    room = rooms.get(requested_room)
    if not room or room.get("host_ws") is None:
        await ws.send_bytes(make_frame(FRAME_REJECTED, 0, 0, b"Room not found or host offline"))
        await ws.close(code=4004)
        return

    if len(room["clients"]) >= MAX_CLIENTS_PER_ROOM:
        await ws.send_bytes(make_frame(FRAME_REJECTED, 0, 0, b"Room is full (4/4)"))
        await ws.close(code=4003)
        return

    peer_id = room["next_peer_id"]
    room["next_peer_id"] = 2 if peer_id >= 30000 else (peer_id + 1)
    room["clients"][peer_id] = ws

    try:
        host_ws: Optional[WebSocket] = room.get("host_ws")
        if host_ws:
            await host_ws.send_bytes(make_frame(FRAME_PEER_CONNECTED, peer_id, 0))

        while True:
            msg = await ws.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            data = msg.get("bytes")
            if not data or len(data) < 4:
                continue
            frame_type = data[0]
            channel = data[3]
            payload = data[4:]
            host_ws = room.get("host_ws")
            if frame_type == FRAME_DATA and host_ws:
                await host_ws.send_bytes(make_frame(FRAME_DATA, peer_id, channel, payload))
    except Exception:
        pass
    finally:
        if room["clients"].get(peer_id) is ws:
            room["clients"].pop(peer_id, None)
            host_ws = room.get("host_ws")
            if host_ws:
                try:
                    await host_ws.send_bytes(make_frame(FRAME_PEER_DISCONNECTED, peer_id, 0))
                except Exception:
                    pass


with gr.Blocks(title="Brotato Web Multiplayer Relay") as demo:
    gr.Markdown("## 🥔 Brotato Web Multiplayer Relay Server (Active)")
    gr.Markdown("This free Gradio Space powers `/ws`, `/api/info`, and `/api/rooms` for Brotato Web Multiplayer.")
    status_btn = gr.Button("Refresh Active Rooms")
    rooms_json = gr.JSON(value=get_room_list_snapshot, label="Active Multiplayer Rooms")
    status_btn.click(fn=get_room_list_snapshot, outputs=rooms_json)

app = gr.mount_gradio_app(app, demo, path="/")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "7860"))
    uvicorn.run(app, host="0.0.0.0", port=port)

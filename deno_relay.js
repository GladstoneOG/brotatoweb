// ============================================================================
// Brotato Web Multiplayer Relay — Zero-Dependency Deno Deploy Entrypoint
// Deploy for FREE (No Credit Card) at https://dash.deno.com from GitHub!
// ============================================================================

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_PEER_DISCONNECTED = 3;
const FRAME_KICK_PEER = 4;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;
const FRAME_REJECTED = 7;

const MAX_CLIENTS_PER_ROOM = 3; // 1 Host + 3 Clients = 4 Players
const rooms = new Map();

function allocateRoomPort(requestedPort) {
  let p = requestedPort >= 1024 && requestedPort <= 65535 ? requestedPort : 27462;
  while (rooms.has(p)) {
    p++;
    if (p > 65535) p = 27000;
  }
  return p;
}

function makeFrame(type, peerId, channel, payloadBytes) {
  const payload = payloadBytes ? new Uint8Array(payloadBytes) : new Uint8Array(0);
  const out = new Uint8Array(4 + payload.byteLength);
  out[0] = type & 0xff;
  out[1] = (peerId >> 8) & 0xff;
  out[2] = peerId & 0xff;
  out[3] = channel & 0xff;
  if (payload.byteLength > 0) {
    out.set(payload, 4);
  }
  return out;
}

function getRoomListSnapshot() {
  const list = [];
  for (const [port, room] of rooms.entries()) {
    if (!room.hostWs || room.hostWs.readyState !== WebSocket.OPEN) continue;
    const baseInfo = room.info || {};
    const memberCount = Number(baseInfo.member_count) || 1 + room.clients.size;
    const memberLimit = Number(baseInfo.member_limit) || 4;
    list.push({
      protocol: "brotato_online_lan",
      protocol_version: 2,
      host_name: baseInfo.host_name || "Player",
      room_name: baseInfo.room_name || `${baseInfo.host_name || "Player"}'s Room (#${port})`,
      game_port: port,
      address: "relay",
      member_count: memberCount,
      member_limit: memberLimit,
      state: baseInfo.state || "character_selection",
      mod_version: baseInfo.mod_version || "6.3.0",
      game_version: baseInfo.game_version || "1.2.0.1",
      host_mods_format: baseInfo.host_mods_format || "1",
      host_mods_payload: baseInfo.host_mods_payload || "",
      host_mods_too_large: Boolean(baseInfo.host_mods_too_large),
      joinable: baseInfo.joinable !== undefined ? Boolean(baseInfo.joinable) : memberCount < memberLimit,
    });
  }
  return list;
}

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "*",
  "Cache-Control": "no-store",
};

function handleSocket(ws, url) {
  ws.binaryType = "arraybuffer";
  const role = (url.searchParams.get("role") || "client").toLowerCase();
  const requestedRoom = parseInt(url.searchParams.get("room") || "27462", 10);

  if (role === "discover") {
    ws.onmessage = () => {
      for (const entry of getRoomListSnapshot()) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(entry));
        }
      }
    };
    return;
  }

  if (role === "host") {
    let room = rooms.get(requestedRoom);
    let roomPort = requestedRoom;

    if (room && room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
      roomPort = allocateRoomPort(requestedRoom);
      room = null;
    }

    if (room && room.cleanupTimer) {
      clearTimeout(room.cleanupTimer);
      room.cleanupTimer = null;
      room.hostWs = ws;
    } else {
      room = {
        port: roomPort,
        hostWs: ws,
        nextPeerId: 2,
        clients: new Map(),
        info: {
          host_name: "Player",
          room_name: `Room #${roomPort}`,
          game_port: roomPort,
          member_count: 1,
          member_limit: 4,
          state: "character_selection",
          joinable: true,
        },
        cleanupTimer: null,
      };
      rooms.set(roomPort, room);
    }

    ws.onopen = () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(makeFrame(FRAME_HOST_ASSIGNED, roomPort, 0));
      }
    };

    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") {
        try {
          const parsed = JSON.parse(ev.data);
          if (parsed && typeof parsed === "object") {
            room.info = { ...room.info, ...parsed, game_port: roomPort };
          }
        } catch (_) {}
        return;
      }
      const buf = new Uint8Array(ev.data);
      if (buf.byteLength < 4) return;
      const frameType = buf[0];
      const targetPeer = (buf[1] << 8) | buf[2];
      const channel = buf[3];
      const payload = buf.subarray(4);

      if (frameType === FRAME_ROOM_INFO) {
        try {
          const infoJson = new TextDecoder().decode(payload);
          const parsed = JSON.parse(infoJson);
          if (parsed && typeof parsed === "object") {
            room.info = { ...room.info, ...parsed, game_port: roomPort };
          }
        } catch (_) {}
        return;
      }

      if (frameType === FRAME_KICK_PEER) {
        const clientWs = room.clients.get(targetPeer);
        if (clientWs) {
          room.clients.delete(targetPeer);
          try {
            clientWs.close(4001, "Kicked by host");
          } catch (_) {}
        }
        return;
      }

      if (frameType === FRAME_DATA) {
        const outFrame = makeFrame(FRAME_DATA, 1, channel, payload);
        if (targetPeer === 0) {
          for (const clientWs of room.clients.values()) {
            if (clientWs.readyState === WebSocket.OPEN) {
              clientWs.send(outFrame);
            }
          }
        } else {
          const clientWs = room.clients.get(targetPeer);
          if (clientWs && clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(outFrame);
          }
        }
      }
    };

    ws.onclose = () => {
      if (room.hostWs !== ws) return;
      room.cleanupTimer = setTimeout(() => {
        for (const clientWs of room.clients.values()) {
          try {
            clientWs.close(4000, "Host closed room");
          } catch (_) {}
        }
        room.clients.clear();
        rooms.delete(roomPort);
      }, 6000);
    };
    return;
  }

  // Client role
  const room = rooms.get(requestedRoom);
  if (!room || !room.hostWs || room.hostWs.readyState !== WebSocket.OPEN) {
    ws.onopen = () => {
      const msg = new TextEncoder().encode("Room not found or host offline");
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, msg));
      ws.close(4004, "Room not found");
    };
    return;
  }

  if (room.clients.size >= MAX_CLIENTS_PER_ROOM) {
    ws.onopen = () => {
      const msg = new TextEncoder().encode("Room is full (4/4)");
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, msg));
      ws.close(4003, "Room full");
    };
    return;
  }

  const peerId = room.nextPeerId++;
  if (room.nextPeerId > 30000) room.nextPeerId = 2;
  room.clients.set(peerId, ws);

  ws.onopen = () => {
    if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
      room.hostWs.send(makeFrame(FRAME_PEER_CONNECTED, peerId, 0));
    }
  };

  ws.onmessage = (ev) => {
    if (typeof ev.data === "string") return;
    const buf = new Uint8Array(ev.data);
    if (buf.byteLength < 4) return;
    const frameType = buf[0];
    const channel = buf[3];
    const payload = buf.subarray(4);
    if (frameType === FRAME_DATA && room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
      room.hostWs.send(makeFrame(FRAME_DATA, peerId, channel, payload));
    }
  };

  ws.onclose = () => {
    if (room.clients.get(peerId) === ws) {
      room.clients.delete(peerId);
      if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
        room.hostWs.send(makeFrame(FRAME_PEER_DISCONNECTED, peerId, 0));
      }
    }
  };
}

Deno.serve((req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  const url = new URL(req.url);

  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    handleSocket(socket, url);
    return response;
  }

  if (url.pathname === "/api/info") {
    return new Response(
      JSON.stringify({
        ok: true,
        port: 443,
        lanIps: [],
        roomCount: rooms.size,
        rooms: getRoomListSnapshot(),
      }),
      { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
    );
  }

  if (url.pathname === "/api/rooms") {
    return new Response(
      JSON.stringify({ rooms: getRoomListSnapshot() }),
      { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
    );
  }

  return new Response(
    JSON.stringify({ ok: true, service: "Brotato Web Multiplayer Relay (Deno)", roomCount: rooms.size }),
    { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
  );
});

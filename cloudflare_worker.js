// ============================================================================
// Brotato Web Multiplayer Relay — Cloudflare Worker + Durable Object (Free Plan)
// Deploy with: npx wrangler deploy
// ============================================================================

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_PEER_DISCONNECTED = 3;
const FRAME_KICK_PEER = 4;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;
const FRAME_REJECTED = 7;
const MAX_CLIENTS_PER_ROOM = 3;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "*",
  "Cache-Control": "no-store",
};

function makeFrame(type, peerId, channel, payloadBytes) {
  const payload = payloadBytes ? new Uint8Array(payloadBytes) : new Uint8Array(0);
  const out = new Uint8Array(4 + payload.byteLength);
  out[0] = type & 0xff;
  out[1] = (peerId >> 8) & 0xff;
  out[2] = peerId & 0xff;
  out[3] = channel & 0xff;
  if (payload.byteLength > 0) out.set(payload, 4);
  return out;
}

export class BrotatoRelayHub {
  constructor(state, env) {
    this.state = state;
    this.rooms = new Map();
  }

  allocateRoomPort(requestedPort) {
    let p = requestedPort >= 1024 && requestedPort <= 65535 ? requestedPort : 27462;
    while (this.rooms.has(p)) {
      p++;
      if (p > 65535) p = 27000;
    }
    return p;
  }

  getRoomListSnapshot() {
    const list = [];
    for (const [port, room] of this.rooms.entries()) {
      if (!room.hostWs || !room.hostWs._isOpen) continue;
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

  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    const url = new URL(request.url);

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();
      server._isOpen = true;
      server.addEventListener("close", () => { server._isOpen = false; });
      server.addEventListener("error", () => { server._isOpen = false; });
      this.handleSocket(server, url);
      return new Response(null, { status: 101, webSocket: client });
    }

    if (url.pathname === "/api/info") {
      const rooms = this.getRoomListSnapshot();
      return new Response(
        JSON.stringify({ ok: true, port: 443, lanIps: [], roomCount: rooms.length, rooms }),
        { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
      );
    }

    if (url.pathname === "/api/rooms") {
      return new Response(
        JSON.stringify({ rooms: this.getRoomListSnapshot() }),
        { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
      );
    }

    return new Response(
      JSON.stringify({ ok: true, service: "Brotato Web Multiplayer Relay (Cloudflare DO)", roomCount: this.rooms.size }),
      { status: 200, headers: { ...CORS_HEADERS, "Content-Type": "application/json; charset=utf-8" } }
    );
  }

  handleSocket(ws, url) {
    const role = (url.searchParams.get("role") || "client").toLowerCase();
    const requestedRoom = parseInt(url.searchParams.get("room") || "27462", 10);

    if (role === "discover") {
      ws.addEventListener("message", () => {
        for (const entry of this.getRoomListSnapshot()) {
          try {
            ws.send(JSON.stringify(entry));
          } catch (_) {}
        }
      });
      return;
    }

    if (role === "host") {
      let room = this.rooms.get(requestedRoom);
      let roomPort = requestedRoom;
      if (room && room.hostWs && room.hostWs._isOpen) {
        roomPort = this.allocateRoomPort(requestedRoom);
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
        this.rooms.set(roomPort, room);
      }

      ws.send(makeFrame(FRAME_HOST_ASSIGNED, roomPort, 0));

      ws.addEventListener("message", (ev) => {
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
            const parsed = JSON.parse(new TextDecoder().decode(payload));
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
            try { clientWs.close(4001, "Kicked by host"); } catch (_) {}
          }
          return;
        }

        if (frameType === FRAME_DATA) {
          const outFrame = makeFrame(FRAME_DATA, 1, channel, payload);
          if (targetPeer === 0) {
            for (const clientWs of room.clients.values()) {
              try { clientWs.send(outFrame); } catch (_) {}
            }
          } else {
            const clientWs = room.clients.get(targetPeer);
            if (clientWs) {
              try { clientWs.send(outFrame); } catch (_) {}
            }
          }
        }
      });

      ws.addEventListener("close", () => {
        if (room.hostWs !== ws) return;
        room.cleanupTimer = setTimeout(() => {
          for (const clientWs of room.clients.values()) {
            try { clientWs.close(4000, "Host closed room"); } catch (_) {}
          }
          room.clients.clear();
          this.rooms.delete(roomPort);
        }, 6000);
      });
      return;
    }

    // Client role
    const room = this.rooms.get(requestedRoom);
    if (!room || !room.hostWs || !room.hostWs._isOpen) {
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, new TextEncoder().encode("Room not found or host offline")));
      ws.close(4004, "Room not found");
      return;
    }

    if (room.clients.size >= MAX_CLIENTS_PER_ROOM) {
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, new TextEncoder().encode("Room is full (4/4)")));
      ws.close(4003, "Room full");
      return;
    }

    const peerId = room.nextPeerId++;
    if (room.nextPeerId > 30000) room.nextPeerId = 2;
    room.clients.set(peerId, ws);

    try {
      room.hostWs.send(makeFrame(FRAME_PEER_CONNECTED, peerId, 0));
    } catch (_) {}

    ws.addEventListener("message", (ev) => {
      if (typeof ev.data === "string") return;
      const buf = new Uint8Array(ev.data);
      if (buf.byteLength < 4) return;
      const frameType = buf[0];
      const channel = buf[3];
      const payload = buf.subarray(4);
      if (frameType === FRAME_DATA && room.hostWs) {
        try {
          room.hostWs.send(makeFrame(FRAME_DATA, peerId, channel, payload));
        } catch (_) {}
      }
    });

    ws.addEventListener("close", () => {
      if (room.clients.get(peerId) === ws) {
        room.clients.delete(peerId);
        if (room.hostWs) {
          try {
            room.hostWs.send(makeFrame(FRAME_PEER_DISCONNECTED, peerId, 0));
          } catch (_) {}
        }
      }
    });
  }
}

export default {
  async fetch(request, env) {
    const id = env.RELAY_HUB.idFromName("global-brotato-hub");
    const stub = env.RELAY_HUB.get(id);
    return stub.fetch(request);
  },
};

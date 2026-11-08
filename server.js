const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer, WebSocket } = require("ws");

const PORT = parseInt(process.env.PORT || "8080", 10);
const PUBLIC_DIR = path.join(__dirname, "public");

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_PEER_DISCONNECTED = 3;
const FRAME_KICK_PEER = 4;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;
const FRAME_REJECTED = 7;

const MAX_CLIENTS_PER_ROOM = 3; // 1 Host + 3 Clients = 4 Players

// Active rooms keyed by integer room port (e.g. 27462)
const rooms = new Map();

function getLanAddresses() {
  const interfaces = os.networkInterfaces();
  const results = [];
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name] || []) {
      if (iface.family === "IPv4" && !iface.internal && !iface.address.startsWith("169.254.")) {
        results.push(iface.address);
      }
    }
  }
  results.sort((a, b) => {
    const score = (ip) => (ip.startsWith("192.168.1.") ? 0 : ip.startsWith("192.168.") ? 1 : ip.startsWith("10.") ? 2 : 3);
    return score(a) - score(b);
  });
  return results;
}

function allocateRoomPort(requestedPort) {
  let p = requestedPort >= 1024 && requestedPort <= 65535 ? requestedPort : 27462;
  while (rooms.has(p)) {
    p++;
    if (p > 65535) p = 27000;
  }
  return p;
}

function makeFrame(type, peerId, channel, payloadBuf) {
  const payload = payloadBuf ? Buffer.from(payloadBuf) : Buffer.alloc(0);
  const out = Buffer.allocUnsafe(4 + payload.length);
  out[0] = type & 0xff;
  out[1] = (peerId >> 8) & 0xff;
  out[2] = peerId & 0xff;
  out[3] = channel & 0xff;
  if (payload.length > 0) {
    payload.copy(out, 4);
  }
  return out;
}

function getRoomListSnapshot() {
  const list = [];
  for (const [port, room] of rooms.entries()) {
    if (room.hostWs.readyState !== WebSocket.OPEN) continue;
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

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".wasm": "application/wasm",
  ".pck": "application/octet-stream",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".json": "application/json; charset=utf-8",
  ".css": "text/css; charset=utf-8",
};

const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const pathname = decodeURIComponent(parsedUrl.pathname);

  if (pathname === "/api/info") {
    const payload = JSON.stringify({
      ok: true,
      port: PORT,
      lanIps: getLanAddresses(),
      roomCount: rooms.size,
      rooms: getRoomListSnapshot(),
    });
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
    res.end(payload);
    return;
  }

  if (pathname === "/api/rooms") {
    const payload = JSON.stringify({ rooms: getRoomListSnapshot() });
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
    res.end(payload);
    return;
  }

  let relPath = pathname === "/" ? "/index.html" : pathname;
  const safePath = path.normalize(path.join(PUBLIC_DIR, relPath));
  if (!safePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    res.end("Forbidden");
    return;
  }

  fs.stat(safePath, (err, stat) => {
    if (err || !stat.isFile()) {
      if (pathname === "/" || pathname === "/health") {
        res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
        res.end(JSON.stringify({ ok: true, service: "Brotato Web Multiplayer Relay", roomCount: rooms.size }));
        return;
      }
      res.writeHead(404);
      res.end("Not found");
      return;
    }

    const ext = path.extname(safePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || "application/octet-stream";
    const etag = `"${stat.size}-${Number(stat.mtimeMs).toString(16)}"`;

    res.setHeader("Content-Type", contentType);
    res.setHeader("Accept-Ranges", "bytes");
    res.setHeader("ETag", etag);
    if (ext === ".wasm" || ext === ".pck") {
      res.setHeader("Cache-Control", "public, max-age=3600");
    } else {
      res.setHeader("Cache-Control", "no-cache");
    }

    if (req.headers["if-none-match"] === etag) {
      res.writeHead(304);
      res.end();
      return;
    }

    const rangeHeader = req.headers.range;
    if (rangeHeader) {
      const match = /bytes=(\d*)-(\d*)/.exec(rangeHeader);
      if (match) {
        const start = match[1] ? parseInt(match[1], 10) : 0;
        const end = match[2] ? parseInt(match[2], 10) : stat.size - 1;
        if (start >= 0 && start < stat.size && end >= start && end < stat.size) {
          res.writeHead(206, {
            "Content-Range": `bytes ${start}-${end}/${stat.size}`,
            "Content-Length": end - start + 1,
          });
          fs.createReadStream(safePath, { start, end }).pipe(res);
          return;
        }
      }
    }

    res.writeHead(200, { "Content-Length": stat.size });
    fs.createReadStream(safePath).pipe(res);
  });
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });

server.on("upgrade", (req, socket, head) => {
  socket.setNoDelay(true);
  const parsedUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  if (parsedUrl.pathname !== "/ws") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req, parsedUrl);
  });
});

wss.on("connection", (ws, _req, parsedUrl) => {
  const role = parsedUrl.searchParams.get("role") || "discover";
  const requestedRoom = parseInt(parsedUrl.searchParams.get("room") || "27462", 10) || 27462;

  if (role === "discover") {
    const sendRooms = () => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ rooms: getRoomListSnapshot() }), { binary: false });
      }
    };
    sendRooms();
    ws.on("message", () => sendRooms());
    return;
  }

  if (role === "host") {
    const assignedPort = allocateRoomPort(requestedRoom);
    const room = {
      port: assignedPort,
      hostWs: ws,
      clients: new Map(),
      nextPeerId: 2,
      info: {
        host_name: "Player",
        room_name: `Room #${assignedPort}`,
        game_port: assignedPort,
        member_count: 1,
        member_limit: 4,
        state: "character_selection",
        mod_version: "6.3.0",
        game_version: "1.2.0.1",
        joinable: true,
      },
      createdAt: Date.now(),
    };
    rooms.set(assignedPort, room);
    console.log(`[ROOM] Host created room #${assignedPort} (active rooms: ${rooms.size})`);

    // Notify host of its assigned port
    ws.send(makeFrame(FRAME_HOST_ASSIGNED, assignedPort, 0), { binary: true });

    ws.on("message", (data, isBinary) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (!isBinary || buf.length < 4) return;
      const frameType = buf[0];
      const targetPeer = (buf[1] << 8) | buf[2];
      const channel = buf[3];
      const payload = buf.subarray(4);

      if (frameType === FRAME_DATA) {
        const clientWs = room.clients.get(targetPeer);
        if (clientWs && clientWs.readyState === WebSocket.OPEN) {
          clientWs.send(makeFrame(FRAME_DATA, 1, channel, payload), { binary: true });
        }
      } else if (frameType === FRAME_KICK_PEER) {
        const clientWs = room.clients.get(targetPeer);
        if (clientWs) {
          room.clients.delete(targetPeer);
          try {
            clientWs.send(makeFrame(FRAME_REJECTED, 1, 0, Buffer.from("kicked")), { binary: true });
            clientWs.close(1000, "kicked");
          } catch (_) {}
        }
      } else if (frameType === FRAME_ROOM_INFO) {
        try {
          const parsed = JSON.parse(payload.toString("utf8"));
          if (parsed && typeof parsed === "object") {
            room.info = {
              ...room.info,
              ...parsed,
              game_port: assignedPort,
              address: "relay",
            };
          }
        } catch (_) {}
      }
    });

    ws.on("close", () => {
      if (rooms.get(assignedPort) === room) {
        rooms.delete(assignedPort);
      }
      console.log(`[ROOM] Host closed room #${assignedPort} (active rooms: ${rooms.size})`);
      for (const clientWs of room.clients.values()) {
        if (clientWs.readyState === WebSocket.OPEN) {
          try {
            clientWs.send(makeFrame(FRAME_PEER_DISCONNECTED, 1, 0), { binary: true });
            clientWs.close(1000, "host_disconnected");
          } catch (_) {}
        }
      }
      room.clients.clear();
    });
    return;
  }

  if (role === "client") {
    let room = rooms.get(requestedRoom);
    // Convenience: if client requested default port 27462 and only 1 room exists on another port, use it
    if (!room && requestedRoom === 27462 && rooms.size === 1) {
      room = rooms.values().next().value;
    }

    if (!room || room.hostWs.readyState !== WebSocket.OPEN) {
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, Buffer.from("room_not_found")), { binary: true });
      ws.close(1000, "room_not_found");
      return;
    }

    if (room.clients.size >= MAX_CLIENTS_PER_ROOM) {
      ws.send(makeFrame(FRAME_REJECTED, 0, 0, Buffer.from("room_full")), { binary: true });
      ws.close(1000, "room_full");
      return;
    }

    const peerId = room.nextPeerId++;
    room.clients.set(peerId, ws);
    console.log(`[ROOM] Client peer=${peerId} joined room #${room.port} (${1 + room.clients.size}/4)`);

    // 1. Tell Client it is connected to Host (peer 1) FIRST so client sets __pending_host__
    ws.send(makeFrame(FRAME_PEER_CONNECTED, 1, 0), { binary: true });
    // 2. Tell Host that Client (peerId) connected
    room.hostWs.send(makeFrame(FRAME_PEER_CONNECTED, peerId, 0), { binary: true });

    ws.on("message", (data, isBinary) => {
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (!isBinary || buf.length < 4) return;
      const frameType = buf[0];
      const channel = buf[3];
      const payload = buf.subarray(4);

      if (frameType === FRAME_DATA && room.hostWs.readyState === WebSocket.OPEN) {
        room.hostWs.send(makeFrame(FRAME_DATA, peerId, channel, payload), { binary: true });
      }
    });

    ws.on("close", () => {
      if (room.clients.get(peerId) === ws) {
        room.clients.delete(peerId);
        console.log(`[ROOM] Client peer=${peerId} left room #${room.port}`);
        if (room.hostWs.readyState === WebSocket.OPEN) {
          room.hostWs.send(makeFrame(FRAME_PEER_DISCONNECTED, peerId, 0), { binary: true });
        }
      }
    });
  }
});

server.listen(PORT, "0.0.0.0", () => {
  const lanIps = getLanAddresses();
  console.log("============================================================");
  console.log("  Brotato Web Multiplayer Server Running!");
  console.log(`  Local URL : http://localhost:${PORT}`);
  for (const ip of lanIps) {
    console.log(`  LAN URL   : http://${ip}:${PORT}`);
  }
  console.log("============================================================");
});

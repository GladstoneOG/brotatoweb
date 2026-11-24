// ============================================================================
// Brotato Web Multiplayer — Authoritative Room DO + Hibernation + Directory DO
// Free-Tier Cloudflare Workers + SQLite Durable Objects
// Deploy with: npx wrangler deploy
// ============================================================================

import { DurableObject } from "cloudflare:workers";

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_PEER_DISCONNECTED = 3;
const FRAME_KICK_PEER = 4;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;
const FRAME_REJECTED = 7;

const MAX_CLIENTS_PER_ROOM = 3; // 1 Host + 3 Clients = 4 Players
const HOST_RECONNECT_GRACE_MS = 20000;
const MIGRATION_TIMEOUT_MS = 35000;
const SOLO_RECONNECT_GRACE_MS = 15000;
const MAX_CHECKPOINT_BYTES = 512 * 1024;
const DIRECTORY_STALE_MS = 180000;
const DIRECTORY_CLEANUP_ALARM_MS = 120000;
const DIR_SYNC_THROTTLE_MS = 3000;

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

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

function safeSend(ws, data) {
  if (!ws) return false;
  try {
    ws.send(data);
    return true;
  } catch (_) {
    return false;
  }
}

function safeCloseWebSocket(ws, code, reason) {
  if (!ws) return;
  try {
    if (
      typeof code === "number" &&
      code >= 1000 &&
      code < 5000 &&
      ![1004, 1005, 1006, 1015].includes(code)
    ) {
      ws.close(code, typeof reason === "string" ? reason.slice(0, 120) : "");
    } else {
      ws.close();
    }
  } catch (_) {}
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "application/json; charset=utf-8",
    },
  });
}

function clampPort(raw, fallback = 27462) {
  const n = parseInt(String(raw ?? ""), 10);
  if (Number.isInteger(n) && n >= 1024 && n <= 65535) return n;
  return fallback;
}

function clampGraceMs(raw, fallback = HOST_RECONNECT_GRACE_MS) {
  if (raw === null || raw === undefined || raw === "") return fallback;
  const n = parseInt(String(raw), 10);
  if (Number.isInteger(n) && n >= 500 && n <= 60000) return n;
  return fallback;
}

function normalizeRoomInfo(port, rawInfo = {}, fallbackMemberCount = 1) {
  const base = rawInfo && typeof rawInfo === "object" ? rawInfo : {};
  const memberLimit = Number(base.member_limit) || 4;
  const memberCount = Number(base.member_count) || fallbackMemberCount;
  const hostName = String(base.host_name || "Player").slice(0, 32);
  return {
    protocol: "brotato_online_lan",
    protocol_version: 2,
    host_name: hostName,
    room_name: String(base.room_name || `${hostName}'s Room (#${port})`).slice(0, 48),
    game_port: port,
    address: "relay",
    member_count: memberCount,
    member_limit: memberLimit,
    state: String(base.state || "character_selection"),
    mod_version: String(base.mod_version || "6.3.0"),
    game_version: String(base.game_version || "1.2.0.1"),
    host_mods_format: String(base.host_mods_format || "1"),
    host_mods_payload: String(base.host_mods_payload || ""),
    host_mods_too_large: Boolean(base.host_mods_too_large),
    joinable: base.joinable !== undefined ? Boolean(base.joinable) : memberCount < memberLimit,
  };
}

// ============================================================================
// Class 1: Hibernating SQLite Lobby Directory Durable Object
// ============================================================================
export class BrotatoRelayHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS directory_rooms (
          port INTEGER PRIMARY KEY,
          info_json TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        )
      `);
    });
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  pruneStaleRooms() {
    const cutoff = Date.now() - DIRECTORY_STALE_MS;
    this.ctx.storage.sql.exec("DELETE FROM directory_rooms WHERE updated_at < ?", cutoff);
  }

  async ensureCleanupAlarm() {
    const row = this.ctx.storage.sql
      .exec("SELECT COUNT(*) AS cnt FROM directory_rooms")
      .one();
    if (Number(row?.cnt || 0) > 0) {
      const existing = await this.ctx.storage.getAlarm();
      if (existing === null) {
        await this.ctx.storage.setAlarm(Date.now() + DIRECTORY_CLEANUP_ALARM_MS);
      }
    }
  }

  async allocatePort(requestedPort) {
    this.pruneStaleRooms();
    const rows = this.ctx.storage.sql.exec("SELECT port FROM directory_rooms").toArray();
    const used = new Set(rows.map((r) => Number(r.port)));
    let p = clampPort(requestedPort, 27462);
    let tries = 0;
    while (used.has(p) && tries < 38536) {
      p++;
      if (p > 65535) p = 27000;
      tries++;
    }
    const now = Date.now();
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO directory_rooms (port, info_json, updated_at) VALUES (?, ?, ?)",
      p,
      JSON.stringify({ _pending: true, game_port: p }),
      now
    );
    await this.ensureCleanupAlarm();
    return p;
  }

  async upsertRoom(port, infoObj) {
    const p = clampPort(port, 27462);
    const normalized = normalizeRoomInfo(p, infoObj);
    const now = Date.now();
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO directory_rooms (port, info_json, updated_at) VALUES (?, ?, ?)",
      p,
      JSON.stringify(normalized),
      now
    );
    await this.ensureCleanupAlarm();
    this.broadcastRooms();
    return normalized;
  }

  async removeRoom(port) {
    const p = clampPort(port, 0);
    if (p > 0) {
      this.ctx.storage.sql.exec("DELETE FROM directory_rooms WHERE port = ?", p);
      this.broadcastRooms();
    }
  }

  async findSingleRoomPort() {
    const rooms = this.listRooms();
    if (rooms.length === 1) {
      return Number(rooms[0].game_port);
    }
    return null;
  }

  listRooms() {
    this.pruneStaleRooms();
    const rows = this.ctx.storage.sql
      .exec("SELECT port, info_json FROM directory_rooms ORDER BY port ASC")
      .toArray();
    const list = [];
    for (const row of rows) {
      try {
        const parsed = JSON.parse(row.info_json);
        if (parsed && parsed._pending) continue;
        list.push(normalizeRoomInfo(Number(row.port), parsed));
      } catch (_) {}
    }
    return list;
  }

  broadcastRooms() {
    const sockets = this.ctx.getWebSockets("discover");
    if (!sockets || sockets.length === 0) return;
    const msg = JSON.stringify({ rooms: this.listRooms() });
    for (const ws of sockets) {
      safeSend(ws, msg);
    }
  }

  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    const url = new URL(request.url);

    if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      this.ctx.acceptWebSocket(server, ["discover"]);
      server.serializeAttachment({ role: "discover" });
      safeSend(server, JSON.stringify({ rooms: this.listRooms() }));
      return new Response(null, { status: 101, webSocket: client });
    }

    if (url.pathname === "/api/info") {
      const rooms = this.listRooms();
      return jsonResponse({
        ok: true,
        port: 443,
        lanIps: [],
        roomCount: rooms.length,
        rooms,
      });
    }

    if (url.pathname === "/api/rooms") {
      return jsonResponse({ rooms: this.listRooms() });
    }

    const rooms = this.listRooms();
    return jsonResponse({
      ok: true,
      service: "Brotato Web Directory (Cloudflare DO + Hibernation)",
      roomCount: rooms.length,
    });
  }

  async webSocketMessage(ws, _message) {
    safeSend(ws, JSON.stringify({ rooms: this.listRooms() }));
  }

  async webSocketClose(ws, code, reason, _wasClean) {
    safeCloseWebSocket(ws, code, reason);
  }

  async webSocketError(ws, _error) {
    safeCloseWebSocket(ws);
  }

  async alarm() {
    this.pruneStaleRooms();
    const row = this.ctx.storage.sql
      .exec("SELECT COUNT(*) AS cnt FROM directory_rooms")
      .one();
    if (Number(row?.cnt || 0) > 0) {
      await this.ctx.storage.setAlarm(Date.now() + DIRECTORY_CLEANUP_ALARM_MS);
    }
  }
}

// ============================================================================
// Class 2: Authoritative Per-Room Durable Object (Hibernation + SQLite + Alarms)
// ============================================================================
export class BrotatoRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this._closing = new WeakSet();
    this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS room_state (
          id INTEGER PRIMARY KEY CHECK(id = 1),
          port INTEGER NOT NULL,
          host_token TEXT NOT NULL,
          host_connected INTEGER NOT NULL,
          host_disconnected_at INTEGER NOT NULL,
          migrating INTEGER NOT NULL,
          pending_host_token TEXT,
          pending_host_peer_id INTEGER,
          next_peer_id INTEGER NOT NULL,
          info_json TEXT NOT NULL,
          checkpoint_json TEXT,
          checkpoint_wave INTEGER NOT NULL,
          checkpoint_updated_at INTEGER NOT NULL,
          last_dir_sync_at INTEGER NOT NULL,
          grace_ms INTEGER NOT NULL,
          last_migrated_at INTEGER NOT NULL DEFAULT 0
        )
      `);
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS peers (
          peer_id INTEGER PRIMARY KEY,
          token TEXT UNIQUE NOT NULL,
          name TEXT NOT NULL,
          joined_at INTEGER NOT NULL,
          connected INTEGER NOT NULL
        )
      `);
    });
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  getRoomState() {
    const rows = this.ctx.storage.sql.exec("SELECT * FROM room_state WHERE id = 1").toArray();
    return rows[0] || null;
  }

  isLiveSocket(ws) {
    if (!ws || this._closing.has(ws)) return false;
    try {
      if (typeof ws.readyState === "number" && ws.readyState !== 1) return false;
    } catch (_) {}
    return true;
  }

  getActiveHostSocket() {
    for (const ws of this.ctx.getWebSockets("host")) {
      if (this.isLiveSocket(ws)) return ws;
    }
    return null;
  }

  getActiveClientSockets(excludePeerId = null) {
    const out = [];
    for (const ws of this.ctx.getWebSockets("client")) {
      if (!this.isLiveSocket(ws)) continue;
      const att = ws.deserializeAttachment();
      if (!att || att.role !== "client") continue;
      if (excludePeerId !== null && att.peerId === excludePeerId) continue;
      out.push({ ws, att });
    }
    return out;
  }

  getPeerSocket(peerId) {
    for (const ws of this.ctx.getWebSockets("peer-" + peerId)) {
      if (this.isLiveSocket(ws)) return ws;
    }
    return null;
  }

  closeTaggedSockets(tag, code, reason, exceptWs = null) {
    for (const ws of this.ctx.getWebSockets(tag)) {
      if (ws === exceptWs) continue;
      this._closing.add(ws);
      safeCloseWebSocket(ws, code, reason);
    }
  }

  getDirectoryStub() {
    return this.env.RELAY_HUB.getByName("global-directory");
  }

  async syncDirectoryRoom(state, force = false, newInfoObj = null) {
    if (!state) return;
    let currentInfo = {};
    try {
      currentInfo = JSON.parse(state.info_json || "{}");
    } catch (_) {}

    let changedCritical = force;
    let mergedInfo = currentInfo;
    if (newInfoObj && typeof newInfoObj === "object") {
      const criticalKeys = ["member_count", "state", "joinable", "host_name", "room_name"];
      for (const k of criticalKeys) {
        if (newInfoObj[k] !== undefined && newInfoObj[k] !== currentInfo[k]) {
          changedCritical = true;
        }
      }
      mergedInfo = { ...currentInfo, ...newInfoObj };
    }

    const activeClients = this.getActiveClientSockets().length;
    const normalized = normalizeRoomInfo(state.port, mergedInfo, 1 + activeClients);
    if (newInfoObj && newInfoObj.member_count === undefined && state.host_connected === 1) {
      normalized.member_count = Math.max(normalized.member_count, 1 + activeClients);
    }

    const now = Date.now();
    const infoJsonStr = JSON.stringify(normalized);
    if (changedCritical || now - Number(state.last_dir_sync_at || 0) >= DIR_SYNC_THROTTLE_MS) {
      this.ctx.storage.sql.exec(
        "UPDATE room_state SET info_json = ?, last_dir_sync_at = ? WHERE id = 1",
        infoJsonStr,
        now
      );
      try {
        await this.getDirectoryStub().upsertRoom(state.port, normalized);
      } catch (_) {}
    } else if (newInfoObj) {
      this.ctx.storage.sql.exec(
        "UPDATE room_state SET info_json = ? WHERE id = 1",
        infoJsonStr
      );
    }
  }

  rejectAndClose(ws, message, code) {
    safeSend(ws, makeFrame(FRAME_REJECTED, 0, 0, TEXT_ENCODER.encode(message)));
    this._closing.add(ws);
    safeCloseWebSocket(ws, code, message);
  }

  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      const state = this.getRoomState();
      return jsonResponse({
        ok: true,
        service: "BrotatoRoom",
        active: Boolean(state),
        port: state ? state.port : null,
        clients: this.getActiveClientSockets().length,
        checkpointWave: state ? state.checkpoint_wave : 0,
      });
    }

    const url = new URL(request.url);
    const role = (url.searchParams.get("role") || "client").toLowerCase();
    const requestedRoom = clampPort(url.searchParams.get("room"), 27462);
    const token = (url.searchParams.get("token") || "").trim();
    const reattach = url.searchParams.get("reattach") === "1";
    const isMigrationConnect = url.searchParams.get("migration") === "1";
    const clientName = (url.searchParams.get("name") || "").trim().slice(0, 24);
    const requestedGraceMs = clampGraceMs(url.searchParams.get("graceMs"), HOST_RECONNECT_GRACE_MS);

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    if (role === "host") {
      const state = this.getRoomState();

      // Case B: Promoted host migration takeover
      if (
        state &&
        state.migrating === 1 &&
        token &&
        state.pending_host_token &&
        token === state.pending_host_token
      ) {
        const promotedPeerId = Number(state.pending_host_peer_id || 0);
        if (promotedPeerId > 0) {
          this.closeTaggedSockets("peer-" + promotedPeerId, 1000, "promoted");
        }
        this.closeTaggedSockets("host", 4002, "replaced");

        this.ctx.acceptWebSocket(server, ["host"]);
        server.serializeAttachment({
          role: "host",
          peerId: 1,
          token,
          port: state.port,
        });

        this.ctx.storage.sql.exec(
          `UPDATE room_state
           SET host_token = ?, host_connected = 1, host_disconnected_at = 0,
               migrating = 0, pending_host_token = NULL, pending_host_peer_id = NULL,
               next_peer_id = 2, last_migrated_at = ?
           WHERE id = 1`,
          token,
          Date.now()
        );
        this.ctx.storage.sql.exec("DELETE FROM peers");
        await this.ctx.storage.deleteAlarm();

        safeSend(
          server,
          JSON.stringify({
            t: "session_welcome",
            role: "host",
            roomPort: state.port,
            peerId: 1,
            token,
            migrated: true,
            checkpointWave: state.checkpoint_wave,
          })
        );
        safeSend(server, makeFrame(FRAME_HOST_ASSIGNED, state.port, 0));

        const waitingClients = this.getActiveClientSockets(promotedPeerId);
        const readyMsg = JSON.stringify({ t: "migration_ready", roomPort: state.port });
        for (const { ws } of waitingClients) {
          safeSend(ws, readyMsg);
        }

        const updatedState = this.getRoomState();
        let existingInfo = {};
        try {
          existingInfo = JSON.parse(updatedState.info_json || "{}");
        } catch (_) {}
        const newHostName = clientName || "New Host";
        await this.syncDirectoryRoom(updatedState, true, {
          ...existingInfo,
          host_name: newHostName,
          room_name: `${newHostName}'s Room (#${updatedState.port})`,
          member_count: 1,
          state: "shop",
          joinable: true,
        });

        return new Response(null, { status: 101, webSocket: client });
      }

      // Case A: Seamless Host Reconnect (only if migration hasn't already promoted someone else)
      if (state && token && token === state.host_token) {
        if (state.migrating === 1) {
          this.ctx.acceptWebSocket(server, ["rejected"]);
          this.rejectAndClose(server, "Host migrated to another player", 4010);
          return new Response(null, { status: 101, webSocket: client });
        }

        this.closeTaggedSockets("host", 4002, "replaced");
        this.ctx.acceptWebSocket(server, ["host"]);
        server.serializeAttachment({
          role: "host",
          peerId: 1,
          token,
          port: state.port,
        });

        this.ctx.storage.sql.exec(
          "UPDATE room_state SET host_connected = 1, host_disconnected_at = 0, migrating = 0 WHERE id = 1"
        );
        await this.ctx.storage.deleteAlarm();

        safeSend(
          server,
          JSON.stringify({
            t: "session_welcome",
            role: "host",
            roomPort: state.port,
            peerId: 1,
            token,
            reattached: true,
            checkpointWave: state.checkpoint_wave,
          })
        );
        safeSend(server, makeFrame(FRAME_HOST_ASSIGNED, state.port, 0));

        const clients = this.getActiveClientSockets();
        const reconnectedMsg = JSON.stringify({ t: "host_reconnected", roomPort: state.port });
        for (const { ws, att } of clients) {
          safeSend(ws, reconnectedMsg);
          if (!reattach) {
            safeSend(server, makeFrame(FRAME_PEER_CONNECTED, att.peerId, 0));
          }
        }

        const updatedState = this.getRoomState();
        await this.syncDirectoryRoom(updatedState, true);
        return new Response(null, { status: 101, webSocket: client });
      }

      // If migrating=1 token was provided but didn't match pending_host_token
      if (isMigrationConnect) {
        this.ctx.acceptWebSocket(server, ["rejected"]);
        this.rejectAndClose(server, "Invalid or expired host promotion token", 4011);
        return new Response(null, { status: 101, webSocket: client });
      }

      // Case C: Fresh Host
      const activeHost = this.getActiveHostSocket();
      if (state && (activeHost || state.migrating === 1)) {
        this.ctx.acceptWebSocket(server, ["rejected"]);
        this.rejectAndClose(server, "Room port already in use", 4009);
        return new Response(null, { status: 101, webSocket: client });
      }

      const newHostToken = crypto.randomUUID();
      const hostDisplayName = clientName || "Player";
      const defaultInfo = normalizeRoomInfo(requestedRoom, {
        host_name: hostDisplayName,
        room_name: `${hostDisplayName}'s Room (#${requestedRoom})`,
        member_count: 1,
        member_limit: 4,
        state: "character_selection",
        joinable: true,
      });

      this.ctx.storage.sql.exec("DELETE FROM peers");
      this.ctx.storage.sql.exec(
        `INSERT OR REPLACE INTO room_state (
          id, port, host_token, host_connected, host_disconnected_at,
          migrating, pending_host_token, pending_host_peer_id, next_peer_id,
          info_json, checkpoint_json, checkpoint_wave, checkpoint_updated_at,
          last_dir_sync_at, grace_ms, last_migrated_at
        ) VALUES (1, ?, ?, 1, 0, 0, NULL, NULL, 2, ?, NULL, 0, 0, ?, ?, 0)`,
        requestedRoom,
        newHostToken,
        JSON.stringify(defaultInfo),
        Date.now(),
        requestedGraceMs
      );
      await this.ctx.storage.deleteAlarm();

      this.ctx.acceptWebSocket(server, ["host"]);
      server.serializeAttachment({
        role: "host",
        peerId: 1,
        token: newHostToken,
        port: requestedRoom,
      });

      safeSend(
        server,
        JSON.stringify({
          t: "session_welcome",
          role: "host",
          roomPort: requestedRoom,
          peerId: 1,
          token: newHostToken,
        })
      );
      safeSend(server, makeFrame(FRAME_HOST_ASSIGNED, requestedRoom, 0));

      try {
        await this.getDirectoryStub().upsertRoom(requestedRoom, defaultInfo);
      } catch (_) {}

      return new Response(null, { status: 101, webSocket: client });
    }

    // Role === "client"
    const state = this.getRoomState();
    const now = Date.now();
    const withinHostGrace =
      state &&
      state.host_connected === 0 &&
      state.host_disconnected_at > 0 &&
      now - Number(state.host_disconnected_at) <= Number(state.grace_ms || HOST_RECONNECT_GRACE_MS) + 5000;

    if (!state) {
      this.ctx.acceptWebSocket(server, ["rejected"]);
      this.rejectAndClose(server, "Room not found or host offline", 4004);
      return new Response(null, { status: 101, webSocket: client });
    }

    // Client Reattach check
    if (token) {
      const peerRows = this.ctx.storage.sql
        .exec("SELECT * FROM peers WHERE token = ?", token)
        .toArray();
      if (peerRows.length > 0 && (state.host_connected === 1 || withinHostGrace || state.migrating === 1)) {
        const peer = peerRows[0];
        const peerId = Number(peer.peer_id);
        this.closeTaggedSockets("peer-" + peerId, 4002, "replaced");

        this.ctx.acceptWebSocket(server, ["client", "peer-" + peerId]);
        server.serializeAttachment({
          role: "client",
          peerId,
          token,
          port: state.port,
          name: clientName || peer.name,
        });
        this.ctx.storage.sql.exec(
          "UPDATE peers SET connected = 1, name = ? WHERE peer_id = ?",
          clientName || peer.name,
          peerId
        );

        safeSend(
          server,
          JSON.stringify({
            t: "session_welcome",
            role: "client",
            roomPort: state.port,
            peerId,
            token,
            reattached: true,
            hostPresent: state.host_connected === 1,
            checkpointWave: state.checkpoint_wave,
          })
        );

        if (!reattach) {
          safeSend(server, makeFrame(FRAME_PEER_CONNECTED, 1, 0));
          const hostWs = this.getActiveHostSocket();
          if (hostWs) {
            safeSend(hostWs, makeFrame(FRAME_PEER_CONNECTED, peerId, 0));
          }
        }

        if (state.host_connected === 0 && state.migrating === 0) {
          const deadlineMs = Number(state.host_disconnected_at) + Number(state.grace_ms || HOST_RECONNECT_GRACE_MS);
          safeSend(
            server,
            JSON.stringify({
              t: "host_reconnecting",
              graceMs: Math.max(0, deadlineMs - Date.now()),
              deadlineMs,
            })
          );
        }

        return new Response(null, { status: 101, webSocket: client });
      }
      if (reattach) {
        if (state.host_connected === 1 && now - Number(state.last_migrated_at || 0) < 30000) {
          this.ctx.acceptWebSocket(server, ["client"]);
          server.serializeAttachment({ role: "client", peerId: 0, token: "", port: state.port, name: clientName });
          safeSend(server, JSON.stringify({ t: "migration_ready", roomPort: state.port }));
          return new Response(null, { status: 101, webSocket: client });
        }
        this.ctx.acceptWebSocket(server, ["rejected"]);
        this.rejectAndClose(server, "Session expired", 4004);
        return new Response(null, { status: 101, webSocket: client });
      }
    }

    // Fresh Client Join
    if (state.migrating === 1) {
      this.ctx.acceptWebSocket(server, ["rejected"]);
      this.rejectAndClose(server, "Room migrating host, please retry", 4004);
      return new Response(null, { status: 101, webSocket: client });
    }

    const hostWs = this.getActiveHostSocket();
    if (state.host_connected !== 1 || !hostWs) {
      this.ctx.acceptWebSocket(server, ["rejected"]);
      this.rejectAndClose(server, "Room not found or host offline", 4004);
      return new Response(null, { status: 101, webSocket: client });
    }

    const activeClients = this.getActiveClientSockets();
    if (activeClients.length >= MAX_CLIENTS_PER_ROOM) {
      this.ctx.acceptWebSocket(server, ["rejected"]);
      this.rejectAndClose(server, "Room is full (4/4)", 4003);
      return new Response(null, { status: 101, webSocket: client });
    }

    const peerId = Number(state.next_peer_id || 2);
    let nextPeerId = peerId + 1;
    if (nextPeerId > 30000) nextPeerId = 2;
    while (this.getPeerSocket(nextPeerId)) {
      nextPeerId++;
      if (nextPeerId > 30000) nextPeerId = 2;
    }

    const clientToken = crypto.randomUUID();
    const peerName = clientName || `Player ${peerId}`;

    this.ctx.storage.sql.exec("UPDATE room_state SET next_peer_id = ? WHERE id = 1", nextPeerId);
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO peers (peer_id, token, name, joined_at, connected) VALUES (?, ?, ?, ?, 1)",
      peerId,
      clientToken,
      peerName,
      now
    );

    this.ctx.acceptWebSocket(server, ["client", "peer-" + peerId]);
    server.serializeAttachment({
      role: "client",
      peerId,
      token: clientToken,
      port: state.port,
      name: peerName,
    });

    safeSend(
      server,
      JSON.stringify({
        t: "session_welcome",
        role: "client",
        roomPort: state.port,
        peerId,
        token: clientToken,
        hostPresent: true,
        checkpointWave: state.checkpoint_wave,
      })
    );
    // Notify client first (matches server.js so Godot sets __pending_host__), then notify host
    safeSend(server, makeFrame(FRAME_PEER_CONNECTED, 1, 0));
    safeSend(hostWs, makeFrame(FRAME_PEER_CONNECTED, peerId, 0));

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const att = ws.deserializeAttachment();
    if (!att || !att.role || att.role === "rejected") return;

    if (typeof message === "string") {
      let msg = null;
      try {
        msg = JSON.parse(message);
      } catch (_) {
        return;
      }
      if (!msg || typeof msg !== "object") return;

      if (msg.t === "checkpoint_save") {
        if (att.role !== "host") return;
        const runJson = typeof msg.runJson === "string" ? msg.runJson : JSON.stringify(msg.runJson || {});
        const byteLen = TEXT_ENCODER.encode(runJson).byteLength;
        if (byteLen > MAX_CHECKPOINT_BYTES) {
          safeSend(ws, JSON.stringify({ t: "checkpoint_rejected", reason: "too_large", bytes: byteLen }));
          return;
        }
        const wave = Number(msg.wave) || 0;
        const profileId = Number(msg.profileId) || 0;
        const checkpointPayload = JSON.stringify({ runJson, profileId });
        const now = Date.now();
        this.ctx.storage.sql.exec(
          "UPDATE room_state SET checkpoint_json = ?, checkpoint_wave = ?, checkpoint_updated_at = ? WHERE id = 1",
          checkpointPayload,
          wave,
          now
        );
        safeSend(ws, JSON.stringify({ t: "checkpoint_ack", wave, bytes: byteLen }));
        return;
      }

      if (msg.t === "rtc_signal") {
        const targetPeer = Number(msg.targetPeer || 0);
        const out = JSON.stringify({
          t: "rtc_signal",
          fromPeer: att.peerId,
          payload: msg.payload,
        });
        if (targetPeer === 1) {
          const hostWs = this.getActiveHostSocket();
          if (hostWs) safeSend(hostWs, out);
        } else if (targetPeer > 1) {
          const clientWs = this.getPeerSocket(targetPeer);
          if (clientWs) safeSend(clientWs, out);
        }
        return;
      }

      if (msg.t === "peer_meta") {
        if (att.role === "client" && typeof msg.name === "string" && msg.name.trim()) {
          const cleanName = msg.name.trim().slice(0, 24);
          att.name = cleanName;
          ws.serializeAttachment(att);
          this.ctx.storage.sql.exec(
            "UPDATE peers SET name = ? WHERE peer_id = ?",
            cleanName,
            att.peerId
          );
        }
        return;
      }

      if (msg.t === "host_leave_graceful") {
        if (att.role !== "host") return;
        this._closing.add(ws);
        this.ctx.storage.sql.exec(
          "UPDATE room_state SET host_connected = 0, host_disconnected_at = ? WHERE id = 1",
          Date.now()
        );
        await this.triggerHostFailoverOrClose();
        safeCloseWebSocket(ws, 1000, "host_left");
        return;
      }

      // Legacy plain JSON room info from host
      if (att.role === "host" && !msg.t) {
        const state = this.getRoomState();
        if (state) {
          await this.syncDirectoryRoom(state, false, msg);
        }
      }
      return;
    }

    // Binary game frames
    const buf = message instanceof ArrayBuffer ? new Uint8Array(message) : new Uint8Array(message.buffer || message);
    if (buf.byteLength < 4) return;
    const frameType = buf[0];
    const targetPeer = (buf[1] << 8) | buf[2];
    const channel = buf[3];
    const payload = buf.subarray(4);

    if (att.role === "host") {
      if (frameType === FRAME_ROOM_INFO) {
        try {
          const parsed = JSON.parse(TEXT_DECODER.decode(payload));
          if (parsed && typeof parsed === "object") {
            const state = this.getRoomState();
            if (state) {
              await this.syncDirectoryRoom(state, false, parsed);
            }
          }
        } catch (_) {}
        return;
      }

      if (frameType === FRAME_KICK_PEER) {
        this.ctx.storage.sql.exec("DELETE FROM peers WHERE peer_id = ?", targetPeer);
        const targetWs = this.getPeerSocket(targetPeer);
        if (targetWs) {
          this.rejectAndClose(targetWs, "Kicked by host", 4001);
        }
        return;
      }

      if (frameType === FRAME_DATA) {
        const outFrame = makeFrame(FRAME_DATA, 1, channel, payload);
        if (targetPeer === 0) {
          for (const { ws: clientWs } of this.getActiveClientSockets()) {
            safeSend(clientWs, outFrame);
          }
        } else {
          const clientWs = this.getPeerSocket(targetPeer);
          if (clientWs) safeSend(clientWs, outFrame);
        }
      }
      return;
    }

    // Client binary frames -> Host
    if (att.role === "client" && frameType === FRAME_DATA) {
      const hostWs = this.getActiveHostSocket();
      if (hostWs) {
        safeSend(hostWs, makeFrame(FRAME_DATA, att.peerId, channel, payload));
      }
    }
  }

  async handleSocketDisconnect(ws, code, reason) {
    this._closing.add(ws);
    safeCloseWebSocket(ws, code, reason);

    const att = ws.deserializeAttachment();
    if (!att || !att.role || att.role === "rejected") return;

    const state = this.getRoomState();
    if (!state) return;

    if (att.role === "host") {
      // If a newer host socket already replaced this one, do nothing
      const remainingHost = this.getActiveHostSocket();
      if (remainingHost) return;
      if (state.migrating === 1 || state.host_connected === 0) return;

      const now = Date.now();
      const graceMs = Number(state.grace_ms || HOST_RECONNECT_GRACE_MS);
      this.ctx.storage.sql.exec(
        "UPDATE room_state SET host_connected = 0, host_disconnected_at = ? WHERE id = 1",
        now
      );

      const clients = this.getActiveClientSockets();
      if (clients.length > 0) {
        const deadlineMs = now + graceMs;
        const msg = JSON.stringify({
          t: "host_reconnecting",
          graceMs,
          deadlineMs,
        });
        for (const { ws: clientWs } of clients) {
          safeSend(clientWs, msg);
        }
        await this.ctx.storage.setAlarm(deadlineMs);
      } else {
        await this.ctx.storage.setAlarm(now + SOLO_RECONNECT_GRACE_MS);
      }
      return;
    }

    if (att.role === "client") {
      // Ignore disconnect of a superseded or migrated/promoted client socket
      const replacement = this.getPeerSocket(att.peerId);
      if (replacement) return;

      const peerRows = this.ctx.storage.sql
        .exec("SELECT peer_id FROM peers WHERE peer_id = ? AND token = ?", att.peerId, att.token)
        .toArray();
      if (peerRows.length === 0) return;

      if (state.migrating === 1) {
        this.ctx.storage.sql.exec("UPDATE peers SET connected = 0 WHERE peer_id = ?", att.peerId);
        return;
      }

      if (code === 1000 || code === 4001) {
        this.ctx.storage.sql.exec("DELETE FROM peers WHERE peer_id = ?", att.peerId);
      } else {
        this.ctx.storage.sql.exec("UPDATE peers SET connected = 0 WHERE peer_id = ?", att.peerId);
      }

      const hostWs = this.getActiveHostSocket();
      if (hostWs) {
        safeSend(hostWs, makeFrame(FRAME_PEER_DISCONNECTED, att.peerId, 0));
      }
    }
  }

  async webSocketClose(ws, code, reason, _wasClean) {
    await this.handleSocketDisconnect(ws, code, reason);
  }

  async webSocketError(ws, _error) {
    await this.handleSocketDisconnect(ws, 1006, "socket_error");
  }

  async triggerHostFailoverOrClose() {
    const state = this.getRoomState();
    if (!state) return;
    if (state.host_connected === 1 && this.getActiveHostSocket()) return;

    // If a previously promoted candidate timed out, remove them from candidates
    const failedCandidateId = state.migrating === 1 ? Number(state.pending_host_peer_id || 0) : 0;
    if (failedCandidateId > 0) {
      this.ctx.storage.sql.exec("DELETE FROM peers WHERE peer_id = ?", failedCandidateId);
      this.closeTaggedSockets("peer-" + failedCandidateId, 4008, "Promotion timed out");
    }

    // Order surviving connected clients by join order
    const peerOrder = this.ctx.storage.sql
      .exec("SELECT peer_id, name, joined_at FROM peers ORDER BY joined_at ASC, peer_id ASC")
      .toArray();

    const activeByPeerId = new Map();
    for (const entry of this.getActiveClientSockets()) {
      activeByPeerId.set(entry.att.peerId, entry);
    }

    const surviving = [];
    for (const p of peerOrder) {
      const pid = Number(p.peer_id);
      const active = activeByPeerId.get(pid);
      if (active) {
        surviving.push({
          peerId: pid,
          name: active.att.name || p.name || `Player ${pid}`,
          ws: active.ws,
        });
      }
    }
    // Include any connected client sockets even if missing from peers query
    for (const [pid, entry] of activeByPeerId.entries()) {
      if (!surviving.some((s) => s.peerId === pid)) {
        surviving.push({
          peerId: pid,
          name: entry.att.name || `Player ${pid}`,
          ws: entry.ws,
        });
      }
    }

    let checkpointObj = null;
    if (state.checkpoint_json) {
      try {
        const parsed = JSON.parse(state.checkpoint_json);
        if (parsed && typeof parsed.runJson === "string" && parsed.runJson.length > 0) {
          checkpointObj = parsed;
        }
      } catch (_) {}
    }

    if (surviving.length > 0 && checkpointObj) {
      const elected = surviving[0];
      const pendingToken = crypto.randomUUID();
      this.ctx.storage.sql.exec(
        "UPDATE room_state SET migrating = 1, pending_host_token = ?, pending_host_peer_id = ? WHERE id = 1",
        pendingToken,
        elected.peerId
      );

      safeSend(
        elected.ws,
        JSON.stringify({
          t: "host_promoted",
          roomPort: state.port,
          hostToken: pendingToken,
          checkpoint: checkpointObj,
          checkpointWave: Number(state.checkpoint_wave || 0),
          remainingPeerCount: surviving.length - 1,
        })
      );

      const migratingNotice = JSON.stringify({
        t: "host_migrating",
        roomPort: state.port,
        newHostName: elected.name,
        checkpointWave: Number(state.checkpoint_wave || 0),
      });
      for (let i = 1; i < surviving.length; i++) {
        safeSend(surviving[i].ws, migratingNotice);
      }

      await this.ctx.storage.setAlarm(Date.now() + MIGRATION_TIMEOUT_MS);
      return;
    }

    // No surviving clients OR no checkpoint saved yet (e.g. still in character selection)
    const closedFrame = makeFrame(FRAME_PEER_DISCONNECTED, 1, 0);
    for (const { ws } of this.getActiveClientSockets()) {
      safeSend(ws, closedFrame);
      this._closing.add(ws);
      safeCloseWebSocket(ws, 4000, "Host closed room");
    }

    const port = Number(state.port);
    this.ctx.storage.sql.exec("DELETE FROM peers");
    this.ctx.storage.sql.exec("DELETE FROM room_state");
    await this.ctx.storage.deleteAlarm();

    try {
      await this.getDirectoryStub().removeRoom(port);
    } catch (_) {}
  }

  async alarm() {
    await this.triggerHostFailoverOrClose();
  }
}

// ============================================================================
// Worker Entrypoint & Routing
// ============================================================================
export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const dirStub = env.RELAY_HUB.getByName("global-directory");

    if (url.pathname === "/api/info" || url.pathname === "/api/rooms") {
      return dirStub.fetch(request);
    }

    if (url.pathname === "/ws") {
      const role = (url.searchParams.get("role") || "discover").toLowerCase();

      if (role === "discover") {
        return dirStub.fetch(request);
      }

      if (role === "host") {
        const requestedRoom = clampPort(url.searchParams.get("room"), 27462);
        const token = (url.searchParams.get("token") || "").trim();
        let targetPort = requestedRoom;
        let forwardReq = request;

        if (!token) {
          targetPort = await dirStub.allocatePort(requestedRoom);
          if (targetPort !== requestedRoom || !url.searchParams.has("room")) {
            const rewritten = new URL(request.url);
            rewritten.searchParams.set("room", String(targetPort));
            forwardReq = new Request(rewritten.toString(), request);
          }
        }

        const roomStub = env.ROOM_DO.getByName(`room-${targetPort}`);
        return roomStub.fetch(forwardReq);
      }

      // role === "client"
      let roomPort = clampPort(url.searchParams.get("room"), 27462);
      const token = (url.searchParams.get("token") || "").trim();
      if (!token && roomPort === 27462) {
        try {
          const singlePort = await dirStub.findSingleRoomPort();
          if (singlePort && Number.isInteger(singlePort)) {
            roomPort = singlePort;
          }
        } catch (_) {}
      }

      const roomStub = env.ROOM_DO.getByName(`room-${roomPort}`);
      return roomStub.fetch(request);
    }

    if (url.pathname === "/health") {
      return jsonResponse({
        ok: true,
        service: "Brotato Web Authoritative Room Relay (Cloudflare DO + Hibernation)",
      });
    }

    if (env.ASSETS) {
      return env.ASSETS.fetch(request);
    }

    return jsonResponse({
      ok: true,
      service: "Brotato Web Authoritative Room Relay (Cloudflare DO + Hibernation)",
    });
  },
};

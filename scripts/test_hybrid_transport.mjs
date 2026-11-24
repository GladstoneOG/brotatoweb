#!/usr/bin/env node
/**
 * GodotWebSocket Contract & Handshake Verification for public/hybrid_transport.js
 * Exercises the exact checks performed by Godot 3.6's GodotWebSocket JS bridge in public/index.js.
 *
 * Usage:
 *   node scripts/test_hybrid_transport.mjs [base_http_url]
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import WebSocket from "ws";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BASE_HTTP = (process.argv[2] || "http://127.0.0.1:8787").replace(/\/+$/, "");
const BASE_WS = BASE_HTTP.replace(/^http/, "ws");

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;

function makeFrame(type, peerId, channel, payloadStr = "") {
  const payload = Buffer.from(payloadStr, "utf8");
  const out = new Uint8Array(4 + payload.length);
  out[0] = type & 0xff;
  out[1] = (peerId >> 8) & 0xff;
  out[2] = peerId & 0xff;
  out[3] = channel & 0xff;
  if (payload.length > 0) out.set(payload, 4);
  return out.buffer;
}

function parseFrame(ab) {
  assert.ok(ab instanceof ArrayBuffer, "GodotWebSocket requires event.data to be an ArrayBuffer");
  const u8 = new Uint8Array(ab);
  return {
    frameType: u8[0],
    peerId: (u8[1] << 8) | u8[2],
    channel: u8[3],
    payload: Buffer.from(u8.subarray(4)).toString("utf8"),
  };
}

// Replicate GodotWebSocket.send and GodotWebSocket.close checks from public/index.js
function godotJsWebSocketSend(ref, arrayBuffer) {
  if (!ref || ref.readyState !== ref.OPEN) {
    return 1;
  }
  ref.send(arrayBuffer);
  return 0;
}

function godotJsWebSocketClose(ref, code = 1000, reason = "done") {
  if (ref && ref.readyState < ref.CLOSING) {
    ref.close(code, reason);
  }
}

function loadHybridEnv() {
  const listeners = {};
  const windowObj = {
    WebSocket,
    BROTATO_PLAYER_NAME: "ContractTester",
    addEventListener(type, fn) {
      listeners[type] = listeners[type] || [];
      listeners[type].push(fn);
    },
    BrotatoNetBridge: {
      onTransportMeta() {},
    },
    BrotatoSaveBridge: {
      queueAction() {},
    },
  };
  const locationObj = new URL(BASE_HTTP);
  const ctx = vm.createContext({
    window: windowObj,
    location: locationObj,
    URL,
    console,
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    Uint8Array,
    ArrayBuffer,
  });
  const code = fs.readFileSync(path.join(ROOT, "public", "hybrid_transport.js"), "utf8");
  vm.runInContext(code, ctx, { filename: "public/hybrid_transport.js" });
  return windowObj;
}

function attachQueue(socket) {
  const frames = [];
  const waiters = [];
  let opened = false;
  let closed = false;
  const openWaiters = [];
  const closeWaiters = [];

  socket.binaryType = "arraybuffer";

  socket.onopen = () => {
    opened = true;
    while (openWaiters.length) openWaiters.shift()();
  };
  socket.onmessage = (ev) => {
    const frame = parseFrame(ev.data);
    frames.push(frame);
    for (let i = waiters.length - 1; i >= 0; i--) {
      if (waiters[i].pred(frame)) {
        const [f] = frames.splice(frames.length - 1, 1);
        const w = waiters.splice(i, 1)[0];
        clearTimeout(w.timer);
        w.resolve(f);
        break;
      }
    }
  };
  socket.onclose = (ev) => {
    closed = true;
    while (closeWaiters.length) closeWaiters.shift()(ev);
  };

  return {
    waitOpen(timeoutMs = 8000) {
      if (opened) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("Timeout waiting for onopen")), timeoutMs);
        openWaiters.push(() => {
          clearTimeout(t);
          resolve();
        });
      });
    },
    waitFrame(pred, label, timeoutMs = 8000) {
      const idx = frames.findIndex(pred);
      if (idx !== -1) return Promise.resolve(frames.splice(idx, 1)[0]);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const i = waiters.indexOf(entry);
          if (i !== -1) waiters.splice(i, 1);
          reject(new Error(`Timeout waiting for ${label} (seen=${JSON.stringify(frames)})`));
        }, timeoutMs);
        const entry = { pred, resolve, timer };
        waiters.push(entry);
      });
    },
    waitClose(timeoutMs = 8000) {
      if (closed) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("Timeout waiting for onclose")), timeoutMs);
        closeWaiters.push((ev) => {
          clearTimeout(t);
          resolve(ev);
        });
      });
    },
  };
}

async function main() {
  console.log(`[CONTRACT] Testing hybrid_transport.js against ${BASE_WS}`);
  const win = loadHybridEnv();

  // Verify discover passthrough returns NativeWebSocket with constants intact
  const disc = new win.WebSocket(`${BASE_WS}/ws?role=discover`);
  assert.equal(disc.CONNECTING, 0);
  assert.equal(disc.OPEN, 1);
  assert.equal(disc.CLOSING, 2);
  assert.equal(disc.CLOSED, 3);
  await new Promise((resolve, reject) => {
    disc.once("open", resolve);
    disc.once("error", reject);
  });
  disc.close();

  // Open Host via HybridWebSocket wrapper
  const hostWs = new win.WebSocket(`${BASE_WS}/ws?role=host&room=27462`);
  assert.equal(hostWs.CONNECTING, 0, "hostWs.CONNECTING must equal 0");
  assert.equal(hostWs.OPEN, 1, "hostWs.OPEN must equal 1");
  assert.equal(hostWs.CLOSING, 2, "hostWs.CLOSING must equal 2");
  assert.equal(hostWs.CLOSED, 3, "hostWs.CLOSED must equal 3");
  assert.equal(typeof hostWs.protocol, "string", "hostWs.protocol must be a string");

  const hostQ = attachQueue(hostWs);
  await hostQ.waitOpen();
  assert.equal(hostWs.readyState, hostWs.OPEN, "hostWs.readyState must equal hostWs.OPEN after open");
  assert.ok(hostWs.readyState < hostWs.CLOSING, "hostWs.readyState < hostWs.CLOSING must be true");

  const assigned = await hostQ.waitFrame((f) => f.frameType === FRAME_HOST_ASSIGNED, "FRAME_HOST_ASSIGNED");
  const roomPort = assigned.peerId;
  assert.ok(roomPort >= 1024, `Assigned roomPort must be valid, got ${roomPort}`);

  // Send FRAME_ROOM_INFO using exact GodotWebSocket.send check
  const roomInfoPayload = JSON.stringify({
    host_name: "ContractTester",
    room_name: "Contract Test Room",
    member_count: 1,
    state: "character_selection",
  });
  const sendInfoRc = godotJsWebSocketSend(hostWs, makeFrame(FRAME_ROOM_INFO, 0, 0, roomInfoPayload));
  assert.equal(sendInfoRc, 0, "GodotWebSocket.send must return 0 (not 1) for FRAME_ROOM_INFO");

  // Open Client via HybridWebSocket wrapper
  const clientWs = new win.WebSocket(`${BASE_WS}/ws?role=client&room=${roomPort}`);
  assert.equal(clientWs.CONNECTING, 0);
  assert.equal(clientWs.OPEN, 1);
  assert.equal(clientWs.CLOSING, 2);
  assert.equal(clientWs.CLOSED, 3);
  assert.equal(typeof clientWs.protocol, "string");

  const clientQ = attachQueue(clientWs);
  await clientQ.waitOpen();
  assert.equal(clientWs.readyState, clientWs.OPEN);
  assert.ok(clientWs.readyState < clientWs.CLOSING);

  await clientQ.waitFrame(
    (f) => f.frameType === FRAME_PEER_CONNECTED && f.peerId === 1,
    "client FRAME_PEER_CONNECTED(1)"
  );
  const hostPeerJoin = await hostQ.waitFrame(
    (f) => f.frameType === FRAME_PEER_CONNECTED && f.peerId > 1,
    "host FRAME_PEER_CONNECTED(peerId)"
  );
  const clientPeerId = hostPeerJoin.peerId;

  // Simulate Godot's transport_identity_request / transport_identity handshake over FRAME_DATA
  const reqJson = JSON.stringify({
    msg_type: "transport_identity_request",
    protocol_version: 4,
    mod_version: "6.3.0",
    game_version: "1.2.0.1",
    run_id: "0".repeat(64),
  });
  assert.equal(godotJsWebSocketSend(hostWs, makeFrame(FRAME_DATA, clientPeerId, 0, reqJson)), 0);
  const rxReq = await clientQ.waitFrame((f) => f.frameType === FRAME_DATA && f.peerId === 1, "client transport_identity_request");
  assert.equal(JSON.parse(rxReq.payload).msg_type, "transport_identity_request");

  const respJson = JSON.stringify({
    msg_type: "transport_identity",
    installation_id: "a".repeat(64),
    resume_token: "b".repeat(64),
    protocol_version: 4,
  });
  assert.equal(godotJsWebSocketSend(clientWs, makeFrame(FRAME_DATA, 1, 0, respJson)), 0);
  const rxResp = await hostQ.waitFrame(
    (f) => f.frameType === FRAME_DATA && f.peerId === clientPeerId,
    "host transport_identity"
  );
  assert.equal(JSON.parse(rxResp.payload).msg_type, "transport_identity");

  // Cleanly close both sockets via GodotWebSocket.close check
  godotJsWebSocketClose(clientWs, 1000, "test_done");
  await clientQ.waitClose();
  assert.equal(clientWs.readyState, clientWs.CLOSED);

  godotJsWebSocketClose(hostWs, 1000, "test_done");
  await hostQ.waitClose();
  assert.equal(hostWs.readyState, hostWs.CLOSED);

  console.log("[PASS] GodotWebSocket contract & host/client binary handshake verified!");
}

main().catch((err) => {
  console.error("[FAIL]", err);
  process.exit(1);
});

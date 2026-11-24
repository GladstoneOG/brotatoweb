#!/usr/bin/env node
/**
 * Automated End-to-End Integration Test for Authoritative Room DO + Hibernation + Failover
 * Run against local `npx wrangler dev --port 8787`:
 *   node scripts/test_multiplayer_do.mjs [base_url]
 */

import assert from "node:assert/strict";
import WebSocket from "ws";

const BASE_HTTP = (process.argv[2] || "http://127.0.0.1:8787").replace(/\/+$/, "");
const BASE_WS = BASE_HTTP.replace(/^http/, "ws");

const FRAME_DATA = 1;
const FRAME_PEER_CONNECTED = 2;
const FRAME_PEER_DISCONNECTED = 3;
const FRAME_ROOM_INFO = 5;
const FRAME_HOST_ASSIGNED = 6;
const FRAME_REJECTED = 7;

function makeFrame(type, peerId, channel, payloadStrOrBuf = "") {
  const payload =
    typeof payloadStrOrBuf === "string"
      ? Buffer.from(payloadStrOrBuf, "utf8")
      : Buffer.from(payloadStrOrBuf);
  const out = Buffer.allocUnsafe(4 + payload.length);
  out[0] = type & 0xff;
  out[1] = (peerId >> 8) & 0xff;
  out[2] = peerId & 0xff;
  out[3] = channel & 0xff;
  if (payload.length > 0) payload.copy(out, 4);
  return out;
}

class TestSocket {
  constructor(url) {
    this.url = url;
    this.ws = new WebSocket(url);
    this.messages = [];
    this.waiters = [];
    this.closed = false;
    this.closeInfo = null;

    this.ws.on("message", (data, isBinary) => {
      let item;
      if (!isBinary) {
        const text = data.toString("utf8");
        let json = null;
        try {
          json = JSON.parse(text);
        } catch (_) {}
        item = { kind: "text", text, json };
      } else {
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
        item = {
          kind: "bin",
          frameType: buf[0],
          peerId: (buf[1] << 8) | buf[2],
          channel: buf[3],
          payload: buf.subarray(4),
        };
      }
      this.messages.push(item);
      this._flushWaiters();
    });

    this.ws.on("close", (code, reason) => {
      this.closed = true;
      this.closeInfo = { code, reason: reason.toString("utf8") };
      this._flushWaiters();
    });
  }

  async open(timeoutMs = 8000) {
    if (this.ws.readyState === WebSocket.OPEN) return this;
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`Timeout opening ${this.url}`)), timeoutMs);
      this.ws.once("open", () => {
        clearTimeout(timer);
        resolve();
      });
      this.ws.once("error", (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
    return this;
  }

  _flushWaiters() {
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      const w = this.waiters[i];
      const idx = this.messages.findIndex(w.pred);
      if (idx !== -1) {
        const [found] = this.messages.splice(idx, 1);
        clearTimeout(w.timer);
        this.waiters.splice(i, 1);
        w.resolve(found);
      }
    }
  }

  waitFor(pred, label, timeoutMs = 8000) {
    const idx = this.messages.findIndex(pred);
    if (idx !== -1) {
      const [found] = this.messages.splice(idx, 1);
      return Promise.resolve(found);
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.waiters.indexOf(entry);
        if (i !== -1) this.waiters.splice(i, 1);
        reject(new Error(`Timeout waiting for ${label} (buffered=${JSON.stringify(this.messages)})`));
      }, timeoutMs);
      const entry = { pred, resolve, reject, timer };
      this.waiters.push(entry);
    });
  }

  waitJson(type, timeoutMs = 8000) {
    return this.waitFor((m) => m.kind === "text" && m.json && m.json.t === type, `json:${type}`, timeoutMs).then(
      (m) => m.json
    );
  }

  waitFrame(frameType, peerId = null, timeoutMs = 8000) {
    return this.waitFor(
      (m) => m.kind === "bin" && m.frameType === frameType && (peerId === null || m.peerId === peerId),
      `frame:${frameType}(peer=${peerId})`,
      timeoutMs
    );
  }

  waitClose(timeoutMs = 8000) {
    if (this.closed) return Promise.resolve(this.closeInfo);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Timeout waiting for close")), timeoutMs);
      this.ws.once("close", (code, reason) => {
        clearTimeout(timer);
        resolve({ code, reason: reason.toString("utf8") });
      });
    });
  }

  sendJson(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  sendBin(buf) {
    this.ws.send(buf, { binary: true });
  }

  terminate() {
    this.ws.terminate();
  }

  close(code = 1000, reason = "test_done") {
    try {
      this.ws.close(code, reason);
    } catch (_) {}
  }
}

async function runTests() {
  console.log(`[TEST] Target: ${BASE_HTTP}`);

  // 1. Host connects and allocates port via BrotatoRelayHub
  console.log("[1/8] Testing host allocation & discovery...");
  const host1 = await new TestSocket(`${BASE_WS}/ws?role=host&room=27462&name=HostAlpha&graceMs=1200`).open();
  const hostWelcome = await host1.waitJson("session_welcome");
  assert.equal(hostWelcome.role, "host");
  assert.ok(hostWelcome.token, "hostWelcome must include host token");
  const roomPort = hostWelcome.roomPort;
  const hostAssigned = await host1.waitFrame(FRAME_HOST_ASSIGNED, roomPort);
  assert.equal(hostAssigned.peerId, roomPort);

  host1.sendBin(
    makeFrame(
      FRAME_ROOM_INFO,
      0,
      0,
      JSON.stringify({ host_name: "HostAlpha", room_name: "Alpha Run", member_count: 1, state: "character_selection" })
    )
  );
  await new Promise((r) => setTimeout(r, 250));

  const roomsRes = await fetch(`${BASE_HTTP}/api/rooms`);
  assert.equal(roomsRes.status, 200);
  const roomsJson = await roomsRes.json();
  assert.ok(roomsJson.rooms.some((r) => r.game_port === roomPort && r.host_name === "HostAlpha"), "Room must appear in /api/rooms");

  const infoRes = await fetch(`${BASE_HTTP}/api/info`);
  assert.equal(infoRes.status, 200);
  const infoJson = await infoRes.json();
  assert.ok(infoJson.rooms.some((r) => r.game_port === roomPort), "Room must appear in /api/info");

  const discoverWs = await new TestSocket(`${BASE_WS}/ws?role=discover`).open();
  const discMsg = await discoverWs.waitFor((m) => m.kind === "text" && m.json && Array.isArray(m.json.rooms), "discover rooms");
  assert.ok(discMsg.json.rooms.some((r) => r.game_port === roomPort), "Room must appear in discover WS");
  discoverWs.close();

  // 2. Clients P2 & P3 join and verify FRAME_PEER_CONNECTED ordering
  console.log("[2/8] Testing client joins & FRAME_PEER_CONNECTED...");
  const p2 = await new TestSocket(`${BASE_WS}/ws?role=client&room=${roomPort}&name=PlayerBravo`).open();
  const p2Welcome = await p2.waitJson("session_welcome");
  assert.equal(p2Welcome.role, "client");
  assert.equal(p2Welcome.peerId, 2);
  await p2.waitFrame(FRAME_PEER_CONNECTED, 1);
  await host1.waitFrame(FRAME_PEER_CONNECTED, 2);

  const p3 = await new TestSocket(`${BASE_WS}/ws?role=client&room=${roomPort}&name=PlayerCharlie`).open();
  const p3Welcome = await p3.waitJson("session_welcome");
  assert.equal(p3Welcome.peerId, 3);
  await p3.waitFrame(FRAME_PEER_CONNECTED, 1);
  await host1.waitFrame(FRAME_PEER_CONNECTED, 3);

  // 3. Unicast and broadcast binary routing
  console.log("[3/8] Testing binary unicast & broadcast routing...");
  host1.sendBin(makeFrame(FRAME_DATA, 2, 1, "hello-p2"));
  const rxP2 = await p2.waitFrame(FRAME_DATA, 1);
  assert.equal(rxP2.payload.toString("utf8"), "hello-p2");

  host1.sendBin(makeFrame(FRAME_DATA, 0, 1, "hello-all"));
  const bP2 = await p2.waitFrame(FRAME_DATA, 1);
  const bP3 = await p3.waitFrame(FRAME_DATA, 1);
  assert.equal(bP2.payload.toString("utf8"), "hello-all");
  assert.equal(bP3.payload.toString("utf8"), "hello-all");

  p3.sendBin(makeFrame(FRAME_DATA, 1, 0, "from-p3"));
  const rxHostFromP3 = await host1.waitFrame(FRAME_DATA, 3);
  assert.equal(rxHostFromP3.payload.toString("utf8"), "from-p3");

  // 4. WebRTC signaling routing
  console.log("[4/8] Testing WebRTC signaling relay...");
  host1.sendJson({ t: "rtc_signal", targetPeer: 2, payload: { type: "offer", sdp: "dummy-offer" } });
  const sigP2 = await p2.waitJson("rtc_signal");
  assert.equal(sigP2.fromPeer, 1);
  assert.equal(sigP2.payload.type, "offer");

  p2.sendJson({ t: "rtc_signal", targetPeer: 1, payload: { type: "answer", sdp: "dummy-answer" } });
  const sigHost = await host1.waitJson("rtc_signal");
  assert.equal(sigHost.fromPeer, 2);
  assert.equal(sigHost.payload.type, "answer");

  // 5. Wave checkpoint save
  console.log("[5/8] Testing SQLite wave checkpoint save...");
  const sampleRun = JSON.stringify({
    has_run_state: true,
    is_coop_run: true,
    current_wave: 7,
    players_data: [{ character: "well_rounded" }, { character: "brawler" }, { character: "ranger" }],
  });
  host1.sendJson({ t: "checkpoint_save", wave: 7, profileId: 0, runJson: sampleRun });
  const cpAck = await host1.waitJson("checkpoint_ack");
  assert.equal(cpAck.wave, 7);

  // 6. Transient host drop + reconnect within grace window
  console.log("[6/8] Testing transient host drop & seamless reattach...");
  host1.terminate();
  await p2.waitJson("host_reconnecting");
  await p3.waitJson("host_reconnecting");
  assert.equal(p2.closed, false, "P2 socket must remain open during host grace window");
  assert.equal(p3.closed, false, "P3 socket must remain open during host grace window");

  const host1Reattached = await new TestSocket(
    `${BASE_WS}/ws?role=host&room=${roomPort}&token=${encodeURIComponent(hostWelcome.token)}&reattach=1`
  ).open();
  const reWelcome = await host1Reattached.waitJson("session_welcome");
  assert.equal(reWelcome.reattached, true);
  assert.equal(reWelcome.checkpointWave, 7);
  await p2.waitJson("host_reconnected");
  await p3.waitJson("host_reconnected");

  host1Reattached.sendBin(makeFrame(FRAME_DATA, 0, 1, "post-reattach"));
  assert.equal((await p2.waitFrame(FRAME_DATA, 1)).payload.toString("utf8"), "post-reattach");
  assert.equal((await p3.waitFrame(FRAME_DATA, 1)).payload.toString("utf8"), "post-reattach");

  // 7A. Permanent host drop via DO alarm -> elect P2, migrate P3
  console.log("[7/8] Testing permanent host drop -> DO Alarm election + migration...");
  host1Reattached.terminate();
  await p2.waitJson("host_reconnecting");
  await p3.waitJson("host_reconnecting");

  const promoP2 = await p2.waitJson("host_promoted", 8000);
  assert.equal(promoP2.checkpointWave, 7);
  assert.equal(promoP2.remainingPeerCount, 1);
  assert.ok(promoP2.hostToken, "Promoted host must receive pending hostToken");
  assert.equal(JSON.parse(promoP2.checkpoint.runJson).current_wave, 7);

  const migP3 = await p3.waitJson("host_migrating", 8000);
  assert.equal(migP3.checkpointWave, 7);

  // P2 takes over as promoted host
  const p2AsHost = await new TestSocket(
    `${BASE_WS}/ws?role=host&room=${roomPort}&token=${encodeURIComponent(promoP2.hostToken)}&migration=1&name=PlayerBravo&graceMs=1000`
  ).open();
  const p2HostWelcome = await p2AsHost.waitJson("session_welcome");
  assert.equal(p2HostWelcome.migrated, true);
  await p2AsHost.waitFrame(FRAME_HOST_ASSIGNED, roomPort);

  const readyP3 = await p3.waitJson("migration_ready");
  assert.equal(readyP3.roomPort, roomPort);
  p2.close();
  p3.close();

  // P3 rejoins migrated room
  const p3Rejoined = await new TestSocket(`${BASE_WS}/ws?role=client&room=${roomPort}&name=PlayerCharlie`).open();
  const p3NewWelcome = await p3Rejoined.waitJson("session_welcome");
  assert.equal(p3NewWelcome.role, "client");
  await p3Rejoined.waitFrame(FRAME_PEER_CONNECTED, 1);
  const p3AssignedPeerId = (await p2AsHost.waitFrame(FRAME_PEER_CONNECTED)).peerId;
  assert.equal(p3AssignedPeerId, p3NewWelcome.peerId);

  p2AsHost.sendBin(makeFrame(FRAME_DATA, 0, 1, "wave7-resumed"));
  assert.equal((await p3Rejoined.waitFrame(FRAME_DATA, 1)).payload.toString("utf8"), "wave7-resumed");

  // 7B. Also test immediate host_leave_graceful failover -> elect P3
  console.log("      Testing graceful host leave immediate promotion...");
  const wave8Run = JSON.stringify({ has_run_state: true, is_coop_run: true, current_wave: 8, players_data: [{}, {}] });
  p2AsHost.sendJson({ t: "checkpoint_save", wave: 8, profileId: 0, runJson: wave8Run });
  await p2AsHost.waitJson("checkpoint_ack");

  p2AsHost.sendJson({ t: "host_leave_graceful" });
  const promoP3 = await p3Rejoined.waitJson("host_promoted");
  assert.equal(promoP3.checkpointWave, 8);

  const p3AsHost = await new TestSocket(
    `${BASE_WS}/ws?role=host&room=${roomPort}&token=${encodeURIComponent(promoP3.hostToken)}&migration=1&name=PlayerCharlie&graceMs=700`
  ).open();
  assert.equal((await p3AsHost.waitJson("session_welcome")).migrated, true);
  await p3AsHost.waitFrame(FRAME_HOST_ASSIGNED, roomPort);
  p3Rejoined.close();

  // 8. Empty room cleanup removes room from directory
  console.log("[8/8] Testing empty room cleanup & directory removal...");
  p3AsHost.sendJson({ t: "host_leave_graceful" });
  await p3AsHost.waitClose();
  await new Promise((r) => setTimeout(r, 300));
  const afterRes = await fetch(`${BASE_HTTP}/api/rooms`);
  const afterJson = await afterRes.json();
  assert.equal(
    afterJson.rooms.some((r) => r.game_port === roomPort),
    false,
    "Closed empty room must be removed from /api/rooms"
  );

  console.log("[PASS] All 8 Authoritative Room DO + Hibernation + Failover assertions passed!");
}

runTests().catch((err) => {
  console.error("[FAIL]", err);
  process.exit(1);
});

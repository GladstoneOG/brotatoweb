// ============================================================================
// Brotato Web Multiplayer — Hybrid Authoritative Room WS + P2P WebRTC Transport
// Transparent reconnect, wave-checkpoint upload, and automatic host migration
// ============================================================================
(function () {
  "use strict";

  const NativeWebSocket = window.WebSocket;
  if (!NativeWebSocket) return;

  const FRAME_DATA = 1;
  const FRAME_PEER_CONNECTED = 2;
  const FRAME_PEER_DISCONNECTED = 3;
  const FRAME_HOST_ASSIGNED = 6;

  const ICE_CONFIG = {
    iceServers: [
      {
        urls: [
          "stun:stun.l.google.com:19302",
          "stun:stun1.l.google.com:19302",
          "stun:stun.cloudflare.com:3478",
        ],
      },
    ],
  };

  const RECONNECT_BACKOFF_MS = [400, 800, 1500, 2500];
  const MAX_RECONNECT_WINDOW_MS = 22000;
  const KEEPALIVE_INTERVAL_MS = 25000;
  const MAX_DC_BUFFERED_BYTES = 2 * 1024 * 1024;
  const TERMINAL_CLOSE_CODES = new Set([1000, 4000, 4001, 4003, 4004, 4008, 4009, 4010, 4011]);

  let activeSocket = null;
  let lastUploadedCheckpointHash = "";
  let currentBannerTimer = null;

  function fnv1aHash(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
      h ^= str.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return (h >>> 0).toString(16) + ":" + str.length;
  }

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

  function toUint8Array(data) {
    if (data instanceof ArrayBuffer) return new Uint8Array(data);
    if (ArrayBuffer.isView(data)) {
      return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
    }
    return null;
  }

  function notifyTransportMeta(meta) {
    if (window.BrotatoNetBridge && typeof window.BrotatoNetBridge.onTransportMeta === "function") {
      try {
        window.BrotatoNetBridge.onTransportMeta(meta);
      } catch (_) {}
    }
  }

  function queueGameAction(cmd) {
    if (window.BrotatoSaveBridge && typeof window.BrotatoSaveBridge.queueAction === "function") {
      window.BrotatoSaveBridge.queueAction(cmd);
    }
  }

  function extractActiveRunFromBundle(bundleJsonStr) {
    let bundle = null;
    try {
      bundle = typeof bundleJsonStr === "string" ? JSON.parse(bundleJsonStr) : bundleJsonStr;
    } catch (_) {
      return null;
    }
    if (!bundle || typeof bundle !== "object") return null;

    for (const pid of [0, 1, 2]) {
      const key = `user://user/run_v3_${pid}.json`;
      const rawRun = bundle[key];
      if (typeof rawRun !== "string" || !rawRun.trim()) continue;
      try {
        const parsed = JSON.parse(rawRun);
        if (parsed && parsed.has_run_state) {
          const wave = Number(parsed.current_wave || parsed.wave || 1);
          return { profileId: pid, wave, runJson: rawRun };
        }
      } catch (_) {}
    }
    return null;
  }

  class BrotatoHybridSocket {
    constructor(rawUrl, protocols) {
      this._protocols = protocols;
      this._parsedUrl = new URL(rawUrl, location.href);
      this._role = (this._parsedUrl.searchParams.get("role") || "client").toLowerCase();
      this._roomPort = parseInt(this._parsedUrl.searchParams.get("room") || "27462", 10) || 27462;
      this._sessionToken = "";
      this._peerId = this._role === "host" ? 1 : 0;
      this._checkpointWave = 0;
      this._pendingMigration = false;

      // Apply pending host promotion token if transitioning from client -> promoted host
      if (this._role === "host" && window._BROTATO_PENDING_PROMOTION) {
        const promo = window._BROTATO_PENDING_PROMOTION;
        this._roomPort = Number(promo.roomPort) || this._roomPort;
        this._sessionToken = String(promo.hostToken || "");
        this._checkpointWave = Number(promo.checkpointWave || 0);
        this._pendingMigration = true;
        this._parsedUrl.searchParams.set("room", String(this._roomPort));
        this._parsedUrl.searchParams.set("token", this._sessionToken);
        this._parsedUrl.searchParams.set("migration", "1");
        this._parsedUrl.searchParams.delete("reattach");
      }

      if (window.BROTATO_PLAYER_NAME && !this._parsedUrl.searchParams.has("name")) {
        this._parsedUrl.searchParams.set("name", String(window.BROTATO_PLAYER_NAME).slice(0, 24));
      }

      this.url = this._parsedUrl.toString();
      this.protocol = "";
      this.extensions = "";
      this._binaryType = "arraybuffer";
      this._virtualReadyState = NativeWebSocket.CONNECTING;
      this._initialOpenFired = false;
      this._closedByUser = false;
      this._migratingHold = false;
      this._reconnectingWs = false;
      this._reconnectStartedAt = 0;
      this._reconnectAttempt = 0;
      this._reconnectTimer = null;
      this._keepaliveTimer = null;
      this._graceCountdownTimer = null;
      this._graceDeadlineMs = 0;

      this._ws = null;
      this._wsSendQueue = [];
      this._peers = new Map(); // remotePeerId -> { pc, dc, iceRestarted, queuedIce }
      this._knownPeers = new Set();

      this.onopen = null;
      this.onmessage = null;
      this.onerror = null;
      this.onclose = null;
      this._listeners = { open: [], message: [], error: [], close: [] };

      activeSocket = this;
      lastUploadedCheckpointHash = "";
      this._openNativeSocket(this.url);
      this._startKeepalive();
    }

    get readyState() {
      return this._virtualReadyState;
    }

    get bufferedAmount() {
      return this._ws ? this._ws.bufferedAmount : 0;
    }

    get binaryType() {
      return this._binaryType;
    }

    set binaryType(val) {
      this._binaryType = val;
      if (this._ws) {
        try {
          this._ws.binaryType = val;
        } catch (_) {}
      }
    }

    addEventListener(type, fn) {
      if (this._listeners[type] && typeof fn === "function") {
        this._listeners[type].push(fn);
      }
    }

    removeEventListener(type, fn) {
      if (this._listeners[type]) {
        this._listeners[type] = this._listeners[type].filter((f) => f !== fn);
      }
    }

    _emit(type, eventObj) {
      const handler = this["on" + type];
      if (typeof handler === "function") {
        try {
          handler.call(this, eventObj);
        } catch (err) {
          console.error("[HybridTransport] handler error:", err);
        }
      }
      const list = this._listeners[type] || [];
      for (const fn of list) {
        try {
          fn.call(this, eventObj);
        } catch (err) {
          console.error("[HybridTransport] listener error:", err);
        }
      }
    }

    _buildConnectUrl(isReattach) {
      const u = new URL(this._parsedUrl.toString());
      u.searchParams.set("role", this._role);
      u.searchParams.set("room", String(this._roomPort));
      if (window.BROTATO_PLAYER_NAME) {
        u.searchParams.set("name", String(window.BROTATO_PLAYER_NAME).slice(0, 24));
      }
      if (this._sessionToken) {
        u.searchParams.set("token", this._sessionToken);
      }
      if (isReattach) {
        u.searchParams.set("reattach", "1");
        u.searchParams.delete("migration");
      } else if (this._pendingMigration) {
        u.searchParams.set("migration", "1");
      }
      return u.toString();
    }

    _openNativeSocket(targetUrl) {
      if (this._closedByUser || this._virtualReadyState === NativeWebSocket.CLOSED) return;
      let ws = null;
      try {
        ws = this._protocols ? new NativeWebSocket(targetUrl, this._protocols) : new NativeWebSocket(targetUrl);
        ws.binaryType = "arraybuffer";
      } catch (err) {
        this._handleUnderlyingClose({ code: 1006, reason: String(err), wasClean: false }, ws);
        return;
      }

      this._ws = ws;

      ws.onopen = (ev) => {
        if (this._ws !== ws) return;
        this.protocol = ws.protocol || "";
        this._reconnectingWs = false;
        this._reconnectAttempt = 0;
        this._reconnectStartedAt = 0;

        const wasInitial = !this._initialOpenFired;
        this._initialOpenFired = true;
        this._virtualReadyState = NativeWebSocket.OPEN;

        while (this._wsSendQueue.length > 0 && ws.readyState === NativeWebSocket.OPEN) {
          const queued = this._wsSendQueue.shift();
          try {
            ws.send(queued);
          } catch (_) {
            break;
          }
        }

        if (wasInitial) {
          this._emit("open", ev || { type: "open" });
        } else {
          this._clearBannerSoon("Connection restored!", 2000);
        }
        this._publishMeta();
      };

      ws.onmessage = (ev) => {
        if (this._ws !== ws) return;
        if (typeof ev.data === "string") {
          this._handleControlText(ev.data);
          return;
        }
        const u8 = toUint8Array(ev.data);
        if (!u8 || u8.byteLength < 4) return;
        this._handleBinaryFromServer(u8, ev.data);
      };

      ws.onerror = (ev) => {
        if (this._ws !== ws) return;
        if (!this._initialOpenFired && !this._reconnectingWs) {
          this._emit("error", ev || { type: "error" });
        }
      };

      ws.onclose = (ev) => {
        if (this._ws !== ws) return;
        this._handleUnderlyingClose(ev, ws);
      };
    }

    _handleUnderlyingClose(ev, closedWs) {
      if (this._ws === closedWs) {
        this._ws = null;
      }
      if (this._closedByUser || this._virtualReadyState === NativeWebSocket.CLOSED) return;

      const code = Number(ev && ev.code ? ev.code : 1006);
      const canReconnect = Boolean(this._sessionToken) && !TERMINAL_CLOSE_CODES.has(code);

      if (!canReconnect || !this._initialOpenFired) {
        this._closeVirtual(code, (ev && ev.reason) || "");
        return;
      }

      const now = Date.now();
      if (!this._reconnectingWs) {
        this._reconnectingWs = true;
        this._reconnectStartedAt = now;
        this._reconnectAttempt = 0;
      }

      if (now - this._reconnectStartedAt >= MAX_RECONNECT_WINDOW_MS) {
        this._closeVirtual(1006, "reconnect_timeout");
        return;
      }

      const delay = RECONNECT_BACKOFF_MS[Math.min(this._reconnectAttempt, RECONNECT_BACKOFF_MS.length - 1)];
      this._reconnectAttempt++;
      this._publishMeta("Reconnecting to room server...");
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = setTimeout(() => {
        if (this._closedByUser || this._virtualReadyState === NativeWebSocket.CLOSED) return;
        this._openNativeSocket(this._buildConnectUrl(true));
      }, delay);
    }

    _handleControlText(raw) {
      if (raw === "pong") return;
      let msg = null;
      try {
        msg = JSON.parse(raw);
      } catch (_) {
        return;
      }
      if (!msg || typeof msg !== "object") return;

      if (msg.t === "session_welcome") {
        if (msg.token) this._sessionToken = String(msg.token);
        if (msg.roomPort) this._roomPort = Number(msg.roomPort);
        if (msg.peerId) this._peerId = Number(msg.peerId);
        if (msg.checkpointWave !== undefined) this._checkpointWave = Number(msg.checkpointWave) || 0;
        if (msg.migrated || msg.role === "host") {
          this._pendingMigration = false;
          if (window._BROTATO_PENDING_PROMOTION) {
            delete window._BROTATO_PENDING_PROMOTION;
          }
        }
        if (msg.migrated) {
          this._clearBannerSoon(`You are now Host of Room #${this._roomPort} (Wave ${this._checkpointWave})`, 3500);
        } else if (msg.reattached && this._role === "host") {
          for (const pid of this._knownPeers) {
            this._startWebRtcOffer(pid);
          }
        }
        this._publishMeta();
        return;
      }

      if (msg.t === "checkpoint_ack") {
        this._checkpointWave = Number(msg.wave) || this._checkpointWave;
        this._publishMeta();
        return;
      }

      if (msg.t === "rtc_signal") {
        this._handleRtcSignal(Number(msg.fromPeer || 0), msg.payload);
        return;
      }

      if (msg.t === "host_reconnecting") {
        this._graceDeadlineMs = Number(msg.deadlineMs) || (Date.now() + Number(msg.graceMs || 20000));
        this._startGraceCountdown();
        return;
      }

      if (msg.t === "host_reconnected") {
        this._stopGraceCountdown();
        this._clearBannerSoon("Host reconnected! Resuming...", 2500);
        this._publishMeta();
        return;
      }

      if (msg.t === "host_promoted") {
        this._stopGraceCountdown();
        const wave = Number(msg.checkpointWave || 0);
        window._BROTATO_PENDING_PROMOTION = {
          roomPort: Number(msg.roomPort || this._roomPort),
          hostToken: String(msg.hostToken || ""),
          checkpointWave: wave,
        };
        this._publishMeta(`Host dropped — You are the new Host! Resuming Wave ${wave}...`);
        const cp = msg.checkpoint || {};
        queueGameAction({
          action: "migrate_to_host",
          room_port: Number(msg.roomPort || this._roomPort),
          run_json: String(cp.runJson || ""),
          profile_id: Number(cp.profileId || 0),
          wave,
          remaining_peer_count: Number(msg.remainingPeerCount || 0),
        });
        this._closeVirtual(1000, "promoted_to_host");
        return;
      }

      if (msg.t === "host_migrating") {
        this._stopGraceCountdown();
        this._migratingHold = true;
        const newHost = String(msg.newHostName || "Next Player");
        const wave = Number(msg.checkpointWave || 0);
        this._publishMeta(`Host dropped — Migrating run to ${newHost} (Wave ${wave})...`);
        return;
      }

      if (msg.t === "migration_ready") {
        this._stopGraceCountdown();
        const port = Number(msg.roomPort || this._roomPort);
        this._publishMeta(`New Host ready! Rejoining Room #${port}...`);
        queueGameAction({
          action: "auto_rejoin_migrated_room",
          port,
        });
        this._clearBannerSoon(`Rejoining Room #${port}...`, 3500);
        this._closeVirtual(1000, "migration_ready");
      }
    }

    _handleBinaryFromServer(u8, rawBuffer) {
      const frameType = u8[0];
      const peerId = (u8[1] << 8) | u8[2];

      if (frameType === FRAME_HOST_ASSIGNED && this._role === "host" && peerId > 0) {
        this._roomPort = peerId;
      } else if (frameType === FRAME_PEER_CONNECTED && this._role === "host" && peerId > 1) {
        this._knownPeers.add(peerId);
        this._startWebRtcOffer(peerId);
      } else if (frameType === FRAME_PEER_DISCONNECTED && this._role === "host" && peerId > 1) {
        this._knownPeers.delete(peerId);
        this._closePeerRtc(peerId);
      }

      const payload = rawBuffer instanceof ArrayBuffer
        ? rawBuffer
        : u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength);
      this._emit("message", { data: payload });
    }

    send(data) {
      if (this._virtualReadyState !== NativeWebSocket.OPEN) return;

      if (typeof data === "string") {
        this._sendViaWs(data);
        return;
      }

      const u8 = toUint8Array(data);
      if (!u8) {
        this._sendViaWs(data);
        return;
      }

      // Route high-frequency FRAME_DATA via P2P WebRTC DataChannels when open
      if (u8.byteLength >= 4 && u8[0] === FRAME_DATA) {
        const targetPeer = (u8[1] << 8) | u8[2];
        const channel = u8[3];

        if (this._role === "client") {
          const hostPeer = this._peers.get(1);
          if (this._canSendDc(hostPeer)) {
            try {
              hostPeer.dc.send(u8);
              return;
            } catch (_) {}
          }
          this._sendViaWs(u8);
          return;
        }

        if (this._role === "host") {
          if (targetPeer === 0 && this._knownPeers.size > 0) {
            const clientBound = makeFrame(FRAME_DATA, 1, channel, u8.subarray(4));
            let allSentP2P = true;
            const fallbackTargets = [];
            for (const pid of this._knownPeers) {
              const entry = this._peers.get(pid);
              if (this._canSendDc(entry)) {
                try {
                  entry.dc.send(clientBound);
                  continue;
                } catch (_) {}
              }
              allSentP2P = false;
              fallbackTargets.push(pid);
            }
            if (allSentP2P) return;
            for (const pid of fallbackTargets) {
              this._sendViaWs(makeFrame(FRAME_DATA, pid, channel, u8.subarray(4)));
            }
            return;
          }

          if (targetPeer > 1) {
            const entry = this._peers.get(targetPeer);
            if (this._canSendDc(entry)) {
              try {
                entry.dc.send(makeFrame(FRAME_DATA, 1, channel, u8.subarray(4)));
                return;
              } catch (_) {}
            }
          }
        }
      }

      this._sendViaWs(u8);
    }

    _sendViaWs(payload) {
      if (this._ws && this._ws.readyState === NativeWebSocket.OPEN) {
        try {
          this._ws.send(payload);
          return;
        } catch (_) {}
      }
      if (this._wsSendQueue.length < 256) {
        this._wsSendQueue.push(payload instanceof Uint8Array ? payload.slice() : payload);
      }
    }

    _sendControlJson(obj) {
      this._sendViaWs(JSON.stringify(obj));
    }

    _canSendDc(peerEntry) {
      return (
        peerEntry &&
        peerEntry.dc &&
        peerEntry.dc.readyState === "open" &&
        peerEntry.dc.bufferedAmount < MAX_DC_BUFFERED_BYTES
      );
    }

    _startWebRtcOffer(remotePeerId) {
      if (typeof RTCPeerConnection === "undefined") return;
      this._closePeerRtc(remotePeerId);

      try {
        const pc = new RTCPeerConnection(ICE_CONFIG);
        const dc = pc.createDataChannel("brotato-data", { ordered: true });
        dc.binaryType = "arraybuffer";
        const entry = { pc, dc, iceRestarted: false, queuedIce: [] };
        this._peers.set(remotePeerId, entry);
        this._wirePeerConnection(remotePeerId, entry);
        this._wireDataChannel(remotePeerId, dc);

        pc.createOffer()
          .then((offer) => pc.setLocalDescription(offer))
          .then(() => {
            if (pc.localDescription) {
              this._sendControlJson({
                t: "rtc_signal",
                targetPeer: remotePeerId,
                payload: { type: "offer", sdp: pc.localDescription.sdp },
              });
            }
          })
          .catch(() => {});
      } catch (_) {}
    }

    _handleRtcSignal(fromPeer, payload) {
      if (typeof RTCPeerConnection === "undefined" || !payload || !fromPeer) return;

      if (payload.type === "offer") {
        this._closePeerRtc(fromPeer);
        try {
          const pc = new RTCPeerConnection(ICE_CONFIG);
          const entry = { pc, dc: null, iceRestarted: false, queuedIce: [] };
          this._peers.set(fromPeer, entry);
          this._wirePeerConnection(fromPeer, entry);

          pc.ondatachannel = (ev) => {
            if (this._peers.get(fromPeer) !== entry) return;
            entry.dc = ev.channel;
            entry.dc.binaryType = "arraybuffer";
            this._wireDataChannel(fromPeer, entry.dc);
          };

          pc.setRemoteDescription({ type: "offer", sdp: payload.sdp })
            .then(() => this._flushQueuedIce(entry))
            .then(() => pc.createAnswer())
            .then((answer) => pc.setLocalDescription(answer))
            .then(() => {
              if (pc.localDescription) {
                this._sendControlJson({
                  t: "rtc_signal",
                  targetPeer: fromPeer,
                  payload: { type: "answer", sdp: pc.localDescription.sdp },
                });
              }
            })
            .catch(() => {});
        } catch (_) {}
        return;
      }

      const entry = this._peers.get(fromPeer);
      if (!entry || !entry.pc) return;

      if (payload.type === "answer") {
        entry.pc
          .setRemoteDescription({ type: "answer", sdp: payload.sdp })
          .then(() => this._flushQueuedIce(entry))
          .catch(() => {});
      } else if (payload.type === "ice" && payload.candidate) {
        if (entry.pc.remoteDescription) {
          entry.pc.addIceCandidate(payload.candidate).catch(() => {});
        } else {
          entry.queuedIce.push(payload.candidate);
        }
      }
    }

    _flushQueuedIce(entry) {
      const list = entry.queuedIce.splice(0, entry.queuedIce.length);
      return Promise.all(list.map((c) => entry.pc.addIceCandidate(c).catch(() => {})));
    }

    _wirePeerConnection(remotePeerId, entry) {
      const pc = entry.pc;
      pc.onicecandidate = (ev) => {
        if (ev.candidate && this._virtualReadyState === NativeWebSocket.OPEN) {
          this._sendControlJson({
            t: "rtc_signal",
            targetPeer: remotePeerId,
            payload: { type: "ice", candidate: ev.candidate },
          });
        }
      };

      pc.oniceconnectionstatechange = () => {
        const state = pc.iceConnectionState;
        if (state === "failed" && this._role === "host" && !entry.iceRestarted) {
          entry.iceRestarted = true;
          this._startWebRtcOffer(remotePeerId);
        } else if (state === "closed" || state === "failed") {
          this._publishMeta();
        }
      };
    }

    _wireDataChannel(remotePeerId, dc) {
      dc.onopen = () => {
        this._publishMeta();
      };
      dc.onclose = () => {
        this._publishMeta();
      };
      dc.onerror = () => {
        this._publishMeta();
      };
      dc.onmessage = (ev) => {
        if (this._virtualReadyState !== NativeWebSocket.OPEN) return;
        const u8 = toUint8Array(ev.data);
        if (!u8 || u8.byteLength < 4) return;
        const out = new Uint8Array(u8.byteLength);
        out.set(u8);
        const senderId = this._role === "host" ? remotePeerId : 1;
        out[1] = (senderId >> 8) & 0xff;
        out[2] = senderId & 0xff;
        this._emit("message", { data: out.buffer });
      };
    }

    _closePeerRtc(remotePeerId) {
      const entry = this._peers.get(remotePeerId);
      if (!entry) return;
      this._peers.delete(remotePeerId);
      try {
        if (entry.dc) entry.dc.close();
      } catch (_) {}
      try {
        if (entry.pc) entry.pc.close();
      } catch (_) {}
      this._publishMeta();
    }

    _closeAllRtc() {
      for (const pid of Array.from(this._peers.keys())) {
        this._closePeerRtc(pid);
      }
    }

    _startKeepalive() {
      clearInterval(this._keepaliveTimer);
      this._keepaliveTimer = setInterval(() => {
        if (this._ws && this._ws.readyState === NativeWebSocket.OPEN) {
          try {
            this._ws.send("ping");
          } catch (_) {}
        }
      }, KEEPALIVE_INTERVAL_MS);
    }

    _startGraceCountdown() {
      this._stopGraceCountdown();
      const tick = () => {
        const left = Math.max(0, Math.ceil((this._graceDeadlineMs - Date.now()) / 1000));
        if (left > 0) {
          this._publishMeta(`Host connection lost — waiting ${left}s before host failover...`);
        } else {
          this._publishMeta("Host dropped — selecting new Host...");
        }
      };
      tick();
      this._graceCountdownTimer = setInterval(tick, 1000);
    }

    _stopGraceCountdown() {
      clearInterval(this._graceCountdownTimer);
      this._graceCountdownTimer = null;
    }

    _clearBannerSoon(text, delayMs) {
      clearTimeout(currentBannerTimer);
      this._publishMeta(text);
      currentBannerTimer = setTimeout(() => {
        this._publishMeta("");
      }, delayMs);
    }

    _getP2pOpenCount() {
      let count = 0;
      for (const entry of this._peers.values()) {
        if (entry.dc && entry.dc.readyState === "open") count++;
      }
      return count;
    }

    _publishMeta(bannerOverride) {
      const p2pPeers = this._getP2pOpenCount();
      let transportMode = "relay";
      if (this._migratingHold) transportMode = "migrating";
      else if (this._reconnectingWs) transportMode = "reconnecting";
      else if (p2pPeers > 0) transportMode = "p2p";

      notifyTransportMeta({
        transportMode,
        p2pPeers,
        checkpointWave: this._checkpointWave,
        role: this._role,
        roomPort: this._roomPort,
        banner: bannerOverride !== undefined ? bannerOverride : undefined,
      });
    }

    uploadCheckpoint(bundleJsonStr) {
      if (this._role !== "host" || this._virtualReadyState !== NativeWebSocket.OPEN) return;
      const runInfo = extractActiveRunFromBundle(bundleJsonStr);
      if (!runInfo) return;
      const hash = fnv1aHash(runInfo.runJson);
      if (hash === lastUploadedCheckpointHash) return;
      lastUploadedCheckpointHash = hash;
      this._sendControlJson({
        t: "checkpoint_save",
        wave: runInfo.wave,
        profileId: runInfo.profileId,
        runJson: runInfo.runJson,
      });
    }

    sendGracefulHostLeave() {
      if (
        this._role === "host" &&
        this._checkpointWave > 0 &&
        this._ws &&
        this._ws.readyState === NativeWebSocket.OPEN
      ) {
        try {
          this._ws.send(JSON.stringify({ t: "host_leave_graceful" }));
        } catch (_) {}
      }
    }

    _closeVirtual(code = 1000, reason = "") {
      if (this._virtualReadyState === NativeWebSocket.CLOSED) return;
      this._virtualReadyState = NativeWebSocket.CLOSED;
      clearTimeout(this._reconnectTimer);
      clearInterval(this._keepaliveTimer);
      this._stopGraceCountdown();
      this._closeAllRtc();

      if (this._ws) {
        const ws = this._ws;
        this._ws = null;
        try {
          ws.onopen = ws.onmessage = ws.onerror = ws.onclose = null;
          ws.close(code >= 1000 && code < 5000 ? code : 1000, String(reason || "").slice(0, 120));
        } catch (_) {}
      }

      if (activeSocket === this) {
        activeSocket = null;
        notifyTransportMeta({
          transportMode: "idle",
          p2pPeers: 0,
          checkpointWave: 0,
        });
      }

      this._emit("close", {
        type: "close",
        code: Number(code) || 1000,
        reason: String(reason || ""),
        wasClean: Number(code) === 1000,
      });
    }

    close(code = 1000, reason = "") {
      this._closedByUser = true;
      this.sendGracefulHostLeave();
      this._closeVirtual(code, reason);
    }
  }

  function HybridWebSocket(url, protocols) {
    try {
      const parsed = new URL(String(url), location.href);
      const role = (parsed.searchParams.get("role") || "").toLowerCase();
      if (parsed.pathname.endsWith("/ws") && (role === "host" || role === "client")) {
        return new BrotatoHybridSocket(String(url), protocols);
      }
    } catch (_) {}
    return protocols !== undefined ? new NativeWebSocket(url, protocols) : new NativeWebSocket(url);
  }

  HybridWebSocket.CONNECTING = NativeWebSocket.CONNECTING;
  HybridWebSocket.OPEN = NativeWebSocket.OPEN;
  HybridWebSocket.CLOSING = NativeWebSocket.CLOSING;
  HybridWebSocket.CLOSED = NativeWebSocket.CLOSED;
  HybridWebSocket.prototype = NativeWebSocket.prototype;
  window.WebSocket = HybridWebSocket;

  window.addEventListener("pagehide", () => {
    if (activeSocket) activeSocket.sendGracefulHostLeave();
  });
  window.addEventListener("beforeunload", () => {
    if (activeSocket) activeSocket.sendGracefulHostLeave();
  });

  window.BrotatoHybridTransport = {
    uploadCheckpoint(bundleJsonStr) {
      if (activeSocket) activeSocket.uploadCheckpoint(bundleJsonStr);
    },
    setPendingPromotion(info) {
      window._BROTATO_PENDING_PROMOTION = info;
    },
    getStatus() {
      if (!activeSocket) {
        return { transportMode: "idle", p2pPeers: 0, checkpointWave: 0 };
      }
      const p2pPeers = activeSocket._getP2pOpenCount();
      return {
        transportMode: activeSocket._migratingHold
          ? "migrating"
          : activeSocket._reconnectingWs
          ? "reconnecting"
          : p2pPeers > 0
          ? "p2p"
          : "relay",
        p2pPeers,
        checkpointWave: activeSocket._checkpointWave,
        role: activeSocket._role,
        roomPort: activeSocket._roomPort,
      };
    },
  };
})();

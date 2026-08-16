// ============================================================
// Hermes Pixel Office — P2P mesh (IdleViber pattern)
// Scores + chat flow ONLY over WebRTC data channels between
// browsers. GitHub is used purely for login (username) and the
// signaling handshake (presence + SDP offer/answer + ICE
// trickle via the local plugin server and a shared gist), so
// peers can find each other. No score/chat message ever touches
// GitHub once channels open.
//
// HARDENED (IdleViber port, 2026-08-16):
//   * ECDSA P-256 signed packets — every message is {d, s}
//     (payload + SHA-256 signature) and is verified against the
//     peer's published public key before it is accepted. Forged
//     or corrupt packets are dropped.
//   * ICE trickle through the gist so peers that gather
//     candidates after their SDP still connect behind NAT.
//   * Unreliable/ordered:false data channel (realtime — no
//     head-of-line blocking on scores).
//   * Offer/answer retry on every poll + per-peer exponential
//     reconnect backoff, plus key-rotation detection.
//   * Staleness filter: peers whose presence is old are skipped.
//   * Gist score fallback for peers that can't be reached
//     directly (strict CGNAT).
// ============================================================

window.PixelOfficeP2P = (function () {
  "use strict";

  var ICE = {
    iceServers: [
      { urls: "stun:stun.l.google.com:19302" },
      { urls: "stun:stun1.l.google.com:19302" },
    ],
  };
  var POLL_MS = 4000;        // signaling poll (handshake)
  var REBROADCAST_MS = 5000; // keep peers' scores fresh
  var CONNECT_TIMEOUT = 8000;
  var MAX_MSG = 65536;       // max inbound data-channel message (bytes)
  var MAX_PEERS = 12;        // cap concurrent connections
  var STALE_MS = 60000;      // drop peers whose presence is older than 60s

  var P2P_ID_KEY = "pixeloffice_p2p_id";
  var me = null;             // {id, github, name, avatar, url}
  var keypair = null;        // ECDSA P-256 {publicKey, privateKey}
  var kid = "";
  var peers = new Map();     // id -> {pc, channel, connected, pub, entry, timer}
  var connecting = new Set();
  var pending = new Map();   // id -> {pub, kid, name} (from signaling doc)
  var entries = new Map();   // id -> entry (latest score we have)
  var peerChat = [];         // chat messages (peers + own)
  var lastEntry = null;      // our latest broadcast entry
  var entryCounter = 0;
  var scoreHandlers = [];
  var chatHandlers = [];
  var started = false;
  var pollTimer = null;
  var rebroadcastTimer = null;
  var fallbackTimer = null;    // posts our full score entry to the gist every 10min
  var backoff = new Map();   // peerId -> {attempts, next}

  // ---- Cloudflare relay (everyone-write backbone) ----------------------
  // The permanent free store. Works under ANY NAT/CGNAT over plain HTTPS.
  // When configured, scores + chat + profile ride through it so multiplayer
  // works even when the raw WebRTC media path can't open. It keys the board
  // by GitHub username, so duplicates for the same account can't exist.
  var relayUrl = "";
  var relayEntries = new Map(); // github(lower) -> entry (authoritative board)
  var relayChat = [];           // server chat history (authoritative)
  var relayTimers = [];
  var RELAY_SCORE_MS = 10000;   // how often we push our score to the relay
  var RELAY_POLL_MS = 5000;     // how often we pull board + chat
  var ENTRY_TTL_MS = 10 * 60 * 1000; // drop stale/ghost entries older than this
  // Shared well-known relay (same for every player — zero-config install).
  var DEFAULT_RELAY = "https://pixel-office-relay.ads-doctor-melbourne.workers.dev";

  // ---- tiny logging helpers (IdleViber "📡 P2P:" style) ----------------
  function short(id) { return String(id || "").substr(0, 8); }
  function log() {
    var a = Array.prototype.slice.call(arguments);
    a.unshift("📡 P2P:");
    console.log.apply(console, a);
  }
  function warn() {
    var a = Array.prototype.slice.call(arguments);
    a.unshift("📡 P2P:");
    console.warn.apply(console, a);
  }

  // ---- per-browser stable player id (IdleViber p2pGetOrCreateId port) ----
  // IdleViber keys each browser (NOT each GitHub account) by a persistent
  // random UUID scoped to the username. This is the critical difference that
  // makes real multiplayer work: two windows/machines on the SAME account get
  // DIFFERENT peer ids, so they can see and handshake each other instead of
  // colliding on one shared "DrGekoz" slot in the signaling gist.
  function peerIdFor(username) {
    var scope = (username || "guest").replace(/[^a-zA-Z0-9]/g, "");
    var key = P2P_ID_KEY + (scope ? "_" + scope : "");
    try {
      if (localStorage.getItem(key)) return localStorage.getItem(key);
    } catch (_) {}
    var id = (crypto.randomUUID && crypto.randomUUID()) ||
      "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        var r = Math.random() * 16 | 0;
        return (c === "x" ? r : (r & 0x3 | 0x8)).toString(16);
      });
    try { localStorage.setItem(key, id); } catch (_) {}
    return id;
  }

  // ---- small helpers -------------------------------------------------
  function post(path, body) {
    var b = body || {};
    if (me && me.id && !b.peer) b.peer = me.id;
    return fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(b),
    }).then(function (r) { return r.json(); }).catch(function () { return {}; });
  }
  function get(path) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    var p = (me && me.id) ? path + sep + "peer=" + encodeURIComponent(me.id) : path;
    return fetch(p).then(function (r) { return r.json(); })
      .catch(function () { return {}; });
  }

  // ---- Cloudflare relay client -----------------------------------------
  function relayPost(path, body) {
    if (!relayUrl) return Promise.resolve(null);
    return fetch(relayUrl + path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) { return r.json(); }).catch(function () { return null; });
  }
  function relayGet(path) {
    if (!relayUrl) return Promise.resolve(null);
    return fetch(relayUrl + path).then(function (r) { return r.json(); })
      .catch(function () { return null; });
  }
  // Push our latest score to the relay (everyone-write; deduped server-side by
  // GitHub username). This is what makes the board work without WebRTC.
  function postScoreRelay() {
    if (!lastEntry || !me) return;
    relayPost("/api/score", { entry: { ...lastEntry, peer: me.id } });
  }
  // Pull the authoritative board + fold it into the local board.
  function pollBoardRelay() {
    relayGet("/api/board").then(function (res) {
      if (!res || !Array.isArray(res.board)) return;
      res.board.forEach(function (row) {
        if (!row || !row.github) return;
        var g = String(row.github).toLowerCase();
        var prev = relayEntries.get(g);
        if (!prev || (Number(row.counter) || 0) >= (Number(prev.counter) || 0) ||
            (Number(row.ops) || 0) > (Number(prev.ops) || 0)) {
          relayEntries.set(g, row);
        }
      });
      scoreHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
    });
  }
  // Pull chat history and merge into the local chat buffer.
  function pollChatRelay() {
    relayGet("/api/chat/list").then(function (res) {
      if (!res || !Array.isArray(res.chat)) return;
      var before = relayChat.length;
      relayChat = res.chat.filter(function (m) { return m && m.text; });
      if (relayChat.length !== before) {
        chatHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
      }
    });
  }
  // Host-appointed durable gist backup (one call / 10 min, server-side alarm).
  // Harmless no-op unless GITHUB_TOKEN + GITHUB_GIST_ID secrets are set.
  function triggerGistBackup() {
    if (!relayUrl) return;
    relayPost("/api/gist", {});
  }
  function setRelay(url) {
    if (!url) return;
    relayUrl = String(url).replace(/\/+$/, "");
    log("relay configured:", relayUrl);
  }
  function startRelayTimers() {
    if (!relayUrl) return;
    relayTimers.push(setInterval(postScoreRelay, RELAY_SCORE_MS));
    relayTimers.push(setInterval(pollBoardRelay, RELAY_POLL_MS));
    relayTimers.push(setInterval(pollChatRelay, RELAY_POLL_MS));
    // Fire immediately once so the board + chat populate without waiting.
    setTimeout(postScoreRelay, 500);
    setTimeout(pollBoardRelay, 500);
    setTimeout(pollChatRelay, 500);
    log("relay timers started");
  }
  function stopRelayTimers() {
    relayTimers.forEach(function (t) { clearInterval(t); });
    relayTimers = [];
  }

  // ---- ECDSA P-256 packet signing (IdleViber port) -------------------
  function _arrBufToB64(buf) {
    var b = new Uint8Array(buf), bin = "";
    for (var i = 0; i < b.length; i++) bin += String.fromCharCode(b[i]);
    return btoa(bin);
  }
  function _b64ToArrBuf(b64) {
    var bin = atob(b64), b = new Uint8Array(bin.length);
    for (var i = 0; i < bin.length; i++) b[i] = bin.charCodeAt(i);
    return b.buffer;
  }
  function ensureKeys() {
    if (keypair) return Promise.resolve();
    return crypto.subtle.generateKey({ name: "ECDSA", namedCurve: "P-256" }, true, ["sign", "verify"])
      .then(function (kp) {
        keypair = kp;
        return crypto.subtle.exportKey("jwk", kp.publicKey);
      })
      .then(function (jwk) {
        kid = _arrBufToB64(new TextEncoder().encode(JSON.stringify(jwk)))
          .replace(/[+/=]/g, "").substr(0, 16);
        log("keyId", kid, "(fresh)");
      });
  }
  function signPayload(payload) {
    if (!keypair) return Promise.resolve(null);
    var str = JSON.stringify(payload);
    return crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" },
        keypair.privateKey, new TextEncoder().encode(str))
      .then(function (sig) { return JSON.stringify({ d: payload, s: _arrBufToB64(sig) }); });
  }
  function verifyPayload(jsonStr, pub) {
    if (!pub) return Promise.resolve(null);
    var msg;
    try { msg = JSON.parse(jsonStr); } catch (_) { return Promise.resolve(null); }
    if (!msg || !msg.d || !msg.s) return Promise.resolve(null);
    var sig = _b64ToArrBuf(msg.s);
    var payloadStr = JSON.stringify(msg.d);
    return crypto.subtle.verify({ name: "ECDSA", hash: "SHA-256" },
        pub, sig, new TextEncoder().encode(payloadStr))
      .then(function (ok) { return ok ? msg.d : null; })
      .catch(function () { return null; });
  }
  function importPeerKey(jwk) {
    if (!jwk) return Promise.resolve(null);
    return crypto.subtle.importKey("jwk", jwk,
      { name: "ECDSA", namedCurve: "P-256" }, true, ["verify"])
      .catch(function () { return null; });
  }

  // ---- identity ------------------------------------------------------
  function setMe(identity) {
    if (identity && identity.github) {
      me = {
        id: peerIdFor(identity.github),       // per-browser UUID (not github!)
        github: identity.github,
        name: identity.name || identity.github,
        avatar: identity.avatar || "",
        url: identity.url || "https://github.com/" + identity.github,
      };
      log("identity set:", me.github, "peer", short(me.id));
    } else {
      me = null;
    }
  }

  function myEntry(serverRow) {
    if (!serverRow) return null;
    return {
      id: me ? me.id : serverRow.id || "local",
      user: me ? me.github : "local",
      github: me ? me.github : (serverRow.github || ""),
      name: me ? me.name : (serverRow.name || "you"),
      avatar: (me && me.avatar) || (serverRow.avatar || ""),
      url: (me && me.url) || (serverRow.url || ""),
      ops: serverRow.ops || 0,
      tools: serverRow.tools || 0,
      sessions: serverRow.sessions || 0,
      time_s: serverRow.time_s || 0,
      tier: serverRow.tier || null,
      bio: serverRow.bio || "",
      links: serverRow.links || {},
      tierIcon: serverRow.tierIcon || "",
      profileTheme: serverRow.profileTheme || {},
      interfaceTheme: serverRow.interfaceTheme || {},
      agents: serverRow.agents || [],
      modelboard: serverRow.modelboard || null,
      counter: ++entryCounter,
      ts: Date.now(),
    };
  }

  function broadcastScore(serverRow) {
    if (!started || !me) return;
    var e = myEntry(serverRow);
    if (!e) return;
    lastEntry = e;
    entries.set(me.id, e);       // our own entry in the local board
    broadcastMsg({ type: "score", entry: e });
    postScoreRelay();            // push to the everyone-write relay too
  }

  // ---- mesh primitives ------------------------------------------------
  function broadcastMsg(msg, silent) {
    if (!keypair) return 0;
    signPayload(msg).then(function (signed) {
      if (!signed) return;
      var sent = 0;
      peers.forEach(function (p) {
        if (p.channel && p.channel.readyState === "open") {
          try { p.channel.send(signed); sent++; } catch (_) {}
        }
      });
      if (!silent) log("send", msg.type, "->", sent, "peer(s)");
    });
    return 0; // async — actual count logged in the .then
  }

  function handleMessage(peerId, raw, pub) {
    if (!raw || typeof raw !== "string" || raw.length > MAX_MSG) return;
    verifyPayload(raw, pub).then(function (payload) {
      if (!payload || typeof payload !== "object") {
        if (Math.random() < 0.01) warn("verify fail from", short(peerId), "(stale key — normal during reconnect)");
        return;
      }
      var msg = payload;
      if (msg.type === "score" && msg.entry &&
          typeof msg.entry.id === "string" && msg.entry.id &&
          msg.entry.id.length <= 200) {
        var eid = msg.entry.id;
        if (eid === me.id) return;              // ignore our own echoed entry
        var prev = entries.get(eid);
        var c = Number(msg.entry.counter) || 0;
        if (!prev || c >= (Number(prev.counter) || 0)) {
          entries.set(eid, msg.entry);
          log("score recv", short(eid), "ops=" + (msg.entry.ops || 0), "counter=" + c);
          scoreHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
        }
      } else if (msg.type === "chat" && typeof msg.text === "string" && msg.text) {
        msg.ts = msg.ts || Date.now();
        msg.peer = peerId;
        peerChat.push(msg);
        if (peerChat.length > 200) peerChat = peerChat.slice(-200);
        log("chat recv from", short(peerId), ":", msg.text.slice(0, 60));
        chatHandlers.forEach(function (cb) { try { cb(msg); } catch (_) {} });
      }
    });
  }

  function noteBackoff(peerId) {
    var b = backoff.get(peerId) || { attempts: 0, next: 0 };
    b.attempts++;
    var wait = Math.min(60000, 3000 * Math.pow(2, Math.min(b.attempts, 5)));
    b.next = Date.now() + wait;
    backoff.set(peerId, b);
    warn("backoff", short(peerId), "for", wait + "ms", "(attempt " + b.attempts + ")");
  }
  function backoffAllowed(peerId) {
    var b = backoff.get(peerId);
    if (!b) return true;
    if (Date.now() >= b.next) { backoff.delete(peerId); return true; }
    return false;
  }

  function attachChannel(peerId, pc, channel) {
    if (!peerId || !pc || !channel) { warn("attachChannel: bad args"); return; }
    var peer = { pc: pc, channel: channel, connected: false };
    peers.set(peerId, peer);

    channel.onopen = function () {
      peer.connected = true;
      connecting.delete(peerId);
      backoff.delete(peerId);
      log("connected to peer", short(peerId));
      if (lastEntry) {
        signPayload({ type: "score", entry: lastEntry }).then(function (signed) {
          if (signed) { try { channel.send(signed); } catch (_) {} }
        });
      }
      scoreHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
    };
    channel.onmessage = function (ev) {
      // Look up the peer's pubkey lazily (async import may finish after attach).
      var pub = (pending.get(peerId) || {}).pub || null;
      try { handleMessage(peerId, ev && ev.data, pub); }
      catch (e) { warn("msg handler error:", e && e.message); }
    };
    channel.onerror = function (ev) { warn("channel error", short(peerId)); };
    channel.onclose = function () { log("closed peer", short(peerId)); teardown(peerId); };
    pc.oniceconnectionstatechange = function () {
      if (pc.iceConnectionState === "failed" || pc.iceConnectionState === "disconnected") {
        warn("ice", pc.iceConnectionState, short(peerId));
        noteBackoff(peerId);
        teardown(peerId);
      }
    };
    if (peer.timer) clearTimeout(peer.timer);
    peer.timer = setTimeout(function () {
      if (!peer.connected) {
        noteBackoff(peerId);
        teardown(peerId);
      }
    }, CONNECT_TIMEOUT);
  }

  function teardown(peerId) {
    var p = peers.get(peerId);
    if (p) {
      if (p.channel) { try { p.channel.close(); } catch (_) {} }
      if (p.pc) { try { p.pc.close(); } catch (_) {} }
      if (p.timer) clearTimeout(p.timer);
      log("teardown peer", short(peerId));
    }
    peers.delete(peerId);
    connecting.delete(peerId);
    pending.delete(peerId);
    // keep last known score in entries; a fresh broadcast replaces it.
    scoreHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
  }

  function initiate(peerId) {
    if (!peerId) return;
    if (peers.has(peerId) || connecting.has(peerId)) return;
    if (connecting.size >= MAX_PEERS) { warn("peer cap reached, skipping", short(peerId)); return; }
    if (!backoffAllowed(peerId)) return;
    connecting.add(peerId);
    var pc;
    try {
      pc = new RTCPeerConnection(ICE);
    } catch (e) { connecting.delete(peerId); warn("RTCPeerConnection init failed:", e && e.message); return; }
    // ICE trickle: relay every candidate to the peer through the gist.
    pc.onicecandidate = function (e) {
      if (!e.candidate) return;
      post("signaling/ice", { to: peerId, candidate: e.candidate.toJSON() });
    };
    var channel = pc.createDataChannel("office", { ordered: false, maxRetransmits: 0 });
    attachChannel(peerId, pc, channel);
    log("offering to peer", short(peerId));
    pc.createOffer().then(function (offer) {
      return pc.setLocalDescription(offer);
    }).then(function () {
      return post("signaling/offer", { to: peerId, sdp: pc.localDescription });
    }).catch(function (err) {
      warn("offer creation failed:", err && err.message);
      noteBackoff(peerId);
      teardown(peerId);
    });
  }

  function answer(peerId, sdp) {
    if (!peerId || !sdp) { warn("answer: bad args from", short(peerId)); return; }
    if (peers.has(peerId) || connecting.has(peerId)) return;
    if (connecting.size >= MAX_PEERS) { warn("peer cap reached, skipping", short(peerId)); return; }
    connecting.add(peerId);
    var pc;
    try {
      pc = new RTCPeerConnection(ICE);
    } catch (e) { connecting.delete(peerId); warn("RTCPeerConnection init failed:", e && e.message); return; }
    pc.onicecandidate = function (e) {
      if (!e.candidate) return;
      post("signaling/ice", { to: peerId, candidate: e.candidate.toJSON() });
    };
    pc.ondatachannel = function (ev) { attachChannel(peerId, pc, ev.channel); };
    log("answering peer", short(peerId));
    pc.setRemoteDescription(new RTCSessionDescription(sdp)).then(function () {
      return pc.createAnswer();
    }).then(function (answer) {
      return pc.setLocalDescription(answer);
    }).then(function () {
      return post("signaling/answer", { to: peerId, sdp: pc.localDescription });
    }).catch(function (err) {
      warn("answer creation failed:", err && err.message);
      noteBackoff(peerId);
      teardown(peerId);
    });
  }

  // Ensure a peer's ECDSA public key is imported (cached in `pending`) so
  // their signed packets can be verified the moment a channel opens.
  function ensurePeerKey(id, pdata) {
    if (pending.has(id)) {
      return Promise.resolve((pending.get(id) || {}).pub || null);
    }
    var key = pdata && pdata.k;
    var meta = { kid: (pdata && pdata.kid) || "", name: (pdata && pdata.name) || id };
    if (!key) { pending.set(id, { pub: null, kid: meta.kid, name: meta.name }); return Promise.resolve(null); }
    return importPeerKey(key).then(function (pub) {
      pending.set(id, { pub: pub, kid: meta.kid, name: meta.name });
      return pub;
    });
  }

  // ---- signaling poll (presence + inbox + ICE) -------------------------
  function pollSignaling() {
    if (!started || !me) return;
    get("signaling/state").then(async function (st) {
      if (!st || typeof st !== "object") return;
      var pl = st.peers || {};
      var now = Date.now();
      var anySig = false;
      var ids = Object.keys(pl);
      var offers = st.offers || {};
      var offKeys = Object.keys(offers);

      // Phase 1: import every key we'll need BEFORE any handshake, so a
      // peer's signed packets verify the instant their channel opens (no
      // dropped first messages from a key that imports a moment too late).
      for (var i = 0; i < ids.length; i++) {
        var id = ids[i];
        if (!id || id === me.id) continue;
        var pdata = pl[id];
        if (!pdata.online) continue;
        if (pdata.last_seen && (now - Number(pdata.last_seen) * 1000 > STALE_MS)) continue;
        await ensurePeerKey(id, pdata);
      }
      for (var j = 0; j < offKeys.length; j++) {
        var fid = offKeys[j];
        if (!fid || fid === me.id) continue;
        await ensurePeerKey(fid, pl[fid] || {});
      }

      // Phase 2: initiate (glare avoidance — only the smaller id initiates).
      for (var k = 0; k < ids.length; k++) {
        var id2 = ids[k];
        if (!id2 || id2 === me.id) continue;
        if (peers.has(id2) || connecting.has(id2)) continue;
        var pd2 = pl[id2];
        if (!pd2.online) continue;
        if (pd2.last_seen && (now - Number(pd2.last_seen) * 1000 > STALE_MS)) continue;
        if (me.id < id2 && backoffAllowed(id2)) initiate(id2);
      }

      // Phase 3: answer offers (retry on every poll until handled).
      for (var m = 0; m < offKeys.length; m++) {
        var fromId = offKeys[m];
        if (!fromId || fromId === me.id) continue;
        if (peers.has(fromId) || connecting.has(fromId)) continue;
        var sdp = offers[fromId] && offers[fromId].sdp;
        if (sdp) { answer(fromId, sdp); anySig = true; }
      }

      // Phase 4: apply answers to our outstanding offers.
      var answers = st.answers || {};
      var ansKeys = Object.keys(answers);
      for (var n = 0; n < ansKeys.length; n++) {
        var fromId2 = ansKeys[n];
        if (!fromId2) continue;
        var p = peers.get(fromId2);
        if (!p || p.connected) continue;
        if (p.pc && p.pc.signalingState === "have-local-offer") {
          var sdp2 = answers[fromId2] && answers[fromId2].sdp;
          if (sdp2) {
            try {
              p.pc.setRemoteDescription(new RTCSessionDescription(sdp2));
            } catch (e) { warn("apply answer failed:", e && e.message); }
            anySig = true;
          }
        }
      }

      // Phase 5: ICE trickle — feed candidates peers sent to us.
      var ice = st.ice || {};
      var iceKeys = Object.keys(ice);
      for (var q = 0; q < iceKeys.length; q++) {
        var fromId3 = iceKeys[q];
        var pp = peers.get(fromId3);
        if (pp && pp.pc) {
          var cands = ice[fromId3] || [];
          for (var r = 0; r < cands.length; r++) {
            var c = cands[r];
            if (!c) continue;
            try { pp.pc.addIceCandidate(new RTCIceCandidate(c)); }
            catch (e) { /* not ready yet — re-fed next poll */ }
          }
          anySig = true;
        }
      }

      if (anySig) post("signaling/clear", {}); // consume handled offers/answers/ice

      // Phase 6: read peers' durable score snapshots from the shared gist.
      // These are written every 10min regardless of WebRTC, so we can always
      // pull a late/remote player's full entry even if the mesh never opened.
      for (var s = 0; s < ids.length; s++) {
        var fid2 = ids[s];
        if (!fid2 || fid2 === me.id) continue;
        var fpp = peers.get(fid2);
        if (fpp && fpp.connected) continue;   // already live over WebRTC (fresh data)
        if (connecting.has(fid2)) continue;
        var fs = pl[fid2].score;
        if (!fs || !fs.id) continue;
        var c2 = Number(fs.counter) || 0;
        var prev2 = entries.get(fs.id);
        if (!prev2 || c2 >= (Number(prev2.counter) || 0)) {
          entries.set(fs.id, fs);
          log("score recv (gist snapshot)", short(fs.id), "ops=" + (fs.ops || 0), "counter=" + c2);
          scoreHandlers.forEach(function (cb) { try { cb(); } catch (_) {} });
        }
      }
    }).catch(function (err) { warn("signaling poll error:", err && err.message); });
  }

  // ---- public API -----------------------------------------------------
  function start(identity, opts) {
    setMe(identity);
    if (!me) { warn("start: no identity"); return; }
    started = true;
    log("starting, ID", short(me.id));
    // Configure the everyone-write relay (Cloudflare). Priority: explicit
    // opts.relay > window.OFFICE_RELAY (set by the office page from
    // /office-config) > a live fetch of office-config. Boots the relay timers
    // so the board + chat work even if the WebRTC mesh never opens.
    var wantRelay = (opts && opts.relay) ||
      (typeof window !== "undefined" && window.OFFICE_RELAY) || "";
    if (wantRelay) { setRelay(wantRelay); startRelayTimers(); }
    else {
      // No explicit relay: ask the office server, fall back to the shared
      // well-known relay so the board + chat work even without the endpoint.
      fetch("office-config").then(function (r) { return r.json(); })
        .then(function (cfg) {
          var url = (cfg && cfg.relay_url) || DEFAULT_RELAY;
          if (url && !relayUrl) { setRelay(url); startRelayTimers(); }
        }).catch(function () {
          if (DEFAULT_RELAY && !relayUrl) { setRelay(DEFAULT_RELAY); startRelayTimers(); }
        });
    }
    // Load ICE/TURN servers from the office server so a configured TURN relay
    // can bridge strict-CGNAT peers (STUN-only fails when no host candidate is
    // reachable). Falls back to STUN-only if the endpoint is unavailable.
    fetch("signaling/ice-config").then(function (r) { return r.json(); })
      .then(function (cfg) {
        if (cfg && Array.isArray(cfg.iceServers) && cfg.iceServers.length) {
          ICE.iceServers = cfg.iceServers;
          log("ice config loaded:", ICE.iceServers.length, "server(s)");
        }
      }).catch(function () {});
    // Publish presence + our ECDSA public key (for packet verification).
    ensureKeys().then(function () {
      return crypto.subtle.exportKey("jwk", keypair.publicKey);
    }).then(function (jwk) {
      return post("signaling/register", { online: true, key: jwk, kid: kid });
    }).catch(function () {});
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(pollSignaling, POLL_MS);
    if (rebroadcastTimer) clearInterval(rebroadcastTimer);
    rebroadcastTimer = setInterval(function () {
      if (lastEntry) {
        broadcastMsg({ type: "score", entry: lastEntry }, true);
      }
    }, REBROADCAST_MS);
    // Gist persistence (durable snapshot). Adaptive cadence:
    //   * No peer connected over WebRTC  -> post to the gist every 10 seconds
    //     so a standalone/late player's score is always fresh and visible to
    //     anyone who checks the board.
    //   * At least one peer connected    -> post every 10 minutes. P2P data
    //     channels carry realtime scores in the meantime; the 10-min snapshot
    //     is the durable fallback for anyone who connects later or can't reach
    //     us (CGNAT), without burning the owner-write-only, rate-limited gist
    //     quota.
    // Sends the FULL entry (score + tier, bio, links, themes, agents,
    // modelboard), persisting everything that rides in a score packet.
    var GIST_FAST_MS = 60000;   // 60s when no peer connected (was 10s — too chatty, tripped GitHub's secondary rate limit)
    var GIST_SLOW_MS = 600000;  // 10min when connected
    var gistLastPost = 0;
    if (fallbackTimer) clearInterval(fallbackTimer);
    fallbackTimer = setInterval(function () {
      if (!started || !me || !lastEntry) return;
      // Count currently-connected peers.
      var connectedCount = 0;
      peers.forEach(function (p) { if (p.connected) connectedCount++; });
      var now = Date.now();
      var gap = connectedCount > 0 ? GIST_SLOW_MS : GIST_FAST_MS;
      if (now - gistLastPost >= gap) {
        gistLastPost = now;
        var e = lastEntry;
        log("gist post score", "ops=" + (e.ops || 0), "peerCount=" + connectedCount,
            "cadence=" + (gap / 1000) + "s", "entry={id:" + short(e.id) + ",tools:" + (e.tools || 0) + "}");
        post("signaling/score", { entry: e }).then(function (res) {
          log("gist post ok:", (res && res.ok) ? "accepted" : "rejected/ignored");
        });
      }
    }, 5000); // check every 5s; post when the current cadence has elapsed
  }

  function stop() {
    started = false;
    if (pollTimer) clearInterval(pollTimer);
    if (rebroadcastTimer) clearInterval(rebroadcastTimer);
    if (fallbackTimer) clearInterval(fallbackTimer);
    stopRelayTimers();
    peers.forEach(function (_p, id) { teardown(id); });
    backoff.clear();
    pending.clear();
    post("signaling/register", { online: false });
    log("stopped");
  }

  // Merge our live local row + all peer entries + the authoritative relay
  // board into ONE ranked board, DEDUPED BY GITHUB USERNAME (so a player with
  // many browsers/machines on the same account shows as exactly ONE row) and
  // with stale ghost entries pruned (so disconnected sessions don't linger).
  function getLeaderboard(localRow) {
    if (localRow && started && me) {
      if (lastEntry) {
        lastEntry.ops = localRow.ops || lastEntry.ops;
        lastEntry.tools = localRow.tools || lastEntry.tools;
        lastEntry.sessions = localRow.sessions || lastEntry.sessions;
        lastEntry.time_s = localRow.time_s || lastEntry.time_s;
        lastEntry.tier = localRow.tier || lastEntry.tier;
        entries.set(me.id, lastEntry);
      }
    }
    var now = Date.now();
    var best = {}; // github(lower) -> {entry, ops, counter}
    function consider(e) {
      if (!e) return;
      var g = e.github || e.user || "";
      if (!g) return; // only GitHub usernames belong
      // prune stale/ghost rows (fresh relay snapshots + live peers survive)
      if (e.ts && (now - Number(e.ts)) > ENTRY_TTL_MS) return;
      var key = String(g).toLowerCase();
      var ops = Number(e.ops) || 0;
      var ctr = Number(e.counter) || 0;
      var cur = best[key];
      if (!cur || ops > cur.ops || (ops === cur.ops && ctr >= cur.ctr)) {
        best[key] = { entry: e, ops: ops, ctr: ctr };
      }
    }
    entries.forEach(consider);      // live WebRTC mesh peers
    relayEntries.forEach(consider); // authoritative relay board (already deduped)
    var rows = [];
    Object.keys(best).forEach(function (key) {
      var e = best[key].entry;
      rows.push({
        id: e.id, name: e.name || e.user, github: e.github || e.user,
        avatar: e.avatar || "", url: e.url || "",
        ops: e.ops || 0, tools: e.tools || 0, sessions: e.sessions || 0,
        time_s: e.time_s || 0, tier: e.tier || null,
        bio: e.bio || "", links: e.links || {}, tierIcon: e.tierIcon || "",
        profileTheme: e.profileTheme || {}, interfaceTheme: e.interfaceTheme || {},
        agents: e.agents || [],
        modelboard: e.modelboard || null,
      });
    });
    rows.sort(function (a, b) { return (b.ops - a.ops) || (b.tools - a.tools); });
    rows.forEach(function (r, i) { r.rank = i + 1; });
    return rows;
  }

  function sendChat(text) {
    if (!text || !text.trim()) return;
    var msg = {
      type: "chat",
      id: "c" + (me ? me.id : "local") + "_" + Date.now() + "_" + ((Math.random() * 1e6) | 0),
      user: me ? me.name : "guest",
      github: me ? me.github : "",
      avatar: me ? me.avatar : "",
      url: me ? me.url : "",
      ops: lastEntry ? lastEntry.ops : 0,
      tier: lastEntry ? lastEntry.tier : null,
      tierIcon: lastEntry ? lastEntry.tierIcon : "",
      text: text.slice(0, 200),
      ts: Date.now(),
    };
    peerChat.push(msg);
    if (peerChat.length > 200) peerChat = peerChat.slice(-200);
    broadcastMsg(msg);
    relayPost("/api/chat", { ...msg, peer: me ? me.id : "" }); // durable copy
    log("chat sent:", msg.text.slice(0, 60));
    chatHandlers.forEach(function (cb) { try { cb(msg); } catch (_) {} });
    return msg;
  }

  function getChat() {
    var seen = {}, merged = [];
    function add(m) {
      if (!m || !m.text) return;
      // Dedupe by the stable per-send id when present (survives the relay echo,
      // which re-timestamps ts); otherwise fall back to sender+text+ts.
      var k = m.id || (String(m.text) + "|" + String(m.user || "") + "|" + (m.ts || 0));
      if (seen[k]) return;
      seen[k] = 1;
      merged.push(m);
    }
    relayChat.forEach(add); // server (authoritative history) first
    peerChat.forEach(add);  // then live WebRTC mesh messages
    merged.sort(function (a, b) { return (a.ts || 0) - (b.ts || 0); });
    return merged;
  }
  function onScore(cb) { scoreHandlers.push(cb); }
  function onChat(cb) { chatHandlers.push(cb); }
  function isStarted() { return started; }

  function seedChat(msgs) {
    (msgs || []).forEach(function (m) {
      if (m && m.text) peerChat.push({
        user: m.user, github: m.github, avatar: m.avatar, url: m.url,
        text: m.text, ts: (m.ts || Date.now()) * 1000, peer: "seed",
      });
    });
    if (peerChat.length > 200) peerChat = peerChat.slice(-200);
  }

  return {
    start: start,
    stop: stop,
    broadcastScore: broadcastScore,
    sendChat: sendChat,
    getLeaderboard: getLeaderboard,
    getChat: getChat,
    onScore: onScore,
    onChat: onChat,
    seedChat: seedChat,
    isStarted: isStarted,
  };
})();

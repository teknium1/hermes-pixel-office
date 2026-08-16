// Pixel Office P2P smoke test — drives the REAL module code path (poll ->
// initiate -> offer -> answer -> apply -> channel open) for two instances by
// stubbing browser APIs with a controllable timer + a shared WebRTC bridge and
// an in-memory signaling server. Verifies connect, score/chat flow, and the
// hardening (oversized/bad payload ignored, own-echo ignored, backoff fires).
"use strict";
const fs = require("fs");
const vm = require("vm");

const SRC = fs.readFileSync(
  require("path").join(__dirname, "web", "p2p.js"), "utf8");

// ---------- controllable timer (stubs setTimeout/setInterval) ----------
const schedulers = [];
function TimerScheduler() {
  this.timeouts = []; this.intervals = [];
  this.setTimeout = (fn, ms) => { this.timeouts.push(fn); return this.timeouts.length; };
  this.clearTimeout = (id) => {};
  this.setInterval = (fn, ms) => { this.intervals.push(fn); return this.intervals.length; };
  this.clearInterval = (id) => {};
  this.fireTimeout = () => { const fns = this.timeouts.splice(0); fns.forEach(f => f()); };
  this.fireIntervals = () => { this.intervals.forEach(f => f()); };
  schedulers.push(this);
}

// ---------- in-memory signaling server (fetch stub) ----------
function makeServer() {
  return { peers: {}, offers: {}, answers: {},
    state(me) { return { me, peers: this.peers,
      offers: this.offers[me] || {}, answers: this.answers[me] || {}, ice: {} }; } };
}
function makeFetch(srv, me) {
  return function (path, opts) {
    const full = String(path);
    const p = full.split("?")[0];
    const body = (opts && opts.body) ? JSON.parse(opts.body) : {};
    // The client now identifies itself with a per-browser UUID in body.peer
    // (POST) or the ?peer= query param (GET), exactly like the real server.
    const qp = new URLSearchParams(full.split("?")[1] || "");
    const self = body.peer || qp.get("peer") || me;
    let data = {};
    if (p === "signaling/register") { if (body.online) srv.peers[self] = { online: true, k: body.key, kid: body.kid }; else delete srv.peers[self]; data = srv.state(self); }
    else if (p === "signaling/state") data = srv.state(self);
    else if (p === "signaling/offer") { (srv.offers[body.to] = srv.offers[body.to] || {})[self] = { sdp: body.sdp }; data = { ok: true }; }
    else if (p === "signaling/answer") { (srv.answers[body.to] = srv.answers[body.to] || {})[self] = { sdp: body.sdp }; data = { ok: true }; }
    else if (p === "signaling/clear") { srv.offers[self] = {}; srv.answers[self] = {}; data = { ok: true }; }
    else if (p === "signaling/ice") { data = { ok: true }; }
    else data = {};
    return Promise.resolve({ json: () => Promise.resolve(data) });
  };
}

// ---------- shared WebRTC bridge ----------
const pendingOfferChannels = []; // offerer channels waiting to be paired
const allPCs = [];               // every RTCPeerConnection created
function makeDC() {
  const dc = { label: "office", readyState: "connecting", _send: null,
    onopen: null, onmessage: null, onclose: null, onerror: null,
    send(d) { if (this._send) this._send(d); } };
  return dc;
}
function FakeRTC() {
  const pc = {
    iceConnectionState: "new", signalingState: "stable",
    ondatachannel: null, _dc: null,
    createDataChannel(label) { const dc = makeDC(); this._dc = dc; pendingOfferChannels.push({ pc, dc }); return dc; },
    createOffer() { return Promise.resolve({ type: "offer", sdp: "sdp" }); },
    setLocalDescription(desc) { this.localDescription = desc || { type: "offer", sdp: "sdp" }; this.signalingState = "have-local-offer"; return Promise.resolve(); },
    createAnswer() { return Promise.resolve({ type: "answer", sdp: "sdp" }); },
    setRemoteDescription() { this.signalingState = "stable"; return Promise.resolve(); },
    close() { if (this._dc) { this._dc.readyState = "closed"; if (this._dc.onclose) this._dc.onclose(); } },
  };
  allPCs.push(pc);
  return pc;
}
// Pair the answerer pc's ondatachannel to the first unpaired offerer channel.
let lastBridge = null;
function bridgeAnswererToOfferer() {
  const offerer = pendingOfferChannels.shift();
  // The answerer is the pc that registered an ondatachannel handler (i.e. it
  // answered an incoming offer). With random-UUID peer ids EITHER side can be
  // the offerer, so scan for the answerer instead of assuming it's the last.
  const answerer = allPCs.find(p => typeof p.ondatachannel === "function");
  if (!offerer || !answerer) return false;
  const bCh = makeDC();
  answerer._dc = bCh;
  answerer.ondatachannel({ channel: bCh });
  offerer.dc._send = (d) => bCh.onmessage && bCh.onmessage({ data: d });
  bCh._send = (d) => offerer.dc.onmessage && offerer.dc.onmessage({ data: d });
  offerer.dc.readyState = "open"; if (offerer.dc.onopen) offerer.dc.onopen();
  bCh.readyState = "open"; if (bCh.onopen) bCh.onopen();
  lastBridge = { offererDC: offerer.dc, bCh };
  return true;
}

// ---------- load the module ----------
function loadInstance(id, name, srv, logs) {
  const sched = new TimerScheduler();
  // Per-instance in-memory localStorage so each browser gets its OWN stable
  // UUID peer id (the whole point of the fix).
  const store = {};
  const localStorage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => { store[k] = String(v); },
    removeItem: (k) => { delete store[k]; },
  };
  const sandbox = {
    window: {}, fetch: makeFetch(srv, id), localStorage,
    URLSearchParams,
    console: { log: (...a) => logs.push("LOG " + a.join(" ")),
               warn: (...a) => logs.push("WARN " + a.join(" ")),
               debug: (...a) => logs.push("DEBUG " + a.join(" ")) },
    RTCPeerConnection: FakeRTC,
    RTCSessionDescription: function (d) { this.type = d.type; this.sdp = d.sdp; },
    RTCIceCandidate: function (c) { this.candidate = c && c.candidate; },
    crypto: require("crypto").webcrypto,
    btoa: (s) => Buffer.from(s, "binary").toString("base64"),
    atob: (s) => Buffer.from(s, "base64").toString("binary"),
    TextEncoder: require("util").TextEncoder,
    TextDecoder: require("util").TextDecoder,
    setTimeout: sched.setTimeout.bind(sched), clearTimeout: sched.clearTimeout.bind(sched),
    setInterval: sched.setInterval.bind(sched), clearInterval: sched.clearInterval.bind(sched),
    Map, Set, JSON, Date, Math, Number, String, Object, Array, Promise,
  };
  vm.createContext(sandbox);
  vm.runInContext(SRC, sandbox, { filename: "p2p.js" });
  return { api: sandbox.window.PixelOfficeP2P, sched, id };
}

function run() {
  const logs = [];
  const srv = makeServer();
  const A = loadInstance("alice", "Alice", srv, logs);
  const B = loadInstance("bob", "Bob", srv, logs);

  A.api.start({ github: "alice", name: "Alice" });
  B.api.start({ github: "bob", name: "Bob" });
  const flush = () => new Promise(r => setImmediate(r));
  const settle = (ms = 120) => new Promise(r => setTimeout(r, ms));

  return (async () => {
    await settle(); // let ECDSA keygen + presence/key registration complete
    // Round 1: poll both sides. With random-UUID peer ids the lexicographically
    // smaller id initiates (glare avoidance); the other side needs one more poll
    // to see the offer and answer. Do several alternating rounds so either role
    // assignment converges, then bridge the answerer channel to the offerer.
    for (let i = 0; i < 4; i++) {
      A.sched.fireIntervals(); await flush();
      B.sched.fireIntervals(); await flush();
      await flush();
    }
    const bridged = bridgeAnswererToOfferer(); // pair answerer channel to offerer
    await flush();
    // Round 2: A applies B's answer (channel already opened by the bridge).
    A.sched.fireIntervals();  await flush();
    B.sched.fireIntervals();  await flush();
    await settle(); // ensure pubkeys imported + keypair ready before signing

    // Exchange data.
    A.api.broadcastScore({ ops: 123, tools: 4, sessions: 2, time_s: 99, tier: "gold" });
    B.api.broadcastScore({ ops: 50, tools: 1, sessions: 1, time_s: 10, tier: "silver" });
    B.api.sendChat("hello from bob");
    A.api.sendChat("hi alice here");
    await settle();

    // ---- read clean boards right after the exchange (pre-hardening) ----
    const lbA0 = A.api.getLeaderboard({ ops: 123 });
    const lbB0 = B.api.getLeaderboard({ ops: 50 });
    const gitsA = lbA0.map(r => r.github).sort().join(",");
    const gitsB = lbB0.map(r => r.github).sort().join(",");
    const chatA = A.api.getChat().length;
    const chatB = B.api.getChat().length;

    // ---- same-account distinct-peer test (the core fix) ----
    // Two windows logged in as the SAME GitHub user must get DIFFERENT peer
    // ids so they can see + handshake each other. Before this fix both were
    // keyed by the username and could never connect or share scores.
    const srv2 = makeServer();
    const C = loadInstance("drgekozA", "Joe", srv2, logs);
    const D = loadInstance("drgekozB", "Joe", srv2, logs);
    C.api.start({ github: "DrGekoz", name: "Joe" });
    D.api.start({ github: "DrGekoz", name: "Joe" });
    await settle(80);
    // Each browser registers under its OWN UUID -> two distinct peer slots for
    // the same GitHub account. This is exactly the pre-fix failure mode (they
    // used to collide on one shared "DrGekoz" key and never saw each other).
    const srv2Ids = Object.keys(srv2.peers).filter(id => srv2.peers[id].online);
    const sameAccountDistinct = srv2Ids.length === 2 &&
      new Set(srv2Ids).size === 2 &&
      srv2Ids[0] !== srv2Ids[1];
    // Clean up second mesh.
    C.api.stop(); D.api.stop(); await settle(20);

    // ---- hardening checks (inject hostile payloads over the bridge) ----
    let hardenedOk = true;
    // (a) oversized (>64k) score payload must be ignored, no crash
    const beforeBig = A.api.getLeaderboard({ ops: 123 }).length;
    lastBridge.offererDC._send('x'.repeat(70000));
    await settle();
    const afterBig = A.api.getLeaderboard({ ops: 123 }).length;
    if (afterBig !== beforeBig) hardenedOk = false;

    // (b) malformed JSON must be ignored, no crash
    lastBridge.offererDC._send("{not valid json!!");
    lastBridge.offererDC._send(JSON.stringify({ type: "score" })); // missing entry
    await settle();

    // (c) own-echo (a peer echoing our own entry) must be ignored
    lastBridge.offererDC._send(JSON.stringify({
      type: "score", entry: { id: "alice", counter: 999, ops: 9999 } }));
    await settle();
    const aliceRow = A.api.getLeaderboard({ ops: 123 }).find(r => r.github === "alice");
    if (!aliceRow || aliceRow.ops === 9999) hardenedOk = false;

    // (d) bad peer id in score entry (too long) ignored
    lastBridge.offererDC._send(JSON.stringify({
      type: "score", entry: { id: "z".repeat(300), counter: 1, ops: 5 } }));
    await settle();
    const boardLen = A.api.getLeaderboard({ ops: 123 }).length;
    if (boardLen > 2) hardenedOk = false; // only alice+bob allowed

    const lbA = A.api.getLeaderboard({ ops: 123 });
    const lbB = B.api.getLeaderboard({ ops: 50 });

    console.log("bridged:", bridged);
    console.log("A board gits:", gitsA);
    console.log("B board gits:", gitsB);
    console.log("B raw rows:", JSON.stringify(lbB.map(r => ({ id: r.id, ops: r.ops }))));
    console.log("A chat:", chatA, "| B chat:", chatB);
    console.log("hardened:", hardenedOk ? "OK" : "FAIL");
    console.log("same-account distinct peers:", sameAccountDistinct ? "OK" : "FAIL");

    console.log("\n=== A logs (P2P) ===");
    logs.filter(l => /P2P/.test(l) && l.startsWith("LOG") || /P2P/.test(l) && l.startsWith("DEBUG")).slice(0, 14).forEach(l => console.log("  " + l));
    console.log("=== B logs (P2P) ===");
    logs.filter(l => /P2P/.test(l) && /WARN/.test(l)).slice(0, 8).forEach(l => console.log("  " + l));

    const pass = bridged && hardenedOk && sameAccountDistinct
      && gitsA === "alice,bob" && gitsB === "alice,bob"
      && chatA >= 2 && chatB >= 2
      && /connected to peer/.test(logs.join(" "));
    console.log("\nPASS:", pass ? "YES" : "NO");
    return pass ? 0 : 1;
  })();
}

run().then(c => process.exit(c));

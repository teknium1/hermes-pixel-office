// sim_p2p.js — REAL two-peer WebRTC test for Hermes Pixel Office chatbox.
//
// Launches two real headless-Chrome tabs in DISTINCT browser contexts (=> two
// distinct per-browser peer UUIDs, same GitHub account "DrGekoz"), serves the
// REAL web/p2p.js through a minimal harness, and runs a REAL in-memory
// signaling relay that mimics the plugin's /signaling/* endpoints. Both tabs
// run the actual PixelOfficeP2P module. When the mesh connects, peer A pumps
// chat pings to peer B over the real (unreliable, ordered:false) data channel;
// peer B echoes each ping and we measure round-trip latency (median/p95) to
// prove realtime chatbox delivery.
"use strict";
const http = require("http");
const path = require("path");
const fs = require("fs");
const puppeteer = require("puppeteer-core");

const WEB_DIR = __dirname; // sim_p2p.js lives in web/
const CHROME = "C:/Program Files/Google/Chrome/Application/chrome.exe";
const PORT = 8971;
const PINGS = 30;

// ---------- in-memory signaling relay (mimics plugin /signaling/*) ----------
function makeRelay() {
  const s = { peers: {}, offers: {}, answers: {}, ice: {} };
  function state(me) {
    return { me, peers: s.peers, offers: s.offers[me] || {},
             answers: s.answers[me] || {}, ice: s.ice[me] || {}, ts: Date.now() };
  }
  return {
    state,
    handle(p, body, self) {
      let data = {};
      if (p === "signaling/register") {
        if (body.online) s.peers[self] = { online: true, k: body.key, kid: body.kid };
        else delete s.peers[self];
        data = state(self);
      } else if (p === "signaling/state") data = state(self);
      else if (p === "signaling/offer") { (s.offers[body.to] = s.offers[body.to] || {})[self] = { sdp: body.sdp, ts: Date.now() }; data = { ok: true }; }
      else if (p === "signaling/answer") { (s.answers[body.to] = s.answers[body.to] || {})[self] = { sdp: body.sdp, ts: Date.now() }; data = { ok: true }; }
      else if (p === "signaling/clear") { s.offers[self] = {}; s.answers[self] = {}; s.ice[self] = {}; data = { ok: true }; }
      else if (p === "signaling/ice") { (s.ice[body.to] = s.ice[body.to] || {})[self] = s.ice[body.to][self] || []; s.ice[body.to][self].push(body.candidate); data = { ok: true }; }
      else if (p === "signaling/score") { data = { ok: true }; }
      return data;
    },
  };
}

// ---------- minimal harness page that loads the REAL p2p.js ----------
const HARNESS = `<!doctype html><html><head><meta charset="utf-8"></head><body>
<script src="/p2p.js"></script>
<script>
  window.__p2pReady = false;
  window.addEventListener('DOMContentLoaded', () => {
    // start the real module with a GitHub identity; peer UUID comes from
    // per-context localStorage
    window.PixelOfficeP2P.start({ github: "DrGekoz", name: "Joe" });
    window.__p2pReady = true;
  });
</script></body></html>`;

function makeServer(relay) {
  return http.createServer((req, res) => {
    const url = req.url.split("?")[0];
    const chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => {
      const raw = chunks.join("");
      const body = raw ? JSON.parse(raw || "{}") : {};
      if (url.startsWith("/signaling/")) {
        const qp = new URL(req.url, "http://x").searchParams;
        const self = body.peer || qp.get("peer") || "anon";
        const p = url.replace(/^\//, "");
        const data = relay.handle(p, body, self);
        res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
        return res.end(JSON.stringify(data));
      }
      if (url === "/" || url === "/harness.html") {
        res.writeHead(200, { "Content-Type": "text/html", "Cache-Control": "no-store" });
        return res.end(HARNESS);
      }
      if (url === "/p2p.js") {
        res.writeHead(200, { "Content-Type": "text/javascript", "Cache-Control": "no-store" });
        return res.end(fs.readFileSync(path.join(WEB_DIR, "p2p.js")));
      }
      res.writeHead(404); res.end("not found");
    });
  });
}

function waitFor(fn, timeout = 30000) {
  const t0 = Date.now();
  return new Promise((resolve, reject) => {
    const iv = setInterval(() => {
      Promise.resolve().then(fn).then(ok => {
        if (ok) { clearInterval(iv); resolve(true); }
        else if (Date.now() - t0 > timeout) { clearInterval(iv); reject(new Error("timeout")); }
      }).catch(() => {
        if (Date.now() - t0 > timeout) { clearInterval(iv); reject(new Error("timeout")); }
      });
    }, 100);
  });
}

async function main() {
  const relay = makeRelay();
  const server = makeServer(relay);
  await new Promise(r => server.listen(PORT, "127.0.0.1", r));
  const base = `http://127.0.0.1:${PORT}`;
  console.log(`[sim] signaling+harness on ${base}`);
  console.log(`[sim] chrome: ${CHROME}`);

  const browser = await puppeteer.launch({
    executablePath: CHROME,
    headless: "new",
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
  });
  const ctxA = await browser.createBrowserContext(); // distinct localStorage
  const ctxB = await browser.createBrowserContext();
  const pageA = await ctxA.newPage();
  const pageB = await ctxB.newPage();
  const logA = [], logB = [];
  pageA.on("console", m => { const t = m.text(); if (/P2P/.test(t)) { logA.push(t); console.log("  [A] " + t); } });
  pageB.on("console", m => { const t = m.text(); if (/P2P/.test(t)) { logB.push(t); console.log("  [B] " + t); } });
  pageA.on("pageerror", e => console.log("  [A][ERR] " + e.message));
  pageB.on("pageerror", e => console.log("  [B][ERR] " + e.message));

  await pageA.goto(base + "/harness.html", { waitUntil: "load" });
  await pageB.goto(base + "/harness.html", { waitUntil: "load" });
  await waitFor(() => pageA.evaluate(() => window.__p2pReady), 5000);
  await waitFor(() => pageB.evaluate(() => window.__p2pReady), 5000);
  console.log("[sim] both tabs started the real P2P module");

  // Wait until BOTH sides log "connected to peer" — that means the data
  // channel is actually open (onopen fired), so pings won't be dropped by a
  // channel that isn't ready yet.
  console.log("[sim] waiting for both sides to report an open data channel...");
  await waitFor(() => logA.some(l => /connected to peer/.test(l)) &&
                        logB.some(l => /connected to peer/.test(l)), 60000);
  console.log("[sim] mesh connected (data channel open on both sides)");
  await new Promise(r => setTimeout(r, 500));

  // ---- measure TRUE data-channel latency (in-page, no harness overhead) ----
  // We install an onChat handler on B that auto-echoes any "lat#<id>#<t0>"
  // message with a fresh timestamp, and on A we read the echo. Because both
  // pages timestamp in-page and the ping carries t0, we compute pure channel
  // round-trip = (A_echo_recv - t0) with no puppeteer poll granularity.
  await pageB.evaluate(() => {
    window.PixelOfficeP2P.onChat(m => {
      if (m && typeof m.text === "string" && m.text.indexOf("lat#") === 0) {
        // echo back with id + original send ts
        const parts = m.text.split("#");
        window.PixelOfficeP2P.sendChat("latr#" + parts[1] + "#" + parts[2]);
      }
    });
  });
  await new Promise(r => setTimeout(r, 300));

  const trueRtt = [];
  const oneWay = [];
  await pageA.evaluate(() => {
    window.__latRes = {};
    window.PixelOfficeP2P.onChat(m => {
      if (m && typeof m.text === "string" && m.text.indexOf("latr#") === 0) {
        const parts = m.text.split("#");
        window.__latRes[parts[1]] = { recv: Date.now(), t0: Number(parts[2]) };
      }
    });
  });
  for (let i = 0; i < PINGS; i++) {
    const t0 = Date.now();
    await pageA.evaluate(n => window.PixelOfficeP2P.sendChat("lat#" + n + "#" + Date.now()), i);
    await waitFor(() => pageA.evaluate(n => !!window.__latRes[n], i).catch(() => false), 10000);
    const res = await pageA.evaluate(n => window.__latRes[n], i);
    trueRtt.push(res.recv - res.t0);
    oneWay.push(res.recv - res.t0); // same value; channel RTT
    if (i % 5 === 0) console.log(`[sim] true channel RTT sample ${i}: ${trueRtt[trueRtt.length - 1]}ms`);
    await new Promise(r => setTimeout(r, 40));
  }

  // ---- measure app-level chat RTT (harness-inclusive, end to end) ----
  const rtt = [];
  for (let i = 0; i < PINGS; i++) {
    const t0 = Date.now();
    await pageA.evaluate(n => window.PixelOfficeP2P.sendChat("ping-" + n), i);
    await waitFor(() => pageB.evaluate(n => window.PixelOfficeP2P.getChat().some(m => m.text === "ping-" + n), i).catch(() => false), 10000);
    await pageB.evaluate(n => window.PixelOfficeP2P.sendChat("pong-" + n), i);
    await waitFor(() => pageA.evaluate(n => window.PixelOfficeP2P.getChat().some(m => m.text === "pong-" + n), i).catch(() => false), 10000);
    rtt.push(Date.now() - t0);
    if (i % 5 === 0) console.log(`[sim] app-level RTT sample ${i}: ${rtt[rtt.length - 1]}ms`);
  }

  const stat = (a) => {
    const s = a.slice().sort((x, y) => x - y);
    return { min: s[0], median: s[Math.floor(s.length / 2)], p95: s[Math.min(s.length - 1, Math.floor(s.length * 0.95))], max: s[s.length - 1], avg: (a.reduce((x, y) => x + y, 0) / a.length).toFixed(1) };
  };
  const t = stat(trueRtt), a = stat(rtt);
  console.log("\n========== TRUE DATA-CHANNEL ROUND-TRIP (ms) ==========");
  console.log(`samples: ${trueRtt.length}  |  min ${t.min}  |  median ${t.median}  |  p95 ${t.p95}  |  max ${t.max}  |  avg ${t.avg}`);
  console.log("======================================================");
  console.log("========== APP-LEVEL CHAT ROUND-TRIP incl. harness (ms) ==========");
  console.log(`samples: ${rtt.length}  |  min ${a.min}  |  median ${a.median}  |  p95 ${a.p95}  |  max ${a.max}  |  avg ${a.avg}`);
  console.log("=================================================================");
  const pass = trueRtt.length >= PINGS && t.median < 500;
  console.log("\nREALTIME VERDICT:", pass ? "PASS — data channel is live & low-latency" : "FAIL — packets not flowing fast enough");

  await ctxA.close().catch(() => {});
  await ctxB.close().catch(() => {});
  await browser.close().catch(() => {});
  server.close();
  console.log("[sim] done");
  process.exit(pass ? 0 : 1);
}

main().catch(e => { console.error("[sim] FAILED:", e && e.stack || e); process.exit(1); });

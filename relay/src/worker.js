// ============================================================
// Hermes Pixel Office — Cloudflare Workers + Durable Object relay
// The permanent, FREE, non-stop, everyone-write backbone.
//
// WHY: a public GitHub gist is READ-by-anyone but WRITE-by-owner-only,
// so it can never be the multi-player middleman (that was the silent
// bug). This Worker + Durable Object is a real shared store that every
// player can write to over plain HTTPS — so it works under ANY NAT /
// CGNAT, and it keyed the leaderboard by GitHub username so duplicate
// rows for the same account can't exist.
//
// WebRTC mesh (web/p2p.js) stays for low-latency realtime; this is the
// authoritative fallback + persistence + the 10-min gist backup source.
//
// Deploy (once, free):
//   cd relay
//   npx wrangler login        # browser OAuth, one click
//   npx wrangler deploy
// The printed *.workers.dev URL is the RELAY URL.
// ============================================================

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Max-Age": "86400",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight (office is often served from http://localhost:8113).
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (url.pathname === "/") {
      return json({ ok: true, service: "pixel-office-relay", time: Date.now() });
    }

    if (url.pathname.startsWith("/api/")) {
      const id = env.OFFICE.idFromName("global");
      const stub = env.OFFICE.get(id);
      return stub.fetch(request);
    }

    return json({ error: "not found" }, 404);
  },
};

// Durable Object: single, strongly-consistent instance holds all office state.
// Durable Object storage is durable (survives restarts), so scores + chat are
// preserved forever even with zero players online.
export class OfficeDO {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _boot() {
    if (await this.state.storage.get("boot")) return;
    await this.state.storage.put("boot", 1);
    await this.state.storage.put("scores", {});
    await this.state.storage.put("chat", []);
    await this.state.storage.put("presence", {});
    // Host-appointed 10-min durable backup: schedule a recurring alarm so the
    // WHOLE merged board is written to the gist (one call) forever, even with
    // zero players online. No-op if GITHUB_TOKEN/GITHUB_GIST_ID secrets unset.
    const hasAlarm = await this.state.storage.getAlarm();
    if (!hasAlarm) await this.state.storage.setAlarm(Date.now() + 600000);
  }

  async alarm() {
    // Every 10 minutes: persist the whole board to the gist (one PATCH call).
    await this._boot();
    await this.gistSnapshot({});
    await this.state.storage.setAlarm(Date.now() + 600000);
  }

  async fetch(request) {
    await this._boot();
    const url = new URL(request.url);
    let body = {};
    if (request.method === "POST") {
      try { body = await request.json(); } catch (_) {}
    }

    switch (url.pathname) {
      case "/api/score":
        return this.upsertScore(body.entry || body);
      case "/api/board":
        return this.getBoard();
      case "/api/chat":
        return this.addChat(body);
      case "/api/chat/list":
        return this.getChat();
      case "/api/presence":
        return this.presence(body);
      case "/api/clear":
        return this.clearAll(body);
      case "/api/gist":
        // Host-appointed: one owner call per 10 min persists the WHOLE merged
        // board as a durable offline mirror (see /api/gist-url config).
        return this.gistSnapshot(body);
      default:
        return json({ error: "not found" }, 404);
    }
  }

  // Upsert one player's score. KEYED BY GITHUB USERNAME -> duplicates for the
  // same account collapse automatically (the exact "3 of me" bug dies here).
  async upsertScore(entry) {
    if (!entry || !entry.github) return json({ error: "missing github" }, 400);
    const scores = (await this.state.storage.get("scores")) || {};
    const key = String(entry.github).toLowerCase();
    const prev = scores[key] || {};
    // Always accept (fresh heartbeat). Ops is the score, so it can never go
    // backwards; merge the newest profile/fields on top while keeping max ops.
    const merged = {
      ...prev,
      ...entry,
      github: entry.github,
      ops: Math.max(Number(prev.ops || 0), Number(entry.ops || 0)),
    };
    scores[key] = merged;
    await this.state.storage.put("scores", scores);
    return this.getBoard();
  }

  async getBoard() {
    const scores = (await this.state.storage.get("scores")) || {};
    const rows = Object.values(scores)
      .filter((r) => r && r.github)
      .map((r) => ({
        id: r.id || "", github: r.github, name: r.name || r.github,
        avatar: r.avatar || "", url: r.url || "",
        ops: r.ops || 0, tools: r.tools || 0, sessions: r.sessions || 0,
        time_s: r.time_s || 0, tier: r.tier || null,
        bio: r.bio || "", links: r.links || {}, tierIcon: r.tierIcon || "",
        profileTheme: r.profileTheme || {}, interfaceTheme: r.interfaceTheme || {},
        agents: r.agents || [], modelboard: r.modelboard || null,
        counter: r.counter || 0, ts: r.ts || 0,
      }));
    rows.sort((a, b) => (b.ops - a.ops) || (b.tools - a.tools));
    rows.forEach((r, i) => (r.rank = i + 1));
    return json({ ok: true, board: rows, updated: Date.now() });
  }

  async addChat(msg) {
    if (!msg || !msg.text) return json({ error: "missing text" }, 400);
    const chat = (await this.state.storage.get("chat")) || [];
    chat.push({ ...msg, ts: msg.ts || Date.now(), peer: msg.peer || "" });
    if (chat.length > 300) chat.splice(0, chat.length - 300);
    await this.state.storage.put("chat", chat);
    return json({ ok: true });
  }

  async getChat() {
    const chat = (await this.state.storage.get("chat")) || [];
    return json({ ok: true, chat });
  }

  async presence(body) {
    if (!body || !body.peer) return json({ ok: false }, 400);
    const presence = (await this.state.storage.get("presence")) || {};
    presence[String(body.peer)] = {
      github: body.github || "", name: body.name || "",
      last_seen: Date.now(), online: body.online !== false,
    };
    await this.state.storage.put("presence", presence);
    return json({ ok: true, presence });
  }

  // Admin: wipe scores + chat + presence. FAIL CLOSED: a wipe is only allowed
  // when a valid CLEAR_TOKEN is supplied. Missing secret -> 503, bad/missing
  // token -> 403, correct token -> clears.
  async clearAll(body) {
    const tok = this.env.CLEAR_TOKEN;
    if (!tok) return json({ error: "service_unavailable", reason: "CLEAR_TOKEN not configured" }, 503);
    const supplied = (body && body.token) || "";
    if (supplied !== tok) return json({ error: "forbidden" }, 403);
    await this.state.storage.put("scores", {});
    await this.state.storage.put("chat", []);
    await this.state.storage.put("presence", {});
    return json({ ok: true });
  }

  // Host-appointed durable backup: writes the whole merged board to a gist
  // every 10 min (one call). Requires GITHUB_TOKEN + GITHUB_GIST_ID secrets
  // to be set. Optional — the DO storage is already durable.
  async gistSnapshot(body) {
    const token = this.env.GITHUB_TOKEN;
    const gid = this.env.GITHUB_GIST_ID;
    if (!token || !gid) return json({ ok: false, reason: "no gist secrets configured" });

    const boardRes = await this.getBoard();
    const boardPayload = await boardRes.json();

    const file = {
      "signaling.json": {
        content: JSON.stringify({
          updated: Date.now(),
          mode: "relay-gist-backup",
          board: boardPayload.board || [],
        }),
      },
    };

    try {
      const r = await fetch(`https://api.github.com/gists/${gid}`, {
        method: "PATCH",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Accept": "application/vnd.github+json",
          "User-Agent": "pixel-office-relay",
        },
        body: JSON.stringify({ files: file }),
      });
      if (!r.ok) return json({ ok: false, status: r.status }, 502);
      return json({ ok: true, wrote: (boardPayload.board || []).length, gist: gid });
    } catch (e) {
      return json({ ok: false, error: String(e && e.message) }, 502);
    }
  }
}

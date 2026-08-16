# Hermes Pixel Office — Free Permanent Relay

Cloudflare Workers + Durable Object. This is the everyone-write backbone that
replaces the owner-write-only gist as the multi-player middleman. It:

- stores the leaderboard, chat, profiles and modelboard durably (free forever)
- lets EVERY player write over plain HTTPS -> works under any NAT / CGNAT
- keys the leaderboard by GitHub username -> no more "3 of me" duplicates
- keeps the WebRTC mesh for low-latency realtime when it can connect
- gives a single durable gist backup call every 10 min (host-appointed alarm)

## Zero config for every player

The relay URL is a built-in shared default (like the well-known gist), so
anyone who installs this repo joins the SAME shared leaderboard + chat with no
config edits. To override, set the plugin config key `relay_url` or the env
`PIXEL_OFFICE_RELAY`.

## Deploy (once, by the relay owner — free)

    cd relay
    npm install
    npx wrangler login          # one-click browser OAuth
    npx wrangler deploy

## Secrets (owner-only, optional)

    npx wrangler secret put GITHUB_TOKEN        # PAT with gist scope (owner of the backup gist)
    npx wrangler secret put GITHUB_GIST_ID      # id of the backup gist
    npx wrangler secret put CLEAR_TOKEN         # guards the admin /api/clear (saved to relay_clear_token.txt)

- The Durable Object writes the WHOLE merged board to the gist every 10 minutes
  via a server-side alarm (one call per interval) — runs even with zero players.
- NOTE: GitHub rate-limits gist API writes (~per-user quota). The office's old
  4s signaling-gist polling was the main consumer; the relay removes that load.
  If the gist PATCH returns 403 "rate limit exceeded", it's temporary — the
  Durable Object storage is already the durable store, so leaderboards persist
  regardless.
- `/api/clear` wipes scores+chat and requires the CLEAR_TOKEN secret to be set.

## Local smoke test

    node relay/test_relay.js

Runs the Durable Object logic against an in-memory storage mock and asserts:
score upsert + per-username dedup + board ranking + chat append + gist guard.

## API (all CORS-open, so any office origin can call them)

    POST /api/score   { entry: { github, ops, tools, ... } }   upsert (deduped by github)
    GET  /api/board                                           merged, sorted, ranked
    POST /api/chat    { text, user, github, ... }              append message
    GET  /api/chat/list                                        recent chat
    POST /api/clear   { token }                                wipe (needs CLEAR_TOKEN)
    POST /api/gist    { }                                      manual 10-min backup trigger

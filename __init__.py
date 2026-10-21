"""pixel-office plugin — a pixel-art virtual office for Hermes agents.

Every Hermes agent (main sessions AND delegate_task subagents) shows up as
an animated pixel character sitting at a desk in a tiny office, rendered in
your browser at http://127.0.0.1:8113 (port configurable).

Design:

* Hooks are pure observers — they never block, veto, or transform anything.
  Each hook appends one JSON line to ``~/.hermes/pixel-office/events.jsonl``.
  Appends are O(1) and wrapped in try/except, so the agent loop never pays
  more than a few microseconds.

* A daemon HTTP server thread is started lazily on the first event. It
  serves the office page and ``/state``, which folds the event log into a
  current-agents snapshot. Because state is derived from the shared event
  file (not process memory), agents from OTHER Hermes processes (gateway +
  CLI at once, cron sessions) appear in the same office. If the port is
  already bound, another Hermes process is serving — we just keep appending
  events and skip serving.

* The event log is trimmed when it exceeds ~512 KB (keeps the newest half),
  so it never grows unbounded.

* Kanban: an agent running a Hermes Kanban task carries ``kanban``
  ({id, title, status}) in ``/state``, and ``GET /kanban`` serves the wall
  board (open, blocked and recently done tasks of ``~/.hermes/kanban.db``,
  read-only, re-read at most every 3 s).

Configuration (all optional, config.yaml):

    plugins:
      entries:
        pixel-office:
          port: 8113        # HTTP port for the office page
          enabled: true

Nothing here touches the conversation, the prompt cache, or tool results.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8113
_MAX_LOG_BYTES = 512 * 1024
# An agent with no events for this long is swept from the office.
_STALE_SECONDS = 30 * 60

_lock = threading.Lock()
_server_started = False
_port: int = DEFAULT_PORT
# Approval hooks don't carry session_id (only a gateway session_key), so we
# attribute them to the most recent session that fired a tool in this
# process — approvals always happen inside a tool dispatch.
_last_session_id: str = ""


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

def _office_dir() -> Path:
    d = get_hermes_home() / "pixel-office"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _events_path() -> Path:
    return _office_dir() / "events.jsonl"


# ---------------------------------------------------------------------------
# Event publishing (hook side — must be cheap and never raise)
# ---------------------------------------------------------------------------

def _publish(event: Dict[str, Any]) -> None:
    try:
        event.setdefault("ts", time.time())
        event.setdefault("pid", os.getpid())
        line = json.dumps(event, ensure_ascii=False, default=str)
        path = _events_path()
        with _lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
            _maybe_trim(path)
        _ensure_server()
    except Exception as exc:  # observers must never break the loop — but say so
        logger.warning("pixel-office: failed to record event (%s: %s)",
                       type(exc).__name__, exc)


def _maybe_trim(path: Path) -> None:
    try:
        if path.stat().st_size <= _MAX_LOG_BYTES:
            return
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        keep = lines[len(lines) // 2:]
        tmp = path.with_suffix(".jsonl.tmp")
        tmp.write_text("\n".join(keep) + "\n", encoding="utf-8")
        tmp.replace(path)
    except Exception:
        logger.debug("pixel-office trim failed", exc_info=True)


# ---------------------------------------------------------------------------
# State folding (server side)
# ---------------------------------------------------------------------------

def _read_events() -> List[Dict[str, Any]]:
    path = _events_path()
    if not path.exists():
        return []
    out: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        logger.debug("pixel-office read failed", exc_info=True)
    return out


def _agent_key(ev: Dict[str, Any]) -> Optional[str]:
    sid = ev.get("session_id") or ev.get("child_session_id")
    if sid:
        return str(sid)
    # Fall back to pid so events without a session id still get a character.
    pid = ev.get("pid")
    return f"pid-{pid}" if pid else None


def _short(text: Any, n: int = 60) -> str:
    s = str(text or "").strip().replace("\n", " ")
    return s[: n - 1] + "…" if len(s) > n else s


# ---------------------------------------------------------------------------
# Kanban (read-only): which session works on which task, and the wall board
# ---------------------------------------------------------------------------

_KANBAN_TTL = 3.0
_KANBAN_DONE_SHOWN = 24 * 3600          # done tasks stay on the board for a day
_KANBAN_MAX = 14
# board column per task status; "todo" gathers everything not yet picked up
_KANBAN_COLUMN = {"running": "running", "blocked": "blocked", "review": "review",
                  "ready": "todo", "todo": "todo", "triage": "todo", "scheduled": "todo",
                  "done": "done"}
_KANBAN_ORDER = {"running": 0, "blocked": 1, "review": 2, "todo": 3, "done": 4}
_kanban_cache: Tuple[float, Dict[str, Any]] = (0.0, {"sessions": {}, "tasks": []})


def _kanban_db() -> Path:
    """The default board lives in the main Hermes home, also when this process runs a profile."""
    home = Path(get_hermes_home())
    root = home.parent.parent if home.parent.name == "profiles" else home
    return root / "kanban.db"


def _kanban_snapshot() -> Dict[str, Any]:
    """{"sessions": {session_id: task}, "tasks": [...]} for the default board, re-read at most
    every 3 s. Read-only; a missing or busy database just shows nothing."""
    global _kanban_cache
    now = time.time()
    if now - _kanban_cache[0] < _KANBAN_TTL:
        return _kanban_cache[1]
    snap: Dict[str, Any] = {"sessions": {}, "tasks": [], "total": 0}
    path = _kanban_db()
    if path.exists():
        try:
            import sqlite3

            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
            try:
                tasks: Dict[str, Dict[str, Any]] = {}
                for tid, title, assignee, status, created, started, completed, sid in con.execute(
                        "SELECT id, title, assignee, status, created_at, started_at, completed_at, session_id "
                        "FROM tasks WHERE status != 'archived'"):
                    column = _KANBAN_COLUMN.get(str(status or ""))
                    if not column:
                        continue
                    t = {"id": tid, "title": _short(title, 48), "status": column,
                         "assignee": str(assignee or ""),
                         "updated": float(completed or started or created or 0)}
                    tasks[tid] = t
                    if sid:
                        snap["sessions"][str(sid)] = t
                # every attempt's session, retries included (tasks.session_id holds one at most)
                for tid, payload in con.execute(
                        "SELECT task_id, payload FROM task_events WHERE kind = 'owner_admitted' "
                        "AND created_at > ?", (now - _KANBAN_DONE_SHOWN,)):
                    try:
                        sid = json.loads(payload or "{}").get("session_id")
                    except ValueError:
                        continue
                    if sid and tid in tasks:
                        snap["sessions"][str(sid)] = tasks[tid]
            finally:
                con.close()
            board = [t for t in tasks.values()
                     if t["status"] != "done" or now - t["updated"] < _KANBAN_DONE_SHOWN]
            board.sort(key=lambda t: (_KANBAN_ORDER[t["status"]], -t["updated"]))
            snap["tasks"] = board[:_KANBAN_MAX]
            snap["total"] = len(board)
        except Exception:
            logger.debug("pixel-office: kanban read failed", exc_info=True)
    _kanban_cache = (now, snap)
    return snap


def build_state() -> Dict[str, Any]:
    """Fold the event log into {agents: [...]} for the frontend."""
    agents: Dict[str, Dict[str, Any]] = {}
    now = time.time()

    def ensure(key: str, ev: Dict[str, Any]) -> Dict[str, Any]:
        a = agents.get(key)
        if a is None:
            a = {
                "id": key,
                "label": f"agent {key[-6:]}",
                "kind": "main",
                "status": "idle",
                "tool": "",
                "activity": "",
                "detail": "",
                "platform": ev.get("platform") or "",
                "first_seen": ev.get("ts", now),
                "updated_at": ev.get("ts", now),
            }
            agents[key] = a
        a["updated_at"] = ev.get("ts", a["updated_at"])
        return a

    for ev in _read_events():
        kind = ev.get("event")
        key = _agent_key(ev)
        if not key:
            continue

        if kind == "session_start":
            a = ensure(key, ev)
            a["status"] = "idle"
            plat = ev.get("platform") or ""
            a["label"] = f"{plat or 'hermes'} {key[-6:]}"
            a["detail"] = "session started"
        elif kind == "session_end":
            if key in agents:
                agents[key]["status"] = "gone"
                agents[key]["updated_at"] = ev.get("ts", now)
        elif kind == "subagent_start":
            child = ev.get("child_session_id")
            if child:
                ck = str(child)
                ev2 = dict(ev)
                ev2["session_id"] = ck
                a = ensure(ck, ev2)
                a["kind"] = "subagent"
                a["label"] = _short(ev.get("child_goal"), 26) or f"sub {ck[-6:]}"
                a["status"] = "working"
                a["detail"] = _short(ev.get("child_goal"))
                a["parent"] = str(ev.get("parent_session_id") or "")
        elif kind == "subagent_stop":
            child = ev.get("child_session_id")
            if child and str(child) in agents:
                agents[str(child)]["status"] = "done"
                agents[str(child)]["updated_at"] = ev.get("ts", now)
        elif kind == "tool_start":
            a = ensure(key, ev)
            a["status"] = "working"
            a["tool"] = str(ev.get("tool_name") or "")
            a["activity"] = str(ev.get("activity") or "working")
            a["detail"] = _short(ev.get("preview"))
        elif kind == "tool_end":
            a = ensure(key, ev)
            a["status"] = "thinking"
            a["tool"] = ""
            if ev.get("status") == "error":
                a["detail"] = f"⚠ {_short(ev.get('error_message'), 40)}"
            else:
                a["detail"] = ""
        elif kind == "approval_request":
            a = ensure(key, ev)
            a["status"] = "waiting"
            a["tool"] = ""
            a["detail"] = _short(ev.get("command"), 40) or "needs approval"
        elif kind == "approval_response":
            a = ensure(key, ev)
            choice = str(ev.get("choice") or "")
            if choice in ("deny", "timeout"):
                a["status"] = "thinking"
                a["detail"] = f"approval: {choice}"
            else:
                a["status"] = "working"
                a["detail"] = ""

    # Sweep stale + long-gone agents.
    visible = []
    for a in agents.values():
        age = now - float(a.get("updated_at") or 0)
        if a["status"] == "gone" and age > 20:
            continue
        if a["status"] == "done" and age > 120:
            continue
        if age > _STALE_SECONDS:
            continue
        # Agents quiet for a bit are "idle", not eternally "thinking".
        if a["status"] in ("working", "thinking") and age > 300:
            a["status"] = "idle"
        visible.append(a)

    # Kanban workers: the task they are on (a subagent shows its parent's task).
    on_task = _kanban_snapshot()["sessions"]
    for a in visible:
        t = on_task.get(a["id"]) or (on_task.get(a.get("parent") or "") if a["kind"] == "subagent" else None)
        if t:
            a["kanban"] = {"id": t["id"], "title": t["title"], "status": t["status"]}

    visible.sort(key=lambda a: (a["kind"] != "main", a.get("first_seen", 0)))
    return {"agents": visible, "ts": now}


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def _resolve_port() -> int:
    try:
        from hermes_cli.config import cfg_get, load_config

        p = cfg_get(load_config(), "plugins", "entries", "pixel-office", "port")
        if p:
            return int(p)
    except Exception:
        pass
    return DEFAULT_PORT


def _ensure_server() -> None:
    global _server_started
    if _server_started:
        return
    with _lock:
        if _server_started:
            return
        _server_started = True
    t = threading.Thread(target=_serve, name="pixel-office-http", daemon=True)
    t.start()


def _serve() -> None:
    global _port
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    _port = _resolve_port()
    html_path = Path(__file__).resolve().parent / "web" / "index.html"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # silence stdout
            pass

        def do_GET(self) -> None:
            try:
                if self.path.split("?")[0] in ("/", "/index.html"):
                    body = html_path.read_bytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                elif self.path.split("?")[0] == "/state":
                    body = json.dumps(build_state()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Cache-Control", "no-store")
                elif self.path.split("?")[0] == "/kanban":
                    k = _kanban_snapshot()
                    body = json.dumps({"tasks": k["tasks"], "total": k.get("total", 0),
                                       "ts": time.time()}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Cache-Control", "no-store")
                else:
                    self.send_response(404)
                    body = b"not found"
                    self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                logger.debug("pixel-office request failed", exc_info=True)

    try:
        srv = ThreadingHTTPServer(("127.0.0.1", _port), Handler)
    except OSError as exc:
        # Port already bound. Probe it: a healthy pixel-office answers /state
        # with JSON containing "agents". Anything else is a foreign squatter
        # (another app, or a ghost VS Code port-forward) — say so LOUDLY,
        # because to the user it looks like "office unreachable".
        verdict = _probe_port(_port)
        if verdict == "office":
            logger.info(
                "pixel-office: port %s already serving a healthy office "
                "(another Hermes process) — this process will feed events only",
                _port,
            )
        else:
            logger.warning(
                "pixel-office: could NOT bind 127.0.0.1:%s (%s) and the "
                "current listener does not answer like a pixel-office "
                "(probe: %s). Another app or a stale VS Code port-forward is "
                "squatting the port. Fix: free the port, or set "
                "plugins.entries.pixel-office.port in config.yaml and update "
                "the extension's hermesPixelOffice.stateUrl to match.",
                _port, exc, verdict,
            )
        return
    logger.info("pixel-office serving at http://127.0.0.1:%s", _port)
    try:
        srv.serve_forever()
    except Exception:
        logger.debug("pixel-office server exited", exc_info=True)


def _probe_port(port: int) -> str:
    """Classify whatever is listening on *port*: 'office', 'foreign', or 'dead'."""
    try:
        import urllib.request

        req = urllib.request.Request(f"http://127.0.0.1:{port}/state")
        with urllib.request.urlopen(req, timeout=2) as resp:
            body = resp.read(4096).decode("utf-8", errors="replace")
        return "office" if '"agents"' in body else "foreign"
    except Exception as exc:
        return f"dead/{type(exc).__name__}"


# ---------------------------------------------------------------------------
# Hook callbacks — all **kwargs so core payload changes never break us
# ---------------------------------------------------------------------------

# Tool name → activity shown in the office (drives the character animation).
_ACTIVITY = {
    "write_file": "typing", "patch": "typing", "skill_manage": "typing",
    "read_file": "reading", "search_files": "reading", "skill_view": "reading",
    "web_search": "browsing", "web_extract": "browsing",
    "browser_navigate": "browsing", "browser_click": "browsing",
    "browser_snapshot": "browsing", "browser_vision": "browsing",
    "terminal": "running", "execute_code": "running", "process": "running",
    "delegate_task": "delegating",
}


def _activity_for(tool: str) -> str:
    return _ACTIVITY.get(str(tool or ""), "working")


def _on_session_start(**kw: Any) -> None:
    _publish({
        "event": "session_start",
        "session_id": kw.get("session_id"),
        "platform": kw.get("platform"),
    })


def _on_session_end(**kw: Any) -> None:
    _publish({"event": "session_end", "session_id": kw.get("session_id")})


def _pre_tool_call(**kw: Any) -> None:
    global _last_session_id
    sid = kw.get("session_id") or ""
    if sid:
        _last_session_id = str(sid)
    args = kw.get("args") or {}
    preview = ""
    if isinstance(args, dict):
        for k in ("command", "path", "query", "url", "goal", "pattern", "prompt"):
            if args.get(k):
                preview = str(args[k])
                break
    tool = kw.get("tool_name")
    _publish({
        "event": "tool_start",
        "session_id": sid,
        "tool_name": tool,
        "activity": _activity_for(tool),
        "preview": _short(preview),
    })
    return None  # observer — never blocks


def _post_tool_call(**kw: Any) -> None:
    _publish({
        "event": "tool_end",
        "session_id": kw.get("session_id"),
        "tool_name": kw.get("tool_name"),
        "status": kw.get("status") or "ok",
        "error_message": kw.get("error_message"),
        "duration_ms": kw.get("duration_ms"),
    })


def _subagent_start(**kw: Any) -> None:
    _publish({
        "event": "subagent_start",
        "parent_session_id": kw.get("parent_session_id"),
        "child_session_id": kw.get("child_session_id"),
        "child_role": kw.get("child_role"),
        "child_goal": kw.get("child_goal"),
    })


def _subagent_stop(**kw: Any) -> None:
    _publish({
        "event": "subagent_stop",
        "child_session_id": kw.get("child_session_id"),
    })


def _pre_approval_request(**kw: Any) -> None:
    _publish({
        "event": "approval_request",
        "session_id": _last_session_id,
        "command": kw.get("command") or kw.get("description"),
        "surface": kw.get("surface"),
    })


def _post_approval_response(**kw: Any) -> None:
    _publish({
        "event": "approval_response",
        "session_id": _last_session_id,
        "choice": kw.get("choice"),
    })


def register(ctx: Any) -> None:
    ctx.register_hook("on_session_start", _on_session_start)
    ctx.register_hook("on_session_end", _on_session_end)
    ctx.register_hook("pre_tool_call", _pre_tool_call)
    ctx.register_hook("post_tool_call", _post_tool_call)
    ctx.register_hook("subagent_start", _subagent_start)
    ctx.register_hook("subagent_stop", _subagent_stop)
    ctx.register_hook("pre_approval_request", _pre_approval_request)
    ctx.register_hook("post_approval_response", _post_approval_response)
    logger.info(
        "pixel-office registered — office at http://127.0.0.1:%s once events flow",
        _resolve_port(),
    )

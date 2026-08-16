"""Self-test feed for the pixel-office plugin — no Hermes install required.

Loads the plugin module from this directory and fires its real hook
callbacks with synthetic sessions/subagents/approvals, serving the office
on the default port. Run, then open http://127.0.0.1:8113.

    HERMES_HOME=$(mktemp -d)/.hermes python3 demo_feed.py

Ctrl+C to stop.
"""
import importlib.util
import os
import random
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent

# The plugin only needs get_hermes_home(); stub it if hermes isn't importable
# so this demo runs on a machine with nothing but Python.
try:
    import hermes_constants  # noqa: F401
except ImportError:
    import types
    hh = Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
    stub = types.ModuleType("hermes_constants")
    stub.get_hermes_home = lambda: hh
    sys.modules["hermes_constants"] = stub

spec = importlib.util.spec_from_file_location("pixel_office", HERE / "__init__.py")
mod = importlib.util.module_from_spec(spec)
sys.modules["pixel_office"] = mod
spec.loader.exec_module(mod)

TOOLS = [
    ("terminal", {"command": "pytest tests/gateway -q"}),
    ("read_file", {"path": "run_agent.py"}),
    ("web_search", {"query": "pixel agents vs code"}),
    ("write_file", {"path": "model_tools.py"}),
    ("search_files", {"pattern": "invoke_hook"}),
]
GOALS = ["Review PR #81212", "Research competitor docs", "Fix flaky gateway test",
         "Write release notes", "Audit dep pins"]

mod._on_session_start(session_id="sess-main-cli", platform="cli")
mod._on_session_start(session_id="sess-tg-main", platform="telegram")
# Connect a GitHub identity + post a chat line so the leaderboard / chat / tiers
# panels have content to show.
try:
    mod.set_identity(os.environ.get("PIXEL_OFFICE_GH", "DrGekoz"))
except Exception:
    pass
try:
    mod.post_chat("demo feed online — agents are working the floor")
except Exception:
    pass
print("pixel-office demo feed running — open http://127.0.0.1:%d" % mod._resolve_port())

live_subs, n = [], 0
while True:
    n += 1
    for sid in ("sess-main-cli", "sess-tg-main"):
        tool, args = random.choice(TOOLS)
        mod._pre_tool_call(tool_name=tool, args=args, session_id=sid)
        time.sleep(random.uniform(2, 5))
        mod._post_tool_call(tool_name=tool, args={}, result="{}", session_id=sid,
                            status="ok", duration_ms=random.randint(200, 4000))
    if len(live_subs) < 3 and random.random() < 0.6:
        sub = f"sub-{n}"
        mod._subagent_start(parent_session_id="sess-main-cli", child_session_id=sub,
                            child_role="leaf", child_goal=random.choice(GOALS))
        tool, args = random.choice(TOOLS)
        mod._pre_tool_call(tool_name=tool, args=args, session_id=sub)
        live_subs.append(sub)
    if live_subs and random.random() < 0.4:
        done = live_subs.pop(0)
        mod._post_tool_call(tool_name="terminal", args={}, result="{}",
                            session_id=done, status="ok")
        mod._subagent_stop(child_session_id=done)
    if n % 4 == 2:
        mod._pre_tool_call(tool_name="terminal",
                           args={"command": "sudo systemctl restart nginx"},
                           session_id="sess-main-cli")
        mod._pre_approval_request(command="sudo systemctl restart nginx",
                                  description="restart service", surface="cli")
        time.sleep(8)
        mod._post_approval_response(command="sudo systemctl restart nginx",
                                    choice="once", surface="cli")
    time.sleep(random.uniform(2, 4))

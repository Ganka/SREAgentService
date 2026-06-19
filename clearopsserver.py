"""
clearops_server.py — ClearOps Agent with Web UI (FastAPI + WebSocket)
========================================================================
Same pipeline as clearops_agent.py, but instead of CLI prompts, it streams
every step to a live web dashboard over WebSocket, and waits for approval
clicks from the browser instead of terminal input().


Setup
-----
pip install fastapi uvicorn openai aiohttp python-dotenv websockets


Run
---
python clearops_server.py
# Open http://localhost:3001 in your browser — that's the dashboard.
# React app still POSTs errors to http://localhost:3001/error as before.
"""


import asyncio
import base64
import difflib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional


import aiohttp
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from openai import AsyncOpenAI


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("clearops")


# ─── Config ───────────────────────────────────────────────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO    = os.getenv("GITHUB_REPO", "")
JIRA_BASE      = os.getenv("JIRA_BASE_URL", "").rstrip("/")
JIRA_EMAIL     = os.getenv("JIRA_EMAIL", "")
JIRA_TOKEN     = os.getenv("JIRA_API_TOKEN", "")
JIRA_PROJECT   = os.getenv("JIRA_PROJECT", "OPS")
SLACK_WEBHOOK  = os.getenv("SLACK_WEBHOOK_URL", "")
MODEL          = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
AGENT_PORT     = int(os.getenv("AGENT_PORT", "3001"))
VERCEL_URL     = os.getenv("VERCEL_URL", "")


client = AsyncOpenAI(api_key=OPENAI_API_KEY)
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── WebSocket connection registry + pending approvals ──────────────────────
ACTIVE_SOCKETS: list[WebSocket] = []
PENDING_APPROVALS: dict[str, asyncio.Future] = {}
INCIDENT_LOG: list[dict] = []   # in-memory feed history for late-joining clients




async def broadcast(event: dict):
    """Send an event to every connected browser and store it for replay."""
    event["ts"] = datetime.now(timezone.utc).isoformat()
    INCIDENT_LOG.append(event)
    dead = []
    for ws in ACTIVE_SOCKETS:
        try:
            await ws.send_json(event)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ACTIVE_SOCKETS.remove(ws)




async def wait_for_approval(approval_id: str) -> dict:
    """Block until the browser sends back a decision for this approval_id."""
    fut = asyncio.get_event_loop().create_future()
    PENDING_APPROVALS[approval_id] = fut
    result = await fut
    PENDING_APPROVALS.pop(approval_id, None)
    return result




# ═════════════════════════════════════════════════════════════════════════
# Jira / Slack / GitHub helpers (same logic as clearops_agent.py)
# ═════════════════════════════════════════════════════════════════════════


def _jira_auth() -> dict:
    creds = base64.b64encode(f"{JIRA_EMAIL}:{JIRA_TOKEN}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}




async def create_jira_story(summary, description, priority="High"):
    if not all([JIRA_BASE, JIRA_EMAIL, JIRA_TOKEN]):
        return {"key": "MOCK-001", "url": f"{JIRA_BASE}/browse/MOCK-001", "status": "mock"}
    payload = {"fields": {
        "project": {"key": JIRA_PROJECT}, "summary": summary,
        "description": {"type": "doc", "version": 1,
                        "content": [{"type": "paragraph", "content": [{"type": "text", "text": description}]}]},
        "issuetype": {"name": "Story"}, "priority": {"name": priority},
        "labels": ["clearops", "bug", "auto-created"],
    }}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{JIRA_BASE}/rest/api/3/issue", headers=_jira_auth(), json=payload) as r:
            if r.status not in (200, 201):
                text = await r.text()
                log.error("[Jira] create failed %d: %s", r.status, text[:300])
                return {"key": None, "url": "", "status": "error"}
            data = await r.json()
            return {"key": data["key"], "url": f"{JIRA_BASE}/browse/{data['key']}", "status": "created"}




async def add_jira_comment(issue_key, comment):
    if not all([JIRA_BASE, JIRA_EMAIL, JIRA_TOKEN]) or not issue_key:
        return False
    payload = {"body": {"type": "doc", "version": 1,
                        "content": [{"type": "paragraph", "content": [{"type": "text", "text": comment}]}]}}
    async with aiohttp.ClientSession() as s:
        url = f"{JIRA_BASE}/rest/api/3/issue/{issue_key}/comment"
        async with s.post(url, headers=_jira_auth(), json=payload) as r:
            return r.status in (200, 201)




async def get_jira_transitions(issue_key):
    if not all([JIRA_BASE, JIRA_EMAIL, JIRA_TOKEN]):
        return []
    async with aiohttp.ClientSession() as s:
        url = f"{JIRA_BASE}/rest/api/3/issue/{issue_key}/transitions"
        async with s.get(url, headers=_jira_auth()) as r:
            if r.status != 200:
                return []
            return (await r.json()).get("transitions", [])




async def transition_jira_issue(issue_key, status_name):
    transitions = await get_jira_transitions(issue_key)
    tid = next((t["id"] for t in transitions if t["name"].lower() == status_name.lower()), None)
    if not tid:
        return False
    async with aiohttp.ClientSession() as s:
        url = f"{JIRA_BASE}/rest/api/3/issue/{issue_key}/transitions"
        async with s.post(url, headers=_jira_auth(), json={"transition": {"id": tid}}) as r:
            return r.status == 204




async def send_slack_alert(error_type, message, component, jira_url, analysis):
    if not SLACK_WEBHOOK:
        return False
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"🚨 ClearOps Alert — {error_type}"}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*Component:*\n`{component}`"},
            {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
            {"type": "mrkdwn", "text": f"*Error:*\n{message[:120]}"},
            {"type": "mrkdwn", "text": f"*Jira:*\n<{jira_url}|View story>"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*AI Analysis:*\n{analysis[:600]}"}},
    ]
    async with aiohttp.ClientSession() as s:
        async with s.post(SLACK_WEBHOOK, json={"blocks": blocks}) as r:
            return r.status == 200




async def send_slack_simple(text):
    if not SLACK_WEBHOOK:
        return False
    async with aiohttp.ClientSession() as s:
        async with s.post(SLACK_WEBHOOK, json={"text": text}) as r:
            return r.status == 200




def _gh_headers():
    return {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"}




async def get_github_file(file_path):
    if not all([GITHUB_TOKEN, GITHUB_REPO]):
        return {"content": "", "sha": "", "exists": False}
    async with aiohttp.ClientSession() as s:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}"
        async with s.get(url, headers=_gh_headers()) as r:
            if r.status != 200:
                return {"content": "", "sha": "", "exists": False}
            data = await r.json()
            return {"content": base64.b64decode(data["content"]).decode("utf-8"),
                    "sha": data["sha"], "exists": True}




async def push_fix_to_github(file_path, fixed_content, file_sha, branch, commit_message, pr_body=""):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None
    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.github.com/repos/{GITHUB_REPO}", headers=_gh_headers()) as r:
            if r.status != 200:
                return None
            default_branch = (await r.json()).get("default_branch", "main")
        async with s.get(f"https://api.github.com/repos/{GITHUB_REPO}/git/ref/heads/{default_branch}",
                         headers=_gh_headers()) as r:
            if r.status != 200:
                return None
            base_sha = (await r.json())["object"]["sha"]
        async with s.post(f"https://api.github.com/repos/{GITHUB_REPO}/git/refs", headers=_gh_headers(),
                          json={"ref": f"refs/heads/{branch}", "sha": base_sha}) as r:
            if r.status not in (201, 422):
                return None
        payload = {"message": commit_message, "content": base64.b64encode(fixed_content.encode()).decode(),
                   "branch": branch}
        if file_sha:
            payload["sha"] = file_sha
        async with s.put(f"https://api.github.com/repos/{GITHUB_REPO}/contents/{file_path}",
                         headers=_gh_headers(), json=payload) as r:
            if r.status not in (200, 201):
                text = await r.text()
                log.error("[GitHub] commit failed %d: %s", r.status, text[:300])
                return None
        async with s.post(f"https://api.github.com/repos/{GITHUB_REPO}/pulls", headers=_gh_headers(),
                          json={"title": f"[ClearOps Fix] {commit_message}", "head": branch,
                                "base": default_branch, "body": pr_body}) as r:
            if r.status in (200, 201):
                pr = await r.json()
                return {"pr_url": pr["html_url"], "pr_number": pr["number"],
                        "branch": branch, "default_branch": default_branch}
            return None




async def merge_github_pr(pr_number):
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return False
    async with aiohttp.ClientSession() as s:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls/{pr_number}/merge"
        async with s.put(url, headers=_gh_headers(),
                         json={"merge_method": "squash", "commit_title": f"ClearOps auto-fix — PR #{pr_number}"}) as r:
            return r.status == 200




def make_diff_html(old_code: str, new_code: str) -> str:
    diff = list(difflib.unified_diff(old_code.splitlines(), new_code.splitlines(),
                                     fromfile="before", tofile="after", lineterm=""))
    return "\n".join(diff) if diff else "(new file — no prior version)"




# ═════════════════════════════════════════════════════════════════════════
# OpenAI tool schemas
# ═════════════════════════════════════════════════════════════════════════


TOOLS = [
    {"type": "function", "function": {
        "name": "analyse_error",
        "description": "Analyse a JS/React runtime error. Return root cause, severity, Jira summary/description, fix steps.",
        "parameters": {"type": "object", "properties": {
            "root_cause": {"type": "string"}, "severity": {"type": "string", "enum": ["Low", "Medium", "High", "Critical"]},
            "jira_summary": {"type": "string"}, "jira_description": {"type": "string"},
            "fix_steps": {"type": "array", "items": {"type": "string"}},
        }, "required": ["root_cause", "severity", "jira_summary", "jira_description", "fix_steps"]},
    }},
    {"type": "function", "function": {
        "name": "create_jira_story", "description": "Create a Jira story for the bug.",
        "parameters": {"type": "object", "properties": {
            "summary": {"type": "string"}, "description": {"type": "string"},
            "priority": {"type": "string", "enum": ["Low", "Medium", "High", "Critical"]},
        }, "required": ["summary", "description", "priority"]},
    }},
    {"type": "function", "function": {
        "name": "send_slack_alert", "description": "Send Slack alert with error details + analysis + Jira link.",
        "parameters": {"type": "object", "properties": {
            "error_type": {"type": "string"}, "message": {"type": "string"}, "component": {"type": "string"},
            "jira_url": {"type": "string"}, "analysis": {"type": "string"},
        }, "required": ["error_type", "message", "component", "jira_url", "analysis"]},
    }},
    {"type": "function", "function": {
        "name": "generate_code_fix", "description": "Generate a corrected version of the buggy code.",
        "parameters": {"type": "object", "properties": {
            "fixed_code": {"type": "string"}, "explanation": {"type": "string"}, "file_name": {"type": "string"},
        }, "required": ["fixed_code", "explanation", "file_name"]},
    }},
]




# ═════════════════════════════════════════════════════════════════════════
# Phase 1 — automatic pipeline, streamed to the browser
# ═════════════════════════════════════════════════════════════════════════


async def run_phase1(error_payload: dict) -> dict:
    error_id   = error_payload.get("id", "unknown")
    error_type = error_payload.get("errorType", "Error")
    message    = error_payload.get("message", "")
    stack      = error_payload.get("stack", "")
    component  = error_payload.get("componentName", "unknown")
    file_name  = error_payload.get("file", "App.jsx")


    await broadcast({"type": "incident_start", "error_id": error_id, "error_type": error_type,
                     "message": message, "component": component})


    source_file = await get_github_file(file_name)
    source_code = source_file["content"][:3000]
    file_sha    = source_file["sha"]


    system_prompt = (
        "You are ClearOps, an AI ops agent. When you receive a JS/React error:\n"
        "1. Call analyse_error\n2. Call create_jira_story\n3. Call send_slack_alert\n4. Call generate_code_fix\n"
        "Call all 4 tools in order. Do NOT push to GitHub or change Jira status — that happens separately."
    )
    user_message = f"""Error report:
ID: {error_id} | Type: {error_type} | Component: {component} | File: {file_name}
Message: {message}
Stack: {stack[:1500]}


Source file ({file_name}):
```javascript
{source_code}
```
Run: analyse -> Jira story -> Slack alert -> code fix."""


    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}]
    state = {"error_id": error_id, "error_type": error_type, "component": component,
             "file_name": file_name, "original_code": source_code, "file_sha": file_sha,
             "analysis": None, "jira_key": None, "jira_url": "", "slack_sent": False, "fix": None}


    for _ in range(8):
        response = await client.chat.completions.create(model=MODEL, messages=messages, tools=TOOLS, tool_choice="auto")
        msg = response.choices[0].message
        messages.append(msg)
        if not msg.tool_calls:
            break
        for tc in msg.tool_calls:
            fn, args = tc.function.name, json.loads(tc.function.arguments)
            result = {}


            if fn == "analyse_error":
                state["analysis"] = args
                result = args
                await broadcast({"type": "analysis", "severity": args.get("severity"),
                                 "root_cause": args.get("root_cause"), "fix_steps": args.get("fix_steps", [])})


            elif fn == "create_jira_story":
                jr = await create_jira_story(args["summary"], args["description"], args.get("priority", "High"))
                state["jira_key"], state["jira_url"] = jr.get("key"), jr.get("url", "")
                result = jr
                await broadcast({"type": "jira_created", "key": jr.get("key"), "url": jr.get("url")})


            elif fn == "send_slack_alert":
                sent = await send_slack_alert(args.get("error_type", error_type), args.get("message", message),
                                              args.get("component", component), args.get("jira_url") or state["jira_url"],
                                              args.get("analysis", ""))
                state["slack_sent"] = sent
                result = {"sent": sent}
                await broadcast({"type": "slack_sent", "sent": sent})


            elif fn == "generate_code_fix":
                state["fix"] = args
                result = args
                diff = make_diff_html(state["original_code"], args["fixed_code"])
                await broadcast({"type": "fix_generated", "file_name": args.get("file_name", file_name),
                                 "explanation": args.get("explanation", ""), "diff": diff,
                                 "fixed_code": args["fixed_code"]})


            messages.append({"role": "tool", "tool_call_id": tc.id, "content": json.dumps(result)})


    return state




# ═════════════════════════════════════════════════════════════════════════
# Phase 2 — human approval gates, via browser instead of CLI
# ═════════════════════════════════════════════════════════════════════════


async def run_phase2(state: dict):
    if not state.get("fix"):
        await broadcast({"type": "info", "message": "No fix generated — nothing to approve."})
        return


    fix, error_id, jira_key, jira_url = state["fix"], state["error_id"], state.get("jira_key"), state.get("jira_url", "")


    # ── Gate 1: push to GitHub ──────────────────────────────────────────
    approval_id = str(uuid.uuid4())
    await broadcast({"type": "approval_request", "approval_id": approval_id, "gate": "push",
                     "question": "Push this fix to a new GitHub branch and open a PR?"})
    decision = await wait_for_approval(approval_id)


    pr_info = None
    if decision.get("approved"):
        branch = f"fix/clearops-{error_id.lower()}-{int(datetime.now().timestamp())}"
        pr_body = (f"## ClearOps Auto-Fix\n\n**Error ID:** `{error_id}`\n**Component:** {state['component']}\n\n"
                  f"### Root cause\n{state['analysis']['root_cause'] if state['analysis'] else 'N/A'}\n\n"
                  f"### Fix\n{fix.get('explanation','')}\n\n### Jira\n{jira_url or 'not linked'}\n\n"
                  f"_Approved via ClearOps dashboard._")
        pr_info = await push_fix_to_github(
            file_path=fix.get("file_name") or state["file_name"], fixed_content=fix["fixed_code"],
            file_sha=state["file_sha"], branch=branch,
            commit_message=f"fix: {fix.get('explanation','auto-fix')[:72]}", pr_body=pr_body)
        if pr_info:
            await broadcast({"type": "github_pushed", "branch": pr_info["branch"], "pr_url": pr_info["pr_url"],
                             "pr_number": pr_info["pr_number"]})
            await send_slack_simple(f"✅ ClearOps pushed a fix for `{error_id}` → {pr_info['pr_url']}")
        else:
            await broadcast({"type": "github_push_failed"})
    else:
        await broadcast({"type": "gate_skipped", "gate": "push"})


    # ── Gate 2: merge ────────────────────────────────────────────────────
    merged = False
    if pr_info:
        approval_id2 = str(uuid.uuid4())
        await broadcast({"type": "approval_request", "approval_id": approval_id2, "gate": "merge",
                         "question": f"Merge PR #{pr_info['pr_number']} into '{pr_info['default_branch']}' and deploy?",
                         "pr_url": pr_info["pr_url"]})
        decision2 = await wait_for_approval(approval_id2)
        if decision2.get("approved"):
            merged = await merge_github_pr(pr_info["pr_number"])
            if merged:
                await broadcast({"type": "merged", "pr_number": pr_info["pr_number"],
                                 "default_branch": pr_info["default_branch"], "vercel_url": VERCEL_URL})
                await send_slack_simple(f"🚀 ClearOps merged the fix for `{error_id}` into {pr_info['default_branch']}.")
            else:
                await broadcast({"type": "merge_failed"})
        else:
            await broadcast({"type": "gate_skipped", "gate": "merge"})


    # ── Gate 3: Jira update ──────────────────────────────────────────────
    if jira_key:
        approval_id3 = str(uuid.uuid4())
        await broadcast({"type": "approval_request", "approval_id": approval_id3, "gate": "jira",
                         "question": f"Update Jira story {jira_key} now?", "jira_key": jira_key,
                         "options": ["Add comment only", "Move to In Progress", "Move to Done", "Skip"]})
        decision3 = await wait_for_approval(approval_id3)
        choice = decision3.get("choice", "Skip")


        comment_lines = [f"Fix explanation: {fix.get('explanation','')}"]
        if pr_info:
            comment_lines.append(f"GitHub PR: {pr_info['pr_url']}")
        if merged:
            comment_lines.append(f"Merged into {pr_info['default_branch']}")
        comment_text = "\n".join(comment_lines)


        if choice == "Add comment only":
            await add_jira_comment(jira_key, comment_text)
            await broadcast({"type": "jira_updated", "key": jira_key, "action": "comment"})
        elif choice == "Move to In Progress":
            await add_jira_comment(jira_key, comment_text)
            ok = await transition_jira_issue(jira_key, "In Progress")
            await broadcast({"type": "jira_updated", "key": jira_key, "action": "in_progress", "ok": ok})
        elif choice == "Move to Done":
            await add_jira_comment(jira_key, comment_text)
            ok = await transition_jira_issue(jira_key, "Done") or await transition_jira_issue(jira_key, "Resolved")
            await broadcast({"type": "jira_updated", "key": jira_key, "action": "done", "ok": ok})
        else:
            await broadcast({"type": "gate_skipped", "gate": "jira"})


    await broadcast({"type": "incident_complete", "error_id": error_id, "jira_url": jira_url,
                     "pr_url": pr_info["pr_url"] if pr_info else None, "merged": merged})




# ═════════════════════════════════════════════════════════════════════════
# Routes
# ═════════════════════════════════════════════════════════════════════════


ERROR_QUEUE: asyncio.Queue = asyncio.Queue()




@app.post("/error")
async def receive_error(request: Request):
    payload = await request.json()
    await ERROR_QUEUE.put(payload)
    return JSONResponse({"status": "queued", "id": payload.get("id")}, status_code=202)




@app.options("/error")
async def options_error():
    return JSONResponse({})




@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ACTIVE_SOCKETS.append(websocket)
    # Replay history so a newly opened dashboard isn't blank
    for event in INCIDENT_LOG[-50:]:
        await websocket.send_json(event)
    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") == "approval_response":
                approval_id = data.get("approval_id")
                fut = PENDING_APPROVALS.get(approval_id)
                if fut and not fut.done():
                    fut.set_result(data)
    except WebSocketDisconnect:
        if websocket in ACTIVE_SOCKETS:
            ACTIVE_SOCKETS.remove(websocket)




@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML




async def queue_worker():
    while True:
        payload = await ERROR_QUEUE.get()
        try:
            state = await run_phase1(payload)
            await run_phase2(state)
        except Exception:
            log.exception("Pipeline error")




@app.on_event("startup")
async def startup():
    asyncio.create_task(queue_worker())
    log.info("ClearOps dashboard live at http://localhost:%d", AGENT_PORT)




# ═════════════════════════════════════════════════════════════════════════
# Dashboard HTML (single file, no build step)
# ═════════════════════════════════════════════════════════════════════════


DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ClearOps — Incident Console</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap');


  :root {
    --bg: #0B0E14;
    --panel: #11151F;
    --border: #1C2230;
    --teal: #7DD3C0;
    --coral: #E8784C;
    --muted: #5C6478;
    --text: #E4E7ED;
    --red: #F25555;
    --green: #4ADE80;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font-family: 'Inter', sans-serif; height: 100vh; overflow: hidden;
  }
  .mono { font-family: 'IBM Plex Mono', monospace; }


  .layout { display: flex; height: 100vh; }


  /* ── Left: streaming log ── */
  .stream-col {
    flex: 1; display: flex; flex-direction: column; min-width: 0;
    border-right: 1px solid var(--border);
  }
  .header {
    padding: 18px 24px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 12px;
  }
  .logo {
    width: 30px; height: 30px; border-radius: 8px;
    background: linear-gradient(135deg, var(--teal), #4a9d8a);
    display: flex; align-items: center; justify-content: center;
    font-weight: 700; color: #06140f; font-size: 14px;
  }
  .header-title { font-weight: 600; font-size: 14.5px; letter-spacing: -0.01em; }
  .header-sub { color: var(--muted); font-size: 11.5px; }
  .live-dot {
    margin-left: auto; display: flex; align-items: center; gap: 6px;
    font-size: 11px; color: var(--muted);
  }
  .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--teal); animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.3; } }


  .stream {
    flex: 1; overflow-y: auto; padding: 20px 24px;
    display: flex; flex-direction: column; gap: 10px;
  }
  .stream::-webkit-scrollbar { width: 8px; }
  .stream::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }


  .placeholder {
    margin: auto; text-align: center; color: var(--muted); max-width: 340px;
  }
  .placeholder .icon { font-size: 32px; margin-bottom: 12px; opacity: 0.5; }
  .placeholder .title { font-size: 14px; font-weight: 500; color: var(--text); margin-bottom: 6px; }
  .placeholder .sub { font-size: 12.5px; line-height: 1.6; }


  .event {
    border-left: 2px solid var(--border); padding: 2px 0 2px 14px;
    animation: slideIn 0.25s ease;
  }
  @keyframes slideIn { from { opacity: 0; transform: translateX(-6px); } to { opacity: 1; transform: translateX(0); } }
  .event-time { font-size: 10.5px; color: var(--muted); margin-bottom: 3px; }
  .event-body { font-size: 13px; line-height: 1.55; }
  .event-tag {
    display: inline-block; font-size: 10px; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em; padding: 2px 7px; border-radius: 4px; margin-right: 8px;
  }


  .event.incident_start { border-left-color: var(--red); }
  .event.incident_start .event-tag { background: rgba(242,85,85,0.15); color: var(--red); }
  .event.analysis { border-left-color: #a78bfa; }
  .event.analysis .event-tag { background: rgba(167,139,250,0.15); color: #a78bfa; }
  .event.jira_created { border-left-color: #60a5fa; }
  .event.jira_created .event-tag { background: rgba(96,165,250,0.15); color: #60a5fa; }
  .event.slack_sent { border-left-color: var(--teal); }
  .event.slack_sent .event-tag { background: rgba(125,211,192,0.15); color: var(--teal); }
  .event.fix_generated { border-left-color: var(--green); }
  .event.fix_generated .event-tag { background: rgba(74,222,128,0.15); color: var(--green); }
  .event.github_pushed, .event.merged { border-left-color: var(--coral); }
  .event.github_pushed .event-tag, .event.merged .event-tag { background: rgba(232,120,76,0.15); color: var(--coral); }
  .event.jira_updated { border-left-color: #60a5fa; }
  .event.jira_updated .event-tag { background: rgba(96,165,250,0.15); color: #60a5fa; }
  .event.gate_skipped { border-left-color: var(--muted); opacity: 0.6; }
  .event.gate_skipped .event-tag { background: rgba(92,100,120,0.15); color: var(--muted); }
  .event.incident_complete { border-left-color: var(--green); }
  .event.incident_complete .event-tag { background: rgba(74,222,128,0.15); color: var(--green); }


  .diff-block {
    margin-top: 8px; background: #06080c; border: 1px solid var(--border);
    border-radius: 6px; padding: 10px 12px; font-size: 11.5px; line-height: 1.6;
    max-height: 220px; overflow-y: auto; white-space: pre-wrap;
  }
  .diff-add { color: var(--green); }
  .diff-rem { color: var(--red); }
  .diff-meta { color: var(--muted); }


  .link-pill {
    display: inline-flex; align-items: center; gap: 4px;
    background: var(--panel); border: 1px solid var(--border);
    color: var(--text); text-decoration: none; font-size: 11.5px;
    padding: 3px 10px; border-radius: 999px; margin-top: 6px; margin-right: 6px;
  }
  .link-pill:hover { border-color: var(--teal); }


  /* ── Right: incident state + decision card ── */
  .side-col {
    width: 380px; flex-shrink: 0; display: flex; flex-direction: column;
    background: var(--panel);
  }
  .side-header {
    padding: 18px 22px; border-bottom: 1px solid var(--border);
    font-size: 12.5px; font-weight: 600; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.05em;
  }
  .side-body { flex: 1; overflow-y: auto; padding: 18px 22px; }


  .stat-row { display: flex; justify-content: space-between; margin-bottom: 14px; font-size: 13px; }
  .stat-label { color: var(--muted); }
  .stat-value { font-weight: 500; }


  .decision-card {
    background: linear-gradient(180deg, #1a1410, #15110d);
    border: 1px solid #4a3520; border-radius: 12px;
    padding: 20px; margin-top: 4px;
    animation: cardIn 0.3s ease;
  }
  @keyframes cardIn { from { opacity:0; transform: translateY(8px); } to { opacity:1; transform: translateY(0); } }
  .decision-eyebrow {
    font-size: 10.5px; font-weight: 600; color: var(--coral);
    text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px;
  }
  .decision-question { font-size: 14.5px; font-weight: 500; line-height: 1.5; margin-bottom: 16px; }
  .decision-btns { display: flex; gap: 10px; }
  .btn {
    flex: 1; padding: 10px 0; border-radius: 8px; border: none;
    font-size: 13px; font-weight: 600; cursor: pointer; transition: opacity 0.15s;
    font-family: 'Inter', sans-serif;
  }
  .btn:hover { opacity: 0.85; }
  .btn-yes { background: var(--coral); color: #1a0f08; }
  .btn-no { background: transparent; color: var(--muted); border: 1px solid var(--border); }
  .choice-btn {
    display: block; width: 100%; text-align: left; padding: 10px 14px;
    background: #181210; border: 1px solid #2a2018; color: var(--text);
    border-radius: 8px; font-size: 13px; cursor: pointer; margin-bottom: 8px;
    font-family: 'Inter', sans-serif;
  }
  .choice-btn:hover { border-color: var(--coral); }


  .empty-side { color: var(--muted); font-size: 13px; text-align: center; margin-top: 40px; }
</style>
</head>
<body>
<div class="layout">


  <div class="stream-col">
    <div class="header">
      <div class="logo">C</div>
      <div>
        <div class="header-title">ClearOps Incident Console</div>
        <div class="header-sub">Live agent activity</div>
      </div>
      <div class="live-dot"><span class="dot"></span> LIVE</div>
    </div>
    <div class="stream" id="stream">
      <div class="placeholder" id="placeholder">
        <div class="icon">◎</div>
        <div class="title">Waiting for an incident</div>
        <div class="sub">Trigger a bug in the connected app — this console will stream the agent's full investigation in real time.</div>
      </div>
    </div>
  </div>


  <div class="side-col">
    <div class="side-header">Pending decision</div>
    <div class="side-body" id="side-body">
      <div class="empty-side">No action needed right now.</div>
    </div>
  </div>


</div>


<script>
const stream = document.getElementById('stream');
const placeholder = document.getElementById('placeholder');
const sideBody = document.getElementById('side-body');


let ws;
function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = (msg) => handleEvent(JSON.parse(msg.data));
  ws.onclose = () => setTimeout(connect, 1000);
}
connect();


function fmtTime(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString('en-US', { hour12: false });
}


function addEvent(type, html) {
  if (placeholder) placeholder.remove();
  const div = document.createElement('div');
  div.className = 'event ' + type;
  div.innerHTML = html;
  stream.appendChild(div);
  stream.scrollTop = stream.scrollHeight;
}


function escapeHtml(str) {
  return (str || '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}


function diffHtml(diff) {
  return diff.split('\n').map(line => {
    const esc = escapeHtml(line);
    if (line.startsWith('+') && !line.startsWith('+++')) return `<span class="diff-add">${esc}</span>`;
    if (line.startsWith('-') && !line.startsWith('---')) return `<span class="diff-rem">${esc}</span>`;
    if (line.startsWith('@@')) return `<span class="diff-meta">${esc}</span>`;
    return `<span class="diff-meta">${esc}</span>`;
  }).join('\n');
}


function handleEvent(ev) {
  const t = fmtTime(ev.ts);


  switch (ev.type) {
    case 'incident_start':
      addEvent('incident_start', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Incident</span>
        <strong>${escapeHtml(ev.error_type)}</strong> detected in <code>${escapeHtml(ev.component)}</code><br>
        <span style="color:var(--muted)">${escapeHtml(ev.message)}</span></div>
      `);
      break;


    case 'analysis':
      addEvent('analysis', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">AI Analysis</span>
        Severity: <strong>${escapeHtml(ev.severity)}</strong><br>
        ${escapeHtml(ev.root_cause)}</div>
      `);
      break;


    case 'jira_created':
      addEvent('jira_created', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Jira</span>
        Story created: <strong>${escapeHtml(ev.key || 'N/A')}</strong><br>
        <a class="link-pill" href="${ev.url}" target="_blank">↗ ${escapeHtml(ev.url || '')}</a></div>
      `);
      break;


    case 'slack_sent':
      addEvent('slack_sent', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Slack</span>
        ${ev.sent ? 'Alert posted to channel ✅' : 'Slack not configured — skipped'}</div>
      `);
      break;


    case 'fix_generated':
      addEvent('fix_generated', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Fix</span>
        ${escapeHtml(ev.explanation)}</div>
        <div class="diff-block">${diffHtml(ev.diff)}</div>
      `);
      break;


    case 'github_pushed':
      addEvent('github_pushed', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">GitHub</span>
        Pushed to branch <code>${escapeHtml(ev.branch)}</code><br>
        <a class="link-pill" href="${ev.pr_url}" target="_blank">↗ PR #${ev.pr_number}</a></div>
      `);
      break;


    case 'github_push_failed':
      addEvent('github_pushed', `<div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag" style="color:var(--red)">GitHub</span> Push failed — check server logs.</div>`);
      break;


    case 'merged':
      addEvent('merged', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Merged</span>
        PR #${ev.pr_number} merged into <code>${escapeHtml(ev.default_branch)}</code> — deploy triggered
        ${ev.vercel_url ? `<br><a class="link-pill" href="https://${ev.vercel_url}" target="_blank">↗ ${ev.vercel_url}</a>` : ''}
        </div>
      `);
      break;


    case 'merge_failed':
      addEvent('merged', `<div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag" style="color:var(--red)">Merge</span> Failed — check server logs.</div>`);
      break;


    case 'jira_updated':
      addEvent('jira_updated', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Jira</span>
        ${escapeHtml(ev.key)} updated (${escapeHtml(ev.action)})</div>
      `);
      break;


    case 'gate_skipped':
      addEvent('gate_skipped', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Skipped</span>
        ${escapeHtml(ev.gate)} step declined by operator</div>
      `);
      break;


    case 'incident_complete':
      addEvent('incident_complete', `
        <div class="event-time">${t}</div>
        <div class="event-body"><span class="event-tag">Done</span>
        Incident workflow complete.
        ${ev.pr_url ? `<a class="link-pill" href="${ev.pr_url}" target="_blank">↗ PR</a>` : ''}
        ${ev.jira_url ? `<a class="link-pill" href="${ev.jira_url}" target="_blank">↗ Jira</a>` : ''}
        </div>
      `);
      clearSide();
      break;


    case 'approval_request':
      renderDecision(ev);
      break;
  }
}


function clearSide() {
  sideBody.innerHTML = '<div class="empty-side">No action needed right now.</div>';
}


function renderDecision(ev) {
  if (ev.gate === 'jira' && ev.options) {
    sideBody.innerHTML = `
      <div class="decision-card">
        <div class="decision-eyebrow">Decision needed · ${escapeHtml(ev.gate)}</div>
        <div class="decision-question">${escapeHtml(ev.question)}</div>
        ${ev.options.map(opt => `<button class="choice-btn" data-choice="${escapeHtml(opt)}">${escapeHtml(opt)}</button>`).join('')}
      </div>
    `;
    sideBody.querySelectorAll('.choice-btn').forEach(btn => {
      btn.onclick = () => {
        ws.send(JSON.stringify({ type: 'approval_response', approval_id: ev.approval_id, choice: btn.dataset.choice }));
        clearSide();
      };
    });
  } else {
    sideBody.innerHTML = `
      <div class="decision-card">
        <div class="decision-eyebrow">Decision needed · ${escapeHtml(ev.gate)}</div>
        <div class="decision-question">${escapeHtml(ev.question)}</div>
        ${ev.pr_url ? `<a class="link-pill" href="${ev.pr_url}" target="_blank">↗ Review PR first</a>` : ''}
        <div class="decision-btns" style="margin-top:14px;">
          <button class="btn btn-yes" id="approve-yes">Yes, proceed</button>
          <button class="btn btn-no" id="approve-no">Not now</button>
        </div>
      </div>
    `;
    document.getElementById('approve-yes').onclick = () => {
      ws.send(JSON.stringify({ type: 'approval_response', approval_id: ev.approval_id, approved: true }));
      clearSide();
    };
    document.getElementById('approve-no').onclick = () => {
      ws.send(JSON.stringify({ type: 'approval_response', approval_id: ev.approval_id, approved: false }));
      clearSide();
    };
  }
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=AGENT_PORT)


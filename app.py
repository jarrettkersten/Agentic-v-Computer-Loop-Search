"""
Loop Comparison App
Compares two search approaches for a given query:
  1. Agentic Loop  — Claude uses a web_search tool iteratively via the Anthropic API
  2. Computer Loop — Playwright controls a real browser (mirrors Anthropic computer-use pattern)
"""

import os
import json
import asyncio
import re
import uuid
import time
import secrets
import functools
import sqlite3
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from dotenv import load_dotenv
import anthropic
from playwright.async_api import async_playwright
import msal

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL = "claude-sonnet-4-6"
MAX_AGENTIC_ITERATIONS = 4  # cap on search rounds in the agentic loop
SEARCH_RESULTS_PER_QUERY = 5

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", secrets.token_hex(32))

# ---------------------------------------------------------------------------
# Entra ID / Azure AD Auth Config
# ---------------------------------------------------------------------------
ENTRA_CLIENT_ID     = os.getenv("ENTRA_CLIENT_ID", "")
ENTRA_CLIENT_SECRET = os.getenv("ENTRA_CLIENT_SECRET", "")
ENTRA_TENANT_ID     = os.getenv("ENTRA_TENANT_ID", "")
ENTRA_REDIRECT_URI  = os.getenv("ENTRA_REDIRECT_URI", "")
ENTRA_AUTHORITY     = f"https://login.microsoftonline.com/{ENTRA_TENANT_ID}"
ENTRA_SCOPES        = ["User.Read"]

# Set to "true" to bypass auth during local development
AUTH_DISABLED = os.getenv("AUTH_DISABLED", "false").lower() == "true"

# Admin whitelist — comma-separated emails that can access /admin
ADMIN_EMAILS = [e.strip().lower() for e in os.getenv("ADMIN_EMAILS", "").split(",") if e.strip()]

# ---------------------------------------------------------------------------
# SQLite Database for flagged queries
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flagged_queries.db")

def get_db():
    """Get a SQLite connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    """Create the flagged_queries table if it doesn't exist."""
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_queries (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            query           TEXT NOT NULL,
            loop_type       TEXT NOT NULL,
            answer          TEXT NOT NULL,
            explanation     TEXT NOT NULL,
            flagged_by      TEXT DEFAULT 'anonymous',
            flagged_at      TEXT NOT NULL DEFAULT (datetime('now')),
            status          TEXT NOT NULL DEFAULT 'pending',
            reviewed_by     TEXT,
            reviewed_at     TEXT,
            admin_notes     TEXT DEFAULT ''
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ---------------------------------------------------------------------------
# Auth helpers (mirrors cora-bug-creator pattern)
# ---------------------------------------------------------------------------

def _build_msal_app():
    return msal.ConfidentialClientApplication(
        ENTRA_CLIENT_ID,
        authority=ENTRA_AUTHORITY,
        client_credential=ENTRA_CLIENT_SECRET,
    )

def login_required(f):
    """Decorator: redirects unauthenticated users to /login.
       Returns JSON 401 for AJAX/fetch requests."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if AUTH_DISABLED:
            return f(*args, **kwargs)
        if not session.get("user"):
            if request.is_json or request.headers.get("Accept", "").startswith("application/json"):
                return jsonify({"error": "Session expired — please refresh the page and sign in again."}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    """Decorator: requires login + email in ADMIN_EMAILS whitelist."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if AUTH_DISABLED:
            return f(*args, **kwargs)
        user = session.get("user", {})
        if not user:
            return redirect(url_for("login"))
        email = user.get("preferred_username", user.get("email", "")).lower()
        if email not in ADMIN_EMAILS:
            return "Access denied — you are not authorised to view this page.", 403
        return f(*args, **kwargs)
    return decorated

def _is_admin():
    """Check if the current logged-in user is an admin."""
    if AUTH_DISABLED:
        return True
    user = session.get("user", {})
    email = user.get("preferred_username", user.get("email", "")).lower()
    return email in ADMIN_EMAILS

def _current_user_email():
    if AUTH_DISABLED:
        return "dev@local"
    user = session.get("user", {})
    return user.get("preferred_username", user.get("email", "anonymous"))

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def get_client():
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY environment variable is not set.")
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def ddg_search(query: str, num: int = SEARCH_RESULTS_PER_QUERY) -> list[dict]:
    """Scrape DuckDuckGo HTML results — no API key required."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; LoopTest/1.0)"}
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers=headers,
            timeout=10,
        )
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []
        for r in soup.select(".result__body")[:num]:
            title = r.select_one(".result__title")
            snippet = r.select_one(".result__snippet")
            link = r.select_one("a.result__url")
            results.append({
                "title": title.get_text(strip=True) if title else "",
                "snippet": snippet.get_text(strip=True) if snippet else "",
                "url": link.get_text(strip=True) if link else "",
            })
        return results
    except Exception as e:
        return [{"title": "Search error", "snippet": str(e), "url": ""}]


# ---------------------------------------------------------------------------
# 1. AGENTIC LOOP
#    Claude drives the search by calling a web_search tool.
#    The loop continues until Claude stops calling tools or MAX_ITERATIONS hit.
# ---------------------------------------------------------------------------

def run_agentic_loop(query: str) -> dict:
    client = get_client()

    tools = [
        {
            "name": "web_search",
            "description": (
                "Search the web for information relevant to the query. "
                "Call this multiple times with different search terms to gather "
                "comprehensive context before forming your final answer."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_query": {
                        "type": "string",
                        "description": "The exact search terms to look up.",
                    }
                },
                "required": ["search_query"],
            },
        }
    ]

    system_prompt = (
        "You are a thorough research assistant. "
        "Use the web_search tool to gather information needed to answer the user's query. "
        "Search multiple times with different angles until you have enough context. "
        "When you are confident you have sufficient information, provide a final answer "
        "WITHOUT calling any more tools — just write the answer directly."
    )

    messages = [{"role": "user", "content": f"Please research and answer this query:\n\n{query}"}]
    iterations_log = []
    iteration = 0
    final_answer = ""

    while iteration < MAX_AGENTIC_ITERATIONS:
        iteration += 1

        response = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )

        # Collect any tool calls in this turn
        tool_uses = [b for b in response.content if b.type == "tool_use"]

        if not tool_uses:
            # Claude is done searching — extract final text
            final_answer = " ".join(
                b.text for b in response.content if hasattr(b, "text")
            ).strip()
            iterations_log.append({
                "iteration": iteration,
                "action": "Claude concluded — no more searches needed.",
                "searches": [],
            })
            break

        # Execute each tool call
        searches_this_turn = []
        tool_results = []

        for tu in tool_uses:
            sq = tu.input.get("search_query", query)
            results = ddg_search(sq)
            searches_this_turn.append({"search_query": sq, "results": results})
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": json.dumps(results),
            })

        iterations_log.append({
            "iteration": iteration,
            "action": f"Searched {len(tool_uses)} term(s).",
            "searches": searches_this_turn,
        })

        # Feed results back so Claude can continue
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": tool_results})

        if response.stop_reason == "end_turn":
            # Claude wrote text AND finished — grab any text in this turn
            text = " ".join(
                b.text for b in response.content if hasattr(b, "text")
            ).strip()
            if text:
                final_answer = text
            break

    # If we hit the cap without a final answer, do one last synthesis call
    if not final_answer:
        context_summary = "\n\n".join(
            f"Iteration {it['iteration']}:\n"
            + "\n".join(
                f"  Search: {s['search_query']}\n"
                + "\n".join(f"    - {r['title']}: {r['snippet']}" for r in s["results"])
                for s in it["searches"]
            )
            for it in iterations_log
            if it["searches"]
        )
        fallback = client.messages.create(
            model=MODEL,
            max_tokens=1500,
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Based on the following research, answer this query:\n\n"
                        f"Query: {query}\n\n"
                        f"Research gathered:\n{context_summary}\n\n"
                        "Provide a clear, well-structured final answer."
                    ),
                }
            ],
        )
        final_answer = fallback.content[0].text

    total_searches = sum(len(it["searches"]) for it in iterations_log)
    return {
        "answer": final_answer,
        "iterations": iterations_log,
        "total_iterations": iteration,
        "total_searches": total_searches,
    }


# ---------------------------------------------------------------------------
# 2. COMPUTER LOOP
#    Mirrors the Anthropic computer-use pattern:
#      navigate → observe (read page) → act (click/follow) → extract → synthesize
#
#    Uses Playwright when available (full headless browser).
#    Falls back to requests + BeautifulSoup when Playwright/Chromium is not
#    installed — same step-by-step logic, same output shape.
# ---------------------------------------------------------------------------

PLAYWRIGHT_AVAILABLE = False
try:
    from playwright.sync_api import sync_playwright as _check_pw  # noqa
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    pass


def _fetch_page_requests(url: str, timeout: int = 10) -> tuple[str, str]:
    """Return (final_url, html) via requests."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; LoopTest/1.0)"}
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    return resp.url, resp.text


def _extract_results_from_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for r in soup.select(".result__body")[:SEARCH_RESULTS_PER_QUERY]:
        title   = r.select_one(".result__title")
        snippet = r.select_one(".result__snippet")
        link    = r.select_one("a.result__url")
        if title:
            results.append({
                "title":   title.get_text(strip=True),
                "snippet": snippet.get_text(strip=True) if snippet else "",
                "url":     link.get_text(strip=True) if link else "",
            })
    return results


def _extract_first_href(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("h2 a[href], .result__title a[href]"):
        href = a.get("href", "")
        if href.startswith("http"):
            return href
    return None


def _run_computer_loop_requests(query: str) -> dict:
    """
    Headless-requests fallback: same navigate/observe/extract/synthesize
    pattern as the Playwright version, without a GUI browser.
    """
    steps_log = []
    results = []
    page_content = ""
    visited_url = ""

    # Step 1 — "Navigate" to search engine
    search_url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    steps_log.append({"step": 1, "action": "Navigated to DuckDuckGo (headless HTTP)", "url": search_url})

    # Step 2 — "Observe" the search results page
    try:
        final_url, html = _fetch_page_requests(search_url)
        steps_log.append({"step": 2, "action": f'Submitted query: "{query}"', "url": final_url})

        # Step 3 — "Read" / extract results
        results = _extract_results_from_html(html)
        steps_log.append({
            "step": 3,
            "action": f"Read page — extracted {len(results)} result(s)",
            "results": results,
        })

        # Step 4 — "Click" first result & read that page
        first_href = _extract_first_href(html)
        if first_href:
            try:
                visited_url, page_html = _fetch_page_requests(first_href, timeout=10)
                soup = BeautifulSoup(page_html, "html.parser")
                # Remove scripts/styles for cleaner text
                for tag in soup(["script", "style", "nav", "footer"]):
                    tag.decompose()
                raw = soup.get_text(separator="\n")
                page_content = re.sub(r"\n{3,}", "\n\n", raw).strip()[:3500]
                steps_log.append({
                    "step": 4,
                    "action": "Followed top result — read page content",
                    "url": visited_url,
                    "content_preview": page_content[:300] + "…" if len(page_content) > 300 else page_content,
                })
            except Exception as e:
                steps_log.append({"step": 4, "action": f"Could not follow top result ({e})", "url": first_href})
    except Exception as e:
        steps_log.append({"step": 2, "action": f"Search request failed: {e}"})

    # Step 5 — Synthesise with Claude
    steps_log.append({"step": 5, "action": "Passing page findings to Claude for synthesis"})
    context_text = "Search results:\n" + "\n".join(
        f"• {r['title']}: {r['snippet']}" for r in results
    )
    if page_content:
        context_text += f"\n\nDetailed content from top result ({visited_url}):\n{page_content}"

    client = get_client()
    synthesis = client.messages.create(
        model=MODEL,
        max_tokens=1500,
        messages=[
            {
                "role": "user",
                "content": (
                    f"You are synthesizing research gathered by a browser agent "
                    f"(navigate → observe → extract → synthesize).\n\n"
                    f"Original query: {query}\n\n"
                    f"Browser findings:\n{context_text}\n\n"
                    "Provide a clear, well-structured answer based only on what the browser found."
                ),
            }
        ],
    )
    return {
        "answer": synthesis.content[0].text,
        "steps": steps_log,
        "total_steps": len(steps_log),
        "results_found": len(results),
        "mode": "headless-requests (Playwright not available)",
    }


async def _computer_loop_playwright(query: str) -> dict:
    """Full Playwright browser version (requires Chromium installed)."""
    steps_log = []
    results = []
    page_content = ""
    visited_url = ""

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx  = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (compatible; LoopTest/1.0)",
        )
        page = await ctx.new_page()

        # Step 1
        await page.goto("https://duckduckgo.com", timeout=15000)
        steps_log.append({"step": 1, "action": "Opened Chromium → navigated to DuckDuckGo", "url": page.url})

        # Step 2
        await page.fill('input[name="q"]', query)
        await page.keyboard.press("Enter")
        await page.wait_for_load_state("networkidle", timeout=15000)
        steps_log.append({"step": 2, "action": f'Typed query and pressed Enter: "{query}"', "url": page.url})

        # Step 3
        html = await page.content()
        results = _extract_results_from_html(html)
        steps_log.append({
            "step": 3,
            "action": f"Read SERP — extracted {len(results)} result(s)",
            "results": results,
        })

        # Step 4
        first_link = await page.query_selector("h2 a[href]")
        if first_link:
            href = await first_link.get_attribute("href")
            if href and href.startswith("http"):
                try:
                    await page.goto(href, timeout=12000)
                    await page.wait_for_load_state("domcontentloaded", timeout=10000)
                    visited_url = page.url
                    raw = await page.inner_text("body")
                    page_content = re.sub(r"\s{3,}", "\n\n", raw).strip()[:3500]
                    steps_log.append({
                        "step": 4,
                        "action": "Clicked top result — read full page",
                        "url": visited_url,
                        "content_preview": page_content[:300] + "…",
                    })
                except Exception as e:
                    steps_log.append({"step": 4, "action": f"Could not load result ({e})", "url": href})

        await browser.close()

    # Step 5
    steps_log.append({"step": 5, "action": "Passing browser findings to Claude for synthesis"})
    context_text = "Search results from browser:\n" + "\n".join(
        f"• {r['title']}: {r['snippet']}" for r in results
    )
    if page_content:
        context_text += f"\n\nPage content (from {visited_url}):\n{page_content}"

    client = get_client()
    synthesis = client.messages.create(
        model=MODEL,
        max_tokens=1500,
        messages=[
            {
                "role": "user",
                "content": (
                    f"You are synthesizing research gathered by a browser agent.\n\n"
                    f"Original query: {query}\n\n"
                    f"Browser findings:\n{context_text}\n\n"
                    "Provide a clear, well-structured answer based only on what the browser found."
                ),
            }
        ],
    )
    return {
        "answer": synthesis.content[0].text,
        "steps": steps_log,
        "total_steps": len(steps_log),
        "results_found": len(results),
        "mode": "Playwright (Chromium)",
    }


def run_computer_loop(query: str) -> dict:
    """Use Playwright if available, otherwise fall back to requests."""
    if PLAYWRIGHT_AVAILABLE:
        return asyncio.run(_computer_loop_playwright(query))
    return _run_computer_loop_requests(query)


# ---------------------------------------------------------------------------
# 3. COMPARISON
#    Claude reads both answers and produces a structured diff/summary.
# ---------------------------------------------------------------------------

def compare_results(query: str, agentic: dict, computer: dict) -> str:
    client = get_client()
    resp = client.messages.create(
        model=MODEL,
        max_tokens=1200,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Two different search approaches answered the same query. "
                    f"Compare them and provide:\n"
                    f"1. **Key similarities** — where both agree\n"
                    f"2. **Key differences** — facts or nuances one has that the other lacks\n"
                    f"3. **Completeness** — which answer is more thorough and why\n"
                    f"4. **Verdict** — which approach performed better for this query\n\n"
                    f"---\n"
                    f"**Query:** {query}\n\n"
                    f"**Agentic Loop Answer** ({agentic.get('total_iterations', '?')} iterations, "
                    f"{agentic.get('total_searches', '?')} searches):\n{agentic['answer']}\n\n"
                    f"**Computer Loop Answer** ({computer.get('total_steps', '?')} browser steps, "
                    f"{computer.get('results_found', '?')} results found):\n{computer['answer']}"
                ),
            }
        ],
    )
    return resp.content[0].text


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/")
@login_required
def index():
    user = session.get("user", {}) if not AUTH_DISABLED else {}
    return render_template("index.html",
                           user=user,
                           is_admin=_is_admin(),
                           auth_disabled=AUTH_DISABLED)


@app.route("/api/agentic", methods=["POST"])
@login_required
def api_agentic():
    data = request.get_json()
    query = (data or {}).get("query", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    try:
        result = run_agentic_loop(query)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/computer", methods=["POST"])
@login_required
def api_computer():
    data = request.get_json()
    query = (data or {}).get("query", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    try:
        result = run_computer_loop(query)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/compare", methods=["POST"])
@login_required
def api_compare():
    data = request.get_json() or {}
    query = data.get("query", "").strip()
    agentic = data.get("agentic_result")
    computer = data.get("computer_result")
    if not query or not agentic or not computer:
        return jsonify({"error": "Missing query or results."}), 400
    try:
        comparison = compare_results(query, agentic, computer)
        return jsonify({"success": True, "comparison": comparison})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Flag API — users report unhelpful/missing responses
# ---------------------------------------------------------------------------

@app.route("/api/flag", methods=["POST"])
@login_required
def api_flag():
    data = request.get_json() or {}
    query = data.get("query", "").strip()
    loop_type = data.get("loop_type", "").strip()
    answer = data.get("answer", "").strip()
    explanation = data.get("explanation", "").strip()

    if not query or not loop_type or not explanation:
        return jsonify({"error": "Missing required fields: query, loop_type, explanation."}), 400

    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO flagged_queries (query, loop_type, answer, explanation, flagged_by)
               VALUES (?, ?, ?, ?, ?)""",
            (query, loop_type, answer[:5000], explanation, _current_user_email()),
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True, "message": "Thank you — your feedback has been logged for admin review."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Admin dashboard
# ---------------------------------------------------------------------------

@app.route("/admin")
@admin_required
def admin_dashboard():
    return render_template("admin.html")


@app.route("/api/admin/stats", methods=["GET"])
@admin_required
def api_admin_stats():
    """Return dashboard statistics as JSON."""
    conn = get_db()

    # Total counts by status
    total = conn.execute("SELECT COUNT(*) as c FROM flagged_queries").fetchone()["c"]
    pending = conn.execute("SELECT COUNT(*) as c FROM flagged_queries WHERE status='pending'").fetchone()["c"]
    reviewed = conn.execute("SELECT COUNT(*) as c FROM flagged_queries WHERE status='reviewed'").fetchone()["c"]
    dismissed = conn.execute("SELECT COUNT(*) as c FROM flagged_queries WHERE status='dismissed'").fetchone()["c"]
    article_created = conn.execute("SELECT COUNT(*) as c FROM flagged_queries WHERE status='article_created'").fetchone()["c"]

    # Most common flagged queries (top 10)
    top_queries = [dict(r) for r in conn.execute(
        """SELECT query, COUNT(*) as count
           FROM flagged_queries
           GROUP BY query ORDER BY count DESC LIMIT 10"""
    ).fetchall()]

    # Flags per day (last 30 days)
    daily = [dict(r) for r in conn.execute(
        """SELECT DATE(flagged_at) as day, COUNT(*) as count
           FROM flagged_queries
           WHERE flagged_at >= datetime('now', '-30 days')
           GROUP BY DATE(flagged_at) ORDER BY day"""
    ).fetchall()]

    # By loop type
    by_loop = [dict(r) for r in conn.execute(
        """SELECT loop_type, COUNT(*) as count
           FROM flagged_queries GROUP BY loop_type"""
    ).fetchall()]

    conn.close()

    return jsonify({
        "total": total,
        "pending": pending,
        "reviewed": reviewed,
        "dismissed": dismissed,
        "article_created": article_created,
        "top_queries": top_queries,
        "daily": daily,
        "by_loop_type": by_loop,
    })


@app.route("/api/admin/flags", methods=["GET"])
@admin_required
def api_admin_flags():
    """Return all flagged queries, optionally filtered by status."""
    status_filter = request.args.get("status", "")
    conn = get_db()
    if status_filter:
        rows = conn.execute(
            "SELECT * FROM flagged_queries WHERE status=? ORDER BY flagged_at DESC", (status_filter,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM flagged_queries ORDER BY flagged_at DESC").fetchall()
    conn.close()
    return jsonify({"flags": [dict(r) for r in rows]})


@app.route("/api/admin/flags/<int:flag_id>", methods=["PATCH"])
@admin_required
def api_admin_update_flag(flag_id):
    """Update status/notes on a flagged query."""
    data = request.get_json() or {}
    new_status = data.get("status", "")
    admin_notes = data.get("admin_notes", "")

    if new_status not in ("pending", "reviewed", "dismissed", "article_created"):
        return jsonify({"error": "Invalid status."}), 400

    conn = get_db()
    conn.execute(
        """UPDATE flagged_queries
           SET status=?, admin_notes=?, reviewed_by=?, reviewed_at=datetime('now')
           WHERE id=?""",
        (new_status, admin_notes, _current_user_email(), flag_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# Auth Routes (Entra ID / Azure AD — mirrors cora-bug-creator)
# ---------------------------------------------------------------------------

@app.route("/login")
def login():
    """Initiate the Entra ID OAuth 2.0 auth-code flow."""
    if AUTH_DISABLED:
        return redirect(url_for("index"))
    session["auth_state"] = str(uuid.uuid4())
    auth_app = _build_msal_app()
    auth_url = auth_app.get_authorization_request_url(
        scopes=ENTRA_SCOPES,
        state=session["auth_state"],
        redirect_uri=ENTRA_REDIRECT_URI,
    )
    return redirect(auth_url)


@app.route("/auth/callback")
def auth_callback():
    """Handle the redirect back from Microsoft with the auth code."""
    if AUTH_DISABLED:
        return redirect(url_for("index"))

    # Verify state to prevent CSRF
    if request.args.get("state") != session.get("auth_state"):
        return "State mismatch — possible CSRF. Please try logging in again.", 403

    if "error" in request.args:
        return f"Authentication error: {request.args.get('error_description', request.args['error'])}", 403

    code = request.args.get("code")
    if not code:
        return "No authorization code received.", 400

    auth_app = _build_msal_app()
    result = auth_app.acquire_token_by_authorization_code(
        code,
        scopes=ENTRA_SCOPES,
        redirect_uri=ENTRA_REDIRECT_URI,
    )

    if "error" in result:
        return f"Token error: {result.get('error_description', result['error'])}", 403

    # Store user info in session
    session["user"] = result.get("id_token_claims", {})
    session.pop("auth_state", None)

    return redirect(url_for("index"))


@app.route("/logout")
def logout_route():
    """Clear session and redirect to Microsoft's logout endpoint."""
    session.clear()
    if AUTH_DISABLED:
        return redirect(url_for("login"))
    post_logout = request.url_root.rstrip("/") + "/"
    logout_url = (
        f"{ENTRA_AUTHORITY}/oauth2/v2.0/logout"
        f"?post_logout_redirect_uri={post_logout}"
    )
    return redirect(logout_url)


if __name__ == "__main__":
    app.run(debug=True, port=5000)

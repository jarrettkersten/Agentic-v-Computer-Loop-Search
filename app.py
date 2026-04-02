"""
Loop Comparison App — Cora ADO Edition
Compares two search approaches for querying the CoraSystems/PPM Azure DevOps repository:
  1. Agentic Loop  — Claude iteratively drives search strategy via tool calls
                     (ado_code_search + ado_read_file), deciding WHAT to look for next
                     mimicking a human navigating the ADO web interface

ALL answers come EXCLUSIVELY from the Cora PPM ADO repository.
No external sources. No general knowledge fallback.
"""

import os
import sys
import json
import asyncio
import base64
import io
import csv
import sqlite3
import time
import threading
import uuid
import queue
import secrets
import functools
import urllib.parse
import requests
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, Response, stream_with_context, redirect, url_for, session
import anthropic
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
load_dotenv()

# Playwright on Windows requires ProactorEventLoop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL             = "claude-sonnet-4-6"

ADO_ORG           = "CoraSystems"
ADO_PROJECT       = "PPM"
ADO_REPO          = "ppm"
ADO_BASE_URL      = f"https://dev.azure.com/{ADO_ORG}"
ADO_SEARCH_URL    = (
    f"https://almsearch.dev.azure.com/{ADO_ORG}/{ADO_PROJECT}"
    f"/_apis/search/codesearchresults?api-version=7.0"
)

# Cost estimation — USD per million tokens (update via env vars if pricing changes)
COST_PER_M_INPUT  = float(os.environ.get("COST_PER_M_INPUT",  "3.00"))   # Sonnet 4.6 input
COST_PER_M_OUTPUT = float(os.environ.get("COST_PER_M_OUTPUT", "15.00"))  # Sonnet 4.6 output

MAX_AGENTIC_ITERATIONS = 12    # more headroom for complex multi-file questions
MAX_SEARCH_RESULTS     = 12    # broader initial candidate pool per search term
MAX_FILE_CHARS         = 15000 # read more of each file — key methods are often deep
MAX_FILES_TO_READ      = 18    # read more files before synthesising

# Time-based search control ─────────────────────────────────────────────────
# Agentic loop pauses at these elapsed-second marks and asks the user whether
# to continue. Total hard stop is at SEARCH_HARD_LIMIT_SEC.
SEARCH_PAUSE_FIRST_SEC  = 300   # 5 min  — first "continue?" prompt
SEARCH_PAUSE_REPEAT_SEC = 120   # 2 min  — subsequent prompts
SEARCH_HARD_LIMIT_SEC   = 660   # 11 min — hard stop regardless

# ---------------------------------------------------------------------------
# Detect optional libraries at import time so we give clear errors if missing
try:
    import msal as _msal_mod
    MSAL_AVAILABLE = True
except ImportError:
    MSAL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Entra ID (Azure AD) authentication config
# ---------------------------------------------------------------------------
ENTRA_CLIENT_ID     = os.environ.get("ENTRA_CLIENT_ID", "")
ENTRA_CLIENT_SECRET = os.environ.get("ENTRA_CLIENT_SECRET", "")
ENTRA_TENANT_ID     = os.environ.get("ENTRA_TENANT_ID", "")
ENTRA_REDIRECT_URI  = os.environ.get("ENTRA_REDIRECT_URI", "")
ENTRA_AUTHORITY      = f"https://login.microsoftonline.com/{ENTRA_TENANT_ID}" if ENTRA_TENANT_ID else ""
ENTRA_SCOPES         = ["User.Read"]

# Toggle auth on/off (disabled if no client ID is configured)
AUTH_DISABLED = not ENTRA_CLIENT_ID

# Comma-separated list of admin email addresses
ADMIN_EMAILS = [e.strip().lower() for e in os.environ.get("ADMIN_EMAILS", "").split(",") if e.strip()]

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))

# ---------------------------------------------------------------------------
# Agentic search job store — supports mid-search "continue?" prompts via SSE
# ---------------------------------------------------------------------------
# Each running agentic job gets an entry:
#   job_id -> {
#     "event_queue":  queue.Queue   — server pushes SSE events here
#     "continue_evt": threading.Event — frontend signals "continue" by setting this
#     "stop_evt":     threading.Event — frontend signals "stop" by setting this
#   }
_agentic_jobs: dict = {}
_agentic_jobs_lock  = threading.Lock()

def _job_create(job_id: str):
    with _agentic_jobs_lock:
        _agentic_jobs[job_id] = {
            "event_queue":  queue.Queue(),
            "continue_evt": threading.Event(),
            "stop_evt":     threading.Event(),
        }

def _job_get(job_id: str):
    with _agentic_jobs_lock:
        return _agentic_jobs.get(job_id)

def _job_remove(job_id: str):
    with _agentic_jobs_lock:
        _agentic_jobs.pop(job_id, None)

# ---------------------------------------------------------------------------
# Usage tracking — SQLite
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.environ.get("DB_DIR", os.path.dirname(os.path.abspath(__file__))), "usage.db")
print(f"[db] DB_DIR={os.environ.get('DB_DIR','(not set — using app dir)')}  DB_PATH={DB_PATH}", flush=True)


def init_db():
    """Create usage tracking tables if they don't exist, and migrate token columns."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS search_events (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at           TEXT    NOT NULL DEFAULT (datetime('now')),
                loop_type        TEXT    NOT NULL,
                query            TEXT,
                branch           TEXT,
                status           TEXT    NOT NULL DEFAULT 'success',
                duration_sec     REAL,
                files_read       INTEGER DEFAULT 0,
                iterations       INTEGER DEFAULT 0,
                searches         INTEGER DEFAULT 0,
                confidence_level TEXT,
                input_tokens     INTEGER DEFAULT 0,
                output_tokens    INTEGER DEFAULT 0,
                total_tokens     INTEGER DEFAULT 0,
                error_message    TEXT,
                user_email       TEXT    DEFAULT ''
            )
        """)
        # Migrate existing tables: add token columns if they don't already exist
        for table in ("search_events",):
            for col_def in (
                "input_tokens  INTEGER DEFAULT 0",
                "output_tokens INTEGER DEFAULT 0",
                "total_tokens  INTEGER DEFAULT 0",
            ):
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
                except Exception:
                    pass  # column already exists
        # Migrate: add user_email column to search_events
        try:
            cur.execute("ALTER TABLE search_events ADD COLUMN user_email TEXT DEFAULT ''")
        except Exception:
            pass  # column already exists
        # Migrate: add new flag columns to flagged_queries
        for col_def in (
            "flag_type TEXT NOT NULL DEFAULT 'inaccurate'",
            "container TEXT DEFAULT ''",
            "request_article INTEGER DEFAULT 0",
            "branch TEXT DEFAULT ''",
            "confidence_level TEXT DEFAULT ''",
            "duration_sec REAL DEFAULT 0",
            "input_tokens INTEGER DEFAULT 0",
            "output_tokens INTEGER DEFAULT 0",
            "total_tokens INTEGER DEFAULT 0",
            "files_read INTEGER DEFAULT 0",
            "iterations INTEGER DEFAULT 0",
            "searches INTEGER DEFAULT 0",
        ):
            try:
                cur.execute(f"ALTER TABLE flagged_queries ADD COLUMN {col_def}")
            except Exception:
                pass
        # API jobs table — async query processing
        cur.execute("""
            CREATE TABLE IF NOT EXISTS api_jobs (
                id           TEXT    PRIMARY KEY,
                status       TEXT    NOT NULL DEFAULT 'pending',
                query        TEXT    NOT NULL,
                branch       TEXT,
                user_email   TEXT    DEFAULT 'external_api',
                result_json  TEXT,
                error        TEXT,
                created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
                completed_at TEXT
            )
        """)
        # Flagged queries table — stores responses users flag as unhelpful/missing
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flagged_queries (
                id                TEXT PRIMARY KEY,
                query             TEXT NOT NULL,
                loop_type         TEXT NOT NULL,
                answer            TEXT,
                explanation       TEXT,
                flagged_by        TEXT,
                flagged_at        TEXT NOT NULL DEFAULT (datetime('now')),
                status            TEXT NOT NULL DEFAULT 'pending',
                reviewed_by       TEXT,
                reviewed_at       TEXT,
                admin_notes       TEXT,
                flag_type         TEXT NOT NULL DEFAULT 'inaccurate',
                container         TEXT DEFAULT '',
                request_article   INTEGER DEFAULT 0,
                branch            TEXT DEFAULT '',
                confidence_level  TEXT DEFAULT '',
                duration_sec      REAL DEFAULT 0,
                input_tokens      INTEGER DEFAULT 0,
                output_tokens     INTEGER DEFAULT 0,
                total_tokens      INTEGER DEFAULT 0,
                files_read        INTEGER DEFAULT 0,
                iterations        INTEGER DEFAULT 0,
                searches          INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Warning: could not init tracking tables: {e}")


def track_search_event(loop_type: str, query: str, branch: str, status: str,
                       duration_sec: float, files_read: int = 0,
                       iterations: int = 0, searches: int = 0,
                       confidence_level: str = "", error_message: str = "",
                       input_tokens: int = 0, output_tokens: int = 0,
                       user_email: str = ""):
    """Record a single loop search run."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """INSERT INTO search_events
               (loop_type, query, branch, status, duration_sec, files_read,
                iterations, searches, confidence_level,
                input_tokens, output_tokens, total_tokens, error_message,
                user_email)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (loop_type, (query or "")[:300], branch or "", status,
             round(duration_sec, 2), files_read, iterations, searches,
             confidence_level or "",
             input_tokens, output_tokens, input_tokens + output_tokens,
             error_message or "", user_email or ""),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Warning: could not track search event: {e}")



def query_db(sql: str) -> list:
    """Run a SELECT and return rows as list of dicts."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        cur.execute(sql)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        print(f"[DB] Warning: query failed: {e}")
        return []


# Initialise tables on startup
init_db()


# ---------------------------------------------------------------------------
# Entra ID auth helpers
# ---------------------------------------------------------------------------

def _build_msal_app():
    return _msal_mod.ConfidentialClientApplication(
        ENTRA_CLIENT_ID,
        authority=ENTRA_AUTHORITY,
        client_credential=ENTRA_CLIENT_SECRET,
    )


def _current_user_email():
    if AUTH_DISABLED:
        return "anonymous"
    return session.get("user", {}).get("mail", session.get("user", {}).get("userPrincipalName", ""))


def _is_admin():
    if AUTH_DISABLED:
        return True
    email = _current_user_email().lower()
    return email in ADMIN_EMAILS


def login_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if AUTH_DISABLED:
            return f(*args, **kwargs)
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if AUTH_DISABLED:
            return f(*args, **kwargs)
        if "user" not in session:
            return redirect(url_for("login"))
        if not _is_admin():
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# ADO helpers
# ---------------------------------------------------------------------------

def _ado_headers() -> dict:
    pat   = os.environ.get("AZURE_DEVOPS_PAT", "")
    token = base64.b64encode(f":{pat}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
    }


def ado_get_branches() -> list:
    """Return the branches that are actually indexed by ADO Code Search.

    The Code Search facets API caps returned facet values at ~10 per call,
    ordered by result count.  Newly-added branches with fewer indexed hits
    may fall below that cap and not appear.

    Fix: query with several common code tokens and union all branch names
    across the responses.  Falls back to the Git Refs API if every facets
    call fails.

    Display order: supported_release-* newest-first, then everything else.
    """
    import re as _re

    def _sort_key(b):
        # supported_release-X.Y.Z  → bucket 0, sorted newest-first (descending)
        # anything else             → bucket 1, sorted alphabetically
        m = _re.search(r"(\d+)\.(\d+)\.(\d+)", b)
        if b.lower().startswith("supported_release") and m:
            return (0, -int(m.group(1)), -int(m.group(2)), -int(m.group(3)), b)
        return (1, 0, 0, 0, b)

    # ── Primary: union facets from several search terms ───────────────────────
    # Different terms hit different files/branches; unioning covers branches
    # that might be below the per-call facet cap for any single term.
    _PROBE_TERMS = ["class", "public", "return", "void", "function"]
    all_branches: set = set()
    any_facets_ok = False
    for term in _PROBE_TERMS:
        try:
            body = {
                "searchText": term,
                "$skip": 0,
                "$top": 1,
                "filters": {
                    "Project":    [ADO_PROJECT],
                    "Repository": [ADO_REPO],
                },
                "includeFacets": True,
            }
            resp = requests.post(ADO_SEARCH_URL, headers=_ado_headers(), json=body, timeout=15)
            if resp.ok:
                facets = resp.json().get("facets", {})
                for entry in facets.get("Branch", []):
                    if entry.get("name"):
                        all_branches.add(entry["name"])
                        any_facets_ok = True
        except Exception as e:
            print(f"[branches] Facets probe '{term}' failed: {e}")

    if all_branches:
        print(f"[branches] Facets collected {len(all_branches)} branches: {sorted(all_branches)}")
        return sorted(all_branches, key=_sort_key)

    if not any_facets_ok:
        print("[branches] All facets calls failed — falling back to Git Refs API")

    # ── Fallback: Git Refs API filtered to supported_release branches ─────────
    BRANCH_PATTERN = _re.compile(r"^supported_release-\d+\.\d+\.\d+$")
    url = (
        f"{ADO_BASE_URL}/{ADO_PROJECT}/_apis/git/repositories/{ADO_REPO}"
        f"/refs?filter=heads/supported_release&api-version=7.0"
    )
    resp = requests.get(url, headers=_ado_headers(), timeout=10)
    resp.raise_for_status()
    branches = []
    for ref in resp.json().get("value", []):
        name = ref.get("name", "")
        if name.startswith("refs/heads/"):
            short = name[len("refs/heads/"):]
            if BRANCH_PATTERN.match(short):
                branches.append(short)
    return sorted(branches, key=_sort_key)


def ado_code_search(query, branch, top=None):
    """Search the Cora PPM ADO codebase and return matching file metadata.

    Now that the admin has enabled specific branches for Code Search, we pass
    the branch filter directly so results come from exactly the branch the user
    selected — no more searching the default branch and re-reading files from
    a different branch.

    ADO Code Search API requires $skip/$top (dollar-sign prefix).
    """
    if top is None:
        top = MAX_SEARCH_RESULTS

    filters: dict = {
        "Project":    [ADO_PROJECT],
        "Repository": [ADO_REPO],
    }
    # Pass the branch filter so results come from the selected branch directly.
    # If for any reason the branch isn't indexed, ADO falls back to results
    # from any indexed branch — callers still read files using the explicit
    # branch, so correctness is preserved either way.
    if branch:
        filters["Branch"] = [branch]

    body = {
        "searchText": query,
        "$skip": 0,
        "$top": top,
        "filters": filters,
    }

    resp = requests.post(ADO_SEARCH_URL, headers=_ado_headers(), json=body, timeout=15)

    # Raise with full ADO error body so we can diagnose failures
    if not resp.ok:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text[:500]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} — ADO detail: {detail}",
            response=resp,
        )

    results = []
    for r in resp.json().get("results", [])[:top]:
        entry = {
            "file_path":  r.get("path", ""),
            "file_name":  r.get("fileName", ""),
            "repository": r.get("repository", {}).get("name", ""),
        }
        # Extract content snippets from ADO's hits array.
        # Each hit contains the matching lines of code at a specific char offset.
        # Including these means callers can see matching code from ANY file — even
        # large files that would be truncated during a full read — without an extra
        # API call.  We keep the top 3 snippets per file, trimming whitespace.
        snippets = []
        for hit in r.get("hits", []):
            text = hit.get("content", "").strip()
            offset = hit.get("charOffset", 0)
            if text and len(snippets) < 3:
                snippets.append({"offset": offset, "content": text})
        if snippets:
            entry["snippets"] = snippets
        results.append(entry)
    return results


def ado_read_file(file_path, branch, char_offset=0):
    """Read a file from the Cora PPM ADO repo.

    Returns the FULL file content starting from char_offset.  No truncation —
    the entire file (from the offset onward) is returned so the model never
    misses methods or classes defined deeper in the file.
    """
    url = (
        f"{ADO_BASE_URL}/{ADO_PROJECT}/_apis/git/repositories/{ADO_REPO}/items"
        f"?path={requests.utils.quote(file_path)}"
        f"&versionDescriptor.version={branch}"
        f"&versionDescriptor.versionType=branch"
        f"&$format=text"
        f"&api-version=7.0"
    )
    resp = requests.get(url, headers=_ado_headers(), timeout=15)
    if not resp.ok:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text[:500]
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} — ADO detail: {detail}",
            response=resp,
        )
    full_content = resp.text
    total_len    = len(full_content)

    # Return everything from the requested offset onward — no chunking.
    content = full_content[char_offset:]

    if not content:
        return f"[No content at offset {char_offset:,} — file is {total_len:,} chars total.]"

    # Append a size footer so the model (and logs) know how much was returned.
    content += f"\n\n[Full file returned — {len(content):,} chars"
    if char_offset > 0:
        content += f" (from offset {char_offset:,})"
    content += f" of {total_len:,} total.]"

    return content


def get_client():
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY is not set.")
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)




def _clean_answer(text: str) -> str:
    """Sanitise the user-facing answer by stripping internal directives and
    raw Python data structures that should never appear in output.

    Handles:
      - MISSING_FILE: directives (internal fetch signals)
      - Raw Python dict / list repr leaking from iterations_log formatting
        e.g.  {'type': 'read_file', 'file_path': '...', 'content_length': ...}
    """
    import re

    # ── Strip MISSING_FILE: lines ─────────────────────────────────────────────
    missing_files = [
        line.replace("MISSING_FILE:", "").strip()
        for line in text.splitlines()
        if "MISSING_FILE:" in line
    ]
    cleaned = re.sub(r"^MISSING_FILE:.*$\n?", "", text, flags=re.MULTILINE).strip()
    cleaned = re.sub(
        r"(?m)^(#+\s*Missing Files.*|What You Actually Need.*)\n+$", "", cleaned
    ).strip()

    # ── Strip raw Python dict/list tool-call representations ─────────────────
    # Matches lines like: {'type': 'read_file', 'file_path': '...', ...}
    cleaned = re.sub(
        r"^\{['\"]type['\"]\s*:.*\}\s*$\n?",
        "",
        cleaned,
        flags=re.MULTILINE,
    ).strip()
    # Also strip standalone lines that look like dict key-value pairs leaked
    # from str(iterations_log) formatting
    cleaned = re.sub(
        r"^['\"](?:type|file_path|content_length|query|results|preview)['\"].*$\n?",
        "",
        cleaned,
        flags=re.MULTILINE,
    ).strip()

    # ── Add footnote if files were flagged as missing ─────────────────────────
    if missing_files:
        names = ", ".join(f"`{p.rsplit('/', 1)[-1]}`" for p in missing_files[:5])
        suffix = "…" if len(missing_files) > 5 else ""
        cleaned += (
            f"\n\n---\n> **Note:** A more complete answer may require additional "
            f"files that could not be located in the searched branch: {names}{suffix}"
        )
    return cleaned


def assess_confidence(query: str, answer: str, files_read: int, loop_name: str) -> dict:
    """Quick confidence assessment for a loop answer.
    Returns {"level": "High"|"Medium"|"Low", "reason": "one sentence"}
    """
    import json as _json
    client = get_client()
    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=120,
            system=(
                "You evaluate the confidence level of Cora PPM search answers. "
                "Respond ONLY with a valid JSON object, no extra text:\n"
                '{"level": "High", "reason": "One sentence."}\n'
                "Levels:\n"
                "  High   — multiple relevant files read, answer covers all key layers\n"
                "  Medium — some relevant files found, coverage may be incomplete\n"
                "  Low    — limited or no matching files; answer is partial or inferred"
            ),
            messages=[{
                "role": "user",
                "content": (
                    f"Search method: {loop_name}\n"
                    f"Files read: {files_read}\n"
                    f"Question: {query}\n\n"
                    f"Answer preview (first 400 chars):\n{answer[:400]}\n\n"
                    "Rate the confidence in this answer."
                ),
            }],
        )
        text = resp.content[0].text.strip()
        # Strip markdown fences if present
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        parsed = _json.loads(text)
        # Normalise level capitalisation
        parsed["level"] = parsed.get("level", "Medium").capitalize()
        return parsed
    except Exception:
        return {"level": "Medium", "reason": "Confidence could not be assessed automatically."}


def extract_file_paths_from_text(text: str) -> list:
    """Parse a synthesis response for any ADO-style file paths Claude mentioned.

    Looks for patterns like /code/inetpub/..., /src/..., or Helpers/Foo.cs
    so the agentic loop can fetch files Claude said it needed but didn't have.
    """
    import re
    # Match Unix-style paths that end with a known code extension
    pattern = re.compile(
        r'(?:^|[\s`\'"])(/[\w./ -]+\.(?:cs|vb|aspx|ascx|js|ts|cshtml|vbhtml|sql|config|xml|json))',
        re.MULTILINE | re.IGNORECASE,
    )
    paths = []
    seen  = set()
    for m in pattern.finditer(text):
        p = m.group(1).strip()
        if p not in seen:
            seen.add(p)
            paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# 1. AGENTIC LOOP
#    Claude iteratively decides WHAT to search for and WHICH files to read
#    using two ADO-specific tools, until it has enough context to answer.
# ---------------------------------------------------------------------------

def run_agentic_loop(query, branch, job_id: str | None = None):
    client = get_client()

    tools = [
        {
            "name": "ado_code_search",
            "description": (
                "Search the Cora PPM Azure DevOps repository for relevant code, "
                "files, or implementations. Returns matching file paths AND content "
                f"snippets (the actual matching lines of code) from the {ADO_REPO} repository. "
                "The snippets show the exact code at the match location — use them to "
                "quickly inspect code from large files without reading the full file. "
                "Call this multiple times with different short technical search terms "
                "(class names, method names, feature names, 1-3 words) to find all relevant files. "
                "NEVER use the full user question as the search term — it returns nothing."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "search_query": {
                        "type": "string",
                        "description": (
                            "Search terms to find relevant code or documentation "
                            "in the Cora PPM ADO repository."
                        ),
                    }
                },
                "required": ["search_query"],
            },
        },
        {
            "name": "ado_read_file",
            "description": (
                "Read the content of a specific file from the Cora PPM ADO repository. "
                "Use file paths returned by ado_code_search. "
                "Returns the FULL file content — no truncation. "
                "You will always receive the complete file in a single call."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": (
                            "Full path of the file in the repository, "
                            "e.g. /src/Features/Forecasting/EACSubmit.cs"
                        ),
                    },
                    "char_offset": {
                        "type": "integer",
                        "description": (
                            "Character offset to start reading from. "
                            "Omit or use 0 to read from the beginning."
                        ),
                    },
                },
                "required": ["file_path"],
            },
        },
    ]

    system_prompt = (
        f"You are a senior Cora PPM code analyst. Your job is to give COMPLETE, thorough answers "
        f"about the Cora PPM codebase — matching the quality of a developer who has read every file.\n\n"
        f"REPOSITORY: org={ADO_ORG}, project={ADO_PROJECT}, repo={ADO_REPO}, branch={branch}\n\n"
        "═══ SEARCHING STRATEGY ═══\n"
        "ADO Code Search does NOT understand natural language — long phrases return ZERO results.\n"
        "Use SHORT (1-3 word) technical terms. Search in LAYERS:\n"
        "  Layer 1 — The UI/page file: e.g. 'TimesheetGrid', 'EACSubmit', 'ForecastApproval'\n"
        "  Layer 2 — The short keyword: e.g. 'Timesheet', 'EAC', 'Forecast'\n"
        "  Layer 3 — The service/repo: e.g. 'ITimesheetService', 'TimesheetRepository'\n"
        "  Layer 4 — The controller/codebehind: e.g. 'TimesheetController', 'Timesheet.aspx'\n"
        "  Layer 5 — Alternate casing or legacy name: e.g. 'TimeSheet' if 'Timesheet' returned nothing\n"
        "Run AT LEAST 4 different search terms before concluding you have enough files.\n"
        "If a search returns 0 results, immediately try a shorter or differently-cased variant.\n\n"
        "═══ READING STRATEGY ═══\n"
        "Read files in this priority order and DO NOT stop after 2-3 files:\n"
        "  1. The .aspx / .ascx page or control file that renders the UI\n"
        "  2. The .aspx.vb or .aspx.cs code-behind that handles events\n"
        "  3. The service or business logic class (IXxxService, XxxService)\n"
        "  4. The repository or data-access class (XxxRepository, XxxDA)\n"
        "  5. Any helpers, utilities, or shared controls it calls\n"
        "  6. Any SQL stored procedures or queries referenced\n"
        "Read ALL layers — an answer that only covers the UI without the business logic is incomplete.\n\n"
        "═══ FILE READS — NO TRUNCATION ═══\n"
        "ado_read_file returns the FULL file content in one call — there is no truncation.\n"
        "You will always receive the complete file, so every method and class definition\n"
        "will be visible. Do NOT re-read the same file unless you need a different offset.\n\n"
        "═══ ANSWER REQUIREMENTS ═══\n"
        "Your answer MUST use EXACTLY this three-section structure with these EXACT headings:\n\n"
        "## Overview\n"
        "Write a complete, professional, authoritative plain-English explanation for the end user. "
        "Use plain language — no file paths, no code snippets, no method names. "
        "Explain what the feature/function does, why it exists, and how it works from the "
        "user's perspective. A non-technical stakeholder should fully understand this section.\n\n"
        "---\n\n"
        "## User Guide\n"
        "Provide a guided, step-by-step response that helps the user interact with or configure "
        "the feature. Include:\n"
        "  - Where to find the feature in the Cora PPM UI (navigation path)\n"
        "  - Step-by-step instructions for common tasks related to this feature\n"
        "  - Any important settings, options, or prerequisites the user should know about\n"
        "  - Tips, best practices, or common pitfalls to avoid\n"
        "Write as if you are guiding a user through the feature. Keep language clear and actionable.\n\n"
        "---\n\n"
        "## Technical Reference\n"
        "*For dev/product team review*\n\n"
        "Full technical detail for developers to verify:\n"
        "  - Complete execution flow from the ADO source (every method, file, layer in order).\n"
        "    After EACH step, add an inline annotation showing which Overview claim it explains:\n"
        "    e.g. `TimesheetService.SaveTimesheet()` in `TimesheetService.vb` (line 87) → *Supports: \"data is saved via a service layer\"*\n"
        "  - Key code snippets in fenced code blocks. Each snippet MUST be preceded by:\n"
        "    `File: path/to/File.ext, lines N–M` and annotated with which Overview claim it supports.\n"
        "  - File inventory with each file's role (UI / CodeBehind / Service / Repository / Helper / SQL) "
        "and which Overview statement(s) it backs.\n"
        "  - ⚠️ CONDITIONAL LOGIC & FEATURE TOGGLES: Explicitly call out ANY conditional branches,\n"
        "    feature flags, config-driven paths, or permission checks that affect the behaviour\n"
        "    described in the Overview. Format each as:\n"
        "    > ⚠️ **Conditional:** `If SomeFlag = True` in `File.ext` (line N) — affects: \"[Overview claim]\"\n"
        "  - Any gaps or missing layers identified during the search.\n\n"
        "═══ DISAMBIGUATION — SAME NAME, DIFFERENT UI ═══\n"
        "Cora PPM frequently reuses the same button label, feature name, or function name across "
        "DIFFERENT pages/UIs (e.g. 'Load From Schedule' exists on both the Resource Allocation page "
        "AND the Project Forecasting page, doing completely different things). When you encounter this:\n"
        "  1. SEARCH PHASE: If you discover that a feature name maps to multiple .aspx pages or "
        "     user controls, you MUST search and read EACH page's implementation separately.\n"
        "  2. ANSWER PHASE: NEVER merge or conflate implementations from different pages into one "
        "     explanation. Describe each page's version separately with clear headings:\n"
        "     e.g. '### On the Resource Allocation page' vs '### On the Project Forecasting page'\n"
        "  3. If the user's question specifies a particular page/UI, focus on that one but note "
        "     that the same feature name also exists elsewhere.\n"
        "  4. If the user's question does NOT specify which page, cover ALL pages where the feature "
        "     exists and clearly separate the explanations.\n\n"
        "NEVER use general knowledge. Only answer from files you have read. "
        "If you cannot find a file you need, say which file and why it matters, then continue "
        "with what you have rather than stopping early.\n\n"
        "═══ WHEN TO STOP SEARCHING ═══\n"
        "Do NOT stop searching until you have covered ALL of these layers:\n"
        "  ✓ The .aspx/.ascx page that renders the UI\n"
        "  ✓ The .aspx.vb/.aspx.cs code-behind that handles events\n"
        "  ✓ The service or business-logic class (IXxxService / XxxService)\n"
        "  ✓ The repository or data-access class (XxxRepository / XxxDA / XxxManager)\n"
        "  ✓ Any SQL stored procedures or queries called from the repository\n"
        "  ✓ Key helper / utility classes that the above files delegate to\n"
        "  ✓ Any base classes or shared controls that define important inherited behaviour\n\n"
        "You are NOT done until every layer above is checked. If a layer's file could not be "
        "found (e.g. no stored proc exists), note that explicitly and move on — do not stop early.\n\n"
        "AVOID reading: pure CSS/JS styling files, completely unrelated features, or files you "
        "have already read in full. Skip CSS/layout-only .ascx controls unless the question is "
        "specifically about layout.\n\n"
        "TARGET DEPTH: For most questions a thorough answer requires 12–20 files. If you have "
        "fewer than 10 files read and the question touches business logic, keep searching."
    )

    messages = [{"role": "user", "content": (
        f"Research and answer this question about the Cora PPM codebase.\n\n"
        f"Question: {query}\n\n"
        f"Remember: extract the KEY TECHNICAL TERMS first, then search with those short terms."
    )}]
    iterations_log      = []
    iteration           = 0
    final_answer        = ""
    total_input_tokens  = 0
    total_output_tokens = 0
    # Track full file contents separately so fallback synthesis has real context,
    # not just metadata dicts from the iterations log.
    accumulated_files = {}   # file_path -> full content string

    # Time-based control: hard stop and user-confirm thresholds
    loop_start_time     = time.time()
    next_pause_at       = loop_start_time + SEARCH_PAUSE_FIRST_SEC
    job                 = _job_get(job_id) if job_id else None

    while True:
        iteration += 1

        # ── Iteration hard cap ────────────────────────────────────────────────
        if iteration > 25:
            messages.append({"role": "user", "content": (
                "SYSTEM NOTE: You have reached the 25-iteration limit. "
                "Write your complete answer NOW using every file you have already read. "
                "Do not make any more tool calls."
            )})
            break

        # ── Time-based hard stop ──────────────────────────────────────────────
        elapsed = time.time() - loop_start_time
        if elapsed >= SEARCH_HARD_LIMIT_SEC:
            messages.append({"role": "user", "content": (
                "SYSTEM NOTE: The search has reached its 11-minute hard limit. "
                "Write your complete answer NOW using every file you have already read. "
                "Do not make any more tool calls."
            )})
            break

        # ── Pause threshold: ask user if they want to continue ────────────────
        if elapsed >= next_pause_at - loop_start_time and job:
            # Signal the frontend via SSE
            minutes_so_far = int(elapsed // 60)
            files_so_far   = len(accumulated_files)
            job["event_queue"].put({
                "type":    "pause_prompt",
                "elapsed": int(elapsed),
                "files":   files_so_far,
                "message": (
                    f"Search is still running ({minutes_so_far} min, {files_so_far} files read). "
                    "Continue searching for more files?"
                ),
            })
            # Wait up to 90 s for either a continue or stop signal from the frontend
            continue_evt = job["continue_evt"]
            stop_evt     = job["stop_evt"]
            continue_evt.clear()
            stop_evt.clear()
            deadline = time.time() + 90
            while time.time() < deadline:
                if stop_evt.is_set():
                    # User chose to stop — synthesise with what we have
                    messages.append({"role": "user", "content": (
                        "SYSTEM NOTE: The user has requested the search stop now. "
                        "Write your complete answer using every file you have already read. "
                        "Do not make any more tool calls."
                    )})
                    break
                if continue_evt.is_set():
                    next_pause_at = time.time() + SEARCH_PAUSE_REPEAT_SEC
                    break
                time.sleep(0.5)
            else:
                # Timeout waiting for user response — default to continue
                next_pause_at = time.time() + SEARCH_PAUSE_REPEAT_SEC
            if stop_evt.is_set():
                break

        response = client.messages.create(
            model=MODEL,
            max_tokens=5000,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )
        total_input_tokens  += getattr(getattr(response, "usage", None), "input_tokens",  0)
        total_output_tokens += getattr(getattr(response, "usage", None), "output_tokens", 0)

        tool_uses = [b for b in response.content if b.type == "tool_use"]

        if not tool_uses:
            final_answer = _clean_answer(" ".join(
                b.text for b in response.content if hasattr(b, "text")
            ).strip())
            iterations_log.append({
                "iteration":  iteration,
                "action":     "Claude concluded — generating answer from gathered ADO context.",
                "tool_calls": [],
            })
            break

        tool_calls_this_turn = []
        tool_results         = []

        for tu in tool_uses:
            if tu.name == "ado_code_search":
                sq = tu.input.get("search_query", query)
                try:
                    results = ado_code_search(sq, branch)
                    result_content = json.dumps(results)
                    tool_calls_this_turn.append({
                        "type":    "search",
                        "query":   sq,
                        "results": results,
                    })
                except Exception as e:
                    result_content = json.dumps({"error": str(e)})
                    tool_calls_this_turn.append({"type": "search", "query": sq, "error": str(e)})

            elif tu.name == "ado_read_file":
                fp          = tu.input.get("file_path", "")
                char_offset = int(tu.input.get("char_offset") or 0)
                try:
                    content = ado_read_file(fp, branch, char_offset=char_offset)
                    result_content = content
                    # Continuation reads append to the existing entry so the
                    # full reconstructed file is available for synthesis.
                    if char_offset > 0 and fp in accumulated_files:
                        accumulated_files[fp] += f"\n\n[...continuing from offset {char_offset:,}...]\n\n" + content
                    else:
                        accumulated_files[fp] = content
                    tool_calls_this_turn.append({
                        "type":           "read_file",
                        "file_path":      fp,
                        "char_offset":    char_offset,
                        "content_length": len(content),
                        "preview":        content[:200] + "..." if len(content) > 200 else content,
                    })
                except Exception as e:
                    result_content = "Error reading file: " + str(e)
                    tool_calls_this_turn.append({"type": "read_file", "file_path": fp, "error": str(e)})

            else:
                result_content = "Unknown tool."

            tool_results.append({
                "type":        "tool_result",
                "tool_use_id": tu.id,
                "content":     result_content,
            })

        iterations_log.append({
            "iteration":  iteration,
            "action":     "Made " + str(len(tool_uses)) + " ADO tool call(s).",
            "tool_calls": tool_calls_this_turn,
        })

        # Emit a progress event so the frontend can show live step feedback
        if job:
            _prog_parts = []
            for _tc in tool_calls_this_turn:
                if _tc.get("type") == "search":
                    _prog_parts.append(f"🔎 Searching '{_tc.get('query', '')}'")
                elif _tc.get("type") == "read_file":
                    _fname = _tc.get("file_path", "").rsplit("/", 1)[-1]
                    _off   = _tc.get("char_offset", 0)
                    _prog_parts.append(
                        f"📖 Reading {_fname}" + (f" (+{_off:,})" if _off else "")
                    )
            _searches_so_far = sum(
                1 for _it in iterations_log
                for _tc2 in _it.get("tool_calls", [])
                if _tc2.get("type") == "search"
            ) + sum(1 for _tc2 in tool_calls_this_turn if _tc2.get("type") == "search")
            job["event_queue"].put({
                "type":      "progress",
                "iteration": iteration,
                "files":     len(accumulated_files),
                "searches":  _searches_so_far,
                "text":      " · ".join(_prog_parts) if _prog_parts else f"Iteration {iteration}",
            })

        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user",      "content": tool_results})

        if response.stop_reason == "end_turn":
            text = " ".join(b.text for b in response.content if hasattr(b, "text")).strip()
            if text:
                final_answer = _clean_answer(text)
            break

    # ── Fallback synthesis if iteration cap hit without final text ────────────
    # Build context from ACTUAL file contents (not metadata dicts) so Claude
    # can write a real answer rather than regurgitating internal tool call data.
    if not final_answer:
        if accumulated_files:
            context = "\n\n".join(
                f"### File: {path}\n{content}"
                for path, content in accumulated_files.items()
            )
        else:
            context = (
                "No files were successfully read from the repository. "
                "The searches ran but returned no accessible content."
            )
        fallback = client.messages.create(
            model=MODEL,
            max_tokens=10000,
            system=(
                "You are a senior Cora PPM code analyst. Answer ONLY from the ADO repository "
                "context provided — never from general knowledge.\n\n"
                "Your answer MUST use EXACTLY this three-section structure:\n\n"
                "## Overview\n"
                "Write a complete, professional, authoritative plain-English explanation for the end user. "
                "Use plain language — no file paths, no code snippets, no method names. "
                "Explain what the feature/function does, why it exists, and how it works from the "
                "user's perspective. A non-technical stakeholder should fully understand this section.\n\n"
                "---\n\n"
                "## User Guide\n"
                "Provide guided, step-by-step instructions for using or configuring the feature. "
                "Include navigation paths, prerequisites, tips, and common pitfalls.\n\n"
                "---\n\n"
                "## Technical Reference\n"
                "*For dev/product team review*\n\n"
                "Full technical detail for developers to verify:\n"
                "  - Complete execution flow (every method, file, layer in order) with inline annotations.\n"
                "  - Key code snippets in fenced code blocks with file paths.\n"
                "  - File inventory with each file's role and which Overview statement(s) it backs.\n"
                "  - Any conditional logic, feature flags, or permission checks.\n"
                "  - Any gaps or missing layers identified.\n\n"
                "DISAMBIGUATION: If the provided context contains implementations of the same feature "
                "name (button, function) from DIFFERENT pages or UIs, you MUST describe each separately "
                "with clear headings (e.g. '### On the Resource Allocation page' vs '### On the Project "
                "Forecasting page'). NEVER merge them into one explanation.\n\n"
                "If context is insufficient to cover all layers, state clearly which layer is missing "
                "and what additional files would be needed."
            ),
            messages=[{
                "role":    "user",
                "content": (
                    "Based ONLY on the following ADO repository context, answer in full detail:\n\n"
                    "Question: " + query + "\n\n"
                    "ADO context gathered:\n" + context
                ),
            }],
        )
        total_input_tokens  += getattr(getattr(fallback, "usage", None), "input_tokens",  0)
        total_output_tokens += getattr(getattr(fallback, "usage", None), "output_tokens", 0)
        final_answer = _clean_answer(fallback.content[0].text)

    total_searches   = sum(1 for it in iterations_log for tc in it.get("tool_calls", []) if tc.get("type") == "search")
    total_files_read = sum(1 for it in iterations_log for tc in it.get("tool_calls", []) if tc.get("type") == "read_file")

    confidence = assess_confidence(query, final_answer, total_files_read, "Agentic Loop")

    result = {
        "answer":           final_answer,
        "confidence":       confidence,
        "iterations":       iterations_log,
        "total_iterations": iteration,
        "total_searches":   total_searches,
        "total_files_read": total_files_read,
        "branch":           branch,
        "input_tokens":     total_input_tokens,
        "output_tokens":    total_output_tokens,
        "elapsed_seconds":  round(time.time() - loop_start_time, 1),
    }

    # Notify SSE stream that the job is done
    if job:
        job["event_queue"].put({"type": "done", "result": result})
    return result


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
                           auth_disabled=AUTH_DISABLED,
                           cost_per_m_input=COST_PER_M_INPUT,
                           cost_per_m_output=COST_PER_M_OUTPUT)


@app.route("/api-docs")
@login_required
def api_docs():
    """Interactive Swagger-style API documentation page."""
    user = session.get("user", {}) if not AUTH_DISABLED else {}
    return render_template("api_docs.html",
                           user=user,
                           is_admin=_is_admin(),
                           auth_disabled=AUTH_DISABLED)


@app.route("/api/extract-screenshot", methods=["POST"])
def api_extract_screenshot():
    """
    Receive a base64-encoded screenshot image, ask Claude to extract UI context
    (page name, visible fields, error messages, data shown), and return the
    context as a plain-text string to be appended to the user's query.
    """
    data      = request.get_json() or {}
    image_b64 = data.get("image_b64", "").strip()
    mime_type = data.get("mime_type", "image/png").strip()
    if not image_b64:
        return jsonify({"error": "No image data provided."}), 400
    try:
        client = get_client()
        resp   = client.messages.create(
            model=MODEL,
            max_tokens=600,
            system=(
                "You are a Cora PPM UI analyst. The user has uploaded a screenshot of the Cora PPM application. "
                "Extract ALL visible context that would help a developer understand what page/feature is shown. "
                "Respond ONLY with a concise plain-text summary (no markdown) covering:\n"
                "• Page name / module visible in the screenshot\n"
                "• Key field labels, button names, or column headers visible\n"
                "• Any error messages, warning banners, or validation messages\n"
                "• Any IDs, statuses, or data values that appear to be relevant\n"
                "• Any UI elements or controls that look directly related to the user's question\n"
                "Keep the response under 250 words. Do not include generic observations."
            ),
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type":       "base64",
                            "media_type": mime_type,
                            "data":       image_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": "Please extract the UI context from this Cora PPM screenshot.",
                    },
                ],
            }],
        )
        context = resp.content[0].text.strip()
        return jsonify({"success": True, "context": context})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/branches")
def api_branches():
    try:
        branches = ado_get_branches()
        return jsonify({"success": True, "branches": branches})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/test")
def api_test():
    """Diagnostic endpoint — tests ADO auth, branch listing, and a sample search."""
    results = {}

    # 1. Check env vars
    results["anthropic_key_set"] = bool(os.environ.get("ANTHROPIC_API_KEY"))
    results["ado_pat_set"]       = bool(os.environ.get("AZURE_DEVOPS_PAT"))

    # 2. Test branch listing
    try:
        branches = ado_get_branches()
        results["branches_ok"]    = True
        results["branch_count"]   = len(branches)
        results["branches_sample"] = branches[:3]
    except Exception as e:
        results["branches_ok"]    = False
        results["branches_error"] = str(e)

    # 3. Test a minimal code search against the first branch found
    test_branch = branches[0] if results.get("branches_ok") and branches else ""
    if test_branch:
        try:
            hits = ado_code_search("Forecasting", test_branch, top=2)
            results["search_ok"]      = True
            results["search_branch"]  = test_branch
            results["search_hits"]    = hits
        except Exception as e:
            results["search_ok"]      = False
            results["search_branch"]  = test_branch
            results["search_error"]   = str(e)
    else:
        results["search_ok"]    = False
        results["search_error"] = "No branch available to test against"

    return jsonify(results)


@app.route("/api/agentic/stream", methods=["POST"])
def api_agentic_stream():
    """
    SSE endpoint for the Agentic Loop.
    - Starts the loop in a background thread.
    - Streams progress events (pause_prompt, done, error) as SSE.
    - Returns a job_id in the first event so the frontend can call
      /api/agentic/continue or /api/agentic/stop.
    """
    data   = request.get_json() or {}
    query  = data.get("query",  "").strip()
    branch = data.get("branch", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    if not branch:
        return jsonify({"error": "Please select a branch first."}), 400

    job_id = str(uuid.uuid4())
    _job_create(job_id)
    job    = _job_get(job_id)
    t0     = time.time()
    _user_email = _current_user_email()

    def _run():
        try:
            result = run_agentic_loop(query, branch, job_id=job_id)
            track_search_event(
                "agentic", query, branch, "success",
                time.time() - t0,
                result.get("total_files_read", 0),
                result.get("total_iterations",  0),
                result.get("total_searches",    0),
                result.get("confidence", {}).get("level", ""),
                input_tokens=result.get("input_tokens",  0),
                output_tokens=result.get("output_tokens", 0),
                user_email=_user_email,
            )
        except Exception as e:
            track_search_event("agentic", query, branch, "error", time.time() - t0,
                               error_message=str(e), user_email=_user_email)
            job["event_queue"].put({"type": "error", "message": str(e)})
        finally:
            _job_remove(job_id)

    threading.Thread(target=_run, daemon=True).start()

    def _generate():
        # First event: send the job_id to the frontend
        yield f"data: {json.dumps({'type': 'started', 'job_id': job_id})}\n\n"
        while True:
            try:
                evt = job["event_queue"].get(timeout=30)
                yield f"data: {json.dumps(evt)}\n\n"
                if evt.get("type") in ("done", "error"):
                    break
            except queue.Empty:
                # Keep-alive ping
                yield "data: {\"type\":\"ping\"}\n\n"

    return Response(
        stream_with_context(_generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":        "no-cache",
            "X-Accel-Buffering":    "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.route("/api/agentic/start", methods=["POST"])
def api_agentic_start():
    """
    Polling-friendly replacement for /api/agentic/stream.
    Starts the agentic loop in a background thread and immediately returns
    a job_id.  The frontend then polls /api/agentic/poll/<job_id> every ~2 s.
    This avoids Railway's ~300 s HTTP proxy timeout that kills long SSE streams.
    """
    data   = request.get_json() or {}
    query  = data.get("query",  "").strip()
    branch = data.get("branch", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    if not branch:
        return jsonify({"error": "Please select a branch first."}), 400

    job_id = str(uuid.uuid4())
    _job_create(job_id)
    job = _job_get(job_id)
    t0  = time.time()
    _user_email = _current_user_email()

    def _run():
        try:
            result = run_agentic_loop(query, branch, job_id=job_id)
            track_search_event(
                "agentic", query, branch, "success",
                time.time() - t0,
                result.get("total_files_read", 0),
                result.get("total_iterations",  0),
                result.get("total_searches",    0),
                result.get("confidence", {}).get("level", ""),
                input_tokens=result.get("input_tokens",  0),
                output_tokens=result.get("output_tokens", 0),
                user_email=_user_email,
            )
        except Exception as e:
            track_search_event("agentic", query, branch, "error",
                               time.time() - t0, error_message=str(e),
                               user_email=_user_email)
            job["event_queue"].put({"type": "error", "message": str(e)})
        finally:
            # Keep job alive for 5 minutes after completion so the frontend
            # can drain the last events even if a poll arrives a bit late.
            threading.Timer(300, lambda: _job_remove(job_id)).start()

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/api/agentic/poll/<job_id>", methods=["GET"])
def api_agentic_poll(job_id):
    """
    Drain all queued events for a running/completed agentic job.
    Returns {"events": [...]} — frontend calls this every ~2 s.
    Stops polling when it receives a "done" or "error" event.
    """
    job = _job_get(job_id)
    if not job:
        return jsonify({"events": [{"type": "error",
                                    "message": "Job not found or expired."}]})
    events = []
    try:
        while True:
            evt = job["event_queue"].get_nowait()
            events.append(evt)
            if evt.get("type") in ("done", "error"):
                break
    except queue.Empty:
        pass
    return jsonify({"events": events})


@app.route("/api/agentic/continue", methods=["POST"])
def api_agentic_continue():
    job_id = (request.get_json() or {}).get("job_id", "")
    job    = _job_get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    job["continue_evt"].set()
    return jsonify({"ok": True})


@app.route("/api/agentic/stop", methods=["POST"])
def api_agentic_stop():
    job_id = (request.get_json() or {}).get("job_id", "")
    job    = _job_get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    job["stop_evt"].set()
    return jsonify({"ok": True})


@app.route("/api/agentic", methods=["POST"])
def api_agentic():
    """Legacy synchronous endpoint — kept for backwards compatibility."""
    data   = request.get_json() or {}
    query  = data.get("query",  "").strip()
    branch = data.get("branch", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    if not branch:
        return jsonify({"error": "Please select a branch first."}), 400
    t0 = time.time()
    _user_email = _current_user_email()
    try:
        result = run_agentic_loop(query, branch)
        track_search_event(
            "agentic", query, branch, "success",
            time.time() - t0,
            result.get("total_files_read", 0),
            result.get("total_iterations",  0),
            result.get("total_searches",    0),
            result.get("confidence", {}).get("level", ""),
            input_tokens=result.get("input_tokens",  0),
            output_tokens=result.get("output_tokens", 0),
            user_email=_user_email,
        )
        return jsonify({"success": True, "result": result})
    except Exception as e:
        track_search_event("agentic", query, branch, "error", time.time() - t0,
                           error_message=str(e), user_email=_user_email)
        return jsonify({"error": str(e)}), 500










# ---------------------------------------------------------------------------
# Clarifying question
# ---------------------------------------------------------------------------

@app.route("/api/clarify", methods=["POST"])
def api_clarify():
    """
    Quickly check whether a query is specific enough, or needs one clarifying
    question before searching. Returns JSON:
      {"needs_clarification": false}
    or
      {"needs_clarification": true, "question": "..."}
    """
    data  = request.get_json() or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"needs_clarification": False})
    try:
        client = get_client()
        resp   = client.messages.create(
            model=MODEL,
            max_tokens=200,
            system=(
                "You decide whether a Cora PPM codebase question needs one clarifying question "
                "before searching, or is already specific enough.\n\n"
                "Rules:\n"
                "  - Only ask if the answer would meaningfully change WHAT files to search for.\n"
                "  - Never ask about things that don't affect the search (e.g. 'why do you want to know').\n"
                "  - If the question names a specific feature, button, page, or behaviour → it's specific enough.\n"
                "  - Respond ONLY with valid JSON, no extra text:\n"
                '    {"needs_clarification": false}\n'
                "  or:\n"
                '    {"needs_clarification": true, "question": "One short, specific follow-up question."}'
            ),
            messages=[{"role": "user", "content": f"Question: {query}"}],
        )
        text = resp.content[0].text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
            text = text.strip()
        result = json.loads(text)
        result["needs_clarification"] = bool(result.get("needs_clarification", False))
        return jsonify(result)
    except Exception:
        return jsonify({"needs_clarification": False})


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Export API — generate Word (.docx) or PDF from search results
# ---------------------------------------------------------------------------

from docx import Document as DocxDocument
from docx.shared import Inches, Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.style import WD_STYLE_TYPE
import markdown as md_lib
import re as _re

from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph as RLParagraph, Spacer, Table as RLTable, TableStyle, HRFlowable
from reportlab.lib import colors as rl_colors


def _md_to_plain(text):
    """Strip markdown to plain text (rough but effective)."""
    text = _re.sub(r'```[\s\S]*?```', lambda m: m.group(0).strip('`').strip(), text)
    text = _re.sub(r'`([^`]+)`', r'\1', text)
    text = _re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
    text = _re.sub(r'\*([^*]+)\*', r'\1', text)
    text = _re.sub(r'^#{1,6}\s+', '', text, flags=_re.MULTILINE)
    text = _re.sub(r'^\s*[-*]\s+', '  • ', text, flags=_re.MULTILINE)
    text = _re.sub(r'^\s*\d+\.\s+', '  ', text, flags=_re.MULTILINE)
    text = _re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    return text.strip()


def _build_docx(query, branch, metadata, sections):
    """Build a .docx buffer from search result sections."""
    doc = DocxDocument()

    # Page margins
    for section in doc.sections:
        section.top_margin = Cm(2.54)
        section.bottom_margin = Cm(2.54)
        section.left_margin = Cm(2.54)
        section.right_margin = Cm(2.54)

    # Styles
    style = doc.styles['Normal']
    style.font.name = 'Arial'
    style.font.size = Pt(11)
    style.font.color.rgb = RGBColor(0x1A, 0x25, 0x35)

    # Title
    title = doc.add_heading('Cora Code Investigator', level=0)
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    for run in title.runs:
        run.font.color.rgb = RGBColor(0x1A, 0x25, 0x35)

    # Query
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = p.add_run(query)
    run.font.size = Pt(13)
    run.font.color.rgb = RGBColor(0x64, 0x74, 0x8B)

    # Metadata row
    meta_parts = []
    if branch:
        meta_parts.append(f"Branch: {branch}")
    if metadata.get('confidence'):
        meta_parts.append(f"Confidence: {metadata['confidence']}")
    if metadata.get('elapsed_seconds'):
        meta_parts.append(f"Time: {metadata['elapsed_seconds']}s")
    if metadata.get('total_tokens'):
        meta_parts.append(f"Tokens: {metadata['total_tokens']:,}")
    if meta_parts:
        mp = doc.add_paragraph(' | '.join(meta_parts))
        mp.alignment = WD_ALIGN_PARAGRAPH.CENTER
        for run in mp.runs:
            run.font.size = Pt(9)
            run.font.color.rgb = RGBColor(0x94, 0xA3, 0xB8)

    doc.add_paragraph('')  # spacer

    # Sections
    section_config = [
        ('overview', 'Overview', RGBColor(0x25, 0x63, 0xEB)),
        ('user_guide', 'User Guide', RGBColor(0x16, 0xA3, 0x4A)),
        ('tech_ref', 'Technical Reference', RGBColor(0xD9, 0x77, 0x06)),
    ]

    for key, label, color in section_config:
        content = sections.get(key, '').strip()
        if not content:
            continue

        heading = doc.add_heading(label, level=1)
        for run in heading.runs:
            run.font.color.rgb = color

        # Add content as paragraphs
        plain = _md_to_plain(content)
        for line in plain.split('\n'):
            line = line.rstrip()
            if not line:
                doc.add_paragraph('')
            elif line.startswith('  • '):
                doc.add_paragraph(line[4:], style='List Bullet')
            else:
                doc.add_paragraph(line)

    # Footer
    fp = doc.add_paragraph('')
    fp.add_run('Generated by Cora Code Investigator').font.size = Pt(8)
    fp.runs[0].font.color.rgb = RGBColor(0x94, 0xA3, 0xB8)
    fp.alignment = WD_ALIGN_PARAGRAPH.CENTER

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf


def _build_pdf(query, branch, metadata, sections):
    """Build a PDF buffer from search result sections."""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            topMargin=0.75*inch, bottomMargin=0.75*inch,
                            leftMargin=0.75*inch, rightMargin=0.75*inch)

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='DocTitle', fontName='Helvetica-Bold', fontSize=20,
                              textColor=HexColor('#1A2535'), alignment=1, spaceAfter=6))
    styles.add(ParagraphStyle(name='DocQuery', fontName='Helvetica', fontSize=12,
                              textColor=HexColor('#64748B'), alignment=1, spaceAfter=4))
    styles.add(ParagraphStyle(name='DocMeta', fontName='Helvetica', fontSize=8,
                              textColor=HexColor('#94A3B8'), alignment=1, spaceAfter=16))
    styles.add(ParagraphStyle(name='SectionHead', fontName='Helvetica-Bold', fontSize=14,
                              spaceBefore=18, spaceAfter=8))
    styles.add(ParagraphStyle(name='BodyText2', fontName='Helvetica', fontSize=10,
                              textColor=HexColor('#1A2535'), leading=14, spaceAfter=4))
    styles.add(ParagraphStyle(name='BulletItem', fontName='Helvetica', fontSize=10,
                              textColor=HexColor('#1A2535'), leading=14,
                              leftIndent=18, bulletIndent=6, spaceAfter=3))
    styles.add(ParagraphStyle(name='Footer', fontName='Helvetica', fontSize=7,
                              textColor=HexColor('#94A3B8'), alignment=1, spaceBefore=20))

    elements = []
    elements.append(RLParagraph('Cora Code Investigator', styles['DocTitle']))
    elements.append(RLParagraph(query.replace('&', '&amp;').replace('<', '&lt;'), styles['DocQuery']))

    meta_parts = []
    if branch:
        meta_parts.append(f"Branch: {branch}")
    if metadata.get('confidence'):
        meta_parts.append(f"Confidence: {metadata['confidence']}")
    if metadata.get('elapsed_seconds'):
        meta_parts.append(f"Time: {metadata['elapsed_seconds']}s")
    if metadata.get('total_tokens'):
        meta_parts.append(f"Tokens: {metadata['total_tokens']:,}")
    if meta_parts:
        elements.append(RLParagraph(' | '.join(meta_parts), styles['DocMeta']))

    elements.append(HRFlowable(width="100%", thickness=1, color=HexColor('#E2E8F0'), spaceAfter=12))

    section_config = [
        ('overview', 'Overview', '#2563EB'),
        ('user_guide', 'User Guide', '#16A34A'),
        ('tech_ref', 'Technical Reference', '#D97706'),
    ]

    for key, label, color in section_config:
        content = sections.get(key, '').strip()
        if not content:
            continue

        head_style = ParagraphStyle(f'Head_{key}', parent=styles['SectionHead'],
                                    textColor=HexColor(color))
        elements.append(RLParagraph(label, head_style))

        plain = _md_to_plain(content)
        for line in plain.split('\n'):
            line = line.rstrip()
            if not line:
                elements.append(Spacer(1, 6))
            elif line.startswith('  • '):
                safe = line[4:].replace('&', '&amp;').replace('<', '&lt;')
                elements.append(RLParagraph(f'• {safe}', styles['BulletItem']))
            else:
                safe = line.replace('&', '&amp;').replace('<', '&lt;')
                elements.append(RLParagraph(safe, styles['BodyText2']))

    elements.append(RLParagraph('Generated by Cora Code Investigator', styles['Footer']))

    doc.build(elements)
    buf.seek(0)
    return buf


@app.route("/api/export", methods=["POST"])
@login_required
def api_export():
    """Generate a Word (.docx) or PDF export of the search results.

    JSON body:
        format: "docx" or "pdf"
        query: the search query
        branch: branch searched
        metadata: { confidence, elapsed_seconds, total_tokens, ... }
        sections: { overview: "...", user_guide: "...", tech_ref: "..." }
    """
    data = request.get_json(force=True) or {}
    fmt       = data.get("format", "docx").lower()
    query     = data.get("query", "Search Results")
    branch    = data.get("branch", "")
    metadata  = data.get("metadata", {})
    sections  = data.get("sections", {})

    if fmt not in ("docx", "pdf"):
        return jsonify({"error": "format must be 'docx' or 'pdf'"}), 400

    try:
        if fmt == "docx":
            buf = _build_docx(query, branch, metadata, sections)
            mimetype = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            ext = "docx"
        else:
            buf = _build_pdf(query, branch, metadata, sections)
            mimetype = "application/pdf"
            ext = "pdf"

        slug = _re.sub(r'[^a-z0-9]+', '-', query[:60].lower()).strip('-') or 'search-results'
        filename = f"cora-investigator-{slug}.{ext}"

        return Response(
            buf.getvalue(),
            mimetype=mimetype,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Flag API — users flag unhelpful / missing responses
# ---------------------------------------------------------------------------

@app.route("/api/flag", methods=["POST"])
@login_required
def api_flag():
    """Log a flag (accurate or inaccurate) for a search result container.

    JSON body:
        query, loop_type, answer, flag_type ('accurate'|'inaccurate'),
        container ('overview'|'user_guide'|'tech_ref'),
        explanation (required if inaccurate), request_article (bool, if accurate),
        branch, confidence_level, duration_sec, input_tokens, output_tokens,
        total_tokens, files_read, iterations, searches
    """
    data = request.get_json() or {}
    query       = data.get("query", "").strip()
    loop_type   = data.get("loop_type", "").strip()
    answer      = data.get("answer", "").strip()
    flag_type   = data.get("flag_type", "inaccurate").strip()
    container   = data.get("container", "").strip()
    explanation = data.get("explanation", "").strip()
    request_article = 1 if data.get("request_article") else 0

    if not query or not loop_type:
        return jsonify({"error": "query and loop_type are required"}), 400
    if flag_type not in ("accurate", "inaccurate"):
        return jsonify({"error": "flag_type must be 'accurate' or 'inaccurate'"}), 400
    if flag_type == "inaccurate" and not explanation:
        return jsonify({"error": "explanation is required for inaccurate flags"}), 400

    # Default status based on flag type
    status = "positive" if flag_type == "accurate" else "pending"

    flag_id = str(uuid.uuid4())
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO flagged_queries "
            "(id, query, loop_type, answer, explanation, flagged_by, status, "
            " flag_type, container, request_article, "
            " branch, confidence_level, duration_sec, "
            " input_tokens, output_tokens, total_tokens, "
            " files_read, iterations, searches) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (flag_id, query, loop_type, answer, explanation, _current_user_email(), status,
             flag_type, container, request_article,
             data.get("branch", ""), data.get("confidence_level", ""),
             data.get("duration_sec", 0),
             data.get("input_tokens", 0), data.get("output_tokens", 0),
             data.get("total_tokens", 0),
             data.get("files_read", 0), data.get("iterations", 0),
             data.get("searches", 0)),
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True, "flag_id": flag_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

ADMIN_TABLES = {
    "search_events": (
        "SELECT id, ran_at, loop_type, query, branch, status, "
        "duration_sec, files_read, iterations, searches, confidence_level, "
        "input_tokens, output_tokens, total_tokens, error_message, "
        "COALESCE(user_email, '') AS user_email "
        "FROM search_events ORDER BY ran_at DESC LIMIT 500"
    ),
}


# /admin route removed — admin dashboard lives in the main page (toggle button)


@app.route("/admin/data/<table_name>")
@admin_required
def admin_table_data(table_name):
    if table_name not in ADMIN_TABLES:
        return jsonify({"error": "Invalid table name"}), 400
    rows = query_db(ADMIN_TABLES[table_name])
    return jsonify({"rows": rows, "count": len(rows)})


@app.route("/admin/export/<table_name>")
@admin_required
def admin_export_csv(table_name):
    if table_name not in ADMIN_TABLES:
        return "Invalid table name", 400
    rows = query_db(ADMIN_TABLES[table_name])
    if not rows:
        return "No data", 200
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={table_name}.csv"},
    )


# ---------------------------------------------------------------------------
# Admin flag management routes
# ---------------------------------------------------------------------------

@app.route("/api/admin/stats")
@admin_required
def api_admin_stats():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as total FROM flagged_queries")
        total = cur.fetchone()["total"]
        cur.execute("SELECT status, COUNT(*) as cnt FROM flagged_queries GROUP BY status")
        by_status = {row["status"]: row["cnt"] for row in cur.fetchall()}
        cur.execute(
            "SELECT query, COUNT(*) as cnt FROM flagged_queries "
            "GROUP BY query ORDER BY cnt DESC LIMIT 10"
        )
        top_queries = [{"query": row["query"], "count": row["cnt"]} for row in cur.fetchall()]
        cur.execute(
            "SELECT DATE(flagged_at) as day, COUNT(*) as cnt FROM flagged_queries "
            "WHERE flagged_at >= DATE('now', '-30 days') GROUP BY day ORDER BY day"
        )
        daily_trend = [{"date": row["day"], "count": row["cnt"]} for row in cur.fetchall()]
        cur.execute(
            "SELECT loop_type, COUNT(*) as cnt FROM flagged_queries GROUP BY loop_type"
        )
        by_loop = {row["loop_type"]: row["cnt"] for row in cur.fetchall()}
        conn.close()
        return jsonify({
            "total": total,
            "pending": by_status.get("pending", 0),
            "reviewed": by_status.get("reviewed", 0),
            "dismissed": by_status.get("dismissed", 0),
            "article_created": by_status.get("article_created", 0),
            "top_queries": top_queries,
            "daily_trend": daily_trend,
            "by_loop_type": by_loop,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/flags")
@admin_required
def api_admin_flags():
    status_filter = request.args.get("status", "")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if status_filter:
            cur.execute(
                "SELECT * FROM flagged_queries WHERE status = ? ORDER BY flagged_at DESC",
                (status_filter,),
            )
        else:
            cur.execute("SELECT * FROM flagged_queries ORDER BY flagged_at DESC")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return jsonify({"flags": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/user-rankings")
@admin_required
def api_admin_user_rankings():
    """Return per-user aggregates: query count, token usage, estimated cost.

    Optional query params: start_date, end_date (YYYY-MM-DD) to scope the window.
    """
    try:
        start_date = request.args.get("start_date", "").strip()
        end_date   = request.args.get("end_date", "").strip()
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        where_clause = ""
        params = []
        if start_date and end_date:
            where_clause = "WHERE date(ran_at) >= date(?) AND date(ran_at) <= date(?)"
            params = [start_date, end_date]
        cur.execute(f"""
            SELECT
                COALESCE(NULLIF(user_email, ''), 'unknown') AS user_email,
                COUNT(*)                                    AS query_count,
                SUM(COALESCE(input_tokens, 0))              AS total_input_tokens,
                SUM(COALESCE(output_tokens, 0))             AS total_output_tokens,
                SUM(COALESCE(total_tokens, 0))              AS total_tokens
            FROM search_events
            {where_clause}
            GROUP BY COALESCE(NULLIF(user_email, ''), 'unknown')
            ORDER BY query_count DESC
        """, params)
        rankings = []
        for row in cur.fetchall():
            inp = row["total_input_tokens"] or 0
            out = row["total_output_tokens"] or 0
            cost = (inp / 1_000_000) * COST_PER_M_INPUT + (out / 1_000_000) * COST_PER_M_OUTPUT
            rankings.append({
                "user_email":          row["user_email"],
                "query_count":         row["query_count"],
                "total_input_tokens":  inp,
                "total_output_tokens": out,
                "total_tokens":        row["total_tokens"] or 0,
                "estimated_cost":      round(cost, 4),
            })
        conn.close()
        return jsonify({"rankings": rankings})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/flags/<flag_id>", methods=["PATCH"])
@admin_required
def api_admin_update_flag(flag_id):
    data = request.get_json() or {}
    new_status  = data.get("status", "")
    admin_notes = data.get("admin_notes", "")
    if new_status not in ("pending", "reviewed", "dismissed", "article_created"):
        return jsonify({"error": "Invalid status"}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "UPDATE flagged_queries SET status=?, admin_notes=?, reviewed_by=?, reviewed_at=datetime('now') WHERE id=?",
            (new_status, admin_notes, _current_user_email(), flag_id),
        )
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/clear-events", methods=["POST"])
@admin_required
def api_admin_clear_events():
    """Delete search_events rows within a date range.

    JSON body:
        start_date  – ISO date string (YYYY-MM-DD), inclusive  (required)
        end_date    – ISO date string (YYYY-MM-DD), inclusive  (required)
    """
    data = request.get_json() or {}
    start_date = (data.get("start_date") or "").strip()
    end_date   = (data.get("end_date") or "").strip()
    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date are required (YYYY-MM-DD)"}), 400
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "DELETE FROM search_events WHERE date(ran_at) >= date(?) AND date(ran_at) <= date(?)",
            (start_date, end_date),
        )
        conn.commit()
        deleted = cur.rowcount
        conn.close()
        return jsonify({"success": True, "deleted": deleted,
                        "range": f"{start_date} to {end_date}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Admin — individual record lookups
# ---------------------------------------------------------------------------

@app.route("/api/admin/search-events", methods=["GET"])
@admin_required
def api_admin_search_event_ids():
    """Return a list of all search event IDs with basic metadata for browsing."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, ran_at, loop_type, query, branch, status, "
            "COALESCE(user_email, '') AS user_email "
            "FROM search_events ORDER BY ran_at DESC"
        ).fetchall()
        conn.close()
        return jsonify({
            "success": True,
            "count": len(rows),
            "events": [dict(r) for r in rows],
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/search-events/<int:event_id>", methods=["GET"])
@admin_required
def api_admin_search_event_detail(event_id):
    """Return full details for a single search event by ID."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM search_events WHERE id = ?", (event_id,)
        ).fetchone()
        conn.close()
        if not row:
            return jsonify({"error": f"Search event {event_id} not found"}), 404
        return jsonify({"success": True, "event": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/admin/flags/<flag_id>/detail", methods=["GET"])
@admin_required
def api_admin_flag_detail(flag_id):
    """Return full details for a single flagged query by ID,
    including the original answer and all response containers."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM flagged_queries WHERE id = ?", (flag_id,)
        ).fetchone()
        conn.close()
        if not row:
            return jsonify({"error": f"Flagged query {flag_id} not found"}), 404
        return jsonify({"success": True, "flag": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Auth routes — Entra ID OAuth
# ---------------------------------------------------------------------------

@app.route("/login")
def login():
    if AUTH_DISABLED:
        return redirect(url_for("index"))
    msal_app = _build_msal_app()
    state = secrets.token_urlsafe(16)
    session["auth_state"] = state
    auth_url = msal_app.get_authorization_request_url(
        ENTRA_SCOPES,
        redirect_uri=ENTRA_REDIRECT_URI,
        state=state,
    )
    return redirect(auth_url)


@app.route("/auth/callback")
def auth_callback():
    if AUTH_DISABLED:
        return redirect(url_for("index"))
    if request.args.get("state") != session.get("auth_state"):
        return "State mismatch — possible CSRF attack.", 400
    code = request.args.get("code")
    if not code:
        return f"Auth error: {request.args.get('error_description', 'No code returned')}", 400
    msal_app = _build_msal_app()
    result = msal_app.acquire_token_by_authorization_code(
        code,
        scopes=ENTRA_SCOPES,
        redirect_uri=ENTRA_REDIRECT_URI,
    )
    if "error" in result:
        return f"Auth error: {result.get('error_description', result['error'])}", 400
    # Fetch user profile from Microsoft Graph
    graph_resp = requests.get(
        "https://graph.microsoft.com/v1.0/me",
        headers={"Authorization": f"Bearer {result['access_token']}"},
    )
    if graph_resp.status_code == 200:
        session["user"] = graph_resp.json()
    else:
        session["user"] = {"displayName": "User", "mail": "unknown"}
    return redirect(url_for("index"))


@app.route("/logout")
def logout():
    session.clear()
    if AUTH_DISABLED:
        return redirect(url_for("index"))
    return redirect(
        f"{ENTRA_AUTHORITY}/oauth2/v2.0/logout"
        f"?post_logout_redirect_uri={urllib.parse.quote(ENTRA_REDIRECT_URI.rsplit('/auth/callback', 1)[0], safe='')}"
    )


# ---------------------------------------------------------------------------
# External API — unified query endpoint for other apps (Jira, n8n, etc.)
# ---------------------------------------------------------------------------
EXTERNAL_API_KEY = os.environ.get("EXTERNAL_API_KEY", "")


def _check_api_key():
    """Validate bearer token for external API calls. Returns error response or None."""
    if not EXTERNAL_API_KEY:
        return jsonify({"error": "External API not configured — set EXTERNAL_API_KEY env var."}), 503
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return jsonify({"error": "Missing or invalid Authorization header. Use: Bearer <api_key>"}), 401
    token = auth_header[7:].strip()
    if not secrets.compare_digest(token, EXTERNAL_API_KEY):
        return jsonify({"error": "Invalid API key."}), 403
    return None


# ---------------------------------------------------------------------------
# Async job runner — processes API queries in background threads
# ---------------------------------------------------------------------------

def _run_job_in_background(job_id, query, branch, user_email):
    """Execute the agentic search for a job and write results to the DB."""
    t0 = time.time()
    try:
        result = run_agentic_loop(query, branch)
        total_time = round(time.time() - t0, 1)

        response_payload = json.dumps({
            "success": True,
            "answer":  result.get("answer", ""),
            "metadata": {
                "branch":          branch,
                "files_read":      result.get("total_files_read", 0),
                "iterations":      result.get("total_iterations", 0),
                "searches":        result.get("total_searches", 0),
                "confidence":      result.get("confidence", {}).get("level", ""),
                "elapsed_seconds": result.get("elapsed_seconds", total_time),
                "input_tokens":    result.get("input_tokens", 0),
                "output_tokens":   result.get("output_tokens", 0),
            },
        })

        # Track in search_events
        track_search_event(
            "external_api", query, branch, "success",
            total_time,
            result.get("total_files_read", 0),
            result.get("total_iterations", 0),
            result.get("total_searches", 0),
            result.get("confidence", {}).get("level", ""),
            input_tokens=result.get("input_tokens", 0),
            output_tokens=result.get("output_tokens", 0),
            user_email=user_email,
        )

        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "UPDATE api_jobs SET status='completed', result_json=?, completed_at=datetime('now') WHERE id=?",
            (response_payload, job_id),
        )
        conn.commit()
        conn.close()

    except Exception as e:
        total_time = round(time.time() - t0, 1)
        error_payload = json.dumps({
            "success": False,
            "error":   str(e),
            "metadata": {"total_time_sec": total_time},
        })
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "UPDATE api_jobs SET status='failed', error=?, result_json=?, completed_at=datetime('now') WHERE id=?",
                (str(e), error_payload, job_id),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass


@app.route("/api/v1/query", methods=["POST"])
def api_v1_query():
    """
    Unified external query endpoint.

    Supports two modes:
      • Synchronous (default): blocks until search completes and returns the answer.
      • Async (pass "async": true): returns a job_id immediately, poll /api/v1/query/<job_id> for results.

    Request JSON:
        {
            "query":  "How does the EAC Submit button work on the Project Forecasting page?",
            "branch": "supported_release-1.25.2",   // optional, default: first available
            "async":  true                           // optional, default: false (sync)
        }

    Sync Response JSON:
        {
            "success":  true,
            "answer":   "## Answer\\n...",
            "metadata": { ... }
        }

    Async Response JSON (202 Accepted):
        {
            "job_id":    "abc-123",
            "status":    "pending",
            "poll_url":  "/api/v1/query/abc-123",
            "message":   "Query submitted. Poll poll_url for results."
        }

    Auth: Bearer token via EXTERNAL_API_KEY env var.
    """
    # ── Auth ──
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    # ── Parse request ──
    data   = request.get_json(force=True) or {}
    query  = data.get("query", "").strip()
    branch = data.get("branch", "").strip()
    use_async = data.get("async", False)

    if not query:
        return jsonify({"error": "No query provided."}), 400

    # ── Resolve branch ──
    if not branch:
        try:
            branches = ado_get_branches()
            branch = branches[0] if branches else "main"
        except Exception:
            branch = "main"

    user_email = request.headers.get("X-User-Email", "external_api").strip() or "external_api"

    # ── Async mode: create job and return immediately ──
    if use_async:
        job_id = str(uuid.uuid4())
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.execute(
                "INSERT INTO api_jobs (id, status, query, branch, user_email) VALUES (?, 'processing', ?, ?, ?)",
                (job_id, query, branch, user_email),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            return jsonify({"error": f"Could not create job: {e}"}), 500

        t = threading.Thread(target=_run_job_in_background, args=(job_id, query, branch, user_email), daemon=True)
        t.start()

        return jsonify({
            "job_id":   job_id,
            "status":   "processing",
            "poll_url": f"/api/v1/query/{job_id}",
            "message":  "Query submitted. Poll poll_url for results.",
        }), 202

    # ── Sync mode (default): block until complete ──
    t0 = time.time()

    try:
        result = run_agentic_loop(query, branch)
    except Exception as e:
        return jsonify({
            "success": False,
            "error":   str(e),
            "metadata": {"total_time_sec": round(time.time() - t0, 1)},
        }), 500

    total_time = round(time.time() - t0, 1)

    track_search_event(
        "external_api", query, branch, "success",
        total_time,
        result.get("total_files_read", 0),
        result.get("total_iterations", 0),
        result.get("total_searches", 0),
        result.get("confidence", {}).get("level", ""),
        input_tokens=result.get("input_tokens", 0),
        output_tokens=result.get("output_tokens", 0),
        user_email=user_email,
    )

    return jsonify({
        "success": True,
        "answer":  result.get("answer", ""),
        "metadata": {
            "branch":          branch,
            "files_read":      result.get("total_files_read", 0),
            "iterations":      result.get("total_iterations", 0),
            "searches":        result.get("total_searches", 0),
            "confidence":      result.get("confidence", {}).get("level", ""),
            "elapsed_seconds": result.get("elapsed_seconds", total_time),
            "input_tokens":    result.get("input_tokens", 0),
            "output_tokens":   result.get("output_tokens", 0),
        },
    })


@app.route("/api/v1/query/<job_id>", methods=["GET"])
def api_v1_query_status(job_id):
    """
    Poll for async query results.

    Returns:
      - 200 with full result when job is completed or failed
      - 200 with status "processing" while the search is still running
      - 404 if job_id not found

    Auth: Bearer token via EXTERNAL_API_KEY env var.
    """
    auth_err = _check_api_key()
    if auth_err:
        return auth_err

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM api_jobs WHERE id=?", (job_id,)).fetchone()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not row:
        return jsonify({"error": "Job not found.", "job_id": job_id}), 404

    status = row["status"]

    if status == "processing":
        return jsonify({
            "job_id":  job_id,
            "status":  "processing",
            "message": "Search is still running. Poll again shortly.",
        })

    # completed or failed — return the full result
    result_json = row["result_json"]
    if result_json:
        result = json.loads(result_json)
    else:
        result = {"success": False, "error": row["error"] or "Unknown error"}

    result["job_id"] = job_id
    result["status"] = status
    result["created_at"] = row["created_at"]
    result["completed_at"] = row["completed_at"]
    return jsonify(result)


@app.route("/api/v1/health", methods=["GET"])
def api_v1_health():
    """Public health check for the external API."""
    return jsonify({
        "status":    "ok",
        "api_ready": bool(EXTERNAL_API_KEY),
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)

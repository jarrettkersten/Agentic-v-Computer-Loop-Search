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
import urllib.parse
import requests
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
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

MAX_AGENTIC_ITERATIONS = 12    # more headroom for complex multi-file questions
MAX_SEARCH_RESULTS     = 12    # broader initial candidate pool per search term
MAX_FILE_CHARS         = 15000 # read more of each file — key methods are often deep
MAX_FILES_TO_READ      = 10    # read more files before synthesising

# Time-based search control ─────────────────────────────────────────────────
# Agentic loop pauses at these elapsed-second marks and asks the user whether
# to continue. Total hard stop is at SEARCH_HARD_LIMIT_SEC.
SEARCH_PAUSE_FIRST_SEC  = 300   # 5 min  — first "continue?" prompt
SEARCH_PAUSE_REPEAT_SEC = 120   # 2 min  — subsequent prompts
SEARCH_HARD_LIMIT_SEC   = 660   # 11 min — hard stop regardless

# ---------------------------------------------------------------------------
# SharePoint config
# ---------------------------------------------------------------------------
SP_SITE_URL    = "https://corasystems.sharepoint.com/sites/ProfessionalServicesEMEA"
SP_TENANT_ID   = os.environ.get("M365_TENANT_ID", "corasystems.com")
SP_USERNAME    = os.environ.get("M365_USERNAME", "")
SP_PASSWORD    = os.environ.get("M365_PASSWORD", "")

# Server-relative path to the target folder.
# Content lives one level inside the parent — "PS Confluence Content for AI"
SP_FOLDER_REL  = "/sites/ProfessionalServicesEMEA/Shared Documents/Confluence Content for AI/PS Confluence Content for AI"
# Full URL used in KQL path: restriction
SP_FOLDER_FULL = "https://corasystems.sharepoint.com" + SP_FOLDER_REL

# Simple in-memory token cache
_sp_token_cache: dict = {}

# Detect optional libraries at import time so we give clear errors if missing
try:
    import msal as _msal_mod
    MSAL_AVAILABLE = True
except ImportError:
    MSAL_AVAILABLE = False

app = Flask(__name__)

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
                error_message    TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS synthesis_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ran_at        TEXT    NOT NULL DEFAULT (datetime('now')),
                query         TEXT,
                sources_used  TEXT,
                duration_sec  REAL,
                status        TEXT    NOT NULL DEFAULT 'success',
                input_tokens  INTEGER DEFAULT 0,
                output_tokens INTEGER DEFAULT 0,
                total_tokens  INTEGER DEFAULT 0,
                error_message TEXT
            )
        """)
        # Migrate existing tables: add token columns if they don't already exist
        for table in ("search_events", "synthesis_events"):
            for col_def in (
                "input_tokens  INTEGER DEFAULT 0",
                "output_tokens INTEGER DEFAULT 0",
                "total_tokens  INTEGER DEFAULT 0",
            ):
                try:
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
                except Exception:
                    pass  # column already exists
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Warning: could not init tracking tables: {e}")


def track_search_event(loop_type: str, query: str, branch: str, status: str,
                       duration_sec: float, files_read: int = 0,
                       iterations: int = 0, searches: int = 0,
                       confidence_level: str = "", error_message: str = "",
                       input_tokens: int = 0, output_tokens: int = 0):
    """Record a single loop search run."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """INSERT INTO search_events
               (loop_type, query, branch, status, duration_sec, files_read,
                iterations, searches, confidence_level,
                input_tokens, output_tokens, total_tokens, error_message)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (loop_type, (query or "")[:300], branch or "", status,
             round(duration_sec, 2), files_read, iterations, searches,
             confidence_level or "",
             input_tokens, output_tokens, input_tokens + output_tokens,
             error_message or ""),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Warning: could not track search event: {e}")


def track_synthesis_event(query: str, sources_used: str, duration_sec: float,
                          status: str = "success", error_message: str = "",
                          input_tokens: int = 0, output_tokens: int = 0):
    """Record a synthesis call."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """INSERT INTO synthesis_events
               (query, sources_used, duration_sec, status,
                input_tokens, output_tokens, total_tokens, error_message)
               VALUES (?,?,?,?,?,?,?,?)""",
            ((query or "")[:300], sources_used or "", round(duration_sec, 2),
             status, input_tokens, output_tokens, input_tokens + output_tokens,
             error_message or ""),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Warning: could not track synthesis event: {e}")


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


def ado_read_file(file_path, branch):
    """Read a file from the Cora PPM ADO repo. Returns raw text content."""
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
    content = resp.text
    if len(content) > MAX_FILE_CHARS:
        return (
            content[:MAX_FILE_CHARS]
            + f"\n\n[⚠ FILE TRUNCATED — only the first {MAX_FILE_CHARS:,} chars are shown; "
            "the file continues beyond this point. Methods or classes defined later in the "
            "file are NOT visible here. To find specific code deeper in this file, call "
            "ado_code_search with the exact method or class name as the search term — "
            "the search engine will return the matching snippet at its actual location.]"
        )
    return content


def get_client():
    if not ANTHROPIC_API_KEY:
        raise ValueError("ANTHROPIC_API_KEY is not set.")
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def extract_search_terms(query: str, client) -> list[str]:
    """Ask Claude to derive 4-7 ADO-Code-Search-friendly terms from a natural language
    question. Returns terms like ["TimesheetGrid", "Timesheet", "ITimesheetService"].

    More terms = broader coverage of related files (page, service, controller, helper).
    Falls back to the first 40 chars of the original query if parsing fails.
    """
    resp = client.messages.create(
        model=MODEL,
        max_tokens=350,
        system=(
            "You are a search-term extractor for a large C#/VB.NET/ASP.NET WebForms codebase "
            "stored in Azure DevOps (Cora PPM). ADO Code Search works on exact token matches — "
            "long natural-language phrases return zero results.\n\n"
            "Think in layers:\n"
            "  1. The PRIMARY feature name as it appears in file names (e.g. 'TimesheetGrid', 'EACSubmit')\n"
            "  2. The SHORT keyword — just the core noun (e.g. 'Timesheet', 'EAC', 'Forecast')\n"
            "  3. Related SERVICE / REPOSITORY class names (e.g. 'ITimesheetService', 'TimesheetRepository')\n"
            "  4. Related CONTROLLER or CODEBEHIND patterns (e.g. 'TimesheetController', 'Timesheet.aspx')\n"
            "  5. Any ENUM, CONSTANT, or SQL stored-proc name likely involved (e.g. 'sp_GetTimesheets')\n"
            "  6. Alternative naming variants used in older parts of the codebase (e.g. 'TimeSheet' vs 'Timesheet')\n\n"
            "Rules:\n"
            "  - Each term must be 1-3 tokens, no spaces longer than one word, no punctuation except underscores\n"
            "  - Never use the full user question as a term\n"
            "  - Include BOTH PascalCase compound AND short standalone variants"
        ),
        messages=[{
            "role":    "user",
            "content": (
                "Extract 4-7 search terms that together will find ALL files relevant to this question "
                "in the Cora PPM ADO repository. Cover the page/control, the business logic, "
                "the service/repository layer, and any helpers.\n\n"
                "Return ONLY a JSON array of strings, nothing else.\n\n"
                f"Question: {query}"
            ),
        }],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        terms = json.loads(raw)
        if isinstance(terms, list) and terms:
            return [str(t).strip() for t in terms[:7] if str(t).strip()]
    except Exception:
        pass
    return [query[:40]]


def select_relevant_files(query: str, candidates: list, client, max_files: int = 6) -> list:
    """Given a list of search-result candidates (dicts with file_path/file_name),
    ask Claude to rank them by likely relevance to the question and return the
    top `max_files` in priority order.

    This prevents the Hybrid Loop from blindly reading the top-ranked ADO
    results (which may be irrelevant) and instead reads the files most likely
    to contain the answer.
    """
    if len(candidates) <= max_files:
        return candidates  # Nothing to trim

    # Build a numbered list of filenames for Claude to evaluate
    file_list = "\n".join(
        f"{i+1}. {c.get('file_path', c.get('file_name', ''))}"
        for i, c in enumerate(candidates)
    )
    resp = client.messages.create(
        model=MODEL,
        max_tokens=300,
        system=(
            "You are a C#/.NET/VB.NET code analyst. "
            "Given a question and a list of file paths from an ADO repository, "
            "identify which files are MOST LIKELY to contain code or logic that directly "
            "answers the question. Prioritise: feature-named files, page/control files, "
            "service/repository files over helpers, locale files, or generic utilities."
        ),
        messages=[{
            "role":    "user",
            "content": (
                f"Question: {query}\n\n"
                f"Candidate files:\n{file_list}\n\n"
                f"Return ONLY a JSON array of the {max_files} most relevant 1-based line numbers "
                f"(e.g. [2, 5, 1, 8, 3, 6]), ordered most-relevant first. Nothing else."
            ),
        }],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        indices = json.loads(raw)
        if isinstance(indices, list):
            selected = []
            for idx in indices:
                i = int(idx) - 1
                if 0 <= i < len(candidates):
                    selected.append(candidates[i])
            if selected:
                return selected[:max_files]
    except Exception:
        pass
    return candidates[:max_files]  # Fallback to original order


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
    so the Hybrid Loop can fetch files Claude said it needed but didn't have.
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
                "inspect code from large files without reading the full file, and to "
                "find methods that may be beyond the truncation point of a file you already read. "
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
                "Read the full content of a specific file from the Cora PPM ADO "
                "repository. Use file paths returned by ado_code_search."
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
                    }
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
        "═══ HANDLING TRUNCATED FILES ═══\n"
        "When a file you read ends with '⚠ FILE TRUNCATED', the method or class you need may be\n"
        "further down in the file. DO NOT re-read the same file — it will truncate at the same point.\n"
        "Instead, IMMEDIATELY call ado_code_search with the EXACT method or class name you are\n"
        "looking for (e.g. 'btnSubmitEac_OnClick', 'SubmitEac', 'IProjectForecastUiBuilder').\n"
        "The search engine returns snippets at their actual location, bypassing the truncation.\n"
        "Also check the snippets already returned in earlier search results — they may already\n"
        "contain the code you need at the precise offset in the file.\n\n"
        "═══ ANSWER REQUIREMENTS ═══\n"
        "Your final answer MUST include ALL of the following sections:\n\n"
        "## Overview\n"
        "Plain-language summary of what the feature/function does and why it exists.\n\n"
        "## How It Works — Step by Step\n"
        "Walk through the COMPLETE execution flow: user action → page event → service call → "
        "data access → database → return path. Name each method and file at every step.\n"
        "Format: 'When X happens, `FileName.vb` calls `MethodName()` which does Y, then passes to...'\n\n"
        "## Key Code\n"
        "Show the most important method(s) with file path and brief explanation.\n"
        "Use: `FileName.vb` → `MethodName()` — what it does\n\n"
        "## Files Involved\n"
        "List every file you read, with its role: UI / CodeBehind / Service / Repository / Helper.\n\n"
        "NEVER use general knowledge. Only answer from files you have read. "
        "If you cannot find a file you need, say which file and why it matters, then continue "
        "with what you have rather than stopping early.\n\n"
        "═══ WHEN TO STOP SEARCHING ═══\n"
        "Once you have read files covering all relevant layers (UI page, code-behind, service, "
        "data access), STOP and write your final answer — do NOT keep searching.\n"
        "You have enough to answer when you have:\n"
        "  • The .aspx/.ascx page that renders the UI for this feature\n"
        "  • The .aspx.vb/.aspx.cs code-behind that handles the events\n"
        "  • The service or business-logic class that processes the request\n"
        "  • The repository/SQL that reads or writes the data\n"
        "Do NOT keep searching for: CSS/styling, unrelated helper classes, alternative "
        "implementations, or every file that mentions the topic. "
        "A focused answer from 8-12 key files is better than an exhaustive answer from 25+ files."
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
        if iteration > 15:
            messages.append({"role": "user", "content": (
                "SYSTEM NOTE: You have reached the 15-iteration limit. "
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
            max_tokens=4000,
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
                fp = tu.input.get("file_path", "")
                try:
                    content = ado_read_file(fp, branch)
                    result_content = content
                    accumulated_files[fp] = content   # ← store full content
                    tool_calls_this_turn.append({
                        "type":           "read_file",
                        "file_path":      fp,
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
            max_tokens=5000,
            system=(
                "You are a senior Cora PPM code analyst. Answer ONLY from the ADO repository "
                "context provided — never from general knowledge.\n\n"
                "Your answer MUST follow this structure:\n\n"
                "## Overview\n"
                "Plain-language summary of what the feature/function does.\n\n"
                "## How It Works — Step by Step\n"
                "Trace the COMPLETE execution flow naming every method and file at each step.\n\n"
                "## Key Code\n"
                "Show the most important method bodies with file paths in fenced code blocks.\n\n"
                "## Files Involved\n"
                "List every file referenced with its role (UI / CodeBehind / Service / Repository).\n\n"
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
    }

    # Notify SSE stream that the job is done
    if job:
        job["event_queue"].put({"type": "done", "result": result})
    return result


# ---------------------------------------------------------------------------
# 2. HYBRID LOOP  (structured search  +  adaptive iterative reading)
# ---------------------------------------------------------------------------

def run_hybrid_loop(query, branch):
    """Phase 1 — Structured multi-term search with layer coverage.
    Phase 2 — Agentic-loop-style adaptive reading: Claude reads one file at a time,
               follows the reference chain, stops when it has all layers.
    Phase 3 — Single structured synthesis pass.
    """
    steps_log = []
    client    = get_client()
    h_input_tokens  = 0
    h_output_tokens = 0

    # ── Phase 1: Structured broad search ─────────────────────────────────────

    # Step 1 — Extract ADO-friendly search terms
    search_terms = extract_search_terms(query, client)
    steps_log.append({
        "step":         1,
        "action":       "Extracted search terms: " + ", ".join(f'"{t}"' for t in search_terms),
        "search_terms": search_terms,
        "url":          "https://dev.azure.com/" + ADO_ORG + "/" + ADO_PROJECT
                        + "/_search?type=code&text=" + requests.utils.quote(search_terms[0]),
    })

    # Step 2 — Multi-term ADO search with camelCase fallbacks + layer coverage
    all_results = []
    seen_paths  = set()
    search_log  = []

    def _camel_split(s):
        import re
        parts = re.sub(r'([A-Z][a-z]+)', r' \1',
                       re.sub(r'([A-Z]+)(?=[A-Z][a-z])', r' \1', s)).split()
        return [p for p in parts if len(p) >= 3]

    def _search_and_add(term, label=None):
        hits     = ado_code_search(term, branch, top=MAX_SEARCH_RESULTS)
        new_hits = [r for r in hits if r.get("file_path") not in seen_paths]
        for r in new_hits:
            seen_paths.add(r.get("file_path"))
            all_results.append(r)
        return {"term": label or term, "hits": len(hits), "new_unique": len(new_hits)}

    for term in search_terms:
        try:
            entry = _search_and_add(term)
            search_log.append(entry)
            if entry["hits"] == 0:
                for part in _camel_split(term):
                    if part not in search_terms:
                        try:
                            fb = _search_and_add(part, label=f"{term}→{part}")
                            search_log.append(fb)
                            if fb["hits"] > 0:
                                break
                        except Exception:
                            pass
        except Exception as e:
            search_log.append({"term": term, "error": str(e)})

    # Layer coverage: targeted Service/Repository searches if missing
    _LAYER_SIGNALS = {
        "Service":    lambda p: "service" in p.lower(),
        "Repository": lambda p: any(x in p for x in ("Repository", "Repo", "DataAccess")),
    }
    base_term = search_terms[0] if search_terms else ""
    for layer, fn in _LAYER_SIGNALS.items():
        if not any(fn(r.get("file_path", "")) for r in all_results):
            for suffix in ([layer] if layer == "Service" else ["Repository", "Repo"]):
                try:
                    extra = _search_and_add(base_term + suffix, label=f"layer:{layer}")
                    search_log.append(extra)
                    if extra["hits"] > 0:
                        break
                except Exception:
                    pass

    search_summary = ", ".join(
        f'"{sl["term"]}": {sl.get("hits", 0)} hit(s)' for sl in search_log
    )
    steps_log.append({
        "step":     2,
        "action":   (
            f"Searched {len(search_terms)} term(s) — {search_summary}"
            f" — {len(all_results)} unique file(s) in pool"
        ),
        "searches": search_log,
        "results":  all_results,
    })

    # ── Phase 2: Adaptive iterative reading ──────────────────────────────────

    # Pre-rank the candidate pool once so Claude has a priority-ordered list
    candidates  = select_relevant_files(query, all_results, client, max_files=20)
    file_contents = []
    read_paths    = set()

    def _read_and_log(fp, step_label):
        if not fp or fp in read_paths:
            return False
        read_paths.add(fp)
        try:
            content = ado_read_file(fp, branch)
            file_contents.append({"file_path": fp, "content": content})
            steps_log.append({
                "step":            step_label,
                "action":          "Read file: " + fp,
                "url":             "https://dev.azure.com/" + ADO_ORG + "/" + ADO_PROJECT
                                   + "/_git/" + ADO_REPO + "?path=" + fp + "&version=GB" + branch,
                "content_preview": content[:250] + "..." if len(content) > 250 else content,
                "chars_read":      len(content),
            })
            return True
        except Exception as e:
            steps_log.append({"step": step_label, "action": f"Could not read {fp}: {e}"})
            return False

    MAX_READS          = 10   # max files to read in the adaptive phase
    MAX_TOTAL_ITERS    = 16   # hard ceiling to prevent infinite SEARCH loops
    reads_done         = 0
    last_fp            = None
    last_content       = None
    failed_searches    = set()   # terms that returned 0 hits — never retry
    consecutive_failed = 0       # consecutive zero-hit searches in a row

    steps_log.append({
        "step":   3,
        "action": f"Adaptive read phase — {len(candidates)} candidates ranked, "
                  f"following reference chain (max {MAX_READS} reads)",
    })

    for total_iter in range(1, MAX_TOTAL_ITERS + 1):
        if reads_done >= MAX_READS:
            steps_log.append({"step": f"3.{total_iter}", "action": "Read limit reached — proceeding to synthesis"})
            break

        unread = [r for r in candidates if r.get("file_path") not in read_paths]

        # After 2 consecutive zero-hit searches, force-read the next best candidate
        if consecutive_failed >= 2 and unread:
            fp = unread[0].get("file_path", "")
            steps_log.append({
                "step":   f"3.{total_iter}",
                "action": f"Force-reading next candidate after {consecutive_failed} failed searches: {fp}",
            })
            ok = _read_and_log(fp, f"3.{total_iter}.read")
            if ok:
                reads_done        += 1
                last_fp            = fp
                last_content       = file_contents[-1]["content"]
                consecutive_failed = 0
            continue

        read_summary = (
            "\n".join(f"  - {fp}" for fp in read_paths)
            if read_paths else "  (none yet)"
        )
        candidate_list = (
            "\n".join(f"  - {r.get('file_path','')}" for r in unread[:20])
            if unread else "  (no more candidates)"
        )
        recent_block = (
            f"\nMost recently read — {last_fp}:\n```\n"
            + last_content[:2500]
            + ("\n[...truncated...]" if len(last_content) > 2500 else "")
            + "\n```\n"
        ) if last_fp else ""

        failed_block = (
            "Terms already searched with 0 hits — do NOT search these again:\n"
            + "\n".join(f"  - {t}" for t in sorted(failed_searches))
            + "\n\n"
        ) if failed_searches else ""

        nav_resp = client.messages.create(
            model=MODEL,
            max_tokens=80,
            messages=[{
                "role":    "user",
                "content": (
                    f"Navigating a VB.NET/C# ASP.NET WebForms codebase.\n"
                    f"Question: {query}\n\n"
                    f"Files read so far:\n{read_summary}\n"
                    f"{recent_block}\n"
                    f"Unread candidates (already discovered — prefer READ over SEARCH):\n{candidate_list}\n\n"
                    f"{failed_block}"
                    "DECISION RULES (follow in order):\n"
                    "1. If the answer needs a layer (Service/Repository/CodeBehind) already in the candidate list → READ it.\n"
                    "2. If you see a specific file referenced in code that is NOT in the candidate list → SEARCH once.\n"
                    "3. Never SEARCH a term that already returned 0 hits (listed above).\n"
                    "4. Never repeat the same SEARCH twice.\n"
                    "5. If you have covered UI + at least one server-side layer → DONE.\n\n"
                    "Reply with EXACTLY ONE line:\n"
                    "  READ: /full/path/FileName.ext   (pick the most relevant unread candidate)\n"
                    "  SEARCH: ExactClassName          (only if the file is genuinely missing from candidates)\n"
                    "  DONE                            (enough layers covered to answer the question)"
                ),
            }],
        )
        h_input_tokens  += getattr(getattr(nav_resp, "usage", None), "input_tokens",  0)
        h_output_tokens += getattr(getattr(nav_resp, "usage", None), "output_tokens", 0)

        nav_text = nav_resp.content[0].text.strip().splitlines()[0].strip()
        steps_log.append({"step": f"3.{total_iter}", "action": f"Navigation: {nav_text}"})

        if nav_text.upper().startswith("DONE"):
            steps_log.append({"step": f"3.{total_iter}", "action": "Claude: all layers covered — proceeding to synthesis"})
            break

        elif nav_text.upper().startswith("SEARCH:"):
            term = nav_text[7:].strip()
            # Skip if already tried with 0 results
            if term in failed_searches:
                steps_log.append({
                    "step":   f"3.{total_iter}.search",
                    "action": f"Skipped repeat zero-hit search for '{term}' — forcing READ instead",
                })
                if unread:
                    fp = unread[0].get("file_path", "")
                    ok = _read_and_log(fp, f"3.{total_iter}.read")
                    if ok:
                        reads_done        += 1
                        last_fp            = fp
                        last_content       = file_contents[-1]["content"]
                        consecutive_failed = 0
                continue
            try:
                new_hits  = ado_code_search(term, branch, top=5)
                new_uniq  = [r for r in new_hits if r.get("file_path") not in seen_paths]
                for r in new_uniq:
                    seen_paths.add(r.get("file_path"))
                    candidates.append(r)
                search_log.append({
                    "term": term, "hits": len(new_hits),
                    "new_unique": len(new_uniq), "triggered_by": "hybrid_nav",
                })
                steps_log.append({
                    "step":   f"3.{total_iter}.search",
                    "action": f"Searched '{term}' → {len(new_hits)} hit(s), {len(new_uniq)} new candidate(s)",
                })
                if len(new_hits) == 0:
                    failed_searches.add(term)
                    consecutive_failed += 1
                else:
                    consecutive_failed = 0
            except Exception as e:
                failed_searches.add(term)
                consecutive_failed += 1
                steps_log.append({"step": f"3.{total_iter}.search", "action": f"Search '{term}' failed: {e}"})
            # SEARCH does not consume a read slot — continue to next iter

        elif nav_text.upper().startswith("READ:"):
            fp = nav_text[5:].strip()
            # If Claude gave just a filename (not full path), resolve it from candidates
            if not fp.startswith("/"):
                fname   = fp.rsplit("/", 1)[-1]
                matches = [r.get("file_path") for r in candidates if r.get("file_path", "").endswith(fname)]
                if matches:
                    fp = matches[0]
            # If Claude is trying to re-read an already-read file, redirect to next unread candidate
            if fp in read_paths:
                if unread:
                    fp = unread[0].get("file_path", "")
                    steps_log.append({
                        "step":   f"3.{total_iter}",
                        "action": f"Redirected re-read attempt → next unread candidate: {fp}",
                    })
                else:
                    steps_log.append({"step": f"3.{total_iter}", "action": "All candidates read — proceeding to synthesis"})
                    break
            ok = _read_and_log(fp, f"3.{total_iter}.read")
            if ok:
                reads_done         += 1
                last_fp             = fp
                last_content        = file_contents[-1]["content"]
                consecutive_failed  = 0

        else:
            # Try to salvage a path from the response
            import re as _re
            m = _re.search(r'(/[\w/._%+-]+\.\w+)', nav_text)
            if m:
                fp = m.group(1)
                ok = _read_and_log(fp, f"3.{total_iter}.read")
                if ok:
                    reads_done         += 1
                    last_fp             = fp
                    last_content        = file_contents[-1]["content"]
                    consecutive_failed  = 0
            else:
                break   # unparseable — bail out of navigation loop

    # ── Phase 3: Single synthesis pass ───────────────────────────────────────

    SYNTH_SYSTEM = (
        "You are a senior Cora PPM code analyst. Answer EXCLUSIVELY from the repository files "
        "provided — never from general knowledge.\n\n"
        "Your answer MUST follow this structure:\n\n"
        "## Overview\n"
        "1-2 sentence plain-language summary of what the feature/function does and why it exists.\n\n"
        "## How It Works — Step by Step\n"
        "Trace the COMPLETE execution flow from user action to database and back:\n"
        "  - Name EVERY method called, in order, with the file it lives in\n"
        "  - Describe what each step does in concrete terms — parameters passed, conditions checked, "
        "data transformed\n"
        "  - Do NOT group files together or summarise at a high level. Go file by file, layer by layer.\n"
        "  - Example: 'Clicking Save triggers `btnSave_Click` in `Timesheet.aspx.vb`, which calls "
        "`TimesheetService.SaveTimesheet()`, which validates hours via `ValidateHours()` then "
        "calls `TimesheetRepository.Insert()` which executes `sp_InsertTimesheetRow`'\n\n"
        "## Key Code\n"
        "Quote the most important method bodies (the actual code, not paraphrases), each preceded by "
        "its file path. Use fenced code blocks. Include at least one code block per layer.\n\n"
        "## Files Involved\n"
        "List every file you read with its role: UI / CodeBehind / Service / Repository / Helper / SQL.\n\n"
        "DEPTH REQUIREMENT: You have been given multiple files across multiple layers. You MUST go deep "
        "on each one — cite specific method signatures, parameters, conditional logic, and return values. "
        "A shallow answer that only covers the UI layer or summarises without specifics is NOT acceptable. "
        "The more files you were given, the more detailed your answer should be, not less.\n\n"
        "MISSING FILES: Only declare a file missing if (a) you are certain it exists because "
        "you saw its exact path or filename cited in code you have already read, AND (b) it is "
        "genuinely absent from the files provided. Do NOT speculate about files you haven't seen referenced."
    )

    def _build_context(fc_list, window=3500, top_per_file=2):
        """Build synthesis context using relevance-windowed extraction.

        Rather than dumping full file content (up to 15 k chars × 10 files = 150 k chars,
        which causes shallow per-file synthesis), this extracts the top-N non-overlapping
        windows most dense in query keywords per file — keeping total context tight so
        Claude can reason deeply on each section.

        Files shorter than `window` chars are included in full (no windowing needed).
        """
        if not fc_list:
            return (
                "ADO Code Search for terms ["
                + ", ".join(f'"{t}"' for t in search_terms)
                + "] returned no readable files."
            )

        import re as _re

        # Build a set of query keywords (3+ char alpha tokens, lowercased)
        raw_kws = _re.findall(r'\b[A-Za-z][A-Za-z0-9_]{2,}\b', query)
        # Also include each search term and its camelCase sub-words
        for t in search_terms:
            raw_kws.extend(_re.findall(r'[A-Z][a-z]+|[a-z]+', t))
        kws = set(w.lower() for w in raw_kws if len(w) >= 3)

        parts = []
        for fc in fc_list:
            content = fc["content"]
            fp      = fc["file_path"]

            # ── Small file: include in full ──────────────────────────────────
            if len(content) <= window:
                parts.append(f"### File: {fp}\n{content}")
                continue

            # ── Large file: score overlapping windows ────────────────────────
            step = window // 2           # 50 % overlap so boundary code isn't missed
            scored = []
            for start in range(0, len(content) - 100, step):
                chunk      = content[start : start + window]
                chunk_low  = chunk.lower()
                score      = sum(chunk_low.count(kw) for kw in kws)
                scored.append((score, start, chunk))

            # Select top-N non-overlapping windows by score
            scored.sort(key=lambda x: -x[0])
            selected: list[tuple[int, str]] = []   # (start, chunk)
            used_spans: list[tuple[int, int]] = []
            for score, start, chunk in scored:
                end = start + window
                if any(max(a, start) < min(b, end) for a, b in used_spans):
                    continue                          # overlaps an already-selected window
                selected.append((start, chunk))
                used_spans.append((start, end))
                if len(selected) >= top_per_file:
                    break

            # Sort in document order for coherent reading
            selected.sort(key=lambda x: x[0])

            # Join with ellipsis markers between non-adjacent spans
            file_parts: list[str] = []
            prev_end = None
            for start, chunk in selected:
                if prev_end is not None and start > prev_end + 10:
                    file_parts.append("[...]\n")
                file_parts.append(chunk)
                prev_end = start + len(chunk)

            n_shown  = len(selected)
            note     = (
                f" [showing {n_shown} most query-relevant section(s) "
                f"of {len(content):,} total chars — focus your analysis on these sections]"
            )
            parts.append(f"### File: {fp}{note}\n" + "\n".join(file_parts))

        return "\n\n".join(parts)

    steps_log.append({
        "step":   4,
        "action": f"Synthesis — building structured answer from {len(file_contents)} file(s)",
    })

    synth_resp = client.messages.create(
        model=MODEL,
        max_tokens=8000,
        system=SYNTH_SYSTEM,
        messages=[{
            "role":    "user",
            "content": (
                "Using ONLY the following files from the Cora PPM ADO repository "
                "(branch: " + branch + "), answer this question with full detail:\n\n"
                "Question: " + query + "\n\n"
                "Repository files:\n" + _build_context(file_contents)
            ),
        }],
    )
    h_input_tokens  += getattr(getattr(synth_resp, "usage", None), "input_tokens",  0)
    h_output_tokens += getattr(getattr(synth_resp, "usage", None), "output_tokens", 0)

    final_answer = _clean_answer(synth_resp.content[0].text)
    confidence   = assess_confidence(query, final_answer, len(file_contents), "Hybrid Loop")

    return {
        "answer":        final_answer,
        "confidence":    confidence,
        "steps":         steps_log,
        "total_steps":   len(steps_log),
        "results_found": len(all_results),
        "files_read":    len(file_contents),
        "branch":        branch,
        "input_tokens":  h_input_tokens,
        "output_tokens": h_output_tokens,
    }


# ---------------------------------------------------------------------------
# SharePoint helpers
# ---------------------------------------------------------------------------

def _sp_get_token() -> str:
    """Obtain a SharePoint access token via MSAL ROPC (username + password).

    Token is cached in memory and refreshed automatically when it expires.
    NOTE: ROPC will fail if the account has MFA enforced — in that case an
    Azure AD app registration (client credentials) is required instead.
    """
    if not MSAL_AVAILABLE:
        raise RuntimeError(
            "msal is not installed. Run: pip install msal --break-system-packages"
        )
    if not SP_USERNAME or not SP_PASSWORD:
        raise ValueError(
            "M365_USERNAME and M365_PASSWORD must be set in .env to use SharePoint search."
        )

    # Return cached token if still valid
    if _sp_token_cache.get("expires_at", 0) > time.time() + 60:
        return _sp_token_cache["access_token"]

    # Microsoft Office well-known public client — no secret needed
    CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"
    # .default requests all SharePoint permissions the user already has
    SCOPES    = ["https://corasystems.sharepoint.com/.default"]

    app_msal = _msal_mod.PublicClientApplication(
        CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{SP_TENANT_ID}",
    )
    result = app_msal.acquire_token_by_username_password(
        username=SP_USERNAME,
        password=SP_PASSWORD,
        scopes=SCOPES,
    )

    if "access_token" not in result:
        err = result.get("error_description") or result.get("error") or "Unknown auth error"
        raise ValueError(f"SharePoint authentication failed: {err}")

    _sp_token_cache["access_token"] = result["access_token"]
    _sp_token_cache["expires_at"]   = time.time() + result.get("expires_in", 3600)
    return result["access_token"]


def _sp_headers() -> dict:
    return {
        "Authorization": f"Bearer {_sp_get_token()}",
        "Accept":        "application/json;odata=verbose",
    }


def sp_search(term: str, max_results: int = 8) -> list:
    """Search SharePoint using the Search REST API, restricted to the AI folder.

    Uses KQL `path:` operator to limit results to SP_FOLDER_FULL.
    Returns a list of dicts with title, path, file_type, modified.
    """
    kql = f'"{term}" path:"{SP_FOLDER_FULL}"'
    url = (
        f"{SP_SITE_URL}/_api/search/query"
        f"?querytext={urllib.parse.quote(repr(kql))}"
        f"&rowlimit={max_results}"
        f"&selectproperties='Title%2CPath%2CFileType%2CWrite%2CDescription'"
        f"&trimduplicates=true"
    )
    resp = requests.get(url, headers=_sp_headers(), timeout=25)
    resp.raise_for_status()

    rows = (
        resp.json()
        .get("d", {})
        .get("query", {})
        .get("PrimaryQueryResult", {})
        .get("RelevantResults", {})
        .get("Table", {})
        .get("Rows", {})
        .get("results", [])
    )

    files = []
    for row in rows:
        cells = {
            c["Key"]: c["Value"]
            for c in row.get("Cells", {}).get("results", [])
        }
        path = cells.get("Path", "")
        # Skip .aspx pages and empty paths
        if not path or path.endswith(".aspx"):
            continue
        files.append({
            "title":     cells.get("Title") or path.rsplit("/", 1)[-1],
            "path":      path,
            "file_type": cells.get("FileType", ""),
            "modified":  cells.get("Write", ""),
        })
    return files


def _strip_rtf(rtf_text: str) -> str:
    """Convert RTF markup to plain text.

    Handles common RTF constructs:
      - Unicode escapes: \\uN? → Unicode character
      - Hex escapes: \\'HH → character
      - Control words and groups stripped
    """
    import re
    # Unicode escapes: \u12345? (signed int16, ? is placeholder for unknown char)
    def _unicode_repl(m):
        n = int(m.group(1))
        if n < 0:
            n += 65536
        try:
            return chr(n)
        except (ValueError, OverflowError):
            return ""
    rtf_text = re.sub(r"\\u(-?\d+)\?", _unicode_repl, rtf_text)

    # Hex escapes: \'HH
    def _hex_repl(m):
        try:
            return chr(int(m.group(1), 16))
        except (ValueError, OverflowError):
            return ""
    rtf_text = re.sub(r"\\'([0-9a-fA-F]{2})", _hex_repl, rtf_text)

    # Common paragraph / line break markers → newline
    rtf_text = re.sub(r"\\(par|pard|line|sect|page)\b\s?", "\n", rtf_text)

    # Remove all remaining control words (e.g. \b, \i, \fs24)
    rtf_text = re.sub(r"\\[a-zA-Z]+\-?\d*\s?", " ", rtf_text)

    # Remove remaining curly braces and backslashes
    rtf_text = re.sub(r"[{}\\]", "", rtf_text)

    # Normalise whitespace while preserving paragraph breaks
    rtf_text = re.sub(r"[ \t]+", " ", rtf_text)
    rtf_text = re.sub(r"\n{3,}", "\n\n", rtf_text)
    return rtf_text.strip()


def _extract_doc_text(file_bytes: bytes) -> str:
    """Extract plain text from a .doc file (Word 97-2003 binary OR RTF).

    Confluence typically exports pages as RTF with a .doc extension, so RTF
    detection is attempted first before falling back to the Word binary path.

    Strategy:
      1. RTF detection — if file starts with {\\rtf, strip markup directly
      2. Try python-docx (handles .doc that are actually OOXML / .docx renamed)
      3. Try olefile to read the OLE WordDocument stream
      4. Raw printable-ASCII scan as last resort
    """
    import re

    # ── Attempt 1: RTF (most common Confluence .doc export format) ───────────
    # RTF files always start with "{\rtf"
    try:
        prefix = file_bytes[:6]
        if prefix.startswith(b"{\\rtf"):
            rtf_text = file_bytes.decode("utf-8", errors="replace")
            result = _strip_rtf(rtf_text)
            if len(result) > 50:
                return result
        elif prefix.startswith(b"\xff\xfe") or prefix.startswith(b"\xfe\xff"):
            # UTF-16 BOM — might be RTF encoded as UTF-16
            candidate = file_bytes.decode("utf-16", errors="replace")
            if candidate.lstrip().startswith("{\\rtf"):
                result = _strip_rtf(candidate)
                if len(result) > 50:
                    return result
    except Exception:
        pass

    # ── Attempt 2: treat as OOXML (python-docx) ──────────────────────────────
    try:
        import docx as docx_lib
        doc  = docx_lib.Document(io.BytesIO(file_bytes))
        text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
        for tbl in doc.tables:
            for row in tbl.rows:
                row_text = " | ".join(c.text.strip() for c in row.cells if c.text.strip())
                if row_text:
                    text += "\n" + row_text
        if text.strip():
            return text
    except Exception:
        pass

    # ── Attempt 3: OLE compound document (true Word 97-2003 binary) ──────────
    try:
        import olefile
        if olefile.isOleFile(io.BytesIO(file_bytes)):
            ole = olefile.OleFileIO(io.BytesIO(file_bytes))
            if ole.exists("WordDocument"):
                raw = ole.openstream("WordDocument").read()
                # Word 97 stores character data as CP1252 with UTF-16 runs.
                # Attempt UTF-16 LE first, then CP1252 for the printable fragments.
                try:
                    decoded = raw.decode("utf-16-le", errors="ignore")
                    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", decoded)
                    cleaned = re.sub(r" {4,}", " ", cleaned).strip()
                    if len(cleaned) > 100:
                        return cleaned
                except Exception:
                    pass
                # Fallback: extract printable runs from raw CP1252 bytes
                try:
                    cp_text = raw.decode("cp1252", errors="replace")
                    runs = re.findall(r"[ -~\n\r\t]{4,}", cp_text)
                    result = "\n".join(runs).strip()
                    if len(result) > 100:
                        return result
                except Exception:
                    pass
    except ImportError:
        pass  # olefile not installed; fall through
    except Exception:
        pass

    # ── Attempt 4: raw printable-ASCII scan as last resort ───────────────────
    try:
        # Try UTF-16 LE scan
        decoded = file_bytes.decode("utf-16-le", errors="ignore")
        runs = re.findall(r"[ -~\n\r\t]{4,}", decoded)
        result = "\n".join(runs)
        if len(result) > 50:
            return result
    except Exception:
        pass
    try:
        # Try raw byte scan for long ASCII runs (catches CP1252/UTF-8 mixed)
        raw_str = file_bytes.decode("latin-1", errors="replace")
        runs = re.findall(r"[ -~\n\r\t]{6,}", raw_str)
        result = "\n".join(r for r in runs if not r.isspace())
        if len(result) > 50:
            return result
    except Exception:
        pass
    return "[Could not extract text from .doc file]"


def sp_read_file(file_url: str) -> str:
    """Download a SharePoint file by URL and extract plain text.

    Handles .doc (Word 97 binary), .docx (OOXML), .pdf (PyMuPDF),
    and plain text formats. Falls back to UTF-8 decode for anything else.

    Download strategy (tries in order until one succeeds):
      1. Direct GET of the file URL — simplest, works when token has AllSites.Read
      2. Site-scoped OData API: {SP_SITE_URL}/_api/web/GetFileByServerRelativeUrl(...)/$value
      3. Tenant-scoped OData API (legacy fallback)
    """
    hdrs = _sp_headers()
    hdrs["Accept"] = "*/*"

    rel_path = urllib.parse.unquote(urllib.parse.urlparse(file_url).path)
    rel_escaped = rel_path.replace("'", "''")
    rel_encoded  = urllib.parse.quote(rel_escaped, safe="/")

    download_attempts = [
        # 1. Direct file URL (most reliable)
        file_url,
        # 2. Site-scoped OData API (correct context for GetFileByServerRelativeUrl)
        f"{SP_SITE_URL}/_api/web/GetFileByServerRelativeUrl('{rel_encoded}')/$value",
        # 3. Tenant-scoped OData API (fallback)
        f"https://corasystems.sharepoint.com/_api/web/GetFileByServerRelativeUrl('{rel_encoded}')/$value",
    ]

    file_bytes = None
    last_error  = None
    for attempt_url in download_attempts:
        try:
            resp = requests.get(attempt_url, headers=hdrs, timeout=30, allow_redirects=True)
            if resp.ok:
                file_bytes = resp.content
                break
            last_error = f"{resp.status_code} {resp.reason} — {attempt_url}"
        except Exception as e:
            last_error = str(e)

    if file_bytes is None:
        raise requests.HTTPError(f"All download attempts failed. Last error: {last_error}")

    ext = rel_path.rsplit(".", 1)[-1].lower() if "." in rel_path else ""

    if ext == "doc":
        return _extract_doc_text(file_bytes)[:MAX_FILE_CHARS]

    elif ext == "docx":
        try:
            import docx as docx_lib
            doc  = docx_lib.Document(io.BytesIO(file_bytes))
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            for tbl in doc.tables:
                for row in tbl.rows:
                    row_text = " | ".join(c.text.strip() for c in row.cells if c.text.strip())
                    if row_text:
                        text += "\n" + row_text
            return text[:MAX_FILE_CHARS]
        except ImportError:
            return "[python-docx not installed. Run: pip install python-docx --break-system-packages]"
        except Exception as e:
            return f"[DOCX parse error: {e}]"

    elif ext == "pdf":
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            return "\n".join(page.get_text() for page in doc)[:MAX_FILE_CHARS]
        except ImportError:
            return "[PyMuPDF not installed. Run: pip install PyMuPDF --break-system-packages]"
        except Exception as e:
            return f"[PDF parse error: {e}]"

    else:
        # txt, md, html, csv, etc.
        try:
            return file_bytes.decode("utf-8", errors="replace")[:MAX_FILE_CHARS]
        except Exception:
            return f"[Cannot read file type: {ext}]"


def extract_doc_search_terms(query: str, client) -> list:
    """Extract natural-language search phrases suited for SharePoint full-text search.

    SharePoint search handles multi-word phrases well, unlike ADO Code Search.
    Terms should reflect how the topic would appear in documentation.
    """
    resp = client.messages.create(
        model=MODEL,
        max_tokens=200,
        system=(
            "You extract search terms for querying SharePoint documentation about Cora PPM. "
            "SharePoint full-text search handles natural phrases well. "
            "Terms should match how topics are described in user guides, process docs, and how-to articles."
        ),
        messages=[{
            "role":    "user",
            "content": (
                "Extract 2-4 search terms or short phrases (2-5 words each) "
                "for searching SharePoint documentation.\n\n"
                "Rules:\n"
                "- Use natural language as it would appear in documentation\n"
                "- Include the feature or topic name as one term\n"
                "- Include a shorter fallback keyword as another\n"
                "- Return ONLY a JSON array of strings, nothing else\n\n"
                f"Question: {query}"
            ),
        }],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        terms = json.loads(raw)
        if isinstance(terms, list) and terms:
            return [str(t).strip() for t in terms[:4] if str(t).strip()]
    except Exception:
        pass
    return [query[:60]]


def run_sharepoint_loop(query: str) -> dict:
    """Search the SharePoint 'Confluence Content for AI' folder and synthesize an answer.

    Pattern mirrors the Hybrid Loop:
      Step 1 — Extract doc-search terms from question
      Step 2 — Search SharePoint for each term, deduplicate
      Step 3 — Download and read top files (docx / pdf / txt)
      Step 4 — Claude synthesises a plain-language answer with source links
    """
    steps_log = []
    client    = get_client()

    # ── Step 1: Extract search terms ──────────────────────────────────────────
    search_terms = extract_doc_search_terms(query, client)
    steps_log.append({
        "step":         1,
        "action":       "Extracted search terms: " + ", ".join(f'"{t}"' for t in search_terms),
        "search_terms": search_terms,
        "url":          SP_SITE_URL,
    })

    # ── Step 2: Search SharePoint ─────────────────────────────────────────────
    all_results = []
    seen_urls   = set()
    search_log  = []

    for term in search_terms:
        try:
            hits     = sp_search(term, max_results=8)
            new_hits = [r for r in hits if r["path"] not in seen_urls]
            for r in new_hits:
                seen_urls.add(r["path"])
                all_results.append(r)
            search_log.append({"term": term, "hits": len(hits), "new_unique": len(new_hits)})
        except Exception as e:
            search_log.append({"term": term, "error": str(e)})

    search_summary = ", ".join(
        '"' + sl["term"] + '": ' + str(sl.get("hits", 0)) + " hit(s)"
        for sl in search_log
    )
    steps_log.append({
        "step":     2,
        "action":   (
            "Ran " + str(len(search_terms)) + " SharePoint search(es) — "
            + search_summary
            + " — " + str(len(all_results)) + " unique file(s) found"
        ),
        "searches": search_log,
        "results":  all_results,
    })

    # ── Early exit: detect complete auth failure ───────────────────────────────
    all_errors   = [sl for sl in search_log if "error" in sl]
    auth_blocked = any(
        "AADSTS53003" in sl.get("error", "") or
        "Conditional Access" in sl.get("error", "") or
        "authentication failed" in sl.get("error", "").lower()
        for sl in all_errors
    )
    if auth_blocked and len(all_results) == 0:
        block_msg = (all_errors[0].get("error", "Authentication blocked") if all_errors else "Authentication blocked")
        return {
            "answer":         (
                "## Answer\n\n"
                "SharePoint KB is currently unavailable — the search service account is being blocked by "
                "a Microsoft Conditional Access policy on this server.\n\n"
                "**Error:** " + block_msg + "\n\n"
                "**What this means:** The Railway deployment's IP/identity does not satisfy the Conditional "
                "Access requirements for the Cora SharePoint tenant. This is an infrastructure issue, not a "
                "query issue. The ADO-based loops (Agentic and Hybrid) are unaffected.\n\n"
                "**To fix:** A tenant admin needs to exclude the SharePoint service account from the "
                "blocking Conditional Access policy, or add an allowed IP range for the Railway server.\n\n"
                "---\n\n"
                "## Technical Reference\n\n"
                "**Auth error:** `AADSTS53003` — Blocked by Conditional Access policy.\n"
                "**Service account:** " + SP_USERNAME + "\n"
                "**SharePoint site:** " + SP_SITE_URL
            ),
            "confidence":     {"level": "none", "reason": "SharePoint authentication blocked by Conditional Access policy (AADSTS53003)."},
            "confidence_reason": "SharePoint authentication blocked by Conditional Access policy (AADSTS53003).",
            "results_found":  0,
            "files_read":     0,
            "total_steps":    len(steps_log),
            "steps":          steps_log,
            "folder":         SP_FOLDER_FULL,
        }

    # ── Step 3: Download and read top files ───────────────────────────────────
    file_contents = []
    for i, result in enumerate(all_results[:MAX_FILES_TO_READ], start=1):
        url   = result.get("path", "")
        title = result.get("title", url.rsplit("/", 1)[-1])
        if not url:
            continue
        try:
            content = sp_read_file(url)
            file_contents.append({"title": title, "url": url, "content": content})
            steps_log.append({
                "step":            f"3.{i}",
                "action":          f"Downloaded and read: {title}",
                "url":             url,
                "file_type":       result.get("file_type", ""),
                "content_preview": content[:250] + "…" if len(content) > 250 else content,
                "chars_read":      len(content),
            })
        except Exception as e:
            steps_log.append({
                "step":   f"3.{i}",
                "action": f"Could not read '{title}': {e}",
            })

    # ── Step 4: Synthesise ────────────────────────────────────────────────────
    steps_log.append({"step": 4, "action": "Passing SharePoint documents to Claude for synthesis"})

    if not file_contents:
        context_text = (
            "SharePoint search in the 'Confluence Content for AI' folder for terms ["
            + ", ".join(f'"{t}"' for t in search_terms)
            + "] returned no readable files."
        )
    else:
        context_text = "\n\n".join(
            f"### Document: {fc['title']}\nSource URL: {fc['url']}\n\n{fc['content']}"
            for fc in file_contents
        )

    synthesis = client.messages.create(
        model=MODEL,
        max_tokens=3000,
        system=(
            "You are a Cora PPM documentation analyst. "
            "Answer EXCLUSIVELY from the SharePoint documents provided below. "
            "Do NOT use general knowledge or external sources.\n\n"
            "Your answer MUST:\n"
            "1. Explain the topic clearly in plain language — write like a help article\n"
            "2. Use markdown headers (##), bullet points, and clear sections\n"
            "3. Include relevant quotes or specific details from the source documents\n"
            "4. After each key claim, note the document title it came from in parentheses\n"
            "5. End with a '## Sources' section listing each document title as a "
            "   markdown link using its Source URL\n\n"
            "If the documents do not contain enough information, say so explicitly."
        ),
        messages=[{
            "role":    "user",
            "content": (
                "Using ONLY the following documents from the Cora PPM "
                "Professional Services SharePoint knowledge base, answer:\n\n"
                f"Question: {query}\n\n"
                f"SharePoint documents:\n{context_text}"
            ),
        }],
    )

    sp_answer = _clean_answer(synthesis.content[0].text)
    confidence = assess_confidence(query, sp_answer, len(file_contents), "SharePoint Loop")
    sp_in  = getattr(getattr(synthesis, "usage", None), "input_tokens",  0)
    sp_out = getattr(getattr(synthesis, "usage", None), "output_tokens", 0)

    return {
        "answer":        sp_answer,
        "confidence":    confidence,
        "steps":         steps_log,
        "total_steps":   len(steps_log),
        "results_found": len(all_results),
        "files_read":    len(file_contents),
        "folder":        SP_FOLDER_FULL,
        "input_tokens":  sp_in,
        "output_tokens": sp_out,
    }


# ---------------------------------------------------------------------------
# 4. MASTER SYNTHESIS
#    Combines answers from all three loops into one comprehensive response.
# ---------------------------------------------------------------------------

def synthesize_all(query: str, agentic_result: dict,
                   hybrid_result: dict, sp_result: dict) -> str:
    """Synthesize answers from all search loops into one authoritative answer."""
    client = get_client()

    parts = []
    if agentic_result and agentic_result.get("answer"):
        branch = agentic_result.get("branch", "")
        files  = agentic_result.get("total_files_read", 0)
        parts.append(
            f"=== AGENTIC LOOP (ADO Repository{', branch: ' + branch if branch else ''}, "
            f"{files} file(s) read) ===\n{agentic_result['answer']}"
        )
    if hybrid_result and hybrid_result.get("answer"):
        branch = hybrid_result.get("branch", "")
        files  = hybrid_result.get("files_read", 0)
        parts.append(
            f"=== HYBRID LOOP (ADO Repository{', branch: ' + branch if branch else ''}, "
            f"{files} file(s) read) ===\n{hybrid_result['answer']}"
        )
    if sp_result and sp_result.get("answer"):
        files = sp_result.get("files_read", 0)
        parts.append(
            f"=== SHAREPOINT KB (Confluence Documentation, {files} document(s) read) ===\n"
            f"{sp_result['answer']}"
        )

    if not parts:
        return "No search results available to synthesize."

    combined = "\n\n".join(parts)

    resp = client.messages.create(
        model=MODEL,
        max_tokens=6000,
        system=(
            "You are a senior Cora PPM analyst synthesizing answers from multiple search sources "
            "into a single, definitive response.\n\n"
            "You have been given answers from up to three sources:\n"
            "  1. Agentic Loop — Claude-driven iterative search of the ADO codebase\n"
            "  2. Hybrid Loop — structured search + adaptive reading of the ADO codebase\n"
            "  3. SharePoint KB — Confluence documentation exports\n\n"
            "Synthesize these into the MOST COMPLETE and ACCURATE answer possible. "
            "Where sources agree, consolidate into one clear statement. "
            "Where they add different details, combine them. "
            "Where they conflict, note the discrepancy and favour the more specific/code-backed source.\n\n"
            "Your answer MUST use EXACTLY this two-section structure:\n\n"
            "## Answer\n"
            "Write a complete, professional, authoritative response for the end user. "
            "Use plain language — no file paths, no code snippets, no method names. "
            "Draw on ALL available sources to give the most complete picture possible. "
            "A non-technical stakeholder should fully understand this section.\n\n"
            "---\n\n"
            "## Technical Reference\n"
            "*For dev/product team review*\n\n"
            "Full technical synthesis for developers to verify:\n"
            "  - Complete execution flow from ADO sources (every method, file, layer in order).\n"
            "    After EACH step, add an inline annotation showing which Answer claim it explains:\n"
            "    e.g. `TimesheetService.SaveTimesheet()` in `TimesheetService.vb` (line 87) → *Supports: \"data is saved via a service layer\"*\n"
            "  - Key code snippets in fenced code blocks. Each snippet MUST be preceded by:\n"
            "    `File: path/to/File.ext, lines N–M` and annotated with which Answer claim it supports.\n"
            "  - File inventory across all ADO sources, with each file's role and which Answer statement(s) it backs.\n"
            "  - Relevant documentation references from SharePoint (document titles as links + page numbers),\n"
            "    each annotated with which Answer claim the document supports.\n"
            "  - ⚠️ CONDITIONAL LOGIC & FEATURE TOGGLES: Explicitly call out ANY conditional branches,\n"
            "    feature flags, config-driven paths, or permission checks found across ALL sources that\n"
            "    affect the behaviour described in the Answer. Format each as:\n"
            "    > ⚠️ **Conditional:** `If SomeFlag = True` in `File.ext` (line N) — affects: \"[Answer claim]\"\n"
            "  - Any conflicts or gaps identified between sources"
        ),
        messages=[{
            "role": "user",
            "content": (
                f"Synthesize the following answers into one comprehensive, authoritative response.\n\n"
                f"Question: {query}\n\n"
                f"{combined}"
            ),
        }],
    )
    synth_in  = getattr(getattr(resp, "usage", None), "input_tokens",  0)
    synth_out = getattr(getattr(resp, "usage", None), "output_tokens", 0)
    return _clean_answer(resp.content[0].text), synth_in, synth_out


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


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
            )
        except Exception as e:
            track_search_event("agentic", query, branch, "error", time.time() - t0,
                               error_message=str(e))
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
            )
        except Exception as e:
            track_search_event("agentic", query, branch, "error",
                               time.time() - t0, error_message=str(e))
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
        )
        return jsonify({"success": True, "result": result})
    except Exception as e:
        track_search_event("agentic", query, branch, "error", time.time() - t0,
                           error_message=str(e))
        return jsonify({"error": str(e)}), 500




@app.route("/api/hybrid", methods=["POST"])
def api_hybrid():
    data   = request.get_json() or {}
    query  = data.get("query",  "").strip()
    branch = data.get("branch", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    if not branch:
        return jsonify({"error": "Please select a branch first."}), 400
    t0 = time.time()
    try:
        result = run_hybrid_loop(query, branch)
        track_search_event(
            "hybrid", query, branch, "success",
            time.time() - t0,
            result.get("files_read",    0),
            0,
            result.get("results_found", 0),
            result.get("confidence", {}).get("level", ""),
            input_tokens=result.get("input_tokens",  0),
            output_tokens=result.get("output_tokens", 0),
        )
        return jsonify({"success": True, "result": result})
    except Exception as e:
        track_search_event("hybrid", query, branch, "error", time.time() - t0,
                           error_message=str(e))
        return jsonify({"error": str(e)}), 500




@app.route("/api/sharepoint", methods=["POST"])
def api_sharepoint():
    data  = request.get_json() or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "No query provided."}), 400
    t0 = time.time()
    try:
        result = run_sharepoint_loop(query)
        track_search_event(
            "sharepoint", query, "", "success",
            time.time() - t0,
            result.get("files_read", 0),
            0, 0,
            result.get("confidence", {}).get("level", ""),
            input_tokens=result.get("input_tokens",  0),
            output_tokens=result.get("output_tokens", 0),
        )
        return jsonify({"success": True, "result": result})
    except Exception as e:
        track_search_event("sharepoint", query, "", "error", time.time() - t0,
                           error_message=str(e))
        return jsonify({"error": str(e)}), 500


@app.route("/api/synthesize", methods=["POST"])
def api_synthesize():
    data     = request.get_json() or {}
    query    = data.get("query", "").strip()
    agentic  = data.get("agentic_result")
    hybrid   = data.get("hybrid_result")
    sp       = data.get("sp_result")
    if not query:
        return jsonify({"error": "No query provided."}), 400
    if not any([agentic, hybrid, sp]):
        return jsonify({"error": "At least one search result is required."}), 400
    sources_used = ", ".join(filter(None, [
        "Agentic"    if agentic else "",
        "Hybrid"     if hybrid  else "",
        "SharePoint" if sp      else "",
    ]))
    t0 = time.time()
    try:
        synthesis, synth_in, synth_out = synthesize_all(
            query, agentic or {}, hybrid or {}, sp or {}
        )
        track_synthesis_event(query, sources_used, time.time() - t0, "success",
                              input_tokens=synth_in, output_tokens=synth_out)
        return jsonify({"success": True, "synthesis": synthesis})
    except Exception as e:
        track_synthesis_event(query, sources_used, time.time() - t0, "error", str(e))
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
# Follow-up questions within a session
# ---------------------------------------------------------------------------

@app.route("/api/followup", methods=["POST"])
def api_followup():
    """
    Answer a follow-up question using only the context already gathered in this
    session (the answers from prior search loops). Does NOT re-run ADO searches.
    """
    data              = request.get_json() or {}
    original_question = data.get("original_question", "").strip()
    followup_question = data.get("followup_question", "").strip()
    answers           = data.get("answers", {})          # {agentic, hybrid, sharepoint, synthesis}
    previous_followups = data.get("previous_followups", [])  # [{q, a}, ...]

    if not followup_question:
        return jsonify({"error": "No follow-up question provided."}), 400

    # Build context from whichever answers are available
    context_parts = []
    for key, label in [
        ("synthesis",   "Comprehensive Synthesis (all sources)"),
        ("agentic",     "Agentic Loop answer"),
        ("sharepoint",  "SharePoint KB answer"),
    ]:
        ans = answers.get(key)
        if ans and isinstance(ans, dict):
            text = ans.get("answer") or ans.get("synthesis") or ""
        elif isinstance(ans, str):
            text = ans
        else:
            text = ""
        if text.strip():
            context_parts.append(f"=== {label} ===\n{text.strip()}")

    if not context_parts:
        return jsonify({"error": "No search results available to answer follow-up questions. Run at least one search first."}), 400

    # Build conversation history from previous follow-ups
    history_lines = []
    for i, fu in enumerate(previous_followups, 1):
        history_lines.append(f"Follow-up {i}: {fu.get('q','')}\nAnswer {i}: {fu.get('a','')}")

    history_section = ("\n\n--- Previous follow-ups ---\n" + "\n\n".join(history_lines)) if history_lines else ""

    try:
        client = get_client()
        resp   = client.messages.create(
            model=MODEL,
            max_tokens=2000,
            system=(
                "You are a Cora PPM analyst answering follow-up questions within an existing Q&A session. "
                "You MUST answer ONLY from the search results provided below — never from general knowledge.\n\n"
                "Your answer MUST use EXACTLY this two-section structure:\n\n"
                "## Answer\n"
                "Direct, plain-language answer to the follow-up question, drawing on the search results below.\n\n"
                "---\n\n"
                "## Technical Reference\n"
                "*For dev/product team review*\n\n"
                "Any specific file paths, method names, code references, or document sources from the "
                "search results that support this answer. If nothing specific applies, write 'See prior search results.'"
            ),
            messages=[{
                "role": "user",
                "content": (
                    f"Original question: {original_question}\n\n"
                    f"Search results from this session:\n"
                    + "\n\n".join(context_parts)
                    + history_section
                    + f"\n\n---\nNew follow-up question: {followup_question}"
                ),
            }],
        )
        answer = _clean_answer(resp.content[0].text)
        return jsonify({"success": True, "answer": answer})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Session export  (returns a Markdown file)
# ---------------------------------------------------------------------------

@app.route("/api/session-export", methods=["POST"])
def api_session_export():
    """
    Assemble the full session (original Q, all loop answers, synthesis,
    all follow-ups) into a single Markdown document and return it as a
    downloadable .md file.
    """
    data              = request.get_json() or {}
    original_question = data.get("original_question", "").strip()
    answers           = data.get("answers", {})
    followups         = data.get("followups", [])    # [{q, a}, ...]
    branch            = data.get("branch", "")

    lines = [
        f"# Cora PPM Q&A Session Export",
        f"",
        f"**Question:** {original_question}",
    ]
    if branch:
        lines.append(f"**Branch:** `{branch}`")
    lines += ["", "---", ""]

    # Loop answers
    for key, label, icon in [
        ("synthesis",  "Comprehensive Synthesis",  "✨"),
        ("agentic",    "Agentic Loop",              "⚡"),

        ("sharepoint", "SharePoint KB",             "🔷"),
    ]:
        ans = answers.get(key)
        if ans and isinstance(ans, dict):
            text = ans.get("answer") or ans.get("synthesis") or ""
            conf = ans.get("confidence", {}).get("level", "") if isinstance(ans.get("confidence"), dict) else ""
        elif isinstance(ans, str):
            text = ans
            conf = ""
        else:
            text = ""
            conf = ""
        if not text.strip():
            continue
        conf_str = f" · Confidence: {conf}" if conf else ""
        lines += [f"## {icon} {label}{conf_str}", "", text.strip(), "", "---", ""]

    # Follow-up questions
    if followups:
        lines += ["## 💬 Follow-up Questions", ""]
        for i, fu in enumerate(followups, 1):
            lines += [
                f"### Follow-up {i}: {fu.get('q', '')}",
                "",
                fu.get("a", ""),
                "",
            ]

    md_content = "\n".join(lines)

    safe_name = "".join(c if c.isalnum() or c in " -_" else "_" for c in original_question[:60]).strip()
    filename  = f"CoPPM_QA_{safe_name or 'session'}.md"

    return Response(
        md_content,
        mimetype="text/markdown",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

ADMIN_TABLES = {
    "search_events": (
        "SELECT id, ran_at, loop_type, query, branch, status, "
        "duration_sec, files_read, iterations, searches, confidence_level, "
        "input_tokens, output_tokens, total_tokens, error_message "
        "FROM search_events ORDER BY ran_at DESC LIMIT 500"
    ),
    "synthesis_events": (
        "SELECT id, ran_at, query, sources_used, duration_sec, status, "
        "input_tokens, output_tokens, total_tokens, error_message "
        "FROM synthesis_events ORDER BY ran_at DESC LIMIT 500"
    ),
}


@app.route("/admin")
def admin_dashboard():
    return render_template("admin.html")


@app.route("/admin/data/<table_name>")
def admin_table_data(table_name):
    if table_name not in ADMIN_TABLES:
        return jsonify({"error": "Invalid table name"}), 400
    rows = query_db(ADMIN_TABLES[table_name])
    return jsonify({"rows": rows, "count": len(rows)})


@app.route("/admin/export/<table_name>")
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


if __name__ == "__main__":
    app.run(debug=True, port=5000)

"""
One-time migration: copy all data from SQLite (usage.db) → PostgreSQL.

Usage:
    DATABASE_URL=postgres://... python migrate_sqlite_to_pg.py [path/to/usage.db]

If no path is given, it defaults to usage.db in the current directory.

Safe to run multiple times — it checks for existing rows and skips duplicates.
"""

import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    sys.exit(1)

DB_FILE = sys.argv[1] if len(sys.argv) > 1 else "usage.db"
if not os.path.exists(DB_FILE):
    print(f"ERROR: SQLite file not found: {DB_FILE}")
    sys.exit(1)


def get_sqlite_rows(table):
    """Read all rows from a SQLite table as list of dicts."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(f"SELECT * FROM {table}").fetchall()]
    conn.close()
    return rows


def migrate_search_events(pg):
    """Migrate search_events — uses SERIAL id so we let Postgres assign new IDs."""
    rows = get_sqlite_rows("search_events")
    if not rows:
        print("  search_events: 0 rows (empty)")
        return

    cur = pg.cursor()
    inserted = 0
    skipped = 0

    for row in rows:
        # Check if this exact event already exists (match on ran_at + query + loop_type)
        cur.execute(
            "SELECT 1 FROM search_events WHERE ran_at = %s AND query = %s AND loop_type = %s LIMIT 1",
            (row.get("ran_at"), row.get("query"), row.get("loop_type")),
        )
        if cur.fetchone():
            skipped += 1
            continue

        cur.execute(
            """INSERT INTO search_events
               (ran_at, loop_type, query, branch, status, duration_sec,
                files_read, iterations, searches, confidence_level,
                input_tokens, output_tokens, total_tokens, error_message, user_email)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                row.get("ran_at"),
                row.get("loop_type", ""),
                row.get("query"),
                row.get("branch"),
                row.get("status", "success"),
                row.get("duration_sec"),
                row.get("files_read", 0),
                row.get("iterations", 0),
                row.get("searches", 0),
                row.get("confidence_level"),
                row.get("input_tokens", 0),
                row.get("output_tokens", 0),
                row.get("total_tokens", 0),
                row.get("error_message"),
                row.get("user_email", ""),
            ),
        )
        inserted += 1

    pg.commit()
    print(f"  search_events: {inserted} inserted, {skipped} skipped (duplicates)")


def migrate_flagged_queries(pg):
    """Migrate flagged_queries — uses TEXT id so we preserve the original UUIDs."""
    rows = get_sqlite_rows("flagged_queries")
    if not rows:
        print("  flagged_queries: 0 rows (empty)")
        return

    cur = pg.cursor()
    inserted = 0
    skipped = 0

    for row in rows:
        # Skip if this flag ID already exists
        cur.execute("SELECT 1 FROM flagged_queries WHERE id = %s LIMIT 1", (row["id"],))
        if cur.fetchone():
            skipped += 1
            continue

        cur.execute(
            """INSERT INTO flagged_queries
               (id, query, loop_type, answer, explanation, flagged_by, flagged_at,
                status, reviewed_by, reviewed_at, admin_notes,
                flag_type, container, request_article,
                branch, confidence_level, duration_sec,
                input_tokens, output_tokens, total_tokens,
                files_read, iterations, searches)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                row["id"],
                row.get("query", ""),
                row.get("loop_type", ""),
                row.get("answer"),
                row.get("explanation"),
                row.get("flagged_by"),
                row.get("flagged_at"),
                row.get("status", "pending"),
                row.get("reviewed_by"),
                row.get("reviewed_at"),
                row.get("admin_notes"),
                row.get("flag_type", "inaccurate"),
                row.get("container", ""),
                row.get("request_article", 0),
                row.get("branch", ""),
                row.get("confidence_level", ""),
                row.get("duration_sec", 0),
                row.get("input_tokens", 0),
                row.get("output_tokens", 0),
                row.get("total_tokens", 0),
                row.get("files_read", 0),
                row.get("iterations", 0),
                row.get("searches", 0),
            ),
        )
        inserted += 1

    pg.commit()
    print(f"  flagged_queries: {inserted} inserted, {skipped} skipped (duplicates)")


def migrate_api_jobs(pg):
    """Migrate api_jobs — uses TEXT id so we preserve the original UUIDs."""
    rows = get_sqlite_rows("api_jobs")
    if not rows:
        print("  api_jobs: 0 rows (empty)")
        return

    cur = pg.cursor()
    inserted = 0
    skipped = 0

    for row in rows:
        cur.execute("SELECT 1 FROM api_jobs WHERE id = %s LIMIT 1", (row["id"],))
        if cur.fetchone():
            skipped += 1
            continue

        cur.execute(
            """INSERT INTO api_jobs
               (id, status, query, branch, user_email, result_json, error,
                created_at, completed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                row["id"],
                row.get("status", "pending"),
                row.get("query", ""),
                row.get("branch"),
                row.get("user_email", "external_api"),
                row.get("result_json"),
                row.get("error"),
                row.get("created_at"),
                row.get("completed_at"),
            ),
        )
        inserted += 1

    pg.commit()
    print(f"  api_jobs: {inserted} inserted, {skipped} skipped (duplicates)")


def main():
    print(f"SQLite source: {DB_FILE}")
    print(f"PostgreSQL target: {DATABASE_URL[:40]}...")
    print()

    pg = psycopg2.connect(DATABASE_URL)

    print("Migrating tables:")
    migrate_search_events(pg)
    migrate_flagged_queries(pg)
    migrate_api_jobs(pg)

    pg.close()
    print()
    print("Migration complete!")


if __name__ == "__main__":
    main()

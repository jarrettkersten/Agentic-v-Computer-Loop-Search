"""
One-time script: insert 10 missing search_events records.

Usage:
    DATABASE_URL="postgresql://postgres:JLYjTYgYwTQaiRuodGlsrcDseNhzBbVc@interchange.proxy.rlwy.net:51839/railway" python insert_missing_records.py

Safe to run multiple times — skips rows that already exist (matched on ran_at + query + loop_type).
"""

import os
import sys
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    print('Usage: DATABASE_URL="postgresql://..." python insert_missing_records.py')
    sys.exit(1)

MISSING_RECORDS = [
    {
        "ran_at": "2026-04-03 14:51:24-04",
        "loop_type": "external_api",
        "query": "Description: When using the column filter dropdown on the Resource Plan page, the filter options do not match the actual values displayed in the column. Steps to Reproduce: Navigate to Project > Resource Plan. Click the filter icon on any column (e.g., Role or Resource). Observe that the dropdown list shows values that are either outdated, incomplete, or do not correspond to the visible column data. Expected Result: The filter dropdown should display all unique values currently shown in the column. Actual Result: The dropdown shows mismatched or missing values, making it difficult to filter accurately.",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 203.10,
        "files_read": 8,
        "iterations": 22,
        "searches": 31,
        "confidence_level": "Medium",
        "input_tokens": 1898392,
        "output_tokens": 7706,
        "total_tokens": 1906098,
        "error_message": "",
        "user_email": "external_api",
    },
    {
        "ran_at": "2026-04-03 14:12:24-04",
        "loop_type": "agentic",
        "query": "What populates the \"File Imports\" table shown on the File Imports administration page?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 221.06,
        "files_read": 14,
        "iterations": 26,
        "searches": 33,
        "confidence_level": "Medium",
        "input_tokens": 1793911,
        "output_tokens": 8326,
        "total_tokens": 1802237,
        "error_message": "",
        "user_email": "dstrominger@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 22:22:10-04",
        "loop_type": "agentic",
        "query": "What is the best way to calculate and report on timephased data in Cora?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 230.37,
        "files_read": 16,
        "iterations": 25,
        "searches": 47,
        "confidence_level": "High",
        "input_tokens": 1850134,
        "output_tokens": 9090,
        "total_tokens": 1859224,
        "error_message": "",
        "user_email": "emartin@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 22:11:49-04",
        "loop_type": "agentic",
        "query": "P6 imports have limitations but we don't have documentation on what those limitations are. Can you investigate the P6 import code and identify any limitations, constraints, or unsupported features?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 244.52,
        "files_read": 33,
        "iterations": 25,
        "searches": 41,
        "confidence_level": "High",
        "input_tokens": 680938,
        "output_tokens": 10090,
        "total_tokens": 691028,
        "error_message": "",
        "user_email": "emartin@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 21:43:15-04",
        "loop_type": "agentic",
        "query": "Where do we have gaps with multi currency coverage in the application?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 216.07,
        "files_read": 10,
        "iterations": 26,
        "searches": 39,
        "confidence_level": "Medium",
        "input_tokens": 1728859,
        "output_tokens": 8330,
        "total_tokens": 1737189,
        "error_message": "",
        "user_email": "emartin@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 21:27:02-04",
        "loop_type": "agentic",
        "query": "Where is the swagger json generated from? And can you identify all the API endpoints listed in it?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 177.69,
        "files_read": 14,
        "iterations": 17,
        "searches": 24,
        "confidence_level": "High",
        "input_tokens": 532811,
        "output_tokens": 7471,
        "total_tokens": 540282,
        "error_message": "",
        "user_email": "jkersten@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 21:05:28-04",
        "loop_type": "agentic",
        "query": "How does the Cora Critical Path Analysis compare to Primavera P6's Critical Path Analysis?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 191.96,
        "files_read": 14,
        "iterations": 19,
        "searches": 21,
        "confidence_level": "High",
        "input_tokens": 1058601,
        "output_tokens": 7577,
        "total_tokens": 1066178,
        "error_message": "",
        "user_email": "emartin@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 19:49:09-04",
        "loop_type": "agentic",
        "query": "What page should the 'Scenario Creation' button navigate to when clicked?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 294.86,
        "files_read": 8,
        "iterations": 21,
        "searches": 30,
        "confidence_level": "High",
        "input_tokens": 3760120,
        "output_tokens": 5499,
        "total_tokens": 3765619,
        "error_message": "",
        "user_email": "jkersten@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 19:39:47-04",
        "loop_type": "agentic",
        "query": "what page should the scenario creation button take the user to when clicked?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 129.50,
        "files_read": 8,
        "iterations": 12,
        "searches": 12,
        "confidence_level": "High",
        "input_tokens": 435116,
        "output_tokens": 5607,
        "total_tokens": 440723,
        "error_message": "",
        "user_email": "jkersten@corasystems.com",
    },
    {
        "ran_at": "2026-04-02 19:29:30-04",
        "loop_type": "agentic",
        "query": "what page should the scenario creation button take the user to when clicked?",
        "branch": "supported_release-1.25.2",
        "status": "success",
        "duration_sec": 107.60,
        "files_read": 4,
        "iterations": 7,
        "searches": 13,
        "confidence_level": "Medium",
        "input_tokens": 349143,
        "output_tokens": 4226,
        "total_tokens": 353369,
        "error_message": "",
        "user_email": "jkersten@corasystems.com",
    },
]

INSERT_SQL = """
INSERT INTO search_events
    (ran_at, loop_type, query, branch, status, duration_sec,
     files_read, iterations, searches, confidence_level,
     input_tokens, output_tokens, total_tokens, error_message, user_email)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def main():
    print(f"Connecting to: {DATABASE_URL[:45]}...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for rec in MISSING_RECORDS:
        # Check for duplicates (match on ran_at + query + loop_type)
        cur.execute(
            "SELECT 1 FROM search_events WHERE ran_at = %s AND query = %s AND loop_type = %s LIMIT 1",
            (rec["ran_at"], rec["query"], rec["loop_type"]),
        )
        if cur.fetchone():
            skipped += 1
            print(f"  SKIP (exists): {rec['query'][:60]}...")
            continue

        cur.execute(INSERT_SQL, (
            rec["ran_at"], rec["loop_type"], rec["query"], rec["branch"],
            rec["status"], rec["duration_sec"], rec["files_read"],
            rec["iterations"], rec["searches"], rec["confidence_level"],
            rec["input_tokens"], rec["output_tokens"], rec["total_tokens"],
            rec["error_message"], rec["user_email"],
        ))
        inserted += 1
        print(f"  INSERT: {rec['query'][:60]}...")

    conn.commit()
    conn.close()
    print(f"\nDone! {inserted} inserted, {skipped} skipped (already existed).")


if __name__ == "__main__":
    main()

"""
One-time fix: set user_email on the P6 imports record to emartin@corasystems.com.

Usage:
    $env:DATABASE_URL="postgresql://..."; python fix_p6_user_email.py
"""

import os
import sys
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL environment variable is not set.")
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

cur.execute(
    """UPDATE search_events
       SET user_email = 'emartin@corasystems.com'
       WHERE query LIKE 'P6 imports have limitations but we don%%t have docu%%'
         AND (user_email IS NULL OR user_email = '')""",
)

print(f"Updated {cur.rowcount} row(s).")
conn.commit()
conn.close()

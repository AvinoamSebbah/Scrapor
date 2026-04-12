"""Fetch processed_files from PostgreSQL and save to a local JSON cache.

Used by workflow prefetch steps to avoid repeated DB reads across W2/W3 runs.

Output: processed_files_cache.json in the working directory.
"""
import os
import json
import sys
import time

import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse

_BATCH = 500
_OUTPUT = "processed_files_cache.json"


def main():
    db_url = (
        os.environ.get("POSTGRESQL_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("SUPABASE_DATABASE_URL")
    )
    if not db_url:
        print("[✗] POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")
        sys.exit(1)

    parsed = urlparse(db_url)
    host = parsed.hostname or "unknown-host"
    db_name = parsed.path.lstrip("/") or "unknown-db"
    print(f"[i] DB target: {host}/{db_name}")
    if "supabase.co" in host:
        print("[!] Warning: URL host looks like Supabase. Verify your POSTGRESQL_URL secret.")

    conn = psycopg2.connect(db_url, connect_timeout=15, cursor_factory=RealDictCursor)
    conn.autocommit = True

    rows = []
    page = 0
    try:
        while True:
            for attempt in range(3):
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT file_name, record_count, chain_name "
                            "FROM processed_files "
                            "ORDER BY file_name "
                            "LIMIT %s OFFSET %s",
                            (_BATCH, page * _BATCH),
                        )
                        chunk = [dict(row) for row in cur.fetchall()]
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"[✗] Failed to fetch page {page} after 3 attempts: {e}")
                        sys.exit(1)
                    print(f"  [!] Page {page} attempt {attempt + 1}/3 failed, retrying: {e}")
                    time.sleep(5)

            rows.extend(chunk)
            if len(chunk) < _BATCH:
                break
            page += 1
    finally:
        conn.close()

    with open(_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(rows, f)

    print(f"[✓] Saved {len(rows)} processed_files to {_OUTPUT}")


if __name__ == "__main__":
    main()

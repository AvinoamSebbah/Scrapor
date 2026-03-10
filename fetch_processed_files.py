"""Fetch processed_files from Supabase and save to a local JSON cache.

Used by W3_upload.yml: one job makes a single connection, saves the list,
then all 35 sequential upload jobs read from the file — zero extra DB hits.

Output: processed_files_cache.json in the working directory.
"""
import os
import json
import sys
import time

_BATCH = 500
_OUTPUT = "processed_files_cache.json"


def main():
    from supabase import create_client

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
    if not url or not key:
        print("[✗] SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
        sys.exit(1)

    client = create_client(url, key)
    try:
        client.postgrest.session.timeout = 120
    except Exception:
        pass

    rows = []
    page = 0
    while True:
        for attempt in range(3):
            try:
                result = (
                    client.table("processed_files")
                    .select("file_name,record_count,chain_name")
                    .range(page * _BATCH, (page + 1) * _BATCH - 1)
                    .execute()
                )
                break
            except Exception as e:
                if attempt == 2:
                    print(f"[✗] Failed to fetch page {page} after 3 attempts: {e}")
                    sys.exit(1)
                print(f"  [!] Page {page} attempt {attempt + 1}/3 failed, retrying: {e}")
                time.sleep(5)

        rows.extend(result.data)
        if len(result.data) < _BATCH:
            break
        page += 1

    with open(_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(rows, f)

    print(f"[✓] Saved {len(rows)} processed_files to {_OUTPUT}")


if __name__ == "__main__":
    main()

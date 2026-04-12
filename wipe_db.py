"""Wipe all data from the PostgreSQL database (local dev / migration reset).

Reads POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL).

Usage:
    python wipe_db.py            # dry-run: shows what WOULD be deleted
    python wipe_db.py --execute  # ⚠️  REAL DELETIONS — irreversible!

Tables wiped (in FK-safe order):
    product_prices, promotion_store_items, promotions, processed_files, products, stores
"""

import os
import sys
from urllib.parse import urlparse

# Load .env if present (dev convenience — no hard dependency)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2

# Tables to wipe, in dependency order (children before parents)
_TABLES = [
    "product_prices",
    "promotion_store_items",
    "promotions",
    "processed_files",
    "products",
    "stores",
]


def get_db_url() -> str:
    url = (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )
    if not url:
        print("[✗] No DB URL found. Set POSTGRESQL_URL (or DATABASE_URL).")
        sys.exit(1)
    return url


def print_target(db_url: str) -> None:
    parsed = urlparse(db_url)
    host = parsed.hostname or "unknown"
    db_name = parsed.path.lstrip("/") or "unknown"
    print(f"[i] DB target  : {host}/{db_name}")
    if "supabase.co" in (host or ""):
        print("[!] Warning: host looks like Supabase — verify your URL!")


def count_rows(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 — internal admin script, no user input
    return cur.fetchone()[0]


def main() -> None:
    dry_run = "--execute" not in sys.argv

    db_url = get_db_url()
    print_target(db_url)

    if dry_run:
        print("[i] Mode        : DRY-RUN (pass --execute to apply)")
    else:
        print("[i] Mode        : ⚠️  EXECUTE — data will be permanently deleted")
        confirm = input("[?] Type 'yes' to confirm: ").strip().lower()
        if confirm != "yes":
            print("[i] Aborted.")
            sys.exit(0)

    conn = None
    try:
        conn = psycopg2.connect(db_url, connect_timeout=15)
        conn.autocommit = False

        with conn.cursor() as cur:
            print("\n[i] Current row counts:")
            counts = {}
            for table in _TABLES:
                try:
                    n = count_rows(cur, table)
                    counts[table] = n
                    print(f"    {table:<20}: {n:>10,}")
                except Exception as e:
                    print(f"    {table:<20}: error ({e})")
                    counts[table] = None

            total = sum(v for v in counts.values() if v)
            print(f"    {'TOTAL':<20}: {total:>10,}")

            if dry_run:
                print("\n[i] Dry-run complete — nothing deleted.")
                return

            print("\n[→] Wiping tables...")
            for table in _TABLES:
                try:
                    cur.execute(f"DELETE FROM {table}")  # noqa: S608
                    deleted = cur.rowcount
                    print(f"    ✅ {table:<20}: {deleted:>10,} rows deleted")
                except Exception as e:
                    print(f"    ✗  {table:<20}: error ({e})")
                    conn.rollback()
                    print("[!] Rolled back all changes due to error.")
                    sys.exit(1)

        conn.commit()
        print("\n[✓] All tables wiped and transaction committed.")

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()

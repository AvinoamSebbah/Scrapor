import os
import sys
import argparse
from unittest.mock import MagicMock
import psycopg2

# Mock Logger to avoid dependency issues
sys.modules['utils'] = MagicMock()
from remotes.short_term.supabase_db import SupabaseUploader

def check_db_status(clear=False):
    db_url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not db_url:
        print("Error: POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) not set.")
        return

    try:
        uploader = SupabaseUploader()

        if clear:
            print("Clearing database...")
            uploader._clean_all_destinations()
            print("Database cleared.")

        conn = psycopg2.connect(db_url, connect_timeout=15)
        tables = ["stores", "products", "prices", "promotions", "processed_files"]
        print("Row Counts:")
        with conn.cursor() as cur:
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    print(f"  {table}: {cur.fetchone()[0]}")
                except Exception as e:
                    print(f"  {table}: ERROR ({e})")

        print("Detailed Sampling:")
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM products WHERE manufacturer_name IS NOT NULL AND manufacturer_name <> ''")
                print(f"  Products with manufacturer: {cur.fetchone()[0]}")
            except Exception as e:
                print(f"  Products with manufacturer: ERROR ({e})")

        with conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM promotions WHERE promotion_start_date > '2020-01-01'")
                print(f"  Promotions with start date: {cur.fetchone()[0]}")
            except Exception as e:
                print(f"  Promotions with start date: ERROR ({e})")

        conn.close()

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Clear the database tables before checking")
    args = parser.parse_args()
    check_db_status(clear=args.clear)

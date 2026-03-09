import os
import sys
import argparse
from unittest.mock import MagicMock

# Mock Logger to avoid dependency issues
sys.modules['utils'] = MagicMock()
from remotes.short_term.supabase_db import SupabaseUploader

def check_db_status(clear=False):
    url = os.getenv("SUPABASE_DATABASE_URL")
    if not url:
        print("Error: SUPABASE_DATABASE_URL not set.")
        return

    try:
        uploader = SupabaseUploader(url)
        
        if clear:
            print("Clearing database...")
            with uploader.conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE stores, products, prices, promotions, processed_files CASCADE")
            print("Database cleared.")

        with uploader.conn.cursor() as cur:
            tables = ["stores", "products", "prices", "promotions", "processed_files"]
            print("Row Counts:")
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    print(f"  {table}: {count}")
                except Exception as e:
                    print(f"  {table}: ERROR ({e})")
                    uploader.conn.rollback()
            
            print("Detailed Sampling:")
            # Check for non-null manufacturers
            cur.execute("SELECT COUNT(*) FROM products WHERE manufacturer_name IS NOT NULL AND manufacturer_name != ''")
            print(f"  Products with manufacturer: {cur.fetchone()[0]}")
            
            # Check for promotion dates
            cur.execute("SELECT COUNT(*) FROM promotions WHERE promotion_start_date > '2020-01-01'")
            print(f"  Promotions with start date: {cur.fetchone()[0]}")

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Clear the database tables before checking")
    args = parser.parse_args()
    check_db_status(clear=args.clear)

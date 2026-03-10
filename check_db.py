import os
import sys
import argparse
from unittest.mock import MagicMock

# Mock Logger to avoid dependency issues
sys.modules['utils'] = MagicMock()
from remotes.short_term.supabase_db import SupabaseUploader

def check_db_status(clear=False):
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) not set.")
        return

    try:
        uploader = SupabaseUploader()

        if clear:
            print("Clearing database...")
            uploader._clean_all_destinations()
            print("Database cleared.")

        client = uploader.client
        tables = ["stores", "products", "prices", "promotions", "processed_files"]
        print("Row Counts:")
        for table in tables:
            try:
                result = client.table(table).select("*", count="exact").limit(0).execute()
                print(f"  {table}: {result.count}")
            except Exception as e:
                print(f"  {table}: ERROR ({e})")

        print("Detailed Sampling:")
        try:
            result = (
                client.table("products")
                .select("*", count="exact")
                .not_.is_("manufacturer_name", "null")
                .neq("manufacturer_name", "")
                .limit(0)
                .execute()
            )
            print(f"  Products with manufacturer: {result.count}")
        except Exception as e:
            print(f"  Products with manufacturer: ERROR ({e})")

        try:
            result = (
                client.table("promotions")
                .select("*", count="exact")
                .gt("promotion_start_date", "2020-01-01")
                .limit(0)
                .execute()
            )
            print(f"  Promotions with start date: {result.count}")
        except Exception as e:
            print(f"  Promotions with start date: ERROR ({e})")

    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Clear the database tables before checking")
    args = parser.parse_args()
    check_db_status(clear=args.clear)

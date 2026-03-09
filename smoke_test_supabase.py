import os
import sys
from unittest.mock import MagicMock

# Mock Logger to avoid dependency issues during simple test
mock_logger = MagicMock()
sys.modules['utils'] = MagicMock()
sys.modules['utils'].Logger = mock_logger

from remotes.short_term.supabase_db import SupabaseUploader
from datetime import datetime

def run_smoke_test():
    url = os.getenv("SUPABASE_DATABASE_URL")
    if not url:
        print("❌ Error: SUPABASE_DATABASE_URL environment variable is not set.")
        return

    print(f"Connecting to: {url[:20]}...")
    
    try:
        uploader = SupabaseUploader(url)
        
        # 1. Test Store Upsert
        print("Testing Store upsert...")
        dummy_store = [{
            "content": {
                "ChainId": "999",
                "ChainName": "SmokeTestChain",
                "StoreId": "999",
                "StoreName": "SmokeTestStore",
                "City": "TestCity",
                "Address": "TestAddress"
            }
        }]
        uploader._upsert_stores(dummy_store)
        print("✅ Store upserted successfully (or updated).")

        # 2. Test Product & Price Upsert
        print("Testing Product & Price upsert...")
        dummy_price = [{
            "content": {
                "ChainId": "999",
                "StoreId": "999",
                "ItemCode": "SMOKE-123",
                "ItemName": "SmokeTestProduct",
                "ItemPrice": "9.99",
                "ManufacturerName": "SmokeTestManufacturer"
            }
        }]
        uploader._upsert_prices(dummy_price)
        print("✅ Product and Price upserted successfully.")

        print("\n🚀 Smoke test completed successfully! Your Supabase integration is working.")

    except Exception as e:
        print(f"❌ Smoke test failed: {e}")

if __name__ == "__main__":
    run_smoke_test()

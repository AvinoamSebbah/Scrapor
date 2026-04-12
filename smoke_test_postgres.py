import os
import sys
import warnings
from unittest.mock import MagicMock

# Mock Logger to avoid dependency issues during simple test
mock_logger = MagicMock()
sys.modules['utils'] = MagicMock()
sys.modules['utils'].Logger = mock_logger

# Disable SSL verification for local testing on Windows where certs are missing.
# This is only applied when NO_SSL_VERIFY=1 is set — never affects production.
if os.getenv("NO_SSL_VERIFY") == "1":
    import ssl
    _orig_create_default_context = ssl.create_default_context
    def _no_verify_context(*args, **kwargs):
        ctx = _orig_create_default_context(*args, **kwargs)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    ssl.create_default_context = _no_verify_context
    warnings.filterwarnings("ignore")
    print("⚠️  SSL verification DISABLED (local test only)")

from remotes.short_term.postgres_db import PostgresUploader
from datetime import datetime

def run_smoke_test():
    db_url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        print("\u274c Error: POSTGRESQL_URL (or DATABASE_URL) must be set.")
        return

    print(f"Connecting to PostgreSQL: {db_url[:30]}...")

    try:
        uploader = PostgresUploader()
        
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

        rows = uploader._run_query(
            """
            SELECT pp.product_id, pp.store_id, pp.price, pp.promo_price
            FROM product_prices pp
            JOIN products p ON p.id = pp.product_id
            JOIN stores s ON s.id = pp.store_id
            WHERE p.item_code=%s AND s.chain_id=%s AND s.store_id=%s
            LIMIT 1
            """,
            ("SMOKE-123", "999", "999"),
            fetch=True,
        )
        if not rows:
            raise RuntimeError("No row written to product_prices")

        print("✅ Normalized product_prices validated.")

        print("\nSmoke test completed successfully. PostgreSQL integration is working.")

    except Exception as e:
        print(f"❌ Smoke test failed: {e}")

if __name__ == "__main__":
    run_smoke_test()

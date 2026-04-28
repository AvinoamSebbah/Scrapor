"""
Migration: create observations table for price-drop notifications.
Run once: python create_observations_table.py
"""

import os
import sys
import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRESQL_URL")
if not DATABASE_URL:
    print("❌  DATABASE_URL or POSTGRESQL_URL env var required")
    sys.exit(1)

SQL = """
-- observations: user price-drop watch records
CREATE TABLE IF NOT EXISTS observations (
  id                  SERIAL PRIMARY KEY,
  user_id             TEXT NOT NULL,
  product_id          INTEGER NOT NULL,
  item_code           TEXT NOT NULL,
  city                TEXT NOT NULL,
  store_id            INTEGER,
  min_discount_pct    DECIMAL(5,2) NOT NULL,
  last_notified_price DECIMAL(10,2),
  last_notified_at    TIMESTAMPTZ,
  promo_expires_at    TIMESTAMPTZ,
  status              TEXT NOT NULL DEFAULT 'active',
  expires_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '6 months'),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT observations_status_check CHECK (status IN ('active', 'paused', 'stopped')),
  CONSTRAINT observations_user_product_city_unique UNIQUE (user_id, product_id, city)
);

CREATE INDEX IF NOT EXISTS idx_observations_item_city
  ON observations (item_code, city);

CREATE INDEX IF NOT EXISTS idx_observations_user
  ON observations (user_id);

CREATE INDEX IF NOT EXISTS idx_observations_active
  ON observations (status, expires_at)
  WHERE status = 'active';
"""

def main() -> None:
    print("Connecting to database…")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()
        print("✅  observations table created (or already exists)")
    except Exception as exc:
        conn.rollback()
        print(f"❌  Migration failed: {exc}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

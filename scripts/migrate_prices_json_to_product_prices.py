"""Backfill normalized product_prices from legacy prices.store_prices JSONB in batches.

Usage:
  python scripts/migrate_prices_json_to_product_prices.py
  python scripts/migrate_prices_json_to_product_prices.py --batch-size 5000

Requirements:
  POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL)
"""

import argparse
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values


def get_db_url() -> str:
    db_url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not db_url:
        raise ValueError("POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")
    return db_url


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy JSONB prices into product_prices")
    parser.add_argument("--batch-size", type=int, default=5000, help="Rows per batch from prices source")
    return parser.parse_args()


def migrate(batch_size: int) -> None:
    db_url = get_db_url()
    conn = None
    try:
        conn = psycopg2.connect(db_url, connect_timeout=30)
        conn.autocommit = False

        last_id = 0
        total_rows = 0
        started_at = datetime.utcnow()

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.tables
                  WHERE table_name='prices'
                )
                """
            )
            has_legacy_prices = bool(cur.fetchone()[0])
            if not has_legacy_prices:
                print("[ok] legacy prices table does not exist; migration skipped")
                conn.commit()
                return

            cur.execute(
                """
                SELECT EXISTS (
                  SELECT 1
                  FROM information_schema.columns
                  WHERE table_name='prices'
                    AND column_name='store_prices'
                )
                """
            )
            has_legacy_store_prices = bool(cur.fetchone()[0])
            if not has_legacy_store_prices:
                print("[ok] legacy prices.store_prices does not exist; migration skipped")
                conn.commit()
                return

            while True:
                cur.execute(
                    """
                    SELECT
                        p.id,
                        pr.id AS product_id,
                        s.id AS store_id,
                        CASE
                            WHEN sp.value ~ '^-?\\d+(\\.\\d+)?$' THEN sp.value::numeric
                            ELSE NULL
                        END AS price,
                        p.updated_at
                    FROM prices p
                    JOIN products pr ON pr.item_code = p.item_code
                    JOIN LATERAL jsonb_each_text(COALESCE(p.store_prices, '{}'::jsonb)) sp ON TRUE
                    JOIN stores s ON s.chain_id = p.chain_id AND s.store_id = sp.key
                    WHERE p.id > %s
                    ORDER BY p.id
                    LIMIT %s
                    """,
                    (last_id, batch_size),
                )
                rows = cur.fetchall()
                if not rows:
                    break

                payload = []
                max_id = last_id

                for source_id, product_id, store_id, price, updated_at in rows:
                    payload.append((product_id, store_id, price, None, updated_at))
                    if source_id > max_id:
                        max_id = source_id

                execute_values(
                    cur,
                    """
                    INSERT INTO product_prices (product_id, store_id, price, promo_price, updated_at)
                    VALUES %s
                    ON CONFLICT (product_id, store_id)
                    DO UPDATE SET
                        price = EXCLUDED.price,
                        promo_price = EXCLUDED.promo_price,
                        updated_at = NOW()
                    """,
                    payload,
                )

                conn.commit()
                total_rows += len(payload)
                last_id = max_id
                print(f"[i] migrated rows={total_rows} last_prices_id={last_id}")

        elapsed = (datetime.utcnow() - started_at).total_seconds()
        print(f"[ok] migration complete rows={total_rows} elapsed_s={elapsed:.2f}")

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        print(f"[error] migration failed: {exc}")
        raise
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    args = parse_args()
    try:
        migrate(args.batch_size)
    except Exception:
        sys.exit(1)

# -*- coding: utf-8 -*-
"""Build the store-level active promo cache used by /api/products/search.

Unlike a city summary table, this does not multiply rows by city. It stores one
row per product/store that currently has an active promo, then search can still
filter stores by any city live.

Environment:
  DATABASE_URL or POSTGRESQL_URL
"""

from __future__ import annotations

import os
import time

import psycopg2


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS product_store_current_promos (
    product_id INTEGER NOT NULL,
    store_id INTEGER NOT NULL,
    promo_price NUMERIC NOT NULL,
    chain_id TEXT,
    promotion_id TEXT,
    promotion_description TEXT,
    promotion_end_date DATE,
    additional_is_coupon TEXT,
    additional_restrictions TEXT,
    club_id TEXT,
    refreshed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, store_id)
)
"""

CREATE_INDEXES_SQL = [
    """
    CREATE INDEX IF NOT EXISTS idx_product_store_current_promos_store_product
    ON product_store_current_promos (store_id, product_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_product_store_current_promos_promo_price
    ON product_store_current_promos (promo_price)
    """,
]

REFRESH_SQL = """
CREATE TEMP TABLE tmp_product_store_current_promos ON COMMIT DROP AS
SELECT DISTINCT ON (psi.product_id, psi.store_id)
  psi.product_id,
  psi.store_id,
  psi.promo_price,
  psi.chain_id::text AS chain_id,
  psi.promotion_id::text AS promotion_id,
  p.promotion_description,
  psi.promotion_end_date,
  p.additional_is_coupon,
  p.additional_restrictions,
  p.club_id,
  NOW()::timestamp without time zone AS refreshed_at
FROM promotion_store_items psi
LEFT JOIN promotions p
  ON p.chain_id = psi.chain_id
 AND p.promotion_id = psi.promotion_id
WHERE psi.promo_price IS NOT NULL
  AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
ORDER BY
  psi.product_id,
  psi.store_id,
  psi.promo_price ASC NULLS LAST,
  psi.updated_at DESC NULLS LAST,
  psi.promotion_id ASC
"""


def db_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not value:
        raise SystemExit("Set POSTGRESQL_URL or DATABASE_URL")
    return value


def main() -> None:
    conn = psycopg2.connect(
        db_url(),
        connect_timeout=20,
        application_name="scrapor-refresh-current-promos-cache",
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30min'")
            cur.execute(CREATE_TABLE_SQL)
            for statement in CREATE_INDEXES_SQL:
                cur.execute(statement)
            conn.commit()

            started_at = time.perf_counter()
            print("[current-promos] refreshing product_store_current_promos")
            cur.execute("DROP TABLE IF EXISTS tmp_product_store_current_promos")
            cur.execute(REFRESH_SQL)
            cur.execute("SELECT COUNT(*) FROM tmp_product_store_current_promos")
            row_count = cur.fetchone()[0]
            cur.execute("TRUNCATE product_store_current_promos")
            cur.execute(
                """
                INSERT INTO product_store_current_promos (
                  product_id,
                  store_id,
                  promo_price,
                  chain_id,
                  promotion_id,
                  promotion_description,
                  promotion_end_date,
                  additional_is_coupon,
                  additional_restrictions,
                  club_id,
                  refreshed_at
                )
                SELECT
                  product_id,
                  store_id,
                  promo_price,
                  chain_id,
                  promotion_id,
                  promotion_description,
                  promotion_end_date,
                  additional_is_coupon,
                  additional_restrictions,
                  club_id,
                  refreshed_at
                FROM tmp_product_store_current_promos
                """
            )
            cur.execute("ANALYZE product_store_current_promos")
            conn.commit()
            elapsed = time.perf_counter() - started_at
            print(f"[current-promos] rows={row_count} elapsed={elapsed:.1f}s")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

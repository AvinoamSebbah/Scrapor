#!/usr/bin/env python3
"""
Backfill bundle promotions chain by chain.

Purpose
-------
Some promotions are stored as "X for Y" bundles. Example: 3 for 20 NIS.
The normalized promo table must store the effective unit price: 20 / 3.

This script is resumable:
- It keeps a JSON state file with per-chain todo/done/failed status.
- It uses ON CONFLICT DO NOTHING, so re-running a chain cannot duplicate rows.
- It writes rollback SQL per completed chain for the rows inserted by that run.

Usage
-----
python scripts/backfill_bundle_promos_by_chain.py --dry-run
python scripts/backfill_bundle_promos_by_chain.py
python scripts/backfill_bundle_promos_by_chain.py --chain-id 7290103152017
python scripts/backfill_bundle_promos_by_chain.py --retry-failed
python scripts/backfill_bundle_promos_by_chain.py --skip-chain-id 7290103152017
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


SCRIPT_DIR = Path(__file__).resolve().parent
STATE_PATH = SCRIPT_DIR / "bundle_promo_backfill_state.json"
ROLLBACK_DIR = SCRIPT_DIR / "bundle_promo_backfill_rollbacks"
DB_STATE_TABLE = "bundle_promo_backfill_chain_state"


def db_url() -> str:
    value = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not value:
        print("Missing POSTGRESQL_URL / DATABASE_URL / SUPABASE_DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    return value


def connect():
    conn = psycopg2.connect(
        db_url(),
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        cursor_factory=RealDictCursor,
        application_name="bundle_promos_backfill_by_chain",
    )
    conn.autocommit = False
    return conn


def timeout_sql(seconds: int) -> str:
    if seconds <= 0:
        return "SET LOCAL statement_timeout = 0;"
    return f"SET LOCAL statement_timeout = '{int(seconds)}s';"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"created_at": now_iso(), "chains": {}}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def sql_quote(value: Any) -> str:
    return str(value).replace("'", "''")


def get_chains(conn, only_chain_id: str | None = None) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              p.chain_id,
              COALESCE(NULLIF(MAX(s.chain_name), ''), p.chain_id) AS chain_name,
              COUNT(*)::int AS active_promos_with_items
            FROM promotions p
            LEFT JOIN stores s ON s.chain_id = p.chain_id
            WHERE p.items IS NOT NULL
              AND p.items <> '[]'::jsonb
              AND (%s::text IS NULL OR p.chain_id = %s::text)
              AND (p.promotion_start_date IS NULL OR p.promotion_start_date <= CURRENT_DATE)
              AND (p.promotion_end_date IS NULL OR p.promotion_end_date >= CURRENT_DATE)
            GROUP BY p.chain_id
            ORDER BY active_promos_with_items ASC, p.chain_id ASC;
            """,
            (only_chain_id, only_chain_id),
        )
        return list(cur.fetchall())


def ensure_db_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_STATE_TABLE} (
              chain_id text PRIMARY KEY,
              chain_name text,
              status text NOT NULL,
              inserted_count integer,
              error text,
              run_id text,
              started_at timestamptz,
              updated_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )


def db_done_chains(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT chain_id FROM {DB_STATE_TABLE} WHERE status = 'done';")
        return {str(row["chain_id"]) for row in cur.fetchall()}


def mark_db_state(
    conn,
    chain_id: str,
    chain_name: str,
    status: str,
    run_id: str,
    inserted_count: int | None = None,
    error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {DB_STATE_TABLE} (
              chain_id, chain_name, status, inserted_count, error, run_id, started_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, CASE WHEN %s = 'running' THEN now() ELSE NULL END, now())
            ON CONFLICT (chain_id) DO UPDATE SET
              chain_name = EXCLUDED.chain_name,
              status = EXCLUDED.status,
              inserted_count = EXCLUDED.inserted_count,
              error = EXCLUDED.error,
              run_id = EXCLUDED.run_id,
              started_at = CASE WHEN EXCLUDED.status = 'running' THEN now() ELSE {DB_STATE_TABLE}.started_at END,
              updated_at = now();
            """,
            (chain_id, chain_name, status, inserted_count, error, run_id, status),
        )


def dry_run_chain(conn, chain_id: str, statement_timeout_seconds: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(timeout_sql(statement_timeout_seconds))
        cur.execute(
            """
            WITH extracted AS MATERIALIZED (
              SELECT DISTINCT
                p.chain_id,
                p.promotion_id,
                sid.sid::int AS store_db_id,
                COALESCE(q.obj->>'itemcode', q.obj->>'ItemCode') AS item_code,
                CASE
                  WHEN REPLACE(COALESCE(q.obj->>'minqty', q.obj->>'MinQty'), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                  THEN REPLACE(COALESCE(q.obj->>'minqty', q.obj->>'MinQty'), ',', '.')::numeric
                END AS min_qty,
                CASE
                  WHEN REPLACE(COALESCE(q.obj->>'discountedprice', q.obj->>'DiscountedPrice'), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                  THEN REPLACE(COALESCE(q.obj->>'discountedprice', q.obj->>'DiscountedPrice'), ',', '.')::numeric
                END AS discounted_price
              FROM promotions p
              JOIN LATERAL unnest(COALESCE(p.available_in_store_ids, '{}')) sid(sid) ON TRUE
              JOIN LATERAL (
                SELECT jsonb_path_query(p.items, '$.** ? (@.itemcode != null)') AS obj
                UNION ALL
                SELECT jsonb_path_query(p.items, '$.** ? (@.ItemCode != null)') AS obj
              ) q ON TRUE
              WHERE p.chain_id = %s
                AND (p.promotion_start_date IS NULL OR p.promotion_start_date <= CURRENT_DATE)
                AND (p.promotion_end_date IS NULL OR p.promotion_end_date >= CURRENT_DATE)
                AND p.items IS NOT NULL
                AND p.items <> '[]'::jsonb
            ),
            candidates AS (
              SELECT e.chain_id, e.promotion_id, pr.id AS product_id, pp.store_id
              FROM extracted e
              JOIN products pr ON pr.item_code = e.item_code
              JOIN product_prices pp ON pp.product_id = pr.id AND pp.store_id = e.store_db_id
              WHERE e.item_code IS NOT NULL
                AND e.min_qty IS NOT NULL
                AND e.min_qty > 1
                AND e.discounted_price IS NOT NULL
                AND e.discounted_price > 0
                AND pp.price IS NOT NULL
                AND pp.price > 0
                AND e.discounted_price >= pp.price
                AND (e.discounted_price / e.min_qty) < pp.price
                AND (e.discounted_price / e.min_qty) >= (pp.price * 0.05)
              GROUP BY e.chain_id, e.promotion_id, pr.id, pp.store_id
            )
            SELECT
              COUNT(*)::int AS candidate_rows,
              COUNT(*) FILTER (
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM promotion_store_items psi
                  WHERE psi.chain_id = candidates.chain_id
                    AND psi.promotion_id = candidates.promotion_id
                    AND psi.product_id = candidates.product_id
                    AND psi.store_id = candidates.store_id
                )
              )::int AS missing_rows,
              COUNT(DISTINCT promotion_id)::int AS promos,
              COUNT(DISTINCT product_id)::int AS products,
              COUNT(DISTINCT store_id)::int AS stores
            FROM candidates;
            """,
            (chain_id,),
        )
        return dict(cur.fetchone())


def backfill_chain(conn, chain_id: str, statement_timeout_seconds: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(timeout_sql(statement_timeout_seconds))
        cur.execute(
            """
            WITH extracted AS MATERIALIZED (
              SELECT DISTINCT
                p.chain_id,
                p.promotion_id,
                p.promotion_end_date,
                sid.sid::int AS store_db_id,
                COALESCE(q.obj->>'itemcode', q.obj->>'ItemCode') AS item_code,
                CASE
                  WHEN REPLACE(COALESCE(q.obj->>'minqty', q.obj->>'MinQty'), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                  THEN REPLACE(COALESCE(q.obj->>'minqty', q.obj->>'MinQty'), ',', '.')::numeric
                END AS min_qty,
                CASE
                  WHEN REPLACE(COALESCE(q.obj->>'discountedprice', q.obj->>'DiscountedPrice'), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                  THEN REPLACE(COALESCE(q.obj->>'discountedprice', q.obj->>'DiscountedPrice'), ',', '.')::numeric
                END AS discounted_price
              FROM promotions p
              JOIN LATERAL unnest(COALESCE(p.available_in_store_ids, '{}')) sid(sid) ON TRUE
              JOIN LATERAL (
                SELECT jsonb_path_query(p.items, '$.** ? (@.itemcode != null)') AS obj
                UNION ALL
                SELECT jsonb_path_query(p.items, '$.** ? (@.ItemCode != null)') AS obj
              ) q ON TRUE
              WHERE p.chain_id = %s
                AND (p.promotion_start_date IS NULL OR p.promotion_start_date <= CURRENT_DATE)
                AND (p.promotion_end_date IS NULL OR p.promotion_end_date >= CURRENT_DATE)
                AND p.items IS NOT NULL
                AND p.items <> '[]'::jsonb
            ),
            candidates AS (
              SELECT
                e.chain_id,
                e.promotion_id,
                pr.id AS product_id,
                pp.store_id,
                MIN(e.discounted_price / e.min_qty) AS promo_price,
                e.promotion_end_date
              FROM extracted e
              JOIN products pr ON pr.item_code = e.item_code
              JOIN product_prices pp ON pp.product_id = pr.id AND pp.store_id = e.store_db_id
              WHERE e.item_code IS NOT NULL
                AND e.min_qty IS NOT NULL
                AND e.min_qty > 1
                AND e.discounted_price IS NOT NULL
                AND e.discounted_price > 0
                AND pp.price IS NOT NULL
                AND pp.price > 0
                AND e.discounted_price >= pp.price
                AND (e.discounted_price / e.min_qty) < pp.price
                AND (e.discounted_price / e.min_qty) >= (pp.price * 0.05)
              GROUP BY e.chain_id, e.promotion_id, pr.id, pp.store_id, e.promotion_end_date
            )
            INSERT INTO promotion_store_items (
              chain_id, promotion_id, product_id, store_id, promo_price, promotion_end_date, updated_at
            )
            SELECT chain_id, promotion_id, product_id, store_id, promo_price, promotion_end_date, NOW()
            FROM candidates
            ON CONFLICT (chain_id, promotion_id, product_id, store_id) DO NOTHING
            RETURNING chain_id, promotion_id, product_id, store_id;
            """,
            (chain_id,),
        )
        return list(cur.fetchall())


def write_rollback(chain_id: str, inserted: list[dict[str, Any]]) -> str | None:
    if not inserted:
        return None
    ROLLBACK_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    path = ROLLBACK_DIR / f"rollback_bundle_promos_{chain_id}_{stamp}.sql"
    values = ",\n".join(
        f"('{sql_quote(r['chain_id'])}','{sql_quote(r['promotion_id'])}',{int(r['product_id'])},{int(r['store_id'])})"
        for r in inserted
    )
    path.write_text(
        f"""DELETE FROM promotion_store_items AS psi
USING (VALUES
{values}
) AS v(chain_id, promotion_id, product_id, store_id)
WHERE psi.chain_id = v.chain_id
  AND psi.promotion_id = v.promotion_id
  AND psi.product_id = v.product_id
  AND psi.store_id = v.store_id;
""",
        encoding="utf-8",
    )
    return str(path)


def should_run(entry: dict[str, Any] | None, retry_failed: bool) -> bool:
    if not entry:
        return True
    if entry.get("status") == "done":
        return False
    if entry.get("status") == "failed":
        return retry_failed
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chain-id", help="Process only one chain.")
    parser.add_argument("--dry-run", action="store_true", help="Only estimate; no writes.")
    parser.add_argument("--retry-failed", action="store_true", help="Retry chains marked failed.")
    parser.add_argument("--limit", type=int, default=0, help="Max number of chains to process this run.")
    parser.add_argument("--skip-chain-id", action="append", default=[], help="Mark a chain as already handled and skip it.")
    parser.add_argument("--no-db-state", action="store_true", help="Do not use the DB-backed chain state table.")
    parser.add_argument(
        "--statement-timeout-seconds",
        type=int,
        default=0,
        help="Per-chain SQL timeout. 0 means no timeout.",
    )
    args = parser.parse_args()

    state = load_state()
    processed = 0
    run_id = os.getenv("GITHUB_RUN_ID") or f"local-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    skip_chain_ids = {str(value) for value in args.skip_chain_id}

    with connect() as conn:
      if not args.no_db_state:
        ensure_db_state_table(conn)
        conn.commit()

      chains = get_chains(conn, args.chain_id)
      done_in_db = db_done_chains(conn) if not args.no_db_state else set()
      for chain in chains:
        chain_id = str(chain["chain_id"])
        chain_name = chain.get("chain_name") or chain_id
        entry = state["chains"].get(chain_id)
        if chain_id in skip_chain_ids:
            print(f"skip {chain_id} {chain_name}: explicit skip")
            state["chains"][chain_id] = {
                "chain_name": chain_name,
                "status": "done",
                "inserted": 0,
                "note": "Explicitly skipped.",
                "updated_at": now_iso(),
            }
            save_state(state)
            if not args.no_db_state and not args.dry_run:
                mark_db_state(conn, chain_id, chain_name, "done", run_id, inserted_count=0)
                conn.commit()
            continue
        if not args.dry_run and chain_id in done_in_db:
            print(f"skip {chain_id} {chain_name}: status=done in DB state")
            continue
        if not args.dry_run and not should_run(entry, args.retry_failed):
            print(f"skip {chain_id} {chain_name}: status={entry.get('status')}")
            continue
        if args.limit and processed >= args.limit:
            break

        print(f"\nchain {chain_id} {chain_name} promos={chain['active_promos_with_items']}")
        started = time.time()
        state["chains"][chain_id] = {
            "chain_name": chain_name,
            "status": "running",
            "started_at": now_iso(),
        }
        save_state(state)
        if not args.no_db_state and not args.dry_run:
            mark_db_state(conn, chain_id, chain_name, "running", run_id)
            conn.commit()

        try:
            if args.dry_run:
                stats = dry_run_chain(conn, chain_id, args.statement_timeout_seconds)
                conn.rollback()
                print(f"dry-run {chain_id}: {stats}")
                state["chains"][chain_id] = {
                    "chain_name": chain_name,
                    "status": "dry_run",
                    "stats": stats,
                    "duration_sec": round(time.time() - started, 1),
                    "updated_at": now_iso(),
                }
            else:
                inserted = backfill_chain(conn, chain_id, args.statement_timeout_seconds)
                rollback_path = write_rollback(chain_id, inserted)
                if not args.no_db_state:
                    mark_db_state(conn, chain_id, chain_name, "done", run_id, inserted_count=len(inserted))
                conn.commit()
                print(f"done {chain_id}: inserted={len(inserted)} rollback={rollback_path}")
                state["chains"][chain_id] = {
                    "chain_name": chain_name,
                    "status": "done",
                    "inserted": len(inserted),
                    "rollback_path": rollback_path,
                    "duration_sec": round(time.time() - started, 1),
                    "updated_at": now_iso(),
                }
            processed += 1
        except Exception as exc:
            conn.rollback()
            print(f"failed {chain_id}: {exc}", file=sys.stderr)
            if not args.no_db_state and not args.dry_run:
                mark_db_state(conn, chain_id, chain_name, "failed", run_id, error=str(exc))
                conn.commit()
            state["chains"][chain_id] = {
                "chain_name": chain_name,
                "status": "failed",
                "error": str(exc),
                "duration_sec": round(time.time() - started, 1),
                "updated_at": now_iso(),
            }
        finally:
            save_state(state)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

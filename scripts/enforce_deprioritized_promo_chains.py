#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Apply the promo deprioritization list to live SQL functions.

The app also applies this penalty at API read time. This script keeps the
database-side cache builder aligned for the next W5 refresh.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


SUPER_PHARM_CHAIN_ID = "7290172900007"
SUPER_PHARM_CHAIN_NAME = "סופר פארם ישראל"
TOP_CACHE_LOCK_ID = 55555


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _db_url() -> str:
    url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if url:
        return url

    workspace_root = Path(__file__).resolve().parents[2]
    local_env = _read_env_file(workspace_root / "web-backend" / ".env")
    url = local_env.get("POSTGRESQL_URL") or local_env.get("DATABASE_URL") or local_env.get("SUPABASE_DATABASE_URL")
    if url:
        return url

    raise SystemExit("Set POSTGRESQL_URL or DATABASE_URL")


def _patch_refresh_function(definition: str) -> tuple[str, bool]:
    if SUPER_PHARM_CHAIN_ID in definition:
        return definition, False

    needle = "WHEN LOWER(COALESCE(lsp.chain_name, '')) = 'be'"
    replacement = (
        f"WHEN lsp.chain_id = '{SUPER_PHARM_CHAIN_ID}'\n"
        f"                       OR COALESCE(lsp.chain_name, '') = '{SUPER_PHARM_CHAIN_NAME}'\n"
        "                       OR LOWER(COALESCE(lsp.chain_name, '')) = 'be'"
    )
    if needle not in definition:
        raise RuntimeError("Could not find deprioritization CASE block in refresh_top_promotions_cache")

    return definition.replace(needle, replacement, 1), True


def main() -> int:
    conn = psycopg2.connect(
        _db_url(),
        connect_timeout=20,
        cursor_factory=RealDictCursor,
        application_name="scrapor-enforce-deprioritized-promo-chains",
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (TOP_CACHE_LOCK_ID,))
            cur.execute("SET LOCAL statement_timeout = '10min'")
            cur.execute(
                """
                SELECT p.oid::regprocedure::text AS signature,
                       pg_get_functiondef(p.oid) AS definition
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.proname = 'refresh_top_promotions_cache'
                """
            )
            rows = cur.fetchall()
            if not rows:
                raise RuntimeError("refresh_top_promotions_cache not found")

            patched = []
            for row in rows:
                updated, changed = _patch_refresh_function(row["definition"])
                if changed:
                    cur.execute(updated)
                    patched.append(row["signature"])

            cur.execute("SELECT pg_advisory_unlock(%s)", (TOP_CACHE_LOCK_ID,))
            conn.commit()

        if patched:
            for signature in patched:
                print(f"patched={signature}")
        else:
            print("patched=0 already_present=true")
        print(f"deprioritized_chain_id={SUPER_PHARM_CHAIN_ID}")
        print(f"deprioritized_chain_name={SUPER_PHARM_CHAIN_NAME}")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

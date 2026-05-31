#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Enforce top_promotions_cache as all-time only.

This script is intentionally narrow: it only touches the derived top promotions
cache and the related SQL functions. It does not rebuild source promotion data.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor


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
        value = value.strip().strip('"').strip("'")
        values[key.strip()] = value
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


def _force_function_all_time(definition: str, function_name: str) -> str:
    updated = definition
    updated = updated.replace("p_window_hours integer DEFAULT 24", "p_window_hours integer DEFAULT 0")

    updated = re.sub(
        r"v_window_hours\s+INT\s*:=\s*CASE\s+WHEN\s+p_window_hours\s+IS\s+NULL\s+THEN\s+0\s+"
        r"WHEN\s+p_window_hours\s*<=\s*0\s+THEN\s+0\s+ELSE\s+p_window_hours\s+END;",
        "v_window_hours INT := 0;",
        updated,
        flags=re.IGNORECASE | re.DOTALL,
    )
    updated = re.sub(
        r"v_window_hours\s+INT\s*:=\s*GREATEST\s*\(\s*COALESCE\s*\(\s*p_window_hours\s*,\s*24\s*\)\s*,\s*[01]\s*\)\s*;",
        "v_window_hours INT := 0;",
        updated,
        flags=re.IGNORECASE | re.DOTALL,
    )

    if function_name == "get_top_city_promotions":
        updated = re.sub(
            r"v_window_hours\s+INT\s*;",
            "v_window_hours INT := 0;",
            updated,
            count=1,
            flags=re.IGNORECASE,
        )
        updated = re.sub(
            r"\n\s*-- p_window_hours = 0 = .*?IF v_window_hours IS NULL THEN v_window_hours := 24; END IF;",
            "\n          -- top_promotions_cache is all-time only: ignore legacy requested windows.\n"
            "          v_window_hours := 0;",
            updated,
            flags=re.DOTALL,
        )
        updated = re.sub(
            r"\n\s*-- Snap window_hours.*?IF v_window_hours IS NULL THEN\s+v_window_hours := 24;\s+END IF;",
            "\n          -- top_promotions_cache is all-time only: ignore legacy requested windows.\n"
            "          v_window_hours := 0;",
            updated,
            flags=re.DOTALL,
        )

    if function_name == "refresh_top_promotions_cache" and "window_hours <> 0" not in updated:
        updated = updated.replace(
            "DELETE FROM top_promotions_cache WHERE window_hours = v_window_hours;",
            "DELETE FROM top_promotions_cache WHERE window_hours <> 0;\n\n"
            "          DELETE FROM top_promotions_cache WHERE window_hours = v_window_hours;",
            1,
        )

    return updated


def _patch_functions(cur) -> list[str]:
    cur.execute(
        """
        SELECT p.oid::regprocedure::text AS signature,
               p.proname AS function_name,
               pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
          AND p.proname IN ('refresh_top_promotions_cache', 'get_top_city_promotions')
        ORDER BY p.proname, p.oid::text
        """
    )
    rows = cur.fetchall()

    patched: list[str] = []
    for row in rows:
        updated = _force_function_all_time(row["definition"], row["function_name"])
        if updated != row["definition"]:
            cur.execute(updated)
            patched.append(row["signature"])
    return patched


def main() -> int:
    conn = psycopg2.connect(
        _db_url(),
        connect_timeout=20,
        cursor_factory=RealDictCursor,
        application_name="scrapor-enforce-top-promos-all-time",
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (TOP_CACHE_LOCK_ID,))
            cur.execute("SET LOCAL statement_timeout = '30min'")

            cur.execute("SELECT COUNT(*)::bigint AS cnt FROM top_promotions_cache WHERE window_hours <> 0")
            legacy_before = int(cur.fetchone()["cnt"])

            cur.execute("DELETE FROM top_promotions_cache WHERE window_hours <> 0")
            legacy_deleted = cur.rowcount

            cur.execute("ALTER TABLE top_promotions_cache ALTER COLUMN window_hours SET DEFAULT 0")
            cur.execute(
                """
                ALTER TABLE top_promotions_cache
                DROP CONSTRAINT IF EXISTS top_promotions_cache_window_all_time_chk
                """
            )
            cur.execute(
                """
                ALTER TABLE top_promotions_cache
                ADD CONSTRAINT top_promotions_cache_window_all_time_chk
                CHECK (window_hours = 0) NOT VALID
                """
            )
            cur.execute(
                """
                ALTER TABLE top_promotions_cache
                VALIDATE CONSTRAINT top_promotions_cache_window_all_time_chk
                """
            )

            patched = _patch_functions(cur)

            cur.execute(
                """
                SELECT window_hours, scope_type, COUNT(*)::bigint AS rows
                FROM top_promotions_cache
                GROUP BY window_hours, scope_type
                ORDER BY window_hours, scope_type
                """
            )
            summary = cur.fetchall()

            cur.execute("SELECT pg_advisory_unlock(%s)", (TOP_CACHE_LOCK_ID,))
            conn.commit()

        print(f"legacy_rows_before={legacy_before}")
        print(f"legacy_rows_deleted={legacy_deleted}")
        print(f"patched_functions={len(patched)}")
        for signature in patched:
            print(f"patched={signature}")
        for row in summary:
            print(f"window={row['window_hours']} scope={row['scope_type']} rows={row['rows']}")

        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

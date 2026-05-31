#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run scrapor store cleanup SQL against the configured PostgreSQL database."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2


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
    env_values = _read_env_file(workspace_root / "web-backend" / ".env")
    url = env_values.get("POSTGRESQL_URL") or env_values.get("DATABASE_URL") or env_values.get("SUPABASE_DATABASE_URL")
    if url:
        return url

    raise SystemExit("Set POSTGRESQL_URL or DATABASE_URL")


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    cleanup_sql_path = script_dir / "cleanup_stores.sql"
    cleanup_sql = cleanup_sql_path.read_text(encoding="utf-8")

    conn = psycopg2.connect(
        _db_url(),
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        application_name="scrapor-apply-store-cleanup",
    )
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(cleanup_sql)
        print(f"cleanup_applied={cleanup_sql_path}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

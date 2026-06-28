#!/usr/bin/env python3
"""Build Bot 1 scrape reports for GitHub Actions and Kamatera runs."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegram_notify import escape, send_message


def db_url() -> str | None:
    return os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")


def fetch_db_metrics(hours: int) -> dict[str, Any]:
    url = db_url()
    if not url:
        return {"available": False, "reason": "POSTGRESQL_URL missing"}

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    metrics: dict[str, Any] = {"available": True, "since": since.isoformat()}
    try:
        with psycopg2.connect(url, connect_timeout=15, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      COALESCE(file_type, 'unknown') AS file_type,
                      COALESCE(store_name, chain_name, chain_id, 'unknown') AS store,
                      COUNT(*)::int AS files,
                      COALESCE(SUM(record_count), 0)::bigint AS rows
                    FROM processed_files
                    WHERE processed_at >= %s
                    GROUP BY 1, 2
                    ORDER BY rows DESC, files DESC
                    LIMIT 25
                    """,
                    (since,),
                )
                metrics["processed_files"] = [dict(row) for row in cur.fetchall()]

                for table, field in (
                    ("products", "created_at"),
                    ("promotions", "created_at"),
                    ("product_prices", "updated_at"),
                    ("stores", "created_at"),
                ):
                    try:
                        cur.execute(f"SELECT COUNT(*)::bigint AS count FROM {table} WHERE {field} >= %s", (since,))
                        metrics[table] = int(cur.fetchone()["count"])
                    except Exception as exc:
                        metrics[table] = f"unavailable: {exc}"
                        conn.rollback()
    except Exception as exc:
        return {"available": False, "reason": str(exc)}
    return metrics


def read_summary(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    summary_path = Path(path)
    if not summary_path.exists():
        return {"summary_missing": str(summary_path)}
    try:
        return json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"summary_error": str(exc)}


def short_list(values: list[str], limit: int = 12) -> str:
    if not values:
        return "none"
    shown = values[:limit]
    suffix = "" if len(values) <= limit else f" +{len(values) - limit}"
    return ", ".join(shown) + suffix


def format_db_block(metrics: dict[str, Any]) -> str:
    if not metrics.get("available"):
        return f"DB: unavailable ({escape(metrics.get('reason'))})"

    rows = metrics.get("processed_files") or []
    total_files = sum(int(row.get("files") or 0) for row in rows)
    total_records = sum(int(row.get("rows") or 0) for row in rows)
    no_data_stores = [str(row["store"]) for row in rows if int(row.get("rows") or 0) == 0]
    by_type: dict[str, int] = {}
    for row in rows:
        by_type[str(row["file_type"])] = by_type.get(str(row["file_type"]), 0) + int(row.get("rows") or 0)
    type_text = ", ".join(f"{escape(k)}={v:,}" for k, v in sorted(by_type.items())) or "none"

    return "\n".join(
        [
            f"DB files: <b>{total_files}</b> | rows: <b>{total_records:,}</b>",
            f"By type: {type_text}",
            f"New products: <b>{escape(metrics.get('products'))}</b> | promos: <b>{escape(metrics.get('promotions'))}</b>",
            f"Updated prices: <b>{escape(metrics.get('product_prices'))}</b> | stores: <b>{escape(metrics.get('stores'))}</b>",
            f"No-row stores: {escape(short_list(no_data_stores))}",
        ]
    )


def build_message(args: argparse.Namespace) -> str:
    summary = read_summary(args.summary_file)
    metrics = fetch_db_metrics(args.hours)
    status = args.status or summary.get("status") or "unknown"
    icon = "✅" if status == "success" else "❌" if status == "failure" else "ℹ️"
    title = args.title or ("Kamatera scrape" if args.source == "kamatera" else "GitHub scrape")
    host = summary.get("host") or socket.gethostname()

    parts = [
        f"{icon} <b>Agali Scrapor</b> · {escape(title)}",
        f"Source: <b>{escape(args.source)}</b> | status: <b>{escape(status)}</b>",
    ]
    if args.store or summary.get("store"):
        parts.append(f"Store: <b>{escape(args.store or summary.get('store'))}</b>")
    if args.workflow:
        parts.append(f"Workflow: {escape(args.workflow)}")
    if args.run_url:
        parts.append(f"Run: {escape(args.run_url)}")
    if args.source == "kamatera":
        parts.append(f"Host: {escape(host)}")
    if summary:
        duration = summary.get("duration_seconds")
        if duration is not None:
            parts.append(f"Duration: {float(duration):.0f}s")
        if summary.get("new_files") is not None or summary.get("new_rows") is not None:
            parts.append(f"Kamatera output: files={escape(summary.get('new_files', 0))}, rows={escape(summary.get('new_rows', 0))}")
        if summary.get("error"):
            parts.append(f"Error: <code>{escape(str(summary['error'])[:700])}</code>")
    parts.append(format_db_block(metrics))
    return "\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send an Agali Scrapor Telegram report.")
    parser.add_argument("--source", choices=["github", "kamatera"], default="github")
    parser.add_argument("--status", choices=["success", "failure", "cancelled", "unknown"])
    parser.add_argument("--title")
    parser.add_argument("--workflow")
    parser.add_argument("--store")
    parser.add_argument("--run-url")
    parser.add_argument("--summary-file")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    send_message("scrapor", build_message(args), dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

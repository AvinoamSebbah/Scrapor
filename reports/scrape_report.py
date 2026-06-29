#!/usr/bin/env python3
"""Build Bot 1 scrape reports for GitHub Actions and Kamatera runs."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegram_notify import escape, send_message


GITHUB_STORES = [
    "BAREKET",
    "YAYNO_BITAN_AND_CARREFOUR",
    "COFIX",
    "CITY_MARKET_KIRYATGAT",
    "CITY_MARKET_SHOPS",
    "DOR_ALON",
    "GOOD_PHARM",
    "HAZI_HINAM",
    "HET_COHEN",
    "KESHET",
    "KING_STORE",
    "MAAYAN_2000",
    "MAHSANI_ASHUK",
    "NETIV_HASED",
    "MESHMAT_YOSEF_1",
    "MESHMAT_YOSEF_2",
    "OSHER_AD",
    "POLIZER",
    "RAMI_LEVY",
    "SALACH_DABACH",
    "SHEFA_BARCART_ASHEM",
    "SHUFERSAL",
    "SHUK_AHIR",
    "STOP_MARKET",
    "SUPER_PHARM",
    "SUPER_YUDA",
    "SUPER_SAPIR",
    "FRESH_MARKET_AND_SUPER_DOSH",
    "QUIK",
    "TIV_TAAM",
    "VICTORY",
    "YELLOW",
    "YOHANANOF",
    "ZOL_VEBEGADOL",
    "WOLT",
]


def normalize_store(value: Any) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


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
                      COALESCE(NULLIF(file_type, ''), 'unknown') AS file_type,
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

                cur.execute(
                    """
                    SELECT
                      COALESCE(store_name, chain_name, chain_id, 'unknown') AS store,
                      COALESCE(NULLIF(file_type, ''), 'unknown') AS file_type,
                      COUNT(*)::int AS files,
                      COALESCE(SUM(record_count), 0)::bigint AS rows
                    FROM processed_files
                    WHERE processed_at >= %s
                    GROUP BY 1, 2
                    ORDER BY store ASC, rows DESC
                    """,
                    (since,),
                )
                metrics["processed_by_store"] = [dict(row) for row in cur.fetchall()]

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
        file_type = str(row["file_type"] or "unknown")
        by_type[file_type] = by_type.get(file_type, 0) + int(row.get("rows") or 0)
    type_text = ", ".join(f"{escape(k)}={v:,}" for k, v in sorted(by_type.items())) or "none"

    return "\n".join(
        [
            "📊 <b>DB dernières heures</b>",
            f"📦 Fichiers traités: <b>{total_files}</b> | lignes traitées: <b>{total_records:,}</b>",
            f"🧾 Lignes par type: {type_text}",
            f"🛒 Produits créés: <b>{escape(metrics.get('products'))}</b> | promos créées: <b>{escape(metrics.get('promotions'))}</b>",
            f"💸 Prix touchés: <b>{escape(metrics.get('product_prices'))}</b> | magasins créés: <b>{escape(metrics.get('stores'))}</b>",
            f"⚠️ Magasins sans ligne traitée: {escape(short_list(no_data_stores))}",
        ]
    )


def fetch_notification_metrics() -> dict[str, Any]:
    url = db_url()
    if not url:
        return {"available": False, "reason": "POSTGRESQL_URL missing"}
    try:
        with psycopg2.connect(url, connect_timeout=15, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM observations
                    WHERE status = 'active'
                      AND expires_at > NOW()
                    """
                )
                return {"available": True, "active_observations": int(cur.fetchone()["count"])}
    except Exception as exc:
        return {"available": False, "reason": str(exc)}


def format_notification_block(summary: dict[str, Any]) -> str:
    fallback = fetch_notification_metrics()
    active = summary.get("active_observations")
    if active is None and fallback.get("available"):
        active = fallback.get("active_observations")
    if active is None:
        active = "unknown"

    sent = summary.get("observations_notified", 0)
    reason = summary.get("reason")
    lines = [
        f"📨 Notifications envoyées: <b>{escape(sent)}</b>",
        f"⏳ Notifications inscrites en attente: <b>{escape(active)}</b>",
    ]
    if reason:
        lines.append(f"ℹ️ Note: {escape(reason)}")
    return "\n".join(lines)


def github_json(path: str) -> dict[str, Any] | None:
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_MONITOR_TOKEN")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        return None
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception:
        return None


def github_w3_store_statuses() -> dict[str, str]:
    """Best-effort mapping of W3 store job conclusions for the current W2 cycle."""
    current_run = os.getenv("GITHUB_RUN_ID")
    if not current_run:
        return {}
    current = github_json(f"/actions/runs/{current_run}")
    created_at = current.get("created_at") if current else None
    if not created_at:
        return {}
    runs = github_json("/actions/workflows/W3_upload.yml/runs?per_page=100")
    statuses: dict[str, str] = {}
    for run in (runs or {}).get("workflow_runs", []):
        if str(run.get("created_at", "")) < str(created_at):
            continue
        jobs = github_json(f"/actions/runs/{run.get('id')}/jobs?per_page=20")
        for job in (jobs or {}).get("jobs", []):
            name = str(job.get("name") or "")
            if name.startswith("Upload ") and " -> PostgreSQL" in name:
                store = name.removeprefix("Upload ").split(" -> PostgreSQL", 1)[0]
                statuses[store] = str(job.get("conclusion") or job.get("status") or "unknown")
    return statuses


def format_github_store_lines(metrics: dict[str, Any]) -> str:
    rows = metrics.get("processed_by_store") or []
    statuses = github_w3_store_statuses()
    if not statuses:
        return "🧺 <b>Magasins GitHub</b>\nℹ️ W3 status unavailable; using DB totals only."

    row_totals: dict[str, int] = {}
    normalized_rows = [(normalize_store(row.get("store")), int(row.get("rows") or 0)) for row in rows]
    for store in GITHUB_STORES:
        normalized_store = normalize_store(store)
        row_totals[store] = sum(
            count
            for db_store, count in normalized_rows
            if db_store == normalized_store or normalized_store in db_store or db_store in normalized_store
        )

    lines = ["🧺 <b>Magasins GitHub</b>"]
    for store in enabled_github_stores(os.getenv("ENABLED_STORES", "")):
        conclusion = statuses.get(store)
        store_rows = row_totals.get(store, 0)
        if conclusion is None:
            mark = "⚠️"
            label = "aucune MAJ / aucun upload"
            lines.append(f"{mark} <b>{escape(store)}</b> — {escape(label)} | lignes: <b>{store_rows:,}</b>")
            continue
        if conclusion == "success":
            mark = "✅"
            label = "upload ok" if store_rows > 0 else "upload ok, aucune ligne DB détectée"
        elif conclusion in {"skipped", "cancelled"}:
            mark = "⚠️"
            label = conclusion
        else:
            mark = "❌"
            label = conclusion
        lines.append(f"{mark} <b>{escape(store)}</b> — {escape(label)} | lignes: <b>{store_rows:,}</b>")
    return "\n".join(lines)


def enabled_github_stores(enabled: str) -> list[str]:
    requested = [store.strip() for store in enabled.split(",") if store.strip()]
    if not requested:
        return GITHUB_STORES
    allowed = {store.upper() for store in requested}
    return [store for store in GITHUB_STORES if store.upper() in allowed]


def format_kamatera_store_lines(summary: dict[str, Any]) -> str:
    stores = str(summary.get("store") or "").split(",")
    failed = set(summary.get("stores_failed") or [])
    no_upload = set(summary.get("stores_without_upload") or [])
    metrics = summary.get("metrics") or {}
    lines = ["🧺 <b>Magasins Kamatera</b>"]
    for store in [s for s in stores if s]:
        if store in failed:
            mark = "❌"
            label = "erreur"
        elif store in no_upload:
            mark = "⚠️"
            label = "aucune MAJ"
        else:
            mark = "✅"
            label = "ok"
        lines.append(f"{mark} <b>{escape(store)}</b> — {escape(label)}")
    if metrics:
        metric_text = ", ".join(f"{escape(k)}={escape(v)}" for k, v in sorted(metrics.items())[:10])
        lines.append(f"📊 Lignes: {metric_text}")
    return "\n".join(lines)


def build_message(args: argparse.Namespace) -> str:
    os.environ["ENABLED_STORES"] = args.enabled_stores or ""
    summary = read_summary(args.summary_file)
    status = args.status or summary.get("status") or "unknown"
    icon = "✅" if status == "success" else "❌" if status == "failure" else "ℹ️"
    title = args.title or ("Kamatera scrape" if args.source == "kamatera" else "GitHub scrape")
    host = summary.get("host") or socket.gethostname()

    if args.workflow == "W6_notify_price_drops.yml":
        return "\n".join(
            [
                f"{icon} <b>Agali Scrapor</b> · {escape(title)}",
                format_notification_block(summary),
            ]
        )

    metrics = fetch_db_metrics(args.hours)
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
    if args.source == "kamatera":
        parts.append(format_kamatera_store_lines(summary))
    elif args.workflow == "W2_scrape.yml":
        parts.append(format_github_store_lines(metrics))
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
    parser.add_argument("--enabled-stores", default="")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    send_message("scrapor", build_message(args), dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

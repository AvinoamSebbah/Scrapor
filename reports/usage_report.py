#!/usr/bin/env python3
"""Bot 3 daily usage report from PostHog first, PostgreSQL second."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegram_notify import escape, send_message


def posthog_query(sql: str) -> list[list[Any]]:
    host = os.getenv("POSTHOG_HOST", "https://eu.posthog.com").rstrip("/")
    # Browser ingestion hosts look like eu.i.posthog.com; the query API lives on
    # the app host, so accept either value in secrets.
    host = host.replace(".i.posthog.com", ".posthog.com")
    project_id = os.getenv("POSTHOG_PROJECT_ID") or os.getenv("POSTHOG_ENVIRONMENT_ID")
    token = os.getenv("POSTHOG_PERSONAL_API_KEY")
    if not project_id or not token:
        raise RuntimeError("POSTHOG_PROJECT_ID and POSTHOG_PERSONAL_API_KEY are required")

    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": sql,
        },
        "client_query_id": "agali-telegram-usage-report",
    }
    endpoints = [
        f"{host}/api/projects/{project_id}/query/",
        f"{host}/api/environments/{project_id}/query/",
    ]
    last_error: Exception | None = None
    for endpoint in endpoints:
        request = urllib.request.Request(
            endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data.get("results") or []
        except Exception as exc:
            last_error = exc
    raise RuntimeError(str(last_error))


def posthog_metrics(hours: int) -> dict[str, Any]:
    since = f"now() - INTERVAL {int(hours)} HOUR"
    metrics: dict[str, Any] = {}
    metrics["summary"] = posthog_query(
        f"""
        SELECT
          count() AS events,
          count(DISTINCT person_id) AS active_users,
          count(DISTINCT properties.$session_id) AS sessions,
          countIf(event = '$pageview') AS pageviews,
          countIf(event = 'agali_search') AS searches,
          countIf(event = 'agali_search_fast') AS fast_searches,
          countIf(event = 'agali_product_detail') AS product_details,
          countIf(event = 'agali_top_promotions_view') AS promo_views,
          countIf(event = 'agali_receipt_scan_success') AS receipt_scans,
          countIf(event = 'agali_signup_success') AS signups,
          countIf(event = 'agali_observation_created') AS observations
        FROM events
        WHERE timestamp >= {since}
        """
    )
    metrics["top_pages"] = posthog_query(
        f"""
        SELECT properties.$pathname AS path, count() AS views
        FROM events
        WHERE timestamp >= {since}
          AND event = '$pageview'
          AND properties.$pathname IS NOT NULL
        GROUP BY path
        ORDER BY views DESC
        LIMIT 8
        """
    )
    metrics["top_events"] = posthog_query(
        f"""
        SELECT event, count() AS total
        FROM events
        WHERE timestamp >= {since}
          AND event NOT IN ('$feature_flag_called')
        GROUP BY event
        ORDER BY total DESC
        LIMIT 8
        """
    )
    return metrics


def db_url() -> str | None:
    return os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")


def table_exists(cur, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (f"public.{table}",))
    return bool(cur.fetchone()["exists"])


def db_metrics(hours: int) -> dict[str, Any]:
    url = db_url()
    if not url:
        return {"available": False, "reason": "POSTGRESQL_URL missing"}
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    metrics: dict[str, Any] = {"available": True}
    try:
        with psycopg2.connect(url, connect_timeout=15, cursor_factory=psycopg2.extras.RealDictCursor) as conn:
            with conn.cursor() as cur:
                for key, table, field in (
                    ("new_users", "users", "created_at"),
                    ("new_observations", "observations", "created_at"),
                    ("active_observations", "observations", "created_at"),
                    ("new_share_links", "share_links", "created_at"),
                    ("new_shopping_lists", "shopping_lists", "created_at"),
                    ("new_user_carts", "user_carts", "saved_at"),
                ):
                    if not table_exists(cur, table):
                        metrics[key] = "missing table"
                        continue
                    if key == "active_observations":
                        cur.execute("SELECT COUNT(*)::bigint AS count FROM observations WHERE status = 'active'")
                    else:
                        cur.execute(f"SELECT COUNT(*)::bigint AS count FROM {table} WHERE {field} >= %s", (since,))
                    metrics[key] = int(cur.fetchone()["count"])

                if table_exists(cur, "user_feature_usage"):
                    cur.execute(
                        """
                        SELECT feature_key, SUM(used_count)::bigint AS used
                        FROM user_feature_usage
                        WHERE updated_at >= %s
                        GROUP BY feature_key
                        ORDER BY used DESC
                        """,
                        (since,),
                    )
                    metrics["feature_usage"] = [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        return {"available": False, "reason": str(exc)}
    return metrics


def build_message(hours: int) -> str:
    posthog_error = None
    try:
        ph = posthog_metrics(hours)
    except Exception as exc:
        ph = {}
        posthog_error = str(exc)
    db = db_metrics(hours)

    lines = [f"📊 <b>Agali Users</b> · dernières {hours}h"]
    if posthog_error:
        lines.append(f"❌ <b>PostHog</b>: unavailable ({escape(posthog_error[:300])})")
    else:
        row = (ph.get("summary") or [[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]])[0]
        lines.append("🧭 <b>PostHog</b>")
        lines.append(f"👥 Utilisateurs actifs: <b>{escape(row[1])}</b>")
        lines.append(f"🧑‍💻 Sessions: <b>{escape(row[2])}</b>")
        lines.append(f"📄 Pages vues: <b>{escape(row[3])}</b>")
        lines.append(f"⚡ Events total: <b>{escape(row[0])}</b>")
        lines.append(f"🔎 Recherches: <b>{escape(row[4])}</b> | rapides: <b>{escape(row[5])}</b>")
        lines.append(f"🛒 Fiches produit: <b>{escape(row[6])}</b>")
        lines.append(f"🏷️ Top promos vues: <b>{escape(row[7])}</b>")
        lines.append(f"🧾 Scans ticket: <b>{escape(row[8])}</b>")
        lines.append(f"🆕 Signups captés: <b>{escape(row[9])}</b>")
        lines.append(f"🔔 Alertes créées: <b>{escape(row[10])}</b>")
        top_pages = ph.get("top_pages") or []
        if top_pages:
            lines.append("🏆 <b>Top pages</b>")
            for page, views in top_pages[:6]:
                lines.append(f"• {escape(page)} — <b>{escape(views)}</b>")
        top_events = ph.get("top_events") or []
        if top_events:
            lines.append("🔥 <b>Top events</b>")
            for event, total in top_events[:6]:
                lines.append(f"• {escape(event)} — <b>{escape(total)}</b>")

    if not db.get("available"):
        lines.append(f"❌ <b>DB</b>: unavailable ({escape(db.get('reason'))})")
    else:
        lines.append("🗄️ <b>PostgreSQL métier</b>")
        lines.append(f"🆕 Nouveaux inscrits: <b>{escape(db.get('new_users'))}</b>")
        lines.append(f"🔔 Nouvelles alertes: <b>{escape(db.get('new_observations'))}</b>")
        lines.append(f"📌 Alertes actives: <b>{escape(db.get('active_observations'))}</b>")
        lines.append(f"📝 Listes créées: <b>{escape(db.get('new_shopping_lists'))}</b>")
        lines.append(f"🛒 Carts sauvés: <b>{escape(db.get('new_user_carts'))}</b>")
        lines.append(f"🔗 Share links: <b>{escape(db.get('new_share_links'))}</b>")
        usage = db.get("feature_usage") or []
        if usage:
            lines.append("🧩 <b>Feature usage DB</b>")
            for item in usage[:8]:
                lines.append(f"• {escape(item['feature_key'])} — <b>{escape(item['used'])}</b>")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send Bot 3 usage report.")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    send_message("users", build_message(args.hours), dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

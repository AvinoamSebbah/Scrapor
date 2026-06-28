#!/usr/bin/env python3
"""Preflight checks for Agali Telegram observability setup."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegram_notify import BOT_ENV, escape, send_message


SECRET_GROUPS = {
    "telegram": [
        "TELEGRAM_BOT1_TOKEN",
        "TELEGRAM_BOT1_CHAT_ID",
        "TELEGRAM_BOT2_TOKEN",
        "TELEGRAM_BOT2_CHAT_ID",
        "TELEGRAM_BOT3_TOKEN",
        "TELEGRAM_BOT3_CHAT_ID",
    ],
    "database": ["POSTGRESQL_URL"],
    "github": ["GH_MONITOR_TOKEN"],
    "digitalocean": ["DO_HOST", "DO_USERNAME", "DO_PORT", "DO_SSH_KEY"],
    "kamatera": ["KAMATERA_HOST", "KAMATERA_USER", "KAMATERA_SSH_KEY"],
    "posthog": ["POSTHOG_PERSONAL_API_KEY", "POSTHOG_PROJECT_ID", "POSTHOG_HOST"],
}


def mask(value: str | None) -> str:
    if not value:
        return "missing"
    if len(value) <= 8:
        return "set"
    return f"{value[:4]}...{value[-4:]}"


def check_env() -> list[tuple[str, bool, str]]:
    rows: list[tuple[str, bool, str]] = []
    for group, names in SECRET_GROUPS.items():
        missing = [name for name in names if not os.getenv(name)]
        if missing:
            rows.append((group, False, "missing: " + ", ".join(missing)))
        else:
            rows.append((group, True, "all present"))
    return rows


def http_check(name: str, url: str, headers: dict[str, str] | None = None) -> tuple[str, bool, str]:
    try:
        request_headers = {"User-Agent": "AgaliObservabilityPreflight/1.0"}
        request_headers.update(headers or {})
        request = urllib.request.Request(url, headers=request_headers)
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read(500).decode("utf-8", errors="replace")
        return name, 200 <= response.status < 300, f"HTTP {response.status} {body[:120]}"
    except Exception as exc:
        return name, False, str(exc)


def telegram_get_me(bot: str) -> tuple[str, bool, str]:
    token, chat_id = (os.getenv(name) for name in BOT_ENV[bot])
    if not token or not chat_id:
        return f"telegram_{bot}", False, "token/chat missing"
    return http_check(f"telegram_{bot}", f"https://api.telegram.org/bot{token}/getMe")


def github_check() -> tuple[str, bool, str]:
    token = os.getenv("GH_MONITOR_TOKEN")
    if not token:
        return "github", False, "GH_MONITOR_TOKEN missing"
    return http_check(
        "github",
        "https://api.github.com/repos/AvinoamSebbah/Scrapor/actions/runs?per_page=1",
        {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )


def posthog_check() -> tuple[str, bool, str]:
    host = (os.getenv("POSTHOG_HOST") or "https://eu.posthog.com").rstrip("/").replace(".i.posthog.com", ".posthog.com")
    project_id = os.getenv("POSTHOG_PROJECT_ID")
    token = os.getenv("POSTHOG_PERSONAL_API_KEY")
    if not project_id or not token:
        return "posthog", False, "POSTHOG_PROJECT_ID or POSTHOG_PERSONAL_API_KEY missing"
    payload = {
        "query": {
            "kind": "HogQLQuery",
            "query": "SELECT count() FROM events WHERE timestamp >= now() - INTERVAL 1 HOUR",
        }
    }
    endpoints = [
        f"{host}/api/projects/{project_id}/query/",
        f"{host}/api/environments/{project_id}/query/",
    ]
    last = ""
    for endpoint in endpoints:
        try:
            request = urllib.request.Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read(500).decode("utf-8", errors="replace")
            return "posthog", True, f"HTTP {response.status} {body[:120]}"
        except Exception as exc:
            last = str(exc)
    return "posthog", False, last


def ssh_check(prefix: str) -> tuple[str, bool, str]:
    host = os.getenv(f"{prefix}_HOST")
    user = os.getenv(f"{prefix}_USER") or os.getenv(f"{prefix}_USERNAME") or "root"
    port = os.getenv(f"{prefix}_PORT", "22")
    key = os.getenv(f"{prefix}_SSH_KEY")
    if not host or not key:
        return prefix.lower(), False, f"{prefix}_HOST or {prefix}_SSH_KEY missing"
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as handle:
        handle.write(key.strip() + "\n")
        key_path = handle.name
    os.chmod(key_path, 0o600)
    try:
        proc = subprocess.run(
            [
                "ssh",
                "-i",
                key_path,
                "-p",
                port,
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=12",
                f"{user}@{host}",
                "hostname && test -d / && echo ok",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        return prefix.lower(), proc.returncode == 0, (proc.stdout or proc.stderr).strip()[:300]
    except Exception as exc:
        return prefix.lower(), False, str(exc)


def format_report(rows: list[tuple[str, bool, str]]) -> str:
    lines = ["<b>Agali Observability Preflight</b>"]
    for name, ok, detail in rows:
        mark = "✅" if ok else "❌"
        lines.append(f"{mark} <b>{escape(name)}</b>: {escape(detail)}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Agali observability secrets and integrations.")
    parser.add_argument("--send-test-messages", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rows = check_env()
    if not os.getenv("API_SECRET_KEY"):
        rows.append(("api_secret_key", True, "not set; backend currently accepts public API reads"))
    else:
        rows.append(("api_secret_key", True, "present"))
    rows.extend(telegram_get_me(bot) for bot in ("scrapor", "health", "users"))
    api_base = os.getenv("AGALI_API_BASE", "https://api.agali.live").rstrip("/")
    city = urllib.parse.quote(os.getenv("AGALI_HEALTH_CITY", "תל אביב"))
    rows.append(http_check("api_health", f"{api_base}/health"))
    rows.append(
        http_check(
            "api_product_fetch",
            f"{api_base}/api/products/search-lite?q=milk&limit=1",
            {"x-api-key": os.getenv("API_SECRET_KEY", "")},
        )
    )
    rows.append(http_check("api_top_promotions", f"{api_base}/api/offers/top-promotions?city={city}&limit=1", {"x-api-key": os.getenv("API_SECRET_KEY", "")}))
    rows.append(github_check())
    rows.append(posthog_check())
    rows.append(ssh_check("DO"))
    rows.append(ssh_check("KAMATERA"))

    report = format_report(rows)
    sys.stdout.buffer.write((report + "\n").encode("utf-8"))

    if args.send_test_messages:
        send_message("scrapor", "✅ <b>Test Bot 1</b> · Scrapor Telegram OK", dry_run=args.dry_run)
        send_message("health", "✅ <b>Test Bot 2</b> · Health Telegram OK", dry_run=args.dry_run)
        send_message("users", "✅ <b>Test Bot 3</b> · Users Telegram OK", dry_run=args.dry_run)
        send_message("health", report, dry_run=args.dry_run)

    return 0 if all(ok for _, ok, _ in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())

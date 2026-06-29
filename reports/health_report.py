#!/usr/bin/env python3
"""Bot 2 health checks for Agali API, GitHub workflows, and servers."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegram_notify import escape, send_message


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


def http_json(name: str, url: str, api_key: str | None = None) -> Check:
    headers = {"User-Agent": "AgaliHealthBot/1.0"}
    if api_key:
        headers["x-api-key"] = api_key
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read(4000).decode("utf-8", errors="replace")
            ok = 200 <= response.status < 300
            if ok:
                return Check(name, True, f"OK HTTP {response.status}")
            return Check(name, False, f"HTTP {response.status} {body[:180]}")
    except Exception as exc:
        return Check(name, False, str(exc))


def github_latest(repo: str, workflow: str | None = None) -> Check:
    token = os.getenv("GH_MONITOR_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        return Check(f"GitHub {repo}", False, "GH_MONITOR_TOKEN missing")
    path = f"https://api.github.com/repos/{repo}/actions/runs?per_page=10"
    if workflow:
        path = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs?per_page=5"
    request = urllib.request.Request(
        path,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        runs = payload.get("workflow_runs") or []
        if not runs:
            return Check(f"GitHub {repo}", True, "aucun workflow run")
        latest = runs[0]
        conclusion = latest.get("conclusion") or latest.get("status")
        ok = conclusion in {"success", "completed", "in_progress", "queued", "waiting", "requested"}
        detail = f"{latest.get('name')} -> {conclusion}"
        if not ok:
            detail = f"{detail} ({latest.get('html_url')})"
        return Check(
            f"GitHub {repo}" + (f" {workflow}" if workflow else ""),
            ok,
            detail,
        )
    except Exception as exc:
        return Check(f"GitHub {repo}", False, str(exc))


def ssh_key_file(prefix: str) -> str | None:
    key = os.getenv(f"{prefix}_SSH_KEY")
    if not key:
        return None
    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    handle.write(key.strip() + "\n")
    handle.close()
    os.chmod(handle.name, 0o600)
    return handle.name


def ssh_check(prefix: str, label: str, summary_path: str | None = None) -> Check:
    host = os.getenv(f"{prefix}_HOST")
    user = os.getenv(f"{prefix}_USER", "root")
    port = os.getenv(f"{prefix}_PORT", "22")
    key_file = ssh_key_file(prefix)
    if not host or not key_file:
        return Check(label, False, f"{prefix}_HOST or {prefix}_SSH_KEY missing")

    remote_summary = summary_path or os.getenv(f"{prefix}_SCRAPE_SUMMARY", "/opt/agali-scraper/run_summary.json")
    script = f"""
set -e
echo "host=$(hostname)"
echo "disk=$(df -h / | awk 'NR==2{{print $5 \" used, \" $4 \" free\"}}')"
echo "mem=$(free -m | awk '/Mem:/{{print $3 \"MB/\" $2 \"MB\"}}')"
if command -v docker >/dev/null 2>&1; then echo "docker_up=$(docker ps -q | wc -l)"; fi
if [ -f "{remote_summary}" ]; then
  python3 - <<'PY'
import json, pathlib
p=pathlib.Path('{remote_summary}')
d=json.loads(p.read_text())
print("summary_status=" + str(d.get("status", "unknown")))
if d.get("source") == "kamatera":
    print("stores=" + str(d.get("stores_seen", 0)) + "/" + str(d.get("stores_total", 0)))
    print("failed=" + str(len(d.get("stores_failed") or [])))
    print("no_upload=" + str(len(d.get("stores_without_upload") or [])))
    print("outputs=" + str(d.get("outputs_count", 0)))
PY
else
  echo "summary=missing:{remote_summary}"
fi
"""
    try:
        proc = subprocess.run(
            [
                "ssh",
                "-i",
                key_file,
                "-p",
                port,
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ConnectTimeout=15",
                f"{user}@{host}",
                script,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=35,
        )
        detail = (proc.stdout or proc.stderr).strip()
        ok = proc.returncode == 0 and "summary_status=failure" not in detail
        return Check(label, ok, format_ssh_detail(detail))
    except Exception as exc:
        return Check(label, False, str(exc))


def format_ssh_detail(detail: str) -> str:
    values: dict[str, str] = {}
    for raw in detail.splitlines():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()

    lines = []
    if values.get("host"):
        lines.append(f"host {values['host']}")
    if values.get("disk"):
        lines.append(f"disk {values['disk']}")
    if values.get("mem"):
        lines.append(f"ram {values['mem']}")
    if values.get("docker_up"):
        lines.append(f"docker {values['docker_up']} containers up")
    if values.get("summary_status"):
        lines.append(f"scraper {values['summary_status']}")
    elif values.get("summary"):
        lines.append("summary absent (non bloquant)")
    if values.get("stores"):
        lines.append(f"magasins {values['stores']}")
    if values.get("failed"):
        lines.append(f"erreurs magasins {values['failed']}")
    if values.get("no_upload"):
        lines.append(f"sans upload {values['no_upload']}")
    if values.get("outputs"):
        lines.append(f"outputs {values['outputs']}")
    return "\n".join(lines) if lines else detail[:500]


def build_message(checks: list[Check], always: bool) -> tuple[str, bool]:
    failures = [check for check in checks if not check.ok]
    if not failures and not always:
        return "", False
    icon = "✅" if not failures else "🚨"
    lines = [f"{icon} <b>Agali Health</b> · {datetime.now(timezone.utc).isoformat(timespec='minutes')} UTC"]
    for check in checks:
        mark = "✅" if check.ok else "❌"
        lines.append(f"{mark} <b>{escape(check.name)}</b>: {escape(check.detail)}")
    return "\n".join(lines), bool(failures)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Agali health checks and alert Bot 2 on failures.")
    parser.add_argument("--always", action="store_true", help="Send an OK report too.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_base = os.getenv("AGALI_API_BASE", "https://api.agali.co.il").rstrip("/")
    api_key = os.getenv("API_SECRET_KEY")
    default_city = urllib.parse.quote(os.getenv("AGALI_HEALTH_CITY", "תל אביב"))
    checks = [
        http_json("API /health", f"{api_base}/health"),
        http_json("Product fetch", f"{api_base}/api/products/search-lite?q=milk&limit=1", api_key),
        http_json("Top promotions", f"{api_base}/api/offers/top-promotions?city={default_city}&limit=1", api_key),
        github_latest("AvinoamSebbah/Scrapor"),
        github_latest("AvinoamSebbah/Servor"),
        github_latest("AvinoamSebbah/Agali"),
        ssh_check("DO", "DigitalOcean server", "/root/app/.agali_health_summary.json"),
        ssh_check("KAMATERA", "Kamatera scraper"),
    ]
    message, has_failure = build_message(checks, args.always)
    if message:
        send_message("health", message, dry_run=args.dry_run)
    if args.dry_run:
        return 0
    return 1 if has_failure else 0


if __name__ == "__main__":
    raise SystemExit(main())

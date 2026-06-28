#!/usr/bin/env python3
"""Small Telegram notification helper for Agali operational bots."""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import urllib.error
import urllib.request


BOT_ENV = {
    "scrapor": ("TELEGRAM_BOT1_TOKEN", "TELEGRAM_BOT1_CHAT_ID"),
    "health": ("TELEGRAM_BOT2_TOKEN", "TELEGRAM_BOT2_CHAT_ID"),
    "users": ("TELEGRAM_BOT3_TOKEN", "TELEGRAM_BOT3_CHAT_ID"),
}


def escape(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=False)


def configured(bot: str) -> tuple[str | None, str | None]:
    token_env, chat_env = BOT_ENV[bot]
    return os.getenv(token_env), os.getenv(chat_env)


def send_message(
    bot: str,
    text: str,
    *,
    disable_notification: bool = False,
    dry_run: bool = False,
) -> bool:
    token, chat_id = configured(bot)
    if dry_run:
        payload = {
            "chat_id": chat_id or f"<{BOT_ENV[bot][1]}>",
            "text": text[:3900],
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": disable_notification,
        }
        sys.stdout.buffer.write((json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))
        return True

    if not token or not chat_id:
        print(f"telegram skipped: missing credentials for {bot}", file=sys.stderr)
        return False

    payload = {
        "chat_id": chat_id,
        "text": text[:3900],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "disable_notification": disable_notification,
    }
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8", errors="replace")
        if '"ok":true' not in body:
            print(f"telegram returned unexpected response: {body[:300]}", file=sys.stderr)
            return False
        return True
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"telegram send failed: {exc}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Send a Telegram message to one Agali bot.")
    parser.add_argument("--bot", choices=sorted(BOT_ENV), required=True)
    parser.add_argument("--message", default="")
    parser.add_argument("--message-file")
    parser.add_argument("--silent", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    message = args.message
    if args.message_file:
        with open(args.message_file, "r", encoding="utf-8") as handle:
            message = handle.read()
    if not message.strip():
        print("telegram skipped: empty message", file=sys.stderr)
        return 0

    send_message(args.bot, message, disable_notification=args.silent, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

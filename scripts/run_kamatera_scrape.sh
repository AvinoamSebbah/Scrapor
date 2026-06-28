#!/usr/bin/env bash
# Run one Kamatera-local Scrapor job and notify Bot 1.
#
# Install on Kamatera, for example:
#   /opt/agali-scrapor/scripts/run_kamatera_scrape.sh /opt/agali-scrapor/kamatera.env
#
# The env file should define ENABLED_SCRAPERS plus the same DB/Telegram secrets
# used by GitHub Actions. This wrapper never prints secret values.

set -u

ENV_FILE="${1:-/opt/agali-scrapor/kamatera.env}"
REPO_DIR="${AGALI_SCRAPOR_DIR:-/opt/agali-scrapor}"
APP_DATA_PATH="${APP_DATA_PATH:-$REPO_DIR/app_data}"
SUMMARY_FILE="${KAMATERA_SUMMARY_FILE:-$REPO_DIR/run_summary.json}"
LOG_DIR="${KAMATERA_LOG_DIR:-$REPO_DIR/logs}"
mkdir -p "$LOG_DIR" "$APP_DATA_PATH"

if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  . "$ENV_FILE"
  set +a
fi

cd "$REPO_DIR" || exit 2

START_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
START_EPOCH="$(date +%s)"
RUN_ID="kamatera-$(date -u +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/$RUN_ID.log"
STORE="${ENABLED_SCRAPERS:-unknown}"
OPERATION="${OPERATION:-scraping,converting,api_update,clean_dump_files}"

before_files=0
if [ -d "$APP_DATA_PATH/outputs" ]; then
  before_files="$(find "$APP_DATA_PATH/outputs" -type f 2>/dev/null | wc -l | tr -d ' ')"
fi

STATUS="success"
ERROR=""
{
  echo "[$START_TS] Kamatera scrape start"
  echo "store=$STORE operation=$OPERATION app_data=$APP_DATA_PATH"
  APP_DATA_PATH="$APP_DATA_PATH" OPERATION="$OPERATION" python main.py
} >"$LOG_FILE" 2>&1 || {
  STATUS="failure"
  ERROR="$(tail -40 "$LOG_FILE" | sed 's/[<>]//g' | tr '\n' ' ' | cut -c1-900)"
}

after_files=0
if [ -d "$APP_DATA_PATH/outputs" ]; then
  after_files="$(find "$APP_DATA_PATH/outputs" -type f 2>/dev/null | wc -l | tr -d ' ')"
fi
NEW_FILES=$((after_files - before_files))
if [ "$NEW_FILES" -lt 0 ]; then NEW_FILES=0; fi
END_TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
DURATION="$(($(date +%s) - START_EPOCH))"

python3 - "$SUMMARY_FILE" <<PY
import json, socket, sys
summary = {
    "source": "kamatera",
    "host": socket.gethostname(),
    "status": "$STATUS",
    "store": "$STORE",
    "operation": "$OPERATION",
    "started_at": "$START_TS",
    "finished_at": "$END_TS",
    "duration_seconds": int("$DURATION"),
    "new_files": int("$NEW_FILES"),
    "new_rows": None,
    "log_file": "$LOG_FILE",
    "error": "$ERROR" or None,
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False, indent=2)
PY

python reports/scrape_report.py \
  --source kamatera \
  --status "$STATUS" \
  --title "Kamatera scrape" \
  --store "$STORE" \
  --summary-file "$SUMMARY_FILE" \
  --hours 6 || true

if [ "$STATUS" != "success" ]; then
  python telegram_notify.py \
    --bot health \
    --message "🚨 <b>Agali Health</b> · Kamatera scrape failed for <b>$STORE</b>\nHost: $(hostname)\nLog: $LOG_FILE" || true
  exit 1
fi

exit 0

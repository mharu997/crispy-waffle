#!/usr/bin/env bash
# PITDS daily update wrapper. Invoked by launchd (com.pitds.update-daily.plist).
#
# Responsibilities:
#   1. cd into the project (in case launchd's WorkingDirectory got dropped)
#   2. activate venv and run `pitds update-daily`
#   3. capture exit code, log a one-line summary
#   4. surface any new anomalies from data/raw/anomalies.log

set -uo pipefail

PROJECT_DIR="/Users/mharutyunyan/Projects/Enso/PITDS"
LOG_DIR="$PROJECT_DIR/data/raw"
DAILY_LOG="$LOG_DIR/daily_runs.log"
ANOMALIES_LOG="$LOG_DIR/anomalies.log"

cd "$PROJECT_DIR" || { echo "[ERROR] cannot cd to $PROJECT_DIR"; exit 1; }
mkdir -p "$LOG_DIR"

TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Snapshot anomalies count before run, so we can show only the delta.
ANOM_BEFORE=0
if [ -f "$ANOMALIES_LOG" ]; then
  ANOM_BEFORE=$(wc -l < "$ANOMALIES_LOG" | tr -d ' ')
fi

# Run the update. Capture the full output for the daily log; tee a one-liner.
RUN_OUT="$LOG_DIR/last_run.log"
"$PROJECT_DIR/.venv/bin/pitds" update-daily > "$RUN_OUT" 2>&1
EXIT=$?

# Extract the bottom-line summary: count of OK steps from the report
N_OK=$(grep -E "^  [0-9]+/[0-9]+ steps succeeded" "$RUN_OUT" | tail -1 || true)
N_OK="${N_OK:-(no summary)}"

# Compute new anomalies (lines added during this run)
ANOM_AFTER=0
if [ -f "$ANOMALIES_LOG" ]; then
  ANOM_AFTER=$(wc -l < "$ANOMALIES_LOG" | tr -d ' ')
fi
NEW_ANOM=$((ANOM_AFTER - ANOM_BEFORE))

# One-line summary written to daily_runs.log
{
  echo "$TS exit=$EXIT $N_OK new_anomalies=$NEW_ANOM"
  if [ "$NEW_ANOM" -gt 0 ]; then
    echo "$TS  anomalies_tail:"
    tail -n "$NEW_ANOM" "$ANOMALIES_LOG" | sed "s/^/$TS    /"
  fi
} >> "$DAILY_LOG"

# Also surface to stdout (which launchd writes to launchd_stdout.log)
echo "$TS pitds update-daily: exit=$EXIT, $N_OK, new_anomalies=$NEW_ANOM"

exit "$EXIT"

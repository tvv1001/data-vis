#!/bin/sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$ROOT/server/download.log"
MONITOR_LOG="$ROOT/server/monitor.log"

while true; do
    if ! pgrep -f "node scripts/download-finra-data.js" > /dev/null 2>&1; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Supervisor: starting downloader (rate=8 concurrency=8)" >> "$LOG"
    nohup node "$ROOT/scripts/download-finra-data.js" rate=8 concurrency=8 > "$LOG" 2>&1 &
  fi

  if ! pgrep -f "node scripts/monitor-download.js" > /dev/null 2>&1; then
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) Supervisor: starting monitor" >> "$MONITOR_LOG"
    nohup node "$ROOT/scripts/monitor-download.js" > "$MONITOR_LOG" 2>&1 &
  fi

  sleep 10
done

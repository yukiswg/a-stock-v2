#!/usr/bin/env bash
# 每天 9:30 跑一次，产出 data/output/watchdog/<date>.md
# 被 crontab / launchd 调用，或手动跑做冒烟

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="$PROJECT_ROOT/data/output/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/run_$(date +%Y-%m-%d).log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] === watchdog brief start ===" >> "$LOG_FILE"

PYTHONPATH=src /usr/bin/python3 -m watchdog.cli brief --max-tickers 5 >> "$LOG_FILE" 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] === watchdog brief done ===" >> "$LOG_FILE"

# 可选：失败时发 macOS 通知
if [ $? -ne 0 ]; then
  osascript -e 'display notification "watchdog brief 失败，查看 log" with title "ashare-watchdog"' 2>/dev/null || true
fi

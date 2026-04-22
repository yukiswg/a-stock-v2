#!/usr/bin/env bash
# 一键把每日 9:30 watchdog 加入当前用户 crontab
# 用法：./scripts/setup_cron.sh
# 删除：crontab -l | grep -v watchdog | crontab -

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_SCRIPT="$PROJECT_ROOT/scripts/run_daily.sh"

chmod +x "$RUN_SCRIPT"

# 周一~周五 9:30 跑（A 股开盘前）
CRON_LINE="30 9 * * 1-5 $RUN_SCRIPT  # watchdog"

# 查重
if crontab -l 2>/dev/null | grep -q "watchdog"; then
  echo "⚠️  crontab 里已经有 watchdog 条目，先删后加"
  crontab -l | grep -v "watchdog" | crontab -
fi

(crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -

echo "✅ 已加入 crontab："
echo "    $CRON_LINE"
echo ""
echo "查看："
echo "    crontab -l"
echo ""
echo "⚠️  注意：macOS 上 cron 需要给 /usr/sbin/cron 完全磁盘访问权限："
echo "    系统设置 → 隐私与安全 → 完全磁盘访问 → +  /usr/sbin/cron"

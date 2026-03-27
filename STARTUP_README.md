# STARTUP README

## 你每天先看什么

先看这个首页：

- `data/output/homepage/2026-03-23_homepage_overview.md`
- 或启动 dashboard 打开 `http://127.0.0.1:8765`

首页只看四块：

- 今天怎么做
- 当前最新价格
- 最新告警
- 持仓动作

## 一键入口

最少命令：

```bash
./scripts/run_trading_day.sh --as-of 2026-03-23 --source live --iterations 2 --skip-postclose
```

这会做三件事：

- 生成盘前报告和 monitor plan
- 跑一段实时监控并刷新首页
- 不做盘后复盘，适合白天使用

## 盘中

只开监控：

```bash
./scripts/run_realtime_session.sh --as-of 2026-03-23 --source live --iterations 2
```

## 盘后

复盘和评估：

```bash
./scripts/run_post_close.sh --as-of 2026-03-23 --session-dir data/output/realtime/2026-03-23/session_161529
```

## Web 入口

Dashboard:

```bash
./scripts/serve_dashboard.sh --port 8765
```

API:

```bash
./scripts/serve_api.sh --port 8766
```

接口：

- `GET /api/status`
- `GET /api/homepage`
- `GET /api/advice?question=宁德时代现在能买吗&as_of=2026-03-23`
- `GET /api/discovery?as_of=2026-03-23&limit=5`

## 对话式判断

问单只股票：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli ask-stock --as-of 2026-03-23 --question "宁德时代现在能买吗"
```

主动找候选：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli discover-ideas --as-of 2026-03-23 --limit 5
```

补数说明：

- `ask-stock` 会自动给当前股票补财务、估值、主力资金、公司概况、行业热度
- `discover-ideas` 会自动给前排候选补这些数据，再重新排序
- 补数结果会缓存到 `data/output/state/<date>/`
- 当前桥接依赖仓库内的 `.venv/bin/python` 和 `akshare`

## 历史回放

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli replay-daily --as-of 2026-03-17
```

## Claude 指挥 Codex 干活

如果你希望 Claude 当管理者，Codex 当执行者，可以直接用项目内置调度器：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli delegate-codex \
  --task "改造日报和首页摘要，降低追涨杀跌倾向，并加入可验证的失败原因与替代方案" \
  --workers 2
```

它会做三步：

- 先起一个 Codex planner 拆任务
- 再并行起多个 Codex worker 执行
- 最后把计划、worker 证据和 Leader 判定结果落盘
- 最终 pass/fail 由 Claude 自己判断，不由 Codex 裁决

结果会落到：

- `data/output/codex_delegate/<timestamp>/plan.json`
- `data/output/codex_delegate/<timestamp>/worker_results.json`
- `data/output/codex_delegate/<timestamp>/decision.json`
- `data/output/codex_delegate/<timestamp>/workers/...`
- `data/output/codex_delegate/<timestamp>/summary.json`

如果你只是想先看会生成什么 prompt，不真正执行：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli delegate-codex \
  --task "给我做一个 worker 编排样例" \
  --dry-run
```

## 关键目录

- 报告：`data/output/reports/<date>/`
- 状态快照：`data/output/state/<date>/`
- 补充财务与行业数据：`data/output/state/<date>/fundamentals.json` 等
- 实时会话：`data/output/realtime/<date>/session_xxx/`
- 首页：`data/output/homepage/`
- 评估：`data/output/evaluations/<date>/`
- 回测：`data/output/backtests/<date>/`

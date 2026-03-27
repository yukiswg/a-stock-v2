# A-Stock V2

`A-Stock V2` 是一个面向 A 股的研究与执行辅助系统。它不是单一脚本，而是一条完整流水线：先拉数据，再做结构化判断，再生成报告、首页、候选股、盘中监控和盘后评估。

如果你想先用一句话理解它，可以这样记：

“这是一个把持仓、行情、新闻、补充财务数据和规则判断串起来，最后产出可执行结论的 A 股工作台。”

## 现在这个版本能做什么

- 盘前生成日报、综合报告、监控计划、首页摘要
- 盘中做实时监控，刷新首页和告警
- 盘后做复盘、预测评估、固定股票池评估、回测
- 用自然语言问单只股票值不值得买
- 在股票池里主动发现更好的候选
- 自动生成“明日最佳个股”报告
- 提供本地 Dashboard 和 API
- 支持 Claude 管理、Codex 执行的多 worker 调度

## 先把项目理解成 5 层

### 1. 数据层

目录：

- `src/ashare_harness_v2/data_harness/`

负责的事：

- 读取持仓
- 拉行情和日线缓存
- 抓新闻和公告
- 调补充数据，比如财务、估值、资金流、行业热度

你可以把这一层理解成“喂数据”。

### 2. 决策层

目录：

- `src/ashare_harness_v2/decision_core.py`
- `src/ashare_harness_v2/decision_harness/`

负责的事：

- 把原始数据变成特征
- 判断市场风格和风险状态
- 给持仓和候选股形成结构化结论

你可以把这一层理解成“把杂乱信息变成规则化判断”。

### 3. 运行层

目录：

- `src/ashare_harness_v2/runtime_harness/`

负责的事：

- 串起盘前、盘中、盘后流程
- 写首页、报告、状态快照、实时会话
- 管理每天的输出目录

你可以把这一层理解成“流程编排器”。

### 4. 策略与问答层

目录：

- `src/ashare_harness_v2/advice_harness/`
- `src/ashare_harness_v2/skill_harness/`

负责的事：

- `ask-stock` 回答“这只股票现在能买吗”
- `discover-ideas` 主动找候选股
- `best-stock-report` 生成单只最佳股票报告
- `scan-sector-leaders` 从行业热度里找龙头

你可以把这一层理解成“把底层判断包装成你能直接消费的答案和报告”。

### 5. 评估层

目录：

- `src/ashare_harness_v2/evaluation_harness/`

负责的事：

- 评估历史预测
- 做固定股票池 top-N 评估
- 做回测
- 做盘中会话复盘

你可以把这一层理解成“检查系统到底有没有用，而不是只看它会不会说话”。

## 你每天最常用的入口

更偏操作手册的说明在 [`STARTUP_README.md`](STARTUP_README.md)，这里先放最常用的命令。

### 1. 一键跑完整交易日

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli run-trading-day --as-of 2026-03-27
```

如果白天只想跑盘前和盘中，不做盘后：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli run-trading-day --as-of 2026-03-27 --skip-postclose
```

### 2. 只问一只股票

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli ask-stock --as-of 2026-03-27 --question "宁德时代现在能买吗"
```

### 3. 主动发现候选股

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli discover-ideas --as-of 2026-03-27 --limit 5
```

### 4. 直接生成“明日最佳个股”报告

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli best-stock-report --as-of 2026-03-27
```

### 5. 启动本地页面和 API

Dashboard:

```bash
./scripts/serve_dashboard.sh --port 8765
```

API:

```bash
./scripts/serve_api.sh --port 8766
```

## 当前 CLI 主要命令

- `run-trading-day`: 一键跑盘前、盘中、盘后
- `run-premarket`: 只做盘前报告和首页
- `run-realtime`: 只做盘中监控
- `run-postclose`: 只做盘后复盘和评估
- `replay-daily`: 按历史状态重放
- `evaluate-predictions`: 评估历史预测
- `evaluate-fixed-pool`: 固定股票池 top-N 评估
- `backtest`: 观察池回测
- `ask-stock`: 对话式单股判断
- `discover-ideas`: 主动发现候选
- `best-stock-report`: 明日最佳个股报告
- `scan-sector-leaders`: 行业龙头扫描
- `backfill-history`: 批量回补历史盘前状态
- `delegate-codex`: 让 Codex 多 worker 并行执行任务
- `monitor-worker`: 看 Codex worker 输出

## 关键输出目录

- `data/output/reports/<date>/`: 日报、综合报告、最佳个股报告
- `data/output/homepage/`: 首页 markdown、json、html
- `data/output/state/<date>/`: 每日状态快照
- `data/output/realtime/<date>/`: 盘中监控会话
- `data/output/evaluations/<date>/`: 评估结果
- `data/output/backtests/<date>/`: 回测结果
- `data/output/advice/<date>/`: 问答与候选发现产物
- `data/cache/daily_bars/`: 日线缓存

## 配置从哪里看

主配置文件是：

- [`config/default.toml`](config/default.toml)

这里定义了：

- 输出目录
- 实时监控阈值
- 候选数量
- 新闻抓取参数
- 补充数据脚本和 Python 路径
- 动态股票池策略参数

## 为什么你会觉得它越来越复杂

因为它已经不是“写个脚本出个报告”这种体量了，而是同时做下面几件事：

- 数据采集
- 特征计算
- 决策生成
- 报告渲染
- 页面输出
- 实时监控
- 盘后评估
- 历史回放
- Agent 调度

复杂不是因为代码写得花，而是因为你现在维护的是一个“小型系统”，不是一个“单点工具”。

你以后判断它时，最好不要把它当成一个整体黑盒，而是拆成这三个问题：

1. 数据有没有来对。
2. 规则有没有判断对。
3. 输出有没有写对。

这样排查会简单很多。

## 本地和 GitHub 怎么保持同步

这个项目本地已经初始化成 Git 仓库，并且远端目标是：

- [yukiswg/a-stock-v2](https://github.com/yukiswg/a-stock-v2)

以后你本地更新版本，最常用就是这几步：

```bash
git status
git add .
git commit -m "说明你这次改了什么"
git push origin main
```

如果你只想提交某几个文件，不要直接 `git add .`，改成：

```bash
git add README.md src/ashare_harness_v2/cli.py
git commit -m "update readme and cli"
git push origin main
```

## 现在最值得继续做的事

- 先把 README 和实际能力保持同步
- 把“日常使用入口”和“开发者入口”分开
- 逐步减少 `data/output/` 下不需要入库的生成产物
- 给每个大模块补一页更短的说明文档

如果你愿意，我下一步可以继续把这个项目再整理一层：

- 帮你把 GitHub 推送链路彻底打通
- 帮你把 README 再拆成“使用说明”和“架构说明”
- 再给你画一版这个项目的执行流程图

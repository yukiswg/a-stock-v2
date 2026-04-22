# ashare-watchdog

**A 股每日投资预警**：VIX 逃跑灯 + 6 源热点发现 + seeking-truth 核查 → 每天 9:30 出一份 *今日投资建议*。

## 设计目标

替代旧版 `ashare-phase1-agents-v2`（~18000 行冗余）。新版聚焦三件事：

1. **今天要不要跑**（vix 红绿灯）
2. **今天热点在哪**（6 源并行：财联社电报 / 东财新闻 / 板块涨跌 / 涨停板池 / 龙虎榜 / 研报评级 + 个人 inbox）
3. **这些热点真实吗**（seeking-truth gate：5 个 akshare 上游信号核查，≥2 过为候选，≥3 过为重点推荐）

## 核心原则

- **只做每天真的会用的事**，不做"看起来很专业"的功能
- **上游优先**：新闻是下游（容易撒谎），龙虎榜 / 股东增减持 / 公告是上游（钱已经动了）
- **说做 gap**：6 源的"市场在说什么"对照 akshare 的"钱真的去了哪"，不一致的信号要警惕

## 目录

```
src/watchdog/
├── vix.py               # VIX 红绿灯（搬自 ~/.claude/skills/vix-rotation）
├── truth.py             # 5 源 akshare 真实性核查
├── find_hotspots.py     # 6 源热点发现（Phase 2）
├── gate.py              # hotspots → truth → 候选池（Phase 2）
├── daily_brief.py       # 渲染今日投资建议 markdown（Phase 2）
└── cli.py               # CLI 入口（Phase 2）

config/
├── tickers.json         # 科技/防守/持仓池
└── default.toml         # 阈值/数据源开关

data/
├── input/inbox/         # 手动粘贴小红书推文（财躺平/派大星屁屁）
└── output/watchdog/     # 每日 <date>.md 产出
```

## 每日产出样例

```
# 今日投资建议 2026-04-21

## 🚦 市场红绿灯
状态：留意（1 红）- 科技仓位建议 80%
- RSI: 🟡 42.3 - 正常区间
- VIX: 🔴 26.5 > 均线 24.1 - 恐慌
- 动量: 🟢 +3.4% - 强势（触发中美背离 override）

## 🔥 今日热点（6 源并行）
1. 半导体设备 +4.2%【板块+涨停板+研报3源命中】
2. 铜铝 +2.8%【财联社+东财2源命中】
...

## 🔍 真实性核查
| 标的 | 公告 | 增减持 | 龙虎榜 | 业绩预告 | 机构调研 | 得分 | 判定 |
| 寒武纪 | ✅ | - | ✅ | ✅ | ✅ | 4/5 | 🔥 重点推荐 |
| XX新能源 | - | - | ✅ | - | - | 1/5 | ❌ 不入选 |

## ✅ 今日操作建议
- 持仓：... / 候选：... / 重点：...
```

## 状态

- [x] Phase 1: 骨架 + seeking-truth skill + vix.py + truth.py
- [ ] Phase 2: find_hotspots / gate / daily_brief / cli
- [ ] Phase 3: config + tests + ask-stock + 9:30 定时

## 相关

- 逃跑策略来源：`~/.claude/skills/vix-rotation/`
- 求真方法论：`~/.claude/skills/seeking-truth/`
- 废弃旧版：`../ashare-phase1-agents-v2/`（保留作为参考）

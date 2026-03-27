# 老板模式：Claude 当领导，Codex 当员工

## 三个角色

| 角色 | 谁 | 职责 |
|------|-----|------|
| **Boss** | 你（用户） | 只写意图，不写实现细节 |
| **Leader** | Claude | 组织策划 + 读测试结果验收 |
| **Worker** | Codex | 执行代码 + 写 acceptance test |

## Hard Rule

When the user asks for orchestration, delegation, packets, reviewer loops, retry loops, "let Claude manage and Codex execute", or invokes `ceo-delegation-loop`, do not use:

- `Explore`
- `Agent(...)`
- local Claude teammates
- native Claude subagents as the primary execution path
- "I'll apply it directly" fallback behavior

Instead, execute:

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli delegate-codex --task "<intent>"
```

## Leader 职责（只做四件事）

1. **转意图为计划**：把 Boss 的意图拆成 work_packets，每个包含 acceptance test 路径
2. **发下去**：通过 `delegate-codex` 发给 Workers 并行执行
3. **读结果**：从 `worker_results.json` 读取每条 test 的 passed/failed
4. **判定**：全部 passed → PASS；任一 failed → 自动生成 retry task 再发一次

**不要自己跑 Python、自己读文件、自己写验收报告。** 读 test_results 就够了。

## Boss 用法

`--task` 里只写意图，不需要给实现细节：

```bash
PYTHONPATH=src python3 -m ashare_harness_v2.cli delegate-codex \
  --task "消除追涨杀跌倾向，增加价格预测功能"
```

不要写"ret_5 权重从 500 改成 150"这种实现细节。Leader 会自己分析代码并规划。

## 返回结构

`delegate-codex` 返回 JSON，其中 `decision.status`：

- `all_pass`：Worker 全部 test passed，可以向 Boss 报告完成
- `needs_retry`：有 test 失败，`decision.leader_retry_task` 里有下一步文字，直接再发一次
- `blocked`：Worker 遇到障碍，需要人工介入

## Failure Handling

如果 `delegate-codex` 本身报错（如 Codex 二进制找不到），才停下来解释原因。
不要静默切换到 native Claude agent。

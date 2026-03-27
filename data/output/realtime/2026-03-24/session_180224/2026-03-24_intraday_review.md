# Intraday Review

- 结论: 样本不足，但盘中以风控告警为主，明日优先复核风险阈值是否过敏。

## Alert Evaluation
- 告警数 `9` | 可评估 `0` | 有效率 `0.00%`
- 风险类告警 `6` | 入场类告警 `3` | 样本缺口 `9`

## Rule Diagnostics
- `benchmark_drop` | 状态 `unvalidated` | 命中 `0/0` | 触发 `3` | 均值 `+0.00%`
- `drawdown_break` | 状态 `unvalidated` | 命中 `0/0` | 触发 `3` | 均值 `+0.00%`
- `relative_breakout` | 状态 `unvalidated` | 命中 `0/0` | 触发 `3` | 均值 `+0.00%`

## Policy Feedback
- `benchmark_drop` 触发较多但样本不足，建议延长回看窗口再定是否降权。
- `drawdown_break` 触发较多但样本不足，建议延长回看窗口再定是否降权。
- `relative_breakout` 触发较多但样本不足，建议延长回看窗口再定是否降权。

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..advice_harness.engine import discover_top_ideas
from ..config import UniverseItem
from ..data_harness.market_data import compute_series_features, fetch_daily_series
from ..models import InstrumentFeatures
from ..utils import average, median, write_json


def backtest_watchlist_strategy(
    *,
    universe: list[UniverseItem],
    cache_dir: str | Path,
    benchmark_code: str = "000300",
    begin: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    benchmark = fetch_daily_series(code=benchmark_code, name="沪深300", cache_dir=cache_dir, begin="20240101", end="20500101")
    benchmark_dates = [bar.trade_date for bar in benchmark.bars]
    if begin is not None:
        benchmark_dates = [item for item in benchmark_dates if item >= begin]
    if end is not None:
        benchmark_dates = [item for item in benchmark_dates if item <= end]
    benchmark_dates = benchmark_dates[20:-1]
    universe_series = {
        item.code: fetch_daily_series(code=item.code, name=item.name, cache_dir=cache_dir, begin="20240101", end="20500101")
        for item in universe
        if item.category != "benchmark"
    }
    trades: list[dict[str, Any]] = []
    equity_curve = [1.0]
    for as_of in benchmark_dates:
        feature_rows: list[InstrumentFeatures] = []
        for item in universe:
            if item.category == "benchmark":
                continue
            series = universe_series[item.code]
            sliced = slice_series_to_date(series, as_of)
            if sliced is None or len(sliced.bars) < 21:
                continue
            feature_rows.append(InstrumentFeatures(**compute_series_features(sliced, benchmark_series=slice_series_to_date(benchmark, as_of), category=item.category)))
        if not feature_rows:
            continue
        feature_rows.sort(key=lambda row: row.trend_score, reverse=True)
        candidate = feature_rows[0]
        outcome = next_day_outcome(universe_series[candidate.code], as_of)
        if outcome is None:
            continue
        trades.append({**outcome, "as_of": as_of, "code": candidate.code, "name": candidate.name, "trend_score": candidate.trend_score})
        equity_curve.append(equity_curve[-1] * (1 + outcome["next_day_return"]))
    max_drawdown = 0.0
    peak = equity_curve[0]
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    result = {
        "trade_count": len(trades),
        "win_rate": round((sum(1 for item in trades if item["next_day_return"] > 0) / len(trades)), 4) if trades else 0.0,
        "average_return": round(average([item["next_day_return"] for item in trades]) or 0.0, 4),
        "median_return": round(median([item["next_day_return"] for item in trades]) or 0.0, 4),
        "cumulative_return": round((equity_curve[-1] - 1.0), 4) if len(equity_curve) > 1 else 0.0,
        "max_drawdown": round(max_drawdown, 4),
        "trades": trades,
    }
    return result


def backtest_top_idea_strategy(
    config: dict[str, Any],
    *,
    as_of: str,
    strategy_style: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
    benchmark_code: str = "000300",
    begin: str | None = None,
    end: str | None = None,
    lookback_sessions: int = 25,
) -> dict[str, Any]:
    cache_dir, benchmark_dates, cutoff = _resolve_backtest_dates(
        config,
        as_of=as_of,
        benchmark_code=benchmark_code,
        begin=begin,
        end=end,
        lookback_sessions=lookback_sessions,
    )

    trades: list[dict[str, Any]] = []
    equity_curve = [1.0]
    series_cache: dict[str, Any] = {}
    skipped_signal_count = 0

    for trade_date in benchmark_dates:
        discovery = discover_top_ideas(
            config,
            as_of=trade_date,
            limit=1,
            strategy_style=strategy_style,
            holdings_file=holdings_file,
            cash=cash,
            write_output=False,
        )
        ideas = discovery.get("ideas") or []
        if not ideas:
            continue
        candidate = ideas[0]
        if not _is_actionable_candidate(candidate):
            skipped_signal_count += 1
            continue
        code = str(candidate.get("code") or "")
        if not code:
            continue
        name = str(candidate.get("name") or code)
        series = series_cache.get(code)
        if series is None:
            series = fetch_daily_series(code=code, name=name, cache_dir=cache_dir, begin="20240101", end="20500101")
            series_cache[code] = series
        outcome = next_day_outcome(series, trade_date)
        if outcome is None:
            continue
        selection_score = _selection_score_from_candidate(candidate)
        trade_action = str(candidate.get("trade_action") or candidate.get("decision") or "待确认")
        trades.append(
            {
                **outcome,
                "as_of": trade_date,
                "code": code,
                "name": name,
                "selection_score": round(selection_score, 2),
                "strategy_style": strategy_style,
                "decision": str(candidate.get("decision") or ""),
                "trade_action": trade_action,
            }
        )
        equity_curve.append(equity_curve[-1] * (1 + outcome["next_day_return"]))

    return _summarize_backtest(
        trades,
        equity_curve=equity_curve,
        extras={
            "as_of": as_of,
            "strategy_style": strategy_style,
            "benchmark_code": benchmark_code,
            "begin": begin,
            "end": cutoff,
            "signal_count": len(benchmark_dates),
            "skipped_signal_count": skipped_signal_count,
            "actionable_ratio": round((len(trades) / len(benchmark_dates)), 4) if benchmark_dates else 0.0,
        },
    )


def backtest_candidate_pool(
    config: dict[str, Any],
    *,
    as_of: str,
    candidates: list[dict[str, Any]],
    strategy_style: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
    benchmark_code: str = "000300",
    begin: str | None = None,
    end: str | None = None,
    lookback_sessions: int = 25,
    historical_limit: int | None = None,
) -> list[dict[str, Any]]:
    if not candidates:
        return []

    cache_dir, benchmark_dates, cutoff = _resolve_backtest_dates(
        config,
        as_of=as_of,
        benchmark_code=benchmark_code,
        begin=begin,
        end=end,
        lookback_sessions=lookback_sessions,
    )
    current_by_code = {
        str(item.get("code") or ""): item
        for item in candidates
        if str(item.get("code") or "")
    }
    ordered_codes = [str(item.get("code") or "") for item in candidates if str(item.get("code") or "")]
    per_code_trades: dict[str, list[dict[str, Any]]] = {code: [] for code in ordered_codes}
    skipped_by_code: dict[str, int] = {code: 0 for code in ordered_codes}
    series_cache: dict[str, Any] = {}
    discovery_limit = max(int(historical_limit or 0), len(ordered_codes), 12)

    for trade_date in benchmark_dates:
        discovery = discover_top_ideas(
            config,
            as_of=trade_date,
            limit=discovery_limit,
            strategy_style=strategy_style,
            holdings_file=holdings_file,
            cash=cash,
            write_output=False,
        )
        ideas_by_code = {
            str(item.get("code") or ""): item
            for item in (discovery.get("ideas") or [])
            if str(item.get("code") or "")
        }
        for code in ordered_codes:
            historical_candidate = ideas_by_code.get(code)
            if historical_candidate is None:
                continue
            if not _is_actionable_candidate(historical_candidate):
                skipped_by_code[code] = int(skipped_by_code.get(code) or 0) + 1
                continue
            current_candidate = current_by_code[code]
            name = str(
                historical_candidate.get("name")
                or current_candidate.get("name")
                or current_candidate.get("code")
                or code
            )
            series = series_cache.get(code)
            if series is None:
                series = fetch_daily_series(code=code, name=name, cache_dir=cache_dir, begin="20240101", end="20500101")
                series_cache[code] = series
            outcome = next_day_outcome(series, trade_date)
            if outcome is None:
                continue
            per_code_trades[code].append(
                {
                    **outcome,
                    "as_of": trade_date,
                    "code": code,
                    "name": name,
                    "selection_score": round(_selection_score_from_candidate(historical_candidate), 2),
                    "strategy_style": strategy_style,
                    "decision": str(historical_candidate.get("decision") or ""),
                    "trade_action": str(historical_candidate.get("trade_action") or historical_candidate.get("decision") or "待确认"),
                }
            )

    rankings: list[dict[str, Any]] = []
    for code in ordered_codes:
        current_candidate = current_by_code[code]
        trades = per_code_trades.get(code) or []
        backtest = _summarize_backtest(
            trades,
            equity_curve=_build_equity_curve(trades),
            extras={
                "as_of": as_of,
                "strategy_style": strategy_style,
                "benchmark_code": benchmark_code,
                "begin": begin,
                "end": cutoff,
                "candidate_code": code,
                "candidate_name": str(current_candidate.get("name") or code),
                "signal_count": len(benchmark_dates),
                "skipped_signal_count": int(skipped_by_code.get(code) or 0),
                "actionable_ratio": round((len(trades) / len(benchmark_dates)), 4) if benchmark_dates else 0.0,
            },
        )
        rankings.append(
            {
                **current_candidate,
                "code": code,
                "name": str(current_candidate.get("name") or code),
                "selection_score": round(_selection_score_from_candidate(current_candidate), 2),
                "trade_count": int(backtest.get("trade_count") or 0),
                "signal_count": int(backtest.get("signal_count") or 0),
                "skipped_signal_count": int(backtest.get("skipped_signal_count") or 0),
                "actionable_ratio": float(backtest.get("actionable_ratio") or 0.0),
                "win_rate": float(backtest.get("win_rate") or 0.0),
                "average_return": float(backtest.get("average_return") or 0.0),
                "backtest": backtest,
            }
        )
    return rankings


def write_backtest_report(result: dict[str, Any], *, output_dir: str | Path, as_of: str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    markdown_path = root / f"{as_of}_backtest_report.md"
    markdown_path.write_text(render_backtest_report(result), encoding="utf-8")
    return {"markdown": markdown_path, "json": write_json(root / f"{as_of}_backtest_report.json", result)}


def render_backtest_report(result: dict[str, Any]) -> str:
    lines = [
        "# 历史回测",
        "",
        f"- 交易数 `{result['trade_count']}` | 胜率 `{result['win_rate']:.2%}` | 平均收益 `{result['average_return']:+.2%}`",
        f"- 累计收益 `{result['cumulative_return']:+.2%}` | 最大回撤 `{result['max_drawdown']:+.2%}`",
        "",
        "## Trades",
    ]
    for row in result["trades"][:20]:
        score_key = "selection_score" if "selection_score" in row else "trend_score"
        score_label = "选股分" if score_key == "selection_score" else "趋势分"
        lines.append(f"- {row['as_of']} | {row['name']}({row['code']}) | {score_label} `{row[score_key]:.1f}` | 次日 `{row['next_day_return']:+.2%}`")
    return "\n".join(lines) + "\n"


def slice_series_to_date(series, as_of: str):
    bars = [bar for bar in series.bars if bar.trade_date <= as_of]
    if not bars:
        return None
    cloned = type(series)(
        code=series.code,
        name=series.name,
        secid=series.secid,
        fetched_at=series.fetched_at,
        source=series.source,
        bars=bars,
        used_cache=series.used_cache,
        degraded=series.degraded,
    )
    return cloned


def next_day_outcome(series, as_of: str) -> dict[str, Any] | None:
    dates = [bar.trade_date for bar in series.bars]
    if as_of not in dates:
        return None
    index = dates.index(as_of)
    if index + 1 >= len(series.bars):
        return None
    signal = series.bars[index]
    next_bar = series.bars[index + 1]
    next_day_return = (next_bar.close_price / signal.close_price) - 1.0 if signal.close_price else 0.0
    return {"next_trade_date": next_bar.trade_date, "next_day_return": next_day_return}


def _resolve_backtest_dates(
    config: dict[str, Any],
    *,
    as_of: str,
    benchmark_code: str,
    begin: str | None,
    end: str | None,
    lookback_sessions: int,
) -> tuple[Path, list[str], str | None]:
    cache_dir = Path(config["project"]["daily_bar_cache_dir"])
    benchmark = fetch_daily_series(code=benchmark_code, name="沪深300", cache_dir=cache_dir, begin="20240101", end="20500101")
    benchmark_dates = [bar.trade_date for bar in benchmark.bars]
    if begin is not None:
        benchmark_dates = [item for item in benchmark_dates if item >= begin]
    cutoff = end or as_of
    if cutoff is not None:
        benchmark_dates = [item for item in benchmark_dates if item <= cutoff]
    benchmark_dates = benchmark_dates[20:-1]
    if lookback_sessions > 0:
        benchmark_dates = benchmark_dates[-lookback_sessions:]
    return cache_dir, _filter_to_state_backed_dates(config, benchmark_dates) or benchmark_dates, cutoff


def _selection_score_from_candidate(candidate: dict[str, Any]) -> float:
    return float((candidate.get("metadata") or {}).get("selection_score") or candidate.get("priority_score") or candidate.get("total_score") or 0.0)


def _is_actionable_candidate(candidate: dict[str, Any]) -> bool:
    decision = str(candidate.get("decision") or "").strip().lower()
    if decision in {"buy", "add"}:
        return True
    action = str((candidate.get("action_plan") or {}).get("action") or "").strip()
    return action in {"standard_position", "trial_position", "add_on_strength", "buy_on_pullback"}


def _build_equity_curve(trades: list[dict[str, Any]]) -> list[float]:
    equity_curve = [1.0]
    for trade in trades:
        equity_curve.append(equity_curve[-1] * (1 + float(trade.get("next_day_return") or 0.0)))
    return equity_curve


def _filter_to_state_backed_dates(config: dict[str, Any], benchmark_dates: list[str]) -> list[str]:
    state_dir = ((config.get("project") or {}).get("state_dir")) if isinstance(config, dict) else None
    if not state_dir:
        return []
    root = Path(state_dir)
    available_dates = [
        trade_date
        for trade_date in benchmark_dates
        if (root / trade_date / "decision_bundle.json").exists() and (root / trade_date / "series_snapshots.json").exists()
    ]
    return available_dates


def _summarize_backtest(
    trades: list[dict[str, Any]],
    *,
    equity_curve: list[float],
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    max_drawdown = 0.0
    peak = equity_curve[0]
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return {
        **(extras or {}),
        "trade_count": len(trades),
        "win_rate": round((sum(1 for item in trades if item["next_day_return"] > 0) / len(trades)), 4) if trades else 0.0,
        "average_return": round(average([item["next_day_return"] for item in trades]) or 0.0, 4),
        "median_return": round(median([item["next_day_return"] for item in trades]) or 0.0, 4),
        "cumulative_return": round((equity_curve[-1] - 1.0), 4) if len(equity_curve) > 1 else 0.0,
        "max_drawdown": round(max_drawdown, 4),
        "trades": trades,
    }

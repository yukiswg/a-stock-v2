from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from ..advice_harness.evidence import AdviceSnapshot
from ..data_harness.market_data import code_to_secid, compute_series_features, load_cached_series
from ..decision_core import candidate_selection_score, candidate_style_fit, evaluate_security, infer_market_strategy_style
from ..decision_harness.engine import build_market_decision
from ..evaluation_harness.backtest import next_day_outcome, slice_series_to_date
from ..models import DailySeriesSnapshot
from ..utils import average, load_json, median, write_json


SUPPORTED_STOCK_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
BENCHMARK_CODE = "000300"
BENCHMARK_NAME = "沪深300"


def evaluate_fixed_pool_topn_strategy(
    *,
    state_dir: str | Path,
    cache_dir: str | Path,
    pool_size: int = 20,
    top_n: int = 5,
    begin: str | None = None,
    end: str | None = None,
    benchmark_code: str = BENCHMARK_CODE,
) -> dict[str, Any]:
    state_root = Path(state_dir)
    cache_root = Path(cache_dir)
    pool_rows, pool_assumptions = build_fixed_stock_pool(
        state_dir=state_root,
        cache_dir=cache_root,
        pool_size=pool_size,
    )
    if not pool_rows:
        raise ValueError("Unable to construct a fixed stock pool from state/cache.")

    benchmark_series = load_cached_series_snapshot(code=benchmark_code, name=BENCHMARK_NAME, cache_dir=cache_root)
    if benchmark_series is None or len(benchmark_series.bars) < 25:
        raise ValueError(f"Missing usable benchmark cache for {benchmark_code}")

    series_map = {
        row["code"]: load_cached_series_snapshot(code=row["code"], name=row["name"], cache_dir=cache_root)
        for row in pool_rows
    }
    series_map = {code: series for code, series in series_map.items() if series is not None}
    benchmark_dates = [bar.trade_date for bar in benchmark_series.bars]
    if begin is not None:
        benchmark_dates = [item for item in benchmark_dates if item >= begin]
    if end is not None:
        benchmark_dates = [item for item in benchmark_dates if item <= end]
    benchmark_dates = benchmark_dates[20:-1]

    trades: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    assumptions = list(pool_assumptions)
    state_backed_days = 0
    rebuilt_days = 0

    for as_of in benchmark_dates:
        benchmark_slice = slice_series_to_date(benchmark_series, as_of)
        if benchmark_slice is None or len(benchmark_slice.bars) < 21:
            continue
        state_context = load_state_context(state_root=state_root, as_of=as_of)
        if state_context["used_state"]:
            state_backed_days += 1
        else:
            rebuilt_days += 1

        feature_rows: list[dict[str, Any]] = []
        sliced_series_map: dict[str, dict[str, Any]] = {benchmark_code: benchmark_slice.to_dict()}
        for row in pool_rows:
            series = series_map.get(row["code"])
            if series is None:
                continue
            sliced = slice_series_to_date(series, as_of)
            if sliced is None or len(sliced.bars) < 21:
                continue
            sliced_series_map[row["code"]] = sliced.to_dict()
            feature_rows.append(
                compute_series_features(
                    sliced,
                    benchmark_series=benchmark_slice,
                    category=str(row.get("category") or "watch"),
                )
            )
        if len(feature_rows) < top_n:
            continue

        market_view = state_context["market_view"]
        if not market_view:
            market_view = build_market_decision(
                as_of=as_of,
                feature_map={**{benchmark_code: compute_series_features(benchmark_slice, benchmark_series=benchmark_slice, category="benchmark")}, **{item["code"]: item for item in feature_rows}},
                news_items=[],
                announcements=[],
                sector_metrics=state_context["sector_metrics"],
            ).to_dict()
        strategy_style = infer_market_strategy_style(
            action=str(market_view.get("action") or ""),
            regime=str((market_view.get("metadata") or {}).get("regime") or ""),
        )
        snapshot = AdviceSnapshot(
            as_of=as_of,
            state_root=state_root / as_of if state_context["used_state"] else None,
            holdings={"positions": [], "alerts": [], "sector_weights": []},
            universe=[dict(item) for item in pool_rows],
            feature_map={item["code"]: item for item in feature_rows},
            series_map=sliced_series_map,
            decision_bundle={"market_view": market_view},
            news_items=state_context["news_items"],
            announcements=state_context["announcements"],
            supplemental=state_context["supplemental"],
            explicit_sector_map=state_context["sector_map"],
            sector_metrics=state_context["sector_metrics"],
            name_map={item["code"]: item["name"] for item in pool_rows},
        )
        config = {"project": {"daily_bar_cache_dir": str(cache_root)}, "supplemental": {"enabled": False}}

        rankings: list[dict[str, Any]] = []
        for feature in feature_rows:
            evaluation = evaluate_security(
                snapshot,
                config=config,
                code=str(feature["code"]),
                name=str(feature["name"]),
                category=str(feature.get("category") or "watch"),
                question_type="should_buy",
                horizon="swing",
                risk_profile="balanced",
                strategy_style=strategy_style,
                pdf_payload=None,
                allow_supplemental_refresh=False,
                fetch_announcements=False,
            )
            style_fit_score = candidate_style_fit(evaluation, strategy_style=strategy_style)
            selection_score = candidate_selection_score(evaluation, strategy_style=strategy_style, feature=feature)
            rankings.append(
                {
                    "code": str(feature["code"]),
                    "name": str(feature["name"]),
                    "selection_score": round(float(selection_score), 2),
                    "style_fit_score": round(float(style_fit_score), 2),
                    "total_score": round(float(getattr(evaluation.scorecard, "total_score", 0.0)), 2),
                    "decision": str(getattr(evaluation, "decision", "")),
                    "action": str(getattr(getattr(evaluation, "action_plan", None), "action", "")),
                }
            )
        rankings.sort(
            key=lambda item: (
                float(item.get("selection_score") or 0.0),
                float(item.get("style_fit_score") or 0.0),
                float(item.get("total_score") or 0.0),
                str(item.get("code") or ""),
            ),
            reverse=True,
        )
        selected = rankings[:top_n]
        day_trades: list[dict[str, Any]] = []
        for rank, item in enumerate(selected, start=1):
            full_series = series_map.get(item["code"])
            if full_series is None:
                continue
            outcome = next_day_outcome(full_series, as_of)
            if outcome is None:
                continue
            trade = {
                **item,
                **outcome,
                "as_of": as_of,
                "rank": rank,
            }
            trades.append(trade)
            day_trades.append(trade)
        if not day_trades:
            continue
        daily_return = average([float(item["next_day_return"]) for item in day_trades]) or 0.0
        daily_rows.append(
            {
                "as_of": as_of,
                "next_trade_date": day_trades[0]["next_trade_date"],
                "selected_count": len(day_trades),
                "daily_return": round(daily_return, 6),
                "top_picks": [
                    {
                        "rank": item["rank"],
                        "code": item["code"],
                        "name": item["name"],
                        "selection_score": item["selection_score"],
                        "next_day_return": round(float(item["next_day_return"]), 6),
                    }
                    for item in day_trades
                ],
            }
        )

    equity_curve = build_equity_curve(daily_rows)
    result = {
        "pool_size": len(pool_rows),
        "top_n": top_n,
        "benchmark_code": benchmark_code,
        "begin": begin,
        "end": end,
        "assumptions": assumptions,
        "state_backed_day_count": state_backed_days,
        "rebuilt_day_count": rebuilt_days,
        "day_count": len(daily_rows),
        "trade_count": len(trades),
        "win_rate": round((sum(1 for item in trades if float(item["next_day_return"]) > 0) / len(trades)), 4) if trades else 0.0,
        "average_return": round(average([float(item["next_day_return"]) for item in trades]) or 0.0, 4),
        "median_return": round(median([float(item["next_day_return"]) for item in trades]) or 0.0, 4),
        "cumulative_return": round((equity_curve[-1] - 1.0), 4) if len(equity_curve) > 1 else 0.0,
        "max_drawdown": round(compute_max_drawdown(equity_curve), 4),
        "pool": pool_rows,
        "daily_rows": daily_rows,
        "trades": trades,
    }
    return result


def build_fixed_stock_pool(
    *,
    state_dir: str | Path,
    cache_dir: str | Path,
    pool_size: int = 20,
) -> tuple[list[dict[str, Any]], list[str]]:
    state_root = Path(state_dir)
    cache_root = Path(cache_dir)
    assumptions: list[str] = []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    latest_state = latest_state_root(state_root)
    if latest_state is not None:
        assumptions.append(f"固定池优先采用最新 state `{latest_state.name}` 中的股票候选，再用 cache 补满到 {pool_size} 只。")
        name_map = load_name_map_from_state(latest_state)
        for item in (load_json(latest_state / "universe_effective.json", default=[]) or []):
            code = str((item or {}).get("code") or "").strip()
            if not is_likely_stock_code(code) or code in seen:
                continue
            rows.append({"code": code, "name": str((item or {}).get("name") or name_map.get(code) or code), "category": "watch"})
            seen.add(code)
        for payload_name in ("features.json", "series_snapshots.json"):
            payload = load_json(latest_state / payload_name, default={}) or {}
            for code in sorted(payload):
                if not is_likely_stock_code(code) or code in seen:
                    continue
                rows.append({"code": code, "name": str(name_map.get(code) or code), "category": "watch"})
                seen.add(code)
    if len(rows) < pool_size:
        assumptions.append("仓库内没有现成 20 股 universe，因此使用最新 state + cache 文件名按代码升序补足固定池。")
    for path in sorted(cache_root.glob("*.json")):
        code = code_from_cache_filename(path.stem)
        if not is_likely_stock_code(code) or code in seen:
            continue
        bars = load_cached_series(cache_root, secid=code_to_secid(code))
        if len(bars) < 30:
            continue
        rows.append({"code": code, "name": code, "category": "watch"})
        seen.add(code)
        if len(rows) >= pool_size:
            break
    return rows[:pool_size], assumptions


def render_fixed_pool_topn_report(result: dict[str, Any]) -> str:
    lines = [
        "# 固定池前五评估",
        "",
        f"- 固定池 `{result['pool_size']}` 只 | 每日取前 `{result['top_n']}` 名 | 交易日 `{result['day_count']}` | 交易 `{result['trade_count']}`",
        f"- 胜率 `{result['win_rate']:.2%}` | 平均收益 `{result['average_return']:+.2%}` | 累计收益 `{result['cumulative_return']:+.2%}` | 最大回撤 `{result['max_drawdown']:+.2%}`",
        "",
        "## Assumptions",
    ]
    for item in result.get("assumptions") or []:
        lines.append(f"- {item}")
    lines.extend(["", "## Daily Top Picks"])
    for row in (result.get("daily_rows") or [])[:20]:
        picks = " | ".join(
            f"{item['rank']}. {item['name']}({item['code']}) `{item['selection_score']:.1f}` 次日 `{item['next_day_return']:+.2%}`"
            for item in (row.get("top_picks") or [])
        )
        lines.append(f"- {row['as_of']} -> {row['next_trade_date']} | 组合 `{row['daily_return']:+.2%}` | {picks}")
    return "\n".join(lines) + "\n"


def write_fixed_pool_topn_report(result: dict[str, Any], *, output_dir: str | Path, as_of: str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    markdown_path = root / f"{as_of}_fixed_pool_top{int(result.get('top_n') or 5)}_evaluation.md"
    markdown_path.write_text(render_fixed_pool_topn_report(result), encoding="utf-8")
    return {
        "markdown": markdown_path,
        "json": write_json(root / f"{as_of}_fixed_pool_top{int(result.get('top_n') or 5)}_evaluation.json", result),
    }


def load_state_context(*, state_root: Path, as_of: str) -> dict[str, Any]:
    root = state_root / as_of
    decision_bundle = load_json(root / "decision_bundle.json", default={}) or {}
    holdings = load_json(root / "holdings_snapshot.json", default={}) or {}
    sector_map = load_json(root / "sector_map.json", default={}) or {}
    sector_metrics = load_json(root / "sector_metrics.json", default={}) or {}
    supplemental = {
        "fundamentals": load_json(root / "fundamentals.json", default={}) or {},
        "valuation": load_json(root / "valuation.json", default={}) or {},
        "capital_flow": load_json(root / "capital_flow.json", default={}) or {},
        "company_info": load_json(root / "company_info.json", default={}) or {},
    }
    explicit_sector_map = {
        str(code): str(label)
        for code, label in sector_map.items()
        if isinstance(code, str) and isinstance(label, str)
    }
    for position in (holdings.get("positions") or []):
        code = str((position or {}).get("code") or "").strip()
        sector = str((position or {}).get("sector") or "").strip()
        if code and sector and code not in explicit_sector_map:
            explicit_sector_map[code] = sector
    return {
        "used_state": bool(decision_bundle),
        "market_view": dict(decision_bundle.get("market_view") or {}),
        "news_items": list(load_json(root / "news.json", default=[]) or []),
        "announcements": list(load_json(root / "announcements.json", default=[]) or []),
        "supplemental": supplemental,
        "sector_map": explicit_sector_map,
        "sector_metrics": dict(sector_metrics or {}),
    }


def load_cached_series_snapshot(*, code: str, name: str, cache_dir: str | Path) -> DailySeriesSnapshot | None:
    bars = load_cached_series(cache_dir, secid=code_to_secid(code))
    if not bars:
        return None
    return DailySeriesSnapshot(
        code=code,
        name=name,
        secid=code_to_secid(code),
        fetched_at=datetime.now().isoformat(timespec="seconds"),
        source="cache",
        bars=bars,
        used_cache=True,
        degraded=False,
    )


def latest_state_root(state_root: Path) -> Path | None:
    dated_dirs = sorted(path for path in state_root.iterdir() if path.is_dir()) if state_root.exists() else []
    for path in reversed(dated_dirs):
        if (path / "decision_bundle.json").exists() or (path / "features.json").exists():
            return path
    return None


def load_name_map_from_state(state_root: Path) -> dict[str, str]:
    name_map: dict[str, str] = {}
    for item in (load_json(state_root / "universe_effective.json", default=[]) or []):
        code = str((item or {}).get("code") or "").strip()
        if code:
            name_map[code] = str((item or {}).get("name") or code)
    for code, row in (load_json(state_root / "features.json", default={}) or {}).items():
        name_map[str(code)] = str((row or {}).get("name") or code)
    for code, row in (load_json(state_root / "series_snapshots.json", default={}) or {}).items():
        name_map[str(code)] = str((row or {}).get("name") or code)
    return name_map


def code_from_cache_filename(stem: str) -> str:
    if "_" in stem:
        return stem.split("_", 1)[1]
    return stem


def is_likely_stock_code(code: str) -> bool:
    value = str(code or "").strip()
    if len(value) != 6 or not value.isdigit():
        return False
    return value.startswith(SUPPORTED_STOCK_PREFIXES)


def build_equity_curve(daily_rows: list[dict[str, Any]]) -> list[float]:
    curve = [1.0]
    for row in daily_rows:
        curve.append(curve[-1] * (1.0 + float(row.get("daily_return") or 0.0)))
    return curve


def compute_max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0] if equity_curve else 1.0
    max_drawdown = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        drawdown = (value / peak) - 1.0 if peak else 0.0
        max_drawdown = min(max_drawdown, drawdown)
    return max_drawdown

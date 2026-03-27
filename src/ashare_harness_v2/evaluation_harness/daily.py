from __future__ import annotations

from pathlib import Path
from typing import Any

from ..data_harness.market_data import fetch_daily_series
from ..utils import average, load_json, median, write_json


def evaluate_prediction_history(*, state_dir: str | Path, cache_dir: str | Path) -> dict[str, Any]:
    root = Path(state_dir)
    dated_dirs = sorted(path for path in root.iterdir() if path.is_dir()) if root.exists() else []
    market_rows: list[dict[str, Any]] = []
    watch_rows: list[dict[str, Any]] = []
    basis_hits = {"watch_score_ge_70": {"hits": 0, "total": 0}, "watch_score_lt_70": {"hits": 0, "total": 0}}
    for dated_dir in dated_dirs:
        bundle = load_json(dated_dir / "decision_bundle.json")
        if not bundle:
            continue
        as_of = str(bundle["as_of"])
        market = bundle["market_view"]
        market_outcome = evaluate_market_outcome(as_of=as_of, cache_dir=cache_dir)
        if market_outcome is not None:
            market_rows.append({**market_outcome, "as_of": as_of, "predicted_probability": market.get("probability"), "action": market.get("action")})
        for watch in bundle.get("watchlist", []):
            result = evaluate_stock_outcome(code=str(watch["object_id"]), name=str(watch["object_name"]), as_of=as_of, cache_dir=cache_dir)
            if result is None:
                continue
            row = {**result, "as_of": as_of, "score": watch.get("score"), "probability": watch.get("probability"), "action": watch.get("action")}
            watch_rows.append(row)
            bucket = "watch_score_ge_70" if float(watch.get("score") or 0.0) >= 70 else "watch_score_lt_70"
            basis_hits[bucket]["total"] += 1
            if row["next_day_return"] > 0:
                basis_hits[bucket]["hits"] += 1
    market_hit_rate = average([1.0 if row["actual_up"] else 0.0 for row in market_rows]) or 0.0
    watch_hit_rate = average([1.0 if row["next_day_return"] > 0 else 0.0 for row in watch_rows]) or 0.0
    result = {
        "market_count": len(market_rows),
        "market_hit_rate": round(market_hit_rate, 4),
        "market_average_next_day_return": round(average([row["next_day_return"] for row in market_rows]) or 0.0, 4),
        "watch_count": len(watch_rows),
        "watch_hit_rate": round(watch_hit_rate, 4),
        "watch_average_next_day_return": round(average([row["next_day_return"] for row in watch_rows]) or 0.0, 4),
        "watch_median_next_day_return": round(median([row["next_day_return"] for row in watch_rows]) or 0.0, 4),
        "basis_breakdown": {
            key: {
                "hit_rate": round((value["hits"] / value["total"]), 4) if value["total"] else None,
                "hits": value["hits"],
                "total": value["total"],
            }
            for key, value in basis_hits.items()
        },
        "market_rows": market_rows,
        "watch_rows": watch_rows,
    }
    return result


def write_prediction_evaluation(result: dict[str, Any], *, output_dir: str | Path, as_of: str) -> dict[str, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    markdown_path = root / f"{as_of}_prediction_evaluation.md"
    markdown_path.write_text(render_prediction_evaluation(result), encoding="utf-8")
    return {
        "markdown": markdown_path,
        "json": write_json(root / f"{as_of}_prediction_evaluation.json", result),
    }


def render_prediction_evaluation(result: dict[str, Any]) -> str:
    lines = [
        "# Prediction Evaluation",
        "",
        f"- 市场样本数: `{result['market_count']}` | 命中率 `{result['market_hit_rate']:.2%}` | 次日均值 `{result['market_average_next_day_return']:+.2%}`",
        f"- 观察名单样本数: `{result['watch_count']}` | 命中率 `{result['watch_hit_rate']:.2%}` | 次日均值 `{result['watch_average_next_day_return']:+.2%}`",
        "",
        "## Rule Breakdown",
    ]
    for key, row in result["basis_breakdown"].items():
        lines.append(f"- {key} | 命中 `{row['hits']}/{row['total']}` | 命中率 `{row['hit_rate'] if row['hit_rate'] is not None else '无'}`")
    return "\n".join(lines) + "\n"


def evaluate_market_outcome(*, as_of: str, cache_dir: str | Path) -> dict[str, Any] | None:
    series = fetch_daily_series(code="000300", name="沪深300", cache_dir=cache_dir, begin="20240101", end="20500101")
    dates = [bar.trade_date for bar in series.bars]
    if as_of not in dates:
        return None
    index = dates.index(as_of)
    if index + 1 >= len(series.bars):
        return None
    signal = series.bars[index]
    next_bar = series.bars[index + 1]
    next_day_return = (next_bar.close_price / signal.close_price) - 1.0 if signal.close_price else 0.0
    return {"next_trade_date": next_bar.trade_date, "next_day_return": next_day_return, "actual_up": next_day_return > 0}


def evaluate_stock_outcome(*, code: str, name: str, as_of: str, cache_dir: str | Path) -> dict[str, Any] | None:
    series = fetch_daily_series(code=code, name=name, cache_dir=cache_dir, begin="20240101", end="20500101")
    dates = [bar.trade_date for bar in series.bars]
    if as_of not in dates:
        return None
    index = dates.index(as_of)
    if index + 1 >= len(series.bars):
        return None
    signal = series.bars[index]
    next_bar = series.bars[index + 1]
    next_day_return = (next_bar.close_price / signal.close_price) - 1.0 if signal.close_price else 0.0
    return {"code": code, "name": name, "next_trade_date": next_bar.trade_date, "next_day_return": next_day_return}

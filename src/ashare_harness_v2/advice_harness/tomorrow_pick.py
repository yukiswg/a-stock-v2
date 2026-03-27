from __future__ import annotations

from typing import Any

from ..decision_core import infer_market_strategy_style
from ..evaluation_harness.backtest import backtest_candidate_pool, backtest_top_idea_strategy
from .engine import discover_top_ideas
from .evidence import build_advice_snapshot


def build_tomorrow_best_pick(
    config: dict[str, Any],
    *,
    as_of: str,
    strategy_style: str | None = None,
    holdings_file: str | None = None,
    cash: float = 0.0,
    lookback_sessions: int = 25,
    candidate_limit: int = 8,
) -> dict[str, Any]:
    resolved_style, market_view = _resolve_strategy_style(
        config,
        as_of=as_of,
        strategy_style=strategy_style,
        holdings_file=holdings_file,
        cash=cash,
    )
    discovery = discover_top_ideas(
        config,
        as_of=as_of,
        limit=max(int(candidate_limit or 0), 3),
        strategy_style=resolved_style,
        holdings_file=holdings_file,
        cash=cash,
        write_output=False,
    )
    ideas = [_normalize_pick(item, strategy_style=resolved_style) for item in (discovery.get("ideas") or [])]
    candidate_rankings = _rank_candidate_pool(
        ideas,
        candidate_backtests=backtest_candidate_pool(
            config,
            as_of=as_of,
            candidates=ideas,
            strategy_style=resolved_style,
            holdings_file=holdings_file,
            cash=cash,
            lookback_sessions=lookback_sessions,
            historical_limit=max(len(ideas), 12),
        ),
        strategy_style=resolved_style,
    )
    actionable_rankings = [item for item in candidate_rankings if bool(item.get("is_actionable"))]
    if candidate_rankings:
        selected_ranking = actionable_rankings[0] if actionable_rankings else candidate_rankings[0]
        pick = _normalize_pick(selected_ranking, strategy_style=resolved_style)
        backtest = dict(selected_ranking.get("backtest") or {})
    else:
        pick = ideas[0] if ideas else {}
        backtest = _normalize_backtest(
            backtest_top_idea_strategy(
                config,
                as_of=as_of,
                strategy_style=resolved_style,
                holdings_file=holdings_file,
                cash=cash,
                lookback_sessions=lookback_sessions,
            ),
            strategy_style=resolved_style,
            pick=pick,
        )
    best_stock = _build_best_stock_payload(
        pick=pick,
        backtest=backtest,
        strategy_style=resolved_style,
        composite_score=float((candidate_rankings[0] or {}).get("composite_score") or 0.0) if candidate_rankings else 0.0,
    )
    return {
        "as_of": as_of,
        "strategy_style": resolved_style,
        "market_view": market_view or discovery.get("market_view") or {},
        "pick": pick,
        "backtest": backtest,
        "candidate_rankings": candidate_rankings,
        "actionable_candidate_count": len(actionable_rankings),
        "best_stock": best_stock,
    }


def _resolve_strategy_style(
    config: dict[str, Any],
    *,
    as_of: str,
    strategy_style: str | None,
    holdings_file: str | None,
    cash: float,
) -> tuple[str, dict[str, Any]]:
    if strategy_style:
        return strategy_style, {}
    snapshot = build_advice_snapshot(config, as_of=as_of, holdings_file=holdings_file, cash=cash)
    market_view = snapshot.decision_bundle.get("market_view") or {}
    resolved_style = infer_market_strategy_style(
        action=str(market_view.get("action") or ""),
        regime=str(market_view.get("regime") or ""),
    )
    return resolved_style, market_view


def _normalize_backtest(
    backtest: dict[str, Any],
    *,
    strategy_style: str,
    pick: dict[str, Any],
) -> dict[str, Any]:
    normalized_trades: list[dict[str, Any]] = []
    default_code = str(pick.get("code") or "")
    default_name = str(pick.get("name") or default_code)
    default_action = str(pick.get("trade_action") or pick.get("decision") or "待确认")
    for row in backtest.get("trades") or []:
        trade = dict(row)
        trade["code"] = str(trade.get("code") or default_code)
        trade["name"] = str(trade.get("name") or default_name or trade["code"])
        trade["trade_action"] = str(trade.get("trade_action") or trade.get("decision") or default_action)
        trade["next_day_return"] = float(trade.get("next_day_return") or 0.0)
        if trade.get("selection_score") is not None:
            trade["selection_score"] = round(float(trade.get("selection_score") or 0.0), 2)
        trade["strategy_style"] = str(trade.get("strategy_style") or strategy_style)
        normalized_trades.append(trade)
    normalized_trades.sort(key=lambda item: str(item.get("as_of") or ""), reverse=True)
    return {
        **backtest,
        "trade_count": len(normalized_trades),
        "signal_count": int(backtest.get("signal_count") or len(normalized_trades)),
        "skipped_signal_count": int(backtest.get("skipped_signal_count") or 0),
        "actionable_ratio": round(float(backtest.get("actionable_ratio") or 0.0), 4),
        "win_rate": round(float(backtest.get("win_rate") or 0.0), 4),
        "average_return": round(float(backtest.get("average_return") or 0.0), 4),
        "median_return": round(float(backtest.get("median_return") or 0.0), 4),
        "cumulative_return": round(float(backtest.get("cumulative_return") or 0.0), 4),
        "max_drawdown": round(float(backtest.get("max_drawdown") or 0.0), 4),
        "trades": normalized_trades,
    }


def _build_best_stock_payload(
    *,
    pick: dict[str, Any],
    backtest: dict[str, Any],
    strategy_style: str,
    composite_score: float = 0.0,
) -> dict[str, Any]:
    normalized_pick = dict(pick)
    normalized_pick["code"] = str(normalized_pick.get("code") or "")
    normalized_pick["name"] = str(normalized_pick.get("name") or normalized_pick["code"])
    normalized_pick["strategy_style"] = str(normalized_pick.get("strategy_style") or strategy_style)
    normalized_pick["selection_score"] = round(float(normalized_pick.get("selection_score") or 0.0), 2)
    normalized_pick["composite_score"] = round(float(normalized_pick.get("composite_score") or composite_score or 0.0), 2)
    normalized_pick["trade_action"] = str(normalized_pick.get("trade_action") or normalized_pick.get("decision") or "待确认")
    normalized_pick["backtest"] = dict(backtest)
    return normalized_pick


def _normalize_pick(idea: dict[str, Any], *, strategy_style: str) -> dict[str, Any]:
    metadata = dict(idea.get("metadata") or {})
    selection_score = float(metadata.get("selection_score") or idea.get("priority_score") or idea.get("total_score") or 0.0)
    metadata["selection_score"] = round(selection_score, 2)
    metadata["strategy_style"] = str(metadata.get("strategy_style") or strategy_style)
    metadata["is_actionable"] = _is_actionable_pick(idea)
    return {
        **idea,
        "name": str(idea.get("name") or idea.get("code") or ""),
        "selection_score": round(selection_score, 2),
        "strategy_style": metadata["strategy_style"],
        "is_actionable": bool(metadata.get("is_actionable")),
        "metadata": metadata,
    }


def _rank_candidate_pool(
    ideas: list[dict[str, Any]],
    *,
    candidate_backtests: list[dict[str, Any]],
    strategy_style: str,
) -> list[dict[str, Any]]:
    backtest_by_code = {
        str(item.get("code") or ""): item
        for item in candidate_backtests
        if str(item.get("code") or "")
    }
    rankings: list[dict[str, Any]] = []
    for idea in ideas:
        code = str(idea.get("code") or "")
        candidate_backtest = backtest_by_code.get(code) or {}
        normalized_backtest = _normalize_backtest(
            dict(candidate_backtest.get("backtest") or {}),
            strategy_style=strategy_style,
            pick=idea,
        )
        ranking = {
            **idea,
            "trade_count": int(candidate_backtest.get("trade_count") or normalized_backtest.get("trade_count") or 0),
            "signal_count": int(candidate_backtest.get("signal_count") or normalized_backtest.get("signal_count") or 0),
            "skipped_signal_count": int(candidate_backtest.get("skipped_signal_count") or normalized_backtest.get("skipped_signal_count") or 0),
            "actionable_ratio": round(float(candidate_backtest.get("actionable_ratio") or normalized_backtest.get("actionable_ratio") or 0.0), 4),
            "win_rate": round(float(candidate_backtest.get("win_rate") or normalized_backtest.get("win_rate") or 0.0), 4),
            "average_return": round(float(candidate_backtest.get("average_return") or normalized_backtest.get("average_return") or 0.0), 4),
            "is_actionable": _is_actionable_pick(idea),
            "composite_score": _composite_score(
                selection_score=float(idea.get("selection_score") or 0.0),
                trade_count=int(candidate_backtest.get("trade_count") or normalized_backtest.get("trade_count") or 0),
                win_rate=float(candidate_backtest.get("win_rate") or normalized_backtest.get("win_rate") or 0.0),
                average_return=float(candidate_backtest.get("average_return") or normalized_backtest.get("average_return") or 0.0),
                median_return=float(candidate_backtest.get("median_return") or normalized_backtest.get("median_return") or 0.0),
                max_drawdown=float(candidate_backtest.get("max_drawdown") or normalized_backtest.get("max_drawdown") or 0.0),
                decision=str(idea.get("decision") or ""),
                actionable_ratio=float(candidate_backtest.get("actionable_ratio") or normalized_backtest.get("actionable_ratio") or 0.0),
            ),
            "backtest": normalized_backtest,
        }
        rankings.append(ranking)
    rankings.sort(
        key=lambda item: (
            float(item.get("composite_score") or 0.0),
            1 if item.get("is_actionable") else 0,
            float(item.get("selection_score") or 0.0),
            float(item.get("win_rate") or 0.0),
            float(item.get("average_return") or 0.0),
            int(item.get("trade_count") or 0),
        ),
        reverse=True,
    )
    return rankings


def _composite_score(
    *,
    selection_score: float,
    trade_count: int,
    win_rate: float,
    average_return: float,
    median_return: float,
    max_drawdown: float,
    decision: str,
    actionable_ratio: float,
) -> float:
    sample_strength = min(max(trade_count, 0) / 4.0, 1.0) ** 2
    history_component = sample_strength * (
        (max(win_rate, 0.0) * 100.0 * 0.20)
        + (average_return * 100.0 * 1.80)
        + (median_return * 100.0 * 0.90)
        - (abs(min(max_drawdown, 0.0)) * 100.0 * 0.60)
        + (min(max(trade_count, 0), 6) * 1.2)
    )
    normalized_decision = str(decision or "").strip().lower()
    decision_bonus = 4.0 if normalized_decision == "buy" else 1.5 if normalized_decision == "watch" else -4.0
    readiness_component = min(max(actionable_ratio, 0.0), 1.0) * 6.0
    return round((selection_score * 0.55) + history_component + decision_bonus + readiness_component, 2)


def _is_actionable_pick(idea: dict[str, Any]) -> bool:
    decision = str(idea.get("decision") or "").strip().lower()
    return decision in {"buy", "add"}

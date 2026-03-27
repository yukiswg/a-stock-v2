from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from ..advice_harness import build_tomorrow_best_pick, discover_top_ideas
from ..agent_layer.engine import build_agent_context, run_structured_agent
from ..config import UniverseItem, load_universe
from ..data_harness.holdings import hydrate_holdings_snapshot_with_prices, load_holdings_snapshot
from ..data_harness.market_data import fetch_daily_series
from ..data_harness.news import collect_news, fetch_holdings_announcements
from ..data_harness.supplemental import ensure_sector_metrics_payload, ensure_supplemental_payload, filter_supplemental_codes
from ..decision_core import infer_market_strategy_style
from ..decision_harness.engine import build_decision_bundle, write_daily_outputs
from ..evaluation_harness.backtest import backtest_watchlist_strategy, write_backtest_report
from ..evaluation_harness.daily import evaluate_prediction_history, write_prediction_evaluation
from ..evaluation_harness.intraday import build_intraday_review, write_intraday_review
from ..models import AnnouncementItem, DailyBar, DailyDecisionBundle, DailySeriesSnapshot, HoldingPosition, HoldingsSnapshot, NewsItem, StructuredDecision
from ..skill_harness.sector_rotation import build_dynamic_universe_from_sectors
from ..skill_harness.trading_reports import build_best_stock_report
from ..utils import ensure_dir, load_json, today_stamp, write_json
from .homepage import update_homepage_with_live_quotes, update_homepage_with_session, write_homepage_assets
from .realtime import run_realtime_session
from .sessions import RuntimeSession


def run_premarket(
    *,
    config: dict[str, Any],
    as_of: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
) -> dict[str, Any]:
    project = config["project"]
    analysis = config["analysis"]
    runtime = RuntimeSession(runtime_dir=project["runtime_dir"], as_of=as_of, session_type="premarket")
    runtime.log("premarket_start", as_of=as_of)
    holdings_path = holdings_file or project["holdings_file"]
    holdings = load_holdings_snapshot(holdings_path, as_of=as_of, cash=cash)
    base_universe = load_universe(project["universe_file"])
    holdings_codes = {position.code for position in holdings.positions}
    runtime.log("ensure_sector_metrics_start")
    sector_payload = ensure_sector_metrics_payload(config, as_of=as_of)
    runtime.log("ensure_sector_metrics_done", sector_count=len(sector_payload.get("sector_metrics") or {}))
    dynamic_universe = build_dynamic_universe_from_sectors(
        config=config,
        base_universe=base_universe,
        holdings_codes=holdings_codes,
        sector_metrics=sector_payload.get("sector_metrics") or {},
    )
    runtime.log(
        "dynamic_universe_built",
        leader_count=len(dynamic_universe.get("leaders") or []),
        top_sector_count=len(dynamic_universe.get("top_sectors") or []),
        failure_count=len(dynamic_universe.get("resolver_failures") or []),
    )
    universe = dynamic_universe["universe_items"]
    required = {item.code: item.name for item in universe}
    for position in holdings.positions:
        required[position.code] = position.name
    supplemental_codes = filter_supplemental_codes(
        [item.code for item in universe if item.category != "benchmark"] + [position.code for position in holdings.positions]
    )
    runtime.log("ensure_supplemental_payload_start", code_count=len(supplemental_codes))
    supplemental_payload = ensure_supplemental_payload(config, as_of=as_of, codes=supplemental_codes)
    runtime.log(
        "ensure_supplemental_payload_done",
        fundamentals=len(supplemental_payload.get("fundamentals") or {}),
        sector_map=len(supplemental_payload.get("sector_map") or {}),
    )
    series_map: dict[str, DailySeriesSnapshot] = {}
    for code, name in required.items():
        runtime.log("fetch_daily_series", code=code, name=name)
        series_map[code] = fetch_daily_series(code=code, name=name, cache_dir=project["daily_bar_cache_dir"], end=as_of)
    runtime.log("fetch_daily_series_done", code_count=len(series_map))
    holdings = hydrate_holdings_snapshot_with_prices(holdings, series_map=series_map)
    runtime.log("collect_news_start")
    news_items = collect_news(
        config["news"]["sources"],
        max_items_per_source=int(analysis["news_max_items_per_source"]),
        max_age_days=int(analysis["news_max_age_days"]),
    )
    runtime.log("collect_news_done", item_count=len(news_items))
    runtime.log("fetch_announcements_start", holding_count=len(holdings.positions))
    announcements = fetch_holdings_announcements(holdings.positions, limit_per_stock=int(analysis["announcement_limit_per_stock"]))
    runtime.log("fetch_announcements_done", item_count=len(announcements))
    news_items = [item for item in news_items if item.published_at is None or item.published_at <= as_of]
    announcements = [item for item in announcements if item.published_at is None or item.published_at <= as_of]
    runtime.log("build_decision_bundle_start")
    bundle, feature_map = build_decision_bundle(
        as_of=as_of,
        holdings=holdings,
        universe=universe,
        series_map=series_map,
        news_items=news_items,
        announcements=announcements,
        llm_summary={},
        config=config,
        supplemental={key: (supplemental_payload.get(key) or {}) for key in ("fundamentals", "valuation", "capital_flow", "company_info")},
        sector_map=supplemental_payload.get("sector_map") or {},
        sector_metrics=supplemental_payload.get("sector_metrics") or {},
    )
    runtime.log("build_decision_bundle_done", feature_count=len(feature_map))
    llm_context = build_agent_context(
        as_of=as_of,
        holdings_payload=holdings.to_dict(),
        feature_payload={code: feature.to_dict() for code, feature in feature_map.items()},
        market_payload=bundle.market_view.to_dict(),
        news_payload=[item.to_dict() for item in news_items],
        announcement_payload=[item.to_dict() for item in announcements],
    )
    runtime.log("run_structured_agent_start")
    llm_summary = run_structured_agent(config["llm"], context=llm_context)
    runtime.log("run_structured_agent_done", key_count=len(llm_summary))
    bundle.llm_summary = llm_summary
    bundle.homepage_overview["dynamic_universe"] = {
        "selection_mode": dynamic_universe.get("selection_mode"),
        "top_sectors": list(dynamic_universe.get("top_sectors") or []),
        "leaders": list(dynamic_universe.get("leaders") or []),
    }
    if as_of == today_stamp():
        bundle.homepage_overview = update_homepage_with_live_quotes(
            bundle.homepage_overview,
            as_of=as_of,
            stale_after_seconds=int(config["realtime"]["stale_after_seconds"]),
        )
    discovery_style = infer_market_strategy_style(
        action=bundle.market_view.action,
        regime=str(bundle.market_view.metadata.get("regime") or ""),
    )
    best_stock_payload = _build_best_stock_payload(
        config,
        as_of=as_of,
        strategy_style=discovery_style,
        holdings_file=holdings_path,
        cash=cash,
    )
    bundle.homepage_overview["best_stock"] = dict(best_stock_payload.get("best_stock") or {})
    report_root = ensure_dir(Path(project["report_dir"]) / as_of)
    runtime.log("write_daily_outputs_start")
    artifacts = write_daily_outputs(
        output_dir=report_root,
        bundle=bundle,
        holdings=holdings,
        feature_map=feature_map,
        news_items=news_items,
        announcements=announcements,
    )
    runtime.log("write_daily_outputs_done", artifact_count=len(artifacts))
    best_stock_report_artifacts = _write_best_stock_report_artifacts(config=config, as_of=as_of, payload=best_stock_payload)
    state_root = ensure_dir(Path(project["state_dir"]) / as_of)
    write_json(state_root / "holdings_snapshot.json", holdings.to_dict())
    write_json(state_root / "news.json", [item.to_dict() for item in news_items])
    write_json(state_root / "announcements.json", [item.to_dict() for item in announcements])
    write_json(state_root / "features.json", {code: feature.to_dict() for code, feature in feature_map.items()})
    write_json(state_root / "series_snapshots.json", {code: series.to_dict() for code, series in series_map.items()})
    write_json(state_root / "decision_bundle.json", bundle.to_dict())
    write_json(state_root / "homepage_overview.json", bundle.homepage_overview)
    write_json(state_root / "universe_effective.json", list(dynamic_universe.get("universe") or []))
    write_json(
        state_root / "dynamic_universe.json",
        {
            "selection_mode": dynamic_universe.get("selection_mode"),
            "top_sectors": list(dynamic_universe.get("top_sectors") or []),
            "leaders": list(dynamic_universe.get("leaders") or []),
            "resolver_failures": list(dynamic_universe.get("resolver_failures") or []),
        },
    )
    write_json(state_root / "agent_context.json", llm_context)
    write_json(state_root / "agent_output.json", llm_summary)
    homepage_overview = update_homepage_with_session(bundle.homepage_overview, session_dir=None)
    runtime.log("write_homepage_assets_start")
    homepage_assets = write_homepage_assets(overview=homepage_overview, homepage_dir=project["homepage_dir"], as_of=as_of)
    shutil.copy2(homepage_assets["markdown"], report_root / homepage_assets["markdown"].name)
    shutil.copy2(homepage_assets["json"], report_root / homepage_assets["json"].name)
    runtime.log("write_homepage_assets_done", artifact_count=len(homepage_assets))
    runtime.log("discover_top_ideas_start", strategy_style=discovery_style)
    discovery_payload = discover_top_ideas(
        config,
        as_of=as_of,
        limit=int(analysis["candidate_limit"]),
        strategy_style=discovery_style,
        holdings_file=holdings_path,
        cash=cash,
        write_output=True,
    )
    runtime.log("discover_top_ideas_done", artifact_count=len(discovery_payload.get("artifacts") or {}))
    runtime.add_artifact("daily_report", artifacts["daily_report"])
    runtime.add_artifact("comprehensive_report", artifacts["comprehensive_report"])
    runtime.add_artifact("integrated_report", artifacts["integrated_report"])
    runtime.add_artifact("monitor_plan", artifacts["monitor_plan_json"])
    runtime.add_artifact("homepage", homepage_assets["markdown"])
    for key, value in best_stock_report_artifacts.items():
        runtime.add_artifact(key, value)
    for key, value in (discovery_payload.get("artifacts") or {}).items():
        runtime.add_artifact(key, value)
    runtime.log("premarket_done", artifact_count=len(runtime.manifest.artifacts))
    runtime.finish(status="completed")
    return {
        "report_dir": report_root,
        "state_dir": state_root,
        "artifacts": {
            key: str(value)
            for key, value in {**artifacts, **homepage_assets, **best_stock_report_artifacts, **(discovery_payload.get("artifacts") or {})}.items()
        },
        "bundle": bundle.to_dict(),
        "discovery": discovery_payload,
    }


def run_intraday(
    *,
    config: dict[str, Any],
    as_of: str,
    source_kind: str = "live",
    replay_path: str | None = None,
    iterations: int | None = None,
    interval_seconds: float | None = None,
    sleep_enabled: bool = False,
) -> dict[str, Any]:
    project = config["project"]
    realtime_cfg = config["realtime"]
    report_root = Path(project["report_dir"]) / as_of
    monitor_plan_path = report_root / f"{as_of}_monitor_plan.json"
    if not monitor_plan_path.exists():
        raise FileNotFoundError(f"Missing monitor plan at {monitor_plan_path}")
    runtime = RuntimeSession(runtime_dir=project["runtime_dir"], as_of=as_of, session_type="intraday")
    runtime.log("intraday_start", source_kind=source_kind, as_of=as_of)
    result = run_realtime_session(
        as_of=as_of,
        realtime_dir=project["realtime_dir"],
        monitor_plan_path=monitor_plan_path,
        feature_snapshot_path=report_root / f"{as_of}_features.json",
        source_kind=source_kind,
        replay_path=replay_path,
        iterations=int(iterations or realtime_cfg["default_iterations"]),
        interval_seconds=float(interval_seconds or realtime_cfg["poll_interval_seconds"]),
        stale_after_seconds=int(realtime_cfg["stale_after_seconds"]),
        sleep_enabled=sleep_enabled,
    )
    homepage_json = load_json(report_root / f"{as_of}_homepage_overview.json", default={}) or load_json(Path(project["homepage_dir"]) / "latest_homepage.json", default={}) or {}
    homepage_overview = update_homepage_with_session(homepage_json, session_dir=result["session_dir"])
    state_root = Path(project["state_dir"]) / as_of
    _refresh_intraday_reports(
        state_root=state_root,
        report_root=report_root,
        as_of=as_of,
        homepage_overview=homepage_overview,
    )
    homepage_assets = write_homepage_assets(overview=homepage_overview, homepage_dir=project["homepage_dir"], as_of=as_of)
    shutil.copy2(homepage_assets["markdown"], report_root / homepage_assets["markdown"].name)
    shutil.copy2(homepage_assets["json"], report_root / homepage_assets["json"].name)
    runtime.add_artifact("session_dir", result["session_dir"])
    runtime.add_artifact("homepage", homepage_assets["markdown"])
    runtime.finish(status="completed")
    return {
        **result,
        "homepage_assets": {key: str(value) for key, value in homepage_assets.items()},
    }


def run_postclose(*, config: dict[str, Any], as_of: str, session_dir: str | Path | None = None) -> dict[str, Any]:
    project = config["project"]
    runtime = RuntimeSession(runtime_dir=project["runtime_dir"], as_of=as_of, session_type="postclose")
    runtime.log("postclose_start", as_of=as_of)
    selected_session = Path(session_dir) if session_dir is not None else latest_session_dir(project["realtime_dir"], as_of=as_of)
    if selected_session is None:
        raise FileNotFoundError("No realtime session available for post-close evaluation.")
    review = build_intraday_review(session_dir=selected_session, horizon_steps=int(config["analysis"]["alert_eval_horizon_steps"]))
    review_paths = write_intraday_review(review, output_dir=selected_session, as_of=as_of)
    evaluation_root = ensure_dir(Path(project["evaluation_dir"]) / as_of)
    prediction_result = evaluate_prediction_history(state_dir=project["state_dir"], cache_dir=project["daily_bar_cache_dir"])
    prediction_paths = write_prediction_evaluation(prediction_result, output_dir=evaluation_root, as_of=as_of)
    universe = load_universe(project["universe_file"])
    backtest_result = backtest_watchlist_strategy(universe=universe, cache_dir=project["daily_bar_cache_dir"])
    backtest_paths = write_backtest_report(backtest_result, output_dir=Path(project["backtest_dir"]) / as_of, as_of=as_of)
    runtime.add_artifact("intraday_review", review_paths["markdown"])
    runtime.add_artifact("prediction_evaluation", prediction_paths["markdown"])
    runtime.add_artifact("backtest_report", backtest_paths["markdown"])
    runtime.finish(status="completed")
    return {
        "session_dir": str(selected_session),
        "intraday_review": {key: str(value) for key, value in review_paths.items()},
        "prediction_evaluation": {key: str(value) for key, value in prediction_paths.items()},
        "backtest_report": {key: str(value) for key, value in backtest_paths.items()},
        "headline": review["headline"],
    }


def replay_daily(*, config: dict[str, Any], as_of: str) -> dict[str, Any]:
    project = config["project"]
    state_root = Path(project["state_dir"]) / as_of
    holdings_payload = load_json(state_root / "holdings_snapshot.json")
    series_payload = load_json(state_root / "series_snapshots.json")
    if not holdings_payload or not series_payload:
        raise FileNotFoundError(f"Missing state snapshot for {as_of}")
    universe = load_universe_from_state(
        state_root=state_root,
        fallback=load_universe(project["universe_file"]),
    )
    holdings = holdings_from_payload(holdings_payload)
    series_map = {code: series_from_payload(item) for code, item in (series_payload or {}).items()}
    news_items = [NewsItem(**item) for item in (load_json(state_root / "news.json", default=[]) or [])]
    announcements = [AnnouncementItem(**item) for item in (load_json(state_root / "announcements.json", default=[]) or [])]
    llm_summary = load_json(state_root / "agent_output.json", default={}) or {}
    bundle, feature_map = build_decision_bundle(
        as_of=as_of,
        holdings=holdings,
        universe=universe,
        series_map=series_map,
        news_items=news_items,
        announcements=announcements,
        llm_summary=llm_summary,
        config=config,
        supplemental={key: load_json(state_root / f"{key}.json", default={}) or {} for key in ("fundamentals", "valuation", "capital_flow", "company_info")},
        sector_map=load_json(state_root / "sector_map.json", default={}) or {},
        sector_metrics=load_json(state_root / "sector_metrics.json", default={}) or {},
    )
    bundle.llm_summary = llm_summary
    dynamic_universe = load_json(state_root / "dynamic_universe.json", default={}) or {}
    if dynamic_universe:
        bundle.homepage_overview["dynamic_universe"] = {
            "selection_mode": dynamic_universe.get("selection_mode"),
            "top_sectors": list(dynamic_universe.get("top_sectors") or []),
            "leaders": list(dynamic_universe.get("leaders") or []),
        }
    saved_homepage = load_json(state_root / "homepage_overview.json", default={}) or {}
    if saved_homepage:
        bundle.homepage_overview = {**bundle.homepage_overview, **saved_homepage}
    discovery_style = infer_market_strategy_style(
        action=bundle.market_view.action,
        regime=str(bundle.market_view.metadata.get("regime") or ""),
    )
    best_stock_payload = _build_best_stock_payload(
        config,
        as_of=as_of,
        strategy_style=discovery_style,
    )
    bundle.homepage_overview["best_stock"] = dict(best_stock_payload.get("best_stock") or {})
    report_root = ensure_dir(Path(project["report_dir"]) / as_of)
    artifacts = write_daily_outputs(
        output_dir=report_root,
        bundle=bundle,
        holdings=holdings,
        feature_map=feature_map,
        news_items=news_items,
        announcements=announcements,
    )
    best_stock_report_artifacts = _write_best_stock_report_artifacts(config=config, as_of=as_of, payload=best_stock_payload)
    write_json(state_root / "features.json", {code: feature.to_dict() for code, feature in feature_map.items()})
    write_json(state_root / "decision_bundle.json", bundle.to_dict())
    write_json(state_root / "homepage_overview.json", bundle.homepage_overview)
    homepage_assets = write_homepage_assets(
        overview=bundle.homepage_overview,
        homepage_dir=project["homepage_dir"],
        as_of=as_of,
    )
    shutil.copy2(homepage_assets["markdown"], report_root / homepage_assets["markdown"].name)
    shutil.copy2(homepage_assets["json"], report_root / homepage_assets["json"].name)
    return {
        "decision_bundle": bundle.to_dict(),
        "artifacts": {key: str(value) for key, value in {**artifacts, **homepage_assets, **best_stock_report_artifacts}.items()},
    }


def run_trading_day(
    *,
    config: dict[str, Any],
    as_of: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
    realtime_source: str = "live",
    replay_path: str | None = None,
    realtime_iterations: int | None = None,
    interval_seconds: float | None = None,
    sleep_enabled: bool = False,
    include_realtime: bool = True,
    include_postclose: bool = True,
) -> dict[str, Any]:
    premarket = run_premarket(config=config, as_of=as_of, holdings_file=holdings_file, cash=cash)
    intraday = None
    postclose = None
    if include_realtime:
        intraday = run_intraday(
            config=config,
            as_of=as_of,
            source_kind=realtime_source,
            replay_path=replay_path,
            iterations=realtime_iterations,
            interval_seconds=interval_seconds,
            sleep_enabled=sleep_enabled,
        )
    if include_postclose and intraday is not None:
        postclose = run_postclose(config=config, as_of=as_of, session_dir=intraday["session_dir"])
    return {"premarket": premarket, "intraday": intraday, "postclose": postclose}


def latest_session_dir(realtime_dir: str | Path, *, as_of: str) -> Path | None:
    root = Path(realtime_dir) / as_of
    if not root.exists():
        return None
    sessions = sorted((path for path in root.iterdir() if path.is_dir()))
    return sessions[-1] if sessions else None


def load_universe_from_state(*, state_root: Path, fallback: list[UniverseItem]) -> list[UniverseItem]:
    payload = load_json(state_root / "universe_effective.json", default=[]) or []
    if not payload:
        return fallback
    rows: list[UniverseItem] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip()
        if not code:
            continue
        rows.append(UniverseItem(code=code, name=str(item.get("name") or code), category=str(item.get("category") or "watch")))
    return rows or fallback


def holdings_from_payload(payload: dict[str, Any]) -> HoldingsSnapshot:
    return HoldingsSnapshot(
        as_of=str(payload.get("as_of") or ""),
        source_file=str(payload.get("source_file") or ""),
        positions=[HoldingPosition(**item) for item in (payload.get("positions") or [])],
        total_market_value=float(payload.get("total_market_value") or 0.0),
        total_equity=float(payload.get("total_equity") or 0.0),
        exposure_ratio=float(payload.get("exposure_ratio") or 0.0),
        alerts=[str(item) for item in (payload.get("alerts") or [])],
        sector_weights=list(payload.get("sector_weights") or []),
    )


def structured_decision_from_payload(payload: dict[str, Any]) -> StructuredDecision:
    return StructuredDecision(
        object_type=str(payload.get("object_type") or ""),
        object_id=str(payload.get("object_id") or ""),
        object_name=str(payload.get("object_name") or payload.get("object_id") or ""),
        at=str(payload.get("at") or ""),
        action=str(payload.get("action") or ""),
        score=float(payload.get("score") or 0.0),
        probability=float(payload.get("probability")) if payload.get("probability") is not None else None,
        reason=[str(item) for item in (payload.get("reason") or [])],
        risk=[str(item) for item in (payload.get("risk") or [])],
        sources=[str(item) for item in (payload.get("sources") or [])],
        thesis=str(payload.get("thesis") or ""),
        counterpoints=[str(item) for item in (payload.get("counterpoints") or [])],
        trigger_conditions=[str(item) for item in (payload.get("trigger_conditions") or [])],
        invalidation_conditions=[str(item) for item in (payload.get("invalidation_conditions") or [])],
        priority_score=float(payload.get("priority_score") or 0.0),
        metadata=dict(payload.get("metadata") or {}),
    )


def bundle_from_payload(payload: dict[str, Any]) -> DailyDecisionBundle:
    return DailyDecisionBundle(
        as_of=str(payload.get("as_of") or ""),
        market_view=structured_decision_from_payload(payload.get("market_view") or {}),
        holdings_actions=[structured_decision_from_payload(item) for item in (payload.get("holdings_actions") or [])],
        watchlist=[structured_decision_from_payload(item) for item in (payload.get("watchlist") or [])],
        monitor_plan=[structured_decision_from_payload(item) for item in (payload.get("monitor_plan") or [])],
        final_action_summary=str(payload.get("final_action_summary") or ""),
        homepage_overview=dict(payload.get("homepage_overview") or {}),
        llm_summary=dict(payload.get("llm_summary") or {}),
    )


def series_from_payload(payload: dict[str, Any]) -> DailySeriesSnapshot:
    return DailySeriesSnapshot(
        code=str(payload.get("code") or ""),
        name=str(payload.get("name") or payload.get("code") or ""),
        secid=str(payload.get("secid") or ""),
        fetched_at=str(payload.get("fetched_at") or ""),
        source=str(payload.get("source") or "state"),
        bars=[DailyBar(**row) for row in (payload.get("bars") or [])],
        used_cache=bool(payload.get("used_cache", False)),
        degraded=bool(payload.get("degraded", False)),
    )


def _refresh_intraday_reports(
    *,
    state_root: Path,
    report_root: Path,
    as_of: str,
    homepage_overview: dict[str, Any],
) -> None:
    decision_bundle_payload = load_json(state_root / "decision_bundle.json", default={}) or {}
    holdings_payload = load_json(state_root / "holdings_snapshot.json", default={}) or {}
    feature_payload = load_json(state_root / "features.json", default={}) or {}
    if not decision_bundle_payload or not holdings_payload:
        return

    bundle = bundle_from_payload(decision_bundle_payload)
    bundle.homepage_overview = homepage_overview
    holdings = holdings_from_payload(holdings_payload)
    feature_map = {
        str(code): InstrumentFeatures(**item)
        for code, item in feature_payload.items()
        if isinstance(item, dict) and str(code)
    }
    news_items = [NewsItem(**item) for item in (load_json(state_root / "news.json", default=[]) or [])]
    announcements = [AnnouncementItem(**item) for item in (load_json(state_root / "announcements.json", default=[]) or [])]

    write_json(state_root / "homepage_overview.json", homepage_overview)
    decision_bundle_payload["homepage_overview"] = homepage_overview
    write_json(state_root / "decision_bundle.json", decision_bundle_payload)
    write_daily_outputs(
        output_dir=report_root,
        bundle=bundle,
        holdings=holdings,
        feature_map=feature_map,
        news_items=news_items,
        announcements=announcements,
    )


def _build_best_stock_payload(
    config: dict[str, Any],
    *,
    as_of: str,
    strategy_style: str,
    holdings_file: str | None = None,
    cash: float = 0.0,
) -> dict[str, Any]:
    payload = build_tomorrow_best_pick(
        config,
        as_of=as_of,
        strategy_style=strategy_style,
        holdings_file=holdings_file,
        cash=cash,
    )
    best_stock = dict(payload.get("best_stock") or payload.get("pick") or {})
    if best_stock:
        best_stock["backtest"] = dict(payload.get("backtest") or best_stock.get("backtest") or {})
        best_stock["strategy_style"] = str(best_stock.get("strategy_style") or payload.get("strategy_style") or strategy_style)
        best_stock["selection_score"] = float(best_stock.get("selection_score") or 0.0)
    payload["best_stock"] = best_stock
    return payload


def _write_best_stock_report_artifacts(*, config: dict[str, Any], as_of: str, payload: dict[str, Any]) -> dict[str, Path]:
    report_artifacts = build_best_stock_report(config, as_of=as_of, payload=payload)
    return {
        "best_stock_report_markdown": Path(report_artifacts["markdown"]),
        "best_stock_report_json": Path(report_artifacts["json"]),
    }

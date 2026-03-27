from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path

from .codex_delegate import delegate_codex
from .utils import today_stamp


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Harness-first A-share research, monitoring, and replay system.")
    parser.add_argument("--config", default="config/default.toml", help="Path to TOML config.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_all = subparsers.add_parser("run-trading-day", help="One entry to run premarket, realtime, and postclose pipelines.")
    add_common_daily_args(run_all)
    run_all.add_argument("--skip-realtime", action="store_true", help="Do not run realtime stage.")
    run_all.add_argument("--skip-postclose", action="store_true", help="Do not run postclose stage.")

    premarket = subparsers.add_parser("run-premarket", help="Generate daily report, action summary, monitor plan, and homepage.")
    add_common_daily_args(premarket)

    realtime = subparsers.add_parser("run-realtime", help="Run realtime monitoring using today's monitor plan.")
    realtime.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    realtime.add_argument("--source", default="live", choices=["live", "replay", "synthetic"], help="Realtime source.")
    realtime.add_argument("--replay-path", default=None, help="Replay JSONL or CSV path.")
    realtime.add_argument("--iterations", type=int, default=None, help="Polling iterations.")
    realtime.add_argument("--interval-seconds", type=float, default=None, help="Polling interval seconds.")
    realtime.add_argument("--sleep", action="store_true", help="Sleep between live polling iterations.")

    postclose = subparsers.add_parser("run-postclose", help="Evaluate today's alerts and prediction history.")
    postclose.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    postclose.add_argument("--session-dir", default=None, help="Optional realtime session directory.")

    replay = subparsers.add_parser("replay-daily", help="Replay homepage and decision bundle from saved state.")
    replay.add_argument("--as-of", required=True, help="Trading date to replay.")

    evaluate = subparsers.add_parser("evaluate-predictions", help="Evaluate saved historical daily predictions.")
    evaluate.add_argument("--as-of", default=None, help="Output date tag. Default is today.")

    backtest = subparsers.add_parser("backtest", help="Run watchlist backtest from cached daily bars.")
    backtest.add_argument("--as-of", default=None, help="Output date tag. Default is today.")
    backtest.add_argument("--begin", default=None, help="Optional begin date.")
    backtest.add_argument("--end", default=None, help="Optional end date.")

    fixed_pool_eval = subparsers.add_parser("evaluate-fixed-pool", help="Evaluate a fixed 20-stock pool with daily top-N next-day returns.")
    fixed_pool_eval.add_argument("--as-of", default=None, help="Output date tag. Default is today.")
    fixed_pool_eval.add_argument("--strategy-style", default="general", help="Selection style passed to discovery.")
    fixed_pool_eval.add_argument("--pool-size", type=int, default=20, help="Fixed stock pool size. Default is 20.")
    fixed_pool_eval.add_argument("--top-n", type=int, default=5, help="Daily top-N picks to evaluate. Default is 5.")
    fixed_pool_eval.add_argument("--begin", default=None, help="Optional begin date.")
    fixed_pool_eval.add_argument("--end", default=None, help="Optional end date.")
    fixed_pool_eval.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    fixed_pool_eval.add_argument("--cash", type=float, default=0.0)

    serve_dash = subparsers.add_parser("serve-dashboard", help="Serve the homepage dashboard.")
    serve_dash.add_argument("--host", default="127.0.0.1")
    serve_dash.add_argument("--port", type=int, default=8765)

    serve_api_parser = subparsers.add_parser("serve-api", help="Serve the latest homepage API.")
    serve_api_parser.add_argument("--host", default="127.0.0.1")
    serve_api_parser.add_argument("--port", type=int, default=8766)

    ask_stock = subparsers.add_parser("ask-stock", help="Answer whether a stock is buyable using the advice harness.")
    ask_stock.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    ask_stock.add_argument("--question", required=True, help="Natural-language stock question.")
    ask_stock.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    ask_stock.add_argument("--cash", type=float, default=0.0)

    discover = subparsers.add_parser("discover-ideas", help="Discover top candidate ideas from the tracked and cached universe.")
    discover.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    discover.add_argument("--limit", "--top", dest="limit", type=int, default=5, help="Maximum number of ideas to return.")
    discover.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    discover.add_argument("--cash", type=float, default=0.0)

    best_stock = subparsers.add_parser("best-stock-report", help="Generate the single best-stock report for the next session.")
    best_stock.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    best_stock.add_argument("--strategy-style", default="general", help="Selection style passed to best-pick discovery.")
    best_stock.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    best_stock.add_argument("--cash", type=float, default=0.0)

    sector_scan = subparsers.add_parser("scan-sector-leaders", help="Scan realtime sectors and resolve top-leader candidates.")
    sector_scan.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    sector_scan.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    sector_scan.add_argument("--cash", type=float, default=0.0)

    backfill = subparsers.add_parser("backfill-history", help="Generate multiple historical premarket states for replay and evaluation.")
    backfill.add_argument("--dates", required=True, help="Comma-separated dates like 2026-03-17,2026-03-19,2026-03-23")
    backfill.add_argument("--holdings", default=None, help="Optional holdings CSV path.")
    backfill.add_argument("--cash", type=float, default=0.0)

    monitor = subparsers.add_parser("monitor-worker", help="实时监控 Codex Worker 的输出，并自动打开 Codex App")
    monitor.add_argument("--run-dir", help="指定运行目录（默认用最新的）")
    monitor.add_argument("--worker", default="wp1", help="Worker ID（默认 wp1）")
    monitor.add_argument("--list", action="store_true", help="列出所有 Worker 状态")

    delegate = subparsers.add_parser("delegate-codex", help="Use Codex as parallel workers under a persisted run directory.")
    delegate.add_argument("--task", required=True, help="Business task or implementation request.")
    delegate.add_argument("--workers", type=int, default=4, help="Maximum planner packet count. Default is 4.")
    delegate.add_argument("--planner-model", default=None, help="Optional Codex planner model override.")
    delegate.add_argument("--worker-model", default=None, help="Optional Codex worker model override.")
    delegate.add_argument("--stage-timeout-seconds", type=int, default=900, help="Per-stage timeout for each Codex exec run.")
    delegate.add_argument("--dry-run", action="store_true", help="Only write prompts and schemas, do not invoke Codex.")
    return parser


def add_common_daily_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--as-of", default=None, help="Trading date. Default is today.")
    parser.add_argument("--holdings", default=None, help="Holdings CSV path.")
    parser.add_argument("--cash", type=float, default=0.0, help="Cash not shown in holdings CSV.")
    parser.add_argument("--source", default="live", choices=["live", "replay", "synthetic"], help="Realtime source.")
    parser.add_argument("--replay-path", default=None, help="Replay JSONL or CSV path.")
    parser.add_argument("--iterations", type=int, default=None, help="Realtime iterations.")
    parser.add_argument("--interval-seconds", type=float, default=None, help="Realtime polling interval seconds.")
    parser.add_argument("--sleep", action="store_true", help="Sleep between polling iterations.")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    as_of = getattr(args, "as_of", None) or today_stamp()

    if args.command == "delegate-codex":
        project_root = Path(__file__).resolve().parents[2]
        result = delegate_codex(
            task=args.task,
            project_root=project_root,
            workers=args.workers,
            planner_model=args.planner_model,
            worker_model=args.worker_model,
            dry_run=args.dry_run,
            stage_timeout_seconds=args.stage_timeout_seconds,
        )
        print(dump_json(result))
        return

    from .config import load_config, load_universe

    config = load_config(args.config)

    if args.command == "run-trading-day":
        from .runtime_harness.orchestrator import run_trading_day

        result = run_trading_day(
            config=config,
            as_of=as_of,
            holdings_file=args.holdings,
            cash=args.cash,
            realtime_source=args.source,
            replay_path=args.replay_path,
            realtime_iterations=args.iterations,
            interval_seconds=args.interval_seconds,
            sleep_enabled=args.sleep,
            include_realtime=not args.skip_realtime,
            include_postclose=not args.skip_postclose,
        )
        print(dump_json(result))
        return

    if args.command == "run-premarket":
        from .runtime_harness.orchestrator import run_premarket

        result = run_premarket(config=config, as_of=as_of, holdings_file=args.holdings, cash=args.cash)
        print(dump_json(result))
        return

    if args.command == "run-realtime":
        from .runtime_harness.orchestrator import run_intraday

        result = run_intraday(
            config=config,
            as_of=as_of,
            source_kind=args.source,
            replay_path=args.replay_path,
            iterations=args.iterations,
            interval_seconds=args.interval_seconds,
            sleep_enabled=args.sleep,
        )
        print(dump_json(result))
        return

    if args.command == "run-postclose":
        from .runtime_harness.orchestrator import run_postclose

        result = run_postclose(config=config, as_of=as_of, session_dir=args.session_dir)
        print(dump_json(result))
        return

    if args.command == "replay-daily":
        from .runtime_harness.orchestrator import replay_daily

        result = replay_daily(config=config, as_of=args.as_of)
        print(dump_json(result))
        return

    if args.command == "evaluate-predictions":
        from .evaluation_harness.daily import evaluate_prediction_history, write_prediction_evaluation

        project = config["project"]
        result = evaluate_prediction_history(state_dir=project["state_dir"], cache_dir=project["daily_bar_cache_dir"])
        paths = write_prediction_evaluation(result, output_dir=Path(project["evaluation_dir"]) / as_of, as_of=as_of)
        print(dump_json({"result": result, "artifacts": {key: str(value) for key, value in paths.items()}}))
        return

    if args.command == "backtest":
        from .evaluation_harness.backtest import backtest_watchlist_strategy, write_backtest_report

        project = config["project"]
        universe = load_universe(project["universe_file"])
        result = backtest_watchlist_strategy(universe=universe, cache_dir=project["daily_bar_cache_dir"], begin=args.begin, end=args.end)
        paths = write_backtest_report(result, output_dir=Path(project["backtest_dir"]) / as_of, as_of=as_of)
        print(dump_json({"result": result, "artifacts": {key: str(value) for key, value in paths.items()}}))
        return

    if args.command == "evaluate-fixed-pool":
        from .evaluation_harness import evaluate_fixed_pool_topn_strategy, write_fixed_pool_topn_report

        project = config["project"]
        result = evaluate_fixed_pool_topn_strategy(
            config,
            as_of=as_of,
            strategy_style=args.strategy_style,
            holdings_file=args.holdings,
            cash=args.cash,
            pool_size=args.pool_size,
            top_n=args.top_n,
            begin=args.begin,
            end=args.end,
        )
        paths = write_fixed_pool_topn_report(result, output_dir=Path(project["evaluation_dir"]) / as_of, as_of=as_of)
        print(dump_json({"result": result, "artifacts": {key: str(value) for key, value in paths.items()}}))
        return

    if args.command == "serve-dashboard":
        from .runtime_harness.web import serve_dashboard

        serve_dashboard(homepage_dir=config["project"]["homepage_dir"], host=args.host, port=args.port)
        return

    if args.command == "serve-api":
        from .runtime_harness.web import serve_api

        serve_api(config=config, homepage_dir=config["project"]["homepage_dir"], host=args.host, port=args.port)
        return

    if args.command == "ask-stock":
        from .advice_harness import answer_user_query

        result = answer_user_query(
            config=config,
            question=args.question,
            as_of=as_of,
            holdings_file=args.holdings,
            cash=args.cash,
            write_output=True,
        )
        print(dump_json(result))
        return

    if args.command == "discover-ideas":
        from .advice_harness import discover_top_ideas

        result = discover_top_ideas(
            config=config,
            as_of=as_of,
            limit=args.limit,
            holdings_file=args.holdings,
            cash=args.cash,
            write_output=True,
        )
        print(dump_json(result))
        return

    if args.command == "best-stock-report":
        from .skill_harness import build_best_stock_report

        build_tomorrow_best_pick = resolve_best_stock_builder()
        result = build_tomorrow_best_pick(
            config=config,
            as_of=as_of,
            strategy_style=args.strategy_style,
            holdings_file=args.holdings,
            cash=args.cash,
        )
        artifacts = build_best_stock_report(config, as_of=as_of, payload=result)
        print(dump_json({**result, "artifacts": {**dict(result.get("artifacts") or {}), **artifacts}}))
        return

    if args.command == "scan-sector-leaders":
        from .data_harness.holdings import load_holdings_snapshot
        from .data_harness.supplemental import ensure_sector_metrics_payload
        from .skill_harness.sector_rotation import build_dynamic_universe_from_sectors

        project = config["project"]
        holdings = load_holdings_snapshot(args.holdings or project["holdings_file"], as_of=as_of, cash=args.cash)
        base_universe = load_universe(project["universe_file"])
        supplemental_payload = ensure_sector_metrics_payload(config, as_of=as_of)
        result = build_dynamic_universe_from_sectors(
            config=config,
            base_universe=base_universe,
            holdings_codes={position.code for position in holdings.positions},
            sector_metrics=supplemental_payload.get("sector_metrics") or {},
        )
        print(
            dump_json(
                {
                    "as_of": as_of,
                    "selection_mode": result.get("selection_mode"),
                    "top_sectors": result.get("top_sectors"),
                    "leaders": result.get("leaders"),
                    "resolver_failures": result.get("resolver_failures"),
                    "effective_universe_size": len(result.get("universe") or []),
                }
            )
        )
        return

    if args.command == "monitor-worker":
        from .codex_monitor import main as monitor_main
        monitor_main()
        return

    if args.command == "backfill-history":
        from .runtime_harness.orchestrator import run_premarket

        dates = [item.strip() for item in args.dates.split(",") if item.strip()]
        rows = []
        for item in dates:
            rows.append(run_premarket(config=config, as_of=item, holdings_file=args.holdings, cash=args.cash))
        print(dump_json(rows))
        return

    raise SystemExit(f"Unknown command: {args.command}")


def dump_json(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, default=str)


def resolve_best_stock_builder():
    advice_module = importlib.import_module(".advice_harness", package=__package__)
    builder = getattr(advice_module, "build_tomorrow_best_pick", None)
    if callable(builder):
        return builder
    tomorrow_pick_module = importlib.import_module(".advice_harness.tomorrow_pick", package=__package__)
    builder = getattr(tomorrow_pick_module, "build_tomorrow_best_pick", None)
    if callable(builder):
        return builder
    raise SystemExit("best-stock-report unavailable: build_tomorrow_best_pick is not defined.")


if __name__ == "__main__":
    main()

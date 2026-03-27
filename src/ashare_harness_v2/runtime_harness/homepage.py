from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..data_harness.realtime import LiveQuoteSource
from ..decision_harness.display import enrich_display_fields
from ..decision_harness.rendering import render_homepage_overview
from ..models import QuoteSnapshot
from ..utils import average, clamp, ensure_dir, load_jsonl, write_json, write_text


def update_homepage_with_session(
    base_overview: dict[str, Any],
    *,
    session_dir: str | Path | None,
    benchmark_codes: list[str] | None = None,
) -> dict[str, Any]:
    overview = dict(base_overview)
    if session_dir is None:
        if overview.get("current_prices"):
            overview.setdefault("price_mode", "reference_close")
            overview.setdefault("price_section_title", "参考收盘价（非实时）")
            overview.setdefault("price_note", "未运行实时会话；以下为最近可用收盘价，仅供盘前或离线参考。")
        return overview
    quotes = load_jsonl(Path(session_dir) / "quotes.jsonl")
    features = load_jsonl(Path(session_dir) / "features.jsonl")
    alerts = load_jsonl(Path(session_dir) / "alerts.jsonl")
    latest_quotes = latest_quotes_by_code(quotes)
    latest_features = latest_rows_by_code(features)
    benchmark_codes = benchmark_codes or ["000300", "000001", "399006"]
    requested_codes = []
    for item in overview.get("priority_actions", []):
        requested_codes.append((str(item["code"]), str(item["name"])))
    for item in overview.get("holdings_actions", []):
        requested_codes.append((str(item["code"]), str(item["name"])))
    for item in overview.get("watchlist", []):
        requested_codes.append((str(item["code"]), str(item["name"])))
    for code in benchmark_codes:
        if code in latest_quotes:
            requested_codes.append((code, str(latest_quotes[code].get("name") or code)))
    current_prices = []
    seen: set[str] = set()
    for code, name in requested_codes:
        if code in seen:
            continue
        seen.add(code)
        quote = latest_quotes.get(code)
        if quote is None:
            continue
        prev_close = quote.get("prev_close")
        last_price = quote.get("last_price")
        ret_day = None
        if isinstance(last_price, (float, int)) and isinstance(prev_close, (float, int)) and prev_close:
            ret_day = (float(last_price) / float(prev_close)) - 1.0
        current_prices.append(
            {
                "code": code,
                "name": name,
                "last_price": format_price(last_price),
                "ret_day": format_pct(ret_day),
                "timestamp": quote.get("timestamp") or "无",
                "freshness": format_freshness(quote.get("freshness_seconds")),
            }
        )
    sorted_alerts = sorted(alerts, key=lambda item: str(item.get("timestamp") or ""), reverse=True)
    latest_alerts = sorted_alerts[:5]
    realtime_view = build_realtime_priority_view(
        overview=overview,
        latest_quotes=latest_quotes,
        latest_features=latest_features,
        alerts=sorted_alerts,
        benchmark_codes=benchmark_codes,
    )
    overview["market_state"] = build_realtime_market_state(
        overview=overview,
        latest_quotes=latest_quotes,
        latest_features=latest_features,
        alerts=sorted_alerts,
        benchmark_codes=benchmark_codes,
        priority_actions=realtime_view["priority_actions"],
    )
    overview["price_mode"] = "realtime"
    overview["price_section_title"] = "当前最新价格"
    overview["price_note"] = f"数据来自实时会话 {Path(session_dir).name}。"
    overview["current_prices"] = current_prices
    overview["latest_alerts"] = latest_alerts
    overview["priority_actions"] = realtime_view["priority_actions"]
    overview["holdings_risks"] = realtime_view["holdings_risks"]
    overview["watch_opportunities"] = realtime_view["watch_opportunities"]
    overview["session_dir"] = str(session_dir)
    return overview


def update_homepage_with_live_quotes(
    base_overview: dict[str, Any],
    *,
    as_of: str,
    stale_after_seconds: int,
    live_source: LiveQuoteSource | None = None,
    benchmark_codes: list[str] | None = None,
) -> dict[str, Any]:
    overview = dict(base_overview)
    requested_codes = ordered_price_targets(overview, benchmark_codes=benchmark_codes)
    if not requested_codes:
        return overview
    quotes = (live_source or LiveQuoteSource(stale_after_seconds=stale_after_seconds)).fetch([code for code, _ in requested_codes])
    if not quotes:
        overview.setdefault("price_mode", "reference_close")
        overview.setdefault("price_section_title", "参考收盘价（非实时）")
        overview["price_note"] = "实时行情接口当前未返回有效数据；以下沿用最近可用收盘价，仅供盘前或离线参考。"
        return overview

    quote_map = {quote.code: quote for quote in quotes if quote.code}
    quote_rows = {
        quote.code: {
            "code": quote.code,
            "name": quote.name or "",
            "timestamp": quote.timestamp,
            "last_price": quote.last_price,
            "prev_close": quote.prev_close,
            "freshness_seconds": quote.freshness_seconds,
            "trade_date": quote.trade_date,
        }
        for quote in quotes
        if quote.code
    }
    reference_map = {str(item.get("code") or ""): dict(item) for item in overview.get("current_prices") or [] if str(item.get("code") or "")}
    current_prices: list[dict[str, Any]] = []
    live_count = 0
    same_day_live_count = 0
    fallback_count = 0
    for code, name in requested_codes:
        quote = quote_map.get(code)
        if quote is not None:
            current_prices.append(serialize_live_price_row(code=code, name=name, quote=quote))
            live_count += 1
            if quote.trade_date == as_of or str(quote.timestamp).startswith(as_of):
                same_day_live_count += 1
            continue
        fallback = reference_map.get(code)
        if fallback is None:
            continue
        fallback["freshness"] = "收盘参考"
        current_prices.append(fallback)
        fallback_count += 1

    if not current_prices:
        return overview

    if live_count > 0 and fallback_count == 0:
        overview["price_mode"] = "realtime"
        overview["price_section_title"] = "当前最新价格"
    elif live_count > 0:
        overview["price_mode"] = "mixed"
        overview["price_section_title"] = "当前最新价格（含收盘回退）"
    else:
        overview["price_mode"] = "reference_close"
        overview["price_section_title"] = "参考收盘价（非实时）"

    if same_day_live_count > 0:
        overview["price_note"] = (
            f"盘前已拉取实时行情：{live_count} 条实时，{fallback_count} 条沿用最近收盘价；"
            "各标的以各自行情时间戳为准。"
        )
    elif live_count > 0:
        overview["price_note"] = (
            f"已连接实时行情接口，但供应商尚未返回 {as_of} 当日成交；"
            f"当前展示 {live_count} 条供应商最新可得价格，另有 {fallback_count} 条使用收盘参考。"
        )
    else:
        overview["price_note"] = "实时行情接口未返回有效价格；以下沿用最近可用收盘价，仅供盘前或离线参考。"
    overview["current_prices"] = current_prices
    overview["market_state"] = build_realtime_market_state(
        overview=overview,
        latest_quotes=quote_rows,
        latest_features={},
        alerts=[],
        benchmark_codes=benchmark_codes,
        priority_actions=list(overview.get("priority_actions") or []),
    )
    return overview


def write_homepage_assets(overview: dict[str, Any], *, homepage_dir: str | Path, as_of: str) -> dict[str, Path]:
    root = ensure_dir(homepage_dir)
    markdown_path = write_text(root / f"{as_of}_homepage_overview.md", render_homepage_overview(overview))
    json_path = write_json(root / f"{as_of}_homepage_overview.json", overview)
    latest_json_path = write_json(root / "latest_homepage.json", overview)
    html_path = write_text(root / "index.html", render_homepage_html(overview))
    return {
        "markdown": markdown_path,
        "json": json_path,
        "latest_json": latest_json_path,
        "html": html_path,
    }


def render_homepage_html(overview: dict[str, Any]) -> str:
    def rows(items: list[dict[str, Any]], columns: list[str]) -> str:
        if not items:
            return "<p>暂无。</p>"
        header = "".join(f"<th>{column}</th>" for column in columns)
        body = []
        for item in items:
            body.append("<tr>" + "".join(f"<td>{item.get(column, '')}</td>" for column in columns) + "</tr>")
        return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"

    market_state = overview.get("market_state") or {}
    dynamic = overview.get("dynamic_universe") or {}
    price_title = overview.get("price_section_title") or "当前最新价格"
    price_note = str(overview.get("price_note") or "").strip()
    price_note_html = f"<p>{price_note}</p>" if price_note else ""

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="refresh" content="20" />
  <title>A-share Homepage Overview</title>
  <style>
    :root {{
      --bg: #f4efe4;
      --card: #fffaf2;
      --ink: #22201c;
      --muted: #6a6358;
      --line: #d8cdb9;
      --accent: #9e3d1b;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Songti SC", serif;
      color: var(--ink);
      background: radial-gradient(circle at top left, #fff4d6, var(--bg));
    }}
    main {{
      max-width: 1120px;
      margin: 0 auto;
      padding: 24px;
      display: grid;
      gap: 16px;
    }}
    .card {{
      background: rgba(255, 250, 242, 0.95);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 10px 30px rgba(114, 78, 21, 0.08);
    }}
    h1, h2 {{ margin: 0 0 12px 0; }}
    h1 {{ font-size: 32px; }}
    h2 {{ font-size: 20px; color: var(--accent); }}
    p {{ margin: 0; line-height: 1.6; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 8px 6px; border-top: 1px solid var(--line); font-size: 14px; }}
    th {{ color: var(--muted); border-top: 0; }}
  </style>
</head>
<body>
  <main>
    <section class="card">
      <h1>{overview.get('as_of')} 今日总览</h1>
      <p><strong>{market_state.get('regime') or overview.get('market_label') or '暂无'}</strong></p>
      <p>{market_state.get('summary') or overview.get('today_action') or '暂无'}</p>
    </section>
    <section class="card">
      <h2>最该优先处理的 3 件事</h2>
      {rows(list(overview.get('priority_actions', [])), ['headline', 'display_action_label', 'current_status', 'display_trigger', 'display_invalidation'])}
    </section>
    <section class="card">
      <h2>{price_title}</h2>
      {price_note_html}
      {rows(list(overview.get('current_prices', [])), ['name', 'code', 'last_price', 'ret_day', 'timestamp', 'freshness'])}
    </section>
    <section class="card">
      <h2>持仓风险项</h2>
      {rows(list(overview.get('holdings_risks', [])), ['name', 'code', 'display_action_label', 'display_reason', 'display_trigger', 'display_invalidation'])}
    </section>
    <section class="card">
      <h2>观察机会项</h2>
      {rows(list(overview.get('watch_opportunities', [])), ['name', 'code', 'display_action_label', 'current_status', 'display_trigger', 'display_reason'])}
    </section>
    <section class="card">
      <h2>动态优先级说明（板块 -> 龙头）</h2>
      {rows(list(dynamic.get('top_sectors', [])), ['rank', 'sector', 'score', 'pct_change', 'leader', 'leader_pct_change'])}
    </section>
    <section class="card">
      <h2>最新告警</h2>
      {rows(list(overview.get('latest_alerts', [])), ['timestamp', 'name', 'code', 'severity', 'event_type', 'summary'])}
    </section>
    <section class="card">
      <h2>全部持仓动作</h2>
      {rows(list(overview.get('holdings_actions', [])), ['name', 'code', 'display_action_label', 'priority_score', 'display_reason'])}
    </section>
  </main>
</body>
</html>"""


def ordered_price_targets(overview: dict[str, Any], *, benchmark_codes: list[str] | None = None) -> list[tuple[str, str]]:
    benchmark_codes = benchmark_codes or ["000300", "000001", "399006"]
    requested: list[tuple[str, str]] = []
    for item in overview.get("priority_actions", []):
        requested.append((str(item.get("code") or ""), str(item.get("name") or item.get("code") or "")))
    for item in overview.get("holdings_actions", []):
        requested.append((str(item.get("code") or ""), str(item.get("name") or item.get("code") or "")))
    for item in overview.get("watchlist", []):
        requested.append((str(item.get("code") or ""), str(item.get("name") or item.get("code") or "")))
    for item in overview.get("current_prices", []):
        requested.append((str(item.get("code") or ""), str(item.get("name") or item.get("code") or "")))
    for code in benchmark_codes:
        requested.append((code, code))
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()
    for code, name in requested:
        if not code or code in seen:
            continue
        seen.add(code)
        rows.append((code, name))
    return rows


def serialize_live_price_row(*, code: str, name: str, quote: QuoteSnapshot) -> dict[str, Any]:
    ret_day = None
    if quote.prev_close:
        ret_day = (float(quote.last_price) / float(quote.prev_close)) - 1.0
    return {
        "code": code,
        "name": quote.name or name or code,
        "last_price": format_price(quote.last_price),
        "ret_day": format_pct(ret_day),
        "timestamp": quote.timestamp or quote.fetched_at or "无",
        "freshness": format_freshness(quote.freshness_seconds),
        "source": quote.source,
    }


def latest_quotes_by_code(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return latest_rows_by_code(rows)


def latest_rows_by_code(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("code") or "")
        if not code:
            continue
        current = result.get(code)
        if current is None or str(row.get("timestamp") or "") >= str(current.get("timestamp") or ""):
            result[code] = row
    return result


def format_price(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "无"
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def format_pct(value: float | None) -> str:
    return "无" if value is None else f"{value:+.2%}"


def format_freshness(value: object) -> str:
    if not isinstance(value, (int, float)):
        return "无"
    return f"{float(value):.0f}s"


BENCHMARK_SET = {"000300", "510300", "000001", "399006"}
RISK_ACTIONS = {"cut_on_breakdown", "trim_into_strength"}
SUPPORTIVE_HOLDING_ACTIONS = {"add_on_strength", "buy_on_pullback", "hold_no_add", "hold_core_wait_market", "hold_core"}
OPPORTUNITY_ACTIONS = {
    "standard_position",
    "trial_position",
    "wait_for_pullback",
    "wait_for_breakout",
    "watch_market_turn",
    "watch_only",
    "switch_to_better_alternative",
    "stay_out",
}


def build_realtime_priority_view(
    *,
    overview: dict[str, Any],
    latest_quotes: dict[str, dict[str, Any]],
    latest_features: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]],
    benchmark_codes: list[str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    benchmark_codes = benchmark_codes or sorted(BENCHMARK_SET)
    latest_alert_by_code: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        code = str(alert.get("code") or "")
        if not code:
            continue
        if code not in latest_alert_by_code or str(alert.get("timestamp") or "") > str(latest_alert_by_code[code].get("timestamp") or ""):
            latest_alert_by_code[code] = alert

    benchmark_alert = strongest_benchmark_alert(latest_alert_by_code=latest_alert_by_code, benchmark_codes=benchmark_codes)
    rows: list[dict[str, Any]] = []
    holdings_actions = list(overview.get("holdings_actions") or [])
    watchlist = list(overview.get("watchlist") or [])
    for item in holdings_actions:
        action = str(item.get("action") or "")
        if action in RISK_ACTIONS:
            bucket = "holding_risk"
            headline = f"先处理 {item.get('name') or item.get('code')} 的{item.get('action_label') or action}"
        elif action in SUPPORTIVE_HOLDING_ACTIONS:
            bucket = "holding_follow_up"
            headline = f"确认 {item.get('name') or item.get('code')} 是否具备{item.get('action_label') or action}条件"
        else:
            continue
        rows.append(
            session_priority_row(
                item=item,
                bucket=bucket,
                headline=headline,
                latest_alert=latest_alert_by_code.get(str(item.get("code") or "")),
                benchmark_alert=benchmark_alert,
                latest_quote=latest_quotes.get(str(item.get("code") or "")),
                latest_feature=latest_features.get(str(item.get("code") or "")),
            )
        )
    for item in watchlist:
        action = str(item.get("action") or "")
        if action not in OPPORTUNITY_ACTIONS:
            continue
        headline = f"观察 {item.get('name') or item.get('code')} 的{item.get('action_label') or action}触发"
        rows.append(
            session_priority_row(
                item=item,
                bucket="watch_opportunity",
                headline=headline,
                latest_alert=latest_alert_by_code.get(str(item.get("code") or "")),
                benchmark_alert=benchmark_alert,
                latest_quote=latest_quotes.get(str(item.get("code") or "")),
                latest_feature=latest_features.get(str(item.get("code") or "")),
            )
        )

    rows.sort(
        key=lambda row: (
            float(row.get("execution_urgency_rank") or 0.0),
            float(row["priority_score"]),
            float(row.get("score") or 0.0),
        ),
        reverse=True,
    )
    holdings_risks = [row for row in rows if row["bucket"] == "holding_risk"][:3]
    watch_opportunities = [row for row in rows if row["bucket"] == "watch_opportunity"][:4]
    return {
        "priority_actions": rows[:3],
        "holdings_risks": holdings_risks,
        "watch_opportunities": watch_opportunities,
    }


def build_realtime_market_state(
    *,
    overview: dict[str, Any],
    latest_quotes: dict[str, dict[str, Any]],
    latest_features: dict[str, dict[str, Any]],
    alerts: list[dict[str, Any]],
    benchmark_codes: list[str] | None = None,
    priority_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    base_state = dict(overview.get("market_state") or {})
    base_probability = float(base_state.get("baseline_probability") or base_state.get("probability") or overview.get("market_probability") or 0.5)
    benchmark_codes = benchmark_codes or sorted(BENCHMARK_SET)
    benchmark_returns = [
        value
        for code in benchmark_codes
        if code in latest_quotes
        for value in [quote_day_return(latest_quotes[code])]
        if value is not None
    ]
    benchmark_avg = average(benchmark_returns)

    focus_codes: set[str] = set()
    for key in ("priority_actions", "holdings_actions", "watchlist"):
        for item in overview.get(key, []):
            code = str((item or {}).get("code") or "")
            if code and code not in BENCHMARK_SET:
                focus_codes.add(code)
    focus_returns = [
        value
        for code in focus_codes
        if code in latest_quotes
        for value in [quote_day_return(latest_quotes[code])]
        if value is not None
    ]
    focus_avg = average(focus_returns)
    advancing_count = sum(1 for value in focus_returns if value >= 0.01)
    weakening_count = sum(1 for value in focus_returns if value <= -0.01)

    latest_alert_by_code: dict[str, dict[str, Any]] = {}
    for alert in alerts:
        code = str(alert.get("code") or "")
        if not code:
            continue
        current = latest_alert_by_code.get(code)
        if current is None or str(alert.get("timestamp") or "") >= str(current.get("timestamp") or ""):
            latest_alert_by_code[code] = alert
    benchmark_alert = strongest_benchmark_alert(latest_alert_by_code=latest_alert_by_code, benchmark_codes=benchmark_codes)
    breakout_alerts = sum(
        1 for alert in latest_alert_by_code.values() if str(alert.get("event_type") or "") == "relative_breakout"
    )
    breakdown_alerts = sum(
        1
        for alert in latest_alert_by_code.values()
        if str(alert.get("event_type") or "") in {"drawdown_break", "benchmark_drop"}
    )

    shift = 0.0
    change_log: list[str] = []
    if benchmark_avg is not None:
        if benchmark_avg >= 0.015:
            shift += 0.08
            change_log.append(f"指数日内明显修复，均值 {format_pct(benchmark_avg)}。")
        elif benchmark_avg >= 0.006:
            shift += 0.04
            change_log.append(f"指数日内偏强，均值 {format_pct(benchmark_avg)}。")
        elif benchmark_avg <= -0.015:
            shift -= 0.08
            change_log.append(f"指数日内明显转弱，均值 {format_pct(benchmark_avg)}。")
        elif benchmark_avg <= -0.006:
            shift -= 0.04
            change_log.append(f"指数日内偏弱，均值 {format_pct(benchmark_avg)}。")
    if focus_avg is not None:
        if focus_avg >= 0.03 or advancing_count >= max(3, weakening_count + 2):
            shift += 0.04
            change_log.append("重点跟踪标的整体转强。")
        elif focus_avg <= -0.03 or weakening_count >= max(3, advancing_count + 2):
            shift -= 0.04
            change_log.append("重点跟踪标的整体承压。")
    if breakout_alerts:
        shift += min(0.02 * breakout_alerts, 0.06)
        change_log.append(f"盘中出现 {breakout_alerts} 条转强提示。")
    if breakdown_alerts:
        shift -= min(0.025 * breakdown_alerts, 0.08)
        change_log.append(f"盘中出现 {breakdown_alerts} 条风控提示。")
    if benchmark_alert is not None and str(benchmark_alert.get("event_type") or "") == "benchmark_drop":
        shift -= 0.03
        change_log.append("指数告警要求继续把风控放在前面。")

    updated_probability = round(clamp(base_probability + shift, 0.05, 0.95), 4)
    regime = intraday_regime_label(
        base_probability=base_probability,
        updated_probability=updated_probability,
        base_regime=str(base_state.get("baseline_regime") or base_state.get("regime") or overview.get("market_label") or ""),
    )
    summary = build_market_state_summary(
        base_probability=base_probability,
        updated_probability=updated_probability,
        benchmark_avg=benchmark_avg,
        focus_avg=focus_avg,
        change_log=change_log,
    )
    updated = dict(base_state)
    updated["regime"] = regime
    updated["probability"] = updated_probability
    updated["score"] = round(updated_probability * 100, 2)
    updated.setdefault("baseline_regime", base_state.get("baseline_regime") or base_state.get("regime") or overview.get("market_label"))
    updated.setdefault("baseline_score", base_state.get("baseline_score") or round(base_probability * 100, 2))
    updated.setdefault("baseline_probability", base_probability)
    updated["summary"] = summary
    updated["realtime_change_log"] = change_log
    updated["realtime_observation"] = {
        "benchmark_average_return": benchmark_avg,
        "focus_average_return": focus_avg,
        "advancing_count": advancing_count,
        "weakening_count": weakening_count,
        "breakout_alert_count": breakout_alerts,
        "breakdown_alert_count": breakdown_alerts,
        "latest_priority_count": len(priority_actions or []),
    }
    return updated


def session_priority_row(
    *,
    item: dict[str, Any],
    bucket: str,
    headline: str,
    latest_alert: dict[str, Any] | None,
    benchmark_alert: dict[str, Any] | None,
    latest_quote: dict[str, Any] | None,
    latest_feature: dict[str, Any] | None,
) -> dict[str, Any]:
    base_priority = float(item.get("priority_score") or 0.0)
    local_boost, local_note = alert_impact(bucket=bucket, alert=latest_alert)
    market_boost, market_note = benchmark_impact(bucket=bucket, alert=benchmark_alert)
    proximity_boost, proximity_note = trigger_proximity_impact(item=item, latest_quote=latest_quote)
    dynamic_priority = max(0.0, base_priority + local_boost + market_boost + proximity_boost)
    priority_note = "；".join(part for part in (local_note, market_note, proximity_note) if part) or "按基础优先级执行。"
    row = {
        "bucket": bucket,
        "headline": headline,
        "code": str(item.get("code") or ""),
        "name": str(item.get("name") or item.get("code") or ""),
        "action": str(item.get("action") or ""),
        "action_label": str(item.get("action_label") or item.get("action") or ""),
        "priority_score": round(dynamic_priority, 2),
        "score": round(float(item.get("score") or 0.0), 2),
        "reason": str(item.get("reason") or ""),
        "thesis": str(item.get("thesis") or ""),
        "trigger": str(item.get("trigger") or ""),
        "invalidation": str(item.get("invalidation") or ""),
        "position_guidance": str(item.get("position_guidance") or ""),
        "priority_note": priority_note,
        "base_priority_score": round(base_priority, 2),
        "levels": dict(item.get("levels") or {}),
        "positive_factors": list(item.get("positive_factors") or []),
        "negative_factors": list(item.get("negative_factors") or []),
        "counterpoints": list(item.get("counterpoints") or item.get("risk") or []),
        "risk": list(item.get("risk") or []),
        "position_context": dict(item.get("position_context") or {}),
        "preferred_alternative": dict(item.get("preferred_alternative") or {}),
    }
    urgency_rank, urgency_label, urgency_reason = execution_urgency_assessment(
        row=row,
        latest_quote=latest_quote,
        latest_feature=latest_feature,
        latest_alert=latest_alert,
    )
    benchmark_event = str(benchmark_alert.get("event_type") or "") if benchmark_alert else ""
    if benchmark_event == "benchmark_drop" and bucket == "watch_opportunity":
        urgency_rank = max(0.0, urgency_rank - 18.0)
        urgency_reason = f"{urgency_reason} 指数转弱时，机会类动作先降一档。".strip()
    elif benchmark_event == "benchmark_drop" and bucket == "holding_risk":
        urgency_rank = min(100.0, urgency_rank + 6.0)
        urgency_reason = f"{urgency_reason} 指数转弱时，风控项继续前置。".strip()
    row["execution_urgency_rank"] = urgency_rank
    row["execution_urgency_label"] = urgency_label
    row["execution_urgency_reason"] = urgency_reason
    enriched = enrich_display_fields(row)
    status = build_realtime_status(
        row=row,
        latest_quote=latest_quote,
        latest_feature=latest_feature,
        latest_alert=latest_alert,
    )
    enriched["current_status"] = status
    enriched["execution_urgency_rank"] = urgency_rank
    enriched["execution_urgency_label"] = urgency_label
    enriched["execution_urgency_reason"] = urgency_reason
    if status:
        enriched["display_reason"] = f"{enriched.get('display_reason') or enriched.get('reason') or ''} {status}".strip()
    if latest_alert is not None:
        event_type = str(latest_alert.get("event_type") or "")
        if event_type in {"drawdown_break", "benchmark_drop"} and row["bucket"] == "holding_risk":
            enriched["display_trigger"] = "盘中已经出现风险信号，优先按减仓预案执行。"
        elif event_type == "relative_breakout" and row["bucket"] == "watch_opportunity":
            enriched["display_trigger"] = "盘中已出现转强信号，确认量能后可以升级处理。"
    return enriched


def execution_urgency_assessment(
    *,
    row: dict[str, Any],
    latest_quote: dict[str, Any] | None,
    latest_feature: dict[str, Any] | None,
    latest_alert: dict[str, Any] | None,
) -> tuple[float, str, str]:
    action = str(row.get("action") or "")
    levels = row.get("levels") or {}
    support = numeric_level(levels, "support_price")
    breakout = numeric_level(levels, "breakout_price")
    pullback = numeric_level(levels, "pullback_price")
    if support is None and action in RISK_ACTIONS:
        parsed_levels = extract_price_levels(str(row.get("trigger") or ""))
        support = parsed_levels[0] if parsed_levels else None
    if breakout is None and action in OPPORTUNITY_ACTIONS.union({"watch_market_turn"}):
        parsed_levels = extract_price_levels(str(row.get("trigger") or ""))
        breakout = parsed_levels[0] if parsed_levels else None
    last_price = latest_quote.get("last_price") if isinstance(latest_quote, dict) else None
    day_return = quote_day_return(latest_quote)
    event_type = str(latest_alert.get("event_type") or "") if latest_alert else ""
    volume_ratio = latest_feature.get("volume_ratio") if isinstance(latest_feature, dict) else None

    if latest_quote is None or not isinstance(last_price, (int, float)):
        if action in RISK_ACTIONS:
            return 58.0, "盘前先处理", "盘中价格还没回灌，但这是持仓风控动作，应先放在前面。"
        if action == "switch_to_better_alternative":
            return 46.0, "先看替代", "当前这只不是第一选择，先确认更强替代标的。"
        if action == "stay_out":
            return 38.0, "先不参与", "当前没有必要分配注意力和资金。"
        if action in OPPORTUNITY_ACTIONS:
            return 42.0, "盘前候选", "等待盘中价格回灌后，再决定是否升级。"
        return 36.0, "等待更新", "当前缺少实时价格，先按盘前预案管理。"

    if action in RISK_ACTIONS and support:
        if float(last_price) <= support:
            return 100.0, "立即处理", f"现价已经触到 {format_price(support)} 风控位，优先执行减仓。"
        distance = (float(last_price) - support) / support
        if distance <= 0.01:
            return 94.0, "先处理", f"现价离 {format_price(support)} 风控位不足 1%，最需要先盯。"
        if isinstance(day_return, (int, float)) and day_return <= -0.03:
            return 88.0, "优先盯盘", f"日内 {format_pct(day_return)}，虽然没破位，但风险在快速放大。"
        return 76.0, "继续盯盘", f"还没触到 {format_price(support)} 风控位，但这是持仓风险项。"

    if action in OPPORTUNITY_ACTIONS or action == "watch_market_turn":
        if action == "switch_to_better_alternative":
            alternative = row.get("preferred_alternative") or {}
            name = str(alternative.get("name") or "更强标的")
            return 58.0, "先看替代", f"当前这只不是最优资金去处，先确认 {name}。"
        if action == "stay_out":
            return 40.0, "先不参与", "当前还没有值得执行的新仓理由。"
        target = breakout or pullback or support
        if target and float(last_price) >= target and action in {"wait_for_breakout", "standard_position", "trial_position", "watch_market_turn"}:
            if event_type == "relative_breakout" or (isinstance(volume_ratio, (int, float)) and volume_ratio >= 1.2):
                return 90.0, "确认是否执行", f"现价已经站上 {format_price(target)}，只差最后确认是否下单。"
            return 82.0, "重点确认", f"现价已到 {format_price(target)} 触发位，先确认量能，别直接追。"
        if target:
            distance = abs(float(last_price) - target) / target
            if distance <= 0.01:
                return 74.0, "临近触发", f"现价离 {format_price(target)} 触发位不足 1%，很快会有执行分歧。"
            if distance <= 0.03:
                return 62.0, "继续观察", f"现价正在靠近 {format_price(target)}，需要盯盘等确认。"
        return 48.0, "候选观察", "还没到触发区，先保留在候选层。"

    if action in SUPPORTIVE_HOLDING_ACTIONS:
        if breakout and float(last_price) >= breakout:
            return 70.0, "确认加仓条件", f"现价已回到 {format_price(breakout)} 上方，但仍需确认是否值得加。"
        return 52.0, "继续持有观察", "当前更像跟踪项，不是立刻执行项。"

    return 40.0, "继续观察", "当前不属于立刻执行动作。"


def build_realtime_status(
    *,
    row: dict[str, Any],
    latest_quote: dict[str, Any] | None,
    latest_feature: dict[str, Any] | None,
    latest_alert: dict[str, Any] | None,
) -> str:
    if latest_quote is None:
        return "盘中暂无更新，先按盘前预案处理。"
    action = str(row.get("action") or "")
    levels = row.get("levels") or {}
    last_price = latest_quote.get("last_price")
    if not isinstance(last_price, (int, float)):
        return "盘中价格不可用，先按盘前预案处理。"
    support = numeric_level(levels, "support_price")
    breakout = numeric_level(levels, "breakout_price")
    pullback = numeric_level(levels, "pullback_price")
    retest = numeric_level(levels, "retest_price")
    volume_ratio = latest_feature.get("volume_ratio") if isinstance(latest_feature, dict) else None
    last_return = latest_feature.get("return_1step") if isinstance(latest_feature, dict) else None
    day_return = quote_day_return(latest_quote)
    event_type = str(latest_alert.get("event_type") or "") if latest_alert else ""
    volume_note = ""
    if isinstance(volume_ratio, (int, float)):
        if volume_ratio >= 1.5:
            volume_note = f" 量能放大到 {float(volume_ratio):.2f}x。"
        elif volume_ratio <= 0.9:
            volume_note = f" 量能仍偏弱，仅 {float(volume_ratio):.2f}x。"
    day_note = f"日内 {format_pct(day_return)}" if day_return is not None else ""
    if action in RISK_ACTIONS and support:
        if float(last_price) <= support:
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '盘中继续走弱'}，已经跌到 "
                f"{format_price(support)} 防线附近或以下。{volume_note or ' 优先执行减仓预案。'}"
            ).strip()
        distance = (float(last_price) - support) / support
        recovery = retest or pullback or breakout
        if recovery and float(last_price) >= recovery and isinstance(day_return, (int, float)) and day_return >= 0.02:
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note}，已经重新回到 {format_price(recovery)} 上方。"
                " 先把它从立刻减仓降级为重点观察。"
            )
        if distance <= 0.01:
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '盘中波动仍偏弱'}，离 {format_price(support)} 防线很近。"
                f" 先别加仓，盯紧是否失守。{volume_note}"
            ).strip()
        return (
            f"盘中最新价 {format_price(last_price)}，{day_note or '盘中暂未失守'}，还在 {format_price(support)} 防线之上。"
            f" 暂时没到立刻减仓的点。{volume_note}"
        ).strip()
    if action in {"wait_for_breakout", "standard_position", "trial_position", "watch_market_turn"} and breakout:
        if float(last_price) >= breakout:
            if event_type == "relative_breakout" or (isinstance(volume_ratio, (int, float)) and volume_ratio >= 1.2):
                return (
                    f"盘中最新价 {format_price(last_price)}，{day_note or '走势继续转强'}，已经站上 {format_price(breakout)} 关键价，"
                    "且有量价确认，可以升级为重点处理。"
                )
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '走势正在靠近确认'}，已经站上 {format_price(breakout)} 关键价，"
                "但量能确认还不够，别直接追。"
            )
        distance = abs(float(last_price) - breakout) / breakout
        if distance <= 0.01:
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '盘中正在逼近触发位'}，已经贴近 "
                f"{format_price(breakout)}，是否放量决定能不能升级动作。"
            )
        return (
            f"盘中最新价 {format_price(last_price)}，{day_note or '盘中波动有限'}，还没到 {format_price(breakout)} 触发位，先观察。"
        )
    if action in {"wait_for_pullback", "buy_on_pullback"}:
        anchor = pullback or support
        if anchor:
            distance = abs(float(last_price) - anchor) / anchor
            if distance <= 0.015:
                return (
                    f"盘中最新价 {format_price(last_price)}，{day_note or '盘中已经回落'}，已经回到 {format_price(anchor)} 关注区，"
                    "接下来只看能不能止跌企稳。"
                )
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '盘中尚未回到理想位置'}，离更舒服的回踩位 "
                f"{format_price(anchor)} 还有距离，先不急。"
            )
    if action == "switch_to_better_alternative":
        alternative = row.get("preferred_alternative") or {}
        alternative_name = str(alternative.get("name") or "更强标的")
        return f"当前更适合先看 {alternative_name}，这只保留观察，不急着分配新仓。"
    if action == "stay_out":
        return (
            f"盘中最新价 {format_price(last_price)}，{day_note or '当前没有明确优势'}，"
            "依旧不值得为它腾出资金。"
        )
    if action in SUPPORTIVE_HOLDING_ACTIONS:
        if breakout and float(last_price) >= breakout:
            return (
                f"盘中最新价 {format_price(last_price)}，{day_note or '盘中继续修复'}，已经回到强势确认区，"
                "可以继续跟踪是否满足加仓条件。"
            )
        return (
            f"盘中最新价 {format_price(last_price)}，{day_note or '盘中没有明显新变化'}，当前更适合继续观察，不急着动仓位。"
        )
    if isinstance(last_return, (int, float)) and abs(float(last_return)) >= 0.006:
        return f"盘中短周期波动 {float(last_return):+.2%}，需要继续盯盘确认。"
    if isinstance(day_return, (int, float)) and abs(float(day_return)) >= 0.015:
        return f"盘中最新价 {format_price(last_price)}，{day_note}，但还没有改写原计划的确认信号。"
    return f"盘中最新价 {format_price(last_price)}，目前没有足够的新信号，先按原计划执行。"


def numeric_level(levels: dict[str, Any], key: str) -> float | None:
    value = levels.get(key)
    if isinstance(value, (int, float)) and float(value) > 0:
        return float(value)
    return None


def strongest_benchmark_alert(*, latest_alert_by_code: dict[str, dict[str, Any]], benchmark_codes: list[str]) -> dict[str, Any] | None:
    choices = [latest_alert_by_code.get(code) for code in benchmark_codes if latest_alert_by_code.get(code)]
    if not choices:
        return None
    return max(choices, key=lambda row: (severity_weight(str((row or {}).get("severity") or "")), str((row or {}).get("timestamp") or "")))


def alert_impact(*, bucket: str, alert: dict[str, Any] | None) -> tuple[float, str]:
    if not alert:
        return 0.0, ""
    severity = str(alert.get("severity") or "")
    event_type = str(alert.get("event_type") or "")
    weight = severity_weight(severity)
    if bucket == "holding_risk":
        if event_type in {"drawdown_break", "benchmark_drop"}:
            return weight + 8.0, f"盘中 `{event_type}`({severity}) 放大风险紧急度。"
        if event_type == "relative_breakout":
            return weight + 2.0, f"盘中 `{event_type}`({severity})，先按风险预案管理波动。"
    if bucket == "watch_opportunity":
        if event_type == "relative_breakout":
            return weight + 10.0, f"盘中 `{event_type}`({severity})，触发接近度提升。"
        if event_type == "drawdown_break":
            return -max(weight - 2.0, 0.0), f"盘中 `{event_type}`({severity})，机会优先级下调。"
    return weight * 0.4, f"盘中 `{event_type}`({severity})，优先级小幅调整。"


def benchmark_impact(*, bucket: str, alert: dict[str, Any] | None) -> tuple[float, str]:
    if not alert:
        return 0.0, ""
    event_type = str(alert.get("event_type") or "")
    severity = str(alert.get("severity") or "")
    weight = severity_weight(severity)
    if event_type != "benchmark_drop":
        return 0.0, ""
    if bucket == "watch_opportunity":
        return -(weight * 0.7), "指数转弱，观察类动作降权。"
    if bucket == "holding_risk":
        return weight * 0.5, "指数转弱，风控动作提权。"
    return weight * 0.2, "指数转弱，维持防守排序。"


def trigger_proximity_impact(*, item: dict[str, Any], latest_quote: dict[str, Any] | None) -> tuple[float, str]:
    if latest_quote is None:
        return 0.0, ""
    last_price = latest_quote.get("last_price")
    if not isinstance(last_price, (int, float)):
        return 0.0, ""
    structured_levels = item.get("levels") or {}
    target = preferred_trigger_level(structured_levels)
    if target is None:
        trigger = str(item.get("trigger") or "")
        levels = extract_price_levels(trigger)
        if not levels:
            return 0.0, ""
        target = levels[0]
    if target <= 0:
        return 0.0, ""
    action = str(item.get("action") or "")
    if action in RISK_ACTIONS and float(last_price) <= target:
        return 12.0, f"现价已经压到风控位({target:.2f})，应优先处理。"
    if action in OPPORTUNITY_ACTIONS and float(last_price) >= target:
        return 12.0, f"现价已经站上触发位({target:.2f})，需要尽快确认是否执行。"
    distance = abs(float(last_price) - target) / target
    if distance <= 0.01:
        return 10.0, f"现价接近触发位({target:.2f})，可执行性高。"
    if distance <= 0.03:
        return 6.0, f"现价靠近触发位({target:.2f})，需重点盯盘。"
    if distance <= 0.06:
        return 2.0, f"现价距触发位({target:.2f})不远，保持跟踪。"
    return 0.0, ""


def preferred_trigger_level(levels: dict[str, Any]) -> float | None:
    for key in ("breakout_price", "support_price", "pullback_price", "retest_price"):
        value = levels.get(key)
        if isinstance(value, (int, float)) and float(value) > 0:
            return float(value)
    return None


def extract_price_levels(text: str) -> list[float]:
    rows: list[float] = []
    for raw in re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)", text):
        try:
            value = float(raw)
        except ValueError:
            continue
        if 0.5 <= value <= 5000:
            rows.append(value)
    return rows


def severity_weight(severity: str) -> float:
    if severity == "high":
        return 22.0
    if severity == "medium":
        return 12.0
    if severity == "low":
        return 6.0
    return 4.0


def quote_day_return(quote: dict[str, Any] | None) -> float | None:
    if not isinstance(quote, dict):
        return None
    last_price = quote.get("last_price")
    prev_close = quote.get("prev_close")
    if not isinstance(last_price, (int, float)) or not isinstance(prev_close, (int, float)) or float(prev_close) == 0:
        return None
    return (float(last_price) / float(prev_close)) - 1.0


def intraday_regime_label(*, base_probability: float, updated_probability: float, base_regime: str) -> str:
    if updated_probability >= 0.58:
        return "盘中偏强"
    if updated_probability <= 0.42:
        return "盘中偏弱"
    if updated_probability >= base_probability + 0.06:
        return "盘中修复中"
    if updated_probability <= base_probability - 0.06:
        return "盘中转弱"
    return base_regime or "盘中震荡"


def build_market_state_summary(
    *,
    base_probability: float,
    updated_probability: float,
    benchmark_avg: float | None,
    focus_avg: float | None,
    change_log: list[str],
) -> str:
    benchmark_note = f"指数均值 {format_pct(benchmark_avg)}" if benchmark_avg is not None else "指数暂无完整盘中回灌"
    focus_note = f"重点标的均值 {format_pct(focus_avg)}" if focus_avg is not None else ""
    detail = " ".join(part for part in (benchmark_note, focus_note) if part).strip()
    if updated_probability >= base_probability + 0.08:
        if base_probability <= 0.45:
            return f"盘前偏防守，但盘中有修复迹象。{detail}。观察名单可以上调为重点盯盘，但持仓防线未解除前不要直接追。"
        return f"盘中强势继续抬升。{detail}。优先跟踪最接近触发位的机会，但仍按触发价执行。"
    if updated_probability <= base_probability - 0.08:
        if base_probability >= 0.55:
            return f"盘前偏多，但盘中转弱。{detail}。先把风控动作提到前面，新开仓继续从严。"
        return f"盘中弱势继续延续。{detail}。先处理减仓和风控，不要急着扩仓。"
    if change_log:
        return f"盘中暂未推翻盘前结论。{detail}。{' '.join(change_log[:2])}"
    return f"盘中暂未推翻盘前结论。{detail}。先按原计划执行。"

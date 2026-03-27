from __future__ import annotations

from ..models import AnnouncementItem, DailyDecisionBundle, HoldingsSnapshot, InstrumentFeatures, NewsItem


def render_daily_report(
    *,
    bundle: DailyDecisionBundle,
    holdings: HoldingsSnapshot,
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
) -> str:
    market_state = bundle.homepage_overview.get("market_state") or {}
    dynamic = bundle.homepage_overview.get("dynamic_universe") or {}
    market_probability = float(market_state.get("probability") or bundle.market_view.probability or 0.0)
    market_score = market_state.get("score") or f"{bundle.market_view.score:.1f}"
    lines = [
        f"# Daily Report - {bundle.as_of}",
        "",
        "## Market State",
        f"- 状态: `{market_state.get('regime') or bundle.market_view.metadata.get('label')}` | 概率 `{market_probability:.0%}` | 分数 `{market_score}`",
        f"- 结论: {market_state.get('summary') or bundle.market_view.thesis}",
        "",
    ]
    lines.extend(render_best_stock_section(bundle.homepage_overview, heading="## 明日最优标的"))
    lines.extend(["", "## Top Priorities"])
    change_log = list(market_state.get("realtime_change_log") or [])
    if change_log:
        lines.insert(5, f"- 盘中变化: {' '.join(change_log[:2])}")
    for item in (bundle.homepage_overview.get("priority_actions") or [])[:3]:
        lines.append(
            f"- {item['headline']} | `{item.get('display_action_label') or item['action_label']}` | "
            f"行动紧急度 `{item.get('execution_urgency_label') or '盘前优先'}` | "
            f"排序原因 `{item.get('execution_urgency_reason') or item.get('priority_note') or '按基础优先级执行'}` | "
            f"{item.get('analysis_summary') or item.get('display_reason') or item.get('reason') or ''} | "
            f"现在看 `{item.get('current_status') or item.get('priority_note') or '按基础优先级执行'}` | "
            f"触发 `{item.get('display_trigger') or item.get('trigger') or '继续观察'}` | "
            f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | "
            f"仓位 `{item.get('analysis_position') or item.get('position_guidance') or '按纪律控制仓位'}`"
        )
    lines.extend(["", "## Portfolio Risk Notes"])
    risks = bundle.homepage_overview.get("holdings_risks") or []
    if risks:
        for item in risks:
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item['reason']} | 触发 `{item.get('display_trigger') or item.get('trigger') or '继续观察'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | 仓位 `{item.get('analysis_position') or item.get('position_guidance') or '按纪律控制仓位'}`"
            )
    else:
        lines.append("- 当前没有高优先级持仓风险。")
    lines.extend(["", "## Watch Opportunities"])
    lines.append("> **本列表已过滤纯动量股**：优先低估值、质量可靠、近期涨幅非主要入选依据。")
    opportunities = bundle.homepage_overview.get("watch_opportunities") or []
    if opportunities:
        for item in opportunities:
            ret_5 = item.get("ret_5d") if isinstance(item, dict) else getattr(item, "ret_5d", None)
            chase_warn = " ⚠️ 近期涨幅较大，观察确认后再介入" if isinstance(ret_5, float) and ret_5 > 0.08 else ""
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item['reason']} | 触发 `{item.get('display_trigger') or item.get('trigger') or '继续等待'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | 仓位 `{item.get('analysis_position') or item.get('position_guidance') or '先观察，不抢跑'}`{chase_warn}"
            )
            # 入选理由：从 positive_factors 组合
            reasons = item.get("positive_factors", []) if isinstance(item, dict) else []
            if reasons:
                reason_text = " + ".join(str(r) for r in reasons[:3])
                lines.append(f"  > 入选理由：{reason_text}。等待量能确认，不追涨。")
    else:
        lines.append("- 当前没有接近触发的观察机会。")
    lines.extend(["", "## Dynamic Sector Rotation"])
    top_sectors = dynamic.get("top_sectors") or []
    if top_sectors:
        for item in top_sectors[:5]:
            lines.append(
                f"- 板块 `{item.get('sector')}` | 排名 `{item.get('rank')}` | 强度 `{item.get('score')}` | "
                f"涨跌 `{format_pct(item.get('pct_change'))}` | 龙头 `{item.get('leader') or '无'}`"
            )
    else:
        lines.append("- 当前未启用动态板块轮动，沿用静态观察池。")
    lines.extend(["", "## Holdings Actions"])
    for item in bundle.holdings_actions:
        lines.extend(render_decision_card(item, heading_level="###"))
    lines.extend(["", "## Watchlist"])
    for item in bundle.watchlist:
        lines.extend(render_decision_card(item, heading_level="###"))
    lines.extend(["", "## News"])
    for item in news_items[:6]:
        lines.append(f"- {item.published_at or '未知'} | {item.source_name} | {item.title}")
    lines.extend(["", "## Announcements"])
    for item in announcements[:6]:
        lines.append(f"- {item.published_at or '未知'} | {item.name}({item.code}) | {item.title}")
    lines.extend(["", "## Holdings Risk"])
    for note in holdings.alerts:
        lines.append(f"- {note}")
    return "\n".join(lines) + "\n"


def render_comprehensive_report(
    *,
    bundle: DailyDecisionBundle,
    holdings: HoldingsSnapshot,
    feature_map: dict[str, InstrumentFeatures],
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
) -> str:
    rendered = render_integrated_report(
        bundle=bundle,
        holdings=holdings,
        feature_map=feature_map,
        news_items=news_items,
        announcements=announcements,
    )
    lines = rendered.splitlines()
    if lines:
        lines[0] = f"# Comprehensive Report - {bundle.as_of}"
    return "\n".join(lines) + "\n"


def render_action_summary(bundle: DailyDecisionBundle) -> str:
    market_state = bundle.homepage_overview.get("market_state") or {}
    market_probability = float(market_state.get("probability") or bundle.market_view.probability or 0.0)
    lines = [
        f"# Action Summary - {bundle.as_of}",
        "",
        f"- 今天怎么做: {build_live_action_summary(bundle)}",
        f"- 市场状态: `{market_state.get('regime') or bundle.market_view.metadata.get('regime') or bundle.market_view.action}` | 概率 `{market_probability:.0%}`",
        "",
        "## Top Priorities",
    ]
    for item in (bundle.homepage_overview.get("priority_actions") or [])[:3]:
        lines.append(
            f"- {item['headline']} | `{item.get('display_action_label') or item['action_label']}` | "
            f"行动紧急度 `{item.get('execution_urgency_label') or '盘前优先'}` | "
            f"{item.get('current_status') or item.get('priority_note') or item.get('display_reason') or item['reason']}"
        )
    lines.extend(["", "## Holdings Risks"])
    for item in (bundle.homepage_overview.get("holdings_risks") or [])[:3]:
        lines.append(
            f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
            f"{item.get('analysis_summary') or item.get('display_reason') or item['reason']}"
        )
    lines.extend(["", "## Watch Opportunities"])
    lines.append("> **本列表已过滤纯动量股**：优先低估值、质量可靠、近期涨幅非主要入选依据。")
    opportunities = bundle.homepage_overview.get("watch_opportunities") or []
    if opportunities:
        for item in opportunities[:4]:
            ret_5 = item.get("ret_5d")
            chase_warn = " ⚠️ 近期涨幅较大，等待确认" if isinstance(ret_5, float) and ret_5 > 0.08 else ""
            positive_factors = item.get("positive_factors", [])
            fallback_reason = item.get("analysis_summary") or item.get("display_reason") or item.get("reason") or ""
            reason_text = " + ".join(str(r) for r in positive_factors[:2]) if positive_factors else fallback_reason
            valuation_tag = "估值合理" if item.get("coverage_score", 0) >= 50 else "需关注估值"
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"估值: {valuation_tag} | 入选主因: {reason_text}{chase_warn} | "
                f"{item.get('analysis_comparison') or item.get('analysis_summary') or item.get('display_trigger') or item.get('trigger') or item.get('display_reason') or item.get('reason') or ''}"
            )
    else:
        lines.append("- 当前没有接近触发的观察机会。")
    return "\n".join(lines) + "\n"


def render_homepage_overview(overview: dict[str, object]) -> str:
    market_state = overview.get("market_state") or {}
    dynamic = overview.get("dynamic_universe") or {}
    change_log = list(market_state.get("realtime_change_log") or [])
    lines = [
        f"# Homepage Overview - {overview['as_of']}",
        "",
        "## 今日市场状态",
        f"- `{market_state.get('regime') or overview.get('market_label') or '暂无'}` | 概率 `{((market_state.get('probability') or 0.0) * 100):.0f}%` | 分数 `{market_state.get('score') or '无'}`",
        f"- {market_state.get('summary') or overview.get('today_action') or '暂无'}",
        "",
    ]
    lines.extend(render_best_stock_section(overview, heading="## 明日最优标的"))
    lines.extend(["", "## 最该优先处理的 3 件事"])
    if change_log:
        lines.insert(5, f"- 盘中变化: {' '.join(change_log[:3])}")
    priorities = overview.get("priority_actions", [])
    if priorities:
        for item in priorities[:3]:
            lines.append(
                f"- {item['headline']} | `{item.get('display_action_label') or item['action_label']}` | "
                f"行动紧急度 `{item.get('execution_urgency_label') or '盘前优先'}` | "
                f"排序原因 `{item.get('execution_urgency_reason') or item.get('priority_note') or '按基础优先级执行'}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item.get('reason') or ''} | "
                f"现在看 `{item.get('current_status') or item.get('priority_note') or '按基础优先级执行'}` | "
                f"触发 `{item.get('display_trigger') or item.get('trigger') or '继续跟踪'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | "
                f"仓位 `{item.get('analysis_position') or item.get('position_guidance') or '按纪律控制仓位'}`"
            )
    else:
        lines.append("- 当前没有明确优先动作。")
    price_title = str(overview.get("price_section_title") or "当前最新价格")
    price_note = str(overview.get("price_note") or "").strip()
    lines.extend(["", f"## {price_title}"])
    if price_note:
        lines.append(f"- {price_note}")
    prices = overview.get("current_prices", [])
    if prices:
        for item in prices:
            lines.append(
                f"- {item['name']}({item['code']}) | 最新价 `{item['last_price']}` | 日内 `{item['ret_day']}` | "
                f"成交 `{item['timestamp']}` | 延迟 `{item['freshness']}`"
            )
    else:
        lines.append("- 当前没有可用价格。")
    lines.extend(["", "## 持仓风险项"])
    risks = overview.get("holdings_risks", [])
    if risks:
        for item in risks:
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item['reason']} | 触发 `{item.get('display_trigger') or item.get('trigger') or '继续观察'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | 仓位 `{item.get('analysis_position') or item.get('position_guidance') or '按纪律控制仓位'}`"
            )
    else:
        lines.append("- 当前没有高优先级持仓风险。")
    lines.extend(["", "## 观察机会项"])
    opportunities = overview.get("watch_opportunities", [])
    if opportunities:
        lines.append("> **本列表已过滤纯动量股**：优先低估值、质量可靠、近期涨幅非主要入选依据。")
        for item in opportunities:
            ret_5 = item.get("ret_5d")
            chase_warn = " ⚠️ 近期涨幅较大，等待确认" if isinstance(ret_5, float) and ret_5 > 0.08 else ""
            positive_factors = item.get("positive_factors", [])
            fallback_reason = item.get("analysis_summary") or item.get("display_reason") or item.get("reason") or ""
            reason_text = " + ".join(str(r) for r in positive_factors[:2]) if positive_factors else fallback_reason
            valuation_tag = "估值合理" if item.get("coverage_score", 0) >= 50 else "需关注估值"
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"估值: {valuation_tag} | 入选主因: {reason_text}{chase_warn} | "
                f"现在看 `{item.get('current_status') or item.get('priority_note') or '继续观察'}` | "
                f"触发 `{item.get('display_trigger') or item.get('trigger') or '继续等待'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | 仓位 `{item.get('analysis_position') or item.get('position_guidance') or '先观察，不抢跑'}`"
            )
    else:
        lines.append("- 当前没有接近触发的观察机会。")
    lines.extend(["", "## 动态优先级说明（板块 -> 龙头）"])
    top_sectors = dynamic.get("top_sectors") or []
    if top_sectors:
        for item in top_sectors[:5]:
            lines.append(
                f"- Top{item.get('rank')} `{item.get('sector')}` | 强度 `{item.get('score')}` | "
                f"涨跌 `{format_pct(item.get('pct_change'))}` | 龙头 `{item.get('leader') or '无'}`"
            )
    else:
        lines.append("- 当前未获取到实时板块轮动数据，候选池回退到静态与缓存模式。")
    lines.extend(["", "## 最新告警"])
    alerts = overview.get("latest_alerts", [])
    if alerts:
        for item in alerts:
            lines.append(f"- {item['timestamp']} | {item['name']}({item['code']}) | `{item['severity']}` | {item['summary']}")
    else:
        lines.append("- 当前没有最新告警。")
    lines.extend(["", "## 全部持仓动作"])
    for item in overview.get("holdings_actions", []):
        lines.append(
            f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item.get('action_label') or item['action']}` | "
            f"{item.get('display_reason') or item['reason']} | 触发 `{item.get('display_trigger') or item.get('trigger') or '继续观察'}` | "
            f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}` | 仓位 `{item.get('position_guidance') or '按纪律控制仓位'}`"
        )

    # ---- Price Predictions Section ----
    predictions = overview.get("predictions", [])
    if predictions:
        lines.extend(["", "## 价格预测（统计模型，仅供参考）"])
        lines.append(
            "> **声明**：以下为纯统计预测，不构成投资建议。预测基于历史分布、均值回归、量价模式，"
            "不依赖 ML 或基本面模型。置信度 ≠ 胜率，请勿单独依据预测下单。"
        )
        for pred in predictions[:5]:
            code = pred.get("code", "?")
            name = pred.get("name", code)
            bundle = pred.get("bundle", {})
            ref = bundle.get("reference_price", 0.0)

            def _fmt_p(p: dict | None) -> str:
                if not p:
                    return "无"
                ret = p.get("predicted_return")
                ret_str = f"{ret:+.2%}" if isinstance(ret, float) else "?"
                price = p.get("predicted_price")
                price_str = f"{price:.3f}" if isinstance(price, float) else "?"
                conf = p.get("confidence")
                conf_str = f"{conf:.0%}" if isinstance(conf, float) else "?"
                band_up = p.get("confidence_band_upper")
                band_lo = p.get("confidence_band_lower")
                band_str = f"[{band_lo:.3f}, {band_up:.3f}]" if band_up and band_lo else ""
                return f"→{price_str} ({ret_str}) 置信{conf_str} {band_str} [{p.get('method', '')}]"

            lines.append(
                f"- **{name}({code})** | 参考价 `{ref:.3f}`"
            )
            intraday_p = bundle.get("intraday")
            dayend_p = bundle.get("dayend")
            nextday_p = bundle.get("nextday")
            longterm_p = bundle.get("longterm")
            if intraday_p or dayend_p:
                lines.append(f"  - 盘中预测(午市/尾市收盘): {_fmt_p(intraday_p or dayend_p)}")
            if nextday_p:
                lines.append(f"  - 次日预测: {_fmt_p(nextday_p)}")
            if longterm_p:
                lines.append(f"  - 10日期望: {_fmt_p(longterm_p)}")
    return "\n".join(lines) + "\n"


def render_integrated_report(
    *,
    bundle: DailyDecisionBundle,
    holdings: HoldingsSnapshot,
    feature_map: dict[str, InstrumentFeatures],
    news_items: list[NewsItem],
    announcements: list[AnnouncementItem],
) -> str:
    market_state = bundle.homepage_overview.get("market_state") or {}
    dynamic = bundle.homepage_overview.get("dynamic_universe") or {}
    priorities = list(bundle.homepage_overview.get("priority_actions") or [])
    holdings_risks = list(bundle.homepage_overview.get("holdings_risks") or [])
    opportunities = list(bundle.homepage_overview.get("watch_opportunities") or [])
    current_prices = list(bundle.homepage_overview.get("current_prices") or [])
    market_probability = float(market_state.get("probability") or bundle.market_view.probability or 0.0)
    market_score = market_state.get("score") or f"{bundle.market_view.score:.1f}"

    lines = [
        f"# Integrated Report - {bundle.as_of}",
        "",
        "## 执行摘要",
        f"- {build_live_action_summary(bundle)}",
        f"- 市场: `{market_state.get('regime') or bundle.market_view.metadata.get('regime') or bundle.market_view.action}` | 概率 `{market_probability:.0%}` | 分数 `{market_score}`",
        f"- 核心结论: {market_state.get('summary') or bundle.market_view.thesis}",
    ]
    change_log = list(market_state.get("realtime_change_log") or [])
    if change_log:
        lines.append(f"- 盘中变化: {' '.join(change_log[:2])}")

    lines.extend(render_best_stock_section(bundle.homepage_overview, heading="## 明日最优标的"))

    lines.extend(["", "## 现在最该处理的 3 件事"])
    if priorities:
        for item in priorities[:3]:
            lines.append(
                f"- {item['headline']} | `{item.get('display_action_label') or item['action_label']}` | "
                f"行动紧急度 `{item.get('execution_urgency_label') or '盘前优先'}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item.get('reason') or ''}"
            )
    else:
        lines.append("- 当前没有明确优先动作。")

    lines.extend(["", "## 持仓风险"])
    if holdings_risks:
        for item in holdings_risks[:3]:
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"{item.get('analysis_summary') or item.get('display_reason') or item['reason']} | "
                f"触发 `{item.get('display_trigger') or item.get('trigger') or '继续观察'}` | "
                f"失效 `{item.get('display_invalidation') or item.get('invalidation') or '待复核'}`"
            )
    else:
        lines.append("- 当前没有高优先级持仓风险。")

    lines.extend(["", "## 观察机会"])
    lines.append("> 优先低估值、质量可靠、近期涨幅非主要入选依据。")
    if opportunities:
        for item in opportunities[:4]:
            ret_5 = item.get("ret_5d")
            chase_warn = " ⚠️ 近期涨幅较大，等待确认" if isinstance(ret_5, float) and ret_5 > 0.08 else ""
            positive_factors = item.get("positive_factors", [])
            fallback_reason = item.get("analysis_summary") or item.get("display_reason") or item.get("reason") or ""
            reason_text = " + ".join(str(r) for r in positive_factors[:2]) if positive_factors else fallback_reason
            lines.append(
                f"- {item['name']}({item['code']}) | `{item.get('display_action_label') or item['action_label']}` | "
                f"入选主因: {reason_text}{chase_warn}"
            )
    else:
        lines.append("- 当前没有接近触发的观察机会。")

    lines.extend(["", "## 详细卡片"])
    for item in bundle.holdings_actions[:2]:
        lines.extend(render_decision_card(item, heading_level="###"))
    for item in bundle.watchlist[:2]:
        lines.extend(render_decision_card(item, heading_level="###"))

    lines.extend(["", "## 市场快照"])
    if dynamic.get("top_sectors"):
        for item in dynamic.get("top_sectors")[:3]:
            lines.append(
                f"- 板块 `{item.get('sector')}` | 排名 `{item.get('rank')}` | 强度 `{item.get('score')}` | "
                f"涨跌 `{format_pct(item.get('pct_change'))}` | 龙头 `{item.get('leader') or '无'}`"
            )
    else:
        lines.append("- 当前未获取到实时板块轮动数据。")
    if current_prices:
        for item in current_prices[:3]:
            lines.append(
                f"- {item['name']}({item['code']}) | 最新价 `{item['last_price']}` | 日内 `{item['ret_day']}` | "
                f"成交 `{item['timestamp']}`"
            )
    if holdings.alerts:
        lines.append(f"- 持仓告警: {holdings.alerts[0]}")

    if feature_map:
        lines.extend(["", "## 特征快照"])
        ordered = sorted(feature_map.values(), key=lambda item: item.trend_score, reverse=True)
        for feature in ordered[:6]:
            lines.append(
                f"- {feature.name}({feature.code}) | `{feature.category}` | trend `{feature.trend_score:.1f}` | "
                f"5d `{format_pct(feature.ret_5d)}` | 20d `{format_pct(feature.ret_20d)}` | rs `{format_pct(feature.relative_strength_20d)}`"
            )

    lines.extend(["", "## 新闻与公告"])
    for item in news_items[:4]:
        lines.append(f"- 新闻 | {item.source_name} | {item.title}")
    for item in announcements[:4]:
        lines.append(f"- 公告 | {item.name}({item.code}) | {item.title}")

    lines.extend(
        [
            "",
            "## 说明",
            "",
            "- 本报告整合了日内摘要、最优标的、持仓风险、观察机会与附录信息，作为主阅读入口。",
            "- 旧版日报、首页与行动摘要仍保留，便于兼容其他入口。",
        ]
    )
    return "\n".join(lines) + "\n"


def format_pct(value: float | None) -> str:
    return "无" if value is None else f"{value:+.2%}"


def render_best_stock_section(payload: dict[str, object], *, heading: str) -> list[str]:
    best_stock = _normalize_best_stock_payload(payload)
    lines = ["", heading]
    if not best_stock:
        lines.append("- 当前没有可展示的最优标的。")
        return lines

    pick = best_stock["pick"]
    backtest = best_stock["backtest"]
    candidates = list(best_stock.get("candidate_rankings") or [])
    code = str(pick.get("code") or "unknown")
    name = str(pick.get("name") or code)
    trade_action = str(pick.get("trade_action") or pick.get("decision") or "待确认")
    strategy_style = str(best_stock.get("strategy_style") or pick.get("strategy_style") or "general")
    selection_score = _coerce_float(pick.get("selection_score"), best_stock.get("selection_score"))
    signal_count = int(_coerce_float(backtest.get("signal_count"), backtest.get("trade_count")))
    skipped_signal_count = int(_coerce_float(backtest.get("skipped_signal_count")))
    is_actionable = bool(pick.get("is_actionable") or (pick.get("metadata") or {}).get("is_actionable"))
    lines.append(
        f"- {name}({code}) | 交易结论 `{trade_action}` | 风格 `{strategy_style}` | 选择分 `{selection_score:.2f}` | 可执行 `{'是' if is_actionable else '否'}`"
    )
    lines.append(
        f"- 摘要: {pick.get('summary') or pick.get('thesis') or pick.get('reason') or '暂无摘要。'}"
    )
    lines.append(
        f"- 回测摘要: 有效交易 `{int(_coerce_float(backtest.get('trade_count'))):d}` / 信号 `{signal_count}` | "
        f"跳过 `{skipped_signal_count}` | "
        f"胜率 `{_coerce_float(backtest.get('win_rate')):.2%}` | "
        f"平均收益 `{_coerce_float(backtest.get('average_return')):+.2%}`"
    )
    if int(_coerce_float(backtest.get("trade_count"))):
        lines.append("- 回测口径: 历史同策略第一名的次日表现，只用于候选排序，不等于当前这只股票未来会复刻。")
    if not is_actionable:
        lines.append("- 执行提示: 当前第一名更像观察名单冠军，不是可以直接下单的买入信号。")
    if candidates:
        top_row = dict(candidates[0])
        lines.append(
            f"- 候选对比: 共 `{len(candidates)}` 支，当前最高综合分 `{_coerce_float(top_row.get('composite_score')):.2f}`。"
        )
    trades = list(backtest.get("trades") or [])
    if trades:
        latest = trades[0]
        lines.append(
            f"- 最近样本: {latest.get('name') or latest.get('code') or '未知'}"
            f"({latest.get('code') or 'unknown'}) | "
            f"`{latest.get('trade_action') or latest.get('decision') or '待确认'}` | "
            f"次日 `{_coerce_float(latest.get('next_day_return')):+.2%}`"
        )
    return lines


def _normalize_best_stock_payload(payload: dict[str, object]) -> dict[str, object]:
    candidate = payload.get("best_stock")
    container = candidate if isinstance(candidate, dict) else payload
    if not isinstance(container, dict):
        return {}
    pick_source = container.get("pick") if isinstance(container.get("pick"), dict) else container.get("best_stock")
    if not isinstance(pick_source, dict):
        pick_source = container if container.get("code") or container.get("name") else {}
    pick = dict(pick_source or {})
    if not pick:
        return {}
    backtest = dict(container.get("backtest") or pick.get("backtest") or {})
    strategy_style = container.get("strategy_style") or pick.get("strategy_style")
    selection_score = container.get("selection_score") or pick.get("selection_score")
    candidate_rankings = _normalize_best_stock_rankings(container, pick=pick, backtest=backtest)
    return {
        "pick": pick,
        "backtest": backtest,
        "strategy_style": strategy_style,
        "selection_score": selection_score,
        "candidate_rankings": candidate_rankings,
    }


def _normalize_best_stock_rankings(
    payload: dict[str, object],
    *,
    pick: dict[str, object],
    backtest: dict[str, object],
) -> list[dict[str, object]]:
    rows = payload.get("candidate_rankings")
    normalized: list[dict[str, object]] = []
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict):
                continue
            row_backtest = dict(item.get("backtest") or {})
            normalized.append(
                {
                    "code": str(item.get("code") or ""),
                    "name": str(item.get("name") or item.get("code") or ""),
                    "selection_score": _coerce_float(item.get("selection_score"), item.get("priority_score")),
                    "trade_count": int(_coerce_float(item.get("trade_count"), row_backtest.get("trade_count"))),
                    "win_rate": _coerce_float(item.get("win_rate"), row_backtest.get("win_rate")),
                    "average_return": _coerce_float(item.get("average_return"), row_backtest.get("average_return")),
                    "composite_score": _coerce_float(item.get("composite_score"), item.get("selection_score")),
                }
            )
    if normalized:
        normalized.sort(key=lambda item: _coerce_float(item.get("composite_score")), reverse=True)
        return normalized
    return [
        {
            "code": str(pick.get("code") or ""),
            "name": str(pick.get("name") or pick.get("code") or ""),
            "selection_score": _coerce_float(pick.get("selection_score"), payload.get("selection_score")),
            "trade_count": int(_coerce_float(backtest.get("trade_count"))),
            "win_rate": _coerce_float(backtest.get("win_rate")),
            "average_return": _coerce_float(backtest.get("average_return")),
            "composite_score": _coerce_float(payload.get("selection_score"), pick.get("selection_score")),
        }
    ]


def _coerce_float(*values: object) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def build_live_action_summary(bundle: DailyDecisionBundle) -> str:
    market_state = bundle.homepage_overview.get("market_state") or {}
    regime = str(market_state.get("regime") or bundle.market_view.metadata.get("regime") or bundle.market_view.action)
    priorities = list(bundle.homepage_overview.get("priority_actions") or [])
    if priorities:
        headlines = "；".join(
            f"{item['headline']}({item.get('execution_urgency_label') or '盘前优先'})"
            for item in priorities[:2]
        )
        return f"{regime}，现在先处理：{headlines}。"
    return bundle.final_action_summary


def render_decision_card(item: object, *, heading_level: str = "###") -> list[str]:
    action_plan = getattr(item, "metadata", {}).get("action_plan") or {}
    action_label = str(action_plan.get("label") or getattr(item, "action", ""))
    action_bucket = str(getattr(item, "metadata", {}).get("canonical_action_label") or "")
    position_guidance = str(action_plan.get("position_guidance") or "")
    counterpoints = list(getattr(item, "counterpoints", []) or [])
    risks = list(getattr(item, "risk", []) or [])
    trigger_conditions = list(getattr(item, "trigger_conditions", []) or [])
    invalidation_conditions = list(getattr(item, "invalidation_conditions", []) or [])
    sources = list(getattr(item, "sources", []) or [])
    positive_factors = list((getattr(item, "metadata", {}) or {}).get("positive_factors") or getattr(item, "reason", [])[1:3])
    preferred_alternative = dict((getattr(item, "metadata", {}) or {}).get("preferred_alternative") or {})
    position_context = dict((getattr(item, "metadata", {}) or {}).get("position_context") or {})
    enriched = {
        "action": getattr(item, "action", ""),
        "action_label": action_label,
        "name": getattr(item, "object_name"),
        "thesis": getattr(item, "thesis") or "",
        "reason": list(getattr(item, "reason", []) or []),
        "positive_factors": positive_factors,
        "counterpoints": counterpoints,
        "risk": risks,
        "position_context": position_context,
        "position_guidance": position_guidance,
        "preferred_alternative": preferred_alternative,
        "levels": dict(action_plan.get("levels") or {}),
        "coverage_score": (getattr(item, "metadata", {}) or {}).get("coverage_score"),
        "factor_analysis": dict((getattr(item, "metadata", {}) or {}).get("factor_analysis") or {}),
        "blockers": list(action_plan.get("blockers") or []),
        "execution_brief": list(action_plan.get("execution_brief") or []),
        "trigger_conditions": trigger_conditions,
        "invalidation_conditions": invalidation_conditions,
    }
    from .display import build_analysis_snapshot, canonical_action_label

    analysis = build_analysis_snapshot(enriched)
    action_bucket = action_bucket or canonical_action_label(str(getattr(item, "action", "")), fallback=action_label)
    return [
        f"{heading_level} {getattr(item, 'object_name')}({getattr(item, 'object_id')})",
        f"- 当前结论: `{action_label}` | 动作族 `{action_bucket}` | 优先级 `{getattr(item, 'priority_score'):.1f}`",
        f"- 核心理由: {analysis['core_reason']}",
        f"- 位置与赔率: {analysis['setup_text']}",
        f"- 证据平衡: {analysis['evidence_balance_text']}",
        f"- 因子画像: {analysis['factor_profile_text']}",
        f"- 因子归因: {analysis['factor_attribution_text']}",
        f"- 证据完整度: {analysis['evidence_quality_text']}",
        f"- 核心阻断因素: {analysis['blocker_text']}",
        f"- 执行前提: {analysis['execution_window_text']}",
        f"- 执行三段式: {analysis['execution_triplet_text']}",
        f"- 加仓判断: {analysis['capital_action_text']}",
        f"- 风险/反证: {analysis['risk_counter_case']}",
        f"- 触发条件: {trigger_conditions[0] if trigger_conditions else '待补充'}",
        f"- 失效条件: {invalidation_conditions[0] if invalidation_conditions else '待补充'}",
        f"- 仓位建议: {analysis['position_text']}",
        *( [f"- 替代标的: {analysis['comparison_text']}"] if analysis.get("comparison_text") else [] ),
        f"- 证据来源: {', '.join(sources[:4]) or 'shared_decision_core'}",
        "",
    ]

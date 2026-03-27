from __future__ import annotations

from typing import Any

from ..utils import clamp
from .schemas import ScoreCard


def compute_sector_score(*, feature: dict[str, Any], peer_score: float, has_sector_mapping: bool) -> tuple[float, bool]:
    if has_sector_mapping:
        return round(clamp(peer_score, 0.0, 100.0), 2), False
    proxy_score = (float(feature.get("trend_score") or 50.0) * 0.6) + (peer_score * 0.4)
    return round(clamp(proxy_score, 0.0, 100.0), 2), True


def compute_stock_score(
    *,
    feature: dict[str, Any],
    fundamentals: dict[str, Any] | None,
    valuation: dict[str, Any] | None,
    capital_flow: dict[str, Any] | None,
    external_analysis: dict[str, Any] | None = None,
) -> tuple[float, list[str], list[str], list[str]]:
    score = float(feature.get("trend_score") or 50.0)
    positives: list[str] = []
    negatives: list[str] = []
    missing: list[str] = []

    ret_20 = feature.get("ret_20d")
    rel_strength = feature.get("relative_strength_20d")
    if isinstance(ret_20, (int, float)) and ret_20 > 0.08:
        score += 6
        positives.append(f"20日收益 {ret_20:+.2%}，趋势延续性较强。")
    elif isinstance(ret_20, (int, float)) and ret_20 < -0.05:
        score -= 8
        negatives.append(f"20日收益 {ret_20:+.2%}，中期趋势仍弱。")
    if isinstance(rel_strength, (int, float)) and rel_strength > 0.05:
        score += 6
        positives.append(f"相对强弱 {rel_strength:+.2%}，强于基准。")
    elif isinstance(rel_strength, (int, float)) and rel_strength < -0.03:
        score -= 6
        negatives.append(f"相对强弱 {rel_strength:+.2%}，弱于基准。")

    if fundamentals:
        rev = fundamentals.get("revenue_growth_yoy")
        profit = fundamentals.get("profit_growth_yoy")
        roe = fundamentals.get("roe")
        ocf = fundamentals.get("operating_cashflow_yoy")
        ocf_margin = fundamentals.get("operating_cashflow_margin")
        leverage = fundamentals.get("debt_to_asset")
        gross_margin = fundamentals.get("gross_margin")
        roic = fundamentals.get("roic")
        if isinstance(rev, (int, float)):
            score += clamp(float(rev), -0.2, 0.3) * 30
            if rev > 0.1:
                positives.append(f"营收同比 {rev:+.2%}。")
            elif rev < -0.05:
                negatives.append(f"营收同比 {rev:+.2%}。")
        else:
            missing.append("缺少营收增速。")
        if isinstance(profit, (int, float)):
            score += clamp(float(profit), -0.3, 0.4) * 35
            if profit > 0.12:
                positives.append(f"利润同比 {profit:+.2%}。")
            elif profit < -0.08:
                negatives.append(f"利润同比 {profit:+.2%}。")
        else:
            missing.append("缺少利润增速。")
        if isinstance(roe, (int, float)):
            score += (clamp(float(roe), 0.0, 0.2) - 0.08) * 60
            if roe >= 0.12:
                positives.append(f"ROE {roe:.1%}。")
            elif roe <= 0.08:
                negatives.append(f"ROE {roe:.1%}。")
        else:
            missing.append("缺少 ROE。")
        if isinstance(ocf, (int, float)):
            score += clamp(float(ocf), -0.3, 0.3) * 20
            if ocf > 0.1:
                positives.append(f"经营现金流同比 {ocf:+.2%}。")
            elif ocf < -0.1:
                negatives.append(f"经营现金流同比 {ocf:+.2%}。")
        elif isinstance(ocf_margin, (int, float)):
            score += (clamp(float(ocf_margin), -0.05, 0.25) - 0.05) * 28
            if ocf_margin > 0.12:
                positives.append(f"经营现金流率 {ocf_margin:.1%}。")
            elif ocf_margin < 0.03:
                negatives.append(f"经营现金流率 {ocf_margin:.1%}。")
        if isinstance(leverage, (int, float)) and leverage > 0.7:
            score -= 5
            negatives.append(f"资产负债率 {leverage:.1%} 偏高。")
        if isinstance(gross_margin, (int, float)) and gross_margin >= 0.2:
            score += 3
            positives.append(f"毛利率 {gross_margin:.1%}。")
        if isinstance(roic, (int, float)) and roic >= 0.1:
            score += 4
            positives.append(f"ROIC {roic:.1%}。")
    else:
        missing.extend(["缺少财务指标", "缺少盈利质量"])

    if valuation:
        pe_pct = valuation.get("pe_percentile")
        pb_pct = valuation.get("pb_percentile")
        pe_ttm = valuation.get("pe_ttm")
        pb = valuation.get("pb")
        pe_vs_industry = valuation.get("pe_vs_industry")
        if isinstance(pe_pct, (int, float)):
            if pe_pct <= 0.35:
                score += 5
                positives.append(f"PE 分位 {pe_pct:.0%}，估值不高。")
            elif pe_pct >= 0.8:
                score -= 7
                negatives.append(f"PE 分位 {pe_pct:.0%}，估值偏高。")
        elif isinstance(pe_vs_industry, (int, float)):
            if pe_vs_industry <= 0.9:
                score += 6
                positives.append(f"PE 约为行业的 {pe_vs_industry:.2f}x。")
            elif pe_vs_industry >= 1.25:
                score -= 6
                negatives.append(f"PE 约为行业的 {pe_vs_industry:.2f}x。")
        elif isinstance(pe_ttm, (int, float)):
            if 0 < pe_ttm <= 18:
                score += 4
                positives.append(f"PE {pe_ttm:.1f}x。")
            elif pe_ttm >= 45:
                score -= 5
                negatives.append(f"PE {pe_ttm:.1f}x。")
        else:
            missing.append("缺少 PE 信息。")
        if isinstance(pb_pct, (int, float)):
            if pb_pct <= 0.35:
                score += 3
            elif pb_pct >= 0.8:
                score -= 4
        elif isinstance(pb, (int, float)):
            if 0 < pb <= 2.0:
                score += 2
            elif pb >= 5.0:
                score -= 3
    else:
        missing.append("缺少估值数据")

    if capital_flow:
        north = capital_flow.get("northbound_net_flow_5d")
        margin = capital_flow.get("margin_balance_change_5d")
        main_flow = capital_flow.get("main_net_flow_5d")
        main_ratio = capital_flow.get("main_net_ratio_5d")
        if isinstance(north, (int, float)):
            score += clamp(float(north), -1.0, 1.0) * 3
            if north > 0:
                positives.append("近5日北向资金净流入。")
            elif north < 0:
                negatives.append("近5日北向资金净流出。")
        if isinstance(margin, (int, float)) and margin < -0.05:
            score -= 3
            negatives.append("融资余额回落较快。")
        if isinstance(main_flow, (int, float)):
            score += clamp(float(main_flow) / 300000000.0, -4.0, 4.0)
            if main_flow > 0:
                positives.append("近5日主力资金净流入。")
            elif main_flow < 0:
                negatives.append("近5日主力资金净流出。")
        if isinstance(main_ratio, (int, float)):
            score += clamp(float(main_ratio), -0.08, 0.08) * 45
            if main_ratio > 0.02:
                positives.append(f"主力净占比 {main_ratio:+.2%}。")
            elif main_ratio < -0.02:
                negatives.append(f"主力净占比 {main_ratio:+.2%}。")
    else:
        missing.append("缺少资金行为数据")

    if external_analysis:
        conviction = external_analysis.get("capital_flow_conviction")
        trend_template = external_analysis.get("trend_template_score")
        rps_proxy = external_analysis.get("rps_proxy_20d")
        longhu_activity = external_analysis.get("longhu_activity_90d")
        if isinstance(conviction, (int, float)):
            score += clamp(float(conviction), -1.0, 1.0) * 6.0
            if conviction >= 0.25:
                positives.append(f"外部资金信号偏强 {float(conviction):+.2f}。")
            elif conviction <= -0.25:
                negatives.append(f"外部资金信号偏弱 {float(conviction):+.2f}。")
        if isinstance(trend_template, (int, float)):
            score += clamp((float(trend_template) - 50.0) / 10.0, -4.0, 4.0)
            if trend_template >= 70.0:
                positives.append(f"趋势模板分 {float(trend_template):.1f}。")
            elif trend_template <= 40.0:
                negatives.append(f"趋势模板分 {float(trend_template):.1f}。")
        if isinstance(rps_proxy, (int, float)):
            score += clamp((float(rps_proxy) - 50.0) / 12.5, -3.0, 3.0)
            if rps_proxy >= 80.0:
                positives.append(f"RPS代理 {float(rps_proxy):.1f}。")
            elif rps_proxy <= 30.0:
                negatives.append(f"RPS代理 {float(rps_proxy):.1f}。")
        if isinstance(longhu_activity, (int, float)) and longhu_activity >= 2:
            positives.append(f"近90日龙虎榜活跃 {int(longhu_activity)} 次。")

    # === 防追涨杀跌：5日涨幅过大的股票降低推荐优先级 ===
    # 在 positives/negatives 已有 roe/revenue/ocf 逻辑，此处额外计算综合 value_factor
    # 用于渲染层标注「估值状态」，不对 score 做二次加减（避免重复计算）
    ret_5_local = feature.get("ret_5d")
    value_factor_local = 0.0
    if valuation:
        pe_pct = valuation.get("pe_percentile")
        if isinstance(pe_pct, (int, float)) and pe_pct <= 0.35:
            value_factor_local = 2.0  # 低估加分，供渲染层使用
        elif isinstance(pe_pct, (int, float)) and pe_pct >= 0.80:
            value_factor_local = -2.0  # 偏高标记

    # 将近期涨幅过大标记到 negatives（视觉提示渲染层）
    if isinstance(ret_5_local, (int, float)) and ret_5_local > 0.08:
        if not any("涨幅过大" in n for n in negatives):
            negatives.append(f"近期5日涨幅 {ret_5_local:+.2%}，需等待确认，勿追高。")

    return round(clamp(score, 0.0, 100.0), 2), positives, negatives, dedupe(missing), value_factor_local


def compute_timing_score(*, feature: dict[str, Any], horizon: str) -> tuple[float, list[str], list[str]]:
    """
    Anti-chasing timing score (v2):
    - Ret_5: heavily capped to prevent chasing recent winners.
      >6% in 5 days = overbought, score PENALISED, not rewarded.
    - Pullback bonus: negative ret_5 near low = potential mean-reversion setup.
    - High-gap: stocks near 20d low get a gentle bounce-odds bonus.
    - Volume: must confirm, not just exist.
    - Overbought/oversold zones use mean-reversion logic, not momentum.
    """
    score = 50.0
    positives: list[str] = []
    negatives: list[str] = []
    high_gap = feature.get("high_gap_20d")
    volume_ratio = feature.get("volume_ratio_5d")
    ret_5 = feature.get("ret_5d")
    volatility = feature.get("volatility_20d")

    if isinstance(ret_5, (int, float)):
        # CAP the momentum contribution — 5-day gains > 6% are a WARNING, not a signal
        capped_ret5 = clamp(float(ret_5), -0.10, 0.06)
        score += capped_ret5 * 50
        if ret_5 > 0.06:
            # Overbought zone — penalise aggressively, signal "don't chase"
            overbought_penalty = (ret_5 - 0.06) * 500
            score -= overbought_penalty
            negatives.append("5日涨幅过大进入超买区，均值回归风险大，勿追。")
        elif ret_5 > 0.03:
            positives.append(f"5日收益 {ret_5:+.2%}，短期节奏偏强但未超买。")
        elif ret_5 < -0.03:
            # Pullback zone — if also near 20d low, this is a BOUNCE candidate
            if isinstance(high_gap, (int, float)) and high_gap <= -0.08:
                score += 8.0
                positives.append("5日回撤且处于20日低位，均值回归概率偏高（赔率优于追涨）。")
            else:
                negatives.append(f"5日收益 {ret_5:+.2%}，短期仍在走弱。")
        else:
            positives.append(f"5日收益 {ret_5:+.2%}，短期节奏中性偏稳。")

    if isinstance(high_gap, (int, float)):
        if high_gap >= -0.03:
            # Already at/near high — high chase risk, reduced score boost
            score += 6
            positives.append(f"距20日高点 {high_gap:+.2%}，接近突破位，但注意追高风险。")
        elif high_gap <= -0.12:
            score -= 10
            negatives.append(f"距20日高点 {high_gap:+.2%}，处于回调中，右侧确认不足。")
        elif high_gap <= -0.05:
            # Mid-range — good contrarian zone if stock quality supports it
            score += 2
            positives.append(f"距20日高点 {high_gap:+.2%}，处于中部偏低区域，赔率尚可。")
        else:
            # -3% to -5%: near high, moderate
            score += 5

    if isinstance(volume_ratio, (int, float)):
        if volume_ratio >= 1.3:
            score += 12
            positives.append(f"量比 {volume_ratio:.2f}x，量能有效放大。")
        elif volume_ratio >= 1.1:
            score += 6
            positives.append(f"量比 {volume_ratio:.2f}x，量能偏暖。")
        elif volume_ratio <= 0.80:
            score -= 10
            negatives.append(f"量比 {volume_ratio:.2f}x，量能严重不足。")
        elif volume_ratio <= 0.90:
            score -= 4
            negatives.append(f"量比 {volume_ratio:.2f}x，量能偏低。")

    if isinstance(volatility, (int, float)) and horizon in {"short_term", "swing"}:
        if volatility > 0.05:
            score -= 8
            negatives.append(f"20日波动率 {volatility:.2%} 偏高，短期风险大。")
        elif volatility > 0.035:
            score -= 3
            negatives.append(f"20日波动率 {volatility:.2%} 略高。")

    return round(clamp(score, 0.0, 100.0), 2), positives, negatives


def compute_risk_penalty(
    *,
    market_score: float,
    announcement_negative_count: int,
    feature: dict[str, Any],
) -> tuple[float, list[str]]:
    penalty = 0.0
    risks: list[str] = []
    if market_score < 35:
        penalty += 12
        risks.append("市场环境偏空，新开仓需要明显折价。")
    elif market_score < 45:
        penalty += 6
        risks.append("市场环境偏谨慎，买入阈值应提高。")
    if announcement_negative_count:
        penalty += min(announcement_negative_count * 4, 12)
        risks.append("近期公告中存在风险关键词。")
    ret_20 = feature.get("ret_20d")
    high_gap = feature.get("high_gap_20d")
    if isinstance(ret_20, (int, float)) and ret_20 < -0.08:
        penalty += 6
        risks.append("20日收益显著为负，趋势修复尚未完成。")
    if isinstance(high_gap, (int, float)) and high_gap < -0.12:
        penalty += 5
        risks.append("距离近期高点过远，右侧确认不足。")
    return round(clamp(penalty, 0.0, 40.0), 2), dedupe(risks)


def compute_missing_data_penalty(*, missing_information: list[str]) -> float:
    return round(min(len(dedupe(missing_information)) * 2.5, 20.0), 2)


def compute_coverage_score(*, evidence_count: int, missing_information: list[str], has_sector_mapping: bool, has_announcements: bool, supplemental_count: int) -> float:
    score = 40.0 + min(evidence_count * 6, 24)
    score += 8 if has_sector_mapping else 0
    score += 8 if has_announcements else 0
    score += min(supplemental_count * 6, 18)
    score -= min(len(dedupe(missing_information)) * 5, 35)
    return round(clamp(score, 5.0, 100.0), 2)


def combine_scores(
    *,
    market_score: float,
    sector_score: float,
    stock_score: float,
    timing_score: float,
    risk_penalty: float,
    missing_data_penalty: float,
    coverage_score: float,
) -> ScoreCard:
    raw_total = (market_score * 0.2) + (sector_score * 0.2) + (stock_score * 0.35) + (timing_score * 0.25)
    raw_total -= risk_penalty
    raw_total -= missing_data_penalty
    raw_total *= 0.7 + (coverage_score / 100.0 * 0.3)
    return ScoreCard(
        market_score=round(market_score, 2),
        sector_score=round(sector_score, 2),
        stock_score=round(stock_score, 2),
        timing_score=round(timing_score, 2),
        risk_penalty=round(risk_penalty, 2),
        missing_data_penalty=round(missing_data_penalty, 2),
        coverage_score=round(coverage_score, 2),
        total_score=round(clamp(raw_total, 0.0, 100.0), 2),
    )


def decide_action(scorecard: ScoreCard) -> tuple[str, float]:
    if scorecard.coverage_score < 45:
        return "insufficient_evidence", round(clamp(scorecard.coverage_score / 100.0, 0.05, 0.6), 4)
    if scorecard.market_score < 40 and scorecard.timing_score < 68:
        return "watch", round(clamp(scorecard.total_score / 140.0, 0.05, 0.7), 4)
    if (
        scorecard.total_score >= 74
        and scorecard.market_score >= 50
        and scorecard.timing_score >= 60
        and scorecard.coverage_score >= 60
    ):
        return "buy", round(clamp((scorecard.total_score + scorecard.coverage_score) / 180.0, 0.05, 0.95), 4)
    if scorecard.total_score >= 56:
        return "watch", round(clamp((scorecard.total_score + scorecard.coverage_score) / 200.0, 0.05, 0.85), 4)
    return "avoid", round(clamp((100.0 - scorecard.total_score) / 180.0, 0.05, 0.9), 4)


def dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        rows.append(text)
    return rows

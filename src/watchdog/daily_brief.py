"""每日投资建议渲染模块

把 vix 信号 + 6 源热点 + truth gate 结果拼成一份人类可读的 markdown 日报。

核心函数：
  render_daily_brief(vix_result, hotspots_result, gated_result, config, as_of) -> str
  run_daily_brief(config, as_of, output_dir) -> dict

用法：
  python3 -m watchdog.daily_brief --as-of 2026-04-21 [--output-dir X]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from watchdog.gate import run_gate_pipeline
from watchdog.vix import run_vix_signal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

_LIGHT_EMOJI: Dict[str, str] = {
    "red": "🔴",
    "yellow": "🟡",
    "green": "🟢",
}

_RSI_READING = {"red": "超买", "yellow": "正常", "green": "超卖"}
_VIX_READING = {"red": "恐慌", "yellow": "中性", "green": "平静"}
_MOM_READING = {"red": "转弱", "yellow": "中性", "green": "强势"}

_STRENGTH_CELL = {
    "strong": "🔥 strong",
    "medium": "➖ medium",
    "weak": "· weak",
}

_DEFAULT_OUTPUT_DIR = (
    "/Users/fqyuki/Documents/kd_2026/自学内容/代码类/ashare-watchdog/"
    "data/output/watchdog"
)


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------


def _now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def _norm_as_of(as_of: Optional[str]) -> str:
    """支持 None / 'YYYY-MM-DD' / 'YYYYMMDD' → 'YYYY-MM-DD'。"""
    if not as_of:
        return datetime.now().strftime("%Y-%m-%d")
    s = str(as_of).strip().replace("-", "")
    try:
        return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.now().strftime("%Y-%m-%d")


def _check(flag: Any) -> str:
    return "✅" if flag else "❌"


def _json_default(o: Any) -> Any:
    if isinstance(o, (pd.Timestamp, datetime)):
        return str(o)
    if hasattr(o, "isoformat"):
        try:
            return o.isoformat()
        except Exception:
            return str(o)
    if hasattr(o, "item"):
        try:
            return o.item()
        except Exception:
            return str(o)
    if isinstance(o, set):
        return sorted(o)
    return str(o)


# ---------------------------------------------------------------------------
# 分段渲染
# ---------------------------------------------------------------------------


def _render_header(as_of: str) -> str:
    return (
        f"# 今日投资建议 {as_of}\n\n"
        f"> 自动生成于 {_now_stamp()},"
        f"由 vix + 6 源热点 + seeking-truth 核查 串联产出\n"
    )


def _render_traffic_light(vix_result: Dict[str, Any]) -> str:
    """市场红绿灯段。vix 失败时给降级版本。"""
    lines: List[str] = ["## 🚦 市场红绿灯", ""]

    if not vix_result or vix_result.get("error"):
        err = (vix_result or {}).get("error", "未知错误")
        lines.append(
            f"> ⚠️ VIX 数据拉取失败：{err},"
            f"今日仅依赖 A 股本地信号"
        )
        lines.append("")
        lines.append("| 信号 | 灯 | 数值 | 判读 |")
        lines.append("|------|----|------|------|")
        lines.append("| RSI(7) | - | - | 无法计算 |")
        lines.append("| VIX | - | - | 拉取失败 |")
        lines.append("| A股动量(5日) | - | - | 无法计算 |")
        lines.append("")
        return "\n".join(lines)

    status = vix_result.get("status") or "-"
    red_count = vix_result.get("red_count", 0)
    tech_pct = vix_result.get("tech_pct", 100)
    advice = vix_result.get("advice") or "-"

    lines.append(
        f"**状态：{status}**（{red_count} 红）"
        f"｜建议科技仓位 **{tech_pct}%**"
    )
    lines.append("")
    lines.append(f"> {advice}")
    lines.append("")

    signals = vix_result.get("signals") or {}
    rsi = signals.get("rsi") or {}
    vix = signals.get("vix") or {}
    mom = signals.get("momentum") or {}

    rsi_light = _LIGHT_EMOJI.get(rsi.get("light", ""), "-")
    vix_light = _LIGHT_EMOJI.get(vix.get("light", ""), "-")
    mom_light = _LIGHT_EMOJI.get(mom.get("light", ""), "-")

    rsi_val = rsi.get("value")
    rsi_cell = f"{rsi_val}" if rsi_val is not None else "-"

    vix_val = vix.get("value")
    vix_ma20 = vix.get("ma20")
    if vix_val is not None and vix_ma20 is not None:
        vix_cell = f"{vix_val}（MA20={vix_ma20}）"
    elif vix_val is not None:
        vix_cell = f"{vix_val}"
    else:
        vix_cell = "-"

    mom_val = mom.get("value")
    mom_cell = str(mom_val) if mom_val is not None else "-"

    rsi_read = _RSI_READING.get(rsi.get("light", ""), "-")
    vix_read = _VIX_READING.get(vix.get("light", ""), "-")
    mom_read = _MOM_READING.get(mom.get("light", ""), "-")

    lines.append("| 信号 | 灯 | 数值 | 判读 |")
    lines.append("|------|----|------|------|")
    lines.append(f"| RSI(7) | {rsi_light} | {rsi_cell} | {rsi_read} |")
    lines.append(f"| VIX | {vix_light} | {vix_cell} | {vix_read} |")
    lines.append(f"| A股动量(5日) | {mom_light} | {mom_cell} | {mom_read} |")
    lines.append("")

    if vix_result.get("override_divergence"):
        lines.append(
            "> ⚠️ 触发中美背离 override：VIX 高但 A 股科技强势，暂不动"
        )
        lines.append("")

    return "\n".join(lines)


def _render_hotspots(hotspots_result: Dict[str, Any]) -> str:
    """6 源热点段。"""
    lines: List[str] = ["## 🔥 今日热点（6 源并行）", ""]

    themes = (hotspots_result or {}).get("themes") or []
    if not themes:
        lines.append("今日无显著热点（6 源聚合后无 theme 命中）。")
        lines.append("")
        return "\n".join(lines)

    n = len(themes)
    strong = sum(1 for t in themes if t.get("strength") == "strong")
    medium = sum(1 for t in themes if t.get("strength") == "medium")
    weak = sum(1 for t in themes if t.get("strength") == "weak")
    lines.append(
        f"共 {n} 条 theme，strong {strong} / medium {medium} / weak {weak}。"
    )
    lines.append("")
    lines.append("| # | Theme | Strength | 命中源 | Tickers |")
    lines.append("|---|-------|----------|--------|---------|")

    for i, t in enumerate(themes, 1):
        name = t.get("theme") or "?"
        strength = t.get("strength") or "weak"
        strength_cell = _STRENGTH_CELL.get(strength, strength)
        srcs = t.get("sources_hit") or []
        srcs_cell = " + ".join(srcs) if srcs else "-"
        tickers = t.get("tickers") or []
        head = [str(x) for x in tickers[:6]]
        tickers_cell = ", ".join(head) if head else "-"
        if len(tickers) > 6:
            tickers_cell += f" …(+{len(tickers) - 6})"
        lines.append(
            f"| {i} | {name} | {strength_cell} | {srcs_cell} | {tickers_cell} |"
        )

    lines.append("")

    # inbox
    inbox_src = ((hotspots_result or {}).get("sources") or {}).get("inbox") or {}
    inbox_items = inbox_src.get("items") or []
    if inbox_items:
        lines.append("### 📥 来自 inbox（财躺平 / 派大星屁屁）")
        for it in inbox_items[:5]:
            fname = it.get("file") or "?"
            tickers = it.get("tickers") or []
            preview = (it.get("preview") or "").strip()
            ticker_s = ", ".join(tickers[:8])
            if preview:
                lines.append(
                    f"- `{fname}` → {ticker_s}：{preview[:80]}"
                )
            else:
                lines.append(f"- `{fname}` → {ticker_s}")
        lines.append("")

    return "\n".join(lines)


def _render_gate(gated: Dict[str, Any]) -> str:
    """真实性核查段。"""
    lines: List[str] = ["## 🔍 真实性核查（seeking-truth gate）", ""]

    thr = (gated or {}).get("thresholds") or {}
    key_thr = thr.get("key_recommend", 3)
    cand_thr = thr.get("candidate", 2)
    stats = (gated or {}).get("stats") or {}
    total = stats.get("total_tickers_evaluated", 0)
    dist = stats.get("score_distribution") or {}

    lines.append(
        f"阈值：≥{key_thr}=重点推荐 / ≥{cand_thr}=候选 / 其他=拒绝"
    )
    lines.append("")
    dist_bits: List[str] = []
    for i in range(6):
        cnt = int(dist.get(i, dist.get(str(i), 0)) or 0)
        dist_bits.append(f"{i} 灯 {cnt}")
    lines.append(
        f"共评估 {total} 只，分布：" + " / ".join(dist_bits) + "。"
    )
    lines.append("")

    # 重点推荐
    key_list = (gated or {}).get("key_recommendations") or []
    lines.append(f"### 🔥 重点推荐（{len(key_list)}）")
    lines.append("")
    if not key_list:
        lines.append("今日无通过真实性核查的重点标的。")
        lines.append("")
    else:
        lines.append(
            "| Ticker | 名称 | 得分 | 公告 | 增减持 | 龙虎榜 "
            "| 业绩预告 | 机构调研 | 所属主题 |"
        )
        lines.append(
            "|--------|------|------|------|--------|--------"
            "|----------|----------|----------|"
        )
        for e in key_list:
            ev = e.get("evidence_summary") or {}
            themes_s = ", ".join((e.get("themes") or [])[:3]) or "-"
            lines.append(
                f"| {e.get('ticker', '-')} | {e.get('name') or '-'} | "
                f"{e.get('score', 0)}/5 | "
                f"{_check(ev.get('announcements'))} | "
                f"{_check(ev.get('shareholder_changes'))} | "
                f"{_check(ev.get('dragon_tiger'))} | "
                f"{_check(ev.get('earnings_forecast'))} | "
                f"{_check(ev.get('institutional_research'))} | "
                f"{themes_s} |"
            )
        lines.append("")

    # 候选池
    cand_list = (gated or {}).get("candidates") or []
    lines.append(f"### ✅ 候选池（{len(cand_list)}）")
    lines.append("")
    if not cand_list:
        lines.append("今日候选池为空。")
        lines.append("")
    else:
        lines.append("| Ticker | 名称 | 得分 | 锚点命中 | 所属主题 |")
        lines.append("|--------|------|------|----------|----------|")
        for e in cand_list:
            ev = e.get("evidence_summary") or {}
            hits = [k for k, v in ev.items() if v]
            hits_s = ", ".join(hits) if hits else "-"
            themes_s = ", ".join((e.get("themes") or [])[:3]) or "-"
            tag = ""
            if e.get("forced"):
                tag += " [持仓]"
            if e.get("skipped"):
                tag += " [skipped]"
            lines.append(
                f"| {e.get('ticker', '-')} | "
                f"{(e.get('name') or '-')}{tag} | "
                f"{e.get('score', 0)}/5 | {hits_s} | {themes_s} |"
            )
        lines.append("")

    # 未通过
    rej_list = (gated or {}).get("rejected") or []
    lines.append(f"### ❌ 未通过（{len(rej_list)}）")
    lines.append("")
    if not rej_list:
        lines.append("（无）")
        lines.append("")
    else:
        lines.append("<details>")
        lines.append("<summary>展开查看未通过列表</summary>")
        lines.append("")
        for e in rej_list[:40]:
            tag = " [skipped]" if e.get("skipped") else ""
            lines.append(
                f"- {e.get('ticker', '-')} "
                f"{e.get('name') or ''} "
                f"{e.get('score', 0)}/5{tag}"
            )
        if len(rej_list) > 40:
            lines.append(f"- …（省略 {len(rej_list) - 40} 只）")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return "\n".join(lines)


def _find_entry_for_ticker(
    gated: Dict[str, Any], ticker: str
) -> Dict[str, Any]:
    """在三个桶里找某 ticker 的评估条目。"""
    tk = str(ticker).zfill(6) if str(ticker).isdigit() else str(ticker)
    for bucket_name, entries in (
        ("key_recommendations", (gated or {}).get("key_recommendations") or []),
        ("candidates", (gated or {}).get("candidates") or []),
        ("rejected", (gated or {}).get("rejected") or []),
    ):
        for e in entries:
            if str(e.get("ticker") or "") == tk:
                return {"bucket": bucket_name, "entry": e}
    return {}


def _bucket_label(bucket_name: str) -> str:
    return {
        "key_recommendations": "🔥 重点推荐",
        "candidates": "✅ 候选池",
        "rejected": "❌ 未通过",
    }.get(bucket_name, bucket_name)


def _render_holdings(
    config: Dict[str, Any],
    vix_result: Dict[str, Any],
    gated: Dict[str, Any],
) -> str:
    """持仓状态段：对 config['universe']['holdings'] 每只单独一段。"""
    lines: List[str] = ["## 📊 持仓状态", ""]

    universe = (config or {}).get("universe") or {}
    holdings: List[str] = universe.get("holdings") or []
    names: Dict[str, str] = universe.get("names") or {}

    if not holdings:
        lines.append(
            "（配置里没有 `universe.holdings`，跳过持仓段。）"
        )
        lines.append("")
        return "\n".join(lines)

    status = (vix_result or {}).get("status") or "-"
    tech_pct = (vix_result or {}).get("tech_pct")
    vix_error = bool((vix_result or {}).get("error"))

    for h in holdings:
        code = str(h).zfill(6) if str(h).isdigit() else str(h)
        name = names.get(code) or names.get(h) or "-"
        found = _find_entry_for_ticker(gated or {}, code)

        lines.append(f"### {code} {name}")
        if found:
            bucket = found["bucket"]
            e = found["entry"]
            score = e.get("score", 0)
            flag_bits: List[str] = []
            if e.get("forced"):
                flag_bits.append("持仓强制")
            if e.get("skipped"):
                flag_bits.append("未评分")
            if e.get("error"):
                flag_bits.append("评分出错")
            flag_s = f"（{', '.join(flag_bits)}）" if flag_bits else ""
            lines.append(
                f"- 在 gate 桶里：{_bucket_label(bucket)}"
                f"（score {score}/5）{flag_s}"
            )
            ev = e.get("evidence_summary") or {}
            ev_bits = [
                f"龙虎榜 {_check(ev.get('dragon_tiger'))}",
                f"业绩预告 {_check(ev.get('earnings_forecast'))}",
                f"公告 {_check(ev.get('announcements'))}",
                f"增减持 {_check(ev.get('shareholder_changes'))}",
                f"机构调研 {_check(ev.get('institutional_research'))}",
            ]
            lines.append("- 证据摘要：" + " / ".join(ev_bits))
        else:
            lines.append("- 在 gate 桶里：未被 gate 评估（不在热点/持仓交集）")

        # 仓位建议
        if vix_error:
            advice = (
                "VIX 数据失败，仅按持仓核查结果评估；"
                "如果核查稳定可继续持有，信号恢复后再调"
            )
        elif status == "逃跑":
            advice = "三灯红警报，建议大幅减仓或转防守"
        elif status == "减仓":
            advice = "科技减至约 50%，持仓若属防守板块可继续持有"
        elif status == "留意":
            advice = "一灯红，暂不动，保持关注"
        elif status == "安全":
            advice = "信号正常，安心持有"
        else:
            advice = "根据 gate 结果决定持有/减仓"
        if tech_pct is not None and not vix_error:
            advice += f"（今日建议科技仓位 {tech_pct}%）"
        lines.append(f"- 今日仓位建议：{advice}")
        lines.append("")

    return "\n".join(lines)


def _render_ops_advice(
    vix_result: Dict[str, Any],
    hotspots_result: Dict[str, Any],
    gated: Dict[str, Any],
) -> str:
    """今日操作建议段。"""
    lines: List[str] = ["## ✅ 今日操作建议", ""]
    lines.append("综合三张表，**今日应该：**")
    lines.append("")

    # 科技仓位
    if vix_result and not vix_result.get("error"):
        tech_pct = vix_result.get("tech_pct", 100)
        lines.append(
            f"- 科技仓位：从 100% 调整到 **{tech_pct}%**（vix 灯）"
        )
    else:
        lines.append(
            "- 科技仓位：VIX 拉取失败，按现状持有并密切关注 A 股本地信号"
        )

    # 加仓候选（前 2-3 只重点推荐）
    key_list = (gated or {}).get("key_recommendations") or []
    if key_list:
        bits: List[str] = []
        for e in key_list[:3]:
            tk = e.get("ticker", "-")
            nm = e.get("name") or "-"
            sc = e.get("score", 0)
            themes_s = "/".join((e.get("themes") or [])[:2]) or "-"
            bits.append(f"{tk} {nm}（{sc}/5，主题：{themes_s}）")
        lines.append("- 加仓候选：" + "； ".join(bits))
    else:
        lines.append("- 加仓候选：今日无重点推荐")

    # 观察（候选池前 2-3）
    cand_list = (gated or {}).get("candidates") or []
    if cand_list:
        bits2: List[str] = []
        for e in cand_list[:3]:
            tk = e.get("ticker", "-")
            nm = e.get("name") or "-"
            sc = e.get("score", 0)
            flag = "[持仓] " if e.get("forced") else ""
            bits2.append(f"{flag}{tk} {nm}（{sc}/5）")
        lines.append("- 观察（不动）：" + "； ".join(bits2))
    else:
        lines.append("- 观察（不动）：候选池为空")

    # 避免：热点里 tickers 多但 gate 未通过的
    themes = (hotspots_result or {}).get("themes") or []
    rej_set = {
        str(e.get("ticker") or "")
        for e in ((gated or {}).get("rejected") or [])
        if e.get("ticker")
    }
    avoid: List[str] = []
    for t in themes:
        if t.get("strength") in ("strong", "medium"):
            for tk in t.get("tickers") or []:
                tk_s = str(tk).zfill(6) if str(tk).isdigit() else str(tk)
                if tk_s in rej_set and tk_s not in avoid:
                    avoid.append(tk_s)
                if len(avoid) >= 3:
                    break
        if len(avoid) >= 3:
            break
    if avoid:
        lines.append(
            "- 避免（theme 热但未过 gate）：" + ", ".join(avoid)
        )
    else:
        lines.append("- 避免：无明显需规避的热点假信号")

    lines.append("")
    return "\n".join(lines)


def _render_appendix(
    as_of: str,
    elapsed: Optional[float],
    output_dir_rel: str = "data/output/watchdog",
) -> str:
    lines: List[str] = ["## 🧾 附录", ""]
    lines.append(f"- vix 原始 JSON：`{output_dir_rel}/{as_of}.vix.json`")
    lines.append(f"- gated 原始 JSON：`{output_dir_rel}/{as_of}.gated.json`")
    if elapsed is not None:
        lines.append(f"- 生成耗时：{elapsed:.1f} 秒")
    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 公开接口
# ---------------------------------------------------------------------------


def render_daily_brief(
    vix_result: Dict[str, Any],
    hotspots_result: Dict[str, Any],
    gated_result: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    as_of: Optional[str] = None,
    elapsed_seconds: Optional[float] = None,
) -> str:
    """返回完整的 markdown 文本（不落盘）。

    参数
    ----
    vix_result:
        run_vix_signal 的返回（可能是 {"error": "..."}）。
    hotspots_result:
        find_hotspots 的返回；run_gate_pipeline 的结果里叫 "hotspots"。
    gated_result:
        apply_truth_gate 的返回。
    config:
        universe.holdings / universe.names 由这里读取，用于持仓段。
    as_of:
        'YYYY-MM-DD'。None 时回退到 hotspots/gated 内部的 as_of 或今天。
    elapsed_seconds:
        整条 pipeline 耗时，附录里展示。
    """
    cfg = config or {}
    as_of_norm = _norm_as_of(
        as_of
        or (hotspots_result or {}).get("as_of")
        or (gated_result or {}).get("as_of")
    )

    parts = [
        _render_header(as_of_norm),
        _render_traffic_light(vix_result or {}),
        _render_hotspots(hotspots_result or {}),
        _render_gate(gated_result or {}),
        _render_holdings(cfg, vix_result or {}, gated_result or {}),
        _render_ops_advice(
            vix_result or {}, hotspots_result or {}, gated_result or {}
        ),
        _render_appendix(as_of_norm, elapsed_seconds),
    ]
    return "\n".join(p.rstrip() + "\n" for p in parts)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def run_daily_brief(
    config: Optional[Dict[str, Any]] = None,
    as_of: Optional[str] = None,
    output_dir: Optional[str] = None,
    max_tickers: Optional[int] = None,
) -> Dict[str, Any]:
    """一键 pipeline：

    1. run_vix_signal
    2. run_gate_pipeline (内部 find_hotspots + apply_truth_gate)
    3. render_daily_brief
    4. 把 markdown 写到 <output_dir>/<as_of>.md，副产 .vix.json / .gated.json
    5. 返回 {"path", "markdown", "vix", "hotspots", "gated", "elapsed_seconds"}

    max_tickers 透传给 run_gate_pipeline，控制 truth 评分次数。
    """
    t0 = time.time()
    cfg = config or {}
    out_dir = Path(output_dir or _DEFAULT_OUTPUT_DIR)
    as_of_preview = _norm_as_of(as_of)

    # 1. vix
    logger.info("run_daily_brief: calling run_vix_signal")
    try:
        vix_result = run_vix_signal(cfg) or {}
    except Exception as exc:
        logger.warning("run_vix_signal crashed: %s", exc)
        vix_result = {"error": f"{type(exc).__name__}: {exc}"}

    # 2. gate pipeline (hotspots + gate)
    logger.info("run_daily_brief: calling run_gate_pipeline")
    gate_kwargs: Dict[str, Any] = {"config": cfg, "as_of": as_of}
    if max_tickers is not None:
        gate_kwargs["max_tickers"] = max_tickers
    try:
        gate_pipe = run_gate_pipeline(**gate_kwargs) or {}
    except Exception as exc:
        logger.warning("run_gate_pipeline crashed: %s", exc)
        gate_pipe = {
            "hotspots": {
                "as_of": as_of_preview,
                "sources": {},
                "themes": [],
            },
            "gated": {
                "as_of": as_of_preview,
                "thresholds": {"candidate": 2, "key_recommend": 3},
                "key_recommendations": [],
                "candidates": [],
                "rejected": [],
                "stats": {
                    "total_tickers_evaluated": 0,
                    "score_distribution": {i: 0 for i in range(6)},
                    "elapsed_seconds": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            },
        }

    hotspots = gate_pipe.get("hotspots") or {}
    gated = gate_pipe.get("gated") or {}

    as_of_norm = _norm_as_of(
        as_of or hotspots.get("as_of") or gated.get("as_of")
    )

    elapsed = time.time() - t0

    # 3. render
    markdown = render_daily_brief(
        vix_result=vix_result,
        hotspots_result=hotspots,
        gated_result=gated,
        config=cfg,
        as_of=as_of_norm,
        elapsed_seconds=elapsed,
    )

    # 4. 写盘（.md + 侧路 JSON）
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / f"{as_of_norm}.md"
    md_path.write_text(markdown, encoding="utf-8")

    try:
        _write_json(out_dir / f"{as_of_norm}.vix.json", vix_result)
    except Exception as exc:
        logger.warning("write vix.json failed: %s", exc)
    try:
        _write_json(out_dir / f"{as_of_norm}.gated.json", gated)
    except Exception as exc:
        logger.warning("write gated.json failed: %s", exc)
    # hotspots 对调试很有用，顺手也落盘
    try:
        _write_json(out_dir / f"{as_of_norm}.hotspots.json", hotspots)
    except Exception as exc:
        logger.warning("write hotspots.json failed: %s", exc)

    logger.info(
        "run_daily_brief done: path=%s elapsed=%.1fs", md_path, elapsed
    )

    return {
        "path": str(md_path),
        "markdown": markdown,
        "vix": vix_result,
        "hotspots": hotspots,
        "gated": gated,
        "elapsed_seconds": elapsed,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _load_config(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="watchdog.daily_brief",
        description=(
            "A 股 watchdog 每日投资建议渲染"
            "（vix + 热点 + gate → markdown）"
        ),
    )
    parser.add_argument("--as-of", default=None, help="YYYY-MM-DD，默认今天")
    parser.add_argument(
        "--output-dir",
        default=None,
        help=f"输出目录，默认 {_DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--config", default=None, help="可选 JSON 配置文件"
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        help="DEBUG/INFO/WARNING/ERROR，默认 WARNING",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = _load_config(args.config)
    result = run_daily_brief(
        config=config, as_of=args.as_of, output_dir=args.output_dir
    )
    print(result["markdown"])


if __name__ == "__main__":
    main()

"""A 股真实性 gate

把 find_hotspots 找到的热点主题里涉及的 ticker 批量过 truth.score_truth 的 5 源核查，
按得分分桶成【重点推荐 / 候选 / 拒绝】。

设计要点：
  - 去重：一个 ticker 可能出现在多个 theme 里，只 score 一次，themes 合并成数组
  - 持仓强制通过：config["universe"]["holdings"] 里的标的即使 score < candidate 也进 candidates（forced=True）
  - 性能控制：max_tickers 限制 score_truth 次数，剩下的进 rejected 标 skipped=True
  - 排序优先级：持仓 > theme strength > sources_hit 数量
  - 容错：score_truth 抛异常 -> 放进 rejected 并记 error
  - 利用 truth._CACHE：同进程内 5 源全市场数据缓存共享

用法：
    python3 -m watchdog.gate --as-of 2026-04-21
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from watchdog.find_hotspots import find_hotspots
from watchdog.truth import score_truth


logger = logging.getLogger(__name__)


# 默认阈值与 truth.py 保持一致
DEFAULT_THRESHOLDS = {"candidate": 2, "key_recommend": 3}

# strength 排序权重（越大越优先）
_STRENGTH_RANK = {"strong": 3, "medium": 2, "weak": 1}


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------


def _collect_ticker_themes(
    themes: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """从 themes 里收集每个 ticker 的元信息：
    {
        ticker: {
            "themes": [theme_name, ...],
            "hotspot_sources": sorted_union_of_sources_hit,
            "best_strength": "strong"|"medium"|"weak",
            "hit_count": sum_of_sources_hit_counts,  # 用来排序
        }
    }
    一个 ticker 可能出现在多个 theme，合并。
    """
    out: Dict[str, Dict[str, Any]] = {}
    for t in themes or []:
        theme_name = t.get("theme") or ""
        strength = t.get("strength") or "weak"
        sources_hit = t.get("sources_hit") or []
        for raw_tic in t.get("tickers") or []:
            # 兜底规范化 6 位
            tic = str(raw_tic).zfill(6) if raw_tic else ""
            if not tic or not tic.isdigit() or len(tic) != 6:
                continue
            entry = out.setdefault(
                tic,
                {
                    "themes": [],
                    "hotspot_sources": set(),
                    "best_strength": "weak",
                    "hit_count": 0,
                },
            )
            if theme_name and theme_name not in entry["themes"]:
                entry["themes"].append(theme_name)
            for s in sources_hit:
                entry["hotspot_sources"].add(s)
            # strength 取最优
            if _STRENGTH_RANK.get(strength, 0) > _STRENGTH_RANK.get(
                entry["best_strength"], 0
            ):
                entry["best_strength"] = strength
            # hit_count 累计（用于排序；一个 ticker 在多 theme 越多越优先）
            entry["hit_count"] += len(sources_hit)
    # 固化 set -> list
    for v in out.values():
        v["hotspot_sources"] = sorted(v["hotspot_sources"])
    return out


def _evidence_summary(evidence: Dict[str, Any]) -> Dict[str, bool]:
    """把 score_truth 返回的 evidence 5 个 source 摘成 hit 的 bool 表。"""
    keys = (
        "announcements",
        "shareholder_changes",
        "dragon_tiger",
        "earnings_forecast",
        "institutional_research",
    )
    return {k: bool((evidence or {}).get(k, {}).get("hit")) for k in keys}


def _sort_tickers_for_scoring(
    ticker_map: Dict[str, Dict[str, Any]],
    holdings_set: set,
) -> List[str]:
    """按 持仓 > theme strength > sources_hit 数量 排序，决定 top-N 先 score 谁。"""

    def key(tic: str):
        meta = ticker_map[tic]
        is_holding = tic in holdings_set
        strength_rank = _STRENGTH_RANK.get(meta["best_strength"], 0)
        hit_count = meta["hit_count"]
        # 升序 sort 时希望持仓在前 -> (not is_holding)=False 排前
        return (not is_holding, -strength_rank, -hit_count)

    return sorted(ticker_map.keys(), key=key)


def _build_bucket_entry(
    ticker: str,
    meta: Dict[str, Any],
    truth_result: Optional[Dict[str, Any]],
    *,
    forced: bool = False,
    skipped: bool = False,
    error: Optional[str] = None,
    include_detail: bool = True,
) -> Dict[str, Any]:
    """统一构造三个桶的 entry。"""
    evidence = (truth_result or {}).get("evidence") or {}
    entry: Dict[str, Any] = {
        "ticker": ticker,
        "name": (truth_result or {}).get("name"),
        "score": (truth_result or {}).get("score", 0) or 0,
        "verdict": (truth_result or {}).get("verdict", "❌ 不入选"),
        "themes": list(meta.get("themes") or []),
        "hotspot_sources": list(meta.get("hotspot_sources") or []),
        "evidence_summary": _evidence_summary(evidence) if evidence else {
            "announcements": False,
            "shareholder_changes": False,
            "dragon_tiger": False,
            "earnings_forecast": False,
            "institutional_research": False,
        },
    }
    if forced:
        entry["forced"] = True
    if skipped:
        entry["skipped"] = True
    if error:
        entry["error"] = error
    if include_detail and evidence:
        entry["truth_detail"] = evidence
    return entry


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


def apply_truth_gate(
    hotspots_result: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
    thresholds: Optional[Dict[str, int]] = None,
    max_tickers: int = 20,
) -> Dict[str, Any]:
    """把 hotspots.themes 里的 ticker 过 truth 5 源核查，分三桶返回。

    细节见模块 docstring。
    """
    start = time.time()
    cfg = config or {}
    thr = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    cand_thr = thr.get("candidate", 2)
    key_thr = thr.get("key_recommend", 3)

    universe = (cfg.get("universe") or {}) if isinstance(cfg, dict) else {}
    holdings: List[str] = [str(h).zfill(6) for h in (universe.get("holdings") or [])]
    holdings_set = set(holdings)

    as_of = hotspots_result.get("as_of") or datetime.now().strftime("%Y-%m-%d")
    themes = hotspots_result.get("themes") or []

    # 1. 收集 ticker -> 元信息
    ticker_map = _collect_ticker_themes(themes)

    # 2. 把持仓也塞进 ticker_map（若未出现）—— 强制过 gate
    for h in holdings:
        if h not in ticker_map:
            ticker_map[h] = {
                "themes": [],
                "hotspot_sources": [],
                "best_strength": "weak",
                "hit_count": 0,
            }
        # 保留原有 themes 即可

    if not ticker_map:
        logger.warning("apply_truth_gate: no tickers to evaluate")
        return {
            "as_of": as_of,
            "thresholds": {"candidate": cand_thr, "key_recommend": key_thr},
            "key_recommendations": [],
            "candidates": [],
            "rejected": [],
            "stats": {
                "total_tickers_evaluated": 0,
                "score_distribution": {i: 0 for i in range(6)},
                "elapsed_seconds": round(time.time() - start, 2),
            },
        }

    # 3. 排序决定 top-N 先 score
    ordered = _sort_tickers_for_scoring(ticker_map, holdings_set)
    to_score = ordered[:max_tickers]
    to_skip = ordered[max_tickers:]

    logger.info(
        "apply_truth_gate: %d tickers total, scoring top %d, skipping %d",
        len(ordered),
        len(to_score),
        len(to_skip),
    )

    key_recommendations: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    score_distribution: Dict[int, int] = {i: 0 for i in range(6)}

    # 4. 逐个 score（同进程共享 truth._CACHE，相当于批处理）
    for idx, tic in enumerate(to_score, 1):
        meta = ticker_map[tic]
        forced_holding = tic in holdings_set
        logger.info(
            "[%d/%d] scoring %s (themes=%s, holding=%s)",
            idx,
            len(to_score),
            tic,
            meta.get("themes"),
            forced_holding,
        )
        t0 = time.time()
        err: Optional[str] = None
        result: Optional[Dict[str, Any]] = None
        try:
            result = score_truth(tic, config=cfg)
        except Exception as e:  # 容错：不崩
            err = f"{type(e).__name__}: {e}"
            logger.exception("score_truth failed for %s: %s", tic, err)

        elapsed = time.time() - t0

        if result is None:
            # 彻底挂了 -> rejected
            logger.warning("%s: score_truth error -> rejected (%.1fs)", tic, elapsed)
            entry = _build_bucket_entry(
                tic, meta, None, error=err, include_detail=False
            )
            rejected.append(entry)
            score_distribution[0] += 1
            continue

        score = int(result.get("score") or 0)
        score_distribution[max(0, min(5, score))] += 1
        logger.info(
            "%s: score=%d verdict=%s (%.1fs)",
            tic,
            score,
            result.get("verdict"),
            elapsed,
        )

        if score >= key_thr:
            key_recommendations.append(
                _build_bucket_entry(tic, meta, result, include_detail=True)
            )
        elif score >= cand_thr:
            candidates.append(
                _build_bucket_entry(tic, meta, result, include_detail=True)
            )
        else:
            # 持仓强制入 candidates（标 forced）
            if forced_holding:
                candidates.append(
                    _build_bucket_entry(
                        tic,
                        meta,
                        result,
                        forced=True,
                        include_detail=True,
                    )
                )
                logger.info("%s: forced into candidates (holding)", tic)
            else:
                rejected.append(
                    _build_bucket_entry(
                        tic, meta, result, include_detail=False
                    )
                )

    # 5. skipped 的 ticker 直接进 rejected（不浪费 truth 接口）
    for tic in to_skip:
        meta = ticker_map[tic]
        forced_holding = tic in holdings_set
        if forced_holding:
            # 持仓不应被 skip；万一 max_tickers 小于持仓数，保底塞 candidates
            logger.warning(
                "%s: holding skipped due to max_tickers; forcing into candidates without truth score",
                tic,
            )
            candidates.append(
                _build_bucket_entry(
                    tic,
                    meta,
                    None,
                    forced=True,
                    skipped=True,
                    include_detail=False,
                )
            )
            score_distribution[0] += 1
            continue
        rejected.append(
            _build_bucket_entry(
                tic, meta, None, skipped=True, include_detail=False
            )
        )
        score_distribution[0] += 1

    # 6. 桶内部再排序：按 score 降序（相同按持仓优先）
    def _entry_key(e: Dict[str, Any]):
        is_h = e["ticker"] in holdings_set
        return (-int(e.get("score") or 0), not is_h, e["ticker"])

    key_recommendations.sort(key=_entry_key)
    candidates.sort(key=_entry_key)
    rejected.sort(key=_entry_key)

    total_evaluated = (
        len(key_recommendations) + len(candidates) + len(rejected)
    )
    elapsed_total = round(time.time() - start, 2)
    logger.info(
        "apply_truth_gate done: total=%d key=%d cand=%d rej=%d in %.1fs",
        total_evaluated,
        len(key_recommendations),
        len(candidates),
        len(rejected),
        elapsed_total,
    )

    return {
        "as_of": as_of,
        "thresholds": {"candidate": cand_thr, "key_recommend": key_thr},
        "key_recommendations": key_recommendations,
        "candidates": candidates,
        "rejected": rejected,
        "stats": {
            "total_tickers_evaluated": total_evaluated,
            "score_distribution": score_distribution,
            "elapsed_seconds": elapsed_total,
        },
    }


def run_gate_pipeline(
    config: Optional[Dict[str, Any]] = None,
    as_of: Optional[str] = None,
    max_tickers: int = 20,
    thresholds: Optional[Dict[str, int]] = None,
    lookback_hours: int = 24,
) -> Dict[str, Any]:
    """一键 pipeline：find_hotspots -> apply_truth_gate。"""
    cfg = config or {}
    logger.info("run_gate_pipeline: calling find_hotspots as_of=%s", as_of)
    hotspots = find_hotspots(
        config=cfg, lookback_hours=lookback_hours, as_of=as_of
    )
    logger.info(
        "find_hotspots done: %d themes, sources_ok=%s",
        len(hotspots.get("themes") or []),
        {k: v.get("ok") for k, v in (hotspots.get("sources") or {}).items()},
    )
    gated = apply_truth_gate(
        hotspots,
        config=cfg,
        thresholds=thresholds,
        max_tickers=max_tickers,
    )
    return {"hotspots": hotspots, "gated": gated}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _json_default(o: Any) -> Any:
    if isinstance(o, (pd.Timestamp, datetime)):
        return str(o)
    if hasattr(o, "item"):
        try:
            return o.item()
        except Exception:
            return str(o)
    return str(o)


def _print_gate_summary(gated: Dict[str, Any]) -> None:
    """人看的 summary：stats + top 5 key_recommendations。"""
    stats = gated.get("stats") or {}
    thr = gated.get("thresholds") or {}
    print("=" * 60)
    print(f"gate as_of={gated.get('as_of')} thresholds={thr}")
    print(
        f"total_evaluated={stats.get('total_tickers_evaluated')} "
        f"elapsed={stats.get('elapsed_seconds')}s"
    )
    print(f"score_distribution={stats.get('score_distribution')}")
    print(
        f"buckets: key={len(gated.get('key_recommendations') or [])} "
        f"cand={len(gated.get('candidates') or [])} "
        f"rej={len(gated.get('rejected') or [])}"
    )
    print("-" * 60)
    top = (gated.get("key_recommendations") or [])[:5]
    if top:
        print(f"Top {len(top)} key_recommendations:")
        for i, e in enumerate(top, 1):
            flags = []
            if e.get("forced"):
                flags.append("forced")
            if e.get("skipped"):
                flags.append("skipped")
            flag_s = f" [{','.join(flags)}]" if flags else ""
            print(
                f"  {i}. {e.get('ticker')} {e.get('name') or ''} "
                f"score={e.get('score')} {e.get('verdict')}{flag_s}"
            )
            print(f"     themes={e.get('themes')}")
            print(f"     hotspot_sources={e.get('hotspot_sources')}")
            print(f"     evidence={e.get('evidence_summary')}")
    else:
        print("No key_recommendations today.")

    # 也打 candidates 简表，便于观察持仓状态
    cands = gated.get("candidates") or []
    if cands:
        print("-" * 60)
        print(f"Candidates ({len(cands)}):")
        for e in cands[:10]:
            flags = []
            if e.get("forced"):
                flags.append("forced")
            if e.get("skipped"):
                flags.append("skipped")
            if e.get("error"):
                flags.append("error")
            flag_s = f" [{','.join(flags)}]" if flags else ""
            print(
                f"  - {e.get('ticker')} {e.get('name') or ''} "
                f"score={e.get('score')}{flag_s} themes={e.get('themes')}"
            )
    print("=" * 60)
    # 最后一行 summary
    print(
        f"SUMMARY: as_of={gated.get('as_of')} "
        f"total={stats.get('total_tickers_evaluated')} "
        f"key={len(gated.get('key_recommendations') or [])} "
        f"cand={len(cands)} "
        f"rej={len(gated.get('rejected') or [])} "
        f"elapsed={stats.get('elapsed_seconds')}s"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="A 股真实性 gate")
    parser.add_argument(
        "--as-of", default=None, help="YYYY-MM-DD 或 YYYYMMDD；默认今天"
    )
    parser.add_argument(
        "--lookback-hours", type=int, default=24, help="find_hotspots 回溯小时数"
    )
    parser.add_argument(
        "--max-tickers",
        type=int,
        default=20,
        help="top-N 调 truth 评分（默认 20）",
    )
    parser.add_argument(
        "--candidate-thr", type=int, default=2, help="candidate 阈值"
    )
    parser.add_argument(
        "--key-thr", type=int, default=3, help="key_recommend 阈值"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="DEBUG/INFO/WARNING/ERROR (默认 INFO)",
    )
    parser.add_argument(
        "--dump-json",
        action="store_true",
        help="同时打印 gated 完整 JSON（默认只打 summary）",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    pipeline = run_gate_pipeline(
        as_of=args.as_of,
        max_tickers=args.max_tickers,
        thresholds={
            "candidate": args.candidate_thr,
            "key_recommend": args.key_thr,
        },
        lookback_hours=args.lookback_hours,
    )
    gated = pipeline["gated"]

    if args.dump_json:
        print(
            json.dumps(
                gated,
                ensure_ascii=False,
                indent=2,
                default=_json_default,
            )
        )

    _print_gate_summary(gated)


if __name__ == "__main__":
    main()

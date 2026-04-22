"""
find_hotspots.py — 6+1 源热点发现

早上 9:30 跑一次，回答"今天可能被炒的是什么"。

6 个 akshare 源 + 1 个本地 inbox（小红书手动粘贴）：
  1. 财联社电报 (stock_info_global_cls)
  2. 东财个股新闻 (stock_news_em, 按持仓池扫)
  3. 行业板块涨跌 (stock_board_industry_name_em)
  4. 涨停板池 (stock_zt_pool_em)
  5. 龙虎榜 (stock_lhb_detail_daily_sina + fallback stock_lhb_detail_em)
  6. 近期研报评级 (stock_research_report_em)
  7. inbox 博主推文 (data/input/inbox/*.md)

设计原则：
  - 每个源独立 try/except，一个坏不影响其他
  - 某源拉不到就老实 ok=false，不 mock
  - themes 按 industry_boards top 5 骨架聚合，其他源按"板块名 in title" / "ticker in 成分股" 归并
  - 持仓命中的 theme 置顶

用法：
    python3 -m watchdog.find_hotspots --as-of 2026-04-21
"""

from __future__ import annotations

import argparse
import json
import re
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    import akshare as ak
except ImportError as e:  # pragma: no cover
    raise ImportError("akshare is required, run: pip install akshare") from e


# --------------------------------------------------------------------------
# 日期工具
# --------------------------------------------------------------------------


def _as_of_date(as_of: Optional[str]) -> datetime:
    """接受 None / 'YYYY-MM-DD' / 'YYYYMMDD'，返回 datetime。"""
    if as_of is None:
        return datetime.now()
    s = as_of.replace("-", "").strip()
    return datetime.strptime(s, "%Y%m%d")


def _ymd(d: datetime) -> str:
    """YYYYMMDD for akshare."""
    return d.strftime("%Y%m%d")


def _iso(d: datetime) -> str:
    """YYYY-MM-DD for output."""
    return d.strftime("%Y-%m-%d")


def _within_lookback(ts_str: str, lookback_hours: int, now: datetime) -> bool:
    """粗略判断 ts 字符串是否在 [now - lookback_hours, now] 内。解析失败默认 True（宁可多保留）。"""
    if not ts_str:
        return True
    cutoff = now - timedelta(hours=lookback_hours)
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%Y%m%d",
    ):
        try:
            ts = datetime.strptime(str(ts_str).strip()[: len(fmt) + 2], fmt)
            return ts >= cutoff
        except ValueError:
            continue
    return True


# --------------------------------------------------------------------------
# Source 包装器：每个源返回 {"ok", "items", "error"}
# --------------------------------------------------------------------------


def _safe_call(fn, *args, **kwargs) -> Dict[str, Any]:
    """包一层 try/except；成功返回 {"ok": True, "items": [...]}；失败返回 {"ok": False, "items": [], "error": str}。"""
    try:
        items = fn(*args, **kwargs)
        return {"ok": True, "items": items, "error": None}
    except Exception as e:
        return {
            "ok": False,
            "items": [],
            "error": f"{type(e).__name__}: {str(e)[:200]}",
        }


# ---- 1. 财联社电报 --------------------------------------------------------


def _fetch_cls_telegraph(lookback_hours: int, now: datetime) -> List[Dict]:
    df = ak.stock_info_global_cls(symbol="全部")
    if df is None or df.empty:
        return []
    items: List[Dict] = []
    for _, row in df.iterrows():
        title = str(row.get("标题", "") or "")
        content = str(row.get("内容", "") or "")
        pub_date = str(row.get("发布日期", "") or "")
        pub_time = str(row.get("发布时间", "") or "")
        ts = f"{pub_date} {pub_time}".strip()
        if not _within_lookback(ts, lookback_hours, now):
            continue
        items.append(
            {
                "title": title,
                "content": content[:200],
                "publish_time": ts,
                "tickers": _extract_tickers_from_text(title + " " + content),
            }
        )
    return items


# ---- 2. 东财个股新闻（按持仓池扫） -------------------------------------


def _fetch_eastmoney_news(holdings: List[str], lookback_hours: int, now: datetime) -> List[Dict]:
    items: List[Dict] = []
    for code in holdings[:5]:  # 最多扫 5 只（从 10 降到 5，每只 ~0.3s）
        try:
            df = ak.stock_news_em(symbol=code)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            title = str(row.get("新闻标题", "") or "")
            content = str(row.get("新闻内容", "") or "")
            ts = str(row.get("发布时间", "") or "")
            if not _within_lookback(ts, lookback_hours, now):
                continue
            items.append(
                {
                    "ticker": code,
                    "title": title,
                    "content": content[:200],
                    "publish_time": ts,
                    "source": str(row.get("文章来源", "") or ""),
                }
            )
    return items


# ---- 3. 行业板块涨跌 -----------------------------------------------------


def _fetch_industry_boards(top_n: int = 10) -> List[Dict]:
    df = ak.stock_board_industry_name_em()
    if df is None or df.empty:
        return []
    # 期望字段包含涨跌幅；按涨跌幅降序
    pct_col = None
    for cand in ["涨跌幅", "最新价"]:
        if cand in df.columns:
            pct_col = cand
            break
    if pct_col is None:
        # 字段未知，返回前 top_n
        return df.head(top_n).to_dict(orient="records")
    df_sorted = df.sort_values(pct_col, ascending=False).head(top_n)
    items = []
    for _, row in df_sorted.iterrows():
        items.append(
            {
                "board_name": str(row.get("板块名称", "") or ""),
                "board_code": str(row.get("板块代码", "") or ""),
                "change_pct": float(row.get("涨跌幅", 0) or 0),
                "leading_stock": str(row.get("领涨股票", "") or ""),
                "turnover": row.get("成交额"),
            }
        )
    return items


# ---- 4. 涨停板池 ---------------------------------------------------------


def _fetch_zt_pool(date_ymd: str) -> List[Dict]:
    df = ak.stock_zt_pool_em(date=date_ymd)
    if df is None or df.empty:
        return []
    items = []
    for _, row in df.iterrows():
        items.append(
            {
                "ticker": str(row.get("代码", "") or ""),
                "name": str(row.get("名称", "") or ""),
                "industry": str(row.get("所属行业", "") or ""),
                "change_pct": float(row.get("涨跌幅", 0) or 0),
                "streak": int(row.get("连板数", 0) or 0),
                "reason": str(row.get("涨停原因", "") or ""),
            }
        )
    return items


# ---- 5. 龙虎榜 ----------------------------------------------------------


def _fetch_dragon_tiger(date_ymd: str) -> List[Dict]:
    """优先 sina 每日详情；失败回退 em detail。"""
    last_err: Optional[Exception] = None
    try:
        df = ak.stock_lhb_detail_daily_sina(date=date_ymd)
        if df is not None and not df.empty:
            items = []
            for _, row in df.iterrows():
                items.append(
                    {
                        "ticker": str(row.get("股票代码", "") or ""),
                        "name": str(row.get("股票名称", "") or ""),
                        "close": row.get("收盘价"),
                        "reason": str(row.get("指标", "") or ""),
                        "turnover": row.get("成交额"),
                    }
                )
            return items
    except Exception as e:
        last_err = e

    # fallback
    df = ak.stock_lhb_detail_em(start_date=date_ymd, end_date=date_ymd)
    if df is None or df.empty:
        if last_err:
            raise last_err
        return []
    items = []
    # em detail 的列名版本间不稳定，尽量兼容
    name_col = next((c for c in ["代码", "股票代码"] if c in df.columns), None)
    label_col = next((c for c in ["名称", "股票名称"] if c in df.columns), None)
    reason_col = next((c for c in ["上榜原因", "解读", "指标"] if c in df.columns), None)
    for _, row in df.iterrows():
        items.append(
            {
                "ticker": str(row.get(name_col, "") or "") if name_col else "",
                "name": str(row.get(label_col, "") or "") if label_col else "",
                "reason": str(row.get(reason_col, "") or "") if reason_col else "",
            }
        )
    return items


# ---- 6. 近期研报评级 ----------------------------------------------------


def _fetch_research_rating(lookback_hours: int, now: datetime) -> List[Dict]:
    """注意：stock_research_report_em 的 symbol 参数实际作为关键词模糊匹配，
    传任意常见代码都会返回全市场近期研报；symbol='全部' 会报 KeyError。"""
    df = ak.stock_research_report_em(symbol="600519")
    if df is None or df.empty:
        return []
    items = []
    # lookback 粗略用天数：hours//24 + 2 天 buffer
    cutoff_days = max(1, lookback_hours // 24 + 2)
    cutoff = now - timedelta(days=cutoff_days)
    for _, row in df.iterrows():
        date_str = str(row.get("日期", "") or "")
        try:
            d = datetime.strptime(date_str[:10], "%Y-%m-%d")
            if d < cutoff:
                continue
        except ValueError:
            pass
        items.append(
            {
                "ticker": str(row.get("股票代码", "") or ""),
                "name": str(row.get("股票简称", "") or ""),
                "report_title": str(row.get("报告名称", "") or ""),
                "rating": str(row.get("东财评级", "") or ""),
                "institution": str(row.get("机构", "") or ""),
                "industry": str(row.get("行业", "") or ""),
                "date": date_str,
            }
        )
    return items


# ---- 7. inbox（小红书手动粘贴） ------------------------------------------


TICKER_RE = re.compile(r"(?<![0-9])([0-9]{6})(?![0-9])")


def _fetch_inbox(inbox_dir: Path, universe_names: Dict[str, str]) -> List[Dict]:
    """
    universe_names: {"601699": "潞安环能", ...}（name -> code 反查），
    MVP：扫 *.md，抽 6 位代码 + 比对中文名。
    """
    if not inbox_dir.exists():
        return []
    items: List[Dict] = []
    name_to_code = {v: k for k, v in universe_names.items() if v}
    for md_path in sorted(inbox_dir.glob("*.md")):
        try:
            text = md_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        hits: List[str] = []
        for m in TICKER_RE.findall(text):
            if m not in hits:
                hits.append(m)
        for name, code in name_to_code.items():
            if name in text and code not in hits:
                hits.append(code)
        if hits:
            items.append(
                {
                    "file": md_path.name,
                    "tickers": hits,
                    "preview": text[:200].replace("\n", " "),
                }
            )
    return items


# ---- 辅助：从文本里抽 ticker ---------------------------------------------


def _extract_tickers_from_text(text: str) -> List[str]:
    out: List[str] = []
    for m in TICKER_RE.findall(text or ""):
        if m not in out:
            out.append(m)
    return out


# --------------------------------------------------------------------------
# themes 聚合
# --------------------------------------------------------------------------


STRENGTH_LABEL = {3: "strong", 2: "medium", 1: "weak"}


def _aggregate_themes(
    sources: Dict[str, Dict[str, Any]],
    holdings: List[str],
) -> List[Dict[str, Any]]:
    """
    MVP 聚合：
      - 以 industry_boards top 5 为骨架；如果 industry_boards 挂了，用 zt_pool 的 top 所属行业补位
      - 每个 theme：
          * zt_pool: 行业匹配
          * cls/news: 板块名 in title
          * research: 行业字段匹配
          * dragon_tiger / inbox: ticker 命中则归到含该 ticker 的 theme
    """
    board_items = sources.get("industry_boards", {}).get("items", []) or []
    zt_items = sources.get("zt_pool", {}).get("items", []) or []
    cls_items = sources.get("cls_telegraph", {}).get("items", []) or []
    news_items = sources.get("eastmoney_news", {}).get("items", []) or []
    rr_items = sources.get("research_rating", {}).get("items", []) or []
    lhb_items = sources.get("dragon_tiger", {}).get("items", []) or []
    inbox_items = sources.get("inbox", {}).get("items", []) or []

    # 1. 骨架：top 5 板块
    board_names: List[str] = []
    for b in board_items[:5]:
        nm = b.get("board_name") or b.get("板块名称") or ""
        if nm:
            board_names.append(nm)

    # 1b. 骨架补位：如果板块挂了，从 zt_pool 的 industry 字段取 top 5 频次
    if not board_names:
        from collections import Counter

        ind_counter: Counter = Counter()
        for z in zt_items:
            ind = z.get("industry") or ""
            if ind:
                ind_counter[ind] += 1
        board_names = [n for n, _ in ind_counter.most_common(5)]

    # 2. 按 theme 初始化容器
    themes: Dict[str, Dict[str, Any]] = {}
    for name in board_names:
        themes[name] = {
            "theme": name,
            "tickers": [],
            "sources_hit": set(),
            "board_change_pct": 0.0,
            "zt_count": 0,
            "rating_up_count": 0,
            "news_samples": [],
            "lhb_tickers": [],
            "inbox_tickers": [],
        }
    # 其他桶
    themes.setdefault(
        "其他",
        {
            "theme": "其他",
            "tickers": [],
            "sources_hit": set(),
            "board_change_pct": 0.0,
            "zt_count": 0,
            "rating_up_count": 0,
            "news_samples": [],
            "lhb_tickers": [],
            "inbox_tickers": [],
        },
    )

    def _match_theme(text: str) -> Optional[str]:
        """找 text 里包含哪个 theme 名字（最长优先）。"""
        for nm in sorted(board_names, key=len, reverse=True):
            if nm and nm in text:
                return nm
        return None

    # 3. 板块本身
    for b in board_items[:5]:
        nm = b.get("board_name") or ""
        if nm in themes:
            themes[nm]["sources_hit"].add("industry_boards")
            try:
                themes[nm]["board_change_pct"] = float(b.get("change_pct", 0) or 0)
            except (TypeError, ValueError):
                pass

    # 4. zt_pool：按 industry 归队
    for z in zt_items:
        ind = z.get("industry") or ""
        t = z.get("ticker") or ""
        target = ind if ind in themes else "其他"
        themes[target]["sources_hit"].add("zt_pool")
        themes[target]["zt_count"] += 1
        if t and t not in themes[target]["tickers"]:
            themes[target]["tickers"].append(t)

    # 5. cls_telegraph：标题命中板块名
    for c in cls_items:
        title = (c.get("title") or "") + " " + (c.get("content") or "")
        nm = _match_theme(title)
        target = nm if nm else "其他"
        themes[target]["sources_hit"].add("cls_telegraph")
        if len(themes[target]["news_samples"]) < 3:
            themes[target]["news_samples"].append(c.get("title", "")[:60])
        for t in c.get("tickers") or []:
            if t and t not in themes[target]["tickers"]:
                themes[target]["tickers"].append(t)

    # 6. eastmoney_news：标题命中板块；同时按 ticker
    for n in news_items:
        title = n.get("title") or ""
        t = n.get("ticker") or ""
        nm = _match_theme(title)
        target = nm if nm else None
        # 如果没命中板块但 ticker 属于某 theme（tickers 列表里），归过去
        if target is None:
            for tn, td in themes.items():
                if t and t in td["tickers"]:
                    target = tn
                    break
        target = target or "其他"
        themes[target]["sources_hit"].add("eastmoney_news")
        if len(themes[target]["news_samples"]) < 3 and title:
            themes[target]["news_samples"].append(title[:60])
        if t and t not in themes[target]["tickers"]:
            themes[target]["tickers"].append(t)

    # 7. research_rating：行业字段匹配
    for r in rr_items:
        ind = r.get("industry") or ""
        t = r.get("ticker") or ""
        rating = r.get("rating") or ""
        nm = _match_theme(ind) if ind else None
        target = nm if nm else "其他"
        themes[target]["sources_hit"].add("research_rating")
        # 粗略：评级含"买入/增持/推荐"算上调
        if any(k in rating for k in ("买入", "增持", "推荐", "强烈")):
            themes[target]["rating_up_count"] += 1
        if t and t not in themes[target]["tickers"]:
            themes[target]["tickers"].append(t)

    # 8. dragon_tiger：ticker 归到已有 theme；否则"其他"
    for l in lhb_items:
        t = l.get("ticker") or ""
        matched = None
        for tn, td in themes.items():
            if t and t in td["tickers"]:
                matched = tn
                break
        target = matched or "其他"
        themes[target]["sources_hit"].add("dragon_tiger")
        if t and t not in themes[target]["lhb_tickers"]:
            themes[target]["lhb_tickers"].append(t)
        if t and t not in themes[target]["tickers"]:
            themes[target]["tickers"].append(t)

    # 9. inbox：同上
    for ib in inbox_items:
        for t in ib.get("tickers") or []:
            matched = None
            for tn, td in themes.items():
                if t in td["tickers"]:
                    matched = tn
                    break
            target = matched or "其他"
            themes[target]["sources_hit"].add("inbox")
            if t not in themes[target]["inbox_tickers"]:
                themes[target]["inbox_tickers"].append(t)
            if t not in themes[target]["tickers"]:
                themes[target]["tickers"].append(t)

    # 10. 固化 + 写 summary
    out: List[Dict[str, Any]] = []
    for name, td in themes.items():
        hit_cnt = len(td["sources_hit"])
        if hit_cnt == 0:
            continue
        strength = STRENGTH_LABEL.get(min(hit_cnt, 3), "weak")
        summary_bits: List[str] = []
        if "industry_boards" in td["sources_hit"] and td["board_change_pct"]:
            summary_bits.append(f"板块 {td['board_change_pct']:+.2f}%")
        if td["zt_count"]:
            summary_bits.append(f"涨停 {td['zt_count']} 只")
        if td["rating_up_count"]:
            summary_bits.append(f"{td['rating_up_count']} 家机构上调")
        if td["lhb_tickers"]:
            summary_bits.append(f"龙虎榜 {len(td['lhb_tickers'])} 只")
        if td["inbox_tickers"]:
            summary_bits.append(f"inbox {len(td['inbox_tickers'])} 只")
        if td["news_samples"]:
            summary_bits.append(f"新闻:{td['news_samples'][0]}")
        summary = "；".join(summary_bits) if summary_bits else f"命中 {hit_cnt} 源"
        out.append(
            {
                "theme": name,
                "tickers": td["tickers"][:20],
                "sources_hit": sorted(td["sources_hit"]),
                "strength": strength,
                "summary": f"今日{name}：{summary}",
                "_hit_count": hit_cnt,
                "_board_pct": td["board_change_pct"],
                "_has_holding": any(t in holdings for t in td["tickers"]),
            }
        )

    # 11. 排序：持仓命中优先 → 源数多的优先 → 板块涨幅高的优先
    out.sort(
        key=lambda x: (
            not x["_has_holding"],  # False(有持仓) 在前
            -x["_hit_count"],
            -(x["_board_pct"] or 0),
        )
    )

    # 去掉内部字段
    for x in out:
        x.pop("_hit_count", None)
        x.pop("_board_pct", None)
        x.pop("_has_holding", None)

    return out


# --------------------------------------------------------------------------
# 对外主函数
# --------------------------------------------------------------------------


DEFAULT_HOLDINGS: List[str] = ["601699", "513310", "588000"]


def find_hotspots(
    config: Optional[Dict] = None,
    lookback_hours: int = 24,
    as_of: Optional[str] = None,
) -> Dict[str, Any]:
    """6+1 源热点发现。返回结构见模块 docstring / 注释。"""
    config = config or {}
    universe = (config.get("universe") or {}) if isinstance(config, dict) else {}
    holdings: List[str] = universe.get("holdings") or DEFAULT_HOLDINGS
    universe_names: Dict[str, str] = universe.get("names") or {}
    inbox_dir = Path(
        config.get("inbox_dir")
        or "/Users/fqyuki/Documents/kd_2026/自学内容/代码类/ashare-watchdog/data/input/inbox"
    )

    now = _as_of_date(as_of)
    date_ymd = _ymd(now)

    sources: Dict[str, Dict[str, Any]] = {}
    sources["cls_telegraph"] = _safe_call(_fetch_cls_telegraph, lookback_hours, now)
    sources["eastmoney_news"] = _safe_call(_fetch_eastmoney_news, holdings, lookback_hours, now)
    sources["industry_boards"] = _safe_call(_fetch_industry_boards, 10)
    sources["zt_pool"] = _safe_call(_fetch_zt_pool, date_ymd)
    sources["dragon_tiger"] = _safe_call(_fetch_dragon_tiger, date_ymd)
    sources["research_rating"] = _safe_call(_fetch_research_rating, lookback_hours, now)
    sources["inbox"] = _safe_call(_fetch_inbox, inbox_dir, universe_names)

    themes = _aggregate_themes(sources, holdings)

    return {
        "as_of": _iso(now),
        "sources": sources,
        "themes": themes,
    }


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _json_default(o: Any) -> Any:
    if isinstance(o, (pd.Timestamp, datetime)):
        return str(o)
    if isinstance(o, (pd.Series,)):
        return o.to_dict()
    if hasattr(o, "item"):  # numpy scalars
        try:
            return o.item()
        except Exception:
            return str(o)
    return str(o)


def main() -> None:
    parser = argparse.ArgumentParser(description="6+1 源热点发现")
    parser.add_argument("--as-of", default=None, help="日期，YYYY-MM-DD 或 YYYYMMDD；默认今天")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--summary", action="store_true", help="只打印精简摘要")
    args = parser.parse_args()

    result = find_hotspots(lookback_hours=args.lookback_hours, as_of=args.as_of)

    if args.summary:
        summary = {
            "as_of": result["as_of"],
            "sources_ok": {k: v["ok"] for k, v in result["sources"].items()},
            "sources_errors": {
                k: v["error"] for k, v in result["sources"].items() if not v["ok"]
            },
            "source_item_counts": {
                k: len(v["items"]) if isinstance(v["items"], list) else 0
                for k, v in result["sources"].items()
            },
            "themes_count": len(result["themes"]),
            "themes": [
                {
                    "theme": t["theme"],
                    "strength": t["strength"],
                    "sources_hit": t["sources_hit"],
                    "tickers_head": t["tickers"][:5],
                    "summary": t["summary"],
                }
                for t in result["themes"]
            ],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default))
    else:
        # 为保持 JSON 可打印，裁剪 items（每源前 5）
        preview = {
            "as_of": result["as_of"],
            "sources": {
                k: {
                    "ok": v["ok"],
                    "error": v["error"],
                    "item_count": len(v["items"]) if isinstance(v["items"], list) else 0,
                    "items_head": v["items"][:5] if isinstance(v["items"], list) else [],
                }
                for k, v in result["sources"].items()
            },
            "themes": result["themes"],
        }
        print(json.dumps(preview, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    main()

"""ask-stock: 自然语言问单票 → 结构化判断（瘦身版）

设计目标：从旧项目 ashare_harness_v2/advice_harness 的 ~18000 行
管线里，只保留 "用户问一只票 → 返回结构化判断" 的核心。

核心链路：
  1. 从问题抽取 ticker（6 位数字 / 常见中文名映射）
  2. akshare 拉最近 1 个月日线 + 新闻 + 公告
  3. 规则引擎综合给 verdict（不依赖 LLM）

用法：
  PYTHONPATH=src python3 -m watchdog.ask_stock --question "宁德时代现在能买吗"
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

try:
    import akshare as ak
except Exception:  # pragma: no cover
    ak = None

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

# 股票名 → 代码 映射（常见票先 hardcode，找不到再 error）
NAME_TO_CODE: dict[str, str] = {
    "宁德时代": "300750",
    "寒武纪": "688256",
    "潞安环能": "601699",
    "中韩半导体": "513310",
    "科创50": "588000",
    "北方华创": "002371",
    "中芯国际": "688981",
    "比亚迪": "002594",
    "贵州茅台": "600519",
    "中国平安": "601318",
    "招商银行": "600036",
}
CODE_TO_NAME: dict[str, str] = {v: k for k, v in NAME_TO_CODE.items()}

# 6 位代码正则
_CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")

# 公告/新闻标题里的利空关键词（命中 → verdict 倾向降级）
NEGATIVE_KEYWORDS = (
    "减持", "质押", "违规", "处罚", "立案", "起诉", "亏损",
    "下滑", "ST", "退市", "问询函", "风险警示", "裁员", "预减",
    "大股东减持", "辞职", "商誉减值",
)
POSITIVE_KEYWORDS = (
    "增持", "回购", "中标", "签约", "获批", "新高", "利润大增",
    "业绩超预期", "股权激励", "分红", "业绩预增", "订单",
)


# ---------------------------------------------------------------------------
# 股票代码抽取
# ---------------------------------------------------------------------------

def extract_ticker(question: str) -> tuple[str | None, str | None]:
    """从问题里抽取 ticker。返回 (ticker, name_or_None)。"""
    q = (question or "").strip()
    # 1) 先看有没有 6 位数字
    m = _CODE_RE.search(q.replace(" ", ""))
    if m:
        code = m.group(1)
        return code, CODE_TO_NAME.get(code)
    # 2) 查中文名映射
    for name, code in NAME_TO_CODE.items():
        if name in q:
            return code, name
    return None, None


# ---------------------------------------------------------------------------
# 数据拉取（每个源 try/except，失败降级）
# ---------------------------------------------------------------------------

def fetch_price_action(ticker: str, lookback_days: int = 30) -> dict[str, Any]:
    """近 N 日日线。返回 {latest_close, pct_1d, pct_5d, pct_20d, high, low, bars, error}。"""
    if ak is None:
        return {"error": "akshare 未安装"}
    try:
        today = datetime.now()
        start = (today - timedelta(days=lookback_days * 2 + 10)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")
        df = ak.stock_zh_a_hist(
            symbol=ticker, period="daily",
            start_date=start, end_date=end, adjust="qfq",
        )
        if df is None or df.empty:
            return {"error": "no data"}
        df = df.tail(lookback_days).reset_index(drop=True)
        latest = float(df["收盘"].iloc[-1])
        pct_1d = _pct_change(df["收盘"], 1)
        pct_5d = _pct_change(df["收盘"], 5)
        pct_20d = _pct_change(df["收盘"], min(20, len(df) - 1))
        return {
            "latest_close": round(latest, 3),
            "pct_1d": _round_pct(pct_1d),
            "pct_5d": _round_pct(pct_5d),
            "pct_20d": _round_pct(pct_20d),
            "high_1m": round(float(df["最高"].max()), 3),
            "low_1m": round(float(df["最低"].min()), 3),
            "bars": int(len(df)),
            "error": None,
        }
    except Exception as e:
        logger.debug("fetch_price_action failed for %s: %s", ticker, e)
        return {"error": f"{type(e).__name__}: {e}"}


def _pct_change(series: pd.Series, n: int) -> float | None:
    if series is None or len(series) <= n:
        return None
    try:
        prev = float(series.iloc[-1 - n])
        curr = float(series.iloc[-1])
        if prev == 0:
            return None
        return (curr / prev - 1) * 100
    except Exception:
        return None


def _round_pct(v: float | None) -> float | None:
    return None if v is None else round(v, 2)


def fetch_recent_news(ticker: str, lookback_days: int = 14, limit: int = 10) -> list[dict[str, Any]]:
    """近 N 日新闻。失败返回 []。"""
    if ak is None:
        return []
    try:
        df = ak.stock_news_em(symbol=ticker)
        if df is None or df.empty:
            return []
        time_col = "发布时间" if "发布时间" in df.columns else None
        title_col = "新闻标题" if "新闻标题" in df.columns else None
        if time_col and title_col:
            cutoff = datetime.now() - timedelta(days=lookback_days)
            ts = pd.to_datetime(df[time_col], errors="coerce")
            df = df.loc[ts >= cutoff].copy()
        items = []
        for _, row in df.head(limit).iterrows():
            items.append({
                "date": str(row.get(time_col, ""))[:19] if time_col else "",
                "title": str(row.get(title_col, "") if title_col else row.iloc[0]),
            })
        return items
    except Exception as e:
        logger.debug("fetch_recent_news failed for %s: %s", ticker, e)
        return []


def fetch_recent_notices(ticker: str, lookback_days: int = 14, limit: int = 10) -> list[dict[str, Any]]:
    """近 N 日公告。

    性能重写：旧实现按日跑 stock_notice_report(symbol='全部')，每天 85 页 + PDF
    拖累 ask 命令 ~90s。改用 stock_news_em(symbol=ticker) 单股直查（~0.3s）并
    按标题里是否含「公告」类关键词粗筛出公告类条目。
    """
    if ak is None:
        return []
    try:
        df = ak.stock_news_em(symbol=ticker)
    except Exception as e:
        logger.debug("fetch_recent_notices failed for %s: %s", ticker, e)
        return []
    if df is None or df.empty:
        return []

    time_col = "发布时间" if "发布时间" in df.columns else None
    title_col = "新闻标题" if "新闻标题" in df.columns else None
    if not title_col:
        return []

    cutoff = datetime.now() - timedelta(days=lookback_days)
    if time_col:
        ts = pd.to_datetime(df[time_col], errors="coerce")
        df = df.loc[ts >= cutoff].copy()

    # 公告类关键词（粗筛，保留误差但速度快）
    notice_kw = ("公告", "关于", "披露", "董事会", "股东大会", "减持", "增持", "停牌", "复牌")
    items: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        title = str(row.get(title_col, "") or "")
        if not any(kw in title for kw in notice_kw):
            continue
        items.append(
            {
                "date": str(row.get(time_col, ""))[:19] if time_col else "",
                "title": title,
                "type": "notice",
            }
        )
        if len(items) >= limit:
            break
    return items


# ---------------------------------------------------------------------------
# 规则引擎：price_action + news/notices → verdict
# ---------------------------------------------------------------------------

def _count_keyword_hits(items: list[dict[str, Any]], keywords: tuple[str, ...]) -> tuple[int, list[str]]:
    hits = 0
    examples: list[str] = []
    for it in items:
        title = it.get("title", "") or ""
        for kw in keywords:
            if kw in title:
                hits += 1
                if len(examples) < 3:
                    examples.append(title[:60])
                break
    return hits, examples


def judge(
    question: str,
    ticker: str,
    name: str | None,
    price: dict[str, Any],
    news: list[dict[str, Any]],
    notices: list[dict[str, Any]],
) -> dict[str, Any]:
    """规则打分 → verdict + summary + confidence。"""
    score = 0  # 正分倾向买，负分倾向避
    reasons: list[str] = []

    pct_5d = price.get("pct_5d")
    pct_20d = price.get("pct_20d")
    pct_1d = price.get("pct_1d")

    # 价格动能
    if pct_5d is not None:
        if pct_5d >= 5:
            score += 2
            reasons.append(f"近5日涨{pct_5d}%，动能偏强")
        elif pct_5d <= -5:
            score -= 2
            reasons.append(f"近5日跌{pct_5d}%，动能偏弱")
    if pct_20d is not None:
        if pct_20d >= 15:
            score -= 1  # 过热，回撤风险
            reasons.append(f"近1月已涨{pct_20d}%，追高风险")
        elif pct_20d <= -15:
            score -= 1
            reasons.append(f"近1月跌{pct_20d}%，趋势偏弱")
        elif pct_20d >= 3:
            score += 1
            reasons.append(f"近1月+{pct_20d}%，温和上行")

    # 公告/新闻情绪
    combined = (notices or []) + (news or [])
    neg_hits, neg_examples = _count_keyword_hits(combined, NEGATIVE_KEYWORDS)
    pos_hits, pos_examples = _count_keyword_hits(combined, POSITIVE_KEYWORDS)
    if neg_hits >= 1:
        score -= min(neg_hits, 3)
        reasons.append(f"近期 {neg_hits} 条负面信息（如：{neg_examples[0] if neg_examples else ''}）")
    if pos_hits >= 1:
        score += min(pos_hits, 2)
        reasons.append(f"近期 {pos_hits} 条正面信息（如：{pos_examples[0] if pos_examples else ''}）")

    # 数据缺失 → 降 confidence
    missing = []
    if price.get("error"):
        missing.append("价格")
    if not news:
        missing.append("新闻")
    if not notices:
        missing.append("公告")

    # 映射 verdict
    if score >= 3:
        verdict = "buy"
    elif score >= 1:
        verdict = "hold"
    elif score <= -3:
        verdict = "avoid"
    elif score <= -1:
        verdict = "sell"
    else:
        verdict = "hold"

    # confidence
    if price.get("error") or len(missing) >= 2:
        confidence = "low"
    elif len(missing) == 1 or abs(score) <= 1:
        confidence = "medium"
    else:
        confidence = "high"

    label = name or ticker
    summary = f"{label}（{ticker}）: " + ("；".join(reasons) if reasons else "数据不足，建议观望")
    if missing:
        summary += f"。数据缺失: {', '.join(missing)}"

    return {
        "verdict": verdict,
        "summary": summary,
        "confidence": confidence,
        "score": score,
    }


# ---------------------------------------------------------------------------
# 顶层入口
# ---------------------------------------------------------------------------

def ask_stock(question: str, config: dict | None = None) -> dict[str, Any]:
    """主入口：自然语言问题 → 结构化判断。"""
    _ = config  # 目前未使用，预留
    ticker, name = extract_ticker(question)
    if ticker is None:
        return {
            "ticker_inferred": None,
            "name": None,
            "summary": "无法从问题中识别股票代码或常见名称，请提供 6 位股票代码。",
            "verdict": "avoid",
            "evidence": {},
            "confidence": "low",
            "error": "ticker_not_found",
        }

    price = fetch_price_action(ticker)
    news = fetch_recent_news(ticker)
    notices = fetch_recent_notices(ticker)

    j = judge(question, ticker, name, price, news, notices)

    evidence = {
        "price_action": price,
        "recent_news": news[:5],
        "fundamentals": {
            "notices": notices[:5],
            "notice_count": len(notices),
            "news_count": len(news),
        },
    }
    return {
        "ticker_inferred": ticker,
        "name": name,
        "summary": j["summary"],
        "verdict": j["verdict"],
        "evidence": evidence,
        "confidence": j["confidence"],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ask-stock: 问单票，返回结构化判断")
    parser.add_argument("--question", required=True, help="自然语言问题（如 '宁德时代现在能买吗'）")
    parser.add_argument("--use-llm", action="store_true", help="（预留）是否启用 LLM，默认纯规则")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    result = ask_stock(args.question, config={"use_llm": args.use_llm})
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

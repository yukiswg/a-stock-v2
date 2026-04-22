"""A 股真实性核查模块

调 5 个 akshare 接口，拉取一只股票的上游信号，算综合得分，判定是否进入候选池。

5 个锚点：
  1. 公司公告 (stock_notice_report / stock_news_em)
  2. 股东/高管增减持 (stock_ggcg_em)
  3. 龙虎榜 (stock_lhb_detail_em)
  4. 业绩预告 (stock_yjyg_em)
  5. 机构调研 (stock_jgdy_detail_em)

设计原则：
  - 每个 fetch_* 都整体 try/except，接口挂了不崩。
  - ticker 统一 6 位纯数字字符串输入。
  - 结果结构一致：{"hit": bool, "count": int, "items": [...], "error": str|None}
  - score_truth 可通过 config["truth_thresholds"] 配置阈值。
"""
from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

try:
    import akshare as ak
except ImportError:  # pragma: no cover - 环境缺失时仅在运行时报错
    ak = None  # type: ignore

from watchdog._cache import cached_market_df


logger = logging.getLogger(__name__)

# 默认阈值
DEFAULT_THRESHOLDS = {"candidate": 2, "key_recommend": 3}

# 默认性能开关（可被 config['truth'] 覆盖）
DEFAULT_ENABLE = {
    "enable_announcements": True,
    "enable_shareholder_changes": True,
    "enable_dragon_tiger": True,
    "enable_earnings_forecast": True,
    # 默认关闭：akshare 上游 stock_jgdy_detail_em 在节假日会 TypeError（'NoneType' object is not subscriptable），
    # 且一次要扫 30+ 天全市场，性价比极低。需要时 config 里打开。
    "enable_institutional_research": False,
}

# 内部：全市场级别数据的简易缓存（同一进程多次调用复用）
_CACHE: dict[str, pd.DataFrame] = {}


# ---------------------------------------------------------------------------
# 通用 helper
# ---------------------------------------------------------------------------

def _today() -> datetime:
    return datetime.now()


def _fmt_date(d: datetime) -> str:
    return d.strftime("%Y%m%d")


def _to_iso(val: Any) -> str:
    """pandas/datetime 日期转 ISO str，失败就 str()."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%Y-%m-%d")
        return str(val)
    except Exception:
        return str(val)


def _parse_date(val: Any) -> Optional[datetime]:
    """把各种日期表达转成 datetime；失败返回 None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        if hasattr(val, "year") and hasattr(val, "month"):
            return datetime(val.year, val.month, val.day)
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _filter_by_ticker(df: pd.DataFrame, ticker: str, code_cols: list[str]) -> pd.DataFrame:
    """按 ticker 过滤，兼容 '代码' / '股票代码' 等列名变种."""
    for col in code_cols:
        if col in df.columns:
            return df[df[col].astype(str).str.zfill(6) == ticker]
    return df.iloc[0:0]  # 没有任何 code 列 -> 空


def _filter_by_date_window(
    df: pd.DataFrame, date_col: str, lookback_days: int
) -> pd.DataFrame:
    """保留 date_col >= today - lookback_days 的行."""
    if df.empty or date_col not in df.columns:
        return df
    cutoff = _today() - timedelta(days=lookback_days)
    parsed = df[date_col].apply(_parse_date)
    mask = parsed.apply(lambda x: x is not None and x >= cutoff)
    return df[mask]


def _cached_call(key: str, fn, *args, **kwargs) -> pd.DataFrame:
    """同进程内复用全市场数据，避免每只票都重拉 143k 行."""
    if key in _CACHE:
        return _CACHE[key]
    df = fn(*args, **kwargs)
    _CACHE[key] = df
    return df


def _err_result(msg: str) -> dict:
    return {"hit": False, "count": 0, "items": [], "error": msg}


# ---------------------------------------------------------------------------
# 1. 公司公告
# ---------------------------------------------------------------------------

def fetch_announcements(ticker: str, lookback_days: int = 7) -> dict:
    """近 N 日公司公告。

    用 stock_news_em(symbol=ticker) 单股直查（~0.3s），取代
    stock_notice_report(symbol='全部') 按日全市场扫（每天 85 页 + PDF 下载，7 天 240+s）。
    stock_news_em 覆盖了大部分公司层面的公告/新闻类推送，对 ticker 是直接维度。
    """
    if ak is None:
        return _err_result("akshare 未安装")

    try:
        df = ak.stock_news_em(symbol=ticker)
    except Exception as e:
        return _err_result(f"news_em:{type(e).__name__}:{e}")

    if df is None or df.empty:
        return {"hit": False, "count": 0, "items": [], "error": None}

    sub = _filter_by_date_window(df, "发布时间", lookback_days)
    items: list[dict] = []
    for _, row in sub.iterrows():
        items.append(
            {
                "date": _to_iso(row.get("发布时间")),
                "title": str(row.get("新闻标题", "")),
                "type": "news",
            }
        )

    return {
        "hit": len(items) > 0,
        "count": len(items),
        "items": items,
        "error": None,
    }


# ---------------------------------------------------------------------------
# 2. 股东/高管增减持
# ---------------------------------------------------------------------------

def _fetch_shareholder_per_stock(ticker: str, lookback_days: int) -> Optional[list[dict]]:
    """按所属交易所挨个试 stock_share_hold_change_[sse|szse|bse]，单股直查（~0.6s）。
    成功返回 items 列表；所有接口都失败/空返回 None。
    """
    if ak is None:
        return None

    # 上交所 (60/68/9), 深交所 (00/3), 北交所 (4/8)
    first = ticker[0]
    if first in ("6", "9"):
        order = ("sse", "szse", "bse")
    elif first in ("0", "3"):
        order = ("szse", "sse", "bse")
    else:
        order = ("bse", "sse", "szse")

    fn_map = {
        "sse": getattr(ak, "stock_share_hold_change_sse", None),
        "szse": getattr(ak, "stock_share_hold_change_szse", None),
        "bse": getattr(ak, "stock_share_hold_change_bse", None),
    }

    for market in order:
        fn = fn_map.get(market)
        if fn is None:
            continue
        try:
            df = fn(symbol=ticker)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        # 日期窗口过滤：列名在三家里都叫「变动日期」
        sub = _filter_by_date_window(df, "变动日期", lookback_days)
        items: list[dict] = []
        for _, row in sub.iterrows():
            items.append(
                {
                    "date": _to_iso(row.get("变动日期")),
                    "who": str(row.get("姓名") or row.get("股东名称") or ""),
                    "action": str(row.get("变动原因") or row.get("持股变动信息-增减") or ""),
                    "shares": row.get("变动数量") or row.get("持股变动信息-变动数量"),
                    "ratio_total": row.get("变动比例")
                    or row.get("持股变动信息-占总股本比例"),
                    "start": _to_iso(row.get("变动开始日")),
                    "end": _to_iso(row.get("变动截止日") or row.get("变动日期")),
                }
            )
        return items
    return None


def fetch_shareholder_changes(ticker: str, lookback_days: int = 30) -> dict:
    """近 N 日股东/高管增减持。

    优化优先级：
      1. 首选 stock_share_hold_change_[sse|szse|bse](symbol=ticker) 单股直查（~0.6s）
      2. 失败回退 stock_ggcg_em(symbol='全部')（全市场 14w 行，170s 首次 + 磁盘缓存）

    磁盘缓存：全市场数据按日期 parquet 化，同日跨进程命中秒级。
    """
    if ak is None:
        return _err_result("akshare 未安装")

    # 先试单股直查
    try:
        items = _fetch_shareholder_per_stock(ticker, lookback_days)
    except Exception:
        items = None
    if items is not None:
        return {
            "hit": len(items) > 0,
            "count": len(items),
            "items": items,
            "error": None,
        }

    # 回退全市场扫（首次 170s，之后命中磁盘缓存）
    try:
        today_str = _fmt_date(_today())
        mem_key = f"ggcg_em_全部:{today_str}"
        if mem_key in _CACHE:
            df = _CACHE[mem_key]
        else:
            df = cached_market_df(
                "ggcg_em_全部",
                today_str,
                lambda: ak.stock_ggcg_em(symbol="全部"),
            )
            _CACHE[mem_key] = df
        if df is None or df.empty:
            return {"hit": False, "count": 0, "items": [], "error": None}
        sub = _filter_by_ticker(df, ticker, ["代码", "股票代码"])
        # 按公告日期窗口过滤
        if "公告日" in sub.columns:
            sub = _filter_by_date_window(sub, "公告日", lookback_days)
        items: list[dict] = []
        for _, row in sub.iterrows():
            items.append(
                {
                    "date": _to_iso(row.get("公告日")),
                    "who": str(row.get("股东名称", "")),
                    "action": str(row.get("持股变动信息-增减", "")),
                    "shares": row.get("持股变动信息-变动数量"),
                    "ratio_total": row.get("持股变动信息-占总股本比例"),
                    "start": _to_iso(row.get("变动开始日")),
                    "end": _to_iso(row.get("变动截止日")),
                }
            )
        return {
            "hit": len(items) > 0,
            "count": len(items),
            "items": items,
            "error": None,
        }
    except Exception as e:
        return _err_result(f"{type(e).__name__}:{e}")


# ---------------------------------------------------------------------------
# 3. 龙虎榜
# ---------------------------------------------------------------------------

def fetch_dragon_tiger(ticker: str, lookback_days: int = 7) -> dict:
    """近 N 日龙虎榜上榜情况."""
    if ak is None:
        return _err_result("akshare 未安装")
    try:
        end = _today()
        start = end - timedelta(days=lookback_days)
        cache_key = f"lhb_detail_em:{_fmt_date(start)}:{_fmt_date(end)}"
        df = _cached_call(
            cache_key,
            ak.stock_lhb_detail_em,
            start_date=_fmt_date(start),
            end_date=_fmt_date(end),
        )
        if df is None or df.empty:
            return {"hit": False, "count": 0, "items": [], "error": None}
        sub = _filter_by_ticker(df, ticker, ["代码", "股票代码"])
        items: list[dict] = []
        for _, row in sub.iterrows():
            net_buy_raw = row.get("龙虎榜净买额")
            try:
                net_buy = float(net_buy_raw) if net_buy_raw is not None else None
            except (TypeError, ValueError):
                net_buy = None
            items.append(
                {
                    "date": _to_iso(row.get("上榜日")),
                    "reason": str(row.get("上榜原因", "")),
                    "net_buy": net_buy,
                    "interpret": str(row.get("解读", "")),
                    "change_pct": row.get("涨跌幅"),
                }
            )
        return {
            "hit": len(items) > 0,
            "count": len(items),
            "items": items,
            "error": None,
        }
    except Exception as e:
        return _err_result(f"{type(e).__name__}:{e}")


# ---------------------------------------------------------------------------
# 4. 业绩预告
# ---------------------------------------------------------------------------

def fetch_earnings_forecast(ticker: str, lookback_days: int = 90) -> dict:
    """最近季度业绩预告。

    stock_yjyg_em(date=季度末日期) 返回对应季度的全市场预告。我们扫描近 lookback_days
    内能覆盖到的季度节点，再按 ticker 过滤并按 公告日期 做窗口过滤。
    """
    if ak is None:
        return _err_result("akshare 未安装")

    today = _today()
    # 季度末候选：取最近 ~1 年的 4 个季末 + 当前季末
    quarter_ends: list[str] = []
    years = {today.year, today.year - 1}
    for y in sorted(years):
        for md in ("0331", "0630", "0930", "1231"):
            qs = f"{y}{md}"
            # 只保留 <= 今天 的季度末
            try:
                if datetime.strptime(qs, "%Y%m%d") <= today:
                    quarter_ends.append(qs)
            except ValueError:
                continue
    # 最近优先，只扫最近 2 个季度（省掉一半 API 调用；lookback_days=90 本来就只看近 1 季度）
    quarter_ends = sorted(set(quarter_ends), reverse=True)[:2]

    items: list[dict] = []
    errors: list[str] = []
    today_str = _fmt_date(today)
    for qs in quarter_ends:
        mem_key = f"yjyg_em:{qs}"
        try:
            if mem_key in _CACHE:
                df = _CACHE[mem_key]
            else:
                df = cached_market_df(
                    f"yjyg_em_{qs}",
                    today_str,
                    lambda: ak.stock_yjyg_em(date=qs),
                )
                _CACHE[mem_key] = df
        except Exception as e:
            errors.append(f"{qs}:{type(e).__name__}")
            continue
        if df is None or df.empty:
            continue
        sub = _filter_by_ticker(df, ticker, ["股票代码", "代码"])
        if sub.empty:
            continue
        # 按公告日期窗口过滤
        if "公告日期" in sub.columns:
            sub = _filter_by_date_window(sub, "公告日期", lookback_days)
        for _, row in sub.iterrows():
            items.append(
                {
                    "date": _to_iso(row.get("公告日期")),
                    "quarter": qs,
                    "indicator": str(row.get("预测指标", "")),
                    "change_desc": str(row.get("业绩变动", "")),
                    "change_pct": row.get("业绩变动幅度"),
                    "forecast_type": str(row.get("预告类型", "")),
                }
            )
    err = "; ".join(errors) if errors and not items else None
    return {
        "hit": len(items) > 0,
        "count": len(items),
        "items": items,
        "error": err,
    }


# ---------------------------------------------------------------------------
# 5. 机构调研
# ---------------------------------------------------------------------------

def fetch_institutional_research(ticker: str, lookback_days: int = 14) -> dict:
    """近 N 日机构调研.

    性能备注：stock_jgdy_detail_em 按日查询，一次 30+ 天 = 30+ 次全市场调用，
    且 akshare 上游在周末/节假日会抛 TypeError('NoneType' object is not subscriptable)。
    默认在 score_truth 里通过 config.truth.enable_institutional_research 关闭。
    """
    if ak is None:
        return _err_result("akshare 未安装")

    today = _today()
    items: list[dict] = []
    errors: list[str] = []
    # 按日遍历：jgdy_detail_em 是按日查询；缩窄到 lookback_days + 3 天（周末兜底）
    today_str = _fmt_date(today)
    max_scan = min(lookback_days + 1, 20)
    for delta in range(max_scan):
        day = today - timedelta(days=delta)
        date_str = _fmt_date(day)
        # 周六日直接跳（机构调研本身不在周末产出，且 akshare 上游在节假日会崩）
        if day.weekday() >= 5:
            continue
        mem_key = f"jgdy_detail_em:{date_str}"
        try:
            if mem_key in _CACHE:
                df = _CACHE[mem_key]
            else:
                df = cached_market_df(
                    f"jgdy_detail_em_{date_str}",
                    today_str,
                    lambda: ak.stock_jgdy_detail_em(date=date_str),
                )
                _CACHE[mem_key] = df
        except Exception as e:
            errors.append(f"{date_str}:{type(e).__name__}")
            continue
        if df is None or df.empty:
            continue
        sub = _filter_by_ticker(df, ticker, ["代码", "股票代码"])
        for _, row in sub.iterrows():
            items.append(
                {
                    "date": _to_iso(row.get("调研日期")),
                    "publish_date": _to_iso(row.get("公告日期")),
                    "institution": str(row.get("调研机构", "")),
                    "institution_type": str(row.get("机构类型", "") or ""),
                    "method": str(row.get("接待方式", "")),
                }
            )
    err = "; ".join(errors) if errors and not items else None
    return {
        "hit": len(items) > 0,
        "count": len(items),
        "items": items,
        "error": err,
    }


# ---------------------------------------------------------------------------
# 综合打分
# ---------------------------------------------------------------------------

def _disabled_result() -> dict:
    return {"hit": False, "count": 0, "items": [], "error": "disabled"}


def score_truth(
    ticker: str, name: Optional[str] = None, config: Optional[dict] = None
) -> dict:
    """调 5 个 fetch_* 汇总成分数与判定.

    可由 config['truth'] 覆盖每个源开关（enable_*）与阈值。
    默认：institutional_research 禁用（见 DEFAULT_ENABLE）。
    """
    ticker = str(ticker).zfill(6)
    cfg = config or {}
    # 兼容 truth_thresholds（旧） 与 truth.*（toml 扁平命名）
    thr = {**DEFAULT_THRESHOLDS, **(cfg.get("truth_thresholds") or {})}
    truth_cfg = cfg.get("truth") or {}
    if "candidate_threshold" in truth_cfg:
        thr["candidate"] = truth_cfg["candidate_threshold"]
    if "key_recommend_threshold" in truth_cfg:
        thr["key_recommend"] = truth_cfg["key_recommend_threshold"]

    enable = {**DEFAULT_ENABLE}
    for k in DEFAULT_ENABLE:
        if k in truth_cfg:
            enable[k] = bool(truth_cfg[k])

    evidence = {
        "announcements": fetch_announcements(ticker)
        if enable["enable_announcements"]
        else _disabled_result(),
        "shareholder_changes": fetch_shareholder_changes(ticker)
        if enable["enable_shareholder_changes"]
        else _disabled_result(),
        "dragon_tiger": fetch_dragon_tiger(ticker)
        if enable["enable_dragon_tiger"]
        else _disabled_result(),
        "earnings_forecast": fetch_earnings_forecast(ticker)
        if enable["enable_earnings_forecast"]
        else _disabled_result(),
        "institutional_research": fetch_institutional_research(ticker)
        if enable["enable_institutional_research"]
        else _disabled_result(),
    }

    score = sum(1 for src in evidence.values() if src.get("hit"))

    if score >= thr.get("key_recommend", 3):
        verdict = "🔥 重点推荐"
    elif score >= thr.get("candidate", 2):
        verdict = "✅ 候选"
    else:
        verdict = "❌ 不入选"

    return {
        "ticker": ticker,
        "name": name,
        "score": score,
        "verdict": verdict,
        "evidence": evidence,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _json_default(obj: Any):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    return str(obj)


def main() -> None:
    parser = argparse.ArgumentParser(description="A 股真实性核查")
    parser.add_argument("--ticker", required=True, help="6 位 ticker，如 688256")
    parser.add_argument("--name", default=None, help="可选股票名称")
    parser.add_argument(
        "--log-level", default="WARNING", help="DEBUG/INFO/WARNING/ERROR"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    result = score_truth(args.ticker, name=args.name)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))


if __name__ == "__main__":
    main()

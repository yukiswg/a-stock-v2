"""
A股科技/防守 逃跑预警系统（watchdog 版）

从 skill `vix-rotation/scripts/check_signal.py` 移植而来。
核心算法（RSI Wilder EMA + VIX + 5 日动量 + 中美背离 override）保持不变，
只把股票池从硬编码改成从 config dict 读取，并把入口函数改名为 run_vix_signal。
"""

from __future__ import annotations

import argparse
import json
import logging
import warnings
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


DEFAULT_TICKERS_TECH: dict[str, str] = {
    "688256.SS": "寒武纪",
    "588000.SS": "科创50ETF",
    "513310.SS": "中韩半导体ETF",
    "512480.SS": "半导体ETF",
}

DEFAULT_TICKERS_DEFENSE: dict[str, str] = {
    "515220.SS": "煤炭ETF",
    "601857.SS": "中国石油",
    "601088.SS": "中国神华",
}


def _fetch_history(tickers: dict[str, str]) -> dict[str, pd.Series]:
    """拉取一组 ticker 的 3mo 收盘价；空返回时打 warning，不吞异常。"""
    prices: dict[str, pd.Series] = {}
    for tk, name in tickers.items():
        try:
            hist = yf.Ticker(tk).history(period="3mo")
        except Exception as exc:  # 网络/解析错误
            logger.warning("yfinance history failed for %s (%s): %s", tk, name, exc)
            continue
        if len(hist) == 0:
            logger.warning("yfinance returned empty history for %s (%s)", tk, name)
            continue
        prices[name] = hist["Close"]
    return prices


def _strip_tz(obj: pd.DataFrame | pd.Series) -> None:
    """原地去掉 DatetimeIndex 的时区信息（与源脚本行为一致）。"""
    if hasattr(obj.index, "tz") and obj.index.tz is not None:
        obj.index = obj.index.tz_localize(None)


def run_vix_signal(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    计算 A 股科技/防守逃跑信号。

    Parameters
    ----------
    config : dict, optional
        形如::

            {
              "tickers_tech":    {"688256.SS": "寒武纪", ...},
              "tickers_defense": {"515220.SS": "煤炭ETF", ...}
            }

        字段缺失或 config 为 None 时使用内置默认股票池（与原 check_signal.py 一致）。

    Returns
    -------
    dict
        与原 check_signal.py 的 result 结构严格一致：
        date / status / advice / tech_pct / red_count / override_divergence /
        signals.{rsi,vix,momentum} / vix_trend / tech_prices / defense_prices。
        数据不足时返回 ``{"error": "数据不足，请检查网络"}``。
    """
    cfg = config or {}
    tickers_tech = cfg.get("tickers_tech") or DEFAULT_TICKERS_TECH
    tickers_defense = cfg.get("tickers_defense") or DEFAULT_TICKERS_DEFENSE

    # ===== 1. 拉数据 =====
    tech_prices = _fetch_history(tickers_tech)
    def_prices = _fetch_history(tickers_defense)

    try:
        vix_ticker = yf.Ticker("^VIX")
        vix_hist = vix_ticker.history(period="3mo")["Close"]
    except Exception as exc:
        logger.warning("yfinance history failed for ^VIX: %s", exc)
        return {"error": "数据不足，请检查网络"}

    # 实时 VIX（fast_info 可能失败，属于正常降级路径）
    realtime_vix: float | None = None
    try:
        fi = vix_ticker.fast_info
        realtime_vix = float(fi.last_price)
    except Exception as exc:
        logger.debug("VIX fast_info unavailable, falling back to last close: %s", exc)

    if not tech_prices or not def_prices or len(vix_hist) < 20:
        return {"error": "数据不足，请检查网络"}

    tech_df = pd.DataFrame(tech_prices).ffill().dropna()
    def_df = pd.DataFrame(def_prices).ffill().dropna()

    # 去时区（源脚本行为）
    _strip_tz(tech_df)
    _strip_tz(def_df)
    if isinstance(vix_hist, pd.Series):
        _strip_tz(vix_hist)

    if tech_df.empty or def_df.empty:
        return {"error": "数据不足，请检查网络"}

    # 等权组合
    tech_ret = tech_df.pct_change().fillna(0).mean(axis=1)
    tech_nav = (1 + tech_ret).cumprod()

    # ===== 2. 信号1：RSI(7天, Wilder EMA 标准算法) =====
    delta = tech_nav.diff().fillna(0)
    up = delta.clip(lower=0)
    dn = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1 / 7, adjust=False).mean()
    roll_dn = dn.ewm(alpha=1 / 7, adjust=False).mean()
    rs = roll_up / roll_dn.replace(0, np.nan)
    rsi = (100 - 100 / (1 + rs)).fillna(100.0)
    current_rsi = float(rsi.iloc[-1])

    if current_rsi > 70:
        rsi_signal = "red"
        rsi_text = f"RSI={current_rsi:.1f} > 70,超买"
    elif current_rsi < 30:
        rsi_signal = "green"
        rsi_text = f"RSI={current_rsi:.1f} < 30,超卖"
    else:
        rsi_signal = "yellow"
        rsi_text = f"RSI={current_rsi:.1f},正常区间"

    # ===== 3. 信号2：VIX =====
    vix_aligned = vix_hist.reindex(tech_df.index, method="ffill").dropna()
    if vix_aligned.empty:
        return {"error": "数据不足，请检查网络"}
    current_vix = realtime_vix if realtime_vix else float(vix_aligned.iloc[-1])
    vix_ma20 = float(vix_aligned.rolling(20).mean().iloc[-1])

    if current_vix > 25 and current_vix > vix_ma20:
        vix_signal = "red"
        vix_text = f"VIX={current_vix:.1f} > 25 且高于均线({vix_ma20:.1f}),恐慌"
    elif current_vix < 20 and current_vix < vix_ma20:
        vix_signal = "green"
        vix_text = f"VIX={current_vix:.1f} < 20 且低于均线({vix_ma20:.1f}),平静"
    else:
        vix_signal = "yellow"
        vix_text = f"VIX={current_vix:.1f},均线={vix_ma20:.1f},中间地带"

    # ===== 4. 信号3：A 股科技 5 日动量 =====
    tech_mom_5d = float(tech_nav.pct_change(5).iloc[-1])

    if tech_mom_5d < -0.03:
        mom_signal = "red"
        mom_text = f"科技5日动量={tech_mom_5d:+.2%},动量转弱"
    elif tech_mom_5d > 0.03:
        mom_signal = "green"
        mom_text = f"科技5日动量={tech_mom_5d:+.2%},强势"
    else:
        mom_signal = "yellow"
        mom_text = f"科技5日动量={tech_mom_5d:+.2%},中性"

    # ===== 5. 综合判断 =====
    signals_list = [rsi_signal, vix_signal, mom_signal]
    red_count = signals_list.count("red")

    # 中美背离 override：A 股科技强势时不跑
    override = mom_signal == "green" and vix_signal == "red"

    if override:
        status = "留意"
        advice = "VIX高但A股科技强势(可能中美背离),暂不动,密切关注"
        tech_pct = 70
    elif red_count >= 3:
        status = "逃跑"
        advice = "三灯全红!建议全面转防守"
        tech_pct = 0
    elif red_count >= 2:
        status = "减仓"
        advice = "两灯红,建议科技减至50%,加防守50%"
        tech_pct = 50
    elif red_count == 1:
        status = "留意"
        advice = "一灯红,暂不动,保持关注"
        tech_pct = 80
    else:
        status = "安全"
        advice = "信号正常,安心持有科技"
        tech_pct = 100

    recent_vix = vix_aligned.tail(5)
    vix_trend = [
        {"date": d.strftime("%Y-%m-%d"), "vix": round(float(v), 2)}
        for d, v in recent_vix.items()
    ]

    tech_latest = {name: round(float(tech_df[name].iloc[-1]), 2) for name in tech_df.columns}
    def_latest = {name: round(float(def_df[name].iloc[-1]), 2) for name in def_df.columns}

    return {
        "date": tech_df.index[-1].strftime("%Y-%m-%d"),
        "status": status,
        "advice": advice,
        "tech_pct": tech_pct,
        "red_count": red_count,
        "override_divergence": override,
        "signals": {
            "rsi": {
                "light": rsi_signal,
                "detail": rsi_text,
                "value": round(current_rsi, 1),
            },
            "vix": {
                "light": vix_signal,
                "detail": vix_text,
                "value": round(current_vix, 2),
                "ma20": round(vix_ma20, 2),
            },
            "momentum": {
                "light": mom_signal,
                "detail": mom_text,
                "value": f"{tech_mom_5d:+.2%}",
            },
        },
        "vix_trend": vix_trend,
        "tech_prices": tech_latest,
        "defense_prices": def_latest,
    }


def _load_config(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="watchdog.vix",
        description="A股科技/防守 逃跑预警信号",
    )
    parser.add_argument(
        "--config",
        help="可选 JSON 配置文件，含 tickers_tech / tickers_defense 两个 dict",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        help="logging level (DEBUG/INFO/WARNING/ERROR)，默认 WARNING",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(levelname)s %(name)s: %(message)s",
    )

    config = _load_config(args.config)
    result = run_vix_signal(config)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

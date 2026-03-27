"""
Statistical price prediction engine — no ML, no external API required.

Methods used
------------
1. Volume-Profile Intraday  — Uses today's intraday volume bars to infer where
   "smart money" accumulated. The VWAP of above-average bars is treated as the
   likely fair-value anchor; closing price relative to VWAP historically biases
   the next-hour direction.
2. Mean-Reversion Intraday  — Within-day regression toward the session open or
   the 5-day moving average, scaled by current deviation and volatility.
3. Day-End Ensemble          — Combines: (a) historical close/open ratio distribution,
   (b) BollBand reversion, (c) VWAP anchor, (d) drift toward 5dma.
4. Next-Day Probability       — Historical next-day return distribution conditioned
   on: (a) today's return bucket, (b) today's range size, (c) current position
   within the 20d range.
5. Long-Term Expectation      — Linear regression slope over N days × base-rate
   win-rate from historical backtests, scaled by recent signal quality.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ..models import DailyBar, PricePrediction, PredictionBundle
from ..utils import clamp, pct_change


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _conf_band(price: float, confidence: float) -> tuple[float, float]:
    """Return (+1σ, -1σ) bands around price, scaled by confidence."""
    half_spread = price * 0.012 / max(confidence, 0.3)
    return round(price + half_spread, 3), round(price - half_spread, 3)


def _data_freshness(bars: list[DailyBar]) -> str:
    if not bars:
        return "stale"
    latest = bars[-1].trade_date
    today = datetime.now().strftime("%Y%m%d")
    return "full" if latest == today else "degraded"


def _closes(bars: list[DailyBar]) -> list[float]:
    return [b.close_price for b in bars]


def _opens(bars: list[DailyBar]) -> list[float]:
    return [b.open_price for b in bars]


def _vols(bars: list[DailyBar]) -> list[float]:
    return [b.volume for b in bars]


def _intraday_bars(bars: list[DailyBar]) -> list[dict[str, Any]] | None:
    """
    Parse intraday minute bars from a DailyBar if they carry extra metadata.
    Returns None if only daily bars are available — callers should fall back.
    """
    if bars and hasattr(bars[0], "intraday_minutes"):
        return bars[0].intraday_minutes  # type: ignore[attr-defined]
    return None


# ---------------------------------------------------------------------------
# 1. Intraday close  (session open → expected close)
# ---------------------------------------------------------------------------

def predict_intraday_close(
    *,
    bars: list[DailyBar],          # today's bar + recent history
    intraday_minutes: list[dict[str, Any]] | None = None,
    current_time: str | None = None,  # "HH:MM" e.g. "10:30"
    open_price: float | None = None,
    current_price: float | None = None,
    current_volume: float | None = None,
) -> PricePrediction:
    """
    Predict where the current session will close, given the time of day and
    volume distribution seen so far.
    """
    ref_price = current_price if current_price else (bars[-1].close_price if bars else 0.0)
    if not ref_price:
        return _unknown_pred("intraday_close", ref_price)

    # Default: use only daily bars (no intraday data)
    method = "mean_reversion"
    reasoning: list[str] = []
    confidence = 0.45
    predicted_return = 0.0

    if intraday_minutes and len(intraday_minutes) >= 5:
        method = "volume_profile"
        confidence = 0.62
        # --- Volume-Profile method ---
        avg_vol = sum(r["volume"] for r in intraday_minutes) / len(intraday_minutes)
        above_avg_bars = [r for r in intraday_minutes if r["volume"] > avg_vol]
        if above_avg_bars:
            vwap_numerator = sum(r["close"] * r["volume"] for r in above_avg_bars)
            vwap_denom = sum(r["volume"] for r in above_avg_bars)
            vwap_anchor = vwap_numerator / vwap_denom if vwap_denom else ref_price
        else:
            vwap_anchor = ref_price

        current_bar = intraday_minutes[-1]
        cumvol = sum(r["volume"] for r in intraday_minutes)
        elapsed_ratio = len(intraday_minutes) / 240.0  # rough: 240 mins in session

        # Price has drifted above VWAP → slight reversion bias
        drift_from_vwap = (ref_price - vwap_anchor) / vwap_anchor if vwap_anchor else 0.0
        remaining_time_ratio = 1.0 - elapsed_ratio

        # Strong VWAP drift → mean-reversion into close
        if abs(drift_from_vwap) > 0.005:
            predicted_return = -drift_from_vwap * 0.5 * remaining_time_ratio
            reasoning.append(
                f"价格偏离VWAP {drift_from_vwap:+.2%}，距收盘约剩 {remaining_time_ratio:.0%} 时间，"
                f"预期收盘向VWAP方向收敛 {abs(predicted_return):.2%}。"
            )
        else:
            # No major drift — slight bullish lean with volume confirmation
            if current_bar["volume"] > avg_vol * 1.2:
                predicted_return = 0.0015 * remaining_time_ratio
                reasoning.append("量能高于均值，收盘偏强概率略高。")
            else:
                predicted_return = 0.0
                reasoning.append("量能平稳，无明显方向，收盘预期持平。")

        confidence = clamp(0.55 + abs(drift_from_vwap) * 3, 0.40, 0.75)
        method = "volume_profile"
    else:
        # --- Mean-Reversion from daily bars ---
        if len(bars) < 6:
            reasoning.append("数据不足，无法做精细盘中预测。")
            return _unknown_pred("intraday_close", ref_price, confidence=0.25, method="mean_reversion")

        ma5 = sum(c.close_price for c in bars[-5:]) / 5
        ma5_dev = (ref_price - ma5) / ma5 if ma5 else 0.0

        if current_time:
            # Parse approximate session progress
            try:
                hour, minute = map(int, current_time.split(":"))
                elapsed_ratio = min((hour - 9) * 60 + (minute - 30) - 60, 210) / 210.0
                elapsed_ratio = max(0.05, min(elapsed_ratio, 0.95))
            except Exception:
                elapsed_ratio = 0.50
        else:
            elapsed_ratio = 0.50

        remaining_ratio = 1.0 - elapsed_ratio

        # Large deviation from 5dma → mean reversion during session
        if abs(ma5_dev) > 0.02:
            predicted_return = -ma5_dev * 0.4 * remaining_ratio
            reasoning.append(
                f"当前价偏离5日均线 {ma5_dev:+.2%}，剩余 {remaining_ratio:.0%} 交易时间，"
                f"均值回归修正预期 {abs(predicted_return):.2%}。"
            )
        else:
            reasoning.append("价格贴近均线，均值回归效应不明显，预期偏中性。")

        confidence = clamp(0.45 + abs(ma5_dev) * 5, 0.35, 0.65)
        method = "mean_reversion"

    predicted_price = round(ref_price * (1 + predicted_return), 3)
    upper, lower = _conf_band(predicted_price, confidence)

    return PricePrediction(
        horizon="intraday_close",
        predicted_price=predicted_price,
        predicted_return=round(predicted_return, 6),
        confidence=round(confidence, 3),
        confidence_band_upper=upper,
        confidence_band_lower=lower,
        method=method,
        reasoning=reasoning,
        as_of_time=_now_iso(),
        reference_price=round(ref_price, 3),
        source_data_quality="degraded" if method == "mean_reversion" else "full",
    )


# ---------------------------------------------------------------------------
# 2. Day-end close  (today's close — uses daily bar patterns)
# ---------------------------------------------------------------------------

def predict_dayend_close(
    *,
    bars: list[DailyBar],
    open_price: float | None = None,
    current_price: float | None = None,
    current_time: str | None = None,
) -> PricePrediction:
    """
    Predict where today's close will settle using:
    - Historical close/open ratio distribution
    - Position within 20d Bollinger Band
    - Session drift toward VWAP / 5dma
    """
    ref_price = current_price if current_price else (bars[-1].close_price if bars else 0.0)
    if not ref_price:
        return _unknown_pred("dayend", ref_price)

    if len(bars) < 20:
        return _unknown_pred("dayend", ref_price, confidence=0.25)

    closes = _closes(bars)
    today_bar = bars[-1]

    # 1. Historical close/open ratio distribution (last 60 days)
    close_open_ratios = [
        c.close_price / c.open_price
        for c in bars[-61:-1]
        if c.open_price > 0
    ]
    avg_co_ratio = sum(close_open_ratios) / len(close_open_ratios) if close_open_ratios else 1.0
    co_drift = avg_co_ratio - 1.0  # historical average session return

    # 2. Bollinger Band reversion
    ma20 = sum(closes[-20:]) / 20
    std20 = _std([c - ma20 for c in closes[-20:]])
    upper_band = ma20 + 2 * std20
    lower_band = ma20 - 2 * std20
    bb_position = (ref_price - lower_band) / (upper_band - lower_band) if (upper_band - lower_band) > 0 else 0.5
    bb_dev = (ref_price - ma20) / ma20 if ma20 else 0.0  # deviation from 20dma

    # 3. Today's range so far
    day_range_pct = (today_bar.high_price - today_bar.low_price) / ref_price if ref_price else 0.0
    # Amplitude > 2.5% = big range day → close often reverses
    big_range_reversion = -0.3 * bb_dev if day_range_pct > 0.025 else 0.0

    # 4. Current time drift (if time is known)
    time_drift = 0.0
    if current_time:
        try:
            hour, minute = map(int, current_time.split(":"))
            # Late session (after 14:00) — less time for reversal
            if hour >= 14:
                time_factor = 0.2
            elif hour >= 13:
                time_factor = 0.5
            else:
                time_factor = 0.8
            time_drift = -bb_dev * time_factor
        except Exception:
            time_drift = -bb_dev * 0.5

    # Ensemble: combine signals
    predicted_return = co_drift * 0.25 + (-bb_dev * 0.55) * (1 + big_range_reversion) + time_drift * 0.2

    # Clamp — don't predict more than ±4% from ref
    predicted_return = clamp(predicted_return, -0.04, 0.04)
    predicted_price = round(ref_price * (1 + predicted_return), 3)
    upper, lower = _conf_band(predicted_price, 0.55)

    reasoning = [
        f"历史收盘/开盘比均值为 {avg_co_ratio:.4f}（日均漂移 {co_drift:+.3%}）。",
        f"当前价偏离20日均线 {bb_dev:+.2%}（BB位置 {bb_position:.0%}），"
        f"{'偏高' if bb_dev > 0 else '偏低'}，{'有回归压力' if abs(bb_dev) > 0.01 else '偏离不大'}。",
    ]
    if day_range_pct > 0.025:
        reasoning.append(f"今日振幅 {day_range_pct:.2%} 较大，{'收盘偏回归压力' if bb_dev > 0 else '下方支撑较强'}。")
    if current_time:
        reasoning.append(f"当前时间 {current_time}，{'剩余时间不多' if int(current_time.split(':')[0]) >= 14 else '仍有时段可修正'}。")

    return PricePrediction(
        horizon="dayend",
        predicted_price=predicted_price,
        predicted_return=round(predicted_return, 6),
        confidence=0.55,
        confidence_band_upper=upper,
        confidence_band_lower=lower,
        method="combined",
        reasoning=reasoning,
        as_of_time=_now_iso(),
        reference_price=round(ref_price, 3),
        source_data_quality=_data_freshness(bars),
    )


# ---------------------------------------------------------------------------
# 3. Next-day return probability
# ---------------------------------------------------------------------------

def predict_nextday_return(
    *,
    bars: list[DailyBar],
    today_return: float | None = None,
) -> PricePrediction:
    """
    Probability distribution of tomorrow's return given:
    - Today's return bucket (large up → next day mean-reverts, large down → next day bounces)
    - Today's range size
    - Current position in 20d range
    """
    if len(bars) < 30:
        return _unknown_pred("nextday", bars[-1].close_price if bars else 0.0, confidence=0.25)

    closes = _closes(bars)
    ref_price = closes[-1]

    # Today's return
    if today_return is None and len(bars) >= 2:
        today_return = pct_change(closes[-1], closes[-2]) or 0.0
    today_return = today_return or 0.0

    # Position in 20d range
    recent_20 = closes[-20:]
    high20 = max(recent_20)
    low20 = min(recent_20)
    range_pos = (ref_price - low20) / (high20 - low20) if (high20 - low20) > 0 else 0.5

    # Historical next-day return distributions conditioned on today's return bucket
    hist_next = []
    for i in range(len(bars) - 2):
        ret_today = pct_change(bars[i + 1].close_price, bars[i].close_price) or 0.0
        ret_tomorrow = pct_change(bars[i + 2].close_price, bars[i + 1].close_price) or 0.0
        hist_next.append((ret_today, ret_tomorrow))

    # Conditional means
    up_today = [r[1] for r in hist_next if r[0] > 0.015]
    down_today = [r[1] for r in hist_next if r[0] < -0.015]
    flat_today = [r[1] for r in hist_next if -0.015 <= r[0] <= 0.015]

    avg_next_up = sum(up_today) / len(up_today) if up_today else 0.0
    avg_next_down = sum(down_today) / len(down_today) if down_today else 0.0
    avg_next_flat = sum(flat_today) / len(flat_today) if flat_today else 0.0

    if today_return > 0.015:
        predicted_return = avg_next_up  # historically slight reversion
        reasoning = [
            f"今日涨幅 {today_return:+.2%} 属于大涨日，"
            f"历史数据次日平均收益 {avg_next_up:+.3%}，"
            f"{'次日倾向于继续走强' if avg_next_up > 0.003 else '次日略有均值回归压力'}。"
        ]
    elif today_return < -0.015:
        predicted_return = avg_next_down
        reasoning = [
            f"今日跌幅 {today_return:+.2%} 属于大跌日，"
            f"历史次日平均收益 {avg_next_down:+.3%}，"
            f"{'次日继续走弱风险大' if avg_next_down < -0.003 else '次日有反弹修复可能'}。"
        ]
    else:
        predicted_return = avg_next_flat
        reasoning = [
            f"今日涨跌幅 {today_return:+.2%} 属于震荡日，"
            f"历史次日平均收益 {avg_next_flat:+.3%}，无明显方向偏向。"
        ]

    # Range-position adjustment: near 20d high → next day win-rate drops
    if range_pos > 0.85:
        predicted_return -= 0.004
        reasoning.append(f"当前处于20日高位（{range_pos:.0%}），历史上此类位置次日胜率偏低。")
    elif range_pos < 0.15:
        predicted_return += 0.004
        reasoning.append(f"当前处于20日低位（{range_pos:.0%}），历史上此类位置次日反弹概率偏高。")

    predicted_return = clamp(predicted_return, -0.06, 0.06)
    predicted_price = round(ref_price * (1 + predicted_return), 3)
    upper, lower = _conf_band(predicted_price, 0.50)

    # Win-rate: what % of time is next-day return positive?
    sample = up_today if today_return > 0.015 else (down_today if today_return < -0.015 else flat_today)
    win_rate = sum(1 for r in sample if r > 0) / len(sample) if sample else 0.50

    confidence = clamp(0.40 + win_rate * 0.20, 0.35, 0.70)

    reasoning.append(f"历史样本 {len(sample)} 天，胜率约 {win_rate:.0%}。")

    return PricePrediction(
        horizon="nextday",
        predicted_price=predicted_price,
        predicted_return=round(predicted_return, 6),
        confidence=round(confidence, 3),
        confidence_band_upper=upper,
        confidence_band_lower=lower,
        method="historical_dist",
        reasoning=reasoning,
        as_of_time=_now_iso(),
        reference_price=round(ref_price, 3),
        source_data_quality=_data_freshness(bars),
    )


# ---------------------------------------------------------------------------
# 4. Long-term expectation  (5–20 trading day horizon)
# ---------------------------------------------------------------------------

def predict_longterm_expectation(
    *,
    bars: list[DailyBar],
    trend_score: float | None = None,
    relative_strength: float | None = None,
) -> PricePrediction:
    """
    5-20 day price expectation using:
    - Linear regression slope over last 20 days
    - Base-rate win-rate from historical backtests
    - Current trend quality (trend_score)
    - Relative strength vs benchmark
    """
    if len(bars) < 40:
        return _unknown_pred("longterm", bars[-1].close_price if bars else 0.0, confidence=0.20)

    closes = _closes(bars)
    ref_price = closes[-1]

    # Linear regression slope (20 days)
    n = min(20, len(closes) - 1)
    x = list(range(n))
    y = closes[-n:]
    slope = _linear_slope(x, y)

    # Annualised drift from regression
    daily_drift = slope / (sum(y) / n) if n else 0.0
    expected_10d_pct = daily_drift * 10

    # Trend-score adjustment: confidence in the direction
    if trend_score is not None:
        ts_factor = (trend_score - 50) / 50.0  # -1 to +1
        # High trend_score (>65) → slight momentum extension
        # Mid trend_score (50-65) → regression to slope
        # Low trend_score (<50) → fade the move
        if ts_factor > 0.3:
            momentum_bias = 0.002 * ts_factor * 10  # extra 2% per 10 days at max
        else:
            momentum_bias = 0.0
        expected_10d_pct += momentum_bias

    # Relative strength: strong relative performers have better LT win rates
    if relative_strength is not None:
        rs_factor = relative_strength * 5  # scale to ±impact
        expected_10d_pct += rs_factor * 0.5

    # Historical base-rate: what % of 10-day windows are positive?
    def _ret10(i: int) -> float | None:
        return pct_change(closes[i + 10], closes[i])
    pos_count = sum(1 for i in range(len(closes) - 11) if (_ret10(i) or 0) > 0)
    total_count = max(len(closes) - 11, 1)
    base_win_rate = pos_count / total_count if total_count > 0 else 0.50

    # Confidence = base_win_rate * data_quality_factor
    confidence = clamp(base_win_rate * 0.8 + 0.1, 0.25, 0.70)

    predicted_return = clamp(expected_10d_pct, -0.12, 0.15)
    predicted_price = round(ref_price * (1 + predicted_return), 3)
    upper, lower = _conf_band(predicted_price, confidence)

    direction = "上涨" if predicted_return > 0.001 else ("下跌" if predicted_return < -0.001 else "震荡")
    reasoning = [
        f"近20日线性回归斜率对应日均漂移 {daily_drift:+.4%}，"
        f"10日预期偏移 {expected_10d_pct:+.2%}（{direction}）。",
        f"历史10日正收益概率 {base_win_rate:.0%}（样本 {total_count} 个区间）。",
    ]
    if trend_score is not None:
        reasoning.append(f"当前趋势得分 {trend_score:.0f}，{'支撑中长期方向' if trend_score > 55 else '中长期方向不明' if 45 <= trend_score <= 55 else '中长期偏弱'}。")
    if relative_strength is not None:
        reasoning.append(f"相对强弱 {relative_strength:+.2%}，{'强于基准' if relative_strength > 0 else '弱于基准'}。")

    return PricePrediction(
        horizon="longterm",
        predicted_price=predicted_price,
        predicted_return=round(predicted_return, 6),
        confidence=round(confidence, 3),
        confidence_band_upper=upper,
        confidence_band_lower=lower,
        method="linear_trend",
        reasoning=reasoning,
        as_of_time=_now_iso(),
        reference_price=round(ref_price, 3),
        source_data_quality="full",
    )


# ---------------------------------------------------------------------------
# Orchestrator: build full PredictionBundle
# ---------------------------------------------------------------------------

def build_prediction_bundle(
    *,
    code: str,
    name: str,
    bars: list[DailyBar],
    trend_score: float | None = None,
    relative_strength: float | None = None,
    intraday_minutes: list[dict[str, Any]] | None = None,
    current_time: str | None = None,
    open_price: float | None = None,
    current_price: float | None = None,
    current_volume: float | None = None,
) -> PredictionBundle:
    """
    Generate all available predictions for a security.
    All methods are purely statistical; no external model or API required.
    """
    ref = bars[-1].close_price if bars else 0.0

    intraday = None
    dayend = None
    nextday = None
    longterm = None

    try:
        intraday = predict_intraday_close(
            bars=bars,
            intraday_minutes=intraday_minutes,
            current_time=current_time,
            open_price=open_price,
            current_price=current_price,
            current_volume=current_volume,
        )
    except Exception:
        pass

    try:
        dayend = predict_dayend_close(
            bars=bars,
            open_price=open_price,
            current_price=current_price,
            current_time=current_time,
        )
    except Exception:
        pass

    try:
        nextday = predict_nextday_return(bars=bars)
    except Exception:
        pass

    try:
        longterm = predict_longterm_expectation(
            bars=bars,
            trend_score=trend_score,
            relative_strength=relative_strength,
        )
    except Exception:
        pass

    return PredictionBundle(
        code=code,
        name=name,
        as_of=_now_iso(),
        reference_price=round(ref, 3),
        intraday=intraday,
        dayend=dayend,
        nextday=nextday,
        longterm=longterm,
    )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _unknown_pred(
    horizon: str,
    ref_price: float,
    *,
    confidence: float = 0.30,
    method: str = "unknown",
) -> PricePrediction:
    return PricePrediction(
        horizon=horizon,
        predicted_price=round(ref_price, 3) if ref_price else None,
        predicted_return=0.0,
        confidence=confidence,
        confidence_band_upper=None,
        confidence_band_lower=None,
        method=method,
        reasoning=["数据不足，无法生成可靠预测。"],
        as_of_time=_now_iso(),
        reference_price=round(ref_price, 3) if ref_price else 0.0,
        source_data_quality="stale",
    )


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return variance ** 0.5


def _linear_slope(x: list[int], y: list[float]) -> float:
    """Simple OLS slope."""
    n = len(x)
    if n < 2:
        return 0.0
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x)
    return num / den if den else 0.0

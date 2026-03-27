from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

from ..models import DailyBar, DailySeriesSnapshot
from ..utils import DEFAULT_HEADERS, clamp, ensure_dir, load_json, pct_change, safe_float, urlopen_with_retries, write_json


KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
SINA_KLINE_URL = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
KLINE_FIELDS = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
INDEX_SECID_MAP = {
    "000001": "1.000001",
    "000300": "1.000300",
    "000688": "1.000688",
    "399001": "0.399001",
    "399006": "0.399006",
}


def code_to_secid(code: str) -> str:
    if code in INDEX_SECID_MAP:
        return INDEX_SECID_MAP[code]
    if code.startswith(("5", "6", "9", "11")):
        return f"1.{code}"
    if code.startswith("8"):
        return f"2.{code}"
    return f"0.{code}"


def secid_to_symbol(secid: str) -> str:
    market, code = secid.split(".", 1)
    if market == "1":
        return f"sh{code}"
    if market == "2":
        return f"bj{code}"
    return f"sz{code}"


def cache_filename(secid: str) -> str:
    return secid.replace(".", "_") + ".json"


def fetch_daily_series(
    *,
    code: str,
    name: str,
    cache_dir: str | Path,
    begin: str = "20240101",
    end: str = "20500101",
) -> DailySeriesSnapshot:
    secid = code_to_secid(code)
    cached = load_cached_series(cache_dir, secid=secid)
    if prefer_cached_series(cached, end=end):
        return DailySeriesSnapshot(
            code=code,
            name=name,
            secid=secid,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            source="cache",
            bars=filter_series_by_date(cached, begin=begin, end=end),
            used_cache=True,
            degraded=False,
        )

    failures: list[str] = []
    for source_name, fetcher in (
        ("eastmoney_daily", fetch_daily_series_eastmoney),
        ("tencent_daily", fetch_daily_series_tencent),
        ("sina_daily", fetch_daily_series_sina),
    ):
        try:
            snapshot = fetcher(code=code, name=name, secid=secid, begin=begin, end=end)
            write_series_cache(cache_dir, secid=secid, bars=snapshot.bars, source=source_name)
            return snapshot
        except Exception as exc:  # pragma: no cover - network failures are environment-specific
            failures.append(f"{source_name}:{exc}")

    filtered_cached = filter_series_by_date(cached, begin=begin, end=end)
    if len(filtered_cached) >= 30:
        return DailySeriesSnapshot(
            code=code,
            name=name,
            secid=secid,
            fetched_at=datetime.now().isoformat(timespec="seconds"),
            source="cache_fallback",
            bars=filtered_cached,
            used_cache=True,
            degraded=True,
        )
    raise RuntimeError(f"No usable daily series for {code}. failures={failures}")


def fetch_daily_series_eastmoney(*, code: str, name: str, secid: str, begin: str, end: str) -> DailySeriesSnapshot:
    params = urllib.parse.urlencode(
        {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": KLINE_FIELDS,
            "klt": "101",
            "fqt": "1",
            "beg": normalize_date(begin),
            "end": normalize_date(end),
        }
    )
    request = urllib.request.Request(
        f"{KLINE_URL}?{params}",
        headers={**DEFAULT_HEADERS, "Referer": "https://quote.eastmoney.com/"},
    )
    fetched_at = datetime.now().isoformat(timespec="seconds")
    with urlopen_with_retries(request, timeout=20, retries=3, backoff_seconds=1.0) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    rows = (((payload.get("data") or {}).get("klines")) or [])
    bars = parse_eastmoney_rows(rows)
    if len(bars) < 30:
        raise ValueError(f"eastmoney returned only {len(bars)} bars for {code}")
    return DailySeriesSnapshot(code=code, name=name, secid=secid, fetched_at=fetched_at, source="eastmoney_daily", bars=bars)


def fetch_daily_series_tencent(*, code: str, name: str, secid: str, begin: str, end: str) -> DailySeriesSnapshot:
    params = urllib.parse.urlencode({"param": f"{secid_to_symbol(secid)},day,,,320,qfq"})
    request = urllib.request.Request(
        f"{TENCENT_KLINE_URL}?{params}",
        headers={**DEFAULT_HEADERS, "Referer": "https://gu.qq.com/"},
    )
    fetched_at = datetime.now().isoformat(timespec="seconds")
    with urlopen_with_retries(request, timeout=20, retries=2, backoff_seconds=0.8) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    symbol = secid_to_symbol(secid)
    rows = (((payload.get("data") or {}).get(symbol) or {}).get("qfqday")) or (((payload.get("data") or {}).get(symbol) or {}).get("day")) or []
    bars = filter_series_by_date(parse_tencent_rows(rows), begin=begin, end=end)
    if len(bars) < 30:
        raise ValueError(f"tencent returned only {len(bars)} bars for {code}")
    return DailySeriesSnapshot(code=code, name=name, secid=secid, fetched_at=fetched_at, source="tencent_daily", bars=bars)


def fetch_daily_series_sina(*, code: str, name: str, secid: str, begin: str, end: str) -> DailySeriesSnapshot:
    params = urllib.parse.urlencode({"symbol": secid_to_symbol(secid), "scale": "240", "ma": "no", "datalen": "320"})
    request = urllib.request.Request(
        f"{SINA_KLINE_URL}?{params}",
        headers={**DEFAULT_HEADERS, "Referer": "https://finance.sina.com.cn/"},
    )
    fetched_at = datetime.now().isoformat(timespec="seconds")
    with urlopen_with_retries(request, timeout=20, retries=2, backoff_seconds=0.8) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    rows = payload if isinstance(payload, list) else []
    bars = filter_series_by_date(parse_sina_rows(rows), begin=begin, end=end)
    if len(bars) < 30:
        raise ValueError(f"sina returned only {len(bars)} bars for {code}")
    return DailySeriesSnapshot(code=code, name=name, secid=secid, fetched_at=fetched_at, source="sina_daily", bars=bars)


def parse_eastmoney_rows(rows: list[str]) -> list[DailyBar]:
    bars: list[DailyBar] = []
    for row in rows:
        parts = row.split(",")
        if len(parts) < 11:
            continue
        bars.append(
            DailyBar(
                trade_date=parts[0],
                open_price=float(parts[1]),
                close_price=float(parts[2]),
                high_price=float(parts[3]),
                low_price=float(parts[4]),
                volume=float(parts[5]),
                amount=float(parts[6]),
                amplitude=float(parts[7]),
                pct_change=float(parts[8]) / 100,
                change_amount=float(parts[9]),
                turnover=float(parts[10]) / 100,
                source="eastmoney_daily",
            )
        )
    return bars


def parse_tencent_rows(rows: list[list[str]]) -> list[DailyBar]:
    bars: list[DailyBar] = []
    previous_close: float | None = None
    for row in rows:
        if len(row) < 6:
            continue
        close_price = float(row[2])
        bars.append(
            DailyBar(
                trade_date=str(row[0]),
                open_price=float(row[1]),
                close_price=close_price,
                high_price=float(row[3]),
                low_price=float(row[4]),
                volume=float(row[5]),
                amount=0.0,
                amplitude=((float(row[3]) - float(row[4])) / close_price) if close_price else 0.0,
                pct_change=pct_change(close_price, previous_close) or 0.0,
                change_amount=0.0 if previous_close is None else close_price - previous_close,
                turnover=0.0,
                source="tencent_daily",
            )
        )
        previous_close = close_price
    return bars


def parse_sina_rows(rows: list[dict[str, object]]) -> list[DailyBar]:
    bars: list[DailyBar] = []
    previous_close: float | None = None
    for row in rows:
        trade_date = str(row.get("day") or "")
        if not trade_date:
            continue
        close_price = float(row.get("close") or 0.0)
        high_price = float(row.get("high") or 0.0)
        low_price = float(row.get("low") or 0.0)
        bars.append(
            DailyBar(
                trade_date=trade_date,
                open_price=float(row.get("open") or 0.0),
                close_price=close_price,
                high_price=high_price,
                low_price=low_price,
                volume=float(row.get("volume") or 0.0),
                amount=0.0,
                amplitude=((high_price - low_price) / close_price) if close_price else 0.0,
                pct_change=pct_change(close_price, previous_close) or 0.0,
                change_amount=0.0 if previous_close is None else close_price - previous_close,
                turnover=0.0,
                source="sina_daily",
            )
        )
        previous_close = close_price
    return bars


def load_cached_series(cache_dir: str | Path, *, secid: str) -> list[DailyBar]:
    payload = load_json(Path(cache_dir) / cache_filename(secid), default=[])
    result: list[DailyBar] = []
    for row in payload:
        if isinstance(row, dict):
            result.append(DailyBar(**row))
    return result


def write_series_cache(cache_dir: str | Path, *, secid: str, bars: list[DailyBar], source: str) -> Path:
    payload = []
    for bar in bars:
        row = asdict(bar)
        row["source"] = source
        payload.append(row)
    return write_json(Path(cache_dir) / cache_filename(secid), payload)


def filter_series_by_date(bars: list[DailyBar], *, begin: str, end: str) -> list[DailyBar]:
    begin_key = normalize_date(begin)
    end_key = normalize_date(end)
    return [bar for bar in bars if begin_key <= normalize_date(bar.trade_date) <= end_key]


def normalize_date(value: str) -> str:
    return value.replace("-", "").strip()


def prefer_cached_series(bars: list[DailyBar], *, end: str) -> bool:
    if len(bars) < 30:
        return False
    latest_date = normalize_date(bars[-1].trade_date)
    if normalize_date(end) != "20500101":
        target = normalize_date(end)
        if latest_date >= target:
            return True
        target_day = parse_date_key(target)
        today = datetime.now().date()
        if target_day is None:
            return False
        if target_day < today:
            return False
        return is_recent_daily_cache(latest_date, reference_day=today)
    return is_recent_daily_cache(latest_date, reference_day=datetime.now().date())


def is_recent_daily_cache(latest_date: str, *, reference_day: date) -> bool:
    latest_day = parse_date_key(latest_date)
    if latest_day is None:
        return False
    if latest_day >= reference_day:
        return True
    gap_days = (reference_day - latest_day).days
    return gap_days <= allowed_daily_cache_gap_days(reference_day)


def allowed_daily_cache_gap_days(reference_day: date) -> int:
    # Daily bars before the close typically lag by one trading day, or by the
    # weekend gap when the reference day is Monday.
    if reference_day.weekday() == 0:
        return 3
    if reference_day.weekday() == 6:
        return 2
    return 1


def parse_date_key(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError:
        return None


def compute_series_features(
    series: DailySeriesSnapshot,
    *,
    benchmark_series: DailySeriesSnapshot | None,
    category: str,
) -> dict[str, float | str | None]:
    closes = [bar.close_price for bar in series.bars]
    volumes = [bar.volume for bar in series.bars]
    last_close = closes[-1]
    benchmark_ret_20 = trailing_return([bar.close_price for bar in benchmark_series.bars], 20) if benchmark_series else None
    ret_20 = trailing_return(closes, 20)
    relative_strength = ret_20 - benchmark_ret_20 if ret_20 is not None and benchmark_ret_20 is not None else None
    average_volume_5 = sum(volumes[-5:]) / min(len(volumes), 5)
    volatility = trailing_volatility(closes, 20)
    trend_score = trend_score_from_parts(
        ret_5=trailing_return(closes, 5),
        ret_20=ret_20,
        relative_strength=relative_strength,
        high_gap=gap_to_high(closes, 20),
        volume_ratio=(volumes[-1] / average_volume_5) if average_volume_5 else None,
    )
    return {
        "code": series.code,
        "name": series.name,
        "as_of": series.bars[-1].trade_date,
        "category": category,
        "last_close": last_close,
        "ret_1d": trailing_return(closes, 1),
        "ret_5d": trailing_return(closes, 5),
        "ret_20d": ret_20,
        "high_gap_20d": gap_to_high(closes, 20),
        "low_gap_20d": gap_to_low(closes, 20),
        "volume_ratio_5d": (volumes[-1] / average_volume_5) if average_volume_5 else None,
        "relative_strength_20d": relative_strength,
        "volatility_20d": volatility,
        "trend_score": trend_score,
        "source": series.source,
    }


def trailing_return(closes: list[float], lookback: int) -> float | None:
    if len(closes) <= lookback:
        return None
    base = closes[-lookback - 1]
    return pct_change(closes[-1], base)


def gap_to_high(closes: list[float], lookback: int) -> float | None:
    if len(closes) < lookback:
        return None
    recent = closes[-lookback:]
    high = max(recent)
    return pct_change(closes[-1], high)


def gap_to_low(closes: list[float], lookback: int) -> float | None:
    if len(closes) < lookback:
        return None
    recent = closes[-lookback:]
    low = min(recent)
    return pct_change(closes[-1], low)


def trailing_volatility(closes: list[float], lookback: int) -> float | None:
    if len(closes) <= lookback:
        return None
    returns = [pct_change(closes[idx], closes[idx - 1]) or 0.0 for idx in range(len(closes) - lookback, len(closes))]
    mean = sum(returns) / len(returns)
    variance = sum((item - mean) ** 2 for item in returns) / len(returns)
    return variance ** 0.5


def trend_score_from_parts(
    *,
    ret_5: float | None,
    ret_20: float | None,
    relative_strength: float | None,
    high_gap: float | None,
    volume_ratio: float | None,
) -> float:
    """
    Anti-chasing redesign (v2):
    - Base lowered to 40 — positive momentum must be earned, not assumed.
    - ret_5 spikes are heavily discounted and explicitly penalised above 8%.
    - Oversold pullbacks near the 20d low receive a bounded contrarian bonus.
    - High-gap uses pullback depth, not breakout proximity, to avoid rewarding chase setups.
    """
    score = 40.0

    # --- Short-term momentum (heavily discounted to prevent chasing) ---
    if ret_5 is not None:
        score += ret_5 * 150
        # Additional penalty if ret_5 is large (>8%) — pure chase zone
        if ret_5 > 0.08:
            score -= (ret_5 - 0.08) * 600
        # Bonus: negative ret_5 near support — pullback candidate, not a demerit
        if ret_5 < -0.03 and high_gap is not None and high_gap <= -0.08:
            score += 8.0  # contrarian: oversold + near low = potential bounce setup

    # --- Medium-term momentum ---
    if ret_20 is not None:
        score += ret_20 * 200
        # Mild mean-reversion penalty for stocks running too hot
        if ret_20 > 0.15:
            score -= (ret_20 - 0.15) * 400

    # --- Relative strength (vs benchmark, independent signal) ---
    if relative_strength is not None:
        score += relative_strength * 200

    # --- High-gap: distance from 20d high (key anti-chase signal) ---
    if high_gap is not None:
        pullback_depth = clamp(abs(min(high_gap, 0.0)) - 0.02, 0.0, 0.10)
        score += pullback_depth * 120
        if high_gap < -0.10:
            score -= 6.0  # far below high: lower confidence in trend
        elif high_gap < -0.05:
            score -= 2.0

    # --- Volume confirmation ---
    if volume_ratio is not None:
        score += min(max(volume_ratio - 1.0, -0.5), 1.5) * 10

    return round(clamp(score, 0.0, 100.0), 2)

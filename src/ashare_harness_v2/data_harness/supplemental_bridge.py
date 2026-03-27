from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Any, Callable

import requests


SUPPORTED_STOCK_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}


def build_empty_payload(as_of: str, provider: str) -> dict[str, Any]:
    return {
        "as_of": as_of,
        "provider": provider,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "fundamentals": {},
        "valuation": {},
        "capital_flow": {},
        "external_analysis": {},
        "company_info": {},
        "sector_map": {},
        "sector_metrics": {},
        "errors": {},
    }


def filter_supported_stock_codes(values: list[str]) -> list[str]:
    return dedupe_codes([code for code in values if is_likely_stock_code(code)])


def is_likely_stock_code(code: str) -> bool:
    value = str(code or "").strip()
    if len(value) != 6 or not value.isdigit():
        return False
    return value.startswith(SUPPORTED_STOCK_PREFIXES)


def dedupe_codes(values: list[str]) -> list[str]:
    seen: set[str] = set()
    rows: list[str] = []
    for value in values:
        code = str(value or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        rows.append(code)
    return rows


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def safe_call(
    func: Callable[[], Any],
    *,
    default: Any,
    errors: list[str] | None = None,
    label: str | None = None,
) -> Any:
    try:
        return func()
    except Exception as exc:  # pragma: no cover - live providers are unstable
        if errors is not None:
            prefix = label or getattr(func, "__name__", "fetch")
            errors.append(f"{prefix}: {type(exc).__name__}: {exc}")
        return default


def build_qstock_payload(as_of: str, codes: list[str], *, timeout_seconds: float = 15.0) -> dict[str, Any]:
    payload = build_empty_payload(as_of, provider="qstock_bridge")
    for code in filter_supported_stock_codes(codes):
        errors: list[str] = []
        capital_flow: dict[str, Any] = {}
        capital_flow.update(
            safe_call(
                lambda: fetch_hist_moneyflow(code, timeout_seconds=timeout_seconds),
                default={},
                errors=errors,
                label="qstock_moneyflow",
            )
        )
        capital_flow.update(
            safe_call(
                lambda: fetch_trend_snapshot(code, as_of=as_of, timeout_seconds=timeout_seconds),
                default={},
                errors=errors,
                label="qstock_trend",
            )
        )
        sector_payload = safe_call(
            lambda: fetch_sector_snapshot(code, timeout_seconds=timeout_seconds),
            default={},
            errors=errors,
            label="qstock_sector",
        )
        company_info: dict[str, Any] = {}
        if capital_flow:
            capital_flow["provider"] = "qstock_adapter"
            payload["capital_flow"][code] = capital_flow
            payload["external_analysis"][code] = build_qstock_external_analysis(capital_flow)
        sector_label = str(sector_payload.get("label") or "").strip()
        if sector_label:
            payload["sector_map"][code] = sector_label
        sector_candidates = sector_payload.get("candidates") or []
        if sector_candidates:
            company_info["sector_candidates"] = sector_candidates
            company_info["sector_provider"] = "qstock_adapter"
        if company_info:
            payload["company_info"][code] = company_info
        if errors:
            payload["errors"][code] = errors
    return payload


def build_capitalfarmer_payload(as_of: str, codes: list[str], *, timeout_seconds: float = 15.0) -> dict[str, Any]:
    payload = build_empty_payload(as_of, provider="capitalfarmer_bridge")
    for code in filter_supported_stock_codes(codes):
        errors: list[str] = []
        capital_flow: dict[str, Any] = {}
        capital_flow.update(
            safe_call(
                lambda: fetch_hist_moneyflow(code, timeout_seconds=timeout_seconds),
                default={},
                errors=errors,
                label="capitalfarmer_moneyflow",
            )
        )
        northbound = safe_call(
            lambda: fetch_northbound_stats(code, timeout_seconds=timeout_seconds),
            default={},
            errors=errors,
            label="capitalfarmer_northbound",
        )
        capital_flow.update(northbound)
        capital_flow.update(
            safe_call(
                lambda: fetch_billboard_stats(code, as_of=as_of, timeout_seconds=timeout_seconds),
                default={},
                errors=errors,
                label="capitalfarmer_billboard",
            )
        )
        company_info = build_capitalfarmer_company_info_from_northbound(northbound)
        if capital_flow:
            capital_flow["provider"] = "capitalfarmer_adapter"
            payload["capital_flow"][code] = capital_flow
            payload["external_analysis"][code] = build_capitalfarmer_external_analysis(capital_flow)
        if company_info:
            payload["company_info"][code] = company_info
        if errors:
            payload["errors"][code] = errors
    return payload


def build_qstock_capital_flow(code: str, *, as_of: str, timeout_seconds: float) -> dict[str, Any]:
    capital_flow: dict[str, Any] = {}
    for fetcher in (
        lambda: fetch_hist_moneyflow(code, timeout_seconds=timeout_seconds),
        lambda: fetch_trend_snapshot(code, as_of=as_of, timeout_seconds=timeout_seconds),
    ):
        try:
            capital_flow.update(fetcher())
        except Exception:  # pragma: no cover - live providers are unstable
            continue
    return capital_flow


def build_capitalfarmer_capital_flow(code: str, *, as_of: str, timeout_seconds: float) -> dict[str, Any]:
    capital_flow: dict[str, Any] = {}
    for fetcher in (
        lambda: fetch_hist_moneyflow(code, timeout_seconds=timeout_seconds),
        lambda: fetch_northbound_stats(code, timeout_seconds=timeout_seconds),
        lambda: fetch_billboard_stats(code, as_of=as_of, timeout_seconds=timeout_seconds),
    ):
        try:
            capital_flow.update(fetcher())
        except Exception:  # pragma: no cover - live providers are unstable
            continue
    return capital_flow


def build_capitalfarmer_company_info(code: str, *, timeout_seconds: float) -> dict[str, Any]:
    northbound = fetch_northbound_stats(code, timeout_seconds=timeout_seconds)
    return build_capitalfarmer_company_info_from_northbound(northbound)


def build_capitalfarmer_company_info_from_northbound(northbound: dict[str, Any]) -> dict[str, Any]:
    info: dict[str, Any] = {}
    industry_name = str((northbound or {}).get("industry_name") or "").strip()
    concept_names = (northbound or {}).get("concept_names") or []
    if industry_name:
        info["industry_name"] = industry_name
    if concept_names:
        info["concept_names"] = concept_names
    if info:
        info["capitalfarmer_provider"] = "capitalfarmer_adapter"
    return info


def fetch_hist_moneyflow(code: str, *, timeout_seconds: float) -> dict[str, Any]:
    data = request_json(
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
        params={
            "lmt": "100000",
            "klt": "101",
            "secid": eastmoney_secid(code),
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63",
        },
        timeout_seconds=timeout_seconds,
    )
    rows = (((data or {}).get("data") or {}).get("klines")) or []
    if not rows:
        return {}
    parsed = [item.split(",") for item in rows if item]
    latest = parsed[-1]
    top_5 = parsed[-5:]
    top_10 = parsed[-10:]
    return {
        "latest_date": normalize_date_value(latest[0]),
        "main_net_flow_1d": to_number(latest[1]),
        "main_net_ratio_1d": pct_to_ratio(latest[6]),
        "main_net_flow_5d": safe_sum([item[1] for item in top_5]),
        "main_net_ratio_5d": safe_average_ratio([item[6] for item in top_5]),
        "main_net_flow_10d": safe_sum([item[1] for item in top_10]),
        "main_net_ratio_10d": safe_average_ratio([item[6] for item in top_10]),
        "close_price": to_number(latest[11]),
        "pct_change_1d": pct_to_ratio(latest[12]),
    }


def fetch_trend_snapshot(code: str, *, as_of: str, timeout_seconds: float) -> dict[str, Any]:
    end_date = normalize_trade_date(as_of)
    if len(end_date) != 8:
        end_date = datetime.now().strftime("%Y%m%d")
    begin_date = (datetime.strptime(end_date, "%Y%m%d") - timedelta(days=220)).strftime("%Y%m%d")
    data = request_json(
        "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        params={
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "beg": begin_date,
            "end": end_date,
            "rtntype": "6",
            "secid": eastmoney_secid(code),
            "klt": "101",
            "fqt": "1",
        },
        timeout_seconds=timeout_seconds,
    )
    rows = (((data or {}).get("data") or {}).get("klines")) or []
    if not rows:
        return {}
    parsed = [item.split(",") for item in rows if item]
    closes = [to_number(item[2]) for item in parsed]
    closes = [item for item in closes if isinstance(item, (int, float))]
    if len(closes) < 20:
        return {}
    turnover_rates = [pct_to_ratio(item[10]) for item in parsed]
    turnover_rates = [item for item in turnover_rates if isinstance(item, (int, float))]
    latest_close = closes[-1]
    ma_20 = average(closes[-20:])
    ma_60 = average(closes[-60:]) if len(closes) >= 60 else average(closes)
    ret_20d = safe_return(closes, window=20)
    ret_5d = safe_return(closes, window=5)
    turnover_ratio_5d = None
    if len(turnover_rates) >= 6:
        baseline = average(turnover_rates[-6:-1])
        if isinstance(baseline, (int, float)) and baseline > 0:
            turnover_ratio_5d = float(turnover_rates[-1]) / float(baseline)
    return {
        "trend_close_above_ma20": latest_close > ma_20 if isinstance(ma_20, (int, float)) else None,
        "trend_close_above_ma60": latest_close > ma_60 if isinstance(ma_60, (int, float)) else None,
        "trend_price_vs_ma20": ratio_delta(latest_close, ma_20),
        "trend_price_vs_ma60": ratio_delta(latest_close, ma_60),
        "trend_ret_5d": ret_5d,
        "trend_ret_20d": ret_20d,
        "turnover_rate_1d": turnover_rates[-1] if turnover_rates else None,
        "turnover_ratio_5d": turnover_ratio_5d,
    }


def build_qstock_external_analysis(capital_flow: dict[str, Any]) -> dict[str, Any]:
    trend_score = 50.0
    if capital_flow.get("trend_close_above_ma20"):
        trend_score += 10.0
    if capital_flow.get("trend_close_above_ma60"):
        trend_score += 10.0
    trend_ret_20d = capital_flow.get("trend_ret_20d")
    turnover_ratio_5d = capital_flow.get("turnover_ratio_5d")
    if isinstance(trend_ret_20d, (int, float)):
        trend_score += clamp(float(trend_ret_20d) * 80.0, -14.0, 14.0)
    if isinstance(turnover_ratio_5d, (int, float)):
        trend_score += clamp((float(turnover_ratio_5d) - 1.0) * 18.0, -8.0, 8.0)
    conviction = derive_capital_conviction(capital_flow)
    return {
        "provider_status": {"qstock": "available"},
        "provider_fields": {"qstock": sorted(capital_flow)},
        "trend_template_score": round(clamp(trend_score, 0.0, 100.0), 2),
        "capital_flow_conviction": conviction,
        "capital_flow_style": conviction_label(conviction),
    }


def fetch_sector_snapshot(code: str, *, timeout_seconds: float) -> dict[str, Any]:
    data = request_json(
        "https://push2.eastmoney.com/api/qt/slist/get",
        params={
            "forcect": "1",
            "spt": "3",
            "fields": "f1,f12,f152,f3,f14,f128,f136",
            "pi": "0",
            "pz": "1000",
            "po": "1",
            "fid": "f3",
            "fid0": "f4003",
            "invt": "2",
            "secid": eastmoney_secid(code),
        },
        timeout_seconds=timeout_seconds,
    )
    diff = (((data or {}).get("data") or {}).get("diff")) or []
    if isinstance(diff, dict):
        diff = list(diff.values())
    if not isinstance(diff, list):
        diff = []
    candidates: list[dict[str, Any]] = []
    for row in diff:
        if not isinstance(row, dict):
            continue
        label = str((row or {}).get("f14") or "").strip()
        if not label:
            continue
        candidates.append(
            {
                "label": label,
                "code": str((row or {}).get("f12") or "").strip(),
                "pct_change": pct_to_ratio((row or {}).get("f3")),
            }
        )
    return {
        "label": candidates[0]["label"] if candidates else None,
        "candidates": candidates[:5],
    }


def fetch_northbound_stats(code: str, *, timeout_seconds: float) -> dict[str, Any]:
    result: dict[str, Any] = {}
    latest_row: dict[str, Any] | None = None
    for interval in ("1", "5", "10"):
        data = request_json(
            "https://datacenter-web.eastmoney.com/api/data/v1/get",
            params={
                "sortColumns": "ADD_MARKET_CAP",
                "sortTypes": "-1",
                "pageSize": "5",
                "pageNumber": "1",
                "reportName": "RPT_MUTUAL_STOCK_NORTHSTA",
                "columns": "ALL",
                "source": "WEB",
                "client": "WEB",
                "filter": f'(INTERVAL_TYPE="{interval}")(SECURITY_CODE="{code}")',
            },
            timeout_seconds=timeout_seconds,
        )
        rows = (((data or {}).get("result") or {}).get("data")) or []
        if not rows:
            continue
        row = rows[0]
        latest_row = latest_row or row
        suffix = f"{interval}d"
        hold_mcap_change = to_number(row.get(f"HOLD_MARKETCAP_CHG{interval}"))
        add_market_cap = to_number(row.get("ADD_MARKET_CAP"))
        if isinstance(hold_mcap_change, (int, float)):
            result[f"northbound_hold_marketcap_change_{suffix}"] = hold_mcap_change
            if interval == "5":
                result["northbound_net_flow_5d"] = hold_mcap_change
                result["northbound_proxy_basis"] = "hold_marketcap_change"
        if isinstance(add_market_cap, (int, float)):
            result[f"northbound_add_marketcap_{suffix}"] = add_market_cap
    if latest_row:
        result["northbound_hold_ratio"] = pct_to_ratio(latest_row.get("HOLD_SHARES_RATIO"))
        result["northbound_free_float_ratio"] = pct_to_ratio(latest_row.get("FREE_SHARES_RATIO"))
        result["industry_name"] = str(latest_row.get("INDUSTRY_NAME") or "").strip() or None
        result["concept_names"] = split_values(latest_row.get("CONCEPT_NAME"))
    return result


def fetch_billboard_stats(code: str, *, as_of: str, timeout_seconds: float) -> dict[str, Any]:
    end_date = normalize_date_value(as_of) or datetime.now().date().isoformat()
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=90)).date().isoformat()
    data = request_json(
        "https://datacenter-web.eastmoney.com/api/data/v1/get",
        params={
            "sortColumns": "TRADE_DATE,SECURITY_CODE",
            "sortTypes": "-1,1",
            "pageSize": "100",
            "pageNumber": "1",
            "reportName": "RPT_DAILYBILLBOARD_DETAILS",
            "columns": "ALL",
            "source": "WEB",
            "client": "WEB",
            "filter": f'(TRADE_DATE<="{end_date}")(TRADE_DATE>="{start_date}")(SECURITY_CODE="{code}")',
        },
        timeout_seconds=timeout_seconds,
    )
    rows = (((data or {}).get("result") or {}).get("data")) or []
    if not rows:
        return {}
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        trade_date = normalize_date_value(row.get("TRADE_DATE"))
        if not trade_date or trade_date in deduped:
            continue
        deduped[trade_date] = row
    ordered = [deduped[key] for key in sorted(deduped.keys(), reverse=True)]
    latest = ordered[0]
    net_buy_sum = safe_sum([row.get("BILLBOARD_NET_AMT") for row in ordered])
    return {
        "longhu_appearances_90d": len(ordered),
        "longhu_net_buy_90d": net_buy_sum,
        "longhu_last_date": normalize_date_value(latest.get("TRADE_DATE")),
        "longhu_last_reason": str(latest.get("EXPLANATION") or "").strip() or None,
        "longhu_last_explain": str(latest.get("EXPLAIN") or "").strip() or None,
    }


def build_capitalfarmer_external_analysis(capital_flow: dict[str, Any]) -> dict[str, Any]:
    conviction = derive_capital_conviction(capital_flow)
    northbound = capital_flow.get("northbound_net_flow_5d")
    longhu_count = capital_flow.get("longhu_appearances_90d")
    return {
        "provider_status": {"capitalfarmer": "available"},
        "provider_fields": {"capitalfarmer": sorted(capital_flow)},
        "capital_flow_conviction": conviction,
        "capital_flow_style": conviction_label(conviction),
        "northbound_net_flow_5d": float(northbound) if isinstance(northbound, (int, float)) else None,
        "longhu_activity_90d": int(longhu_count) if isinstance(longhu_count, (int, float)) else None,
    }


def request_json(url: str, *, params: dict[str, Any], timeout_seconds: float) -> dict[str, Any]:
    response = requests.get(url, params=params, headers=REQUEST_HEADERS, timeout=max(timeout_seconds, 5.0))
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else {}


def eastmoney_secid(code: str) -> str:
    return f"1.{code}" if str(code).startswith("6") else f"0.{code}"


def split_values(value: Any) -> list[str]:
    raw = str(value or "")
    for token in ("、", ",", "，", ";", "；", "|"):
        raw = raw.replace(token, "/")
    return [item.strip() for item in raw.split("/") if item.strip()]


def average(values: list[float]) -> float | None:
    cleaned = [float(item) for item in values if isinstance(item, (int, float))]
    if not cleaned:
        return None
    return float(sum(cleaned) / len(cleaned))


def safe_sum(values: list[Any]) -> float | None:
    cleaned = [to_number(item) for item in values]
    cleaned = [item for item in cleaned if isinstance(item, (int, float))]
    if not cleaned:
        return None
    return float(sum(cleaned))


def safe_average_ratio(values: list[Any]) -> float | None:
    cleaned = [pct_to_ratio(item) for item in values]
    cleaned = [item for item in cleaned if isinstance(item, (int, float))]
    if not cleaned:
        return None
    return float(sum(cleaned) / len(cleaned))


def safe_return(values: list[float], *, window: int) -> float | None:
    if len(values) <= window:
        return None
    base = values[-(window + 1)]
    latest = values[-1]
    if not isinstance(base, (int, float)) or base == 0:
        return None
    return (float(latest) / float(base)) - 1.0


def ratio_delta(left: float | None, right: float | None) -> float | None:
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)) or right == 0:
        return None
    return (float(left) / float(right)) - 1.0


def derive_capital_conviction(capital_flow: dict[str, Any]) -> float:
    score = 0.0
    main_flow = capital_flow.get("main_net_flow_5d")
    main_ratio = capital_flow.get("main_net_ratio_5d")
    northbound = capital_flow.get("northbound_net_flow_5d")
    if isinstance(main_flow, (int, float)):
        score += clamp(float(main_flow) / 500000000.0, -1.0, 1.0) * 0.5
    if isinstance(main_ratio, (int, float)):
        score += clamp(float(main_ratio) / 0.08, -1.0, 1.0) * 0.35
    if isinstance(northbound, (int, float)):
        score += clamp(float(northbound) / 300000000.0, -1.0, 1.0) * 0.15
    return round(clamp(score, -1.0, 1.0), 3)


def conviction_label(value: float) -> str:
    if value >= 0.25:
        return "accumulation"
    if value <= -0.25:
        return "distribution"
    return "neutral"


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def pct_to_ratio(value: Any) -> float | None:
    number = to_number(value)
    if number is None:
        return None
    return number / 100.0


def to_number(value: Any) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def normalize_trade_date(value: str) -> str:
    return str(value or "").replace("-", "").strip()[:8]


def normalize_date_value(value: Any) -> str | None:
    if value in (None, "", "-", "--"):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if " " in text:
        text = text.split(" ", 1)[0]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text

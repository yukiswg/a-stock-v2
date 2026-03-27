from __future__ import annotations

import argparse
import importlib
import json
import math
import sys
import time
from datetime import date, datetime
from typing import Any, Callable

import akshare as ak


SECTOR_ALIASES = {
    "乘用车": "汽车整车",
    "汽车制造业": "汽车整车",
    "汽车零部件": "汽车零部件",
    "核电": "电力",
    "电力及电网": "电力",
    "电力、热力生产和供应业": "电力",
    "电池部件及材料": "电池",
    "储能设备": "电池",
    "信息技术服务": "IT服务",
    "信息科技咨询与其他服务": "IT服务",
    "软件与互联网": "软件开发",
    "纸材料包装": "包装印刷",
    "容器与包装": "包装印刷",
    "计算机、通信和其他电子设备制造业": "消费电子",
}
SUPPORTED_STOCK_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch supplemental A-share data through AKShare.")
    parser.add_argument("--as-of", default=date.today().isoformat(), help="Trading date tag.")
    parser.add_argument("--codes", nargs="*", default=[], help="Security codes to enrich.")
    args = parser.parse_args()

    codes = filter_supported_stock_codes(args.codes)
    industry_pe = safe_call(lambda: fetch_industry_pe_map(args.as_of), default={}) if codes else {}
    sector_metrics = safe_call(fetch_sector_metrics, default={})

    payload: dict[str, Any] = {
        "as_of": args.as_of,
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "fundamentals": {},
        "valuation": {},
        "capital_flow": {},
        "external_analysis": {},
        "company_info": {},
        "sector_map": {},
        "sector_metrics": sector_metrics,
        "errors": {},
    }

    for code in codes:
        errors: list[str] = []
        profile = safe_call(lambda: fetch_profile(code), default={}, errors=errors)
        industry_change = safe_call(lambda: fetch_industry_change(code), default={}, errors=errors)
        business_info = safe_call(lambda: fetch_business_info(code), default={}, errors=errors)
        fundamentals = safe_call(lambda: fetch_financials(code), default={}, errors=errors)
        capital_flow = safe_call(lambda: fetch_capital_flow(code), default={}, errors=errors)
        external_analysis = safe_call(
            lambda: fetch_external_analysis(code, capital_flow=capital_flow),
            default={},
            errors=errors,
        )

        company_info = merge_company_info(profile, industry_change, business_info)
        sector_label, sector_match_source = match_sector_label(company_info=company_info, sector_metrics=sector_metrics)
        if sector_label:
            payload["sector_map"][code] = sector_label
            company_info["sector_match"] = sector_label
            company_info["sector_match_source"] = sector_match_source

        if company_info:
            payload["company_info"][code] = company_info
        if fundamentals:
            payload["fundamentals"][code] = fundamentals
        if capital_flow:
            payload["capital_flow"][code] = capital_flow
        if external_analysis:
            payload["external_analysis"][code] = external_analysis

        industry_name = str(company_info.get("industry_name") or "").strip()
        valuation = build_valuation_context(industry_name=industry_name, industry_pe_map=industry_pe)
        if valuation:
            payload["valuation"][code] = valuation
        if errors:
            payload["errors"][code] = errors

    json.dump(payload, sys.stdout, ensure_ascii=False, indent=2, default=json_default)


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


def filter_supported_stock_codes(values: list[str]) -> list[str]:
    return dedupe_codes([code for code in values if is_likely_stock_code(code)])


def is_likely_stock_code(code: str) -> bool:
    value = str(code or "").strip()
    if len(value) != 6 or not value.isdigit():
        return False
    return value.startswith(SUPPORTED_STOCK_PREFIXES)


def safe_call(
    func: Callable[[], Any],
    *,
    default: Any,
    errors: list[str] | None = None,
    attempts: int = 3,
    pause_seconds: float = 0.6,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(max(attempts, 1)):
        try:
            return func()
        except Exception as exc:  # pragma: no cover - depends on live provider stability
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(pause_seconds * (attempt + 1))
    if errors is not None and last_error is not None:
        errors.append(f"{func_name(func)}: {type(last_error).__name__}: {last_error}")
    return default


def func_name(func: Callable[[], Any]) -> str:
    return getattr(func, "__name__", None) or getattr(getattr(func, "__wrapped__", None), "__name__", None) or "fetch"


def fetch_profile(code: str) -> dict[str, Any]:
    frame = ak.stock_profile_cninfo(symbol=code)
    if frame.empty:
        return {}
    row = frame.iloc[0].to_dict()
    indexes = [item.strip() for item in str(row.get("入选指数") or "").split(",") if item.strip()]
    return {
        "code": code,
        "name": str(row.get("A股简称") or code),
        "company_name": str(row.get("公司名称") or ""),
        "industry_name": str(row.get("所属行业") or ""),
        "market": str(row.get("所属市场") or ""),
        "business": str(row.get("主营业务") or ""),
        "business_scope": str(row.get("经营范围") or ""),
        "listing_date": normalize_date_value(row.get("上市日期")),
        "selected_indexes": indexes,
        "website": str(row.get("官方网站") or ""),
    }


def fetch_industry_change(code: str) -> dict[str, Any]:
    frame = ak.stock_industry_change_cninfo(symbol=code)
    if frame.empty:
        return {}
    frame = frame.sort_values(by="变更日期", ascending=False)
    row = frame.iloc[0].to_dict()
    return {
        "industry_standard": str(row.get("分类标准") or ""),
        "industry_large": str(row.get("行业大类") or ""),
        "industry_medium": str(row.get("行业中类") or ""),
        "industry_small": str(row.get("行业次类") or ""),
        "industry_root": str(row.get("行业门类") or ""),
        "industry_changed_at": normalize_date_value(row.get("变更日期")),
    }


def fetch_business_info(code: str) -> dict[str, Any]:
    frame = ak.stock_zyjs_ths(symbol=code)
    if frame.empty:
        return {}
    row = frame.iloc[0].to_dict()
    return {
        "product_types": split_values(row.get("产品类型")),
        "products": split_values(row.get("产品名称")),
        "business_outline": str(row.get("主营业务") or ""),
    }


def fetch_financials(code: str) -> dict[str, Any]:
    symbol = f"{code}.{market_suffix(code)}"
    frame = ak.stock_financial_analysis_indicator_em(symbol=symbol, indicator="按报告期")
    if frame.empty:
        return {}
    frame = frame.sort_values(by="REPORT_DATE", ascending=False)
    row = frame.iloc[0].to_dict()
    return {
        "report_date": normalize_date_value(row.get("REPORT_DATE")),
        "notice_date": normalize_date_value(row.get("NOTICE_DATE")),
        "revenue_growth_yoy": pct_to_ratio(row.get("TOTALOPERATEREVETZ")),
        "profit_growth_yoy": pct_to_ratio(row.get("PARENTNETPROFITTZ")),
        "roe": pct_to_ratio(row.get("ROEJQ")),
        "roic": pct_to_ratio(row.get("ROIC")),
        "gross_margin": pct_to_ratio(row.get("XSMLL")),
        "net_margin": pct_to_ratio(row.get("XSJLL")),
        "operating_cashflow_yoy": pct_to_ratio(row.get("MGJYXJJETZ")),
        "operating_cashflow_margin": to_number(row.get("JYXJLYYSR")),
        "debt_to_asset": pct_to_ratio(row.get("ZCFZL")),
        "eps": to_number(row.get("EPSJB")),
        "bps": to_number(row.get("BPS")),
    }


def fetch_capital_flow(code: str) -> dict[str, Any]:
    frame = ak.stock_individual_fund_flow(stock=code, market=market_tag(code))
    if frame.empty:
        return {}
    frame = frame.sort_values(by="日期", ascending=False)
    latest = frame.iloc[0]
    top5 = frame.head(5)
    return {
        "latest_date": normalize_date_value(latest.get("日期")),
        "main_net_flow_1d": to_number(latest.get("主力净流入-净额")),
        "main_net_ratio_1d": pct_to_ratio(latest.get("主力净流入-净占比")),
        "main_net_flow_5d": safe_sum(top5["主力净流入-净额"].tolist()),
        "main_net_ratio_5d": safe_average_ratio(top5["主力净流入-净占比"].tolist()),
        "close_price": to_number(latest.get("收盘价")),
        "pct_change_1d": pct_to_ratio(latest.get("涨跌幅")),
    }


def fetch_external_analysis(code: str, *, capital_flow: dict[str, Any]) -> dict[str, Any]:
    row = derive_local_external_analysis(capital_flow=capital_flow)
    provider_status: dict[str, str] = {}
    provider_fields: dict[str, list[str]] = {}

    qstock_data, qstock_status = fetch_qstock_analysis(code)
    if qstock_status:
        provider_status["qstock"] = qstock_status
    if qstock_data:
        row.update(qstock_data)
        provider_fields["qstock"] = sorted(qstock_data)

    capitalfarmer_data, capitalfarmer_status = fetch_capitalfarmer_analysis(code)
    if capitalfarmer_status:
        provider_status["capitalfarmer"] = capitalfarmer_status
    if capitalfarmer_data:
        row.update(capitalfarmer_data)
        provider_fields["capitalfarmer"] = sorted(capitalfarmer_data)

    if provider_status:
        row["provider_status"] = provider_status
    if provider_fields:
        row["provider_fields"] = provider_fields
    return row


def derive_local_external_analysis(*, capital_flow: dict[str, Any]) -> dict[str, Any]:
    score = 0.0
    main_flow = capital_flow.get("main_net_flow_5d")
    main_ratio = capital_flow.get("main_net_ratio_5d")
    pct_change = capital_flow.get("pct_change_1d")
    if isinstance(main_flow, (int, float)):
        score += clamp(float(main_flow) / 500000000.0, -1.0, 1.0) * 0.6
    if isinstance(main_ratio, (int, float)):
        score += clamp(float(main_ratio) / 0.08, -1.0, 1.0) * 0.4
    score = round(clamp(score, -1.0, 1.0), 3)
    conviction = "accumulation" if score >= 0.25 else "distribution" if score <= -0.25 else "neutral"
    row: dict[str, Any] = {
        "capital_flow_conviction": score,
        "capital_flow_style": conviction,
    }
    if isinstance(main_flow, (int, float)):
        row["capital_flow_conviction_amount"] = float(main_flow)
    if isinstance(main_ratio, (int, float)):
        row["capital_flow_conviction_ratio"] = float(main_ratio)
    if isinstance(pct_change, (int, float)):
        row["session_pct_change"] = float(pct_change)
    return row


def fetch_qstock_analysis(code: str) -> tuple[dict[str, Any], str]:
    try:
        module = importlib.import_module("qstock")
    except Exception as exc:
        return {}, f"unavailable:{type(exc).__name__}"

    row: dict[str, Any] = {}
    snapshot_func = getattr(module, "stock_snapshot", None)
    if callable(snapshot_func):
        snapshot = snapshot_func(code)
        record = dataframe_last_row(snapshot)
        if record:
            price = to_number(record.get("最新价"))
            turnover = pct_to_ratio(record.get("换手率"))
            amount = to_number(record.get("成交额"))
            if isinstance(price, (int, float)):
                row["qstock_last_price"] = float(price)
            if isinstance(turnover, (int, float)):
                row["qstock_turnover_ratio"] = float(turnover)
            if isinstance(amount, (int, float)):
                row["qstock_amount"] = float(amount)

    money_func = getattr(module, "stock_money", None)
    if callable(money_func):
        money = money_func(code, ndays=[3, 5, 10, 20])
        record = dataframe_last_row(money)
        if record:
            flow_5d = to_number(record.get("5日主力净流入"))
            flow_20d = to_number(record.get("20日主力净流入"))
            if isinstance(flow_5d, (int, float)):
                row["qstock_main_net_flow_5d"] = float(flow_5d) * 10000.0
            if isinstance(flow_20d, (int, float)):
                row["qstock_main_net_flow_20d"] = float(flow_20d) * 10000.0

    if not row:
        return {}, "available:no_supported_payload"
    return row, "available"


def fetch_capitalfarmer_analysis(code: str) -> tuple[dict[str, Any], str]:
    try:
        module = importlib.import_module("capitalfarmer")
    except Exception as exc:
        return {}, f"unavailable:{type(exc).__name__}"

    row: dict[str, Any] = {}

    money = invoke_provider_function(module, ("hist_moneyflow", "moneyflow"), code)
    money_record = dataframe_last_row(money)
    if money_record:
        main_flow = fuzzy_pick_number(money_record, ("主力", "净流"))
        main_ratio = fuzzy_pick_ratio(money_record, ("主力", "占比"))
        if isinstance(main_flow, (int, float)):
            row["capitalfarmer_main_net_flow"] = float(main_flow)
        if isinstance(main_ratio, (int, float)):
            row["capitalfarmer_main_net_ratio"] = float(main_ratio)

    north = invoke_provider_function(module, ("northbound", "north_money", "northbound_stock"), code)
    north_record = dataframe_last_row(north, code=code)
    if north_record:
        holding_ratio = fuzzy_pick_ratio(north_record, ("持股占流通股比",))
        add_ratio = fuzzy_pick_ratio(north_record, ("增持占流通股比",))
        add_value = fuzzy_pick_number(north_record, ("增持市值",))
        if isinstance(holding_ratio, (int, float)):
            row["northbound_holding_ratio"] = float(holding_ratio)
        if isinstance(add_ratio, (int, float)):
            row["northbound_share_change_5d"] = float(add_ratio)
        if isinstance(add_value, (int, float)):
            row["northbound_net_flow_5d"] = float(add_value)

    margin = invoke_provider_function(module, ("margin", "rzrq", "margin_trading"), code)
    margin_record = dataframe_last_row(margin, code=code)
    if margin_record:
        margin_ratio = fuzzy_pick_ratio(margin_record, ("融资", "增幅"))
        margin_flow = fuzzy_pick_number(margin_record, ("融资", "净"))
        if isinstance(margin_ratio, (int, float)):
            row["margin_balance_change_5d"] = float(margin_ratio)
        if isinstance(margin_flow, (int, float)):
            row["margin_net_change"] = float(margin_flow)

    lhb = invoke_provider_function(module, ("lhb", "billboard", "dragon_tiger"), code)
    lhb_record = dataframe_last_row(lhb, code=code)
    if lhb_record:
        institutional_net = fuzzy_pick_number(lhb_record, ("机构", "净买"))
        hot_money_net = fuzzy_pick_number(lhb_record, ("游资", "净买"))
        if isinstance(institutional_net, (int, float)):
            row["institutional_net_buy"] = float(institutional_net)
        if isinstance(hot_money_net, (int, float)):
            row["hot_money_net_buy"] = float(hot_money_net)

    if not row:
        return {}, "available:no_supported_payload"
    return row, "available"


def invoke_provider_function(module: Any, names: tuple[str, ...], code: str) -> Any:
    for name in names:
        func = getattr(module, name, None)
        if not callable(func):
            continue
        for kwargs in ({}, {"code": code}, {"stock": code}, {"symbol": code}):
            try:
                return func(**kwargs) if kwargs else func(code)
            except TypeError:
                continue
    return None


def dataframe_last_row(frame: Any, *, code: str | None = None) -> dict[str, Any]:
    if frame is None:
        return {}
    try:
        import pandas as pd
    except Exception:
        pd = None
    if pd is not None and isinstance(frame, pd.Series):
        return {str(key): value for key, value in frame.to_dict().items()}
    if pd is not None and isinstance(frame, pd.DataFrame):
        if frame.empty:
            return {}
        candidate = frame
        if code:
            for column in ("代码", "code", "Code"):
                if column in candidate.columns:
                    matched = candidate[candidate[column].astype(str).str.contains(str(code), na=False)]
                    if not matched.empty:
                        candidate = matched
                        break
        return {str(key): value for key, value in candidate.iloc[-1].to_dict().items()}
    if isinstance(frame, dict):
        return {str(key): value for key, value in frame.items()}
    return {}


def fuzzy_pick_number(row: dict[str, Any], terms: tuple[str, ...]) -> float | None:
    for key, value in row.items():
        text = str(key)
        if all(term in text for term in terms):
            number = to_number(value)
            if isinstance(number, (int, float)):
                return float(number)
    return None


def fuzzy_pick_ratio(row: dict[str, Any], terms: tuple[str, ...]) -> float | None:
    for key, value in row.items():
        text = str(key)
        if all(term in text for term in terms):
            ratio = pct_to_ratio(value)
            if isinstance(ratio, (int, float)):
                return float(ratio)
    return None


def fetch_industry_pe_map(as_of: str) -> dict[str, dict[str, Any]]:
    trade_date = normalize_trade_date(as_of)
    frame = ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业分类", date=trade_date)
    if frame.empty:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        name = str(row.get("行业名称") or "").strip()
        if not name:
            continue
        rows[name] = {
            "industry_name": name,
            "industry_pe": to_number(row.get("静态市盈率-加权平均")),
            "industry_pe_median": to_number(row.get("静态市盈率-中位数")),
            "industry_level": to_number(row.get("行业层级")),
            "company_count": to_number(row.get("公司数量")),
        }
    return rows


def fetch_sector_metrics() -> dict[str, dict[str, Any]]:
    frame = ak.stock_board_industry_summary_ths()
    if frame.empty:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        label = str(row.get("板块") or "").strip()
        if not label:
            continue
        pct_change = pct_to_ratio(row.get("涨跌幅"))
        rising = to_number(row.get("上涨家数"))
        falling = to_number(row.get("下跌家数"))
        breadth = None
        if isinstance(rising, (int, float)) and isinstance(falling, (int, float)) and (rising + falling) > 0:
            breadth = float(rising) / float(rising + falling)
        net_inflow = to_number(row.get("净流入"))
        score = 50.0
        if isinstance(pct_change, (int, float)):
            score += max(min(pct_change * 420.0, 18.0), -18.0)
        if isinstance(breadth, (int, float)):
            score += (float(breadth) - 0.5) * 28.0
        if isinstance(net_inflow, (int, float)):
            score += max(min(net_inflow / 12.0, 7.0), -7.0)
        rows[label] = {
            "label": label,
            "score": round(max(0.0, min(score, 100.0)), 2),
            "pct_change": pct_change,
            "breadth": breadth,
            "net_inflow": net_inflow,
            "leader": str(row.get("领涨股") or ""),
            "leader_pct_change": pct_to_ratio(row.get("领涨股-涨跌幅")),
        }
    return rows


def merge_company_info(profile: dict[str, Any], industry_change: dict[str, Any], business_info: dict[str, Any]) -> dict[str, Any]:
    merged = dict(profile or {})
    merged.update({key: value for key, value in (industry_change or {}).items() if has_value(value)})
    merged.update({key: value for key, value in (business_info or {}).items() if has_value(value)})
    return merged


def build_valuation_context(*, industry_name: str, industry_pe_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not industry_name:
        return {}
    row = industry_pe_map.get(industry_name) or {}
    if not row:
        return {"industry_name": industry_name}
    return {
        "industry_name": industry_name,
        "industry_pe": row.get("industry_pe"),
        "industry_pe_median": row.get("industry_pe_median"),
        "industry_level": row.get("industry_level"),
        "industry_company_count": row.get("company_count"),
    }


def match_sector_label(*, company_info: dict[str, Any], sector_metrics: dict[str, dict[str, Any]]) -> tuple[str | None, str | None]:
    labels = [str(item).strip() for item in sector_metrics]
    exact_candidates = [
        str(company_info.get("industry_medium") or "").strip(),
        str(company_info.get("industry_small") or "").strip(),
        str(company_info.get("industry_large") or "").strip(),
        str(company_info.get("industry_name") or "").strip(),
        str(company_info.get("industry_root") or "").strip(),
    ]
    for candidate in exact_candidates:
        if candidate and candidate in sector_metrics:
            return candidate, "industry_exact"
    for candidate in exact_candidates:
        alias = sector_alias(candidate)
        if alias and alias in sector_metrics:
            return alias, "industry_alias"
    for candidate in exact_candidates:
        if not candidate:
            continue
        for label in sorted(labels, key=len, reverse=True):
            if label in candidate or candidate in label:
                return label, "industry_fuzzy"

    text_parts = [
        str(company_info.get("business") or ""),
        str(company_info.get("business_outline") or ""),
        str(company_info.get("business_scope") or ""),
        " ".join(company_info.get("product_types") or []),
        " ".join(company_info.get("products") or []),
    ]
    joined = " ".join(part for part in text_parts if part).strip()
    if joined:
        for label in sorted(labels, key=len, reverse=True):
            if len(label) < 3:
                continue
            if label in joined:
                return label, "business_match"
    return None, None


def sector_alias(candidate: str) -> str | None:
    text = str(candidate or "").strip()
    if not text:
        return None
    for key, value in SECTOR_ALIASES.items():
        if key == text or key in text or text in key:
            return value
    return None


def market_suffix(code: str) -> str:
    return "SH" if code.startswith("6") else "SZ"


def market_tag(code: str) -> str:
    return "sh" if code.startswith("6") else "sz"


def split_values(value: Any) -> list[str]:
    raw = str(value or "")
    separators = ["、", ",", "，", ";", "；", "/"]
    for token in separators:
        raw = raw.replace(token, "|")
    return [item.strip() for item in raw.split("|") if item.strip()]


def has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


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


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


if __name__ == "__main__":
    main()

from __future__ import annotations

import csv
from dataclasses import replace
from pathlib import Path

from ..models import DailySeriesSnapshot, HoldingPosition, HoldingsSnapshot


COLUMN_ALIASES = {
    "code": ["证券代码", "股票代码", "代码", "证券编号", "stock_code"],
    "name": ["证券名称", "股票名称", "名称", "证券简称", "name"],
    "quantity": ["持仓数量", "当前拥股", "股份余额", "证券数量", "quantity"],
    "available_quantity": ["可卖数量", "可用数量", "可卖余额", "available_quantity"],
    "cost_price": ["成本价", "摊薄成本价", "保本价", "cost_price"],
    "last_price": ["现价", "最新价", "市价", "last_price"],
    "market_value": ["市值", "参考市值", "最新市值", "market_value"],
    "pnl_amount": ["浮动盈亏", "参考盈亏", "盈亏", "pnl_amount"],
    "pnl_pct": ["盈亏比例", "盈亏比", "涨跌幅", "pnl_pct"],
    "sector": ["行业", "板块", "sector"],
}


def load_holdings_snapshot(path: str | Path, *, as_of: str, cash: float = 0.0) -> HoldingsSnapshot:
    rows = _load_rows(Path(path))
    positions = [row_to_position(row) for row in rows]
    positions = [position for position in positions if position is not None]
    total_market_value = sum(position.market_value for position in positions)
    total_equity = total_market_value + cash
    exposure_ratio = (total_market_value / total_equity) if total_equity else 0.0
    sector_weights = summarize_sectors(positions, total_market_value)
    alerts = build_holdings_alerts(positions, cash=cash, total_equity=total_equity)
    return HoldingsSnapshot(
        as_of=as_of,
        source_file=str(path),
        positions=positions,
        total_market_value=round(total_market_value, 2),
        total_equity=round(total_equity, 2),
        exposure_ratio=round(exposure_ratio, 4),
        alerts=alerts,
        sector_weights=sector_weights,
    )


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def row_to_position(row: dict[str, str]) -> HoldingPosition | None:
    code = pick_field(row, "code")
    if not code:
        return None
    quantity = parse_number(pick_field(row, "quantity"))
    available_quantity = parse_number(pick_field(row, "available_quantity"))
    cost_price = parse_nullable_number(pick_field(row, "cost_price"))
    last_price = parse_nullable_number(pick_field(row, "last_price"))
    market_value = parse_number(pick_field(row, "market_value"))
    pnl_amount = parse_nullable_number(pick_field(row, "pnl_amount"))
    pnl_pct = parse_percent(pick_field(row, "pnl_pct"))

    if market_value == 0 and quantity and last_price is not None:
        market_value = quantity * last_price
    if pnl_amount is None and quantity and last_price is not None and cost_price not in (None, 0):
        pnl_amount = (last_price - cost_price) * quantity
    if pnl_pct is None and last_price is not None and cost_price not in (None, 0):
        pnl_pct = (last_price / cost_price) - 1.0

    return HoldingPosition(
        code=code,
        name=pick_field(row, "name") or code,
        quantity=quantity,
        available_quantity=available_quantity,
        market_value=market_value,
        cost_price=cost_price,
        last_price=last_price,
        pnl_amount=pnl_amount,
        pnl_pct=pnl_pct,
        sector=pick_field(row, "sector") or None,
    )


def pick_field(row: dict[str, str], canonical: str) -> str:
    for alias in COLUMN_ALIASES[canonical]:
        value = str(row.get(alias) or "").strip()
        if value:
            return value
    return ""


def parse_number(value: str) -> float:
    if not value:
        return 0.0
    cleaned = value.replace(",", "").replace("￥", "").replace("¥", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_nullable_number(value: str) -> float | None:
    return None if not value else parse_number(value)


def parse_percent(value: str) -> float | None:
    if not value:
        return None
    text = value.strip().replace(",", "")
    try:
        numeric = float(text[:-1]) / 100 if text.endswith("%") else float(text)
    except ValueError:
        return None
    return numeric / 100 if abs(numeric) > 1 else numeric


def summarize_sectors(positions: list[HoldingPosition], total_market_value: float) -> list[dict[str, float | str]]:
    buckets: dict[str, float] = {}
    for position in positions:
        sector = position.sector or "未分类"
        buckets[sector] = buckets.get(sector, 0.0) + position.market_value
    result = [
        {
            "sector": sector,
            "market_value": round(value, 2),
            "weight": round(value / total_market_value, 4) if total_market_value else 0.0,
        }
        for sector, value in buckets.items()
    ]
    result.sort(key=lambda item: float(item["market_value"]), reverse=True)
    return result


def build_holdings_alerts(positions: list[HoldingPosition], *, cash: float, total_equity: float) -> list[str]:
    if not positions:
        return ["当前无持仓。"]
    alerts: list[str] = []
    sorted_positions = sorted(positions, key=lambda item: item.market_value, reverse=True)
    total_market_value = sum(position.market_value for position in positions)
    top_weight = (sorted_positions[0].market_value / total_market_value) if total_market_value else 0.0
    if top_weight >= 0.35:
        alerts.append(f"第一大持仓占比 {top_weight:.1%}，集中度偏高。")
    cash_ratio = (cash / total_equity) if total_equity else 0.0
    if cash_ratio < 0.05:
        alerts.append(f"现金占比 {cash_ratio:.1%}，缓冲偏低。")
    deep_losers = [item for item in positions if item.pnl_pct is not None and item.pnl_pct <= -0.1]
    if deep_losers:
        alerts.append("存在深度回撤持仓，需复核减仓纪律。")
    if not alerts:
        alerts.append("默认风险阈值内，仍需结合公告和盘中异动复核。")
    return alerts


def hydrate_holdings_snapshot_with_prices(
    snapshot: HoldingsSnapshot,
    *,
    series_map: dict[str, DailySeriesSnapshot],
) -> HoldingsSnapshot:
    enriched_positions: list[HoldingPosition] = []
    for position in snapshot.positions:
        last_price = position.last_price
        if last_price in (None, 0):
            series = series_map.get(position.code)
            if series and series.bars:
                last_price = float(series.bars[-1].close_price)
        market_value = position.market_value
        if (market_value == 0 or market_value is None) and position.quantity and last_price not in (None, 0):
            market_value = float(position.quantity) * float(last_price)
        pnl_amount = position.pnl_amount
        if pnl_amount is None and position.quantity and last_price not in (None, 0) and position.cost_price not in (None, 0):
            pnl_amount = (float(last_price) - float(position.cost_price)) * float(position.quantity)
        pnl_pct = position.pnl_pct
        if pnl_pct is None and last_price not in (None, 0) and position.cost_price not in (None, 0):
            pnl_pct = (float(last_price) / float(position.cost_price)) - 1.0
        enriched_positions.append(
            replace(
                position,
                last_price=last_price,
                market_value=round(float(market_value or 0.0), 2),
                pnl_amount=round(float(pnl_amount), 2) if pnl_amount is not None else None,
                pnl_pct=round(float(pnl_pct), 6) if pnl_pct is not None else None,
            )
        )
    total_market_value = round(sum(position.market_value for position in enriched_positions), 2)
    cash = max(round(float(snapshot.total_equity) - total_market_value, 2), 0.0)
    total_equity = round(total_market_value + cash, 2)
    exposure_ratio = round((total_market_value / total_equity) if total_equity else 0.0, 4)
    sector_weights = summarize_sectors(enriched_positions, total_market_value)
    alerts = build_holdings_alerts(enriched_positions, cash=cash, total_equity=total_equity)
    return HoldingsSnapshot(
        as_of=snapshot.as_of,
        source_file=snapshot.source_file,
        positions=enriched_positions,
        total_market_value=total_market_value,
        total_equity=total_equity,
        exposure_ratio=exposure_ratio,
        alerts=alerts,
        sector_weights=sector_weights,
    )

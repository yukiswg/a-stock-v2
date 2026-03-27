from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class HoldingPosition:
    code: str
    name: str
    quantity: float
    available_quantity: float
    market_value: float
    cost_price: float | None = None
    last_price: float | None = None
    pnl_amount: float | None = None
    pnl_pct: float | None = None
    sector: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class HoldingsSnapshot:
    as_of: str
    source_file: str
    positions: list[HoldingPosition]
    total_market_value: float
    total_equity: float
    exposure_ratio: float
    alerts: list[str]
    sector_weights: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DailyBar:
    trade_date: str
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: float
    amount: float
    amplitude: float
    pct_change: float
    change_amount: float
    turnover: float
    source: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DailySeriesSnapshot:
    code: str
    name: str
    secid: str
    fetched_at: str
    source: str
    bars: list[DailyBar]
    used_cache: bool = False
    degraded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QuoteSnapshot:
    code: str
    name: str
    timestamp: str
    fetched_at: str
    freshness_seconds: float | None
    is_stale: bool
    last_price: float
    prev_close: float
    open_price: float | None
    high_price: float | None
    low_price: float | None
    volume: float | None
    amount: float | None
    turnover: float | None
    source: str
    trade_date: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsItem:
    source_id: str
    source_name: str
    title: str
    url: str
    published_at: str | None
    tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AnnouncementItem:
    code: str
    name: str
    title: str
    url: str
    published_at: str | None
    source: str = "cninfo"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class InstrumentFeatures:
    code: str
    name: str
    as_of: str
    category: str
    last_close: float
    ret_1d: float | None
    ret_5d: float | None
    ret_20d: float | None
    high_gap_20d: float | None
    low_gap_20d: float | None
    volume_ratio_5d: float | None
    relative_strength_20d: float | None
    volatility_20d: float | None
    trend_score: float
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StructuredDecision:
    object_type: str
    object_id: str
    object_name: str
    at: str
    action: str
    score: float
    probability: float | None
    reason: list[str]
    risk: list[str]
    sources: list[str]
    thesis: str = ""
    counterpoints: list[str] = field(default_factory=list)
    trigger_conditions: list[str] = field(default_factory=list)
    invalidation_conditions: list[str] = field(default_factory=list)
    priority_score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DailyDecisionBundle:
    as_of: str
    market_view: StructuredDecision
    holdings_actions: list[StructuredDecision]
    watchlist: list[StructuredDecision]
    monitor_plan: list[StructuredDecision]
    final_action_summary: str
    homepage_overview: dict[str, Any]
    llm_summary: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AlertEvent:
    session_id: str
    timestamp: str
    code: str
    name: str
    event_type: str
    severity: str
    score: float
    action_hint: str
    summary: str
    explanation: str
    rationale: list[str]
    source: str
    price: float
    benchmark_return: float | None
    freshness_seconds: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SessionManifest:
    run_id: str
    as_of: str
    session_type: str
    created_at: str
    cwd: str
    status: str
    artifacts: dict[str, str]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvaluationRow:
    key: str
    label: str
    value: float | int | str | None
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PricePrediction:
    """Single price forecast with confidence and reasoning."""
    horizon: str           # "intraday_close" | "dayend" | "nextday" | "longterm"
    predicted_price: float | None
    predicted_return: float | None   # % return from reference price
    confidence: float              # 0-1, how confident the model is
    confidence_band_upper: float | None  # +1 std dev price
    confidence_band_lower: float | None  # -1 std dev price
    method: str                   # "volume_profile" | "mean_reversion" | "historical_dist" | "linear_trend" | "combined"
    reasoning: list[str]           # human-readable factors
    as_of_time: str               # timestamp when this prediction was made
    reference_price: float        # price the return is computed from
    source_data_quality: str      # "full" | "degraded" | "stale"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PredictionBundle:
    """Container for all prediction outputs for a single security."""
    code: str
    name: str
    as_of: str
    reference_price: float
    intraday: PricePrediction | None = None
    dayend: PricePrediction | None = None
    nextday: PricePrediction | None = None
    longterm: PricePrediction | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class ParsedUserQuery:
    raw_query: str
    normalized_query: str
    question_type: str
    horizon: str
    risk_profile: str
    wants_discovery: bool
    strategy_style: str = "general"
    symbol_hint: str | None = None
    stock_name_hint: str | None = None
    has_position_hint: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ExecutionPlan:
    use_market: bool
    use_sector: bool
    use_stock: bool
    use_timing: bool
    use_discovery: bool
    need_announcements: bool
    need_supplemental: bool
    need_sector_proxy: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceItem:
    category: str
    signal: str
    strength: float
    summary: str
    source: str
    verified: bool
    freshness: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ScoreCard:
    market_score: float
    sector_score: float
    stock_score: float
    timing_score: float
    risk_penalty: float
    missing_data_penalty: float
    coverage_score: float
    total_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ActionPlan:
    action: str
    label: str
    rationale: str
    position_guidance: str
    urgency: str
    urgency_score: float
    levels: dict[str, float] = field(default_factory=dict)
    trigger_conditions: list[str] = field(default_factory=list)
    invalidation_conditions: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    execution_brief: list[str] = field(default_factory=list)
    do_not: list[str] = field(default_factory=list)
    monitoring_focus: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StrategyProfile:
    style: str
    label: str
    policy_summary: str
    regime_overlay: str
    checklist: list[str] = field(default_factory=list)
    do_not: list[str] = field(default_factory=list)
    preferred_actions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CandidateIdea:
    code: str
    name: str
    decision: str
    trade_action: str
    total_score: float
    coverage_score: float
    market_score: float
    summary: str
    thesis: str
    catalysts: list[str]
    risks: list[str]
    trigger_conditions: list[str] = field(default_factory=list)
    invalidation_conditions: list[str] = field(default_factory=list)
    position_guidance: str = ""
    priority_score: float = 0.0
    action_plan: ActionPlan | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.action_plan is not None:
            payload["action_plan"] = self.action_plan.to_dict()
        return payload


@dataclass(slots=True)
class AdviceDecision:
    as_of: str
    question: ParsedUserQuery
    security: dict[str, Any]
    plan: ExecutionPlan
    decision: str
    confidence: float
    summary: str
    thesis: str
    scorecard: ScoreCard
    positive_factors: list[str]
    negative_factors: list[str]
    counter_evidence: list[str]
    missing_information: list[str]
    next_checks: list[str]
    trigger_conditions: list[str]
    invalidation_conditions: list[str]
    action_plan: ActionPlan
    strategy_profile: StrategyProfile
    position_guidance: str
    evidence_used: list[EvidenceItem]
    factor_analysis: dict[str, Any] = field(default_factory=dict)
    better_candidates: list[CandidateIdea] = field(default_factory=list)
    position_context: dict[str, Any] = field(default_factory=dict)
    pdf_insights: list[dict[str, Any]] = field(default_factory=list)
    artifacts: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["question"] = self.question.to_dict()
        payload["plan"] = self.plan.to_dict()
        payload["scorecard"] = self.scorecard.to_dict()
        payload["action_plan"] = self.action_plan.to_dict()
        payload["strategy_profile"] = self.strategy_profile.to_dict()
        payload["evidence_used"] = [item.to_dict() for item in self.evidence_used]
        payload["better_candidates"] = [item.to_dict() for item in self.better_candidates]
        return payload

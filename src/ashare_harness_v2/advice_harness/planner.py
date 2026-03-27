from __future__ import annotations

from .schemas import ExecutionPlan, ParsedUserQuery


def build_execution_plan(query: ParsedUserQuery, *, sector_available: bool, supplemental_available: bool) -> ExecutionPlan:
    return ExecutionPlan(
        use_market=True,
        use_sector=True,
        use_stock=True,
        use_timing=True,
        use_discovery=query.wants_discovery,
        need_announcements=True,
        need_supplemental=supplemental_available,
        need_sector_proxy=not sector_available,
    )

from __future__ import annotations

import re

from .schemas import ParsedUserQuery


DISCOVERY_KEYWORDS = ("还有哪些", "找几个", "推荐", "候选", "挑几个", "哪些可以买", "值得关注", "top ideas")
TREND_STYLE_KEYWORDS = ("趋势", "右侧", "突破", "放量突破", "强势跟随", "顺趋势")
PULLBACK_STYLE_KEYWORDS = ("回踩", "低吸", "左侧", "回调", "抄底", "回落再买", "等调整")
DEFENSIVE_STYLE_KEYWORDS = ("防守", "稳健", "保守", "低波", "高股息", "白马", "质量优先")

BUY_PATTERNS = (
    r"(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24}?)(?:今天|现在)?(?:能不能买|能买吗|可不可以买|值不值得买|该不该买|适合买入吗|适合买吗|还能买吗)",
    r"(?:想问下|想问|请问)(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24})(?:这只)?股票?(?:现在)?(?:能买吗|该不该买)",
)
ADD_PATTERNS = (
    r"(?:我|那我|请问)?(?:今天|现在)?(?:还)?(?:适不适合|适合不适合|适合|应该不应该|应该|能不能|能否|可不可以|可以)?(?:继续)?(?:加仓|补仓)(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24})(?:吗)?",
    r"(?:我|那我|请问)?(?:今天|现在)?(?:要不要|能不能|还能不能|还适不适合)?(?:继续)?(?:加仓|补仓)(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24})(?:吗)?",
    r"(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24}?)(?:今天|现在)?(?:应该)?(?:加仓|补仓|继续加仓|还能加吗|还能加仓吗|适合加仓吗)",
    r"(?:我持有|我的持仓|请问)(?P<subject>[A-Za-z0-9\u4e00-\u9fff]{2,24})(?:今天|现在)?(?:应该)?(?:加仓|补仓)",
)

CODE_PATTERN = re.compile(r"(?<!\d)([0-9]{6})(?!\d)")


def parse_user_query(query: str) -> ParsedUserQuery:
    normalized = " ".join(str(query or "").strip().split())
    compact = normalized.replace(" ", "")
    symbol_match = CODE_PATTERN.search(compact)
    subject = None
    question_type = "should_buy"
    if symbol_match is None:
        for pattern in ADD_PATTERNS:
            match = re.search(pattern, compact)
            if match:
                subject = clean_subject(match.group("subject"))
                if subject:
                    question_type = "add_position"
                    break
        for pattern in BUY_PATTERNS:
            match = re.search(pattern, compact)
            if match:
                subject = clean_subject(match.group("subject"))
                if subject:
                    break
    if symbol_match is None and subject is None:
        fallback = clean_subject(compact)
        if fallback and not any(token in fallback for token in ("能买吗", "加仓", "减仓", "买")):
            subject = fallback
    horizon = "swing"
    if any(keyword in compact for keyword in ("短线", "日内", "今天", "两三天")):
        horizon = "short_term"
    elif any(keyword in compact for keyword in ("中线", "一个季度", "三个月", "半年")):
        horizon = "medium_term"
    elif any(keyword in compact for keyword in ("长线", "长期", "一年", "两年")):
        horizon = "long_term"
    risk_profile = "balanced"
    if any(keyword in compact for keyword in ("保守", "稳一点", "低风险")):
        risk_profile = "conservative"
    elif any(keyword in compact for keyword in ("激进", "高风险", "冲一点")):
        risk_profile = "aggressive"
    wants_discovery = any(keyword in compact for keyword in DISCOVERY_KEYWORDS)
    strategy_style = detect_strategy_style(compact)
    has_position_hint = None
    if any(keyword in compact for keyword in ("我持有", "我的持仓", "继续拿", "要不要加仓", "减仓")):
        has_position_hint = True
    if "加仓" in compact or "补仓" in compact:
        question_type = "add_position"
        has_position_hint = True
    return ParsedUserQuery(
        raw_query=normalized,
        normalized_query=compact,
        question_type=question_type,
        horizon=horizon,
        risk_profile=risk_profile,
        wants_discovery=wants_discovery,
        strategy_style=strategy_style,
        symbol_hint=symbol_match.group(1) if symbol_match else None,
        stock_name_hint=subject,
        has_position_hint=has_position_hint,
    )


def detect_strategy_style(query: str) -> str:
    if any(keyword in query for keyword in TREND_STYLE_KEYWORDS):
        return "trend_following"
    if any(keyword in query for keyword in PULLBACK_STYLE_KEYWORDS):
        return "pullback_accumulation"
    if any(keyword in query for keyword in DEFENSIVE_STYLE_KEYWORDS):
        return "defensive_quality"
    return "general"


def clean_subject(value: str | None) -> str | None:
    subject = str(value or "").strip("，。！？?!. ")
    for prefix in ("我想问下", "我想问", "请问", "这个", "这只", "股票", "我现在适合", "我现在", "现在适合", "适合", "应该", "还能", "继续"):
        subject = subject.removeprefix(prefix)
    subject = subject.removesuffix("股票")
    for suffix in ("吗", "么", "嘛", "呢"):
        subject = subject.removesuffix(suffix)
    for token in ("怎么样", "咋样", "如何", "怎么办", "行不行", "可不可以"):
        subject = subject.removesuffix(token)
    for suffix in (
        "短线",
        "中线",
        "长线",
        "现在",
        "今天",
        "中期",
        "长期",
        "现在还能",
        "还能",
        "要不要",
        "该不该",
        "能不能",
        "适不适合",
    ):
        subject = subject.removesuffix(suffix)
    for token in (
        "想等回踩低吸",
        "稳健防守一点",
        "趋势跟随",
        "顺趋势",
        "右侧",
        "放量突破",
        "回踩低吸",
        "回踩",
        "低吸",
        "左侧",
        "防守",
        "稳健",
        "现在怎么看",
        "怎么看",
        "怎么做",
    ):
        index = subject.find(token)
        if index >= 2:
            subject = subject[:index]
            break
    subject = subject.removesuffix("想等")
    subject = subject.strip()
    if len(subject) < 2:
        return None
    return subject

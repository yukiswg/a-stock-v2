from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

from ..utils import DEFAULT_HEADERS


def build_agent_context(
    *,
    as_of: str,
    holdings_payload: dict[str, Any],
    feature_payload: dict[str, Any],
    market_payload: dict[str, Any],
    news_payload: list[dict[str, Any]],
    announcement_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "as_of": as_of,
        "market": market_payload,
        "holdings": holdings_payload,
        "features": feature_payload,
        "news": news_payload[:8],
        "announcements": announcement_payload[:8],
        "task": (
            "在严格结构化约束下，给出今天的市场摘要、重点风险、最重要观察对象。"
            "不能扩展动作空间，不能输出散文式长文。"
        ),
    }


def run_structured_agent(llm_config: dict[str, Any], *, context: dict[str, Any]) -> dict[str, Any]:
    if bool(llm_config.get("enabled")) and llm_config.get("url") and llm_config.get("api_key"):
        try:
            output = call_remote_agent(llm_config, context=context)
            return validate_agent_output(output, provider=str(llm_config.get("provider") or "generic_chat"), model=str(llm_config.get("model") or "unknown"))
        except Exception as exc:
            return fallback_agent_output(context=context, error=str(exc))
    return fallback_agent_output(context=context, error=None)


def call_remote_agent(llm_config: dict[str, Any], *, context: dict[str, Any]) -> dict[str, Any]:
    system_prompt = (
        "你是 A 股研究监控系统中的 LLM agent。"
        "你只能在既有 harness 提供的上下文里输出结构化 JSON，"
        "字段必须是 market_summary, primary_focus, risk_flags, confidence_note。"
        "每个字段都必须简洁。"
    )
    body = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(context, ensure_ascii=False)},
        ],
        "temperature": float(llm_config.get("temperature", 0.15)),
        "max_completion_tokens": int(llm_config.get("max_completion_tokens", 1400)),
        "response_format": {"type": "json_object"},
    }
    request = urllib.request.Request(
        str(llm_config["url"]),
        data=json.dumps(body).encode("utf-8"),
        headers={
            **DEFAULT_HEADERS,
            "Content-Type": "application/json",
            "api-key": str(llm_config["api_key"]),
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=int(llm_config.get("timeout_seconds", 30))) as response:
        payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    content = payload["choices"][0]["message"]["content"]
    return json.loads(extract_json_object(content) or content)


def validate_agent_output(payload: dict[str, Any], *, provider: str, model: str) -> dict[str, Any]:
    required_keys = {"market_summary", "primary_focus", "risk_flags", "confidence_note"}
    missing = sorted(required_keys.difference(payload))
    if missing:
        raise ValueError(f"agent output missing keys: {missing}")
    if not isinstance(payload.get("risk_flags"), list):
        raise ValueError("risk_flags must be list")
    validated = {
        "market_summary": str(payload["market_summary"]).strip(),
        "primary_focus": str(payload["primary_focus"]).strip(),
        "risk_flags": [str(item).strip() for item in payload.get("risk_flags", [])][:4],
        "confidence_note": str(payload["confidence_note"]).strip(),
        "_meta": {"provider": provider, "model": model, "validated": True},
    }
    return validated


def fallback_agent_output(*, context: dict[str, Any], error: str | None) -> dict[str, Any]:
    market = context.get("market", {})
    watch_features = list((context.get("features") or {}).values())
    focus = watch_features[0]["name"] if watch_features else "指数强弱"
    market_label = market.get("metadata", {}).get("label") or market.get("action") or "震荡"
    result = {
        "market_summary": f"{market_label}环境下，优先执行既有仓位和观察名单，不扩大动作空间。",
        "primary_focus": f"先看 {focus} 是否出现量价确认。",
        "risk_flags": [
            "新闻和公告只做增量信号，不替代正文核验。",
            "盘中若指数转弱，单票告警价值需要折价。",
        ],
        "confidence_note": "当前为 harness 回退摘要，结构化字段仍可用于首页和复盘。",
        "_meta": {"provider": "fallback", "model": "rules", "validated": True, "error": error},
    }
    return result


def extract_json_object(content: str) -> str | None:
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    return content[start : end + 1]

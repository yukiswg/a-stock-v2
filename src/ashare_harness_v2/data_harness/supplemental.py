from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from ..utils import ensure_dir, load_json, write_json


SUPPLEMENTAL_STATE_FILES = {
    "fundamentals": "fundamentals.json",
    "valuation": "valuation.json",
    "capital_flow": "capital_flow.json",
    "external_analysis": "external_analysis.json",
    "company_info": "company_info.json",
    "sector_map": "sector_map.json",
    "sector_metrics": "sector_metrics.json",
}
CODE_BUCKETS = ("fundamentals", "valuation", "capital_flow", "external_analysis", "company_info", "sector_map")
SUPPORTED_STOCK_PREFIXES = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
DEFAULT_SUPPLEMENTAL_SCRIPTS = {
    "akshare_bridge": "scripts/fetch_akshare_supplemental.py",
    "qstock_bridge": "scripts/fetch_qstock_supplemental.py",
    "capitalfarmer_bridge": "scripts/fetch_capitalfarmer_supplemental.py",
}


def supplemental_enabled(config: dict[str, Any]) -> bool:
    return bool((config.get("supplemental") or {}).get("enabled", False))


def ensure_supplemental_payload(
    config: dict[str, Any],
    *,
    as_of: str,
    codes: list[str],
    refresh: bool = False,
) -> dict[str, Any]:
    payload = load_supplemental_payload(config, as_of=as_of)
    if not supplemental_enabled(config):
        return payload
    requested = filter_supplemental_codes(codes)
    if not requested:
        return payload
    settings = config.get("supplemental") or {}
    if not refresh and not settings.get("refresh_on_missing", True):
        return payload
    missing = requested if refresh else [code for code in requested if is_code_missing(payload, code)]
    if not missing and payload.get("sector_metrics"):
        return payload

    live_payload = run_supplemental_bridge(config, as_of=as_of, codes=missing or requested)
    if not live_payload:
        return payload
    merged = merge_payloads(payload, live_payload)
    persist_supplemental_payload(config, as_of=as_of, payload=merged)
    return merged


def ensure_sector_metrics_payload(
    config: dict[str, Any],
    *,
    as_of: str,
    refresh: bool = False,
) -> dict[str, Any]:
    payload = load_supplemental_payload(config, as_of=as_of)
    if not supplemental_enabled(config):
        return payload
    if payload.get("sector_metrics") and not refresh:
        return payload
    live_payload = run_supplemental_bridge(config, as_of=as_of, codes=[])
    if not live_payload:
        return payload
    merged = merge_payloads(payload, live_payload)
    persist_supplemental_payload(config, as_of=as_of, payload=merged)
    return merged


def load_supplemental_payload(config: dict[str, Any], *, as_of: str) -> dict[str, Any]:
    state_root = Path(config["project"]["state_dir"]) / as_of
    payload: dict[str, Any] = {}
    for key, filename in SUPPLEMENTAL_STATE_FILES.items():
        payload[key] = load_json(state_root / filename, default={}) or {}
    return payload


def persist_supplemental_payload(config: dict[str, Any], *, as_of: str, payload: dict[str, Any]) -> None:
    state_root = ensure_dir(Path(config["project"]["state_dir"]) / as_of)
    for key, filename in SUPPLEMENTAL_STATE_FILES.items():
        bucket = payload.get(key) or {}
        write_json(state_root / filename, bucket)


def run_supplemental_bridge(config: dict[str, Any], *, as_of: str, codes: list[str]) -> dict[str, Any]:
    settings = config.get("supplemental") or {}
    repo_root = Path(__file__).resolve().parents[3]
    filtered_codes = dedupe_codes(codes)
    merged: dict[str, Any] = {}
    for provider, python_path, script_path in resolve_bridge_specs(repo_root, settings):
        live_payload = invoke_bridge_script(
            provider=provider,
            python_path=python_path,
            script_path=script_path,
            repo_root=repo_root,
            as_of=as_of,
            codes=filtered_codes,
            timeout_seconds=float(settings.get("timeout_seconds") or 120.0),
        )
        if live_payload:
            merged = merge_payloads(merged, live_payload)
    return merged


def build_bridge_env() -> dict[str, str]:
    env: dict[str, str] = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin"),
        "HOME": os.environ.get("HOME", str(Path.home())),
        "TMPDIR": os.environ.get("TMPDIR", "/tmp"),
        "LANG": os.environ.get("LANG", "en_US.UTF-8"),
        "LC_ALL": os.environ.get("LC_ALL", "en_US.UTF-8"),
    }
    for key in ("SSL_CERT_FILE", "SSL_CERT_DIR"):
        value = os.environ.get(key)
        if value:
            env[key] = value
    return env


def merge_payloads(current: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for key in SUPPLEMENTAL_STATE_FILES:
        left = current.get(key) or {}
        right = incoming.get(key) or {}
        bucket = merge_nested_dicts(left, right)
        merged[key] = bucket
    extra_keys = set(current).union(incoming).difference(SUPPLEMENTAL_STATE_FILES)
    for key in extra_keys:
        left = current.get(key)
        right = incoming.get(key)
        if isinstance(left, dict) or isinstance(right, dict):
            bucket = merge_nested_dicts(left or {}, right or {})
            merged[key] = bucket
        elif right is not None:
            merged[key] = right
        elif left is not None:
            merged[key] = left
    return merged


def merge_nested_dicts(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    merged = dict(left or {})
    for key, value in (right or {}).items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = merge_nested_dicts(current, value)
        else:
            merged[key] = value
    return merged


def is_code_missing(payload: dict[str, Any], code: str) -> bool:
    for bucket in CODE_BUCKETS:
        rows = payload.get(bucket) or {}
        if code not in rows:
            return True
    return False


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


def filter_supplemental_codes(values: list[str]) -> list[str]:
    return dedupe_codes([code for code in values if is_likely_stock_code(code)])


def is_likely_stock_code(code: str) -> bool:
    value = str(code or "").strip()
    if len(value) != 6 or not value.isdigit():
        return False
    return value.startswith(SUPPORTED_STOCK_PREFIXES)


def resolve_bridge_specs(repo_root: Path, settings: dict[str, Any]) -> list[tuple[str, Path, Path]]:
    python_setting = str(settings.get("python") or ".venv/bin/python")
    python_path = Path(python_setting)
    if not python_path.is_absolute():
        python_path = repo_root / python_setting
    if not python_path.exists():
        return []
    provider_names = parse_provider_names(settings)
    provider_scripts = settings.get("provider_scripts") or {}
    legacy_script = str(settings.get("script") or DEFAULT_SUPPLEMENTAL_SCRIPTS["akshare_bridge"])
    specs: list[tuple[str, Path, Path]] = []
    for index, provider in enumerate(provider_names):
        script_value = provider_scripts.get(provider)
        if not script_value:
            if index == 0 and "script" in settings:
                script_value = legacy_script
            else:
                script_value = DEFAULT_SUPPLEMENTAL_SCRIPTS.get(provider)
        if not script_value:
            continue
        script_path = Path(str(script_value))
        if not script_path.is_absolute():
            script_path = repo_root / str(script_value)
        if not script_path.exists():
            continue
        specs.append((provider, python_path, script_path))
    return specs


def parse_provider_names(settings: dict[str, Any]) -> list[str]:
    raw = settings.get("providers")
    if isinstance(raw, list):
        values = [str(item or "").strip() for item in raw]
    elif isinstance(raw, str):
        values = split_provider_names(raw)
    else:
        values = split_provider_names(str(settings.get("provider") or "akshare_bridge"))
    fallback_raw = settings.get("fallback_providers") or []
    if isinstance(fallback_raw, str):
        fallback_values = split_provider_names(fallback_raw)
    else:
        fallback_values = [str(item or "").strip() for item in fallback_raw]
    return dedupe_codes([item for item in [*values, *fallback_values] if item])


def split_provider_names(raw: str) -> list[str]:
    value = str(raw or "").strip()
    if not value:
        return []
    for token in ("+", ",", ";", "|"):
        value = value.replace(token, " ")
    return [item.strip() for item in value.split() if item.strip()]


def invoke_bridge_script(
    *,
    provider: str,
    python_path: Path,
    script_path: Path,
    repo_root: Path,
    as_of: str,
    codes: list[str],
    timeout_seconds: float,
) -> dict[str, Any]:
    command = [str(python_path), str(script_path), "--as-of", as_of]
    if codes:
        command.extend(["--codes", *codes])
    env = build_bridge_env()
    try:
        completed = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            env=env,
            timeout=max(timeout_seconds, 5.0),
        )
    except subprocess.TimeoutExpired:
        return {"errors": {provider: [f"{provider}: timeout"]}}
    except OSError as exc:
        return {"errors": {provider: [f"{provider}: {type(exc).__name__}: {exc}"]}}
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip() or f"{provider}: bridge failed"
        return {"errors": {provider: [message]}}
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {"errors": {provider: [f"{provider}: invalid json payload"]}}
    return payload if isinstance(payload, dict) else {"errors": {provider: [f"{provider}: invalid payload type"]}}

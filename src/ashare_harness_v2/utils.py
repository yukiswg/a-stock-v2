from __future__ import annotations

import json
import os
import random
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def ensure_dir(path: str | Path) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output


def read_text(path: str | Path) -> str:
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str | Path, content: str) -> Path:
    output = Path(path)
    ensure_dir(output.parent)
    output.write_text(content, encoding="utf-8")
    return output


def json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def write_json(path: str | Path, payload: Any) -> Path:
    output = Path(path)
    ensure_dir(output.parent)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")
    return output


def append_jsonl(path: str | Path, rows: list[Any]) -> Path:
    output = Path(path)
    ensure_dir(output.parent)
    with output.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, default=json_default) + "\n")
    return output


def load_json(path: str | Path, default: Any = None) -> Any:
    target = Path(path)
    if not target.exists():
        return default
    return json.loads(target.read_text(encoding="utf-8"))


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    return [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def today_stamp() -> str:
    return date.today().isoformat()


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def dated_dir(root: str | Path, as_of: str, *parts: str) -> Path:
    return ensure_dir(Path(root) / as_of / Path(*parts))


def safe_float(value: Any) -> float | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def safe_int(value: Any) -> int | None:
    if value in (None, "", "-", "--"):
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def pct_change(current: float | None, base: float | None) -> float | None:
    if current is None or base in (None, 0):
        return None
    return (current / base) - 1.0


def moving_average(values: list[float], window: int) -> float | None:
    if len(values) < window or window <= 0:
        return None
    sample = values[-window:]
    return sum(sample) / len(sample)


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def average(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def slugify(value: str) -> str:
    filtered = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    compact = "_".join(part for part in filtered.split("_") if part)
    return compact[:120] or "item"


def fetch_url(url: str, *, timeout: int = 20, retries: int = 3, headers: dict[str, str] | None = None) -> str:
    request = urllib.request.Request(url, headers={**DEFAULT_HEADERS, **(headers or {})})
    with urlopen_with_retries(request, timeout=timeout, retries=retries) as response:
        data = response.read()
    return data.decode("utf-8", errors="ignore")


def urlopen_with_retries(
    request: urllib.request.Request | str,
    *,
    timeout: int = 20,
    retries: int = 3,
    backoff_seconds: float = 1.0,
):
    last_error: Exception | None = None
    for attempt in range(max(retries, 1)):
        try:
            return urllib.request.urlopen(request, timeout=timeout)
        except Exception as exc:  # pragma: no cover - network failures are environment-specific
            last_error = exc
            if attempt + 1 >= retries:
                break
            sleep_seconds = backoff_seconds * (attempt + 1) + random.uniform(0, 0.3)
            time.sleep(sleep_seconds)
    if last_error is None:
        raise RuntimeError("urlopen_with_retries exhausted without error state")
    raise last_error


def resolve_url(base_url: str, href: str) -> str:
    return urllib.parse.urljoin(base_url, href)


def normalize_whitespace(value: str) -> str:
    return " ".join(str(value or "").split())


def extract_meta_content(html: str, meta_name: str) -> str | None:
    lower_name = meta_name.lower()
    for marker in (f'name="{lower_name}"', f"property=\"{lower_name}\"", f"content=\""):
        if marker not in html.lower():
            continue
    import re

    match = re.search(
        rf"<meta[^>]+(?:name|property)=[\"']{re.escape(meta_name)}[\"'][^>]+content=[\"']([^\"']+)[\"']",
        html,
        flags=re.I,
    )
    return match.group(1).strip() if match else None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def seconds_between(newer: str | None, older: str | None) -> float | None:
    newer_dt = parse_iso_datetime(newer)
    older_dt = parse_iso_datetime(older)
    if newer_dt is None or older_dt is None:
        return None
    return (newer_dt - older_dt).total_seconds()


def repo_relative_env_path() -> str:
    return os.getcwd()

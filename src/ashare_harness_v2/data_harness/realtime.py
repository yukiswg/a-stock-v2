from __future__ import annotations

import csv
import json
import math
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from ..models import QuoteSnapshot
from ..utils import DEFAULT_HEADERS, safe_float, seconds_between, urlopen_with_retries


REALTIME_FIELDS = "f12,f14,f2,f3,f4,f5,f6,f8,f15,f16,f17,f18,f20,f21,f22,f124"
ULIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
TENCENT_URL = "https://qt.gtimg.cn/q="
SINA_URL = "https://hq.sinajs.cn/list="
INDEX_SECID_MAP = {
    "000001": "1.000001",
    "000300": "1.000300",
    "000688": "1.000688",
    "399001": "0.399001",
    "399006": "0.399006",
}


class LiveQuoteSource:
    def __init__(self, *, stale_after_seconds: int = 90) -> None:
        self.stale_after_seconds = stale_after_seconds

    def fetch(self, codes: list[str]) -> list[QuoteSnapshot]:
        unique_codes = sorted({code for code in codes if code})
        if not unique_codes:
            return []
        snapshots = self._safe_fetch(self._fetch_eastmoney, unique_codes)
        missing = [code for code in unique_codes if code not in snapshots or snapshots[code].is_stale]
        if missing:
            for code, snapshot in self._safe_fetch(self._fetch_tencent, missing).items():
                snapshots[code] = fresher_snapshot(snapshots.get(code), snapshot)
        missing = [code for code in unique_codes if code not in snapshots or snapshots[code].is_stale]
        if missing:
            for code, snapshot in self._safe_fetch(self._fetch_sina, missing).items():
                snapshots[code] = fresher_snapshot(snapshots.get(code), snapshot)
        return [snapshots[code] for code in unique_codes if code in snapshots]

    def _safe_fetch(self, fn, codes: list[str]) -> dict[str, QuoteSnapshot]:
        try:
            return fn(codes)
        except Exception:
            return {}

    def _fetch_eastmoney(self, codes: list[str]) -> dict[str, QuoteSnapshot]:
        secids = ",".join(realtime_code_to_secid(code) for code in codes)
        params = urllib.parse.urlencode({"fltt": "2", "invt": "2", "fields": REALTIME_FIELDS, "secids": secids, "pn": "1", "pz": str(len(codes))})
        request = urllib.request.Request(
            f"{ULIST_URL}?{params}",
            headers={**DEFAULT_HEADERS, "Referer": "https://quote.eastmoney.com/"},
        )
        fetched_at = datetime.now().isoformat(timespec="seconds")
        with urlopen_with_retries(request, timeout=20, retries=3, backoff_seconds=1.0) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
        rows = (((payload.get("data") or {}).get("diff")) or [])
        result: dict[str, QuoteSnapshot] = {}
        for row in rows:
            snapshot = normalize_eastmoney_row(row, fetched_at=fetched_at, stale_after_seconds=self.stale_after_seconds)
            if snapshot.code:
                result[snapshot.code] = snapshot
        return result

    def _fetch_tencent(self, codes: list[str]) -> dict[str, QuoteSnapshot]:
        request = urllib.request.Request(
            f"{TENCENT_URL}{','.join(to_vendor_code(code) for code in codes)}",
            headers={**DEFAULT_HEADERS, "Referer": "https://gu.qq.com/"},
        )
        fetched_at = datetime.now().isoformat(timespec="seconds")
        with urlopen_with_retries(request, timeout=20, retries=2, backoff_seconds=0.8) as response:
            text = response.read().decode("gbk", errors="ignore")
        result: dict[str, QuoteSnapshot] = {}
        for line in text.split(";"):
            snapshot = parse_tencent_line(line.strip(), fetched_at=fetched_at, stale_after_seconds=self.stale_after_seconds)
            if snapshot is not None:
                result[snapshot.code] = snapshot
        return result

    def _fetch_sina(self, codes: list[str]) -> dict[str, QuoteSnapshot]:
        request = urllib.request.Request(
            f"{SINA_URL}{','.join(to_vendor_code(code) for code in codes)}",
            headers={**DEFAULT_HEADERS, "Referer": "https://finance.sina.com.cn/"},
        )
        fetched_at = datetime.now().isoformat(timespec="seconds")
        with urlopen_with_retries(request, timeout=20, retries=2, backoff_seconds=0.8) as response:
            text = response.read().decode("gbk", errors="ignore")
        result: dict[str, QuoteSnapshot] = {}
        for line in text.split(";"):
            snapshot = parse_sina_line(line.strip(), fetched_at=fetched_at, stale_after_seconds=self.stale_after_seconds)
            if snapshot is not None:
                result[snapshot.code] = snapshot
        return result


class ReplayQuoteSource:
    def __init__(self, path: str | Path, *, stale_after_seconds: int = 90) -> None:
        self.path = Path(path)
        self.stale_after_seconds = stale_after_seconds

    def fetch_batches(self) -> list[list[QuoteSnapshot]]:
        grouped: dict[str, list[QuoteSnapshot]] = defaultdict(list)
        if self.path.suffix.lower() == ".csv":
            with self.path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    snapshot = QuoteSnapshot(
                        code=str(row.get("code") or ""),
                        name=str(row.get("name") or row.get("code") or ""),
                        timestamp=str(row.get("timestamp") or ""),
                        fetched_at=str(row.get("fetched_at") or row.get("timestamp") or ""),
                        freshness_seconds=safe_float(row.get("freshness_seconds")),
                        is_stale=str(row.get("is_stale") or "").lower() == "true",
                        last_price=float(row.get("last_price") or 0.0),
                        prev_close=float(row.get("prev_close") or row.get("last_price") or 0.0),
                        open_price=safe_float(row.get("open_price")),
                        high_price=safe_float(row.get("high_price")),
                        low_price=safe_float(row.get("low_price")),
                        volume=safe_float(row.get("volume")),
                        amount=safe_float(row.get("amount")),
                        turnover=safe_float(row.get("turnover")),
                        source=str(row.get("source") or "replay_csv"),
                        trade_date=str(row.get("trade_date") or str(row.get("timestamp") or "")[:10]),
                    )
                    grouped[snapshot.timestamp].append(snapshot)
        else:
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                snapshot = QuoteSnapshot(**payload)
                grouped[snapshot.timestamp].append(snapshot)
        return [grouped[key] for key in sorted(grouped)]


class SyntheticQuoteSource:
    def __init__(
        self,
        *,
        stale_after_seconds: int = 90,
        base_price_map: dict[str, float] | None = None,
        name_map: dict[str, str] | None = None,
    ) -> None:
        self.stale_after_seconds = stale_after_seconds
        self.counter = 0
        self.base_price_map = {str(code): float(price) for code, price in (base_price_map or {}).items() if isinstance(price, (int, float)) and float(price) > 0}
        self.name_map = {str(code): str(name) for code, name in (name_map or {}).items() if str(code)}

    def fetch(self, codes: list[str]) -> list[QuoteSnapshot]:
        now = datetime.now().replace(microsecond=0) + timedelta(seconds=self.counter * 10)
        self.counter += 1
        rows: list[QuoteSnapshot] = []
        for index, code in enumerate(sorted(set(codes))):
            base = float(self.base_price_map.get(code) or (10.0 + index * 5))
            amplitude = 0.03 if index % 2 == 0 else 0.022
            last_price = round(base * (1 + math.sin(self.counter + index) * amplitude), 3)
            prev_close = round(base, 3)
            fetched_at = now.isoformat()
            rows.append(
                QuoteSnapshot(
                    code=code,
                    name=self.name_map.get(code) or code,
                    timestamp=fetched_at,
                    fetched_at=fetched_at,
                    freshness_seconds=0.0,
                    is_stale=False,
                    last_price=last_price,
                    prev_close=prev_close,
                    open_price=prev_close,
                    high_price=max(last_price, prev_close),
                    low_price=min(last_price, prev_close),
                    volume=(100000 + index * 1000) * (1 + abs(math.cos(self.counter + index)) * 1.8),
                    amount=((100000 + index * 1000) * (1 + abs(math.cos(self.counter + index)) * 1.8)) * last_price,
                    turnover=0.02,
                    source="synthetic",
                    trade_date=fetched_at[:10],
                )
            )
        return rows


def normalize_eastmoney_row(row: dict[str, object], *, fetched_at: str, stale_after_seconds: int) -> QuoteSnapshot:
    last_price = safe_float(row.get("f2")) or 0.0
    pct_value = safe_float(row.get("f3"))
    prev_close = safe_float(row.get("f18")) or (last_price / (1 + pct_value / 100) if pct_value not in (None, -100) else last_price)
    timestamp = parse_eastmoney_timestamp(row.get("f124")) or fetched_at
    freshness = freshness_seconds(timestamp, fetched_at)
    return QuoteSnapshot(
        code=str(row.get("f12") or ""),
        name=str(row.get("f14") or ""),
        timestamp=timestamp,
        fetched_at=fetched_at,
        freshness_seconds=freshness,
        is_stale=bool(freshness is not None and freshness > stale_after_seconds),
        last_price=last_price,
        prev_close=prev_close or 0.0,
        open_price=safe_float(row.get("f17")),
        high_price=safe_float(row.get("f15")),
        low_price=safe_float(row.get("f16")),
        volume=safe_float(row.get("f5")),
        amount=safe_float(row.get("f6")),
        turnover=(safe_float(row.get("f8")) or 0.0) / 100 if row.get("f8") not in (None, "-") else None,
        source="eastmoney_realtime",
        trade_date=timestamp[:10],
    )


def parse_tencent_line(line: str, *, fetched_at: str, stale_after_seconds: int) -> QuoteSnapshot | None:
    if not line or '"' not in line:
        return None
    payload = line.split('"', 1)[1].rsplit('"', 1)[0]
    parts = payload.split("~")
    if len(parts) < 38 or not parts[2]:
        return None
    timestamp = parse_compact_timestamp(parts[30]) or fetched_at
    freshness = freshness_seconds(timestamp, fetched_at)
    composite = parts[35].split("/") if len(parts) > 35 and "/" in parts[35] else []
    volume = safe_float(composite[1]) if len(composite) >= 2 else safe_float(parts[36] if len(parts) > 36 else None)
    amount = safe_float(composite[2]) if len(composite) >= 3 else safe_float(parts[37] if len(parts) > 37 else None)
    turnover = safe_float(parts[38] if len(parts) > 38 else None)
    return QuoteSnapshot(
        code=parts[2],
        name=parts[1],
        timestamp=timestamp,
        fetched_at=fetched_at,
        freshness_seconds=freshness,
        is_stale=bool(freshness is not None and freshness > stale_after_seconds),
        last_price=safe_float(parts[3]) or 0.0,
        prev_close=safe_float(parts[4]) or 0.0,
        open_price=safe_float(parts[5]),
        high_price=safe_float(parts[33] if len(parts) > 33 else None),
        low_price=safe_float(parts[34] if len(parts) > 34 else None),
        volume=volume,
        amount=amount,
        turnover=turnover / 100 if turnover is not None and turnover > 1 else turnover,
        source="tencent_realtime",
        trade_date=timestamp[:10],
    )


def parse_sina_line(line: str, *, fetched_at: str, stale_after_seconds: int) -> QuoteSnapshot | None:
    if not line or '"' not in line:
        return None
    payload = line.split('"', 1)[1].rsplit('"', 1)[0]
    parts = payload.split(",")
    if len(parts) < 32 or not parts[0]:
        return None
    timestamp = parse_sina_timestamp(parts[30], parts[31]) or fetched_at
    freshness = freshness_seconds(timestamp, fetched_at)
    return QuoteSnapshot(
        code=extract_vendor_code(line),
        name=parts[0].strip(),
        timestamp=timestamp,
        fetched_at=fetched_at,
        freshness_seconds=freshness,
        is_stale=bool(freshness is not None and freshness > stale_after_seconds),
        last_price=safe_float(parts[3]) or 0.0,
        prev_close=safe_float(parts[2]) or 0.0,
        open_price=safe_float(parts[1]),
        high_price=safe_float(parts[4]),
        low_price=safe_float(parts[5]),
        volume=safe_float(parts[8]),
        amount=safe_float(parts[9]),
        turnover=None,
        source="sina_realtime",
        trade_date=timestamp[:10],
    )


def fresher_snapshot(current: QuoteSnapshot | None, candidate: QuoteSnapshot | None) -> QuoteSnapshot:
    if current is None:
        if candidate is None:
            raise ValueError("candidate snapshot missing")
        return candidate
    if candidate is None:
        return current
    if candidate.timestamp > current.timestamp:
        return candidate
    if candidate.timestamp < current.timestamp:
        return current
    current_freshness = current.freshness_seconds if current.freshness_seconds is not None else float("inf")
    candidate_freshness = candidate.freshness_seconds if candidate.freshness_seconds is not None else float("inf")
    return candidate if candidate_freshness < current_freshness else current


def parse_eastmoney_timestamp(value: object) -> str | None:
    try:
        raw = int(float(value))
    except (TypeError, ValueError):
        return None
    if raw <= 0:
        return None
    if raw > 10_000_000_000:
        raw //= 1000
    return datetime.fromtimestamp(raw).isoformat(timespec="seconds")


def parse_compact_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if len(text) != 14 or not text.isdigit():
        return None
    return datetime.strptime(text, "%Y%m%d%H%M%S").isoformat(timespec="seconds")


def parse_sina_timestamp(date_value: str | None, time_value: str | None) -> str | None:
    if not date_value or not time_value:
        return None
    try:
        return datetime.strptime(f"{date_value.strip()} {time_value.strip()}", "%Y-%m-%d %H:%M:%S").isoformat(timespec="seconds")
    except ValueError:
        return None


def extract_vendor_code(line: str) -> str:
    if "hq_str_" not in line:
        return ""
    segment = line.split("hq_str_", 1)[1].split("=", 1)[0].strip()
    if segment.startswith(("sh", "sz", "bj")):
        return segment[2:]
    return segment


def to_vendor_code(code: str) -> str:
    if code.startswith(("sh", "sz", "bj")):
        return code
    secid = realtime_code_to_secid(code)
    if secid.startswith("1."):
        return f"sh{code}"
    if secid.startswith("2."):
        return f"bj{code}"
    return f"sz{code}"


def realtime_code_to_secid(code: str) -> str:
    if code in INDEX_SECID_MAP:
        return INDEX_SECID_MAP[code]
    if code.startswith(("5", "6", "9", "11")):
        return f"1.{code}"
    if code.startswith("8"):
        return f"2.{code}"
    return f"0.{code}"


def freshness_seconds(timestamp: str | None, fetched_at: str | None) -> float | None:
    delta = seconds_between(fetched_at, timestamp)
    return max(delta, 0.0) if delta is not None else None

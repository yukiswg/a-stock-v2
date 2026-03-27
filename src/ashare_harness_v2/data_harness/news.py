from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from ..models import AnnouncementItem, HoldingPosition, NewsItem
from ..utils import DEFAULT_HEADERS, extract_meta_content, fetch_url, normalize_whitespace, resolve_url


TAG_MAPPING = {
    "证监会": "监管",
    "处罚": "处罚",
    "问询": "问询",
    "公告": "公告",
    "统计局": "宏观",
    "价格": "通胀",
    "指数": "宏观",
    "货币": "货币政策",
    "银行": "流动性",
}

CNINFO_BASE = "https://www.cninfo.com.cn/new"
CNINFO_STATIC_BASE = "https://static.cninfo.com.cn/"


def collect_news(sources: list[dict[str, Any]], *, max_items_per_source: int, max_age_days: int) -> list[NewsItem]:
    items: list[NewsItem] = []
    for source in sources:
        try:
            html = fetch_url(str(source["url"]))
        except Exception:
            continue
        items.extend(parse_news_items(source, html, limit=max_items_per_source, max_age_days=max_age_days))
    return items


def parse_news_items(source: dict[str, Any], html: str, *, limit: int, max_age_days: int) -> list[NewsItem]:
    pattern = re.compile(str(source["pattern"]), flags=re.S)
    seen: set[tuple[str, str]] = set()
    rows: list[NewsItem] = []
    for match in pattern.finditer(html):
        href = normalize_whitespace(match.group("href"))
        title = normalize_whitespace(strip_tags(match.group("title")))
        published_at = normalize_published_at(match.groupdict().get("date"))
        key = (href, title)
        if not href or not title or key in seen:
            continue
        seen.add(key)
        url = resolve_url(str(source["base_url"]), href)
        if published_at is None and source.get("article_date_meta"):
            published_at = fetch_article_published_at(url, str(source["article_date_meta"]))
        if not is_recent_enough(published_at, max_age_days=max_age_days):
            continue
        tags = [tag for keyword, tag in TAG_MAPPING.items() if keyword in title]
        rows.append(
            NewsItem(
                source_id=str(source["id"]),
                source_name=str(source["name"]),
                title=title,
                url=url,
                published_at=published_at,
                tags=tags,
            )
        )
        if len(rows) >= limit:
            break
    return rows


def fetch_holdings_announcements(positions: list[HoldingPosition], *, limit_per_stock: int) -> list[AnnouncementItem]:
    return fetch_security_announcements(
        [{"code": position.code, "name": position.name} for position in positions if position.code],
        limit_per_stock=limit_per_stock,
    )


def fetch_security_announcements(securities: list[dict[str, str]], *, limit_per_stock: int) -> list[AnnouncementItem]:
    rows: list[AnnouncementItem] = []
    for security_input in securities:
        code = str(security_input.get("code") or "").strip()
        name = str(security_input.get("name") or code).strip()
        if not code:
            continue
        security = resolve_security(code)
        if not security and name and name != code:
            security = resolve_security(name)
        if not security:
            continue
        try:
            items = query_announcements(str(security["code"]), str(security["orgId"]), page_size=limit_per_stock)
        except Exception:
            continue
        for item in items[:limit_per_stock]:
            title = str(item.get("announcementTitle") or item.get("shortTitle") or "").strip()
            if not title:
                continue
            rows.append(
                AnnouncementItem(
                    code=code,
                    name=name,
                    title=title,
                    url=CNINFO_STATIC_BASE + str(item.get("adjunctUrl", "")).lstrip("/"),
                    published_at=format_cninfo_timestamp(item.get("announcementTime")),
                )
            )
    return rows


def strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value)


def fetch_article_published_at(url: str, meta_name: str) -> str | None:
    try:
        html = fetch_url(url)
    except Exception:
        return None
    return normalize_published_at(extract_meta_content(html, meta_name))


def normalize_published_at(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", normalize_whitespace(value))
    return match.group(1) if match else None


def is_recent_enough(published_at: str | None, *, max_age_days: int, today: date | None = None) -> bool:
    if published_at is None:
        return False
    try:
        published = datetime.strptime(published_at[:10], "%Y-%m-%d").date()
    except ValueError:
        return False
    current = today or datetime.now().date()
    return published >= current - timedelta(days=max_age_days)


@lru_cache(maxsize=512)
def resolve_security(keyword: str) -> dict[str, str] | None:
    payload = post_form(
        f"{CNINFO_BASE}/information/topSearch/query",
        {"keyWord": keyword, "maxNum": "10"},
        timeout=5.0,
    )
    data = json.loads(payload)
    for item in data:
        if item.get("code") == keyword and item.get("type") == "shj":
            return {"code": str(item["code"]), "orgId": str(item["orgId"]), "zwjc": str(item.get("zwjc") or "")}
    for item in data:
        if item.get("type") == "shj":
            return {"code": str(item["code"]), "orgId": str(item["orgId"]), "zwjc": str(item.get("zwjc") or "")}
    return None


def query_announcements(code: str, org_id: str, *, page_size: int) -> list[dict[str, object]]:
    payload = post_form(
        f"{CNINFO_BASE}/hisAnnouncement/query",
        {"stock": f"{code},{org_id}", "pageSize": str(page_size)},
    )
    data = json.loads(payload)
    rows = data.get("announcements", [])
    return rows if isinstance(rows, list) else []


def post_form(url: str, params: dict[str, str], *, timeout: float = 20.0) -> str:
    request = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(params).encode("utf-8"),
        headers={
            **DEFAULT_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{CNINFO_BASE}/fulltextSearch",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def format_cninfo_timestamp(raw_value: object) -> str | None:
    if raw_value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(raw_value) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return str(raw_value)

from __future__ import annotations

import hashlib
import re
import urllib.request
from pathlib import Path
from typing import Any

import pdfplumber
from pypdf import PdfReader

from ..utils import DEFAULT_HEADERS, ensure_dir, normalize_whitespace, urlopen_with_retries, write_json, write_text


NEGATIVE_KEYWORDS = ("风险", "亏损", "波动", "质押", "问询", "诉讼", "减持", "担保", "违约", "下滑")
POSITIVE_KEYWORDS = ("增长", "订单", "回购", "增持", "中标", "突破", "扩产", "高景气", "改善", "盈利")
NEUTRAL_TITLE_HINTS = ("综合授信", "授信额度", "董事会工作报告", "投资者关系活动记录表", "股东大会决议")
SEVERE_NEGATIVE_KEYWORDS = ("违约", "诉讼", "亏损", "减持", "质押", "问询", "处罚")


def analyze_announcement_pdfs(
    config: dict[str, Any],
    *,
    as_of: str,
    code: str,
    announcements: list[dict[str, Any]],
    limit: int = 2,
) -> dict[str, Any]:
    root = ensure_dir(Path(config["project"].get("pdf_dir") or "data/output/pdf") / as_of / code)
    insights: list[dict[str, Any]] = []
    for item in announcements[:limit]:
        url = str(item.get("url") or "")
        if not url.lower().endswith(".pdf"):
            continue
        title = str(item.get("title") or f"{code}_announcement")
        slug = _slug_from_url(url)
        pdf_path = root / f"{slug}.pdf"
        text_path = root / f"{slug}.txt"
        json_path = root / f"{slug}.json"
        try:
            if not pdf_path.exists():
                _download_pdf(url, pdf_path)
            text = extract_pdf_text(pdf_path, max_pages=5)
            if text:
                write_text(text_path, text)
            insight = summarize_pdf_text(
                title=title,
                text=text,
                published_at=str(item.get("published_at") or as_of),
                pdf_path=pdf_path,
                text_path=text_path,
            )
            insight["title"] = title
            insight["url"] = url
            write_json(json_path, insight)
            insights.append(insight)
        except Exception as exc:
            insights.append(
                {
                    "title": title,
                    "url": url,
                    "published_at": str(item.get("published_at") or as_of),
                    "signal": "neutral",
                    "strength": 35.0,
                    "summary": f"{title} PDF 深读失败：{exc}",
                    "key_lines": [],
                    "pdf_path": str(pdf_path),
                    "text_path": str(text_path),
                    "verified": False,
                }
            )
    payload = {"as_of": as_of, "code": code, "insights": insights}
    write_json(root / f"{code}_announcement_insights.json", payload)
    return payload


def extract_pdf_text(path: str | Path, *, max_pages: int = 5) -> str:
    target = Path(path)
    chunks: list[str] = []
    try:
        with pdfplumber.open(target) as handle:
            for page in handle.pages[:max_pages]:
                text = normalize_whitespace(page.extract_text() or "")
                if text:
                    chunks.append(text)
    except Exception:
        reader = PdfReader(str(target))
        for page in reader.pages[:max_pages]:
            text = normalize_whitespace(page.extract_text() or "")
            if text:
                chunks.append(text)
    return "\n".join(part for part in chunks if part)


def summarize_pdf_text(
    *,
    title: str,
    text: str,
    published_at: str,
    pdf_path: Path,
    text_path: Path,
) -> dict[str, Any]:
    lines = [normalize_whitespace(row) for row in re.split(r"[\n。；;]+", text or "") if normalize_whitespace(row)]
    key_lines: list[str] = []
    positive_hits = 0
    negative_hits = 0
    for line in lines:
        compact = compress_key_line(line)
        if is_noise_line(compact):
            continue
        if any(keyword in line for keyword in NEGATIVE_KEYWORDS):
            negative_hits += 1
            key_lines.append(compact)
        elif any(keyword in line for keyword in POSITIVE_KEYWORDS):
            positive_hits += 1
            key_lines.append(compact)
        if len(key_lines) >= 5:
            break
    if any(hint in title for hint in NEUTRAL_TITLE_HINTS) and not any(keyword in text for keyword in SEVERE_NEGATIVE_KEYWORDS):
        signal = "neutral"
        strength = 52.0
    elif negative_hits > positive_hits and negative_hits > 0:
        signal = "negative"
        strength = 32.0
    elif positive_hits > 0:
        signal = "positive"
        strength = 68.0
    else:
        signal = "neutral"
        strength = 52.0
    takeaway = derive_pdf_takeaway(title=title, signal=signal, key_lines=key_lines)
    summary = build_pdf_summary(title=title, takeaway=takeaway, published_at=published_at)
    return {
        "published_at": published_at,
        "signal": signal,
        "strength": strength,
        "summary": summary,
        "key_lines": key_lines[:4],
        "pdf_path": str(pdf_path),
        "text_path": str(text_path),
        "verified": True,
    }


def _download_pdf(url: str, path: Path) -> None:
    request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    with urlopen_with_retries(request, timeout=30, retries=3) as response:
        data = response.read()
    ensure_dir(path.parent)
    path.write_bytes(data)


def _slug_from_url(url: str) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return digest


def compress_key_line(value: str, *, limit: int = 56) -> str:
    compact = normalize_whitespace(value)
    compact = compact.replace("□", "").replace("■", "")
    for token in ("本公司董事会", "以下简称", "详见", "特此公告", "网站", "www.", "http://", "https://"):
        compact = compact.replace(token, "")
    compact = normalize_whitespace(compact)
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def is_noise_line(value: str) -> bool:
    if not value:
        return True
    noise_tokens = ("董事会", "特此公告", "网站", "http", "www.", "以下简称", "理性投资", "注意投资风险")
    return any(token in value for token in noise_tokens) and len(value) > 24


def derive_pdf_takeaway(*, title: str, signal: str, key_lines: list[str]) -> str:
    text = " ".join(key_lines).lower()
    if signal == "positive":
        if any(token in text for token in ("订单", "中标", "回购", "增持", "增长", "扩产", "盈利")):
            return "公告释放了偏正面的经营催化，但仍需等价格与量能确认。"
        return "公告信息偏正面，可作为加分项，但不足以单独触发交易。"
    if signal == "negative":
        if any(token in text for token in ("诉讼", "处罚", "问询", "违规", "调查")):
            return "监管或法律风险上升，需收缩预期并跟踪后续披露。"
        if any(token in text for token in ("减持", "质押")):
            return "股东层面信号偏负面，需防范估值与流动性压力。"
        if any(token in text for token in ("不确定", "审批", "进展", "投建周期", "风险提示")):
            return "关键事项推进存在不确定性，执行上应降低仓位和胜率预期。"
        if any(token in text for token in ("亏损", "下滑", "下降")):
            return "经营指标存在走弱信号，短期基本面承压。"
        return "公告中包含风险措辞，当前应保持防守式执行。"
    if any(hint in title for hint in NEUTRAL_TITLE_HINTS):
        return "本公告以常规披露为主，未形成明确交易催化。"
    return "本公告信息中性，主要用于更新事实背景，不改变当前交易计划。"


def build_pdf_summary(*, title: str, takeaway: str, published_at: str) -> str:
    return f"{published_at} PDF 深读：{title}。提炼结论：{takeaway}"

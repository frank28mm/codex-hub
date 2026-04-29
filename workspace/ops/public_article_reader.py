#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)
MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
WECHAT_RISK_CONTROL_MARKERS = (
    "环境异常",
    "完成验证后即可继续访问",
    "wappoc_appmsgcaptcha",
)
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36 CodexHubPublicArticleReader/1.0",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_READER_MODE = "http_html"
DEFAULT_BROWSER_FALLBACK_READER_MODE = "browser_opencli_read"


def trim_url_candidate(value: str) -> str:
    return str(value or "").strip().rstrip("，。；;,)】》」』")


def extract_urls(text: str, *, limit: int | None = None) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for match in URL_RE.findall(str(text or "")):
        url = trim_url_candidate(match)
        if not url or url in seen:
            continue
        seen.add(url)
        results.append(url)
        if limit is not None and len(results) >= limit:
            break
    return results


def should_auto_hydrate_url(url: str, *, allowed_domains: set[str] | None = None) -> bool:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc.strip():
        return False
    if not allowed_domains:
        return True
    return parsed.netloc.lower() in {domain.lower() for domain in allowed_domains}


def _soup_title(soup: BeautifulSoup, fallback: str) -> str:
    title = ""
    if soup.title and soup.title.text.strip():
        title = soup.title.text.strip()
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content", "").strip():
        title = og_title["content"].strip()
    return title or fallback


def _soup_description(soup: BeautifulSoup) -> str:
    og_description = soup.find("meta", attrs={"property": "og:description"})
    if og_description and og_description.get("content", "").strip():
        return og_description["content"].strip()
    description = soup.find("meta", attrs={"name": "description"})
    if description and description.get("content", "").strip():
        return description["content"].strip()
    return ""


def _text_excerpt(soup: BeautifulSoup) -> str:
    for selector in ("script", "style", "noscript", "header", "footer", "nav", "aside", "form"):
        for node in soup.select(selector):
            node.decompose()
    root = soup.find("article") or soup.find("main") or soup.body or soup
    parts: list[str] = []
    for node in root.find_all(["h1", "h2", "h3", "p", "li"], limit=180):
        text = " ".join(node.get_text(" ", strip=True).split())
        if len(text) < 16:
            continue
        lowered = text.lower()
        if any(blocked in lowered for blocked in ("cookie", "subscribe", "sign up", "advertisement", "sponsored")):
            continue
        parts.append(text)
    return "\n\n".join(parts[:60]).strip()


def _detect_blocked_reason(*, final_url: str, title: str, excerpt: str, raw_text: str) -> str:
    haystack = "\n".join([str(final_url or ""), str(title or ""), str(excerpt or ""), str(raw_text or "")]).lower()
    if any(marker.lower() in haystack for marker in WECHAT_RISK_CONTROL_MARKERS):
        return "blocked_by_wechat_risk_control"
    return ""


def _normalize_payload(url: str, payload: dict[str, Any], *, reader_mode: str) -> dict[str, Any]:
    normalized_url = str(url or "").strip()
    final_url = str(payload.get("final_url") or payload.get("fetched_url") or normalized_url).strip() or normalized_url
    title = str(payload.get("title") or "").strip() or normalized_url
    excerpt = str(payload.get("excerpt") or "").strip()
    blocked_reason = str(payload.get("blocked_reason") or "").strip()
    content_status = str(payload.get("content_status") or "").strip()
    if not content_status:
        if blocked_reason:
            content_status = "blocked"
        elif excerpt:
            content_status = "captured"
        else:
            content_status = "empty"
    return {
        "ok": content_status == "captured",
        "url": normalized_url,
        "domain": urlparse(final_url or normalized_url).netloc.lower(),
        "title": title,
        "excerpt": excerpt[:6000].strip(),
        "fetched_url": final_url,
        "final_url": final_url,
        "status_code": int(payload.get("status_code") or 200),
        "content_status": content_status,
        "blocked_reason": blocked_reason,
        "reader_mode": str(payload.get("reader_mode") or reader_mode).strip() or reader_mode,
        "fallback_used": bool(payload.get("fallback_used", False)),
        "fallback_attempted": bool(payload.get("fallback_attempted", False)),
        "fallback_error": str(payload.get("fallback_error") or "").strip(),
        "artifact_id": str(payload.get("artifact_id") or "").strip(),
        "artifact_root": str(payload.get("artifact_root") or "").strip(),
        "artifact_json_path": str(payload.get("artifact_json_path") or "").strip(),
        "artifact_markdown_path": str(payload.get("artifact_markdown_path") or "").strip(),
    }


def empty_summary() -> dict[str, Any]:
    return {
        "count": 0,
        "captured_count": 0,
        "blocked_count": 0,
        "fallback_used_count": 0,
        "fallback_attempted_count": 0,
        "fallback_error_count": 0,
        "artifact_count": 0,
        "reader_modes": [],
    }


def empty_hydration_payload(prompt: str = "") -> dict[str, Any]:
    return {
        "prompt": str(prompt or "").strip(),
        "augmented_prompt": str(prompt or "").strip(),
        "contexts": [],
        "summary": empty_summary(),
    }


def _fixture_payload(url: str) -> dict[str, Any] | None:
    raw = os.environ.get("WORKSPACE_HUB_PUBLIC_ARTICLE_FETCH_FIXTURE_JSON", "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return _normalize_payload(url, payload, reader_mode="fixture")


def _browser_fallback_fixture(url: str) -> dict[str, Any] | None:
    raw = os.environ.get("WORKSPACE_HUB_PUBLIC_ARTICLE_BROWSER_FALLBACK_FIXTURE_JSON", "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    normalized = _normalize_payload(url, payload, reader_mode="browser_fixture")
    normalized["fallback_used"] = True
    normalized["fallback_attempted"] = True
    normalized["blocked_reason"] = ""
    if normalized["content_status"] != "captured" and normalized["excerpt"]:
        normalized["content_status"] = "captured"
        normalized["ok"] = True
    return normalized


def artifacts_root() -> Path:
    configured = str(os.environ.get("WORKSPACE_HUB_PUBLIC_ARTICLE_ARTIFACTS_ROOT", "")).strip()
    if configured:
        return Path(configured)
    runtime_root = str(os.environ.get("WORKSPACE_HUB_RUNTIME_ROOT", "")).strip()
    if runtime_root:
        return Path(runtime_root) / "public-article-reader"
    return REPO_ROOT / "runtime" / "public-article-reader"


def _article_artifact_identity(url: str) -> tuple[str, Path]:
    normalized_url = str(url or "").strip()
    parsed = urlparse(normalized_url)
    domain = re.sub(r"[^a-z0-9]+", "-", parsed.netloc.lower()).strip("-") or "url"
    digest = hashlib.sha1(normalized_url.encode("utf-8")).hexdigest()[:12]
    artifact_id = f"public-article-{digest}"
    return artifact_id, artifacts_root() / domain / artifact_id


def _artifact_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# {str(payload.get('title') or payload.get('url') or 'Public Article').strip()}",
        "",
        "## Metadata",
        "",
        f"- url: `{str(payload.get('url') or '').strip()}`",
        f"- final_url: `{str(payload.get('final_url') or '').strip()}`",
        f"- domain: `{str(payload.get('domain') or '').strip()}`",
        f"- status_code: `{str(payload.get('status_code') or '')}`",
        f"- content_status: `{str(payload.get('content_status') or '').strip()}`",
        f"- blocked_reason: `{str(payload.get('blocked_reason') or '').strip() or 'n/a'}`",
        f"- reader_mode: `{str(payload.get('reader_mode') or '').strip()}`",
        f"- fallback_used: `{bool(payload.get('fallback_used'))}`",
        f"- fallback_attempted: `{bool(payload.get('fallback_attempted'))}`",
    ]
    fallback_error = str(payload.get("fallback_error") or "").strip()
    if fallback_error:
        lines.append(f"- fallback_error: `{fallback_error}`")
    lines.extend(
        [
            "",
            "## Excerpt",
            "",
            str(payload.get("excerpt") or "").strip() or "（当前未抓到可用正文摘录。）",
            "",
        ]
    )
    return "\n".join(lines)


def persist_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    artifact_id, root = _article_artifact_identity(str(normalized.get("url") or ""))
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "latest.json"
    markdown_path = root / "latest.md"
    normalized["artifact_id"] = artifact_id
    normalized["artifact_root"] = str(root.resolve())
    normalized["artifact_json_path"] = str(json_path.resolve())
    normalized["artifact_markdown_path"] = str(markdown_path.resolve())
    normalized["fetched_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    json_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(_artifact_markdown(normalized), encoding="utf-8")
    return normalized


def _markdown_title(markdown: str, fallback: str) -> str:
    for line in str(markdown or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip() or fallback
    return fallback


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _strip_markdown_artifacts(value: str) -> str:
    text = MARKDOWN_IMAGE_RE.sub(" ", value or "")
    text = re.sub(r"\[(.*?)\]\((?:https?:)?//[^)\s]+\)", r"\1", text)
    text = text.replace("[", " ").replace("]", " ")
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
    return _normalize_whitespace(text)


def _markdown_excerpt(markdown: str, *, max_chars: int = 6000) -> str:
    parts: list[str] = []
    for raw in str(markdown or "").splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped in {"Preview", "Navigation"}:
            continue
        normalized = _strip_markdown_artifacts(stripped)
        if len(normalized) < 12:
            continue
        parts.append(normalized)
        if len(" ".join(parts)) >= max_chars:
            break
    excerpt = "\n\n".join(parts[:60]).strip()
    return excerpt[:max_chars].strip()


def _should_try_browser_fallback(url: str, payload: dict[str, Any]) -> bool:
    if str(os.environ.get("WORKSPACE_HUB_PUBLIC_ARTICLE_ENABLE_BROWSER_FALLBACK", "1")).strip().lower() in {"0", "false", "no"}:
        return False
    if str(payload.get("blocked_reason") or "").strip() != "blocked_by_wechat_risk_control":
        return False
    return urlparse(str(url or "").strip()).netloc.lower() == "mp.weixin.qq.com"


def _browser_fallback_read(url: str, *, wait_seconds: int = 4) -> dict[str, Any]:
    fixture = _browser_fallback_fixture(url)
    if fixture is not None:
        return fixture
    try:
        from ops import opencli_agent
    except ImportError:  # pragma: no cover
        import opencli_agent  # type: ignore

    capture = opencli_agent._run_web_read_capture(url, wait_seconds=wait_seconds)
    markdown = str(capture.get("markdown") or "").strip()
    excerpt = _markdown_excerpt(markdown)
    title = _markdown_title(markdown, str(url or "").strip())
    return {
        "ok": bool(excerpt),
        "url": str(url).strip(),
        "domain": urlparse(str(url).strip()).netloc.lower(),
        "title": title,
        "excerpt": excerpt,
        "fetched_url": str(capture.get("url") or url).strip(),
        "final_url": str(capture.get("url") or url).strip(),
        "status_code": 200,
        "content_status": "captured" if excerpt else "empty",
        "blocked_reason": "",
        "reader_mode": DEFAULT_BROWSER_FALLBACK_READER_MODE,
        "fallback_used": True,
        "fallback_attempted": True,
        "fallback_error": "",
    }


def read_url(url: str, *, timeout: int = 25, persist_artifact_result: bool = False) -> dict[str, Any]:
    normalized_url = str(url or "").strip()
    if not normalized_url:
        raise ValueError("missing url")
    fixture = _fixture_payload(normalized_url)
    if fixture is not None:
        return persist_artifact(fixture) if persist_artifact_result else fixture

    response = requests.get(normalized_url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title = _soup_title(soup, normalized_url)
    description = _soup_description(soup)
    excerpt = _text_excerpt(soup)
    if description and description not in excerpt:
        excerpt = f"{description}\n\n{excerpt}".strip()
    blocked_reason = _detect_blocked_reason(
        final_url=response.url,
        title=title,
        excerpt=excerpt,
        raw_text=soup.get_text("\n", strip=True),
    )
    content_status = "blocked" if blocked_reason else ("captured" if excerpt else "empty")
    payload = {
        "ok": content_status == "captured",
        "url": normalized_url,
        "domain": urlparse(response.url or normalized_url).netloc.lower(),
        "title": title,
        "excerpt": excerpt[:6000].strip(),
        "fetched_url": response.url,
        "final_url": response.url,
        "status_code": response.status_code,
        "content_status": content_status,
        "blocked_reason": blocked_reason,
        "reader_mode": DEFAULT_READER_MODE,
        "fallback_used": False,
        "fallback_attempted": False,
        "fallback_error": "",
    }
    if not _should_try_browser_fallback(normalized_url, payload):
        return persist_artifact(payload) if persist_artifact_result else payload
    try:
        fallback = _browser_fallback_read(normalized_url)
    except Exception as exc:  # noqa: BLE001
        payload["fallback_attempted"] = True
        payload["fallback_error"] = str(exc).strip()
        return persist_artifact(payload) if persist_artifact_result else payload
    normalized_fallback = _normalize_payload(normalized_url, fallback, reader_mode=DEFAULT_BROWSER_FALLBACK_READER_MODE)
    normalized_fallback["fallback_used"] = True
    normalized_fallback["fallback_attempted"] = True
    normalized_fallback["fallback_error"] = ""
    if normalized_fallback["content_status"] == "captured":
        normalized_fallback["blocked_reason"] = ""
        normalized_fallback["ok"] = True
        return persist_artifact(normalized_fallback) if persist_artifact_result else normalized_fallback
    payload["fallback_attempted"] = True
    payload["fallback_error"] = str(normalized_fallback.get("fallback_error") or "").strip()
    return persist_artifact(payload) if persist_artifact_result else payload


def _format_context_block(index: int, context: dict[str, Any]) -> str:
    lines = [
        f"链接材料 {index}",
        f"- url: {str(context.get('fetched_url') or context.get('url') or '').strip()}",
        f"- title: {str(context.get('title') or 'Untitled').strip() or 'Untitled'}",
        f"- content_status: {str(context.get('content_status') or 'unknown').strip() or 'unknown'}",
        f"- reader_mode: {str(context.get('reader_mode') or 'unknown').strip() or 'unknown'}",
    ]
    if bool(context.get("fallback_used")):
        lines.append("- fallback_used: true")
    elif bool(context.get("fallback_attempted")):
        lines.append("- fallback_attempted: true")
    blocked_reason = str(context.get("blocked_reason") or "").strip()
    if blocked_reason:
        lines.append(f"- blocked_reason: {blocked_reason}")
    fallback_error = str(context.get("fallback_error") or "").strip()
    if fallback_error:
        lines.append(f"- fallback_error: {fallback_error}")
    excerpt = str(context.get("excerpt") or "").strip()
    if excerpt:
        lines.extend(["", "网页正文摘录：", excerpt[:2200].strip()])
    elif blocked_reason:
        lines.extend(["", "读取说明：", "系统已识别到平台风控或环境校验，当前未抓到可用正文。"])
    return "\n".join(lines).strip()


def augment_prompt(
    prompt: str,
    *,
    max_urls: int = 2,
    allowed_domains: set[str] | None = None,
    fetcher: Callable[[str], dict[str, Any]] | None = None,
    should_hydrate: Callable[[str], bool] | None = None,
    persist_artifacts: bool = False,
) -> tuple[str, list[dict[str, Any]]]:
    source = str(prompt or "").strip()
    if not source:
        return source, []
    fetch = fetcher or (lambda url: read_url(url, persist_artifact_result=persist_artifacts))
    hydrate = should_hydrate or (lambda url: should_auto_hydrate_url(url, allowed_domains=allowed_domains))
    contexts: list[dict[str, Any]] = []
    for url in extract_urls(source, limit=max_urls):
        if not hydrate(url):
            continue
        try:
            context = fetch(url)
        except Exception:
            continue
        if not isinstance(context, dict):
            continue
        normalized = _normalize_payload(url, context, reader_mode=str(context.get("reader_mode") or "adapter"))
        if not normalized["excerpt"] and normalized["content_status"] not in {"blocked"}:
            continue
        contexts.append(normalized)
    if not contexts:
        return source, []
    appendix = "\n\n---\n\n".join(_format_context_block(index, item) for index, item in enumerate(contexts, start=1))
    augmented = (
        source
        + "\n\n以下是系统自动读取的链接材料。若正文已抓取，请直接基于摘录回答；若某条链接被阻断，请明确说明具体阻断原因和当前读取状态，不要笼统地说无法读取链接：\n\n"
        + appendix
    )
    return augmented, contexts


def hydrate_prompt(
    prompt: str,
    *,
    max_urls: int = 2,
    allowed_domains: set[str] | None = None,
    fetcher: Callable[[str], dict[str, Any]] | None = None,
    should_hydrate: Callable[[str], bool] | None = None,
    persist_artifacts: bool = False,
) -> dict[str, Any]:
    if not str(prompt or "").strip():
        return empty_hydration_payload(prompt)
    augmented, contexts = augment_prompt(
        prompt,
        max_urls=max_urls,
        allowed_domains=allowed_domains,
        fetcher=fetcher,
        should_hydrate=should_hydrate,
        persist_artifacts=persist_artifacts,
    )
    return {
        "prompt": str(prompt or "").strip(),
        "augmented_prompt": augmented,
        "contexts": contexts,
        "summary": contexts_summary(contexts),
    }


def context_count(text: str) -> int:
    return len(re.findall(r"链接材料\s+\d+", str(text or "")))


def contexts_summary(contexts: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [dict(item) for item in contexts if isinstance(item, dict)]
    summary = empty_summary()
    summary["count"] = len(rows)
    summary["captured_count"] = sum(1 for item in rows if str(item.get("content_status") or "").strip() == "captured")
    summary["blocked_count"] = sum(1 for item in rows if str(item.get("content_status") or "").strip() == "blocked")
    summary["fallback_used_count"] = sum(1 for item in rows if bool(item.get("fallback_used")))
    summary["fallback_attempted_count"] = sum(1 for item in rows if bool(item.get("fallback_attempted")))
    summary["fallback_error_count"] = sum(1 for item in rows if str(item.get("fallback_error") or "").strip())
    summary["artifact_count"] = sum(1 for item in rows if str(item.get("artifact_markdown_path") or "").strip())
    summary["reader_modes"] = sorted(
        {
            str(item.get("reader_mode") or "").strip()
            for item in rows
            if str(item.get("reader_mode") or "").strip()
        }
    )
    return summary


def cmd_fetch_url(args: argparse.Namespace) -> int:
    print(json.dumps(read_url(args.url, persist_artifact_result=True), ensure_ascii=False, indent=2))
    return 0


def cmd_augment_prompt(args: argparse.Namespace) -> int:
    payload = hydrate_prompt(args.prompt, persist_artifacts=True)
    print(payload["augmented_prompt"])
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified public article reader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_url_cmd = subparsers.add_parser("fetch-url")
    fetch_url_cmd.add_argument("--url", required=True)
    fetch_url_cmd.set_defaults(func=cmd_fetch_url)

    augment_prompt_cmd = subparsers.add_parser("augment-prompt")
    augment_prompt_cmd.add_argument("--prompt", required=True)
    augment_prompt_cmd.set_defaults(func=cmd_augment_prompt)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

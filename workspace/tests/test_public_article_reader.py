from __future__ import annotations

import json
from pathlib import Path

from ops import public_article_reader


class _FakeResponse:
    def __init__(self, *, text: str, url: str, status_code: int = 200) -> None:
        self.text = text
        self.url = url
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None


def test_read_url_detects_wechat_risk_control_page(monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_PUBLIC_ARTICLE_ENABLE_BROWSER_FALLBACK", "0")
    html = """
    <html>
      <head><title>环境异常</title></head>
      <body>
        <main>
          <p>环境异常，完成验证后即可继续访问</p>
        </main>
      </body>
    </html>
    """

    monkeypatch.setattr(
        public_article_reader.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            text=html,
            url="https://mp.weixin.qq.com/mp/wappoc_appmsgcaptcha?foo=bar",
        ),
    )

    payload = public_article_reader.read_url("https://mp.weixin.qq.com/s/demo")

    assert payload["ok"] is False
    assert payload["content_status"] == "blocked"
    assert payload["blocked_reason"] == "blocked_by_wechat_risk_control"
    assert payload["fetched_url"].startswith("https://mp.weixin.qq.com/mp/wappoc_appmsgcaptcha")
    assert payload["fallback_attempted"] is False


def test_read_url_uses_browser_fallback_after_wechat_risk_control(monkeypatch) -> None:
    html = """
    <html>
      <head><title>环境异常</title></head>
      <body><main><p>环境异常，完成验证后即可继续访问</p></main></body>
    </html>
    """

    monkeypatch.setattr(
        public_article_reader.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            text=html,
            url="https://mp.weixin.qq.com/mp/wappoc_appmsgcaptcha?foo=bar",
        ),
    )
    monkeypatch.setattr(
        public_article_reader,
        "_browser_fallback_read",
        lambda url, wait_seconds=4: {
            "url": url,
            "fetched_url": url,
            "title": "浏览器态正文标题",
            "excerpt": "这里是通过浏览器态补抓回来的正文。",
            "content_status": "captured",
            "blocked_reason": "",
            "reader_mode": "browser_opencli_read",
            "fallback_used": True,
            "fallback_attempted": True,
            "fallback_error": "",
            "status_code": 200,
        },
    )

    payload = public_article_reader.read_url("https://mp.weixin.qq.com/s/demo")

    assert payload["ok"] is True
    assert payload["content_status"] == "captured"
    assert payload["reader_mode"] == "browser_opencli_read"
    assert payload["fallback_used"] is True
    assert payload["fallback_attempted"] is True
    assert payload["blocked_reason"] == ""
    assert "浏览器态补抓" in payload["excerpt"]


def test_read_url_keeps_blocked_status_when_browser_fallback_fails(monkeypatch) -> None:
    html = """
    <html>
      <head><title>环境异常</title></head>
      <body><main><p>环境异常，完成验证后即可继续访问</p></main></body>
    </html>
    """

    monkeypatch.setattr(
        public_article_reader.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            text=html,
            url="https://mp.weixin.qq.com/mp/wappoc_appmsgcaptcha?foo=bar",
        ),
    )

    def _boom(url: str, wait_seconds: int = 4) -> dict[str, object]:
        raise RuntimeError("browser fallback unavailable")

    monkeypatch.setattr(public_article_reader, "_browser_fallback_read", _boom)

    payload = public_article_reader.read_url("https://mp.weixin.qq.com/s/demo")

    assert payload["ok"] is False
    assert payload["content_status"] == "blocked"
    assert payload["blocked_reason"] == "blocked_by_wechat_risk_control"
    assert payload["fallback_attempted"] is True
    assert payload["fallback_used"] is False
    assert payload["fallback_error"] == "browser fallback unavailable"


def test_augment_prompt_keeps_structured_blocked_context() -> None:
    augmented, contexts = public_article_reader.augment_prompt(
        "请阅读这个链接：https://mp.weixin.qq.com/s/demo",
        fetcher=lambda _url: {
            "url": "https://mp.weixin.qq.com/s/demo",
            "fetched_url": "https://mp.weixin.qq.com/mp/wappoc_appmsgcaptcha?foo=bar",
            "title": "环境异常",
            "excerpt": "环境异常，完成验证后即可继续访问",
            "content_status": "blocked",
            "blocked_reason": "blocked_by_wechat_risk_control",
            "reader_mode": "http_html",
            "fallback_used": False,
            "status_code": 200,
        },
        should_hydrate=lambda _url: True,
    )

    assert len(contexts) == 1
    assert contexts[0]["content_status"] == "blocked"
    assert "blocked_reason: blocked_by_wechat_risk_control" in augmented
    assert "不要笼统地说无法读取链接" in augmented


def test_augment_prompt_reports_fallback_metadata() -> None:
    augmented, contexts = public_article_reader.augment_prompt(
        "请阅读这个链接：https://mp.weixin.qq.com/s/demo",
        fetcher=lambda _url: {
            "url": "https://mp.weixin.qq.com/s/demo",
            "fetched_url": "https://mp.weixin.qq.com/s/demo",
            "title": "浏览器态正文标题",
            "excerpt": "浏览器态回来了正文。",
            "content_status": "captured",
            "blocked_reason": "",
            "reader_mode": "browser_opencli_read",
            "fallback_used": True,
            "fallback_attempted": True,
            "fallback_error": "",
            "status_code": 200,
        },
        should_hydrate=lambda _url: True,
    )

    assert len(contexts) == 1
    assert contexts[0]["fallback_used"] is True
    assert "fallback_used: true" in augmented
    assert "reader_mode: browser_opencli_read" in augmented


def test_read_url_persists_artifact_when_requested(monkeypatch, tmp_path: Path) -> None:
    html = """
    <html>
      <head><title>示例标题</title></head>
      <body><article><p>这是一段足够长的示例正文，用于验证 artifact 落盘能力已经接入统一 reader。</p></article></body>
    </html>
    """

    monkeypatch.setenv("WORKSPACE_HUB_PUBLIC_ARTICLE_ARTIFACTS_ROOT", str(tmp_path))
    monkeypatch.setattr(
        public_article_reader.requests,
        "get",
        lambda *args, **kwargs: _FakeResponse(
            text=html,
            url="https://mp.weixin.qq.com/s/demo",
        ),
    )

    payload = public_article_reader.read_url(
        "https://mp.weixin.qq.com/s/demo",
        persist_artifact_result=True,
    )

    assert payload["artifact_id"].startswith("public-article-")
    json_path = Path(payload["artifact_json_path"])
    markdown_path = Path(payload["artifact_markdown_path"])
    assert json_path.exists()
    assert markdown_path.exists()
    saved = json.loads(json_path.read_text(encoding="utf-8"))
    assert saved["title"] == "示例标题"
    assert "## Metadata" in markdown_path.read_text(encoding="utf-8")


def test_contexts_summary_reports_blocked_fallback_and_artifact_counts() -> None:
    summary = public_article_reader.contexts_summary(
        [
            {
                "content_status": "captured",
                "fallback_used": True,
                "fallback_attempted": True,
                "artifact_markdown_path": "/tmp/a.md",
                "reader_mode": "browser_opencli_read",
            },
            {
                "content_status": "blocked",
                "fallback_used": False,
                "fallback_attempted": True,
                "fallback_error": "browser fallback unavailable",
                "artifact_markdown_path": "",
                "reader_mode": "http_html",
            },
        ]
    )

    assert summary["count"] == 2
    assert summary["captured_count"] == 1
    assert summary["blocked_count"] == 1
    assert summary["fallback_used_count"] == 1
    assert summary["fallback_attempted_count"] == 2
    assert summary["fallback_error_count"] == 1
    assert summary["artifact_count"] == 1


def test_empty_hydration_payload_keeps_fallback_summary_contract() -> None:
    payload = public_article_reader.empty_hydration_payload()

    assert payload["prompt"] == ""
    assert payload["augmented_prompt"] == ""
    assert payload["contexts"] == []
    assert payload["summary"] == {
        "count": 0,
        "captured_count": 0,
        "blocked_count": 0,
        "fallback_used_count": 0,
        "fallback_attempted_count": 0,
        "fallback_error_count": 0,
        "artifact_count": 0,
        "reader_modes": [],
    }

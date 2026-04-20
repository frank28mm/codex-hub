from __future__ import annotations

from ops import local_broker


def test_should_hydrate_prompt_url_only_accepts_wechat_public_article_urls() -> None:
    assert local_broker._should_hydrate_prompt_url("https://mp.weixin.qq.com/s/demo")
    assert not local_broker._should_hydrate_prompt_url("https://example.com/post")


def test_augment_prompt_with_url_context_embeds_fetched_excerpt(monkeypatch) -> None:
    monkeypatch.setattr(
        local_broker,
        "_fetch_prompt_url_context",
        lambda url: {
            "url": url,
            "title": "测试文章",
            "excerpt": "这是自动抓取到的正文摘录。",
            "fetched_url": url,
        },
    )

    prompt, contexts = local_broker._augment_prompt_with_url_context(
        "看看这个链接 https://mp.weixin.qq.com/s/demo"
    )

    assert len(contexts) == 1
    assert "系统自动抓取的链接正文摘要" in prompt
    assert "测试文章" in prompt
    assert "这是自动抓取到的正文摘录。" in prompt

from __future__ import annotations

from ops import growth_content_curation


def test_apply_batch_updates_local_truth_and_projection(monkeypatch) -> None:
    captured: dict[str, list[dict[str, object]]] = {"asset": [], "publish": [], "feedback": []}
    monkeypatch.setattr(
        growth_content_curation.growth_content_truth,
        "upsert_rows",
        lambda table_kind, rows: captured[table_kind].extend(rows) or rows,
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_content_record",
        lambda **kwargs: {"ok": True, "kind": "asset", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_publish_record",
        lambda row: {"ok": True, "kind": "publish", "row": row},
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_feedback_record",
        lambda row: {"ok": True, "kind": "feedback", "row": row},
    )

    result = growth_content_curation.apply_batch(
        {
            "assets": [{"asset_id": "GC-ASSET-001", "product_or_service": "AI Coding课程", "channel": "朋友圈", "topic": "测试标题", "status": "curated", "source_path": "/tmp/a.png"}],
            "publishes": [{"publish_id": "GC-PUB-001", "asset_id": "GC-ASSET-001", "product_or_service": "AI Coding课程", "channel": "朋友圈", "published_at": "2026-04-12T11:01:00+08:00", "body_excerpt": "测试正文", "status": "published", "source_path": "/tmp/a.png"}],
            "feedbacks": [{"feedback_id": "GC-FEEDBACK-001", "publish_id": "GC-PUB-001", "asset_id": "GC-ASSET-001", "product_or_service": "AI Coding课程", "channel": "朋友圈", "feedback_at": "2026-04-12T21:46:00+08:00", "signal_summary": "测试反馈", "status": "observed", "source_path": "/tmp/a.png"}],
        }
    )
    assert result["asset_count"] == 1
    assert result["publish_count"] == 1
    assert result["feedback_count"] == 1
    assert captured["asset"][0]["asset_id"] == "GC-ASSET-001"
    assert captured["publish"][0]["publish_id"] == "GC-PUB-001"
    assert captured["feedback"][0]["feedback_id"] == "GC-FEEDBACK-001"


def test_apply_batch_projects_provider_generation_source_into_feishu_rows(monkeypatch) -> None:
    asset_calls: list[dict[str, object]] = []
    publish_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        growth_content_curation.growth_content_truth,
        "upsert_rows",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_views,
        "refresh_views",
        lambda: {"ok": True},
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_content_record",
        lambda **kwargs: asset_calls.append(kwargs) or {"ok": True, "kind": "asset"},
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_publish_record",
        lambda row: publish_calls.append(row) or {"ok": True, "kind": "publish"},
    )
    monkeypatch.setattr(
        growth_content_curation.growth_content_control,
        "upsert_feedback_record",
        lambda row: {"ok": True, "kind": "feedback", "row": row},
    )

    growth_content_curation.apply_batch(
        {
            "assets": [
                {
                    "asset_id": "GC-ASSET-002",
                    "product_or_service": "TINT",
                    "channel": "朋友圈",
                    "topic": "测试标题",
                    "status": "draft_generated",
                    "source_bucket": "creator_workflow",
                    "source_path": "/tmp/tint.md",
                    "live_body": "测试正文",
                }
            ],
            "publishes": [
                {
                    "publish_id": "GC-PUB-002",
                    "asset_id": "GC-ASSET-002",
                    "product_or_service": "TINT",
                    "channel": "朋友圈",
                    "title": "测试标题",
                    "body": "测试正文",
                    "status": "draft_generated",
                    "source_path": "/tmp/tint.md",
                }
            ],
            "feedbacks": [],
            "metadata": {
                "execution_tool": "manual-draft",
                "task_source": "creator_workflow",
            },
        }
    )

    assert asset_calls[0]["source"] == "manual-draft"
    assert asset_calls[0]["task_source"] == "creator_workflow"
    assert publish_calls[0]["generation_source"] == "manual-draft"
    assert publish_calls[0]["task_source"] == "creator_workflow"

from __future__ import annotations

import importlib
from pathlib import Path


def test_growth_content_views_refresh_generates_detail_and_dashboard(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", str(tmp_path / "vault"))
    from ops import growth_content_truth as truth_module
    from ops import growth_content_views as views_module

    growth_content_truth = importlib.reload(truth_module)
    growth_content_views = importlib.reload(views_module)

    growth_content_truth.upsert_rows(
        "asset",
        [
            growth_content_truth.build_asset_row(
                asset_id="GC-ASSET-TEST-001",
                asset_type="snapshot",
                product_or_service="TINT",
                channel="朋友圈",
                topic="测试内容",
                source_bucket="已发布",
                source_path="/tmp/test.png",
                checksum="abc",
                status="curated",
                created_at="2026-04-13T08:00:00+08:00",
                updated_at="2026-04-13T08:05:00+08:00",
            )
        ],
    )
    growth_content_truth.upsert_rows(
        "publish",
        [
            {
                "publish_id": "GC-PUB-TEST-001",
                "asset_id": "GC-ASSET-TEST-001",
                "project_name": "增长与营销",
                "product_or_service": "TINT",
                "channel": "朋友圈",
                "publish_date": "2026-04-13",
                "publish_time": "09:00",
                "visible_time_text": "09:00",
                "location": "上海",
                "title": "测试发布",
                "body": "测试正文",
                "content_kind": "图文",
                "topic_tags": "TINT,测试",
                "like_count": "3",
                "comment_count": "1",
                "dm_count": "0",
                "qualified_lead_count": "1",
                "status": "published",
                "next_action": "继续跟进",
                "source_path": "/tmp/test.png",
            }
        ],
    )
    growth_content_truth.upsert_rows(
        "feedback",
        [
            {
                "feedback_id": "GC-FEEDBACK-TEST-001",
                "publish_id": "GC-PUB-TEST-001",
                "asset_id": "GC-ASSET-TEST-001",
                "project_name": "增长与营销",
                "product_or_service": "TINT",
                "channel": "朋友圈",
                "feedback_date": "2026-04-13",
                "feedback_time": "09:30",
                "signal_summary": "已有一位家长明确表达试用兴趣",
                "like_count": "3",
                "comment_count": "1",
                "dm_count": "0",
                "qualified_lead_count": "1",
                "followup_status": "observed",
                "next_action": "回访家长",
                "source_path": "/tmp/test.png",
            }
        ],
    )

    payload = growth_content_views.refresh_views()

    assert payload["ok"] is True
    dashboard_text = Path(payload["dashboard_path"]).read_text(encoding="utf-8")
    detail_text = (Path(payload["detail_root"]) / "GC-PUB-TEST-001.md").read_text(encoding="utf-8")
    assert "增长与营销｜内容中控" in dashboard_text
    assert "测试发布" in dashboard_text
    assert "GC-FEEDBACK-TEST-001" in detail_text
    assert "已有一位家长明确表达试用兴趣" in detail_text

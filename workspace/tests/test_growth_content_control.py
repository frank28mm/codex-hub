from __future__ import annotations

import json

from ops import growth_content_control


def test_schema_snapshot_contains_three_tables() -> None:
    payload = growth_content_control.schema_snapshot()
    assert payload["ok"] is True
    assert set(payload["tables"]) == {"asset", "publish", "feedback"}


def test_build_publish_payload_maps_metrics() -> None:
    payload = growth_content_control.build_publish_payload(
        {
            "publish_id": "GC-PUB-001",
            "asset_id": "GC-ASSET-001",
            "product_or_service": "Codex Hub",
            "channel": "朋友圈",
            "published_at": "2026-04-13T00:00:00+08:00",
            "body_excerpt": "测试文案",
            "like_count": "3",
            "comment_count": "2",
            "qualified_lead_count": "1",
            "status": "captured",
            "next_action": "继续跟进",
            "source_path": "/tmp/test.png",
        }
    )
    assert payload["项目"] == "增长与营销"
    assert payload["有效销售线索"] == "1"


def test_bootstrap_live_base_creates_three_tables(monkeypatch) -> None:
    monkeypatch.setattr(growth_content_control, "STATE_PATH", growth_content_control.Path("/tmp/growth-content-control-test.json"))
    monkeypatch.setattr(
        growth_content_control.lark_cli_backend,
        "base_app_create",
        lambda **kwargs: {"app": {"app_token": "app_test"}},
    )
    created: list[str] = []
    monkeypatch.setattr(
        growth_content_control.lark_cli_backend,
        "base_table_create",
        lambda **kwargs: created.append(kwargs["name"]) or {"table": {"table_id": f"tbl_{len(created)}"}},
    )
    monkeypatch.setattr(
        growth_content_control,
        "ensure_fields",
        lambda **kwargs: {"ok": True, "table_id": kwargs["table_id"], "created_fields": ["项目"]},
    )
    payload = growth_content_control.bootstrap_live_base(base_name="测试Base")
    assert payload["base_token"] == "app_test"
    assert created == ["内容资产主表", "已发布记录", "反馈线索记录"]


def test_ensure_surface_creates_dashboard_and_views(monkeypatch) -> None:
    calls: list[list[str]] = []
    state = {
        "dashboards": [],
        "publish_views": [],
        "feedback_views": [],
        "asset_views": [],
    }

    def fake_run(argv, *, cwd=None):
        calls.append(list(argv))
        if "+field-list" in argv:
            table_id = argv[argv.index("--table-id") + 1]
            if table_id == "tbl_publish":
                items = [
                    {"field_id": "fld_pub_date", "field_name": "发布日期"},
                    {"field_id": "fld_pub_time", "field_name": "发布时间"},
                    {"field_id": "fld_pub_comment", "field_name": "评论条数"},
                    {"field_id": "fld_pub_dm", "field_name": "私聊数"},
                    {"field_id": "fld_pub_lead", "field_name": "有效销售线索"},
                    {"field_id": "fld_pub_channel", "field_name": "渠道"},
                    {"field_id": "fld_pub_product", "field_name": "产品/服务"},
                ]
            elif table_id == "tbl_feedback":
                items = [
                    {"field_id": "fld_fb_date", "field_name": "反馈日期"},
                    {"field_id": "fld_fb_time", "field_name": "反馈时间"},
                    {"field_id": "fld_fb_lead", "field_name": "有效销售线索"},
                    {"field_id": "fld_fb_status", "field_name": "跟进状态"},
                    {"field_id": "fld_fb_channel", "field_name": "渠道"},
                    {"field_id": "fld_fb_product", "field_name": "产品/服务"},
                ]
            else:
                items = []
            return {"ok": True, "data": {"items": items}}
        if "+view-list" in argv:
            table_id = argv[argv.index("--table-id") + 1]
            key = "asset_views" if table_id == "tbl_asset" else "publish_views" if table_id == "tbl_publish" else "feedback_views"
            return {"ok": True, "data": {"items": list(state[key])}}
        if "+view-create" in argv:
            table_id = argv[argv.index("--table-id") + 1]
            payload = json.loads(argv[argv.index("--json") + 1])
            key = "asset_views" if table_id == "tbl_asset" else "publish_views" if table_id == "tbl_publish" else "feedback_views"
            state[key].append({"view_id": f"vew_{len(state[key]) + 1}", "view_name": payload["name"]})
            return {"ok": True, "data": {"views": state[key]}}
        if "+view-set-filter" in argv or "+view-set-sort" in argv:
            return {"ok": True, "data": {}}
        if "+dashboard-list" in argv:
            return {"ok": True, "data": {"items": list(state["dashboards"])}}  # type: ignore[index]
        if "+dashboard-create" in argv:
            state["dashboards"].append({"dashboard_id": "dsh_1", "name": growth_content_control.DASHBOARD_NAME})
            return {"ok": True, "data": {"dashboard_id": "dsh_1"}}
        if "+dashboard-update" in argv:
            return {"ok": True, "data": {}}
        if "+dashboard-block-list" in argv:
            return {"ok": True, "data": {"items": []}}
        if "+dashboard-block-create" in argv or "+dashboard-block-delete" in argv:
            return {"ok": True, "data": {}}
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(growth_content_control, "_run_lark_json", fake_run)
    monkeypatch.setattr(growth_content_control, "resolved_base_token", lambda: "base_test")
    monkeypatch.setattr(
        growth_content_control,
        "resolved_table_id",
        lambda table_kind: {"asset": "tbl_asset", "publish": "tbl_publish", "feedback": "tbl_feedback"}[table_kind],
    )
    payload = growth_content_control.ensure_surface(base_token="base_test")
    assert payload["ok"] is True
    assert payload["dashboard"]["dashboard_id"] == "dsh_1"
    assert any("+dashboard-block-create" in call for call in calls)
    assert any("+view-create" in call for call in calls)

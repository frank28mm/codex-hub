from __future__ import annotations

from pathlib import Path
from typing import Any

from ops import feishu_agent, feishu_projection, runtime_state


class ProjectionAgent:
    def __init__(self) -> None:
        self.app_token = "app_projection"
        self.tables: dict[str, dict[str, Any]] = {}
        self._table_counter = 0
        self._field_counter = 0
        self._view_counter = 0
        self._record_counter = 0

    def _new_table(self, name: str, default_view_name: str = "") -> dict[str, Any]:
        self._table_counter += 1
        table_id = f"tbl_projection_{self._table_counter}"
        table = {
            "table_id": table_id,
            "name": name,
            "fields": [],
            "views": [],
            "records": {},
        }
        self.tables[table_id] = table
        if default_view_name:
            self.table_create_view({"app": self.app_token, "table": table_id, "name": default_view_name, "type": "grid"})
        return table

    def table_create_app(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self._new_table(str(payload.get("table_name") or "项目总览"), str(payload.get("default_view_name") or "全部项目"))
        return {
            "ok": True,
            "app_token": self.app_token,
            "default_table_id": table["table_id"],
            "table": {"table_id": table["table_id"], "name": table["name"]},
        }

    def table_tables(self, payload: dict[str, Any]) -> dict[str, Any]:
        _ = payload
        return {
            "tables": [{"table_id": item["table_id"], "name": item["name"]} for item in self.tables.values()],
        }

    def table_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self._new_table(str(payload.get("name") or "新表"), str(payload.get("default_view_name") or "默认视图"))
        for field in list(payload.get("fields") or []):
            self.table_create_field({"app": self.app_token, "table": table["table_id"], "field": field})
        return {"ok": True, "table_id": table["table_id"], "name": table["name"]}

    def table_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        return {"fields": list(table["fields"])}

    def table_create_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        field = dict(payload.get("field") or {})
        if not field:
            field = {"field_name": payload.get("field_name"), "type": payload.get("type")}
        field_name = str(field.get("field_name") or "").strip()
        for existing in table["fields"]:
            if str(existing.get("field_name") or "").strip() == field_name:
                return {"field_id": existing["field_id"], "field_name": existing["field_name"]}
        self._field_counter += 1
        created = {"field_id": f"fld_projection_{self._field_counter}", **field}
        table["fields"].append(created)
        return {"field_id": created["field_id"], "field_name": created["field_name"]}

    def table_views(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        return {"views": list(table["views"])}

    def table_get_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        view_id = str(payload.get("view") or "")
        for view in table["views"]:
            if view["view_id"] == view_id:
                return {"view": dict(view)}
        return {"view": {}}

    def table_create_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        self._view_counter += 1
        created = {
            "view_id": f"vew_projection_{self._view_counter}",
            "view_name": str(payload.get("name") or payload.get("view_name") or ""),
            "view_type": str(payload.get("type") or payload.get("view_type") or "grid"),
        }
        table["views"].append(created)
        return {"view": created}

    def table_update_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        view_id = str(payload.get("view") or "")
        for view in table["views"]:
            if view["view_id"] != view_id:
                continue
            if payload.get("name"):
                view["view_name"] = str(payload.get("name") or "")
            if payload.get("type"):
                view["view_type"] = str(payload.get("type") or "")
            return {"view": dict(view)}
        raise AssertionError(f"unknown view {view_id}")

    def table_records(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        limit = int(payload.get("limit") or 200)
        offset = int(str(payload.get("page_token") or "0") or "0")
        all_records = list(table["records"].values())
        items = all_records[offset : offset + limit]
        next_offset = offset + limit
        return {
            "records": [dict(item) for item in items],
            "total": len(all_records),
            "has_more": next_offset < len(all_records),
            "page_token": str(next_offset) if next_offset < len(all_records) else "",
        }

    def table_add(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        self._record_counter += 1
        record_id = f"rec_projection_{self._record_counter}"
        record = {"record_id": record_id, "fields": dict(payload.get("data") or {})}
        table["records"][record_id] = record
        return {"record_id": record_id, "record": dict(record)}

    def table_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        record_id = str(payload.get("record") or "")
        record = table["records"][record_id]
        record["fields"] = dict(payload.get("data") or {})
        return {"record": dict(record)}

    def table_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        table = self.tables[str(payload.get("table") or "")]
        record_id = str(payload.get("record") or "")
        table["records"].pop(record_id, None)
        return {"ok": True}


def test_snapshot_builds_project_and_task_rows_from_structured_facts(monkeypatch) -> None:
    fact = {
        "project_name": "SampleProj",
        "status": "active",
        "priority": "high",
        "updated_at": "2026-03-12",
        "next_action": "Ship it",
        "board_path": "/tmp/SampleProj-项目板.md",
        "project_rows": [
            {
                "ID": "SP-1",
                "来源": "project",
                "事项": "冻结主线契约",
                "状态": "doing",
                "下一步": "补齐回归测试",
                "更新时间": "2026-03-12T10:00:00Z",
            }
        ],
        "rollup_rows": [
            {
                "ID": "TP-1",
                "来源": "topic:需求",
                "事项": "确认需求边界",
                "状态": "blocked",
                "下一步": "等待用户反馈",
                "更新时间": "2026-03-12T11:00:00Z",
            }
        ],
    }
    monkeypatch.setattr(feishu_projection, "_filter_facts", lambda project_name="": ([fact], []))
    monkeypatch.setattr(
        feishu_projection,
        "_topic_metrics",
        lambda project_name: (1, {"需求": {"path": "/tmp/SampleProj-需求-跟进板.md", "topic_name": "需求"}}),
    )
    monkeypatch.setattr(
        feishu_projection,
        "_topic_sources",
        lambda project_name: {"需求": {"path": "/tmp/SampleProj-需求-跟进板.md", "topic_name": "需求"}},
    )

    payload = feishu_projection.snapshot(project_name="SampleProj")

    assert payload["ok"] is True
    assert payload["schema_version"] == "feishu-projection.v2"
    assert payload["row_counts"] == {"projects_overview": 1, "tasks_current": 2}
    assert payload["projects_overview_rows"][0]["阻塞任务数"] == 1
    assert payload["projects_overview_rows"][0]["未完成任务数"] == 2
    assert payload["tasks_current_rows"][0]["projection_key"].startswith("task::SampleProj::")
    assert {item["状态"] for item in payload["tasks_current_rows"]} == {"doing", "blocked"}
    assert any(item["专题"] == "需求" for item in payload["tasks_current_rows"])


def test_bitable_target_status_and_preview_reflect_projection_contract(monkeypatch) -> None:
    rows = {
        "ok": True,
        "row_counts": {"projects_overview": 1, "tasks_current": 2},
        "projects_overview_rows": [{"projection_key": "project::SampleProj", "项目名": "SampleProj"}],
        "tasks_current_rows": [{"projection_key": "task::SampleProj::project::SP-1", "任务标题": "冻结主线契约"}],
    }
    monkeypatch.setattr(feishu_projection, "snapshot", lambda project_name="": rows)

    target = feishu_projection.bitable_target_status(project_name="SampleProj")
    preview = feishu_projection.bitable_publish_preview(project_name="SampleProj")

    assert target["bitable_mode"] == "read_only_projection"
    assert target["tables"]["projects_overview"]["view_names"] == ["全部项目", "按状态看板", "按优先级", "最近更新", "需关注项目"]
    assert target["tables"]["tasks_current"]["view_names"] == ["全部任务", "按状态看板", "按项目分组", "阻塞项", "最近更新任务"]
    assert preview["preview_counts"] == {"projects_overview": 1, "tasks_current": 2}
    assert preview["preview_rows"]["projects_overview"][0]["项目名"] == "SampleProj"


def test_ensure_projection_resources_creates_app_tables_fields_views_and_persists_registry(sample_env, monkeypatch) -> None:
    fake_agent = ProjectionAgent()
    monkeypatch.setattr(feishu_projection.feishu_agent, "FeishuAgent", lambda *args, **kwargs: fake_agent)

    result = feishu_projection.ensure_projection_resources()
    registry = feishu_projection.load_projection_registry()

    assert result["app_token"] == "app_projection"
    assert registry["projection"]["app"]["app_token"] == "app_projection"
    assert registry["projection"]["tables"]["projects_overview"]["table_id"]
    assert registry["projection"]["tables"]["tasks_current"]["table_id"]
    assert registry["aliases"]["tables"]["codex_hub_projects_overview"]["table_id"] == registry["projection"]["tables"]["projects_overview"]["table_id"]
    assert registry["aliases"]["tables"]["codex_hub_tasks_current"]["table_id"] == registry["projection"]["tables"]["tasks_current"]["table_id"]

    projects_table = fake_agent.tables[registry["projection"]["tables"]["projects_overview"]["table_id"]]
    tasks_table = fake_agent.tables[registry["projection"]["tables"]["tasks_current"]["table_id"]]
    assert {field["field_name"] for field in projects_table["fields"]} >= {"projection_key", "项目名", "状态", "需关注"}
    assert {field["field_name"] for field in tasks_table["fields"]} >= {"projection_key", "项目", "任务标题", "状态"}
    assert {view["view_name"] for view in projects_table["views"]} >= {"全部项目", "按状态看板", "最近更新"}
    assert {view["view_name"] for view in tasks_table["views"]} >= {"全部任务", "按项目分组", "阻塞项"}


def test_run_sync_consumes_queue_and_upserts_projection_rows(sample_env, monkeypatch) -> None:
    fake_agent = ProjectionAgent()
    monkeypatch.setattr(feishu_projection.feishu_agent, "FeishuAgent", lambda *args, **kwargs: fake_agent)
    resources = feishu_projection.ensure_projection_resources()
    runtime_state.init_db()

    first_snapshot = {
        "ok": True,
        "row_counts": {"projects_overview": 1, "tasks_current": 2},
        "projects_overview_rows": [
            {
                "projection_key": "project::SampleProj",
                "项目名": "SampleProj",
                "状态": "active",
                "优先级": "high",
                "当前下一步": "Ship it",
                "最近更新时间": "2026-03-12",
                "活跃专题数": 1,
                "未完成任务数": 2,
                "阻塞任务数": 1,
                "需关注": True,
                "项目板链接": "obsidian://project",
                "NEXT_ACTIONS 链接": "obsidian://next",
            }
        ],
        "tasks_current_rows": [
            {
                "projection_key": "task::SampleProj::project::SP-1",
                "项目": "SampleProj",
                "专题": "",
                "任务 ID": "SP-1",
                "任务标题": "冻结主线契约",
                "状态": "doing",
                "优先级": "high",
                "下一步": "补齐回归测试",
                "是否阻塞": False,
                "更新时间": "2026-03-12T10:00:00Z",
                "来源板链接": "obsidian://project",
            },
            {
                "projection_key": "task::SampleProj::topic:需求::TP-1",
                "项目": "SampleProj",
                "专题": "需求",
                "任务 ID": "TP-1",
                "任务标题": "确认需求边界",
                "状态": "blocked",
                "优先级": "high",
                "下一步": "等待用户反馈",
                "是否阻塞": True,
                "更新时间": "2026-03-12T11:00:00Z",
                "来源板链接": "obsidian://topic",
            },
        ],
        "errors": [],
    }
    second_snapshot = {
        **first_snapshot,
        "row_counts": {"projects_overview": 1, "tasks_current": 1},
        "projects_overview_rows": [{**first_snapshot["projects_overview_rows"][0], "阻塞任务数": 0, "未完成任务数": 1, "需关注": False}],
        "tasks_current_rows": [first_snapshot["tasks_current_rows"][0]],
    }
    snapshots = [first_snapshot, second_snapshot]
    monkeypatch.setattr(feishu_projection, "snapshot", lambda project_name="": snapshots.pop(0))

    queued = runtime_state.enqueue_runtime_event(
        queue_name=feishu_projection.QUEUE_NAME,
        event_type="project_writeback",
        payload={"project_name": "SampleProj", "event_id": "evt-projection-1"},
        dedupe_key="evt-projection-1",
    )
    first = feishu_projection.run_sync(force_full=False)

    projects_table = fake_agent.tables[resources["tables"]["projects_overview"]["table_id"]]
    tasks_table = fake_agent.tables[resources["tables"]["tasks_current"]["table_id"]]
    assert first["status"] == "ok"
    assert first["claimed_events"] == 1
    assert first["tables"]["projects_overview"]["created"] == 1
    assert first["tables"]["tasks_current"]["created"] == 2
    assert len(projects_table["records"]) == 1
    assert len(tasks_table["records"]) == 2
    assert runtime_state.fetch_runtime_event(queued["event_key"])["status"] == "completed"

    queued_second = runtime_state.enqueue_runtime_event(
        queue_name=feishu_projection.QUEUE_NAME,
        event_type="project_writeback",
        payload={"project_name": "SampleProj", "event_id": "evt-projection-2"},
        dedupe_key="evt-projection-2",
    )
    second = feishu_projection.run_sync(force_full=False)

    assert second["status"] == "ok"
    assert second["tables"]["projects_overview"]["updated"] == 1
    assert second["tables"]["tasks_current"]["deleted"] == 1
    assert len(tasks_table["records"]) == 1
    remaining = next(iter(tasks_table["records"].values()))
    assert remaining["fields"]["任务 ID"] == "SP-1"
    assert runtime_state.fetch_runtime_event(queued_second["event_key"])["status"] == "completed"

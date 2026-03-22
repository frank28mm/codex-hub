from __future__ import annotations

import argparse
import json

from ops import feishu_agent


class FakeAgent(feishu_agent.FeishuAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []
        self._views: dict[tuple[str, str], list[dict[str, str]]] = {}

    def api(self, method: str, path: str, *, data=None, params=None):  # type: ignore[override]
        self.calls.append((method, path, data, params))
        if path == "/im/v1/messages":
            return {"message_id": "om_msg_123"}
        if path == "/contact/v3/users/batch_get_id":
            return {"user_list": [{"user_id": "ou_lookup"}]}
        if path == "/contact/v3/users/search":
            return {"results": [{"user": {"open_id": "ou_search"}}]}
        if path == "/contact/v3/users":
            return {
                "items": [
                    {
                        "name": "叮爸吉祥",
                        "nickname": "Frank",
                        "en_name": "Frank",
                        "email": "operator@example.com",
                        "open_id": "ou_search",
                    }
                ]
            }
        if path == "/docx/v1/documents":
            return {"document": {"document_id": "doc_123", "document_uri": "https://feishu.cn/docx/doc_123"}}
        if path.startswith("/docx/v1/documents/") and path.endswith("/raw_content"):
            return {"content": "hello"}
        if path.startswith("/docx/v1/documents/") and "/children" in path:
            return {"ok": True}
        if path.startswith("/drive/v1/permissions/"):
            return {"ok": True}
        if path == "/drive/v1/files":
            return {"files": [{"token": "fld_reports"}]}
        if path.endswith("/records") and method == "POST":
            return {"record": {"record_id": "rec_123"}}
        if path.endswith("/records") and method == "GET":
            return {"items": [{"record_id": "rec_1"}], "total": 1}
        if "/records/" in path and method == "PUT":
            return {"record": {"record_id": "rec_123", "fields": {"状态": "完成"}}}
        if "/records/" in path and method == "DELETE":
            return {"ok": True}
        if path == "/bitable/v1/apps" and method == "POST":
            return {
                "app": {
                    "app_token": "app_new",
                    "default_table_id": "tbl_default",
                    "name": (data or {}).get("name", ""),
                    "url": "https://feishu.cn/base/app_new",
                }
            }
        if path == "/bitable/v1/apps/app_new/tables" and method == "POST":
            table = ((data or {}).get("table") or {})
            return {
                "table": {
                    "table_id": "tbl_new",
                    "name": table.get("name", ""),
                    "default_view_id": "vew_new",
                    "field_id_list": ["fld_title", "fld_status"],
                }
            }
        if path == "/bitable/v1/apps/app_book/tables" and method == "POST":
            table = ((data or {}).get("table") or {})
            return {
                "table": {
                    "table_id": "tbl_extra",
                    "name": table.get("name", ""),
                    "default_view_id": "vew_extra",
                    "field_id_list": ["fld_a"],
                }
            }
        if path.endswith("/tables"):
            return {"items": [{"table_id": "tbl_book"}]}
        if path == "/bitable/v1/apps/app_book/tables/tbl_book/fields" and method == "POST":
            return {"field": {"field_id": "fld_author", "field_name": (data or {}).get("field_name", "")}}
        if path.endswith("/fields"):
            return {"items": [{"field_name": "书名"}]}
        if path.endswith("/views") and method == "GET":
            parts = path.split("/")
            app_token = parts[4]
            table_id = parts[6]
            return {"items": list(self._views.get((app_token, table_id), []))}
        if path.endswith("/views") and method == "POST":
            parts = path.split("/")
            app_token = parts[4]
            table_id = parts[6]
            created = {
                "view_id": f"vew_{len(self._views.get((app_token, table_id), [])) + 1}",
                "view_name": str((data or {}).get("view_name") or ""),
                "view_type": str((data or {}).get("view_type") or "grid"),
            }
            self._views.setdefault((app_token, table_id), []).append(created)
            return {"view": dict(created)}
        if "/views/" in path and method == "GET":
            parts = path.split("/")
            app_token = parts[4]
            table_id = parts[6]
            view_id = parts[8]
            for item in self._views.get((app_token, table_id), []):
                if item["view_id"] == view_id:
                    return {"view": dict(item)}
            return {"view": {}}
        if "/views/" in path and method == "PATCH":
            parts = path.split("/")
            app_token = parts[4]
            table_id = parts[6]
            view_id = parts[8]
            items = self._views.setdefault((app_token, table_id), [])
            for item in items:
                if item["view_id"] == view_id:
                    if (data or {}).get("view_name"):
                        item["view_name"] = str((data or {}).get("view_name") or "")
                    if (data or {}).get("view_type"):
                        item["view_type"] = str((data or {}).get("view_type") or "")
                    return {"view": dict(item)}
            raise AssertionError(f"Unknown view for patch: {path}")
        if path.endswith("/events") and method == "POST":
            event = {
                "event_id": "evt_123",
                "summary": (data or {}).get("summary", ""),
                "app_link": "https://feishu.cn/calendar/event/evt_123",
                "start_time": (data or {}).get("start_time"),
                "end_time": (data or {}).get("end_time"),
                "vchat": (data or {}).get("vchat", {}),
            }
            return {"event": event}
        if path.endswith("/attendees"):
            return {"ok": True}
        if path.endswith("/events") and method == "GET":
            return {
                "items": [
                    {
                        "event_id": "evt_123",
                        "summary": "会议",
                        "start_time": {"timestamp": "1710000000", "timezone": "Asia/Shanghai"},
                        "end_time": {"timestamp": "1710003600", "timezone": "Asia/Shanghai"},
                        "vchat": {"vc_type": "vc"},
                    }
                ]
            }
        if "/events/" in path and method == "GET":
            return {
                "event": {
                    "event_id": "evt_123",
                    "summary": "会议",
                    "app_link": "https://feishu.cn/calendar/event/evt_123",
                    "start_time": {"timestamp": "1710000000", "timezone": "Asia/Shanghai"},
                    "end_time": {"timestamp": "1710003600", "timezone": "Asia/Shanghai"},
                    "vchat": {"vc_type": "vc"},
                }
            }
        if "/events/" in path and method == "DELETE":
            return {"ok": True}
        if path == "/task/v2/tasks" and method == "POST":
            return {"task": {"guid": "tsk_123", "summary": (data or {}).get("summary", "")}}
        if path == "/task/v2/tasks" and method == "GET":
            return {"items": [{"guid": "tsk_123", "summary": "准备季度述职"}]}
        if path == "/task/v2/tasks/tsk_123" and method == "PATCH":
            return {"task": {"guid": "tsk_123", "completed_at": (data or {}).get("task", {}).get("completed_at")}}
        if path.startswith("/task/v2/tasks/") and method == "DELETE":
            return {"ok": True}
        if path == "/im/v1/chats":
            return {"items": [{"chat_id": "oc_group_123", "name": "产品群", "chat_type": "group", "member_count": 12}]}
        if path == "/im/v1/messages/search":
            return {"items": [{"message_id": "om_search_1"}]}
        if path == "/im/v1/messages" and method == "GET":
            return {"items": [{"message_id": "om_history_1", "create_time": "1710000000000", "body": {"content": "{\"text\":\"hello\"}"}, "sender": {"id": "ou_owner"}, "msg_type": "text"}]}
        raise AssertionError(f"Unhandled API call: {method} {path}")

    def user_api(self, method: str, path: str, *, data=None, params=None):  # type: ignore[override]
        return self.api(method, path, data=data, params=params)


def build_agent(sample_env) -> FakeAgent:
    env = {
        "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
        "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
        "FEISHU_APP_ID": "cli_test",
        "FEISHU_APP_SECRET": "secret",
        "FEISHU_OWNER_OPEN_ID": "ou_owner",
    }
    return FakeAgent(env=env)


def test_msg_send_resolves_chat_alias(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.msg_send({"to": "产品群", "text": "hello"})
    assert payload["message_id"] == "om_msg_123"
    method, path, data, params = agent.calls[-1]
    assert (method, path) == ("POST", "/im/v1/messages")
    assert data["receive_id"] == "oc_group_123"
    assert params["receive_id_type"] == "chat_id"


def test_user_get_and_search(sample_env) -> None:
    agent = build_agent(sample_env)
    get_payload = agent.user_get({"email": "operator@example.com"})
    search_payload = agent.user_search({"name": "Frank"})
    assert get_payload["users"][0]["user_id"] == "ou_lookup"
    assert search_payload["users"][0]["open_id"] == "ou_search"


def test_doc_create_uses_folder_alias_and_owner_permission(sample_env, tmp_path) -> None:
    agent = build_agent(sample_env)
    content_path = tmp_path / "doc.md"
    content_path.write_text("# 标题\n\n正文", encoding="utf-8")
    payload = agent.doc_create({"title": "周报", "folder": "报告", "file": str(content_path)})
    assert payload["document_id"] == "doc_123"
    paths = [call[1] for call in agent.calls]
    assert "/docx/v1/documents" in paths
    assert any(path.endswith("/children") for path in paths)
    assert any(path.startswith("/drive/v1/permissions/") for path in paths)


def test_table_operations_resolve_aliases(sample_env) -> None:
    agent = build_agent(sample_env)
    add_payload = agent.table_add({"table": "书单", "data": {"书名": "穷查理宝典"}})
    records_payload = agent.table_records({"table": "书单"})
    update_payload = agent.table_update({"table": "书单", "record": "rec_123", "data": {"状态": "完成"}})
    delete_payload = agent.table_delete({"table": "书单", "record": "rec_123"})
    fields_payload = agent.table_fields({"table": "书单"})
    tables_payload = agent.table_tables({"app": "书单"})
    assert add_payload["record_id"] == "rec_123"
    assert records_payload["total"] == 1
    assert update_payload["record"]["record_id"] == "rec_123"
    assert delete_payload["ok"] is True
    assert fields_payload["fields"][0]["field_name"] == "书名"
    assert tables_payload["tables"][0]["table_id"] == "tbl_book"


def test_table_creation_supports_new_base_table_and_field(sample_env) -> None:
    agent = build_agent(sample_env)
    create_app_payload = agent.table_create_app(
        {
            "name": "阅读计划",
            "table_name": "书单",
            "fields": [
                {"field_name": "书名", "type": 1},
                {"field_name": "状态", "type": 3},
            ],
        }
    )
    create_table_payload = agent.table_create(
        {
            "app": "书单",
            "name": "需求池",
            "fields": [{"field_name": "标题", "type": 1}],
        }
    )
    create_field_payload = agent.table_create_field({"table": "书单", "field_name": "作者", "type": 1})
    assert create_app_payload["app_token"] == "app_new"
    assert create_app_payload["table"]["table_id"] == "tbl_default"
    assert create_table_payload["table_id"] == "tbl_extra"
    assert create_field_payload["field_id"] == "fld_author"


def test_table_view_operations_support_list_create_get_and_update(sample_env) -> None:
    agent = build_agent(sample_env)
    create_payload = agent.table_create_view({"table": "书单", "name": "按状态看板", "type": "kanban"})
    list_payload = agent.table_views({"table": "书单"})
    get_payload = agent.table_get_view({"table": "书单", "view": create_payload["view"]["view_id"]})
    update_payload = agent.table_update_view(
        {"table": "书单", "view": create_payload["view"]["view_id"], "name": "全部书单", "type": "grid"}
    )
    assert create_payload["view"]["view_name"] == "按状态看板"
    assert list_payload["views"][0]["view_id"] == create_payload["view"]["view_id"]
    assert get_payload["view"]["view_id"] == create_payload["view"]["view_id"]
    assert update_payload["view"]["view_name"] == "全部书单"
    assert update_payload["view"]["view_type"] == "grid"


def test_calendar_event_creation_uses_defaults_and_owner(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.cal_add({"title": "产品评审会", "start": "2026-03-18 15:00", "end": "2026-03-18 16:00", "location": "3楼"})
    assert payload["calendar_id"] == "cal_default"
    assert payload["event"]["event_id"] == "evt_123"
    attendee_calls = [call for call in agent.calls if call[1].endswith("/attendees")]
    assert attendee_calls


def test_calendar_default_alias_uses_default_calendar(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.cal_list({"calendar": "default"})
    assert payload["calendar_id"] == "cal_default"
    assert payload["events"][0]["id"] == "evt_123"


def test_task_operations_use_owner(sample_env) -> None:
    agent = build_agent(sample_env)
    add_payload = agent.task_add({"title": "准备季度述职", "due": "2026-03-25 18:00"})
    list_payload = agent.task_list({})
    done_payload = agent.task_done({"id": "tsk_123"})
    delete_payload = agent.task_delete({"id": "tsk_123"})
    assert add_payload["task_id"] == "tsk_123"
    assert list_payload["tasks"][0]["guid"] == "tsk_123"
    assert done_payload["ok"] is True
    assert delete_payload["ok"] is True
    done_call = next(call for call in agent.calls if call[1] == "/task/v2/tasks/tsk_123" and call[0] == "PATCH")
    assert done_call[2]["update_fields"] == ["completed_at"]
    assert "completed_at" in done_call[2]["task"]


def test_task_operations_require_user_access_token_when_not_provided(sample_env) -> None:
    agent = feishu_agent.FeishuAgent(
        env={
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "FEISHU_APP_ID": "cli_test",
            "FEISHU_APP_SECRET": "secret",
            "FEISHU_OWNER_OPEN_ID": "ou_owner",
        }
    )
    try:
        agent.task_list({})
    except feishu_agent.FeishuAgentError as exc:
        assert exc.code == "missing_user_access_token"
    else:
        raise AssertionError("expected missing_user_access_token")


def test_auth_status_reads_token_store(sample_env) -> None:
    token_store = sample_env["runtime_root"] / "feishu_user_token.json"
    token_store.write_text(
        json.dumps(
            {
                "access_token": "u_test_access",
                "refresh_token": "u_test_refresh",
                "access_token_expire_at": "2026-03-18T12:00:00",
                "refresh_token_expire_at": "2026-03-24T12:00:00",
                "redirect_uri": "http://127.0.0.1:14589/feishu-auth/callback",
                "auth_method": "oidc_v1",
                "profile": {"name": "Frank"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    agent = feishu_agent.FeishuAgent(
        env={
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "FEISHU_APP_ID": "cli_test",
            "FEISHU_APP_SECRET": "secret",
        }
    )
    payload = agent.auth_status({})
    assert payload["has_user_access_token"] is True
    assert payload["has_refresh_token"] is True
    assert payload["auto_refresh_ready"] is True
    assert payload["auth_method"] == "oidc_v1"
    assert payload["profile"]["name"] == "Frank"


def test_user_token_auto_refresh_uses_oidc_and_preserves_refresh_token(sample_env) -> None:
    token_store = sample_env["runtime_root"] / "feishu_user_token.json"
    token_store.write_text(
        json.dumps(
            {
                "access_token": "u_expired",
                "refresh_token": "u_refresh",
                "access_token_expire_at": "2000-01-01T00:00:00",
                "refresh_token_expire_at": "2026-03-24T12:00:00",
                "redirect_uri": "http://127.0.0.1:14589/feishu-auth/callback",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class RefreshingAgent(feishu_agent.FeishuAgent):
        def _token(self) -> str:  # type: ignore[override]
            return "tenant_access_token"

        def _http(self, method: str, path: str, *, data=None, params=None, token=None):  # type: ignore[override]
            if path == "/authen/v1/oidc/refresh_access_token":
                assert token == "tenant_access_token"
                assert data["grant_type"] == "refresh_token"
                assert data["refresh_token"] == "u_refresh"
                return {"access_token": "u_refreshed", "expires_in": 7200}
            raise AssertionError(f"Unhandled API call: {method} {path}")

    agent = RefreshingAgent(
        env={
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "FEISHU_APP_ID": "cli_test",
            "FEISHU_APP_SECRET": "secret",
        }
    )
    assert agent._user_token() == "u_refreshed"
    stored = json.loads(token_store.read_text(encoding="utf-8"))
    assert stored["access_token"] == "u_refreshed"
    assert stored["refresh_token"] == "u_refresh"
    assert stored["auth_method"] == "oidc_v1"


def test_meeting_create_and_queries_use_vchat(sample_env) -> None:
    agent = build_agent(sample_env)
    create_payload = agent.meeting_create({"title": "SampleProj 周会", "start": "2026-03-18 19:00", "attendees": ["operator@example.com"]})
    get_payload = agent.meeting_get({"id": "evt_123", "calendar": "cal_meeting"})
    list_payload = agent.meeting_list({"calendar": "cal_meeting"})
    cancel_payload = agent.meeting_cancel({"id": "evt_123", "calendar": "cal_meeting"})
    assert create_payload["meeting"]["event_id"] == "evt_123"
    assert create_payload["calendar_id"] == "cal_meeting"
    assert create_payload["attendees"][0]["type"] == "user"
    assert create_payload["attendees"][0]["id"] == "ou_lookup"
    create_call = next(call for call in agent.calls if call[1].endswith("/events") and call[0] == "POST")
    assert create_call[2]["vchat"]["vc_type"] == "vc"
    assert get_payload["meeting"]["event_id"] == "evt_123"
    assert list_payload["meetings"][0]["event_id"] == "evt_123"
    assert cancel_payload["meeting_id"] == "evt_123"


def test_dispatch_supports_all_domains(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.perform("msg", "search", {"query": "hello"})
    assert payload["ok"] is True
    assert payload["domain"] == "msg"
    assert payload["action"] == "search"


def test_broker_feishu_op_returns_structured_result(sample_env, monkeypatch, capsys) -> None:
    from ops import local_broker

    monkeypatch.setattr(
        local_broker.feishu_agent,
        "perform_operation",
        lambda domain, action, payload: {"ok": True, "domain": domain, "action": action, "result": {"document_id": "doc_123"}},
    )
    exit_code = local_broker.cmd_feishu_op(
        argparse.Namespace(domain="doc", action="create", payload_json=json.dumps({"title": "周报"}))
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is True
    assert payload["broker_action"] == "feishu_op"
    assert payload["domain"] == "doc"
    assert payload["action"] == "create"
    assert payload["result"]["result"]["document_id"] == "doc_123"

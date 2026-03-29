from __future__ import annotations

import argparse
import json

from ops import feishu_agent


class FakeAgent(feishu_agent.FeishuAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls: list[tuple[str, str, dict | None, dict | None]] = []
        self._views: dict[tuple[str, str], list[dict[str, str]]] = {}
        self._doc_blocks: dict[str, dict[str, dict[str, object]]] = {}

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
            document_id = path.split("/")[4]
            doc_blocks = self._doc_blocks.setdefault(document_id, {})
            children = []
            for index, child in enumerate((data or {}).get("children") or []):
                block_type = child.get("block_type")
                block_id = f"blk_{len(doc_blocks) + index + 1}"
                if block_type == 27:
                    block = {
                        "block_id": block_id,
                        "block_type": 27,
                        "image": {"align": 2, "height": 100, "scale": 1, "token": "", "width": 100},
                        "parent_id": document_id,
                    }
                else:
                    block = {"block_id": block_id, "block_type": block_type, "parent_id": document_id}
                doc_blocks[block_id] = block
                children.append(block)
            return {"children": children}
        if path.startswith("/docx/v1/documents/") and "/blocks/" in path and method == "PATCH":
            parts = path.split("/")
            document_id = parts[4]
            block_id = parts[6]
            block = self._doc_blocks.setdefault(document_id, {}).setdefault(
                block_id,
                {"block_id": block_id, "block_type": 27, "image": {"token": ""}, "parent_id": document_id},
            )
            replace_image = (data or {}).get("replace_image") or {}
            block["image"] = {
                "align": 2,
                "height": 800,
                "scale": 1,
                "token": str(replace_image.get("token") or ""),
                "width": 1200,
            }
            return {"block": block}
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

    def _http_multipart(self, method: str, path: str, *, data=None, files=None, token=None):  # type: ignore[override]
        self.calls.append((method, path, data, {"token": token, "files": sorted((files or {}).keys())}))
        if path == "/drive/v1/medias/upload_all":
            return {"file_token": "img_token_123"}
        raise AssertionError(f"Unhandled multipart call: {method} {path}")

    def _token(self) -> str:  # type: ignore[override]
        return "tenant_access_token"

    def user_api(self, method: str, path: str, *, data=None, params=None):  # type: ignore[override]
        return self.api(method, path, data=data, params=params)


def build_agent(sample_env) -> FakeAgent:
    env = {
        "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
        "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
        "WORKSPACE_HUB_FEISHU_BACKEND": "legacy",
        "FEISHU_APP_ID": "cli_test",
        "FEISHU_APP_SECRET": "secret",
        "FEISHU_OWNER_OPEN_ID": "ou_owner",
    }
    return FakeAgent(env=env)


def build_plain_agent(sample_env) -> feishu_agent.FeishuAgent:
    env = {
        "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
        "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
        "WORKSPACE_HUB_FEISHU_BACKEND": "legacy",
        "FEISHU_APP_ID": "cli_test",
        "FEISHU_APP_SECRET": "secret",
        "FEISHU_OWNER_OPEN_ID": "ou_owner",
    }
    return feishu_agent.FeishuAgent(env=env)


def test_msg_send_resolves_chat_alias(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.msg_send({"to": "产品群", "text": "hello"})
    assert payload["message_id"] == "om_msg_123"
    method, path, data, params = agent.calls[-1]
    assert (method, path) == ("POST", "/im/v1/messages")
    assert data["receive_id"] == "oc_group_123"
    assert params["receive_id_type"] == "chat_id"


def test_msg_send_supports_interactive_card(sample_env) -> None:
    agent = build_agent(sample_env)
    payload = agent.msg_send(
        {
            "to": "operator@example.com",
            "msg_type": "interactive",
            "card": {
                "schema": "2.0",
                "header": {"title": {"tag": "plain_text", "content": "审批卡片"}},
                "body": {"elements": [{"tag": "markdown", "content": "请确认"}]},
            },
        }
    )
    assert payload["message_id"] == "om_msg_123"
    method, path, data, params = agent.calls[-1]
    assert (method, path) == ("POST", "/im/v1/messages")
    assert params["receive_id_type"] == "open_id"
    assert data["msg_type"] == "interactive"
    card = json.loads(data["content"])
    assert card["header"]["title"]["content"] == "审批卡片"


def test_msg_send_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "im")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "im_send",
        lambda **kwargs: {"message_id": "om_cli_send_1", "backend": "lark-cli", "args": kwargs},
    )

    payload = agent.msg_send({"to": "产品群", "text": "hello"})

    assert payload["message_id"] == "om_cli_send_1"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_msg_reply_prefers_lark_cli_backend_and_keeps_cards(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "im")
    captured: dict[str, object] = {}

    def _reply(**kwargs):
        captured.update(kwargs)
        return {"message_id": "om_cli_reply_1", "backend": "lark-cli"}

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "im_reply", _reply)

    payload = agent.msg_reply(
        {
            "message_id": "om_parent_1",
            "msg_type": "interactive",
            "card": {"schema": "2.0", "header": {"title": {"tag": "plain_text", "content": "审批"}}},
        }
    )

    assert payload["message_id"] == "om_cli_reply_1"
    assert payload["backend"] == "lark-cli"
    assert captured["msg_type"] == "interactive"
    assert json.loads(str(captured["content"]))["header"]["title"]["content"] == "审批"
    assert agent.calls == []


def test_msg_history_and_chats_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "im")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "im_chat_messages_list",
        lambda **kwargs: {
            "messages": [
                {
                    "message_id": "om_cli_history_1",
                    "create_time": "1710000000000",
                    "body": {"content": "{\"text\":\"hello from cli\"}"},
                    "sender": {"id": "ou_cli"},
                    "msg_type": "text",
                }
            ],
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "im_chat_search",
        lambda **kwargs: {
            "chats": [{"chat_id": "oc_cli_1", "name": "产品群", "chat_type": "group", "member_count": 10}],
            "backend": "lark-cli",
        },
    )

    history_payload = agent.msg_history({"chat": "产品群", "limit": 5})
    chats_payload = agent.msg_chats({"query": "产品", "limit": 5})

    assert history_payload["backend"] == "lark-cli"
    assert history_payload["messages"][0]["id"] == "om_cli_history_1"
    assert history_payload["messages"][0]["content"]["text"] == "hello from cli"
    assert chats_payload["backend"] == "lark-cli"
    assert chats_payload["chats"][0]["id"] == "oc_cli_1"
    assert agent.calls == []


def test_msg_search_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "im")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "im_messages_search",
        lambda **kwargs: {"messages": [{"message_id": "om_cli_search_1"}], "backend": "lark-cli"},
    )

    payload = agent.msg_search({"query": "Program Harness", "limit": 5})

    assert payload["backend"] == "lark-cli"
    assert payload["messages"][0]["message_id"] == "om_cli_search_1"
    assert agent.calls == []


def test_msg_media_and_resource_download_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "im")
    captured: dict[str, dict[str, object]] = {}

    def _send(**kwargs):
        captured["send"] = kwargs
        return {"message_id": "om_cli_media_send", "backend": "lark-cli"}

    def _reply(**kwargs):
        captured["reply"] = kwargs
        return {"message_id": "om_cli_media_reply", "backend": "lark-cli"}

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "im_send", _send)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "im_reply", _reply)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "im_download_resources",
        lambda **kwargs: {
            "message_id": kwargs["message_id"],
            "file_key": kwargs["file_key"],
            "type": kwargs["resource_type"],
            "path": "/tmp/cli-download.bin",
            "backend": "lark-cli",
        },
    )

    send_payload = agent.msg_send({"to": "产品群", "msg_type": "file", "file": "/tmp/demo.pdf"})
    reply_payload = agent.msg_reply(
        {"message_id": "om_parent_1", "msg_type": "image", "image": "img_cli_token", "reply_in_thread": True}
    )
    download_payload = agent.msg_download_resources(
        {"message_id": "om_parent_1", "file_key": "file_cli_key", "type": "file"}
    )

    assert send_payload["backend"] == "lark-cli"
    assert captured["send"]["chat_id"] == "oc_group_123"
    assert captured["send"]["file"] == "/tmp/demo.pdf"
    assert reply_payload["backend"] == "lark-cli"
    assert captured["reply"]["image"] == "img_cli_token"
    assert captured["reply"]["reply_in_thread"] is True
    assert download_payload["backend"] == "lark-cli"
    assert download_payload["path"] == "/tmp/cli-download.bin"
    assert agent.calls == []


def test_user_get_and_search(sample_env) -> None:
    agent = build_agent(sample_env)
    get_payload = agent.user_get({"email": "operator@example.com"})
    search_payload = agent.user_search({"name": "Frank"})
    assert get_payload["users"][0]["user_id"] == "ou_lookup"
    assert search_payload["users"][0]["open_id"] == "ou_search"


def test_user_get_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "contact")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "contact_get",
        lambda **kwargs: {"user": {"open_id": kwargs["user_id"], "name": "CLI User"}, "backend": "lark-cli"},
    )

    payload = agent.user_get({"id": "ou_cli_123"})

    assert payload["user"]["open_id"] == "ou_cli_123"
    assert payload["backend"] == "lark-cli"


def test_user_search_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "contact")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "contact_search",
        lambda **kwargs: {
            "users": [{"open_id": "ou_cli_search", "name": kwargs["query"]}],
            "backend": "lark-cli",
        },
    )

    payload = agent.user_search({"name": "Frank", "limit": 5})

    assert payload["users"][0]["open_id"] == "ou_cli_search"
    assert payload["backend"] == "lark-cli"


def test_doc_create_uses_folder_alias_and_owner_permission(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    content_path = tmp_path / "doc.md"
    content_path.write_text("# 标题\n\n正文", encoding="utf-8")
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: False)
    payload = agent.doc_create({"title": "周报", "folder": "报告", "file": str(content_path)})
    assert payload["document_id"] == "doc_123"
    paths = [call[1] for call in agent.calls]
    assert "/docx/v1/documents" in paths
    assert any(path.endswith("/children") for path in paths)
    assert any(path.startswith("/drive/v1/permissions/") for path in paths)


def test_doc_insert_image_creates_block_uploads_media_and_replaces_token(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    image_path = tmp_path / "diagram.png"
    image_path.write_bytes(b"png-bytes")
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: False)
    payload = agent.doc_insert_image({"id": "doc_123", "file_path": str(image_path), "index": 0})
    assert payload["document_id"] == "doc_123"
    assert payload["file_token"] == "img_token_123"
    assert payload["block"]["image"]["token"] == "img_token_123"
    paths = [call[1] for call in agent.calls]
    assert "/drive/v1/medias/upload_all" in paths
    assert "/docx/v1/documents/doc_123/blocks/doc_123/children" in paths
    assert "/docx/v1/documents/doc_123/blocks/blk_1" in paths


def test_doc_create_prefers_lark_cli_backend_for_owner_scoped_docs(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    content_path = tmp_path / "doc.md"
    content_path.write_text("# 标题\n\n正文", encoding="utf-8")

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "doc_create",
        lambda **kwargs: {
            "ok": True,
            "document_id": "doc_cli_123",
            "url": "https://feishu.cn/docx/doc_cli_123",
            "backend": "lark-cli",
        },
    )

    payload = agent.doc_create({"title": "CLI 周报", "file": str(content_path)})

    assert payload["document_id"] == "doc_cli_123"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_doc_create_falls_back_when_share_target_is_not_owner(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    content_path = tmp_path / "doc.md"
    content_path.write_text("# 标题\n\n正文", encoding="utf-8")

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)

    payload = agent.doc_create({"title": "共享周报", "file": str(content_path), "share_to": "operator@example.com"})

    assert payload["document_id"] == "doc_123"
    paths = [call[1] for call in agent.calls]
    assert "/docx/v1/documents" in paths
    assert any(path.startswith("/drive/v1/permissions/") for path in paths)


def test_doc_insert_image_prefers_lark_cli_backend(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    image_path = tmp_path / "diagram.png"
    image_path.write_bytes(b"png-bytes")

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "doc_insert_image",
        lambda **kwargs: {
            "ok": True,
            "document_id": "doc_cli_123",
            "url": "https://feishu.cn/docx/doc_cli_123",
            "file_path": kwargs["file_path"],
            "backend": "lark-cli",
        },
    )

    payload = agent.doc_insert_image({"id": "doc_123", "file_path": str(image_path), "index": 0})

    assert payload["document_id"] == "doc_cli_123"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_doc_get_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "doc_fetch",
        lambda **kwargs: {
            "document_id": kwargs["document"],
            "content": "来自 lark-cli",
            "backend": "lark-cli",
        },
    )

    payload = agent.doc_get({"id": "doc_cli_123"})

    assert payload["document_id"] == "doc_cli_123"
    assert payload["content"] == "来自 lark-cli"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_doc_search_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "doc_search",
        lambda **kwargs: {
            "files": [{"title": kwargs["query"], "obj_token": "doc_cli_search"}],
            "page_token": "",
            "has_more": False,
            "backend": "lark-cli",
        },
    )

    payload = agent.doc_search({"query": "Codex Hub", "limit": 5})

    assert payload["files"][0]["obj_token"] == "doc_cli_search"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_doc_list_prefers_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "doc_backend_enabled", lambda env=None: True)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "doc_list",
        lambda **kwargs: {
            "files": [{"token": "doc_cli_list_1", "name": "CLI 文档"}],
            "backend": "lark-cli",
        },
    )

    payload = agent.doc_list({"folder": "报告", "limit": 5})

    assert payload["files"][0]["token"] == "doc_cli_list_1"
    assert payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_drive_operations_prefer_lark_cli_backend(sample_env, tmp_path, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "drive")
    captured: dict[str, dict[str, object]] = {}

    def _upload(**kwargs):
        captured["upload"] = kwargs
        return {"file_token": "file_cli_1", "name": "weekly.md", "url": "https://feishu.cn/file/file_cli_1", "backend": "lark-cli"}

    def _download(**kwargs):
        captured["download"] = kwargs
        return {"file_token": kwargs["file_token"], "path": "/tmp/weekly.md", "backend": "lark-cli"}

    def _comment(**kwargs):
        captured["comment"] = kwargs
        return {"comment_id": "cmt_cli_1", "backend": "lark-cli"}

    monkeypatch.setattr(feishu_agent.lark_cli_backend, "drive_upload", _upload)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "drive_download", _download)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "drive_add_comment", _comment)

    file_path = tmp_path / "weekly.md"
    file_path.write_text("# Weekly", encoding="utf-8")

    upload_payload = agent.drive_upload({"file_path": str(file_path), "folder": "报告", "name": "weekly.md"})
    download_payload = agent.drive_download({"file_token": "file_cli_1", "output": "/tmp"})
    comment_payload = agent.drive_add_comment({"doc": "https://feishu.cn/docx/doc_123", "content": "请补充结论"})

    assert upload_payload["backend"] == "lark-cli"
    assert captured["upload"]["folder_token"]
    assert download_payload["backend"] == "lark-cli"
    assert captured["download"]["file_token"] == "file_cli_1"
    assert comment_payload["backend"] == "lark-cli"
    assert captured["comment"]["doc"] == "https://feishu.cn/docx/doc_123"
    assert agent.calls == []


def test_resolve_folder_token_ignores_unresolved_human_name(sample_env) -> None:
    agent = build_agent(sample_env)
    assert agent.resolve_folder_token("不存在的目录名") == ""


def test_user_api_routes_bitable_calls_through_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_plain_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "table")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "api_call",
        lambda method, path, **kwargs: {
            "items": [{"field_id": "fld_cli_1"}],
            "path": path,
            "method": method,
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(agent, "_user_token", lambda: (_ for _ in ()).throw(AssertionError("legacy user token should not be used")))

    payload = agent.user_api("GET", "/bitable/v1/apps/app_123/tables/tbl_123/fields", params={"page_size": 1})

    assert payload["backend"] == "lark-cli"
    assert payload["path"] == "/bitable/v1/apps/app_123/tables/tbl_123/fields"


def test_api_routes_calendar_calls_through_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_plain_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "calendar")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "api_call",
        lambda method, path, **kwargs: {
            "items": [{"event_id": "evt_cli_1"}],
            "path": path,
            "method": method,
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(agent, "_token", lambda: (_ for _ in ()).throw(AssertionError("legacy tenant token should not be used")))

    payload = agent.api("GET", "/calendar/v4/calendars/default/events", params={"page_size": 1})

    assert payload["backend"] == "lark-cli"
    assert payload["path"] == "/calendar/v4/calendars/default/events"


def test_table_read_operations_prefer_lark_cli_base_shortcuts(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "table")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_table_list",
        lambda **kwargs: {
            "tables": [{"table_id": "tbl_cli_1", "table_name": "CLI 表"}],
            "total": 1,
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_get",
        lambda **kwargs: {
            "base": {"base_token": kwargs["base_token"], "name": "CLI Base"},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_field_list",
        lambda **kwargs: {
            "fields": [{"field_id": "fld_cli_1", "field_name": "CLI 字段"}],
            "total": 1,
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_view_list",
        lambda **kwargs: {
            "views": [{"view_id": "vew_cli_1", "view_name": "CLI 视图"}],
            "total": 1,
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_record_list",
        lambda **kwargs: {
            "records": [{"record_id": "rec_cli_1", "fields": {"书名": "CLI 书单"}}],
            "fields": ["书名"],
            "record_id_list": ["rec_cli_1"],
            "total": 1,
            "backend": "lark-cli",
        },
    )

    tables_payload = agent.table_tables({"app": "书单"})
    app_payload = agent.table_get_app({"app": "书单"})
    fields_payload = agent.table_fields({"table": "书单"})
    views_payload = agent.table_views({"table": "书单"})
    records_payload = agent.table_records({"table": "书单", "limit": 5})

    assert tables_payload["tables"][0]["table_id"] == "tbl_cli_1"
    assert tables_payload["backend"] == "lark-cli"
    assert app_payload["app"]["name"] == "CLI Base"
    assert app_payload["backend"] == "lark-cli"
    assert fields_payload["fields"][0]["field_name"] == "CLI 字段"
    assert fields_payload["backend"] == "lark-cli"
    assert views_payload["views"][0]["view_id"] == "vew_cli_1"
    assert views_payload["backend"] == "lark-cli"
    assert records_payload["records"][0]["record_id"] == "rec_cli_1"
    assert records_payload["backend"] == "lark-cli"
    assert agent.calls == []


def test_table_write_operations_prefer_lark_cli_record_upsert(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "table")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_record_upsert",
        lambda **kwargs: {
            "record_id": kwargs.get("record_id") or "rec_cli_new",
            "record": {"record_id": kwargs.get("record_id") or "rec_cli_new", "fields": kwargs["fields"]},
            "backend": "lark-cli",
        },
    )

    add_payload = agent.table_add({"table": "书单", "data": {"书名": "CLI 新书"}})
    update_payload = agent.table_update({"table": "书单", "record": "rec_cli_old", "data": {"状态": "完成"}})

    assert add_payload["record_id"] == "rec_cli_new"
    assert add_payload["backend"] == "lark-cli"
    assert update_payload["record"]["record_id"] == "rec_cli_old"
    assert update_payload["backend"] == "lark-cli"
    assert agent.calls == []


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


def test_table_advanced_operations_prefer_explicit_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "table")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_app_create",
        lambda **kwargs: {
            "app": {
                "app_token": "app_cli_new",
                "default_table_id": "tbl_cli_default",
                "url": "https://feishu.cn/base/app_cli_new",
            },
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_table_create",
        lambda **kwargs: {
            "table": {"table_id": "tbl_cli_new", "name": kwargs["name"], "default_view_id": "vew_cli_new"},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_field_create",
        lambda **kwargs: {
            "field": {"field_id": "fld_cli_new", "field_name": kwargs["field"]["field_name"]},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_field_update",
        lambda **kwargs: {
            "field": {"field_id": kwargs["field_id"], "field_name": kwargs["field"]["field_name"]},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_field_delete",
        lambda **kwargs: {"ok": True, "field_id": kwargs["field_id"], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_view_get",
        lambda **kwargs: {
            "view": {"view_id": kwargs["view_id"], "view_name": "CLI 视图", "view_type": "grid"},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_view_create",
        lambda **kwargs: {
            "view": {"view_id": "vew_cli_new", "view_name": kwargs["view"]["view_name"], "view_type": kwargs["view"]["view_type"]},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_view_update",
        lambda **kwargs: {
            "view": {"view_id": kwargs["view_id"], "view_name": kwargs["name"], "view_type": "grid"},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_view_delete",
        lambda **kwargs: {"ok": True, "view_id": kwargs["view_id"], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_table_delete",
        lambda **kwargs: {"ok": True, "table_id": kwargs["table_id"], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "base_record_delete",
        lambda **kwargs: {"ok": True, "record_id": kwargs["record_id"], "backend": "lark-cli"},
    )

    create_app_payload = agent.table_create_app({"name": "CLI Base", "table_name": "表1"})
    create_table_payload = agent.table_create({"app": "书单", "name": "CLI 扩展表"})
    create_field_payload = agent.table_create_field({"table": "书单", "field_name": "标签", "type": 1})
    update_field_payload = agent.table_update_field({"table": "书单", "field": "fld_cli_new", "field_name": "标签（新）"})
    delete_field_payload = agent.table_delete_field({"table": "书单", "field": "fld_cli_new"})
    create_view_payload = agent.table_create_view({"table": "书单", "name": "CLI 视图", "type": "grid"})
    get_view_payload = agent.table_get_view({"table": "书单", "view": "vew_cli_new"})
    update_view_payload = agent.table_update_view({"table": "书单", "view": "vew_cli_new", "name": "CLI 重命名视图"})
    delete_view_payload = agent.table_delete_view({"table": "书单", "view": "vew_cli_new"})
    delete_table_payload = agent.table_delete_table({"table": "书单"})
    delete_record_payload = agent.table_delete({"table": "书单", "record": "rec_cli_1"})

    assert create_app_payload["backend"] == "lark-cli"
    assert create_app_payload["app_token"] == "app_cli_new"
    assert create_table_payload["backend"] == "lark-cli"
    assert create_table_payload["table_id"] == "tbl_cli_new"
    assert create_field_payload["backend"] == "lark-cli"
    assert create_field_payload["field_id"] == "fld_cli_new"
    assert update_field_payload["backend"] == "lark-cli"
    assert update_field_payload["field"]["field_name"] == "标签（新）"
    assert delete_field_payload["backend"] == "lark-cli"
    assert create_view_payload["backend"] == "lark-cli"
    assert get_view_payload["backend"] == "lark-cli"
    assert update_view_payload["backend"] == "lark-cli"
    assert update_view_payload["view"]["view_name"] == "CLI 重命名视图"
    assert delete_view_payload["backend"] == "lark-cli"
    assert delete_table_payload["backend"] == "lark-cli"
    assert delete_record_payload["backend"] == "lark-cli"
    assert agent.calls == []


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


def test_calendar_operations_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "calendar")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_agenda",
        lambda **kwargs: {
            "calendar_id": kwargs["calendar_id"],
            "events": [{"event_id": "evt_cli_1", "summary": "CLI 会议"}],
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_create",
        lambda **kwargs: {
            "calendar_id": kwargs["calendar_id"],
            "event": {"event_id": "evt_cli_create", "summary": kwargs["summary"]},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_delete",
        lambda **kwargs: {
            "ok": True,
            "calendar_id": kwargs["calendar_id"],
            "event_id": kwargs["event_id"],
            "backend": "lark-cli",
        },
    )

    list_payload = agent.cal_list({"calendar": "default"})
    add_payload = agent.cal_add({"title": "CLI 产品评审会", "start": "2026-03-18 15:00", "end": "2026-03-18 16:00"})
    delete_payload = agent.cal_delete({"calendar": "default", "id": "evt_cli_1"})

    assert list_payload["backend"] == "lark-cli"
    assert list_payload["events"][0]["id"] == "evt_cli_1"
    assert add_payload["backend"] == "lark-cli"
    assert add_payload["event"]["event_id"] == "evt_cli_create"
    assert delete_payload["backend"] == "lark-cli"
    assert delete_payload["event_id"] == "evt_cli_1"
    assert agent.calls == []


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


def test_task_operations_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "task")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "task_list",
        lambda **kwargs: {"tasks": [{"guid": "tsk_cli_1", "summary": "CLI 任务"}], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "task_create",
        lambda **kwargs: {
            "task_id": "tsk_cli_1",
            "task": {"guid": "tsk_cli_1", "summary": kwargs["summary"]},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "task_complete",
        lambda **kwargs: {"ok": True, "task_id": kwargs["task_id"], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "task_delete",
        lambda **kwargs: {"ok": True, "task_id": kwargs["task_id"], "backend": "lark-cli"},
    )

    add_payload = agent.task_add({"title": "CLI 准备季度述职", "due": "2026-03-25 18:00"})
    list_payload = agent.task_list({"query": "CLI"})
    done_payload = agent.task_done({"id": "tsk_cli_1"})
    delete_payload = agent.task_delete({"id": "tsk_cli_1"})

    assert add_payload["backend"] == "lark-cli"
    assert add_payload["task_id"] == "tsk_cli_1"
    assert list_payload["backend"] == "lark-cli"
    assert list_payload["tasks"][0]["guid"] == "tsk_cli_1"
    assert done_payload["backend"] == "lark-cli"
    assert done_payload["task_id"] == "tsk_cli_1"
    assert delete_payload["backend"] == "lark-cli"
    assert delete_payload["task_id"] == "tsk_cli_1"
    assert agent.calls == []


def test_task_operations_require_user_access_token_when_legacy_backend_forced(sample_env) -> None:
    agent = feishu_agent.FeishuAgent(
        env={
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "WORKSPACE_HUB_FEISHU_BACKEND": "legacy",
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
    create_payload = agent.meeting_create({"title": "项目周会", "start": "2026-03-18 19:00", "attendees": ["operator@example.com"]})
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


def test_meeting_operations_prefer_lark_cli_calendar_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(feishu_agent.lark_cli_backend, "backend_enabled", lambda domain, env=None: domain == "calendar")
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "api_call",
        lambda method, path, **kwargs: (
            {"event": {"event_id": "evt_cli_meeting", "summary": "CLI 周会", "app_link": "https://feishu.cn/calendar/event/evt_cli_meeting", "start_time": {"timestamp": "1710000000", "timezone": "Asia/Shanghai"}, "end_time": {"timestamp": "1710003600", "timezone": "Asia/Shanghai"}, "vchat": {"vc_type": "vc"}}}
            if method == "POST" and path.endswith("/events")
            else {"ok": True}
            if method == "POST" and path.endswith("/attendees")
            else {"event": {"event_id": "evt_cli_meeting", "summary": "CLI 周会", "app_link": "https://feishu.cn/calendar/event/evt_cli_meeting", "start_time": {"timestamp": "1710000000", "timezone": "Asia/Shanghai"}, "end_time": {"timestamp": "1710003600", "timezone": "Asia/Shanghai"}, "vchat": {"vc_type": "vc"}}}
            if method == "GET" and "/events/" in path
            else {"unexpected": [method, path, kwargs]}
        ),
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_agenda",
        lambda **kwargs: {
            "calendar_id": kwargs["calendar_id"],
            "events": [{"event_id": "evt_cli_meeting", "summary": "CLI 周会", "vchat": {"vc_type": "vc"}}],
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_get",
        lambda **kwargs: {
            "calendar_id": kwargs["calendar_id"],
            "event": {
                "event_id": kwargs["event_id"],
                "summary": "CLI 周会",
                "app_link": "https://feishu.cn/calendar/event/evt_cli_meeting",
                "start_time": {"timestamp": "1710000000", "timezone": "Asia/Shanghai"},
                "end_time": {"timestamp": "1710003600", "timezone": "Asia/Shanghai"},
                "vchat": {"vc_type": "vc"},
            },
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "calendar_delete",
        lambda **kwargs: {
            "ok": True,
            "calendar_id": kwargs["calendar_id"],
            "event_id": kwargs["event_id"],
            "backend": "lark-cli",
        },
    )

    create_payload = agent.meeting_create({"title": "CLI 周会", "start": "2026-03-18 19:00"})
    get_payload = agent.meeting_get({"id": "evt_cli_meeting", "calendar": "cal_meeting"})
    list_payload = agent.meeting_list({"calendar": "cal_meeting"})
    cancel_payload = agent.meeting_cancel({"id": "evt_cli_meeting", "calendar": "cal_meeting"})

    assert create_payload["meeting_id"] == "evt_cli_meeting"
    assert create_payload["meeting"]["vchat"]["vc_type"] == "vc"
    assert get_payload["meeting"]["event_id"] == "evt_cli_meeting"
    assert list_payload["meetings"][0]["event_id"] == "evt_cli_meeting"
    assert cancel_payload["meeting_id"] == "evt_cli_meeting"
    assert agent.calls == []


def test_vc_and_minutes_operations_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "backend_enabled",
        lambda domain, env=None: domain in {"vc", "minutes"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "vc_search",
        lambda **kwargs: {
            "meetings": [{"meeting_id": "mtg_cli_1", "topic": kwargs.get("query") or "CLI 周会"}],
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "vc_notes",
        lambda **kwargs: {
            "notes": [{"minute_token": "min_cli_1", "title": "CLI 纪要"}],
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "minutes_get",
        lambda **kwargs: {
            "minute": {"minute_token": kwargs["minute_token"], "title": "CLI 妙记"},
            "backend": "lark-cli",
        },
    )

    search_payload = agent.vc_search({"query": "CLI 周会"})
    notes_payload = agent.vc_notes({"meeting_ids": ["mtg_cli_1"]})
    minutes_payload = agent.minutes_get({"url": "https://feishu.cn/minutes/min_cli_1"})

    assert search_payload["backend"] == "lark-cli"
    assert search_payload["meetings"][0]["meeting_id"] == "mtg_cli_1"
    assert notes_payload["backend"] == "lark-cli"
    assert notes_payload["notes"][0]["minute_token"] == "min_cli_1"
    assert minutes_payload["backend"] == "lark-cli"
    assert minutes_payload["minute"]["minute_token"] == "min_cli_1"
    assert agent.calls == []


def test_sheet_wiki_mail_and_whiteboard_operations_prefer_lark_cli_backend(sample_env, monkeypatch) -> None:
    agent = build_agent(sample_env)
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "backend_enabled",
        lambda domain, env=None: domain in {"sheet", "wiki", "mail", "whiteboard"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_create",
        lambda **kwargs: {
            "spreadsheet_token": "sht_cli_1",
            "url": "https://feishu.cn/sheets/sht_cli_1",
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_info",
        lambda **kwargs: {
            "spreadsheet": {"spreadsheet_token": kwargs["spreadsheet_token"] or "sht_cli_1", "title": "CLI 表格"},
            "backend": "lark-cli",
        },
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_read",
        lambda **kwargs: {"values": [["A", "B"], ["1", "2"]], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_write",
        lambda **kwargs: {"ok": True, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_append",
        lambda **kwargs: {"ok": True, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "sheet_find",
        lambda **kwargs: {"matches": [{"sheet_id": "sheet1", "range": "A2", "text": kwargs["text"]}], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "wiki_get_node",
        lambda **kwargs: {"node": {"token": kwargs["token"], "title": "CLI Wiki"}, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "mail_triage",
        lambda **kwargs: {"messages": [{"message_id": "mail_cli_1", "subject": "CLI Mail"}], "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "mail_send",
        lambda **kwargs: {"message": {"message_id": "mail_cli_send_1"}, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "mail_reply",
        lambda **kwargs: {"message": {"message_id": "mail_cli_reply_1"}, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "mail_message",
        lambda **kwargs: {"message": {"message_id": kwargs["message_id"], "body": "<p>hello</p>"}, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "mail_thread",
        lambda **kwargs: {"thread": {"thread_id": kwargs["thread_id"], "messages": []}, "backend": "lark-cli"},
    )
    monkeypatch.setattr(
        feishu_agent.lark_cli_backend,
        "whiteboard_update",
        lambda **kwargs: {"whiteboard_token": kwargs["whiteboard_token"], "backend": "lark-cli"},
    )

    create_sheet_payload = agent.sheet_create({"title": "CLI 表格", "headers": ["A"], "data": [["1"]]})
    info_payload = agent.sheet_info({"spreadsheet": "sht_cli_1"})
    read_payload = agent.sheet_read({"spreadsheet": "sht_cli_1", "range": "A1:B2"})
    write_payload = agent.sheet_write({"spreadsheet": "sht_cli_1", "range": "A1", "values": [["A"]]})
    append_payload = agent.sheet_append({"spreadsheet": "sht_cli_1", "range": "A2", "values": [["2"]]})
    find_payload = agent.sheet_find({"spreadsheet": "sht_cli_1", "text": "2"})
    wiki_payload = agent.wiki_get_node({"token": "wiki_cli_1"})
    triage_payload = agent.mail_triage({"query": "CLI"})
    send_payload = agent.mail_send({"to": "operator@example.com", "subject": "CLI", "body": "hello"})
    reply_payload = agent.mail_reply({"message_id": "mail_cli_1", "body": "收到"})
    message_payload = agent.mail_message({"message_id": "mail_cli_1"})
    thread_payload = agent.mail_thread({"thread_id": "thd_cli_1"})
    whiteboard_payload = agent.whiteboard_update({"whiteboard": "https://feishu.cn/whiteboard/wb_cli_1", "dsl": "{\"nodes\":[]}"})

    assert create_sheet_payload["backend"] == "lark-cli"
    assert create_sheet_payload["spreadsheet_token"] == "sht_cli_1"
    assert info_payload["backend"] == "lark-cli"
    assert read_payload["backend"] == "lark-cli"
    assert write_payload["backend"] == "lark-cli"
    assert append_payload["backend"] == "lark-cli"
    assert find_payload["backend"] == "lark-cli"
    assert find_payload["matches"][0]["text"] == "2"
    assert wiki_payload["backend"] == "lark-cli"
    assert wiki_payload["node"]["token"] == "wiki_cli_1"
    assert triage_payload["backend"] == "lark-cli"
    assert send_payload["backend"] == "lark-cli"
    assert reply_payload["backend"] == "lark-cli"
    assert message_payload["backend"] == "lark-cli"
    assert thread_payload["backend"] == "lark-cli"
    assert whiteboard_payload["backend"] == "lark-cli"
    assert whiteboard_payload["whiteboard_token"] == "wb_cli_1"
    assert agent.calls == []


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

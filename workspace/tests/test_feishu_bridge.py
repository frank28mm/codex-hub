from __future__ import annotations

import json
import threading
import urllib.request
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
import pytest

from ops import feishu_bridge, runtime_state


def test_receive_chat_fails_closed_without_broker(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: False)
    result = feishu_bridge.route_chat_message(
        {
            "message_id": "msg-1",
            "project_name": "SampleProj",
            "text": "请总结当前状态",
        }
    )
    assert result["ok"] is False
    assert result["reason"] == "broker_unavailable"
    assert result["error_type"] == "broker_unavailable"
    assert result["delivery_record"]["status"] == "blocked_no_broker"

    summary = runtime_state.fetch_runtime_summary()
    assert summary["bridge_message_count"] >= 1
    assert summary["delivery_count"] >= 1


def test_receive_chat_routes_to_broker_and_records_delivery(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)
    captured: dict[str, object] = {}

    def fake_broker(command: str, args=None):
        captured["command"] = command
        captured["args"] = args or []
        return {"ok": True, "command": command, "args": args or [], "result": "accepted"}

    monkeypatch.setattr(
        feishu_bridge,
        "call_broker",
        fake_broker,
    )
    result = feishu_bridge.route_chat_message(
        {
            "message_id": "msg-2",
            "project_name": "SampleProj",
            "text": "/projects",
        }
    )
    assert result["ok"] is True
    assert result["command"] == "projects"
    assert captured["command"] == "projects"
    assert captured["args"] == ["--project-name", "SampleProj"]
    assert result["delivery_record"]["status"] == "accepted"
    assert result["inbound_record"]["project_name"] == "SampleProj"
    assert result["request"]["payload_schema"] == "bridge-cli"
    assert result["contract"]["allowed_write_tables"] == [
        "bridge_messages",
        "delivery_status",
        "bridge_execution_leases",
    ]
    assert result["contract"]["read_only_tables"] == ["review_items", "coordination_items"]


def test_receive_chat_ignores_empty_text(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)
    result = feishu_bridge.route_chat_message({"message_id": "msg-3", "project_name": "SampleProj"})
    assert result["ok"] is False
    assert result["reason"] == "empty_text"
    assert result["delivery_record"]["status"] == "ignored_empty_message"


def test_receive_chat_parses_feishu_webhook_content_json(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)
    captured: dict[str, object] = {}

    def fake_broker(command: str, args=None):
        captured["command"] = command
        captured["args"] = args or []
        return {"ok": True, "command": command, "args": args or [], "result": "accepted"}

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    result = feishu_bridge.route_chat_message(
        {
            "schema": "2.0",
            "header": {"event_type": "im.message.receive_v1", "event_id": "evt-1"},
            "event": {
                "message": {
                    "message_id": "msg-4",
                    "content": "{\"text\":\"请总结当前状态\"}",
                }
            },
        }
    )

    assert result["ok"] is True
    assert result["message_id"] == "msg-4"
    assert result["request"]["payload_schema"] == "feishu-webhook"
    assert result["request"]["event_type"] == "im.message.receive_v1"
    assert result["request"]["text"] == "请总结当前状态"
    assert captured["command"] == "codex-exec"
    assert captured["args"] == ["--prompt", "请总结当前状态"]


def test_receive_chat_supports_readonly_query_commands(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)
    captured: dict[str, object] = {}

    def fake_broker(command: str, args=None):
        captured["command"] = command
        captured["args"] = args or []
        return {"ok": True, "command": command, "args": args or [], "result": "accepted"}

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    result = feishu_bridge.route_chat_message({"message_id": "msg-5", "text": "/review SampleProj"})

    assert result["ok"] is True
    assert result["command"] == "review-inbox"
    assert result["project_name"] == "SampleProj"
    assert captured["command"] == "review-inbox"
    assert captured["args"] == ["--project-name", "SampleProj"]


def test_receive_chat_handles_url_verification_without_broker(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: False)
    result = feishu_bridge.route_chat_message({"type": "url_verification", "challenge": "challenge-token"})

    assert result["ok"] is True
    assert result["kind"] == "url_verification"
    assert result["challenge"] == "challenge-token"
    assert result["delivery_record"]["status"] == "challenge_only"


def test_receive_chat_rejects_unknown_slash_command(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)
    result = feishu_bridge.route_chat_message({"message_id": "msg-6", "text": "/unknown"})

    assert result["ok"] is False
    assert result["reason"] == "unsupported_command"
    assert result["error_type"] == "unsupported_command"
    assert result["delivery_record"]["status"] == "ignored_unsupported_command"


def test_contract_status_exposes_feishu_runtime_ownership(sample_env) -> None:
    contract = feishu_bridge.bridge_contract()

    assert contract["entry_mode"] == "compatibility_webhook_only"
    assert contract["default_entry_mode"] == "electron_long_connection"
    assert contract["host_mode"] == "electron"
    assert contract["truth_source"] == "obsidian_vault"
    assert contract["bitable_mode"] == "read_only_projection"
    assert contract["allowed_write_tables"] == [
        "bridge_messages",
        "delivery_status",
        "bridge_execution_leases",
    ]
    assert "approval_tokens" in contract["reserved_tables"]
    assert "direct_vault_writes" in contract["forbidden_capabilities"]


def test_workspace_root_falls_back_to_mainline_broker(monkeypatch, tmp_path: Path) -> None:
    configured = tmp_path / "configured-root"
    configured.mkdir()
    mainline = tmp_path / "mainline"
    (mainline / "ops").mkdir(parents=True)
    (mainline / "ops" / "local_broker.py").write_text("#!/usr/bin/env python3\n", encoding="utf-8")

    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(configured))
    monkeypatch.setattr(feishu_bridge, "MAINLINE_ROOT", mainline)

    assert feishu_bridge.workspace_root() == mainline


def test_webhook_event_fails_closed_when_verification_token_is_missing(sample_env, monkeypatch) -> None:
    monkeypatch.delenv("FEISHU_VERIFICATION_TOKEN", raising=False)
    result = feishu_bridge.route_webhook_event(
        {
            "schema": "2.0",
            "header": {"event_type": "im.message.receive_v1", "event_id": "evt-token-missing"},
            "event": {"message": {"message_id": "msg-token-missing", "content": "{\"text\":\"hello\"}"}},
            "token": "expected-token",
        }
    )

    assert result["ok"] is False
    assert result["reason"] == "verification_not_configured"
    assert result["delivery_record"]["status"] == "blocked_invalid_webhook"


def test_webhook_event_validates_token_and_builds_reply_preview(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "expected-token")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    def fake_broker(command: str, *, args=None):
        return {"ok": True, "broker_action": command, "result": "accepted", "args": args or []}

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    result = feishu_bridge.route_webhook_event(
        {
            "schema": "2.0",
            "token": "expected-token",
            "header": {"event_type": "im.message.receive_v1", "event_id": "evt-verified"},
            "event": {"message": {"message_id": "msg-verified", "content": "{\"text\":\"请总结当前状态\"}"}},
        }
    )

    assert result["ok"] is True
    assert result["verification"]["ok"] is True
    assert result["route_result"]["command"] == "codex-exec"
    assert result["reply_result"]["status"] == "preview_only"
    assert "Codex" in result["reply_text"]


def test_webhook_event_can_send_reply_with_credentials(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "expected-token")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    def fake_broker(command: str, *, args=None):
        return {
            "ok": True,
            "broker_action": command,
            "projects": [{"project_name": "SampleProj"}],
            "args": args or [],
        }

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    monkeypatch.setattr(
        feishu_bridge,
        "send_reply_message",
        lambda **kwargs: {"ok": True, "reason": "", "error_type": "", "reply_result": {"status": 200}, "kwargs": kwargs},
    )
    result = feishu_bridge.route_webhook_event(
        {
            "schema": "2.0",
            "token": "expected-token",
            "header": {"event_type": "im.message.receive_v1", "event_id": "evt-reply"},
            "event": {"message": {"message_id": "msg-reply", "content": "{\"text\":\"/projects SampleProj\"}"}},
        },
        send_reply=True,
    )

    assert result["ok"] is True
    assert result["reply_result"]["status"] == "sent"
    assert result["reply_result"]["delivery_record"]["status"] == "reply_sent"


def test_tenant_status_reports_readiness(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "token-1")
    monkeypatch.setenv("FEISHU_SIGNING_SECRET", "secret-1")
    monkeypatch.setenv("FEISHU_APP_ID", "cli_a1")
    monkeypatch.setenv("FEISHU_APP_SECRET", "secret-a1")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    result = feishu_bridge.tenant_status()

    assert result["ok"] is True
    assert result["webhook_ready"] is True
    assert result["reply_ready"] is True
    assert result["send_reply_ready"] is True
    assert result["verification_mode"] == "verification+signature"


def test_tenant_smoke_builds_locally_valid_webhook(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "token-1")
    monkeypatch.setenv("FEISHU_SIGNING_SECRET", "secret-1")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    def fake_broker(command: str, *, args=None):
        return {"ok": True, "broker_action": command, "projects": [{"project_name": "SampleProj"}], "args": args or []}

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    result = feishu_bridge.tenant_smoke(text="/projects SampleProj")

    assert result["ok"] is True
    assert result["tenant_status"]["webhook_ready"] is True
    assert result["payload_preview"]["event"]["message"]["content"] == "{\"text\": \"/projects SampleProj\"}"
    assert result["headers_preview"]["X-Lark-Signature"]
    assert result["route_result"]["reply_result"]["status"] == "preview_only"


def test_tenant_credentials_requires_reply_settings(sample_env) -> None:
    result = feishu_bridge.tenant_credentials_status()
    assert result["ok"] is False
    assert result["reason"] == "reply_not_ready"
    assert result["error_type"] == "reply_not_ready"


def test_tenant_credentials_fetches_token(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "token-1")
    monkeypatch.setenv("FEISHU_SIGNING_SECRET", "secret-1")
    monkeypatch.setenv("FEISHU_APP_ID", "cli_a1")
    monkeypatch.setenv("FEISHU_APP_SECRET", "secret-a1")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    def fake_token(app_id: str, app_secret: str) -> dict[str, Any]:
        assert app_id == "cli_a1"
        assert app_secret == "secret-a1"
        return {"ok": True, "payload": {"tenant_access_token": "tenant-token"}}

    monkeypatch.setattr(feishu_bridge, "fetch_tenant_access_token", fake_token)
    result = feishu_bridge.tenant_credentials_status()
    assert result["ok"] is True
    assert result["tenant_access_token"] == "tenant-token"


def test_http_json_uses_certifi_context_when_available(sample_env, monkeypatch) -> None:
    fake_context = object()
    fake_response = MagicMock()
    fake_response.__enter__.return_value = fake_response
    fake_response.__exit__.return_value = False
    fake_response.read.return_value = b'{"ok": true}'
    fake_response.status = 200
    captured: dict[str, Any] = {}

    monkeypatch.setattr(feishu_bridge, "_ssl_context", lambda: fake_context)

    def fake_urlopen(request, *, timeout, context):
        captured["timeout"] = timeout
        captured["context"] = context
        captured["url"] = request.full_url
        return fake_response

    monkeypatch.setattr(feishu_bridge.urllib.request, "urlopen", fake_urlopen)
    result = feishu_bridge._http_json(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        payload={"app_id": "cli_a1"},
        headers={"Content-Type": "application/json"},
    )

    assert result["ok"] is True
    assert captured["timeout"] == 15
    assert captured["context"] is fake_context


def test_local_webhook_server_handles_signed_preview(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("FEISHU_VERIFICATION_TOKEN", "token-1")
    monkeypatch.setenv("FEISHU_SIGNING_SECRET", "secret-1")
    monkeypatch.setattr(feishu_bridge, "broker_available", lambda: True)

    def fake_broker(command: str, *, args=None):
        return {"ok": True, "broker_action": command, "projects": [{"project_name": "SampleProj"}], "args": args or []}

    monkeypatch.setattr(feishu_bridge, "call_broker", fake_broker)
    try:
        server = feishu_bridge.make_webhook_server(host="127.0.0.1", port=0, send_reply=False)
    except PermissionError as exc:
        pytest.skip(f"local socket bind unavailable in this environment: {exc}")
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    payload = feishu_bridge.build_tenant_smoke_payload(text="/projects SampleProj")
    headers = feishu_bridge.build_webhook_headers(payload)
    request = urllib.request.Request(
        f"http://127.0.0.1:{server.server_address[1]}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={**headers, "Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = json.loads(response.read().decode("utf-8"))
    finally:
        thread.join(timeout=5)
        server.server_close()

    assert body["ok"] is True
    assert body["command"] == "projects"
    assert body["reply_status"] == "preview_only"
    assert "已获取项目视图" in body["reply_text"]

from __future__ import annotations

import argparse

from ops import runtime_state, weixin_bridge


def _sample_message(text: str, *, user_id: str = "wx-user", message_id: str = "msg-1", context_token: str = "ctx-1") -> dict:
    return {
        "message_id": message_id,
        "from_user_id": user_id,
        "context_token": context_token,
        "message_type": 1,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
    }


def test_weixin_contract_status_exposes_dm_runtime_ownership(sample_env) -> None:
    contract = weixin_bridge.bridge_contract()

    assert contract["bridge"] == "weixin"
    assert contract["entry_mode"] == "python_dm_long_poll"
    assert contract["chat_types"] == ["direct"]
    assert contract["truth_source"] == "obsidian_vault"
    assert "group_chat" in contract["forbidden_capabilities"]


def test_start_login_qr_persists_local_qr_image(sample_env, monkeypatch) -> None:
    class _FakeImage:
        def __init__(self) -> None:
            self.saved_path = ""

        def save(self, path) -> None:
            self.saved_path = str(path)
            with open(path, "wb") as handle:
                handle.write(b"png")

    fake_image = _FakeImage()

    monkeypatch.setattr(
        weixin_bridge,
        "_request_json",
        lambda **kwargs: {
            "qrcode": "qr-token",
            "qrcode_img_content": "https://liteapp.weixin.qq.com/q/demo",
        },
    )
    monkeypatch.setattr(weixin_bridge, "qrcode", type("_QR", (), {"make": staticmethod(lambda text: fake_image)}))

    payload = weixin_bridge.start_login_qr()

    assert payload["qrcode"] == "qr-token"
    assert payload["qrcode_url"] == "https://liteapp.weixin.qq.com/q/demo"
    assert payload["qrcode_image_path"].endswith("runtime/weixin/login_qr.png")
    assert fake_image.saved_path.endswith("runtime/weixin/login_qr.png")


def test_route_private_message_routes_to_broker_and_records_binding(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
            "user_id": "bot-user",
            "bot_id": "bot-id",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "project_name": "SampleProj",
                    "session_id": "sess-weixin-1",
                    "summary_excerpt": "已完成 SampleProj 当前状态整理。",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-1", "result": {"ok": True, "text": text}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("请继续 SampleProj 当前状态"))

    assert payload["ok"] is True
    assert payload["reply_text"] == "已完成 SampleProj 当前状态整理。"
    binding = runtime_state.fetch_bridge_chat_binding(bridge="weixin", chat_ref="weixin:default:wx-user")
    assert binding["binding_scope"] == "workspace"
    assert binding["project_name"] == ""
    assert binding["session_id"] == "sess-weixin-1"
    assert binding["metadata"]["last_project_name"] == "SampleProj"
    assert payload["normalized"]["thread_name"] == "CoCo 私聊"
    assert payload["normalized"]["thread_label"] == "CoCo 私聊"


def test_route_private_message_keeps_workspace_scope_for_unbound_prompt(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "project_name": "AI Coding课程",
                    "session_id": "sess-general-1",
                    "summary_excerpt": "我是 CoCo，这里是整个 Codex Hub 工作区的微信私聊入口。",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-2", "result": {"ok": True, "text": text}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("你是谁？", message_id="msg-general"))

    assert payload["ok"] is True
    binding = runtime_state.fetch_bridge_chat_binding(bridge="weixin", chat_ref="weixin:default:wx-user")
    assert binding["binding_scope"] == "workspace"
    assert binding["project_name"] == ""
    assert binding["session_id"] == "sess-general-1"
    assert binding["metadata"].get("last_project_name", "") == ""


def test_route_private_message_only_resumes_when_user_explicitly_continues(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    runtime_state.upsert_bridge_chat_binding(
        bridge="weixin",
        chat_ref="weixin:default:wx-user",
        binding_scope="workspace",
        project_name="",
        session_id="sess-existing",
        metadata={"thread_name": "CoCo 私聊", "thread_label": "CoCo 私聊"},
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {"ok": True, "finalize_launch": {"session_id": "sess-existing", "summary_excerpt": "好的。"}},
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-3", "result": {"ok": True, "text": text}},
    )

    weixin_bridge.route_private_message(_sample_message("你是谁？", message_id="msg-no-resume"))
    assert observed["args"][:3] == ["command-center", "--action", "codex-exec"]

    weixin_bridge.route_private_message(_sample_message("继续刚才的话题", message_id="msg-resume"))
    assert observed["args"][:3] == ["command-center", "--action", "codex-resume"]
    assert "--session-id" in observed["args"]


def test_route_private_message_blocks_high_risk_actions(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-risk", "result": {"ok": True, "text": text}},
    )
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: (_ for _ in ()).throw(AssertionError("high risk message should not hit broker")),
    )

    payload = weixin_bridge.route_private_message(_sample_message("请帮我 git push 到 github"))

    assert payload["ok"] is False
    assert payload["reason"] == "high_risk_not_supported"
    assert "Feishu 私聊 CoCo" in payload["reply_text"]


def test_run_once_polls_updates_and_persists_cursor(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
            "get_updates_buf": "cursor-1",
        }
    )

    def fake_api_post(*, base_url, endpoint, payload, token="", timeout_ms=0, extra_headers=None):
        assert endpoint == "ilink/bot/getupdates"
        assert payload["get_updates_buf"] == "cursor-1"
        return {
            "get_updates_buf": "cursor-2",
            "msgs": [_sample_message("请看一下 SampleProj", message_id="msg-run-once")],
        }

    monkeypatch.setattr(weixin_bridge, "_api_post", fake_api_post)
    monkeypatch.setattr(
        weixin_bridge,
        "route_private_message",
        lambda message, *, send_reply=True: {"ok": True, "message_id": message["message_id"], "send_reply": send_reply},
    )

    payload = weixin_bridge.run_once(send_reply=False)

    assert payload["ok"] is True
    assert payload["message_count"] == 1
    assert weixin_bridge.load_account()["get_updates_buf"] == "cursor-2"


def test_launch_agent_payload_uses_daemon_entry(sample_env) -> None:
    payload = weixin_bridge.launch_agent_payload(poll_interval=5, error_backoff=12)

    assert payload["Label"] == weixin_bridge.LAUNCH_AGENT_NAME
    assert payload["RunAtLoad"] is True
    assert payload["KeepAlive"] is True
    assert "daemon" in payload["ProgramArguments"]
    assert "--poll-interval" in payload["ProgramArguments"]
    assert "--error-backoff" in payload["ProgramArguments"]
    assert payload["StandardOutPath"].endswith("logs/weixin-bridge.log")
    assert payload["StandardErrorPath"].endswith("logs/weixin-bridge.err.log")


def test_safe_run_once_persists_error_state(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(weixin_bridge, "run_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bridge boom")))

    payload = weixin_bridge.safe_run_once(mode="daemon")

    assert payload["ok"] is False
    assert payload["error"] == "bridge boom"
    status = weixin_bridge.bridge_status()
    assert status["loop_state"]["last_error"] == "bridge boom"
    assert status["loop_state"]["consecutive_failures"] == 1
    assert status["connection"]["status"] == "error"


def test_weixin_parser_exposes_phase2_commands() -> None:
    parser = weixin_bridge.build_parser()
    action = next(item for item in parser._actions if isinstance(item, argparse._SubParsersAction))

    assert "daemon" in action.choices
    assert "install-launchagent" in action.choices
    assert "uninstall-launchagent" in action.choices

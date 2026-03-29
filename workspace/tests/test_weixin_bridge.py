from __future__ import annotations

import argparse
import sqlite3
import subprocess
import threading
import time
from pathlib import Path

from ops import runtime_state, weixin_bridge


def _sample_message(text: str, *, user_id: str = "wx-user", message_id: str = "msg-1", context_token: str = "ctx-1") -> dict:
    return {
        "message_id": message_id,
        "from_user_id": user_id,
        "context_token": context_token,
        "message_type": 1,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
    }


def _sample_image_message(*, user_id: str = "wx-user", message_id: str = "msg-image", context_token: str = "ctx-image") -> dict:
    return {
        "message_id": message_id,
        "from_user_id": user_id,
        "context_token": context_token,
        "message_type": 1,
        "item_list": [{"type": 2, "image_item": {"media": {"encrypt_query_param": "enc"}}}],
    }


def _sample_voice_message(
    *,
    user_id: str = "wx-user",
    message_id: str = "msg-voice",
    context_token: str = "ctx-voice",
    transcript: str = "这是一段语音转写",
    aes_key: str = "",
) -> dict:
    return {
        "message_id": message_id,
        "from_user_id": user_id,
        "context_token": context_token,
        "message_type": 1,
        "item_list": [
            {
                "type": 3,
                "voice_item": {
                    "text": transcript,
                    "media": {"encrypt_query_param": "voice-enc", "aes_key": aes_key},
                },
            }
        ],
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
    assert binding["metadata"]["session_lane"] == "weixin:default:wx-user"
    assert binding["metadata"]["session_launch_source"] == "weixin"
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
                    "project_name": "SampleProj",
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


def test_route_private_message_prefers_full_reply_text_and_splits_long_replies(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    long_reply = "第一段。" * 180 + "\n\n" + "第二段。" * 180
    sent_texts: list[str] = []

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
                    "session_id": "sess-long-1",
                    "reply_text": long_reply,
                    "summary_excerpt": "这段短摘要不该盖过完整回复。",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": f"out-{len(sent_texts)}", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("请给我完整答复", message_id="msg-long"))

    assert payload["ok"] is True
    assert payload["reply_text"] == long_reply
    assert len(payload["deliveries"]) >= 2
    assert sent_texts[0].startswith("（1/")
    assert "第一段。" in "".join(sent_texts)
    assert "第二段。" in "".join(sent_texts)


def test_route_private_message_strips_markdown_for_weixin_delivery(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    sent_texts: list[str] = []
    reply_text = """# 标题

**重点** 请看这个[链接](https://example.com)。

```python
print("hello")
```
"""

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
                    "session_id": "sess-md-1",
                    "reply_text": reply_text,
                    "summary_excerpt": "短摘要",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": f"out-{len(sent_texts)}", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("请按微信格式回复", message_id="msg-md"))

    assert payload["ok"] is True
    assert sent_texts
    combined = "\n".join(sent_texts)
    assert "标题" in combined
    assert "重点 请看这个链接。" in combined
    assert "print(\"hello\")" in combined
    assert "[" not in combined
    assert "](https://example.com)" not in combined
    assert "**" not in combined
    assert "```" not in combined


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
    monkeypatch.setattr(weixin_bridge, "detect_project_name", lambda text: "SampleProj")
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
    assert "--no-auto-resume" in observed["args"]

    weixin_bridge.route_private_message(_sample_message("继续刚才的话题", message_id="msg-resume"))
    assert observed["args"][:3] == ["command-center", "--action", "codex-resume"]
    assert "--session-id" in observed["args"]
    assert "--no-auto-resume" not in observed["args"]


def test_route_private_message_does_not_resume_without_matching_session_lane(sample_env, monkeypatch) -> None:
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
        metadata={
            "thread_name": "CoCo 私聊",
            "thread_label": "CoCo 私聊",
            "session_lane": "weixin:default:other-user",
        },
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "session_id": "sess-weixin-new",
                    "summary_excerpt": "好的。",
                }
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-lane", "result": {"ok": True, "text": text}},
    )

    weixin_bridge.route_private_message(_sample_message("继续刚才的话题", message_id="msg-resume"))

    assert observed["args"][:3] == ["command-center", "--action", "codex-exec"]
    assert "--no-auto-resume" in observed["args"]
    assert "--session-id" not in observed["args"]


def test_route_private_message_backfills_legacy_session_lane_and_resumes(sample_env, monkeypatch) -> None:
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
        session_id="sess-legacy",
        metadata={"thread_name": "CoCo 私聊", "thread_label": "CoCo 私聊"},
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "session_id": "sess-legacy",
                    "summary_excerpt": "好的。",
                }
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-legacy", "result": {"ok": True, "text": text}},
    )

    weixin_bridge.route_private_message(_sample_message("继续刚才的话题", message_id="msg-legacy-resume"))

    assert observed["args"][:3] == ["command-center", "--action", "codex-resume"]
    assert "--session-id" in observed["args"]
    binding = runtime_state.fetch_bridge_chat_binding(bridge="weixin", chat_ref="weixin:default:wx-user")
    assert binding["metadata"]["session_lane"] == "weixin:default:wx-user"


def test_route_private_message_keeps_explicit_project_exec_isolated_from_hot_resume(sample_env, monkeypatch) -> None:
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
        metadata={"thread_name": "CoCo 私聊", "thread_label": "CoCo 私聊", "last_project_name": "SampleProj"},
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(weixin_bridge, "detect_project_name", lambda text: "SampleProj")
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "project_name": "SampleProj",
                    "session_id": "sess-weixin-new",
                    "summary_excerpt": "好的。",
                }
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-project", "result": {"ok": True, "text": text}},
    )

    weixin_bridge.route_private_message(_sample_message("SampleProj 这个项目我想再问一个问题", message_id="msg-project"))

    assert observed["args"][:3] == ["command-center", "--action", "codex-exec"]
    assert "--project-name" in observed["args"]
    assert "--no-auto-resume" in observed["args"]
    assert "--session-id" not in observed["args"]


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


def test_prepare_inbound_message_defers_image_download_until_worker(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(
        weixin_bridge,
        "_download_attachment_for_message",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("prepare should not download attachments")),
    )

    normalized, inbound_record = weixin_bridge._prepare_inbound_message(
        _sample_image_message(message_id="msg-image-download"),
        {"account_id": "default", "base_url": "https://weixin.example"},
    )

    assert normalized["attachment_type"] == "image"
    assert normalized["attachment_path"] == ""
    stored = runtime_state.fetch_bridge_message_detail(
        bridge="weixin",
        direction="inbound",
        message_id="msg-image-download",
    )
    assert stored["payload"]["attachment_path"] == ""
    assert inbound_record["status"] == "received"


def test_route_private_message_processes_image_attachment(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_download_cdn_bytes", lambda *args, **kwargs: b"\x89PNG\r\n\x1a\nfake-png")
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}
    sent_texts: list[str] = []

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "project_name": "",
                    "session_id": "sess-image-1",
                    "summary_excerpt": "已收到并分析图片。",
                },
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": "out-image", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(_sample_image_message())

    assert payload["ok"] is True
    assert observed["args"][:3] == ["command-center", "--action", "codex-exec"]
    assert "--attachment-path" in observed["args"]
    assert "--attachment-type" in observed["args"]
    assert observed["args"][observed["args"].index("--attachment-type") + 1] == "image"
    assert "用户通过微信发送了一张图片附件" in observed["args"][observed["args"].index("--prompt") + 1]
    assert sent_texts
    assert "已收到并分析图片" in sent_texts[0]


def test_route_private_message_forwards_voice_attachment_context(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(
        weixin_bridge,
        "_download_attachment_for_message",
        lambda message, normalized: {
            "attachment_path": "/tmp/weixin-voice.silk",
            "attachment_media_type": "voice",
            "attachment_download_error": "",
        },
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "session_id": "sess-voice-1",
                    "summary_excerpt": "已按语音内容处理。",
                },
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-voice", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(
        _sample_voice_message(aes_key="MDEyMzQ1Njc4OWFiY2RlZg=="),
    )

    assert payload["ok"] is True
    assert "--attachment-path" in observed["args"]
    assert observed["args"][observed["args"].index("--attachment-path") + 1] == "/tmp/weixin-voice.silk"
    assert "--attachment-type" in observed["args"]
    assert observed["args"][observed["args"].index("--attachment-type") + 1] == "voice"
    assert "--voice-transcript" in observed["args"]
    assert observed["args"][observed["args"].index("--voice-transcript") + 1] == "这是一段语音转写"
    assert observed["args"][observed["args"].index("--prompt") + 1] == "这是一段语音转写"


def test_run_queue_once_transcribes_voice_when_provider_transcript_missing(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    event = weixin_bridge.enqueue_private_message(
        _sample_voice_message(message_id="msg-voice-worker", transcript="", aes_key="MDEyMzQ1Njc4OWFiY2RlZg=="),
    )
    monkeypatch.setattr(
        weixin_bridge,
        "_download_attachment_for_message",
        lambda message, normalized: {
            "attachment_path": "/tmp/weixin-voice-worker.silk",
            "attachment_media_type": "voice",
            "attachment_download_error": "",
        },
    )
    monkeypatch.setattr(weixin_bridge, "_transcode_voice_attachment_to_wav", lambda path: ("/tmp/weixin-voice-worker.wav", ""))
    monkeypatch.setattr(weixin_bridge, "_transcribe_voice_attachment", lambda path: ("这是补出来的语音转写", ""))
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "session_id": "sess-voice-worker",
                    "summary_excerpt": "已按语音内容处理。",
                },
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-voice-worker", "result": {"ok": True}},
    )

    payload = weixin_bridge.run_queue_once(limit=5)

    assert payload["ok"] is True
    stored = runtime_state.fetch_runtime_event(event["event_key"])
    assert stored["status"] == "completed"
    assert observed["args"][observed["args"].index("--attachment-path") + 1] == "/tmp/weixin-voice-worker.wav"
    assert observed["args"][observed["args"].index("--voice-transcript") + 1] == "这是补出来的语音转写"
    assert observed["args"][observed["args"].index("--prompt") + 1] == "这是补出来的语音转写"


def test_route_private_message_replies_when_voice_transcription_is_unavailable(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(
        weixin_bridge,
        "_download_attachment_for_message",
        lambda message, normalized: {
            "attachment_path": "/tmp/weixin-voice-no-text.silk",
            "attachment_media_type": "voice",
            "attachment_download_error": "",
        },
    )
    monkeypatch.setattr(weixin_bridge, "_transcode_voice_attachment_to_wav", lambda path: ("", "openai_api_key_missing"))
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    sent_texts: list[str] = []
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: (_ for _ in ()).throw(AssertionError("voice without transcript should not hit broker")),
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": "out-voice-no-text", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(
        _sample_voice_message(message_id="msg-voice-no-text", transcript="", aes_key="MDEyMzQ1Njc4OWFiY2RlZg=="),
    )

    assert payload["ok"] is False
    assert payload["reason"] == "unsupported_input"
    assert sent_texts
    assert "语音转写能力" in sent_texts[0]


def test_normalize_inbound_preserves_media_envelope(sample_env) -> None:
    account = {"account_id": "default"}

    image_payload = weixin_bridge._normalize_inbound(_sample_image_message(), account)
    assert image_payload["attachment_type"] == "image"
    assert image_payload["attachment_ref"] == "enc"

    voice_payload = weixin_bridge._normalize_inbound(_sample_voice_message(), account)
    assert voice_payload["attachment_type"] == "voice"
    assert voice_payload["attachment_ref"] == "voice-enc"
    assert voice_payload["voice_transcript"] == "这是一段语音转写"


def test_broker_command_returns_timeout_payload(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(weixin_bridge, "BROKER_TIMEOUT_MS", 60_000)

    def fake_run(*args, **kwargs):
        assert kwargs["timeout"] == 60
        raise subprocess.TimeoutExpired(cmd=kwargs.get("args", args[0] if args else []), timeout=60)

    monkeypatch.setattr(weixin_bridge.subprocess, "run", fake_run)

    payload = weixin_bridge._broker_command(["command-center", "--action", "codex-exec"])

    assert payload["ok"] is False
    assert payload["reason"] == "session_timed_out"
    assert payload["response"]["timeout_ms"] == weixin_bridge.BROKER_TIMEOUT_MS


def test_broker_command_omits_timeout_by_default(sample_env, monkeypatch) -> None:
    observed: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        observed["kwargs"] = dict(kwargs)
        return subprocess.CompletedProcess(args=args[0] if args else kwargs.get("args", []), returncode=0, stdout='{"ok": true}', stderr="")

    monkeypatch.setattr(weixin_bridge.subprocess, "run", fake_run)

    payload = weixin_bridge._broker_command(["command-center", "--action", "codex-exec"])

    assert payload["ok"] is True
    assert "timeout" not in observed["kwargs"]


def test_route_private_message_replies_when_broker_times_out(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "BROKER_TIMEOUT_MS", 60_000)
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    sent_texts: list[str] = []
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: {
            "ok": False,
            "reason": "session_timed_out",
            "response": {"ok": False, "reason": "session_timed_out", "timeout_ms": weixin_bridge.BROKER_TIMEOUT_MS},
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": "out-timeout", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("帮我看看当前总板和各个项目板都挂着什么样的任务？", message_id="msg-timeout"))

    assert payload["ok"] is False
    assert payload["reason"] == "session_timed_out"
    assert sent_texts
    assert "长时间未返回" in sent_texts[0]
    assert "60 秒" in sent_texts[0]


def test_route_private_message_does_not_echo_raw_broker_json_on_error(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    sent_texts: list[str] = []
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: {
            "ok": False,
            "stdout": '{"ok":false,"broker_action":"command-center","delegated_broker_action":"codex-exec","result_status":"error"}',
            "stderr": "",
            "response": {
                "ok": False,
                "broker_action": "command-center",
                "delegated_broker_action": "codex-exec",
                "result_status": "error",
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: sent_texts.append(text) or {"message_id": "out-broker-error", "result": {"ok": True}},
    )

    payload = weixin_bridge.route_private_message(_sample_message("继续处理这个任务", message_id="msg-broker-error"))

    assert payload["ok"] is False
    assert sent_texts
    assert "执行链路异常中断" in sent_texts[0]
    assert "codex-exec" in sent_texts[0]
    assert "{\"ok\":false" not in sent_texts[0]


def test_route_private_message_persists_outbound_error_when_delivery_fails(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    monkeypatch.setattr(weixin_bridge, "BROKER_TIMEOUT_MS", 60_000)
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        weixin_bridge,
        "_broker_command",
        lambda args: {
            "ok": False,
            "reason": "session_timed_out",
            "response": {"ok": False, "reason": "session_timed_out", "timeout_ms": weixin_bridge.BROKER_TIMEOUT_MS},
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("weixin send failed")),
    )

    payload = weixin_bridge.route_private_message(_sample_message("继续", message_id="msg-delivery-fail"))

    assert payload["ok"] is False
    record = runtime_state.fetch_bridge_message_detail(
        bridge="weixin",
        direction="outbound",
        message_id="out-error-msg-delivery-fail",
    )
    assert record["status"] == "error"
    assert "delivery_error" in record["payload"]


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
    queued: list[tuple[dict, bool]] = []
    monkeypatch.setattr(
        weixin_bridge,
        "enqueue_private_message",
        lambda message, *, send_reply=True: queued.append((message, send_reply))
        or {"ok": True, "message_id": message["message_id"], "send_reply": send_reply, "queued": True},
    )

    payload = weixin_bridge.run_once(send_reply=False)

    assert payload["ok"] is True
    assert payload["message_count"] == 1
    assert payload["enqueued_count"] == 1
    assert queued[0][0]["message_id"] == "msg-run-once"
    assert queued[0][1] is False
    assert weixin_bridge.load_account()["get_updates_buf"] == "cursor-2"


def test_run_queue_once_downloads_image_attachment_in_worker(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    event = weixin_bridge.enqueue_private_message(_sample_image_message(message_id="msg-queue-image"))
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(weixin_bridge, "_download_cdn_bytes", lambda *args, **kwargs: b"\x89PNG\r\n\x1a\nfake-png")
    observed: dict[str, list[str]] = {}

    def fake_broker(args):
        observed["args"] = list(args)
        return {
            "ok": True,
            "command": ["python3", "/broker.py", *args],
            "response": {
                "ok": True,
                "finalize_launch": {
                    "session_id": "sess-queue-image",
                    "summary_excerpt": "已完成图片处理。",
                },
            },
        }

    monkeypatch.setattr(weixin_bridge, "_broker_command", fake_broker)
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-queue-image", "result": {"ok": True}},
    )

    payload = weixin_bridge.run_queue_once(limit=5)

    assert payload["ok"] is True
    stored = runtime_state.fetch_runtime_event(event["event_key"])
    assert stored["status"] == "completed"
    assert "--attachment-path" in observed["args"]
    attachment_path = observed["args"][observed["args"].index("--attachment-path") + 1]
    assert attachment_path.endswith(".png")
    assert Path(attachment_path).exists()


def test_run_queue_once_processes_enqueued_message_and_marks_completed(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    event = weixin_bridge.enqueue_private_message(_sample_message("继续 SampleProj 当前状态", message_id="msg-queued"))
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
                    "session_id": "sess-queued-1",
                    "summary_excerpt": "已完成队列处理。",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda account, *, user_id, context_token, text: {"message_id": "out-msg-queued", "result": {"ok": True, "text": text}},
    )

    payload = weixin_bridge.run_queue_once(limit=5)

    assert payload["ok"] is True
    assert payload["claimed_count"] == 1
    assert payload["processed_count"] == 1
    stored = runtime_state.fetch_runtime_event(event["event_key"])
    assert stored["status"] == "completed"
    assert stored["result"]["ok"] is True


def test_run_queue_once_retries_when_delivery_fails(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    event = weixin_bridge.enqueue_private_message(_sample_message("继续", message_id="msg-queue-fail"))
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
                    "session_id": "sess-queue-fail",
                    "summary_excerpt": "好的。",
                },
            },
        },
    )
    monkeypatch.setattr(
        weixin_bridge,
        "send_text_message",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("weixin send failed")),
    )

    payload = weixin_bridge.run_queue_once(limit=5)

    assert payload["ok"] is False
    assert payload["failed_count"] == 1
    stored = runtime_state.fetch_runtime_event(event["event_key"])
    assert stored["status"] == "pending"
    assert "delivery_failed" in stored["last_error"]


def test_run_queue_once_renews_event_lease_during_long_route(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    event = weixin_bridge.enqueue_private_message(_sample_message("继续", message_id="msg-lease-renew"))
    monkeypatch.setattr(weixin_bridge, "_send_typing", lambda *args, **kwargs: None)
    monkeypatch.setattr(weixin_bridge, "WEIXIN_QUEUE_LEASE_SECONDS", 30)
    monkeypatch.setattr(weixin_bridge, "WEIXIN_QUEUE_LEASE_RENEW_INTERVAL_SECONDS", 0.01)
    renew_calls: list[str] = []
    original_renew = runtime_state.renew_runtime_event_lease

    def wrapped_renew(event_key, *, claim_token="", lease_seconds=300):
        renew_calls.append(event_key)
        return original_renew(event_key, claim_token=claim_token, lease_seconds=lease_seconds)

    monkeypatch.setattr(runtime_state, "renew_runtime_event_lease", wrapped_renew)

    def slow_route(*args, **kwargs):
        time.sleep(0.05)
        return {
            "ok": True,
            "reason": "",
            "reply_text": "好的。",
            "delivery": {},
            "deliveries": [],
        }

    monkeypatch.setattr(weixin_bridge, "_route_normalized_message", slow_route)

    payload = weixin_bridge.run_queue_once(limit=5)

    assert payload["ok"] is True
    assert renew_calls
    stored = runtime_state.fetch_runtime_event(event["event_key"])
    assert stored["status"] == "completed"


def test_poll_and_worker_loops_run_independently(sample_env, monkeypatch) -> None:
    stop_event = threading.Event()
    poll_timestamps: list[float] = []

    def fake_safe_run_once(*, mode="manual", send_reply=True):
        poll_timestamps.append(time.monotonic())
        if len(poll_timestamps) >= 2:
            stop_event.set()
        return {"ok": True, "message_count": 0}

    def fake_run_queue_once(*, limit=weixin_bridge.DEFAULT_WORKER_LIMIT):
        time.sleep(0.05)
        return {"ok": True, "claimed_count": 1, "processed_count": 1, "failed_count": 0}

    monkeypatch.setattr(weixin_bridge, "safe_run_once", fake_safe_run_once)
    monkeypatch.setattr(weixin_bridge, "run_queue_once", fake_run_queue_once)

    poll_thread = threading.Thread(
        target=weixin_bridge._run_poll_daemon_loop,
        kwargs={"stop_event": stop_event, "poll_interval": 1, "error_backoff": 1, "verbose": False},
    )
    worker_thread = threading.Thread(
        target=weixin_bridge._run_worker_daemon_loop,
        kwargs={
            "stop_event": stop_event,
            "worker_limit": 1,
            "idle_interval": 1,
            "error_backoff": 1,
            "verbose": False,
        },
    )
    poll_thread.start()
    worker_thread.start()
    poll_thread.join(timeout=1)
    worker_thread.join(timeout=1)

    assert len(poll_timestamps) >= 2


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


def test_smoke_defaults_to_preview_only(sample_env, monkeypatch) -> None:
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
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("preview smoke should not send messages")),
    )

    payload = weixin_bridge.smoke(text="测试消息")

    assert payload["ok"] is True
    assert payload["delivery_mode"] == "preview_only"
    assert payload["sample_payload"]["text"] == "测试消息"
    assert payload["sample_payload"]["user_id"] == "<wechat-user-id>"


def test_smoke_live_send_requires_explicit_user_id(sample_env) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )

    payload = weixin_bridge.smoke(text="测试消息", dry_run=False)

    assert payload["ok"] is False
    assert payload["error"] == "user_id_required_for_live_smoke"
    assert payload["delivery_mode"] == "live_send"


def test_smoke_live_send_delivers_message(sample_env, monkeypatch) -> None:
    weixin_bridge.save_account(
        {
            "account_id": "default",
            "token": "wx-token",
            "base_url": "https://weixin.example",
        }
    )
    observed: dict[str, str] = {}

    def fake_send(account, *, user_id, context_token, text):
        observed["user_id"] = user_id
        observed["context_token"] = context_token
        observed["text"] = text
        return {"message_id": "out-smoke", "result": {"ok": True}}

    monkeypatch.setattr(weixin_bridge, "send_text_message", fake_send)

    payload = weixin_bridge.smoke(text="测试消息", user_id="wx-user", dry_run=False)

    assert payload["ok"] is True
    assert payload["delivery_mode"] == "live_send"
    assert observed["user_id"] == "wx-user"
    assert observed["context_token"] == "smoke-context-token"
    assert observed["text"] == "测试消息"
    assert payload["reply"]["message_id"] == "out-smoke"


def test_safe_run_once_persists_error_state(sample_env, monkeypatch) -> None:
    monkeypatch.setattr(weixin_bridge, "run_once", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("bridge boom")))

    payload = weixin_bridge.safe_run_once(mode="daemon")

    assert payload["ok"] is False
    assert payload["error"] == "bridge boom"
    status = weixin_bridge.bridge_status()
    assert status["loop_state"]["last_error"] == "bridge boom"
    assert status["loop_state"]["consecutive_failures"] == 1
    assert status["connection"]["status"] == "error"


def test_runtime_state_connect_context_closes_connection(sample_env) -> None:
    with runtime_state.connect() as conn:
        conn.execute("SELECT 1")
    try:
        conn.execute("SELECT 1")
    except sqlite3.ProgrammingError:
        pass
    else:  # pragma: no cover - explicit regression guard
        raise AssertionError("runtime_state.connect() context should close sqlite connection")


def test_safe_run_once_survives_bridge_state_save_failure(sample_env, monkeypatch) -> None:
    save_calls = {"count": 0}

    def flaky_save(_updates):
        save_calls["count"] += 1
        if save_calls["count"] == 1:
            raise OSError("too many open files")
        return {}

    monkeypatch.setattr(weixin_bridge, "_save_bridge_state", flaky_save)
    monkeypatch.setattr(weixin_bridge, "_bridge_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(weixin_bridge, "run_once", lambda **kwargs: {"ok": True, "message_count": 0})

    payload = weixin_bridge.safe_run_once(mode="daemon")

    assert payload["ok"] is True
    assert save_calls["count"] >= 2


def test_worker_loop_survives_run_queue_exception(sample_env, monkeypatch) -> None:
    stop_event = threading.Event()
    calls = {"count": 0}

    def flaky_run_queue_once(*, limit=weixin_bridge.DEFAULT_WORKER_LIMIT):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("worker boom")
        stop_event.set()
        return {"ok": True, "claimed_count": 0, "processed_count": 0, "failed_count": 0}

    monkeypatch.setattr(weixin_bridge, "run_queue_once", flaky_run_queue_once)
    monkeypatch.setattr(weixin_bridge, "_bridge_log", lambda *args, **kwargs: None)

    thread = threading.Thread(
        target=weixin_bridge._run_worker_daemon_loop,
        kwargs={
            "stop_event": stop_event,
            "worker_limit": 1,
            "idle_interval": 1,
            "error_backoff": 0,
            "verbose": False,
        },
    )
    thread.start()
    thread.join(timeout=2)

    assert calls["count"] >= 2
    assert thread.is_alive() is False


def test_weixin_parser_exposes_phase2_commands() -> None:
    parser = weixin_bridge.build_parser()
    action = next(item for item in parser._actions if isinstance(item, argparse._SubParsersAction))

    assert "daemon" in action.choices
    assert "install-launchagent" in action.choices
    assert "uninstall-launchagent" in action.choices


def test_weixin_parser_keeps_smoke_preview_by_default() -> None:
    parser = weixin_bridge.build_parser()
    args = parser.parse_args(["smoke"])

    assert args.live_send is False

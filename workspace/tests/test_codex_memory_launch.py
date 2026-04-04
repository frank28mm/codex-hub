from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys

from ops import codex_memory


def test_finalize_launch_exposes_full_reply_text_while_keeps_short_excerpt(sample_env, monkeypatch) -> None:
    printed: list[str] = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: printed.append(" ".join(str(arg) for arg in args)))

    register_args = argparse.Namespace(
        project_name="SampleProj",
        prompt="请给我一段较长回复",
        mode="new",
        resume_session_id="",
        binding_scope="project",
        binding_board_path="",
        topic_name="",
        rollup_target="",
        launch_source="weixin",
        source_chat_ref="weixin:test",
        source_thread_name="CoCo 私聊",
        source_thread_label="CoCo 私聊",
        source_message_id="msg-long",
    )
    assert codex_memory.cmd_register_launch(register_args) == 0
    launch_id = printed.pop().strip()

    long_reply = ("第一段完整回复。" * 50) + "\n\n" + ("第二段完整回复。" * 50)
    summary_file = sample_env["workspace_root"] / "long-reply.txt"
    summary_file.write_text(long_reply, encoding="utf-8")

    finalize_args = argparse.Namespace(
        launch_id=launch_id,
        session_id="sess-long",
        thread_name="CoCo 私聊",
        summary_file=str(summary_file),
        final_status="aborted",
    )
    assert codex_memory.cmd_finalize_launch(finalize_args) == 0
    finalize_payload = json.loads(printed.pop())

    assert finalize_payload["reply_text"] == long_reply
    assert finalize_payload["summary_excerpt"] == long_reply[:400]
    assert len(finalize_payload["summary_excerpt"]) == 400


def test_workspace_lock_is_reentrant(sample_env) -> None:
    module = importlib.reload(codex_memory)
    entered: list[str] = []

    with module.workspace_lock():
        entered.append("outer")
        with module.workspace_lock():
            entered.append("inner")

    assert entered == ["outer", "inner"]


def test_invalid_sync_timeout_env_falls_back_to_default(sample_env, monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_SYNC_TRIGGER_TIMEOUT_SECONDS", "oops")

    module = importlib.reload(codex_memory)

    assert module.SYNC_TRIGGER_TIMEOUT_SECONDS == 15


def test_trigger_dashboard_sync_once_returns_timeout_result(sample_env, monkeypatch) -> None:
    module = importlib.reload(codex_memory)
    sync_script = sample_env["workspace_root"] / "ops" / "codex_dashboard_sync.py"
    sync_script.parent.mkdir(parents=True, exist_ok=True)
    sync_script.write_text("print('stub')\n", encoding="utf-8")

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout", 0))

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    result = module.trigger_dashboard_sync_once()

    assert result is not None
    assert result.returncode == 124
    assert "timed out" in (result.stderr or "")


def test_trigger_dashboard_sync_once_can_spawn_async(sample_env, monkeypatch) -> None:
    module = importlib.reload(codex_memory)
    sync_script = sample_env["workspace_root"] / "ops" / "codex_dashboard_sync.py"
    sync_script.parent.mkdir(parents=True, exist_ok=True)
    sync_script.write_text("print('stub')\n", encoding="utf-8")

    captured: dict[str, object] = {}

    class DummyProcess:
        pid = 43210

    def fake_popen(command, **kwargs):
        captured["command"] = list(command)
        captured["kwargs"] = kwargs
        return DummyProcess()

    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)

    result = module.trigger_dashboard_sync_once(wait=False)

    assert result == {"ok": True, "label": "dashboard sync", "pid": 43210, "mode": "async"}
    assert captured["command"][0] == sys.executable


def test_finalize_launch_triggers_dashboard_sync_async(sample_env, monkeypatch) -> None:
    module = importlib.reload(codex_memory)
    printed: list[str] = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: printed.append(" ".join(str(arg) for arg in args)))

    register_args = argparse.Namespace(
        project_name="SampleProj",
        prompt="请帮我收口这轮会话",
        mode="new",
        resume_session_id="",
        binding_scope="project",
        binding_board_path="",
        topic_name="",
        rollup_target="",
        launch_source="feishu",
        source_chat_ref="feishu:test",
        source_thread_name="CoCo 私聊",
        source_thread_label="CoCo 私聊",
        source_message_id="msg-async-dashboard",
    )
    assert module.cmd_register_launch(register_args) == 0
    launch_id = printed.pop().strip()

    summary_file = sample_env["workspace_root"] / "reply.txt"
    summary_file.write_text("已经完成。", encoding="utf-8")

    waits: list[bool] = []
    monkeypatch.setattr(module, "trigger_retrieval_sync_once", lambda: subprocess.CompletedProcess(args=["retrieval"], returncode=0))
    monkeypatch.setattr(
        module,
        "trigger_dashboard_sync_once",
        lambda *, wait=True: waits.append(wait) or {"ok": True, "mode": "async"},
    )

    finalize_args = argparse.Namespace(
        launch_id=launch_id,
        session_id="sess-async",
        thread_name="CoCo 私聊",
        summary_file=str(summary_file),
        final_status="completed",
    )

    assert module.cmd_finalize_launch(finalize_args) == 0
    assert waits == [False]


def test_trigger_retrieval_sync_once_requeues_claimed_events_after_timeout(sample_env, monkeypatch) -> None:
    module = importlib.reload(codex_memory)
    from ops import runtime_state
    retrieval_script = sample_env["workspace_root"] / "ops" / "codex_retrieval.py"
    retrieval_script.parent.mkdir(parents=True, exist_ok=True)
    retrieval_script.write_text("print('stub')\n", encoding="utf-8")

    runtime_state.enqueue_runtime_event(
        queue_name="retrieval_sync",
        event_type="project_writeback",
        event_key="retrieval-timeout-test",
        payload={"project_name": "SampleProj"},
    )

    def fake_run(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs.get("timeout", 0))

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    result = module.trigger_retrieval_sync_once()
    event = runtime_state.fetch_runtime_event("retrieval-timeout-test")

    assert result is not None
    assert result.returncode == 124
    assert event["status"] == "pending"
    assert "timed out" in str(event.get("last_error", ""))


def test_trigger_sync_helpers_use_current_python(sample_env, monkeypatch) -> None:
    module = importlib.reload(codex_memory)
    dashboard_script = sample_env["workspace_root"] / "ops" / "codex_dashboard_sync.py"
    retrieval_script = sample_env["workspace_root"] / "ops" / "codex_retrieval.py"
    dashboard_script.parent.mkdir(parents=True, exist_ok=True)
    dashboard_script.write_text("print('stub dashboard')\n", encoding="utf-8")
    retrieval_script.write_text("print('stub retrieval')\n", encoding="utf-8")

    captured: list[list[str]] = []

    def fake_run(command, **kwargs):
        captured.append(list(command))
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    module.trigger_dashboard_sync_once()
    module.trigger_retrieval_sync_once()

    assert captured[0][0] == sys.executable
    assert captured[1][0] == sys.executable

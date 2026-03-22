from __future__ import annotations

import argparse
import json

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

from __future__ import annotations

import json
from pathlib import Path

from ops import claude_code_runner


def test_load_claude_env_filters_allowed_keys(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "env": {
                    "ANTHROPIC_AUTH_TOKEN": "test-token",
                    "ANTHROPIC_BASE_URL": "http://127.0.0.1:3456",
                    "IGNORED_KEY": "ignored",
                }
            }
        ),
        encoding="utf-8",
    )

    env = claude_code_runner.load_claude_env(settings_path=settings_path)

    assert env == {
        "ANTHROPIC_AUTH_TOKEN": "test-token",
        "ANTHROPIC_BASE_URL": "http://127.0.0.1:3456",
    }


def test_render_prompt_uses_mode_preamble() -> None:
    rendered = claude_code_runner.render_prompt("challenge", "Challenge this rollout plan.")
    assert "adversarial second-opinion challenger" in rendered
    assert "Challenge this rollout plan." in rendered


def test_render_prompt_supports_writing_mode() -> None:
    rendered = claude_code_runner.render_prompt("writing", "Rewrite this launch note.")
    assert "dedicated writing specialist" in rendered
    assert "Rewrite this launch note." in rendered


def test_parse_json_output_returns_dict_only_for_valid_json_object() -> None:
    assert claude_code_runner.parse_json_output('{"status":"ok"}') == {"status": "ok"}
    assert claude_code_runner.parse_json_output('["not-an-object"]') is None
    assert claude_code_runner.parse_json_output("not-json") is None


def test_normalize_structured_output_prefers_inner_contract_object() -> None:
    provider_payload = {
        "type": "result",
        "structured_output": {
            "status": "completed",
            "question_or_focus": "q",
            "key_judgment": "j",
            "difference_from_current_judgment": "d",
            "recommended_next_step": "n",
        },
    }

    normalized = claude_code_runner.normalize_structured_output(provider_payload)

    assert normalized == {
        "status": "completed",
        "question_or_focus": "q",
        "key_judgment": "j",
        "difference_from_current_judgment": "d",
        "recommended_next_step": "n",
    }


def test_run_claude_uses_temp_home_and_filtered_env(monkeypatch, tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps(
            {
                "env": {
                    "ANTHROPIC_AUTH_TOKEN": "test-token",
                    "ANTHROPIC_BASE_URL": "http://127.0.0.1:3456",
                }
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    class Result:
        returncode = 0
        stdout = "OK\n"
        stderr = ""

    def fake_run(cmd, *, text, capture_output, check, env):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        captured["env"] = env
        return Result()

    monkeypatch.setattr(claude_code_runner.subprocess, "run", fake_run)

    payload = claude_code_runner.run_claude(
        mode="review",
        prompt="Review this diff.",
        model="sonnet",
        settings_path=settings_path,
    )

    assert payload["ok"] is True
    assert payload["stdout"] == "OK"
    assert captured["cmd"] == [
        "claude",
        "-p",
        "--no-session-persistence",
        "--permission-mode",
        claude_code_runner.SECOND_OPINION_PERMISSION_MODE,
        "--tools",
        claude_code_runner.SECOND_OPINION_TOOLS,
        "--append-system-prompt",
        claude_code_runner.SAFETY_APPEND_PROMPT,
        "--model",
        "sonnet",
        claude_code_runner.render_prompt("review", "Review this diff."),
    ]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["ANTHROPIC_AUTH_TOKEN"] == "test-token"
    assert env["ANTHROPIC_BASE_URL"] == "http://127.0.0.1:3456"
    assert str(env["HOME"]).startswith("/tmp/workspace-hub-claude-home-")
    assert payload["safety_mode"]["permission_mode"] == claude_code_runner.SECOND_OPINION_PERMISSION_MODE
    assert payload["safety_mode"]["tools"] == claude_code_runner.SECOND_OPINION_TOOLS


def test_run_claude_can_request_structured_json_output(monkeypatch, tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps({"env": {"ANTHROPIC_AUTH_TOKEN": "test-token"}}),
        encoding="utf-8",
    )

    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "type": "result",
                "structured_output": {
                    "status": "ok",
                    "question_or_focus": "q",
                    "key_judgment": "j",
                    "difference_from_current_judgment": "d",
                    "recommended_next_step": "n",
                },
            }
        )
        stderr = ""

    def fake_run(cmd, *, text, capture_output, check, env):  # type: ignore[no-untyped-def]
        return Result()

    monkeypatch.setattr(claude_code_runner.subprocess, "run", fake_run)

    payload = claude_code_runner.run_claude(
        mode="consult",
        prompt="Consult on this choice.",
        settings_path=settings_path,
        json_schema={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]},
    )

    assert payload["ok"] is True
    assert payload["structured_output"]["status"] == "ok"
    assert payload["provider_output"]["type"] == "result"
    assert "--output-format" in payload["command"]

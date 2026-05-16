from __future__ import annotations

from ops import lark_cli_backend


def test_backend_enabled_respects_custom_lark_cli_bin(monkeypatch) -> None:
    monkeypatch.setattr(
        lark_cli_backend.shutil,
        "which",
        lambda cmd: "/tmp/custom-lark-cli" if cmd == "/tmp/custom-lark-cli" else None,
    )

    assert lark_cli_backend.backend_enabled(
        "im",
        env={
            "WORKSPACE_HUB_FEISHU_BACKEND": "lark-cli",
            "WORKSPACE_HUB_LARK_CLI_BIN": "/tmp/custom-lark-cli",
        },
    )


def test_run_lark_cli_uses_custom_lark_cli_bin(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class Result:
        returncode = 0
        stdout = "{}"
        stderr = ""

    def fake_run(command, *, text, capture_output, input, check):  # type: ignore[no-untyped-def]
        captured["command"] = command
        return Result()

    monkeypatch.setenv("WORKSPACE_HUB_LARK_CLI_BIN", "/tmp/custom-lark-cli")
    monkeypatch.setattr(lark_cli_backend.subprocess, "run", fake_run)

    payload = lark_cli_backend._run_lark_cli(["im", "send", "--help"])

    assert payload == {}
    assert captured["command"][0] == "/tmp/custom-lark-cli"

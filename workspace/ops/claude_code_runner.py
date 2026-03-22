#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any


CLAUDE_SETTINGS_PATH = Path.home() / ".claude" / "settings.json"
CLAUDE_COMMAND = "claude"
DEFAULT_MODEL = "sonnet"
SECOND_OPINION_PERMISSION_MODE = "plan"
SECOND_OPINION_TOOLS = ""
ALLOWED_ENV_KEYS = {
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "CLAUDE_CODE_SUBAGENT_MODEL",
    "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
    "API_TIMEOUT_MS",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
}
MODE_PREAMBLES = {
    "review": (
        "You are Claude Code acting as a focused second-opinion reviewer. "
        "Review the concrete plan, diff, or judgment below. Return: "
        "1) Status 2) Question handed to the second opinion 3) Claude assessment "
        "4) Agreement or disagreement with the current judgment 5) Recommended next step."
    ),
    "challenge": (
        "You are Claude Code acting as an adversarial second-opinion challenger. "
        "Pressure-test the concrete plan or claim below. Return: "
        "1) Status 2) Claim being challenged 3) Strongest counterargument "
        "4) Evidence needed to resolve the challenge 5) Recommended next step."
    ),
    "consult": (
        "You are Claude Code acting as a lightweight second-opinion consultant. "
        "Reframe the concrete decision or option set below. Return: "
        "1) Status 2) Consult question 3) Claude perspective "
        "4) What changed versus the current framing 5) Recommended next step."
    ),
}
SAFETY_APPEND_PROMPT = (
    "Operate in strict second-opinion mode. "
    "Do not attempt file edits, shell commands, git operations, release actions, "
    "or any tool-driven mutation. "
    "Stay advisory, read from the provided prompt only, and return a concise structured judgment."
)


def load_claude_env(settings_path: Path = CLAUDE_SETTINGS_PATH) -> dict[str, str]:
    env: dict[str, str] = {}
    if settings_path.exists():
        payload = json.loads(settings_path.read_text(encoding="utf-8"))
        raw_env = payload.get("env", {})
        if isinstance(raw_env, dict):
            for key, value in raw_env.items():
                if key in ALLOWED_ENV_KEYS and isinstance(value, str):
                    env[key] = value
    for key in ALLOWED_ENV_KEYS:
        value = os.environ.get(key)
        if value:
            env[key] = value
    return env


def render_prompt(mode: str, prompt: str) -> str:
    preamble = MODE_PREAMBLES[mode]
    return f"{preamble}\n\nUser request:\n{prompt.strip()}\n"


def parse_json_output(stdout: str) -> dict[str, Any] | None:
    text = stdout.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def normalize_structured_output(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("status"), str) and isinstance(payload.get("key_judgment"), str):
        return payload
    nested = payload.get("structured_output")
    if (
        isinstance(nested, dict)
        and isinstance(nested.get("status"), str)
        and isinstance(nested.get("key_judgment"), str)
    ):
        return nested
    return None


def run_claude(
    *,
    mode: str,
    prompt: str,
    model: str = DEFAULT_MODEL,
    settings_path: Path = CLAUDE_SETTINGS_PATH,
    json_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env = os.environ.copy()
    env.update(load_claude_env(settings_path=settings_path))
    home_root = Path(tempfile.mkdtemp(prefix="workspace-hub-claude-home-", dir="/tmp"))
    env["HOME"] = str(home_root)
    home_root.joinpath(".claude").mkdir(parents=True, exist_ok=True)

    cmd = [
        CLAUDE_COMMAND,
        "-p",
        "--no-session-persistence",
        "--permission-mode",
        SECOND_OPINION_PERMISSION_MODE,
        "--tools",
        SECOND_OPINION_TOOLS,
        "--append-system-prompt",
        SAFETY_APPEND_PROMPT,
        "--model",
        model,
    ]
    if json_schema is not None:
        cmd.extend(
            [
                "--output-format",
                "json",
                "--json-schema",
                json.dumps(json_schema, ensure_ascii=False),
            ]
        )
    cmd.append(render_prompt(mode, prompt))
    result = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    parsed_output = parse_json_output(result.stdout) if json_schema is not None else None
    normalized_output = normalize_structured_output(parsed_output)
    return {
        "ok": result.returncode == 0,
        "mode": mode,
        "model": model,
        "safety_mode": {
            "permission_mode": SECOND_OPINION_PERMISSION_MODE,
            "tools": SECOND_OPINION_TOOLS,
            "append_system_prompt": SAFETY_APPEND_PROMPT,
        },
        "command": [
            CLAUDE_COMMAND,
            "-p",
            "--no-session-persistence",
            "--permission-mode",
            SECOND_OPINION_PERMISSION_MODE,
            "--tools",
            SECOND_OPINION_TOOLS,
            "--append-system-prompt",
            "<safety_prompt>",
            "--model",
            model,
            *(
                ["--output-format", "json", "--json-schema", "<json_schema>"]
                if json_schema is not None
                else []
            ),
            "<prompt>",
        ],
        "temp_home": str(home_root),
        "env_keys": sorted(load_claude_env(settings_path=settings_path).keys()),
        "stdout": result.stdout.strip(),
        "provider_output": parsed_output,
        "structured_output": normalized_output,
        "stderr": result.stderr.strip(),
        "returncode": result.returncode,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Claude Code as a second-opinion helper.")
    parser.add_argument("--mode", choices=sorted(MODE_PREAMBLES), default="consult")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--settings-path", default=str(CLAUDE_SETTINGS_PATH))
    parser.add_argument("--json", action="store_true", help="Emit a JSON envelope instead of raw stdout.")
    parser.add_argument("prompt")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = run_claude(
        mode=args.mode,
        prompt=args.prompt,
        model=args.model,
        settings_path=Path(args.settings_path),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        if payload["stdout"]:
            print(payload["stdout"])
        if not payload["ok"] and payload["stderr"]:
            print(payload["stderr"])
    return int(payload["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())

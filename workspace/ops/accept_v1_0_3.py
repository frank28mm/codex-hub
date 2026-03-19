#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> dict[str, object]:
    completed = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def require_ok(result: dict[str, object]) -> None:
    if int(result["returncode"]) != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(result['cmd'])}\nstdout:\n{result['stdout']}\nstderr:\n{result['stderr']}"
        )


def main() -> int:
    checks: list[str] = []

    py_compile = run(
        [
            "python3",
            "-m",
            "py_compile",
            "ops/control_gate.py",
            "ops/controlled_common.py",
            "ops/controlled_git.py",
            "ops/controlled_gh.py",
            "ops/controlled_ssh.py",
            "ops/controlled_browser.py",
            "ops/codex_control.py",
            "ops/runtime_state.py",
            "ops/local_broker.py",
            "tests/test_v1_0_3.py",
        ]
    )
    require_ok(py_compile)
    checks.append("py_compile")

    pytest = run(
        [
            "python3",
            "-m",
            "pytest",
            "-q",
            "tests/test_control_gate.py",
            "tests/test_v1_0_1.py",
            "tests/test_v1_0_3.py",
        ]
    )
    require_ok(pytest)
    checks.append("pytest")

    broker = run(["python3", "ops/local_broker.py", "status"])
    require_ok(broker)
    checks.append("broker-status")

    print(json.dumps({"ok": True, "version": "v1.0.3", "checks": checks}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

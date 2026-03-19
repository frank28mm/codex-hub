#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
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
            "ops/codex_memory.py",
            "ops/coordination_plane.py",
            "ops/codex_context.py",
            "tests/test_review_coordination.py",
        ]
    )
    require_ok(py_compile)
    checks.append("py_compile")

    pytest = run(["python3", "-m", "pytest", "-q", "tests/test_review_coordination.py", "-k", "coordination_plane"])
    require_ok(pytest)
    checks.append("pytest-coordination")

    rebuild = run(["python3", "ops/coordination_plane.py", "rebuild"])
    require_ok(rebuild)
    checks.append("coordination-rebuild")

    print(json.dumps({"ok": True, "version": "v1.0.5", "checks": checks}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

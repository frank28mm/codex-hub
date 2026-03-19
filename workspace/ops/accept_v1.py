#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.fixture_builder import build_sample_environment  # noqa: E402


def run_command(cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, object]:
    completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def require_success(result: dict[str, object]) -> None:
    if int(result["returncode"]) != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(result['cmd'])}\nstdout:\n{result['stdout']}\nstderr:\n{result['stderr']}"
        )


def build_fixture_env(temp_root: Path) -> dict[str, str]:
    sample = build_sample_environment(temp_root)
    shutil.copytree(REPO_ROOT / "control", sample["control_root"], dirs_exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "WORKSPACE_HUB_ROOT": str(sample["workspace_root"]),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample["vault_root"]),
            "WORKSPACE_HUB_PROJECTS_ROOT": str(sample["projects_root"]),
            "WORKSPACE_HUB_REPORTS_ROOT": str(sample["reports_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample["runtime_root"]),
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample["control_root"]),
        }
    )
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env["SAMPLE_PROJECT_GUIDE"] = str(sample["sample_project"] / "guide.md")
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description="Run workspace-hub v1 acceptance checks")
    parser.add_argument("--keep", action="store_true", help="Keep the temporary fixture directory")
    args = parser.parse_args()

    temp_root = Path(tempfile.mkdtemp(prefix="workspace-hub-v1-accept-"))
    try:
        fixture_env = build_fixture_env(temp_root)
        results: list[dict[str, object]] = []

        results.append(run_command(["python3", "-m", "pytest", "-q", "tests"], env=os.environ.copy()))
        require_success(results[-1])

        py_files = [
            "ops/control_gate.py",
            "ops/codex_control.py",
            "ops/codex_retrieval.py",
            "ops/accept_v1.py",
            "tests/conftest.py",
            "tests/fixture_builder.py",
            "tests/test_control_gate.py",
            "tests/test_retrieval.py",
        ]
        results.append(run_command(["python3", "-m", "py_compile", *py_files], env=os.environ.copy()))
        require_success(results[-1])

        results.append(
            run_command(
                [
                    "python3",
                    "ops/codex_control.py",
                    "decide",
                    "--target",
                    "https://example.com",
                    "--action",
                    "read",
                    "--execution-context",
                    "noninteractive",
                ],
                env=fixture_env,
            )
        )
        require_success(results[-1])

        results.append(
            run_command(
                [
                    "python3",
                    "ops/codex_control.py",
                    "audit",
                    "--target",
                    "https://github.com",
                    "--action",
                    "read",
                    "--result",
                    "success",
                    "--target-class",
                    "owned-low",
                    "--action-class",
                    "read",
                    "--execution-context",
                    "interactive",
                ],
                env=fixture_env,
            )
        )
        require_success(results[-1])

        results.append(run_command(["python3", "ops/codex_control.py", "status"], env=fixture_env))
        require_success(results[-1])

        results.append(run_command(["python3", "ops/codex_retrieval.py", "build-index"], env=fixture_env))
        require_success(results[-1])

        results.append(
            run_command(
                ["python3", "ops/codex_retrieval.py", "search", "--query", "PDF Fixture Marker", "--limit", "5"],
                env=fixture_env,
            )
        )
        require_success(results[-1])

        results.append(
            run_command(
                [
                    "python3",
                    "ops/codex_retrieval.py",
                    "get",
                    "--path",
                    fixture_env["SAMPLE_PROJECT_GUIDE"],
                ],
                env=fixture_env,
            )
        )
        require_success(results[-1])

        results.append(run_command(["python3", "ops/codex_retrieval.py", "sync-index"], env=fixture_env))
        require_success(results[-1])

        results.append(run_command(["python3", "ops/codex_retrieval.py", "status"], env=fixture_env))
        require_success(results[-1])

        results.append(run_command(["python3", "ops/codex_dashboard_sync.py", "verify-consistency"], env=os.environ.copy()))
        require_success(results[-1])

        summary = {
            "ok": True,
            "commands": len(results),
            "fixture_root": str(temp_root),
            "kept": bool(args.keep),
        }
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "fixture_root": str(temp_root)}, ensure_ascii=False))
        return 1
    finally:
        if not args.keep and temp_root.exists():
            shutil.rmtree(temp_root)

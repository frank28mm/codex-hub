#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
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


def require_returncode(result: dict[str, object], allowed: set[int]) -> None:
    if int(result["returncode"]) not in allowed:
        raise RuntimeError(
            f"Command failed: {' '.join(result['cmd'])}\nstdout:\n{result['stdout']}\nstderr:\n{result['stderr']}"
        )


def build_fixture_env(temp_root: Path) -> tuple[dict[str, str], dict[str, Path]]:
    sample = build_sample_environment(temp_root)
    shutil.copytree(REPO_ROOT / "control", sample["control_root"], dirs_exist_ok=True)
    codex_home = temp_root / ".codex-home"
    automation_dir = codex_home / "automations" / "workspace-health"
    automation_dir.mkdir(parents=True, exist_ok=True)
    (automation_dir / "automation.toml").write_text(
        "\n".join(
            [
                "version = 1",
                'id = "workspace-health"',
                'name = "Workspace Health"',
                'status = "ACTIVE"',
                'prompt = "Run health check"',
                f'cwds = ["{REPO_ROOT}", "{sample["vault_root"]}"]',
                "",
            ]
        ),
        encoding="utf-8",
    )
    sqlite_dir = codex_home / "sqlite"
    sqlite_dir.mkdir(parents=True, exist_ok=True)
    db_path = sqlite_dir / "codex-dev.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "create table automations (id text primary key, status text, last_run_at integer, next_run_at integer)"
        )
        conn.execute(
            "create table automation_runs (thread_id text primary key, automation_id text not null, status text not null, created_at integer not null, updated_at integer not null)"
        )
        conn.execute(
            "insert into automations (id, status, last_run_at, next_run_at) values (?, ?, ?, ?)",
            ("workspace-health", "ACTIVE", None, 1773225994000),
        )
        conn.commit()
    env = os.environ.copy()
    env.update(
        {
            "WORKSPACE_HUB_CODE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_ROOT": str(sample["workspace_root"]),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample["vault_root"]),
            "WORKSPACE_HUB_FIXTURE_MODE": "1",
            "WORKSPACE_HUB_EXPECTED_WORKSPACE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_EXPECTED_VAULT_ROOT": str(sample["vault_root"]),
            "WORKSPACE_HUB_EXPECTED_PROJECTS_ROOT": str(sample["projects_root"]),
            "WORKSPACE_HUB_PROJECTS_ROOT": str(sample["projects_root"]),
            "WORKSPACE_HUB_REPORTS_ROOT": str(sample["reports_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample["runtime_root"]),
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample["control_root"]),
            "WORKSPACE_HUB_SKIP_DISCOVERY": "1",
            "CODEX_HOME": str(codex_home),
        }
    )
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return env, sample


def main() -> int:
    parser = argparse.ArgumentParser(description="Run workspace-hub v1.0.2 acceptance checks")
    parser.add_argument("--keep", action="store_true", help="Keep the temporary fixture directory")
    args = parser.parse_args()

    temp_root = Path(tempfile.mkdtemp(prefix="workspace-hub-v1-0-2-accept-"))
    try:
        fixture_env, sample = build_fixture_env(temp_root)
        results: list[dict[str, object]] = []

        results.append(
            run_command(
                [
                    "python3",
                    "-m",
                    "pytest",
                    "-q",
                    "tests/test_workspace_hub_ops.py",
                    "tests/test_v1_0_2.py",
                    "tests/test_control_gate.py",
                    "tests/test_v1_0_1.py",
                ],
                env=os.environ.copy(),
            )
        )
        require_returncode(results[-1], {0})

        py_files = [
            "ops/workspace_hub_health_check.py",
            "ops/workspace_hub_route_check.py",
            "ops/codex_session_watcher.py",
            "ops/codex_memory.py",
            "ops/codex_dashboard_sync.py",
            "ops/accept_v1_0_2.py",
            "tests/test_workspace_hub_ops.py",
            "tests/test_v1_0_2.py",
        ]
        results.append(run_command(["python3", "-m", "py_compile", *py_files], env=os.environ.copy()))
        require_returncode(results[-1], {0})

        results.append(
            run_command(
                [
                    "python3",
                    "-c",
                    "\n".join(
                        [
                            "import json",
                            "from ops import workspace_hub_health_check",
                            "checks = {",
                            "    'checked_at': '2026-03-11T16:00:00+08:00',",
                            "    'watcher': {'installed': True, 'loaded': True},",
                            "    'dashboard_sync': {'installed': True, 'loaded': True, 'pending_events': 0},",
                            "    'consistency': {'ok': True, 'issues': [], 'issue_count': 0, 'exit_code': 0},",
                            "    'routing': {'ok': True, 'case_count': 1, 'results': []},",
                            "    'official_scheduler': {",
                            "        'configured': True,",
                            "        'active': True,",
                            "        'cwd_matches': True,",
                            "        'run_count': 0,",
                            "        'verified_run_count': 0,",
                            "        'last_run_at': '',",
                            "        'next_run_at': '2026-03-11T18:46:34+08:00',",
                            "    },",
                            "    'health_launchagent': {'configured': True, 'active': True},",
                            "    'codex_automation': {'configured': True, 'active': False, 'runtime_status': 'PAUSED'},",
                            "    'run_context': {'trigger_source': 'codex_automation'},",
                            "}",
                            "payload = workspace_hub_health_check.run_health_check(checks=checks, trigger_source='codex_automation')",
                            "print(json.dumps(payload, ensure_ascii=False))",
                        ]
                    ),
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        health_payload = json.loads(str(results[-1]["stdout"]))
        if health_payload["run_record"]["trigger_source"] != "codex_automation":
            raise RuntimeError(f"Unexpected trigger source: {health_payload}")
        if not Path(health_payload["log_paths"]["archive_path"]).exists():
            raise RuntimeError(f"Archive report missing: {health_payload}")
        if not Path(health_payload["log_paths"]["latest_path"]).exists():
            raise RuntimeError(f"Latest report missing: {health_payload}")
        if not (sample["reports_root"] / "ops" / "workspace-hub-health" / "history.ndjson").exists():
            raise RuntimeError("Run ledger missing after health check")
        if not (sample["reports_root"] / "ops" / "workspace-hub-health" / "alerts.ndjson").exists():
            raise RuntimeError("Alert ledger missing after health check")

        print(
            json.dumps(
                {
                    "ok": True,
                    "temp_root": str(temp_root),
                    "checks": [
                        "pytest",
                        "py_compile",
                        "health-check-run-once",
                    ],
                    "health_report": health_payload["log_paths"]["archive_path"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    finally:
        if not args.keep:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

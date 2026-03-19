from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def append_pause(control_root: Path, entry: dict) -> None:
    path = control_root / "project-pauses.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {"version": 1, "entries": []}
    data.setdefault("entries", []).append(entry)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def read_payload(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip())


def test_project_pause_matches_sample_project(sample_env) -> None:
    from ops import project_pause

    append_pause(
        sample_env["control_root"],
        {
            "id": "sampleproj-rest-day",
            "project_name": "SampleProj",
            "start_date": "2020-01-01",
            "end_date": "2100-01-01",
            "scopes": ["session_start", "session_writeback", "broker_execution"],
            "reason": "User is off today.",
        },
    )

    payload = project_pause.active_pause(project_name="SampleProj", scope="broker_execution", on_date=project_pause.parse_date("2026-03-14"))
    assert payload["active"] is True
    assert payload["entry"]["project_name"] == "SampleProj"
    assert payload["entry"]["reason"] == "User is off today."


def test_local_broker_blocks_paused_project(sample_env, capsys) -> None:
    from ops import local_broker as local_broker_module

    append_pause(
        sample_env["control_root"],
        {
            "id": "sampleproj-broker-pause",
            "project_name": "SampleProj",
            "start_date": "2020-01-01",
            "end_date": "2100-01-01",
            "scopes": ["broker_execution"],
            "reason": "Pause project execution for today.",
        },
    )

    local_broker = importlib.reload(local_broker_module)
    assert (
        local_broker.cmd_command_center(
            argparse.Namespace(
                action="codex-exec",
                prompt="Continue SampleProj",
                session_id="",
                project_name="SampleProj",
                execution_profile="",
            )
        )
        == 0
    )
    payload = read_payload(capsys)
    assert payload["ok"] is False
    assert payload["reason"] == "project_paused"
    assert payload["project_name"] == "SampleProj"


def test_session_watcher_suppresses_paused_project(sample_env) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import codex_session_watcher as watcher_module

    append_pause(
        sample_env["control_root"],
        {
            "id": "sampleproj-writeback-pause",
            "project_name": "SampleProj",
            "start_date": "2020-01-01",
            "end_date": "2100-01-01",
            "scopes": ["session_writeback"],
            "reason": "No writeback today.",
        },
    )

    importlib.reload(codex_memory_module)
    watcher = importlib.reload(watcher_module)
    result = watcher.sync_snapshot(
        {
            "id": "sess-paused",
            "started_at": "2026-03-14T01:00:00Z",
            "last_active_at": "2026-03-14T01:05:00Z",
            "cwd": str(sample_env["workspace_root"]),
            "user_message": "继续处理 SampleProj",
            "last_agent_message": "已完成一部分工作。",
            "completed": True,
            "path": str(sample_env["workspace_root"] / "sess-paused.jsonl"),
            "mtime": 1.0,
        }
    )
    assert result is not None
    assert result["action"] == "suppressed"
    assert result["reason"] == "project_paused"
    assert result["project_name"] == "SampleProj"


def test_workspace_hub_health_check_skips_when_project_paused(sample_env) -> None:
    from ops import workspace_hub_health_check as health_module

    append_pause(
        sample_env["control_root"],
        {
            "id": "codex-hub-ops-pause",
            "project_name": "Codex Hub",
            "start_date": "2020-01-01",
            "end_date": "2100-01-01",
            "scopes": ["automation"],
            "reason": "No automation today.",
        },
    )

    workspace_hub_health_check = importlib.reload(health_module)
    payload = workspace_hub_health_check.run_health_check(trigger_source="manual_cli")
    assert payload["ok"] is True
    assert payload["skipped"] is True
    assert payload["reason"] == "project_paused"


def test_start_codex_dry_run_reports_pause(sample_env) -> None:
    append_pause(
        sample_env["control_root"],
        {
            "id": "sampleproj-start-pause",
            "project_name": "SampleProj",
            "start_date": "2020-01-01",
            "end_date": "2100-01-01",
            "scopes": ["session_start"],
            "reason": "No new sessions today.",
        },
    )

    result = subprocess.run(
        [
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "Continue SampleProj",
            "--dry-run",
        ],
        cwd=sample_env["workspace_root"],
        env={
            **os.environ,
            "WORKSPACE_HUB_ROOT": str(sample_env["workspace_root"]),
            "WORKSPACE_HUB_CODE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample_env["vault_root"]),
            "WORKSPACE_HUB_EXPECTED_WORKSPACE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_EXPECTED_VAULT_ROOT": str(sample_env["vault_root"]),
            "WORKSPACE_HUB_EXPECTED_PROJECTS_ROOT": str(sample_env["projects_root"]),
            "WORKSPACE_HUB_PROJECTS_ROOT": str(sample_env["projects_root"]),
            "WORKSPACE_HUB_REPORTS_ROOT": str(sample_env["reports_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_SKIP_DISCOVERY": "1",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "pause_active=1" in result.stdout
    assert "No new sessions today." in result.stdout

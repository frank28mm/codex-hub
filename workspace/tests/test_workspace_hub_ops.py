from __future__ import annotations

import importlib
import json
import sqlite3
import subprocess
from pathlib import Path

from ops import codex_retrieval
from ops.workspace_hub_project import PROJECT_NAME


def test_workspace_hub_route_check_matches_start_and_app(sample_env) -> None:
    from ops import workspace_hub_route_check

    codex_retrieval.build_index()
    case = workspace_hub_route_check.RouteCase(
        name="sample-topic",
        project_name="SampleProj",
        prompt="请查看需求 demand Topic Retrieval Marker",
        expected_scope="topic",
        expected_board_suffix="SampleProj-需求-跟进板.md",
    )

    payload = workspace_hub_route_check.run_checks([case])
    assert payload["ok"] is True
    assert payload["case_count"] == 1
    assert payload["results"][0]["start_codex"]["binding_scope"] == "topic"
    assert payload["results"][0]["app_direct"]["binding_scope"] == "topic"


def test_workspace_hub_route_check_uses_expected_roots_when_outer_run_is_isolated(sample_env, monkeypatch) -> None:
    from ops import workspace_hub_route_check as route_module

    codex_retrieval.build_index()
    isolated_root = sample_env["workspace_root"] / "isolated-run"
    isolated_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(isolated_root))
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", "/tmp/codex-hub-shadow-workspace")
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", str(sample_env["vault_root"]))
    workspace_hub_route_check = importlib.reload(route_module)
    case = workspace_hub_route_check.RouteCase(
        name="sample-topic",
        project_name="SampleProj",
        prompt="请查看需求 demand Topic Retrieval Marker",
        expected_scope="topic",
        expected_board_suffix="SampleProj-需求-跟进板.md",
    )

    payload = workspace_hub_route_check.run_checks([case])

    assert payload["ok"] is True
    assert payload["results"][0]["app_direct"]["board_path"].startswith(str(sample_env["vault_root"]))


def test_workspace_hub_route_check_ignores_shadow_vault_override_outside_fixture_mode(sample_env, monkeypatch) -> None:
    from ops import workspace_hub_route_check as route_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("WORKSPACE_HUB_FIXTURE_MODE", raising=False)
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", "/tmp/codex-hub-shadow-workspace")
    workspace_hub_route_check = importlib.reload(route_module)

    assert workspace_hub_route_check.expected_vault_root() == workspace_hub_route_check.DEFAULT_VAULT_ROOT


def test_workspace_hub_route_check_default_cases_skip_missing_topic_boards(sample_env) -> None:
    from ops import workspace_hub_route_check as route_module

    workspace_hub_route_check = importlib.reload(route_module)
    names = {case.name for case in workspace_hub_route_check.default_cases()}

    assert "workspace-system-project" in names
    assert "workspace-system-legacy-alias" in names
    assert len(names) == 2


def test_workspace_hub_health_check_codex_automation_status_uses_expected_roots(sample_env, monkeypatch, tmp_path: Path) -> None:
    from ops import workspace_hub_health_check as health_module

    codex_home = tmp_path / ".codex-home"
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
                f'cwds = ["/tmp/codex-hub-workspace", "{sample_env["vault_root"]}"]',
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

    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("WORKSPACE_HUB_FIXTURE_MODE", "1")
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(sample_env["workspace_root"] / "isolated-run"))
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_WORKSPACE_ROOT", "/tmp/codex-hub-workspace")
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", str(sample_env["vault_root"]))
    workspace_hub_health_check = importlib.reload(health_module)

    status = workspace_hub_health_check.load_codex_automation_status()

    assert status["configured"] is True
    assert status["cwd_matches"] is True
    assert status["missing_cwds"] == []
    assert status["active"] is True


def test_workspace_hub_health_check_ignores_shadow_vault_override_outside_fixture_mode(monkeypatch) -> None:
    from ops import workspace_hub_health_check as health_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("WORKSPACE_HUB_FIXTURE_MODE", raising=False)
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", "/tmp/codex-hub-shadow-workspace")
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", "/tmp/codex-hub-shadow-workspace")
    workspace_hub_health_check = importlib.reload(health_module)

    assert workspace_hub_health_check.expected_vault_root() == workspace_hub_health_check.DEFAULT_VAULT_ROOT
    assert workspace_hub_health_check.vault_root() == workspace_hub_health_check.DEFAULT_VAULT_ROOT


def test_workspace_hub_health_check_writes_logs_and_syncs_board(sample_env, monkeypatch) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)

    checks = {
        "checked_at": "2026-03-11T13:00:00+08:00",
        "watcher": {"installed": True, "loaded": True},
        "dashboard_sync": {"installed": True, "loaded": True, "pending_events": 0},
        "consistency": {"ok": True, "issues": [], "issue_count": 0, "exit_code": 0},
        "routing": {"ok": True, "case_count": 1, "results": []},
        "official_scheduler": {
            "configured": True,
            "active": True,
            "cwd_matches": True,
            "run_count": 1,
            "verified_run_count": 1,
            "last_run_at": "2026-03-11T12:00:00+08:00",
            "next_run_at": "2026-03-11T16:00:00+08:00",
        },
        "health_launchagent": {"configured": True, "active": True},
        "codex_automation": {"configured": True, "active": False, "runtime_status": "PAUSED"},
        "run_context": {"trigger_source": "manual_cli"},
    }
    monkeypatch.setattr(workspace_hub_health_check.codex_memory, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(workspace_hub_health_check, "trigger_dashboard_rebuild", lambda: None)

    payload = workspace_hub_health_check.run_health_check(checks=checks, trigger_source="manual_cli")

    latest = Path(payload["log_paths"]["latest_path"])
    assert latest.exists()
    assert f"{PROJECT_NAME} 健康巡检日志" in latest.read_text(encoding="utf-8")
    topic_board = sample_env["vault_root"] / "01_working" / f"{PROJECT_NAME}-运维巡检-跟进板.md"
    assert topic_board.exists()
    topic_text = topic_board.read_text(encoding="utf-8")
    assert "WH-HC-01" in topic_text
    assert "WH-HC-06" in topic_text
    assert str(topic_board) in payload["changed_targets"]

    project_board = sample_env["vault_root"] / "01_working" / f"{PROJECT_NAME}-项目板.md"
    assert project_board.exists()
    project_text = project_board.read_text(encoding="utf-8")
    assert "topic:运维巡检" in project_text

    history_entry = json.loads(
        (sample_env["reports_root"] / "ops" / "workspace-hub-health" / "history.ndjson")
        .read_text(encoding="utf-8")
        .splitlines()[-1]
    )
    assert history_entry["run_id"].startswith("whc-")
    assert history_entry["trigger_source"] == "manual_cli"
    assert history_entry["script_version"]
    assert history_entry["writeback_targets"]

    alerts_path = sample_env["reports_root"] / "ops" / "workspace-hub-health" / "alerts.ndjson"
    assert alerts_path.exists()
    assert alerts_path.read_text(encoding="utf-8") == ""


def test_workspace_hub_health_check_collects_checks_before_acquiring_workspace_lock(sample_env, monkeypatch) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)

    state = {"locked": False, "collect_called": False, "sync_called_under_lock": False}

    class DummyLock:
        def __enter__(self):
            assert state["locked"] is False
            state["locked"] = True
            return self

        def __exit__(self, exc_type, exc, tb):
            state["locked"] = False
            return False

    checks = {
        "checked_at": "2026-03-11T13:00:00+08:00",
        "watcher": {"installed": True, "loaded": True},
        "dashboard_sync": {"installed": True, "loaded": True, "pending_events": 0},
        "consistency": {"ok": True, "issues": [], "issue_count": 0, "exit_code": 0},
        "routing": {"ok": True, "case_count": 1, "results": []},
        "official_scheduler": {
            "configured": True,
            "active": True,
            "cwd_matches": True,
            "run_count": 1,
            "verified_run_count": 1,
            "last_run_at": "2026-03-11T12:00:00+08:00",
            "next_run_at": "2026-03-11T16:00:00+08:00",
        },
        "health_launchagent": {"installed": False, "loaded": False},
        "run_context": {"trigger_source": "manual_cli"},
    }

    def fake_collect(run_context):
        state["collect_called"] = True
        assert state["locked"] is False
        return checks

    monkeypatch.setattr(workspace_hub_health_check, "collect_checks", fake_collect)
    monkeypatch.setattr(workspace_hub_health_check.codex_memory, "workspace_lock", lambda: DummyLock())
    monkeypatch.setattr(workspace_hub_health_check.codex_memory, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(workspace_hub_health_check, "trigger_dashboard_rebuild", lambda: None)
    monkeypatch.setattr(workspace_hub_health_check, "write_run_ledger", lambda run_record: None)
    monkeypatch.setattr(
        workspace_hub_health_check,
        "update_alert_ledger",
        lambda result, **kwargs: {
            "updates": [],
            "open_alerts": [],
            "pending_alerts": [],
            "resolved_alerts": [],
            "latest_states": {},
        },
    )

    def fake_sync(result, *, trigger_followup_syncs):
        state["sync_called_under_lock"] = state["locked"]
        return []

    monkeypatch.setattr(workspace_hub_health_check, "sync_health_topic_board", fake_sync)

    payload = workspace_hub_health_check.run_health_check(trigger_source="manual_cli")

    assert payload["ok"] is True
    assert state["collect_called"] is True
    assert state["sync_called_under_lock"] is True


def test_workspace_hub_health_check_surfaces_failures() -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)

    checks = {
        "checked_at": "2026-03-11T13:00:00+08:00",
        "watcher": {"installed": False, "loaded": False},
        "dashboard_sync": {"installed": True, "loaded": False, "pending_events": 3},
        "consistency": {"ok": False, "issues": ["ACTIONS mismatch"], "issue_count": 1, "exit_code": 1},
        "routing": {
            "ok": False,
            "case_count": 1,
            "results": [{"name": "case-1", "issues": ["binding_scope mismatch"]}],
        },
        "official_scheduler": {
            "configured": False,
            "active": False,
            "cwd_matches": True,
            "run_count": 0,
            "verified_run_count": 0,
            "last_run_at": "",
            "next_run_at": "2026-03-11T16:00:00+08:00",
        },
        "health_launchagent": {"configured": False, "active": False},
        "codex_automation": {"configured": True, "active": True, "runtime_status": "ACTIVE"},
        "run_context": {"trigger_source": "manual_cli"},
    }
    result = workspace_hub_health_check.evaluate_checks(checks)
    assert result["ok"] is False
    assert any("watcher" in issue for issue in result["issues"])
    assert any("ACTIONS mismatch" in issue for issue in result["issues"])
    statuses = {row["ID"]: row["状态"] for row in result["rows"]}
    assert statuses["WH-HC-01"] == "blocked"
    assert statuses["WH-HC-03"] == "blocked"
    assert statuses["WH-HC-04"] == "blocked"
    assert statuses["WH-HC-05"] == "blocked"
    assert statuses["WH-HC-06"] == "blocked"


def test_workspace_hub_health_check_collect_checks_uses_post_refresh_consistency(sample_env, monkeypatch) -> None:
    from ops import workspace_hub_health_check as health_module

    workspace_hub_health_check = importlib.reload(health_module)

    verify_results = iter(
        [
            ({"ok": False, "issues": ["stale rollup"], "issue_count": 52}, 1),
            ({"ok": True, "issues": [], "issue_count": 0}, 0),
        ]
    )

    def fake_run_json_command(command):
        script = Path(command[1]).name
        action = command[2]
        if script == "codex_session_watcher.py" and action == "status":
            return ({"installed": True, "loaded": True}, 0)
        if script == "codex_dashboard_sync.py" and action == "status":
            return ({"installed": True, "loaded": True, "pending_events": 0}, 0)
        if script == "codex_dashboard_sync.py" and action == "verify-consistency":
            return next(verify_results)
        if script == "codex_memory.py" and action == "refresh-index":
            return ({"changed": True}, 0)
        raise AssertionError(f"unexpected command: {command}")

    def fake_subprocess_run(command, **kwargs):
        if command[:2] == ["launchctl", "print"]:
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="not loaded")
        script = Path(command[1]).name
        action = command[2]
        if script == "codex_dashboard_sync.py" and action == "rebuild-all":
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"status": "ok"}), stderr="")
        raise AssertionError(f"unexpected subprocess command: {command}")

    monkeypatch.setattr(workspace_hub_health_check, "run_json_command", fake_run_json_command)
    monkeypatch.setattr(workspace_hub_health_check.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        workspace_hub_health_check,
        "load_official_scheduler_status",
        lambda: {
            "configured": True,
            "active": True,
            "cwd_matches": True,
            "verified_run_count": 1,
            "run_count": 1,
            "direct_run_count": 1,
            "last_run_at": "2026-03-13T08:00:00+08:00",
            "next_run_at": "2026-03-13T12:00:00+08:00",
        },
    )
    monkeypatch.setattr(
        workspace_hub_health_check,
        "load_codex_automation_status",
        lambda: {
            "configured": True,
            "active": False,
            "cwd_matches": True,
            "verified_run_count": 1,
            "run_count": 1,
            "direct_run_count": 1,
            "last_run_at": "2026-03-13T08:00:00+08:00",
            "next_run_at": "2026-03-13T12:00:00+08:00",
        },
    )
    monkeypatch.setattr(
        workspace_hub_health_check.workspace_hub_route_check,
        "run_checks",
        lambda: {"ok": True, "case_count": 1, "results": []},
    )

    checks = workspace_hub_health_check.collect_checks({"trigger_source": "manual_cli", "scheduler_id": ""})
    result = workspace_hub_health_check.evaluate_checks(checks)

    assert checks["consistency_pre_refresh"]["issue_count"] == 52
    assert checks["refresh_index"]["changed"] is True
    assert checks["rebuild_all"]["status"] == "ok"
    assert checks["consistency"]["ok"] is True
    assert result["ok"] is True

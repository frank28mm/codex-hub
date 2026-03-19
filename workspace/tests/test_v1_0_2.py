from __future__ import annotations

import argparse
import datetime as dt
import importlib
import json
import os


def test_update_now_and_next_actions_suppresses_absorbed_followups(sample_env) -> None:
    from ops import codex_memory as codex_memory_module

    codex_memory = importlib.reload(codex_memory_module)

    board_path = sample_env["vault_root"] / "01_working" / "SampleProj-项目板.md"
    board_time = dt.datetime(2026, 3, 11, 14, 0, tzinfo=dt.timezone.utc).timestamp()
    os.utime(board_path, (board_time, board_time))

    codex_memory.update_now_and_next_actions(
        [
            {
                "status": "completed",
                "project_name": "SampleProj",
                "binding_board_path": str(board_path),
                "last_active_at": "2026-03-11T13:00:00+00:00",
                "started_at": "2026-03-11T12:55:00+00:00",
                "prompt": "请检查这个会话",
                "thread_name": "请检查这个会话",
                "mode": "new",
                "session_id": "sess-followup-hidden",
            }
        ]
    )

    next_actions = (sample_env["vault_root"] / "NEXT_ACTIONS.md").read_text(encoding="utf-8")
    assert "检查 `SampleProj` 最近会话沉淀" not in next_actions
    assert "暂无自动跟进项" in next_actions


def test_normalize_vault_path_rewrites_legacy_icloud_root(sample_env) -> None:
    from ops import codex_memory as codex_memory_module

    codex_memory = importlib.reload(codex_memory_module)

    legacy = "/tmp/legacy-codex-hub-memory/01_working/SampleProj-项目板.md"
    normalized = codex_memory.normalize_vault_path(legacy)
    assert normalized == str(sample_env["vault_root"] / "01_working" / "SampleProj-项目板.md")


def test_refresh_next_actions_rollup_keeps_blocked_alerts_visible(sample_env) -> None:
    from ops import codex_memory as codex_memory_module

    codex_memory = importlib.reload(codex_memory_module)

    board = codex_memory.load_project_board("SampleProj")
    project_rows = [
        {
            "ID": "SP-01",
            "父ID": "SP-01",
            "来源": "project",
            "范围": "delivery",
            "事项": "继续推进样例项目",
            "状态": "doing",
            "下一步": "保持推进",
            "更新时间": "2026-03-11T13:00:00+08:00",
            "指向": board["path"].name,
        }
    ]
    rollup_rows = [
        {
            "ID": "SP-HC-01",
            "父ID": "SP-OPS-01",
            "来源": "topic:运维巡检",
            "范围": "调度",
            "事项": "官方巡检尚未完成真实定时运行验收",
            "状态": "blocked",
            "下一步": "等待下一次定时窗口完成验收",
            "更新时间": "2026-03-11T13:10:00+08:00",
            "指向": "SampleProj-运维巡检-跟进板.md",
        }
    ]
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], project_rows, rollup_rows)
    codex_memory.refresh_next_actions_rollup()

    next_actions = (sample_env["vault_root"] / "NEXT_ACTIONS.md").read_text(encoding="utf-8")
    assert "`SampleProj` SP-01 继续推进样例项目" in next_actions
    assert "`SampleProj` SP-HC-01 官方巡检尚未完成真实定时运行验收" in next_actions


def test_health_check_alert_ledger_resolves_after_two_clean_runs(sample_env, monkeypatch) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)
    monkeypatch.setattr(workspace_hub_health_check.codex_memory, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(workspace_hub_health_check, "trigger_dashboard_rebuild", lambda: None)

    failing = {
        "checked_at": "2026-03-11T13:00:00+08:00",
        "watcher": {"installed": False, "loaded": False},
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
    healthy = {
        **failing,
        "checked_at": "2026-03-11T13:10:00+08:00",
        "watcher": {"installed": True, "loaded": True},
    }

    workspace_hub_health_check.run_health_check(checks=failing, trigger_source="manual_cli")
    workspace_hub_health_check.run_health_check(checks=healthy, trigger_source="manual_cli")
    workspace_hub_health_check.run_health_check(
        checks={**healthy, "checked_at": "2026-03-11T13:20:00+08:00"},
        trigger_source="manual_cli",
    )

    latest_states = workspace_hub_health_check.load_latest_alert_states()
    watcher_alert = latest_states["health.watcher.launchagent"]
    assert watcher_alert["status"] == "resolved"
    assert watcher_alert["confirmation_passes"] == 2
    assert watcher_alert["occurrence_count"] == 1

    ledger_lines = (
        sample_env["reports_root"] / "ops" / "workspace-hub-health" / "alerts.ndjson"
    ).read_text(encoding="utf-8").splitlines()
    statuses = [json.loads(line)["status"] for line in ledger_lines if line.strip()]
    assert "open" in statuses
    assert "resolved_pending_confirmation" in statuses
    assert "resolved" in statuses


def test_health_check_evaluate_checks_flags_bridge_continuity(sample_env) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)

    checks = {
        "checked_at": "2026-03-11T13:00:00+08:00",
        "watcher": {"installed": True, "loaded": True},
        "dashboard_sync": {"installed": True, "loaded": True, "pending_events": 0},
        "consistency_pre_refresh": {"ok": True, "issues": [], "issue_count": 0, "exit_code": 0},
        "refresh_index": {"changed": True, "exit_code": 0},
        "rebuild_all": {"status": "ok", "exit_code": 0},
        "consistency": {"ok": True, "issues": [], "issue_count": 0, "exit_code": 0},
        "routing": {"ok": True, "case_count": 1, "results": []},
        "bridge_continuity": {
            "ok": False,
            "issue_count": 2,
            "shared_session_count": 1,
            "response_delayed_count": 1,
            "progress_stalled_count": 0,
            "issues": [
                {"issue_type": "shared_session_across_chats", "summary": "session drift"},
                {"issue_type": "response_delayed", "summary": "response drift"},
            ],
        },
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
        "catchup_status": {"should_run": False, "reason": "fresh", "due_at": "", "overdue_seconds": 0},
        "run_context": {"trigger_source": "manual_cli"},
    }

    result = workspace_hub_health_check.evaluate_checks(checks)

    assert result["ok"] is False
    alert = next(item for item in result["alerts"] if item["alert_key"] == "health.bridge.continuity")
    assert alert["severity"] == "warning"
    assert "shared_session=1" in alert["current_summary"]
    row = next(item for item in result["rows"] if item["ID"] == "WH-HC-07")
    assert row["状态"] == "doing"
    assert "response_delayed=1" in row["阻塞/依赖"]


def test_health_check_catchup_executes_for_stale_window(monkeypatch) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)
    now = dt.datetime(2026, 3, 11, 13, 0, tzinfo=dt.timezone.utc)
    scheduler_status = {
        "configured": True,
        "active": True,
        "cwd_matches": True,
        "last_run_at": "",
        "next_run_at": "2026-03-11T08:00:00+00:00",
    }
    captured: dict[str, str] = {}

    monkeypatch.setattr(workspace_hub_health_check, "load_official_scheduler_status", lambda: scheduler_status)
    monkeypatch.setattr(workspace_hub_health_check, "latest_run_record", lambda: {})

    def fake_run_health_check(**kwargs):
        captured.update({key: str(value) for key, value in kwargs.items()})
        return {
            "ok": True,
            "run_record": {
                "run_id": "whc-test-catchup",
                "issue_count": 0,
            },
        }

    monkeypatch.setattr(workspace_hub_health_check, "run_health_check", fake_run_health_check)

    payload = workspace_hub_health_check.run_catchup_if_stale(now=now)

    assert payload["executed"] is True
    assert payload["decision"]["reason"] == "stale_after_sleep_or_missed_window"
    assert captured["trigger_source"] == "wake_catchup"
    assert captured["scheduler_id"] == workspace_hub_health_check.OFFICIAL_SCHEDULER_ID
    assert captured["scheduled_for"] == "2026-03-11T08:00:00+00:00"


def test_health_check_catchup_skips_when_recent_run_exists(monkeypatch) -> None:
    from ops import codex_memory as codex_memory_module
    from ops import workspace_hub_health_check as health_module

    importlib.reload(codex_memory_module)
    workspace_hub_health_check = importlib.reload(health_module)
    now = dt.datetime(2026, 3, 11, 13, 0, tzinfo=dt.timezone.utc)
    scheduler_status = {
        "configured": True,
        "active": True,
        "cwd_matches": True,
        "last_run_at": "",
        "next_run_at": "2026-03-11T08:00:00+00:00",
    }

    monkeypatch.setattr(workspace_hub_health_check, "load_official_scheduler_status", lambda: scheduler_status)
    monkeypatch.setattr(
        workspace_hub_health_check,
        "latest_run_record",
        lambda: {"finished_at": "2026-03-11T12:45:00+00:00"},
    )

    def fail_run_health_check(**_kwargs):
        raise AssertionError("catch-up should not run when the latest health check is still fresh")

    monkeypatch.setattr(workspace_hub_health_check, "run_health_check", fail_run_health_check)

    payload = workspace_hub_health_check.run_catchup_if_stale(now=now)

    assert payload["executed"] is False
    assert payload["decision"]["reason"] == "fresh"


def test_session_watcher_scan_reports_health_catchup(sample_env, monkeypatch) -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    monkeypatch.setattr(watcher, "auto_discover_projects", lambda: False)
    monkeypatch.setattr(watcher, "load_recent_session_files", lambda days=14: [])
    monkeypatch.setattr(
        watcher,
        "maybe_run_health_check_catchup",
        lambda: {"executed": True, "reason": "stale_after_sleep_or_missed_window", "run_id": "whc-catchup"},
    )

    payload = watcher.scan_once()

    assert payload["processed"] == 0
    assert payload["health_check"]["executed"] is True
    assert payload["health_check"]["run_id"] == "whc-catchup"


def test_session_watcher_parse_snapshot_tracks_incomplete_last_activity(tmp_path) -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    session_path = tmp_path / "session.jsonl"
    session_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "timestamp": "2026-03-14T03:00:00Z",
                        "type": "session_meta",
                        "payload": {"id": "sess-live", "timestamp": "2026-03-14T03:00:00Z", "cwd": "/tmp/workspace"},
                    }
                ),
                json.dumps(
                    {
                        "timestamp": "2026-03-14T03:00:10Z",
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "继续跑"},
                    }
                ),
                json.dumps({"timestamp": "2026-03-14T03:20:00Z", "type": "response_item", "payload": {}}),
            ]
        ),
        encoding="utf-8",
    )

    snapshot = watcher.parse_session_snapshot(session_path)

    assert snapshot is not None
    assert snapshot["completed"] is False
    assert snapshot["last_active_at"] == "2026-03-14T03:20:00Z"


def test_session_watcher_parse_snapshot_supports_schema_driven_mapping(tmp_path) -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    session_path = tmp_path / "session-schema.jsonl"
    session_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "ts": "2026-03-18T01:00:00Z",
                        "kind": "meta",
                        "meta": {
                            "session_id": "sess-schema",
                            "started_at": "2026-03-18T01:00:00Z",
                            "cwd": "/tmp/schema-workspace",
                            "launch": {
                                "project_name": "SampleProj",
                                "binding_scope": "topic",
                                "binding_board_path": "/tmp/SampleProj-Feishu Bridge-跟进板.md",
                                "topic_name": "Feishu Bridge",
                                "rollup_target": "/tmp/SampleProj-项目板.md",
                                "launch_source": "feishu",
                                "source_chat_ref": "oc_schema_chat",
                                "source_thread_name": "Codex Hub",
                                "execution_profile": "feishu",
                                "model": "gpt-5.4",
                                "reasoning_effort": "high",
                            },
                        },
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-03-18T01:00:10Z",
                        "kind": "prompt",
                        "content": {"text": "继续推进 Feishu Bridge"},
                    }
                ),
                json.dumps(
                    {
                        "ts": "2026-03-18T01:05:00Z",
                        "kind": "done",
                        "summary": {"assistant": "已固化 schema parser"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    snapshot = watcher.parse_session_snapshot(
        session_path,
        schema={
            "timestamp_paths": ["ts"],
            "shared_fields": {
                "project_name": ["meta.launch.project_name"],
                "binding_scope": ["meta.launch.binding_scope"],
                "binding_board_path": ["meta.launch.binding_board_path"],
                "topic_name": ["meta.launch.topic_name"],
                "rollup_target": ["meta.launch.rollup_target"],
                "launch_source": ["meta.launch.launch_source"],
                "source_chat_ref": ["meta.launch.source_chat_ref"],
                "source_thread_name": ["meta.launch.source_thread_name"],
                "execution_profile": ["meta.launch.execution_profile"],
                "model": ["meta.launch.model"],
                "reasoning_effort": ["meta.launch.reasoning_effort"],
            },
            "rules": [
                {
                    "match": {"kind": "meta"},
                    "fields": {
                        "id": ["meta.session_id"],
                        "started_at": ["meta.started_at"],
                        "cwd": ["meta.cwd"],
                    },
                },
                {
                    "match": {"kind": "prompt"},
                    "fields": {
                        "user_message": ["content.text"],
                    },
                },
                {
                    "match": {"kind": "done"},
                    "fields": {
                        "last_agent_message": ["summary.assistant"],
                        "last_active_at": ["ts"],
                    },
                    "set": {"completed": True},
                },
            ],
        },
    )

    assert snapshot is not None
    assert snapshot["id"] == "sess-schema"
    assert snapshot["project_name"] == "SampleProj"
    assert snapshot["binding_scope"] == "topic"
    assert snapshot["topic_name"] == "Feishu Bridge"
    assert snapshot["launch_source"] == "feishu"
    assert snapshot["source_chat_ref"] == "oc_schema_chat"
    assert snapshot["execution_profile"] == "feishu"
    assert snapshot["model"] == "gpt-5.4"
    assert snapshot["reasoning_effort"] == "high"
    assert snapshot["thread_name"] == "Codex Hub"
    assert snapshot["completed"] is True
    assert snapshot["last_agent_message"] == "已固化 schema parser"
    assert snapshot["last_active_at"] == "2026-03-18T01:05:00Z"


def test_session_watcher_idle_monitor_notifies_once_and_resets_after_activity(sample_env, monkeypatch) -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    delivered: list[dict[str, str]] = []
    monkeypatch.setattr(
        watcher,
        "send_local_notification",
        lambda **kwargs: delivered.append(kwargs) or {"ok": True, "returncode": 0, "stdout": "", "stderr": ""},
    )
    state = {
        "version": 1,
        "updated_at": None,
        "sessions": {
            "sess-idle": {
                "path": str(sample_env["workspace_root"] / "sess-idle.jsonl"),
            }
        },
        "idle_monitors": {
            "sess-idle": {
                "session_id": "sess-idle",
                "label": "Codex Hub thread",
                "idle_seconds": 1800,
                "notify_on_complete": True,
                "created_at": "2026-03-14T03:00:00Z",
            }
        },
    }
    idle_snapshot = {
        "id": "sess-idle",
        "path": str(sample_env["workspace_root"] / "sess-idle.jsonl"),
        "user_message": "监控这个线程",
        "last_active_at": "2026-03-14T03:00:00Z",
        "completed": False,
    }

    first = watcher.evaluate_idle_monitors(
        state,
        {"sess-idle": idle_snapshot},
        now=dt.datetime(2026, 3, 14, 3, 31, tzinfo=dt.timezone.utc),
    )
    second = watcher.evaluate_idle_monitors(
        state,
        {"sess-idle": idle_snapshot},
        now=dt.datetime(2026, 3, 14, 3, 36, tzinfo=dt.timezone.utc),
    )

    active_snapshot = {
        **idle_snapshot,
        "last_active_at": "2026-03-14T03:40:00Z",
    }
    watcher.evaluate_idle_monitors(
        state,
        {"sess-idle": active_snapshot},
        now=dt.datetime(2026, 3, 14, 3, 45, tzinfo=dt.timezone.utc),
    )
    third = watcher.evaluate_idle_monitors(
        state,
        {"sess-idle": active_snapshot},
        now=dt.datetime(2026, 3, 14, 4, 11, tzinfo=dt.timezone.utc),
    )

    assert len(first) == 1
    assert first[0]["reason"] == "inactive"
    assert second == []
    assert len(third) == 1
    assert state["idle_monitors"]["sess-idle"]["notification_count"] == 2
    assert len(delivered) == 2


def test_session_watcher_idle_monitor_notifies_on_completion(sample_env, monkeypatch) -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    delivered: list[dict[str, str]] = []
    monkeypatch.setattr(
        watcher,
        "send_local_notification",
        lambda **kwargs: delivered.append(kwargs) or {"ok": True, "returncode": 0, "stdout": "", "stderr": ""},
    )
    state = {
        "version": 1,
        "updated_at": None,
        "sessions": {"sess-complete": {}},
        "idle_monitors": {
            "sess-complete": {
                "session_id": "sess-complete",
                "label": "Codex Hub thread",
                "idle_seconds": 1800,
                "notify_on_complete": True,
                "created_at": "2026-03-14T03:00:00Z",
            }
        },
    }
    completed_snapshot = {
        "id": "sess-complete",
        "path": str(sample_env["workspace_root"] / "sess-complete.jsonl"),
        "user_message": "监控这个线程",
        "last_active_at": "2026-03-14T03:05:00Z",
        "completed": True,
    }

    notifications = watcher.evaluate_idle_monitors(
        state,
        {"sess-complete": completed_snapshot},
        now=dt.datetime(2026, 3, 14, 3, 6, tzinfo=dt.timezone.utc),
    )

    assert len(notifications) == 1
    assert notifications[0]["reason"] == "completed"
    assert state["idle_monitors"]["sess-complete"]["notification_count"] == 1
    assert len(delivered) == 1


def test_session_watcher_parser_uses_single_daemon_entry() -> None:
    import ops.codex_session_watcher as watcher_module

    watcher = importlib.reload(watcher_module)
    parser = watcher.build_parser()
    subparsers_action = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )

    assert "daemon" in subparsers_action.choices
    assert "monitor-daemon" not in subparsers_action.choices

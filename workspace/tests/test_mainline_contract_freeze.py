from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path


def reload_modules():
    from ops import codex_dashboard_sync, codex_memory, codex_session_watcher, coordination_plane, local_broker, review_plane, runtime_state

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    codex_memory = importlib.reload(codex_memory)
    review_plane = importlib.reload(review_plane)
    coordination_plane = importlib.reload(coordination_plane)
    runtime_state = importlib.reload(runtime_state)
    local_broker = importlib.reload(local_broker)
    codex_session_watcher = importlib.reload(codex_session_watcher)
    return codex_dashboard_sync, codex_memory, review_plane, coordination_plane, runtime_state, local_broker, codex_session_watcher


def write_topic_board(codex_memory, *, project_name: str, topic_name: str, topic_key: str, rows: list[dict[str, str]]) -> Path:
    path = codex_memory.WORKING_ROOT / f"{project_name}-{topic_name}-跟进板.md"
    frontmatter = codex_memory.render_frontmatter(
        {
            "board_type": "topic",
            "project_name": project_name,
            "topic_name": topic_name,
            "topic_key": topic_key,
            "rollup_target": str(codex_memory.project_board_path(project_name)),
            "updated_at": "2026-03-12",
            "purpose": f"{topic_name} topic board",
        }
    )
    body = (
        f"{frontmatter}\n\n"
        f"# {topic_name}\n\n"
        "## 任务主表\n\n"
        f"{codex_memory.AUTO_TASK_TABLE_MARKERS[0]}\n"
        + "\n".join(codex_memory.markdown_table_lines(codex_memory.TOPIC_BOARD_HEADERS, rows))
        + f"\n{codex_memory.AUTO_TASK_TABLE_MARKERS[1]}\n"
    )
    codex_memory.write_text(path, body)
    return path


def seed_console_state(codex_memory, review_plane, coordination_plane) -> None:
    board = codex_memory.load_project_board("SampleProj")
    project_rows = [
        {
            "ID": "SP-1",
            "父ID": "SP-1",
            "来源": "project",
            "范围": "共享底座",
            "事项": "冻结 broker contract",
            "状态": "doing",
            "交付物": "",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "补齐 watcher worktree route",
            "更新时间": "2026-03-12T00:00:00Z",
            "指向": str(board["path"]),
        }
    ]
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], project_rows, [])

    write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="需求",
        topic_key="demand",
        rows=[
            {
                "ID": "TP-1",
                "模块": "需求",
                "事项": "输出需求说明",
                "状态": "done",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "提交审核",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "TP-1",
            }
        ],
    )
    codex_memory.refresh_project_rollups("SampleProj")
    review_plane.submit_review("SampleProj", "TP-1", deliverable_ref="/tmp/output.md", reviewer="Frank")
    coordination_plane.create_coordination(
        coordination_id="CO-1",
        from_project="SampleProj",
        to_project="OtherProj",
        source_ref="/tmp/ref.md",
        requested_action="请复核统一契约",
        assignee="Alex",
        due_at="2026-03-20",
    )


def read_payload(capsys) -> dict:
    return json.loads(capsys.readouterr().out.strip())


def parser_command_names(parser: argparse.ArgumentParser) -> set[str]:
    action = next(item for item in parser._actions if isinstance(item, argparse._SubParsersAction))
    return set(action.choices.keys())


def test_runtime_root_prefers_canonical_runtime_for_worktree(monkeypatch) -> None:
    monkeypatch.delenv("WORKSPACE_HUB_RUNTIME_ROOT", raising=False)
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", "/tmp/workspace-hub-worktrees/core")

    from ops import codex_retrieval, control_gate

    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, _local_broker, _watcher = reload_modules()
    codex_retrieval = importlib.reload(codex_retrieval)
    control_gate = importlib.reload(control_gate)

    expected = Path(__file__).resolve().parents[1] / "runtime"
    assert runtime_state.runtime_root() == expected
    assert control_gate.runtime_root() == expected
    assert codex_retrieval.runtime_root() == expected


def test_feishu_writable_roots_include_canonical_workspace_and_support_worktrees(monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", "/tmp/workspace-hub-worktrees/core")
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    original_exists = local_broker.Path.exists
    expected_paths = {
        "/tmp/workspace-hub-worktrees/core",
        "/tmp/workspace-hub-worktrees/feishu-bridge",
        "/tmp/workspace-hub-worktrees/electron-console",
    }

    def fake_exists(path: Path) -> bool:
        if str(path) in expected_paths:
            return True
        return original_exists(path)

    monkeypatch.setattr(local_broker.Path, "exists", fake_exists)

    roots = {str(path) for path in local_broker._feishu_writable_roots()}

    canonical_workspace = str(Path(__file__).resolve().parents[1])
    assert canonical_workspace in roots
    assert f"{canonical_workspace}/projects" in roots
    assert "/tmp/workspace-hub-worktrees/core" in roots
    assert "/tmp/workspace-hub-worktrees/feishu-bridge" in roots
    assert "/tmp/workspace-hub-worktrees/electron-console" in roots


def test_authorized_profiles_escalate_to_full_access(monkeypatch, tmp_path) -> None:
    canonical_workspace = tmp_path / "workspace-hub"
    (canonical_workspace / "projects").mkdir(parents=True)
    worktree_root = tmp_path / "workspace-hub-worktrees" / "core"
    worktree_root.mkdir(parents=True)
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(worktree_root))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    monkeypatch.setattr(local_broker, "_codex_command_prefix", lambda: ["/opt/homebrew/bin/node", "/tmp/codex"])

    standard = local_broker._codex_exec_command(prompt="status", execution_profile="feishu")
    approved = local_broker._codex_exec_command(prompt="git push", execution_profile="feishu-approved")
    local_system = local_broker._codex_exec_command(
        prompt="install launch agent",
        execution_profile="feishu-local-system-approved",
    )
    electron_full = local_broker._codex_exec_command(prompt="继续 Electron 会话", execution_profile="electron-full-access")

    assert 'approval_policy="never"' in standard
    assert "sandbox_workspace_write.network_access=true" in standard
    assert "sandbox_workspace_write.network_access=false" not in standard
    assert "--add-dir" in standard
    assert "--sandbox" in approved and "danger-full-access" in approved
    assert "sandbox_workspace_write.network_access=true" not in approved
    assert "--add-dir" not in approved
    assert "--sandbox" in local_system and "workspace-write" in local_system
    assert "danger-full-access" not in local_system
    assert any(str(Path("/Applications")) == part for part in local_system)
    assert "--sandbox" in electron_full and "danger-full-access" in electron_full
    assert 'approval_policy="never"' in electron_full


def test_feishu_local_extend_profile_adds_codex_home_roots(monkeypatch, tmp_path) -> None:
    canonical_workspace = tmp_path / "workspace-hub"
    (canonical_workspace / "projects").mkdir(parents=True)
    worktree_root = tmp_path / "workspace-hub-worktrees" / "core-v1-0-3-to-v1-0-5"
    worktree_root.mkdir(parents=True)
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(worktree_root))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    monkeypatch.setattr(local_broker, "_codex_command_prefix", lambda: ["/opt/homebrew/bin/node", "/tmp/codex"])

    command = local_broker._codex_exec_command(
        prompt="install skill",
        execution_profile="feishu-local-extend",
    )

    command_str = " ".join(command)
    codex_home = tmp_path / "home" / ".codex"
    assert "--sandbox" in command and "workspace-write" in command
    assert 'approval_policy="never"' in command_str
    assert str(codex_home) in command_str
    assert str(codex_home / "skills") in command_str
    assert str(codex_home / "agents") in command_str
    assert (codex_home / "skills").exists()
    assert (codex_home / "agents").exists()


def test_validate_execution_profile_access_requires_approved_token(sample_env) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()

    payload = local_broker._validate_execution_profile_access(
        "codex_exec",
        execution_profile="feishu-approved",
        approval_token="",
        source="feishu",
    )

    assert payload is not None
    assert payload["ok"] is False
    assert payload["error"] == "approval_token_required"
    assert payload["expected_scope"] == "feishu_high_risk_execution"


def test_validate_execution_profile_access_rejects_scope_mismatch(sample_env) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    runtime_state.upsert_approval_token(
        token="coco-scope-mismatch",
        scope="feishu_local_system_execution",
        status="approved",
        metadata={"approved_execution_profile": "feishu-local-system-approved"},
    )

    payload = local_broker._validate_execution_profile_access(
        "codex_exec",
        execution_profile="feishu-approved",
        approval_token="coco-scope-mismatch",
        source="feishu",
    )

    assert payload is not None
    assert payload["ok"] is False
    assert payload["error"] == "approval_scope_mismatch"


def test_validate_execution_profile_access_requires_electron_source(sample_env) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()

    payload = local_broker._validate_execution_profile_access(
        "codex_exec",
        execution_profile="electron-full-access",
        approval_token="",
        source="feishu",
    )

    assert payload is not None
    assert payload["ok"] is False
    assert payload["error"] == "electron_full_access_requires_electron_source"


def test_local_broker_freezes_status_panel_and_command_contract(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, review_plane, coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    seed_console_state(_codex_memory, review_plane, coordination_plane)
    runtime_state.upsert_bridge_settings(
        "feishu",
        {
            "app_id": "cli_a",
            "app_secret": "secret",
            "domain": "feishu",
            "allowed_users": ["alice"],
            "group_policy": "mentions_only",
            "require_mention": True,
        },
    )
    _codex_memory.save_user_profile(
        preferred_name="Frank",
        alternate_names=["吉祥"],
        relationship="workspace owner",
        note="Use Frank when talking to the primary workspace user.",
    )
    runtime_state.upsert_bridge_connection(
        "feishu",
        status="connected",
        host_mode="electron",
        transport="websocket",
        last_event_at="2026-03-13T10:00:00Z",
    )

    monkeypatch.setattr(
        local_broker,
        "_health_snapshot",
        lambda: {
            "open_alert_count": 1,
            "latest_report": "/tmp/health.md",
            "last_entry": {"issue_count": 2},
        },
    )
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )
    monkeypatch.setattr(local_broker, "_codex_command_prefix", lambda: ["/opt/homebrew/bin/node", "/tmp/codex"])

    assert local_broker.cmd_status(argparse.Namespace()) == 0
    status_payload = read_payload(capsys)
    assert status_payload["ok"] is True
    assert status_payload["broker_action"] == "status"
    assert "commands" in status_payload
    assert "bridge-status" in status_payload["commands"]
    assert "bridge-settings" in status_payload["commands"]
    assert "approval-token" in status_payload["commands"]
    assert "approval-tokens" in status_payload["commands"]
    assert "material-inspect" in status_payload["commands"]
    assert "material-suggest" in status_payload["commands"]
    assert "feishu-op" in status_payload["commands"]

    assert local_broker.cmd_panel(argparse.Namespace(name="overview", project_name="")) == 0
    overview_payload = read_payload(capsys)
    assert overview_payload["ok"] is True
    assert overview_payload["broker_action"] == "panel"
    assert overview_payload["panel_name"] == "overview"
    assert overview_payload["cards"]
    overview_cards = {item["label"]: item["value"] for item in overview_payload["cards"]}
    assert overview_cards["Threads Needing Attention"] == "0"


def test_local_broker_status_contract_matches_parser(sample_env, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()

    assert local_broker.cmd_status(argparse.Namespace()) == 0
    payload = read_payload(capsys)

    assert payload["ok"] is True
    assert set(payload["commands"]) == parser_command_names(local_broker.build_parser())
    assert payload["capabilities"]["bridge_message_detail"] is True


def test_local_broker_material_suggest_returns_route_payload(sample_env, capsys) -> None:
    from ops import codex_retrieval

    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    codex_retrieval = importlib.reload(codex_retrieval)
    codex_retrieval.build_index()
    assert local_broker.cmd_material_suggest(
        argparse.Namespace(project_name="SampleProj", prompt="Project Document Marker")
    ) == 0
    payload = read_payload(capsys)
    assert payload["ok"] is True
    assert payload["broker_action"] == "material_suggest"
    assert payload["project_name"] == "SampleProj"
    grouped_hits = payload["material_hits"] + payload["report_hits"] + payload["deliverable_hits"] + payload["hotset_hits"]
    assert any(item["path"].endswith("guide.md") for item in grouped_hits)
    assert payload["hotset_hits"][0]["is_hotset"] is True
    assert payload["retrieval_protocol"]["name"] == "search-timeline-detail"
    assert payload["retrieval_protocol"]["steps"] == ["search", "timeline", "detail"]
    assert payload["retrieval_protocol"]["next_step"] in {"timeline", "detail"}
    assert payload["retrieval_protocol"]["timeline_candidate_count"] >= 1


def test_bridge_status_marks_event_stall_as_stale(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    monkeypatch.setattr(
        local_broker.runtime_state,
        "fetch_bridge_connection",
        lambda bridge: {
            "status": "connected",
            "host_mode": "electron",
            "transport": "sdk_websocket_plus_rest",
            "last_error": "",
            "last_event_at": "2026-03-14T00:00:00Z",
            "updated_at": "2026-03-15T00:00:00Z",
            "metadata": {
                "heartbeat_at": "2026-03-15T00:00:00Z",
                "stale_after_seconds": 90,
                "event_idle_after_seconds": 300,
            },
        },
    )
    monkeypatch.setattr(local_broker, "_bridge_settings_summary", lambda bridge: {"has_app_credentials": True})

    payload = local_broker._bridge_status_snapshot("feishu")

    assert payload["connection_status"] == "stale"
    assert payload["stale"] is True
    assert payload["event_stalled"] is True
    assert payload["event_idle_after_seconds"] == 300

    _dashboard_sync, _codex_memory, review_plane, coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    seed_console_state(_codex_memory, review_plane, coordination_plane)
    runtime_state.upsert_bridge_settings(
        "feishu",
        {
            "app_id": "cli_a",
            "app_secret": "secret",
            "domain": "feishu",
            "allowed_users": ["alice"],
            "group_policy": "mentions_only",
            "require_mention": True,
        },
    )
    _codex_memory.save_user_profile(
        preferred_name="Frank",
        alternate_names=["吉祥"],
        relationship="workspace owner",
        note="Use Frank when talking to the primary workspace user.",
    )
    runtime_state.upsert_bridge_connection(
        "feishu",
        status="connected",
        host_mode="electron",
        transport="websocket",
        last_event_at="2026-03-13T10:00:00Z",
    )
    monkeypatch.setattr(
        local_broker,
        "_health_snapshot",
        lambda: {
            "open_alert_count": 1,
            "latest_report": "/tmp/health.md",
            "last_entry": {"issue_count": 2},
        },
    )
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )
    monkeypatch.setattr(local_broker, "_codex_command_prefix", lambda: ["/opt/homebrew/bin/node", "/tmp/codex"])

    assert local_broker.cmd_panel(argparse.Namespace(name="projects", project_name="SampleProj")) == 0
    projects_payload = read_payload(capsys)
    assert projects_payload["ok"] is True
    assert projects_payload["panel_name"] == "projects"
    assert projects_payload["rows"][0]["project_name"] == "SampleProj"

    assert local_broker.cmd_panel(argparse.Namespace(name="review", project_name="SampleProj")) == 0
    review_payload = read_payload(capsys)
    assert review_payload["ok"] is True
    assert review_payload["panel_name"] == "review"
    assert review_payload["rows"][0]["task_id"] == "TP-1"

    assert local_broker.cmd_panel(argparse.Namespace(name="coordination", project_name="SampleProj")) == 0
    coordination_payload = read_payload(capsys)
    assert coordination_payload["ok"] is True
    assert coordination_payload["panel_name"] == "coordination"
    assert coordination_payload["rows"][0]["coordination_id"] == "CO-1"

    assert local_broker.cmd_panel(argparse.Namespace(name="health", project_name="")) == 0
    health_payload = read_payload(capsys)
    assert health_payload["ok"] is True
    assert health_payload["panel_name"] == "health"
    assert health_payload["rows"][0]["report_path"] == "/tmp/health.md"

    assert local_broker.cmd_bridge_status(argparse.Namespace(bridge="feishu")) == 0
    bridge_status_payload = read_payload(capsys)
    assert bridge_status_payload["ok"] is True
    assert bridge_status_payload["broker_action"] == "bridge_status"
    assert bridge_status_payload["connection_status"] == "connected"
    assert bridge_status_payload["settings_summary"]["has_app_credentials"] is True
    assert bridge_status_payload["stale"] is False

    assert local_broker.cmd_bridge_settings(argparse.Namespace(bridge="feishu", settings_json="")) == 0
    bridge_settings_payload = read_payload(capsys)
    assert bridge_settings_payload["ok"] is True
    assert bridge_settings_payload["broker_action"] == "bridge_settings"
    assert bridge_settings_payload["settings"]["domain"] == "feishu"

    assert (
        local_broker.cmd_bridge_settings(
            argparse.Namespace(bridge="feishu", settings_json=json.dumps({"domain": "lark", "require_mention": False}))
        )
        == 0
    )
    bridge_settings_update_payload = read_payload(capsys)
    assert bridge_settings_update_payload["ok"] is True
    assert bridge_settings_update_payload["updated"] is True
    assert bridge_settings_update_payload["settings"]["domain"] == "lark"

    assert (
        local_broker.cmd_bridge_connection(
            argparse.Namespace(
                bridge="feishu",
                connection_json=json.dumps(
                    {
                        "status": "connected",
                        "host_mode": "electron",
                        "transport": "sdk_websocket_plus_rest",
                        "last_event_at": "2026-03-13T12:00:00Z",
                        "metadata": {"recent_error_count": 0},
                    }
                ),
            )
        )
        == 0
    )
    bridge_connection_update_payload = read_payload(capsys)
    assert bridge_connection_update_payload["ok"] is True
    assert bridge_connection_update_payload["updated"] is True
    assert bridge_connection_update_payload["status"] == "connected"
    assert bridge_connection_update_payload["host_mode"] == "electron"

    assert local_broker.cmd_bridge_connection(argparse.Namespace(bridge="feishu", connection_json="")) == 0
    bridge_connection_payload = read_payload(capsys)
    assert bridge_connection_payload["ok"] is True
    assert bridge_connection_payload["status"] == "connected"

    assert (
        local_broker.cmd_bridge_connection(
            argparse.Namespace(
                bridge="feishu",
                connection_json=json.dumps(
                    {
                        "status": "connected",
                        "host_mode": "launchagent",
                        "transport": "sdk_websocket_plus_rest",
                        "last_event_at": "2026-03-13T12:00:00Z",
                        "metadata": {
                            "heartbeat_at": "2026-03-13T11:55:00Z",
                            "stale_after_seconds": 10,
                            "recent_error_count": 0,
                        },
                    }
                ),
            )
        )
        == 0
    )
    stale_connection_payload = read_payload(capsys)
    assert stale_connection_payload["ok"] is True

    assert local_broker.cmd_bridge_status(argparse.Namespace(bridge="feishu")) == 0
    stale_bridge_status_payload = read_payload(capsys)
    assert stale_bridge_status_payload["ok"] is True
    assert stale_bridge_status_payload["connection_status"] == "stale"
    assert stale_bridge_status_payload["stale"] is True
    assert stale_bridge_status_payload["heartbeat_at"] == "2026-03-13T11:55:00Z"
    assert stale_bridge_status_payload["stale_after_seconds"] == 10

    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="inbound",
        message_id="msg-in-1",
        status="received",
        session_id="sess-bridge-1",
        payload={
            "chat_id": "chat_123",
            "chat_type": "p2p",
            "open_id": "ou_user",
            "text": "当前系统状态",
            "phase": "",
        },
    )
    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="inbound",
        message_id="msg-in-2",
        status="received",
        session_id="sess-bridge-2",
        payload={
            "chat_id": "chat_456",
            "chat_type": "group",
            "open_id": "ou_runner",
            "text": "继续执行最新任务",
            "phase": "",
        },
    )
    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="outbound",
        message_id="msg-out-1",
        status="sent",
        session_id="sess-bridge-1",
        payload={
            "chat_id": "chat_123",
            "reply_target": "chat_123",
            "reply_target_type": "chat_id",
            "text": "当前系统状态：一切正常",
            "source_message_id": "msg-in-1",
            "phase": "reply",
        },
    )
    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="outbound",
        message_id="msg-out-2",
        status="sent",
        session_id="sess-bridge-2",
        payload={
            "chat_id": "chat_456",
            "reply_target": "chat_456",
            "reply_target_type": "chat_id",
            "text": "我先开始处理，完成后再汇报。",
            "source_message_id": "msg-in-2",
            "phase": "ack",
        },
    )

    assert local_broker.cmd_bridge_conversations(argparse.Namespace(bridge="feishu", limit=20)) == 0
    bridge_conversations_payload = read_payload(capsys)
    assert bridge_conversations_payload["ok"] is True
    assert bridge_conversations_payload["broker_action"] == "bridge_conversations"
    conversations_by_chat = {row["chat_ref"]: row for row in bridge_conversations_payload["rows"]}
    assert conversations_by_chat["chat_123"]["message_count"] == 2
    assert conversations_by_chat["chat_123"]["last_delivery_phase"] == "reply"
    assert conversations_by_chat["chat_123"]["execution_state"] == "reported"
    assert conversations_by_chat["chat_123"]["last_user_request"] == "当前系统状态"
    assert conversations_by_chat["chat_123"]["last_report"] == "当前系统状态：一切正常"
    assert conversations_by_chat["chat_123"]["binding_required"] is False
    assert conversations_by_chat["chat_123"]["pending_request"] is False
    assert conversations_by_chat["chat_123"]["needs_attention"] is False
    assert conversations_by_chat["chat_123"]["binding_label"] == "unbound"
    assert conversations_by_chat["chat_123"]["thread_label"] == "chat_123"
    assert conversations_by_chat["chat_456"]["execution_state"] == "running"
    assert conversations_by_chat["chat_456"]["last_user_request"] == "继续执行最新任务"
    assert conversations_by_chat["chat_456"]["last_report"] == "我先开始处理，完成后再汇报。"
    assert conversations_by_chat["chat_456"]["binding_required"] is True
    assert conversations_by_chat["chat_456"]["pending_request"] is True
    assert conversations_by_chat["chat_456"]["ack_pending"] is True
    assert conversations_by_chat["chat_456"]["needs_attention"] is True
    assert conversations_by_chat["chat_456"]["attention_reason"] == "binding_required"
    assert conversations_by_chat["chat_456"]["last_user_request_age_seconds"] is not None

    assert local_broker.cmd_bridge_messages(argparse.Namespace(bridge="feishu", chat_ref="chat_123", limit=20)) == 0
    bridge_messages_payload = read_payload(capsys)
    assert bridge_messages_payload["ok"] is True
    assert bridge_messages_payload["broker_action"] == "bridge_messages"
    assert {row["direction"] for row in bridge_messages_payload["rows"]} == {"outbound", "inbound"}
    assert {row["chat_ref"] for row in bridge_messages_payload["rows"]} == {"chat_123"}

    assert local_broker.cmd_panel(argparse.Namespace(name="bridge-conversations", project_name="")) == 0
    bridge_panel_payload = read_payload(capsys)
    assert bridge_panel_payload["ok"] is True
    assert bridge_panel_payload["panel_name"] == "bridge-conversations"
    bridge_panel_by_chat = {row["chat_ref"]: row for row in bridge_panel_payload["rows"]}
    assert bridge_panel_by_chat["chat_123"]["execution_state"] == "reported"
    assert bridge_panel_by_chat["chat_456"]["execution_state"] == "running"
    assert bridge_panel_by_chat["chat_456"]["needs_attention"] is True
    assert bridge_panel_by_chat["chat_456"]["attention_reason"] == "binding_required"

    assert local_broker.cmd_panel(argparse.Namespace(name="overview", project_name="")) == 0
    overview_with_attention_payload = read_payload(capsys)
    overview_cards = {item["label"]: item["value"] for item in overview_with_attention_payload["cards"]}
    assert overview_cards["Threads Needing Attention"] == "1"

    assert local_broker.cmd_user_profile(argparse.Namespace(profile_json="")) == 0
    user_profile_payload = read_payload(capsys)
    assert user_profile_payload["ok"] is True
    assert user_profile_payload["profile"]["preferred_name"] == "Frank"

    assert (
        local_broker.cmd_user_profile(
            argparse.Namespace(
                profile_json=json.dumps(
                    {
                        "preferred_name": "吉祥",
                        "alternate_names": ["Frank"],
                        "relationship": "workspace owner",
                        "note": "Prefer 吉祥 in future conversations.",
                    }
                )
            )
        )
        == 0
    )
    user_profile_update_payload = read_payload(capsys)
    assert user_profile_update_payload["ok"] is True
    assert user_profile_update_payload["updated"] is True
    assert user_profile_update_payload["profile"]["preferred_name"] == "吉祥"

    assert (
        local_broker.cmd_bridge_chat_binding(
            argparse.Namespace(
                bridge="feishu",
                chat_ref="chat_123",
                binding_json=json.dumps(
                    {
                        "project_name": "SampleProj",
                        "topic_name": "demand",
                        "binding_scope": "topic",
                        "session_id": "sess-bridge-1",
                        "metadata": {"declared_by": "Frank"},
                    }
                ),
            )
        )
        == 0
    )
    binding_update_payload = read_payload(capsys)
    assert binding_update_payload["ok"] is True
    assert binding_update_payload["binding"]["project_name"] == "SampleProj"
    assert binding_update_payload["binding"]["topic_name"] == "需求"

    runtime_state.upsert_approval_token(
        token="appr_chat_123",
        scope="feishu_high_risk_execution",
        status="pending",
        project_name="SampleProj",
        session_id="sess-bridge-1",
        expires_at="2036-03-20T01:00:00Z",
        metadata={
            "requested_action": "git push origin codex/feishu-bridge",
            "chat_id": "chat_123",
            "source_message_id": "msg-in-1",
        },
    )
    runtime_state.upsert_approval_token(
        token="appr_chat_expired",
        scope="feishu_high_risk_execution",
        status="pending",
        project_name="SampleProj",
        session_id="sess-bridge-1",
        expires_at="2020-03-20T01:00:00Z",
        metadata={
            "requested_action": "git push stale token",
            "chat_id": "chat_123",
            "source_message_id": "msg-in-2",
        },
    )

    assert local_broker.cmd_bridge_conversations(argparse.Namespace(bridge="feishu", limit=20)) == 0
    bridge_conversations_with_approval_payload = read_payload(capsys)
    conversations_with_approval = {row["chat_ref"]: row for row in bridge_conversations_with_approval_payload["rows"]}
    assert conversations_with_approval["chat_123"]["approval_pending"] is True
    assert conversations_with_approval["chat_123"]["pending_approval_count"] == 1
    assert conversations_with_approval["chat_123"]["pending_approval_token"] == "appr_chat_123"
    assert conversations_with_approval["chat_123"]["pending_approval_action"] == "git push origin codex/feishu-bridge"
    assert conversations_with_approval["chat_123"]["attention_reason"] == "approval_pending"

    assert (
        local_broker.cmd_bridge_chat_binding(
            argparse.Namespace(
                bridge="feishu",
                chat_ref="chat_123",
                binding_json=json.dumps(
                    {
                        "project_name": "SampleProj",
                        "topic_name": "bad-topic",
                        "binding_scope": "topic",
                    }
                ),
            )
        )
        == 0
    )
    bad_binding_payload = read_payload(capsys)
    assert bad_binding_payload["ok"] is False
    assert "unknown topic_name" in bad_binding_payload["error"]
    assert bad_binding_payload["available_topics"] == ["需求"]

    assert local_broker.cmd_bridge_bindings(argparse.Namespace(bridge="feishu", limit=20)) == 0
    bindings_payload = read_payload(capsys)
    assert bindings_payload["ok"] is True
    assert bindings_payload["rows"][0]["chat_ref"] == "chat_123"

    assert (
        local_broker.cmd_approval_token(
            argparse.Namespace(
                token="appr_123",
                token_json=json.dumps(
                    {
                        "scope": "high_risk_command",
                        "status": "pending",
                        "project_name": "SampleProj",
                        "session_id": "sess-bridge-2",
                        "expires_at": "2026-03-20T00:00:00Z",
                        "metadata": {"requested_action": "git push"},
                    }
                ),
            )
        )
        == 0
    )
    approval_update_payload = read_payload(capsys)
    assert approval_update_payload["ok"] is True
    assert approval_update_payload["updated"] is True
    assert approval_update_payload["item"]["scope"] == "high_risk_command"
    assert approval_update_payload["item"]["metadata"]["requested_action"] == "git push"

    assert local_broker.cmd_approval_token(argparse.Namespace(token="appr_123", token_json="")) == 0
    approval_payload = read_payload(capsys)
    assert approval_payload["ok"] is True
    assert approval_payload["updated"] is False
    assert approval_payload["item"]["status"] == "pending"

    runtime_state.upsert_approval_token(
        token="appr_456",
        scope="high_risk_command",
        status="approved",
        project_name="SampleProj",
        metadata={"requested_action": "git push"},
    )
    assert (
        local_broker.cmd_approval_tokens(
            argparse.Namespace(status="approved", scope="high_risk_command", limit=20)
        )
        == 0
    )
    approvals_payload = read_payload(capsys)
    assert approvals_payload["ok"] is True
    assert approvals_payload["status_filter"] == "approved"
    assert approvals_payload["scope_filter"] == "high_risk_command"
    assert [row["token"] for row in approvals_payload["rows"]] == ["appr_456"]
    assert local_broker.cmd_panel(argparse.Namespace(name="bridge-conversations", project_name="")) == 0
    bridge_panel_with_binding = read_payload(capsys)
    bridge_panel_with_binding_by_chat = {row["chat_ref"]: row for row in bridge_panel_with_binding["rows"]}
    assert bridge_panel_with_binding_by_chat["chat_123"]["project_name"] == "SampleProj"
    assert bridge_panel_with_binding_by_chat["chat_123"]["topic_name"] == "需求"
    assert bridge_panel_with_binding_by_chat["chat_123"]["binding_label"] == "SampleProj / 需求"
    assert bridge_panel_with_binding_by_chat["chat_123"]["thread_label"] == "SampleProj / 需求"
    assert bridge_panel_with_binding_by_chat["chat_123"]["pending_approval_token"] == "appr_chat_123"

    assert local_broker.cmd_panel(argparse.Namespace(name="overview", project_name="")) == 0
    enriched_overview_payload = read_payload(capsys)
    overview_cards = {item["label"]: item["value"] for item in enriched_overview_payload["cards"]}
    assert overview_cards["CoCo Threads"] == "2"
    assert overview_cards["Bound Threads"] == "1"
    assert overview_cards["Running Threads"] == "1"

    args = argparse.Namespace(action="codex-exec", prompt="echo hi", session_id="", project_name="")
    assert local_broker.cmd_command_center(args) == 0
    command_payload = read_payload(capsys)
    assert command_payload["ok"] is True
    assert command_payload["broker_action"] == "command_center"
    assert command_payload["action"] == "codex-exec"
    assert command_payload["delegated_broker_action"] == "codex_exec"
    assert command_payload["command"][:2] == ["/opt/homebrew/bin/node", "/tmp/codex"]

    resume_args = argparse.Namespace(
        action="codex-resume",
        prompt="Reply with RESUMEOK only.",
        session_id="019ce580-b051-7821-b858-245373d53f5a",
        project_name="",
    )
    assert local_broker.cmd_command_center(resume_args) == 0
    resume_payload = read_payload(capsys)
    assert resume_payload["ok"] is True
    assert resume_payload["action"] == "codex-resume"
    assert resume_payload["delegated_broker_action"] == "codex_resume"
    command = resume_payload["command"]
    assert command[:5] == [
        "/opt/homebrew/bin/node",
        "/tmp/codex",
        "exec",
        "-C",
        str(local_broker.workspace_root()),
    ]
    resume_index = command.index("resume")
    assert command[resume_index:] == [
        "resume",
        "019ce580-b051-7821-b858-245373d53f5a",
        "Reply with RESUMEOK only.",
    ]
    if "--model" in command:
        model_index = command.index("--model")
        assert model_index < resume_index
        assert command[model_index + 1]
    if '-c' in command:
        reasoning_index = command.index('-c')
        assert reasoning_index < resume_index
        assert command[reasoning_index + 1].startswith('model_reasoning_effort=')


def test_feishu_profiles_route_through_start_codex(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    runtime_state.upsert_approval_token(
        token="coco-allow-mainline",
        scope="feishu_high_risk_execution",
        status="approved",
        metadata={"approved_execution_profile": "feishu-approved"},
    )
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续 Codex Hub 的 Feishu 入口",
        session_id="",
        project_name="Codex Hub",
        execution_profile="feishu",
        source="feishu",
        chat_ref="",
        thread_name="",
        thread_label="",
        source_message_id="",
        approval_token="",
        no_auto_resume=False,
        model="",
        reasoning_effort="",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    exec_payload = read_payload(capsys)
    assert exec_payload["ok"] is True
    assert exec_payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu",
        "--project",
        "Codex Hub",
        "--source",
        "feishu",
        "--prompt",
        "继续 Codex Hub 的 Feishu 入口",
    ]

    resume_args = argparse.Namespace(
        action="codex-resume",
        prompt="继续处理上一轮问题",
        session_id="sess-feishu-1",
        project_name="",
        execution_profile="feishu-approved",
        source="feishu",
        chat_ref="",
        thread_name="",
        thread_label="",
        source_message_id="",
        approval_token="coco-allow-mainline",
        no_auto_resume=False,
        model="",
        reasoning_effort="",
    )
    assert local_broker.cmd_command_center(resume_args) == 0
    resume_payload = read_payload(capsys)
    assert resume_payload["ok"] is True
    assert resume_payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu-approved",
        "--approval-token",
        "coco-allow-mainline",
        "--resume-session-id",
        "sess-feishu-1",
        "--source",
        "feishu",
        "--prompt",
        "继续处理上一轮问题",
    ]


def test_feishu_exec_can_disable_project_auto_resume(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续 Codex Hub 的 Feishu 入口",
        session_id="",
        project_name="Codex Hub",
        execution_profile="feishu",
        no_auto_resume=True,
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    exec_payload = read_payload(capsys)
    assert exec_payload["ok"] is True
    assert exec_payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu",
        "--project",
        "Codex Hub",
        "--no-auto-resume",
        "--prompt",
        "继续 Codex Hub 的 Feishu 入口",
    ]


def test_start_codex_payload_exposes_launch_context_and_finalize(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": '\n'.join(
                [
                    'WORKSPACE_HUB_LAUNCH_CONTEXT={"mode":"new","project_name":"Codex Hub","launch_source":"feishu","source_chat_ref":"oc_demo_chat"}',
                    'WORKSPACE_HUB_FINALIZE_LAUNCH={"project_name":"Codex Hub","launch_source":"feishu","source_chat_ref":"oc_demo_chat","status":"completed"}',
                ]
            ),
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续 Codex Hub 项目",
        session_id="",
        project_name="Codex Hub",
        execution_profile="feishu",
        source="feishu",
        chat_ref="oc_demo_chat",
        thread_name="继续 Codex Hub 项目",
        thread_label="CoCo 私聊",
        source_message_id="om_demo_msg",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    payload = read_payload(capsys)
    assert payload["ok"] is True
    assert payload["launch_context"]["launch_source"] == "feishu"
    assert payload["launch_context"]["source_chat_ref"] == "oc_demo_chat"
    assert payload["finalize_launch"]["project_name"] == "Codex Hub"
    assert payload["finalize_launch"]["status"] == "completed"
    assert payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu",
        "--project",
        "Codex Hub",
        "--source",
        "feishu",
        "--chat-ref",
        "oc_demo_chat",
        "--thread-name",
        "继续 Codex Hub 项目",
        "--thread-label",
        "CoCo 私聊",
        "--source-message-id",
        "om_demo_msg",
        "--prompt",
        "继续 Codex Hub 项目",
    ]


def test_start_codex_payload_freezes_model_and_reasoning_contract(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": '\n'.join(
                [
                    'WORKSPACE_HUB_LAUNCH_CONTEXT={"mode":"new","project_name":"Codex Hub","launch_source":"feishu","source_chat_ref":"oc_demo_chat","execution_profile":"feishu","model":"gpt-5.4","reasoning_effort":"high"}',
                    'WORKSPACE_HUB_FINALIZE_LAUNCH={"project_name":"Codex Hub","launch_source":"feishu","source_chat_ref":"oc_demo_chat","status":"completed","model":"gpt-5.4","reasoning_effort":"high"}',
                ]
            ),
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续 Codex Hub 项目",
        session_id="",
        project_name="Codex Hub",
        execution_profile="feishu",
        source="feishu",
        chat_ref="oc_demo_chat",
        thread_name="继续 Codex Hub 项目",
        thread_label="CoCo 私聊",
        source_message_id="om_demo_msg",
        model="gpt-5.4",
        reasoning_effort="high",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    payload = read_payload(capsys)
    assert payload["ok"] is True
    assert payload["launch_context"]["execution_profile"] == "feishu"
    assert payload["launch_context"]["model"] == "gpt-5.4"
    assert payload["launch_context"]["reasoning_effort"] == "high"
    assert payload["finalize_launch"]["model"] == "gpt-5.4"
    assert payload["finalize_launch"]["reasoning_effort"] == "high"
    assert payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu",
        "--model",
        "gpt-5.4",
        "--reasoning-effort",
        "high",
        "--project",
        "Codex Hub",
        "--source",
        "feishu",
        "--chat-ref",
        "oc_demo_chat",
        "--thread-name",
        "继续 Codex Hub 项目",
        "--thread-label",
        "CoCo 私聊",
        "--source-message-id",
        "om_demo_msg",
        "--prompt",
        "继续 Codex Hub 项目",
    ]


def test_electron_profile_routes_through_start_codex(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续当前桌面对话",
        session_id="",
        project_name="Codex Hub",
        execution_profile="electron",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    exec_payload = read_payload(capsys)
    assert exec_payload["ok"] is True
    assert exec_payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "electron",
        "--project",
        "Codex Hub",
        "--prompt",
        "继续当前桌面对话",
    ]


def test_electron_full_access_profile_routes_through_start_codex(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="继续 Electron 完全访问任务",
        session_id="",
        project_name="Codex Hub",
        execution_profile="electron-full-access",
        source="electron",
        chat_ref="",
        thread_name="",
        thread_label="",
        source_message_id="",
        approval_token="",
        no_auto_resume=False,
        model="",
        reasoning_effort="",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    exec_payload = read_payload(capsys)
    assert exec_payload["ok"] is True
    assert exec_payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "electron-full-access",
        "--project",
        "Codex Hub",
        "--source",
        "electron",
        "--prompt",
        "继续 Electron 完全访问任务",
    ]


def test_local_system_approved_profile_routes_through_start_codex(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    runtime_state.upsert_approval_token(
        token="coco-local-system",
        scope="feishu_local_system_execution",
        status="approved",
        metadata={"approved_execution_profile": "feishu-local-system-approved"},
    )
    monkeypatch.setattr(
        local_broker,
        "_run",
        lambda command, cwd=None: {
            "command": command,
            "returncode": 0,
            "stdout": "ok\n",
            "stderr": "",
        },
    )

    exec_args = argparse.Namespace(
        action="codex-exec",
        prompt="请帮我安装 launch agent",
        session_id="",
        project_name="Codex Hub",
        execution_profile="feishu-local-system-approved",
        source="feishu",
        chat_ref="",
        thread_name="",
        thread_label="",
        source_message_id="",
        approval_token="coco-local-system",
        no_auto_resume=False,
        model="",
        reasoning_effort="",
    )
    assert local_broker.cmd_command_center(exec_args) == 0
    payload = read_payload(capsys)
    assert payload["ok"] is True
    assert payload["command"] == [
        str(local_broker.workspace_root() / "ops" / "start-codex"),
        "--execution-profile",
        "feishu-local-system-approved",
        "--approval-token",
        "coco-local-system",
        "--project",
        "Codex Hub",
        "--source",
        "feishu",
        "--prompt",
        "请帮我安装 launch agent",
    ]


def test_bridge_conversations_suppress_attention_for_stale_threads(sample_env, monkeypatch, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(runtime_state, "iso_now", lambda: "2026-03-14T00:00:00Z")

    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="inbound",
        message_id="msg-stale-in",
        status="received",
        payload={
            "chat_id": "chat_stale",
            "chat_type": "group",
            "open_id": "ou_stale",
            "text": "这个群只聊 TINT",
            "created_at": "2026-03-14T00:00:00Z",
        },
    )
    runtime_state.upsert_bridge_chat_binding(
        bridge="feishu",
        chat_ref="chat_stale",
        binding_scope="project",
        project_name="TINT",
        topic_name="",
        session_id="sess-stale",
    )

    assert local_broker.cmd_bridge_conversations(argparse.Namespace(bridge="feishu", limit=20)) == 0
    payload = read_payload(capsys)
    rows_by_chat = {row["chat_ref"]: row for row in payload["rows"]}
    assert rows_by_chat["chat_stale"]["stale_thread"] is True
    assert rows_by_chat["chat_stale"]["needs_attention"] is False
    assert rows_by_chat["chat_stale"]["attention_reason"] == ""


def test_bridge_runtime_retrieval_protocol_and_message_detail(sample_env, capsys) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, local_broker, _watcher = reload_modules()
    runtime_state.init_db()

    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="inbound",
        message_id="msg-proto-in",
        status="received",
        session_id="sess-proto",
        payload={
            "chat_id": "chat_proto",
            "chat_type": "p2p",
            "open_id": "ou_proto",
            "text": "继续检查 runtime",
        },
    )
    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="outbound",
        message_id="msg-proto-out",
        status="sent",
        session_id="sess-proto",
        payload={
            "chat_id": "chat_proto",
            "reply_target": "chat_proto",
            "reply_target_type": "chat_id",
            "text": "已完成汇报",
            "phase": "reply",
        },
    )

    assert local_broker.cmd_bridge_conversations(argparse.Namespace(bridge="feishu", limit=20)) == 0
    conversations_payload = read_payload(capsys)
    assert conversations_payload["ok"] is True
    assert conversations_payload["retrieval_protocol"]["name"] == "search-timeline-detail"
    assert conversations_payload["retrieval_protocol"]["next_step"] == "timeline"
    assert "chat_proto" in conversations_payload["retrieval_protocol"]["timeline_refs"]

    assert (
        local_broker.cmd_bridge_messages(argparse.Namespace(bridge="feishu", chat_ref="chat_proto", limit=20)) == 0
    )
    messages_payload = read_payload(capsys)
    assert messages_payload["ok"] is True
    assert messages_payload["retrieval_protocol"]["next_step"] == "detail"
    assert "msg-proto-out" in messages_payload["retrieval_protocol"]["detail_refs"]

    assert (
        local_broker.cmd_bridge_message_detail(
            argparse.Namespace(bridge="feishu", message_id="msg-proto-out", direction="outbound")
        )
        == 0
    )
    detail_payload = read_payload(capsys)
    assert detail_payload["ok"] is True
    assert detail_payload["detail"]["chat_ref"] == "chat_proto"
    assert detail_payload["detail"]["payload"]["text"] == "已完成汇报"


def test_runtime_event_queue_claim_complete_and_stale_recovery(sample_env) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, _local_broker, _watcher = reload_modules()
    runtime_state.init_db()

    first = runtime_state.enqueue_runtime_event(
        queue_name="retrieval_sync",
        event_type="project_writeback",
        payload={"project_name": "SampleProj", "event_id": "evt-retrieval-1"},
        dedupe_key="evt-retrieval-1",
    )
    assert first["status"] == "pending"

    claimed = runtime_state.claim_runtime_events(
        queue_name="retrieval_sync",
        claimed_by="pytest",
        limit=10,
        lease_seconds=60,
    )
    assert len(claimed) == 1
    assert claimed[0]["event_key"] == first["event_key"]
    assert claimed[0]["status"] == "processing"
    assert claimed[0]["claim_token"]

    completed = runtime_state.complete_runtime_event(
        claimed[0]["event_key"],
        claim_token=claimed[0]["claim_token"],
        result={"ok": True},
    )
    assert completed["status"] == "completed"
    assert completed["result"]["ok"] is True

    second = runtime_state.enqueue_runtime_event(
        queue_name="dashboard_sync",
        event_type="project_writeback",
        payload={"project_name": "SampleProj", "event_id": "evt-dashboard-1"},
        dedupe_key="evt-dashboard-1",
    )
    claimed_second = runtime_state.claim_runtime_events(
        queue_name="dashboard_sync",
        claimed_by="pytest",
        limit=10,
        lease_seconds=60,
    )
    assert claimed_second[0]["event_key"] == second["event_key"]
    with runtime_state.transaction() as conn:
        conn.execute(
            "UPDATE runtime_events SET lease_expires_at = ?, updated_at = ? WHERE event_key = ?",
            ("2026-03-10T00:00:00Z", "2026-03-10T00:00:00Z", second["event_key"]),
        )

    reclaimed = runtime_state.claim_runtime_events(
        queue_name="dashboard_sync",
        claimed_by="pytest-reclaimer",
        limit=10,
        lease_seconds=60,
    )
    assert reclaimed[0]["event_key"] == second["event_key"]
    assert reclaimed[0]["claimed_by"] == "pytest-reclaimer"

    queue_status = runtime_state.fetch_runtime_queue_status(queue_name="dashboard_sync")
    assert queue_status["aggregate"]["processing"] == 1
    assert runtime_state.fetch_runtime_summary()["runtime_event_count"] >= 2


def test_record_project_writeback_enqueues_runtime_queue_events(sample_env) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, runtime_state, _local_broker, _watcher = reload_modules()
    runtime_state.init_db()

    binding = {
        "project_name": "SampleProj",
        "session_id": "sess-writeback-1",
        "binding_scope": "project",
        "binding_board_path": str(codex_memory.project_board_path("SampleProj")),
        "topic_name": "",
        "rollup_target": str(codex_memory.project_board_path("SampleProj")),
        "last_active_at": "2026-03-12T02:00:00Z",
        "started_at": "2026-03-12T01:50:00Z",
    }
    event = codex_memory.record_project_writeback(
        binding,
        source="pytest",
        changed_targets=[str(codex_memory.project_board_path("SampleProj"))],
        trigger_dashboard_sync=False,
    )

    retrieval_events = runtime_state.fetch_runtime_events(queue_name="retrieval_sync", limit=20)
    dashboard_events = runtime_state.fetch_runtime_events(queue_name="dashboard_sync", limit=20)
    projection_events = runtime_state.fetch_runtime_events(queue_name="feishu_projection_sync", limit=20)

    assert any(item["payload"].get("event_id") == event["event_id"] for item in retrieval_events)
    assert any(item["payload"].get("event_id") == event["event_id"] for item in dashboard_events)
    assert any(item["payload"].get("event_id") == event["event_id"] for item in projection_events)


def test_dashboard_sync_consumes_runtime_queue_events(sample_env) -> None:
    dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, _local_broker, _watcher = reload_modules()
    runtime_state.init_db()

    queued = runtime_state.enqueue_runtime_event(
        queue_name="dashboard_sync",
        event_type="project_writeback",
        payload={"project_name": "SampleProj", "event_id": "evt-dashboard-consume"},
        dedupe_key="evt-dashboard-consume",
    )

    result = dashboard_sync.run_sync(force_full=False)
    assert result["status"] == "ok"

    detail = runtime_state.fetch_runtime_event(queued["event_key"])
    assert detail["status"] == "completed"
    assert detail["result"]["status"] == "ok"


def test_codex_model_defaults_remain_in_declared_catalog(sample_env) -> None:
    from ops import codex_models as codex_models_module

    codex_models = importlib.reload(codex_models_module)
    summary = codex_models.summarize_settings()

    allowed_models = {item["id"] for item in summary["choices"] if item.get("id")}
    allowed_reasoning = {item["id"] for item in summary["reasoning_choices"] if item.get("id")}

    assert set(summary["defaults"].values()) <= allowed_models
    assert set(summary["reasoning_defaults"].values()) <= allowed_reasoning


def test_runtime_ingestion_contract_matches_watcher_schema(sample_env) -> None:
    from ops import runtime_ingestion as runtime_ingestion_module
    from ops import codex_session_watcher as watcher_module

    runtime_ingestion = importlib.reload(runtime_ingestion_module)
    watcher = importlib.reload(watcher_module)

    assert watcher.DEFAULT_TRANSCRIPT_SCHEMA["shared_fields"] == runtime_ingestion.transcript_shared_fields()


def test_start_codex_and_local_broker_follow_runtime_ingestion_contract(sample_env) -> None:
    from ops import runtime_ingestion as runtime_ingestion_module

    runtime_ingestion = importlib.reload(runtime_ingestion_module)
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, _runtime_state, local_broker, _watcher = reload_modules()

    command = local_broker._start_codex_command(
        prompt="继续当前线程",
        project_name="SampleProj",
        session_id="sess-ctx-1",
        execution_profile="electron",
        model="gpt-5.4",
        reasoning_effort="xhigh",
        source="electron",
        chat_ref="oc_ctx",
        thread_name="当前线程",
        thread_label="桌面对话",
        source_message_id="om_ctx",
    )

    assert "--reasoning-effort" in command
    for _field_name, option in runtime_ingestion.start_codex_forward_options():
        assert option in command

    start_codex_text = (Path(__file__).resolve().parents[1] / "ops" / "start-codex").read_text(encoding="utf-8")
    for key in runtime_ingestion.LAUNCH_CONTEXT_SCHEMA_KEYS:
        assert f'"{key}": ' in start_codex_text


def test_electron_and_feishu_entrypoints_keep_runtime_ingestion_fields(sample_env) -> None:
    electron_main = (Path(__file__).resolve().parents[1] / "apps" / "electron-console" / "main.js").read_text(
        encoding="utf-8"
    )
    bridge_host = (Path(__file__).resolve().parents[1] / "apps" / "electron-console" / "bridge-host.js").read_text(
        encoding="utf-8"
    )
    local_broker_text = (Path(__file__).resolve().parents[1] / "ops" / "local_broker.py").read_text(
        encoding="utf-8"
    )

    assert 'payload?.reasoning_effort' in electron_main
    assert '--reasoning-effort' in electron_main
    assert 'payload.thread_label' in bridge_host
    assert 'payload.source_message_id' in bridge_host
    assert 'approval_token=getattr(args, "approval_token", "")' in local_broker_text
    assert 'reasoning_effort=getattr(args, "reasoning_effort", "")' in local_broker_text


def test_bridge_continuity_status_flags_shared_sessions_and_response_delay(sample_env, monkeypatch) -> None:
    _dashboard_sync, _codex_memory, _review_plane, _coordination_plane, runtime_state, _local_broker, _watcher = reload_modules()
    runtime_state.init_db()
    monkeypatch.setattr(runtime_state, "iso_now", lambda: "2026-03-14T00:00:00Z")

    runtime_state.upsert_bridge_message(
        bridge="feishu",
        direction="inbound",
        message_id="msg-live-in",
        status="received",
        session_id="sess-chat-1",
        payload={
            "chat_id": "chat_live",
            "chat_type": "p2p",
            "open_id": "ou_live",
            "text": "继续处理这个问题",
        },
    )
    runtime_state.upsert_bridge_chat_binding(
        bridge="feishu",
        chat_ref="chat_live",
        binding_scope="project",
        project_name="SampleProj",
        topic_name="",
        session_id="sess-shared",
    )
    runtime_state.upsert_bridge_chat_binding(
        bridge="feishu",
        chat_ref="chat_shadow",
        binding_scope="project",
        project_name="SampleProj",
        topic_name="",
        session_id="sess-shared",
    )

    monkeypatch.setattr(runtime_state, "iso_now", lambda: "2026-03-14T00:03:00Z")
    monkeypatch.setattr(runtime_state, "age_seconds", lambda value: 180 if value else None)
    payload = runtime_state.fetch_bridge_continuity_status(bridge="feishu", limit=20)

    assert payload["ok"] is False
    assert payload["shared_session_count"] == 1
    assert payload["response_delayed_count"] == 1
    assert payload["progress_stalled_count"] == 0
    issue_types = {item["issue_type"] for item in payload["issues"]}
    assert "shared_session_across_chats" in issue_types
    assert "response_delayed" in issue_types
    shared_session_issue = next(item for item in payload["issues"] if item["issue_type"] == "shared_session_across_chats")
    assert shared_session_issue["chat_refs"] == ["chat_live", "chat_shadow"]


def test_watcher_routes_worktrees_and_only_updates_tasks_when_explicit(sample_env, monkeypatch) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, watcher = reload_modules()

    feishu_board = write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-01",
                "模块": "Chat ingress",
                "事项": "固化 chat payload 解析",
                "状态": "todo",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "待补充",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-01",
            }
        ],
    )
    electron_board = write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Electron Console",
        topic_key="electron",
        rows=[
            {
                "ID": "WH-EC-01",
                "模块": "Command Center",
                "事项": "打通 command center",
                "状态": "todo",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "待补充",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-02",
            }
        ],
    )
    codex_memory.refresh_project_rollups("SampleProj")

    fake_feishu_workspace = sample_env["workspace_root"].parent / "workspace-hub-worktrees" / "feishu-bridge"
    fake_electron_workspace = sample_env["workspace_root"].parent / "workspace-hub-worktrees" / "electron-console"
    fake_feishu_workspace.mkdir(parents=True, exist_ok=True)
    fake_electron_workspace.mkdir(parents=True, exist_ok=True)

    registry = {
        str(fake_feishu_workspace.resolve()): {
            "project_name": "SampleProj",
            "binding_scope": "topic",
            "binding_board_path": str(feishu_board),
            "topic_name": "Feishu Bridge",
            "rollup_target": str(codex_memory.project_board_path("SampleProj")),
        },
        str(fake_electron_workspace.resolve()): {
            "project_name": "SampleProj",
            "binding_scope": "topic",
            "binding_board_path": str(electron_board),
            "topic_name": "Electron Console",
            "rollup_target": str(codex_memory.project_board_path("SampleProj")),
        },
    }
    monkeypatch.setattr(watcher, "load_worktree_route_registry", lambda: registry)
    monkeypatch.setattr(watcher, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(watcher, "trigger_dashboard_sync_once", lambda: None)

    feishu_result = watcher.sync_snapshot(
        {
            "id": "sess-feishu",
            "started_at": "2026-03-12T02:00:00Z",
            "last_active_at": "2026-03-12T02:15:00Z",
            "cwd": str(fake_feishu_workspace),
            "user_message": "继续推进 Feishu Bridge",
            "last_agent_message": (
                "已固化 payload parser。\n"
                'TASK_WRITEBACK: {"task_id":"WH-FS-01","status":"doing","deliverable":"ops/feishu_bridge.py","next_action":"补 webhook schema"}'
            ),
            "completed": True,
            "path": str(fake_feishu_workspace / "session.jsonl"),
            "mtime": 1.0,
        }
    )
    assert feishu_result is not None
    assert feishu_result["action"] == "synced"
    assert feishu_result["binding_scope"] == "topic"
    assert feishu_result["topic_name"] == "Feishu Bridge"
    assert feishu_result["task_update_count"] == 1

    feishu_rows = codex_memory.load_topic_board(feishu_board)["rows"]
    assert feishu_rows[0]["状态"] == "doing"
    assert feishu_rows[0]["交付物"] == "ops/feishu_bridge.py"
    assert feishu_rows[0]["下一步"] == "补 webhook schema"
    assert feishu_rows[0]["更新时间"] == "2026-03-12T02:15:00Z"

    electron_result = watcher.sync_snapshot(
        {
            "id": "sess-electron",
            "started_at": "2026-03-12T03:00:00Z",
            "last_active_at": "2026-03-12T03:10:00Z",
            "cwd": str(fake_electron_workspace),
            "user_message": "继续推进 Electron Console",
            "last_agent_message": "已补 command center 空态。",
            "completed": True,
            "path": str(fake_electron_workspace / "session.jsonl"),
            "mtime": 2.0,
        }
    )
    assert electron_result is not None
    assert electron_result["action"] == "synced"
    assert electron_result["topic_name"] == "Electron Console"
    assert electron_result["task_update_count"] == 0

    electron_rows = codex_memory.load_topic_board(electron_board)["rows"]
    assert electron_rows[0]["状态"] == "todo"
    assert electron_rows[0]["交付物"] == ""
    assert electron_rows[0]["下一步"] == "待补充"


def test_watcher_uses_snapshot_launch_binding_when_prompt_is_generic(sample_env, monkeypatch) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, watcher = reload_modules()

    feishu_board = write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-02",
                "模块": "Continuity",
                "事项": "让 watcher 使用启动上下文回写",
                "状态": "todo",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "待补充",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-03",
            }
        ],
    )
    codex_memory.refresh_project_rollups("SampleProj")

    monkeypatch.setattr(watcher, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(watcher, "trigger_dashboard_sync_once", lambda: None)

    result = watcher.sync_snapshot(
        {
            "id": "sess-launch-bound",
            "started_at": "2026-03-12T04:00:00Z",
            "last_active_at": "2026-03-12T04:15:00Z",
            "cwd": str(sample_env["workspace_root"]),
            "user_message": "继续刚才那项",
            "last_agent_message": (
                "已切到显式 launch binding。\n"
                'TASK_WRITEBACK: {"task_id":"WH-FS-02","status":"doing","deliverable":"ops/codex_session_watcher.py","next_action":"补 schema-driven transcript mapping 测试"}'
            ),
            "completed": True,
            "path": str(sample_env["workspace_root"] / "session-launch-bound.jsonl"),
            "mtime": 3.0,
            "project_name": "SampleProj",
            "binding_scope": "topic",
            "binding_board_path": str(feishu_board),
            "topic_name": "Feishu Bridge",
            "rollup_target": str(codex_memory.project_board_path("SampleProj")),
            "source_thread_name": "SampleProj / Feishu Bridge",
            "source_chat_ref": "oc_launch_bound",
            "execution_profile": "feishu",
            "launch_source": "feishu",
            "model": "gpt-5.4",
            "reasoning_effort": "high",
        }
    )

    assert result is not None
    assert result["action"] == "synced"
    assert result["project_name"] == "SampleProj"
    assert result["binding_scope"] == "topic"
    assert result["topic_name"] == "Feishu Bridge"
    rows = codex_memory.load_topic_board(feishu_board)["rows"]
    assert rows[0]["状态"] == "doing"
    assert rows[0]["交付物"] == "ops/codex_session_watcher.py"
    bindings = codex_memory.load_bindings()["bindings"]
    latest = next(item for item in reversed(bindings) if item.get("session_id") == "sess-launch-bound")
    assert latest["thread_name"] == "SampleProj / Feishu Bridge"
    assert latest["source_chat_ref"] == "oc_launch_bound"
    assert latest["execution_profile"] == "feishu"
    assert latest["model"] == "gpt-5.4"
    assert latest["reasoning_effort"] == "high"

    project_board = codex_memory.load_project_board("SampleProj")
    rollup_ids = {row["ID"] for row in project_board["rollup_rows"]}
    assert "WH-FS-02" in rollup_ids


def test_dashboard_rebuild_refreshes_project_rollups(sample_env) -> None:
    dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, _watcher = reload_modules()

    topic_board = write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-01",
                "模块": "Chat ingress",
                "事项": "固化 chat payload 解析",
                "状态": "doing",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "补 webhook schema",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-01",
            }
        ],
    )
    board = codex_memory.load_project_board("SampleProj")
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], board["project_rows"], [])

    result = dashboard_sync.run_sync(force_full=True)

    assert result["status"] == "ok"
    refreshed = codex_memory.load_project_board("SampleProj")
    assert {row["ID"] for row in refreshed["rollup_rows"]} == {"WH-FS-01"}
    next_actions = codex_memory.read_text(codex_memory.NEXT_ACTIONS_MD)
    assert "WH-FS-01" in next_actions
    assert topic_board.exists()


def test_refresh_index_refreshes_project_rollups(sample_env) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, _watcher = reload_modules()

    write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-01",
                "模块": "Chat ingress",
                "事项": "固化 chat payload 解析",
                "状态": "doing",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "补 webhook schema",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-01",
            }
        ],
    )
    board = codex_memory.load_project_board("SampleProj")
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], board["project_rows"], [])

    assert codex_memory.cmd_refresh_index(argparse.Namespace()) == 0

    refreshed = codex_memory.load_project_board("SampleProj")
    assert {row["ID"] for row in refreshed["rollup_rows"]} == {"WH-FS-01"}
    next_actions = codex_memory.read_text(codex_memory.NEXT_ACTIONS_MD)
    assert "WH-FS-01" in next_actions


def test_dashboard_sync_rebuilds_when_topic_board_changes_without_events(sample_env) -> None:
    dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, _watcher = reload_modules()

    write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-01",
                "模块": "Chat ingress",
                "事项": "固化 chat payload 解析",
                "状态": "doing",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "补 webhook schema",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-01",
            }
        ],
    )
    board = codex_memory.load_project_board("SampleProj")
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], board["project_rows"], [])
    dashboard_sync.save_state(
        {
            "version": 1,
            "last_processed_event_line": 0,
            "last_incremental_sync_at": "2026-03-11T00:00:00+00:00",
            "last_full_rebuild_at": "2026-03-11T00:00:00+00:00",
            "last_status": "ok",
            "last_error": "",
        }
    )

    result = dashboard_sync.run_sync(force_full=False)

    assert result["status"] == "ok"
    refreshed = codex_memory.load_project_board("SampleProj")
    assert {row["ID"] for row in refreshed["rollup_rows"]} == {"WH-FS-01"}
    next_actions = codex_memory.read_text(codex_memory.NEXT_ACTIONS_MD)
    assert "WH-FS-01" in next_actions


def test_write_text_falls_back_to_in_place_overwrite_when_tempfile_is_blocked(sample_env, monkeypatch) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, _watcher = reload_modules()

    target = codex_memory.WORKING_ROOT / "fallback-write.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("before\n", encoding="utf-8")

    def blocked_named_tempfile(*args, **kwargs):
        raise PermissionError(1, "Operation not permitted", str(target.parent / "tmpblocked"))

    monkeypatch.setattr(codex_memory.tempfile, "NamedTemporaryFile", blocked_named_tempfile)

    codex_memory.write_text(target, "after\n")

    assert target.read_text(encoding="utf-8") == "after\n"


def test_scan_once_retries_previously_ignored_fixed_worktree_session(sample_env, monkeypatch) -> None:
    _dashboard_sync, codex_memory, _review_plane, _coordination_plane, _runtime_state, _local_broker, watcher = reload_modules()

    feishu_board = write_topic_board(
        codex_memory,
        project_name="SampleProj",
        topic_name="Feishu Bridge",
        topic_key="feishu",
        rows=[
            {
                "ID": "WH-FS-01",
                "模块": "Chat ingress",
                "事项": "固化 chat payload 解析",
                "状态": "todo",
                "交付物": "",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "待补充",
                "更新时间": "2026-03-12T00:00:00Z",
                "阻塞/依赖": "",
                "上卷ID": "WH-PAR-01",
            }
        ],
    )
    codex_memory.refresh_project_rollups("SampleProj")

    fake_feishu_workspace = sample_env["workspace_root"].parent / "workspace-hub-worktrees" / "feishu-bridge"
    fake_feishu_workspace.mkdir(parents=True, exist_ok=True)
    session_path = fake_feishu_workspace / "session.jsonl"
    session_path.write_text("", encoding="utf-8")
    snapshot = {
        "id": "sess-feishu-retry",
        "started_at": "2026-03-12T02:00:00Z",
        "last_active_at": "2026-03-12T02:15:00Z",
        "cwd": str(fake_feishu_workspace),
        "user_message": "继续推进 Feishu Bridge",
        "last_agent_message": (
            "已固化 payload parser。\n"
            'TASK_WRITEBACK: {"task_id":"WH-FS-01","status":"doing","deliverable":"ops/feishu_bridge.py","next_action":"补 webhook schema"}'
        ),
        "completed": True,
        "path": str(session_path),
        "mtime": 1.0,
    }
    registry = {
        str(fake_feishu_workspace.resolve()): {
            "project_name": "SampleProj",
            "binding_scope": "topic",
            "binding_board_path": str(feishu_board),
            "topic_name": "Feishu Bridge",
            "rollup_target": str(codex_memory.project_board_path("SampleProj")),
        }
    }

    watcher.save_state(
        {
            "version": 1,
            "updated_at": None,
            "sessions": {
                snapshot["id"]: {
                    "path": snapshot["path"],
                    "project_name": "",
                    "last_seen_mtime": snapshot["mtime"],
                    "last_synced_mtime": snapshot["mtime"],
                    "last_status": "ignored",
                    "last_active_at": snapshot["last_active_at"],
                }
            },
        }
    )

    monkeypatch.setattr(watcher, "load_recent_session_files", lambda days=14: [session_path])
    monkeypatch.setattr(watcher, "parse_session_snapshot", lambda path: snapshot)
    monkeypatch.setattr(watcher, "load_worktree_route_registry", lambda: registry)
    monkeypatch.setattr(watcher, "trigger_retrieval_sync_once", lambda: None)
    monkeypatch.setattr(watcher, "trigger_dashboard_sync_once", lambda: None)
    monkeypatch.setattr(watcher, "maybe_run_health_check_catchup", lambda: {"executed": False, "reason": "test"})

    result = watcher.scan_once(days=1, limit=10)

    assert result["processed"] == 1
    saved_state = watcher.load_state()["sessions"][snapshot["id"]]
    assert saved_state["last_status"] == "synced"
    assert saved_state["project_name"] == "SampleProj"

    feishu_rows = codex_memory.load_topic_board(feishu_board)["rows"]
    assert feishu_rows[0]["状态"] == "doing"
    assert feishu_rows[0]["交付物"] == "ops/feishu_bridge.py"

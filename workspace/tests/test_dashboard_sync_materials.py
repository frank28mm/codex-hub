from __future__ import annotations

import importlib


def test_rebuild_dashboards_generates_material_inspection_page(sample_env) -> None:
    from ops import codex_dashboard_sync, codex_memory, codex_retrieval

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    codex_memory = importlib.reload(codex_memory)
    codex_retrieval = importlib.reload(codex_retrieval)

    codex_retrieval.build_index()
    state = codex_dashboard_sync.load_state()
    result = codex_dashboard_sync.rebuild_dashboards(state=state, full=True, registry=codex_memory.load_registry())

    assert "SampleProj" in result["projects"]

    page = codex_memory.materials_dashboard_path("SampleProj")
    text = codex_memory.read_text(page)

    assert "# SampleProj｜材料检查" in text
    assert "truth board" in text
    assert "guide.md" in text
    assert "system-overview.md" in text
    assert "dirty count" in text


def test_render_materials_dashboard_includes_harness_observability(sample_env, monkeypatch) -> None:
    from ops import codex_dashboard_sync

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    monkeypatch.setattr(
        codex_dashboard_sync.board_job_projector,
        "list_projectable_jobs",
        lambda _project_name: [
            {
                "project_name": "SampleProj",
                "task_id": "SP-EXEC-01",
                "job_id": "board-job.sampleproj.sp-exec-01.sample-background-job",
                "task_item": "Run Sample background job",
                "executor_kind": "implementation_loop",
            }
        ],
    )
    monkeypatch.setattr(
        codex_dashboard_sync.background_job_executor,
        "job_status_payload",
        lambda _job: {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "harness_state": "queued",
            "last_decision": "continue",
            "next_action": "继续推进 IM contract",
            "next_wake_at": "2026-03-31T09:30:00+08:00",
            "blocked_reason": "",
            "current_focus": "IM contract",
            "runtime_overlay": {
                "current_stage": "execute",
                "current_focus": "IM contract",
                "run_id": "run-1",
            },
            "task_runtime_snapshot": {
                "completed_subgoal_count": 0,
                "pending_subgoal_count": 1,
                "active_run_id": "run-1",
            },
            "compression_policy": {
                "l1_strategy": "tool-output-trim",
                "l2_strategy": "session-summary",
                "l3_strategy": "handoff-summary",
            },
            "middleware": {
                "precompletion_checklist": {"status": "armed"},
                "loop_detection": {"status": "watching"},
                "local_context": {"allow_paths": ["/tmp/sample", "/tmp/another"]},
            },
            "project_runtime": {
                "task_status": "doing",
                "next_action": "继续推进 IM contract",
            },
            "bridge_runtime": {
                "status": "connected",
                "transport": "lark_cli_event_plus_cli_im",
                "continuity_issue_count": 0,
            },
            "run_tree": {
                "run_id": "run-1",
                "children": [{"run_id": "run-2"}],
                "shared_artifacts": [{"artifact_id": "progress_path"}],
            },
            "delivery_contract": {
                "aggregate_status": "delivered",
                "writeback_targets": ["project_board", "dashboard"],
                "pending_targets": [],
                "failed_targets": [],
            },
            "execution_boundary": {
                "sandbox_mode": "workspace_write",
                "network_access": "conditional",
                "writable_roots": ["/tmp/sample", "/tmp/another", "/tmp/third"],
            },
            "instruction_surface": {
                "human_guides": ["/tmp/AGENTS.md", "/tmp/MEMORY_SYSTEM.md"],
                "generated_rules": ["/tmp/generated.rules"],
                "hook_enforcement": ["pre_completion_checklist", "loop_detection", "local_context"],
                "policy_enforcement": ["operation_policy", "principal_policy"],
            },
            "extension_manifest": {
                "kind": "workflow",
                "lifecycle_state": "enabled",
                "capabilities": ["runtime_overlay", "extension_manifest", "delivery_writeback"],
                "hook_subscriptions": ["run_started", "delivery_done"],
                "supported_profiles": ["workspace-default", "background-job", "implementation_loop"],
            },
            "workflow_manifest": {
                "entry_command": "background-job-intent",
                "lifecycle_state": "loaded",
                "trigger_modes": ["explicit_intent", "wake_broker", "project_writeback"],
                "status_surfaces": ["job_status_payload", "materials_dashboard", "latest_report"],
            },
            "instruction_migration": {
                "retained_in_guides": ["workspace invariants"],
                "migrate_to_hooks": ["pre_completion_checklist", "loop_detection"],
                "migrate_to_policy": ["approval_gate", "execution_boundary"],
                "migrate_to_commands": ["continue long task", "pause long task", "doctor/recover"],
            },
            "open_source_boundary": {
                "public_contracts": ["runtime_overlay", "extension_manifest", "workflow_manifest", "instruction_surface"],
                "private_only": ["growth_operator_surface"],
                "migration_sequence": ["private_mainline_contract_freeze", "public_snapshot_status"],
                "not_recommended": ["raw_operator_playbooks"],
            },
        },
    )

    text = codex_dashboard_sync.render_materials_dashboard("SampleProj")

    assert "## Harness 派生状态" in text
    assert "harness_state=`queued`" in text
    assert "last_decision=`continue`" in text
    assert "next_action: 继续推进 IM contract" in text
    assert "next_wake_at=`2026-03-31T09:30:00+08:00`" in text
    assert "blocked_reason=`n/a`" in text
    assert "overlay: stage=`execute` | focus=`IM contract` | run_id=`run-1`" in text
    assert "snapshot: completed=`0` | pending=`1` | active_run_id=`run-1`" in text
    assert "compression: L1=`tool-output-trim` | L2=`session-summary` | L3=`handoff-summary`" in text
    assert "middleware: checklist=`armed` | loop=`watching` | local_roots=`2`" in text
    assert "project_runtime: status=`doing` | next_action=`继续推进 IM contract`" in text
    assert "bridge_runtime: status=`connected` | transport=`lark_cli_event_plus_cli_im` | continuity_issues=`0`" in text
    assert "run_tree: root=`run-1` | children=`1` | artifacts=`1`" in text
    assert "delivery_contract: aggregate=`delivered` | writebacks=`2` | pending=`0` | failed=`0`" in text
    assert "execution_boundary: sandbox=`workspace_write` | network=`conditional` | writable_roots=`3`" in text
    assert "instruction_surface: guides=`2` | generated_rules=`1` | hooks=`3` | policies=`2`" in text
    assert "extension_manifest: kind=`workflow` | lifecycle=`enabled` | capabilities=`3` | hooks=`2` | profiles=`3`" in text
    assert "workflow_manifest: entry=`background-job-intent` | lifecycle=`loaded` | triggers=`3` | surfaces=`3`" in text
    assert "instruction_migration: retain=`1` | hooks=`2` | policy=`2` | commands=`3`" in text
    assert "open_source_boundary: public=`4` | private_only=`1` | sequence=`2` | no_go=`1`" in text


def test_runtime_contract_view_normalizes_schema_backed_surfaces() -> None:
    from ops import workspace_job_schema

    workspace_job_schema = importlib.reload(workspace_job_schema)
    payload = workspace_job_schema.runtime_contract_view(
        {
            "execution_boundary": {
                "boundary_id": "b-1",
                "sandbox_mode": "workspace_write",
                "network_access": "conditional",
                "writable_roots": "/tmp/root",
                "requires_approval": True,
                "expected_scope": "SampleProj",
                "monitor_mode": "runtime_state",
            },
            "instruction_surface": {
                "human_guides": "/tmp/AGENTS.md",
                "generated_rules": "/tmp/generated.rules",
                "hook_enforcement": "pre_completion_checklist",
                "policy_enforcement": "execution_boundary",
                "command_surfaces": "background-job-intent",
                "migration_checklist": "move repeated rules",
            },
            "instruction_migration": {
                "retained_in_guides": "workspace invariants",
                "migrate_to_hooks": "pre_completion_checklist",
                "migrate_to_policy": "execution_boundary",
                "migrate_to_commands": "continue long task",
                "deferred_items": "operator-only playbooks",
            },
            "open_source_boundary": {
                "public_contracts": "runtime_overlay",
                "private_only": "growth_operator_surface",
                "migration_sequence": "public_snapshot_status",
                "not_recommended": "raw_operator_playbooks",
            },
        }
    )

    assert payload["execution_boundary"]["writable_roots"] == ["/tmp/root"]
    assert payload["instruction_surface"]["human_guides"] == ["/tmp/AGENTS.md"]
    assert payload["instruction_surface"]["generated_rules"] == ["/tmp/generated.rules"]
    assert payload["instruction_surface"]["hook_enforcement"] == ["pre_completion_checklist"]
    assert payload["instruction_migration"]["migrate_to_policy"] == ["execution_boundary"]
    assert payload["open_source_boundary"]["public_contracts"] == ["runtime_overlay"]


def test_verify_consistency_accepts_current_tasks_with_gflow_rows(sample_env) -> None:
    from ops import codex_dashboard_sync, gstack_automation

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    gstack_automation = importlib.reload(gstack_automation)

    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-dashboard-gflow",
    )
    gstack_automation.pause_workflow_run(
        started["run_id"],
        reason="当前 run 已无必要，停止继续推进。",
        next_action="无需继续；如后续需要，再显式 resume 或新开 run。",
        evidence=["dashboard-sync-check"],
    )

    issues = codex_dashboard_sync.verify_project_rollup_consistency("SampleProj")
    assert issues == []


def test_verify_consistency_accepts_current_tasks_with_harness_rows(sample_env, monkeypatch) -> None:
    from ops import board_job_projector, codex_dashboard_sync, codex_memory

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    codex_memory = importlib.reload(codex_memory)
    board_job_projector = importlib.reload(board_job_projector)

    board = codex_memory.load_project_board("SampleProj")
    project_rows = [
        {
            "ID": "SP-EXEC-01",
            "父ID": "",
            "来源": "project",
            "范围": "runtime",
            "事项": "Run Sample background job",
            "状态": "doing",
            "交付物": "",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "继续推进 harness contract",
            "更新时间": "2026-04-01T10:00:00+08:00",
            "指向": "SampleProj-项目板.md",
        }
    ]
    monkeypatch.setattr(
        board_job_projector,
        "task_harness_snapshot",
        lambda project_name, task_id: {
            "harness_state": "running",
            "last_decision": "continue",
            "next_wake_at": "",
            "blocked_reason": "",
        }
        if project_name == "SampleProj" and task_id == "SP-EXEC-01"
        else {},
    )

    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        project_rows,
        [],
        board.get("gflow_rows", []),
    )

    issues = codex_dashboard_sync.verify_project_rollup_consistency("SampleProj")
    assert issues == []


def test_verify_consistency_flags_missing_absolute_deliverable_paths(sample_env) -> None:
    from ops import codex_dashboard_sync, codex_memory

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    codex_memory = importlib.reload(codex_memory)

    board = codex_memory.load_project_board("SampleProj")
    project_rows = [
        {
            "ID": "SP-DOC-01",
            "父ID": "",
            "来源": "project",
            "范围": "docs",
            "事项": "Keep docs in sync",
            "状态": "doing",
            "交付物": "`/Users/frank/missing-report.md`",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "补齐本地文档归档",
            "更新时间": "2026-04-03T10:00:00+08:00",
            "指向": "SampleProj-项目板.md",
        }
    ]
    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        project_rows,
        [],
        board.get("gflow_rows", []),
    )

    payload = codex_dashboard_sync.verify_consistency(["SampleProj"])

    assert payload["ok"] is False
    assert any("missing-report.md" in issue for issue in payload["issues"])


def test_project_board_next_action_prefers_row_next_step(sample_env) -> None:
    from ops import codex_memory

    codex_memory = importlib.reload(codex_memory)

    next_action = codex_memory.project_board_next_action(
        [
            {
                "ID": "SP-EXEC-01",
                "事项": "Run Sample background job",
                "状态": "doing",
                "下一步": "继续推进 harness contract",
                "更新时间": "2026-04-02T09:00:00+08:00",
            }
        ],
        [],
    )

    assert next_action == "继续推进 harness contract"

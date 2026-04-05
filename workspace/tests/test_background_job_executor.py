from __future__ import annotations

import datetime as dt
import importlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml


def seed_sample_project_board(sample_env) -> None:
    from ops import codex_memory

    board = codex_memory.load_project_board("SampleProj")
    board["project_rows"] = [
        {
            "ID": "SP-EXEC-01",
            "父ID": "",
            "来源": "project",
            "范围": "automation",
            "事项": "Run Sample background job",
            "状态": "todo",
            "交付物": "sample brief",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "collect sources and prepare a brief",
            "更新时间": "2026-03-25",
            "指向": "SampleProj-项目板.md",
        }
    ]
    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        board["project_rows"],
        board["rollup_rows"],
        board["gflow_rows"],
    )


def growth_control_for_sample_task() -> dict[str, object]:
    return {
        "project_name": "SampleProj",
        "system_name": "Codex Growth System",
        "primary_product": "Codex Hub",
        "primary_platforms": ["xianyu", "xiaohongshu"],
        "workflow_specs": {
            "signal_scan": {
                "workflow_id": "signal_scan",
                "executor_kind": "growth_signal_scan",
                "allowed_actions": ["read", "write_report", "write_board"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 3,
                "time_budget_minutes": 20,
                "success_criteria": ["Freeze growth contract."],
                "input_objects": ["Offer", "Listing"],
            }
        },
        "objects": {
            "Offer": {"table_path": "/tmp/offer.md"},
            "Listing": {"table_path": "/tmp/listing.md"},
        },
        "platform_policies": {
            "xianyu": {"role": "承接与测试"},
            "xiaohongshu": {"role": "导流与信任建立"},
        },
        "risk_controls": {"max_actions_per_hour": {"xianyu": 12}},
        "task_specs": {
            "SP-EXEC-01": {
                "job_slug": "sample-growth-scan",
                "workflow_id": "signal_scan",
                "automation_mode": "background_assist",
                "summary_focus": "Truth Layer 与投影主表落地",
                "analysis_focus": ["Freeze truth tables.", "Freeze platform policy."],
            }
        },
    }


def implementation_loop_spec() -> dict[str, object]:
    return {
        "job_slug": "sample-implementation-loop",
        "executor_kind": "implementation_loop",
        "automation_mode": "background_assist",
        "allowed_actions": ["read", "write_code", "run_tests", "write_report", "write_board"],
        "delivery_targets": ["board", "report"],
        "gate_policy": "none",
        "max_rounds": 3,
        "time_budget_minutes": 10,
        "subgoal_schema_version": 2,
        "acceptance_criteria": ["Close the sample capability loop."],
        "implementation_tracks": [
            {
                "subgoal_id": "im-contract",
                "summary": "IM contract",
                "current_truth": ["IM baseline exists."],
                "target_files": ["/tmp/im.py"],
                "execute_actions": ["Advance IM contract."],
                "verify_commands": [{"label": "verify-im", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                "adapt_actions": ["Narrow the IM fix."],
            },
            {
                "subgoal_id": "calendar-contract",
                "summary": "Calendar contract",
                "current_truth": ["Calendar baseline exists."],
                "target_files": ["/tmp/calendar.py"],
                "execute_actions": ["Advance calendar contract."],
                "verify_commands": [{"label": "verify-calendar", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                "adapt_actions": ["Narrow the calendar fix."],
            },
        ],
    }


def test_final_next_action_marks_done_tasks_complete() -> None:
    from ops import background_job_executor

    assert (
        background_job_executor._final_next_action({"decision": "done"}, "进入 verify，运行 smoke。")
        == "已完成，无需继续。"
    )


def save_project_rows(sample_env, rows: list[dict[str, str]]) -> None:
    from ops import codex_memory

    board = codex_memory.load_project_board("SampleProj")
    board["project_rows"] = rows
    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        board["project_rows"],
        board["rollup_rows"],
        board["gflow_rows"],
    )


def test_board_job_projector_projects_runnable_task(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")

    assert payload["task_item"] == "Run Sample background job"
    assert payload["automation_mode"] == "background_assist"
    assert payload["allowed_actions"] == ["read", "write_report"]
    assert payload["max_rounds"] == 2
    assert payload["program_spec"]["scope_type"] == "project"
    assert payload["program_spec"]["scope_ref"] == "SampleProj"
    assert payload["program_spec"]["approval_state"] == "not-required"
    assert payload["handoff_bundle"]["task_spec_path"].endswith("task-spec.json")
    assert payload["handoff_bundle"]["latest_smoke_path"].endswith("latest-smoke.md")


def save_project_rows(sample_env, rows: list[dict[str, str]]) -> None:
    from ops import codex_memory

    board = codex_memory.load_project_board("SampleProj")
    board["project_rows"] = rows
    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        board["project_rows"],
        board["rollup_rows"],
        board["gflow_rows"],
    )


def test_board_job_projector_projects_runnable_task(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")

    assert payload["task_item"] == "Run Sample background job"
    assert payload["automation_mode"] == "background_assist"
    assert payload["allowed_actions"] == ["read", "write_report"]
    assert payload["max_rounds"] == 2
    assert payload["program_spec"]["scope_type"] == "project"
    assert payload["program_spec"]["scope_ref"] == "SampleProj"
    assert payload["program_spec"]["approval_state"] == "not-required"
    assert payload["handoff_bundle"]["task_spec_path"].endswith("task-spec.json")
    assert payload["handoff_bundle"]["latest_smoke_path"].endswith("latest-smoke.md")


def test_board_job_projector_synthesizes_harness_upgrade_manual_long_task(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-24",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "讨论任务",
                "事项": "讨论并升级 generic 长任务进入 harness 后无法自动循环推进到底的问题：为私有->开源迁移这类任务设计更完整的 implementation_tracks / execution_packets / verify_commands / privacy gate。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "围绕 generic long task spec、privacy gate 和 public migration task type 形成正式升级方案。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-24")

    assert payload["executor_kind"] == "implementation_loop"
    assert payload["subgoal_schema_version"] >= 2
    assert len(payload["implementation_tracks"]) == 3
    assert payload["implementation_tracks"][0]["subgoal_id"] == "generic-harness-contract"
    assert any(
        item["summary"] == "为公开迁移类长任务补 privacy 与 public-safe 边界"
        for item in payload["implementation_tracks"]
    )
    verify_commands = payload["implementation_tracks"][0]["verify_commands"]
    assert any("test_background_job_executor.py" in item["command"] for item in verify_commands)
    assert any("latest.md" in item["command"] for item in verify_commands)


def test_board_job_projector_resolves_family_by_registry(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-REG",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "梳理开源版 Codex Hub 从零安装和使用所需的真实依赖，并补强一键检测和安装链路。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先盘点公开版真实依赖与当前 bootstrap/accept 覆盖缺口。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-REG")

    assert payload["task_family"] == "install_readiness"
    assert payload["family_source"] == "registry"
    assert payload["family_resolution_reason"] == "match_rules"


def test_board_job_projector_prefers_explicit_spec_over_family_registry(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-EXPLICIT",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "梳理开源版 Codex Hub 从零安装和使用所需的真实依赖，并补强一键检测和安装链路。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先盘点公开版真实依赖与当前 bootstrap/accept 覆盖缺口。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-EXPLICIT": {
                "job_slug": "explicit-spec",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a brief."],
            }
        },
    )

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-EXPLICIT")

    assert payload["executor_kind"] == "research_brief"
    assert payload["job_slug"] == "explicit-spec"
    assert payload["implementation_tracks"] == []


def test_board_job_projector_prefers_pointer_family_over_text_heuristic(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    pointer_name = "SampleProj-FamilyHarness.md"
    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-FAMILY",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "梳理依赖并补强一键安装链路。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先锁定公开迁移差异面。",
                "更新时间": "2026-04-04",
                "指向": pointer_name,
            }
        ],
    )
    (sample_env["vault_root"] / "01_working" / pointer_name).write_text(
        "---\n"
        "harness:\n"
        "  family: public_migration\n"
        "---\n\n"
        "# Pointer Family\n",
        encoding="utf-8",
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-FAMILY")

    assert payload["task_family"] == "public_migration"
    assert payload["family_source"] == "pointer_family"
    assert payload["family_resolution_reason"] == "harness.family"


def test_board_job_projector_projects_wh_ops_24_as_harness_upgrade(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "WH-OPS-24",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "讨论任务",
                "事项": "讨论并升级 generic 长任务进入 harness 后无法自动循环推进到底的问题：为私有->开源迁移这类任务设计更完整的 implementation_tracks / execution_packets / verify_commands / privacy gate。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "围绕 generic long task spec、privacy gate 和 public migration task type 形成正式升级方案。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "WH-OPS-24")

    assert payload["task_family"] == "harness_upgrade"
    assert payload["executor_kind"] == "implementation_loop"


def test_board_job_projector_projects_wh_ops_25_as_install_readiness(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "WH-OPS-25",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "梳理开源版 Codex Hub 从零安装和使用所需的真实依赖，并补强一键检测和安装链路。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先盘点公开版真实依赖与当前 bootstrap/accept 覆盖缺口，再设计更强的一键检测/安装方案。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "WH-OPS-25")

    assert payload["task_family"] == "install_readiness"
    assert payload["executor_kind"] == "implementation_loop"


def test_board_job_projector_materializes_execution_packets_and_verify_commands_from_family_blueprint(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-BLUEPRINT",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "讨论任务",
                "事项": "讨论并升级 generic 长任务进入 harness 后无法自动循环推进到底的问题。",
                "状态": "doing",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "收成 implementation tracks。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-BLUEPRINT")

    track = payload["implementation_tracks"][0]
    assert track["execution_packets"]
    packet = track["execution_packets"][0]
    assert packet["kind"] == "codex_exec"
    assert packet["packet_contract_version"] == "codex-hub.execution-packet.compact.v1"
    assert packet["packet_outline"]["target_count"] >= 1
    assert packet["target_files"]
    assert packet["execute_actions"]
    assert "[Packet]" in packet["prompt"]
    assert "targets:" in packet["prompt"]
    assert "目标文件：" not in packet["prompt"]
    assert track["verify_commands"]
    assert any("latest.md" in item["command"] for item in track["verify_commands"])


def test_board_job_projector_synthesizes_install_readiness_manual_long_task(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-25",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "梳理开源版 Codex Hub 从零安装和使用所需的真实依赖，并补强一键检测和安装链路。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先盘点公开版真实依赖与当前 bootstrap/accept 覆盖缺口，再设计更强的一键检测/安装方案。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-25")

    assert payload["executor_kind"] == "implementation_loop"
    assert len(payload["implementation_tracks"]) == 2
    assert payload["implementation_tracks"][0]["summary"] == "盘点开源版真实依赖与前置条件"
    assert any(
        entry.endswith("/ops/bootstrap_workspace_hub.py")
        for entry in payload["implementation_tracks"][0]["target_files"]
    )
    assert any(
        "accept_product.py run" in item["command"]
        for item in payload["implementation_tracks"][1]["verify_commands"]
    )


def test_board_job_projector_synthesizes_generic_manual_long_task_report_verifier(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-26",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "长任务",
                "事项": "持续推进一个没有显式模板的通用长任务。",
                "状态": "doing",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "先把当前 contract、实现包和下一步收清楚。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-26")

    assert payload["executor_kind"] == "implementation_loop"
    assert payload["implementation_tracks"][0]["verify_commands"][0]["label"].startswith("verify-")
    assert "latest.md" in payload["implementation_tracks"][0]["verify_commands"][0]["command"]
    assert "Latest Smoke" not in payload["implementation_tracks"][0]["verify_commands"][0]["command"]
    assert payload["implementation_tracks"][0]["execution_packets"][0]["kind"] == "codex_exec"


def test_background_job_executor_rejects_empty_implementation_loop_tracks() -> None:
    from ops import background_job_executor

    with pytest.raises(ValueError, match="implementation_loop requires non-empty implementation_tracks"):
        background_job_executor.run_implementation_loop(
            {
                "task_id": "SP-EXEC-EMPTY",
                "task_item": "Empty loop",
                "executor_kind": "implementation_loop",
                "implementation_tracks": [],
                "task_spec": {"stage": "discover", "current_focus": "Empty loop", "subgoals": []},
                "max_rounds": 3,
            }
        )


def test_runtime_contract_surfaces_task_family_metadata(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "WH-OPS-24",
                "父ID": "",
                "来源": "manual_long_task",
                "范围": "讨论任务",
                "事项": "讨论并升级 generic 长任务进入 harness 后无法自动循环推进到底的问题。",
                "状态": "todo",
                "交付物": "待补充",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "形成正式升级方案。",
                "更新时间": "2026-04-04",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "WH-OPS-24")

    scaffold = background_job_executor.initialize_program_scaffold(
        job,
        run_context={"run_id": "family-run"},
        persist=False,
    )

    task_spec = scaffold["task_spec"]
    assert task_spec["task_family"] == "harness_upgrade"
    assert task_spec["family_source"] == "registry"
    assert task_spec["family_resolution_reason"] == "match_rules"
    assert task_spec["workflow_manifest"]["metadata"]["task_family"] == "harness_upgrade"
    assert task_spec["instruction_migration"]["metadata"]["task_family"] == "harness_upgrade"
    assert task_spec["open_source_boundary"]["metadata"]["task_family"] == "harness_upgrade"


def test_board_job_projector_reads_pointer_harness_frontmatter_when_task_is_unregistered(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    pointer_name = "SampleProj-HarnessTask.md"
    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-02",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Run pointer-driven harness task",
                "状态": "doing",
                "交付物": "pointer brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "continue",
                "更新时间": "2026-03-30",
                "指向": pointer_name,
            }
        ],
    )
    (sample_env["vault_root"] / "01_working" / pointer_name).write_text(
        (
            "---\n"
            "harness:\n"
            "  executor_kind: implementation_loop\n"
            "  job_slug: pointer-driven-harness\n"
            "  acceptance_criteria:\n"
            "    - close the pointer-driven task\n"
            "  implementation_tracks:\n"
            "    - subgoal_id: pointer-subgoal\n"
            "      summary: Pointer track\n"
            "      current_truth:\n"
            "        - pointer truth exists\n"
            "      execute_actions:\n"
            "        - advance pointer task\n"
            "      execution_packets:\n"
            "        - label: pointer-shell\n"
            "          kind: shell\n"
            "          command: python3 -c \"print('pointer')\"\n"
            "      verify_commands:\n"
            "        - label: verify-pointer\n"
            "          command: python3 -c \"print('ok')\"\n"
            "---\n\n"
            "# Pointer Task\n"
        ),
        encoding="utf-8",
    )

    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-02")

    assert payload["executor_kind"] == "implementation_loop"
    assert payload["job_slug"] == "pointer-driven-harness"
    assert payload["implementation_tracks"][0]["subgoal_id"] == "pointer-subgoal"
    assert payload["implementation_tracks"][0]["execution_packets"][0]["kind"] == "shell"


def test_board_job_projector_isolates_identity_and_artifacts_per_task(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-01",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Task one",
                "状态": "todo",
                "交付物": "brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "one",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
            {
                "ID": "SP-EXEC-02",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Task two",
                "状态": "todo",
                "交付物": "brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "two",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "shared-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Task one goal."],
            },
            "SP-EXEC-02": {
                "job_slug": "shared-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Task two goal."],
            },
        },
    )

    first = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    second = board_job_projector.project_background_job("SampleProj", "SP-EXEC-02")

    assert first["job_id"] != second["job_id"]
    assert first["program_id"] != second["program_id"]
    assert first["artifacts_root"] != second["artifacts_root"]
    assert first["handoff_bundle"]["task_spec_path"] != second["handoff_bundle"]["task_spec_path"]
    assert "sp-exec-01" in first["artifacts_root"]
    assert "sp-exec-02" in second["artifacts_root"]


def test_board_job_projector_uses_canonical_identity_for_aliases(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    registry = sample_env["vault_root"] / "PROJECT_REGISTRY.md"
    registry.write_text(
        registry.read_text(encoding="utf-8").replace('"aliases": []', '"aliases": ["sampleproj-alias"]'),
        encoding="utf-8",
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )

    canonical = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    alias = board_job_projector.project_background_job("sampleproj-alias", "SP-EXEC-01")

    assert alias["project_name"] == "SampleProj"
    assert alias["job_id"] == canonical["job_id"]
    assert alias["program_id"] == canonical["program_id"]
    assert alias["artifacts_root"] == canonical["artifacts_root"]
    assert alias["program_spec"]["scope_ref"] == "SampleProj"


def test_board_job_projector_skips_legacy_programs_that_already_handed_off(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    legacy_root = sample_env["workspace_root"] / "reports" / "ops" / "background-jobs" / "sample-background-job"
    legacy_bundle = board_job_projector.workspace_job_schema.handoff_bundle_paths(legacy_root)
    board_job_projector.workspace_job_schema.write_json_file(
        legacy_bundle["task_spec_path"],
        {
            "task_id": "SP-EXEC-01",
            "program_id": "legacy-program",
            "objective": "Legacy objective",
            "scope_type": "project",
            "scope_ref": "SampleProj",
            "approval_required": False,
            "approval_state": "not-required",
            "stage": "handoff",
            "stage_plan": ["discover", "frame", "execute", "verify", "adapt", "handoff"],
            "wake_policy": {"manual_wake": True},
            "iteration_count": 2,
            "current_focus": "",
            "subgoals": [{"summary": "Produce a sample brief.", "status": "completed"}],
            "updated_at": "2026-03-28T12:00:00+08:00",
            "last_run_id": "legacy-run",
            "last_evaluation": {"current_stage": "verify", "next_stage": "handoff", "decision": "done"},
            "stage_history": [],
            "last_decision": "done",
        },
    )

    assert board_job_projector.list_projectable_jobs("SampleProj") == []
    with pytest.raises(ValueError, match="not runnable from status `done`"):
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")


def test_board_job_projector_loads_growth_task_from_control(sample_env, monkeypatch) -> None:
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "load_growth_control", lambda: growth_control_for_sample_task())
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})

    payload = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")

    assert payload["executor_kind"] == "growth_signal_scan"
    assert payload["workflow_id"] == "signal_scan"
    assert payload["input_objects"] == ["Offer", "Listing"]
    assert payload["object_tables"]["Offer"] == "/tmp/offer.md"
    assert payload["primary_product"] == "Codex Hub"


def test_background_job_executor_writes_report_and_ledger(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    payload = background_job_executor.execute_projected_job(job, trigger_source="manual_cli")

    assert payload["ok"] is True
    assert payload["run_record"]["job_id"] == job["job_id"]
    assert payload["run_record"]["execution_outcome"]["status"] == "ok"
    assert payload["run_record"]["delivery_status"] == "delivered"
    assert payload["run_record"]["metadata"]["program_id"] == job["program_spec"]["program_id"]
    assert payload["run_record"]["metadata"]["scope_type"] == "project"
    assert payload["run_record"]["metadata"]["iteration"] == 1
    latest_report = Path(payload["log_paths"]["latest_path"])
    latest_ops_report = Path(payload["log_paths"]["latest_ops_path"])
    task_spec_path = Path(payload["run_record"]["artifacts"]["task_spec_path"])
    acceptance_path = Path(payload["run_record"]["artifacts"]["acceptance_path"])
    progress_path = Path(payload["run_record"]["artifacts"]["progress_path"])
    latest_smoke_path = Path(payload["run_record"]["artifacts"]["latest_smoke_path"])
    assert latest_report.exists()
    assert latest_ops_report.exists()
    assert task_spec_path.exists()
    assert acceptance_path.exists()
    assert progress_path.exists()
    assert latest_smoke_path.exists()
    assert "# 后台任务报告｜SP-EXEC-01" in latest_report.read_text(encoding="utf-8")
    assert "## Agent Loop" not in latest_report.read_text(encoding="utf-8")
    assert "Background Job Brief" in latest_ops_report.read_text(encoding="utf-8")
    task_spec = json.loads(task_spec_path.read_text(encoding="utf-8"))
    assert task_spec["scope_type"] == "project"
    assert task_spec["stage"] == "frame"
    assert task_spec["current_focus"] == ""
    assert task_spec["last_decision"] == "continue"
    assert task_spec["subgoals"][0]["status"] == "completed"
    assert task_spec["last_evaluation"]["next_stage"] == "frame"
    assert task_spec["stage_history"][-1]["to_stage"] == "frame"
    assert "Progress｜SP-EXEC-01" in progress_path.read_text(encoding="utf-8")
    assert "Latest Smoke｜SP-EXEC-01" in latest_smoke_path.read_text(encoding="utf-8")

    history_entry = json.loads(Path(payload["run_record"]["artifacts"]["history_path"]).read_text(encoding="utf-8").splitlines()[-1])
    assert history_entry["job_id"] == job["job_id"]
    assert history_entry["metadata"]["phase"] == "phase-2"
    assert history_entry["metadata"]["scope_ref"] == "SampleProj"
    assert payload["run_record"]["artifacts"]["latest_ops_report_path"].endswith("latest-ops.md")
    assert payload["run_record"]["metadata"]["program_evaluation"]["decision"] == "continue"


def test_evaluate_program_iteration_advances_to_verify_after_execute_success(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    evaluation, updated_subgoals = background_job_executor.evaluate_program_iteration(
        {"task_id": "SP-EXEC-01"},
        scaffold={
            "task_spec": {
                "stage": "execute",
                "current_focus": "Collect official sources.",
                "subgoals": [
                    {"summary": "Collect official sources.", "status": "pending"},
                    {"summary": "Split replicable boundaries.", "status": "pending"},
                ],
                "scope_type": "project",
                "scope_ref": "SampleProj",
            }
        },
        execution_status="ok",
        delivery_status="delivered",
        gate_state={"status": "approved"},
    )

    assert evaluation["decision"] == "continue"
    assert evaluation["current_stage"] == "execute"
    assert evaluation["next_stage"] == "verify"
    assert evaluation["completed_subgoal_count"] == 1
    assert evaluation["pending_subgoal_count"] == 1
    assert updated_subgoals[0]["status"] == "completed"
    assert updated_subgoals[1]["status"] == "pending"


def test_evaluate_program_iteration_marks_done_after_verify_without_pending_subgoals(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    evaluation, updated_subgoals = background_job_executor.evaluate_program_iteration(
        {"task_id": "SP-EXEC-01"},
        scaffold={
            "task_spec": {
                "stage": "verify",
                "current_focus": "",
                "subgoals": [{"summary": "Collect official sources.", "status": "completed"}],
                "scope_type": "project",
                "scope_ref": "SampleProj",
            }
        },
        execution_status="ok",
        delivery_status="delivered",
        gate_state={"status": "approved"},
    )

    assert evaluation["decision"] == "done"
    assert evaluation["next_stage"] == "handoff"
    assert evaluation["acceptance_status"] == "accepted"
    assert updated_subgoals[0]["status"] == "completed"


def test_evaluate_program_iteration_moves_to_adapt_after_verify_failure(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    evaluation, updated_subgoals = background_job_executor.evaluate_program_iteration(
        {"task_id": "SP-EXEC-01"},
        scaffold={
            "task_spec": {
                "stage": "verify",
                "current_focus": "Collect official sources.",
                "subgoals": [{"summary": "Collect official sources.", "status": "pending"}],
                "scope_type": "project",
                "scope_ref": "SampleProj",
            }
        },
        execution_status="error",
        delivery_status="delivered",
        gate_state={"status": "approved"},
    )

    assert evaluation["decision"] == "adapt"
    assert evaluation["next_stage"] == "adapt"
    assert evaluation["acceptance_status"] == "needs-adaptation"
    assert updated_subgoals[0]["status"] == "pending"


def test_initialize_program_scaffold_preserves_existing_program_state(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    bundle = background_job_executor.handoff_bundle(job)
    background_job_executor.workspace_job_schema.write_json_file(
        bundle["task_spec_path"],
        {
            "task_id": "SP-EXEC-01",
            "program_id": job["program_spec"]["program_id"],
            "objective": "Existing objective",
            "scope_type": "project",
            "scope_ref": "SampleProj",
            "approval_required": False,
            "approval_state": "not-required",
            "stage": "verify",
            "stage_plan": ["discover", "frame", "execute", "verify", "adapt", "handoff"],
            "wake_policy": {"manual_wake": True, "scheduled": False},
            "iteration_count": 2,
            "current_focus": "",
            "subgoals": [
                {"summary": "Collect official sources.", "status": "completed"},
                {"summary": "Write pilot summary.", "status": "pending"},
            ],
            "updated_at": "2026-03-28T12:00:00+08:00",
            "last_run_id": "prev-run",
            "last_evaluation": {"current_stage": "execute", "next_stage": "verify", "decision": "continue"},
            "stage_history": [{"from_stage": "execute", "to_stage": "verify", "decision": "continue", "run_id": "prev-run"}],
            "last_decision": "continue",
        },
    )

    scaffold = background_job_executor.initialize_program_scaffold(
        job,
        run_context={"run_id": "new-run"},
    )

    assert scaffold["task_spec"]["stage"] == "verify"
    assert scaffold["task_spec"]["objective"] == "Existing objective"
    assert scaffold["task_spec"]["current_focus"] == "Write pilot summary."
    assert scaffold["task_spec"]["last_evaluation"]["next_stage"] == "verify"
    assert scaffold["task_spec"]["last_decision"] == "continue"
    assert scaffold["task_spec"]["iteration_count"] == 3
    assert scaffold["task_spec"]["stage_history"][0]["run_id"] == "prev-run"
    assert scaffold["task_spec"]["wake_policy"]["scheduled"] is False


def test_background_job_executor_implementation_loop_migrates_legacy_subgoals(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {"SP-EXEC-01": implementation_loop_spec()},
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    bundle = background_job_executor.handoff_bundle(job)
    background_job_executor.workspace_job_schema.write_json_file(
        bundle["task_spec_path"],
        {
            "task_id": "SP-EXEC-01",
            "program_id": job["program_spec"]["program_id"],
            "objective": "Existing objective",
            "scope_type": "project",
            "scope_ref": "SampleProj",
            "approval_required": False,
            "approval_state": "not-required",
            "subgoal_schema_version": 1,
            "stage": "frame",
            "stage_plan": ["discover", "frame", "execute", "verify", "adapt", "handoff"],
            "wake_policy": {"manual_wake": True, "scheduled": True},
            "iteration_count": 1,
            "current_focus": "Legacy research focus",
            "subgoals": [{"summary": "Legacy research focus", "status": "pending"}],
            "updated_at": "2026-03-28T12:00:00+08:00",
            "last_run_id": "prev-run",
            "last_evaluation": {"current_stage": "discover", "next_stage": "frame", "decision": "continue"},
            "stage_history": [],
            "last_decision": "continue",
        },
    )

    payload = background_job_executor.execute_projected_job(job, trigger_source="manual_cli")

    assert payload["ok"] is True
    task_spec = json.loads(Path(payload["run_record"]["artifacts"]["task_spec_path"]).read_text(encoding="utf-8"))
    assert task_spec["subgoal_schema_version"] == 2
    assert task_spec["stage"] == "execute"
    assert task_spec["current_focus"] == "IM contract"
    assert task_spec["subgoals"][0]["summary"] == "IM contract"
    assert task_spec["subgoals"][0]["status"] == "pending"
    latest_report = Path(payload["log_paths"]["latest_path"]).read_text(encoding="utf-8")
    assert "# 后台实施报告｜SP-EXEC-01" in latest_report


def test_evaluate_program_iteration_keeps_subgoal_pending_until_verify(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    evaluation, updated_subgoals = background_job_executor.evaluate_program_iteration(
        {"task_id": "SP-EXEC-01"},
        scaffold={
            "task_spec": {
                "stage": "execute",
                "current_focus": "IM contract",
                "subgoals": [
                    {"summary": "IM contract", "status": "pending"},
                    {"summary": "Calendar contract", "status": "pending"},
                ],
                "scope_type": "project",
                "scope_ref": "SampleProj",
            }
        },
        execution_status="ok",
        delivery_status="delivered",
        gate_state={"status": "approved"},
        focus_completed=False,
    )

    assert evaluation["decision"] == "continue"
    assert evaluation["next_stage"] == "verify"
    assert updated_subgoals[0]["status"] == "pending"
    assert updated_subgoals[1]["status"] == "pending"


def test_run_implementation_loop_marks_focus_complete_only_after_verify(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    def _fake_verify(_command_spec: dict[str, object]) -> dict[str, object]:
        return {
            "label": "verify-im",
            "command": "pytest sample",
            "status": "ok",
            "exit_code": 0,
            "duration_ms": 12,
            "stdout_tail": "passed",
            "stderr_tail": "",
        }

    monkeypatch.setattr(background_job_executor, "_run_verify_command", _fake_verify)

    payload = background_job_executor.run_implementation_loop(
        {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "max_rounds": 3,
            "acceptance_criteria": ["Close the sample capability loop."],
            "implementation_tracks": implementation_loop_spec()["implementation_tracks"],
            "current_focus": "IM contract",
            "program_spec": {"stage": "verify"},
            "task_spec": {
                "subgoals": [
                    {"summary": "IM contract", "status": "pending"},
                    {"summary": "Calendar contract", "status": "pending"},
                ]
            },
        }
    )

    assert payload["phase"] == "implementation-v1"
    assert payload["execution_status"] == "ok"
    assert payload["focus_completed"] is True
    assert payload["implementation_payload"]["verification_results"][0]["status"] == "ok"
    assert "Calendar contract" in payload["next_action"]


def test_run_execution_packet_codex_exec_routes_through_local_broker(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    class _Completed:
        returncode = 0
        stdout = json.dumps({"ok": True, "action": "codex-exec"}, ensure_ascii=False)
        stderr = ""

    observed: dict[str, object] = {}

    def _fake_run(command, cwd=None, capture_output=None, text=None, timeout=None, check=None):
        observed["command"] = command
        observed["cwd"] = cwd
        observed["timeout"] = timeout
        return _Completed()

    monkeypatch.setattr(background_job_executor.subprocess, "run", _fake_run)

    result = background_job_executor._run_execution_packet(
        {
            "label": "Implement pointer fallback",
            "kind": "codex_exec",
            "project_name": "SampleProj",
            "source": "background_job_harness",
            "prompt": "Implement pointer fallback",
            "timeout_seconds": 120,
        },
        {"project_name": "SampleProj"},
    )

    assert result["status"] == "ok"
    assert result["broker_result"]["ok"] is True
    assert observed["command"][:4] == [
        background_job_executor.sys.executable,
        str(background_job_executor.REPO_ROOT / "ops" / "local_broker.py"),
        "command-center",
        "--action",
    ]
    assert observed["cwd"] == str(background_job_executor.workspace_root())


def test_run_execution_packet_engine_exec_routes_through_local_broker(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    class _Completed:
        returncode = 0
        stdout = json.dumps({"ok": True, "action": "engine-exec"}, ensure_ascii=False)
        stderr = ""

    observed: dict[str, object] = {}

    def _fake_run(command, cwd=None, capture_output=None, text=None, timeout=None, check=None):
        observed["command"] = command
        observed["cwd"] = cwd
        observed["timeout"] = timeout
        return _Completed()

    monkeypatch.setattr(background_job_executor.subprocess, "run", _fake_run)

    result = background_job_executor._run_execution_packet(
        {
            "label": "Implement Claude adapter",
            "kind": "engine_exec",
            "engine_name": "claude",
            "entry_surface": "codepilot",
            "project_name": "SampleProj",
            "source": "background_job_harness",
            "prompt": "Implement Claude adapter",
            "timeout_seconds": 120,
        },
        {"project_name": "SampleProj"},
    )

    assert result["status"] == "ok"
    assert result["broker_result"]["ok"] is True
    assert "--engine-name" in observed["command"]
    assert "claude" in observed["command"]
    assert "--entry-surface" in observed["command"]
    assert "codepilot" in observed["command"]
    assert observed["cwd"] == str(background_job_executor.workspace_root())


def test_run_execution_packet_codex_exec_marks_timeout(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    def _fake_run(*_args, **_kwargs):
        raise subprocess.TimeoutExpired(cmd="codex-exec", timeout=120, stderr="timed out")

    monkeypatch.setattr(background_job_executor.subprocess, "run", _fake_run)

    result = background_job_executor._run_execution_packet(
        {
            "label": "Implement timeout recovery",
            "kind": "codex_exec",
            "project_name": "SampleProj",
            "source": "background_job_harness",
            "prompt": "Implement timeout recovery",
            "timeout_seconds": 120,
        },
        {"project_name": "SampleProj"},
    )

    assert result["status"] == "timeout"
    assert result["timed_out"] is True
    assert result["error_type"] == "command_timeout"


def test_run_implementation_loop_execute_runs_execution_packets(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    def _fake_execute(_packet: dict[str, object], _job: dict[str, object]) -> dict[str, object]:
        return {
            "label": "execute-im",
            "kind": "shell",
            "status": "ok",
            "exit_code": 0,
            "duration_ms": 10,
            "stdout_tail": "done",
            "stderr_tail": "",
        }

    monkeypatch.setattr(background_job_executor, "_run_execution_packet", _fake_execute)

    payload = background_job_executor.run_implementation_loop(
        {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "max_rounds": 3,
            "acceptance_criteria": ["Close the sample capability loop."],
            "implementation_tracks": [
                {
                    "subgoal_id": "im-contract",
                    "summary": "IM contract",
                    "current_truth": ["IM baseline exists."],
                    "target_files": ["/tmp/im.py"],
                    "execute_actions": ["Advance IM contract."],
                    "execution_packets": [{"label": "execute-im", "kind": "shell", "command": "echo ok"}],
                    "verify_commands": [{"label": "verify-im", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                    "adapt_actions": ["Narrow the IM fix."],
                }
            ],
            "current_focus": "IM contract",
            "program_spec": {"stage": "execute"},
            "task_spec": {"subgoals": [{"summary": "IM contract", "status": "pending"}]},
        }
    )

    assert payload["execution_status"] == "ok"
    assert payload["focus_completed"] is False
    assert payload["implementation_payload"]["execution_results"][0]["status"] == "ok"
    assert "进入 verify" in payload["next_action"]


def test_run_implementation_loop_execute_enters_adapt_on_execution_failure(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    def _fake_execute(_packet: dict[str, object], _job: dict[str, object]) -> dict[str, object]:
        return {
            "label": "execute-im",
            "kind": "shell",
            "status": "error",
            "exit_code": 1,
            "duration_ms": 10,
            "stdout_tail": "",
            "stderr_tail": "boom",
        }

    monkeypatch.setattr(background_job_executor, "_run_execution_packet", _fake_execute)

    payload = background_job_executor.run_implementation_loop(
        {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "max_rounds": 3,
            "acceptance_criteria": ["Close the sample capability loop."],
            "implementation_tracks": [
                {
                    "subgoal_id": "im-contract",
                    "summary": "IM contract",
                    "current_truth": ["IM baseline exists."],
                    "target_files": ["/tmp/im.py"],
                    "execute_actions": ["Advance IM contract."],
                    "execution_packets": [{"label": "execute-im", "kind": "shell", "command": "false"}],
                    "verify_commands": [{"label": "verify-im", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                    "adapt_actions": ["Narrow the IM fix."],
                }
            ],
            "current_focus": "IM contract",
            "program_spec": {"stage": "execute"},
            "task_spec": {"subgoals": [{"summary": "IM contract", "status": "pending"}]},
        }
    )

    assert payload["execution_status"] == "error"
    assert "进入 adapt" in payload["next_action"]
    assert "执行失败" in payload["implementation_payload"]["adaptation_notes"][0]


def test_run_implementation_loop_execute_requests_retry_on_timeout(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    def _fake_execute(_packet: dict[str, object], _job: dict[str, object]) -> dict[str, object]:
        return {
            "label": "execute-im",
            "kind": "codex_exec",
            "status": "timeout",
            "exit_code": None,
            "duration_ms": 10,
            "stdout_tail": "",
            "stderr_tail": "TIMEOUT after 120s",
            "timed_out": True,
            "error_type": "command_timeout",
        }

    monkeypatch.setattr(background_job_executor, "_run_execution_packet", _fake_execute)

    payload = background_job_executor.run_implementation_loop(
        {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "max_rounds": 3,
            "acceptance_criteria": ["Close the sample capability loop."],
            "implementation_tracks": [
                {
                    "subgoal_id": "im-contract",
                    "summary": "IM contract",
                    "current_truth": ["IM baseline exists."],
                    "target_files": ["/tmp/im.py"],
                    "execute_actions": ["Advance IM contract."],
                    "execution_packets": [{"label": "execute-im", "kind": "codex_exec", "prompt": "do it"}],
                    "verify_commands": [{"label": "verify-im", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                    "adapt_actions": ["Narrow the IM fix."],
                }
            ],
            "current_focus": "IM contract",
            "program_spec": {"stage": "execute"},
            "task_spec": {"subgoals": [{"summary": "IM contract", "status": "pending"}]},
        }
    )

    assert payload["execution_status"] == "error"
    assert payload["execution_metadata"]["recovery_decision"] == "retry"
    assert "保持 execute" in payload["next_action"]


def test_run_implementation_loop_adapt_recovers_when_existing_artifact_verifies(sample_env, monkeypatch, tmp_path) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    target = tmp_path / "open-agent-sdk-typescript-review.md"
    target.write_text(
        "## 结论\n\n## SDK 能力模型\n\n## 集成成本与约束\n",
        encoding="utf-8",
    )

    def _fake_verify(_command_spec: dict[str, object]) -> dict[str, object]:
        return {
            "label": "verify-sdk",
            "command": "python3 -c \"print('ok')\"",
            "status": "ok",
            "exit_code": 0,
            "duration_ms": 9,
            "stdout_tail": "ok",
            "stderr_tail": "",
        }

    monkeypatch.setattr(background_job_executor, "_run_verify_command", _fake_verify)

    payload = background_job_executor.run_implementation_loop(
        {
            "task_id": "SP-EXEC-01",
            "task_item": "Run Sample background job",
            "max_rounds": 3,
            "acceptance_criteria": ["Close the sample capability loop."],
            "implementation_tracks": [
                {
                    "subgoal_id": "sdk-contract",
                    "summary": "SDK contract",
                    "current_truth": ["SDK review exists."],
                    "target_files": [str(target)],
                    "execute_actions": ["Advance SDK review."],
                    "execution_packets": [{"label": "execute-sdk", "kind": "codex_exec", "prompt": "do it"}],
                    "verify_commands": [{"label": "verify-sdk", "command": "python3 -c \"print('ok')\"", "timeout_seconds": 10}],
                    "adapt_actions": ["Reuse the existing SDK review if it already verifies."],
                }
            ],
            "current_focus": "SDK contract",
            "program_spec": {"stage": "adapt"},
            "task_spec": {"subgoals": [{"summary": "SDK contract", "status": "pending"}]},
        }
    )

    assert payload["execution_status"] == "ok"
    assert payload["focus_completed"] is True
    assert payload["implementation_payload"]["verification_results"][0]["status"] == "ok"
    assert "恢复成功" in payload["implementation_payload"]["headline"]
    assert "通过 verify" in payload["next_action"]


def test_evaluate_program_iteration_retry_keeps_stage(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)

    evaluation, updated_subgoals = background_job_executor.evaluate_program_iteration(
        {"task_id": "SP-EXEC-01"},
        scaffold={
            "task_spec": {
                "stage": "execute",
                "scope_type": "project",
                "scope_ref": "SampleProj",
                "current_focus": "IM contract",
                "subgoals": [{"summary": "IM contract", "status": "pending"}],
            }
        },
        execution_status="error",
        delivery_status="delivered",
        gate_state={},
        focus_completed=False,
        execution_metadata={"recovery_decision": "retry", "recovery_reason": "execution_timeout"},
    )

    assert evaluation["decision"] == "retry"
    assert evaluation["next_stage"] == "execute"
    assert evaluation["metadata"]["execution_recovery_reason"] == "execution_timeout"
    assert updated_subgoals[0]["status"] == "pending"


def test_derive_harness_observability_marks_stale_running(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    stale_claimed_at = (
        dt.datetime.now().astimezone()
        - dt.timedelta(seconds=background_job_executor.workspace_wake_broker.RUNNING_STALE_SECONDS + 5)
    ).isoformat(timespec="seconds")

    payload = background_job_executor.derive_harness_observability(
        {
            "job_id": "job.sample",
            "task_id": "SP-EXEC-01",
            "project_name": "SampleProj",
            "artifacts_root": str(sample_env["reports_root"] / "sample"),
        },
        task_spec={
            "stage": "execute",
            "last_decision": "continue",
            "subgoals": [{"summary": "IM contract", "status": "pending"}],
            "current_focus": "IM contract",
            "last_run_id": "run-1",
        },
        wake_status={"running": {"claimed_at": stale_claimed_at}},
        last_run={},
        gate_event={},
    )

    assert payload["harness_state"] == "stalled"
    assert payload["blocked_reason"] == "wake_running_stale"
    assert payload["running_stale"] is True


def test_derive_harness_observability_marks_orphaned_active_run(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    stale_started_at = (
        dt.datetime.now().astimezone()
        - dt.timedelta(seconds=background_job_executor.workspace_wake_broker.RUNNING_STALE_SECONDS + 5)
    ).isoformat(timespec="seconds")

    payload = background_job_executor.derive_harness_observability(
        {
            "job_id": "job.sample",
            "task_id": "SP-EXEC-01",
            "project_name": "SampleProj",
            "artifacts_root": str(sample_env["reports_root"] / "sample"),
        },
        task_spec={
            "stage": "execute",
            "last_decision": "",
            "subgoals": [{"summary": "IM contract", "status": "pending"}],
            "current_focus": "IM contract",
            "last_run_id": "run-1",
            "active_run_id": "run-2",
            "running_started_at": stale_started_at,
        },
        wake_status={"running": {}},
        last_run={},
        gate_event={},
    )

    assert payload["harness_state"] == "stalled"
    assert payload["blocked_reason"] == "orphaned_active_run"
    assert payload["running_stale"] is True
    assert payload["last_decision"] == "retry"


def test_projected_job_status_exposes_observability(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-01",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Run Sample background job",
                "状态": "doing",
                "交付物": "sample brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "continue",
                "更新时间": "2026-03-30",
                "指向": "SampleProj-项目板.md",
            }
        ],
    )
    monkeypatch.setattr(
        background_job_executor.board_job_projector,
        "load_task_job_specs",
        lambda _project_name: {"SP-EXEC-01": implementation_loop_spec()},
    )
    job = background_job_executor.board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    monkeypatch.setattr(
        background_job_executor,
        "_load_task_spec",
        lambda _job: {
            "stage": "execute",
            "last_decision": "continue",
            "subgoals": [{"summary": "IM contract", "status": "pending"}],
            "current_focus": "IM contract",
            "last_run_id": "run-1",
        },
    )
    monkeypatch.setattr(
        background_job_executor.workspace_wake_broker,
        "job_status",
        lambda _job_id: {"pending": {"requested_at": "2026-03-31T01:00:00+08:00"}, "running": {}},
    )
    monkeypatch.setattr(
        background_job_executor,
        "latest_run_record",
        lambda _job: {
            "run_id": "run-1",
            "delivery_status": "delivered",
            "writeback_targets": ["project_board", "dashboard"],
            "delivery_outcomes": [
                {"delivery_id": "d-1", "status": "delivered", "targets": ["Codex Hub项目"]},
            ],
            "artifacts": {"history_path": "/tmp/history.ndjson", "gates_path": "/tmp/gates.ndjson"},
        },
    )
    monkeypatch.setattr(background_job_executor, "latest_gate_event", lambda _job: {})
    monkeypatch.setattr(
        background_job_executor.runtime_state,
        "fetch_bridge_connection",
        lambda _bridge: {
            "status": "connected",
            "transport": "lark_cli_event_plus_cli_im",
            "last_event_at": "2026-03-31T01:05:00+08:00",
            "last_error": "",
            "host_mode": "daemon",
            "updated_at": "2026-03-31T01:05:10+08:00",
        },
    )
    monkeypatch.setattr(
        background_job_executor.runtime_state,
        "fetch_bridge_message_activity",
        lambda _bridge: {
            "inbound": {"message_id": "im-1", "cursor_at": "2026-03-31T01:04:00+08:00"},
            "outbound": {"message_id": "om-1", "cursor_at": "2026-03-31T01:04:30+08:00"},
        },
    )
    monkeypatch.setattr(
        background_job_executor.runtime_state,
        "fetch_bridge_continuity_status",
        lambda **_kwargs: {"ok": True, "issue_count": 0, "shared_session_count": 1},
    )
    monkeypatch.setattr(
        background_job_executor.runtime_state,
        "fetch_runtime_queue_status",
        lambda queue_name: {"counts": {queue_name: {"pending": 0, "processing": 0}}, "latest_created_at": ""},
    )

    payload = background_job_executor.projected_job_status(job)

    assert payload["harness_state"] == "queued"
    assert payload["last_decision"] == "continue"
    assert payload["next_wake_at"] == "2026-03-31T01:00:00+08:00"
    assert payload["blocked_reason"] == ""
    assert payload["observability"]["harness_state"] == "queued"
    assert payload["observability"]["next_wake_at"] == "2026-03-31T01:00:00+08:00"
    assert payload["observability"]["pending_subgoal_count"] == 1
    assert payload["status_payload"]["harness_state"] == "queued"
    assert payload["status_payload"]["last_decision"] == "continue"
    assert payload["status_payload"]["pending_subgoal_count"] == 1
    assert payload["runtime_overlay"]["task_id"] == "SP-EXEC-01"
    assert payload["runtime_overlay"]["current_stage"] == "execute"
    assert payload["task_runtime_snapshot"]["harness_state"] == "queued"
    assert payload["compression_policy"]["l1_strategy"] == "tool-output-trim"
    assert payload["middleware"]["precompletion_checklist"]["status"] == "armed"
    assert payload["handoff_packet"]["task_spec_path"].endswith("task-spec.json")
    assert payload["project_runtime"]["task_status"] == "doing"
    assert payload["bridge_runtime"]["status"] == "connected"
    assert payload["run_tree"]["run_id"] == "run-1"
    assert payload["delivery_contract"]["aggregate_status"] == "delivered"
    assert payload["execution_boundary"]["sandbox_mode"] == "workspace_write"
    assert payload["instruction_surface"]["hook_enforcement"][0] == "pre_completion_checklist"
    assert payload["extension_manifest"]["kind"] == "workflow"
    assert payload["extension_manifest"]["lifecycle_state"] == "enabled"
    assert "workflow_manifest" in payload["extension_manifest"]["capabilities"]
    assert payload["workflow_manifest"]["entry_command"] == "background-job-intent"
    assert payload["workflow_manifest"]["lifecycle_state"] == "loaded"
    assert "runtime_overlay" in payload["extension_manifest"]["capabilities"]
    assert payload["instruction_migration"]["migrate_to_policy"][0] == "approval_gate"
    assert "extension_manifest" in payload["open_source_boundary"]["public_contracts"]
    assert "workflow_manifest" in payload["open_source_boundary"]["public_contracts"]
    assert payload["harness_snapshot"]["runtime_overlay"]["task_id"] == "SP-EXEC-01"


def test_job_status_payload_flattens_harness_observability(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    job = {
        "project_name": "SampleProj",
        "task_id": "SP-EXEC-01",
        "job_id": "board-job.sampleproj.sp-exec-01.sample-background-job",
        "task_item": "Run Sample background job",
        "executor_kind": "implementation_loop",
    }
    monkeypatch.setattr(
        background_job_executor,
        "projected_job_status",
        lambda _job: {
            "job": job,
            "observability": {
                "harness_state": "blocked",
                "last_decision": "blocked",
                "next_action": "先处理审批阻塞",
                "next_wake_at": "2026-03-31T09:30:00+08:00",
                "blocked_reason": "awaiting_gate",
                "current_stage": "verify",
                "current_focus": "IM contract",
                "last_run_id": "run-1",
                "active_run_id": "",
                "running_started_at": "",
            },
            "status_payload": {
                "harness_state": "blocked",
                "last_decision": "blocked",
                "next_action": "先处理审批阻塞",
                "next_wake_at": "2026-03-31T09:30:00+08:00",
                "blocked_reason": "awaiting_gate",
                "current_stage": "verify",
                "current_focus": "IM contract",
                "last_run_id": "run-1",
                "active_run_id": "",
                "running_started_at": "",
                "completed_subgoal_count": 0,
                "pending_subgoal_count": 1,
                "running_stale": False,
                "history_path": "/tmp/history.ndjson",
                "gates_path": "/tmp/gates.ndjson",
                "latest_report_path": "/tmp/latest.md",
            },
            "runtime_overlay": {
                "task_id": "SP-EXEC-01",
                "current_stage": "verify",
                "current_focus": "IM contract",
                "run_id": "run-1",
            },
            "compression_policy": {
                "l1_strategy": "tool-output-trim",
                "l2_strategy": "session-summary",
                "l3_strategy": "handoff-summary",
            },
            "middleware": {
                "precompletion_checklist": {"status": "armed"},
                "loop_detection": {"status": "watching"},
                "local_context": {"allow_paths": ["/tmp/sample"]},
            },
            "task_runtime_snapshot": {
                "harness_state": "blocked",
                "completed_subgoal_count": 0,
                "pending_subgoal_count": 1,
                "active_run_id": "",
            },
            "handoff_packet": {"task_spec_path": "/tmp/task-spec.json"},
            "project_runtime": {
                "task_status": "doing",
                "next_action": "先处理审批阻塞",
                "updated_at": "2026-03-31T09:00:00+08:00",
            },
            "bridge_runtime": {
                "status": "connected",
                "transport": "lark_cli_event_plus_cli_im",
                "continuity_issue_count": 0,
            },
            "run_tree": {"run_id": "run-1", "children": [], "shared_artifacts": [{"artifact_id": "task_spec_path"}]},
            "delivery_contract": {
                "aggregate_status": "delivered",
                "writeback_targets": ["project_board"],
                "pending_targets": [],
                "failed_targets": [],
            },
            "execution_boundary": {
                "sandbox_mode": "workspace_write",
                "network_access": "conditional",
                "writable_roots": ["/tmp/a", "/tmp/b"],
            },
            "instruction_surface": {
                "human_guides": ["/tmp/AGENTS.md", "/tmp/skills"],
                "generated_rules": ["/tmp/generated.rules"],
                "hook_enforcement": ["pre_completion_checklist", "loop_detection"],
                "policy_enforcement": ["operation_policy", "execution_boundary"],
            },
            "extension_manifest": {
                "kind": "workflow",
                "lifecycle_state": "enabled",
                "capabilities": ["runtime_overlay", "extension_manifest"],
                "hook_subscriptions": ["run_started", "delivery_done"],
                "supported_profiles": ["workspace-default", "background-job"],
            },
            "workflow_manifest": {
                "entry_command": "background-job-intent",
                "lifecycle_state": "paused",
                "trigger_modes": ["explicit_intent", "wake_broker"],
                "status_surfaces": ["job_status_payload", "materials_dashboard"],
            },
            "instruction_migration": {
                "retained_in_guides": ["workspace invariants"],
                "migrate_to_hooks": ["pre_completion_checklist"],
                "migrate_to_policy": ["approval_gate", "execution_boundary"],
                "migrate_to_commands": ["continue long task", "pause long task"],
            },
            "open_source_boundary": {
                "public_contracts": ["runtime_overlay", "extension_manifest"],
                "private_only": ["growth_operator_surface"],
                "migration_sequence": ["private_mainline_contract_freeze", "public_snapshot_status"],
                "not_recommended": ["raw_operator_playbooks"],
            },
            "harness_snapshot": {
                "runtime_overlay": {"task_id": "SP-EXEC-01"},
            },
            "harness_state": "blocked",
            "last_decision": "blocked",
            "next_wake_at": "2026-03-31T09:30:00+08:00",
            "blocked_reason": "awaiting_gate",
            "history_path": "/tmp/history.ndjson",
            "gates_path": "/tmp/gates.ndjson",
            "latest_report_path": "/tmp/latest.md",
        },
    )

    payload = background_job_executor.job_status_payload(job)

    assert payload["task_id"] == "SP-EXEC-01"
    assert payload["harness_state"] == "blocked"
    assert payload["last_decision"] == "blocked"
    assert payload["next_action"] == "先处理审批阻塞"
    assert payload["next_wake_at"] == "2026-03-31T09:30:00+08:00"
    assert payload["blocked_reason"] == "awaiting_gate"
    assert payload["current_focus"] == "IM contract"
    assert payload["pending_subgoal_count"] == 1
    assert payload["running_stale"] is False
    assert payload["runtime_overlay"]["current_stage"] == "verify"
    assert payload["compression_policy"]["l2_strategy"] == "session-summary"
    assert payload["middleware"]["loop_detection"]["status"] == "watching"
    assert payload["extension_manifest"]["lifecycle_state"] == "enabled"
    assert payload["workflow_manifest"]["lifecycle_state"] == "paused"


def test_build_workflow_manifest_tracks_lifecycle_states(sample_env) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    job = {
        "project_name": "SampleProj",
        "task_id": "SP-EXEC-01",
        "executor_kind": "implementation_loop",
    }
    extension_manifest = {"extension_id": "codex-hub.background-job.SP-EXEC-01"}

    loaded = background_job_executor._build_workflow_manifest(
        job,
        observability={"harness_state": "ready", "last_decision": "continue", "blocked_reason": ""},
        project_runtime={"task_status": "doing"},
        extension_manifest=extension_manifest,
    )
    paused = background_job_executor._build_workflow_manifest(
        job,
        observability={"harness_state": "blocked", "last_decision": "blocked", "blocked_reason": "awaiting_gate"},
        project_runtime={"task_status": "doing"},
        extension_manifest=extension_manifest,
    )
    unloaded = background_job_executor._build_workflow_manifest(
        job,
        observability={"harness_state": "done", "last_decision": "done", "blocked_reason": ""},
        project_runtime={"task_status": "done"},
        extension_manifest=extension_manifest,
    )
    errored = background_job_executor._build_workflow_manifest(
        job,
        observability={"harness_state": "failed", "last_decision": "failed", "blocked_reason": "verify_failed"},
        project_runtime={"task_status": "doing"},
        extension_manifest=extension_manifest,
    )

    assert loaded["lifecycle_state"] == "loaded"
    assert paused["lifecycle_state"] == "paused"
    assert unloaded["lifecycle_state"] == "unloaded"
    assert errored["lifecycle_state"] == "errored"


def test_projected_job_status_execution_boundary_uses_program_spec(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    job = {
        "project_name": "SampleProj",
        "task_id": "SP-EXEC-01",
        "job_id": "board-job.sampleproj.sp-exec-01.sample-background-job",
        "task_item": "Run Sample background job",
        "executor_kind": "implementation_loop",
        "artifacts_root": "/tmp/sample-artifacts",
        "program_spec": {
            "scope_type": "workspace",
            "scope_ref": "SampleProj",
            "approval_required": True,
            "approval_state": "pending",
        },
    }
    monkeypatch.setattr(background_job_executor.workspace_wake_broker, "job_status", lambda _job_id: {})
    monkeypatch.setattr(background_job_executor, "latest_run_record", lambda _job: {})
    monkeypatch.setattr(background_job_executor, "latest_gate_event", lambda _job: {})
    monkeypatch.setattr(background_job_executor, "_load_task_spec", lambda _job: {})
    monkeypatch.setattr(
        background_job_executor,
        "derive_harness_observability",
        lambda *_args, **_kwargs: {
            "harness_state": "ready",
            "last_decision": "continue",
            "next_action": "继续执行",
            "next_wake_at": "",
            "blocked_reason": "",
            "current_stage": "execute",
            "current_focus": "runtime contract",
            "last_run_id": "",
            "completed_subgoal_count": 0,
            "pending_subgoal_count": 1,
            "running_stale": False,
        },
    )
    monkeypatch.setattr(background_job_executor, "_build_project_runtime_snapshot", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(background_job_executor, "_build_bridge_runtime_snapshot", lambda **_kwargs: {})
    monkeypatch.setattr(background_job_executor, "_build_delivery_contract", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(background_job_executor, "_build_run_tree", lambda *_args, **_kwargs: {})

    payload = background_job_executor.projected_job_status(job)

    assert payload["execution_boundary"]["requires_approval"] is True
    assert payload["execution_boundary"]["expected_scope"] == "SampleProj"
    assert payload["execution_boundary"]["metadata"]["approval_state"] == "pending"


def test_background_job_executor_blocks_workspace_scope_without_approval(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
                "scope_type": "workspace",
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")

    try:
        background_job_executor.execute_projected_job(job, trigger_source="manual_cli")
    except ValueError as exc:
        assert "requires explicit approval" in str(exc)
    else:
        raise AssertionError("expected workspace scope approval failure")


def test_background_job_executor_unapproved_workspace_scope_does_not_persist_scaffold(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
                "scope_type": "workspace",
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    bundle = background_job_executor.handoff_bundle(job)

    with pytest.raises(ValueError, match="requires explicit approval"):
        background_job_executor.execute_projected_job(job, trigger_source="manual_cli")

    assert not Path(bundle["task_spec_path"]).exists()
    assert not Path(bundle["acceptance_path"]).exists()
    assert not Path(bundle["progress_path"]).exists()
    assert not Path(bundle["latest_smoke_path"]).exists()


def test_background_job_executor_blocks_project_writeback_wake_when_policy_disables_it(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
                "wake_policy": {
                    "mode": "scheduled_or_event",
                    "scheduled": True,
                    "project_writeback": False,
                    "manual_wake": True,
                    "wake_catchup": True,
                },
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="project_writeback")

    assert wake["accepted"] is False
    assert wake["reason"] == "wake_policy_blocked"
    assert wake["policy"]["project_writeback"] is False


def test_background_job_executor_task_wake_rejects_unapproved_workspace_scope(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
                "scope_type": "workspace",
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")

    assert wake["accepted"] is False
    assert wake["reason"] == "workspace_scope_requires_approval"


def test_background_job_executor_project_wake_skips_unapproved_workspace_scope(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
                "scope_type": "workspace",
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_project_wake("SampleProj", reason="manual_wake")

    assert wake["accepted"] is False
    assert wake["reason"] == "no_eligible_program"
    assert wake["blocked_jobs"] == [{"task_id": "SP-EXEC-01", "reason": "workspace_scope_requires_approval"}]


def test_background_job_executor_project_wake_prefers_doing_tasks(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-01",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Todo background job",
                "状态": "todo",
                "交付物": "todo brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "todo action",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
            {
                "ID": "SP-EXEC-02",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Doing background job",
                "状态": "doing",
                "交付物": "doing brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "doing action",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "todo-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a todo brief."],
            },
            "SP-EXEC-02": {
                "job_slug": "doing-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a doing brief."],
            },
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_project_wake("SampleProj", reason="manual_wake")

    assert wake["accepted"] is True
    assert wake["selected_task_id"] == "SP-EXEC-02"
    assert wake["selected_job_id"] == "board-job.sampleproj.sp-exec-02.doing-background-job"


def test_background_job_executor_run_project_skips_busy_job(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    save_project_rows(
        sample_env,
        [
            {
                "ID": "SP-EXEC-01",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Doing background job",
                "状态": "doing",
                "交付物": "doing brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "doing action",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
            {
                "ID": "SP-EXEC-02",
                "父ID": "",
                "来源": "project",
                "范围": "automation",
                "事项": "Todo background job",
                "状态": "todo",
                "交付物": "todo brief",
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": "todo action",
                "更新时间": "2026-03-25",
                "指向": "SampleProj-项目板.md",
            },
        ],
    )
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "doing-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a doing brief."],
            },
            "SP-EXEC-02": {
                "job_slug": "todo-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a todo brief."],
            },
        },
    )
    background_job_executor = importlib.reload(executor_module)

    busy_wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")
    assert busy_wake["accepted"] is True
    claimed = background_job_executor.workspace_wake_broker.claim_wake("board-job.sampleproj.sp-exec-01.doing-background-job")
    assert claimed["claimed"] is True

    payload = background_job_executor.run_requested_project_wake("SampleProj", reason="manual_wake", dry_run=True)

    assert payload["executed"] is True
    assert payload["selected_task_id"] == "SP-EXEC-02"
    assert payload["selected_job_id"] == "board-job.sampleproj.sp-exec-02.todo-background-job"
    assert {"task_id": "SP-EXEC-01", "reason": "wake_in_flight"} in payload["blocked_jobs"]
    assert payload["payload"]["executed"] is True
    assert payload["payload"]["payload"]["dry_run"] is True


def test_background_job_executor_run_project_returns_false_when_nothing_claims(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")
    assert wake["accepted"] is True
    claimed = background_job_executor.workspace_wake_broker.claim_wake("board-job.sampleproj.sp-exec-01.sample-background-job")
    assert claimed["claimed"] is True

    payload = background_job_executor.run_requested_project_wake("SampleProj", reason="manual_wake", dry_run=True)

    assert payload["executed"] is False
    assert payload["reason"] == "no_claimable_program"
    assert {"task_id": "SP-EXEC-01", "reason": "wake_in_flight"} in payload["blocked_jobs"]


def test_background_job_executor_claims_and_completes_wake(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")
    assert wake["accepted"] is True

    payload = background_job_executor.run_requested_task("SampleProj", "SP-EXEC-01")

    assert payload["executed"] is True
    assert payload["payload"]["run_record"]["execution_outcome"]["status"] == "ok"
    status = background_job_executor.workspace_wake_broker.job_status(payload["payload"]["job"]["job_id"])
    assert status["running"] == {}
    assert status["last_completed"]["status"] == "succeeded"


def test_background_job_executor_request_task_wake_migrates_legacy_wake_state(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    legacy_job_id = job["legacy_job_ids"][0]

    background_job_executor.workspace_wake_broker.request_wake(legacy_job_id, reason="manual_wake")
    claimed = background_job_executor.workspace_wake_broker.claim_wake(legacy_job_id)
    assert claimed["claimed"] is True
    background_job_executor.workspace_wake_broker.complete_wake(
        legacy_job_id,
        wake_id=claimed["wake"]["wake_id"],
        status="succeeded",
        result={"ok": True, "run_id": "legacy-run"},
    )

    wake = background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")
    state = background_job_executor.workspace_wake_broker.load_state()

    assert wake["accepted"] is True
    assert legacy_job_id not in state["jobs"]
    status = background_job_executor.workspace_wake_broker.job_status(job["job_id"])
    assert status["last_completed"]["result"]["run_id"] == "legacy-run"


def test_background_job_executor_accepts_legacy_job_id_in_approved_token(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module
    from ops import runtime_state

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report", "feishu:coco-private"],
                "gate_policy": "before_external_send",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    legacy_job_id = job["legacy_job_ids"][0]

    runtime_state.upsert_approval_token(
        token="legacy-approved-token",
        scope=background_job_executor.EXTERNAL_DELIVERY_SCOPE,
        status="approved",
        project_name="SampleProj",
        session_id="legacy-run",
        metadata={"job_id": legacy_job_id, "task_id": "SP-EXEC-01"},
    )

    validated = background_job_executor.validate_external_delivery_approval(job, "legacy-approved-token")
    updated = runtime_state.fetch_approval_token("legacy-approved-token")

    assert validated["metadata"]["job_id"] == job["job_id"]
    assert updated["metadata"]["job_id"] == job["job_id"]
    assert updated["metadata"]["job_id_aliases"] == [legacy_job_id]


def test_background_job_executor_migrates_legacy_artifacts_root(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    legacy_root = Path(job["legacy_artifacts_roots"][0])
    legacy_bundle = background_job_executor.workspace_job_schema.handoff_bundle_paths(legacy_root)
    background_job_executor.workspace_job_schema.write_json_file(
        legacy_bundle["task_spec_path"],
        {
            "task_id": "SP-EXEC-01",
            "program_id": "legacy-program",
            "objective": "Legacy objective",
            "scope_type": "project",
            "scope_ref": "SampleProj",
            "approval_required": False,
            "approval_state": "not-required",
            "stage": "verify",
            "stage_plan": ["discover", "frame", "execute", "verify", "adapt", "handoff"],
            "wake_policy": {"manual_wake": True},
            "iteration_count": 2,
            "current_focus": "",
            "subgoals": [{"summary": "Produce a sample brief.", "status": "completed"}],
            "updated_at": "2026-03-28T12:00:00+08:00",
            "last_run_id": "legacy-run",
            "last_evaluation": {"current_stage": "execute", "next_stage": "verify", "decision": "continue"},
            "stage_history": [{"from_stage": "execute", "to_stage": "verify", "decision": "continue", "run_id": "legacy-run"}],
            "last_decision": "continue",
        },
    )

    payload = background_job_executor.execute_projected_job(job, trigger_source="manual_cli")
    migrated_task_spec = json.loads(Path(job["handoff_bundle"]["task_spec_path"]).read_text(encoding="utf-8"))

    assert payload["ok"] is True
    assert not legacy_root.exists()
    assert migrated_task_spec["iteration_count"] == 3
    assert migrated_task_spec["stage_history"][0]["run_id"] == "legacy-run"


def test_background_job_executor_dry_run_does_not_persist_program_scaffold(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    payload = background_job_executor.execute_projected_job(job, trigger_source="manual_cli", dry_run=True)
    artifacts = payload["run_record"]["artifacts"]

    assert payload["dry_run"] is True
    assert not Path(artifacts["task_spec_path"]).exists()
    assert not Path(artifacts["acceptance_path"]).exists()
    assert not Path(artifacts["progress_path"]).exists()
    assert not Path(artifacts["latest_smoke_path"]).exists()
    assert not Path(artifacts["history_path"]).exists()


def test_background_job_executor_requests_gate_and_writes_board(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module
    from ops import codex_memory
    from ops import runtime_state

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report", "write_board"],
                "delivery_targets": ["board", "report", "feishu:coco-private"],
                "gate_policy": "before_external_send",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    sent_gate_cards: list[dict[str, object]] = []
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", **kwargs: sent_gate_cards.append(
            {
                "target": target,
                "text": text,
                "kwargs": kwargs,
            }
        )
        or {
            "ok": True,
            "kind": "interactive_card",
            "message_id": "om_gate_card",
            "receive_id_type": "open_id",
            "target": "ou_operator",
            "msg_type": kwargs.get("msg_type", "text"),
        },
    )

    payload = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert payload["gate_state"]["status"] == "awaiting_gate"
    assert payload["run_record"]["delivery_status"] == "unknown"
    feishu_delivery = next(item for item in payload["run_record"]["delivery_outcomes"] if item["delivery_id"] == "feishu-notify")
    assert feishu_delivery["status"] == "unknown"
    assert feishu_delivery["metadata"]["delivery_mode"] == "interactive_card"
    assert sent_gate_cards[0]["kwargs"]["msg_type"] == "interactive"
    assert sent_gate_cards[0]["kwargs"]["card"]["header"]["title"]["content"] == "CoCo 授权确认"
    token_item = runtime_state.fetch_approval_token(payload["gate_state"]["token"])
    assert token_item["metadata"]["approval_message_id"] == "om_gate_card"
    assert token_item["metadata"]["open_id"] == "ou_operator"
    board = codex_memory.load_project_board("SampleProj")
    row = board["project_rows"][0]
    assert row["状态"] == "doing"
    assert "latest.md" in row["交付物"]
    assert "批准 token" in row["下一步"]


def test_background_job_executor_delivers_after_approval(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report", "write_board"],
                "delivery_targets": ["board", "report", "feishu:coco-private"],
                "gate_policy": "before_external_send",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    created_docs: list[dict[str, object]] = []
    sent_payloads: list[dict[str, object]] = []
    monkeypatch.setattr(
        background_job_executor,
        "create_feishu_doc_target",
        lambda target, *, title, file_path: created_docs.append(
            {"target": target, "title": title, "file_path": file_path}
        )
        or {
            "ok": True,
            "kind": "doc",
            "target": target,
            "document_id": "doc_delivery",
            "url": "https://feishu.cn/docx/doc_delivery",
        },
    )
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", **kwargs: sent_payloads.append({"target": target, "text": text, "kwargs": kwargs})
        or {"ok": True, "target": target, "text": text, "msg_type": kwargs.get("msg_type", "text")},
    )
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    approval = background_job_executor.runtime_state.upsert_approval_token(
        token="bgate-approved",
        scope=background_job_executor.EXTERNAL_DELIVERY_SCOPE,
        status="approved",
        project_name="SampleProj",
        session_id="sess-approved",
        metadata={"job_id": job["job_id"]},
    )

    payload = background_job_executor.execute_projected_job(
        job,
        trigger_source="manual_cli",
        approval_token=approval["token"],
    )

    assert payload["gate_state"]["status"] == "approved"
    assert payload["run_record"]["delivery_status"] == "delivered"
    feishu_doc_delivery = next(item for item in payload["run_record"]["delivery_outcomes"] if item["delivery_id"] == "feishu-doc")
    feishu_delivery = next(item for item in payload["run_record"]["delivery_outcomes"] if item["delivery_id"] == "feishu-notify")
    assert feishu_doc_delivery["status"] == "delivered"
    assert feishu_doc_delivery["targets"] == ["https://feishu.cn/docx/doc_delivery"]
    assert feishu_delivery["status"] == "delivered"
    assert created_docs[0]["target"] == "feishu:coco-private"
    assert sent_payloads[0]["kwargs"].get("msg_type", "text") == "text"
    assert "批准 token" not in str(sent_payloads[0]["text"])
    assert "https://feishu.cn/docx/doc_delivery" in str(sent_payloads[0]["text"])
    assert "/latest.md" not in str(sent_payloads[0]["text"])


def test_create_feishu_doc_target_skips_chat_share_target(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    captured: dict[str, object] = {}

    def fake_create_doc(target: str, *, title: str, file_path: str) -> dict[str, str]:
        captured["target"] = target
        captured["title"] = title
        captured["file_path"] = file_path
        return {
            "ok": True,
            "kind": "doc",
            "target": target,
            "document_id": "doc_123",
            "url": "https://feishu.cn/docx/doc_123",
        }

    monkeypatch.setattr(background_job_executor.feishu_outbound_gateway, "create_doc", fake_create_doc)
    result = background_job_executor.create_feishu_doc_target(
        "feishu:chat:Growth System Updates",
        title="Demo Report",
        file_path="/tmp/demo.md",
    )

    assert result["document_id"] == "doc_123"
    assert captured["target"] == "feishu:chat:Growth System Updates"


def test_background_job_executor_payload_ok_tracks_failed_delivery(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report", "write_board"],
                "delivery_targets": ["board", "report", "feishu:coco-private"],
                "gate_policy": "before_external_send",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    monkeypatch.setattr(
        background_job_executor,
        "create_feishu_doc_target",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("doc create failed")),
    )
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", **kwargs: {"ok": True, "target": target, "text": text, "msg_type": kwargs.get("msg_type", "text")},
    )
    job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
    approval = background_job_executor.runtime_state.upsert_approval_token(
        token="bgate-failed-doc",
        scope=background_job_executor.EXTERNAL_DELIVERY_SCOPE,
        status="approved",
        project_name="SampleProj",
        session_id="sess-approved",
        metadata={"job_id": job["job_id"]},
    )

    payload = background_job_executor.execute_projected_job(
        job,
        trigger_source="manual_cli",
        approval_token=approval["token"],
    )

    assert payload["ok"] is False
    assert payload["run_record"]["ok"] is False
    feishu_doc_delivery = next(item for item in payload["run_record"]["delivery_outcomes"] if item["delivery_id"] == "feishu-doc")
    assert feishu_doc_delivery["status"] == "not-delivered"


def test_background_job_executor_run_requested_marks_failed_wake_on_exception(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    background_job_executor.request_task_wake("SampleProj", "SP-EXEC-01", reason="manual_wake")
    monkeypatch.setattr(
        background_job_executor,
        "execute_projected_job",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    try:
        background_job_executor.run_requested_task("SampleProj", "SP-EXEC-01")
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("expected RuntimeError")

    status = background_job_executor.workspace_wake_broker.job_status("board-job.sampleproj.sp-exec-01.sample-background-job")
    assert status["running"] == {}
    assert status["last_completed"]["status"] == "failed"
    assert "boom" in status["last_completed"]["result"]["error"]


def test_background_job_executor_runs_phase3_research_loop(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "browse", "write_report", "write_board"],
                "delivery_targets": ["board", "report"],
                "gate_policy": "none",
                "max_rounds": 3,
                "time_budget_minutes": 10,
                "acceptance_criteria": [
                    "Collect official sources.",
                    "Split replicable and non-replicable boundaries.",
                    "Produce a next-step route.",
                ],
                "research_sources": [
                    {
                        "source_id": "official-docs",
                        "kind": "url",
                        "title": "Official docs",
                        "url": "https://example.com/docs",
                        "lens": "replicable",
                        "expected_signal": "开放平台具备 API 与回调入口。",
                    },
                    {
                        "source_id": "native-platform",
                        "kind": "url",
                        "title": "Native platform",
                        "url": "https://example.com/native",
                        "lens": "native-boundary",
                        "expected_signal": "托管调试与运行额度属于平台能力。",
                    },
                ],
                "analysis_focus": [
                    "What can be replicated.",
                    "What stays platform-native.",
                ],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)

    def fake_fetch(source):
        return {
            "source_id": source["source_id"],
            "kind": source["kind"],
            "title": source["title"],
            "uri": source.get("url", ""),
            "lens": source["lens"],
            "expected_signal": source["expected_signal"],
            "excerpt": source["expected_signal"],
            "excerpt_lead": source["expected_signal"],
        }

    monkeypatch.setattr(background_job_executor, "_fetch_research_source", fake_fetch)

    payload = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert payload["run_record"]["metadata"]["phase"] == "phase-3"
    assert payload["research_payload"]["status"] == "research-report-ready"
    assert payload["run_record"]["execution_outcome"]["summary"] == "background research report prepared for SP-EXEC-01"
    report_text = Path(payload["log_paths"]["latest_path"]).read_text(encoding="utf-8")
    ops_report_text = Path(payload["log_paths"]["latest_ops_path"]).read_text(encoding="utf-8")
    assert "## 结论" in report_text
    assert "## 可复刻能力" in report_text
    assert "## 建议路线" in report_text
    assert "## Agent Loop" not in report_text
    assert "## Research Findings" in ops_report_text
    assert payload["research_paths"]["latest_path"].endswith("latest-research.json")


def test_background_job_executor_runs_growth_loop(sample_env, monkeypatch, tmp_path) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    offer_path = tmp_path / "offer.md"
    listing_path = tmp_path / "listing.md"
    offer_path.write_text("# Offer\n\n| offer_id |\n| --- |\n| CGS-OFFER-001 |\n", encoding="utf-8")
    listing_path.write_text("# Listing\n\n| listing_id |\n| --- |\n", encoding="utf-8")

    control = growth_control_for_sample_task()
    control["objects"] = {
        "Offer": {"table_path": str(offer_path)},
        "Listing": {"table_path": str(listing_path)},
    }

    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "load_growth_control", lambda: control)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})
    background_job_executor = importlib.reload(executor_module)

    payload = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert payload["ok"] is True
    assert payload["run_record"]["metadata"]["phase"] == "growth-v1"
    assert payload["run_record"]["execution_outcome"]["summary"] == "growth system report prepared for SP-EXEC-01"
    report_text = Path(payload["log_paths"]["latest_path"]).read_text(encoding="utf-8")
    ops_report_text = Path(payload["log_paths"]["latest_ops_path"]).read_text(encoding="utf-8")
    assert "# Growth System 报告｜SP-EXEC-01" in report_text
    assert "## 关键输出" in report_text
    assert "## 当前缺口" in report_text
    assert "## Growth Findings" in ops_report_text


def test_background_job_executor_growth_loop_directly_delivers_external_targets_without_gate(sample_env, monkeypatch, tmp_path) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    offer_path = tmp_path / "offer.md"
    listing_path = tmp_path / "listing.md"
    offer_path.write_text("# Offer\n\n| offer_id |\n| --- |\n| CGS-OFFER-001 |\n", encoding="utf-8")
    listing_path.write_text("# Listing\n\n| listing_id |\n| --- |\n", encoding="utf-8")

    control = growth_control_for_sample_task()
    control["workflow_specs"]["signal_scan"]["delivery_targets"] = [
        "board",
        "report",
        "feishu:chat:Growth System Updates",
    ]
    control["objects"] = {
        "Offer": {"table_path": str(offer_path)},
        "Listing": {"table_path": str(listing_path)},
    }

    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "load_growth_control", lambda: control)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})
    background_job_executor = importlib.reload(executor_module)
    sent_messages: list[dict[str, object]] = []
    sent_docs: list[dict[str, object]] = []
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", msg_type="text", card=None: sent_messages.append(
            {"target": target, "text": text, "msg_type": msg_type, "card": card}
        )
        or {"message_id": "msg-001", "target": target, "receive_id_type": "chat_id", "receive_id": "chat-001"},
    )
    monkeypatch.setattr(
        background_job_executor,
        "create_feishu_doc_target",
        lambda target, title, file_path: sent_docs.append(
            {"target": target, "title": title, "file_path": file_path}
        )
        or {"url": "https://feishu.example/doc/001", "document_id": "doc-001"},
    )

    payload = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert payload["ok"] is True
    assert payload["gate_state"]["status"] == "approved"
    assert payload["gate_state"]["summary"] == "无需审批，直接外发"
    assert any(item["delivery_id"] == "feishu-doc" and item["status"] == "delivered" for item in payload["run_record"]["delivery_outcomes"])
    assert any(item["delivery_id"] == "feishu-notify" and item["status"] == "delivered" for item in payload["run_record"]["delivery_outcomes"])
    assert sent_docs
    assert sent_messages


def test_background_job_executor_suppresses_duplicate_progress_delivery_until_handoff(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module
    from ops import codex_memory

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report", "feishu:chat:sample-room"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    sent_messages: list[dict[str, object]] = []
    sent_docs: list[dict[str, object]] = []
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", msg_type="text", card=None: sent_messages.append(
            {"target": target, "text": text, "msg_type": msg_type, "card": card}
        )
        or {"message_id": f"msg-{len(sent_messages)}", "target": target, "receive_id_type": "chat_id", "receive_id": "chat-001"},
    )
    monkeypatch.setattr(
        background_job_executor,
        "create_feishu_doc_target",
        lambda target, title, file_path: sent_docs.append(
            {"target": target, "title": title, "file_path": file_path}
        )
        or {"url": f"https://feishu.example/doc/{len(sent_docs):03d}", "document_id": f"doc-{len(sent_docs):03d}"},
    )

    payloads = []
    for _ in range(4):
        job = board_job_projector.project_background_job("SampleProj", "SP-EXEC-01")
        payloads.append(background_job_executor.execute_projected_job(job, trigger_source="manual_cli"))

    assert len(sent_docs) == 2
    assert len(sent_messages) == 2
    assert "阶段：discover -> frame" in str(sent_messages[0]["text"])
    assert "阶段：verify -> handoff" in str(sent_messages[1]["text"])
    assert "discover -> frame" in str(sent_docs[0]["title"])
    assert "verify -> handoff" in str(sent_docs[1]["title"])
    assert any(
        item["delivery_id"] == "feishu-notify" and item["status"] == "not-requested" and "duplicate" in item["summary"]
        for item in payloads[1]["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "feishu-notify" and item["status"] == "not-requested" and "duplicate" in item["summary"]
        for item in payloads[2]["run_record"]["delivery_outcomes"]
    )
    assert payloads[3]["run_record"]["metadata"]["program_evaluation"]["decision"] == "done"
    board = codex_memory.load_project_board("SampleProj")
    assert board["project_rows"][0]["状态"] == "done"


def test_background_job_executor_retries_only_failed_external_targets(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report", "feishu:chat:sample-room", "weixin:sample-thread"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    sent_feishu: list[dict[str, object]] = []
    sent_docs: list[dict[str, object]] = []
    weixin_attempts = {"count": 0}
    monkeypatch.setattr(background_job_executor, "_external_delivery_fingerprint", lambda *args, **kwargs: "stable-fingerprint")
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", msg_type="text", card=None: sent_feishu.append(
            {"target": target, "text": text, "msg_type": msg_type, "card": card}
        )
        or {"message_id": f"msg-{len(sent_feishu)}", "target": target, "receive_id_type": "chat_id", "receive_id": "chat-001"},
    )
    monkeypatch.setattr(
        background_job_executor,
        "create_feishu_doc_target",
        lambda target, title, file_path: sent_docs.append(
            {"target": target, "title": title, "file_path": file_path}
        )
        or {"url": f"https://feishu.example/doc/{len(sent_docs):03d}", "document_id": f"doc-{len(sent_docs):03d}"},
    )

    def fake_weixin_delivery(target: str, text: str = "") -> dict[str, str]:
        weixin_attempts["count"] += 1
        if weixin_attempts["count"] == 1:
            raise RuntimeError("weixin temporary fail")
        return {"message_id": f"wx-{weixin_attempts['count']}", "target": target, "text": text}

    monkeypatch.setattr(background_job_executor, "deliver_weixin_target", fake_weixin_delivery)

    first = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )
    second = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert len(sent_docs) == 1
    assert len(sent_feishu) == 1
    assert weixin_attempts["count"] == 2
    assert any(
        item["delivery_id"] == "weixin-notify" and item["status"] == "not-delivered"
        for item in first["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "feishu-doc" and item["status"] == "not-requested" and "duplicate" in item["summary"]
        for item in second["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "feishu-notify" and item["status"] == "not-requested" and "duplicate" in item["summary"]
        for item in second["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "weixin-notify" and item["status"] == "delivered"
        for item in second["run_record"]["delivery_outcomes"]
    )
    task_spec = json.loads(Path(second["run_record"]["artifacts"]["task_spec_path"]).read_text(encoding="utf-8"))
    assert task_spec["last_external_delivery"]["status"] == "delivered"


def test_background_job_executor_retries_failed_feishu_doc_even_if_notify_succeeded(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module

    seed_sample_project_board(sample_env)
    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(
        board_job_projector,
        "TASK_JOB_SPECS",
        {
            "SP-EXEC-01": {
                "job_slug": "sample-background-job",
                "executor_kind": "research_brief",
                "automation_mode": "background_assist",
                "allowed_actions": ["read", "write_report"],
                "delivery_targets": ["board", "report", "feishu:chat:sample-room"],
                "gate_policy": "none",
                "max_rounds": 2,
                "time_budget_minutes": 10,
                "acceptance_criteria": ["Produce a sample brief."],
            }
        },
    )
    background_job_executor = importlib.reload(executor_module)
    sent_messages: list[dict[str, object]] = []
    doc_attempts = {"count": 0}
    monkeypatch.setattr(background_job_executor, "_external_delivery_fingerprint", lambda *args, **kwargs: "stable-fingerprint")
    monkeypatch.setattr(
        background_job_executor,
        "deliver_feishu_target",
        lambda target, text="", msg_type="text", card=None: sent_messages.append(
            {"target": target, "text": text, "msg_type": msg_type, "card": card}
        )
        or {"message_id": f"msg-{len(sent_messages)}", "target": target, "receive_id_type": "chat_id", "receive_id": "chat-001"},
    )

    def fake_doc_delivery(target: str, title: str, file_path: str) -> dict[str, str]:
        doc_attempts["count"] += 1
        if doc_attempts["count"] == 1:
            raise RuntimeError("doc failed once")
        return {"url": "https://feishu.example/doc/fixed", "document_id": "doc-fixed"}

    monkeypatch.setattr(background_job_executor, "create_feishu_doc_target", fake_doc_delivery)

    first = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )
    second = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    assert doc_attempts["count"] == 2
    assert len(sent_messages) == 1
    assert any(
        item["delivery_id"] == "feishu-doc" and item["status"] == "not-delivered"
        for item in first["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "feishu-doc" and item["status"] == "delivered"
        for item in second["run_record"]["delivery_outcomes"]
    )
    assert any(
        item["delivery_id"] == "feishu-notify" and item["status"] == "not-requested" and "duplicate" in item["summary"]
        for item in second["run_record"]["delivery_outcomes"]
    )


def test_background_job_executor_growth_loop_writes_action_and_evidence_truth(sample_env, monkeypatch, tmp_path) -> None:
    from ops import background_job_executor as executor_module
    from ops import board_job_projector as projector_module
    from ops import growth_truth

    seed_sample_project_board(sample_env)
    offer_path = tmp_path / "offer.md"
    listing_path = tmp_path / "listing.md"
    action_path = tmp_path / "action.md"
    evidence_path = tmp_path / "evidence.md"
    offer_path.write_text("# Offer\n\n| offer_id |\n| --- |\n| CGS-OFFER-001 |\n", encoding="utf-8")
    listing_path.write_text("# Listing\n\n| listing_id |\n| --- |\n", encoding="utf-8")

    control = growth_control_for_sample_task()
    control["objects"] = {
        "Offer": {
            "table_path": str(offer_path),
            "fields": ["offer_id"],
        },
        "Listing": {
            "table_path": str(listing_path),
            "fields": ["listing_id"],
        },
        "Action": {
            "table_path": str(action_path),
            "fields": ["action_id", "platform", "command", "target_type", "target_id", "status", "risk_level", "run_id", "error", "executed_at"],
        },
        "Evidence": {
            "table_path": str(evidence_path),
            "fields": ["evidence_id", "source_type", "source_id", "signal_type", "content", "decision", "merged_into", "created_at"],
        },
    }
    (sample_env["control_root"] / "codex_growth_system.yaml").write_text(
        yaml.safe_dump(control, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

    board_job_projector = importlib.reload(projector_module)
    monkeypatch.setattr(board_job_projector, "TASK_JOB_SPECS", {})
    background_job_executor = importlib.reload(executor_module)

    payload = background_job_executor.execute_projected_job(
        board_job_projector.project_background_job("SampleProj", "SP-EXEC-01"),
        trigger_source="manual_cli",
    )

    action_rows = growth_truth.load_rows("Action")
    evidence_rows = growth_truth.load_rows("Evidence")

    assert payload["ok"] is True
    assert action_rows[0]["action_id"] == payload["run_context"]["run_id"]
    assert action_rows[0]["command"] == "growth_signal_scan"
    assert action_rows[0]["status"] == "done"
    assert evidence_rows[0]["source_id"] == payload["run_context"]["run_id"]
    assert evidence_rows[0]["signal_type"] == "growth_signal_scan"
    assert any(item["delivery_id"] == "growth-truth-writeback" for item in payload["run_record"]["delivery_outcomes"])


def test_writeback_job_progress_triggers_growth_projection_sync(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    triggered: list[str] = []

    monkeypatch.setattr(background_job_executor, "fixture_mode", lambda: False)
    monkeypatch.setattr(background_job_executor.codex_memory, "sync_project_layers", lambda *args, **kwargs: ["board"])
    monkeypatch.setattr(background_job_executor.codex_memory, "record_project_writeback", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_retrieval_sync_once", lambda: triggered.append("retrieval"))
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_dashboard_sync_once", lambda: triggered.append("dashboard"))
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_feishu_projection_sync_once", lambda: triggered.append("feishu"))
    monkeypatch.setattr(
        background_job_executor.codex_memory,
        "trigger_growth_feishu_projection_sync_once",
        lambda: triggered.append("growth-feishu"),
    )
    monkeypatch.setattr(
        background_job_executor.codex_memory,
        "trigger_growth_operator_surface_report_once",
        lambda: triggered.append("growth-operator-surface"),
    )

    job = {
        "project_name": "SampleProj",
        "task_id": "SP-EXEC-01",
        "source_type": "topic",
        "source": "topic:操盘线",
        "source_path": "/tmp/topic.md",
        "project_board_path": "/tmp/project.md",
    }

    changed = background_job_executor.writeback_job_progress(
        job,
        run_id="run-1",
        deliverable="/tmp/latest.md",
        next_action="continue",
        trigger_followup_syncs=True,
    )

    assert changed == ["board"]
    assert triggered == ["retrieval", "dashboard", "feishu"]


def test_writeback_job_progress_triggers_growth_projection_only_for_growth_project(sample_env, monkeypatch) -> None:
    from ops import background_job_executor as executor_module

    background_job_executor = importlib.reload(executor_module)
    triggered: list[str] = []

    monkeypatch.setattr(background_job_executor, "fixture_mode", lambda: False)
    monkeypatch.setattr(background_job_executor.codex_memory, "sync_project_layers", lambda *args, **kwargs: ["board"])
    monkeypatch.setattr(background_job_executor.codex_memory, "record_project_writeback", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_retrieval_sync_once", lambda: triggered.append("retrieval"))
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_dashboard_sync_once", lambda: triggered.append("dashboard"))
    monkeypatch.setattr(background_job_executor.codex_memory, "trigger_feishu_projection_sync_once", lambda: triggered.append("feishu"))
    monkeypatch.setattr(
        background_job_executor.codex_memory,
        "trigger_growth_feishu_projection_sync_once",
        lambda *args, **kwargs: triggered.append("growth-feishu"),
    )
    monkeypatch.setattr(
        background_job_executor.codex_memory,
        "trigger_growth_operator_surface_report_once",
        lambda: triggered.append("growth-operator-surface"),
    )

    job = {
        "project_name": "增长与营销",
        "task_id": "GM-O29",
        "source_type": "topic",
        "source": "topic:操盘线",
        "source_path": "/tmp/topic.md",
        "project_board_path": "/tmp/project.md",
    }

    changed = background_job_executor.writeback_job_progress(
        job,
        run_id="run-1",
        deliverable="/tmp/latest.md",
        next_action="continue",
        trigger_followup_syncs=True,
    )

    assert changed == ["board"]
    assert triggered == ["retrieval", "dashboard", "feishu", "growth-feishu", "growth-operator-surface"]

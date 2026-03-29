from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from ops import codex_memory, gstack_automation, runtime_state


REPO_ROOT = Path(__file__).resolve().parents[1]


def set_fixture_stage_executor(monkeypatch: pytest.MonkeyPatch, responses: dict[str, dict[str, object]]) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_GFLOW_STAGE_EXECUTOR", "fixture")
    monkeypatch.setenv("WORKSPACE_HUB_GFLOW_STAGE_RESPONSES", json.dumps(responses, ensure_ascii=False))


def test_detect_gflow_trigger_matches_leading_token_and_strips_entry_prompt() -> None:
    payload = gstack_automation.detect_gflow_trigger(
        "GFlow，帮我梳理这个需求，并从技术上评估主要风险。"
    )

    assert payload["matched"] is True
    assert payload["invocation_mode"] == "gflow-explicit"
    assert payload["trigger_token"].lower() == "gflow"
    assert payload["entry_prompt"] == "帮我梳理这个需求，并从技术上评估主要风险。"


def test_detect_gflow_trigger_matches_command_form() -> None:
    payload = gstack_automation.detect_gflow_trigger("用 GFlow 处理这件事：先 review 再做 QA。")

    assert payload["matched"] is True
    assert payload["entry_prompt"] == "先 review 再做 QA。"


def test_detect_gflow_trigger_does_not_treat_reference_as_invocation() -> None:
    payload = gstack_automation.detect_gflow_trigger("GFlow 的系统有几个 phase？")

    assert payload["matched"] is False
    assert payload["entry_prompt"] == ""


def test_detect_gflow_trigger_does_not_treat_meta_question_as_invocation() -> None:
    payload = gstack_automation.detect_gflow_trigger("GFlow 现在有哪些阶段？")

    assert payload["matched"] is False
    assert payload["entry_prompt"] == ""


def test_detect_gflow_trigger_command_form_does_not_treat_reference_as_invocation() -> None:
    payload = gstack_automation.detect_gflow_trigger("请 GFlow 的系统有几个 phase？")

    assert payload["matched"] is False
    assert payload["entry_prompt"] == ""


def test_detect_gflow_trigger_command_form_does_not_treat_meta_question_as_invocation() -> None:
    payload = gstack_automation.detect_gflow_trigger("请 GFlow 现在有哪些阶段？")

    assert payload["matched"] is False
    assert payload["entry_prompt"] == ""


def test_detect_gflow_trigger_ignores_launcher_history_and_nonleading_quotes() -> None:
    prompt = (
        "这是续接会话。\n"
        "当前已命中 `GFlow` 显式强制流程模式。\n"
        "用户这次的直接请求是：\n"
        "你刚才最后明确给我的指令是：\n"
        "要按 GFlow 的完整工作方式做，不是只写点代码就算完。\n"
        "@_user_1 好的，你可以开始了。先做 phase 1。"
    )

    payload = gstack_automation.detect_gflow_trigger(prompt)
    assert payload["matched"] is False


def test_create_workflow_run_from_prompt_builds_phase1_contracts() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(
        "GFlow，帮我梳理这个需求，并从技术上评估主要风险。"
    )

    assert payload["status"] == "gflow-run-created"
    assert payload["invocation_mode"] == "gflow-explicit"
    assert payload["suggested_path"] == ["office-hours", "plan-eng-review"]
    assert payload["initial_stage"] == "office-hours"
    assert payload["workflow_plan"]["schema_version"] == gstack_automation.WORKFLOW_PLAN_SCHEMA_VERSION
    assert payload["initial_stage_result"]["schema_version"] == (
        gstack_automation.WORKFLOW_STAGE_RESULT_SCHEMA_VERSION
    )
    assert payload["run_summary"]["schema_version"] == (
        gstack_automation.WORKFLOW_RUN_SUMMARY_SCHEMA_VERSION
    )
    assert payload["workflow_plan"]["runtime_mode"] == "phase1-main-thread"
    assert "GFlow explicit mode handoff" in payload["main_thread_handoff"]


def test_create_workflow_run_from_prompt_falls_back_to_default_readonly_path() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt("GFlow，直接做这个小改动。")

    assert payload["path_source"] == "default-readonly-template"
    assert payload["suggested_path"] == ["office-hours", "plan-eng-review"]
    assert payload["workflow_detection"]["recognized_stage"] == "multi-stage"


@pytest.mark.parametrize(
    ("prompt", "expected_template_id", "expected_path"),
    [
        (
            "GFlow，帮我审一下这次改动，然后再让 Claude 给一个第二意见。",
            "review-claude-review",
            ["review", "claude-review"],
        ),
        (
            "GFlow，先 review，再做 QA，最后判断能不能 ship。",
            "review-qa-ship",
            ["review", "qa", "ship"],
        ),
        (
            "GFlow，帮我梳理这个想法，再从产品角度判断值不值得做，最后从技术角度判断怎么落地。",
            "office-hours-plan-ceo-plan-eng",
            ["office-hours", "plan-ceo-review", "plan-eng-review"],
        ),
        (
            "GFlow，先同步发布说明，再判断这个版本现在能不能 ship。",
            "document-release-ship",
            ["document-release", "ship"],
        ),
    ],
)
def test_create_workflow_run_from_prompt_matches_phase4_templates(
    prompt: str,
    expected_template_id: str,
    expected_path: list[str],
) -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(prompt)

    assert payload["template_id"] == expected_template_id
    assert payload["workflow_plan"]["template_id"] == expected_template_id
    assert payload["suggested_path"] == expected_path
    assert payload["workflow_detection"]["path_source"] == "workflow-template"
    assert payload["workflow_plan"]["template"]["template_id"] == expected_template_id
    assert payload["workflow_plan"]["stages"][0]["template_contract"]["skill"] == expected_path[0]
    assert expected_template_id in payload["main_thread_handoff"]


def test_create_workflow_run_from_prompt_adds_gflow_project_scope_for_review_request() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。"
    )

    assert payload["suggested_path"] == ["review", "fix", "qa", "writeback"]
    assert payload["template_id"] == "review-fix-qa-writeback"
    assert payload["project_scope_id"] == "gflow-codebase-review"
    assert payload["project_scope_label"] == "GFlow 项目代码"
    scope = payload["workflow_plan"]["project_scope"]
    assert scope["scope_id"] == "gflow-codebase-review"
    assert any(path.endswith("/ops/gstack_automation.py") for path in scope["files"])
    assert any(path.endswith("/ops/gstack_phase1_entry.py") for path in scope["files"])
    assert any(path.endswith("/ops/claude_code_runner.py") for path in scope["files"])
    assert any(path.endswith("/ops/start-codex") for path in scope["files"])


def test_create_workflow_run_from_prompt_treats_gflow_audit_phrase_as_review_scope() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(
        "GFlow 帮我审核一下「GFlow」这个workflow的代码"
    )

    assert payload["suggested_path"] == ["review", "fix", "qa", "writeback"]
    assert payload["template_id"] == "review-fix-qa-writeback"
    assert payload["project_scope_id"] == "gflow-codebase-review"
    assert payload["project_scope_label"] == "GFlow 项目代码"


def test_create_workflow_run_from_prompt_expands_all_code_scope() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(
        "GFlow 帮我review 「GFlow」这个workflow的所有代码"
    )

    files = payload["workflow_plan"]["project_scope"]["files"]
    assert payload["template_id"] == "review-fix-qa-writeback"
    assert any(path.endswith("/ops/claude_code_runner.py") for path in files)


def test_build_workflow_preview_uses_single_runtime_source_without_preview_run_id() -> None:
    payload = gstack_automation.build_workflow_preview(
        "GFlow 帮我review一下「GFlow」这个项目的代码"
    )

    assert payload["status"] == "gflow-preview-ready"
    assert payload["workflow_plan"]["run_id"] == ""
    assert "- run_id:" not in payload["main_thread_handoff"]


def test_create_workflow_run_from_prompt_resolves_generic_review_scope_subject() -> None:
    payload = gstack_automation.create_workflow_run_from_prompt(
        "GFlow 帮我review一下「runtime_state」这个模块的代码。"
    )

    assert payload["project_scope_id"].startswith("review-scope-")
    files = payload["workflow_plan"]["project_scope"]["files"]
    assert any(path.endswith("/ops/runtime_state.py") for path in files)


def test_build_stage_result_and_run_summary_validate_required_fields() -> None:
    with pytest.raises(ValueError):
        gstack_automation.build_stage_result(
            run_id="",
            stage_id="stage-1",
            skill="office-hours",
            status="pending",
            summary="x",
        )

    with pytest.raises(ValueError):
        gstack_automation.build_run_summary(
            run_id="run-1",
            status="",
            current_stage="office-hours",
            summary="x",
        )


def test_gstack_automation_cli_plan_outputs_json() -> None:
    result = subprocess.run(
        [
            "python3",
            str(REPO_ROOT / "ops" / "gstack_automation.py"),
            "plan",
            "--prompt",
            "GFlow，帮我梳理这个需求，并从技术上评估主要风险。",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == gstack_automation.WORKFLOW_PLAN_SCHEMA_VERSION
    assert payload["initial_stage"] == "office-hours"


def test_gstack_automation_cli_from_prompt_outputs_full_payload_json() -> None:
    result = subprocess.run(
        [
            "python3",
            str(REPO_ROOT / "ops" / "gstack_automation.py"),
            "from-prompt",
            "--prompt",
            "GFlow: 先 review 再做 QA。",
            "--json",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "gflow-run-created"
    assert payload["suggested_path"] == ["review", "qa"]
    assert payload["initial_stage"] == "review"


def test_start_workflow_run_persists_runtime_state(sample_env) -> None:
    payload = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-1",
    )

    assert payload["status"] == "gflow-run-started"
    assert payload["run_status"] == "running"
    assert payload["project_name"] == "SampleProj"
    assert payload["session_id"] == "sess-1"
    assert payload["current_stage"] == "review"
    assert payload["workflow_plan"]["runtime_mode"] == "phase2-runtime"
    assert [stage["status"] for stage in payload["workflow_plan"]["stages"]] == ["running", "pending"]
    assert [stage["status"] for stage in payload["stage_results"]] == ["running", "pending"]
    assert payload["main_thread_handoff"].startswith(gstack_automation.RUNTIME_HANDOFF_TITLE)

    listed = gstack_automation.list_workflow_runs(project_name="SampleProj")
    assert len(listed) == 1
    assert listed[0]["run_id"] == payload["run_id"]

    counts = runtime_state.init_db()["counts"]
    assert counts["gflow_runs"] == 1
    assert counts["gflow_stage_results"] == 2


def test_start_explicit_workflow_run_if_requested_creates_runtime_run(sample_env) -> None:
    payload = gstack_automation.start_explicit_workflow_run_if_requested(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
        project_name="SampleProj",
        session_id="sess-gflow-auto-start",
    )

    assert payload["started"] is True
    assert payload["status"] == "gflow-run-started"
    assert payload["project_name"] == "SampleProj"
    assert payload["project_scope_id"] == "gflow-codebase-review"
    assert payload["template_id"] == "review-fix-qa-writeback"
    listed = gstack_automation.list_workflow_runs(project_name="SampleProj")
    assert len(listed) == 1
    assert listed[0]["run_id"] == payload["run_id"]


def test_pause_and_resume_workflow_run_with_approval_gate(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-approval",
    )

    paused = gstack_automation.pause_workflow_run(
        started["run_id"],
        reason="等待人工批准",
        gate_type=gstack_automation.GATE_TYPE_APPROVAL,
        next_action="批准后继续 review。",
        evidence=["需要产品确认"],
    )

    assert paused["status"] == "gflow-run-paused"
    assert paused["run_status"] == "awaiting_approval"
    assert paused["gate"]["type"] == "approval"
    assert paused["gate"]["reason"] == "等待人工批准"
    assert paused["gate"]["token"]
    assert paused["stage_results"][0]["status"] == "awaiting_approval"
    assert paused["stage_results"][0]["evidence"] == ["需要产品确认"]

    with pytest.raises(ValueError, match="approval_token is required"):
        gstack_automation.resume_workflow_run(started["run_id"])

    approval = runtime_state.fetch_approval_token(paused["gate"]["token"])
    assert approval["status"] == "pending"
    runtime_state.upsert_approval_token(
        token=paused["gate"]["token"],
        scope=approval["scope"],
        status="approved",
        project_name=approval["project_name"],
        session_id=approval["session_id"],
        metadata=approval["metadata"],
    )

    resumed = gstack_automation.resume_workflow_run(
        started["run_id"],
        note="批准通过，继续执行。",
        approval_token=paused["gate"]["token"],
        evidence=["批准已记录"],
    )

    assert resumed["status"] == "gflow-run-resumed"
    assert resumed["run_status"] == "running"
    assert resumed["gate"]["type"] == ""
    assert resumed["stage_results"][0]["status"] == "running"
    assert resumed["stage_results"][0]["evidence"] == ["需要产品确认", "批准已记录"]


def test_pause_and_resume_workflow_run_with_freeze_gate(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-freeze",
    )

    frozen = gstack_automation.pause_workflow_run(
        started["run_id"],
        reason="先冻结写操作",
        gate_type=gstack_automation.GATE_TYPE_FREEZE,
        freeze_scope="write-actions",
        next_action="等待解除 freeze。",
        evidence=["风险较高"],
    )

    assert frozen["run_status"] == "frozen"
    assert frozen["gate"] == {
        "type": "freeze",
        "reason": "先冻结写操作",
        "token": "",
        "freeze_scope": "write-actions",
    }
    assert frozen["stage_results"][0]["status"] == "frozen"
    assert frozen["stage_results"][0]["evidence"] == ["风险较高"]

    resumed = gstack_automation.resume_workflow_run(
        started["run_id"],
        note="freeze 已解除。",
        evidence=["恢复执行"],
    )

    assert resumed["run_status"] == "running"
    assert resumed["gate"]["freeze_scope"] == ""
    assert resumed["stage_results"][0]["status"] == "running"
    assert resumed["stage_results"][0]["evidence"] == ["风险较高", "恢复执行"]


def test_resume_workflow_run_auto_executes_current_auto_stage(sample_env, monkeypatch: pytest.MonkeyPatch) -> None:
    set_fixture_stage_executor(
        monkeypatch,
        {
            "fix": {
                "summary": "fix 阶段需要人工补充。",
                "next_action": "补充上下文后继续。",
                "evidence": ["fix-blocked"],
                "stop_condition": "blocked",
                "stop_reason": "fix 阶段命中 blocker。",
            }
        },
    )
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
        project_name="SampleProj",
        session_id="sess-auto-resume",
    )

    paused = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成，进入 fix。",
        next_action="进入 `fix` 阶段。",
        evidence=["review-findings"],
    )

    assert paused["run_status"] == "paused"
    assert paused["current_stage"] == "fix"

    set_fixture_stage_executor(
        monkeypatch,
        {
            "fix": {
                "summary": "fix 已完成，恢复后继续推进。",
                "next_action": "进入 `qa` 阶段。",
                "evidence": ["fix-after-resume"],
            },
            "qa": {
                "summary": "qa 已通过。",
                "next_action": "进入 `writeback` 阶段。",
                "evidence": ["qa-after-resume"],
            },
        },
    )

    resumed = gstack_automation.resume_workflow_run(
        started["run_id"],
        note="blocker 已解除，继续执行。",
        evidence=["resume-note"],
    )

    assert resumed["status"] == "gflow-run-completed"
    assert resumed["run_status"] == "completed"
    assert [stage["status"] for stage in resumed["workflow_plan"]["stages"]] == [
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    assert resumed["stage_results"][1]["evidence"] == ["fix-blocked", "resume-note", "fix-after-resume"]
    assert resumed["stage_results"][2]["evidence"] == ["qa-after-resume"]


def test_advance_workflow_run_moves_to_next_stage_and_completes(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-advance",
    )

    advanced = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成，交给 QA。",
        next_action="执行 QA 验证。",
        evidence=["review-checklist"],
    )

    assert advanced["status"] == "gflow-stage-advanced"
    assert advanced["run_status"] == "running"
    assert advanced["current_stage"] == "qa"
    assert advanced["run_summary"]["completed_stages"] == ["review"]
    assert [stage["status"] for stage in advanced["workflow_plan"]["stages"]] == ["completed", "running"]
    assert advanced["stage_results"][0]["evidence"] == ["review-checklist"]
    assert advanced["stage_results"][1]["status"] == "running"

    completed = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="QA 已完成。",
        evidence=["qa-checklist"],
    )

    assert completed["status"] == "gflow-run-completed"
    assert completed["run_status"] == "completed"
    assert completed["run_summary"]["completed_stages"] == ["review", "qa"]
    assert completed["run_summary"]["summary"] == "GFlow 显式流程记录已完成全部阶段。"
    assert [stage["status"] for stage in completed["workflow_plan"]["stages"]] == ["completed", "completed"]
    assert completed["stage_results"][1]["evidence"] == ["qa-checklist"]


@pytest.mark.parametrize(
    ("prompt", "expected_template_id", "expected_template_label", "expected_path"),
    [
        (
            "GFlow，帮我审一下这次改动，然后再让 Claude 给一个第二意见。",
            "review-claude-review",
            "Review -> Claude Review",
            ["review", "claude-review"],
        ),
        (
            "GFlow，先 review，再做 QA，最后判断能不能 ship。",
            "review-qa-ship",
            "Review -> QA -> Ship",
            ["review", "qa", "ship"],
        ),
        (
            "GFlow，帮我梳理这个想法，再从产品角度判断值不值得做，最后从技术角度判断怎么落地。",
            "office-hours-plan-ceo-plan-eng",
            "Office Hours -> CEO Review -> Eng Review",
            ["office-hours", "plan-ceo-review", "plan-eng-review"],
        ),
        (
            "GFlow，先同步发布说明，再判断这个版本现在能不能 ship。",
            "document-release-ship",
            "Document Release -> Ship",
            ["document-release", "ship"],
        ),
    ],
)
def test_template_runtime_flows_can_advance_to_completion_manually(
    sample_env,
    prompt: str,
    expected_template_id: str,
    expected_template_label: str,
    expected_path: list[str],
) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        prompt,
        project_name="SampleProj",
        session_id=f"sess-{expected_template_id}",
    )

    assert started["template_id"] == expected_template_id
    assert started["template_label"] == expected_template_label
    assert [stage["skill"] for stage in started["stage_results"]] == expected_path
    assert started["current_stage"] == expected_path[0]

    payload = started
    for index, skill in enumerate(expected_path):
        evidence = [f"{skill}-evidence"]
        is_last = index == len(expected_path) - 1
        next_action = "" if is_last else f"进入 `{expected_path[index + 1]}` 阶段。"
        payload = gstack_automation.advance_workflow_run(
            started["run_id"],
            summary=f"{skill} 已完成。",
            next_action=next_action,
            evidence=evidence,
            allow_auto_execute=False,
        )

        assert payload["stage_results"][index]["status"] == "completed"
        assert payload["stage_results"][index]["evidence"] == evidence
        if is_last:
            assert payload["status"] == "gflow-run-completed"
            assert payload["run_status"] == "completed"
        else:
            assert payload["status"] == "gflow-stage-advanced"
            assert payload["run_status"] == "running"
            assert payload["current_stage"] == expected_path[index + 1]
            assert payload["stage_results"][index + 1]["status"] == "running"

    assert payload["run_summary"]["completed_stages"] == expected_path
    assert [stage["status"] for stage in payload["workflow_plan"]["stages"]] == [
        "completed"
    ] * len(expected_path)

    summary = gstack_automation.latest_project_workflow_summary("SampleProj")
    assert summary["run_id"] == started["run_id"]
    assert summary["run_status"] == "completed"
    assert summary["template_id"] == expected_template_id
    assert summary["template_label"] == expected_template_label

    board = codex_memory.load_project_board("SampleProj")
    assert board["gflow_rows"][0]["ID"] == started["run_id"]
    assert board["gflow_rows"][0]["状态"] == "done"
    assert expected_template_label in board["gflow_rows"][0]["事项"]


def test_review_qa_ship_template_auto_executes_qa_before_ship(sample_env, monkeypatch: pytest.MonkeyPatch) -> None:
    set_fixture_stage_executor(
        monkeypatch,
        {
            "qa": {
                "summary": "qa 已完成，进入 ship。",
                "next_action": "进入 `ship` 阶段。",
                "evidence": ["qa-pass"],
            }
        },
    )
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow，先 review，再做 QA，最后判断能不能 ship。",
        project_name="SampleProj",
        session_id="sess-review-qa-ship-auto",
    )

    advanced = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成。",
        next_action="进入 `qa` 阶段。",
        evidence=["review-pass"],
    )

    assert advanced["status"] == "gflow-stage-advanced"
    assert advanced["run_status"] == "running"
    assert advanced["current_stage"] == "ship"
    assert [stage["status"] for stage in advanced["workflow_plan"]["stages"]] == [
        "completed",
        "completed",
        "running",
    ]
    assert advanced["stage_results"][1]["summary"] == "qa 已完成，进入 ship。"
    assert advanced["stage_results"][1]["evidence"] == ["qa-pass"]
    assert advanced["stage_results"][2]["status"] == "running"


def test_advance_workflow_run_auto_executes_fix_qa_writeback_chain(sample_env, monkeypatch: pytest.MonkeyPatch) -> None:
    set_fixture_stage_executor(
        monkeypatch,
        {
            "fix": {
                "summary": "fix 已完成，关键问题已修复。",
                "next_action": "进入 `qa` 阶段。",
                "evidence": ["fix-diff"],
            },
            "qa": {
                "summary": "qa 已通过，准备写回。",
                "next_action": "进入 `writeback` 阶段。",
                "evidence": ["qa-pass"],
            },
        },
    )
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
        project_name="SampleProj",
        session_id="sess-auto-chain",
    )

    completed = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成，进入 fix。",
        next_action="进入 `fix` 阶段。",
        evidence=["review-findings"],
    )

    assert completed["status"] == "gflow-run-completed"
    assert completed["run_status"] == "completed"
    assert completed["current_stage"] == "writeback"
    assert [stage["status"] for stage in completed["workflow_plan"]["stages"]] == [
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    assert [stage["skill"] for stage in completed["stage_results"]] == [
        "review",
        "fix",
        "qa",
        "writeback",
    ]
    assert completed["stage_results"][1]["summary"] == "fix 已完成，关键问题已修复。"
    assert completed["stage_results"][1]["evidence"] == ["fix-diff"]
    assert completed["stage_results"][2]["summary"] == "qa 已通过，准备写回。"
    assert completed["stage_results"][2]["evidence"] == ["qa-pass"]
    assert completed["stage_results"][3]["status"] == "completed"
    assert completed["run_summary"]["next_action"] == "已完成，无需继续。"

    board = codex_memory.load_project_board("SampleProj")
    assert board["gflow_rows"][0]["ID"] == started["run_id"]
    assert board["gflow_rows"][0]["状态"] == "done"
    assert board["gflow_rows"][0]["下一步"] == "已完成，无需继续。"


def test_advance_workflow_run_auto_execute_pauses_on_stop_condition(sample_env, monkeypatch: pytest.MonkeyPatch) -> None:
    set_fixture_stage_executor(
        monkeypatch,
        {
            "fix": {
                "summary": "fix 阶段需要人工处理。",
                "next_action": "补充上下文后重试。",
                "evidence": ["fix-blocked"],
                "stop_condition": "blocked",
                "stop_reason": "fix 阶段命中 blocker。",
            }
        },
    )
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
        project_name="SampleProj",
        session_id="sess-auto-stop",
    )

    paused = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成，进入 fix。",
        next_action="进入 `fix` 阶段。",
        evidence=["review-findings"],
    )

    assert paused["status"] == "gflow-run-paused"
    assert paused["run_status"] == "paused"
    assert paused["current_stage"] == "fix"
    assert paused["gate"]["type"] == "user"
    assert paused["gate"]["reason"] == "fix 阶段命中 blocker。"
    assert paused["stage_results"][1]["status"] == "paused"
    assert paused["stage_results"][1]["evidence"] == ["fix-blocked"]


def test_phase3_writeback_updates_project_board_and_rollups(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-phase3",
    )

    board = codex_memory.load_project_board("SampleProj")
    gflow_rows = board["gflow_rows"]
    assert len(gflow_rows) == 1
    assert gflow_rows[0]["ID"] == started["run_id"]
    assert gflow_rows[0]["来源"] == "gflow"
    assert gflow_rows[0]["状态"] == "doing"
    assert gflow_rows[0]["事项"].startswith("GFlow ")
    assert "先 review 再做 QA。" in gflow_rows[0]["事项"]
    assert "review" in gflow_rows[0]["下一步"]

    retrieval_events = runtime_state.fetch_runtime_events(queue_name="retrieval_sync", limit=10)
    dashboard_events = runtime_state.fetch_runtime_events(queue_name="dashboard_sync", limit=10)
    writeback_events = [item for item in retrieval_events if (item.get("payload") or {}).get("source") == "gflow-runtime"]
    assert writeback_events
    assert any((item.get("payload") or {}).get("project_name") == "SampleProj" for item in writeback_events)
    assert any((item.get("payload") or {}).get("source") == "gflow-runtime" for item in dashboard_events)

    summary_text = codex_memory.project_summary_path("SampleProj").read_text(encoding="utf-8")
    assert "last_writeback_excerpt:" in summary_text
    assert "GFlow 当前运行：" in summary_text


def test_phase3_writeback_reflects_gate_and_completion(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-phase3-gate",
    )

    paused = gstack_automation.pause_workflow_run(
        started["run_id"],
        reason="等待人工批准",
        gate_type=gstack_automation.GATE_TYPE_APPROVAL,
        next_action="批准后继续 review。",
    )
    board = codex_memory.load_project_board("SampleProj")
    paused_row = board["gflow_rows"][0]
    assert paused_row["状态"] == "blocked"
    assert "gate: 等待人工批准" in paused_row["交付物"]

    runtime_state.upsert_approval_token(
        token=paused["gate"]["token"],
        scope="gflow_run_gate",
        status="approved",
        project_name="SampleProj",
        session_id="sess-phase3-gate",
        metadata={"run_id": started["run_id"]},
    )
    gstack_automation.resume_workflow_run(
        started["run_id"],
        note="批准通过，继续执行。",
        approval_token=paused["gate"]["token"],
    )
    gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="review 已完成，交给 QA。",
        next_action="执行 QA 验证。",
    )
    completed = gstack_automation.advance_workflow_run(
        started["run_id"],
        summary="QA 已完成。",
    )
    board = codex_memory.load_project_board("SampleProj")
    completed_row = board["gflow_rows"][0]
    assert completed["run_status"] == "completed"
    assert completed_row["状态"] == "done"
    assert completed_row["下一步"] == "已完成，无需继续。"


def test_latest_project_workflow_summary_prefers_active_run_over_newer_completed_run(sample_env) -> None:
    running = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-running",
    )
    newer = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-completed",
    )
    gstack_automation.advance_workflow_run(newer["run_id"], summary="review 已完成。")
    gstack_automation.advance_workflow_run(newer["run_id"], summary="qa 已完成。")

    summary = gstack_automation.latest_project_workflow_summary("SampleProj")
    assert summary["run_id"] == running["run_id"]
    assert summary["run_status"] == "running"


def test_latest_project_workflow_summary_keeps_active_run_even_with_many_newer_completed_runs(sample_env) -> None:
    running = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-running-many",
    )
    for index in range(25):
        completed = gstack_automation.start_workflow_run_from_prompt(
            "GFlow: 先 review 再做 QA。",
            project_name="SampleProj",
            session_id=f"sess-completed-{index}",
        )
        gstack_automation.advance_workflow_run(completed["run_id"], summary="review 已完成。")
        gstack_automation.advance_workflow_run(completed["run_id"], summary="qa 已完成。")

    summary = gstack_automation.latest_project_workflow_summary("SampleProj")
    assert summary["run_id"] == running["run_id"]
    assert summary["run_status"] == "running"


def test_phase3_writeback_prioritizes_active_run_and_terminal_completed_copy(sample_env) -> None:
    running = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-running-row",
    )
    completed = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-completed-row",
    )
    gstack_automation.advance_workflow_run(completed["run_id"], summary="review 已完成。")
    gstack_automation.advance_workflow_run(completed["run_id"], summary="qa 已完成。")

    board = codex_memory.load_project_board("SampleProj")
    assert board["gflow_rows"][0]["ID"] == running["run_id"]
    completed_row = next(row for row in board["gflow_rows"] if row["ID"] == completed["run_id"])
    assert completed_row["下一步"] == "已完成，无需继续。"


def test_phase4_template_runtime_summary_and_board_rows(sample_env) -> None:
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow，先 review，再做 QA，最后判断能不能 ship。",
        project_name="SampleProj",
        session_id="sess-phase4-template",
    )

    summary = gstack_automation.latest_project_workflow_summary("SampleProj")
    assert summary["run_id"] == started["run_id"]
    assert summary["template_id"] == "review-qa-ship"
    assert summary["template_label"] == "Review -> QA -> Ship"

    board = codex_memory.load_project_board("SampleProj")
    assert "Review -> QA -> Ship" in board["gflow_rows"][0]["事项"]

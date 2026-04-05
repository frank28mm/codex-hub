from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import subprocess
import uuid
from pathlib import Path

from ops import codex_context, codex_memory, codex_retrieval, controlled_gh, controlled_git, controlled_ssh, gstack_automation
from ops.workspace_hub_project import PROJECT_NAME


REPO_ROOT = Path(__file__).resolve().parents[1]


def make_env(sample_env: dict[str, Path]) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "WORKSPACE_HUB_CODE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_ROOT": str(sample_env["workspace_root"]),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample_env["vault_root"]),
            "WORKSPACE_HUB_PROJECTS_ROOT": str(sample_env["projects_root"]),
            "WORKSPACE_HUB_REPORTS_ROOT": str(sample_env["reports_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample_env["runtime_root"]),
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample_env["control_root"]),
            "WORKSPACE_HUB_SKIP_DISCOVERY": "1",
        }
    )
    return env


def init_git_repo(root: Path) -> Path:
    repo = root / "git-sample"
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repo, check=True)
    (repo / "README.md").write_text("fixture\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "remote", "add", "origin", "https://github.com/example/sample.git"], cwd=repo, check=True)
    return repo


def test_codex_context_suggest_topic_bound(sample_env) -> None:
    codex_retrieval.build_index()
    payload = codex_context.suggest_context(project_name="SampleProj", prompt="请查看需求 demand Topic Retrieval Marker")
    assert payload["binding_scope"] == "topic"
    assert payload["board_path"].endswith("SampleProj-需求-跟进板.md")
    assert "topic-bound" in payload["reasoning_tags"]
    assert any(item["path"].endswith("SampleProj.md") for item in payload["recommended_files"])
    assert any(item["path"].endswith("SampleProj-需求-跟进板.md") for item in payload["search_hits"])
    assert payload["timeline_hits"]
    assert payload["detail_hits"]
    assert payload["retrieval_protocol"]["name"] == "search-timeline-detail"
    assert payload["retrieval_protocol"]["next_step"] in {"timeline", "detail"}
    assert any(path.endswith("SampleProj-需求-跟进板.md") for path in payload["retrieval_protocol"]["detail_paths"])


def test_codex_context_includes_workflow_and_second_opinion_recommendation(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="先帮我审一下这次改动，然后再让 Claude 给一个第二意见。",
    )

    workflow = payload["workflow_recommendation"]
    assert workflow["recognized_stage"] == "multi-stage"
    assert workflow["suggested_path"] == ["review", "claude-review"]
    assert workflow["second_opinion"]["skill"] == "claude-review"
    assert workflow["second_opinion"]["source"] == "explicit"
    assert workflow["second_opinion"]["package_template_id"] == "review-risk-scan"
    assert workflow["second_opinion"]["material_source"] == "extractor"
    assert workflow["second_opinion"]["required_fields"] == [
        "question",
        "artifact",
        "current_judgment",
        "extra_context",
    ]
    assert "workflow-recommended" in payload["reasoning_tags"]
    assert "second-opinion-ready" in payload["reasoning_tags"]


def test_codex_context_derives_followup_second_opinion_for_review_prompt(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="帮我审一下这次改动有没有问题。",
    )

    workflow = payload["workflow_recommendation"]
    assert workflow["recognized_stage"] == "execution"
    assert workflow["suggested_path"] == ["review"]
    assert workflow["second_opinion"]["skill"] == "claude-review"
    assert workflow["second_opinion"]["source"] == "followup"
    assert workflow["second_opinion"]["package_template_id"] == "review-risk-scan"
    assert workflow["second_opinion"]["material_source"] == "extractor"
    assert workflow["second_opinion"]["main_thread_execution"]["entrypoint"] == "workflow-second-opinion"
    assert workflow["second_opinion"]["packaged_request"]["autofilled_fields"] == [
        "question",
        "artifact",
        "current_judgment",
        "extra_context",
    ]
    assert "当前 review 证据包" in workflow["second_opinion"]["packaged_request"]["request"]["artifact"]
    assert "workflow-recommended" in payload["reasoning_tags"]
    assert "second-opinion-ready" in payload["reasoning_tags"]


def test_codex_context_prefers_review_browse_qa_chain_for_frontend_validation_prompt(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="帮我 review 这个改动，再看页面流转，再做 QA。",
    )

    workflow = payload["workflow_recommendation"]
    assert workflow["recognized_stage"] == "execution"
    assert workflow["suggested_path"] == ["review", "browse", "qa"]
    assert "我先按 `review -> browse -> qa` 这条链推进" in workflow["assistant_message"]
    assert "workflow-recommended" in payload["reasoning_tags"]


def test_codex_context_prefers_browse_qa_ship_chain_for_page_readiness_prompt(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="这个页面修好没，能不能准备 ship。",
    )

    workflow = payload["workflow_recommendation"]
    assert workflow["recognized_stage"] == "multi-stage"
    assert workflow["suggested_path"] == ["browse", "qa", "ship"]
    assert "我先按 `browse -> qa -> ship` 这条链推进" in workflow["assistant_message"]
    assert "workflow-recommended" in payload["reasoning_tags"]


def test_codex_context_derives_consult_second_opinion_materials_for_plan_prompt(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="这个方向值不值得做？从产品角度看呢？",
    )

    workflow = payload["workflow_recommendation"]
    assert workflow["recognized_stage"] == "entry"
    assert workflow["suggested_path"] == ["plan-ceo-review"]
    assert workflow["second_opinion"]["skill"] == "claude-consult"
    assert workflow["second_opinion"]["source"] == "followup"
    assert workflow["second_opinion"]["package_template_id"] == "consult-tradeoff-check"
    assert workflow["second_opinion"]["material_source"] == "extractor"
    assert workflow["second_opinion"]["main_thread_execution"]["entrypoint"] == "workflow-second-opinion"
    assert "当前方案摘要" in workflow["second_opinion"]["packaged_request"]["request"]["artifact"]
    assert "识别路径: plan-ceo-review" in workflow["second_opinion"]["packaged_request"]["request"]["artifact"]


def test_codex_context_includes_gflow_recommendation_for_explicit_prompt(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="GFlow，帮我梳理这个需求，并从技术上评估主要风险。",
    )

    gflow = payload["gflow_recommendation"]
    assert gflow["invocation_mode"] == "gflow-explicit"
    assert gflow["entry_prompt"] == "帮我梳理这个需求，并从技术上评估主要风险。"
    assert gflow["suggested_path"] == ["office-hours", "plan-eng-review"]
    assert gflow["initial_stage"] == "office-hours"
    assert gflow["template_id"] == ""
    assert gflow["workflow_plan"]["schema_version"] == "codex-hub.workflow.plan.v1"
    assert "gflow-explicit" in payload["reasoning_tags"]

    workflow = payload["workflow_recommendation"]
    assert workflow["suggested_path"] == ["office-hours", "plan-eng-review"]


def test_codex_context_includes_active_gflow_runtime_summary(sample_env) -> None:
    codex_retrieval.build_index()
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-runtime-summary",
    )

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="帮我继续看一下这个项目。",
    )

    runtime_summary = payload["gflow_runtime_summary"]
    assert runtime_summary["run_id"] == started["run_id"]
    assert runtime_summary["run_status"] == "running"
    assert runtime_summary["current_stage"] == "review"
    assert runtime_summary["template_id"] == ""
    assert "gflow-runtime-active" in payload["reasoning_tags"]


def test_codex_context_includes_project_runtime_snapshot_and_hot_window_summary(sample_env) -> None:
    codex_retrieval.build_index()
    bindings = codex_memory.load_bindings()
    bindings["bindings"].append(
        {
            "project_name": "SampleProj",
            "status": "completed",
            "mode": "new",
            "session_id": "sess-hot-window-1",
            "started_at": "2026-03-11T04:50:00Z",
            "last_active_at": "2026-03-11T05:00:00Z",
            "thread_name": "继续看看当前状态",
            "prompt": "继续看看当前状态",
            "launch_source": "feishu",
            "source_chat_ref": "oc_demo_chat",
            "summary_excerpt": "最近完成了需求梳理，并收口了下一步。",
        }
    )
    codex_memory.save_bindings(bindings)
    router = codex_memory.load_router()
    router["routes"]["SampleProj"] = {
        "project_name": "SampleProj",
        "last_session_id": "sess-hot-window-1",
        "last_active_at": "2026-03-11T05:00:00Z",
        "last_thread_name": "继续看看当前状态",
        "last_launch_source": "feishu",
    }
    codex_memory.save_router(router)
    summary_path = codex_memory.project_summary_path("SampleProj")
    frontmatter, body = codex_memory.parse_frontmatter(codex_memory.read_text(summary_path))
    frontmatter["last_writeback_at"] = "2026-03-11T13:00:00+08:00"
    frontmatter["last_writeback_excerpt"] = "最近完成了需求梳理，并收口了下一步。"
    codex_memory.write_text(summary_path, f"{codex_memory.render_frontmatter(frontmatter)}\n\n{body.lstrip()}")

    payload = codex_context.suggest_context(project_name="SampleProj", prompt="继续看看当前状态。")

    snapshot = payload["project_runtime_snapshot"]
    hot_window = payload["hot_window_summary"]
    assert snapshot["project_name"] == "SampleProj"
    assert snapshot["board_path"].endswith("SampleProj-项目板.md")
    assert snapshot["task_status"] in {"doing", "todo", "blocked", "done", "active"}
    assert snapshot["next_action"]
    assert snapshot["metadata"]["doing_count"] >= 0
    assert hot_window["last_writeback_excerpt"] == "最近完成了需求梳理，并收口了下一步。"
    assert hot_window["active_session"]["session_id"] == "sess-hot-window-1"
    assert hot_window["recent_sessions"][0]["summary_excerpt"] == "最近完成了需求梳理，并收口了下一步。"
    assert "runtime-snapshot" in payload["reasoning_tags"]
    assert "hot-window" in payload["reasoning_tags"]


def test_codex_context_includes_bridge_runtime_snapshot_for_launch_source(sample_env, monkeypatch) -> None:
    codex_retrieval.build_index()
    monkeypatch.setattr(
        codex_context.runtime_state,
        "bridge_status_surface",
        lambda bridge="feishu": {
            "bridge": bridge,
            "connection_status": "connected",
            "transport": "fixture",
            "stale": False,
        },
    )

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="继续看看当前状态。",
        launch_source="feishu",
    )

    assert payload["bridge_runtime_snapshot"]["bridge"] == "feishu"
    assert payload["bridge_runtime_snapshot"]["connection_status"] == "connected"
    assert "bridge-runtime" in payload["reasoning_tags"]


def test_codex_context_reconciles_missing_gflow_board_rows_for_active_runtime(sample_env) -> None:
    codex_retrieval.build_index()
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-runtime-reconcile",
    )

    board = codex_memory.load_project_board("SampleProj")
    assert board["gflow_rows"][0]["ID"] == started["run_id"]
    codex_memory.save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        board["project_rows"],
        board["rollup_rows"],
        [],
    )

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="继续看看当前 workflow 的状态。",
    )

    runtime_summary = payload["gflow_runtime_summary"]
    assert runtime_summary["run_id"] == started["run_id"]
    refreshed_board = codex_memory.load_project_board("SampleProj")
    assert refreshed_board["gflow_rows"][0]["ID"] == started["run_id"]


def test_codex_context_uses_stable_gflow_failure_dedupe_key(sample_env, monkeypatch) -> None:
    codex_retrieval.build_index()
    captured: dict[str, object] = {}

    def raise_preview(prompt: str) -> dict[str, object]:
        raise RuntimeError("preview boom")

    monkeypatch.setattr(codex_context.gstack_automation, "build_workflow_preview", raise_preview)
    monkeypatch.setattr(
        codex_context.gstack_automation,
        "detect_gflow_trigger",
        lambda prompt: {
            "matched": True,
            "invocation_mode": "gflow-explicit",
            "trigger_token": "GFlow",
            "entry_prompt": "帮我 review 这个项目",
        },
    )

    def capture_enqueue_runtime_event(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(codex_context.runtime_state, "enqueue_runtime_event", capture_enqueue_runtime_event)

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="GFlow，帮我 review 这个项目",
    )

    assert payload["gflow_recommendation"]["status"] == "gflow-recommendation-error"
    assert captured["event_type"] == "gflow_recommendation_failed"
    assert captured["dedupe_key"] == (
        "gflow-recommendation-failed:"
        + hashlib.sha1("GFlow，帮我 review 这个项目".encode("utf-8")).hexdigest()
    )


def test_codex_context_includes_phase4_template_metadata(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="GFlow，先 review，再做 QA，最后判断能不能 ship。",
    )

    gflow = payload["gflow_recommendation"]
    assert gflow["template_id"] == "review-qa-ship"
    assert gflow["template_label"] == "Review -> QA -> Ship"
    assert gflow["suggested_path"] == ["review", "qa", "ship"]


def test_codex_context_includes_gflow_project_scope_for_review_request(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="GFlow 帮我review一下「GFlow」这个项目目前的代码。",
    )

    gflow = payload["gflow_recommendation"]
    assert gflow["suggested_path"] == ["review", "fix", "qa", "writeback"]
    assert gflow["template_id"] == "review-fix-qa-writeback"
    assert gflow["project_scope_id"] == "gflow-codebase-review"
    assert gflow["project_scope_label"] == "GFlow 项目代码"
    assert any(path.endswith("/ops/gstack_automation.py") for path in gflow["project_scope"]["files"])
    assert any(path.endswith("/ops/gstack_phase1_entry.py") for path in gflow["project_scope"]["files"])
    assert any(path.endswith("/ops/claude_code_runner.py") for path in gflow["project_scope"]["files"])
    assert payload["workflow_recommendation"]["suggested_path"] == ["review"]


def test_codex_context_includes_gflow_project_scope_for_audit_phrase(sample_env) -> None:
    codex_retrieval.build_index()

    payload = codex_context.suggest_context(
        project_name="SampleProj",
        prompt="GFlow 帮我审核一下「GFlow」这个workflow的代码",
    )

    gflow = payload["gflow_recommendation"]
    assert gflow["suggested_path"] == ["review", "fix", "qa", "writeback"]
    assert gflow["template_id"] == "review-fix-qa-writeback"
    assert gflow["project_scope_id"] == "gflow-codebase-review"
    assert gflow["project_scope_label"] == "GFlow 项目代码"


def test_export_rules_generates_valid_rules(sample_env) -> None:
    env = make_env(sample_env)
    out_dir = sample_env["workspace_root"] / ".codex" / "rules"
    result = subprocess.run(
        ["python3", str(REPO_ROOT / "ops" / "codex_control.py"), "export-rules", "--output-dir", str(out_dir)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    rule_path = Path(payload["output_path"])
    text = rule_path.read_text(encoding="utf-8")
    assert "prefix_rule(" in text
    assert 'decision = "prompt"' in text
    assert 'pattern = ["cat"]' not in text
    assert 'pattern = ["find"]' not in text
    assert 'pattern = ["sed"]' not in text
    assert 'pattern = ["ls"]' not in text
    assert 'pattern = ["rg"]' not in text
    check = subprocess.run(
        ["codex", "execpolicy", "check", "--rules", str(rule_path), "git", "push", "origin", "main"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "prompt" in check.stdout
    local_read = subprocess.run(
        ["codex", "execpolicy", "check", "--rules", str(rule_path), "cat", "~/.ssh/config"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    local_payload = json.loads(local_read.stdout)
    assert local_payload.get("decision") != "allow"
    assert not local_payload.get("matchedRules")


def test_controlled_git_status_and_push(sample_env) -> None:
    repo = init_git_repo(sample_env["workspace_root"])
    payload, exit_code = controlled_git.run_git_command(
        repo=repo,
        git_args=["status", "--short"],
        execution_context="interactive",
        dry_run=False,
        explicit_remote="",
        project_name=PROJECT_NAME,
        session_id="sess-1",
    )
    assert exit_code == 0
    assert payload["decision"] == "allow"
    assert payload["executed"] is True

    push_payload, push_exit = controlled_git.run_git_command(
        repo=repo,
        git_args=["push", "origin", "main"],
        execution_context="noninteractive",
        dry_run=False,
        explicit_remote="origin",
        project_name=PROJECT_NAME,
        session_id="sess-2",
    )
    assert push_exit == 3
    assert push_payload["decision"] == "confirm"


def test_controlled_gh_and_ssh_and_browser(sample_env) -> None:
    gh_payload, gh_exit = controlled_gh.run_gh_command(
        gh_args=["pr", "create", "--title", "x", "--body", "y"],
        execution_context="noninteractive",
        dry_run=False,
        project_name=PROJECT_NAME,
        session_id="sess-gh",
    )
    assert gh_exit == 3
    assert gh_payload["decision"] == "confirm"

    ssh_payload, ssh_exit = controlled_ssh.run_ssh_command(
        tool="ssh",
        command=["user@example.com", "ls /var/log"],
        target="ssh://example.com",
        action="read",
        execution_context="interactive",
        dry_run=True,
        project_name=PROJECT_NAME,
        session_id="sess-ssh",
    )
    assert ssh_exit == 0
    assert ssh_payload["decision"] == "allow"
    assert ssh_payload["dry_run"] is True

    env = make_env(sample_env)
    browser = subprocess.run(
        [
            "python3",
            str(REPO_ROOT / "ops" / "controlled_browser.py"),
            "--target",
            "https://console.aliyun.com",
            "--action",
            "read",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(browser.stdout)
    assert payload["skeleton_only"] is True
    assert payload["executed"] is False


def test_start_codex_dry_run_prints_context(sample_env) -> None:
    codex_retrieval.build_index()
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "需求 demand Topic Retrieval Marker",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "context_scope=topic" in result.stdout
    assert "context_recommendation_count=" in result.stdout
    assert "context_retrieval_next_step=" in result.stdout
    assert "context_retrieval_counts=" in result.stdout
    assert "context_detail_path=" in result.stdout


def test_start_codex_dry_run_prints_launch_source_context(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续这个项目",
            "--source",
            "feishu",
            "--chat-ref",
            "oc_demo_chat",
            "--thread-name",
            "继续这个项目",
            "--thread-label",
            "CoCo 私聊",
            "--source-message-id",
            "om_demo_msg",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "launch_source=feishu" in result.stdout
    assert "chat_ref=oc_demo_chat" in result.stdout
    assert "thread_label=CoCo 私聊" in result.stdout
    assert '"launch_source": "feishu"' in result.stdout
    assert '"source_chat_ref": "oc_demo_chat"' in result.stdout
    assert "launcher_prompt_preview=" in result.stdout
    assert "[Launch]" in result.stdout
    assert "source=feishu" in result.stdout
    assert "thread=CoCo 私聊" in result.stdout
    assert "context_retrieval_next_step=" in result.stdout
    assert "context_retrieval_counts=" in result.stdout
    assert "feishu_resources.yaml" in result.stdout
    assert ".agents/skills/feishu-ops/SKILL.md" in result.stdout


def test_start_codex_dry_run_prints_weixin_launch_source_context(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续处理这个项目",
            "--source",
            "weixin",
            "--chat-ref",
            "weixin:default:wx-user",
            "--thread-name",
            "CoCo 私聊",
            "--thread-label",
            "CoCo 私聊",
            "--source-message-id",
            "wx_msg_demo",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "launch_source=weixin" in result.stdout
    assert "chat_ref=weixin:default:wx-user" in result.stdout
    assert "thread_label=CoCo 私聊" in result.stdout
    assert '"launch_source": "weixin"' in result.stdout
    assert "[Launch]" in result.stdout
    assert "source=weixin" in result.stdout
    assert "thread=CoCo 私聊" in result.stdout


def test_start_codex_dry_run_marks_weixin_private_dm_as_workspace_entry(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--prompt",
            "你是谁？",
            "--source",
            "weixin",
            "--chat-ref",
            "weixin:default:wx-user",
            "--thread-name",
            "CoCo 私聊",
            "--thread-label",
            "CoCo 私聊",
            "--source-message-id",
            "wx_msg_workspace",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=general" in result.stdout
    assert "[Launch]" in result.stdout
    assert "source=weixin" in result.stdout
    assert "workspace_entry=coco_private_dm" in result.stdout


def test_start_codex_dry_run_includes_attachment_context(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "帮我看一下这张图",
            "--source",
            "weixin",
            "--chat-ref",
            "weixin:default:wx-user",
            "--thread-name",
            "CoCo 私聊",
            "--thread-label",
            "CoCo 私聊",
            "--source-message-id",
            "wx_msg_attachment",
            "--attachment-path",
            "/tmp/weixin-image.png",
            "--attachment-type",
            "image",
            "--voice-transcript",
            "",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "attachment_type=image" in result.stdout
    assert "attachment_path=/tmp/weixin-image.png" in result.stdout
    assert '"attachment_path": "/tmp/weixin-image.png"' in result.stdout
    assert '"attachment_type": "image"' in result.stdout
    assert "launcher_prompt_preview=" in result.stdout
    assert "attachment=image" in result.stdout


def test_start_codex_dry_run_does_not_auto_resume_bridge_session_from_source_less_entry(sample_env) -> None:
    env = make_env(sample_env)
    session_router = sample_env["runtime_root"] / "session-router.json"
    session_router.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-03-25T00:00:00Z",
                "routes": {
                    "SampleProj": {
                        "project_name": "SampleProj",
                        "last_session_id": "sess-weixin-1",
                        "last_active_at": "2026-03-25T00:00:00Z",
                        "last_summary_path": "",
                        "last_thread_name": "CoCo 私聊",
                        "last_launch_source": "weixin",
                        "last_source_chat_ref": "weixin:default:wx-user",
                        "binding_scope": "project",
                        "binding_board_path": "",
                        "topic_name": "",
                        "rollup_target": "",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续处理这个项目",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=new" in result.stdout
    assert "session_id=" in result.stdout
    assert "session_id=sess-weixin-1" not in result.stdout


def test_start_codex_dry_run_keeps_auto_resume_with_matching_bridge_lane(sample_env) -> None:
    env = make_env(sample_env)
    session_router = sample_env["runtime_root"] / "session-router.json"
    recent_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    session_router.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": recent_at,
                "routes": {
                    "SampleProj": {
                        "project_name": "SampleProj",
                        "last_session_id": "sess-weixin-1",
                        "last_active_at": recent_at,
                        "last_summary_path": "",
                        "last_thread_name": "CoCo 私聊",
                        "last_launch_source": "weixin",
                        "last_source_chat_ref": "weixin:default:wx-user",
                        "binding_scope": "project",
                        "binding_board_path": "",
                        "topic_name": "",
                        "rollup_target": "",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续处理这个项目",
            "--source",
            "weixin",
            "--chat-ref",
            "weixin:default:wx-user",
            "--thread-name",
            "CoCo 私聊",
            "--thread-label",
            "CoCo 私聊",
            "--source-message-id",
            "wx_msg_demo",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=resume" in result.stdout
    assert "session_id=sess-weixin-1" in result.stdout


def test_start_codex_dry_run_uses_project_bindings_to_block_old_bridge_route(sample_env) -> None:
    env = make_env(sample_env)
    session_router = sample_env["runtime_root"] / "session-router.json"
    session_router.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-03-25T00:00:00Z",
                "routes": {
                    "SampleProj": {
                        "project_name": "SampleProj",
                        "last_session_id": "sess-weixin-1",
                        "last_active_at": "2026-03-25T00:00:00Z",
                        "last_summary_path": "",
                        "last_thread_name": "CoCo 私聊",
                        "binding_scope": "project",
                        "binding_board_path": "",
                        "topic_name": "",
                        "rollup_target": "",
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    project_bindings = sample_env["runtime_root"] / "project-bindings.json"
    project_bindings.write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": "2026-03-25T00:00:00Z",
                "bindings": [
                    {
                        "project_name": "SampleProj",
                        "session_id": "sess-weixin-1",
                        "last_active_at": "2026-03-25T00:00:00Z",
                        "launch_source": "weixin",
                        "source_chat_ref": "weixin:default:wx-user",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续处理这个项目",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=new" in result.stdout
    assert "session_id=sess-weixin-1" not in result.stdout


def test_start_codex_dry_run_surfaces_workflow_and_second_opinion_guidance(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "先帮我审一下这次改动，然后再让 Claude 给一个第二意见。",
            "--source",
            "feishu",
            "--chat-ref",
            "oc_demo_chat",
            "--thread-name",
            "Codex Hub",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "context_workflow_path=review -> claude-review" in result.stdout
    assert "context_second_opinion_skill=claude-review" in result.stdout
    assert "launcher_prompt_preview=" in result.stdout


def test_start_codex_dry_run_surfaces_followup_second_opinion_guidance(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "帮我审一下这次改动有没有问题。",
            "--source",
            "feishu",
            "--chat-ref",
            "oc_demo_chat",
            "--thread-name",
            "Codex Hub",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "context_workflow_path=review" in result.stdout
    assert "context_second_opinion_skill=claude-review" in result.stdout
    assert "context_second_opinion_source=followup" in result.stdout
    assert "context_second_opinion_template=review-risk-scan" in result.stdout
    assert "context_second_opinion_material_source=extractor" in result.stdout
    assert "context_second_opinion_fields=question,artifact,current_judgment,extra_context" in result.stdout
    assert "context_second_opinion_entrypoint=workflow-second-opinion" in result.stdout
    assert "context_second_opinion_focus=这个改动或方案最大的回归风险、盲区或缺失验证是什么？" in result.stdout


def test_start_codex_dry_run_surfaces_gflow_metadata(sample_env) -> None:
    env = make_env(sample_env)
    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "GFlow，帮我梳理这个需求，并从技术上评估主要风险。",
            "--source",
            "feishu",
            "--chat-ref",
            "oc_demo_chat",
            "--thread-name",
            "Codex Hub",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "context_gflow_mode=gflow-explicit" in result.stdout
    assert "context_gflow_entry_prompt=帮我梳理这个需求，并从技术上评估主要风险。" in result.stdout
    assert "context_gflow_path=office-hours -> plan-eng-review" in result.stdout
    assert "context_gflow_initial_stage=office-hours" in result.stdout
    assert "context_gflow_handoff_preview=GFlow explicit mode handoff" in result.stdout
    assert "run_id:" not in result.stdout
    assert "context_workflow_path=office-hours -> plan-eng-review" in result.stdout


def test_start_codex_dry_run_surfaces_active_gflow_runtime_summary(sample_env) -> None:
    env = make_env(sample_env)
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow: 先 review 再做 QA。",
        project_name="SampleProj",
        session_id="sess-runtime-preview",
    )

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "继续看看当前 workflow 的状态。",
            "--source",
            "feishu",
            "--chat-ref",
            "oc_demo_chat",
            "--thread-name",
            "Codex Hub",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert f"context_gflow_run_id={started['run_id']}" in result.stdout
    assert "context_gflow_run_status=running" in result.stdout
    assert "context_gflow_run_stage=review" in result.stdout
    assert "context_gflow_run_template_id=review-qa-ship" not in result.stdout


def test_start_codex_dry_run_surfaces_phase4_template_metadata(sample_env) -> None:
    env = make_env(sample_env)

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "GFlow，先 review，再做 QA，最后判断能不能 ship。",
            "--source",
            "weixin",
            "--chat-ref",
            "weixin:default:demo",
            "--thread-name",
            "CoCo 私聊",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "context_gflow_template_id=review-qa-ship" in result.stdout
    assert "context_gflow_template_label=Review -> QA -> Ship" in result.stdout


def test_start_codex_non_dry_run_auto_starts_gflow_and_passes_project_scope(sample_env) -> None:
    env = make_env(sample_env)
    fake_bin = sample_env["workspace_root"] / "fake-bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    capture_path = sample_env["runtime_root"] / "fake-codex-args.json"
    fake_codex = fake_bin / "codex"
    fake_codex.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, os, pathlib, sys",
                "args = sys.argv[1:]",
                "capture = os.environ.get('FAKE_CODEX_CAPTURE', '').strip()",
                "if capture:",
                "    pathlib.Path(capture).write_text(json.dumps(args, ensure_ascii=False), encoding='utf-8')",
                "if '-o' in args:",
                "    out = pathlib.Path(args[args.index('-o') + 1])",
                "    out.parent.mkdir(parents=True, exist_ok=True)",
                "    out.write_text('fake codex output\\n', encoding='utf-8')",
                "print('fake codex ok')",
                "sys.exit(0)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_codex.chmod(0o755)
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["FAKE_CODEX_CAPTURE"] = str(capture_path)

    subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--project",
            "SampleProj",
            "--prompt",
            "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
            "--no-open-obsidian",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    with sqlite3.connect(sample_env["runtime_root"] / "state" / "workspace-hub.db") as conn:
        run_row = conn.execute(
            "SELECT project_name, status, current_stage_skill FROM gflow_runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert run_row == ("SampleProj", "running", "review")

    captured_args = json.loads(capture_path.read_text(encoding="utf-8"))
    final_prompt = captured_args[-1]
    assert "[Launch]" in final_prompt
    assert "[Runtime]" in final_prompt
    assert "[Workflow]" in final_prompt
    assert "next_action=" in final_prompt
    assert "gflow_started=run_id=" in final_prompt
    assert "gflow_run=run_id=" in final_prompt
    assert "template=Review -> Fix -> QA -> Writeback" in final_prompt
    assert str(REPO_ROOT / "ops" / "gstack_automation.py") not in final_prompt
    assert str(REPO_ROOT / "ops" / "start-codex") not in final_prompt

    board_text = (
        sample_env["vault_root"] / "01_working" / "SampleProj-项目板.md"
    ).read_text(encoding="utf-8")
    assert "## GFlow Runs" in board_text
    assert "帮我review一下「GFlow」这个项目目前的代码。" in board_text


def test_gstack_automation_advance_cli_auto_executes_full_runtime_chain(sample_env) -> None:
    env = make_env(sample_env)
    env["WORKSPACE_HUB_GFLOW_STAGE_EXECUTOR"] = "fixture"
    env["WORKSPACE_HUB_GFLOW_STAGE_RESPONSES"] = json.dumps(
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
        ensure_ascii=False,
    )
    started = gstack_automation.start_workflow_run_from_prompt(
        "GFlow 帮我review一下「GFlow」这个项目目前的代码。",
        project_name="SampleProj",
        session_id="sess-runtime-e2e",
    )

    result = subprocess.run(
        [
            "python3",
            str(REPO_ROOT / "ops" / "gstack_automation.py"),
            "advance",
            "--run-id",
            started["run_id"],
            "--summary",
            "review 已完成，进入 fix。",
            "--next-action",
            "进入 `fix` 阶段。",
            "--evidence",
            "review-findings",
            "--json",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "gflow-run-completed"
    assert payload["run_status"] == "completed"
    assert [stage["skill"] for stage in payload["stage_results"]] == ["review", "fix", "qa", "writeback"]
    assert [stage["status"] for stage in payload["stage_results"]] == [
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    board_text = (
        sample_env["vault_root"] / "01_working" / "SampleProj-项目板.md"
    ).read_text(encoding="utf-8")
    assert "Review -> Fix -> QA -> Writeback" in board_text
    assert "已完成，无需继续。" in board_text


def test_start_codex_dry_run_resolves_unregistered_project_from_filesystem(sample_env) -> None:
    env = make_env(sample_env)
    new_project = sample_env["projects_root"] / "NewProject"
    new_project.mkdir(parents=True, exist_ok=True)
    (new_project / "README.md").write_text("# NewProject\n", encoding="utf-8")
    registry_before = (sample_env["vault_root"] / "PROJECT_REGISTRY.md").read_text(encoding="utf-8")

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--prompt",
            "我们来聊聊 NewProject 项目",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=new" in result.stdout
    assert "project=NewProject" in result.stdout
    registry_after = (sample_env["vault_root"] / "PROJECT_REGISTRY.md").read_text(encoding="utf-8")
    assert registry_after == registry_before


def test_start_codex_workspace_root_does_not_fall_back_to_code_root(sample_env) -> None:
    repo_only_name = f"WorktreeOnlyProject-{uuid.uuid4().hex}"
    repo_only_project = REPO_ROOT / "projects" / repo_only_name
    repo_only_project.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "WORKSPACE_HUB_ROOT": str(sample_env["workspace_root"]),
            "WORKSPACE_HUB_CODE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample_env["vault_root"]),
            "WORKSPACE_HUB_SKIP_DISCOVERY": "1",
        }
    )
    env.pop("WORKSPACE_HUB_PROJECTS_ROOT", None)
    env.pop("WORKSPACE_HUB_RUNTIME_ROOT", None)

    try:
        result = subprocess.run(
            [
                "bash",
                str(REPO_ROOT / "ops" / "start-codex"),
                "--prompt",
                f"我们来聊聊 {repo_only_name} 项目",
                "--dry-run",
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        assert f"project={repo_only_name}" not in result.stdout
    finally:
        repo_only_project.rmdir()


def test_start_codex_dry_run_supports_general_resume_without_project(sample_env) -> None:
    env = make_env(sample_env)

    result = subprocess.run(
        [
            "bash",
            str(REPO_ROOT / "ops" / "start-codex"),
            "--resume-session-id",
            "sess-general-1",
            "--prompt",
            "继续当前工作区会话",
            "--dry-run",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    assert "mode=resume" in result.stdout
    assert "project=general" in result.stdout
    assert "session_id=sess-general-1" in result.stdout

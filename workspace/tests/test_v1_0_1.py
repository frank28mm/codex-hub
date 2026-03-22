from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path

from ops import codex_context, codex_retrieval, controlled_gh, controlled_git, controlled_ssh, tint_backup_sync
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


def test_tint_backup_sync_uses_controlled_git(monkeypatch, sample_env) -> None:
    calls: list[dict[str, object]] = []

    def fake_run_git_command(**kwargs):
        calls.append(
            {
                "git_args": tuple(kwargs["git_args"]),
                "execution_context": kwargs["execution_context"],
                "explicit_remote": kwargs["explicit_remote"],
            }
        )
        return {"stdout": "ok\n", "stderr": ""}, 0

    monkeypatch.setattr(tint_backup_sync, "run_git_command", fake_run_git_command)
    output = tint_backup_sync.run_git(sample_env["workspace_root"], "fetch", "backup", "--prune", remote="backup")
    assert output == "ok"
    assert calls == [
        {
            "git_args": ("fetch", "backup", "--prune"),
            "execution_context": "noninteractive",
            "explicit_remote": "backup",
        }
    ]


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
    assert "来源渠道：Feishu 远程聊天线程" in result.stdout
    assert "来源线程标签：CoCo 私聊" in result.stdout
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
    assert "来源渠道：微信私聊线程" in result.stdout
    assert "来源线程名称：CoCo 私聊" in result.stdout


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
    assert "来源渠道：微信私聊线程" in result.stdout
    assert "来源线程名称：CoCo 私聊" in result.stdout
    assert "工作区级入口" in result.stdout


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

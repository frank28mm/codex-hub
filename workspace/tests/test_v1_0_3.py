from __future__ import annotations

import json
from pathlib import Path
import subprocess

from ops import controlled_browser, controlled_gh, controlled_git, controlled_ssh
from ops.workspace_hub_project import PROJECT_NAME

REPO_ROOT = Path(__file__).resolve().parents[1]


def init_git_repo(root: Path) -> Path:
    repo = root / "git-sample-v103"
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repo, check=True)
    (repo / "README.md").write_text("fixture\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, check=True, capture_output=True, text=True)
    subprocess.run(["git", "remote", "add", "origin", "https://github.com/example/sample.git"], cwd=repo, check=True)
    return repo


def assert_unified_shape(payload: dict) -> None:
    assert payload["request"]["request_id"]
    assert payload["request_id"] == payload["request"]["request_id"]
    assert payload["result"]["audit_ref"] == payload["audit_ref"]
    assert payload["result"]["decision"] == payload["decision"]
    assert payload["result"]["target_class"] == payload["target_class"]
    assert payload["result"]["action_class"] == payload["action_class"]
    assert payload["result"]["result_status"] == payload["result_status"]


def test_wrappers_emit_unified_contract(sample_env) -> None:
    repo = init_git_repo(sample_env["workspace_root"])

    git_payload, git_exit = controlled_git.run_git_command(
        repo=repo,
        git_args=["status", "--short"],
        execution_context="interactive",
        dry_run=False,
        explicit_remote="",
        project_name=PROJECT_NAME,
        session_id="sess-git",
    )
    assert git_exit == 0
    assert_unified_shape(git_payload)
    assert git_payload["result_status"] == "success"

    gh_payload, gh_exit = controlled_gh.run_gh_command(
        gh_args=["pr", "create", "--title", "x", "--body", "y"],
        execution_context="noninteractive",
        dry_run=False,
        project_name=PROJECT_NAME,
        session_id="sess-gh",
    )
    assert gh_exit == 3
    assert_unified_shape(gh_payload)
    assert gh_payload["result_status"] == "confirmation-required"

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
    assert_unified_shape(ssh_payload)
    assert ssh_payload["result_status"] == "dry-run"

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
        capture_output=True,
        text=True,
        check=True,
    )
    browser_payload = json.loads(browser.stdout)
    assert_unified_shape(browser_payload)
    assert browser_payload["result_status"] == "dry-run"

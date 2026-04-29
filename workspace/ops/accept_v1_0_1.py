#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.fixture_builder import build_sample_environment  # noqa: E402
from ops.workspace_hub_project import PROJECT_NAME  # noqa: E402


def run_command(cmd: list[str], *, env: dict[str, str] | None = None) -> dict[str, object]:
    completed = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def require_returncode(result: dict[str, object], allowed: set[int]) -> None:
    if int(result["returncode"]) not in allowed:
        raise RuntimeError(
            f"Command failed: {' '.join(result['cmd'])}\nstdout:\n{result['stdout']}\nstderr:\n{result['stderr']}"
        )


def build_fixture_env(temp_root: Path) -> tuple[dict[str, str], dict[str, Path]]:
    sample = build_sample_environment(temp_root)
    shutil.copytree(REPO_ROOT / "control", sample["control_root"], dirs_exist_ok=True)
    env = os.environ.copy()
    env.update(
        {
            "WORKSPACE_HUB_CODE_ROOT": str(REPO_ROOT),
            "WORKSPACE_HUB_ROOT": str(sample["workspace_root"]),
            "WORKSPACE_HUB_VAULT_ROOT": str(sample["vault_root"]),
            "WORKSPACE_HUB_FIXTURE_MODE": "1",
            "WORKSPACE_HUB_PROJECTS_ROOT": str(sample["projects_root"]),
            "WORKSPACE_HUB_REPORTS_ROOT": str(sample["reports_root"]),
            "WORKSPACE_HUB_RUNTIME_ROOT": str(sample["runtime_root"]),
            "WORKSPACE_HUB_CONTROL_ROOT": str(sample["control_root"]),
            "WORKSPACE_HUB_SKIP_DISCOVERY": "1",
        }
    )
    env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    return env, sample


def init_git_repo(root: Path) -> Path:
    repo = root / "acceptance-git"
    repo.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-b", "main"], cwd=repo, capture_output=True, check=True, text=True)
    subprocess.run(["git", "config", "user.email", "accept@example.com"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "config", "user.name", "Acceptance"], cwd=repo, capture_output=True, check=True)
    (repo / "README.md").write_text("acceptance\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=repo, capture_output=True, check=True)
    subprocess.run(["git", "commit", "-m", "init"], cwd=repo, capture_output=True, check=True, text=True)
    subprocess.run(["git", "remote", "add", "origin", "https://github.com/example/acceptance.git"], cwd=repo, capture_output=True, check=True)
    return repo


def main() -> int:
    parser = argparse.ArgumentParser(description="Run workspace-hub v1.0.1 acceptance checks")
    parser.add_argument("--keep", action="store_true", help="Keep the temporary fixture directory")
    args = parser.parse_args()

    temp_root = Path(tempfile.mkdtemp(prefix="workspace-hub-v1-0-1-accept-"))
    try:
        fixture_env, sample = build_fixture_env(temp_root)
        results: list[dict[str, object]] = []

        results.append(run_command(["python3", "-m", "pytest", "-q", "tests"], env=os.environ.copy()))
        require_returncode(results[-1], {0})

        py_files = [
            "ops/control_gate.py",
            "ops/codex_control.py",
            "ops/codex_retrieval.py",
            "ops/codex_context.py",
            "ops/controlled_common.py",
            "ops/controlled_git.py",
            "ops/controlled_gh.py",
            "ops/controlled_ssh.py",
            "ops/controlled_browser.py",
            "ops/accept_v1_0_1.py",
            "tests/conftest.py",
            "tests/fixture_builder.py",
            "tests/test_control_gate.py",
            "tests/test_retrieval.py",
            "tests/test_v1_0_1.py",
        ]
        results.append(run_command(["python3", "-m", "py_compile", *py_files], env=os.environ.copy()))
        require_returncode(results[-1], {0})

        results.append(run_command(["python3", "ops/codex_control.py", "status"], env=fixture_env))
        require_returncode(results[-1], {0})

        rules_dir = sample["workspace_root"] / ".codex" / "rules"
        results.append(
            run_command(
                ["python3", "ops/codex_control.py", "export-rules", "--output-dir", str(rules_dir)],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        export_payload = json.loads(str(results[-1]["stdout"]))
        generated_rules = Path(export_payload["output_path"])
        if not generated_rules.exists():
            raise RuntimeError(f"Generated rules file missing: {generated_rules}")

        results.append(
            run_command(
                ["codex", "execpolicy", "check", "--rules", str(generated_rules), "git", "push", "origin", "main"],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        policy_payload = json.loads(str(results[-1]["stdout"]))
        if policy_payload.get("decision") != "allow":
            raise RuntimeError(f"Unexpected execpolicy decision: {policy_payload}")
        results.append(
            run_command(
                ["codex", "execpolicy", "check", "--rules", str(generated_rules), "gh", "pr", "create"],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        external_write_payload = json.loads(str(results[-1]["stdout"]))
        if external_write_payload.get("decision") != "prompt":
            raise RuntimeError(f"Unexpected external write execpolicy decision: {external_write_payload}")
        results.append(
            run_command(
                ["codex", "execpolicy", "check", "--rules", str(generated_rules), "cat", "~/.ssh/config"],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        local_read_payload = json.loads(str(results[-1]["stdout"]))
        if local_read_payload.get("decision") == "allow" or local_read_payload.get("matchedRules"):
            raise RuntimeError(f"Unexpected filesystem auto-allow in rules export: {local_read_payload}")

        results.append(run_command(["python3", "ops/codex_retrieval.py", "build-index"], env=fixture_env))
        require_returncode(results[-1], {0})

        results.append(
            run_command(
                [
                    "python3",
                    "ops/codex_context.py",
                    "suggest",
                    "--project-name",
                    "SampleProj",
                    "--prompt",
                    "需求 demand Topic Retrieval Marker",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        context_payload = json.loads(str(results[-1]["stdout"]))
        if context_payload.get("binding_scope") != "topic":
            raise RuntimeError(f"Unexpected context payload: {context_payload}")

        new_project = sample["projects_root"] / "NewProject"
        new_project.mkdir(parents=True, exist_ok=True)
        (new_project / "README.md").write_text("# NewProject\n", encoding="utf-8")
        registry_before = (sample["vault_root"] / "PROJECT_REGISTRY.md").read_text(encoding="utf-8")
        results.append(
            run_command(
                [
                    "bash",
                    "ops/start-codex",
                    "--prompt",
                    "我们来聊聊 NewProject 项目",
                    "--dry-run",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})
        if "project=NewProject" not in str(results[-1]["stdout"]):
            raise RuntimeError(f"Dry-run failed to resolve unregistered project: {results[-1]['stdout']}")
        registry_after = (sample["vault_root"] / "PROJECT_REGISTRY.md").read_text(encoding="utf-8")
        if registry_after != registry_before:
            raise RuntimeError("Dry-run mutated project registry during preview discovery")

        repo = init_git_repo(sample["workspace_root"])
        results.append(
            run_command(
                [
                    "python3",
                    "ops/controlled_git.py",
                    "--repo",
                    str(repo),
                    "--execution-context",
                    "interactive",
                    "--project-name",
                    PROJECT_NAME,
                    "--session-id",
                    "accept-git-read",
                    "--",
                    "status",
                    "--short",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})

        results.append(
            run_command(
                [
                    "python3",
                    "ops/controlled_git.py",
                    "--repo",
                    str(repo),
                    "--remote",
                    "origin",
                    "--execution-context",
                    "noninteractive",
                    "--project-name",
                    PROJECT_NAME,
                    "--session-id",
                    "accept-git-push",
                    "--",
                    "push",
                    "origin",
                    "main",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {3})

        results.append(
            run_command(
                [
                    "python3",
                    "ops/controlled_gh.py",
                    "--execution-context",
                    "noninteractive",
                    "--project-name",
                    PROJECT_NAME,
                    "--session-id",
                    "accept-gh",
                    "--",
                    "pr",
                    "create",
                    "--title",
                    "demo",
                    "--body",
                    "demo",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {3})

        results.append(
            run_command(
                [
                    "python3",
                    "ops/controlled_ssh.py",
                    "--tool",
                    "ssh",
                    "--target",
                    "ssh://example.com",
                    "--action",
                    "read",
                    "--execution-context",
                    "interactive",
                    "--dry-run",
                    "--project-name",
                    PROJECT_NAME,
                    "--session-id",
                    "accept-ssh",
                    "--",
                    "user@example.com",
                    "ls /var/log",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})

        results.append(
            run_command(
                [
                    "python3",
                    "ops/controlled_browser.py",
                    "--target",
                    "https://console.aliyun.com",
                    "--action",
                    "read",
                    "--dry-run",
                    "--project-name",
                    PROJECT_NAME,
                    "--session-id",
                    "accept-browser",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})

        results.append(
            run_command(
                [
                    "bash",
                    "ops/start-codex",
                    "--project",
                    "SampleProj",
                    "--prompt",
                    "需求 demand Topic Retrieval Marker",
                    "--dry-run",
                ],
                env=fixture_env,
            )
        )
        require_returncode(results[-1], {0})

        results.append(run_command(["python3", "ops/codex_retrieval.py", "sync-index"], env=fixture_env))
        require_returncode(results[-1], {0})

        results.append(run_command(["python3", "ops/codex_retrieval.py", "status"], env=fixture_env))
        require_returncode(results[-1], {0})

        results.append(run_command(["python3", "ops/codex_dashboard_sync.py", "verify-consistency"], env=os.environ.copy()))
        require_returncode(results[-1], {0})

        summary = {
            "ok": True,
            "commands": len(results),
            "fixture_root": str(temp_root),
            "kept": bool(args.keep),
        }
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "fixture_root": str(temp_root)}, ensure_ascii=False))
        return 1
    finally:
        if not args.keep and temp_root.exists():
            shutil.rmtree(temp_root)


if __name__ == "__main__":
    raise SystemExit(main())

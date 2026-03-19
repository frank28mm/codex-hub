#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from ops.controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit, resolve_repo
except ImportError:  # pragma: no cover
    from controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit, resolve_repo


READ_COMMANDS = {"fetch", "pull", "status", "diff", "log", "show", "rev-parse", "branch", "ls-remote"}
WRITE_BUSINESS_COMMANDS = {"push"}
DESTRUCTIVE_COMMANDS = {
    ("reset", "--hard"): "privileged-or-irreversible",
    ("clean", "-fd"): "privileged-or-irreversible",
    ("clean", "-fdx"): "privileged-or-irreversible",
}


def infer_git_action(git_args: list[str]) -> str:
    if not git_args:
        return "read"
    first = git_args[0]
    for pattern, action in DESTRUCTIVE_COMMANDS.items():
        if git_args[: len(pattern)] == list(pattern):
            return action
    if first in READ_COMMANDS:
        return "read"
    if first in WRITE_BUSINESS_COMMANDS:
        return "reversible-write-business"
    if first == "remote":
        if len(git_args) >= 2 and git_args[1] in {"add", "remove", "rename", "set-url"}:
            return "reversible-write-system"
        return "read"
    if first in {"checkout", "switch", "merge", "rebase", "commit", "cherry-pick"}:
        return "reversible-write-system"
    return "reversible-write-system"


def infer_remote_name(git_args: list[str], explicit_remote: str) -> str:
    if explicit_remote:
        return explicit_remote
    if not git_args:
        return "origin"
    first = git_args[0]
    if first in {"fetch", "pull", "push"}:
        for item in git_args[1:]:
            if item.startswith("-"):
                continue
            return item
    return "origin"


def read_remote_url(repo: Path, remote: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), "remote", "get-url", remote],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout.strip()
    return f"local://git/{repo.name}"


def run_git_command(
    *,
    repo: Path,
    git_args: list[str],
    execution_context: str,
    dry_run: bool,
    explicit_remote: str,
    project_name: str,
    session_id: str,
) -> tuple[dict, int]:
    action = infer_git_action(git_args)
    remote = infer_remote_name(git_args, explicit_remote)
    target = read_remote_url(repo, remote)
    command = ["git", "-C", str(repo), *git_args]
    evaluation = evaluate_request(
        target=target,
        action=action,
        execution_context=execution_context,
        dry_run=dry_run,
        session_authority="explicit",
        data_sensitivity="internal-data",
        project_name=project_name,
        session_id=session_id,
        command=command,
        wrapper="controlled_git",
    )
    decision_payload = evaluation["result"]
    if dry_run:
        finalize_audit(
            target=target,
            action=action,
            result="dry-run",
            target_class=decision_payload["target_class"],
            action_class=decision_payload["action_class"],
            execution_context=execution_context,
            project_name=project_name,
            session_id=session_id,
            request_id=evaluation["request"]["request_id"],
            audit_ref=evaluation["audit_ref"],
        )
        return (
            build_action_result(
                evaluation=evaluation,
                target=target,
                action=action,
                command=command,
                dry_run=True,
                wrapper="controlled_git",
                result_status="dry-run",
                executed=False,
                extra={"remote": remote, "repo": str(repo)},
            ),
            0,
        )
    if decision_payload["decision"] == "deny":
        finalize_audit(
            target=target,
            action=action,
            result="denied",
            target_class=decision_payload["target_class"],
            action_class=decision_payload["action_class"],
            execution_context=execution_context,
            project_name=project_name,
            session_id=session_id,
            request_id=evaluation["request"]["request_id"],
            audit_ref=evaluation["audit_ref"],
        )
        return (
            build_action_result(
                evaluation=evaluation,
                target=target,
                action=action,
                command=command,
                dry_run=False,
                wrapper="controlled_git",
                result_status="denied",
                executed=False,
            )
            | {"repo": str(repo), "remote": remote},
            4,
        )
    if decision_payload["decision"] == "confirm":
        finalize_audit(
            target=target,
            action=action,
            result="confirmation-required",
            target_class=decision_payload["target_class"],
            action_class=decision_payload["action_class"],
            execution_context=execution_context,
            project_name=project_name,
            session_id=session_id,
            request_id=evaluation["request"]["request_id"],
            audit_ref=evaluation["audit_ref"],
        )
        return (
            build_action_result(
                evaluation=evaluation,
                target=target,
                action=action,
                command=command,
                dry_run=False,
                wrapper="controlled_git",
                result_status="confirmation-required",
                executed=False,
            )
            | {"repo": str(repo), "remote": remote},
            3,
        )

    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    finalize_audit(
        target=target,
        action=action,
        result="success" if completed.returncode == 0 else f"returncode:{completed.returncode}",
        target_class=decision_payload["target_class"],
        action_class=decision_payload["action_class"],
        execution_context=execution_context,
        project_name=project_name,
        session_id=session_id,
        request_id=evaluation["request"]["request_id"],
        audit_ref=evaluation["audit_ref"],
    )
    return (
        build_action_result(
            evaluation=evaluation,
            target=target,
            action=action,
            command=command,
            dry_run=False,
            wrapper="controlled_git",
            result_status="success" if completed.returncode == 0 else f"returncode:{completed.returncode}",
            executed=True,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            extra={"repo": str(repo), "remote": remote},
        ),
        completed.returncode,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run git commands through workspace-hub control policy.")
    parser.add_argument("--repo", default=".")
    parser.add_argument("--remote", default="")
    parser.add_argument("--execution-context", choices=["interactive", "noninteractive", "dry-run-capable"], default="interactive")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project-name", default="")
    parser.add_argument("--session-id", default="")
    parser.add_argument("git_args", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    git_args = list(args.git_args)
    if git_args and git_args[0] == "--":
        git_args = git_args[1:]
    if not git_args:
        parser.error("missing git arguments")
    payload, exit_code = run_git_command(
        repo=resolve_repo(args.repo),
        git_args=git_args,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        explicit_remote=args.remote,
        project_name=args.project_name,
        session_id=args.session_id,
    )
    emit_json(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

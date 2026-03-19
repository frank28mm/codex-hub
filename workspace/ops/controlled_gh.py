#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys

try:
    from ops.controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit
except ImportError:  # pragma: no cover
    from controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit


READ_PREFIXES = {
    ("pr", "view"),
    ("pr", "list"),
    ("issue", "list"),
    ("issue", "view"),
    ("repo", "view"),
    ("auth", "status"),
}
WRITE_PREFIXES = {
    ("pr", "create"),
    ("pr", "comment"),
    ("pr", "edit"),
    ("issue", "create"),
    ("issue", "comment"),
    ("issue", "edit"),
}
PRIVILEGED_PREFIXES = {
    ("secret", "set"),
    ("secret", "delete"),
    ("auth", "token"),
    ("repo", "delete"),
}


def infer_gh_action(gh_args: list[str]) -> str:
    if not gh_args:
        return "read"
    if gh_args[:2] in [list(item) for item in READ_PREFIXES]:
        return "read"
    if gh_args[:2] in [list(item) for item in WRITE_PREFIXES]:
        return "reversible-write-business"
    if gh_args[:2] in [list(item) for item in PRIVILEGED_PREFIXES]:
        return "privileged-or-irreversible"
    if gh_args[:2] == ["auth", "login"]:
        return "session-establish"
    return "read"


def run_gh_command(
    *,
    gh_args: list[str],
    execution_context: str,
    dry_run: bool,
    project_name: str,
    session_id: str,
) -> tuple[dict, int]:
    action = infer_gh_action(gh_args)
    target = "https://github.com"
    command = ["gh", *gh_args]
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
        wrapper="controlled_gh",
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
        return build_action_result(
            evaluation=evaluation,
            target=target,
            action=action,
            command=command,
            dry_run=True,
            wrapper="controlled_gh",
            result_status="dry-run",
            executed=False,
        ), 0
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
        return build_action_result(
            evaluation=evaluation,
            target=target,
            action=action,
            command=command,
            dry_run=False,
            wrapper="controlled_gh",
            result_status="denied",
            executed=False,
        ), 4
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
        return build_action_result(
            evaluation=evaluation,
            target=target,
            action=action,
            command=command,
            dry_run=False,
            wrapper="controlled_gh",
            result_status="confirmation-required",
            executed=False,
        ), 3

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
            wrapper="controlled_gh",
            result_status="success" if completed.returncode == 0 else f"returncode:{completed.returncode}",
            executed=True,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        ),
        completed.returncode,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run gh commands through workspace-hub control policy.")
    parser.add_argument("--execution-context", choices=["interactive", "noninteractive", "dry-run-capable"], default="interactive")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project-name", default="")
    parser.add_argument("--session-id", default="")
    parser.add_argument("gh_args", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    gh_args = list(args.gh_args)
    if gh_args and gh_args[0] == "--":
        gh_args = gh_args[1:]
    if not gh_args:
        parser.error("missing gh arguments")
    payload, exit_code = run_gh_command(
        gh_args=gh_args,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        project_name=args.project_name,
        session_id=args.session_id,
    )
    emit_json(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

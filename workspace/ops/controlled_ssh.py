#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

try:
    from ops.controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit
except ImportError:  # pragma: no cover
    from controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit


READ_ONLY_REMOTE_COMMANDS = {
    "cat",
    "df",
    "echo",
    "env",
    "find",
    "grep",
    "hostname",
    "journalctl",
    "ls",
    "pwd",
    "ps",
    "rg",
    "tail",
    "uname",
    "whoami",
}


def parse_target(tool: str, command: list[str], explicit_target: str) -> str:
    if explicit_target:
        return explicit_target if explicit_target.startswith("ssh://") else f"ssh://{explicit_target}"
    if tool == "ssh" and command:
        host = command[0]
        return host if host.startswith("ssh://") else f"ssh://{host}"
    for item in command:
        if ":" in item and not item.startswith("-"):
            host = item.split(":", 1)[0]
            if "@" in host:
                host = host.split("@", 1)[1]
            return host if host.startswith("ssh://") else f"ssh://{host}"
    return "ssh://unknown"


def infer_action(tool: str, command: list[str], explicit_action: str) -> str:
    if explicit_action:
        return explicit_action
    if tool in {"scp", "rsync"}:
        return "reversible-write-system"
    if len(command) >= 2:
        remote_command = command[1].split()[0]
        if remote_command in READ_ONLY_REMOTE_COMMANDS:
            return "read"
    return "reversible-write-system"


def run_ssh_command(
    *,
    tool: str,
    command: list[str],
    target: str,
    action: str,
    execution_context: str,
    dry_run: bool,
    project_name: str,
    session_id: str,
) -> tuple[dict, int]:
    shell_command = [tool, *command]
    evaluation = evaluate_request(
        target=target,
        action=action,
        execution_context=execution_context,
        dry_run=dry_run,
        session_authority="explicit",
        data_sensitivity="sensitive-data",
        project_name=project_name,
        session_id=session_id,
        command=shell_command,
        wrapper="controlled_ssh",
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
            command=shell_command,
            dry_run=True,
            wrapper="controlled_ssh",
            result_status="dry-run",
            executed=False,
            extra={"tool": tool},
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
            command=shell_command,
            dry_run=False,
            wrapper="controlled_ssh",
            result_status="denied",
            executed=False,
        ) | {"tool": tool}, 4
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
            command=shell_command,
            dry_run=False,
            wrapper="controlled_ssh",
            result_status="confirmation-required",
            executed=False,
        ) | {"tool": tool}, 3
    completed = subprocess.run(shell_command, capture_output=True, text=True, check=False)
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
            command=shell_command,
            dry_run=False,
            wrapper="controlled_ssh",
            result_status="success" if completed.returncode == 0 else f"returncode:{completed.returncode}",
            executed=True,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            extra={"tool": tool},
        ),
        completed.returncode,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ssh/scp/rsync commands through workspace-hub control policy.")
    parser.add_argument("--tool", choices=["ssh", "scp", "rsync"], required=True)
    parser.add_argument("--target", default="")
    parser.add_argument(
        "--action",
        choices=[
            "read",
            "session-establish",
            "reversible-write-business",
            "reversible-write-system",
            "privileged-or-irreversible",
        ],
        default="",
    )
    parser.add_argument("--execution-context", choices=["interactive", "noninteractive", "dry-run-capable"], default="interactive")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project-name", default="")
    parser.add_argument("--session-id", default="")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("missing ssh/scp/rsync arguments")
    target = parse_target(args.tool, command, args.target)
    action = infer_action(args.tool, command, args.action)
    payload, exit_code = run_ssh_command(
        tool=args.tool,
        command=command,
        target=target,
        action=action,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        project_name=args.project_name,
        session_id=args.session_id,
    )
    emit_json(payload)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

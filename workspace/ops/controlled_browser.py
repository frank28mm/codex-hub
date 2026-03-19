#!/usr/bin/env python3
from __future__ import annotations

import argparse

try:
    from ops.controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit
except ImportError:  # pragma: no cover
    from controlled_common import build_action_result, emit_json, evaluate_request, finalize_audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Policy-aware browser/cloud action skeleton.")
    parser.add_argument("--target", required=True)
    parser.add_argument(
        "--action",
        choices=[
            "read",
            "session-establish",
            "reversible-write-business",
            "reversible-write-system",
            "privileged-or-irreversible",
        ],
        required=True,
    )
    parser.add_argument("--operation", default="browser-skeleton")
    parser.add_argument("--execution-context", choices=["interactive", "noninteractive", "dry-run-capable"], default="interactive")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project-name", default="")
    parser.add_argument("--session-id", default="")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    evaluation = evaluate_request(
        target=args.target,
        action=args.action,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        session_authority="explicit",
        data_sensitivity="sensitive-data",
        project_name=args.project_name,
        session_id=args.session_id,
        command=[args.operation],
        wrapper="controlled_browser",
    )
    result = evaluation["result"]
    audit_result = "dry-run" if args.dry_run else "skeleton-only"
    finalize_audit(
        target=args.target,
        action=args.action,
        result=audit_result,
        target_class=result["target_class"],
        action_class=result["action_class"],
        execution_context=args.execution_context,
        project_name=args.project_name,
        session_id=args.session_id,
        request_id=evaluation["request"]["request_id"],
        audit_ref=evaluation["audit_ref"],
    )
    if not args.dry_run and result["decision"] == "deny":
        emit_json(
            build_action_result(
                evaluation=evaluation,
                target=args.target,
                action=args.action,
                command=[args.operation],
                dry_run=False,
                wrapper="controlled_browser",
                result_status="denied",
                executed=False,
            )
            | {"operation": args.operation, "skeleton_only": True}
        )
        return 4
    if not args.dry_run and result["decision"] == "confirm":
        emit_json(
            build_action_result(
                evaluation=evaluation,
                target=args.target,
                action=args.action,
                command=[args.operation],
                dry_run=False,
                wrapper="controlled_browser",
                result_status="confirmation-required",
                executed=False,
            )
            | {"operation": args.operation, "skeleton_only": True}
        )
        return 3

    emit_json(
        build_action_result(
            evaluation=evaluation,
            target=args.target,
            action=args.action,
            command=[args.operation],
            dry_run=args.dry_run,
            wrapper="controlled_browser",
            result_status=audit_result,
            executed=False,
            extra={"operation": args.operation, "skeleton_only": True},
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

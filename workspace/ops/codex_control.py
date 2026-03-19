#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

try:
    from ops.control_gate import audit_event, control_root, record_decision, decide_action, status_summary, workspace_root
    from ops.controlled_common import build_action_request
except ImportError:  # pragma: no cover
    from control_gate import audit_event, control_root, record_decision, decide_action, status_summary, workspace_root
    from controlled_common import build_action_request


def render_rules_file(exports: list[dict[str, object]]) -> str:
    lines = [
        "# Generated from control/action-policy.yaml",
        "# Do not hand-edit; run `python3 ops/codex_control.py export-rules` instead.",
        "",
    ]
    for item in exports:
        lines.append(f"# {item.get('id', 'unnamed-rule')}")
        justification = str(item.get("justification", "")).strip()
        decision = str(item.get("decision", "allow")).strip()
        patterns = item.get("patterns", [])
        if not isinstance(patterns, list):
            continue
        for pattern in patterns:
            if not isinstance(pattern, list) or not pattern:
                continue
            lines.append("prefix_rule(")
            lines.append(f"    pattern = {json.dumps(pattern, ensure_ascii=False)},")
            lines.append(f'    decision = "{decision}",')
            if justification:
                lines.append(f"    justification = {json.dumps(justification, ensure_ascii=False)},")
            lines.append(")")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def cmd_export_rules(args: argparse.Namespace) -> int:
    policy_path = control_root() / "action-policy.yaml"
    import yaml

    data = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
    exports = data.get("command_prefix_exports", [])
    if not isinstance(exports, list):
        raise ValueError("action-policy.yaml command_prefix_exports must be a list")
    output_dir = Path(args.output_dir or (workspace_root() / ".codex" / "rules"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (args.filename or "generated.rules")
    text = render_rules_file(exports)
    output_path.write_text(text, encoding="utf-8")
    payload = {
        "output_path": str(output_path),
        "rule_group_count": len(exports),
        "generated": True,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_decide(args: argparse.Namespace) -> int:
    request = build_action_request(
        target=args.target,
        action=args.action,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        session_authority=args.session_authority,
        data_sensitivity=args.data_sensitivity,
        project_name=args.project_name,
        session_id=args.session_id,
        wrapper="codex_control",
    )
    result = decide_action(
        target=args.target,
        action=args.action,
        execution_context=args.execution_context,
        dry_run=args.dry_run,
        session_authority=args.session_authority,
        data_sensitivity=args.data_sensitivity,
    )
    record = record_decision(
        target=args.target,
        action=args.action,
        execution_context=args.execution_context,
        result=result,
        project_name=args.project_name,
        session_id=args.session_id,
        request_id=request["request_id"],
    )
    payload = {
        "request": request,
        "result": {
            **result,
            "audit_ref": f"audit:{request['request_id']}",
            "result_status": "decision-only",
        },
        "request_id": request["request_id"],
        "decision": result["decision"],
        "reason_code": result["reason_code"],
        "audit_required": result["audit_required"],
        "audit_ref": f"audit:{request['request_id']}",
        "target_class": result["target_class"],
        "action_class": result["action_class"],
        "result_status": "decision-only",
        "target": args.target,
        "action": args.action,
        "project_name": args.project_name,
        "session_id": args.session_id,
        "recorded_at": record["timestamp"],
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    audit_ref = args.audit_ref or f"audit:{args.request_id}" if args.request_id else ""
    event = audit_event(
        target=args.target,
        action=args.action,
        result=args.result,
        target_class=args.target_class,
        action_class=args.action_class,
        execution_context=args.execution_context,
        project_name=args.project_name,
        session_id=args.session_id,
        request_id=args.request_id,
        audit_ref=audit_ref,
    )
    print(json.dumps(event, ensure_ascii=False))
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    print(json.dumps(status_summary(), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub control policy utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    decide = subparsers.add_parser("decide")
    decide.add_argument("--target", required=True)
    decide.add_argument("--action", required=True)
    decide.add_argument("--execution-context", choices=["interactive", "noninteractive", "dry-run-capable"], required=True)
    decide.add_argument("--dry-run", action="store_true")
    decide.add_argument("--session-authority", default="explicit")
    decide.add_argument("--data-sensitivity", default="internal-data")
    decide.add_argument("--project-name", default="")
    decide.add_argument("--session-id", default="")
    decide.set_defaults(func=cmd_decide)

    audit = subparsers.add_parser("audit")
    audit.add_argument("--target", required=True)
    audit.add_argument("--action", required=True)
    audit.add_argument("--result", required=True)
    audit.add_argument("--target-class", default="")
    audit.add_argument("--action-class", default="")
    audit.add_argument("--execution-context", default="")
    audit.add_argument("--project-name", default="")
    audit.add_argument("--session-id", default="")
    audit.add_argument("--request-id", default="")
    audit.add_argument("--audit-ref", default="")
    audit.set_defaults(func=cmd_audit)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    export = subparsers.add_parser("export-rules")
    export.add_argument("--output-dir", default="")
    export.add_argument("--filename", default="generated.rules")
    export.set_defaults(func=cmd_export_rules)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

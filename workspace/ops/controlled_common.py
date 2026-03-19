from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

try:
    from ops.control_gate import audit_event, decide_action, record_decision
except ImportError:  # pragma: no cover
    from control_gate import audit_event, decide_action, record_decision


def build_action_request(
    *,
    target: str,
    action: str,
    execution_context: str,
    dry_run: bool,
    session_authority: str,
    data_sensitivity: str,
    project_name: str,
    session_id: str,
    command: list[str] | None = None,
    wrapper: str = "",
    request_id: str = "",
) -> dict[str, Any]:
    return {
        "request_id": request_id or str(uuid.uuid4()),
        "target": target,
        "action": action,
        "execution_context": execution_context,
        "dry_run": dry_run,
        "project_name": project_name,
        "session_id": session_id,
        "session_authority": session_authority,
        "data_sensitivity": data_sensitivity,
        "command": command or [],
        "wrapper": wrapper,
    }


def evaluate_request(
    *,
    target: str,
    action: str,
    execution_context: str,
    dry_run: bool,
    session_authority: str,
    data_sensitivity: str,
    project_name: str,
    session_id: str,
    command: list[str] | None = None,
    wrapper: str = "",
    request_id: str = "",
) -> dict[str, Any]:
    request = build_action_request(
        target=target,
        action=action,
        execution_context=execution_context,
        dry_run=dry_run,
        session_authority=session_authority,
        data_sensitivity=data_sensitivity,
        project_name=project_name,
        session_id=session_id,
        command=command,
        wrapper=wrapper,
        request_id=request_id,
    )
    result = decide_action(
        target=target,
        action=action,
        execution_context=execution_context,
        dry_run=dry_run,
        session_authority=session_authority,
        data_sensitivity=data_sensitivity,
    )
    record = record_decision(
        target=target,
        action=action,
        execution_context=execution_context,
        result=result,
        project_name=project_name,
        session_id=session_id,
        request_id=request["request_id"],
    )
    return {"request": request, "result": result, "record": record, "audit_ref": f"audit:{request['request_id']}"}


def emit_json(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def finalize_audit(
    *,
    target: str,
    action: str,
    result: str,
    target_class: str,
    action_class: str,
    execution_context: str,
    project_name: str,
    session_id: str,
    request_id: str = "",
    audit_ref: str = "",
) -> None:
    audit_event(
        target=target,
        action=action,
        result=result,
        target_class=target_class,
        action_class=action_class,
        execution_context=execution_context,
        project_name=project_name,
        session_id=session_id,
        request_id=request_id,
        audit_ref=audit_ref,
    )


def build_action_result(
    *,
    evaluation: dict[str, Any],
    target: str,
    action: str,
    command: list[str],
    dry_run: bool,
    wrapper: str,
    result_status: str,
    executed: bool,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request = dict(evaluation["request"])
    result = dict(evaluation["result"])
    payload = {
        "request": request,
        "result": {
            "decision": result["decision"],
            "reason_code": result["reason_code"],
            "audit_required": result["audit_required"],
            "audit_ref": evaluation["audit_ref"],
            "result_status": result_status,
            "target_class": result["target_class"],
            "action_class": result["action_class"],
            "execution_profile": result["execution_profile"],
            "requires_dry_run": result["requires_dry_run"],
            "matched_rule": result["matched_rule"],
        },
        "request_id": request["request_id"],
        "target": target,
        "action": action,
        "command": command,
        "dry_run": dry_run,
        "wrapper": wrapper,
        "decision": result["decision"],
        "reason_code": result["reason_code"],
        "audit_required": result["audit_required"],
        "audit_ref": evaluation["audit_ref"],
        "result_status": result_status,
        "target_class": result["target_class"],
        "action_class": result["action_class"],
        "execution_profile": result["execution_profile"],
        "requires_dry_run": result["requires_dry_run"],
        "matched_rule": result["matched_rule"],
        "executed": executed,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    if extra:
        payload.update(extra)
    return payload


def blocked_payload(
    *,
    decision_payload: dict[str, Any],
    target: str,
    action: str,
    command: list[str],
    dry_run: bool,
    wrapper: str,
) -> dict[str, Any]:
    return {
        **decision_payload,
        "target": target,
        "action": action,
        "command": command,
        "dry_run": dry_run,
        "executed": False,
        "wrapper": wrapper,
    }


def completed_payload(
    *,
    decision_payload: dict[str, Any],
    target: str,
    action: str,
    command: list[str],
    dry_run: bool,
    wrapper: str,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = blocked_payload(
        decision_payload=decision_payload,
        target=target,
        action=action,
        command=command,
        dry_run=dry_run,
        wrapper=wrapper,
    )
    payload.update(
        {
            "executed": not dry_run,
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
        }
    )
    if extra:
        payload.update(extra)
    return payload


def resolve_repo(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()

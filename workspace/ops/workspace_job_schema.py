from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


RUN_STATUS_VALUES = {"ok", "error", "skipped"}
DELIVERY_STATUS_VALUES = {"delivered", "not-delivered", "unknown", "not-requested"}
SCOPE_TYPE_VALUES = {"project", "workspace"}
APPROVAL_STATE_VALUES = {"not-required", "pending", "approved", "rejected"}
PROGRAM_STAGE_VALUES = {"discover", "frame", "execute", "verify", "adapt", "handoff"}
PROGRAM_DECISION_VALUES = {"continue", "gate", "blocked", "done", "adapt", "initialized"}


def _normalize_run_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized not in RUN_STATUS_VALUES:
        raise ValueError(f"unsupported run status: {status}")
    return normalized


def _normalize_delivery_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized not in DELIVERY_STATUS_VALUES:
        raise ValueError(f"unsupported delivery status: {status}")
    return normalized


def _normalize_scope_type(scope_type: str) -> str:
    normalized = scope_type.strip().lower()
    if normalized not in SCOPE_TYPE_VALUES:
        raise ValueError(f"unsupported scope type: {scope_type}")
    return normalized


def _normalize_approval_state(state: str) -> str:
    normalized = state.strip().lower()
    if normalized not in APPROVAL_STATE_VALUES:
        raise ValueError(f"unsupported approval state: {state}")
    return normalized


def _normalize_program_stage(stage: str) -> str:
    normalized = stage.strip().lower()
    if normalized not in PROGRAM_STAGE_VALUES:
        raise ValueError(f"unsupported program stage: {stage}")
    return normalized


def _normalize_program_decision(decision: str) -> str:
    normalized = decision.strip().lower()
    if normalized not in PROGRAM_DECISION_VALUES:
        raise ValueError(f"unsupported program decision: {decision}")
    return normalized


def _normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not metadata:
        return {}
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if value in (None, "", [], {}):
            continue
        normalized[str(key)] = value
    return normalized


def _parse_timestamp(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def timestamp_millis(text: str) -> int:
    parsed = _parse_timestamp(text)
    if parsed is None:
        return 0
    return int(parsed.timestamp() * 1000)


def duration_millis(started_at: str, finished_at: str) -> int:
    started = _parse_timestamp(started_at)
    finished = _parse_timestamp(finished_at)
    if started is None or finished is None:
        return 0
    return max(0, int((finished - started).total_seconds() * 1000))


def handoff_bundle_paths(artifacts_root: str | Path) -> dict[str, str]:
    root = Path(artifacts_root)
    return {
        "task_spec_path": str(root / "task-spec.json"),
        "acceptance_path": str(root / "acceptance.json"),
        "progress_path": str(root / "progress.md"),
        "latest_smoke_path": str(root / "latest-smoke.md"),
    }


def read_json_file(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_file(path_text: str, payload: dict[str, Any]) -> None:
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_text_file(path_text: str, text: str) -> None:
    path = Path(path_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def next_program_stage(current_stage: str, *, decision: str, has_pending_subgoals: bool) -> str:
    stage = _normalize_program_stage(current_stage)
    normalized_decision = _normalize_program_decision(decision)
    if normalized_decision in {"gate", "blocked", "initialized"}:
        return stage
    if normalized_decision == "done":
        return "handoff"
    if normalized_decision == "adapt":
        return "adapt"
    if stage == "discover":
        return "frame"
    if stage == "frame":
        return "execute"
    if stage == "execute":
        return "verify"
    if stage == "verify":
        return "execute" if has_pending_subgoals else "handoff"
    if stage == "adapt":
        return "execute"
    return "handoff"


@dataclass(frozen=True)
class ProgramSpec:
    program_id: str
    workspace_scope: str
    objective: str
    priority: str = "medium"
    scope_type: str = "project"
    scope_ref: str = ""
    approval_required: bool = False
    approval_state: str = "not-required"
    stage: str = "discover"
    stage_plan: list[str] = field(default_factory=list)
    wake_policy: dict[str, Any] = field(default_factory=dict)
    loop_policy: dict[str, Any] = field(default_factory=dict)
    delivery_policy: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scope_type"] = _normalize_scope_type(self.scope_type)
        payload["approval_state"] = _normalize_approval_state(self.approval_state)
        payload["stage"] = _normalize_program_stage(self.stage)
        payload["metadata"] = _normalize_metadata(self.metadata)
        payload["wake_policy"] = _normalize_metadata(self.wake_policy)
        payload["loop_policy"] = _normalize_metadata(self.loop_policy)
        payload["delivery_policy"] = _normalize_metadata(self.delivery_policy)
        payload["stage_plan"] = [item for item in self.stage_plan if item]
        return payload


@dataclass(frozen=True)
class ProgramEvaluation:
    current_stage: str
    next_stage: str
    decision: str
    acceptance_status: str
    delivery_status: str
    completed_subgoal_count: int = 0
    pending_subgoal_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "current_stage": _normalize_program_stage(self.current_stage),
            "next_stage": _normalize_program_stage(self.next_stage),
            "decision": _normalize_program_decision(self.decision),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class JobExecutionOutcome:
    status: str
    summary: str
    error: str = ""
    issue_count: int = 0
    alert_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "status": _normalize_run_status(self.status),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class JobDeliveryOutcome:
    delivery_id: str
    status: str
    requested: bool = True
    summary: str = ""
    error: str = ""
    targets: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "status": _normalize_delivery_status(self.status),
            "targets": [item for item in self.targets if item],
            "metadata": _normalize_metadata(self.metadata),
        }


def aggregate_delivery_status(outcomes: list[JobDeliveryOutcome | dict[str, Any]]) -> str:
    normalized: list[dict[str, Any]] = []
    for item in outcomes:
        payload = item.to_dict() if isinstance(item, JobDeliveryOutcome) else dict(item)
        payload["status"] = _normalize_delivery_status(str(payload.get("status", "unknown")))
        normalized.append(payload)
    requested = [item for item in normalized if bool(item.get("requested", True))]
    if not requested:
        return "not-requested"
    statuses = {str(item.get("status", "unknown")) for item in requested}
    if statuses == {"delivered"}:
        return "delivered"
    if "not-delivered" in statuses:
        return "not-delivered"
    if statuses == {"not-requested"}:
        return "not-requested"
    return "unknown"


def build_run_ledger_entry(
    *,
    job_id: str,
    run_id: str,
    started_at: str,
    finished_at: str,
    trigger_source: str,
    scheduled_for: str,
    automation_run_id: str,
    scheduler_id: str,
    script_version: str,
    report_path: str,
    latest_report_path: str,
    writeback_targets: list[str],
    execution_outcome: JobExecutionOutcome | dict[str, Any],
    delivery_outcomes: list[JobDeliveryOutcome | dict[str, Any]],
    overall_ok: bool,
    artifacts: dict[str, Any] | None = None,
    gate_state: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    execution_payload = (
        execution_outcome.to_dict() if isinstance(execution_outcome, JobExecutionOutcome) else dict(execution_outcome)
    )
    execution_payload["status"] = _normalize_run_status(str(execution_payload.get("status", "error")))
    normalized_deliveries = [
        item.to_dict() if isinstance(item, JobDeliveryOutcome) else dict(item) for item in delivery_outcomes
    ]
    for item in normalized_deliveries:
        item["status"] = _normalize_delivery_status(str(item.get("status", "unknown")))
        item["metadata"] = _normalize_metadata(item.get("metadata"))
        item["targets"] = [target for target in item.get("targets", []) if target]
    delivery_status = aggregate_delivery_status(normalized_deliveries)
    requested_deliveries = [item for item in normalized_deliveries if bool(item.get("requested", True))]
    delivered_deliveries = [item for item in requested_deliveries if item.get("status") == "delivered"]
    failed_deliveries = [item for item in requested_deliveries if item.get("status") == "not-delivered"]
    entry = {
        "version": 1,
        "ts": timestamp_millis(finished_at),
        "action": "finished",
        "job_id": job_id,
        "run_id": run_id,
        "status": execution_payload["status"],
        "summary": str(execution_payload.get("summary", "")),
        "error": str(execution_payload.get("error", "")),
        "delivered": delivery_status == "delivered",
        "delivery_status": delivery_status,
        "delivery_requested_count": len(requested_deliveries),
        "delivery_success_count": len(delivered_deliveries),
        "delivery_failure_count": len(failed_deliveries),
        "started_at": started_at,
        "finished_at": finished_at,
        "checked_at": finished_at,
        "duration_ms": duration_millis(started_at, finished_at),
        "trigger_source": trigger_source,
        "scheduled_for": scheduled_for,
        "automation_run_id": automation_run_id,
        "scheduler_id": scheduler_id,
        "script_version": script_version,
        "report_path": report_path,
        "latest_report_path": latest_report_path,
        "writeback_targets": [item for item in writeback_targets if item],
        "issue_count": int(execution_payload.get("issue_count", 0) or 0),
        "alert_count": int(execution_payload.get("alert_count", 0) or 0),
        "ok": bool(overall_ok),
        "execution_outcome": execution_payload,
        "delivery_outcomes": normalized_deliveries,
        "artifacts": _normalize_metadata(artifacts),
        "gate_state": _normalize_metadata(gate_state),
        "metadata": _normalize_metadata(metadata),
    }
    return entry

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
PROGRAM_DECISION_VALUES = {"continue", "retry", "gate", "blocked", "done", "adapt", "initialized"}
EXTENSION_MANIFEST_LIFECYCLE_VALUES = {"registered", "loaded", "enabled", "disabled", "errored"}


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


def _normalize_extension_manifest_lifecycle(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in EXTENSION_MANIFEST_LIFECYCLE_VALUES:
        return "registered"
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


def _normalize_string_list(items: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if not items:
        return []
    if isinstance(items, str):
        items = [items]
    normalized: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        normalized.append(text)
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
    if normalized_decision == "retry":
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


@dataclass(frozen=True)
class CompressionCheckpoint:
    checkpoint_id: str
    level: str
    trigger: str
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "checkpoint_id": str(self.checkpoint_id).strip(),
            "level": str(self.level).strip(),
            "trigger": str(self.trigger).strip(),
            "summary": str(self.summary).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class CompressionPolicy:
    l1_strategy: str = "tool-output-trim"
    l2_strategy: str = "session-summary"
    l3_strategy: str = "handoff-summary"
    checkpoints: list[CompressionCheckpoint | dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "l1_strategy": str(self.l1_strategy).strip(),
            "l2_strategy": str(self.l2_strategy).strip(),
            "l3_strategy": str(self.l3_strategy).strip(),
            "checkpoints": [
                item.to_dict() if isinstance(item, CompressionCheckpoint) else dict(item)
                for item in self.checkpoints
            ],
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class PreCompletionChecklist:
    checklist_id: str = "precompletion-checklist"
    status: str = "armed"
    required_checks: list[str] = field(default_factory=list)
    reminder: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "checklist_id": str(self.checklist_id).strip(),
            "status": str(self.status).strip(),
            "required_checks": _normalize_string_list(self.required_checks),
            "reminder": str(self.reminder).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class LoopDetectionState:
    detector_id: str = "loop-detection"
    status: str = "watching"
    repeated_target_limit: int = 3
    repeated_targets: list[str] = field(default_factory=list)
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "detector_id": str(self.detector_id).strip(),
            "status": str(self.status).strip(),
            "repeated_target_limit": max(1, int(self.repeated_target_limit or 1)),
            "repeated_targets": _normalize_string_list(self.repeated_targets),
            "summary": str(self.summary).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class LocalContextOverlay:
    workspace_root: str = ""
    project_root: str = ""
    board_path: str = ""
    allow_paths: list[str] = field(default_factory=list)
    hot_paths: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_root": str(self.workspace_root).strip(),
            "project_root": str(self.project_root).strip(),
            "board_path": str(self.board_path).strip(),
            "allow_paths": _normalize_string_list(self.allow_paths),
            "hot_paths": _normalize_string_list(self.hot_paths),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class RuntimeOverlay:
    project_name: str
    task_id: str
    run_id: str = ""
    source: str = ""
    scope: str = ""
    current_stage: str = ""
    current_focus: str = ""
    board_path: str = ""
    task_pointer: str = ""
    deliverable: str = ""
    artifacts_root: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_name": str(self.project_name).strip(),
            "task_id": str(self.task_id).strip(),
            "run_id": str(self.run_id).strip(),
            "source": str(self.source).strip(),
            "scope": str(self.scope).strip(),
            "current_stage": str(self.current_stage).strip(),
            "current_focus": str(self.current_focus).strip(),
            "board_path": str(self.board_path).strip(),
            "task_pointer": str(self.task_pointer).strip(),
            "deliverable": str(self.deliverable).strip(),
            "artifacts_root": str(self.artifacts_root).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class HandoffPacket:
    task_spec_path: str
    acceptance_path: str
    progress_path: str
    latest_smoke_path: str
    latest_report_path: str = ""
    latest_ops_report_path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_spec_path": str(self.task_spec_path).strip(),
            "acceptance_path": str(self.acceptance_path).strip(),
            "progress_path": str(self.progress_path).strip(),
            "latest_smoke_path": str(self.latest_smoke_path).strip(),
            "latest_report_path": str(self.latest_report_path).strip(),
            "latest_ops_report_path": str(self.latest_ops_report_path).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class TaskRuntimeSnapshot:
    harness_state: str = ""
    current_stage: str = ""
    last_decision: str = ""
    current_focus: str = ""
    next_action: str = ""
    next_wake_at: str = ""
    blocked_reason: str = ""
    last_run_id: str = ""
    active_run_id: str = ""
    running_started_at: str = ""
    completed_subgoal_count: int = 0
    pending_subgoal_count: int = 0
    running_stale: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "harness_state": str(self.harness_state).strip(),
            "current_stage": str(self.current_stage).strip(),
            "last_decision": str(self.last_decision).strip(),
            "current_focus": str(self.current_focus).strip(),
            "next_action": str(self.next_action).strip(),
            "next_wake_at": str(self.next_wake_at).strip(),
            "blocked_reason": str(self.blocked_reason).strip(),
            "last_run_id": str(self.last_run_id).strip(),
            "active_run_id": str(self.active_run_id).strip(),
            "running_started_at": str(self.running_started_at).strip(),
            "completed_subgoal_count": max(0, int(self.completed_subgoal_count or 0)),
            "pending_subgoal_count": max(0, int(self.pending_subgoal_count or 0)),
            "running_stale": bool(self.running_stale),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class HarnessSnapshot:
    runtime_overlay: dict[str, Any] = field(default_factory=dict)
    task_runtime: dict[str, Any] = field(default_factory=dict)
    handoff_packet: dict[str, Any] = field(default_factory=dict)
    compression_policy: dict[str, Any] = field(default_factory=dict)
    middleware: dict[str, Any] = field(default_factory=dict)
    project_runtime: dict[str, Any] = field(default_factory=dict)
    bridge_runtime: dict[str, Any] = field(default_factory=dict)
    run_tree: dict[str, Any] = field(default_factory=dict)
    delivery_contract: dict[str, Any] = field(default_factory=dict)
    execution_boundary: dict[str, Any] = field(default_factory=dict)
    instruction_surface: dict[str, Any] = field(default_factory=dict)
    extension_manifest: dict[str, Any] = field(default_factory=dict)
    workflow_manifest: dict[str, Any] = field(default_factory=dict)
    instruction_migration: dict[str, Any] = field(default_factory=dict)
    open_source_boundary: dict[str, Any] = field(default_factory=dict)
    updated_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_overlay": dict(self.runtime_overlay),
            "task_runtime": dict(self.task_runtime),
            "handoff_packet": dict(self.handoff_packet),
            "compression_policy": dict(self.compression_policy),
            "middleware": dict(self.middleware),
            "project_runtime": dict(self.project_runtime),
            "bridge_runtime": dict(self.bridge_runtime),
            "run_tree": dict(self.run_tree),
            "delivery_contract": dict(self.delivery_contract),
            "execution_boundary": dict(self.execution_boundary),
            "instruction_surface": dict(self.instruction_surface),
            "extension_manifest": dict(self.extension_manifest),
            "workflow_manifest": dict(self.workflow_manifest),
            "instruction_migration": dict(self.instruction_migration),
            "open_source_boundary": dict(self.open_source_boundary),
            "updated_at": str(self.updated_at).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class ActionRegistryEntry:
    action_id: str
    broker_action: str
    operation_key: str
    surface: str = "local_broker"
    target_ref: str = ""
    gate_policy: str = "none"
    execution_profile: str = ""
    retry_semantics: str = "caller_defined"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": str(self.action_id).strip(),
            "broker_action": str(self.broker_action).strip(),
            "operation_key": str(self.operation_key).strip(),
            "surface": str(self.surface).strip(),
            "target_ref": str(self.target_ref).strip(),
            "gate_policy": str(self.gate_policy).strip(),
            "execution_profile": str(self.execution_profile).strip(),
            "retry_semantics": str(self.retry_semantics).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class OperationPolicy:
    mode: str = "auto"
    risk: str = ""
    reason: str = ""
    expected_scope: str = ""
    retryable: bool = True
    blocked_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": str(self.mode).strip(),
            "risk": str(self.risk).strip(),
            "reason": str(self.reason).strip(),
            "expected_scope": str(self.expected_scope).strip(),
            "retryable": bool(self.retryable),
            "blocked_reason": str(self.blocked_reason).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class PrincipalPolicy:
    actor_id: str = ""
    actor_surface: str = "local_broker"
    principal_kind: str = "workspace_operator"
    principal_ref: str = ""
    source: str = ""
    project_name: str = ""
    session_id: str = ""
    approval_token: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": str(self.actor_id).strip(),
            "actor_surface": str(self.actor_surface).strip(),
            "principal_kind": str(self.principal_kind).strip(),
            "principal_ref": str(self.principal_ref).strip(),
            "source": str(self.source).strip(),
            "project_name": str(self.project_name).strip(),
            "session_id": str(self.session_id).strip(),
            "approval_token": str(self.approval_token).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class ExecutionBoundary:
    boundary_id: str
    sandbox_mode: str = ""
    network_access: str = ""
    writable_roots: list[str] = field(default_factory=list)
    requires_approval: bool = False
    expected_scope: str = ""
    monitor_mode: str = "runtime_state"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "boundary_id": str(self.boundary_id).strip(),
            "sandbox_mode": str(self.sandbox_mode).strip(),
            "network_access": str(self.network_access).strip(),
            "writable_roots": _normalize_string_list(self.writable_roots),
            "requires_approval": bool(self.requires_approval),
            "expected_scope": str(self.expected_scope).strip(),
            "monitor_mode": str(self.monitor_mode).strip(),
            "metadata": _normalize_metadata(self.metadata),
        }


def execution_boundary_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return ExecutionBoundary(
        boundary_id=str(data.get("boundary_id", "")).strip(),
        sandbox_mode=str(data.get("sandbox_mode", "")).strip(),
        network_access=str(data.get("network_access", "")).strip(),
        writable_roots=_normalize_string_list(data.get("writable_roots")),
        requires_approval=bool(data.get("requires_approval", False)),
        expected_scope=str(data.get("expected_scope", "")).strip(),
        monitor_mode=str(data.get("monitor_mode", "runtime_state")).strip() or "runtime_state",
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


@dataclass(frozen=True)
class InstructionSurface:
    human_guides: list[str] = field(default_factory=list)
    generated_rules: list[str] = field(default_factory=list)
    hook_enforcement: list[str] = field(default_factory=list)
    policy_enforcement: list[str] = field(default_factory=list)
    command_surfaces: list[str] = field(default_factory=list)
    migration_checklist: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "human_guides": _normalize_string_list(self.human_guides),
            "generated_rules": _normalize_string_list(self.generated_rules),
            "hook_enforcement": _normalize_string_list(self.hook_enforcement),
            "policy_enforcement": _normalize_string_list(self.policy_enforcement),
            "command_surfaces": _normalize_string_list(self.command_surfaces),
            "migration_checklist": _normalize_string_list(self.migration_checklist),
            "metadata": _normalize_metadata(self.metadata),
        }


def instruction_surface_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return InstructionSurface(
        human_guides=_normalize_string_list(data.get("human_guides")),
        generated_rules=_normalize_string_list(data.get("generated_rules")),
        hook_enforcement=_normalize_string_list(data.get("hook_enforcement")),
        policy_enforcement=_normalize_string_list(data.get("policy_enforcement")),
        command_surfaces=_normalize_string_list(data.get("command_surfaces")),
        migration_checklist=_normalize_string_list(data.get("migration_checklist")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


@dataclass(frozen=True)
class ExtensionManifest:
    extension_id: str
    kind: str = ""
    lifecycle_state: str = "registered"
    load_boundary: list[str] = field(default_factory=list)
    last_error: str = ""
    capabilities: list[str] = field(default_factory=list)
    required_permissions: list[str] = field(default_factory=list)
    hook_subscriptions: list[str] = field(default_factory=list)
    supported_profiles: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "extension_id": str(self.extension_id).strip(),
            "kind": str(self.kind).strip(),
            "lifecycle_state": _normalize_extension_manifest_lifecycle(self.lifecycle_state),
            "load_boundary": _normalize_string_list(self.load_boundary),
            "last_error": str(self.last_error).strip(),
            "capabilities": _normalize_string_list(self.capabilities),
            "required_permissions": _normalize_string_list(self.required_permissions),
            "hook_subscriptions": _normalize_string_list(self.hook_subscriptions),
            "supported_profiles": _normalize_string_list(self.supported_profiles),
            "metadata": _normalize_metadata(self.metadata),
        }


def extension_manifest_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return ExtensionManifest(
        extension_id=str(data.get("extension_id", "")).strip(),
        kind=str(data.get("kind", "")).strip(),
        lifecycle_state=str(data.get("lifecycle_state", "registered")).strip(),
        load_boundary=_normalize_string_list(data.get("load_boundary")),
        last_error=str(data.get("last_error", "")).strip(),
        capabilities=_normalize_string_list(data.get("capabilities")),
        required_permissions=_normalize_string_list(data.get("required_permissions")),
        hook_subscriptions=_normalize_string_list(data.get("hook_subscriptions")),
        supported_profiles=_normalize_string_list(data.get("supported_profiles")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def _normalize_workflow_manifest_lifecycle(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"declared", "loaded", "running", "paused", "unloaded", "errored"}:
        return normalized
    return "declared"


@dataclass(frozen=True)
class WorkflowManifest:
    workflow_id: str
    extension_id: str = ""
    kind: str = ""
    entry_command: str = ""
    trigger_modes: list[str] = field(default_factory=list)
    load_policy: str = ""
    unload_policy: str = ""
    lifecycle_state: str = "declared"
    runtime_contracts: list[str] = field(default_factory=list)
    status_surfaces: list[str] = field(default_factory=list)
    failure_semantics: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": str(self.workflow_id).strip(),
            "extension_id": str(self.extension_id).strip(),
            "kind": str(self.kind).strip(),
            "entry_command": str(self.entry_command).strip(),
            "trigger_modes": _normalize_string_list(self.trigger_modes),
            "load_policy": str(self.load_policy).strip(),
            "unload_policy": str(self.unload_policy).strip(),
            "lifecycle_state": _normalize_workflow_manifest_lifecycle(self.lifecycle_state),
            "runtime_contracts": _normalize_string_list(self.runtime_contracts),
            "status_surfaces": _normalize_string_list(self.status_surfaces),
            "failure_semantics": _normalize_string_list(self.failure_semantics),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class InstructionMigrationChecklist:
    retained_in_guides: list[str] = field(default_factory=list)
    migrate_to_hooks: list[str] = field(default_factory=list)
    migrate_to_policy: list[str] = field(default_factory=list)
    migrate_to_commands: list[str] = field(default_factory=list)
    deferred_items: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "retained_in_guides": _normalize_string_list(self.retained_in_guides),
            "migrate_to_hooks": _normalize_string_list(self.migrate_to_hooks),
            "migrate_to_policy": _normalize_string_list(self.migrate_to_policy),
            "migrate_to_commands": _normalize_string_list(self.migrate_to_commands),
            "deferred_items": _normalize_string_list(self.deferred_items),
            "metadata": _normalize_metadata(self.metadata),
        }


def instruction_migration_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return InstructionMigrationChecklist(
        retained_in_guides=_normalize_string_list(data.get("retained_in_guides")),
        migrate_to_hooks=_normalize_string_list(data.get("migrate_to_hooks")),
        migrate_to_policy=_normalize_string_list(data.get("migrate_to_policy")),
        migrate_to_commands=_normalize_string_list(data.get("migrate_to_commands")),
        deferred_items=_normalize_string_list(data.get("deferred_items")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


@dataclass(frozen=True)
class OpenSourceBoundary:
    public_contracts: list[str] = field(default_factory=list)
    private_only: list[str] = field(default_factory=list)
    migration_sequence: list[str] = field(default_factory=list)
    not_recommended: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "public_contracts": _normalize_string_list(self.public_contracts),
            "private_only": _normalize_string_list(self.private_only),
            "migration_sequence": _normalize_string_list(self.migration_sequence),
            "not_recommended": _normalize_string_list(self.not_recommended),
            "metadata": _normalize_metadata(self.metadata),
        }


def open_source_boundary_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return OpenSourceBoundary(
        public_contracts=_normalize_string_list(data.get("public_contracts")),
        private_only=_normalize_string_list(data.get("private_only")),
        migration_sequence=_normalize_string_list(data.get("migration_sequence")),
        not_recommended=_normalize_string_list(data.get("not_recommended")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


@dataclass(frozen=True)
class SharedArtifactRef:
    artifact_id: str
    path: str
    kind: str = ""
    producer_run_id: str = ""
    source_ref: str = ""
    exists: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": str(self.artifact_id).strip(),
            "path": str(self.path).strip(),
            "kind": str(self.kind).strip(),
            "producer_run_id": str(self.producer_run_id).strip(),
            "source_ref": str(self.source_ref).strip(),
            "exists": bool(self.exists),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class ChildRunOverlay:
    run_id: str
    state: str = ""
    stage: str = ""
    focus: str = ""
    started_at: str = ""
    updated_at: str = ""
    artifacts: list[SharedArtifactRef | dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id).strip(),
            "state": str(self.state).strip(),
            "stage": str(self.stage).strip(),
            "focus": str(self.focus).strip(),
            "started_at": str(self.started_at).strip(),
            "updated_at": str(self.updated_at).strip(),
            "artifacts": [
                item.to_dict() if isinstance(item, SharedArtifactRef) else dict(item)
                for item in self.artifacts
            ],
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class RunTreeNode:
    run_id: str
    task_id: str
    state: str = ""
    stage: str = ""
    focus: str = ""
    parent_run_id: str = ""
    children: list[ChildRunOverlay | dict[str, Any]] = field(default_factory=list)
    shared_artifacts: list[SharedArtifactRef | dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": str(self.run_id).strip(),
            "task_id": str(self.task_id).strip(),
            "state": str(self.state).strip(),
            "stage": str(self.stage).strip(),
            "focus": str(self.focus).strip(),
            "parent_run_id": str(self.parent_run_id).strip(),
            "children": [
                item.to_dict() if isinstance(item, ChildRunOverlay) else dict(item)
                for item in self.children
            ],
            "shared_artifacts": [
                item.to_dict() if isinstance(item, SharedArtifactRef) else dict(item)
                for item in self.shared_artifacts
            ],
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class ProjectRuntimeSnapshot:
    project_name: str
    task_id: str
    board_path: str = ""
    source_path: str = ""
    task_status: str = ""
    next_action: str = ""
    updated_at: str = ""
    writeback_targets: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_name": str(self.project_name).strip(),
            "task_id": str(self.task_id).strip(),
            "board_path": str(self.board_path).strip(),
            "source_path": str(self.source_path).strip(),
            "task_status": str(self.task_status).strip(),
            "next_action": str(self.next_action).strip(),
            "updated_at": str(self.updated_at).strip(),
            "writeback_targets": _normalize_string_list(self.writeback_targets),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class BridgeRuntimeSnapshot:
    bridge: str = "feishu"
    status: str = ""
    transport: str = ""
    last_event_at: str = ""
    last_error: str = ""
    inbound_message_id: str = ""
    inbound_cursor_at: str = ""
    outbound_message_id: str = ""
    outbound_cursor_at: str = ""
    continuity_issue_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bridge": str(self.bridge).strip(),
            "status": str(self.status).strip(),
            "transport": str(self.transport).strip(),
            "last_event_at": str(self.last_event_at).strip(),
            "last_error": str(self.last_error).strip(),
            "inbound_message_id": str(self.inbound_message_id).strip(),
            "inbound_cursor_at": str(self.inbound_cursor_at).strip(),
            "outbound_message_id": str(self.outbound_message_id).strip(),
            "outbound_cursor_at": str(self.outbound_cursor_at).strip(),
            "continuity_issue_count": max(0, int(self.continuity_issue_count or 0)),
            "metadata": _normalize_metadata(self.metadata),
        }


@dataclass(frozen=True)
class WritebackDeliveryContract:
    aggregate_status: str = ""
    writeback_targets: list[str] = field(default_factory=list)
    delivery_ids: list[str] = field(default_factory=list)
    delivered_targets: list[str] = field(default_factory=list)
    pending_targets: list[str] = field(default_factory=list)
    failed_targets: list[str] = field(default_factory=list)
    queue_status: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_status": str(self.aggregate_status).strip(),
            "writeback_targets": _normalize_string_list(self.writeback_targets),
            "delivery_ids": _normalize_string_list(self.delivery_ids),
            "delivered_targets": _normalize_string_list(self.delivered_targets),
            "pending_targets": _normalize_string_list(self.pending_targets),
            "failed_targets": _normalize_string_list(self.failed_targets),
            "queue_status": _normalize_metadata(self.queue_status),
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


def _payload_dict(payload: Any) -> dict[str, Any]:
    return dict(payload) if isinstance(payload, dict) else {}


def shared_artifact_ref_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return SharedArtifactRef(
        artifact_id=str(data.get("artifact_id", "")).strip(),
        path=str(data.get("path", "")).strip(),
        kind=str(data.get("kind", "")).strip(),
        producer_run_id=str(data.get("producer_run_id", "")).strip(),
        source_ref=str(data.get("source_ref", "")).strip(),
        exists=bool(data.get("exists", False)),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def child_run_overlay_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return ChildRunOverlay(
        run_id=str(data.get("run_id", "")).strip(),
        state=str(data.get("state", "")).strip(),
        stage=str(data.get("stage", "")).strip(),
        focus=str(data.get("focus", "")).strip(),
        started_at=str(data.get("started_at", "")).strip(),
        updated_at=str(data.get("updated_at", "")).strip(),
        artifacts=[shared_artifact_ref_payload(item) for item in data.get("artifacts", []) if isinstance(item, dict)],
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def run_tree_node_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return RunTreeNode(
        run_id=str(data.get("run_id", "")).strip(),
        task_id=str(data.get("task_id", "")).strip(),
        state=str(data.get("state", "")).strip(),
        stage=str(data.get("stage", "")).strip(),
        focus=str(data.get("focus", "")).strip(),
        parent_run_id=str(data.get("parent_run_id", "")).strip(),
        children=[child_run_overlay_payload(item) for item in data.get("children", []) if isinstance(item, dict)],
        shared_artifacts=[
            shared_artifact_ref_payload(item) for item in data.get("shared_artifacts", []) if isinstance(item, dict)
        ],
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def project_runtime_snapshot_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return ProjectRuntimeSnapshot(
        project_name=str(data.get("project_name", "")).strip(),
        task_id=str(data.get("task_id", "")).strip(),
        board_path=str(data.get("board_path", "")).strip(),
        source_path=str(data.get("source_path", "")).strip(),
        task_status=str(data.get("task_status", "")).strip(),
        next_action=str(data.get("next_action", "")).strip(),
        updated_at=str(data.get("updated_at", "")).strip(),
        writeback_targets=_normalize_string_list(data.get("writeback_targets")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def bridge_runtime_snapshot_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return BridgeRuntimeSnapshot(
        bridge=str(data.get("bridge", "feishu")).strip() or "feishu",
        status=str(data.get("status", "")).strip(),
        transport=str(data.get("transport", "")).strip(),
        last_event_at=str(data.get("last_event_at", "")).strip(),
        last_error=str(data.get("last_error", "")).strip(),
        inbound_message_id=str(data.get("inbound_message_id", "")).strip(),
        inbound_cursor_at=str(data.get("inbound_cursor_at", "")).strip(),
        outbound_message_id=str(data.get("outbound_message_id", "")).strip(),
        outbound_cursor_at=str(data.get("outbound_cursor_at", "")).strip(),
        continuity_issue_count=int(data.get("continuity_issue_count", 0) or 0),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def writeback_delivery_contract_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return WritebackDeliveryContract(
        aggregate_status=str(data.get("aggregate_status", "")).strip(),
        writeback_targets=_normalize_string_list(data.get("writeback_targets")),
        delivery_ids=_normalize_string_list(data.get("delivery_ids")),
        delivered_targets=_normalize_string_list(data.get("delivered_targets")),
        pending_targets=_normalize_string_list(data.get("pending_targets")),
        failed_targets=_normalize_string_list(data.get("failed_targets")),
        queue_status=_payload_dict(data.get("queue_status")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def workflow_manifest_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    return WorkflowManifest(
        workflow_id=str(data.get("workflow_id", "")).strip(),
        extension_id=str(data.get("extension_id", "")).strip(),
        kind=str(data.get("kind", "")).strip(),
        entry_command=str(data.get("entry_command", "")).strip(),
        trigger_modes=_normalize_string_list(data.get("trigger_modes")),
        load_policy=str(data.get("load_policy", "")).strip(),
        unload_policy=str(data.get("unload_policy", "")).strip(),
        lifecycle_state=str(data.get("lifecycle_state", "declared")).strip(),
        runtime_contracts=_normalize_string_list(data.get("runtime_contracts")),
        status_surfaces=_normalize_string_list(data.get("status_surfaces")),
        failure_semantics=_normalize_string_list(data.get("failure_semantics")),
        metadata=_payload_dict(data.get("metadata")),
    ).to_dict()


def runtime_contract_view(payload: dict[str, Any] | None) -> dict[str, Any]:
    data = _payload_dict(payload)
    harness_snapshot_payload = _payload_dict(data.get("harness_snapshot"))
    runtime_overlay_source = _payload_dict(data.get("runtime_overlay")) or _payload_dict(harness_snapshot_payload.get("runtime_overlay"))
    runtime_overlay = RuntimeOverlay(
        project_name=str(runtime_overlay_source.get("project_name", "")).strip(),
        task_id=str(runtime_overlay_source.get("task_id", "")).strip(),
        run_id=str(runtime_overlay_source.get("run_id", "")).strip(),
        source=str(runtime_overlay_source.get("source", "")).strip(),
        scope=str(runtime_overlay_source.get("scope", "")).strip(),
        current_stage=str(runtime_overlay_source.get("current_stage", "")).strip(),
        current_focus=str(runtime_overlay_source.get("current_focus", "")).strip(),
        board_path=str(runtime_overlay_source.get("board_path", "")).strip(),
        task_pointer=str(runtime_overlay_source.get("task_pointer", "")).strip(),
        deliverable=str(runtime_overlay_source.get("deliverable", "")).strip(),
        artifacts_root=str(runtime_overlay_source.get("artifacts_root", "")).strip(),
        metadata=_payload_dict(runtime_overlay_source.get("metadata")),
    ).to_dict()
    task_runtime_source = (
        _payload_dict(data.get("task_runtime_snapshot"))
        or _payload_dict(data.get("task_runtime"))
        or _payload_dict(harness_snapshot_payload.get("task_runtime"))
        or _payload_dict(harness_snapshot_payload.get("task_runtime_snapshot"))
    )
    task_runtime_snapshot = TaskRuntimeSnapshot(
        harness_state=str(task_runtime_source.get("harness_state", "")).strip(),
        current_stage=str(task_runtime_source.get("current_stage", "")).strip(),
        last_decision=str(task_runtime_source.get("last_decision", "")).strip(),
        current_focus=str(task_runtime_source.get("current_focus", "")).strip(),
        next_action=str(task_runtime_source.get("next_action", "")).strip(),
        next_wake_at=str(task_runtime_source.get("next_wake_at", "")).strip(),
        blocked_reason=str(task_runtime_source.get("blocked_reason", "")).strip(),
        last_run_id=str(task_runtime_source.get("last_run_id", "")).strip(),
        active_run_id=str(task_runtime_source.get("active_run_id", "")).strip(),
        running_started_at=str(task_runtime_source.get("running_started_at", "")).strip(),
        completed_subgoal_count=int(task_runtime_source.get("completed_subgoal_count", 0) or 0),
        pending_subgoal_count=int(task_runtime_source.get("pending_subgoal_count", 0) or 0),
        running_stale=bool(task_runtime_source.get("running_stale", False)),
        metadata=_payload_dict(task_runtime_source.get("metadata")),
    ).to_dict()
    handoff_source = _payload_dict(data.get("handoff_packet")) or _payload_dict(harness_snapshot_payload.get("handoff_packet"))
    handoff_packet = HandoffPacket(
        task_spec_path=str(handoff_source.get("task_spec_path", "")).strip(),
        acceptance_path=str(handoff_source.get("acceptance_path", "")).strip(),
        progress_path=str(handoff_source.get("progress_path", "")).strip(),
        latest_smoke_path=str(handoff_source.get("latest_smoke_path", "")).strip(),
        latest_report_path=str(handoff_source.get("latest_report_path", "")).strip(),
        latest_ops_report_path=str(handoff_source.get("latest_ops_report_path", "")).strip(),
        metadata=_payload_dict(handoff_source.get("metadata")),
    ).to_dict()
    compression_policy = _payload_dict(data.get("compression_policy")) or _payload_dict(harness_snapshot_payload.get("compression_policy"))
    middleware = _payload_dict(data.get("middleware")) or _payload_dict(harness_snapshot_payload.get("middleware"))
    project_runtime = project_runtime_snapshot_payload(
        _payload_dict(data.get("project_runtime")) or _payload_dict(harness_snapshot_payload.get("project_runtime"))
    )
    bridge_runtime = bridge_runtime_snapshot_payload(
        _payload_dict(data.get("bridge_runtime")) or _payload_dict(harness_snapshot_payload.get("bridge_runtime"))
    )
    run_tree = run_tree_node_payload(_payload_dict(data.get("run_tree")) or _payload_dict(harness_snapshot_payload.get("run_tree")))
    delivery_contract = writeback_delivery_contract_payload(
        _payload_dict(data.get("delivery_contract")) or _payload_dict(harness_snapshot_payload.get("delivery_contract"))
    )
    execution_boundary = execution_boundary_payload(
        _payload_dict(data.get("execution_boundary")) or _payload_dict(harness_snapshot_payload.get("execution_boundary"))
    )
    instruction_surface = instruction_surface_payload(
        _payload_dict(data.get("instruction_surface")) or _payload_dict(harness_snapshot_payload.get("instruction_surface"))
    )
    extension_manifest = extension_manifest_payload(
        _payload_dict(data.get("extension_manifest")) or _payload_dict(harness_snapshot_payload.get("extension_manifest"))
    )
    workflow_manifest = workflow_manifest_payload(
        _payload_dict(data.get("workflow_manifest")) or _payload_dict(harness_snapshot_payload.get("workflow_manifest"))
    )
    instruction_migration = instruction_migration_payload(
        _payload_dict(data.get("instruction_migration")) or _payload_dict(harness_snapshot_payload.get("instruction_migration"))
    )
    open_source_boundary = open_source_boundary_payload(
        _payload_dict(data.get("open_source_boundary")) or _payload_dict(harness_snapshot_payload.get("open_source_boundary"))
    )
    harness_snapshot = HarnessSnapshot(
        runtime_overlay=runtime_overlay,
        task_runtime=task_runtime_snapshot,
        handoff_packet=handoff_packet,
        compression_policy=compression_policy,
        middleware=middleware,
        project_runtime=project_runtime,
        bridge_runtime=bridge_runtime,
        run_tree=run_tree,
        delivery_contract=delivery_contract,
        execution_boundary=execution_boundary,
        instruction_surface=instruction_surface,
        extension_manifest=extension_manifest,
        workflow_manifest=workflow_manifest,
        instruction_migration=instruction_migration,
        open_source_boundary=open_source_boundary,
        updated_at=str(data.get("updated_at") or harness_snapshot_payload.get("updated_at") or "").strip(),
        metadata=_payload_dict(harness_snapshot_payload.get("metadata")) or _payload_dict(data.get("metadata")),
    ).to_dict()
    return {
        "runtime_overlay": runtime_overlay,
        "compression_policy": compression_policy,
        "middleware": middleware,
        "task_runtime_snapshot": task_runtime_snapshot,
        "handoff_packet": handoff_packet,
        "project_runtime": project_runtime,
        "bridge_runtime": bridge_runtime,
        "run_tree": run_tree,
        "delivery_contract": delivery_contract,
        "execution_boundary": execution_boundary,
        "instruction_surface": instruction_surface,
        "extension_manifest": extension_manifest,
        "workflow_manifest": workflow_manifest,
        "instruction_migration": instruction_migration,
        "open_source_boundary": open_source_boundary,
        "harness_snapshot": harness_snapshot,
    }


def runtime_contract_summary_lines(
    payload: dict[str, Any] | None,
    *,
    include_project_board_path: bool,
    include_project_updated_at: bool,
    include_handoff_packet: bool,
    include_local_context_roots: bool,
    include_bridge_name: bool = True,
    snapshot_mode: str = "decision_pending",
) -> list[str]:
    view = runtime_contract_view(payload)
    runtime_overlay = view["runtime_overlay"]
    runtime_snapshot = view["task_runtime_snapshot"]
    compression_policy = view["compression_policy"]
    middleware = view["middleware"]
    handoff_packet = view["handoff_packet"]
    project_runtime = view["project_runtime"]
    bridge_runtime = view["bridge_runtime"]
    run_tree = view["run_tree"]
    delivery_contract = view["delivery_contract"]
    execution_boundary = view["execution_boundary"]
    instruction_surface = view["instruction_surface"]
    extension_manifest = view["extension_manifest"]
    workflow_manifest = view["workflow_manifest"]
    instruction_migration = view["instruction_migration"]
    open_source_boundary = view["open_source_boundary"]
    lines: list[str] = []
    if runtime_overlay and any(runtime_overlay.values()):
        suffix = f" | run_id=`{runtime_overlay.get('run_id', '')}`"
        if include_project_board_path:
            suffix = f" | board=`{runtime_overlay.get('board_path', '')}`"
        lines.append(
            f"overlay: stage=`{runtime_overlay.get('current_stage', '')}` | focus=`{runtime_overlay.get('current_focus', '')}`{suffix}"
        )
    if runtime_snapshot and any(runtime_snapshot.values()):
        if snapshot_mode == "completed_pending_active":
            snapshot_summary = (
                f"snapshot: completed=`{runtime_snapshot.get('completed_subgoal_count', 0)}` | pending=`{runtime_snapshot.get('pending_subgoal_count', 0)}` | active_run_id=`{runtime_snapshot.get('active_run_id', '') or 'n/a'}`"
            )
        else:
            snapshot_summary = (
                f"snapshot: harness_state=`{runtime_snapshot.get('harness_state', '')}` | decision=`{runtime_snapshot.get('last_decision', '')}`"
            )
        if snapshot_mode == "completed_pending":
            snapshot_summary += (
                f" | completed=`{runtime_snapshot.get('completed_subgoal_count', 0)}` | pending=`{runtime_snapshot.get('pending_subgoal_count', 0)}`"
            )
        elif snapshot_mode == "decision_pending":
            snapshot_summary += f" | pending=`{runtime_snapshot.get('pending_subgoal_count', 0)}`"
        lines.append(snapshot_summary)
    if compression_policy:
        lines.append(
            f"compression: L1=`{compression_policy.get('l1_strategy', '')}` | L2=`{compression_policy.get('l2_strategy', '')}` | L3=`{compression_policy.get('l3_strategy', '')}`"
        )
    if middleware:
        checklist = _payload_dict(middleware.get("precompletion_checklist"))
        loop_detection = _payload_dict(middleware.get("loop_detection"))
        local_context = _payload_dict(middleware.get("local_context"))
        local_context_text = "local_context=`armed`"
        if include_local_context_roots:
            local_context_text = f"local_roots=`{len(local_context.get('allow_paths', []) or [])}`"
        lines.append(
            f"middleware: checklist=`{checklist.get('status', '') or 'n/a'}` | loop=`{loop_detection.get('status', '') or 'n/a'}` | {local_context_text}"
        )
    if include_handoff_packet and handoff_packet:
        lines.append(
            f"handoff_packet: task_spec=`{handoff_packet.get('task_spec_path', '')}` | progress=`{handoff_packet.get('progress_path', '')}` | smoke=`{handoff_packet.get('latest_smoke_path', '')}`"
        )
    if project_runtime and any(project_runtime.values()):
        project_summary = (
            f"project_runtime: status=`{project_runtime.get('task_status', '')}` | next_action=`{project_runtime.get('next_action', '')}`"
        )
        if include_project_board_path:
            project_summary += f" | board=`{project_runtime.get('board_path', '')}`"
        elif include_project_updated_at:
            project_summary += f" | updated_at=`{project_runtime.get('updated_at', '')}`"
        lines.append(project_summary)
    if bridge_runtime and any(bridge_runtime.values()):
        bridge_summary = "bridge_runtime: "
        if include_bridge_name:
            bridge_summary += f"bridge=`{bridge_runtime.get('bridge', '')}` | "
        bridge_summary += (
            f"status=`{bridge_runtime.get('status', '')}` | transport=`{bridge_runtime.get('transport', '')}` | continuity_issues=`{bridge_runtime.get('continuity_issue_count', 0)}`"
        )
        lines.append(bridge_summary)
    if run_tree and any(run_tree.values()):
        lines.append(
            f"run_tree: root=`{run_tree.get('run_id', '')}` | children=`{len(run_tree.get('children', []) or [])}` | artifacts=`{len(run_tree.get('shared_artifacts', []) or [])}`"
        )
    if delivery_contract and any(delivery_contract.values()):
        lines.append(
            f"delivery_contract: aggregate=`{delivery_contract.get('aggregate_status', '')}` | writebacks=`{len(delivery_contract.get('writeback_targets', []) or [])}` | pending=`{len(delivery_contract.get('pending_targets', []) or [])}` | failed=`{len(delivery_contract.get('failed_targets', []) or [])}`"
        )
    if execution_boundary:
        lines.append(
            f"execution_boundary: sandbox=`{execution_boundary.get('sandbox_mode', '')}` | network=`{execution_boundary.get('network_access', '')}` | writable_roots=`{len(execution_boundary.get('writable_roots', []) or [])}`"
        )
    if instruction_surface:
        lines.append(
            f"instruction_surface: guides=`{len(instruction_surface.get('human_guides', []) or [])}` | generated_rules=`{len(instruction_surface.get('generated_rules', []) or [])}` | hooks=`{len(instruction_surface.get('hook_enforcement', []) or [])}` | policies=`{len(instruction_surface.get('policy_enforcement', []) or [])}`"
        )
    if extension_manifest:
        lines.append(
            f"extension_manifest: kind=`{extension_manifest.get('kind', '')}` | lifecycle=`{extension_manifest.get('lifecycle_state', '')}` | capabilities=`{len(extension_manifest.get('capabilities', []) or [])}` | hooks=`{len(extension_manifest.get('hook_subscriptions', []) or [])}` | profiles=`{len(extension_manifest.get('supported_profiles', []) or [])}`"
        )
    if workflow_manifest:
        lines.append(
            f"workflow_manifest: entry=`{workflow_manifest.get('entry_command', '')}` | lifecycle=`{workflow_manifest.get('lifecycle_state', '')}` | triggers=`{len(workflow_manifest.get('trigger_modes', []) or [])}` | surfaces=`{len(workflow_manifest.get('status_surfaces', []) or [])}`"
        )
    if instruction_migration:
        lines.append(
            f"instruction_migration: retain=`{len(instruction_migration.get('retained_in_guides', []) or [])}` | hooks=`{len(instruction_migration.get('migrate_to_hooks', []) or [])}` | policy=`{len(instruction_migration.get('migrate_to_policy', []) or [])}` | commands=`{len(instruction_migration.get('migrate_to_commands', []) or [])}`"
        )
    if open_source_boundary:
        lines.append(
            f"open_source_boundary: public=`{len(open_source_boundary.get('public_contracts', []) or [])}` | private_only=`{len(open_source_boundary.get('private_only', []) or [])}` | sequence=`{len(open_source_boundary.get('migration_sequence', []) or [])}` | no_go=`{len(open_source_boundary.get('not_recommended', []) or [])}`"
        )
    return lines


def _prefer_latest_timestamp_text(current: str, candidate: str) -> str:
    current_dt = _parse_timestamp(str(current or "").strip())
    candidate_dt = _parse_timestamp(str(candidate or "").strip())
    if current_dt is None:
        return str(candidate or "").strip()
    if candidate_dt is None:
        return str(current or "").strip()
    return str(candidate if candidate_dt >= current_dt else current).strip()


def bridge_status_surface(
    payload: dict[str, Any] | None,
    *,
    settings_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = bridge_runtime_snapshot_payload(payload)
    metadata = _payload_dict(snapshot.get("metadata"))
    heartbeat_at = str(metadata.get("heartbeat_at") or "").strip()
    stale_after_seconds = max(0, int(metadata.get("stale_after_seconds") or 90))
    event_idle_after_seconds = max(0, int(metadata.get("event_idle_after_seconds") or 0))
    heartbeat_ts = timestamp_millis(heartbeat_at or str(metadata.get("updated_at") or "").strip())
    event_ts = max(
        timestamp_millis(str(snapshot.get("last_event_at") or "").strip()),
        timestamp_millis(str(metadata.get("connected_at") or metadata.get("updated_at") or "").strip()),
    )
    now_ts = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
    stale = False
    event_stalled = False
    if str(snapshot.get("status", "")).strip() == "connected":
        if heartbeat_ts and stale_after_seconds > 0:
            stale = (now_ts - heartbeat_ts) / 1000 > stale_after_seconds
        if event_ts and event_idle_after_seconds > 0:
            event_stalled = (now_ts - event_ts) / 1000 > event_idle_after_seconds
        stale = stale or event_stalled
    effective_status = "stale" if stale and str(snapshot.get("status", "")).strip() == "connected" else str(snapshot.get("status", "")).strip()
    return {
        "bridge": snapshot.get("bridge", "feishu"),
        "connection_status": effective_status,
        "host_mode": str(metadata.get("host_mode") or "").strip(),
        "transport": snapshot.get("transport", ""),
        "last_error": snapshot.get("last_error", ""),
        "last_event_at": snapshot.get("last_event_at", ""),
        "updated_at": str(metadata.get("updated_at") or "").strip(),
        "heartbeat_at": heartbeat_at,
        "stale": stale,
        "event_stalled": event_stalled,
        "stale_after_seconds": stale_after_seconds,
        "event_idle_after_seconds": event_idle_after_seconds,
        "backfill_degraded": bool(metadata.get("backfill_degraded")),
        "backfill_degraded_count": int(metadata.get("backfill_degraded_count") or 0),
        "last_backfill_error": str(metadata.get("last_backfill_error") or "").strip(),
        "last_backfill_error_at": str(metadata.get("last_backfill_error_at") or "").strip(),
        "continuity_issue_count": int(snapshot.get("continuity_issue_count", 0) or 0),
        "metadata": metadata,
        "settings_summary": _payload_dict(settings_summary),
        "runtime_snapshot": snapshot,
    }


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

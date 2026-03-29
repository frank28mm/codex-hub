#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import codex_memory, workspace_job_schema


PROJECTED_JOB_VERSION = 1
RUNNABLE_STATUSES = {"todo", "doing"}
DEFAULT_STAGE_PLAN = ["discover", "frame", "execute", "verify", "adapt", "handoff"]
STATUS_PRIORITY = {"doing": 0, "todo": 1}

# Public Codex Hub ships the shared projection/runtime contract without bundling
# the author's private task templates. Downstream users can inject project
# background-job specs through project boards or control overlays.
TASK_JOB_SPECS: dict[str, dict[str, Any]] = {}


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(REPO_ROOT)))


def control_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", str(REPO_ROOT / "control")))


@lru_cache(maxsize=1)
def load_growth_control() -> dict[str, Any]:
    path = control_root() / "codex_growth_system.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _growth_task_specs(project_name: str) -> dict[str, dict[str, Any]]:
    config = load_growth_control()
    if not config:
        return {}
    configured_project = codex_memory.canonical_project_name(str(config.get("project_name", "")).strip())
    if configured_project and configured_project != codex_memory.canonical_project_name(project_name):
        return {}
    workflow_specs = config.get("workflow_specs", {})
    object_specs = config.get("objects", {})
    if not isinstance(workflow_specs, dict) or not isinstance(object_specs, dict):
        return {}
    table_map = {
        str(name): str(item.get("table_path", "")).strip()
        for name, item in object_specs.items()
        if isinstance(item, dict) and str(item.get("table_path", "")).strip()
    }
    task_specs = config.get("task_specs", {})
    if not isinstance(task_specs, dict):
        return {}
    projected: dict[str, dict[str, Any]] = {}
    for task_id, item in task_specs.items():
        if not isinstance(item, dict):
            continue
        workflow_id = str(item.get("workflow_id", "")).strip()
        workflow = workflow_specs.get(workflow_id, {})
        if not workflow_id or not isinstance(workflow, dict):
            continue
        input_objects = [str(name).strip() for name in workflow.get("input_objects", []) if str(name).strip()]
        projected[str(task_id).strip()] = {
            "job_slug": str(item.get("job_slug", "")).strip() or str(task_id).strip().lower(),
            "executor_kind": str(workflow.get("executor_kind", "")).strip() or "growth_signal_scan",
            "automation_mode": str(item.get("automation_mode", "")).strip() or "background_assist",
            "allowed_actions": [str(value).strip() for value in workflow.get("allowed_actions", []) if str(value).strip()],
            "delivery_targets": [str(value).strip() for value in workflow.get("delivery_targets", []) if str(value).strip()],
            "gate_policy": str(workflow.get("gate_policy", "")).strip() or "none",
            "max_rounds": int(workflow.get("max_rounds", 3) or 3),
            "time_budget_minutes": int(workflow.get("time_budget_minutes", 20) or 20),
            "acceptance_criteria": [
                str(value).strip() for value in workflow.get("success_criteria", []) if str(value).strip()
            ],
            "analysis_focus": [str(value).strip() for value in item.get("analysis_focus", []) if str(value).strip()],
            "workflow_id": workflow_id,
            "summary_focus": str(item.get("summary_focus", "")).strip(),
            "input_objects": input_objects,
            "object_tables": {name: table_map.get(name, "") for name in input_objects},
            "system_name": str(config.get("system_name", "")).strip(),
            "primary_product": str(config.get("primary_product", "")).strip(),
            "primary_platforms": [str(value).strip() for value in config.get("primary_platforms", []) if str(value).strip()],
            "supporting_platforms": [
                str(value).strip() for value in config.get("supporting_platforms", []) if str(value).strip()
            ],
            "platform_policies": config.get("platform_policies", {}),
            "risk_controls": config.get("risk_controls", {}),
            "delivery_contract": config.get("delivery_targets", {}),
        }
    return projected


def load_task_job_specs(project_name: str) -> dict[str, dict[str, Any]]:
    merged = dict(TASK_JOB_SPECS)
    merged.update(_growth_task_specs(project_name))
    return merged


def _task_refs(project_name: str) -> list[dict[str, Any]]:
    project_name = codex_memory.canonical_project_name(project_name)
    refs: list[dict[str, Any]] = []
    project_board = codex_memory.load_project_board(project_name)
    for index, row in enumerate(project_board["project_rows"]):
        refs.append(
            {
                "project_name": project_name,
                "task_id": row.get("ID", ""),
                "source_type": "project",
                "source_path": project_board["path"],
                "row_group": "project_rows",
                "rows": project_board["project_rows"],
                "index": index,
                "row": row,
                "project_board_path": project_board["path"],
            }
        )
    for index, row in enumerate(project_board["rollup_rows"]):
        source = str(row.get("来源", "")).strip()
        topic_path = project_board["path"]
        if source.startswith("topic:"):
            topic_name = source.split(":", 1)[1]
            for candidate in codex_memory.topic_board_paths(project_name):
                if f"-{topic_name}-" in candidate.name:
                    topic_path = candidate
                    break
        refs.append(
            {
                "project_name": project_name,
                "task_id": row.get("ID", ""),
                "source_type": "topic" if source.startswith("topic:") else "project",
                "source_path": topic_path,
                "row_group": "rollup_rows",
                "rows": project_board["rollup_rows"],
                "index": index,
                "row": row,
                "project_board_path": project_board["path"],
            }
        )
    return refs


def find_task_ref(project_name: str, task_id: str) -> dict[str, Any]:
    canonical_project = codex_memory.canonical_project_name(project_name)
    for item in _task_refs(canonical_project):
        if item["task_id"] == task_id:
            return item
    raise KeyError(f"unknown task id `{task_id}` for project `{canonical_project}`")


def _task_pointer_path(project_name: str, ref: dict[str, Any]) -> str:
    pointer = str(ref["row"].get("指向", "")).strip()
    if not pointer:
        return ""
    if pointer.startswith("/") or pointer.startswith("gflow:"):
        return pointer
    if pointer.endswith(".md"):
        candidate = codex_memory.WORKING_ROOT / pointer
        if candidate.exists():
            return str(candidate)
        project_candidate = codex_memory.PROJECT_SUMMARY_ROOT / pointer
        if project_candidate.exists():
            return str(project_candidate)
    return pointer


def _identity_segment(value: str) -> str:
    normalized = re.sub(r"[^\w.-]+", "-", value.strip().lower()).strip("-._")
    return normalized or "item"


def _legacy_job_id(project_name: str, job_slug: str) -> str:
    return f"board-job.{project_name.lower().replace(' ', '-')}.{job_slug}"


def _legacy_artifacts_root(job_slug: str) -> Path:
    return workspace_root() / "reports" / "ops" / "background-jobs" / job_slug


def _persisted_program_state(artifacts_roots: list[Path]) -> dict[str, Any]:
    for root in artifacts_roots:
        payload = workspace_job_schema.read_json_file(
            workspace_job_schema.handoff_bundle_paths(root)["task_spec_path"]
        )
        if payload:
            return payload
    return {}


def _projected_task_status(row: dict[str, Any], task_spec: dict[str, Any]) -> str:
    task_status = codex_memory.normalize_task_status(row.get("状态", "todo"))
    last_decision = str(task_spec.get("last_decision", "")).strip()
    stage = str(task_spec.get("stage", "")).strip()
    if last_decision == "done" or stage == "handoff":
        return "done"
    if last_decision == "blocked":
        return "blocked"
    return task_status


def project_background_job(project_name: str, task_id: str) -> dict[str, Any]:
    project_name = codex_memory.canonical_project_name(project_name)
    ref = find_task_ref(project_name, task_id)
    canonical_project = codex_memory.canonical_project_name(str(ref.get("project_name", "")).strip() or project_name)
    row = ref["row"]
    spec = load_task_job_specs(canonical_project).get(task_id)
    if spec is None:
        raise KeyError(f"task `{task_id}` is not configured for background execution")
    project_segment = _identity_segment(canonical_project)
    task_segment = _identity_segment(task_id)
    slug_segment = _identity_segment(str(spec["job_slug"]))
    job_id = f"board-job.{project_segment}.{task_segment}.{slug_segment}"
    legacy_job_id = _legacy_job_id(canonical_project, str(spec["job_slug"]))
    scope_type = str(spec.get("scope_type", "project")).strip() or "project"
    scope_ref = str(spec.get("scope_ref", "")).strip() or canonical_project
    approval_required = bool(spec.get("approval_required", False)) or scope_type == "workspace"
    approval_state = str(spec.get("approval_state", "pending" if approval_required else "not-required")).strip()
    program_id = f"program.{project_segment}.{task_segment}.{slug_segment}"
    artifacts_root = workspace_root() / "reports" / "ops" / "background-jobs" / project_segment / f"{task_segment}-{slug_segment}"
    legacy_artifacts_root = _legacy_artifacts_root(str(spec["job_slug"]))
    task_spec = _persisted_program_state([artifacts_root, legacy_artifacts_root])
    task_status = _projected_task_status(row, task_spec)
    if task_status not in RUNNABLE_STATUSES:
        raise ValueError(f"task `{task_id}` is not runnable from status `{task_status}`")
    program_spec = workspace_job_schema.ProgramSpec(
        program_id=program_id,
        workspace_scope=canonical_project,
        objective=str(row.get("事项", "")).strip(),
        priority="high" if task_status == "doing" else "medium",
        scope_type=scope_type,
        scope_ref=scope_ref,
        approval_required=approval_required,
        approval_state=approval_state,
        stage=str(spec.get("initial_stage", "discover")).strip() or "discover",
        stage_plan=list(spec.get("stage_plan", DEFAULT_STAGE_PLAN)),
        wake_policy=dict(spec.get("wake_policy", {})) if isinstance(spec.get("wake_policy"), dict) else {
            "mode": "scheduled_or_event",
            "scheduled": True,
            "project_writeback": True,
            "manual_wake": True,
            "wake_catchup": True,
        },
        loop_policy=dict(spec.get("loop_policy", {})) if isinstance(spec.get("loop_policy"), dict) else {
            "single_focus": True,
            "max_rounds": int(spec["max_rounds"]),
        },
        delivery_policy={
            "gate_policy": spec["gate_policy"],
            "targets": list(spec["delivery_targets"]),
        },
        metadata={
            "task_id": task_id,
            "executor_kind": spec["executor_kind"],
            "source_type": ref["source_type"],
        },
    ).to_dict()
    handoff_bundle = workspace_job_schema.handoff_bundle_paths(artifacts_root)
    return {
        "version": PROJECTED_JOB_VERSION,
        "job_id": job_id,
        "program_id": program_id,
        "job_slug": spec["job_slug"],
        "project_name": canonical_project,
        "task_id": task_id,
        "task_item": row.get("事项", ""),
        "task_status": task_status,
        "scope": row.get("范围", ""),
        "source": row.get("来源", ""),
        "source_type": ref["source_type"],
        "source_path": str(ref["source_path"]),
        "project_board_path": str(ref["project_board_path"]),
        "task_pointer": _task_pointer_path(canonical_project, ref),
        "deliverable": row.get("交付物", ""),
        "next_action": row.get("下一步", ""),
        "updated_at": row.get("更新时间", ""),
        "executor_kind": spec["executor_kind"],
        "automation_mode": spec["automation_mode"],
        "allowed_actions": list(spec["allowed_actions"]),
        "delivery_targets": list(spec["delivery_targets"]),
        "gate_policy": spec["gate_policy"],
        "max_rounds": int(spec["max_rounds"]),
        "time_budget_minutes": int(spec["time_budget_minutes"]),
        "acceptance_criteria": list(spec["acceptance_criteria"]),
        "research_sources": [dict(item) for item in spec.get("research_sources", [])],
        "analysis_focus": list(spec.get("analysis_focus", [])),
        "workflow_id": str(spec.get("workflow_id", "")).strip(),
        "summary_focus": str(spec.get("summary_focus", "")).strip(),
        "input_objects": [str(item).strip() for item in spec.get("input_objects", []) if str(item).strip()],
        "object_tables": dict(spec.get("object_tables", {})) if isinstance(spec.get("object_tables"), dict) else {},
        "system_name": str(spec.get("system_name", "")).strip(),
        "primary_product": str(spec.get("primary_product", "")).strip(),
        "primary_platforms": [str(item).strip() for item in spec.get("primary_platforms", []) if str(item).strip()],
        "supporting_platforms": [
            str(item).strip() for item in spec.get("supporting_platforms", []) if str(item).strip()
        ],
        "platform_policies": dict(spec.get("platform_policies", {}))
        if isinstance(spec.get("platform_policies"), dict)
        else {},
        "risk_controls": dict(spec.get("risk_controls", {})) if isinstance(spec.get("risk_controls"), dict) else {},
        "delivery_contract": dict(spec.get("delivery_contract", {}))
        if isinstance(spec.get("delivery_contract"), dict)
        else {},
        "artifacts_root": str(artifacts_root),
        "legacy_job_ids": [legacy_job_id] if legacy_job_id != job_id else [],
        "legacy_artifacts_roots": [str(legacy_artifacts_root)] if legacy_artifacts_root != artifacts_root else [],
        "program_spec": program_spec,
        "handoff_bundle": handoff_bundle,
    }


def list_projectable_jobs(project_name: str) -> list[dict[str, Any]]:
    projected: list[dict[str, Any]] = []
    specs = load_task_job_specs(project_name)
    for ref in _task_refs(project_name):
        task_id = str(ref.get("task_id", "")).strip()
        if not task_id or task_id not in specs:
            continue
        row = ref["row"]
        if codex_memory.normalize_task_status(row.get("状态", "todo")) not in RUNNABLE_STATUSES:
            continue
        try:
            projected.append(project_background_job(project_name, task_id))
        except ValueError as exc:
            if "is not runnable from status" not in str(exc):
                raise
    projected.sort(key=lambda item: (STATUS_PRIORITY.get(str(item.get("task_status", "")).strip(), 99), item["task_id"]))
    return projected


def cmd_list(args: argparse.Namespace) -> int:
    payload = {
        "project_name": codex_memory.canonical_project_name(args.project_name),
        "jobs": list_projectable_jobs(args.project_name),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    payload = project_background_job(args.project_name, args.task_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Project background jobs from Codex Hub task boards.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    show = subparsers.add_parser("show", help="Show the projected background job for a task.")
    show.add_argument("--project-name", required=True)
    show.add_argument("--task-id", required=True)
    show.set_defaults(func=cmd_show)

    listing = subparsers.add_parser("list", help="List runnable background jobs for a project.")
    listing.add_argument("--project-name", required=True)
    listing.set_defaults(func=cmd_list)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

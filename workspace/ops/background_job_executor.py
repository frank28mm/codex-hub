#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import (
    board_job_projector,
    codex_memory,
    feishu_outbound_gateway,
    growth_truth,
    runtime_state,
    workspace_job_schema,
    workspace_wake_broker,
)


SCRIPT_VERSION = "background-job-executor.v1"
EXTERNAL_DELIVERY_SCOPE = "background_job_external_delivery"
APPROVAL_CARD_ACTION_PREFIX = "perm"
WAKE_REASON_POLICY_KEYS = {
    "manual_wake": "manual_wake",
    "wake_now": "manual_wake",
    "project_writeback": "project_writeback",
    "wake_catchup": "wake_catchup",
    "interval": "scheduled",
    "schedule": "scheduled",
}


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(REPO_ROOT)))


def fixture_mode() -> bool:
    return os.environ.get("WORKSPACE_HUB_FIXTURE_MODE", "").strip() == "1" or "PYTEST_CURRENT_TEST" in os.environ


def iso_now_local() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def job_root(job: dict[str, Any]) -> Path:
    return Path(job["artifacts_root"])


def _legacy_job_ids(job: dict[str, Any]) -> list[str]:
    return [str(item).strip() for item in job.get("legacy_job_ids", []) if str(item).strip()]


def _legacy_artifacts_roots(job: dict[str, Any]) -> list[Path]:
    roots: list[Path] = []
    for item in job.get("legacy_artifacts_roots", []):
        text = str(item).strip()
        if not text:
            continue
        roots.append(Path(text))
    return roots


def _copy_missing_tree(source: Path, target: Path) -> None:
    for path in source.rglob("*"):
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            continue
        shutil.copy2(path, destination)


def _migrate_legacy_artifacts(job: dict[str, Any]) -> None:
    root = job_root(job)
    for legacy_root in _legacy_artifacts_roots(job):
        if legacy_root == root or not legacy_root.exists():
            continue
        root.parent.mkdir(parents=True, exist_ok=True)
        if not root.exists():
            shutil.move(str(legacy_root), str(root))
            continue
        _copy_missing_tree(legacy_root, root)


def _merge_wake_job_state(current: dict[str, Any], legacy: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    merged = dict(current)
    changed = False
    for key in ("pending", "running", "last_completed", "last_abandoned"):
        if merged.get(key):
            continue
        if not legacy.get(key):
            continue
        merged[key] = legacy[key]
        changed = True
    return merged, changed


def _migrate_legacy_wake_state(job: dict[str, Any]) -> None:
    legacy_ids = _legacy_job_ids(job)
    if not legacy_ids:
        return
    state = workspace_wake_broker.load_state()
    jobs = state.setdefault("jobs", {})
    current_key = str(job["job_id"]).strip()
    current_state = dict(jobs.get(current_key, {})) if isinstance(jobs.get(current_key), dict) else {}
    changed = False
    for legacy_id in legacy_ids:
        legacy_state = jobs.get(legacy_id)
        if not isinstance(legacy_state, dict):
            continue
        current_state, merged = _merge_wake_job_state(current_state, legacy_state)
        changed = changed or merged or legacy_id != current_key
        jobs.pop(legacy_id, None)
    if current_state:
        existing_state = jobs.get(current_key)
        if existing_state != current_state:
            jobs[current_key] = current_state
            changed = True
    if changed:
        workspace_wake_broker.save_state(state)


def _ensure_job_identity_compat(job: dict[str, Any], *, migrate_artifacts: bool) -> None:
    _migrate_legacy_wake_state(job)
    if migrate_artifacts:
        _migrate_legacy_artifacts(job)


def _job_id_matches(job: dict[str, Any], candidate: str) -> bool:
    normalized = str(candidate).strip()
    if not normalized:
        return False
    return normalized == str(job["job_id"]).strip() or normalized in _legacy_job_ids(job)


def history_path(job: dict[str, Any]) -> Path:
    return job_root(job) / "history.ndjson"


def gates_path(job: dict[str, Any]) -> Path:
    return job_root(job) / "gates.ndjson"


def latest_report_path(job: dict[str, Any]) -> Path:
    return job_root(job) / "latest.md"


def archive_report_path(job: dict[str, Any], run_id: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return job_root(job) / f"{stamp}-{run_id}.md"


def latest_ops_report_path(job: dict[str, Any]) -> Path:
    return job_root(job) / "latest-ops.md"


def archive_ops_report_path(job: dict[str, Any], run_id: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return job_root(job) / f"{stamp}-{run_id}-ops.md"


def archive_corpus_path(job: dict[str, Any], run_id: str) -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return job_root(job) / f"{stamp}-{run_id}-research.json"


def latest_corpus_path(job: dict[str, Any]) -> Path:
    return job_root(job) / "latest-research.json"


def program_spec(job: dict[str, Any]) -> dict[str, Any]:
    payload = job.get("program_spec", {})
    return dict(payload) if isinstance(payload, dict) else {}


def handoff_bundle(job: dict[str, Any]) -> dict[str, str]:
    payload = job.get("handoff_bundle", {})
    primary = (
        {str(key): str(value) for key, value in payload.items() if str(value).strip()}
        if isinstance(payload, dict) and payload
        else workspace_job_schema.handoff_bundle_paths(job_root(job))
    )
    if Path(primary["task_spec_path"]).exists() or Path(primary["acceptance_path"]).exists():
        return primary
    for legacy_root in _legacy_artifacts_roots(job):
        legacy_bundle = workspace_job_schema.handoff_bundle_paths(legacy_root)
        if Path(legacy_bundle["task_spec_path"]).exists() or Path(legacy_bundle["acceptance_path"]).exists():
            return legacy_bundle
    return primary


def _default_subgoals(job: dict[str, Any]) -> list[dict[str, str]]:
    criteria = [str(item).strip() for item in job.get("acceptance_criteria", []) if str(item).strip()]
    if not criteria:
        criteria = [str(job.get("task_item", "")).strip() or str(job.get("task_id", "")).strip()]
    return [
        {"subgoal_id": f"goal-{index + 1}", "summary": item, "status": "pending"}
        for index, item in enumerate(criteria)
    ]


def _load_task_spec(job: dict[str, Any]) -> dict[str, Any]:
    bundle = handoff_bundle(job)
    return workspace_job_schema.read_json_file(bundle["task_spec_path"])


def _load_acceptance(job: dict[str, Any]) -> dict[str, Any]:
    bundle = handoff_bundle(job)
    return workspace_job_schema.read_json_file(bundle["acceptance_path"])


def _next_pending_subgoal(subgoals: list[dict[str, Any]]) -> dict[str, Any]:
    for item in subgoals:
        if str(item.get("status", "pending")).strip() != "completed":
            return item
    return {}


def _render_progress_markdown(
    job: dict[str, Any],
    *,
    task_spec: dict[str, Any],
    iteration: int,
    next_action: str,
    last_decision: str,
    last_run_id: str,
) -> str:
    completed = [item for item in task_spec.get("subgoals", []) if str(item.get("status", "")).strip() == "completed"]
    pending = [item for item in task_spec.get("subgoals", []) if str(item.get("status", "")).strip() != "completed"]
    lines = [
        f"# Progress｜{job['task_id']}",
        "",
        f"- program_id: `{task_spec.get('program_id', '')}`",
        f"- iteration: `{iteration}`",
        f"- stage: `{task_spec.get('stage', '')}`",
        f"- scope_type: `{task_spec.get('scope_type', '')}`",
        f"- scope_ref: `{task_spec.get('scope_ref', '')}`",
        f"- approval_state: `{task_spec.get('approval_state', '')}`",
        f"- current_focus: {task_spec.get('current_focus', '')}",
        f"- last_decision: `{last_decision}`",
        f"- last_run_id: `{last_run_id}`",
        "",
        "## Completed",
        "",
    ]
    lines.extend([f"- {item.get('summary', '')}" for item in completed] or ["- none"])
    lines.extend(["", "## Pending", ""])
    lines.extend([f"- {item.get('summary', '')}" for item in pending] or ["- none"])
    lines.extend(["", "## Next", "", f"- {next_action or job.get('next_action', '') or '待补充'}", ""])
    return "\n".join(lines)


def _render_smoke_markdown(
    job: dict[str, Any],
    *,
    run_id: str,
    execution_status: str,
    execution_summary: str,
    decision: str,
    report_path: str,
    delivery_status: str,
) -> str:
    lines = [
        f"# Latest Smoke｜{job['task_id']}",
        "",
        f"- run_id: `{run_id}`",
        f"- execution_status: `{execution_status}`",
        f"- delivery_status: `{delivery_status}`",
        f"- decision: `{decision}`",
        f"- summary: {execution_summary}",
        f"- report: {report_path or 'n/a'}",
        "",
    ]
    return "\n".join(lines)


def initialize_program_scaffold(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    persist: bool = True,
) -> dict[str, Any]:
    program = program_spec(job)
    bundle = handoff_bundle(job)
    task_spec = _load_task_spec(job)
    acceptance = _load_acceptance(job)
    subgoals = list(task_spec.get("subgoals", [])) if isinstance(task_spec.get("subgoals"), list) else _default_subgoals(job)
    if not subgoals:
        subgoals = _default_subgoals(job)
    current_focus_item = _next_pending_subgoal(subgoals)
    iteration = int(task_spec.get("iteration_count", 0) or 0) + 1
    if not acceptance:
        acceptance = {
            "task_id": job["task_id"],
            "program_id": program.get("program_id", ""),
            "criteria": list(job.get("acceptance_criteria", [])),
            "required_evidence": ["progress.md", "latest-smoke.md"],
            "required_smoke": [str(job.get("executor_kind", "")).strip() or "executor-smoke"],
            "updated_at": iso_now_local(),
        }
    previous_stage = str(task_spec.get("stage", "")).strip() or str(program.get("stage", "discover")).strip()
    previous_stage_plan = (
        list(task_spec.get("stage_plan", []))
        if isinstance(task_spec.get("stage_plan"), list)
        else list(program.get("stage_plan", []))
    )
    previous_wake_policy = (
        dict(task_spec.get("wake_policy", {}))
        if isinstance(task_spec.get("wake_policy"), dict)
        else dict(program.get("wake_policy", {}))
    )
    previous_last_evaluation = (
        dict(task_spec.get("last_evaluation", {}))
        if isinstance(task_spec.get("last_evaluation"), dict)
        else {}
    )
    task_spec_payload = {
        "task_id": job["task_id"],
        "program_id": program.get("program_id", ""),
        "objective": str(task_spec.get("objective", "")).strip() or program.get("objective", "") or job.get("task_item", ""),
        "scope_type": program.get("scope_type", "project"),
        "scope_ref": program.get("scope_ref", job.get("project_name", "")),
        "approval_required": bool(program.get("approval_required", False)),
        "approval_state": str(task_spec.get("approval_state", "")).strip() or program.get("approval_state", "not-required"),
        "stage": previous_stage,
        "stage_plan": previous_stage_plan,
        "wake_policy": previous_wake_policy,
        "iteration_count": iteration,
        "current_focus": str(current_focus_item.get("summary", "")).strip() or str(job.get("task_item", "")).strip(),
        "subgoals": subgoals,
        "updated_at": iso_now_local(),
        "last_run_id": run_context["run_id"],
        "last_evaluation": previous_last_evaluation,
        "stage_history": list(task_spec.get("stage_history", [])),
        "last_decision": str(task_spec.get("last_decision", "")).strip(),
        "last_external_delivery": dict(task_spec.get("last_external_delivery", {}))
        if isinstance(task_spec.get("last_external_delivery"), dict)
        else {},
    }
    runtime_program = {
        **program,
        "approval_state": task_spec_payload["approval_state"],
        "stage": task_spec_payload["stage"],
        "stage_plan": list(task_spec_payload["stage_plan"]),
        "wake_policy": dict(task_spec_payload["wake_policy"]),
    }
    if persist:
        workspace_job_schema.write_json_file(bundle["task_spec_path"], task_spec_payload)
        workspace_job_schema.write_json_file(bundle["acceptance_path"], acceptance)
        if not Path(bundle["progress_path"]).exists():
            workspace_job_schema.write_text_file(
                bundle["progress_path"],
                _render_progress_markdown(
                    job,
                    task_spec=task_spec_payload,
                    iteration=iteration,
                    next_action=str(job.get("next_action", "")).strip(),
                    last_decision="initialized",
                    last_run_id=run_context["run_id"],
                ),
            )
        if not Path(bundle["latest_smoke_path"]).exists():
            workspace_job_schema.write_text_file(
                bundle["latest_smoke_path"],
                _render_smoke_markdown(
                    job,
                    run_id=run_context["run_id"],
                    execution_status="skipped",
                    execution_summary="program scaffold initialized",
                    decision="initialized",
                    report_path="",
                    delivery_status="not-requested",
                ),
            )
    return {
        "program": runtime_program,
        "task_spec": task_spec_payload,
        "acceptance": acceptance,
        "paths": bundle,
        "iteration": iteration,
        "current_focus": task_spec_payload["current_focus"],
    }


def finalize_program_iteration(
    job: dict[str, Any],
    *,
    scaffold: dict[str, Any],
    evaluation: dict[str, Any],
    updated_subgoals: list[dict[str, Any]],
    execution_status: str,
    execution_summary: str,
    next_action: str,
    report_path: str,
    delivery_status: str,
    external_delivery_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    task_spec = dict(scaffold.get("task_spec", {}))
    previous_stage = str(task_spec.get("stage", "")).strip() or str(scaffold.get("program", {}).get("stage", "discover")).strip()
    subgoals = [dict(item) for item in updated_subgoals]
    next_focus_item = _next_pending_subgoal(subgoals)
    task_spec["subgoals"] = subgoals
    task_spec["current_focus"] = str(next_focus_item.get("summary", "")).strip()
    task_spec["stage"] = str(evaluation.get("next_stage", previous_stage)).strip() or previous_stage
    task_spec["last_decision"] = str(evaluation.get("decision", "")).strip()
    task_spec["last_evaluation"] = dict(evaluation)
    task_spec["last_run_id"] = str(scaffold.get("run_id", "")).strip() or str(task_spec.get("last_run_id", "")).strip()
    task_spec["updated_at"] = iso_now_local()
    stage_history = list(task_spec.get("stage_history", []))
    stage_history.append(
        {
            "from_stage": previous_stage,
            "to_stage": task_spec["stage"],
            "decision": task_spec["last_decision"],
            "run_id": task_spec["last_run_id"],
            "ts": task_spec["updated_at"],
        }
    )
    task_spec["stage_history"] = stage_history[-20:]
    if external_delivery_state is not None:
        task_spec["last_external_delivery"] = dict(external_delivery_state)
    bundle = scaffold["paths"]
    workspace_job_schema.write_json_file(bundle["task_spec_path"], task_spec)
    workspace_job_schema.write_text_file(
        bundle["progress_path"],
        _render_progress_markdown(
            job,
            task_spec=task_spec,
            iteration=int(scaffold.get("iteration", 1) or 1),
            next_action=next_action,
            last_decision=task_spec["last_decision"],
            last_run_id=str(task_spec.get("last_run_id", "")).strip() or str(scaffold.get("run_id", "")).strip(),
        ),
    )
    workspace_job_schema.write_text_file(
        bundle["latest_smoke_path"],
        _render_smoke_markdown(
            job,
            run_id=str(task_spec.get("last_run_id", "")).strip() or str(scaffold.get("run_id", "")).strip(),
            execution_status=execution_status,
            execution_summary=execution_summary,
            decision=task_spec["last_decision"],
            report_path=report_path,
            delivery_status=delivery_status,
        ),
    )
    return task_spec


def enforce_program_scope(program: dict[str, Any]) -> None:
    scope_type = str(program.get("scope_type", "project")).strip()
    approval_required = bool(program.get("approval_required", False))
    approval_state = str(program.get("approval_state", "not-required")).strip()
    if scope_type == "workspace" and approval_required and approval_state != "approved":
        raise ValueError("workspace-scoped program requires explicit approval before execution")


def _apply_execution_result_to_subgoals(
    subgoals: list[dict[str, Any]],
    *,
    current_focus: str,
    execution_status: str,
) -> list[dict[str, Any]]:
    updated = [dict(item) for item in subgoals]
    if execution_status != "ok" or not current_focus:
        return updated
    for item in updated:
        if str(item.get("summary", "")).strip() == current_focus and str(item.get("status", "")).strip() != "completed":
            item["status"] = "completed"
            break
    return updated


def evaluate_program_iteration(
    job: dict[str, Any],
    *,
    scaffold: dict[str, Any],
    execution_status: str,
    delivery_status: str,
    gate_state: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    task_spec = dict(scaffold.get("task_spec", {}))
    current_stage = str(task_spec.get("stage", "")).strip() or "discover"
    current_focus = str(task_spec.get("current_focus", "")).strip()
    updated_subgoals = _apply_execution_result_to_subgoals(
        list(task_spec.get("subgoals", [])),
        current_focus=current_focus,
        execution_status=execution_status,
    )
    pending_count = sum(1 for item in updated_subgoals if str(item.get("status", "")).strip() != "completed")
    completed_count = len(updated_subgoals) - pending_count
    gate_status = str(gate_state.get("status", "")).strip()
    if gate_status == "awaiting_gate":
        decision = "gate"
        acceptance_status = "awaiting-gate"
    elif execution_status != "ok":
        decision = "adapt" if current_stage in {"execute", "verify", "adapt", "handoff"} else "blocked"
        acceptance_status = "needs-adaptation" if decision == "adapt" else "blocked"
    elif delivery_status == "not-delivered":
        decision = "adapt"
        acceptance_status = "needs-adaptation"
    elif current_stage in {"verify", "handoff"} and pending_count == 0:
        decision = "done"
        acceptance_status = "accepted"
    else:
        decision = "continue"
        acceptance_status = "criteria-met" if pending_count == 0 else "criteria-pending"
    next_stage = workspace_job_schema.next_program_stage(
        current_stage,
        decision=decision,
        has_pending_subgoals=pending_count > 0,
    )
    evaluation = workspace_job_schema.ProgramEvaluation(
        current_stage=current_stage,
        next_stage=next_stage,
        decision=decision,
        acceptance_status=acceptance_status,
        delivery_status=delivery_status,
        completed_subgoal_count=completed_count,
        pending_subgoal_count=pending_count,
        metadata={
            "gate_status": gate_status,
            "scope_type": str(task_spec.get("scope_type", "")).strip(),
            "scope_ref": str(task_spec.get("scope_ref", "")).strip(),
            "task_id": job.get("task_id", ""),
        },
    ).to_dict()
    return evaluation, updated_subgoals


def _final_board_status(evaluation: dict[str, Any]) -> str:
    decision = str(evaluation.get("decision", "")).strip()
    if decision == "done":
        return "done"
    if decision == "blocked":
        return "blocked"
    return "doing"


def wake_policy_allows(job: dict[str, Any], *, reason: str) -> bool:
    policy = dict(program_spec(job).get("wake_policy", {}))
    if not policy:
        return True
    key = WAKE_REASON_POLICY_KEYS.get(str(reason).strip(), str(reason).strip())
    value = policy.get(key)
    if value is None:
        return True
    return bool(value)


def _topic_name(job: dict[str, Any]) -> str:
    source = str(job.get("source", "")).strip()
    if source.startswith("topic:"):
        return source.split(":", 1)[1]
    return ""


def build_run_context(
    job: dict[str, Any],
    *,
    trigger_source: str = "",
    scheduled_for: str = "",
    automation_run_id: str = "",
    scheduler_id: str = "",
) -> dict[str, str]:
    started_at = iso_now_local()
    return {
        "run_id": f"bge-{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}",
        "trigger_source": trigger_source or "manual_cli",
        "scheduled_for": scheduled_for or started_at,
        "automation_run_id": automation_run_id,
        "scheduler_id": scheduler_id,
        "started_at": started_at,
        "job_id": str(job["job_id"]),
    }


def _generic_brief_plan(job: dict[str, Any]) -> list[str]:
    return [
        "读取任务主表与来源板面，固定当前任务语义。",
        "按验收标准形成第一轮研究与取证顺序。",
        "输出一份可直接进入 Phase 2 delivery/gate 的执行简报。",
    ]


def job_focus(job: dict[str, Any]) -> str:
    return str(job.get("current_focus", "")).strip()


def _is_growth_executor(job: dict[str, Any]) -> bool:
    return str(job.get("executor_kind", "")).strip().startswith("growth_")


def _first_meaningful_line(text: str) -> str:
    for line in text.splitlines():
        stripped = " ".join(str(line).split()).strip()
        if len(stripped) >= 20:
            return stripped
    return ""


def _read_local_excerpt(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    text = path.read_text(encoding="utf-8", errors="ignore")
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped == "---":
            continue
        if stripped.startswith(("title:", "project_name:", "aliases:", "status:", "priority:", "updated_at:", "summary:")):
            continue
        lines.append(stripped)
    excerpt = "\n\n".join(lines[:60]).strip()[:6000]
    title = ""
    for line in lines:
        if line.startswith("# "):
            title = line[2:].strip()
            break
    return {
        "title": title or path.name,
        "excerpt": excerpt,
        "fetched_url": str(path),
        "status_code": 200,
    }


def _fetch_research_source(source: dict[str, Any]) -> dict[str, Any]:
    kind = str(source.get("kind", "url")).strip() or "url"
    if kind == "url":
        from ops import knowledge_intake

        captured = knowledge_intake.fetch_html_excerpt(str(source.get("url", "")).strip())
    elif kind == "file":
        captured = _read_local_excerpt(str(source.get("path", "")).strip())
    else:
        raise ValueError(f"unsupported research source kind: {kind}")
    excerpt = str(captured.get("excerpt", "")).strip()
    return {
        "source_id": str(source.get("source_id", "")).strip(),
        "kind": kind,
        "title": str(source.get("title", "")).strip() or str(captured.get("title", "")).strip(),
        "uri": str(captured.get("fetched_url", "")).strip() or str(source.get("url", "")).strip() or str(source.get("path", "")).strip(),
        "lens": str(source.get("lens", "")).strip(),
        "expected_signal": str(source.get("expected_signal", "")).strip(),
        "excerpt": excerpt,
        "excerpt_lead": _first_meaningful_line(excerpt),
    }


def collect_research_corpus(job: dict[str, Any]) -> dict[str, Any]:
    sources = [dict(item) for item in job.get("research_sources", []) if isinstance(item, dict)]
    collected: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for item in sources:
        try:
            collected.append(_fetch_research_source(item))
        except Exception as exc:
            failures.append(
                {
                    "source_id": str(item.get("source_id", "")).strip(),
                    "title": str(item.get("title", "")).strip(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    return {
        "sources": collected,
        "failures": failures,
        "requested_count": len(sources),
        "collected_count": len(collected),
    }


def synthesize_research_findings(job: dict[str, Any], corpus: dict[str, Any]) -> dict[str, Any]:
    sources = list(corpus.get("sources", []))
    failures = list(corpus.get("failures", []))
    by_lens: dict[str, list[dict[str, Any]]] = {}
    for item in sources:
        by_lens.setdefault(str(item.get("lens", "")).strip() or "general", []).append(item)

    def _render_bucket(lenses: list[str], *, prefix: str) -> list[str]:
        bullets: list[str] = []
        for lens in lenses:
            for source in by_lens.get(lens, []):
                signal = str(source.get("expected_signal", "")).strip() or str(source.get("excerpt_lead", "")).strip()
                if not signal:
                    continue
                bullets.append(f"{prefix}{signal} 证据：{source.get('title', '')}")
        return bullets

    replicable = _render_bucket(["baseline", "replicable"], prefix="")
    partial = _render_bucket(["partial"], prefix="部分可替代：")
    boundaries = _render_bucket(["native-boundary", "partial"], prefix="")
    if not replicable:
        replicable.append("当前可直接复刻的证据仍不足，需继续补齐官方接口与现网能力对应关系。")
    if not boundaries:
        boundaries.append("当前未明确找到强约束边界，需继续补齐飞书原生托管能力与外部执行器之间的差异证据。")

    recommended_route = [
        "以飞书开放平台和现有 `feishu-op` 对象操作链为基础接口层，继续扩展消息、对象操作与回调接入，而不是从零复制飞书原生平台。",
        "把智能伙伴类交互收成“飞书入口 + Codex Hub 后台 executor + board/report/writeback + gate”的外部执行器模式，优先利用可配置回调能力承接后续动作。",
        "多维表格 AI 先走“Bitable 对象操作 + 外部模型生成结构化结果/消息投递”的替代路线，不追求 Prompt IDE、运行额度和内置问答的 1:1 同构。",
    ]
    open_questions: list[str] = []
    if not any("CLI" in (item.get("title", "") + item.get("excerpt", "")) for item in sources):
        open_questions.append("官方公开资料里尚未稳定锁定单独的飞书原生 CLI 文档入口；当前先按开放平台 + Aily 回调 + 外部执行器模式推进。")
    if failures:
        open_questions.append(f"本轮有 {len(failures)} 个研究源抓取失败，下一轮应补抓失败源并校验正文抽取质量。")

    status = "research-report-ready" if sources else "research-blocked"
    headline = (
        "官方资料显示：飞书开放平台与 Aily 回调足以支撑“飞书前台 + Codex Hub 后台执行器”的替代路线，"
        "但 Aily 运行额度、Prompt IDE 与多维表格 AI 的原生体验仍属于平台内建优势。"
        if sources
        else "本轮未抓取到足够官方资料，无法形成稳定研究结论。"
    )
    return {
        "status": status,
        "headline": headline,
        "replicable_capabilities": replicable + partial,
        "non_replicable_boundaries": boundaries,
        "recommended_route": recommended_route,
        "open_questions": open_questions,
        "source_failures": failures,
        "collected_sources": sources,
        "analysis_focus": list(job.get("analysis_focus", [])),
    }


def _table_row_count(path_text: str) -> int:
    path = Path(path_text)
    if not path.exists():
        return 0
    count = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        if stripped.startswith("| ---"):
            continue
        if stripped.startswith("| offer_id ") or stripped.startswith("| listing_id ") or stripped.startswith("| lead_id "):
            continue
        if stripped.startswith("| conversation_id ") or stripped.startswith("| action_id ") or stripped.startswith("| evidence_id "):
            continue
        count += 1
    return count


def _growth_table_sources(job: dict[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    table_map = job.get("object_tables", {})
    if not isinstance(table_map, dict):
        return sources
    for object_name, path_text in table_map.items():
        path_value = str(path_text or "").strip()
        if not path_value:
            continue
        item = _read_local_excerpt(path_value)
        sources.append(
            {
                "source_id": f"{str(job.get('task_id', '')).strip().lower()}-{str(object_name).strip().lower()}",
                "kind": "file",
                "title": f"{object_name} 主表",
                "uri": path_value,
                "lens": "growth-truth",
                "expected_signal": f"{object_name} 已作为 Growth System 的 canonical truth 接入。",
                "excerpt": item.get("excerpt", ""),
                "excerpt_lead": _first_meaningful_line(str(item.get("excerpt", ""))),
                "row_count": _table_row_count(path_value),
            }
        )
    return sources


def _growth_payload(
    job: dict[str, Any],
    *,
    headline: str,
    confirmed: list[str],
    gaps: list[str],
    next_steps: list[str],
    sources: list[dict[str, Any]],
    open_questions: list[str] | None = None,
    status: str = "growth-report-ready",
) -> dict[str, Any]:
    return {
        "status": status,
        "headline": headline,
        "replicable_capabilities": confirmed,
        "non_replicable_boundaries": gaps,
        "recommended_route": next_steps,
        "open_questions": open_questions or [],
        "source_failures": [],
        "collected_sources": sources,
        "analysis_focus": list(job.get("analysis_focus", [])),
    }


def _growth_action_status(*, execution_status: str, gate_state: dict[str, Any]) -> str:
    if str(gate_state.get("status", "")).strip() == "awaiting_gate":
        return "gated"
    return "done" if execution_status == "ok" else "failed"


def write_growth_truth_records(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    finished_at: str,
    execution_status: str,
    execution_summary: str,
    research_payload: dict[str, Any],
    gate_state: dict[str, Any],
) -> dict[str, Any]:
    action_status = _growth_action_status(execution_status=execution_status, gate_state=gate_state)
    primary_platforms = [str(item).strip() for item in job.get("primary_platforms", []) if str(item).strip()]
    action_row = {
        "action_id": run_context["run_id"],
        "platform": ",".join(primary_platforms) or "growth-system",
        "command": str(job.get("executor_kind", "")).strip(),
        "target_type": "task",
        "target_id": str(job.get("task_id", "")).strip(),
        "status": action_status,
        "risk_level": str(job.get("gate_policy", "")).strip() or str(job.get("automation_mode", "")).strip(),
        "run_id": run_context["run_id"],
        "error": str(gate_state.get("summary", "")).strip() if action_status == "gated" else (execution_summary if execution_status != "ok" else ""),
        "executed_at": finished_at,
    }
    growth_truth.upsert_rows("Action", [action_row])

    evidence_rows: list[dict[str, str]] = []
    headline = str(research_payload.get("headline", "")).strip()
    if headline:
        evidence_rows.append(
            {
                "evidence_id": f"{run_context['run_id']}-summary",
                "source_type": "job_run",
                "source_id": run_context["run_id"],
                "signal_type": str(job.get("executor_kind", "")).strip(),
                "content": headline,
                "decision": "accepted" if execution_status == "ok" else "rejected",
                "merged_into": "",
                "created_at": finished_at,
            }
        )
    if evidence_rows:
        growth_truth.upsert_rows("Evidence", evidence_rows)

    targets = [str(growth_truth.object_path("Action"))]
    if evidence_rows:
        targets.append(str(growth_truth.object_path("Evidence")))
    return {
        "action_row_count": 1,
        "evidence_row_count": len(evidence_rows),
        "targets": targets,
        "action_status": action_status,
    }


def _growth_truth_targets_if_configured() -> list[str]:
    targets: list[str] = []
    for object_name in ("Action", "Evidence"):
        try:
            targets.append(str(growth_truth.object_path(object_name)))
        except Exception:
            continue
    return targets


def run_growth_signal_scan(job: dict[str, Any]) -> dict[str, Any]:
    plan = [
        "读取 Growth System 控制真源、任务语义与对象主表。",
        "扫描当前 workflow family、对象状态与平台策略。",
        "输出当前已固定 contract、当前缺口和下一步接法。",
    ]
    sources = _growth_table_sources(job)
    platform_policies = dict(job.get("platform_policies", {})) if isinstance(job.get("platform_policies"), dict) else {}
    confirmed = [
        f"已固定 workflow：`{job.get('workflow_id') or 'n/a'}`，executor：`{job.get('executor_kind')}`。",
        f"已固定主产品：`{job.get('primary_product') or 'Codex Hub'}`；主平台：{', '.join(job.get('primary_platforms', [])) or '待补充'}。",
        f"已接入对象主表：{', '.join(job.get('input_objects', [])) or '待补充'}。",
    ]
    if platform_policies:
        confirmed.append(
            "已固定平台角色："
            + "；".join(
                f"{name}={str(item.get('role', '')).strip()}"
                for name, item in platform_policies.items()
                if isinstance(item, dict)
            )
        )
    gaps: list[str] = []
    if any(int(item.get("row_count", 0)) == 0 for item in sources):
        gaps.append("部分对象主表当前还没有业务记录；下一步需要用真实发布、互动与线索回写去填充。")
    gaps.append("Feishu projection app 与 6 张表仍需落地，当前真相源仍以 Vault 主表为主。")
    gaps.append("Growth jobs、executor kinds 和平台写动作仍需接入现有 runtime。")
    next_steps = [
        "先让 `增长与营销` 任务板稳定投影出 Growth jobs。",
        "再补 `growth_signal_scan / growth_offer_publish / growth_lead_cycle` 的 executor loop。",
        "最后把自动发布、自动互动、对象回写和飞书备份接成第一条闭环。",
    ]
    rounds = [
        {
            "round_index": 1,
            "state": "planning",
            "current_plan": plan,
            "action_taken": "freeze_growth_contract",
            "evidence": {
                "workflow_id": job.get("workflow_id", ""),
                "input_objects": job.get("input_objects", []),
                "summary_focus": job.get("summary_focus", ""),
            },
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "growth-contract-ready"},
            "decision": "continue",
        },
        {
            "round_index": 2,
            "state": "acting",
            "current_plan": plan,
            "action_taken": "scan_growth_truth_and_policy",
            "evidence": {
                "object_table_count": len(sources),
                "object_row_counts": {item["title"]: item.get("row_count", 0) for item in sources},
                "platform_policy_count": len(platform_policies),
            },
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "growth-scan-collected"},
            "decision": "continue",
        },
        {
            "round_index": 3,
            "state": "checking",
            "current_plan": plan,
            "action_taken": "synthesize_growth_scan",
            "evidence": {"headline": job.get("summary_focus", "") or job.get("task_item", ""), "gap_count": len(gaps)},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "growth-report-ready"},
            "decision": "done",
        },
    ]
    payload = _growth_payload(
        job,
        headline=f"{job.get('summary_focus') or job.get('task_item')} 已形成当前实施扫描，下一步可以直接进入 runtime 与 execution 接线。",
        confirmed=confirmed,
        gaps=gaps,
        next_steps=next_steps,
        sources=sources,
    )
    return {"phase": "growth-v1", "rounds": rounds[: max(1, int(job.get("max_rounds", 3)))], "research_payload": payload}


def run_growth_offer_publish(job: dict[str, Any]) -> dict[str, Any]:
    plan = [
        "读取 Offer / Listing 主表并固定首发 offer 与 listing 版本。",
        "总结当前可发布资产、素材缺口和平台表达策略。",
        "输出发布前准备、模板与下一步执行顺序。",
    ]
    sources = _growth_table_sources(job)
    row_counts = {item["title"]: item.get("row_count", 0) for item in sources}
    confirmed = [
        "已固定 2 个 active offer：`AI 工作流诊断`、`Codex 工作台搭建 / 自动化梳理`。",
        "已固定 6 条 draft listing：3 条闲鱼版本、3 条小红书版本。",
        "已把 Offer 与 Listing 放入 Vault first 的对象主表，后续发布和回写都可追踪。",
    ]
    gaps = [
        "首批媒体素材路径、封面图与视频脚本仍待补齐。",
        "FAQ、objection、评论模板和私信模板还需要进一步结构化。",
        "Listing 还没有写入真实 remote_ref 与 published_at。",
    ]
    next_steps = [
        "补齐 listing 文案、封面和媒体路径。",
        "接入自动发布动作，先写 `Action` 记录，再执行平台 publish。",
        "发布后把 remote_ref、发布时间和结果写回 Listing 主表。",
    ]
    rounds = [
        {
            "round_index": 1,
            "state": "planning",
            "current_plan": plan,
            "action_taken": "freeze_offer_listing_scope",
            "evidence": {"input_objects": job.get("input_objects", []), "summary_focus": job.get("summary_focus", "")},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "offer-scope-ready"},
            "decision": "continue",
        },
        {
            "round_index": 2,
            "state": "acting",
            "current_plan": plan,
            "action_taken": "scan_offer_listing_tables",
            "evidence": {"row_counts": row_counts},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "offer-listing-collected"},
            "decision": "continue",
        },
        {
            "round_index": 3,
            "state": "checking",
            "current_plan": plan,
            "action_taken": "synthesize_offer_publish_pack",
            "evidence": {"headline": job.get("summary_focus", ""), "gap_count": len(gaps)},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "growth-report-ready"},
            "decision": "done",
        },
    ]
    payload = _growth_payload(
        job,
        headline=f"{job.get('summary_focus') or job.get('task_item')} 已具备首发 offer 与 listing 基线，下一步进入自动发布与资产补齐。",
        confirmed=confirmed,
        gaps=gaps,
        next_steps=next_steps,
        sources=sources,
    )
    return {"phase": "growth-v1", "rounds": rounds[: max(1, int(job.get("max_rounds", 3)))], "research_payload": payload}


def run_growth_lead_cycle(job: dict[str, Any]) -> dict[str, Any]:
    plan = [
        "读取 Lead / Conversation / Action / Evidence 主表并固定闭环对象。",
        "总结当前自动互动、handoff 与回写的最小 pilot 路径。",
        "输出闭环 pilot 的缺口、熔断点与下一步动作。",
    ]
    sources = _growth_table_sources(job)
    row_counts = {item["title"]: item.get("row_count", 0) for item in sources}
    confirmed = [
        "已固定 `Lead / Conversation / Action / Evidence` 4 类闭环对象。",
        "已固定自动化边界：发布与互动都自动，高价值成交仍由你接手。",
        "已固定学习闭环：`Action -> Result -> Evidence -> Learning -> Next Run`。",
    ]
    gaps = [
        "Lead、Conversation、Action、Evidence 主表当前还没有真实 pilot 记录。",
        "评论、私信、咨询回复动作需要接入统一 envelope 与稳定性控制。",
        "高意向判定、handoff 阈值和异常接管还需要写入结构化规则。",
    ]
    next_steps = [
        "接入 `comment-send / dm-send / inquiry-reply`，每次动作都先生成 `Action`。",
        "命中高意向条件后，把 `Lead.status` 切到 `handoff` 并交给你接手。",
        "成交或拒绝后沉淀 `Evidence`，再用它更新下一轮模板和规则。",
    ]
    rounds = [
        {
            "round_index": 1,
            "state": "planning",
            "current_plan": plan,
            "action_taken": "freeze_lead_cycle_contract",
            "evidence": {"input_objects": job.get("input_objects", []), "summary_focus": job.get("summary_focus", "")},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "lead-cycle-ready"},
            "decision": "continue",
        },
        {
            "round_index": 2,
            "state": "acting",
            "current_plan": plan,
            "action_taken": "scan_lead_cycle_tables",
            "evidence": {"row_counts": row_counts},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "lead-cycle-collected"},
            "decision": "continue",
        },
        {
            "round_index": 3,
            "state": "checking",
            "current_plan": plan,
            "action_taken": "synthesize_lead_cycle_pilot",
            "evidence": {"headline": job.get("summary_focus", ""), "gap_count": len(gaps)},
            "acceptance_check": {"criteria_count": len(job["acceptance_criteria"]), "result": "growth-report-ready"},
            "decision": "done",
        },
    ]
    payload = _growth_payload(
        job,
        headline=f"{job.get('summary_focus') or job.get('task_item')} 已具备对象 contract，下一步进入自动互动、handoff 与 evidence 回写。",
        confirmed=confirmed,
        gaps=gaps,
        next_steps=next_steps,
        sources=sources,
    )
    return {"phase": "growth-v1", "rounds": rounds[: max(1, int(job.get("max_rounds", 3)))], "research_payload": payload}


def write_research_payload(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    research_payload: dict[str, Any],
) -> dict[str, str]:
    root = job_root(job)
    root.mkdir(parents=True, exist_ok=True)
    archive_path = archive_corpus_path(job, run_context["run_id"])
    latest_path = latest_corpus_path(job)
    text = json.dumps(research_payload, ensure_ascii=False, indent=2)
    archive_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return {
        "archive_path": str(archive_path),
        "latest_path": str(latest_path),
    }


def run_generic_agent_loop(job: dict[str, Any]) -> dict[str, Any]:
    plan = _generic_brief_plan(job)
    focus = job_focus(job)
    rounds: list[dict[str, Any]] = [
        {
            "round_index": 1,
            "state": "planning",
            "current_plan": plan,
            "action_taken": "project_task_into_background_job",
            "evidence": {
                "task_id": job["task_id"],
                "task_item": job["task_item"],
                "source_path": job["source_path"],
                "task_pointer": job["task_pointer"],
                "current_focus": focus,
            },
            "acceptance_check": {
                "criteria": job["acceptance_criteria"],
                "result": "plan-ready",
            },
            "decision": "continue",
        },
        {
            "round_index": 2,
            "state": "checking",
            "current_plan": plan,
            "action_taken": "render_phase1_execution_brief",
            "evidence": {
                "automation_mode": job["automation_mode"],
                "allowed_actions": job["allowed_actions"],
                "delivery_targets": job["delivery_targets"],
                "gate_policy": job["gate_policy"],
                "max_rounds": job["max_rounds"],
                "time_budget_minutes": job["time_budget_minutes"],
            },
            "acceptance_check": {
                "criteria_count": len(job["acceptance_criteria"]),
                "result": "phase1-brief-ready",
            },
            "decision": "done",
        },
    ]
    return {
        "phase": "phase-2",
        "rounds": rounds[: max(1, int(job.get("max_rounds", 2)))],
        "research_payload": {},
    }


def run_research_agent_loop(job: dict[str, Any]) -> dict[str, Any]:
    sources = [dict(item) for item in job.get("research_sources", []) if isinstance(item, dict)]
    focus = list(job.get("analysis_focus", []))
    current_focus = job_focus(job)
    plan = [
        "读取任务与当前板面状态，固定本轮研究问题。",
        "抓取官方资料与内部基线，形成可回查的 research corpus。",
        "按可复刻能力、原生边界与替代路线输出结构化研究结论。",
    ]
    rounds: list[dict[str, Any]] = [
        {
            "round_index": 1,
            "state": "planning",
            "current_plan": plan,
            "action_taken": "plan_research_corpus",
            "evidence": {
                "task_id": job["task_id"],
                "source_count": len(sources),
                "analysis_focus": focus,
                "current_focus": current_focus,
            },
            "acceptance_check": {
                "criteria_count": len(job["acceptance_criteria"]),
                "result": "plan-ready",
            },
            "decision": "continue",
        }
    ]
    corpus = collect_research_corpus(job)
    rounds.append(
        {
            "round_index": 2,
            "state": "acting",
            "current_plan": plan,
            "action_taken": "collect_research_sources",
            "evidence": {
                "requested_count": corpus["requested_count"],
                "collected_count": corpus["collected_count"],
                "collected_titles": [item.get("title", "") for item in corpus.get("sources", [])],
                "failure_count": len(corpus.get("failures", [])),
            },
            "acceptance_check": {
                "criteria_count": len(job["acceptance_criteria"]),
                "result": "research-corpus-collected" if corpus["collected_count"] else "research-corpus-missing",
            },
            "decision": "continue" if corpus["collected_count"] else "blocked",
        }
    )
    research_payload = synthesize_research_findings(job, corpus)
    rounds.append(
        {
            "round_index": 3,
            "state": "checking",
            "current_plan": plan,
            "action_taken": "synthesize_research_findings",
            "evidence": {
                "headline": research_payload["headline"],
                "replicable_count": len(research_payload["replicable_capabilities"]),
                "boundary_count": len(research_payload["non_replicable_boundaries"]),
                "open_question_count": len(research_payload["open_questions"]),
            },
            "acceptance_check": {
                "criteria_count": len(job["acceptance_criteria"]),
                "result": research_payload["status"],
            },
            "decision": "done" if research_payload["status"] == "research-report-ready" else "blocked",
        }
    )
    return {
        "phase": "phase-3",
        "rounds": rounds[: max(1, int(job.get("max_rounds", 3)))],
        "research_payload": research_payload,
    }


def run_agent_loop(job: dict[str, Any]) -> dict[str, Any]:
    executor_kind = str(job.get("executor_kind", "")).strip()
    if executor_kind == "growth_signal_scan":
        return run_growth_signal_scan(job)
    if executor_kind == "growth_offer_publish":
        return run_growth_offer_publish(job)
    if executor_kind == "growth_lead_cycle":
        return run_growth_lead_cycle(job)
    if executor_kind == "research_brief" and job.get("research_sources"):
        return run_research_agent_loop(job)
    return run_generic_agent_loop(job)


def render_ops_report(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    rounds: list[dict[str, Any]],
    phase: str = "phase-2",
    research_payload: dict[str, Any] | None = None,
    gate_state: dict[str, Any] | None = None,
    delivery_outcomes: list[dict[str, Any]] | None = None,
) -> str:
    is_growth = _is_growth_executor(job)
    lines = [
        f"# Background Job Brief｜{job['task_id']}",
        "",
        f"- job_id: `{job['job_id']}`",
        f"- run_id: `{run_context['run_id']}`",
        f"- project: `{job['project_name']}`",
        f"- task_item: {job['task_item']}",
        f"- trigger_source: `{run_context['trigger_source']}`",
        f"- automation_mode: `{job['automation_mode']}`",
        f"- executor_kind: `{job['executor_kind']}`",
        f"- gate_policy: `{job['gate_policy']}`",
        "",
        "## Task Snapshot",
        "",
        f"- source: `{job['source']}`",
        f"- scope: `{job['scope']}`",
        f"- source_path: `{job['source_path']}`",
        f"- project_board_path: `{job['project_board_path']}`",
        f"- task_pointer: `{job['task_pointer'] or 'n/a'}`",
        f"- next_action: {job['next_action'] or '待补充'}",
        f"- deliverable: {job['deliverable'] or '待补充'}",
        "",
        "## Acceptance Criteria",
        "",
    ]
    lines.extend([f"- {item}" for item in job["acceptance_criteria"]])
    lines.extend(
        [
        "",
        "## Agent Loop",
        "",
        ]
    )
    for round_item in rounds:
        lines.extend(
            [
                f"### Round {round_item['round_index']}",
                "",
                f"- state: `{round_item['state']}`",
                f"- action: `{round_item['action_taken']}`",
                f"- decision: `{round_item['decision']}`",
                f"- acceptance: `{round_item['acceptance_check'].get('result', '')}`",
                "",
            ]
        )
    if research_payload:
        lines.extend(["## Growth Focus" if is_growth else "## Research Focus", ""])
        for item in research_payload.get("analysis_focus", []):
            lines.append(f"- {item}")
        lines.extend(["", "## Growth Sources" if is_growth else "## Research Corpus", ""])
        for item in research_payload.get("collected_sources", []):
            lines.extend(
                [
                    f"### {item.get('title', '')}",
                    "",
                    f"- lens: `{item.get('lens', '')}`",
                    f"- uri: {item.get('uri', '')}",
                    f"- expected_signal: {item.get('expected_signal', '')}",
                    f"- excerpt: {item.get('excerpt_lead', '') or item.get('excerpt', '')[:280]}",
                    "",
                ]
            )
        if research_payload.get("source_failures"):
            lines.extend(["## Source Failures", ""])
            for item in research_payload.get("source_failures", []):
                lines.append(f"- `{item.get('source_id', '')}` {item.get('title', '')}: {item.get('error', '')}")
            lines.append("")
        lines.extend(["## Growth Findings" if is_growth else "## Research Findings", "", f"- headline: {research_payload.get('headline', '')}", ""])
        lines.append("### 当前已固定 contract" if is_growth else "### 可复刻能力")
        lines.append("")
        for item in research_payload.get("replicable_capabilities", []):
            lines.append(f"- {item}")
        lines.extend(["", "### 当前缺口" if is_growth else "### 不可完整复刻边界", ""])
        for item in research_payload.get("non_replicable_boundaries", []):
            lines.append(f"- {item}")
        lines.extend(["", "### 下一步" if is_growth else "### 下一阶段接入路线", ""])
        for item in research_payload.get("recommended_route", []):
            lines.append(f"- {item}")
        if research_payload.get("open_questions"):
            lines.extend(["", "### 待补问题", ""])
            for item in research_payload.get("open_questions", []):
                lines.append(f"- {item}")
        lines.append("")
    if gate_state:
        lines.extend(
            [
                "## Gate State",
                "",
                f"- status: `{gate_state.get('status', '')}`",
                f"- policy: `{gate_state.get('policy', '')}`",
                f"- token: `{gate_state.get('token', '') or 'n/a'}`",
                f"- summary: {gate_state.get('summary', '')}",
                "",
            ]
        )
    if delivery_outcomes:
        lines.extend(["## Delivery", ""])
        for item in delivery_outcomes:
            lines.append(
                f"- `{item.get('delivery_id', '')}` => `{item.get('status', '')}` | {item.get('summary', '')}"
            )
        lines.append("")
    lines.extend(
        [
            "## Phase Output",
            "",
            (
                "- 这次运行已经覆盖到当前最小的 Growth v1：控制真源读取、对象主表扫描、结构化结论、板面写回，以及按 delivery target 准备后续投递。"
                if _is_growth_executor(job)
                else (
                "- 这次运行已经覆盖到当前最小的 Phase 3：任务投影、官方资料抓取、结构化研究结论、板面写回，以及按 gate 决定是否外发。"
                if phase == "phase-3"
                else "- 这次运行已经覆盖到当前最小的 Phase 2：任务投影、受控 loop、执行简报、板面写回，以及按 gate 决定是否外发。"
                )
            ),
            (
                "- 当前产物已经对齐 Growth System 的对象模型、平台策略和下一步闭环接线点。"
                if _is_growth_executor(job)
                else (
                "- 当前产物已不再只是 delivery/gate 骨架，而是包含官方资料、能力边界和下一阶段接入路线的真实后台研究报告。"
                if phase == "phase-3"
                else "- 真实业务研究执行还没有展开；当前产物仍是“后台智能 job 的执行简报 + delivery/gate 骨架”。"
                )
            ),
            (
                "- 下一步按任务语义继续推进 Truth / Runtime / Platform / Productization / Closed Loop 的实现，并把真实动作写回对象主表。"
                if _is_growth_executor(job)
                else (
                "- 下一步是在审批通过后把研究摘要投递到指定入口，并继续补第二轮更细的资料与路线对比。"
                if phase == "phase-3"
                else "- 下一步是继续把这条后台研究任务推进到真实循环，并把外发审批后的继续执行链接上。"
                )
            ),
            "",
        ]
    )
    return "\n".join(lines)


def render_report(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    rounds: list[dict[str, Any]],
    phase: str = "phase-2",
    research_payload: dict[str, Any] | None = None,
    gate_state: dict[str, Any] | None = None,
    delivery_outcomes: list[dict[str, Any]] | None = None,
) -> str:
    is_growth = _is_growth_executor(job)
    headline = str((research_payload or {}).get("headline", "")).strip()
    lines = [
        (
            f"# Growth System 报告｜{job['task_id']}"
            if is_growth and research_payload
            else (f"# 后台研究报告｜{job['task_id']}" if research_payload else f"# 后台任务报告｜{job['task_id']}")
        ),
        "",
        f"- 事项：{job['task_item']}",
        f"- 项目：`{job['project_name']}`",
        f"- 运行：`{run_context['run_id']}`",
        "",
    ]
    if headline:
        lines.extend(["## 结论", "", headline, ""])
    else:
        lines.extend(
            [
                "## 结论",
                "",
                (
                    "本轮后台任务已完成最小研究/简报输出，并准备进入后续交付。"
                    if phase == "phase-2"
                    else "本轮后台任务已完成，当前没有形成更细的研究 headline。"
                ),
                "",
            ]
        )
    if research_payload:
        lines.extend(["## 关键输出" if is_growth else "## 可复刻能力", ""])
        for item in research_payload.get("replicable_capabilities", []):
            lines.append(f"- {item}")
        lines.extend(["", "## 当前缺口" if is_growth else "## 原生边界", ""])
        for item in research_payload.get("non_replicable_boundaries", []):
            lines.append(f"- {item}")
        lines.extend(["", "## 推荐动作" if is_growth else "## 建议路线", ""])
        for item in research_payload.get("recommended_route", []):
            lines.append(f"- {item}")
        if research_payload.get("open_questions"):
            lines.extend(["", "## 待补问题", ""])
            for item in research_payload.get("open_questions", []):
                lines.append(f"- {item}")
        if research_payload.get("collected_sources"):
            lines.extend(["", "## 关键来源", ""])
            for item in research_payload.get("collected_sources", []):
                title = str(item.get("title", "")).strip()
                uri = str(item.get("uri", "")).strip()
                if uri:
                    lines.append(f"- {title}：{uri}")
                else:
                    lines.append(f"- {title}")
    else:
        lines.extend(["## 验收结果", ""])
        if rounds:
            lines.append(f"- 当前阶段：`{phase}`")
            lines.append(f"- 最终判断：`{rounds[-1].get('acceptance_check', {}).get('result', '')}`")
        for item in job.get("acceptance_criteria", []):
            lines.append(f"- {item}")
    if gate_state and str(gate_state.get("summary", "")).strip():
        lines.extend(["", "## 当前状态", ""])
        lines.append(f"- {gate_state.get('summary', '')}")
    if delivery_outcomes:
        delivered = [item for item in delivery_outcomes if str(item.get("status", "")) == "delivered"]
        if delivered:
            lines.extend(["", "## 已完成交付", ""])
            for item in delivered:
                summary = str(item.get("summary", "")).strip()
                if summary:
                    lines.append(f"- {summary}")
    lines.extend(["", "## 下一步", ""])
    next_action = str(job.get("next_action", "")).strip()
    gate_status = str((gate_state or {}).get("status", "")).strip()
    if gate_status == "awaiting_gate":
        next_action = _gate_next_action(str(gate_state.get("token", "")))
    elif gate_status == "approved":
        next_action = _post_delivery_next_action(phase)
    lines.append(f"- {next_action or '待补充'}")
    lines.append("")
    return "\n".join(lines)


def write_report(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    rounds: list[dict[str, Any]],
    phase: str = "phase-2",
    research_payload: dict[str, Any] | None = None,
    gate_state: dict[str, Any] | None = None,
    delivery_outcomes: list[dict[str, Any]] | None = None,
    report_paths: dict[str, str] | None = None,
) -> dict[str, str]:
    root = job_root(job)
    root.mkdir(parents=True, exist_ok=True)
    paths = report_paths or {
        "archive_path": str(archive_report_path(job, run_context["run_id"])),
        "latest_path": str(latest_report_path(job)),
        "archive_ops_path": str(archive_ops_report_path(job, run_context["run_id"])),
        "latest_ops_path": str(latest_ops_report_path(job)),
    }
    archive_path = Path(paths["archive_path"])
    latest_path = Path(paths["latest_path"])
    archive_ops_path = Path(paths["archive_ops_path"])
    latest_ops_path = Path(paths["latest_ops_path"])
    text = render_report(
        job,
        run_context=run_context,
        rounds=rounds,
        phase=phase,
        research_payload=research_payload,
        gate_state=gate_state,
        delivery_outcomes=delivery_outcomes,
    )
    ops_text = render_ops_report(
        job,
        run_context=run_context,
        rounds=rounds,
        phase=phase,
        research_payload=research_payload,
        gate_state=gate_state,
        delivery_outcomes=delivery_outcomes,
    )
    archive_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    archive_ops_path.write_text(ops_text, encoding="utf-8")
    latest_ops_path.write_text(ops_text, encoding="utf-8")
    return {
        "archive_path": str(archive_path),
        "latest_path": str(latest_path),
        "archive_ops_path": str(archive_ops_path),
        "latest_ops_path": str(latest_ops_path),
    }


def append_gate_event(job: dict[str, Any], event: dict[str, Any]) -> None:
    codex_memory.append_ndjson(gates_path(job), event)


def latest_gate_event(job: dict[str, Any]) -> dict[str, Any]:
    path = gates_path(job)
    if not path.exists():
        return {}
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return {}


def _external_targets(job: dict[str, Any]) -> list[str]:
    return [
        str(item).strip()
        for item in job.get("delivery_targets", [])
        if str(item).strip().startswith("feishu:") or str(item).strip().startswith("weixin:")
    ]


def _stage_snapshot(
    scaffold: dict[str, Any],
    evaluation: dict[str, Any] | None = None,
) -> dict[str, str]:
    task_spec = dict(scaffold.get("task_spec", {}))
    current_stage = str((evaluation or {}).get("current_stage") or task_spec.get("stage") or "discover").strip() or "discover"
    next_stage = str((evaluation or {}).get("next_stage") or current_stage).strip() or current_stage
    decision = str((evaluation or {}).get("decision") or task_spec.get("last_decision") or "").strip()
    tier = "handoff" if decision == "done" or next_stage == "handoff" or current_stage == "handoff" else "progress"
    return {
        "current_stage": current_stage,
        "next_stage": next_stage,
        "decision": decision,
        "tier": tier,
    }


def _stage_transition_text(stage_snapshot: dict[str, Any] | None = None) -> str:
    snapshot = stage_snapshot or {}
    current_stage = str(snapshot.get("current_stage", "")).strip()
    next_stage = str(snapshot.get("next_stage", "")).strip()
    if current_stage and next_stage and current_stage != next_stage:
        return f"{current_stage} -> {next_stage}"
    return current_stage or next_stage or "n/a"


def _external_delivery_mode(
    job: dict[str, Any],
    *,
    external_targets: list[str],
    approved_gate: dict[str, Any],
) -> str:
    if not external_targets:
        return "none"
    if job.get("gate_policy") == "before_external_send" and not approved_gate:
        return "gate"
    return "delivery"


def _external_delivery_fingerprint(
    job: dict[str, Any],
    *,
    delivery_mode: str,
    stage_snapshot: dict[str, Any],
    external_targets: list[str],
    research_payload: dict[str, Any] | None = None,
    next_action: str = "",
) -> str:
    stage_tier = str(stage_snapshot.get("tier", "")).strip()
    payload = {
        "task_id": str(job.get("task_id", "")).strip(),
        "mode": delivery_mode,
        "tier": stage_tier,
        "stage": _stage_transition_text(stage_snapshot) if stage_tier == "handoff" else "progress",
        "headline": str((research_payload or {}).get("headline", "")).strip(),
        "route": list((research_payload or {}).get("recommended_route", []))[:1],
        "next_action": next_action.strip() if delivery_mode == "delivery" else "",
        "targets": sorted(str(item).strip() for item in external_targets if str(item).strip()),
    }
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(serialized.encode("utf-8")).hexdigest()


def _last_external_delivery(scaffold: dict[str, Any]) -> dict[str, Any]:
    task_spec = dict(scaffold.get("task_spec", {}))
    payload = task_spec.get("last_external_delivery", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _external_delivery_entry_key(delivery_id: str, target: str) -> str:
    return f"{delivery_id}:{target}"


def _required_external_delivery_keys(target: str, *, delivery_mode: str) -> set[str]:
    if delivery_mode == "none":
        return set()
    stripped = str(target).strip()
    if not stripped:
        return set()
    delivery_id = "feishu-notify" if stripped.startswith("feishu:") else "weixin-notify"
    keys = {_external_delivery_entry_key(delivery_id, stripped)}
    if delivery_mode == "delivery" and stripped.startswith("feishu:"):
        keys.add(_external_delivery_entry_key("feishu-doc", stripped))
    return keys


def _external_delivery_target(item: workspace_job_schema.JobDeliveryOutcome) -> str:
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    candidate = str(metadata.get("delivery_target", "")).strip()
    if candidate:
        return candidate
    targets = [str(target).strip() for target in item.targets if str(target).strip()]
    if len(targets) == 1 and targets[0].startswith(("feishu:", "weixin:")):
        return targets[0]
    return ""


def _suppressed_external_delivery_keys(
    scaffold: dict[str, Any],
    *,
    delivery_mode: str,
    fingerprint: str,
) -> set[str]:
    if delivery_mode == "none" or not fingerprint:
        return set()
    previous = _last_external_delivery(scaffold)
    if str(previous.get("fingerprint", "")).strip() != fingerprint:
        return set()
    suppressed_status = "delivered" if delivery_mode == "delivery" else "unknown"
    keys: set[str] = set()
    for entry in previous.get("entries", []):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status", "")).strip() != suppressed_status:
            continue
        delivery_id = str(entry.get("delivery_id", "")).strip()
        target = str(entry.get("target", "")).strip()
        if not delivery_id or not target:
            continue
        keys.add(_external_delivery_entry_key(delivery_id, target))
    return keys


def _external_delivery_state_from_outcomes(
    scaffold: dict[str, Any],
    delivery_outcomes: list[workspace_job_schema.JobDeliveryOutcome],
    *,
    delivery_mode: str,
    fingerprint: str,
    stage_snapshot: dict[str, Any],
    run_context: dict[str, str],
) -> dict[str, Any] | None:
    if not fingerprint or delivery_mode == "none":
        return None
    previous = _last_external_delivery(scaffold)
    entries: dict[str, dict[str, str]] = {}
    if str(previous.get("fingerprint", "")).strip() == fingerprint:
        for entry in previous.get("entries", []):
            if not isinstance(entry, dict):
                continue
            delivery_id = str(entry.get("delivery_id", "")).strip()
            target = str(entry.get("target", "")).strip()
            status = str(entry.get("status", "")).strip()
            if not delivery_id or not target or status not in {"delivered", "not-delivered", "unknown"}:
                continue
            entries[_external_delivery_entry_key(delivery_id, target)] = {
                "delivery_id": delivery_id,
                "target": target,
                "status": status,
            }
    for item in delivery_outcomes:
        delivery_id = str(item.delivery_id).strip()
        status = str(item.status).strip()
        if delivery_id not in {"feishu-doc", "feishu-notify", "weixin-notify"}:
            continue
        if status not in {"delivered", "not-delivered", "unknown"}:
            continue
        target = _external_delivery_target(item)
        if not target:
            continue
        entries[_external_delivery_entry_key(delivery_id, target)] = {
            "delivery_id": delivery_id,
            "target": target,
            "status": status,
        }
    if not entries:
        return None
    entry_list = sorted(entries.values(), key=lambda item: (item["target"], item["delivery_id"]))
    statuses = {item["status"] for item in entry_list}
    overall_status = "partial"
    if delivery_mode == "gate":
        if statuses == {"unknown"}:
            overall_status = "awaiting_gate"
        elif statuses == {"delivered"}:
            overall_status = "delivered"
    elif statuses == {"delivered"}:
        overall_status = "delivered"
    return {
        "fingerprint": fingerprint,
        "status": overall_status,
        "mode": delivery_mode,
        "tier": str(stage_snapshot.get("tier", "")).strip(),
        "stage": _stage_transition_text(stage_snapshot),
        "decision": str(stage_snapshot.get("decision", "")).strip(),
        "run_id": run_context["run_id"],
        "updated_at": iso_now_local(),
        "entries": entry_list,
    }


def _wake_block_reason(job: dict[str, Any], *, reason: str) -> str:
    if not wake_policy_allows(job, reason=reason):
        return "wake_policy_blocked"
    program = program_spec(job)
    if (
        str(program.get("scope_type", "project")).strip() == "workspace"
        and bool(program.get("approval_required", False))
        and str(program.get("approval_state", "pending")).strip() != "approved"
    ):
        return "workspace_scope_requires_approval"
    return ""


def _delivery_summary_text(
    job: dict[str, Any],
    *,
    report_paths: dict[str, str],
    run_context: dict[str, str],
    next_action: str,
    stage_snapshot: dict[str, Any] | None = None,
    research_payload: dict[str, Any] | None = None,
) -> str:
    lines = [
        f"后台任务更新：{job['task_id']}",
        f"事项：{job['task_item']}",
        f"运行：{run_context['run_id']}",
        f"阶段：{_stage_transition_text(stage_snapshot)}",
    ]
    if research_payload:
        lines.append(f"结论：{research_payload.get('headline', '')}")
        route = list(research_payload.get("recommended_route", []))
        if route:
            lines.append(f"路线：{route[0]}")
    lines.extend(
        [
            f"简报：{report_paths['latest_path']}",
            f"下一步：{next_action or job['next_action'] or '待补充'}",
        ]
    )
    return "\n".join(lines).strip()


def _delivery_doc_title(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    stage_snapshot: dict[str, Any] | None = None,
) -> str:
    task_id = str(job.get("task_id", "")).strip() or "background-job"
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"{task_id}｜{_stage_transition_text(stage_snapshot)}｜后台研究报告｜{timestamp}"


def _extract_nested_result_value(payload: Any, key: str) -> str:
    stack: list[Any] = [payload]
    visited: set[int] = set()
    while stack:
        current = stack.pop()
        if not isinstance(current, dict):
            continue
        marker = id(current)
        if marker in visited:
            continue
        visited.add(marker)
        value = current.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        nested = current.get("result")
        if isinstance(nested, dict):
            stack.append(nested)
    return ""


def _short_delivery_summary_text(
    job: dict[str, Any],
    *,
    next_action: str,
    stage_snapshot: dict[str, Any] | None = None,
    research_payload: dict[str, Any] | None = None,
    doc_url: str = "",
) -> str:
    lines = [f"后台结果：{job['task_id']}"]
    lines.append(f"阶段：{_stage_transition_text(stage_snapshot)}")
    if research_payload and str(research_payload.get("headline", "")).strip():
        lines.append(f"结论：{research_payload.get('headline', '')}")
    if doc_url:
        lines.append(f"报告：{doc_url}")
    else:
        lines.append("报告：已生成完整研究报告。")
    if next_action:
        lines.append(f"下一步：{next_action}")
    return "\n".join(lines).strip()


def deliver_feishu_target(
    target: str,
    text: str = "",
    *,
    msg_type: str = "text",
    card: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if msg_type == "interactive":
        return feishu_outbound_gateway.send_card(target, card or {}, text=text, kind="interactive_card")
    return feishu_outbound_gateway.send_message(target, text=text, msg_type=msg_type)


def create_feishu_doc_target(target: str, *, title: str, file_path: str) -> dict[str, Any]:
    return feishu_outbound_gateway.create_doc(target, title=title, file_path=file_path)


def deliver_weixin_target(_target: str, _text: str) -> dict[str, Any]:
    raise NotImplementedError("background weixin delivery is not implemented yet")


def _board_binding(job: dict[str, Any], run_id: str) -> dict[str, Any]:
    return {
        "project_name": job["project_name"],
        "session_id": run_id,
        "binding_scope": "topic" if job.get("source_type") == "topic" else "project",
        "binding_board_path": job["source_path"] if job.get("source_type") == "topic" else job["project_board_path"],
        "topic_name": _topic_name(job),
        "rollup_target": job["project_board_path"],
        "last_active_at": iso_now_local(),
    }


def writeback_job_progress(
    job: dict[str, Any],
    *,
    run_id: str,
    deliverable: str,
    next_action: str,
    status: str = "doing",
    trigger_followup_syncs: bool = True,
) -> list[str]:
    binding = _board_binding(job, run_id)
    changed_targets = codex_memory.sync_project_layers(
        binding,
        task_updates=[
            {
                "task_id": job["task_id"],
                "status": status,
                "deliverable": deliverable,
                "next_action": next_action,
                "updated_at": iso_now_local(),
            }
        ],
    )
    codex_memory.record_project_writeback(
        binding,
        source="background-job-executor",
        changed_targets=changed_targets,
        trigger_dashboard_sync=False,
    )
    if trigger_followup_syncs and not fixture_mode():
        codex_memory.trigger_retrieval_sync_once()
        codex_memory.trigger_dashboard_sync_once()
        codex_memory.trigger_feishu_projection_sync_once()
        codex_memory.trigger_growth_feishu_projection_sync_once()
        codex_memory.trigger_growth_operator_surface_report_once()
    return changed_targets


def _gate_next_action(token: str) -> str:
    return f"查看后台研究报告；如需外发，先批准 token `{token}`，再继续该 job 的外部投递。"


def _post_delivery_next_action(phase: str) -> str:
    if phase == "phase-3":
        return "查看后台研究报告，并继续补第二轮资料、替代路线细化与对外摘要。"
    return "查看后台简报，并继续进入真实研究执行与结果沉淀。"


def _approval_card_fallback_hint(token: str) -> str:
    return f"也可以回复：`/approve {token}` 或 `/deny {token}`"


def _build_gate_card_payload(
    job: dict[str, Any],
    *,
    token: str,
    run_context: dict[str, str],
    report_paths: dict[str, str],
    external_targets: list[str],
    research_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body_lines = [
        "**后台外发待确认**",
        f"任务：`{job['task_id']}`",
        f"运行：`{run_context['run_id']}`",
        f"目标：{', '.join(external_targets)}",
    ]
    if research_payload and str(research_payload.get("headline", "")).strip():
        body_lines.append(f"结论：{research_payload.get('headline', '')}")
    body_lines.append(f"简报：{report_paths['latest_path']}")
    body_lines.append("请直接点击下方按钮确认，或使用文本命令继续。")
    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "CoCo 授权确认"},
            "template": "orange",
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": "\n".join(body_lines)},
                {
                    "tag": "column_set",
                    "flex_mode": "none",
                    "horizontal_align": "left",
                    "columns": [
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                {
                                    "tag": "button",
                                    "text": {"tag": "plain_text", "content": "批准执行"},
                                    "type": "primary",
                                    "size": "medium",
                                    "value": {
                                        "callback_data": f"{APPROVAL_CARD_ACTION_PREFIX}:allow:{token}",
                                    },
                                }
                            ],
                        },
                        {
                            "tag": "column",
                            "width": "auto",
                            "elements": [
                                {
                                    "tag": "button",
                                    "text": {"tag": "plain_text", "content": "拒绝执行"},
                                    "type": "danger",
                                    "size": "medium",
                                    "value": {
                                        "callback_data": f"{APPROVAL_CARD_ACTION_PREFIX}:deny:{token}",
                                    },
                                }
                            ],
                        },
                    ],
                },
                {
                    "tag": "markdown",
                    "content": _approval_card_fallback_hint(token),
                    "text_size": "notation",
                },
            ]
        },
    }


def _record_gate_delivery_metadata(
    token: str,
    *,
    job: dict[str, Any],
    run_context: dict[str, str],
    target: str,
    delivery_result: dict[str, Any],
) -> None:
    current = runtime_state.fetch_approval_token(token)
    metadata = dict(current.get("metadata", {}) if isinstance(current.get("metadata"), dict) else {})
    resolved_target = str(delivery_result.get("target") or "").strip()
    receive_id_type = str(delivery_result.get("receive_id_type") or "").strip()
    if receive_id_type == "chat_id":
        metadata["chat_id"] = resolved_target
    elif receive_id_type == "open_id":
        metadata["open_id"] = resolved_target
    metadata.update(
        {
            "task_id": job["task_id"],
            "job_id": job["job_id"],
            "job_id_aliases": _legacy_job_ids(job),
            "run_id": run_context["run_id"],
            "delivery_target": target,
            "approval_delivery": "interactive_card",
            "approval_message_id": str(delivery_result.get("message_id") or "").strip(),
        }
    )
    runtime_state.upsert_approval_token(
        token=token,
        scope=EXTERNAL_DELIVERY_SCOPE,
        status="pending",
        project_name=job["project_name"],
        session_id=run_context["run_id"],
        metadata=metadata,
    )


def request_external_delivery_gate(
    job: dict[str, Any],
    *,
    run_context: dict[str, str],
    report_paths: dict[str, str],
    external_targets: list[str],
) -> dict[str, Any]:
    token = f"bgate-{uuid.uuid4().hex[:12]}"
    metadata = {
        "job_id": job["job_id"],
        "job_id_aliases": _legacy_job_ids(job),
        "task_id": job["task_id"],
        "run_id": run_context["run_id"],
        "requested_action": "external_delivery",
        "delivery_targets": external_targets,
        "report_path": report_paths["latest_path"],
    }
    runtime_state.upsert_approval_token(
        token=token,
        scope=EXTERNAL_DELIVERY_SCOPE,
        status="pending",
        project_name=job["project_name"],
        session_id=run_context["run_id"],
        metadata=metadata,
    )
    gate_state = {
        "status": "awaiting_gate",
        "policy": job["gate_policy"],
        "scope": EXTERNAL_DELIVERY_SCOPE,
        "token": token,
        "requested_targets": external_targets,
        "summary": "等待外部投递审批",
        "requested_at": iso_now_local(),
    }
    append_gate_event(
        job,
        {
            "ts": iso_now_local(),
            "action": "gate_requested",
            "job_id": job["job_id"],
            "run_id": run_context["run_id"],
            "gate_state": gate_state,
        },
    )
    return gate_state


def validate_external_delivery_approval(job: dict[str, Any], approval_token: str) -> dict[str, Any]:
    token = str(approval_token or "").strip()
    if not token:
        return {}
    item = runtime_state.fetch_approval_token(token)
    if str(item.get("scope") or "").strip() != EXTERNAL_DELIVERY_SCOPE:
        raise ValueError("approval token scope mismatch for background external delivery")
    if str(item.get("status") or "").strip() != "approved":
        raise ValueError("approval token is not approved")
    metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
    token_job_id = str(metadata.get("job_id") or "").strip()
    if not _job_id_matches(job, token_job_id):
        raise ValueError("approval token does not match current job")
    if token_job_id != str(job["job_id"]).strip():
        metadata["job_id"] = str(job["job_id"]).strip()
        metadata["job_id_aliases"] = _legacy_job_ids(job)
        item = runtime_state.upsert_approval_token(
            token=token,
            scope=str(item.get("scope", "")).strip(),
            status=str(item.get("status", "")).strip(),
            project_name=str(item.get("project_name", "")).strip(),
            session_id=str(item.get("session_id", "")).strip(),
            expires_at=str(item.get("expires_at", "")).strip(),
            metadata=metadata,
        )
    return item


def latest_run_record(job: dict[str, Any]) -> dict[str, Any]:
    path = history_path(job)
    if not path.exists():
        return {}
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        return {}
    try:
        return json.loads(lines[-1])
    except json.JSONDecodeError:
        return {}


def execute_projected_job(
    job: dict[str, Any],
    *,
    trigger_source: str = "",
    scheduled_for: str = "",
    automation_run_id: str = "",
    scheduler_id: str = "",
    approval_token: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    run_context = build_run_context(
        job,
        trigger_source=trigger_source,
        scheduled_for=scheduled_for,
        automation_run_id=automation_run_id,
        scheduler_id=scheduler_id,
    )
    _ensure_job_identity_compat(job, migrate_artifacts=not dry_run)
    scaffold = initialize_program_scaffold(job, run_context=run_context, persist=False)
    enforce_program_scope(scaffold["program"])
    if not dry_run:
        scaffold = initialize_program_scaffold(job, run_context=run_context, persist=True)
    runtime_job = {
        **job,
        "current_focus": scaffold["current_focus"],
        "program_spec": scaffold["program"],
    }
    scaffold["run_id"] = run_context["run_id"]
    loop_payload = run_agent_loop(runtime_job)
    rounds = list(loop_payload.get("rounds", []))
    phase = str(loop_payload.get("phase", "phase-2")).strip() or "phase-2"
    research_payload = dict(loop_payload.get("research_payload", {})) if isinstance(loop_payload.get("research_payload"), dict) else {}
    report_paths = {
        "archive_path": str(archive_report_path(job, run_context["run_id"])) if not dry_run else "",
        "latest_path": str(latest_report_path(job)),
        "archive_ops_path": str(archive_ops_report_path(job, run_context["run_id"])) if not dry_run else "",
        "latest_ops_path": str(latest_ops_report_path(job)),
    }
    research_paths = {
        "archive_path": str(archive_corpus_path(job, run_context["run_id"])) if not dry_run and research_payload else "",
        "latest_path": str(latest_corpus_path(job)) if research_payload else "",
    }
    execution_status_value = "ok" if not research_payload or research_payload.get("status") != "research-blocked" else "error"
    execution_summary_value = (
        f"growth system report prepared for {job['task_id']}"
        if _is_growth_executor(job)
        else (
            f"background research report prepared for {job['task_id']}"
            if phase == "phase-3"
            else f"background job brief prepared for {job['task_id']}"
        )
    )
    delivery_outcomes: list[workspace_job_schema.JobDeliveryOutcome] = []
    gate_state: dict[str, Any] = {}
    changed_targets: list[str] = []
    overall_ok = True
    board_writeback_ok = False
    initial_board_status = "doing"
    stage_snapshot = _stage_snapshot(scaffold)
    external_delivery_mode = "none"
    external_delivery_fingerprint = ""
    suppressed_external_delivery_keys: set[str] = set()
    external_delivery_state: dict[str, Any] | None = None
    if dry_run:
        delivery_outcomes.append(
            workspace_job_schema.JobDeliveryOutcome(
                delivery_id="report-file",
                status="not-requested",
                requested=False,
                summary="dry-run; report file not written",
                targets=[str(latest_report_path(job))],
            )
        )
    else:
        if research_payload:
            research_paths = write_research_payload(job, run_context=run_context, research_payload=research_payload)
        report_paths = write_report(
            job,
            run_context=run_context,
            rounds=rounds,
            phase=phase,
            research_payload=research_payload,
            report_paths=report_paths,
        )
        delivery_outcomes.append(
            workspace_job_schema.JobDeliveryOutcome(
                delivery_id="report-file",
                status="delivered",
                summary=(
                    "wrote background job research report"
                    if phase == "phase-3"
                    else "wrote background job phase-1 brief"
                ),
                targets=[
                    report_paths["archive_path"],
                    report_paths["latest_path"],
                    report_paths["archive_ops_path"],
                    report_paths["latest_ops_path"],
                    *([research_paths["archive_path"], research_paths["latest_path"]] if research_payload else []),
                ],
            )
        )
        try:
            approved_gate = validate_external_delivery_approval(job, approval_token)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        external_targets = _external_targets(job)
        preview_evaluation, _ = evaluate_program_iteration(
            job,
            scaffold=scaffold,
            execution_status=execution_status_value,
            delivery_status="not-requested",
            gate_state={},
        )
        stage_snapshot = _stage_snapshot(scaffold, preview_evaluation)
        external_delivery_mode = _external_delivery_mode(
            job,
            external_targets=external_targets,
            approved_gate=approved_gate,
        )
        if external_targets and job.get("gate_policy") == "before_external_send" and not approved_gate:
            gate_state = request_external_delivery_gate(
                job,
                run_context=run_context,
                report_paths=report_paths,
                external_targets=external_targets,
            )
            next_action = _gate_next_action(str(gate_state["token"]))
        else:
            next_action = _post_delivery_next_action(phase)
            if approved_gate:
                gate_state = {
                    "status": "approved",
                    "policy": job["gate_policy"],
                    "scope": EXTERNAL_DELIVERY_SCOPE,
                    "token": str(approved_gate.get("token", "")),
                    "requested_targets": external_targets,
                    "summary": "外部投递已获批准",
                    "approved_at": iso_now_local(),
                }
                append_gate_event(
                    job,
                    {
                        "ts": iso_now_local(),
                        "action": "gate_approved",
                        "job_id": job["job_id"],
                        "run_id": run_context["run_id"],
                        "gate_state": gate_state,
                    },
                )
            elif external_targets and job.get("gate_policy") == "none":
                gate_state = {
                    "status": "approved",
                    "policy": job["gate_policy"],
                    "scope": EXTERNAL_DELIVERY_SCOPE,
                    "token": "",
                    "requested_targets": external_targets,
                    "summary": "无需审批，直接外发",
                    "approved_at": iso_now_local(),
                }
                append_gate_event(
                    job,
                    {
                        "ts": iso_now_local(),
                        "action": "gate_bypassed",
                        "job_id": job["job_id"],
                        "run_id": run_context["run_id"],
                        "gate_state": gate_state,
                    },
                )
        external_delivery_fingerprint = _external_delivery_fingerprint(
            job,
            delivery_mode=external_delivery_mode,
            stage_snapshot=stage_snapshot,
            external_targets=external_targets,
            research_payload=research_payload,
            next_action=next_action,
        )
        suppressed_external_delivery_keys = _suppressed_external_delivery_keys(
            scaffold,
            delivery_mode=external_delivery_mode,
            fingerprint=external_delivery_fingerprint,
        )
        delivery_summary = _delivery_summary_text(
            job,
            report_paths=report_paths,
            run_context=run_context,
            next_action=next_action,
            stage_snapshot=stage_snapshot,
            research_payload=research_payload,
        )
        board_writeback_ok = True
        try:
            changed_targets = writeback_job_progress(
                job,
                run_id=run_context["run_id"],
                deliverable=report_paths["latest_path"],
                next_action=next_action,
                status=initial_board_status,
            )
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="board-writeback",
                    status="delivered",
                    summary=f"updated {len(changed_targets)} board targets",
                    targets=changed_targets,
                )
            )
        except Exception as exc:
            board_writeback_ok = False
            overall_ok = False
            gate_state = gate_state or {
                "status": "blocked",
                "policy": job["gate_policy"],
                "summary": "板面写回失败，已阻断外部投递",
            }
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="board-writeback",
                    status="not-delivered",
                    summary="board writeback failed",
                    error=f"{type(exc).__name__}: {exc}",
                    targets=[job["project_board_path"]],
                )
            )
        if (
            external_targets
            and board_writeback_ok
            and suppressed_external_delivery_keys
            and all(
                _required_external_delivery_keys(target, delivery_mode=external_delivery_mode).issubset(
                    suppressed_external_delivery_keys
                )
                for target in external_targets
            )
        ):
            for target in external_targets:
                delivery_id = "feishu-notify" if target.startswith("feishu:") else "weixin-notify"
                if (
                    target.startswith("feishu:")
                    and external_delivery_mode == "delivery"
                    and _external_delivery_entry_key("feishu-doc", target) in suppressed_external_delivery_keys
                ):
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id="feishu-doc",
                            status="not-requested",
                            requested=False,
                            summary=f"skipped duplicate Feishu doc delivery to {target}",
                            targets=[target],
                        )
                    )
                delivery_outcomes.append(
                    workspace_job_schema.JobDeliveryOutcome(
                        delivery_id=delivery_id,
                        status="not-requested",
                        requested=False,
                        summary=f"skipped duplicate external delivery to {target}",
                        targets=[target],
                        metadata={"fingerprint": external_delivery_fingerprint},
                    )
                )
        elif external_targets and board_writeback_ok and gate_state.get("status") == "awaiting_gate":
            for target in external_targets:
                delivery_id = "feishu-notify" if target.startswith("feishu:") else "weixin-notify"
                entry_key = _external_delivery_entry_key(delivery_id, target)
                if entry_key in suppressed_external_delivery_keys:
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id=delivery_id,
                            status="not-requested",
                            requested=False,
                            summary=f"skipped duplicate external delivery to {target}",
                            targets=[target],
                            metadata={"fingerprint": external_delivery_fingerprint},
                        )
                    )
                    continue
                if target.startswith("feishu:"):
                    try:
                        delivery_result = deliver_feishu_target(
                            target,
                            msg_type="interactive",
                            card=_build_gate_card_payload(
                                job,
                                token=str(gate_state.get("token", "")),
                                run_context=run_context,
                                report_paths=report_paths,
                                external_targets=external_targets,
                                research_payload=research_payload,
                            ),
                        )
                        _record_gate_delivery_metadata(
                            str(gate_state.get("token", "")),
                            job=job,
                            run_context=run_context,
                            target=target,
                            delivery_result=delivery_result,
                        )
                        delivery_outcomes.append(
                            workspace_job_schema.JobDeliveryOutcome(
                                delivery_id=delivery_id,
                                status="unknown",
                                summary=f"sent approval card to {target}; awaiting approval",
                                targets=[target],
                                metadata={
                                    "gate_token": gate_state.get("token", ""),
                                    "delivery_mode": "interactive_card",
                                    "delivery_target": target,
                                    "result": delivery_result,
                                },
                            )
                        )
                    except Exception as exc:
                        overall_ok = False
                        delivery_outcomes.append(
                            workspace_job_schema.JobDeliveryOutcome(
                                delivery_id=delivery_id,
                                status="not-delivered",
                                summary=f"failed to send approval card to {target}",
                                error=f"{type(exc).__name__}: {exc}",
                                targets=[target],
                                metadata={"gate_token": gate_state.get("token", ""), "delivery_target": target},
                            )
                        )
                else:
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id=delivery_id,
                            status="unknown",
                            summary=f"awaiting approval before delivering to {target}",
                            targets=[target],
                            metadata={"gate_token": gate_state.get("token", ""), "delivery_target": target},
                        )
                    )
        elif external_targets and board_writeback_ok and gate_state.get("status") == "approved":
            for target in external_targets:
                delivery_id = "feishu-notify" if target.startswith("feishu:") else "weixin-notify"
                doc_url = ""
                if target.startswith("feishu:"):
                    doc_entry_key = _external_delivery_entry_key("feishu-doc", target)
                    if doc_entry_key in suppressed_external_delivery_keys:
                        delivery_outcomes.append(
                            workspace_job_schema.JobDeliveryOutcome(
                                delivery_id="feishu-doc",
                                status="not-requested",
                                requested=False,
                                summary=f"skipped duplicate Feishu doc delivery to {target}",
                                targets=[target],
                            )
                        )
                    else:
                        try:
                            doc_result = create_feishu_doc_target(
                                target,
                                title=_delivery_doc_title(job, run_context=run_context, stage_snapshot=stage_snapshot),
                                file_path=report_paths["latest_path"],
                            )
                            doc_url = str(doc_result.get("url") or "").strip()
                            document_id = str(doc_result.get("document_id") or "").strip()
                            delivery_outcomes.append(
                                workspace_job_schema.JobDeliveryOutcome(
                                    delivery_id="feishu-doc",
                                    status="delivered",
                                    summary=f"mirrored report to Feishu doc for {target}",
                                    targets=[doc_url or document_id],
                                    metadata={
                                        "delivery_target": target,
                                        "result": doc_result,
                                        "url": doc_url,
                                        "document_id": document_id,
                                    },
                                )
                            )
                        except Exception as exc:
                            delivery_outcomes.append(
                                workspace_job_schema.JobDeliveryOutcome(
                                    delivery_id="feishu-doc",
                                    status="not-delivered",
                                    summary=f"failed to mirror report to Feishu doc for {target}",
                                    error=f"{type(exc).__name__}: {exc}",
                                    targets=[target],
                                    metadata={"delivery_target": target},
                                )
                            )
                entry_key = _external_delivery_entry_key(delivery_id, target)
                if entry_key in suppressed_external_delivery_keys:
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id=delivery_id,
                            status="not-requested",
                            requested=False,
                            summary=f"skipped duplicate external delivery to {target}",
                            targets=[target],
                            metadata={"fingerprint": external_delivery_fingerprint},
                        )
                    )
                    continue
                try:
                    final_summary = (
                        _short_delivery_summary_text(
                            job,
                            next_action=next_action,
                            stage_snapshot=stage_snapshot,
                            research_payload=research_payload,
                            doc_url=doc_url,
                        )
                        if target.startswith("feishu:")
                        else delivery_summary
                    )
                    if target.startswith("feishu:"):
                        result = deliver_feishu_target(target, final_summary)
                    else:
                        result = deliver_weixin_target(target, delivery_summary)
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id=delivery_id,
                            status="delivered",
                            summary=f"delivered update to {target}",
                            targets=[target],
                            metadata={"delivery_target": target, "result": result},
                        )
                    )
                except Exception as exc:
                    overall_ok = False
                    delivery_outcomes.append(
                        workspace_job_schema.JobDeliveryOutcome(
                            delivery_id=delivery_id,
                            status="not-delivered",
                            summary=f"delivery to {target} failed",
                            error=f"{type(exc).__name__}: {exc}",
                            targets=[target],
                            metadata={"delivery_target": target},
                        )
                    )
        elif external_targets and not board_writeback_ok:
            for target in external_targets:
                delivery_id = "feishu-notify" if target.startswith("feishu:") else "weixin-notify"
                delivery_outcomes.append(
                    workspace_job_schema.JobDeliveryOutcome(
                        delivery_id=delivery_id,
                        status="not-requested",
                        requested=False,
                        summary=f"skipped external delivery to {target} because board writeback failed",
                        targets=[target],
                    )
                )
        else:
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="feishu-notify",
                    status="not-requested",
                    requested=False,
                    summary="job has no Feishu target requiring delivery",
                )
            )
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="weixin-notify",
                    status="not-requested",
                    requested=False,
                    summary="job has no Weixin target requiring delivery",
                )
            )
        external_delivery_state = _external_delivery_state_from_outcomes(
            scaffold,
            delivery_outcomes,
            delivery_mode=external_delivery_mode,
            fingerprint=external_delivery_fingerprint,
            stage_snapshot=stage_snapshot,
            run_context=run_context,
        )
    if dry_run:
        delivery_outcomes.append(
            workspace_job_schema.JobDeliveryOutcome(
                delivery_id="board-writeback",
                status="not-requested",
                requested=False,
                summary="dry-run; board writeback not executed",
                targets=[job["project_board_path"]],
            )
        )
        delivery_outcomes.append(
            workspace_job_schema.JobDeliveryOutcome(
                delivery_id="feishu-notify",
                status="not-requested",
                requested=False,
                summary="dry-run; Feishu delivery not executed",
            )
        )
        delivery_outcomes.append(
            workspace_job_schema.JobDeliveryOutcome(
                delivery_id="weixin-notify",
                status="not-requested",
                requested=False,
                summary="dry-run; Weixin delivery not executed",
            )
        )
    execution_outcome = workspace_job_schema.JobExecutionOutcome(
        status=execution_status_value,
        summary=execution_summary_value,
        issue_count=len(research_payload.get("source_failures", [])) if research_payload else 0,
        metadata={
            "round_count": len(rounds),
            "executor_kind": job["executor_kind"],
            "dry_run": dry_run,
            "phase": phase,
        },
    )
    overall_ok = overall_ok and execution_status_value == "ok"
    if _is_growth_executor(job) and not dry_run:
        try:
            truth_writeback = write_growth_truth_records(
                job,
                run_context=run_context,
                finished_at=iso_now_local(),
                execution_status=execution_status_value,
                execution_summary=execution_summary_value,
                research_payload=research_payload,
                gate_state=gate_state,
            )
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="growth-truth-writeback",
                    status="delivered",
                    summary=(
                        f"wrote {truth_writeback['action_row_count']} Action row"
                        + (
                            f" and {truth_writeback['evidence_row_count']} Evidence row"
                            if truth_writeback["evidence_row_count"]
                            else ""
                        )
                    ),
                    targets=list(truth_writeback["targets"]),
                    metadata={
                        "action_status": truth_writeback["action_status"],
                        "action_row_count": truth_writeback["action_row_count"],
                        "evidence_row_count": truth_writeback["evidence_row_count"],
                    },
                )
            )
        except Exception as exc:
            targets = _growth_truth_targets_if_configured()
            if isinstance(exc, (FileNotFoundError, KeyError, RuntimeError)):
                delivery_outcomes.append(
                    workspace_job_schema.JobDeliveryOutcome(
                        delivery_id="growth-truth-writeback",
                        status="not-requested",
                        requested=False,
                        summary="growth truth tables are not configured in this workspace",
                        error=f"{type(exc).__name__}: {exc}",
                        targets=targets,
                    )
                )
            else:
                overall_ok = False
                delivery_outcomes.append(
                    workspace_job_schema.JobDeliveryOutcome(
                        delivery_id="growth-truth-writeback",
                        status="not-delivered",
                        summary="failed to write growth truth records",
                        error=f"{type(exc).__name__}: {exc}",
                        targets=targets,
                    )
                )
    delivery_payloads = [item.to_dict() for item in delivery_outcomes]
    delivery_status = workspace_job_schema.aggregate_delivery_status(delivery_payloads)
    overall_ok = overall_ok and all(item["status"] != "not-delivered" for item in delivery_payloads)
    program_evaluation, updated_subgoals = evaluate_program_iteration(
        job,
        scaffold=scaffold,
        execution_status=execution_status_value,
        delivery_status=delivery_status,
        gate_state=gate_state,
    )
    final_board_status = _final_board_status(program_evaluation)
    if not dry_run and board_writeback_ok and final_board_status != initial_board_status:
        try:
            finalized_targets = writeback_job_progress(
                job,
                run_id=run_context["run_id"],
                deliverable=report_paths["latest_path"],
                next_action=next_action,
                status=final_board_status,
                trigger_followup_syncs=False,
            )
            for target in finalized_targets:
                if target not in changed_targets:
                    changed_targets.append(target)
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="board-finalize-writeback",
                    status="delivered",
                    summary=f"updated {len(finalized_targets)} board targets to {final_board_status}",
                    targets=finalized_targets,
                )
            )
        except Exception as exc:
            overall_ok = False
            delivery_outcomes.append(
                workspace_job_schema.JobDeliveryOutcome(
                    delivery_id="board-finalize-writeback",
                    status="not-delivered",
                    summary=f"failed to finalize board status to {final_board_status}",
                    error=f"{type(exc).__name__}: {exc}",
                    targets=[job["project_board_path"]],
                )
            )
        delivery_payloads = [item.to_dict() for item in delivery_outcomes]
        delivery_status = workspace_job_schema.aggregate_delivery_status(delivery_payloads)
        overall_ok = overall_ok and all(item["status"] != "not-delivered" for item in delivery_payloads)
        program_evaluation, updated_subgoals = evaluate_program_iteration(
            job,
            scaffold=scaffold,
            execution_status=execution_status_value,
            delivery_status=delivery_status,
            gate_state=gate_state,
        )
    report_paths = write_report(
        job,
        run_context=run_context,
        rounds=rounds,
        phase=phase,
        research_payload=research_payload,
        gate_state=gate_state,
        delivery_outcomes=delivery_payloads,
        report_paths=report_paths,
    ) if not dry_run else report_paths
    updated_task_spec = finalize_program_iteration(
        job,
        scaffold=scaffold,
        evaluation=program_evaluation,
        updated_subgoals=updated_subgoals,
        execution_status=execution_status_value,
        execution_summary=execution_summary_value,
        next_action=next_action if not dry_run else (job.get("next_action", "") or "待补充"),
        report_path=report_paths["latest_path"],
        delivery_status=delivery_status,
        external_delivery_state=external_delivery_state,
    ) if not dry_run else dict(scaffold.get("task_spec", {}))
    finished_at = iso_now_local()
    run_record = workspace_job_schema.build_run_ledger_entry(
        job_id=job["job_id"],
        run_id=run_context["run_id"],
        started_at=run_context["started_at"],
        finished_at=finished_at,
        trigger_source=run_context["trigger_source"],
        scheduled_for=run_context["scheduled_for"],
        automation_run_id=run_context["automation_run_id"],
        scheduler_id=run_context["scheduler_id"],
        script_version=SCRIPT_VERSION,
        report_path=report_paths["archive_path"],
        latest_report_path=report_paths["latest_path"],
        writeback_targets=changed_targets,
        execution_outcome=execution_outcome,
        delivery_outcomes=delivery_outcomes,
        overall_ok=overall_ok,
        artifacts={
            "history_path": str(history_path(job)),
            "gates_path": str(gates_path(job)),
            "latest_report_path": report_paths["latest_path"],
            "latest_ops_report_path": report_paths["latest_ops_path"],
            "latest_research_path": research_paths["latest_path"],
            "task_spec_path": scaffold["paths"]["task_spec_path"],
            "acceptance_path": scaffold["paths"]["acceptance_path"],
            "progress_path": scaffold["paths"]["progress_path"],
            "latest_smoke_path": scaffold["paths"]["latest_smoke_path"],
            "task_id": job["task_id"],
        },
        gate_state=gate_state,
        metadata={
            "project_name": job["project_name"],
            "task_id": job["task_id"],
            "phase": phase,
            "program_id": scaffold["program"].get("program_id", ""),
            "scope_type": scaffold["program"].get("scope_type", ""),
            "scope_ref": scaffold["program"].get("scope_ref", ""),
            "approval_state": scaffold["program"].get("approval_state", ""),
            "stage": updated_task_spec.get("stage", scaffold["program"].get("stage", "")),
            "iteration": scaffold.get("iteration", 1),
            "current_focus": updated_task_spec.get("current_focus", scaffold.get("current_focus", "")),
            "program_evaluation": program_evaluation,
            "rounds": rounds,
            "research": research_payload,
            "task_spec": updated_task_spec,
        },
    )
    if not dry_run:
        codex_memory.append_ndjson(history_path(job), run_record)
    return {
        "ok": bool(run_record.get("ok")),
        "job": job,
        "run_context": run_context,
        "rounds": rounds,
        "run_record": run_record,
        "log_paths": report_paths,
        "research_paths": research_paths,
        "research_payload": research_payload,
        "gate_state": gate_state,
        "changed_targets": changed_targets,
        "dry_run": dry_run,
    }


def request_task_wake(
    project_name: str,
    task_id: str,
    *,
    reason: str,
    trigger_source: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job = board_job_projector.project_background_job(project_name, task_id)
    _ensure_job_identity_compat(job, migrate_artifacts=False)
    blocked_reason = _wake_block_reason(job, reason=reason)
    if blocked_reason:
        payload = {
            "accepted": False,
            "reason": blocked_reason,
            "job": job,
        }
        if blocked_reason == "wake_policy_blocked":
            payload["policy"] = dict(program_spec(job).get("wake_policy", {}))
        return payload
    payload = {
        "project_name": job["project_name"],
        "task_id": job["task_id"],
        "trigger_source": trigger_source or reason,
    }
    if metadata:
        payload.update(metadata)
    return workspace_wake_broker.request_wake(job["job_id"], reason=reason, metadata=payload)


def request_project_wake(
    project_name: str,
    *,
    reason: str,
    trigger_source: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    jobs, blocked = _policy_eligible_project_jobs(project_name, reason=reason)
    if not jobs:
        return {
            "accepted": False,
            "reason": "no_eligible_program",
            "project_name": codex_memory.canonical_project_name(project_name),
            "blocked_jobs": blocked,
        }
    selected = jobs[0]
    wake = request_task_wake(
        project_name,
        selected["task_id"],
        reason=reason,
        trigger_source=trigger_source,
        metadata=metadata,
    )
    wake["selected_task_id"] = selected["task_id"]
    wake["selected_job_id"] = selected["job_id"]
    wake["blocked_jobs"] = blocked
    return wake


def _policy_eligible_project_jobs(project_name: str, *, reason: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    jobs = board_job_projector.list_projectable_jobs(project_name)
    eligible: list[dict[str, Any]] = []
    blocked: list[dict[str, str]] = []
    for job in jobs:
        blocked_reason = _wake_block_reason(job, reason=reason)
        if blocked_reason:
            blocked.append({"task_id": job["task_id"], "reason": blocked_reason})
            continue
        eligible.append(job)
    return eligible, blocked


def _job_has_active_wake(job: dict[str, Any]) -> bool:
    _ensure_job_identity_compat(job, migrate_artifacts=False)
    status = workspace_wake_broker.job_status(job["job_id"])
    running = status.get("running") or {}
    if not running:
        return False
    claimed_at = workspace_wake_broker.parse_timestamp(str(running.get("claimed_at", "")))
    if claimed_at is None:
        return True
    age_seconds = (dt.datetime.now().astimezone() - claimed_at).total_seconds()
    return age_seconds < workspace_wake_broker.RUNNING_STALE_SECONDS


def run_requested_task(project_name: str, task_id: str, *, approval_token: str = "", dry_run: bool = False) -> dict[str, Any]:
    job = board_job_projector.project_background_job(project_name, task_id)
    _ensure_job_identity_compat(job, migrate_artifacts=False)
    claimed = workspace_wake_broker.claim_wake(job["job_id"])
    if not claimed.get("claimed"):
        return {
            "executed": False,
            "reason": claimed.get("reason", ""),
            "pending": claimed.get("pending", {}),
            "running": claimed.get("running", {}),
            "job": job,
        }
    wake = claimed["wake"]
    metadata = wake.get("metadata", {}) or {}
    try:
        payload = execute_projected_job(
            job,
            trigger_source=str(metadata.get("trigger_source") or wake.get("reason") or "wake_broker"),
            scheduled_for=str(metadata.get("scheduled_for") or ""),
            automation_run_id=str(metadata.get("automation_run_id") or ""),
            scheduler_id=str(metadata.get("scheduler_id") or ""),
            approval_token=approval_token,
            dry_run=dry_run,
        )
    except Exception as exc:
        workspace_wake_broker.complete_wake(
            job["job_id"],
            wake_id=str(wake.get("wake_id", "")),
            status="failed",
            result={"error": f"{type(exc).__name__}: {exc}"},
        )
        raise
    workspace_wake_broker.complete_wake(
        job["job_id"],
        wake_id=str(wake.get("wake_id", "")),
        status="succeeded" if payload.get("ok") else "failed",
        result={
            "ok": bool(payload.get("ok")),
            "run_id": str(payload.get("run_record", {}).get("run_id", "")),
        },
    )
    return {
        "executed": True,
        "wake": wake,
        "payload": payload,
    }


def run_requested_project_wake(
    project_name: str,
    *,
    reason: str,
    trigger_source: str = "",
    approval_token: str = "",
    dry_run: bool = False,
) -> dict[str, Any]:
    jobs, blocked = _policy_eligible_project_jobs(project_name, reason=reason)
    canonical_project = codex_memory.canonical_project_name(project_name)
    if not jobs:
        return {
            "executed": False,
            "reason": "no_eligible_program",
            "project_name": canonical_project,
            "blocked_jobs": blocked,
        }
    attempts: list[dict[str, str]] = []
    for job in jobs:
        if _job_has_active_wake(job):
            blocked.append({"task_id": job["task_id"], "reason": "wake_in_flight"})
            continue
        wake = request_task_wake(
            canonical_project,
            job["task_id"],
            reason=reason,
            trigger_source=trigger_source or reason,
        )
        wake["selected_task_id"] = job["task_id"]
        wake["selected_job_id"] = job["job_id"]
        wake["blocked_jobs"] = list(blocked)
        payload = run_requested_task(
            canonical_project,
            job["task_id"],
            approval_token=approval_token,
            dry_run=dry_run,
        )
        if payload.get("executed"):
            return {
                "executed": True,
                "wake": wake,
                "payload": payload,
                "selected_task_id": job["task_id"],
                "selected_job_id": job["job_id"],
                "blocked_jobs": blocked,
            }
        attempt_reason = str(payload.get("reason", "")).strip() or "claim_failed"
        attempts.append({"task_id": job["task_id"], "reason": attempt_reason})
        if attempt_reason in {"wake_in_flight", "no_pending"}:
            blocked.append({"task_id": job["task_id"], "reason": attempt_reason})
            continue
        return {
            "executed": False,
            "reason": attempt_reason,
            "project_name": canonical_project,
            "wake": wake,
            "payload": payload,
            "blocked_jobs": blocked,
            "attempts": attempts,
        }
    return {
        "executed": False,
        "reason": "no_claimable_program",
        "project_name": canonical_project,
        "blocked_jobs": blocked,
        "attempts": attempts,
    }


def cmd_list(args: argparse.Namespace) -> int:
    payload = {
        "project_name": args.project_name,
        "jobs": board_job_projector.list_projectable_jobs(args.project_name),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    payload = board_job_projector.project_background_job(args.project_name, args.task_id)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    job = board_job_projector.project_background_job(args.project_name, args.task_id)
    payload = execute_projected_job(
        job,
        trigger_source=args.trigger_source,
        approval_token=args.approval_token,
        dry_run=args.dry_run,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_wake(args: argparse.Namespace) -> int:
    payload = request_task_wake(
        args.project_name,
        args.task_id,
        reason=args.reason,
        trigger_source=args.trigger_source,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_wake_project(args: argparse.Namespace) -> int:
    payload = request_project_wake(
        args.project_name,
        reason=args.reason,
        trigger_source=args.trigger_source,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_run_requested(args: argparse.Namespace) -> int:
    payload = run_requested_task(
        args.project_name,
        args.task_id,
        approval_token=args.approval_token,
        dry_run=args.dry_run,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_run_project(args: argparse.Namespace) -> int:
    payload = run_requested_project_wake(
        args.project_name,
        reason=args.reason,
        trigger_source=args.trigger_source,
        approval_token=args.approval_token,
        dry_run=args.dry_run,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    job = board_job_projector.project_background_job(args.project_name, args.task_id)
    _ensure_job_identity_compat(job, migrate_artifacts=True)
    payload = {
        "job": job,
        "wake_broker": workspace_wake_broker.job_status(job["job_id"]),
        "last_run_record": latest_run_record(job),
        "last_gate_event": latest_gate_event(job),
        "history_path": str(history_path(job)),
        "gates_path": str(gates_path(job)),
        "latest_report_path": str(latest_report_path(job)),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run projected Codex Hub background jobs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    listing = subparsers.add_parser("list", help="List runnable projected jobs for a project.")
    listing.add_argument("--project-name", required=True)
    listing.set_defaults(func=cmd_list)

    show = subparsers.add_parser("show", help="Show a projected job.")
    show.add_argument("--project-name", required=True)
    show.add_argument("--task-id", required=True)
    show.set_defaults(func=cmd_show)

    run = subparsers.add_parser("run", help="Execute a projected job directly.")
    run.add_argument("--project-name", required=True)
    run.add_argument("--task-id", required=True)
    run.add_argument("--trigger-source", default="manual_cli")
    run.add_argument("--approval-token", default="")
    run.add_argument("--dry-run", action="store_true")
    run.set_defaults(func=cmd_run)

    wake = subparsers.add_parser("wake", help="Queue a wake request for a projected job.")
    wake.add_argument("--project-name", required=True)
    wake.add_argument("--task-id", required=True)
    wake.add_argument("--reason", default="manual_wake")
    wake.add_argument("--trigger-source", default="manual_cli")
    wake.set_defaults(func=cmd_wake)

    wake_project = subparsers.add_parser("wake-project", help="Queue a wake request for the highest-priority program in a project.")
    wake_project.add_argument("--project-name", required=True)
    wake_project.add_argument("--reason", default="manual_wake")
    wake_project.add_argument("--trigger-source", default="manual_cli")
    wake_project.set_defaults(func=cmd_wake_project)

    run_requested = subparsers.add_parser("run-requested", help="Claim and execute a queued wake for a job.")
    run_requested.add_argument("--project-name", required=True)
    run_requested.add_argument("--task-id", required=True)
    run_requested.add_argument("--approval-token", default="")
    run_requested.add_argument("--dry-run", action="store_true")
    run_requested.set_defaults(func=cmd_run_requested)

    run_project = subparsers.add_parser(
        "run-project",
        help="Select, claim, and execute the highest-priority wake-eligible program for a project.",
    )
    run_project.add_argument("--project-name", required=True)
    run_project.add_argument("--reason", default="manual_wake")
    run_project.add_argument("--trigger-source", default="manual_cli")
    run_project.add_argument("--approval-token", default="")
    run_project.add_argument("--dry-run", action="store_true")
    run_project.set_defaults(func=cmd_run_project)

    status = subparsers.add_parser("status", help="Show wake and run status for a projected job.")
    status.add_argument("--project-name", required=True)
    status.add_argument("--task-id", required=True)
    status.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

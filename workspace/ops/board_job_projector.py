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

# Public Codex Hub ships a generic sample task catalog so downstream users can
# experience long-running programs immediately, without depending on the
# author's private board ids or private report paths.
TASK_JOB_SPECS: dict[str, dict[str, Any]] = {
    "SP-EXEC-01": {
        "job_slug": "sample-program-loop",
        "executor_kind": "research_brief",
        "automation_mode": "background_assist",
        "allowed_actions": ["read", "write_report", "write_board"],
        "delivery_targets": ["board", "report"],
        "gate_policy": "none",
        "max_rounds": 3,
        "time_budget_minutes": 20,
        "acceptance_criteria": [
            "Freeze the project-scoped program contract for the sample task.",
            "Capture a visible stage transition and handoff bundle for the sample task.",
            "Write back the next-step summary so users can resume the loop cleanly.",
        ],
        "analysis_focus": [
            "Treat the sample task as a real multi-stage program instead of a one-shot brief.",
            "Show how wake/loop/handoff artifacts move together inside a project scope.",
        ],
    },
    "SP-FS-01": {
        "job_slug": "sample-feishu-followup",
        "executor_kind": "research_brief",
        "automation_mode": "background_assist",
        "allowed_actions": ["read", "write_report", "write_board"],
        "delivery_targets": ["board", "report"],
        "gate_policy": "before_external_send",
        "max_rounds": 3,
        "time_budget_minutes": 20,
        "acceptance_criteria": [
            "Gather official Feishu references needed for a scoped follow-up.",
            "Separate native Feishu capabilities from Codex Hub orchestration rules.",
            "Produce a concrete next-step plan that can be resumed in a later wake.",
        ],
        "analysis_focus": [
            "Demonstrate how a Feishu-facing research task becomes a resumable project program.",
            "Keep delivery gated while still preserving the shared program harness flow.",
        ],
    },
}


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(REPO_ROOT)))


def control_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", str(REPO_ROOT / "control")))


@lru_cache(maxsize=1)
def load_task_family_registry() -> dict[str, dict[str, Any]]:
    path = control_root() / "task_families.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    families = payload.get("families", {})
    if isinstance(families, dict):
        normalized: dict[str, dict[str, Any]] = {}
        for family_id, item in families.items():
            if isinstance(item, dict):
                normalized[str(family_id).strip() or str(item.get("family_id", "")).strip()] = dict(item)
        return {key: value for key, value in normalized.items() if key}
    return {}


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
        executor_kind = str(workflow.get("executor_kind", "")).strip() or "growth_signal_scan"
        output_objects = [str(name).strip() for name in workflow.get("output_objects", []) if str(name).strip()]
        if not output_objects and executor_kind.startswith("growth_"):
            output_objects = [name for name in ("Action", "Evidence") if table_map.get(name, "")]
        projected_objects: list[str] = []
        for name in [*input_objects, *output_objects]:
            if name and name not in projected_objects:
                projected_objects.append(name)
        projected[str(task_id).strip()] = {
            "job_slug": str(item.get("job_slug", "")).strip() or str(task_id).strip().lower(),
            "executor_kind": executor_kind,
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
            "object_tables": {name: table_map.get(name, "") for name in projected_objects if table_map.get(name, "")},
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


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _safe_template_context(context: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in context.items()}


def _render_template(value: Any, context: dict[str, Any]) -> str:
    text = str(value or "")
    try:
        return text.format_map(_safe_template_context(context))
    except Exception:
        return text


def _coerce_family_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _family_program_defaults(family: dict[str, Any], executor_kind: str = "implementation_loop") -> dict[str, Any]:
    defaults = _coerce_family_dict(family.get("program_defaults"))
    template = _default_spec_template(str(defaults.get("executor_kind", executor_kind)).strip() or executor_kind)
    merged = {**template, **defaults}
    merged["allowed_actions"] = _coerce_string_list(merged.get("allowed_actions")) or list(template["allowed_actions"])
    merged["delivery_targets"] = _coerce_string_list(merged.get("delivery_targets")) or list(template["delivery_targets"])
    merged["executor_kind"] = str(merged.get("executor_kind", template["executor_kind"])).strip() or template["executor_kind"]
    merged["automation_mode"] = str(merged.get("automation_mode", template["automation_mode"])).strip() or template["automation_mode"]
    merged["gate_policy"] = str(merged.get("gate_policy", template["gate_policy"])).strip() or template["gate_policy"]
    merged["max_rounds"] = int(merged.get("max_rounds", template["max_rounds"]) or template["max_rounds"])
    merged["time_budget_minutes"] = int(
        merged.get("time_budget_minutes", template["time_budget_minutes"]) or template["time_budget_minutes"]
    )
    merged["subgoal_schema_version"] = int(
        merged.get("subgoal_schema_version", template.get("subgoal_schema_version", 1))
        or template.get("subgoal_schema_version", 1)
    )
    return merged


def _default_spec_template(executor_kind: str) -> dict[str, Any]:
    normalized = str(executor_kind or "").strip() or "research_brief"
    if normalized == "implementation_loop":
        return {
            "executor_kind": "implementation_loop",
            "automation_mode": "background_assist",
            "allowed_actions": ["read", "write_code", "run_tests", "write_report", "write_board"],
            "delivery_targets": ["board", "report"],
            "gate_policy": "none",
            "max_rounds": 6,
            "time_budget_minutes": 30,
            "subgoal_schema_version": 1,
            "implementation_tracks": [],
        }
    return {
        "executor_kind": "research_brief",
        "automation_mode": "background_assist",
        "allowed_actions": ["read", "write_report", "write_board"],
        "delivery_targets": ["board", "report"],
        "gate_policy": "none",
        "max_rounds": 3,
        "time_budget_minutes": 20,
    }


def _task_scope_label(ref: dict[str, Any]) -> str:
    return str(ref["row"].get("范围", "")).strip() or "任务推进"


def _task_source_label(ref: dict[str, Any]) -> str:
    return str(ref["row"].get("来源", "")).strip() or str(ref.get("source_type", "project")).strip() or "project"


def _task_text_corpus(ref: dict[str, Any]) -> str:
    row = ref["row"]
    parts = [
        str(row.get("ID", "")).strip(),
        _task_scope_label(ref),
        str(row.get("事项", "")).strip(),
        str(row.get("下一步", "")).strip(),
        _task_source_label(ref),
    ]
    return "\n".join(item for item in parts if item)


def _is_manual_long_task(ref: dict[str, Any]) -> bool:
    source_label = _task_source_label(ref)
    scope = _task_scope_label(ref)
    return source_label == "manual_long_task" or scope in {"长任务", "讨论任务"}


def _task_family_context(ref: dict[str, Any], *, artifacts_root: Path) -> dict[str, Any]:
    row = ref["row"]
    return {
        "workspace_root": workspace_root(),
        "project_name": str(ref.get("project_name", "")).strip(),
        "task_id": str(row.get("ID", "")).strip(),
        "task_item": str(row.get("事项", "")).strip(),
        "next_action": str(row.get("下一步", "")).strip(),
        "scope": _task_scope_label(ref),
        "source": _task_source_label(ref),
        "project_board_path": str(ref.get("project_board_path", "")).strip(),
        "source_path": str(ref.get("source_path", "")).strip(),
        "artifacts_root": artifacts_root,
        "latest_report_path": artifacts_root / "latest.md",
        "latest_smoke_path": artifacts_root / "latest-smoke.md",
    }


def _family_matches(ref: dict[str, Any], family: dict[str, Any]) -> bool:
    rules = _coerce_family_dict(family.get("match_rules"))
    sources = _coerce_string_list(rules.get("sources"))
    scopes = _coerce_string_list(rules.get("scopes"))
    keywords_any = [item.lower() for item in _coerce_string_list(rules.get("keywords_any"))]
    keywords_all = [item.lower() for item in _coerce_string_list(rules.get("keywords_all"))]
    regex_any = _coerce_string_list(rules.get("regex_any"))

    source_label = _task_source_label(ref)
    scope_label = _task_scope_label(ref)
    text = _task_text_corpus(ref).lower()

    if sources and source_label not in sources:
        return False
    if scopes and scope_label not in scopes:
        return False
    if keywords_all and not all(token in text for token in keywords_all):
        return False
    if keywords_any and not any(token in text for token in keywords_any):
        return False
    if regex_any and not any(re.search(pattern, _task_text_corpus(ref), re.I) for pattern in regex_any):
        return False
    return bool(sources or scopes or keywords_any or keywords_all or regex_any)


def _path_text(path: Path | str) -> str:
    return str(path if isinstance(path, Path) else Path(path))


def _generic_report_verify_command(*, report_path: Path, smoke_path: Path, task_id: str, summary: str) -> str:
    return (
        "python3 - <<'PY'\n"
        "from pathlib import Path\n"
        f"report = Path({report_path.as_posix()!r})\n"
        f"smoke = Path({smoke_path.as_posix()!r})\n"
        f"task_id = {task_id!r}\n"
        f"summary = {summary!r}\n"
        "assert report.exists(), 'missing latest report'\n"
        "report_text = report.read_text(encoding='utf-8', errors='ignore')\n"
        "assert task_id in report_text, 'latest report missing task id'\n"
        "assert summary in report_text, 'latest report missing current focus summary'\n"
        "assert smoke.exists(), 'missing latest smoke'\n"
        "smoke_text = smoke.read_text(encoding='utf-8', errors='ignore')\n"
        "assert task_id in smoke_text, 'latest smoke missing task id'\n"
        "PY"
    )


def _generic_codex_exec_packet(
    *,
    packet_template: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    target_lines = str(context.get("target_files_joined", "")).strip() or "- 先自行定位最小必要改动面"
    action_lines = str(context.get("execute_actions_joined", "")).strip() or "- 围绕当前 track 推进最小必要改动"
    boundary_lines = str(context.get("boundary_notes_joined", "")).strip() or "- 保持板面、报告与真源一致"
    prompt = _render_template(
        packet_template.get(
            "prompt_template",
            (
                "继续推进任务 `{task_id}`。\n"
                "当前 track：{summary}\n"
                "任务目标：{task_item}\n"
                "当前下一步：{next_action}\n"
                "目标文件：\n"
                "{target_lines}\n"
                "本轮动作：\n"
                "{action_lines}\n"
                "边界要求：\n"
                "{boundary_lines}\n"
                "要求：完成最小必要代码/测试/报告更新，并保留可验证的下一步。"
            ),
        ),
        {
            **context,
            "target_lines": target_lines,
            "action_lines": action_lines,
            "boundary_lines": boundary_lines,
        },
    )
    return {
        "label": _render_template(packet_template.get("label_template", "{subgoal_id}"), context),
        "kind": str(packet_template.get("kind", "codex_exec")).strip() or "codex_exec",
        "project_name": _render_template(packet_template.get("project_name", "{project_name}"), context),
        "source": str(packet_template.get("source", "background_job_harness")).strip() or "background_job_harness",
        "execution_profile": str(packet_template.get("execution_profile", "background-job")).strip() or "background-job",
        "reasoning_effort": str(packet_template.get("reasoning_effort", "medium")).strip() or "medium",
        "timeout_seconds": int(packet_template.get("timeout_seconds", 900) or 900),
        "no_auto_resume": bool(packet_template.get("no_auto_resume", True)),
        "prompt": prompt,
    }


def _materialize_generic_tracks(
    ref: dict[str, Any],
    *,
    family: str,
    artifacts_root: Path,
) -> list[dict[str, Any]]:
    registry = load_task_family_registry()
    family_payload = registry.get(family, {})
    if not isinstance(family_payload, dict):
        return []

    task_id = str(ref["row"].get("ID", "")).strip()
    report_path = artifacts_root / "latest.md"
    smoke_path = artifacts_root / "latest-smoke.md"
    base_context = _task_family_context(ref, artifacts_root=artifacts_root)
    rendered_tracks: list[dict[str, Any]] = []

    for index, template in enumerate(family_payload.get("track_templates", []), start=1):
        if not isinstance(template, dict):
            continue
        subgoal_id = str(template.get("subgoal_id", "")).strip() or f"goal-{index}"
        summary = _render_template(template.get("summary_template", subgoal_id), {**base_context, "subgoal_id": subgoal_id})
        track_context = {
            **base_context,
            "family_id": family,
            "subgoal_id": subgoal_id,
            "summary": summary,
        }
        current_truth = [
            _render_template(item, track_context)
            for item in template.get("current_truth_templates", [])
            if _render_template(item, track_context).strip()
        ]
        target_files = [
            _render_template(item, track_context)
            for item in template.get("target_files_templates", [])
            if _render_template(item, track_context).strip()
        ]
        execute_actions = [
            _render_template(item, track_context)
            for item in template.get("execute_actions", [])
            if _render_template(item, track_context).strip()
        ]
        adapt_actions = [
            _render_template(item, track_context)
            for item in template.get("adapt_actions", [])
            if _render_template(item, track_context).strip()
        ]
        boundary_notes = [
            _render_template(item, track_context)
            for item in template.get("boundary_notes", [])
            if _render_template(item, track_context).strip()
        ]
        packet_context = {
            **track_context,
            "target_files_joined": "\n".join(f"- {item}" for item in target_files) or "- 先自行定位最小必要改动面",
            "execute_actions_joined": "\n".join(f"- {item}" for item in execute_actions) or "- 围绕当前 track 推进最小必要改动",
            "boundary_notes_joined": "\n".join(f"- {item}" for item in boundary_notes) or "- 保持板面、报告与真源一致",
        }
        packet_template = _coerce_family_dict(template.get("execution_packet_template"))
        execution_packets = [
            _generic_codex_exec_packet(
                packet_template=packet_template,
                context=packet_context,
            )
        ] if packet_template else []

        verify_contract = _coerce_family_dict(template.get("verify_contract"))
        verify_commands: list[dict[str, Any]] = []
        for command in verify_contract.get("pytest_commands", []):
            if not isinstance(command, dict):
                continue
            command_text = _render_template(command.get("command_template", ""), packet_context).strip()
            if not command_text:
                continue
            verify_commands.append(
                {
                    "label": _render_template(command.get("label", f"verify-{subgoal_id}"), packet_context).strip() or f"verify-{subgoal_id}",
                    "command": command_text,
                    "timeout_seconds": int(command.get("timeout_seconds", 180) or 180),
                }
            )
        if bool(verify_contract.get("include_report_verifier", False)):
            report_summary = _render_template(
                verify_contract.get("report_summary_template", "{summary}"),
                packet_context,
            ).strip() or summary
            verify_commands.append(
                {
                    "label": f"verify-report-{subgoal_id}",
                    "command": _generic_report_verify_command(
                        report_path=report_path,
                        smoke_path=smoke_path,
                        task_id=task_id,
                        summary=report_summary,
                    ),
                    "timeout_seconds": 60,
                }
            )
        rendered_tracks.append(
            {
                "subgoal_id": subgoal_id,
                "summary": summary,
                "current_truth": current_truth,
                "target_files": target_files,
                "execute_actions": execute_actions,
                "execution_packets": execution_packets,
                "verify_commands": verify_commands,
                "adapt_actions": adapt_actions,
            }
        )
    return rendered_tracks


def _pointer_task_spec(project_name: str, ref: dict[str, Any]) -> dict[str, Any] | None:
    pointer = Path(_task_pointer_path(project_name, ref))
    if not pointer.exists() or pointer.suffix.lower() != ".md":
        return None
    text = codex_memory.read_text(pointer)
    frontmatter: dict[str, Any] = {}
    match = re.match(r"^---\n(.*?)\n---\n?(.*)$", text, re.S)
    if match:
        try:
            parsed = yaml.safe_load(match.group(1)) or {}
        except yaml.YAMLError:
            parsed = {}
        if isinstance(parsed, dict):
            frontmatter = parsed
    if not frontmatter:
        frontmatter, _body = codex_memory.parse_frontmatter(text)
    harness = frontmatter.get("harness")
    if not isinstance(harness, dict):
        return None
    if harness.get("enabled") is False:
        return None
    explicit_keys = {str(key).strip() for key in harness.keys()} - {"family", "enabled"}
    if not explicit_keys:
        return None

    executor_kind = str(harness.get("executor_kind", "")).strip() or "research_brief"
    spec = _default_spec_template(executor_kind)
    spec["job_slug"] = str(harness.get("job_slug", "")).strip() or str(ref.get("task_id", "")).strip().lower()
    spec["acceptance_criteria"] = _coerce_string_list(harness.get("acceptance_criteria")) or [
        str(ref["row"].get("事项", "")).strip()
    ]
    spec["analysis_focus"] = _coerce_string_list(harness.get("analysis_focus"))
    spec["research_sources"] = [dict(item) for item in harness.get("research_sources", []) if isinstance(item, dict)]
    spec["implementation_tracks"] = [dict(item) for item in harness.get("implementation_tracks", []) if isinstance(item, dict)]
    spec["allowed_actions"] = _coerce_string_list(harness.get("allowed_actions")) or list(spec["allowed_actions"])
    spec["delivery_targets"] = _coerce_string_list(harness.get("delivery_targets")) or list(spec["delivery_targets"])
    spec["automation_mode"] = str(harness.get("automation_mode", "")).strip() or str(spec["automation_mode"])
    spec["gate_policy"] = str(harness.get("gate_policy", "")).strip() or str(spec["gate_policy"])
    spec["max_rounds"] = int(harness.get("max_rounds", spec["max_rounds"]) or spec["max_rounds"])
    spec["time_budget_minutes"] = int(
        harness.get("time_budget_minutes", spec["time_budget_minutes"]) or spec["time_budget_minutes"]
    )
    spec["subgoal_schema_version"] = int(
        harness.get("subgoal_schema_version", spec.get("subgoal_schema_version", 1))
        or spec.get("subgoal_schema_version", 1)
    )
    if isinstance(harness.get("wake_policy"), dict):
        spec["wake_policy"] = dict(harness["wake_policy"])
    if isinstance(harness.get("loop_policy"), dict):
        spec["loop_policy"] = dict(harness["loop_policy"])
    if isinstance(harness.get("stage_plan"), list):
        spec["stage_plan"] = [str(item).strip() for item in harness["stage_plan"] if str(item).strip()]
    if harness.get("initial_stage"):
        spec["initial_stage"] = str(harness.get("initial_stage", "")).strip()
    return spec


def _pointer_task_family(ref: dict[str, Any]) -> str:
    pointer = Path(_task_pointer_path(str(ref.get("project_name", "")).strip(), ref))
    if not pointer.exists() or pointer.suffix.lower() != ".md":
        return ""
    text = codex_memory.read_text(pointer)
    frontmatter: dict[str, Any] = {}
    match = re.match(r"^---\n(.*?)\n---\n?(.*)$", text, re.S)
    if match:
        try:
            parsed = yaml.safe_load(match.group(1)) or {}
        except yaml.YAMLError:
            parsed = {}
        if isinstance(parsed, dict):
            frontmatter = parsed
    if not frontmatter:
        frontmatter, _body = codex_memory.parse_frontmatter(text)
    harness = frontmatter.get("harness")
    if not isinstance(harness, dict):
        return ""
    return str(harness.get("family", "")).strip()


def _registry_family_spec(
    ref: dict[str, Any],
    *,
    family_id: str,
    family_source: str,
    family_resolution_reason: str,
) -> dict[str, Any] | None:
    registry = load_task_family_registry()
    family = registry.get(family_id)
    if not isinstance(family, dict):
        return None
    defaults = _family_program_defaults(family, executor_kind="implementation_loop")
    row = ref["row"]
    acceptance = [f"围绕任务 `{str(row.get('ID', '')).strip()}` 推进 `{str(row.get('事项', '')).strip()}` 到可验证状态。"]
    next_action = str(row.get("下一步", "")).strip()
    deliverable = str(row.get("交付物", "")).strip()
    if deliverable:
        acceptance.append(f"产出或更新交付物：{deliverable}。")
    if next_action:
        acceptance.append(f"完成当前下一步：{next_action}")
    return {
        "job_slug": f"auto-{_identity_segment(str(row.get('ID', '')).strip())}",
        **defaults,
        "acceptance_criteria": acceptance,
        "analysis_focus": [
            f"按 family `{family_id}` 实例化任务 `{str(row.get('ID', '')).strip()}` 的 program blueprint。",
            f"围绕范围 `{_task_scope_label(ref)}` 与当前下一步推进 `{str(row.get('事项', '')).strip()}`，并保持板面写回可解释。",
        ],
        "task_family": family_id,
        "family_source": family_source,
        "family_resolution_reason": family_resolution_reason,
        "stop_conditions": _coerce_string_list(family.get("stop_conditions")),
        "family_metadata_defaults": _coerce_family_dict(family.get("metadata_defaults")),
    }


def _generic_task_family(ref: dict[str, Any]) -> str:
    family_id, _family_source, _resolution_reason = _registry_matched_family(ref)
    if family_id:
        return family_id
    return "generic_manual_long_task" if _is_manual_long_task(ref) else ""


def _registry_matched_family(ref: dict[str, Any]) -> tuple[str, str, str]:
    registry = load_task_family_registry()
    for family_id, family in registry.items():
        if family_id == "generic_manual_long_task":
            continue
        if _family_matches(ref, family):
            return family_id, "registry", "match_rules"
    if _is_manual_long_task(ref) and "generic_manual_long_task" in registry:
        return "generic_manual_long_task", "registry_fallback", "manual_long_task_fallback"
    return "", "", ""


def _generic_job_spec(ref: dict[str, Any]) -> dict[str, Any]:
    row = ref["row"]
    task_id = str(row.get("ID", "")).strip()
    task_status = codex_memory.normalize_task_status(row.get("状态", "todo"))
    scope = _task_scope_label(ref)
    task_item = str(row.get("事项", "")).strip() or task_id
    next_action = str(row.get("下一步", "")).strip()
    deliverable = str(row.get("交付物", "")).strip()
    source_label = _task_source_label(ref)
    manual_long_task = _is_manual_long_task(ref)
    family = _generic_task_family(ref)
    if task_status == "doing" or manual_long_task:
        acceptance = [
            f"围绕任务 `{task_id}` 推进 `{task_item}` 到可验证状态。",
        ]
        if deliverable:
            acceptance.append(f"产出或更新交付物：{deliverable}。")
        if next_action:
            acceptance.append(f"完成当前下一步：{next_action}")
        return {
            "job_slug": f"auto-{_identity_segment(task_id)}",
            "executor_kind": "implementation_loop",
            "automation_mode": "background_assist",
            "allowed_actions": ["read", "write_code", "run_tests", "write_report", "write_board"],
            "delivery_targets": ["board", "report"],
            "gate_policy": "none",
            "max_rounds": 4,
            "time_budget_minutes": 20,
            "subgoal_schema_version": 1,
            "acceptance_criteria": acceptance,
            "analysis_focus": [
                f"按板面事实自动接住 {source_label} 里的 `{task_id}`，不再依赖显式 TASK_JOB_SPECS。",
                f"围绕范围 `{scope}` 与当前下一步推进 `{task_item}`，并保持板面写回可解释。",
            ],
            "implementation_tracks": [],
            "task_family": family,
            "family_source": "heuristic_fallback",
            "family_resolution_reason": "generic_manual_long_task",
            "family_metadata_defaults": {},
            "stop_conditions": [],
        }
    acceptance = [f"澄清并收口任务 `{task_id}`：{task_item}。"]
    if next_action:
        acceptance.append(f"把下一步收成更清晰的执行入口：{next_action}")
    return {
        "job_slug": f"auto-{_identity_segment(task_id)}",
        "executor_kind": "research_brief",
        "automation_mode": "background_assist",
        "allowed_actions": ["read", "write_report", "write_board"],
        "delivery_targets": ["board", "report"],
        "gate_policy": "none",
        "max_rounds": 2,
        "time_budget_minutes": 15,
        "acceptance_criteria": acceptance,
        "analysis_focus": [
            f"把 `{task_id}` 从普通板面任务提升成 Harness-ready 的可执行入口。",
            f"先澄清范围 `{scope}`、交付物与下一步，再决定是否进入更重的 implementation loop。",
        ],
        "task_family": family,
        "family_source": "heuristic_fallback",
        "family_resolution_reason": "generic_manual_long_task",
        "family_metadata_defaults": {},
        "stop_conditions": [],
    }


def _resolve_task_job_spec(project_name: str, ref: dict[str, Any]) -> dict[str, Any] | None:
    task_id = str(ref["row"].get("ID", "")).strip()
    spec = load_task_job_specs(project_name).get(task_id)
    if spec is not None:
        return spec
    spec = _pointer_task_spec(project_name, ref)
    if spec is not None:
        return spec
    pointer_family = _pointer_task_family(ref)
    if pointer_family:
        spec = _registry_family_spec(
            ref,
            family_id=pointer_family,
            family_source="pointer_family",
            family_resolution_reason="harness.family",
        )
        if spec is not None:
            return spec
    task_status = codex_memory.normalize_task_status(ref["row"].get("状态", "todo"))
    if task_status not in RUNNABLE_STATUSES:
        return None
    family_id, family_source, resolution_reason = _registry_matched_family(ref)
    if family_id:
        spec = _registry_family_spec(
            ref,
            family_id=family_id,
            family_source=family_source,
            family_resolution_reason=resolution_reason,
        )
        if spec is not None:
            return spec
    return _generic_job_spec(ref)


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


def _row_reopened_persisted_program(row: dict[str, Any], task_spec: dict[str, Any]) -> bool:
    if not task_spec:
        return False
    row_updated = codex_memory.parse_iso_timestamp(str(row.get("更新时间", "")).strip())
    spec_updated = codex_memory.parse_iso_timestamp(str(task_spec.get("updated_at", "")).strip())
    if row_updated and spec_updated:
        return row_updated > spec_updated
    return bool(row_updated) and not bool(spec_updated)


def _projected_task_status(row: dict[str, Any], task_spec: dict[str, Any]) -> str:
    task_status = codex_memory.normalize_task_status(row.get("状态", "todo"))
    if _row_reopened_persisted_program(row, task_spec):
        return task_status
    last_decision = str(task_spec.get("last_decision", "")).strip()
    stage = str(task_spec.get("stage", "")).strip()
    if last_decision == "done" or stage == "handoff":
        return "done"
    if last_decision == "blocked":
        return "blocked"
    return task_status


def task_harness_snapshot(project_name: str, task_id: str) -> dict[str, Any]:
    ref = find_task_ref(project_name, task_id)
    canonical_project = codex_memory.canonical_project_name(str(ref.get("project_name", "")).strip() or project_name)
    spec = _resolve_task_job_spec(canonical_project, ref)
    if spec is None:
        return {}
    try:
        from ops import background_job_executor
    except ImportError:  # pragma: no cover
        import background_job_executor  # type: ignore

    payload = background_job_executor.safe_job_status_payload(project_background_job(canonical_project, task_id))
    return {
        "harness_state": str(payload.get("harness_state", "")).strip(),
        "last_decision": str(payload.get("last_decision", "")).strip(),
        "next_wake_at": str(payload.get("next_wake_at", "")).strip(),
        "blocked_reason": str(payload.get("blocked_reason", "")).strip(),
        "current_stage": str(payload.get("current_stage", "")).strip(),
        "current_focus": str(payload.get("current_focus", "")).strip(),
        "last_run_id": str(payload.get("last_run_id", "")).strip(),
        "active_run_id": str(payload.get("active_run_id", "")).strip(),
        "project_runtime": dict(payload.get("project_runtime", {}) or {}),
        "bridge_runtime": dict(payload.get("bridge_runtime", {}) or {}),
        "run_tree": dict(payload.get("run_tree", {}) or {}),
        "delivery_contract": dict(payload.get("delivery_contract", {}) or {}),
        "execution_boundary": dict(payload.get("execution_boundary", {}) or {}),
        "instruction_surface": dict(payload.get("instruction_surface", {}) or {}),
        "extension_manifest": dict(payload.get("extension_manifest", {}) or {}),
        "workflow_manifest": dict(payload.get("workflow_manifest", {}) or {}),
        "instruction_migration": dict(payload.get("instruction_migration", {}) or {}),
        "open_source_boundary": dict(payload.get("open_source_boundary", {}) or {}),
        "harness_snapshot": dict(payload.get("harness_snapshot", {}) or {}),
    }


def project_background_job(project_name: str, task_id: str) -> dict[str, Any]:
    project_name = codex_memory.canonical_project_name(project_name)
    ref = find_task_ref(project_name, task_id)
    canonical_project = codex_memory.canonical_project_name(str(ref.get("project_name", "")).strip() or project_name)
    row = ref["row"]
    spec = _resolve_task_job_spec(canonical_project, ref)
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
    if (
        str(spec.get("executor_kind", "")).strip() == "implementation_loop"
        and not [item for item in spec.get("implementation_tracks", []) if isinstance(item, dict)]
        and _is_manual_long_task(ref)
    ):
        spec = dict(spec)
        spec["implementation_tracks"] = _materialize_generic_tracks(
            ref,
            family=str(spec.get("task_family", "")).strip() or _generic_task_family(ref),
            artifacts_root=artifacts_root,
        )
        spec["subgoal_schema_version"] = max(int(spec.get("subgoal_schema_version", 1) or 1), 2)
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
            "task_family": str(spec.get("task_family", "")).strip(),
            "family_source": str(spec.get("family_source", "")).strip(),
            "family_resolution_reason": str(spec.get("family_resolution_reason", "")).strip(),
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
        "task_family": str(spec.get("task_family", "")).strip(),
        "family_source": str(spec.get("family_source", "")).strip(),
        "family_resolution_reason": str(spec.get("family_resolution_reason", "")).strip(),
        "stop_conditions": [str(item).strip() for item in spec.get("stop_conditions", []) if str(item).strip()],
        "family_metadata_defaults": dict(spec.get("family_metadata_defaults", {}))
        if isinstance(spec.get("family_metadata_defaults"), dict)
        else {},
        "executor_kind": spec["executor_kind"],
        "automation_mode": spec["automation_mode"],
        "allowed_actions": list(spec["allowed_actions"]),
        "delivery_targets": list(spec["delivery_targets"]),
        "gate_policy": spec["gate_policy"],
        "max_rounds": int(spec["max_rounds"]),
        "time_budget_minutes": int(spec["time_budget_minutes"]),
        "subgoal_schema_version": int(spec.get("subgoal_schema_version", 1) or 1),
        "acceptance_criteria": list(spec["acceptance_criteria"]),
        "research_sources": [dict(item) for item in spec.get("research_sources", [])],
        "analysis_focus": list(spec.get("analysis_focus", [])),
        "implementation_tracks": [dict(item) for item in spec.get("implementation_tracks", []) if isinstance(item, dict)],
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
    for ref in _task_refs(project_name):
        task_id = str(ref.get("task_id", "")).strip()
        if not task_id:
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
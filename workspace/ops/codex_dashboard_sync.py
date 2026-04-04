#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

try:
    from ops.codex_memory import (
        ACTIONS_DASHBOARD_MD,
        ACTIVE_PROJECTS_MD,
        AUTO_CURRENT_TASKS_MARKERS,
        AUTO_PROJECT_ROLLUP_MARKERS,
        AUTO_TASK_TABLE_MARKERS,
        DASHBOARD_SYNC_NAME,
        DASHBOARD_SYNC_STATE_JSON,
        EVENTS_NDJSON,
        build_current_task_lines,
        extract_marked_block,
        HOME_DASHBOARD_MD,
        MATERIALS_DASHBOARD_ROOT,
        materials_dashboard_path,
        load_project_board,
        load_topic_board,
        MEMORY_HEALTH_MD,
        NEXT_ACTIONS_MD,
        project_board_next_action,
        topic_board_paths,
        topic_rollup_rows,
        PROJECTS_DASHBOARD_MD,
        REGISTRY_MD,
        WATCHER_NAME,
        WorkspaceLockBusy,
        dump_json,
        display_timestamp,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        load_bindings,
        load_registry,
        load_router,
        normalize_task_status,
        parse_markdown_table,
        project_board_facts,
        project_board_path,
        refresh_next_actions_rollup,
        refresh_project_rollups,
        read_text,
        refresh_active_projects,
        replace_or_append_marked_section,
        unique_completed_bindings,
        workspace_lock,
        write_text,
    )
except ImportError:  # pragma: no cover
    from codex_memory import (  # type: ignore
        ACTIONS_DASHBOARD_MD,
        ACTIVE_PROJECTS_MD,
        AUTO_CURRENT_TASKS_MARKERS,
        AUTO_PROJECT_ROLLUP_MARKERS,
        AUTO_TASK_TABLE_MARKERS,
        DASHBOARD_SYNC_NAME,
        DASHBOARD_SYNC_STATE_JSON,
        EVENTS_NDJSON,
        build_current_task_lines,
        extract_marked_block,
        HOME_DASHBOARD_MD,
        MATERIALS_DASHBOARD_ROOT,
        materials_dashboard_path,
        load_project_board,
        load_topic_board,
        MEMORY_HEALTH_MD,
        NEXT_ACTIONS_MD,
        project_board_next_action,
        topic_board_paths,
        topic_rollup_rows,
        PROJECTS_DASHBOARD_MD,
        REGISTRY_MD,
        WATCHER_NAME,
        WorkspaceLockBusy,
        dump_json,
        display_timestamp,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        load_bindings,
        load_registry,
        load_router,
        normalize_task_status,
        parse_markdown_table,
        project_board_facts,
        project_board_path,
        refresh_next_actions_rollup,
        refresh_project_rollups,
        read_text,
        refresh_active_projects,
        replace_or_append_marked_section,
        unique_completed_bindings,
        workspace_lock,
        write_text,
    )

try:
    from ops import background_job_executor, board_job_projector, codex_retrieval, material_router, runtime_state, workspace_job_schema
except ImportError:  # pragma: no cover
    import background_job_executor  # type: ignore
    import board_job_projector  # type: ignore
    import codex_retrieval  # type: ignore
    import material_router  # type: ignore
    import runtime_state  # type: ignore
    import workspace_job_schema  # type: ignore


HOME_MARKERS = ("<!-- AUTO_HOME_START -->", "<!-- AUTO_HOME_END -->")
PROJECTS_MARKERS = ("<!-- AUTO_PROJECTS_START -->", "<!-- AUTO_PROJECTS_END -->")
ACTIONS_MARKERS = ("<!-- AUTO_ACTIONS_START -->", "<!-- AUTO_ACTIONS_END -->")
HEALTH_MARKERS = ("<!-- AUTO_HEALTH_START -->", "<!-- AUTO_HEALTH_END -->")
FOLLOWUP_MARKERS = ("<!-- AUTO_FOLLOWUPS_START -->", "<!-- AUTO_FOLLOWUPS_END -->")
WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
SYNC_LOG_STDOUT = WORKSPACE_ROOT / "logs" / "codex-dashboard-sync.log"
SYNC_LOG_STDERR = WORKSPACE_ROOT / "logs" / "codex-dashboard-sync.err.log"
GLOBAL_TASK_HEADERS = ["ID", "范围", "事项", "状态", "下一步", "指向"]
PROJECT_DASHBOARD_HEADERS = ["项目", "状态", "优先级", "更新时间", "下一步", "最近会话", "最近线程"]
DASHBOARD_RULES = [
    {
        "path": HOME_DASHBOARD_MD,
        "markers": HOME_MARKERS,
        "disallowed_headings": ["## Status"],
        "disallowed_patterns": [
            ("已注册项目数", r"已注册项目："),
            ("待执行状态摘要", r"待执行："),
        ],
    },
    {
        "path": PROJECTS_DASHBOARD_MD,
        "markers": PROJECTS_MARKERS,
        "disallowed_headings": ["## Current"],
        "disallowed_patterns": [
            ("手写项目清单", r"^\d+\.\s*`"),
        ],
    },
    {
        "path": ACTIONS_DASHBOARD_MD,
        "markers": ACTIONS_MARKERS,
        "disallowed_headings": ["## todo", "## doing", "## blocked", "## done"],
        "disallowed_patterns": [],
    },
    {
        "path": MEMORY_HEALTH_MD,
        "markers": HEALTH_MARKERS,
        "disallowed_headings": ["## Current"],
        "disallowed_patterns": [
            ("手写项目注册数量", r"项目注册数量："),
            ("手写活跃项目数量", r"活跃项目数量："),
            ("手写最近写回状态", r"最近写回："),
            ("手写待归档状态", r"待归档项："),
        ],
    },
]


def parse_iso(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_state() -> dict[str, Any]:
    fallback = {
        "version": 1,
        "updated_at": None,
        "last_processed_event_line": 0,
        "last_incremental_sync_at": None,
        "last_full_rebuild_at": None,
        "last_status": "never-run",
        "last_error": "",
    }
    try:
        return json.loads(read_text(DASHBOARD_SYNC_STATE_JSON))
    except json.JSONDecodeError:
        return fallback


def save_state(data: dict[str, Any]) -> None:
    data["updated_at"] = iso_now()
    dump_json(DASHBOARD_SYNC_STATE_JSON, data)


def read_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for index, line in enumerate(read_text(EVENTS_NDJSON).splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        item["_line"] = index
        events.append(item)
    return events


def pending_event_count(events: list[dict[str, Any]], last_processed_line: int) -> int:
    return sum(1 for item in events if int(item.get("_line", 0)) > last_processed_line)


def should_rebuild_all(state: dict[str, Any], now: dt.datetime) -> bool:
    last_full = parse_iso(state.get("last_full_rebuild_at", ""))
    if not last_full:
        return True
    return (now - last_full).total_seconds() >= 1800


def dashboard_source_paths(registry: list[dict[str, Any]] | None = None) -> list[Path]:
    entries = registry or load_registry()
    paths: list[Path] = [REGISTRY_MD, NEXT_ACTIONS_MD]
    for entry in entries:
        project_name = entry["project_name"]
        paths.append(project_board_path(project_name))
        paths.extend(topic_board_paths(project_name))
        route_path = Path(material_router.material_route_path(project_name))
        if route_path.exists():
            paths.append(route_path)
    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        deduped.append(path)
    return deduped


def latest_dashboard_source_mtime(registry: list[dict[str, Any]] | None = None) -> dt.datetime | None:
    latest: dt.datetime | None = None
    for path in dashboard_source_paths(registry):
        if not path.exists():
            continue
        updated = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
        if latest is None or updated > latest:
            latest = updated
    return latest


def dashboard_sources_changed_since_last_sync(state: dict[str, Any], registry: list[dict[str, Any]] | None = None) -> bool:
    latest_source = latest_dashboard_source_mtime(registry)
    if latest_source is None:
        return False
    last_sync = parse_iso(state.get("last_incremental_sync_at", "")) or parse_iso(state.get("last_full_rebuild_at", ""))
    if last_sync is None:
        return True
    return latest_source > last_sync


def extract_generated_followups() -> list[str]:
    text = read_text(NEXT_ACTIONS_MD)
    start, end = FOLLOWUP_MARKERS
    if start not in text or end not in text:
        return []
    block = text.split(start, 1)[1].split(end, 1)[0]
    return [line.strip() for line in block.splitlines() if line.strip().startswith("- [ ]")]


def extract_manual_task_rows() -> list[dict[str, str]]:
    text = read_text(NEXT_ACTIONS_MD)
    match = re.search(r"## Global Manual Tasks\s+(.*?)(?:\n## |\Z)", text, re.S)
    if not match:
        return []
    rows = parse_markdown_table(match.group(1), GLOBAL_TASK_HEADERS)
    for row in rows:
        row["状态"] = normalize_task_status(row.get("状态", "todo"))
    return rows


def compact_path(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        path = Path(raw).resolve(strict=False)
    except Exception:
        return raw
    for root in (
        MATERIALS_DASHBOARD_ROOT.parent,
        REGISTRY_MD.parent,
        WORKSPACE_ROOT,
    ):
        try:
            return str(path.relative_to(root))
        except ValueError:
            continue
    return str(path)


def material_hit_lines(title: str, items: list[dict[str, Any]]) -> list[str]:
    lines = [f"### {title}", ""]
    if not items:
        lines.append("- 暂无")
        lines.append("")
        return lines
    for item in items:
        label = str(item.get("title") or Path(str(item.get("path", ""))).name)
        group = str(item.get("source_group", "other"))
        heading = str(item.get("heading", "")).strip()
        path_value = compact_path(str(item.get("path", "")))
        line_start = int(item.get("line_start", 0) or 0)
        line_end = int(item.get("line_end", 0) or 0)
        meta_parts = [group]
        if heading:
            meta_parts.append(heading)
        if line_start and line_end:
            meta_parts.append(f"L{line_start}-{line_end}")
        if item.get("is_hotset"):
            meta_parts.append("hotset")
        lines.append(f"- `{label}`")
        lines.append(f"  - {' | '.join(meta_parts)}")
        if path_value:
            lines.append(f"  - `{path_value}`")
    lines.append("")
    return lines


def project_harness_facts(project_name: str) -> tuple[list[dict[str, Any]], list[str]]:
    facts: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        jobs = board_job_projector.list_projectable_jobs(project_name)
    except Exception as exc:
        return [], [f"{project_name}: harness job projection failed: {exc}"]
    for job in jobs:
        try:
            facts.append(background_job_executor.job_status_payload(job))
        except Exception as exc:
            task_id = str(job.get("task_id", "")).strip() or "unknown-task"
            errors.append(f"{project_name}: harness status failed for `{task_id}`: {exc}")
    facts.sort(key=lambda item: (str(item.get("task_id", "")), str(item.get("job_id", ""))))
    return facts, errors


def harness_status_lines(items: list[dict[str, Any]]) -> list[str]:
    lines = ["## Harness 派生状态", ""]
    if not items:
        lines.extend(["- 暂无可投影 harness 任务", ""])
        return lines
    for item in items:
        task_id = str(item.get("task_id", "")).strip() or "unknown-task"
        task_item = str(item.get("task_item", "")).strip() or "未命名任务"
        harness_state = str(item.get("harness_state", "")).strip() or "unknown"
        last_decision = str(item.get("last_decision", "")).strip() or "n/a"
        next_action = str(item.get("next_action", "")).strip() or "n/a"
        next_wake_at = str(item.get("next_wake_at", "")).strip() or "n/a"
        blocked_reason = str(item.get("blocked_reason", "")).strip() or "n/a"
        current_focus = str(item.get("current_focus", "")).strip()
        runtime_lines = workspace_job_schema.runtime_contract_summary_lines(
            item,
            include_project_board_path=False,
            include_project_updated_at=False,
            include_handoff_packet=False,
            include_local_context_roots=True,
            include_bridge_name=False,
            snapshot_mode="completed_pending_active",
        )
        lines.append(
            f"- `{task_id}` {task_item} | harness_state=`{harness_state}` | "
            f"last_decision=`{last_decision}` | next_wake_at=`{next_wake_at}` | blocked_reason=`{blocked_reason}`"
        )
        lines.append(f"  - next_action: {next_action}")
        if current_focus:
            lines.append(f"  - focus: {current_focus}")
        for runtime_line in runtime_lines:
            lines.append(f"  - {runtime_line}")
    lines.append("")
    return lines


def render_materials_dashboard(project_name: str) -> str:
    inspect_payload = material_router.inspect_material_route(project_name)
    suggest_payload = material_router.suggest_material_route(project_name, "")
    retrieval_state = codex_retrieval.load_state()
    harness_facts, harness_errors = project_harness_facts(project_name)
    lines = [
        "---",
        f"project_name: {project_name}",
        "dashboard_type: materials",
        f"updated_at: {iso_now()}",
        "---",
        "",
        f"# {project_name}｜材料检查",
        "",
        "> 只读检查页。这里展示项目真相入口、材料路由配置和最近材料命中；它不是任务状态事实源。",
        "",
        "## 当前绑定与真相入口",
        "",
        f"- 项目：`{project_name}`",
        f"- truth board：`{compact_path(str(suggest_payload.get('board_path', '')) or str(project_board_path(project_name)))}`",
        f"- 材料配置：present=`{inspect_payload.get('config_present')}` valid=`{inspect_payload.get('config_valid')}` complete=`{inspect_payload.get('complete')}`",
        f"- 配置文件：`{compact_path(str(inspect_payload.get('config_path', '')) )}`",
        "",
        "## 材料路由配置",
        "",
        f"- allow roots：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('allow_roots', [])) or '无'}",
        f"- 材料根：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('project_material_roots', [])) or '无'}",
        f"- 报告根：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('report_roots', [])) or '无'}",
        f"- 交付根：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('deliverable_roots', [])) or '无'}",
        f"- hotset：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('hotset_paths', [])) or '无'}",
        f"- ignore：{', '.join(f'`{compact_path(item)}`' for item in inspect_payload.get('ignore_paths', [])) or '无'}",
        f"- 推荐 query：{', '.join(f'`{item}`' for item in inspect_payload.get('preferred_queries', [])) or '无'}",
        "",
        "## 检索状态",
        "",
        f"- 最近 build：`{retrieval_state.get('last_build_at') or 'never'}`",
        f"- 最近 sync：`{retrieval_state.get('last_sync_at') or 'never'}`",
        f"- 文档数：`{retrieval_state.get('doc_count', 0)}`",
        f"- dirty count：`{retrieval_state.get('dirty_count', 0)}`",
        "",
    ]
    issues = inspect_payload.get("issues", [])
    issues = list(issues) + harness_errors
    if issues:
        lines.extend(["## 配置问题", "", *[f"- `{item}`" for item in issues], ""])
    lines.extend(harness_status_lines(harness_facts))
    lines.extend(material_hit_lines("Hotset 命中", suggest_payload.get("hotset_hits", [])))
    lines.extend(material_hit_lines("报告命中", suggest_payload.get("report_hits", [])))
    lines.extend(material_hit_lines("交付命中", suggest_payload.get("deliverable_hits", [])))
    lines.extend(material_hit_lines("材料命中", suggest_payload.get("material_hits", [])))
    return "\n".join(lines).strip() + "\n"


def extract_action_sections() -> dict[str, list[str]]:
    sections = {"todo": [], "doing": [], "blocked": [], "done": []}
    for row in extract_manual_task_rows():
        status = normalize_task_status(row.get("状态", "todo"))
        prefix = "[x]" if status == "done" else "[ ]"
        scope = row.get("范围", "").replace("|", "/")
        item = row.get("事项", "").replace("|", "/")
        next_step = row.get("下一步", "待补充").replace("|", "/")
        sections.setdefault(status, []).append(
            f"- {prefix} {row.get('ID', '')} {scope} {item} | 下一步：{next_step}"
        )

    text = extract_marked_block(read_text(NEXT_ACTIONS_MD), AUTO_PROJECT_ROLLUP_MARKERS)
    current: str | None = None
    for raw in text.splitlines():
        line = raw.strip()
        if line == "### todo":
            current = "todo"
            continue
        if line == "### doing":
            current = "doing"
            continue
        if line == "### blocked":
            current = "blocked"
            continue
        if line == "### done":
            current = "done"
            continue
        if line.startswith("## ") and not line.startswith("## 总看板"):
            current = None
        if current and line.startswith("- ["):
            sections[current].append(line)
    return sections


def strip_marked_block(text: str, markers: tuple[str, str]) -> str:
    start, end = markers
    if start not in text or end not in text:
        return text
    prefix, rest = text.split(start, 1)
    _block, suffix = rest.split(end, 1)
    return prefix + suffix


def build_projects_dashboard_lines(projects: list[dict[str, Any]]) -> list[str]:
    def cell(value: Any, *, limit: int | None = None) -> str:
        text = str(value or "").replace("|", "/").replace("\r", " ").replace("\n", " ")
        text = " ".join(text.split())
        if limit is not None:
            return text[:limit]
        return text

    lines = [
        "| 项目 | 状态 | 优先级 | 更新时间 | 下一步 | 最近会话 | 最近线程 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in projects:
        lines.append(
            "| `{project_name}` | `{status}` | `{priority}` | {updated_at} | {next_action} | `{last_session_id}` | {last_thread_name} |".format(
                project_name=item["project_name"],
                status=item.get("status", "active"),
                priority=item.get("priority", "medium"),
                updated_at=item.get("updated_at", "未知"),
                next_action=cell(item.get("next_action", "待补充")),
                last_session_id=item.get("last_session_id", ""),
                last_thread_name=cell(item.get("last_thread_name", ""), limit=80),
            )
        )
    return lines


def build_actions_dashboard_lines(sections: dict[str, list[str]], followups: list[str]) -> list[str]:
    cleaned_sections: dict[str, list[str]] = {}
    for status, items in sections.items():
        placeholders = [item for item in items if "暂无 " in item]
        real_items = [item for item in items if "暂无 " not in item]
        cleaned_sections[status] = real_items or placeholders
    lines = ["### todo"]
    lines.extend(cleaned_sections["todo"] or ["- [ ] 暂无 todo"])
    lines.extend(["", "### doing"])
    lines.extend(cleaned_sections["doing"] or ["- [ ] 暂无 doing"])
    lines.extend(["", "### blocked"])
    lines.extend(cleaned_sections["blocked"] or ["- [ ] 暂无 blocked"])
    lines.extend(["", "### done"])
    lines.extend(cleaned_sections["done"] or ["- [x] 暂无 done"])
    lines.extend(["", "### followups"])
    lines.extend(followups or ["- [ ] 暂无自动跟进项"])
    return lines


def dashboard_structure_warnings() -> list[str]:
    warnings: list[str] = []
    for rule in DASHBOARD_RULES:
        path = rule["path"]
        text = read_text(path)
        start, end = rule["markers"]
        if start not in text or end not in text:
            warnings.append(f"{path.name}: missing auto block markers `{start}` / `{end}`")
            static_text = text
        else:
            static_text = strip_marked_block(text, rule["markers"])
        for heading in rule["disallowed_headings"]:
            if re.search(rf"^{re.escape(heading)}\s*$", static_text, re.M):
                warnings.append(f"{path.name}: contains deprecated manual section `{heading}`")
        for label, pattern in rule["disallowed_patterns"]:
            if re.search(pattern, static_text, re.M):
                warnings.append(f"{path.name}: contains conflicting static content `{label}`")
    return warnings


def clean_cell(value: str) -> str:
    return str(value).strip().strip("`")


def extract_absolute_paths_from_cell(value: str) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    matches = re.findall(r"`(/Users/frank/[^`]+)`", text)
    cleaned = clean_cell(text)
    if cleaned.startswith("/Users/frank/"):
        parts = [
            item.strip().strip("`")
            for item in re.split(r"[、,;；]\s*", cleaned)
            if item.strip()
        ]
        matches.extend(parts)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in matches:
        candidate = item.strip()
        if not candidate.startswith("/Users/frank/"):
            continue
        if any(char in candidate for char in "*{}"):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def verify_project_path_references(project_name: str) -> list[str]:
    issues: list[str] = []
    project_board = load_project_board(project_name)
    seen: set[str] = set()

    def record_missing(path_text: str, context: str) -> None:
        candidate = path_text.strip()
        if not candidate or candidate in seen:
            return
        if any(char in candidate for char in "*{}"):
            return
        seen.add(candidate)
        if not Path(candidate).exists():
            issues.append(f"{project_board['path'].name}: {context} references missing path `{candidate}`")

    for path_text in re.findall(r"`(/Users/frank/[^`]+)`", project_board["body"]):
        record_missing(path_text, "board body")

    for row_group_name, rows in (
        ("project row", project_board["project_rows"]),
        ("rollup row", project_board["rollup_rows"]),
        ("gflow row", project_board.get("gflow_rows", [])),
    ):
        for row in rows:
            task_id = row.get("ID", "") or "n/a"
            for field in ("交付物", "指向"):
                for path_text in extract_absolute_paths_from_cell(row.get(field, "")):
                    record_missing(path_text, f"{row_group_name} `{task_id}` field `{field}`")
    return issues


def verify_project_rollup_consistency(project_name: str) -> list[str]:
    issues: list[str] = []
    project_board = load_project_board(project_name)
    expected_rollups: dict[str, dict[str, str]] = {}
    for topic_path in topic_board_paths(project_name):
        topic_board = load_topic_board(topic_path)
        extract_marked_block(topic_board["body"], AUTO_TASK_TABLE_MARKERS)
        for row in topic_rollup_rows(topic_board):
            if row.get("ID"):
                expected_rollups[row["ID"]] = row

    actual_rollups = {
        row.get("ID", ""): row
        for row in project_board["rollup_rows"]
        if row.get("ID") and row.get("来源", "").startswith("topic:")
    }

    for task_id in sorted(set(expected_rollups) - set(actual_rollups)):
        issues.append(f"{project_board['path'].name}: missing topic rollup `{task_id}` from project board")
    for task_id in sorted(set(actual_rollups) - set(expected_rollups)):
        issues.append(f"{project_board['path'].name}: unexpected topic rollup `{task_id}` not backed by topic board")

    compared_fields = [
        "父ID",
        "来源",
        "范围",
        "事项",
        "状态",
        "交付物",
        "审核状态",
        "审核人",
        "审核结论",
        "审核时间",
        "下一步",
        "更新时间",
        "指向",
    ]
    for task_id in sorted(set(expected_rollups) & set(actual_rollups)):
        expected = expected_rollups[task_id]
        actual = actual_rollups[task_id]
        mismatches = []
        for field in compared_fields:
            if clean_cell(actual.get(field, "")) != clean_cell(expected.get(field, "")):
                mismatches.append(
                    f"{field}: expected `{expected.get(field, '')}` got `{actual.get(field, '')}`"
                )
        if mismatches:
            issues.append(
                f"{project_board['path'].name}: rollup `{task_id}` mismatched with topic source; " + "; ".join(mismatches)
            )

    expected_current = "\n".join(
        build_current_task_lines(
            project_board["project_rows"],
            project_board["rollup_rows"],
            project_board.get("gflow_rows", []),
            project_name=project_name,
        )
    ).strip()
    actual_current = extract_marked_block(project_board["body"], AUTO_CURRENT_TASKS_MARKERS).strip()
    if actual_current != expected_current:
        issues.append(f"{project_board['path'].name}: `## 当前任务` block is out of sync with task tables")
    return issues


def verify_projects_dashboard_consistency(
    projects: list[dict[str, Any]],
    selected_projects: list[str],
) -> list[str]:
    issues: list[str] = []
    actual_rows = parse_markdown_table(
        extract_marked_block(read_text(PROJECTS_DASHBOARD_MD), PROJECTS_MARKERS),
        PROJECT_DASHBOARD_HEADERS,
    )
    row_map = {clean_cell(row.get("项目", "")): row for row in actual_rows}
    selected_set = set(selected_projects)
    fact_map = {item["project_name"]: item for item in projects if item["project_name"] in selected_set}
    for project_name in selected_projects:
        row = row_map.get(project_name)
        fact = fact_map.get(project_name)
        if not fact:
            issues.append(f"PROJECTS.md: missing project fact for `{project_name}`")
            continue
        if not row:
            issues.append(f"PROJECTS.md: missing auto row for `{project_name}`")
            continue
        if clean_cell(row.get("状态", "")) != clean_cell(fact.get("status", "")):
            issues.append(
                f"PROJECTS.md: `{project_name}` status mismatch; expected `{fact.get('status', '')}` got `{row.get('状态', '')}`"
            )
        if clean_cell(row.get("优先级", "")) != clean_cell(fact.get("priority", "")):
            issues.append(
                f"PROJECTS.md: `{project_name}` priority mismatch; expected `{fact.get('priority', '')}` got `{row.get('优先级', '')}`"
            )
        if clean_cell(row.get("更新时间", "")) != clean_cell(fact.get("updated_at", "")):
            issues.append(
                f"PROJECTS.md: `{project_name}` updated_at mismatch; expected `{fact.get('updated_at', '')}` got `{row.get('更新时间', '')}`"
            )
        if clean_cell(row.get("下一步", "")) != clean_cell(fact.get("next_action", "")):
            issues.append(
                f"PROJECTS.md: `{project_name}` next action mismatch; expected `{fact.get('next_action', '')}` got `{row.get('下一步', '')}`"
            )
    return issues


def verify_actions_dashboard_consistency() -> list[str]:
    expected = "\n".join(
        build_actions_dashboard_lines(extract_action_sections(), extract_generated_followups())
    ).strip()
    actual = extract_marked_block(read_text(ACTIONS_DASHBOARD_MD), ACTIONS_MARKERS).strip()
    if actual == expected:
        return []
    return ["ACTIONS.md: `Auto Board` block is out of sync with NEXT_ACTIONS.md"]


def verify_consistency(project_names: list[str] | None = None) -> dict[str, Any]:
    projects, fact_errors = project_facts()
    selected_projects = project_names or [item["project_name"] for item in projects]
    issues: list[str] = []
    issues.extend(dashboard_structure_warnings())
    issues.extend([item for item in fact_errors if any(project_name in item for project_name in selected_projects)])
    for project_name in selected_projects:
        issues.extend(verify_project_rollup_consistency(project_name))
        issues.extend(verify_project_path_references(project_name))
    issues.extend(verify_projects_dashboard_consistency(projects, selected_projects))
    issues.extend(verify_actions_dashboard_consistency())
    return {
        "ok": not issues,
        "checked_projects": selected_projects,
        "issue_count": len(issues),
        "issues": issues,
    }


def project_facts() -> tuple[list[dict[str, Any]], list[str]]:
    router = load_router().get("routes", {})
    facts, errors = project_board_facts(load_registry())
    merged: list[dict[str, Any]] = []
    for item in facts:
        project_name = item["project_name"]
        route = router.get(project_name, {})
        merged.append(
            {
                "project_name": project_name,
                "status": item.get("status", "active"),
                "priority": item.get("priority", "medium"),
                "updated_at": item.get("updated_at", ""),
                "next_action": item.get("next_action", "待补充"),
                "path": item.get("board_path", ""),
                "last_session_id": route.get("last_session_id", ""),
                "last_active_at": route.get("last_active_at", ""),
                "last_thread_name": route.get("last_thread_name", ""),
            }
        )
    return merged, errors


def stale_projects(projects: list[dict[str, Any]], now: dt.datetime) -> list[str]:
    flagged: list[str] = []
    for item in projects:
        updated = parse_iso(f"{item.get('updated_at', '')}T00:00:00+00:00") if item.get("updated_at") else None
        stale = updated is None or (now - updated).days >= 7
        if stale or not item.get("last_session_id") or str(item.get("next_action", "")).startswith("补充"):
            flagged.append(item["project_name"])
    return flagged[:5]


def render_home(text: str, projects: list[dict[str, Any]], bindings: list[dict[str, Any]], now: dt.datetime) -> str:
    active = [item for item in projects if item.get("status") == "active"]
    recent_projects = sorted(
        [item for item in projects if item.get("updated_at")],
        key=lambda item: item.get("updated_at", ""),
        reverse=True,
    )[:3]
    recent_sessions = unique_completed_bindings(bindings, limit=5)
    recent_project_labels = ", ".join(f"`{item['project_name']}`" for item in recent_projects) or "无"
    lines = [
        f"- 活跃项目数：{len(active)}",
        f"- 最近更新项目：{recent_project_labels}",
        f"- 最近完成会话：{len(recent_sessions)}",
        f"- 需要关注项目：{', '.join(f'`{name}`' for name in stale_projects(projects, now)) or '无'}",
    ]
    return replace_or_append_marked_section(text, "## Auto Overview", HOME_MARKERS, lines)


def render_projects_dashboard(text: str, projects: list[dict[str, Any]]) -> str:
    return replace_or_append_marked_section(text, "## Auto Projects", PROJECTS_MARKERS, build_projects_dashboard_lines(projects))


def render_actions_dashboard(text: str, sections: dict[str, list[str]], followups: list[str]) -> str:
    return replace_or_append_marked_section(
        text,
        "## Auto Board",
        ACTIONS_MARKERS,
        build_actions_dashboard_lines(sections, followups),
    )


def render_health_dashboard(
    text: str,
    projects: list[dict[str, Any]],
    errors: list[str],
    state: dict[str, Any],
    events: list[dict[str, Any]],
) -> str:
    queue_status = runtime_state.fetch_runtime_queue_status(queue_name="dashboard_sync")
    backlog = max(
        pending_event_count(events, int(state.get("last_processed_event_line", 0))),
        int(queue_status.get("aggregate", {}).get("pending", 0) or 0)
        + int(queue_status.get("aggregate", {}).get("failed", 0) or 0),
    )
    watcher_plist = launch_agent_plist_path(WATCHER_NAME)
    sync_plist = launch_agent_plist_path(DASHBOARD_SYNC_NAME)
    lines = [
        f"- watcher 状态：installed=`{watcher_plist.exists()}` loaded=`{launch_agent_loaded(WATCHER_NAME)}`",
        f"- dashboard sync 状态：installed=`{sync_plist.exists()}` loaded=`{launch_agent_loaded(DASHBOARD_SYNC_NAME)}`",
        f"- 最近增量同步：{display_timestamp(state.get('last_incremental_sync_at') or '') or '未开始'}",
        f"- 最近全量校准：{display_timestamp(state.get('last_full_rebuild_at') or '') or '未开始'}",
        f"- 事件堆积数量：{backlog}",
        f"- 长时间未更新项目：{', '.join(f'`{name}`' for name in stale_projects(projects, dt.datetime.now(dt.timezone.utc))) or '无'}",
        f"- 最近错误：{state.get('last_error') or '无'}",
    ]
    if errors:
        lines.append("")
        lines.append("### sync warnings")
        lines.extend(f"- {item}" for item in errors[:10])
    return replace_or_append_marked_section(text, "## Auto Health", HEALTH_MARKERS, lines)


def rebuild_dashboards(*, state: dict[str, Any], full: bool, registry: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc)
    registry = registry or load_registry()
    refresh_errors: list[str] = []
    for entry in registry:
        try:
            refresh_project_rollups(entry["project_name"])
        except Exception as exc:
            refresh_errors.append(f"{entry['project_name']}: rollup refresh failed: {exc}")
    refresh_active_projects(registry)
    refresh_next_actions_rollup()
    projects, errors = project_facts()
    bindings = load_bindings().get("bindings", [])
    sections = extract_action_sections()
    followups = extract_generated_followups()
    events = read_events()
    preview_state = dict(state)
    preview_state["last_status"] = "ok"
    preview_state["last_error"] = ""
    preview_state["last_incremental_sync_at"] = iso_now()
    if full:
        preview_state["last_full_rebuild_at"] = preview_state["last_incremental_sync_at"]
    preview_state["last_processed_event_line"] = max((item["_line"] for item in events), default=0)

    write_text(HOME_DASHBOARD_MD, render_home(read_text(HOME_DASHBOARD_MD), projects, bindings, now))
    write_text(PROJECTS_DASHBOARD_MD, render_projects_dashboard(read_text(PROJECTS_DASHBOARD_MD), projects))
    write_text(ACTIONS_DASHBOARD_MD, render_actions_dashboard(read_text(ACTIONS_DASHBOARD_MD), sections, followups))
    warnings = refresh_errors + errors + dashboard_structure_warnings()
    write_text(MEMORY_HEALTH_MD, render_health_dashboard(read_text(MEMORY_HEALTH_MD), projects, warnings, preview_state, events))
    for item in projects:
        project_name = item["project_name"]
        write_text(materials_dashboard_path(project_name), render_materials_dashboard(project_name))

    state.update(preview_state)
    save_state(state)
    return {
        "projects": [item["project_name"] for item in projects],
        "warnings": refresh_errors + errors,
        "full_rebuild": full,
    }


def run_sync(force_full: bool = False, *, skip_if_locked: bool = False) -> dict[str, Any]:
    state = load_state()
    events = read_events()
    last_processed = int(state.get("last_processed_event_line", 0))
    pending_events = [item for item in events if int(item.get("_line", 0)) > last_processed]
    try:
        with workspace_lock(blocking=not skip_if_locked):
            registry = load_registry()
            claimed_runtime_events = runtime_state.claim_runtime_events(
                queue_name="dashboard_sync",
                claimed_by="codex_dashboard_sync.run_sync",
                limit=500,
                lease_seconds=900,
            )
            now = dt.datetime.now(dt.timezone.utc)
            full = force_full or should_rebuild_all(state, now) or dashboard_sources_changed_since_last_sync(state, registry)

            if not pending_events and not claimed_runtime_events and not full:
                state["last_status"] = "idle"
                state["last_error"] = ""
                save_state(state)
                return {
                    "status": "idle",
                    "pending_events": 0,
                    "last_processed_event_line": last_processed,
                }

            try:
                result = rebuild_dashboards(state=state, full=full, registry=registry)
            except Exception as exc:
                for item in claimed_runtime_events:
                    runtime_state.fail_runtime_event(
                        item.get("event_key", ""),
                        claim_token=str(item.get("claim_token", "")).strip(),
                        error=str(exc),
                        retry_after_seconds=60,
                    )
                state["last_status"] = "error"
                state["last_error"] = str(exc)
                save_state(state)
                raise

            for item in claimed_runtime_events:
                runtime_state.complete_runtime_event(
                    item.get("event_key", ""),
                    claim_token=str(item.get("claim_token", "")).strip(),
                    result={"status": "ok", "full_rebuild": bool(full)},
                )
            if events:
                state["last_processed_event_line"] = max(item["_line"] for item in events)
            save_state(state)
            return {
                "status": "ok",
                "pending_events": len(pending_events) + len(claimed_runtime_events),
                "last_processed_event_line": state.get("last_processed_event_line", 0),
                **result,
            }
    except Exception as exc:
        if skip_if_locked and (isinstance(exc, WorkspaceLockBusy) or exc.__class__.__name__ == "WorkspaceLockBusy"):
            return {
                "status": "busy",
                "reason": "workspace_lock_held",
                "pending_events": len(pending_events),
                "last_processed_event_line": last_processed,
            }
        raise


def plist_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def plist_value(value: Any, indent: str = "    ") -> str:
    if isinstance(value, bool):
        return f"{indent}<{str(value).lower()}/>"
    if isinstance(value, int):
        return f"{indent}<integer>{value}</integer>"
    if isinstance(value, str):
        return f"{indent}<string>{plist_escape(value)}</string>"
    if isinstance(value, list):
        lines = [f"{indent}<array>"]
        for item in value:
            lines.append(plist_value(item, indent + "  "))
        lines.append(f"{indent}</array>")
        return "\n".join(lines)
    if isinstance(value, dict):
        lines = [f"{indent}<dict>"]
        for key, item in value.items():
            lines.append(f"{indent}  <key>{plist_escape(str(key))}</key>")
            lines.append(plist_value(item, indent + "  "))
        lines.append(f"{indent}</dict>")
        return "\n".join(lines)
    return f"{indent}<string>{plist_escape(str(value))}</string>"


def plist_dumps(payload: dict[str, Any]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">',
        "  <dict>",
    ]
    for key, value in payload.items():
        lines.append(f"    <key>{plist_escape(str(key))}</key>")
        lines.append(plist_value(value, "    "))
    lines.extend(["  </dict>", "</plist>"])
    return "\n".join(lines) + "\n"


def run_launchctl(*parts: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *parts],
        text=True,
        capture_output=True,
        check=False,
    )


def cmd_sync_once(_args: argparse.Namespace) -> int:
    result = run_sync(force_full=False, skip_if_locked=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_rebuild_all(_args: argparse.Namespace) -> int:
    result = run_sync(force_full=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    state = load_state()
    events = read_events()
    queue_status = runtime_state.fetch_runtime_queue_status(queue_name="dashboard_sync")
    pending = int(queue_status.get("aggregate", {}).get("pending", 0) or 0) + int(
        queue_status.get("aggregate", {}).get("failed", 0) or 0
    )
    pending = max(pending, pending_event_count(events, int(state.get("last_processed_event_line", 0))))
    print(
        json.dumps(
            {
                "installed": launch_agent_plist_path(DASHBOARD_SYNC_NAME).exists(),
                "loaded": launch_agent_loaded(DASHBOARD_SYNC_NAME),
                "plist": str(launch_agent_plist_path(DASHBOARD_SYNC_NAME)),
                "state_path": str(DASHBOARD_SYNC_STATE_JSON),
                "pending_events": pending,
                "last_incremental_sync_at": state.get("last_incremental_sync_at"),
                "last_full_rebuild_at": state.get("last_full_rebuild_at"),
                "last_status": state.get("last_status"),
                "last_error": state.get("last_error"),
                "runtime_queue": queue_status,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_verify_consistency(args: argparse.Namespace) -> int:
    result = verify_consistency(args.project or None)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(DASHBOARD_SYNC_NAME)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    SYNC_LOG_STDOUT.parent.mkdir(parents=True, exist_ok=True)
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    payload = {
        "Label": DASHBOARD_SYNC_NAME,
        "ProgramArguments": [
            python_path,
            str(Path(__file__).resolve()),
            "sync-once",
        ],
        "RunAtLoad": True,
        "StartInterval": int(args.interval),
        "StandardOutPath": str(SYNC_LOG_STDOUT),
        "StandardErrorPath": str(SYNC_LOG_STDERR),
        "WorkingDirectory": str(WORKSPACE_ROOT),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
    }
    plist_path.write_text(plist_dumps(payload), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    bootstrap = run_launchctl("bootstrap", domain, str(plist_path))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=os.sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{DASHBOARD_SYNC_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=os.sys.stderr)
        return kickstart.returncode
    print(
        json.dumps(
            {
                "installed": True,
                "plist": str(plist_path),
                "interval": int(args.interval),
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(DASHBOARD_SYNC_NAME)
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    if plist_path.exists():
        plist_path.unlink()
    print(json.dumps({"installed": False, "plist": str(plist_path)}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync workspace-level dashboards from project writeback events")
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync = subparsers.add_parser("sync-once")
    sync.set_defaults(func=cmd_sync_once)

    rebuild = subparsers.add_parser("rebuild-all")
    rebuild.set_defaults(func=cmd_rebuild_all)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    verify = subparsers.add_parser("verify-consistency")
    verify.add_argument("--project", action="append", default=[])
    verify.set_defaults(func=cmd_verify_consistency)

    install = subparsers.add_parser("install-launchagent")
    install.add_argument("--interval", type=int, default=900)
    install.set_defaults(func=cmd_install_launchagent)

    uninstall = subparsers.add_parser("uninstall-launchagent")
    uninstall.set_defaults(func=cmd_uninstall_launchagent)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

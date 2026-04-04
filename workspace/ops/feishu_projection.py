#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import urllib.parse
from pathlib import Path
from typing import Any

try:
    from ops import feishu_agent, runtime_state
    from ops.codex_memory import (
        NEXT_ACTIONS_MD,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        load_project_board,
        load_registry as load_project_registry,
        load_topic_board,
        project_board_facts,
        project_board_path,
        topic_board_paths,
        workspace_lock,
    )
except ImportError:  # pragma: no cover
    import feishu_agent  # type: ignore
    import runtime_state  # type: ignore
    from codex_memory import (  # type: ignore
        NEXT_ACTIONS_MD,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        load_project_board,
        load_registry as load_project_registry,
        load_topic_board,
        project_board_facts,
        project_board_path,
        topic_board_paths,
        workspace_lock,
    )


BRIDGE_NAME = "feishu"
SCHEMA_VERSION = "feishu-projection.v2"
QUEUE_NAME = "feishu_projection_sync"
PROJECTION_SYNC_NAME = "com.codexhub.codex-feishu-projection-sync"
DEFAULT_WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
STATE_PATH = DEFAULT_WORKSPACE_ROOT / "runtime" / "feishu-projection-state.json"
LOG_STDOUT = DEFAULT_WORKSPACE_ROOT / "logs" / "feishu-projection-sync.log"
LOG_STDERR = DEFAULT_WORKSPACE_ROOT / "logs" / "feishu-projection-sync.err.log"
PROJECTS_TABLE_KEY = "projects_overview"
TASKS_TABLE_KEY = "tasks_current"
OVERVIEW_TABLE_KEY = "operations_overview"
TABLE_KEYS = (PROJECTS_TABLE_KEY, TASKS_TABLE_KEY, OVERVIEW_TABLE_KEY)

DEFAULT_PROJECTION_CONFIG: dict[str, Any] = {
    "app": {
        "alias": "codex_hub_projection",
        "name": "Codex Hub 项目任务看板",
        "app_token": "",
        "folder_token": "",
    },
    "tables": {
        PROJECTS_TABLE_KEY: {
            "alias": "codex_hub_projects_overview",
            "name": "项目总览",
            "table_id": "",
            "default_view_name": "全部项目",
        },
        TASKS_TABLE_KEY: {
            "alias": "codex_hub_tasks_current",
            "name": "当前任务",
            "table_id": "",
            "default_view_name": "全部任务",
        },
        OVERVIEW_TABLE_KEY: {
            "alias": "codex_hub_operations_overview",
            "name": "运营总览",
            "table_id": "",
            "default_view_name": "全部指标",
        },
    },
    "views": {
        PROJECTS_TABLE_KEY: [
            {"name": "全部项目", "type": "grid", "hidden_fields_by_name": ["projection_key", "NEXT_ACTIONS 链接"]},
            {"name": "状态看板", "type": "kanban"},
            {
                "name": "高优先级项目",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "优先级", "operator": "is", "value": "[\"high\"]"}],
                },
                "hidden_fields_by_name": ["projection_key", "NEXT_ACTIONS 链接"],
            },
            {
                "name": "需关注项目",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "需关注", "operator": "isChecked", "value": "true"}],
                },
                "hidden_fields_by_name": ["projection_key", "NEXT_ACTIONS 链接"],
            },
            {
                "name": "阻塞项目",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "状态", "operator": "is", "value": "[\"blocked\"]"}],
                },
                "hidden_fields_by_name": ["projection_key", "NEXT_ACTIONS 链接"],
            },
        ],
        TASKS_TABLE_KEY: [
            {"name": "全部任务", "type": "grid", "hidden_fields_by_name": ["projection_key", "来源板链接"]},
            {"name": "状态看板", "type": "kanban"},
            {
                "name": "项目任务表",
                "type": "grid",
                "hidden_fields_by_name": ["projection_key", "来源板链接", "任务 ID"],
            },
            {
                "name": "Doing任务",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "状态", "operator": "is", "value": "[\"doing\"]"}],
                },
                "hidden_fields_by_name": ["projection_key", "来源板链接"],
            },
            {
                "name": "阻塞项",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "状态", "operator": "is", "value": "[\"blocked\"]"}],
                },
                "hidden_fields_by_name": ["projection_key", "来源板链接"],
            },
        ],
        OVERVIEW_TABLE_KEY: [
            {"name": "全部指标", "type": "grid", "hidden_fields_by_name": ["projection_key"]},
            {"name": "数字卡片", "type": "gallery"},
            {
                "name": "重点指标",
                "type": "grid",
                "filter": {
                    "conjunction": "and",
                    "conditions": [{"field_name": "是否重点", "operator": "isChecked", "value": "true"}],
                },
                "hidden_fields_by_name": ["projection_key"],
            },
        ],
    },
}

PROJECT_OVERVIEW_FIELDS: list[dict[str, Any]] = [
    {"field_name": "项目名", "type": 1},
    {
        "field_name": "状态",
        "type": 3,
        "property": {"options": [{"name": "active"}, {"name": "blocked"}, {"name": "done"}]},
    },
    {
        "field_name": "优先级",
        "type": 3,
        "property": {"options": [{"name": "high"}, {"name": "medium"}, {"name": "low"}]},
    },
    {"field_name": "当前下一步", "type": 1},
    {"field_name": "最近更新时间", "type": 1},
    {"field_name": "活跃专题数", "type": 2},
    {"field_name": "未完成任务数", "type": 2},
    {"field_name": "阻塞任务数", "type": 2},
    {"field_name": "需关注", "type": 7},
    {"field_name": "projection_key", "type": 1},
    {"field_name": "项目板链接", "type": 1},
    {"field_name": "NEXT_ACTIONS 链接", "type": 1},
]

CURRENT_TASK_FIELDS: list[dict[str, Any]] = [
    {"field_name": "任务标题", "type": 1},
    {"field_name": "项目", "type": 1},
    {"field_name": "专题", "type": 1},
    {"field_name": "任务 ID", "type": 1},
    {
        "field_name": "状态",
        "type": 3,
        "property": {"options": [{"name": "todo"}, {"name": "doing"}, {"name": "blocked"}, {"name": "done"}]},
    },
    {
        "field_name": "优先级",
        "type": 3,
        "property": {"options": [{"name": "high"}, {"name": "medium"}, {"name": "low"}]},
    },
    {"field_name": "下一步", "type": 1},
    {"field_name": "是否阻塞", "type": 7},
    {"field_name": "更新时间", "type": 1},
    {"field_name": "projection_key", "type": 1},
    {"field_name": "来源板链接", "type": 1},
]

OPERATIONS_OVERVIEW_FIELDS: list[dict[str, Any]] = [
    {"field_name": "指标名称", "type": 1},
    {
        "field_name": "指标分组",
        "type": 3,
        "property": {"options": [{"name": "项目"}, {"name": "任务"}, {"name": "更新"}]},
    },
    {"field_name": "数值", "type": 2},
    {"field_name": "说明", "type": 1},
    {"field_name": "目标视图", "type": 1},
    {"field_name": "目标链接", "type": 1},
    {"field_name": "是否重点", "type": 7},
    {"field_name": "更新时间", "type": 1},
    {"field_name": "projection_key", "type": 1},
]

TABLE_FIELD_DEFS: dict[str, list[dict[str, Any]]] = {
    PROJECTS_TABLE_KEY: PROJECT_OVERVIEW_FIELDS,
    TASKS_TABLE_KEY: CURRENT_TASK_FIELDS,
    OVERVIEW_TABLE_KEY: OPERATIONS_OVERVIEW_FIELDS,
}


def workspace_root() -> Path:
    explicit = str(os.environ.get("WORKSPACE_HUB_ROOT", "")).strip()
    return Path(explicit) if explicit else DEFAULT_WORKSPACE_ROOT


def projection_state_path() -> Path:
    explicit = str(os.environ.get("WORKSPACE_HUB_FEISHU_PROJECTION_STATE", "")).strip()
    return Path(explicit) if explicit else workspace_root() / "runtime" / "feishu-projection-state.json"


def _status(value: Any) -> str:
    return str(value or "").strip().lower()


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _ensure_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def obsidian_url(path: str | Path) -> str:
    target = str(path or "").strip()
    if not target:
        return ""
    return "obsidian://open?path=" + urllib.parse.quote(target, safe="")


def _merge_projection_defaults(registry: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(registry, ensure_ascii=False))
    projection = payload.setdefault("projection", {})
    app = projection.setdefault("app", {})
    for key, value in DEFAULT_PROJECTION_CONFIG["app"].items():
        app.setdefault(key, value)
    tables = projection.setdefault("tables", {})
    for table_key, defaults in DEFAULT_PROJECTION_CONFIG["tables"].items():
        table = tables.setdefault(table_key, {})
        for key, value in defaults.items():
            table.setdefault(key, value)
    projection["views"] = json.loads(json.dumps(DEFAULT_PROJECTION_CONFIG["views"], ensure_ascii=False))
    aliases = payload.setdefault("aliases", {})
    aliases.setdefault("tables", {})
    return payload


def load_projection_registry() -> dict[str, Any]:
    return _merge_projection_defaults(feishu_agent.load_registry())


def save_projection_registry(payload: dict[str, Any]) -> Path:
    normalized = _merge_projection_defaults(payload)
    return feishu_agent.save_registry(normalized)


def projection_contract() -> dict[str, Any]:
    runtime_contract = runtime_state.feishu_runtime_contract()
    return {
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "projection_mode": "read_only",
        "truth_source": runtime_contract["truth_source"],
        "bitable_mode": runtime_contract["bitable_mode"],
        "allowed_write_tables": runtime_contract["writable_tables"],
        "reserved_tables": runtime_contract["reserved_tables"],
        "read_only_tables": runtime_contract["read_only_tables"],
        "queue_name": QUEUE_NAME,
        "tables": {
            PROJECTS_TABLE_KEY: {"fields": [item["field_name"] for item in PROJECT_OVERVIEW_FIELDS]},
            TASKS_TABLE_KEY: {"fields": [item["field_name"] for item in CURRENT_TASK_FIELDS]},
            OVERVIEW_TABLE_KEY: {"fields": [item["field_name"] for item in OPERATIONS_OVERVIEW_FIELDS]},
        },
    }


def _topic_sources(project_name: str) -> dict[str, dict[str, str]]:
    sources: dict[str, dict[str, str]] = {}
    for path in topic_board_paths(project_name):
        topic_board = load_topic_board(path)
        topic_name = _text(topic_board["frontmatter"].get("topic_name", "")) or path.stem
        sources[topic_name] = {"path": str(path), "topic_name": topic_name}
    return sources


def _topic_metrics(project_name: str) -> tuple[int, dict[str, dict[str, str]]]:
    active = 0
    sources = _topic_sources(project_name)
    for item in sources.values():
        board = load_topic_board(Path(item["path"]))
        if any(_status(row.get("状态")) != "done" for row in board.get("rows", [])):
            active += 1
    return active, sources


def _task_rows_for_fact(fact: dict[str, Any], topic_sources: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    project_name = _text(fact.get("project_name", ""))
    project_priority = _text(fact.get("priority", "medium")) or "medium"
    project_link = obsidian_url(fact.get("board_path", ""))
    for row in list(fact.get("project_rows", [])) + list(fact.get("rollup_rows", [])):
        task_id = _text(row.get("ID", ""))
        status = _status(row.get("状态", "todo")) or "todo"
        if not task_id or status == "done":
            continue
        source = _text(row.get("来源", ""))
        topic_name = ""
        source_link = project_link
        if source.startswith("topic:"):
            topic_name = source.split(":", 1)[1].strip()
            source_meta = topic_sources.get(topic_name)
            if source_meta:
                source_link = obsidian_url(source_meta["path"])
        rows.append(
            {
                "projection_key": f"task::{project_name}::{source or 'project'}::{task_id}",
                "项目": project_name,
                "专题": topic_name,
                "任务 ID": task_id,
                "任务标题": _text(row.get("事项", "")),
                "状态": status,
                "优先级": project_priority,
                "下一步": _text(row.get("下一步", "")),
                "是否阻塞": status == "blocked",
                "更新时间": _text(row.get("更新时间", "")) or _text(fact.get("updated_at", "")),
                "来源板链接": source_link,
            }
        )
    return rows


def build_project_rows(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fact in facts:
        project_name = _text(fact.get("project_name", ""))
        combined_rows = list(fact.get("project_rows", [])) + list(fact.get("rollup_rows", []))
        unfinished = sum(1 for row in combined_rows if _status(row.get("状态", "")) != "done")
        blocked = sum(1 for row in combined_rows if _status(row.get("状态", "")) == "blocked")
        active_topics, _sources = _topic_metrics(project_name)
        rows.append(
            {
                "projection_key": f"project::{project_name}",
                "项目名": project_name,
                "状态": _text(fact.get("status", "active")) or "active",
                "优先级": _text(fact.get("priority", "medium")) or "medium",
                "当前下一步": _text(fact.get("next_action", "")),
                "最近更新时间": _text(fact.get("updated_at", "")),
                "活跃专题数": active_topics,
                "未完成任务数": unfinished,
                "阻塞任务数": blocked,
                "需关注": blocked > 0,
                "项目板链接": obsidian_url(fact.get("board_path", "")),
                "NEXT_ACTIONS 链接": obsidian_url(NEXT_ACTIONS_MD),
            }
        )
    return rows


def build_task_rows(facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for fact in facts:
        topic_sources = _topic_sources(_text(fact.get("project_name", "")))
        rows.extend(_task_rows_for_fact(fact, topic_sources))
    rows.sort(key=lambda item: (item["项目"], item["状态"], item["专题"], item["任务 ID"]))
    return rows


def _parse_timestamp(value: Any) -> dt.datetime | None:
    raw = _text(value)
    if not raw:
        return None
    for candidate in (raw, raw.replace("Z", "+00:00")):
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return None


def _bitable_view_url(app_token: str, table_id: str, view_id: str = "") -> str:
    app = _text(app_token)
    table = _text(table_id)
    view = _text(view_id)
    if not app or not table:
        return ""
    params = {"table": table}
    if view:
        params["view"] = view
    return "https://feishu.cn/base/" + urllib.parse.quote(app, safe="") + "?" + urllib.parse.urlencode(params)


def build_operations_overview_rows(
    facts: list[dict[str, Any]],
    *,
    target_links: dict[str, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    project_rows = build_project_rows(facts)
    all_task_rows: list[dict[str, Any]] = []
    for fact in facts:
        all_task_rows.extend(list(fact.get("project_rows", [])))
        all_task_rows.extend(list(fact.get("rollup_rows", [])))
    status_counts = {"todo": 0, "doing": 0, "blocked": 0, "done": 0}
    recent_updates = 0
    recent_cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)
    for row in all_task_rows:
        status = _status(row.get("状态", "todo")) or "todo"
        if status not in status_counts:
            status = "todo"
        status_counts[status] += 1
        updated_at = _parse_timestamp(row.get("更新时间", ""))
        if updated_at and updated_at >= recent_cutoff:
            recent_updates += 1
    targets = target_links or {}
    metrics = [
        {
            "projection_key": "overview::total_projects",
            "指标名称": "项目总数",
            "指标分组": "项目",
            "数值": len(project_rows),
            "说明": "当前投影中的项目总数",
            "目标视图": "全部项目",
            "目标链接": _text(targets.get(PROJECTS_TABLE_KEY, {}).get("全部项目", "")),
            "是否重点": True,
        },
        {
            "projection_key": "overview::focused_projects",
            "指标名称": "需关注项目",
            "指标分组": "项目",
            "数值": sum(1 for row in project_rows if bool(row.get("需关注"))),
            "说明": "阻塞或需重点跟进的项目数",
            "目标视图": "需关注项目",
            "目标链接": _text(targets.get(PROJECTS_TABLE_KEY, {}).get("需关注项目", "")),
            "是否重点": True,
        },
        {
            "projection_key": "overview::active_tasks",
            "指标名称": "当前任务总数",
            "指标分组": "任务",
            "数值": status_counts["todo"] + status_counts["doing"] + status_counts["blocked"],
            "说明": "当前未完成任务总数",
            "目标视图": "全部任务",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("全部任务", "")),
            "是否重点": True,
        },
        {
            "projection_key": "overview::todo_tasks",
            "指标名称": "Todo",
            "指标分组": "任务",
            "数值": status_counts["todo"],
            "说明": "待开始任务数",
            "目标视图": "状态看板",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("状态看板", "")),
            "是否重点": False,
        },
        {
            "projection_key": "overview::doing_tasks",
            "指标名称": "Doing",
            "指标分组": "任务",
            "数值": status_counts["doing"],
            "说明": "执行中任务数",
            "目标视图": "Doing任务",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("Doing任务", "")),
            "是否重点": True,
        },
        {
            "projection_key": "overview::blocked_tasks",
            "指标名称": "Blocked",
            "指标分组": "任务",
            "数值": status_counts["blocked"],
            "说明": "阻塞任务数",
            "目标视图": "阻塞项",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("阻塞项", "")),
            "是否重点": True,
        },
        {
            "projection_key": "overview::done_tasks",
            "指标名称": "Done",
            "指标分组": "任务",
            "数值": status_counts["done"],
            "说明": "已完成任务数",
            "目标视图": "全部任务",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("全部任务", "")),
            "是否重点": False,
        },
        {
            "projection_key": "overview::recent_updates",
            "指标名称": "近 7 天更新",
            "指标分组": "更新",
            "数值": recent_updates,
            "说明": "近 7 天有更新时间的任务数",
            "目标视图": "全部任务",
            "目标链接": _text(targets.get(TASKS_TABLE_KEY, {}).get("全部任务", "")),
            "是否重点": False,
        },
    ]
    now_text = iso_now()
    for item in metrics:
        item["更新时间"] = now_text
    return metrics


def _attach_overview_target_links(
    rows: list[dict[str, Any]], *, target_links: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        view_name = _text(item.get("目标视图", ""))
        for table_key in (PROJECTS_TABLE_KEY, TASKS_TABLE_KEY, OVERVIEW_TABLE_KEY):
            link = _text(target_links.get(table_key, {}).get(view_name, ""))
            if link:
                item["目标链接"] = link
                break
        enriched.append(item)
    return enriched


def _filter_facts(project_name: str = "") -> tuple[list[dict[str, Any]], list[str]]:
    facts, errors = project_board_facts(load_project_registry())
    if not project_name:
        return facts, errors
    filtered = [item for item in facts if _text(item.get("project_name", "")).lower() == project_name.lower()]
    return filtered, errors


def snapshot(project_name: str = "") -> dict[str, Any]:
    facts, errors = _filter_facts(project_name)
    project_rows = build_project_rows(facts)
    task_rows = build_task_rows(facts)
    overview_rows = build_operations_overview_rows(facts)
    return {
        "ok": not errors,
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso_now(),
        "project_name": project_name,
        "contract": projection_contract(),
        "row_counts": {
            PROJECTS_TABLE_KEY: len(project_rows),
            TASKS_TABLE_KEY: len(task_rows),
            OVERVIEW_TABLE_KEY: len(overview_rows),
        },
        "projects_overview_rows": project_rows,
        "tasks_current_rows": task_rows,
        "operations_overview_rows": overview_rows,
        "errors": errors,
    }


def _preview_rows(rows: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    return rows[: max(1, int(limit or 5))]


def bitable_target_status(project_name: str = "") -> dict[str, Any]:
    registry = load_projection_registry()
    projection = registry["projection"]
    app_cfg = projection["app"]
    table_cfg = projection["tables"]
    snapshot_payload = snapshot(project_name=project_name)
    return {
        "ok": True,
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "project_name": project_name,
        "bitable_mode": "read_only_projection",
        "app": {
            "alias": app_cfg.get("alias", ""),
            "name": app_cfg.get("name", ""),
            "configured": bool(_text(app_cfg.get("app_token", ""))),
            "app_token": _text(app_cfg.get("app_token", "")),
            "folder_token": _text(app_cfg.get("folder_token", "")),
        },
        "tables": {
            table_key: {
                "alias": _text(table_cfg[table_key].get("alias", "")),
                "name": _text(table_cfg[table_key].get("name", "")),
                "configured": bool(_text(table_cfg[table_key].get("table_id", ""))),
                "table_id": _text(table_cfg[table_key].get("table_id", "")),
                "default_view_name": _text(table_cfg[table_key].get("default_view_name", "")),
                "view_names": [item.get("name", "") for item in projection.get("views", {}).get(table_key, [])],
                "row_count": int(snapshot_payload["row_counts"].get(table_key, 0)),
            }
            for table_key in TABLE_KEYS
        },
    }


def bitable_publish_preview(project_name: str = "") -> dict[str, Any]:
    payload = snapshot(project_name=project_name)
    target_status = bitable_target_status(project_name=project_name)
    overview_rows = list(payload.get("operations_overview_rows") or [])
    return {
        "ok": payload["ok"],
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "project_name": project_name,
        "mode": "preview_only",
        "target_status": target_status,
        "preview_counts": payload["row_counts"],
        "preview_rows": {
            PROJECTS_TABLE_KEY: _preview_rows(payload["projects_overview_rows"]),
            TASKS_TABLE_KEY: _preview_rows(payload["tasks_current_rows"]),
            OVERVIEW_TABLE_KEY: _preview_rows(overview_rows),
        },
    }


def _normalize_table_aliases(registry: dict[str, Any], *, app_token: str, table_key: str, table_id: str) -> None:
    aliases = registry.setdefault("aliases", {}).setdefault("tables", {})
    projection = registry["projection"]
    app_alias = _text(projection["app"].get("alias", ""))
    table_alias = _text(projection["tables"][table_key].get("alias", ""))
    if app_alias:
        aliases[app_alias] = {"app_token": app_token}
    if table_alias:
        aliases[table_alias] = {"app_token": app_token, "table_id": table_id}


def _table_field_defs(table_key: str) -> list[dict[str, Any]]:
    return TABLE_FIELD_DEFS[table_key]


def _desired_field_names(table_key: str) -> list[str]:
    return [_text(item.get("field_name", "")) for item in _table_field_defs(table_key) if _text(item.get("field_name", ""))]


def _table_fields(agent: feishu_agent.FeishuAgent, *, app_token: str, table_id: str) -> list[dict[str, Any]]:
    return list(agent.table_fields({"app": app_token, "table": table_id}).get("fields", []))


def _field_name_map(fields: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        _text(item.get("field_name", "")): item
        for item in fields
        if _text(item.get("field_name", ""))
    }


def _table_rebuild_reasons(table_key: str, *, table_name: str, fields: list[dict[str, Any]], desired_name: str) -> list[str]:
    reasons: list[str] = []
    if _text(table_name) != desired_name:
        reasons.append("table_name")
    desired_names = _desired_field_names(table_key)
    existing_names = [_text(item.get("field_name", "")) for item in fields if _text(item.get("field_name", ""))]
    if not existing_names:
        reasons.append("missing_fields")
        return reasons
    if existing_names[: len(desired_names)] != desired_names or len(existing_names) != len(desired_names):
        reasons.append("field_layout")
    primary_name = next((_text(item.get("field_name", "")) for item in fields if bool(item.get("is_primary"))), "")
    if desired_names and primary_name != desired_names[0]:
        reasons.append("primary_field")
    return sorted(set(reasons))


def _build_view_property(view: dict[str, Any], field_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    def _normalize_filter_operator(operator: Any) -> str:
        op = _text(operator)
        if op == "isChecked":
            return "is"
        if op == "isUnchecked":
            return "isNot"
        return op

    def _normalize_filter_value(field_meta: dict[str, Any], value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = value
            else:
                parsed = value
        else:
            parsed = _clone_json(value)
        field_type = int(field_meta.get("type") or 0)
        if field_type == 3:
            option_by_name = {
                _text(option.get("name", "")): _text(option.get("id", ""))
                for option in list(_ensure_dict(field_meta.get("property")).get("options") or [])
                if _text(option.get("name", "")) and _text(option.get("id", ""))
            }
            if isinstance(parsed, list):
                raw_values = parsed
            else:
                raw_values = [parsed]
            option_ids: list[str] = []
            for item in raw_values:
                item_text = _text(item)
                if not item_text:
                    continue
                option_ids.append(option_by_name.get(item_text, item_text))
            return json.dumps(option_ids, ensure_ascii=False)
        if field_type == 7:
            if isinstance(parsed, list) and len(parsed) == 1:
                parsed = parsed[0]
            if isinstance(parsed, str):
                lowered = parsed.strip().lower()
                if lowered == "true":
                    return True
                if lowered == "false":
                    return False
        return parsed

    property_payload: dict[str, Any] = {}
    hidden_field_ids: list[str] = []
    for field_name in list(view.get("hidden_fields_by_name") or []):
        field_meta = field_map.get(_text(field_name))
        if field_meta:
            hidden_field_ids.append(_text(field_meta.get("field_id", "")))
    if hidden_field_ids:
        property_payload["hidden_fields"] = [field_id for field_id in hidden_field_ids if field_id]
    groups_payload: list[dict[str, Any]] = []
    for group in list(view.get("groups") or []):
        field_meta = field_map.get(_text(group.get("field_name", "")))
        if field_meta:
            groups_payload.append({"field_id": _text(field_meta.get("field_id", "")), "desc": bool(group.get("desc", False))})
    if groups_payload:
        property_payload["group_info"] = groups_payload
    sorts_payload: list[dict[str, Any]] = []
    for sort in list(view.get("sorts") or []):
        field_meta = field_map.get(_text(sort.get("field_name", "")))
        if field_meta:
            sorts_payload.append({"field_id": _text(field_meta.get("field_id", "")), "desc": bool(sort.get("desc", False))})
    if sorts_payload:
        property_payload["sort_info"] = sorts_payload
    filter_def = view.get("filter")
    if isinstance(filter_def, dict):
        conditions_payload: list[dict[str, Any]] = []
        for condition in list(filter_def.get("conditions") or []):
            field_meta = field_map.get(_text(condition.get("field_name", "")))
            if not field_meta:
                continue
            conditions_payload.append(
                {
                    "field_id": _text(field_meta.get("field_id", "")),
                    "operator": _normalize_filter_operator(condition.get("operator", "")),
                    "value": _normalize_filter_value(field_meta, condition.get("value")),
                }
            )
        if conditions_payload:
            property_payload["filter_info"] = {
                "conjunction": _text(filter_def.get("conjunction", "and")) or "and",
                "conditions": conditions_payload,
            }
    return property_payload or None


def _ensure_table_schema(agent: feishu_agent.FeishuAgent, *, app_token: str, table_id: str, table_key: str) -> dict[str, dict[str, Any]]:
    field_defs = _table_field_defs(table_key)
    fields = _table_fields(agent, app_token=app_token, table_id=table_id)
    existing_by_name = _field_name_map(fields)
    for field in field_defs:
        field_name = _text(field.get("field_name", ""))
        if not field_name:
            continue
        existing = existing_by_name.get(field_name)
        if existing is None:
            agent.table_create_field({"app": app_token, "table": table_id, "field": field})
            fields = _table_fields(agent, app_token=app_token, table_id=table_id)
            existing_by_name = _field_name_map(fields)
    return existing_by_name


def _ensure_table_views(
    agent: feishu_agent.FeishuAgent,
    *,
    app_token: str,
    table_id: str,
    table_key: str,
    view_defs: list[dict[str, Any]],
    field_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    existing = list(agent.table_views({"app": app_token, "table": table_id}).get("views", []))
    existing_by_name = {str(item.get("view_name") or "").strip(): item for item in existing if str(item.get("view_name") or "").strip()}
    desired_names = {_text(item.get("name", "")) for item in view_defs if _text(item.get("name", ""))}
    created_or_updated: list[str] = []
    property_attempted = False
    property_applied = True
    for view in view_defs:
        view_name = _text(view.get("name", ""))
        view_type = _text(view.get("type", "grid")) or "grid"
        current = existing_by_name.get(view_name)
        if current is None:
            created = agent.table_create_view({"app": app_token, "table": table_id, "name": view_name, "type": view_type})
            current = _ensure_dict(created.get("view"))
            existing_by_name[view_name] = current
            created_or_updated.append(view_name)
        elif _text(current.get("view_type", "grid")) != view_type:
            view_id = _text(current.get("view_id", ""))
            if view_id:
                agent.table_delete_view({"app": app_token, "table": table_id, "view": view_id})
            recreated = agent.table_create_view({"app": app_token, "table": table_id, "name": view_name, "type": view_type})
            current = _ensure_dict(recreated.get("view")) or current
            existing_by_name[view_name] = current
            created_or_updated.append(view_name)
        property_payload = _build_view_property(view, field_map)
        if property_payload:
            property_attempted = True
            agent.table_update_view(
                {
                    "app": app_token,
                    "table": table_id,
                    "view": _text(current.get("view_id", "")),
                    "property": property_payload,
                }
            )
            refreshed = agent.table_get_view({"app": app_token, "table": table_id, "view": _text(current.get("view_id", ""))})
            current = _ensure_dict(refreshed.get("view")) or current
            existing_by_name[view_name] = current
            if _ensure_dict(current.get("property")) != property_payload:
                property_applied = False
    stale_views = [item for item in existing_by_name.values() if _text(item.get("view_name", "")) not in desired_names]
    for stale in stale_views:
        view_id = _text(stale.get("view_id", ""))
        if view_id:
            agent.table_delete_view({"app": app_token, "table": table_id, "view": view_id})
    final_views = list(agent.table_views({"app": app_token, "table": table_id}).get("views", []))
    final_by_name = {str(item.get("view_name") or "").strip(): item for item in final_views if str(item.get("view_name") or "").strip()}
    return {
        "view_ids_by_name": {name: _text(item.get("view_id", "")) for name, item in final_by_name.items()},
        "created_or_updated": created_or_updated,
        "property_attempted": property_attempted,
        "property_applied": property_applied,
    }


def _find_table_by_name(tables: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    target = _text(name)
    for item in tables:
        if _text(item.get("name", "")) == target:
            return item
    return None


def _delete_table_if_exists(agent: feishu_agent.FeishuAgent, *, app_token: str, table_id: str) -> None:
    target = _text(table_id)
    if not target:
        return
    try:
        agent.table_delete_table({"app": app_token, "table": target})
    except Exception:
        return


def ensure_projection_resources() -> dict[str, Any]:
    registry = load_projection_registry()
    projection = registry["projection"]
    app_cfg = projection["app"]
    table_cfg = projection["tables"]
    agent = feishu_agent.FeishuAgent()
    changed = False

    app_token = _text(app_cfg.get("app_token", ""))
    if not app_token:
        created = agent.table_create_app(
            {
                "name": _text(app_cfg.get("name", "")) or DEFAULT_PROJECTION_CONFIG["app"]["name"],
                "folder_token": _text(app_cfg.get("folder_token", "")),
            }
        )
        app_token = _text(created.get("app_token", ""))
        app_cfg["app_token"] = app_token
        changed = True
    keep_table_ids: set[str] = set()
    table_runtime: dict[str, dict[str, Any]] = {}
    tables = list(agent.table_tables({"app": app_token}).get("tables", []))

    for table_key in TABLE_KEYS:
        desired_name = _text(table_cfg[table_key].get("name", ""))
        table_id = _text(table_cfg[table_key].get("table_id", ""))
        table_meta = next((item for item in tables if _text(item.get("table_id", "")) == table_id), None) if table_id else None
        if table_meta is None:
            table_meta = _find_table_by_name(tables, desired_name)
            if table_meta:
                table_id = _text(table_meta.get("table_id", ""))
        rebuild_reasons: list[str] = []
        if table_meta is not None:
            fields = _table_fields(agent, app_token=app_token, table_id=table_id)
            rebuild_reasons = _table_rebuild_reasons(
                table_key,
                table_name=_text(table_meta.get("name", "")),
                fields=fields,
                desired_name=desired_name,
            )
        if table_meta is None or rebuild_reasons:
            if table_meta is not None and table_id:
                _delete_table_if_exists(agent, app_token=app_token, table_id=table_id)
                tables = list(agent.table_tables({"app": app_token}).get("tables", []))
            created = agent.table_create(
                {
                    "app": app_token,
                    "name": desired_name,
                    "default_view_name": _text(table_cfg[table_key].get("default_view_name", "")),
                    "fields": _table_field_defs(table_key),
                }
            )
            table_id = _text(created.get("table_id", ""))
            table_meta = {"table_id": table_id, "name": desired_name}
            tables = list(agent.table_tables({"app": app_token}).get("tables", []))
            changed = True
        table_cfg[table_key]["table_id"] = table_id
        _normalize_table_aliases(registry, app_token=app_token, table_key=table_key, table_id=table_id)
        field_map = _ensure_table_schema(agent, app_token=app_token, table_id=table_id, table_key=table_key)
        view_result = _ensure_table_views(
            agent,
            app_token=app_token,
            table_id=table_id,
            table_key=table_key,
            view_defs=projection.get("views", {}).get(table_key, []),
            field_map=field_map,
        )
        keep_table_ids.add(table_id)
        table_runtime[table_key] = {
            "table_id": table_id,
            "name": desired_name,
            "views": [_text(item.get("name", "")) for item in projection.get("views", {}).get(table_key, [])],
            "view_ids_by_name": view_result["view_ids_by_name"],
            "rebuild_reasons": rebuild_reasons,
            "view_property_supported": bool(view_result["property_applied"] or not view_result["property_attempted"]),
        }

    tables = list(agent.table_tables({"app": app_token}).get("tables", []))
    for table in tables:
        stale_table_id = _text(table.get("table_id", ""))
        if stale_table_id and stale_table_id not in keep_table_ids:
            _delete_table_if_exists(agent, app_token=app_token, table_id=stale_table_id)
            changed = True

    if changed:
        save_projection_registry(registry)
    else:
        # Ensure alias backfill persists even when ids were already present.
        save_projection_registry(registry)
    return {
        "registry_path": str(feishu_agent.default_registry_path()),
        "app_token": app_token,
        "tables": table_runtime,
    }


def _fetch_all_records(agent: feishu_agent.FeishuAgent, *, app_token: str, table_id: str) -> list[dict[str, Any]]:
    page_token = ""
    records: list[dict[str, Any]] = []
    while True:
        result = agent.table_records({"app": app_token, "table": table_id, "limit": 200, "page_token": page_token})
        records.extend(list(result.get("records", [])))
        if not result.get("has_more") or not result.get("page_token"):
            break
        page_token = str(result.get("page_token") or "").strip()
    return records


def _record_fields(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("fields", {})
    return value if isinstance(value, dict) else {}


def _normalize_fields_for_compare(fields: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(fields, ensure_ascii=False, sort_keys=True))


def _clone_json(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _sync_table_rows(
    agent: feishu_agent.FeishuAgent,
    *,
    app_token: str,
    table_id: str,
    desired_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    existing_records = _fetch_all_records(agent, app_token=app_token, table_id=table_id)
    existing_by_key: dict[str, dict[str, Any]] = {}
    for record in existing_records:
        fields = _record_fields(record)
        projection_key = _text(fields.get("projection_key", ""))
        if projection_key:
            existing_by_key[projection_key] = record
    desired_by_key = {str(item["projection_key"]): item for item in desired_rows if _text(item.get("projection_key", ""))}
    created = 0
    updated = 0
    unchanged = 0
    deleted = 0
    for key, desired in desired_by_key.items():
        existing = existing_by_key.get(key)
        if existing is None:
            agent.table_add({"app": app_token, "table": table_id, "data": desired})
            created += 1
            continue
        existing_fields = _normalize_fields_for_compare(_record_fields(existing))
        desired_fields = _normalize_fields_for_compare(desired)
        if existing_fields == desired_fields:
            unchanged += 1
            continue
        agent.table_update(
            {
                "app": app_token,
                "table": table_id,
                "record": _text(existing.get("record_id", "")),
                "data": desired,
            }
        )
        updated += 1
    for key, existing in existing_by_key.items():
        if key in desired_by_key:
            continue
        agent.table_delete({"app": app_token, "table": table_id, "record": _text(existing.get("record_id", ""))})
        deleted += 1
    return {
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
        "desired": len(desired_by_key),
        "existing": len(existing_by_key),
    }


def load_state() -> dict[str, Any]:
    target = projection_state_path()
    if not target.exists():
        return {
            "version": 1,
            "updated_at": "",
            "last_sync_at": "",
            "last_status": "never-run",
            "last_error": "",
        }
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "version": 1,
            "updated_at": "",
            "last_sync_at": "",
            "last_status": "invalid-state",
            "last_error": "invalid_json",
        }
    return payload if isinstance(payload, dict) else {}


def save_state(payload: dict[str, Any]) -> None:
    target = projection_state_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(payload)
    payload["updated_at"] = iso_now()
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _claim_projection_events(limit: int = 200) -> list[dict[str, Any]]:
    return runtime_state.claim_runtime_events(
        queue_name=QUEUE_NAME,
        claimed_by="feishu_projection.run_sync",
        limit=limit,
        lease_seconds=1800,
        event_types=["project_writeback"],
    )


def run_sync(*, force_full: bool = False, project_name: str = "") -> dict[str, Any]:
    claimed_events = _claim_projection_events() if not project_name else []
    try:
        # Only hold the workspace lock while reading the local truth source.
        # The Feishu sync itself is network-heavy and must not block writeback/finalize paths.
        with workspace_lock():
            payload = snapshot(project_name=project_name)
        resources = ensure_projection_resources()
        target_links = {
            table_key: {
                view_name: _bitable_view_url(
                    resources["app_token"],
                    _text(resources["tables"][table_key]["table_id"]),
                    _text(view_id),
                )
                for view_name, view_id in dict(resources["tables"][table_key].get("view_ids_by_name", {})).items()
            }
            for table_key in TABLE_KEYS
        }
        payload["operations_overview_rows"] = _attach_overview_target_links(
            list(payload.get("operations_overview_rows") or []),
            target_links=target_links,
        )
        payload["row_counts"][OVERVIEW_TABLE_KEY] = len(payload["operations_overview_rows"])
        agent = feishu_agent.FeishuAgent()
        projects_table_id = resources["tables"][PROJECTS_TABLE_KEY]["table_id"]
        tasks_table_id = resources["tables"][TASKS_TABLE_KEY]["table_id"]
        overview_table_id = resources["tables"][OVERVIEW_TABLE_KEY]["table_id"]
        app_token = resources["app_token"]
        projects_result = _sync_table_rows(
            agent,
            app_token=app_token,
            table_id=projects_table_id,
            desired_rows=payload["projects_overview_rows"],
        )
        tasks_result = _sync_table_rows(
            agent,
            app_token=app_token,
            table_id=tasks_table_id,
            desired_rows=payload["tasks_current_rows"],
        )
        overview_result = _sync_table_rows(
            agent,
            app_token=app_token,
            table_id=overview_table_id,
            desired_rows=payload["operations_overview_rows"],
        )
        result = {
            "status": "ok",
            "schema_version": SCHEMA_VERSION,
            "project_name": project_name,
            "trigger": "force_full" if force_full else ("queue" if claimed_events else "reconcile"),
            "claimed_events": len(claimed_events),
            "app_token": app_token,
            "tables": {
                PROJECTS_TABLE_KEY: projects_result,
                TASKS_TABLE_KEY: tasks_result,
                OVERVIEW_TABLE_KEY: overview_result,
            },
            "row_counts": payload["row_counts"],
            "errors": payload.get("errors", []),
            "target_links": target_links,
            "synced_at": iso_now(),
        }
        save_state(
            {
                "version": 1,
                "last_sync_at": result["synced_at"],
                "last_status": "ok",
                "last_error": "",
                "last_result": result,
            }
        )
        for event in claimed_events:
            runtime_state.complete_runtime_event(
                event.get("event_key", ""),
                claim_token=_text(event.get("claim_token", "")),
                result={"status": "ok", "synced_at": result["synced_at"]},
            )
        return result
    except Exception as exc:
        save_state(
            {
                "version": 1,
                "last_sync_at": iso_now(),
                "last_status": "error",
                "last_error": str(exc),
            }
        )
        for event in claimed_events:
            runtime_state.fail_runtime_event(
                event.get("event_key", ""),
                claim_token=_text(event.get("claim_token", "")),
                error=str(exc),
                retry_after_seconds=60,
            )
        return {
            "status": "error",
            "schema_version": SCHEMA_VERSION,
            "project_name": project_name,
            "claimed_events": len(claimed_events),
            "error": str(exc),
        }


def plist_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def plist_value(value: Any, indent: str) -> str:
    next_indent = indent + "  "
    if isinstance(value, dict):
        lines = ["<dict>"]
        for key, nested in value.items():
            lines.append(f"{next_indent}<key>{plist_escape(str(key))}</key>")
            lines.append(plist_value(nested, next_indent))
        lines.append(f"{indent}</dict>")
        return "\n".join(lines)
    if isinstance(value, list):
        lines = ["<array>"]
        for nested in value:
            lines.append(f"{next_indent}{plist_value(nested, next_indent).lstrip()}")
        lines.append(f"{indent}</array>")
        return "\n".join(lines)
    if isinstance(value, bool):
        return "<true/>" if value else "<false/>"
    if isinstance(value, int):
        return f"<integer>{value}</integer>"
    return f"<string>{plist_escape(str(value))}</string>"


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
    return subprocess.run(["launchctl", *parts], text=True, capture_output=True, check=False)


def cmd_snapshot(args: argparse.Namespace) -> int:
    print(json.dumps(snapshot(project_name=args.project_name), ensure_ascii=False, indent=2))
    return 0


def cmd_bitable_target_status(args: argparse.Namespace) -> int:
    print(json.dumps(bitable_target_status(project_name=args.project_name), ensure_ascii=False, indent=2))
    return 0


def cmd_bitable_publish_preview(args: argparse.Namespace) -> int:
    print(json.dumps(bitable_publish_preview(project_name=args.project_name), ensure_ascii=False, indent=2))
    return 0


def cmd_bitable_publish(args: argparse.Namespace) -> int:
    result = run_sync(force_full=True, project_name=args.project_name)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "ok" else 1


def cmd_run_sync_once(args: argparse.Namespace) -> int:
    result = run_sync(force_full=False, project_name=args.project_name)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "ok" else 1


def cmd_status(_args: argparse.Namespace) -> int:
    queue_status = runtime_state.fetch_runtime_queue_status(queue_name=QUEUE_NAME)
    state = load_state()
    print(
        json.dumps(
            {
                "installed": launch_agent_plist_path(PROJECTION_SYNC_NAME).exists(),
                "loaded": launch_agent_loaded(PROJECTION_SYNC_NAME),
                "plist": str(launch_agent_plist_path(PROJECTION_SYNC_NAME)),
                "state_path": str(projection_state_path()),
                "last_sync_at": state.get("last_sync_at", ""),
                "last_status": state.get("last_status", ""),
                "last_error": state.get("last_error", ""),
                "runtime_queue": queue_status,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(PROJECTION_SYNC_NAME)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    LOG_STDOUT.parent.mkdir(parents=True, exist_ok=True)
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    payload = {
        "Label": PROJECTION_SYNC_NAME,
        "ProgramArguments": [python_path, str(workspace_root() / "ops" / "feishu_projection.py"), "run-sync-once"],
        "RunAtLoad": True,
        "StartInterval": int(args.interval),
        "StandardOutPath": str(LOG_STDOUT),
        "StandardErrorPath": str(LOG_STDERR),
        "WorkingDirectory": str(workspace_root()),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
    }
    plist_path.write_text(plist_dumps(payload), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    bootstrap = run_launchctl("bootstrap", domain, str(plist_path))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=os.sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{PROJECTION_SYNC_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=os.sys.stderr)
        return kickstart.returncode
    print(json.dumps({"installed": True, "plist": str(plist_path), "interval": int(args.interval)}, ensure_ascii=False))
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(PROJECTION_SYNC_NAME)
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    if plist_path.exists():
        plist_path.unlink()
    print(json.dumps({"installed": False, "plist": str(plist_path)}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Vault project/task truth as read-only Feishu Bitable projections")
    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot_cmd = subparsers.add_parser("snapshot")
    snapshot_cmd.add_argument("--project-name", default="")
    snapshot_cmd.set_defaults(func=cmd_snapshot)

    target_status_cmd = subparsers.add_parser("bitable-target-status")
    target_status_cmd.add_argument("--project-name", default="")
    target_status_cmd.set_defaults(func=cmd_bitable_target_status)

    publish_preview_cmd = subparsers.add_parser("bitable-publish-preview")
    publish_preview_cmd.add_argument("--project-name", default="")
    publish_preview_cmd.set_defaults(func=cmd_bitable_publish_preview)

    publish_cmd = subparsers.add_parser("bitable-publish")
    publish_cmd.add_argument("--project-name", default="")
    publish_cmd.set_defaults(func=cmd_bitable_publish)

    sync_cmd = subparsers.add_parser("run-sync-once")
    sync_cmd.add_argument("--project-name", default="")
    sync_cmd.set_defaults(func=cmd_run_sync_once)

    status_cmd = subparsers.add_parser("status")
    status_cmd.set_defaults(func=cmd_status)

    install_cmd = subparsers.add_parser("install-launchagent")
    install_cmd.add_argument("--interval", type=int, default=900)
    install_cmd.set_defaults(func=cmd_install_launchagent)

    uninstall_cmd = subparsers.add_parser("uninstall-launchagent")
    uninstall_cmd.set_defaults(func=cmd_uninstall_launchagent)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
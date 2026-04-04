#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

try:
    from ops import project_pause, runtime_state, workspace_hub_project
except ImportError:  # pragma: no cover
    import project_pause  # type: ignore
    import runtime_state  # type: ignore
    import workspace_hub_project  # type: ignore


WORKSPACE_ROOT = Path()
VAULT_ROOT = Path()
PROJECTS_ROOT = Path()
RUNTIME_ROOT = Path()
WORKING_ROOT = Path()
DASHBOARD_ROOT = Path()
REGISTRY_MD = Path()
ACTIVE_PROJECTS_MD = Path()
NEXT_ACTIONS_MD = Path()
NOW_MD = Path()
REVIEW_ROOT = Path()
REVIEW_INBOX_MD = Path()
COORDINATION_ROOT = Path()
COORDINATION_MD = Path()
DAILY_ROOT = Path()
PROJECT_SUMMARY_ROOT = Path()
SYSTEM_SUMMARY_ROOT = Path()
USER_PROFILE_MD = Path()
HOME_DASHBOARD_MD = Path()
PROJECTS_DASHBOARD_MD = Path()
ACTIONS_DASHBOARD_MD = Path()
MEMORY_HEALTH_MD = Path()
MATERIALS_DASHBOARD_ROOT = Path()
SESSION_ROUTER_JSON = Path()
PROJECT_BINDINGS_JSON = Path()
EVENTS_NDJSON = Path()
DASHBOARD_SYNC_STATE_JSON = Path()
MEMORY_LOCK_PATH = Path()
SESSION_INDEX_JSONL = Path.home() / ".codex" / "session_index.jsonl"
HISTORY_JSONL = Path.home() / ".codex" / "history.jsonl"
SESSIONS_ROOT = Path.home() / ".codex" / "sessions"
WORKSPACE_LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"
WATCHER_NAME = "com.codexhub.codex-memory-watcher"
DASHBOARD_SYNC_NAME = "com.codexhub.codex-dashboard-sync"
GROWTH_PROJECT_NAME = str(os.environ.get("WORKSPACE_HUB_GROWTH_PROJECT_NAME", "Growth System")).strip() or "Growth System"
GROWTH_CHAT_NAME = str(os.environ.get("WORKSPACE_HUB_GROWTH_CHAT_NAME", f"{GROWTH_PROJECT_NAME} Updates")).strip() or f"{GROWTH_PROJECT_NAME} Updates"


def _int_env(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            print(f"[codex_memory] invalid {name}={raw!r}; falling back to {default}", file=sys.stderr)
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


SYNC_TRIGGER_TIMEOUT_SECONDS = _int_env("WORKSPACE_HUB_SYNC_TRIGGER_TIMEOUT_SECONDS", 15, minimum=5)
_WORKSPACE_LOCK_OWNER_PID = 0
_WORKSPACE_LOCK_DEPTH = 0
_WORKSPACE_LOCK_HANDLE: Any | None = None


def _refresh_roots() -> None:
    global WORKSPACE_ROOT
    global VAULT_ROOT
    global PROJECTS_ROOT
    global RUNTIME_ROOT
    global WORKING_ROOT
    global DASHBOARD_ROOT
    global REGISTRY_MD
    global ACTIVE_PROJECTS_MD
    global NEXT_ACTIONS_MD
    global NOW_MD
    global REVIEW_ROOT
    global REVIEW_INBOX_MD
    global COORDINATION_ROOT
    global COORDINATION_MD
    global DAILY_ROOT
    global PROJECT_SUMMARY_ROOT
    global SYSTEM_SUMMARY_ROOT
    global USER_PROFILE_MD
    global HOME_DASHBOARD_MD
    global PROJECTS_DASHBOARD_MD
    global ACTIONS_DASHBOARD_MD
    global MEMORY_HEALTH_MD
    global MATERIALS_DASHBOARD_ROOT
    global SESSION_ROUTER_JSON
    global PROJECT_BINDINGS_JSON
    global EVENTS_NDJSON
    global DASHBOARD_SYNC_STATE_JSON
    global MEMORY_LOCK_PATH

    workspace_root = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(workspace_hub_project.DEFAULT_WORKSPACE_ROOT)))
    vault_root = Path(
        os.environ.get(
            "WORKSPACE_HUB_VAULT_ROOT",
            str(workspace_hub_project.DEFAULT_LOCAL_VAULT_ROOT),
        )
    )
    WORKSPACE_ROOT = workspace_root
    VAULT_ROOT = vault_root
    PROJECTS_ROOT = workspace_root / "projects"
    RUNTIME_ROOT = workspace_root / "runtime"
    WORKING_ROOT = vault_root / "01_working"
    DASHBOARD_ROOT = vault_root / "07_dashboards"
    REGISTRY_MD = vault_root / "PROJECT_REGISTRY.md"
    ACTIVE_PROJECTS_MD = vault_root / "ACTIVE_PROJECTS.md"
    NEXT_ACTIONS_MD = vault_root / "NEXT_ACTIONS.md"
    NOW_MD = WORKING_ROOT / "NOW.md"
    REVIEW_ROOT = vault_root / "04_review"
    REVIEW_INBOX_MD = REVIEW_ROOT / "INBOX.md"
    COORDINATION_ROOT = vault_root / "04_coordination"
    COORDINATION_MD = COORDINATION_ROOT / "COORDINATION.md"
    DAILY_ROOT = vault_root / "02_episodic" / "daily"
    PROJECT_SUMMARY_ROOT = vault_root / "03_semantic" / "projects"
    SYSTEM_SUMMARY_ROOT = vault_root / "03_semantic" / "systems"
    USER_PROFILE_MD = SYSTEM_SUMMARY_ROOT / "workspace-user-profile.md"
    HOME_DASHBOARD_MD = DASHBOARD_ROOT / "HOME.md"
    PROJECTS_DASHBOARD_MD = DASHBOARD_ROOT / "PROJECTS.md"
    ACTIONS_DASHBOARD_MD = DASHBOARD_ROOT / "ACTIONS.md"
    MEMORY_HEALTH_MD = DASHBOARD_ROOT / "MEMORY_HEALTH.md"
    MATERIALS_DASHBOARD_ROOT = DASHBOARD_ROOT / "materials"
    SESSION_ROUTER_JSON = RUNTIME_ROOT / "session-router.json"
    PROJECT_BINDINGS_JSON = RUNTIME_ROOT / "project-bindings.json"
    EVENTS_NDJSON = RUNTIME_ROOT / "events.ndjson"
    DASHBOARD_SYNC_STATE_JSON = RUNTIME_ROOT / "dashboard-sync-state.json"
    MEMORY_LOCK_PATH = RUNTIME_ROOT / ".memory-system.lock"


_refresh_roots()

REGISTRY_RE = re.compile(
    r"<!-- PROJECT_REGISTRY_DATA_START -->\s*```json\s*(.*?)\s*```\s*<!-- PROJECT_REGISTRY_DATA_END -->",
    re.S,
)
FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n(.*)\Z", re.S)
AUTO_RECENT_MARKERS = ("<!-- AUTO_RECENT_START -->", "<!-- AUTO_RECENT_END -->")
AUTO_FOLLOWUP_MARKERS = ("<!-- AUTO_FOLLOWUPS_START -->", "<!-- AUTO_FOLLOWUPS_END -->")
AUTO_WRITEBACK_MARKERS = ("<!-- AUTO_WRITEBACK_START -->", "<!-- AUTO_WRITEBACK_END -->")
AUTO_PROJECT_TASKS_MARKERS = ("<!-- AUTO_PROJECT_TASKS_START -->", "<!-- AUTO_PROJECT_TASKS_END -->")
AUTO_TOPIC_ROLLUPS_MARKERS = ("<!-- AUTO_TOPIC_ROLLUPS_START -->", "<!-- AUTO_TOPIC_ROLLUPS_END -->")
AUTO_GFLOW_RUNS_MARKERS = ("<!-- AUTO_GFLOW_RUNS_START -->", "<!-- AUTO_GFLOW_RUNS_END -->")
AUTO_TASK_TABLE_MARKERS = ("<!-- AUTO_TASK_TABLE_START -->", "<!-- AUTO_TASK_TABLE_END -->")
AUTO_CURRENT_TASKS_MARKERS = ("<!-- AUTO_CURRENT_TASKS_START -->", "<!-- AUTO_CURRENT_TASKS_END -->")
AUTO_PROJECT_ROLLUP_MARKERS = ("<!-- AUTO_PROJECT_ROLLUP_START -->", "<!-- AUTO_PROJECT_ROLLUP_END -->")
TASK_WRITEBACK_PREFIX = "TASK_WRITEBACK:"

PROJECT_BOARD_HEADERS = [
    "ID",
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
TOPIC_BOARD_HEADERS = [
    "ID",
    "模块",
    "事项",
    "状态",
    "交付物",
    "审核状态",
    "审核人",
    "审核结论",
    "审核时间",
    "下一步",
    "更新时间",
    "阻塞/依赖",
    "上卷ID",
]
ALLOWED_TASK_STATUSES = {"todo", "doing", "blocked", "done"}
ALLOWED_REVIEW_STATUSES = {"", "draft", "pending_review", "approved", "changes_requested", "rejected"}
STATUS_ORDER = {"doing": 0, "todo": 1, "blocked": 2, "done": 3}
DISPLAY_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_timestamp(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def display_datetime(text: str) -> dt.datetime | None:
    parsed = parse_iso_timestamp(text)
    if not parsed:
        return None
    return parsed.astimezone(DISPLAY_TZ)


def display_timestamp(text: str) -> str:
    local = display_datetime(text)
    if not local:
        return text
    return local.isoformat(timespec="seconds")


def display_date(text: str) -> str:
    local = display_datetime(text)
    if not local:
        return text[:10]
    return local.date().isoformat()


@contextmanager
def workspace_lock(*, blocking: bool = True) -> Any:
    global _WORKSPACE_LOCK_OWNER_PID
    global _WORKSPACE_LOCK_DEPTH
    global _WORKSPACE_LOCK_HANDLE

    current_pid = os.getpid()
    if _WORKSPACE_LOCK_OWNER_PID == current_pid and _WORKSPACE_LOCK_HANDLE is not None:
        _WORKSPACE_LOCK_DEPTH += 1
        try:
            yield
        finally:
            _WORKSPACE_LOCK_DEPTH -= 1
        return

    MEMORY_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MEMORY_LOCK_PATH.open("a+", encoding="utf-8") as handle:
        lock_mode = fcntl.LOCK_EX if blocking else (fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            fcntl.flock(handle.fileno(), lock_mode)
        except BlockingIOError as exc:
            raise WorkspaceLockBusy(f"workspace lock is already held: {MEMORY_LOCK_PATH}") from exc
        _WORKSPACE_LOCK_OWNER_PID = current_pid
        _WORKSPACE_LOCK_DEPTH = 1
        _WORKSPACE_LOCK_HANDLE = handle
        try:
            yield
        finally:
            _WORKSPACE_LOCK_DEPTH = 0
            _WORKSPACE_LOCK_OWNER_PID = 0
            _WORKSPACE_LOCK_HANDLE = None
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def read_text(path: Path, default: str = "") -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return default


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            handle.write(text)
            tmp_path = Path(handle.name)
        os.replace(tmp_path, path)
        return
    except PermissionError:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink()
        # Some iCloud-backed directories reject sibling temp-file creation in
        # automation contexts even when the existing target file is writable.
        # Fall back to an in-place overwrite for already-materialized files so
        # health/board writeback can complete without creating a shadow truth.
        if path.exists():
            path.write_text(text, encoding="utf-8")
            return
        raise


def load_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(read_text(path))
    except json.JSONDecodeError:
        return fallback


def dump_json(path: Path, data: Any) -> None:
    write_text(path, json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def append_ndjson(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def recent_event_ids(path: Path, limit: int = 50) -> set[str]:
    ids: set[str] = set()
    for line in read_text(path).splitlines()[-limit:]:
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_id = str(item.get("event_id", "")).strip()
        if event_id:
            ids.add(event_id)
    return ids


def load_registry() -> list[dict[str, Any]]:
    text = read_text(REGISTRY_MD)
    match = REGISTRY_RE.search(text)
    if not match:
        return []
    return json.loads(match.group(1))


def canonical_project_name(project_name: str) -> str:
    name = project_name.strip()
    if not name:
        return ""
    lowered = name.lower()
    for entry in load_registry():
        candidates = [str(entry.get("project_name", "")).strip()]
        candidates.extend(str(item).strip() for item in entry.get("aliases", []) if str(item).strip())
        if any(candidate.lower() == lowered for candidate in candidates if candidate):
            return str(entry.get("project_name", name)).strip() or name
    return workspace_hub_project.canonicalize(name)


def normalize_vault_path(value: str | Path) -> str:
    _refresh_roots()
    raw = str(value or "").strip()
    if not raw:
        return ""
    local_root = str(VAULT_ROOT.resolve())
    legacy_root = str(workspace_hub_project.LEGACY_ICLOUD_VAULT_ROOT.resolve())
    if raw.startswith(legacy_root):
        raw = f"{local_root}{raw[len(legacy_root):]}"
    return str(Path(raw))


def active_project_pause(project_name: str, *, scope: str) -> dict[str, Any]:
    return project_pause.active_pause(project_name=canonical_project_name(project_name), scope=scope)


def render_registry(entries: list[dict[str, Any]]) -> str:
    entries = sorted(entries, key=lambda item: item["project_name"].lower())
    names = "\n".join(f"{index}. `{item['project_name']}`" for index, item in enumerate(entries, start=1))
    block = json.dumps(entries, ensure_ascii=False, indent=2)
    return (
        "# PROJECT_REGISTRY\n\n"
        "说明：\n\n"
        "1. 这是 Vault 根层的项目注册表。\n"
        "2. 人类查看和维护使用本文件。\n"
        "3. 启动器会读取本文件中的 JSON 数据块做项目名和别名匹配。\n"
        "4. 每个项目必须能被唯一识别。\n\n"
        "## Registry Data\n\n"
        "<!-- PROJECT_REGISTRY_DATA_START -->\n"
        "```json\n"
        f"{block}\n"
        "```\n"
        "<!-- PROJECT_REGISTRY_DATA_END -->\n\n"
        "## 字段约定\n\n"
        "1. `project_name`\n"
        "2. `aliases`\n"
        "3. `path`\n"
        "4. `status`\n"
        "5. `summary_note`\n\n"
        "## Registered Projects\n\n"
        f"{names}\n"
    )


def write_registry(entries: list[dict[str, Any]]) -> None:
    _refresh_roots()
    write_text(REGISTRY_MD, render_registry(entries))


def parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    match = FRONTMATTER_RE.match(text)
    if not match:
        return {}, text
    raw, body = match.groups()
    data: dict[str, Any] = {}
    current_list_key: str | None = None
    for line in raw.splitlines():
        if line.startswith("  - ") and current_list_key:
            data.setdefault(current_list_key, []).append(line[4:].strip())
            continue
        current_list_key = None
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if value == "":
            data[key] = []
            current_list_key = key
        else:
            data[key] = value
    return data, body


def render_frontmatter(data: dict[str, Any]) -> str:
    lines = ["---"]
    order = [
        "board_type",
        "project_name",
        "topic_name",
        "topic_key",
        "rollup_target",
        "preferred_name",
        "feishu_open_id",
        "alternate_names",
        "relationship",
        "aliases",
        "status",
        "priority",
        "path",
        "updated_at",
        "purpose",
        "next_action",
        "summary",
        "last_writeback_at",
        "last_writeback_session_id",
        "last_writeback_thread",
        "last_writeback_excerpt",
    ]
    for key in order:
        value = data.get(key)
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {item}")
        elif value is not None:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines)


def project_summary_path(project_name: str) -> Path:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    return PROJECT_SUMMARY_ROOT / f"{project_name}.md"


def user_profile_path() -> Path:
    _refresh_roots()
    return USER_PROFILE_MD


def load_user_profile() -> dict[str, Any]:
    path = user_profile_path()
    frontmatter, body = parse_frontmatter(read_text(path))
    preferred_name = str(frontmatter.get("preferred_name", "")).strip()
    feishu_open_id = str(frontmatter.get("feishu_open_id", "")).strip()
    alternate_names = frontmatter.get("alternate_names", [])
    if not isinstance(alternate_names, list):
        alternate_names = []
    return {
        "path": str(path),
        "preferred_name": preferred_name,
        "feishu_open_id": feishu_open_id,
        "alternate_names": [str(item).strip() for item in alternate_names if str(item).strip()],
        "relationship": str(frontmatter.get("relationship", "workspace owner")).strip() or "workspace owner",
        "updated_at": str(frontmatter.get("updated_at", "")).strip(),
        "body": body.strip(),
    }


def save_user_profile(
    *,
    preferred_name: str,
    alternate_names: list[str] | None = None,
    feishu_open_id: str = "",
    relationship: str = "workspace owner",
    note: str = "",
) -> dict[str, Any]:
    path = user_profile_path()
    preferred = preferred_name.strip()
    normalized_open_id = feishu_open_id.strip()
    aliases = [item.strip() for item in (alternate_names or []) if item and item.strip() and item.strip() != preferred]
    updated_at = display_date(iso_now())
    frontmatter = render_frontmatter(
        {
            "preferred_name": preferred,
            "feishu_open_id": normalized_open_id,
            "alternate_names": aliases,
            "relationship": relationship.strip() or "workspace owner",
            "status": "active",
            "updated_at": updated_at,
            "purpose": "Workspace-wide preferred user name and operator profile for Codex Hub.",
            "summary": f"Preferred name: {preferred}" if preferred else "Preferred name not set.",
        }
    )
    lines = [frontmatter, "", "# Workspace User Profile", ""]
    lines.append(f"- preferred_name: `{preferred}`" if preferred else "- preferred_name: 未设置")
    lines.append(f"- feishu_open_id: `{normalized_open_id}`" if normalized_open_id else "- feishu_open_id: 未设置")
    lines.append(f"- relationship: `{relationship.strip() or 'workspace owner'}`")
    if aliases:
        lines.append(f"- alternate_names: {', '.join(f'`{item}`' for item in aliases)}")
    else:
        lines.append("- alternate_names: none")
    lines.extend(["", "## Notes", "", note.strip() or "This note records how the workspace assistant and the workspace should address the primary user."])
    write_text(path, "\n".join(lines).strip() + "\n")
    profile = load_user_profile()
    profile["relationship"] = relationship.strip() or "workspace owner"
    return profile


def project_board_path(project_name: str) -> Path:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    return WORKING_ROOT / f"{project_name}-项目板.md"


def materials_dashboard_path(project_name: str) -> Path:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    return MATERIALS_DASHBOARD_ROOT / f"{project_name}.md"


def topic_board_paths(project_name: str) -> list[Path]:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    paths: list[Path] = []
    for path in sorted(WORKING_ROOT.glob(f"{project_name}-*跟进板.md")):
        if path.name == f"{project_name}-项目板.md":
            continue
        frontmatter, _body = parse_frontmatter(read_text(path))
        if frontmatter.get("board_type") == "topic":
            paths.append(path)
    return paths


def extract_marked_block(text: str, markers: tuple[str, str]) -> str:
    start, end = markers
    if start not in text or end not in text:
        return ""
    return text.split(start, 1)[1].split(end, 1)[0].strip()


def normalize_task_status(value: str) -> str:
    value = (value or "").strip().lower()
    aliases = {
        "todo": "todo",
        "doing": "doing",
        "blocked": "blocked",
        "done": "done",
        "待办": "todo",
        "待补录": "todo",
        "待确认": "blocked",
        "进行中": "doing",
        "已完成": "done",
    }
    return aliases.get(value, value)


def normalize_task_writebacks(*texts: str) -> list[dict[str, str]]:
    updates_by_id: dict[str, dict[str, str]] = {}
    for text in texts:
        if not text:
            continue
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line.startswith(TASK_WRITEBACK_PREFIX):
                continue
            payload_text = line.replace(TASK_WRITEBACK_PREFIX, "", 1).strip()
            if not payload_text:
                continue
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                continue
            items = payload if isinstance(payload, list) else [payload]
            for item in items:
                if not isinstance(item, dict):
                    continue
                task_ref = str(item.get("task_ref", "")).strip()
                task_id = str(item.get("task_id", "")).strip() or task_ref.rsplit(":", 1)[-1].strip()
                if not task_id:
                    continue
                status = normalize_task_status(str(item.get("status", item.get("task_status", ""))).strip())
                if status and status not in ALLOWED_TASK_STATUSES:
                    status = ""
                update = {
                    "task_id": task_id,
                    "status": status,
                    "deliverable": str(item.get("deliverable", item.get("deliverable_ref", ""))).strip(),
                    "next_action": str(item.get("next_action", "")).strip(),
                    "updated_at": str(item.get("updated_at", "")).strip(),
                    "manual_note": str(item.get("manual_note", item.get("note", ""))).strip(),
                }
                updates_by_id[task_id] = update
    return list(updates_by_id.values())


def markdown_table_lines(headers: list[str], rows: list[dict[str, str]]) -> list[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        values = [str(row.get(header, "")).replace("\n", " ").replace("|", "/").strip() for header in headers]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def parse_markdown_table(block: str, expected_headers: list[str], *, allow_missing: bool = False) -> list[dict[str, str]]:
    lines = [line.strip() for line in block.splitlines() if line.strip().startswith("|")]
    if len(lines) < 2:
        return []
    headers = [cell.strip() for cell in lines[0].strip("|").split("|")]
    if headers != expected_headers and (not allow_missing or any(header not in expected_headers for header in headers)):
        return []
    rows: list[dict[str, str]] = []
    for line in lines[2:]:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != len(headers):
            continue
        row = {header: "" for header in expected_headers}
        row.update(dict(zip(headers, cells)))
        if "状态" in row:
            row["状态"] = normalize_task_status(row["状态"])
        rows.append(row)
    return rows


def _apply_task_update_to_row(row: dict[str, str], update: dict[str, str], *, default_updated_at: str) -> bool:
    changed = False
    status = update.get("status", "")
    if status and row.get("状态", "") != status:
        row["状态"] = status
        changed = True
    deliverable = update.get("deliverable", "")
    if deliverable and row.get("交付物", "") != deliverable:
        row["交付物"] = deliverable
        changed = True
    next_action = update.get("next_action", "")
    if next_action and row.get("下一步", "") != next_action:
        row["下一步"] = next_action
        changed = True
    updated_at = update.get("updated_at", "") or default_updated_at
    if updated_at and row.get("更新时间", "") != updated_at:
        row["更新时间"] = updated_at
        changed = True
    return changed


def _apply_task_updates(rows: list[dict[str, str]], task_updates: list[dict[str, str]], *, default_updated_at: str) -> bool:
    updates_by_id = {item["task_id"]: item for item in task_updates if item.get("task_id")}
    if not updates_by_id:
        return False
    changed = False
    for row in rows:
        task_id = row.get("ID", "").strip()
        if not task_id or task_id not in updates_by_id:
            continue
        changed = _apply_task_update_to_row(row, updates_by_id[task_id], default_updated_at=default_updated_at) or changed
    return changed


TASK_ID_PATTERN = re.compile(r"^([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)-(\d+)$")


def _candidate_task_prefixes(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        match = TASK_ID_PATTERN.match(str(row.get("ID", "")).strip())
        if not match:
            continue
        prefix = match.group(1)
        counts[prefix] = counts.get(prefix, 0) + 1
    return counts


def _fallback_task_prefix(project_name: str) -> str:
    canonical = canonical_project_name(project_name)
    if workspace_hub_project.is_workspace_hub_project(canonical):
        return "WH-OPS"
    ascii_tokens = re.findall(r"[A-Za-z0-9]+", canonical)
    if ascii_tokens:
        token = "".join(part[0].upper() for part in ascii_tokens if part)
        if token:
            return f"{token}-LT"
    return "TASK"


def derive_task_prefix(project_name: str, rows: list[dict[str, str]]) -> str:
    counts = _candidate_task_prefixes(rows)
    ops_like = {prefix: count for prefix, count in counts.items() if prefix.endswith("-OPS")}
    if ops_like:
        return sorted(ops_like.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))[0][0]
    if counts:
        return sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))[0][0]
    return _fallback_task_prefix(project_name)


def allocate_task_id(project_name: str, rows: list[dict[str, str]]) -> str:
    prefix = derive_task_prefix(project_name, rows)
    max_index = 0
    for row in rows:
        match = TASK_ID_PATTERN.match(str(row.get("ID", "")).strip())
        if not match or match.group(1) != prefix:
            continue
        max_index = max(max_index, int(match.group(2)))
    return f"{prefix}-{max_index + 1:02d}"


def create_harness_task(
    project_name: str,
    task_item: str,
    *,
    topic_name: str = "",
    status: str = "doing",
    deliverable: str = "",
    next_action: str = "",
    requested_at: str = "",
    source: str = "manual_long_task",
    scope: str = "长任务",
) -> dict[str, Any]:
    canonical_project = canonical_project_name(project_name)
    normalized_status = normalize_task_status(status or "doing")
    task_item_text = str(task_item or "").strip()
    if not canonical_project:
        raise ValueError("project_name_required")
    if not task_item_text:
        raise ValueError("task_item_required")
    binding = resolve_board_binding(canonical_project, topic_name or "")
    updated_at = requested_at or iso_now()
    deliverable_text = str(deliverable or "").strip() or "待补充"
    next_action_text = str(next_action or "").strip() or "进入 discover 并开始推进。"
    changed_targets: list[str] = []
    with workspace_lock():
        project_board = load_project_board(canonical_project)
        task_id = allocate_task_id(
            canonical_project,
            [*project_board["project_rows"], *project_board["rollup_rows"]],
        )
        board_path = Path(binding["binding_board_path"])
        if binding.get("binding_scope") == "topic" and board_path.exists():
            topic_board = load_topic_board(board_path)
            topic_rows = topic_board["rows"]
            topic_rows.insert(
                0,
                {
                    "ID": task_id,
                    "模块": str(binding.get("topic_name") or topic_name or scope).strip() or "长任务",
                    "事项": task_item_text,
                    "状态": normalized_status,
                    "交付物": deliverable_text,
                    "审核状态": "",
                    "审核人": "",
                    "审核结论": "",
                    "审核时间": "",
                    "下一步": next_action_text,
                    "更新时间": updated_at,
                    "阻塞/依赖": "",
                    "上卷ID": task_id,
                },
            )
            save_topic_board(board_path, topic_board["frontmatter"], topic_board["body"], topic_rows)
            changed_targets.append(str(board_path))
            project_path = refresh_project_rollups(canonical_project, topic_path=board_path)
            changed_targets.append(str(project_path))
        else:
            project_rows = project_board["project_rows"]
            project_rows.insert(
                0,
                {
                    "ID": task_id,
                    "父ID": task_id,
                    "来源": source,
                    "范围": str(scope or topic_name or "长任务").strip() or "长任务",
                    "事项": task_item_text,
                    "状态": normalized_status,
                    "交付物": deliverable_text,
                    "审核状态": "",
                    "审核人": "",
                    "审核结论": "",
                    "审核时间": "",
                    "下一步": next_action_text,
                    "更新时间": updated_at,
                    "指向": str(project_board["path"]),
                },
            )
            save_project_board(
                project_board["path"],
                project_board["frontmatter"],
                project_board["body"],
                project_rows,
                project_board["rollup_rows"],
                project_board.get("gflow_rows", []),
            )
            changed_targets.append(str(project_board["path"]))
        refresh_next_actions_rollup()
    writeback_binding = {
        "project_name": canonical_project,
        "binding_scope": binding.get("binding_scope", "project"),
        "binding_board_path": binding.get("binding_board_path", ""),
        "topic_name": binding.get("topic_name", ""),
        "rollup_target": binding.get("rollup_target", ""),
        "last_active_at": updated_at,
    }
    record_project_writeback(
        writeback_binding,
        source="harness_task_create",
        changed_targets=changed_targets,
        trigger_dashboard_sync=False,
    )
    return {
        "ok": True,
        "project_name": canonical_project,
        "task_id": task_id,
        "task_item": task_item_text,
        "task_status": normalized_status,
        "binding_scope": binding.get("binding_scope", "project"),
        "binding_board_path": binding.get("binding_board_path", ""),
        "topic_name": binding.get("topic_name", ""),
        "rollup_target": binding.get("rollup_target", ""),
        "changed_targets": changed_targets,
    }


def render_table_section(heading: str, markers: tuple[str, str], headers: list[str], rows: list[dict[str, str]]) -> tuple[str, list[str]]:
    lines = [markers[0], *markdown_table_lines(headers, rows), markers[1]]
    return heading, lines


def default_current_task_lines() -> list[str]:
    return [
        AUTO_CURRENT_TASKS_MARKERS[0],
        "### todo",
        "- [ ] 暂无 todo",
        "",
        "### doing",
        "- [ ] 暂无 doing",
        "",
        "### blocked",
        "- [ ] 暂无 blocked",
        "",
        "### done",
        "- [x] 暂无 done",
        AUTO_CURRENT_TASKS_MARKERS[1],
    ]


def build_current_task_lines(
    project_rows: list[dict[str, str]],
    rollup_rows: list[dict[str, str]],
    gflow_rows: list[dict[str, str]] | None = None,
    *,
    project_name: str = "",
) -> list[str]:
    grouped = {"todo": [], "doing": [], "blocked": [], "done": []}
    combined = project_rows + rollup_rows + list(gflow_rows or [])
    combined.sort(key=lambda row: (STATUS_ORDER.get(row.get("状态", "todo"), 99), row.get("更新时间", ""), row.get("ID", "")))
    harness_snapshots: dict[str, dict[str, Any]] = {}
    if project_name:
        try:
            from ops import board_job_projector

            for row in combined:
                task_id = str(row.get("ID", "")).strip()
                if not task_id:
                    continue
                try:
                    snapshot = board_job_projector.task_harness_snapshot(project_name, task_id)
                except Exception:
                    continue
                if snapshot:
                    harness_snapshots[task_id] = snapshot
        except Exception:
            harness_snapshots = {}
    for row in combined:
        status = normalize_task_status(row.get("状态", "todo"))
        prefix = "[x]" if status == "done" else "[ ]"
        source = row.get("来源", "project")
        review_status = row.get("审核状态", "")
        review_suffix = f" | 审核：{review_status}" if review_status in {"pending_review", "changes_requested"} else ""
        harness_suffix = ""
        snapshot = harness_snapshots.get(str(row.get("ID", "")).strip())
        if snapshot:
            snapshot_parts = [f"Harness：{snapshot.get('harness_state', '')}"]
            if snapshot.get("last_decision"):
                snapshot_parts.append(f"决策：{snapshot['last_decision']}")
            if snapshot.get("next_wake_at"):
                snapshot_parts.append(f"下次唤醒：{display_timestamp(str(snapshot['next_wake_at']))}")
            if snapshot.get("blocked_reason"):
                snapshot_parts.append(f"阻塞：{snapshot['blocked_reason']}")
            harness_suffix = " | " + " | ".join(snapshot_parts)
        grouped.setdefault(status, []).append(
            f"- {prefix} {row.get('ID', '')} `{source}` {row.get('事项', '')} | 下一步：{row.get('下一步', '待补充')}{review_suffix}{harness_suffix}"
        )
    lines: list[str] = []
    for status in ["todo", "doing", "blocked", "done"]:
        lines.append(f"### {status}")
        lines.extend(grouped.get(status) or ([f"- {'[x]' if status == 'done' else '[ ]'} 暂无 {status}"]))
        if status != "done":
            lines.append("")
    return lines


def select_project_focus_tasks(
    project_rows: list[dict[str, str]],
    rollup_rows: list[dict[str, str]],
    gflow_rows: list[dict[str, str]] | None = None,
) -> dict[str, list[dict[str, str]]]:
    sections = {"doing": [], "todo": [], "blocked": [], "done": []}
    for row in project_rows + rollup_rows + list(gflow_rows or []):
        status = normalize_task_status(row.get("状态", "todo"))
        if status not in sections:
            continue
        sections[status].append(row)
    for status in sections:
        sections[status].sort(key=lambda row: (row.get("父ID", ""), row.get("更新时间", ""), row.get("ID", "")))
    return sections


def project_board_next_action(
    project_rows: list[dict[str, str]],
    rollup_rows: list[dict[str, str]],
    gflow_rows: list[dict[str, str]] | None = None,
) -> str:
    sections = select_project_focus_tasks(project_rows, rollup_rows, gflow_rows)
    for status in ["doing", "todo", "blocked"]:
        if sections[status]:
            row = sections[status][0]
            next_action = str(row.get("下一步", "")).strip()
            if next_action:
                return next_action
            item = str(row.get("事项", "")).strip()
            if item:
                return item
    return "待补充"


def validate_task_rows(rows: list[dict[str, str]], *, required_headers: list[str], path: Path) -> list[str]:
    errors: list[str] = []
    for row in rows:
        for header in required_headers:
            if header not in row:
                errors.append(f"{path.name}: missing column {header}")
        status = normalize_task_status(row.get("状态", ""))
        if status not in ALLOWED_TASK_STATUSES:
            errors.append(f"{path.name}: invalid status `{row.get('状态', '')}` in task `{row.get('ID', '')}`")
        review_status = row.get("审核状态", "")
        if review_status not in ALLOWED_REVIEW_STATUSES:
            errors.append(f"{path.name}: invalid review status `{review_status}` in task `{row.get('ID', '')}`")
    return errors


def create_project_board(project_name: str, *, status: str = "active", priority: str = "medium") -> None:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    path = project_board_path(project_name)
    if path.exists():
        return
    frontmatter = render_frontmatter(
        {
            "board_type": "project",
            "project_name": project_name,
            "status": status,
            "priority": priority,
            "updated_at": dt.date.today().isoformat(),
            "purpose": f"{project_name} 的一级项目板，汇总项目直属任务与专题回卷任务。",
        }
    )
    body = "\n".join(
        [
            f"{frontmatter}",
            "",
            f"# {project_name}｜项目板",
            "",
            "## 定位",
            "",
            f"- 这是 `{project_name}` 的一级项目板。",
            "- 本页服务人工管理，同时作为项目任务事实源。",
            "",
            "## Project Owned Tasks",
            "",
            AUTO_PROJECT_TASKS_MARKERS[0],
            *markdown_table_lines(PROJECT_BOARD_HEADERS, []),
            AUTO_PROJECT_TASKS_MARKERS[1],
            "",
            "## Topic Rollups",
            "",
            AUTO_TOPIC_ROLLUPS_MARKERS[0],
            *markdown_table_lines(PROJECT_BOARD_HEADERS, []),
            AUTO_TOPIC_ROLLUPS_MARKERS[1],
            "",
            "## GFlow Runs",
            "",
            AUTO_GFLOW_RUNS_MARKERS[0],
            *markdown_table_lines(PROJECT_BOARD_HEADERS, []),
            AUTO_GFLOW_RUNS_MARKERS[1],
            "",
            "## 当前任务",
            "",
            *default_current_task_lines(),
            "",
            "## 专题板",
            "",
            "- 当前无",
            "",
        ]
    ).rstrip() + "\n"
    write_text(path, body)


def ensure_project_board(project_name: str) -> Path:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    summary = summary_metadata(project_name)
    create_project_board(project_name, status=summary.get("status", "active"), priority=summary.get("priority", "medium"))
    return project_board_path(project_name)


def load_project_board(project_name: str) -> dict[str, Any]:
    _refresh_roots()
    project_name = canonical_project_name(project_name)
    path = ensure_project_board(project_name)
    text = read_text(path)
    frontmatter, body = parse_frontmatter(text)
    project_rows = parse_markdown_table(
        extract_marked_block(body, AUTO_PROJECT_TASKS_MARKERS),
        PROJECT_BOARD_HEADERS,
        allow_missing=True,
    )
    rollup_rows = parse_markdown_table(
        extract_marked_block(body, AUTO_TOPIC_ROLLUPS_MARKERS),
        PROJECT_BOARD_HEADERS,
        allow_missing=True,
    )
    gflow_rows = parse_markdown_table(
        extract_marked_block(body, AUTO_GFLOW_RUNS_MARKERS),
        PROJECT_BOARD_HEADERS,
        allow_missing=True,
    )
    return {
        "path": path,
        "frontmatter": frontmatter,
        "body": body,
        "project_rows": project_rows,
        "rollup_rows": rollup_rows,
        "gflow_rows": gflow_rows,
    }


def load_topic_board(path: Path) -> dict[str, Any]:
    text = read_text(path)
    frontmatter, body = parse_frontmatter(text)
    rows = parse_markdown_table(
        extract_marked_block(body, AUTO_TASK_TABLE_MARKERS),
        TOPIC_BOARD_HEADERS,
        allow_missing=True,
    )
    return {
        "path": path,
        "frontmatter": frontmatter,
        "body": body,
        "rows": rows,
    }


def save_project_board(
    path: Path,
    frontmatter: dict[str, Any],
    body: str,
    project_rows: list[dict[str, str]],
    rollup_rows: list[dict[str, str]],
    gflow_rows: list[dict[str, str]] | None = None,
) -> None:
    _refresh_roots()
    frontmatter["updated_at"] = dt.date.today().isoformat()
    body = replace_or_append_marked_section(body, "## Project Owned Tasks", AUTO_PROJECT_TASKS_MARKERS, markdown_table_lines(PROJECT_BOARD_HEADERS, project_rows))
    body = replace_or_append_marked_section(body, "## Topic Rollups", AUTO_TOPIC_ROLLUPS_MARKERS, markdown_table_lines(PROJECT_BOARD_HEADERS, rollup_rows))
    body = replace_or_append_marked_section(body, "## GFlow Runs", AUTO_GFLOW_RUNS_MARKERS, markdown_table_lines(PROJECT_BOARD_HEADERS, gflow_rows or []))
    body = replace_or_append_marked_section(
        body,
        "## 当前任务",
        AUTO_CURRENT_TASKS_MARKERS,
        build_current_task_lines(
            project_rows,
            rollup_rows,
            gflow_rows,
            project_name=str(frontmatter.get("project_name", "")).strip(),
        ),
    )
    write_text(path, f"{render_frontmatter(frontmatter)}\n\n{body.lstrip()}")


def save_topic_board(path: Path, frontmatter: dict[str, Any], body: str, rows: list[dict[str, str]]) -> None:
    frontmatter["updated_at"] = dt.date.today().isoformat()
    body = replace_or_append_marked_section(body, "## 任务主表", AUTO_TASK_TABLE_MARKERS, markdown_table_lines(TOPIC_BOARD_HEADERS, rows))
    write_text(path, f"{render_frontmatter(frontmatter)}\n\n{body.lstrip()}")


def topic_rollup_rows(topic_board: dict[str, Any]) -> list[dict[str, str]]:
    frontmatter = topic_board["frontmatter"]
    topic_name = frontmatter.get("topic_name", "")
    path = topic_board["path"]
    rows: list[dict[str, str]] = []
    for row in topic_board["rows"]:
        rows.append(
            {
                "ID": row.get("ID", ""),
                "父ID": row.get("上卷ID", "") or row.get("ID", ""),
                "来源": f"topic:{topic_name}",
                "范围": row.get("模块", topic_name),
                "事项": row.get("事项", ""),
                "状态": normalize_task_status(row.get("状态", "todo")),
                "交付物": row.get("交付物", ""),
                "审核状态": row.get("审核状态", ""),
                "审核人": row.get("审核人", ""),
                "审核结论": row.get("审核结论", ""),
                "审核时间": row.get("审核时间", ""),
                "下一步": row.get("下一步", ""),
                "更新时间": row.get("更新时间", frontmatter.get("updated_at", "")),
                "指向": path.name,
            }
        )
    return rows


def _gflow_row_status(run_status: str) -> str:
    normalized = str(run_status or "").strip()
    if normalized == "planned":
        return "todo"
    if normalized == "running":
        return "doing"
    if normalized in {"paused", "awaiting_approval", "frozen"}:
        return "blocked"
    if normalized == "completed":
        return "done"
    return "todo"


def _gflow_scope_label(payload: dict[str, Any]) -> str:
    current_stage = str(payload.get("current_stage", "")).strip()
    suggested_path = [str(item).strip() for item in payload.get("suggested_path", []) if str(item).strip()]
    if current_stage and suggested_path:
        return f"{current_stage} | {' -> '.join(suggested_path[:3])}"
    if current_stage:
        return current_stage
    if suggested_path:
        return " -> ".join(suggested_path[:3])
    return "workflow"


def gflow_board_rows(project_name: str, *, limit: int = 12) -> list[dict[str, str]]:
    try:
        from ops import gstack_automation
    except ImportError:  # pragma: no cover
        import gstack_automation  # type: ignore

    rows: list[dict[str, str]] = []
    for payload in gstack_automation.list_workflow_runs(project_name=project_name, limit=limit):
        run_id = str(payload.get("run_id", "")).strip()
        if not run_id:
            continue
        run_summary = payload.get("run_summary") or {}
        summary = str(run_summary.get("summary", "")).strip() or str(payload.get("main_thread_handoff", "")).strip()
        run_status = str(payload.get("run_status", "")).strip()
        next_action = str(run_summary.get("next_action", "")).strip() or str(payload.get("latest_next_action", "")).strip()
        if not next_action:
            if run_status == "completed":
                next_action = "已完成，无需继续。"
            elif run_status in {"awaiting_approval", "paused", "frozen"}:
                next_action = "等待解除当前 gate 后继续。"
            elif run_status == "planned":
                next_action = "进入首阶段并开始推进。"
            else:
                next_action = "继续当前 workflow。"
        entry_prompt = str(payload.get("entry_prompt", "")).strip()
        if len(entry_prompt) > 72:
            entry_prompt = entry_prompt[:69].rstrip() + "..."
        template_label = str(payload.get("template_label", "")).strip()
        gate = payload.get("gate") or {}
        gate_reason = str(gate.get("reason", "")).strip()
        delivery = summary
        if gate_reason:
            delivery = f"{summary} | gate: {gate_reason}" if summary else f"gate: {gate_reason}"
        item_title = f"GFlow / {entry_prompt or '未命名 workflow'}"
        if template_label:
            item_title = f"GFlow / {template_label} / {entry_prompt or '未命名 workflow'}"
        rows.append(
            {
                "ID": run_id,
                "父ID": run_id,
                "来源": "gflow",
                "范围": _gflow_scope_label(payload),
                "事项": item_title,
                "状态": _gflow_row_status(run_status),
                "交付物": delivery,
                "审核状态": "",
                "审核人": "",
                "审核结论": "",
                "审核时间": "",
                "下一步": next_action,
                "更新时间": str(run_summary.get("updated_at", "")).strip() or iso_now(),
                "指向": f"gflow:{run_id}",
            }
        )
    rows.sort(
        key=lambda row: (
            STATUS_ORDER.get(normalize_task_status(row.get("状态", "todo")), 99),
            -(parse_iso_timestamp(row.get("更新时间", "")).timestamp() if parse_iso_timestamp(row.get("更新时间", "")) else 0),
            row.get("ID", ""),
        )
    )
    return rows


def sync_gflow_project_layers(project_name: str, *, limit: int = 12) -> dict[str, Any]:
    project_name = canonical_project_name(project_name)
    board = load_project_board(project_name)
    gflow_rows = gflow_board_rows(project_name, limit=limit)
    save_project_board(
        board["path"],
        board["frontmatter"],
        board["body"],
        board["project_rows"],
        board["rollup_rows"],
        gflow_rows,
    )
    refresh_next_actions_rollup()
    summary_text = ""
    if gflow_rows:
        top = gflow_rows[0]
        summary_text = (
            f"GFlow 当前运行：{top.get('事项', '')} | 状态：{top.get('状态', '')} | "
            f"下一步：{top.get('下一步', '待补充')}"
        )
    return {
        "project_name": project_name,
        "board_path": str(board["path"]),
        "gflow_rows": gflow_rows,
        "summary_text": summary_text,
    }


def refresh_project_rollups(project_name: str, *, topic_path: Path | None = None) -> Path:
    project_name = canonical_project_name(project_name)
    project_board = load_project_board(project_name)
    frontmatter = project_board["frontmatter"]
    body = project_board["body"]
    project_rows = project_board["project_rows"]
    rollup_rows = project_board["rollup_rows"]
    gflow_rows = project_board["gflow_rows"]
    new_rollups = [row for row in rollup_rows if not row.get("来源", "").startswith("topic:")]
    # Always rebuild topic rollups from every topic board of the project.
    # Incremental single-topic refresh would otherwise drop unrelated topic rows.
    topic_paths = topic_board_paths(project_name)
    topic_sources_seen: set[str] = set()
    for path in topic_paths:
        topic_board = load_topic_board(path)
        topic_name = str(topic_board["frontmatter"].get("topic_name", "")).strip()
        if not topic_name:
            continue
        source = f"topic:{topic_name}"
        if source in topic_sources_seen:
            continue
        topic_sources_seen.add(source)
        new_rollups.extend(topic_rollup_rows(topic_board))
    save_project_board(project_board["path"], frontmatter, body, project_rows, new_rollups, gflow_rows)
    return project_board["path"]


def resolve_board_binding(project_name: str, prompt: str = "") -> dict[str, str]:
    project_name = canonical_project_name(project_name)
    project_path = ensure_project_board(project_name)
    result = {
        "binding_scope": "project",
        "binding_board_path": str(project_path),
        "topic_name": "",
        "rollup_target": str(project_path),
    }
    prompt_l = prompt.lower().strip()
    if not prompt_l:
        return result
    matches: list[dict[str, str]] = []
    for path in topic_board_paths(project_name):
        frontmatter, _body = parse_frontmatter(read_text(path))
        topic_name = str(frontmatter.get("topic_name", "")).strip()
        topic_key = str(frontmatter.get("topic_key", "")).strip()
        haystacks = [topic_name, topic_key, path.stem]
        if any(item and item.lower() in prompt_l for item in haystacks):
            matches.append(
                {
                    "binding_scope": "topic",
                    "binding_board_path": str(path),
                    "topic_name": topic_name,
                    "rollup_target": normalize_vault_path(frontmatter.get("rollup_target", project_path)),
                }
            )
    return matches[0] if len(matches) == 1 else result


def default_aliases(project_name: str) -> list[str]:
    aliases: list[str] = []
    lower = project_name.lower()
    if lower != project_name:
        aliases.append(lower)
    return aliases


def create_project_summary(project_name: str, project_path: Path, aliases: list[str]) -> None:
    project_name = canonical_project_name(project_name)
    summary_path = project_summary_path(project_name)
    if summary_path.exists():
        return
    frontmatter = render_frontmatter(
        {
            "project_name": project_name,
            "aliases": aliases,
            "status": "active",
            "priority": "medium",
            "path": str(project_path),
            "updated_at": dt.date.today().isoformat(),
            "summary": "自动发现并纳入 Codex 记忆系统，业务内容待补充。",
        }
    )
    body = (
        f"{frontmatter}\n\n"
        f"# {project_name}\n\n"
        "## 项目定位\n\n"
        "- 待补充\n\n"
        "## 当前目标\n\n"
        "- 纳入 Codex 记忆与行动系统\n\n"
        "## 技术栈\n\n"
        "- 待补充\n\n"
        "## 当前状态\n\n"
        "- 已自动注册到 `PROJECT_REGISTRY.md`\n"
        "- 已创建项目目录\n"
        f"- 已创建一级项目板：`01_working/{project_name}-项目板.md`\n"
        "- 尚未补充业务上下文\n\n"
        "## 看板入口\n\n"
        f"- 一级项目板：`01_working/{project_name}-项目板.md`\n\n"
        "## 最近决策\n\n"
        "- 纳入统一 `workspace-hub` 管理\n\n"
        "## 自动写回\n\n"
        f"{AUTO_WRITEBACK_MARKERS[0]}\n"
        "- 暂无自动写回记录\n"
        f"{AUTO_WRITEBACK_MARKERS[1]}\n"
    )
    write_text(summary_path, body)


def summary_metadata(project_name: str) -> dict[str, Any]:
    project_name = canonical_project_name(project_name)
    summary_path = project_summary_path(project_name)
    frontmatter, _body = parse_frontmatter(read_text(summary_path))
    return frontmatter


def project_board_metadata(project_name: str) -> dict[str, Any]:
    project_name = canonical_project_name(project_name)
    board = load_project_board(project_name)
    return board["frontmatter"]


def refresh_active_projects(entries: list[dict[str, Any]]) -> None:
    active = [item for item in entries if item.get("status", "active") == "active"]
    lines = ["# ACTIVE_PROJECTS", "", "当前活跃项目列表。", "", "## Active", ""]
    if not active:
        lines.append("- 暂无活跃项目")
    for index, item in enumerate(sorted(active, key=lambda x: x["project_name"].lower()), start=1):
        board = load_project_board(item["project_name"])
        meta = board["frontmatter"]
        priority = meta.get("priority", "medium")
        next_action = project_board_next_action(board["project_rows"], board["rollup_rows"])
        lines.extend(
            [
                f"{index}. `{item['project_name']}`",
                f"   - status: `{meta.get('status', item.get('status', 'active'))}`",
                f"   - priority: `{priority}`",
                f"   - next: {next_action}",
            ]
        )
    lines.append("")
    write_text(ACTIVE_PROJECTS_MD, "\n".join(lines))


def project_board_facts(entries: list[dict[str, Any]] | None = None) -> tuple[list[dict[str, Any]], list[str]]:
    entries = entries or load_registry()
    facts: list[dict[str, Any]] = []
    errors: list[str] = []
    for entry in sorted(entries, key=lambda item: item["project_name"].lower()):
        try:
            board = load_project_board(entry["project_name"])
        except Exception as exc:
            errors.append(f"{entry['project_name']}: project board load failed: {exc}")
            continue
        facts.append(
            {
                "project_name": entry["project_name"],
                "board_path": str(board["path"]),
                "status": board["frontmatter"].get("status", entry.get("status", "active")),
                "priority": board["frontmatter"].get("priority", "medium"),
                "updated_at": board["frontmatter"].get("updated_at", ""),
                "next_action": project_board_next_action(board["project_rows"], board["rollup_rows"]),
                "project_rows": board["project_rows"],
                "rollup_rows": board["rollup_rows"],
            }
        )
        if board["frontmatter"].get("board_type") != "project":
            errors.append(f"{board['path'].name}: missing or invalid board_type=project")
        if not board["frontmatter"].get("project_name"):
            errors.append(f"{board['path'].name}: missing project_name")
        errors.extend(validate_task_rows(board["project_rows"], required_headers=PROJECT_BOARD_HEADERS, path=board["path"]))
        errors.extend(validate_task_rows(board["rollup_rows"], required_headers=PROJECT_BOARD_HEADERS, path=board["path"]))
        for topic_path in topic_board_paths(entry["project_name"]):
            topic_board = load_topic_board(topic_path)
            errors.extend(validate_task_rows(topic_board["rows"], required_headers=TOPIC_BOARD_HEADERS, path=topic_path))
            if topic_board["frontmatter"].get("board_type") != "topic":
                errors.append(f"{topic_path.name}: missing or invalid board_type=topic")
            if not topic_board["frontmatter"].get("topic_name"):
                errors.append(f"{topic_path.name}: missing topic_name")
            if not topic_board["frontmatter"].get("rollup_target"):
                errors.append(f"{topic_path.name}: missing rollup_target")
    return facts, errors


def replace_or_append_marked_section(text: str, heading: str, markers: tuple[str, str], lines: list[str]) -> str:
    start, end = markers
    block = "\n".join([heading, "", start, *lines, end]).rstrip() + "\n"
    pattern = re.compile(re.escape(heading) + r"\n\n" + re.escape(start) + r".*?" + re.escape(end) + r"\n?", re.S)
    if pattern.search(text):
        return pattern.sub(block, text)
    suffix = "" if text.endswith("\n") else "\n"
    return f"{text}{suffix}\n{block}"


def launch_agent_plist_path(label: str) -> Path:
    return WORKSPACE_LAUNCH_AGENTS / f"{label}.plist"


def launch_agent_loaded(label: str) -> bool:
    result = subprocess.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def _timed_out_result(command: list[str], *, label: str, exc: subprocess.TimeoutExpired) -> subprocess.CompletedProcess[str]:
    stdout = exc.stdout if isinstance(exc.stdout, str) else ""
    stderr = exc.stderr if isinstance(exc.stderr, str) else ""
    message = stderr.strip() or stdout.strip() or f"{label} timed out after {SYNC_TRIGGER_TIMEOUT_SECONDS}s"
    print(f"[codex_memory] {message}", file=sys.stderr)
    return subprocess.CompletedProcess(command, 124, stdout=stdout, stderr=message)


def _run_sync_trigger(command: list[str], *, cwd: Path, label: str) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=SYNC_TRIGGER_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        return _timed_out_result(command, label=label, exc=exc)
    if result.returncode != 0:
        print(
            f"[codex_memory] {label} failed: {result.stderr.strip() or result.stdout.strip()}",
            file=sys.stderr,
        )
    return result


def _spawn_sync_trigger(command: list[str], *, cwd: Path, label: str) -> dict[str, Any]:
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception as exc:
        print(f"[codex_memory] {label} spawn failed: {exc}", file=sys.stderr)
        return {"ok": False, "label": label, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "label": label, "pid": process.pid, "mode": "async"}


class WorkspaceLockBusy(RuntimeError):
    pass


def _python_command(*args: str) -> list[str]:
    return [sys.executable, *args]


def trigger_dashboard_sync_once(*, wait: bool = True) -> subprocess.CompletedProcess[str] | dict[str, Any] | None:
    _refresh_roots()
    sync_script = WORKSPACE_ROOT / "ops" / "codex_dashboard_sync.py"
    if not sync_script.exists():
        return None
    command = _python_command(str(sync_script), "sync-once")
    if not wait:
        return _spawn_sync_trigger(command, cwd=WORKSPACE_ROOT, label="dashboard sync")
    return _run_sync_trigger(command, cwd=WORKSPACE_ROOT, label="dashboard sync")


def trigger_feishu_projection_sync_once() -> subprocess.CompletedProcess[str] | None:
    _refresh_roots()
    projection_script = WORKSPACE_ROOT / "ops" / "feishu_projection.py"
    if not projection_script.exists():
        return None
    return _run_sync_trigger(
        _python_command(str(projection_script), "run-sync-once"),
        cwd=WORKSPACE_ROOT,
        label="feishu projection sync",
    )


def trigger_growth_feishu_projection_sync_once(*, wait: bool = False) -> subprocess.CompletedProcess[str] | dict[str, Any] | None:
    _refresh_roots()
    projection_script = WORKSPACE_ROOT / "ops" / "growth_feishu_projection.py"
    if not projection_script.exists():
        return None
    command = _python_command(str(projection_script), "run-sync-once")
    if not wait:
        return _spawn_sync_trigger(command, cwd=WORKSPACE_ROOT, label="growth feishu projection sync")
    return _run_sync_trigger(command, cwd=WORKSPACE_ROOT, label="growth feishu projection sync")


def trigger_growth_operator_surface_report_once() -> subprocess.CompletedProcess[str] | None:
    _refresh_roots()
    surface_script = WORKSPACE_ROOT / "ops" / "growth_operator_surface.py"
    if not surface_script.exists():
        return None
    output_path = WORKSPACE_ROOT / "reports" / "system" / f"codex-growth-system-operator-snapshot-{dt.date.today().isoformat()}.md"
    return _run_sync_trigger(
        _python_command(
            str(surface_script),
            "report",
            "--project-name",
            "增长与营销",
            "--output",
            str(output_path),
        ),
        cwd=WORKSPACE_ROOT,
        label="growth operator surface report",
    )


def trigger_growth_daily_brief_once() -> subprocess.CompletedProcess[str] | None:
    _refresh_roots()
    brief_script = WORKSPACE_ROOT / "ops" / "growth_daily_brief.py"
    if not brief_script.exists():
        return None
    return _run_sync_trigger(
        _python_command(
            str(brief_script),
            "deliver-if-needed",
            "--project-name",
            "增长与营销",
            "--chat",
            "增长与营销项目",
        ),
        cwd=WORKSPACE_ROOT,
        label="growth daily brief",
    )


def trigger_retrieval_sync_once() -> subprocess.CompletedProcess[str] | None:
    _refresh_roots()
    retrieval_script = WORKSPACE_ROOT / "ops" / "codex_retrieval.py"
    if not retrieval_script.exists():
        return None
    claimed_events = runtime_state.claim_runtime_events(
        queue_name="retrieval_sync",
        claimed_by="codex_memory.trigger_retrieval_sync_once",
        limit=200,
        lease_seconds=900,
    )
    result = _run_sync_trigger(
        _python_command(str(retrieval_script), "sync-index"),
        cwd=WORKSPACE_ROOT,
        label="retrieval sync",
    )
    for item in claimed_events:
        if result.returncode == 0:
            runtime_state.complete_runtime_event(
                item.get("event_key", ""),
                claim_token=str(item.get("claim_token", "")).strip(),
                result={"trigger": "subprocess", "returncode": result.returncode},
            )
        else:
            runtime_state.fail_runtime_event(
                item.get("event_key", ""),
                claim_token=str(item.get("claim_token", "")).strip(),
                error=result.stderr.strip() or result.stdout.strip(),
                retry_after_seconds=60,
            )
    if result.returncode != 0:
        print(
            f"[codex_memory] retrieval sync failed: {result.stderr.strip() or result.stdout.strip()}",
            file=sys.stderr,
        )
    return result


def record_project_writeback(
    binding: dict[str, Any],
    *,
    source: str,
    changed_targets: list[str] | None = None,
    trigger_dashboard_sync: bool = True,
) -> dict[str, Any]:
    changed = changed_targets or [
        "summary_note",
        "daily_log",
        "next_actions",
        "now_md",
        "session_router",
        "project_bindings",
    ]
    event = {
        "ts": iso_now(),
        "type": "project_writeback",
        "project_name": binding.get("project_name", ""),
        "session_id": binding.get("session_id", ""),
        "source": source,
        "binding_scope": binding.get("binding_scope", "project"),
        "binding_board_path": binding.get("binding_board_path", ""),
        "topic_name": binding.get("topic_name", ""),
        "rollup_target": binding.get("rollup_target", ""),
        "changed_targets": changed,
    }
    identity_payload = json.dumps(
        {
            "project_name": event["project_name"],
            "session_id": event["session_id"],
            "source": event["source"],
            "binding_scope": event["binding_scope"],
            "binding_board_path": event["binding_board_path"],
            "topic_name": event["topic_name"],
            "rollup_target": event["rollup_target"],
            "changed_targets": sorted(event["changed_targets"]),
            "last_active_at": binding.get("last_active_at", binding.get("started_at", "")),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    event["event_id"] = hashlib.sha1(identity_payload.encode("utf-8")).hexdigest()
    project_name = str(binding.get("project_name", "")).strip()
    queue_names = ["retrieval_sync", "dashboard_sync", "feishu_projection_sync"]
    if project_name == "增长与营销":
        queue_names.append("growth_feishu_projection_sync")
    for queue_name in queue_names:
        runtime_state.enqueue_runtime_event(
            queue_name=queue_name,
            event_type="project_writeback",
            event_key=f"{event['event_id']}:{queue_name}",
            dedupe_key=event["event_id"],
            payload=event,
        )
    if event["event_id"] not in recent_event_ids(EVENTS_NDJSON):
        append_ndjson(EVENTS_NDJSON, event)
    event["harness_wake"] = trigger_harness_project_writeback_wake(binding, source=source)
    if trigger_dashboard_sync:
        dashboard_result = trigger_dashboard_sync_once()
        if dashboard_result is None or dashboard_result.returncode == 0:
            trigger_feishu_projection_sync_once()
            if project_name == "增长与营销":
                trigger_growth_feishu_projection_sync_once(wait=False)
                trigger_growth_operator_surface_report_once()
                trigger_growth_daily_brief_once()
    return event


def should_trigger_harness_project_writeback_wake(binding: dict[str, Any], *, source: str) -> bool:
    project_name = str(binding.get("project_name", "")).strip()
    if not project_name:
        return False
    normalized_source = str(source or "").strip().lower()
    if not normalized_source:
        return True
    if normalized_source == "session-watcher" and any(
        str(item).strip() for item in binding.get("task_writeback_refs", []) or []
    ):
        return False
    if normalized_source in {"background-job-executor", "harness_task_create"}:
        return False
    if normalized_source.startswith("background-job"):
        return False
    return True


def trigger_harness_project_writeback_wake(binding: dict[str, Any], *, source: str) -> dict[str, Any]:
    if not should_trigger_harness_project_writeback_wake(binding, source=source):
        return {
            "executed": False,
            "reason": "skipped",
            "project_name": str(binding.get("project_name", "")).strip(),
        }
    try:
        from ops import background_job_executor
    except ImportError:  # pragma: no cover
        import background_job_executor  # type: ignore

    try:
        return background_job_executor.run_requested_project_wake(
            str(binding.get("project_name", "")).strip(),
            reason="project_writeback",
            trigger_source=source or "project_writeback",
        )
    except Exception as exc:  # pragma: no cover - keep writeback path honest but resilient
        return {
            "executed": False,
            "reason": "trigger_failed",
            "project_name": str(binding.get("project_name", "")).strip(),
            "error": f"{type(exc).__name__}: {exc}",
        }


def binding_identity(binding: dict[str, Any]) -> str:
    return (
        binding.get("session_id")
        or binding.get("resume_session_id")
        or binding.get("launch_id")
        or binding.get("started_at")
        or str(id(binding))
    )


def unique_completed_bindings(bindings: list[dict[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    completed = [item for item in bindings if item.get("status") == "completed" and item.get("project_name")]
    completed.sort(key=lambda item: item.get("last_active_at", item.get("started_at", "")), reverse=True)
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in completed:
        key = binding_identity(item)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
        if limit and len(unique) >= limit:
            break
    return unique


def render_binding_line(binding: dict[str, Any]) -> str:
    local_time = display_timestamp(binding.get("last_active_at") or binding.get("started_at") or "")
    thread_name = binding.get("thread_name") or binding.get("prompt") or "Untitled session"
    return (
        f"- {local_time} | `{binding['project_name']}` | `{binding.get('mode', 'new')}` | "
        f"`{binding.get('session_id', 'pending')}` | {thread_name}"
    )


def render_followup_line(binding: dict[str, Any]) -> str:
    thread_name = binding.get("thread_name") or binding.get("prompt") or "最近会话"
    return f"- [ ] 检查 `{binding['project_name']}` 最近会话沉淀：{thread_name}"


def followup_still_needed(binding: dict[str, Any]) -> bool:
    board_path = binding.get("binding_board_path", "")
    if not board_path:
        return True
    path = Path(board_path)
    if not path.exists():
        return True
    last_active = parse_iso_timestamp(binding.get("last_active_at") or binding.get("started_at") or "")
    if not last_active:
        return True
    board_updated = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    return board_updated < last_active.astimezone(dt.timezone.utc)


def unique_actionable_followups(bindings: list[dict[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    actionable = unique_completed_bindings(bindings)
    selected: list[dict[str, Any]] = []
    seen_projects: set[str] = set()
    for item in actionable:
        project_name = item.get("project_name", "")
        if not project_name or project_name in seen_projects:
            continue
        if not followup_still_needed(item):
            continue
        seen_projects.add(project_name)
        selected.append(item)
        if limit and len(selected) >= limit:
            break
    return selected


def load_bindings() -> dict[str, Any]:
    _refresh_roots()
    return load_json(PROJECT_BINDINGS_JSON, {"version": 1, "updated_at": None, "bindings": []})


def save_bindings(data: dict[str, Any]) -> None:
    _refresh_roots()
    data["updated_at"] = iso_now()
    dump_json(PROJECT_BINDINGS_JSON, data)


def load_router() -> dict[str, Any]:
    _refresh_roots()
    return load_json(SESSION_ROUTER_JSON, {"version": 1, "updated_at": None, "routes": {}})


def save_router(data: dict[str, Any]) -> None:
    _refresh_roots()
    data["updated_at"] = iso_now()
    dump_json(SESSION_ROUTER_JSON, data)


def read_session_index() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in read_text(SESSION_INDEX_JSONL).splitlines():
        if not line.strip():
            continue
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return items


def iter_session_files_near(started_at: str) -> list[Path]:
    try:
        started = dt.datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return []
    roots = {
        SESSIONS_ROOT / started.strftime("%Y/%m/%d"),
        SESSIONS_ROOT / (started - dt.timedelta(days=1)).strftime("%Y/%m/%d"),
        SESSIONS_ROOT / (started + dt.timedelta(days=1)).strftime("%Y/%m/%d"),
    }
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(path for path in root.glob("*.jsonl") if path.is_file())
    return sorted(files)


def parse_session_file(path: Path) -> dict[str, Any] | None:
    session_id = ""
    timestamp = ""
    cwd = ""
    first_user_message = ""
    last_agent_message = ""
    for line in read_text(path).splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        item_type = item.get("type")
        payload = item.get("payload", {})
        if item_type == "session_meta":
            session_id = payload.get("id", "")
            timestamp = payload.get("timestamp", "") or item.get("timestamp", "")
            cwd = payload.get("cwd", "")
        elif item_type == "event_msg" and payload.get("type") == "user_message" and not first_user_message:
            first_user_message = payload.get("message", "")
        elif item_type == "event_msg" and payload.get("type") == "task_complete":
            last_agent_message = payload.get("last_agent_message", "")
    if not session_id:
        return None
    return {
        "id": session_id,
        "updated_at": timestamp,
        "cwd": cwd,
        "user_message": first_user_message,
        "last_agent_message": last_agent_message,
    }


def read_history_entries(session_id: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for line in read_text(HISTORY_JSONL).splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("session_id") == session_id:
            entries.append(item)
    return entries


def resolve_recent_session(started_at: str, prompt: str = "") -> dict[str, Any] | None:
    try:
        started = dt.datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    prompt = prompt.strip()
    session_files = iter_session_files_near(started_at)
    file_candidates: list[dict[str, Any]] = []
    for file_path in session_files:
        meta = parse_session_file(file_path)
        if not meta:
            continue
        updated_at = meta.get("updated_at")
        if not updated_at:
            continue
        try:
            updated = dt.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if updated < started - dt.timedelta(minutes=5):
            continue
        if meta.get("cwd") and meta["cwd"] != str(WORKSPACE_ROOT):
            continue
        if prompt and meta.get("user_message") and meta["user_message"].strip() != prompt:
            continue
        file_candidates.append(meta)
    file_candidates.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    if file_candidates:
        return file_candidates[0]

    candidates: list[dict[str, Any]] = []
    for item in read_session_index():
        updated_at = item.get("updated_at")
        if not updated_at:
            continue
        try:
            updated = dt.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        except ValueError:
            continue
        if updated >= started - dt.timedelta(minutes=5):
            candidates.append(item)
    candidates.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return candidates[0] if candidates else None


def update_summary_note(project_name: str, binding: dict[str, Any], summary_text: str) -> None:
    project_name = canonical_project_name(project_name)
    summary_path = project_summary_path(project_name)
    text = read_text(summary_path)
    frontmatter, body = parse_frontmatter(text)
    frontmatter["updated_at"] = dt.date.today().isoformat()
    writeback_at = binding.get("last_active_at", binding["started_at"])
    frontmatter["last_writeback_at"] = display_timestamp(writeback_at)
    frontmatter["last_writeback_session_id"] = binding.get("session_id", "")
    frontmatter["last_writeback_thread"] = binding.get("thread_name") or binding.get("prompt") or "Untitled session"
    if summary_text:
        frontmatter["last_writeback_excerpt"] = summary_text.strip().replace("\n", " ")[:160]
    elif binding.get("thread_name") or binding.get("prompt"):
        frontmatter["last_writeback_excerpt"] = (
            f"最近处理会话：{binding.get('thread_name') or binding.get('prompt')}"
        )[:160]
    entries: list[str] = []
    marker_re = re.compile(re.escape(AUTO_WRITEBACK_MARKERS[0]) + r"(.*?)" + re.escape(AUTO_WRITEBACK_MARKERS[1]), re.S)
    match = marker_re.search(body)
    if match:
        existing_block = match.group(1).strip()
        if existing_block and existing_block != "- 暂无自动写回记录":
            for line in existing_block.splitlines():
                if line.startswith("- "):
                    entries.append(line)
    new_line = (
        f"- {display_timestamp(writeback_at)} | "
        f"`{binding.get('session_id', 'pending')}` | "
        f"{binding.get('thread_name') or binding.get('prompt') or 'Untitled session'}"
    )
    if summary_text:
        new_line = f"{new_line} | {summary_text.strip().replace(chr(10), ' ')[:160]}"
    deduped = []
    for line in entries:
        if binding.get("session_id") and binding["session_id"] in line:
            continue
        if binding.get("prompt") and binding["prompt"] in line and summary_text and summary_text[:40] in line:
            continue
        deduped.append(line)
    rendered = [new_line]
    rendered.extend(deduped[:4])
    body = replace_or_append_marked_section(body.strip() + "\n", "## 自动写回", AUTO_WRITEBACK_MARKERS, rendered)
    write_text(summary_path, f"{render_frontmatter(frontmatter)}\n\n{body.lstrip()}")


def update_daily_log(binding: dict[str, Any], summary_text: str) -> None:
    writeback_at = binding.get("last_active_at", binding["started_at"])
    date_text = display_date(writeback_at)
    daily_path = DAILY_ROOT / f"{date_text}.md"
    current = read_text(daily_path, default=f"# {date_text}\n\n")
    entries: list[dict[str, str]] = []
    chunks = re.split(r"(?=^## )", current, flags=re.M)
    prefix = chunks[0] if chunks else f"# {date_text}\n\n"
    for chunk in chunks[1:]:
        lines = [line for line in chunk.strip().splitlines() if line.strip()]
        if not lines:
            continue
        heading = lines[0].replace("## ", "", 1).strip()
        data = {"project_name": heading}
        for line in lines[1:]:
            if line.startswith("- 时间: "):
                data["time"] = line.replace("- 时间: ", "", 1)
            elif line.startswith("- session_id: "):
                data["session_id"] = line.replace("- session_id: `", "", 1).rstrip("`")
            elif line.startswith("- mode: "):
                data["mode"] = line.replace("- mode: `", "", 1).rstrip("`")
            elif line.startswith("- thread_name: "):
                data["thread_name"] = line.replace("- thread_name: ", "", 1)
            elif line.startswith("- prompt: "):
                data["prompt"] = line.replace("- prompt: ", "", 1)
            elif line.startswith("- summary: "):
                data["summary"] = line.replace("- summary: ", "", 1)
        entries.append(data)
    new_entry = {
        "project_name": binding["project_name"],
        "time": display_timestamp(writeback_at),
        "session_id": binding.get("session_id", ""),
        "mode": binding.get("mode", "new"),
        "thread_name": binding.get("thread_name", ""),
        "prompt": binding.get("prompt", ""),
        "summary": summary_text.strip().replace(chr(10), " ")[:400] if summary_text else "",
    }
    filtered: list[dict[str, str]] = []
    for item in entries:
        if new_entry["session_id"] and item.get("session_id") == new_entry["session_id"]:
            continue
        if item.get("project_name") == new_entry["project_name"] and item.get("prompt") == new_entry["prompt"] and item.get("summary") == new_entry["summary"]:
            continue
        filtered.append(item)
    filtered.insert(0, new_entry)
    rendered = [prefix.rstrip(), ""]
    for item in filtered:
        rendered.append(f"## {item['project_name']}")
        rendered.append(f"- 时间: {item.get('time', '')}")
        rendered.append(f"- session_id: `{item.get('session_id', '')}`")
        rendered.append(f"- mode: `{item.get('mode', 'new')}`")
        if item.get("thread_name"):
            rendered.append(f"- thread_name: {item['thread_name']}")
        if item.get("prompt"):
            rendered.append(f"- prompt: {item['prompt']}")
        if item.get("summary"):
            rendered.append(f"- summary: {item['summary']}")
    write_text(daily_path, "\n".join(rendered).rstrip() + "\n")


def project_rollup_sections() -> dict[str, list[str]]:
    sections = {"doing": [], "todo": [], "blocked": [], "done": []}
    for entry in sorted(load_registry(), key=lambda item: item["project_name"].lower()):
        board = load_project_board(entry["project_name"])
        grouped = select_project_focus_tasks(board["project_rows"], board["rollup_rows"], board.get("gflow_rows", []))
        chosen_by_status: dict[str, list[dict[str, str]]] = {"doing": [], "todo": [], "blocked": [], "done": []}
        if grouped["doing"]:
            chosen_by_status["doing"] = grouped["doing"]
        elif grouped["todo"]:
            chosen_by_status["todo"] = grouped["todo"][:1]
        if grouped["blocked"]:
            chosen_by_status["blocked"] = grouped["blocked"]
        for status, rows in chosen_by_status.items():
            prefix = "[x]" if status == "done" else "[ ]"
            for row in rows:
                sections[status].append(
                    f"- {prefix} `{entry['project_name']}` {row.get('ID', '')} {row.get('事项', '')} | 下一步：{row.get('下一步', '待补充')}"
                )
    return sections


def refresh_next_actions_rollup() -> None:
    text = read_text(NEXT_ACTIONS_MD)
    sections = project_rollup_sections()
    lines = ["### doing"]
    lines.extend(sections["doing"] or ["- [ ] 暂无 doing"])
    lines.extend(["", "### todo"])
    lines.extend(sections["todo"] or ["- [ ] 暂无 todo"])
    lines.extend(["", "### blocked"])
    lines.extend(sections["blocked"] or ["- [ ] 暂无 blocked"])
    lines.extend(["", "### done"])
    lines.extend(sections["done"] or ["- [x] 暂无 done"])
    text = replace_or_append_marked_section(text, "## Auto Project Rollup", AUTO_PROJECT_ROLLUP_MARKERS, lines)
    write_text(NEXT_ACTIONS_MD, text)


def sync_project_layers(binding: dict[str, Any], *, task_updates: list[dict[str, str]] | None = None) -> list[str]:
    changed_targets: list[str] = []
    binding_scope = binding.get("binding_scope", "project")
    board_path = Path(binding.get("binding_board_path") or ensure_project_board(binding["project_name"]))
    default_updated_at = binding.get("last_active_at") or binding.get("started_at") or iso_now()
    if binding_scope == "topic" and board_path.exists():
        topic_board = load_topic_board(board_path)
        if task_updates:
            _apply_task_updates(topic_board["rows"], task_updates, default_updated_at=default_updated_at)
        save_topic_board(board_path, topic_board["frontmatter"], topic_board["body"], topic_board["rows"])
        changed_targets.append(str(board_path))
        project_path = refresh_project_rollups(binding["project_name"], topic_path=board_path)
        changed_targets.append(str(project_path))
    else:
        project_board = load_project_board(binding["project_name"])
        if task_updates:
            _apply_task_updates(project_board["project_rows"], task_updates, default_updated_at=default_updated_at)
            _apply_task_updates(project_board["rollup_rows"], task_updates, default_updated_at=default_updated_at)
        save_project_board(
            project_board["path"],
            project_board["frontmatter"],
            project_board["body"],
            project_board["project_rows"],
            project_board["rollup_rows"],
            project_board.get("gflow_rows", []),
        )
        changed_targets.append(str(project_board["path"]))
    return changed_targets


def update_now_and_next_actions(bindings: list[dict[str, Any]]) -> None:
    recent = unique_completed_bindings(bindings, limit=5)
    now_lines = [render_binding_line(item) for item in recent] or ["- 暂无自动会话记录"]
    followups = unique_actionable_followups(bindings, limit=5)
    next_lines = [render_followup_line(item) for item in followups]
    for import_name, func_name in [
        ("review_plane", "followup_lines"),
        ("coordination_plane", "followup_lines"),
    ]:
        try:
            module = __import__(f"ops.{import_name}", fromlist=[func_name])
        except ImportError:  # pragma: no cover
            try:
                module = __import__(import_name)
            except ImportError:
                module = None
        if module and hasattr(module, func_name):
            next_lines.extend(getattr(module, func_name)(limit=10))
    deduped_lines: list[str] = []
    seen_lines: set[str] = set()
    for line in next_lines:
        if line in seen_lines:
            continue
        seen_lines.add(line)
        deduped_lines.append(line)
    next_lines = deduped_lines or ["- [ ] 暂无自动跟进项"]
    now_text = read_text(NOW_MD)
    now_text = replace_or_append_marked_section(now_text, "## Auto Session State", AUTO_RECENT_MARKERS, now_lines)
    write_text(NOW_MD, now_text)
    next_text = read_text(NEXT_ACTIONS_MD)
    next_text = replace_or_append_marked_section(next_text, "## Generated Follow-ups", AUTO_FOLLOWUP_MARKERS, next_lines)
    write_text(NEXT_ACTIONS_MD, next_text)
    refresh_next_actions_rollup()


def cmd_discover_projects(_args: argparse.Namespace) -> int:
    entries = load_registry()
    known_names = {item["project_name"] for item in entries}
    known_paths = {item["path"] for item in entries}
    changed = False
    PROJECTS_ROOT.mkdir(parents=True, exist_ok=True)
    for project_dir in sorted(PROJECTS_ROOT.iterdir(), key=lambda path: path.name.lower()):
        if not project_dir.is_dir() or project_dir.name.startswith("."):
            continue
        if project_dir.name in known_names or str(project_dir) in known_paths:
            continue
        aliases = default_aliases(project_dir.name)
        create_project_summary(project_dir.name, project_dir, aliases)
        create_project_board(project_dir.name)
        entries.append(
            {
                "project_name": project_dir.name,
                "aliases": aliases,
                "path": str(project_dir),
                "status": "active",
                "summary_note": str(project_summary_path(project_dir.name)),
            }
        )
        changed = True
    if changed:
        write_registry(entries)
        refresh_active_projects(entries)
        refresh_next_actions_rollup()
    print(json.dumps({"changed": changed, "projects": [item["project_name"] for item in entries]}, ensure_ascii=False))
    return 0


def cmd_unregister_project(args: argparse.Namespace) -> int:
    project_name = canonical_project_name(args.project_name)
    entries = load_registry()
    filtered = [item for item in entries if item["project_name"] != project_name]
    if len(filtered) == len(entries):
        print(json.dumps({"changed": False, "project_name": project_name}, ensure_ascii=False))
        return 0
    write_registry(filtered)
    refresh_active_projects(filtered)
    refresh_next_actions_rollup()
    summary_path = project_summary_path(project_name)
    if summary_path.exists():
        summary_path.unlink()
    project_dir = PROJECTS_ROOT / project_name
    if args.delete_project_dir and project_dir.exists() and project_dir.is_dir():
        for child in sorted(project_dir.rglob("*"), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
        project_dir.rmdir()
    print(json.dumps({"changed": True, "project_name": project_name}, ensure_ascii=False))
    return 0


def cmd_refresh_index(_args: argparse.Namespace) -> int:
    entries = load_registry()
    write_registry(entries)
    for entry in entries:
        refresh_project_rollups(entry["project_name"])
    refresh_active_projects(entries)
    refresh_next_actions_rollup()
    for import_name, func_name in [
        ("review_plane", "rebuild_review_inbox"),
        ("coordination_plane", "rebuild_coordination_projection"),
    ]:
        try:
            module = __import__(f"ops.{import_name}", fromlist=[func_name])
        except ImportError:  # pragma: no cover
            try:
                module = __import__(import_name)
            except ImportError:
                module = None
        if module and hasattr(module, func_name):
            getattr(module, func_name)(sync_runtime=True)
    print(json.dumps({"changed": True, "projects": [item["project_name"] for item in entries]}, ensure_ascii=False))
    return 0


def cmd_register_launch(args: argparse.Namespace) -> int:
    with workspace_lock():
        data = load_bindings()
        launch_id = str(uuid.uuid4())
        project_name = canonical_project_name(args.project_name)
        binding = {
            "launch_id": launch_id,
            "project_name": project_name,
            "prompt": args.prompt or "",
            "mode": args.mode,
            "started_at": iso_now(),
            "status": "running",
            "resume_session_id": args.resume_session_id or "",
            "session_id": args.resume_session_id or "",
            "binding_scope": args.binding_scope or "project",
            "binding_board_path": normalize_vault_path(args.binding_board_path or str(ensure_project_board(project_name))),
            "topic_name": args.topic_name or "",
            "rollup_target": normalize_vault_path(args.rollup_target or str(ensure_project_board(project_name))),
            "launch_source": args.launch_source or "",
            "source_chat_ref": args.source_chat_ref or "",
            "source_thread_name": args.source_thread_name or "",
            "source_thread_label": args.source_thread_label or "",
            "source_message_id": args.source_message_id or "",
        }
        data["bindings"].append(binding)
        save_bindings(data)
    print(launch_id)
    return 0


def cmd_finalize_launch(args: argparse.Namespace) -> int:
    final_status = args.final_status
    changed_targets: list[str] = []
    pause_payload: dict[str, Any] = {}
    with workspace_lock():
        data = load_bindings()
        binding = next((item for item in data["bindings"] if item.get("launch_id") == args.launch_id), None)
        if not binding:
            print(f"Unknown launch_id: {args.launch_id}", file=sys.stderr)
            return 1
        session_meta = None
        if args.session_id:
            session_meta = {"id": args.session_id, "updated_at": iso_now(), "thread_name": args.thread_name or ""}
        else:
            session_meta = resolve_recent_session(binding["started_at"], binding.get("prompt", ""))
        if session_meta:
            binding["session_id"] = session_meta.get("id", "")
            binding["last_active_at"] = session_meta.get("updated_at", iso_now())
            if session_meta.get("thread_name"):
                binding["thread_name"] = session_meta["thread_name"]
            elif session_meta.get("user_message"):
                binding["thread_name"] = session_meta["user_message"]
        else:
            binding["last_active_at"] = iso_now()
        binding["binding_board_path"] = normalize_vault_path(binding.get("binding_board_path", ""))
        binding["rollup_target"] = normalize_vault_path(binding.get("rollup_target", ""))
        if args.thread_name:
            binding["thread_name"] = args.thread_name
        elif binding.get("source_thread_name") and not binding.get("thread_name"):
            binding["thread_name"] = binding["source_thread_name"]
        elif binding.get("prompt") and not binding.get("thread_name"):
            binding["thread_name"] = binding["prompt"]
        summary_text = ""
        if args.summary_file:
            summary_text = read_text(Path(args.summary_file)).strip()
        if not summary_text and binding.get("session_id"):
            session_meta = resolve_recent_session(binding["started_at"], binding.get("prompt", ""))
            if session_meta and session_meta.get("last_agent_message"):
                summary_text = session_meta["last_agent_message"].strip()
            if not summary_text:
                history = read_history_entries(binding["session_id"])
                if history:
                    summary_text = history[-1].get("text", "").strip()
        binding["summary_excerpt"] = summary_text[:400] if summary_text else ""
        binding["status"] = final_status
        save_bindings(data)

        if final_status == "completed":
            pause_payload = active_project_pause(binding["project_name"], scope="session_writeback")
            if pause_payload.get("active"):
                binding["writeback_suppressed"] = True
                binding["suppression_reason"] = str(pause_payload.get("entry", {}).get("reason", "")).strip()
                save_bindings(data)
            else:
                router = load_router()
                router["routes"][binding["project_name"]] = {
                    "project_name": binding["project_name"],
                    "last_session_id": binding.get("session_id") or "",
                    "last_active_at": binding.get("last_active_at", binding["started_at"]),
                    "last_summary_path": str(project_summary_path(binding["project_name"])),
                    "last_thread_name": binding.get("thread_name", ""),
                    "last_launch_source": binding.get("launch_source", ""),
                    "last_source_chat_ref": binding.get("source_chat_ref", ""),
                    "last_source_thread_name": binding.get("source_thread_name", ""),
                    "last_source_thread_label": binding.get("source_thread_label", ""),
                    "binding_scope": binding.get("binding_scope", "project"),
                    "binding_board_path": binding.get("binding_board_path", ""),
                    "topic_name": binding.get("topic_name", ""),
                    "rollup_target": binding.get("rollup_target", ""),
                }
                save_router(router)

                changed_targets = sync_project_layers(binding)
                update_summary_note(binding["project_name"], binding, summary_text)
                update_daily_log(binding, summary_text)
                update_now_and_next_actions(data["bindings"])
                changed_targets.extend(
                    [
                        str(project_summary_path(binding["project_name"])),
                        str(DAILY_ROOT / f"{display_date(binding.get('last_active_at', binding['started_at']))}.md"),
                        str(NOW_MD),
                        str(NEXT_ACTIONS_MD),
                        str(SESSION_ROUTER_JSON),
                        str(PROJECT_BINDINGS_JSON),
                    ]
                )
                record_project_writeback(binding, source="start-codex", changed_targets=changed_targets, trigger_dashboard_sync=False)

    if final_status == "completed" and not pause_payload.get("active"):
        trigger_retrieval_sync_once()
        trigger_dashboard_sync_once(wait=False)
    print(
        json.dumps(
            {
                "project_name": binding["project_name"],
                "session_id": binding.get("session_id", ""),
                "last_active_at": binding.get("last_active_at", ""),
                "reply_text": summary_text,
                "summary_excerpt": binding.get("summary_excerpt", ""),
                "binding_scope": binding.get("binding_scope", "project"),
                "binding_board_path": binding.get("binding_board_path", ""),
                "launch_source": binding.get("launch_source", ""),
                "source_chat_ref": binding.get("source_chat_ref", ""),
                "source_thread_name": binding.get("source_thread_name", ""),
                "source_thread_label": binding.get("source_thread_label", ""),
                "source_message_id": binding.get("source_message_id", ""),
                "status": final_status,
                "writeback_suppressed": bool(pause_payload.get("active")),
                "pause": pause_payload,
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_resolve_board_binding(args: argparse.Namespace) -> int:
    print(json.dumps(resolve_board_binding(args.project_name, args.prompt or ""), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex workspace memory utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover-projects")
    discover.set_defaults(func=cmd_discover_projects)

    unregister = subparsers.add_parser("unregister-project")
    unregister.add_argument("--project-name", required=True)
    unregister.add_argument("--delete-project-dir", action="store_true")
    unregister.set_defaults(func=cmd_unregister_project)

    refresh = subparsers.add_parser("refresh-index")
    refresh.set_defaults(func=cmd_refresh_index)

    register = subparsers.add_parser("register-launch")
    register.add_argument("--project-name", required=True)
    register.add_argument("--prompt", default="")
    register.add_argument("--mode", choices=["new", "resume"], default="new")
    register.add_argument("--resume-session-id", default="")
    register.add_argument("--binding-scope", default="project")
    register.add_argument("--binding-board-path", default="")
    register.add_argument("--topic-name", default="")
    register.add_argument("--rollup-target", default="")
    register.add_argument("--launch-source", default="")
    register.add_argument("--source-chat-ref", default="")
    register.add_argument("--source-thread-name", default="")
    register.add_argument("--source-thread-label", default="")
    register.add_argument("--source-message-id", default="")
    register.set_defaults(func=cmd_register_launch)

    finalize = subparsers.add_parser("finalize-launch")
    finalize.add_argument("--launch-id", required=True)
    finalize.add_argument("--session-id", default="")
    finalize.add_argument("--thread-name", default="")
    finalize.add_argument("--summary-file", default="")
    finalize.add_argument("--final-status", choices=["completed", "aborted", "failed"], default="completed")
    finalize.set_defaults(func=cmd_finalize_launch)

    resolve_binding = subparsers.add_parser("resolve-board-binding")
    resolve_binding.add_argument("--project-name", required=True)
    resolve_binding.add_argument("--prompt", default="")
    resolve_binding.set_defaults(func=cmd_resolve_board_binding)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

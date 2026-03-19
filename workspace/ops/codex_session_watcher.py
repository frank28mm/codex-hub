#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops.codex_memory import (
        PROJECTS_ROOT,
        PROJECT_SUMMARY_ROOT,
        PROJECT_BINDINGS_JSON,
        REGISTRY_MD,
        RUNTIME_ROOT,
        NEXT_ACTIONS_MD,
        NOW_MD,
        SESSION_ROUTER_JSON,
        SESSIONS_ROOT,
        WORKING_ROOT,
        WORKSPACE_ROOT,
        create_project_summary,
        default_aliases,
        dump_json,
        iso_now,
        load_bindings,
        load_registry,
        load_router,
        read_text,
        refresh_active_projects,
        record_project_writeback,
        resolve_board_binding,
        normalize_task_writebacks,
        normalize_vault_path,
        save_bindings,
        save_router,
        trigger_retrieval_sync_once,
        trigger_dashboard_sync_once,
        update_daily_log,
        update_now_and_next_actions,
        sync_project_layers,
        update_summary_note,
        workspace_lock,
        write_registry,
    )
    from ops import runtime_ingestion
    from ops import project_pause
    from ops.workspace_hub_project import PROJECT_NAME as WORKSPACE_HUB_PROJECT_NAME
except ImportError:
    from codex_memory import (
        PROJECTS_ROOT,
        PROJECT_SUMMARY_ROOT,
        PROJECT_BINDINGS_JSON,
        REGISTRY_MD,
        RUNTIME_ROOT,
        NEXT_ACTIONS_MD,
        NOW_MD,
        SESSION_ROUTER_JSON,
        SESSIONS_ROOT,
        WORKING_ROOT,
        WORKSPACE_ROOT,
        create_project_summary,
        default_aliases,
        dump_json,
        iso_now,
        load_bindings,
        load_registry,
        load_router,
        read_text,
        refresh_active_projects,
        record_project_writeback,
        resolve_board_binding,
        normalize_task_writebacks,
        normalize_vault_path,
        save_bindings,
        save_router,
        trigger_retrieval_sync_once,
        trigger_dashboard_sync_once,
        update_daily_log,
        update_now_and_next_actions,
        sync_project_layers,
        update_summary_note,
        workspace_lock,
        write_registry,
    )
    import runtime_ingestion  # type: ignore
    import project_pause  # type: ignore
    from workspace_hub_project import PROJECT_NAME as WORKSPACE_HUB_PROJECT_NAME


WATCH_STATE_JSON = RUNTIME_ROOT / "session-watch-state.json"
WATCHER_NAME = "com.codexhub.codex-memory-watcher"
LAUNCH_AGENT_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{WATCHER_NAME}.plist"
LOG_STDOUT = WORKSPACE_ROOT / "logs" / "codex-session-watcher.log"
LOG_STDERR = WORKSPACE_ROOT / "logs" / "codex-session-watcher.err.log"
DEFAULT_IDLE_SECONDS = 30 * 60
IDLE_NOTIFICATION_TITLE = "Codex 线程提醒"
NOTIFICATION_RETRY_SECONDS = 5 * 60


def load_worktree_route_registry() -> dict[str, dict[str, str]]:
    project_board = WORKING_ROOT / f"{WORKSPACE_HUB_PROJECT_NAME}-项目板.md"
    worktrees_root = WORKSPACE_ROOT.parent / "workspace-hub-worktrees"
    return {
        str((worktrees_root / "feishu-bridge").resolve()): {
            "project_name": WORKSPACE_HUB_PROJECT_NAME,
            "binding_scope": "topic",
            "binding_board_path": str((WORKING_ROOT / f"{WORKSPACE_HUB_PROJECT_NAME}-Feishu Bridge-跟进板.md").resolve()),
            "topic_name": "Feishu Bridge",
            "rollup_target": str(project_board.resolve()),
        },
        str((worktrees_root / "electron-console").resolve()): {
            "project_name": WORKSPACE_HUB_PROJECT_NAME,
            "binding_scope": "topic",
            "binding_board_path": str((WORKING_ROOT / f"{WORKSPACE_HUB_PROJECT_NAME}-Electron Console-跟进板.md").resolve()),
            "topic_name": "Electron Console",
            "rollup_target": str(project_board.resolve()),
        },
    }


def resolve_fixed_workspace_binding(cwd: str) -> dict[str, str] | None:
    if not cwd:
        return None
    try:
        key = str(Path(cwd).resolve())
    except OSError:
        return None
    return load_worktree_route_registry().get(key)


def load_state() -> dict[str, Any]:
    fallback = {"version": 1, "updated_at": None, "sessions": {}}
    try:
        return json.loads(read_text(WATCH_STATE_JSON))
    except json.JSONDecodeError:
        return fallback


def save_state(data: dict[str, Any]) -> None:
    data["updated_at"] = iso_now()
    dump_json(WATCH_STATE_JSON, data)


def load_recent_session_files(days: int = 14) -> list[Path]:
    today = dt.datetime.now(dt.timezone.utc).date()
    files: list[Path] = []
    for delta in range(days):
        date_value = today - dt.timedelta(days=delta)
        root = SESSIONS_ROOT / date_value.strftime("%Y/%m/%d")
        if not root.exists():
            continue
        files.extend(path for path in root.glob("*.jsonl") if path.is_file())
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files


DEFAULT_TRANSCRIPT_SCHEMA: dict[str, Any] = {
    "timestamp_paths": ["timestamp"],
    "shared_fields": runtime_ingestion.transcript_shared_fields(),
    "rules": [
        {
            "match": {"type": "session_meta"},
            "fields": {
                "id": ["payload.id"],
                "started_at": ["payload.timestamp", "timestamp"],
                "cwd": ["payload.cwd"],
            },
        },
        {
            "match": {"type": "event_msg", "payload.type": "user_message"},
            "fields": {
                "user_message": ["payload.message", "payload.text", "message"],
            },
        },
        {
            "match": {"type": "event_msg", "payload.type": "task_complete"},
            "fields": {
                "last_agent_message": ["payload.last_agent_message", "payload.summary", "payload.message"],
                "last_active_at": ["timestamp"],
            },
            "set": {"completed": True},
        },
    ],
}


def _iter_specs(spec: Any) -> list[Any]:
    if isinstance(spec, list):
        return spec
    return [spec]


def _value_by_path(payload: Any, path: str) -> Any:
    current = payload
    for part in str(path or "").split("."):
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part)
            continue
        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
                continue
        return None
    return current


def _matches_line(item: dict[str, Any], match: dict[str, Any]) -> bool:
    for path, expected in (match or {}).items():
        actual = _value_by_path(item, path)
        if isinstance(expected, (list, tuple, set)):
            if actual not in expected and str(actual) not in {str(value) for value in expected}:
                return False
            continue
        if actual != expected and str(actual) != str(expected):
            return False
    return True


def _resolve_spec_value(item: dict[str, Any], spec: Any) -> Any:
    if isinstance(spec, dict):
        if not _matches_line(item, spec.get("when", {})):
            return None
        paths = spec.get("paths")
        if paths is None and "path" in spec:
            paths = [spec["path"]]
        return _resolve_spec_value(item, paths or [])

    for candidate in _iter_specs(spec):
        if isinstance(candidate, (dict, list)):
            value = _resolve_spec_value(item, candidate)
        else:
            value = _value_by_path(item, str(candidate))
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        elif value in ({}, [], ()):
            continue
        return value
    return None


def _apply_schema_fields(snapshot: dict[str, Any], item: dict[str, Any], fields: dict[str, Any]) -> None:
    for key, spec in (fields or {}).items():
        value = _resolve_spec_value(item, spec)
        if value is None:
            continue
        snapshot[key] = value


def parse_session_snapshot(path: Path, schema: dict[str, Any] | None = None) -> dict[str, Any] | None:
    transcript_schema = schema or DEFAULT_TRANSCRIPT_SCHEMA
    snapshot: dict[str, Any] = {
        "id": "",
        "started_at": "",
        "last_active_at": "",
        "last_event_at": "",
        "cwd": "",
        "user_message": "",
        "last_agent_message": "",
        "project_name": "",
        "binding_scope": "",
        "binding_board_path": "",
        "topic_name": "",
        "rollup_target": "",
        "launch_source": "",
        "source_chat_ref": "",
        "source_thread_name": "",
        "source_thread_label": "",
        "execution_profile": "",
        "model": "",
        "reasoning_effort": "",
        "completed": False,
    }

    for line in read_text(path).splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue

        timestamp = _resolve_spec_value(item, transcript_schema.get("timestamp_paths", ["timestamp"]))
        if isinstance(timestamp, str) and timestamp:
            snapshot["last_event_at"] = timestamp

        _apply_schema_fields(snapshot, item, transcript_schema.get("shared_fields", {}))

        for rule in transcript_schema.get("rules", []):
            if not _matches_line(item, rule.get("match", {})):
                continue
            _apply_schema_fields(snapshot, item, rule.get("fields", {}))
            for key, value in rule.get("set", {}).items():
                snapshot[key] = value

    if not snapshot["id"]:
        return None

    last_active_at = str(snapshot.get("last_event_at") or snapshot.get("last_active_at") or snapshot.get("started_at") or "").strip()
    snapshot["last_active_at"] = last_active_at
    snapshot["path"] = str(path)
    snapshot["mtime"] = path.stat().st_mtime
    snapshot["thread_name"] = str(snapshot.get("source_thread_name") or snapshot.get("user_message") or "").strip()
    return snapshot


def auto_discover_projects() -> bool:
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
        entries.append(
            {
                "project_name": project_dir.name,
                "aliases": aliases,
                "path": str(project_dir),
                "status": "active",
                "summary_note": str(PROJECT_SUMMARY_ROOT / f"{project_dir.name}.md"),
            }
        )
        changed = True
    if changed:
        write_registry(entries)
        refresh_active_projects(entries)
    return changed


def resolve_project_from_prompt(prompt: str) -> str:
    prompt = prompt.strip()
    if not prompt:
        return ""
    prompt_l = prompt.lower()
    hits: list[str] = []
    for item in load_registry():
        names = [item.get("project_name", "")] + list(item.get("aliases", []))
        for name in names:
            if name and name.lower() in prompt_l:
                hits.append(item["project_name"])
                break
    unique_hits = sorted(set(hits))
    return unique_hits[0] if len(unique_hits) == 1 else ""


def parse_iso(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def iso_datetime(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_idle_monitors(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    monitors = state.setdefault("idle_monitors", {})
    if isinstance(monitors, dict):
        return monitors
    state["idle_monitors"] = {}
    return state["idle_monitors"]


def send_local_notification(*, title: str, message: str, subtitle: str = "") -> dict[str, Any]:
    script = f"display notification {json.dumps(message)} with title {json.dumps(title)}"
    if subtitle:
        script += f" subtitle {json.dumps(subtitle)}"
    result = subprocess.run(
        ["osascript", "-e", script],
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def resolve_monitored_snapshot(
    session_id: str,
    snapshots: dict[str, dict[str, Any]],
    sessions_state: dict[str, dict[str, Any]],
    monitor: dict[str, Any],
) -> dict[str, Any] | None:
    snapshot = snapshots.get(session_id)
    if snapshot:
        return snapshot
    candidates = [
        str(monitor.get("path", "")).strip(),
        str(monitor.get("last_seen_path", "")).strip(),
        str(sessions_state.get(session_id, {}).get("path", "")).strip(),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if not path.exists():
            continue
        snapshot = parse_session_snapshot(path)
        if snapshot and snapshot.get("id") == session_id:
            return snapshot
    return None


def build_idle_notification(
    *,
    monitor: dict[str, Any],
    session_id: str,
    reason: str,
    idle_seconds: int,
    last_active_at: str,
) -> tuple[str, str, str]:
    label = str(monitor.get("label") or session_id[:8]).strip()
    short_id = session_id[:8]
    if reason == "completed":
        message = f"{label} 已结束，当前会话已空闲。Session: {short_id}"
    else:
        minutes = max(1, idle_seconds // 60)
        last_seen = last_active_at or "unknown"
        message = f"{label} 已超过 {minutes} 分钟无新事件。Session: {short_id}，last_active_at={last_seen}"
    return IDLE_NOTIFICATION_TITLE, message, label


def evaluate_idle_monitors(
    state: dict[str, Any],
    snapshots: dict[str, dict[str, Any]],
    *,
    now: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    current_time = now.astimezone(dt.timezone.utc) if now else dt.datetime.now(dt.timezone.utc)
    checked_at = iso_datetime(current_time)
    sessions_state = state.setdefault("sessions", {})
    notifications: list[dict[str, Any]] = []

    for session_id, monitor in load_idle_monitors(state).items():
        snapshot = resolve_monitored_snapshot(session_id, snapshots, sessions_state, monitor)
        monitor["session_id"] = session_id
        monitor["updated_at"] = checked_at
        monitor["last_checked_at"] = checked_at
        if not snapshot:
            monitor["last_state"] = "missing"
            monitor["last_reason"] = "snapshot_missing"
            continue

        last_active_at = str(snapshot.get("last_active_at", "")).strip()
        completed = bool(snapshot.get("completed"))
        idle_seconds = max(60, int(monitor.get("idle_seconds") or DEFAULT_IDLE_SECONDS))
        label = str(monitor.get("label") or snapshot.get("user_message") or session_id[:8]).strip()
        monitor["label"] = label
        monitor["path"] = snapshot.get("path", "")
        monitor["last_seen_path"] = snapshot.get("path", "")
        monitor["last_seen_completed"] = completed
        monitor["last_seen_last_active_at"] = last_active_at
        activity_marker = f"{last_active_at}|{int(completed)}"
        previous_activity_marker = str(monitor.get("last_activity_marker", "")).strip()
        if activity_marker and activity_marker != previous_activity_marker:
            monitor["last_activity_marker"] = activity_marker

        last_active_dt = parse_iso(last_active_at)
        current_state = "running"
        reason = "active_recently"
        if completed:
            current_state = "idle"
            reason = "completed"
        elif last_active_dt and (current_time - last_active_dt.astimezone(dt.timezone.utc)).total_seconds() >= idle_seconds:
            current_state = "idle"
            reason = "inactive"

        if current_state == "running":
            monitor["last_state"] = current_state
            monitor["last_reason"] = reason
            continue

        monitor["last_state"] = current_state
        monitor["last_reason"] = reason
        if str(monitor.get("last_notified_marker", "")).strip() == activity_marker:
            continue
        if reason == "completed" and monitor.get("notify_on_complete", True) is False:
            continue
        last_attempt_at = parse_iso(str(monitor.get("last_notification_attempt_at", "")).strip())
        if last_attempt_at and (current_time - last_attempt_at.astimezone(dt.timezone.utc)).total_seconds() < NOTIFICATION_RETRY_SECONDS:
            continue

        title, message, subtitle = build_idle_notification(
            monitor=monitor,
            session_id=session_id,
            reason=reason,
            idle_seconds=idle_seconds,
            last_active_at=last_active_at,
        )
        monitor["last_notification_attempt_at"] = checked_at
        delivery = send_local_notification(title=title, message=message, subtitle=subtitle)
        if delivery["ok"]:
            monitor["last_notified_at"] = checked_at
            monitor["last_notified_marker"] = activity_marker
            monitor["notification_count"] = int(monitor.get("notification_count", 0) or 0) + 1
        monitor["last_notification_error"] = delivery.get("stderr", "")
        monitor["last_notification_message"] = message
        notifications.append(
            {
                "session_id": session_id,
                "label": label,
                "state": current_state,
                "reason": reason,
                "delivered": bool(delivery["ok"]),
                "message": message,
            }
        )

    return notifications


def find_recent_snapshot(session_id: str, *, days: int = 14) -> dict[str, Any] | None:
    for path in load_recent_session_files(days=days):
        if session_id not in path.name:
            continue
        snapshot = parse_session_snapshot(path)
        if snapshot and snapshot.get("id") == session_id:
            return snapshot
    return None


def snapshot_prompt_text(snapshot: dict[str, Any]) -> str:
    return str(snapshot.get("user_message") or snapshot.get("source_thread_name") or "").strip()


def snapshot_thread_name(snapshot: dict[str, Any]) -> str:
    return str(snapshot.get("source_thread_name") or snapshot.get("thread_name") or snapshot.get("user_message") or "").strip()


def resolve_project_from_snapshot(snapshot: dict[str, Any]) -> str:
    explicit = str(snapshot.get("project_name") or "").strip()
    if explicit:
        for item in load_registry():
            if str(item.get("project_name", "")).strip() == explicit:
                return explicit
    return resolve_project_from_prompt(snapshot_prompt_text(snapshot))


def resolve_board_binding_from_snapshot(snapshot: dict[str, Any], project_name: str) -> dict[str, str]:
    binding_board_path = normalize_vault_path(str(snapshot.get("binding_board_path") or "").strip())
    if not binding_board_path:
        return resolve_board_binding(project_name, snapshot_prompt_text(snapshot))
    return {
        "binding_scope": str(snapshot.get("binding_scope") or "project").strip() or "project",
        "binding_board_path": binding_board_path,
        "topic_name": str(snapshot.get("topic_name") or "").strip(),
        "rollup_target": normalize_vault_path(str(snapshot.get("rollup_target") or "").strip()),
    }


def find_binding(bindings_data: dict[str, Any], snapshot: dict[str, Any], project_name: str, board_binding: dict[str, str]) -> dict[str, Any]:
    session_id = snapshot["id"]
    for binding in reversed(bindings_data["bindings"]):
        if binding.get("session_id") == session_id:
            return binding
        if binding.get("resume_session_id") == session_id:
            return binding

    started_at = parse_iso(snapshot.get("started_at", ""))
    for binding in reversed(bindings_data["bindings"]):
        if binding.get("project_name") != project_name:
            continue
        if binding.get("binding_scope", "project") != board_binding.get("binding_scope", "project"):
            continue
        if binding.get("binding_board_path", "") != board_binding.get("binding_board_path", ""):
            continue
        if binding.get("status") != "running":
            continue
        binding_started = parse_iso(binding.get("started_at", ""))
        if not binding_started or not started_at:
            continue
        if abs((started_at - binding_started).total_seconds()) <= 7200:
            return binding

    binding = {
        "launch_id": f"watcher-{session_id}",
        "project_name": project_name,
        "prompt": snapshot_prompt_text(snapshot),
        "thread_name": snapshot_thread_name(snapshot),
        "mode": "new",
        "started_at": snapshot.get("started_at") or iso_now(),
        "status": "running",
        "resume_session_id": "",
        "session_id": session_id,
        "origin": "session-watcher",
        "binding_scope": board_binding.get("binding_scope", "project"),
        "binding_board_path": board_binding.get("binding_board_path", ""),
        "topic_name": board_binding.get("topic_name", ""),
        "rollup_target": board_binding.get("rollup_target", ""),
    }
    bindings_data["bindings"].append(binding)
    return binding


def extract_structured_task_updates(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    return normalize_task_writebacks(
        snapshot.get("last_agent_message", ""),
        snapshot_prompt_text(snapshot),
    )


def should_retry_ignored_fixed_workspace_snapshot(snapshot: dict[str, Any], previous: dict[str, Any]) -> bool:
    if not snapshot.get("completed"):
        return False
    if previous.get("last_status") != "ignored":
        return False
    if not resolve_fixed_workspace_binding(snapshot.get("cwd", "")):
        return False
    return True


def sync_snapshot(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    source_workspace = snapshot.get("cwd", "")
    prompt_text = snapshot_prompt_text(snapshot)
    thread_name = snapshot_thread_name(snapshot)
    fixed_binding = resolve_fixed_workspace_binding(source_workspace)
    if fixed_binding:
        project_name = fixed_binding["project_name"]
        board_binding = fixed_binding
    else:
        if source_workspace != str(WORKSPACE_ROOT):
            return None
        project_name = resolve_project_from_snapshot(snapshot)
        if not project_name:
            return {"session_id": snapshot["id"], "action": "ignored", "reason": "general-or-ambiguous"}
        board_binding = resolve_board_binding_from_snapshot(snapshot, project_name)

    pause_payload = project_pause.active_pause(project_name=project_name, scope="session_writeback")
    if pause_payload.get("active"):
        return {
            "session_id": snapshot["id"],
            "action": "suppressed",
            "project_name": project_name,
            "reason": "project_paused",
            "pause": pause_payload,
            "binding_scope": board_binding.get("binding_scope", "project"),
            "topic_name": board_binding.get("topic_name", ""),
            "source_workspace": source_workspace,
            "task_update_count": 0,
        }

    summary_text = snapshot.get("last_agent_message", "").strip()
    task_updates = extract_structured_task_updates(snapshot)
    changed_targets: list[str] = []
    with workspace_lock():
        bindings_data = load_bindings()
        binding = find_binding(bindings_data, snapshot, project_name, board_binding)
        if (
            binding.get("status") == "completed"
            and binding.get("session_id") == snapshot["id"]
            and binding.get("last_active_at") == (snapshot.get("last_active_at") or "")
            and binding.get("summary_excerpt", "") == summary_text[:400]
        ):
            return {
                "session_id": snapshot["id"],
                "action": "ignored",
                "project_name": project_name,
                "reason": "already-synced",
            }

        binding["project_name"] = project_name
        binding["session_id"] = snapshot["id"]
        binding["last_active_at"] = snapshot.get("last_active_at") or iso_now()
        binding["binding_scope"] = board_binding.get("binding_scope", "project")
        binding["binding_board_path"] = normalize_vault_path(board_binding.get("binding_board_path", ""))
        binding["topic_name"] = board_binding.get("topic_name", "")
        binding["rollup_target"] = normalize_vault_path(board_binding.get("rollup_target", ""))
        binding["source_workspace"] = source_workspace
        binding["task_writeback_refs"] = [item.get("task_id", "") for item in task_updates if item.get("task_id")]
        for key in (
            "project_name",
            "launch_source",
            "source_chat_ref",
            "source_thread_name",
            "source_thread_label",
            "execution_profile",
            "model",
            "reasoning_effort",
        ):
            value = str(snapshot.get(key) or "").strip()
            if value:
                binding[key] = value
        if prompt_text:
            binding["prompt"] = prompt_text
        if thread_name:
            binding["thread_name"] = thread_name
        binding["summary_excerpt"] = summary_text[:400] if summary_text else ""
        binding["status"] = "completed"
        save_bindings(bindings_data)

        router = load_router()
        router["routes"][project_name] = {
            "project_name": project_name,
            "last_session_id": snapshot["id"],
            "last_active_at": binding["last_active_at"],
            "last_summary_path": str(PROJECT_SUMMARY_ROOT / f"{project_name}.md"),
            "last_thread_name": binding.get("thread_name", ""),
            "binding_scope": binding.get("binding_scope", "project"),
            "binding_board_path": binding.get("binding_board_path", ""),
            "topic_name": binding.get("topic_name", ""),
            "rollup_target": binding.get("rollup_target", ""),
            "source_workspace": binding.get("source_workspace", ""),
        }
        save_router(router)

        changed_targets = sync_project_layers(binding, task_updates=task_updates)
        update_summary_note(project_name, binding, summary_text)
        update_daily_log(binding, summary_text)
        update_now_and_next_actions(bindings_data["bindings"])
        changed_targets.extend(
            [
                str(PROJECT_SUMMARY_ROOT / f"{project_name}.md"),
                str(NEXT_ACTIONS_MD),
                str(NOW_MD),
                str(SESSION_ROUTER_JSON),
                str(PROJECT_BINDINGS_JSON),
            ]
        )
        record_project_writeback(binding, source="session-watcher", changed_targets=changed_targets, trigger_dashboard_sync=False)

    trigger_retrieval_sync_once()
    trigger_dashboard_sync_once()

    return {
        "session_id": snapshot["id"],
        "action": "synced",
        "project_name": project_name,
        "summary_excerpt": binding.get("summary_excerpt", ""),
        "binding_scope": binding.get("binding_scope", "project"),
        "topic_name": binding.get("topic_name", ""),
        "source_workspace": binding.get("source_workspace", ""),
        "task_update_count": len(task_updates),
    }


def maybe_run_health_check_catchup() -> dict[str, Any]:
    try:
        try:
            from ops import workspace_hub_health_check as health_check
        except ImportError:
            import workspace_hub_health_check as health_check  # type: ignore
    except ImportError as exc:
        return {
            "executed": False,
            "reason": "health_check_import_failed",
            "error": str(exc),
        }

    try:
        payload = health_check.run_catchup_if_stale()
    except Exception as exc:  # pragma: no cover - defensive guard for daemon reliability
        return {
            "executed": False,
            "reason": "health_check_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }

    decision = payload.get("decision", {})
    response = {
        "executed": bool(payload.get("executed")),
        "reason": decision.get("reason", ""),
        "scheduled_for": decision.get("scheduled_for", ""),
        "due_at": decision.get("due_at", ""),
        "overdue_seconds": int(decision.get("overdue_seconds", 0) or 0),
    }
    if payload.get("executed"):
        run_record = payload.get("payload", {}).get("run_record", {})
        response.update(
            {
                "ok": bool(payload.get("payload", {}).get("ok")),
                "run_id": run_record.get("run_id", ""),
                "issue_count": int(run_record.get("issue_count", 0) or 0),
            }
        )
    return response


def scan_once(days: int = 14, limit: int = 200) -> dict[str, Any]:
    auto_discover_projects()
    state = load_state()
    sessions_state = state.setdefault("sessions", {})
    results: list[dict[str, Any]] = []
    snapshots: dict[str, dict[str, Any]] = {}

    files = load_recent_session_files(days=days)[:limit]
    for path in files:
        snapshot = parse_session_snapshot(path)
        if not snapshot:
            continue
        session_id = snapshot["id"]
        snapshots[session_id] = snapshot
        previous = sessions_state.get(session_id, {})
        previous_mtime = float(previous.get("last_synced_mtime", 0.0))
        current_mtime = float(snapshot.get("mtime", 0.0))
        retry_ignored_fixed_workspace = should_retry_ignored_fixed_workspace_snapshot(snapshot, previous)
        if current_mtime <= previous_mtime and not retry_ignored_fixed_workspace:
            continue
        if not snapshot.get("completed"):
            sessions_state[session_id] = {
                "path": snapshot["path"],
                "project_name": previous.get("project_name", ""),
                "last_seen_mtime": current_mtime,
                "last_synced_mtime": previous_mtime,
                "last_status": "incomplete",
            }
            continue

        result = sync_snapshot(snapshot)
        sessions_state[session_id] = {
            "path": snapshot["path"],
            "project_name": (result or {}).get("project_name", previous.get("project_name", "")),
            "last_seen_mtime": current_mtime,
            "last_synced_mtime": current_mtime,
            "last_status": (result or {}).get("action", "ignored"),
            "last_active_at": snapshot.get("last_active_at", ""),
        }
        if result:
            results.append(result)

    idle_notifications = evaluate_idle_monitors(state, snapshots)
    save_state(state)
    health_check = maybe_run_health_check_catchup()
    return {
        "processed": len(results),
        "results": results,
        "idle_monitor_count": len(load_idle_monitors(state)),
        "idle_monitor_notifications": idle_notifications,
        "health_check": health_check,
        "state_path": str(WATCH_STATE_JSON),
    }


def launch_agent_plist(poll_interval: int) -> str:
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    program_args = [
        python_path,
        str(WORKSPACE_ROOT / "ops" / "codex_session_watcher.py"),
        "daemon",
        "--poll-interval",
        str(poll_interval),
    ]
    payload = {
        "Label": WATCHER_NAME,
        "ProgramArguments": program_args,
        "RunAtLoad": True,
        "KeepAlive": True,
        "WorkingDirectory": str(WORKSPACE_ROOT),
        "StandardOutPath": str(LOG_STDOUT),
        "StandardErrorPath": str(LOG_STDERR),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
        },
    }
    return plist_dumps(payload)


def plist_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def plist_value(value: Any, indent: str = "    ") -> str:
    if isinstance(value, bool):
        return f"{indent}<{str(value).lower()}/>"
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


def cmd_scan_once(args: argparse.Namespace) -> int:
    result = scan_once(days=args.days, limit=args.limit)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def cmd_daemon(args: argparse.Namespace) -> int:
    while True:
        result = scan_once(days=args.days, limit=args.limit)
        if args.verbose and result["results"]:
            print(json.dumps(result, ensure_ascii=False), flush=True)
        time.sleep(args.poll_interval)


def run_launchctl(*parts: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *parts],
        text=True,
        capture_output=True,
        check=False,
    )


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    LAUNCH_AGENT_PLIST.parent.mkdir(parents=True, exist_ok=True)
    LOG_STDOUT.parent.mkdir(parents=True, exist_ok=True)
    LAUNCH_AGENT_PLIST.write_text(launch_agent_plist(args.poll_interval), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(LAUNCH_AGENT_PLIST))
    bootstrap = run_launchctl("bootstrap", domain, str(LAUNCH_AGENT_PLIST))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=os.sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{WATCHER_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=os.sys.stderr)
        return kickstart.returncode
    print(
        json.dumps(
            {
                "installed": True,
                "plist": str(LAUNCH_AGENT_PLIST),
                "poll_interval": args.poll_interval,
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(LAUNCH_AGENT_PLIST))
    if LAUNCH_AGENT_PLIST.exists():
        LAUNCH_AGENT_PLIST.unlink()
    print(json.dumps({"installed": False, "plist": str(LAUNCH_AGENT_PLIST)}, ensure_ascii=False))
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    state = load_state()
    domain = f"gui/{os.getuid()}/{WATCHER_NAME}"
    loaded = run_launchctl("print", domain).returncode == 0
    print(
        json.dumps(
            {
                "installed": LAUNCH_AGENT_PLIST.exists(),
                "loaded": loaded,
                "plist": str(LAUNCH_AGENT_PLIST),
                "state_path": str(WATCH_STATE_JSON),
                "tracked_sessions": len(state.get("sessions", {})),
                "idle_monitor_count": len(load_idle_monitors(state)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_monitor_add(args: argparse.Namespace) -> int:
    state = load_state()
    monitors = load_idle_monitors(state)
    snapshot = find_recent_snapshot(args.session_id, days=args.days)
    monitor = monitors.get(args.session_id, {})
    monitor.update(
        {
            "session_id": args.session_id,
            "label": args.label or monitor.get("label") or (snapshot or {}).get("user_message") or args.session_id[:8],
            "idle_seconds": int(args.idle_seconds),
            "notify_on_complete": bool(args.notify_on_complete),
            "created_at": monitor.get("created_at") or iso_now(),
        }
    )
    if snapshot:
        monitor["path"] = snapshot.get("path", "")
        monitor["last_seen_path"] = snapshot.get("path", "")
    monitors[args.session_id] = monitor
    notifications = evaluate_idle_monitors(state, {args.session_id: snapshot} if snapshot else {})
    save_state(state)
    print(
        json.dumps(
            {
                "ok": True,
                "monitor": monitors[args.session_id],
                "notifications": notifications,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_monitor_list(_args: argparse.Namespace) -> int:
    state = load_state()
    monitors = list(load_idle_monitors(state).values())
    print(json.dumps({"count": len(monitors), "monitors": monitors}, ensure_ascii=False, indent=2))
    return 0


def cmd_monitor_remove(args: argparse.Namespace) -> int:
    state = load_state()
    monitors = load_idle_monitors(state)
    removed = monitors.pop(args.session_id, None) is not None
    save_state(state)
    print(json.dumps({"removed": removed, "session_id": args.session_id}, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Watch Codex local sessions and sync them into the Vault")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan = subparsers.add_parser("scan-once")
    scan.add_argument("--days", type=int, default=14)
    scan.add_argument("--limit", type=int, default=200)
    scan.set_defaults(func=cmd_scan_once)

    daemon = subparsers.add_parser("daemon")
    daemon.add_argument("--days", type=int, default=14)
    daemon.add_argument("--limit", type=int, default=200)
    daemon.add_argument("--poll-interval", type=int, default=20)
    daemon.add_argument("--verbose", action="store_true")
    daemon.set_defaults(func=cmd_daemon)

    install = subparsers.add_parser("install-launchagent")
    install.add_argument("--poll-interval", type=int, default=20)
    install.set_defaults(func=cmd_install_launchagent)

    uninstall = subparsers.add_parser("uninstall-launchagent")
    uninstall.set_defaults(func=cmd_uninstall_launchagent)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    monitor_add = subparsers.add_parser("monitor-add")
    monitor_add.add_argument("--session-id", required=True)
    monitor_add.add_argument("--label", default="")
    monitor_add.add_argument("--idle-seconds", type=int, default=DEFAULT_IDLE_SECONDS)
    monitor_add.add_argument("--days", type=int, default=14)
    monitor_add.add_argument("--no-notify-on-complete", action="store_false", dest="notify_on_complete")
    monitor_add.set_defaults(func=cmd_monitor_add, notify_on_complete=True)

    monitor_list = subparsers.add_parser("monitor-list")
    monitor_list.set_defaults(func=cmd_monitor_list)

    monitor_remove = subparsers.add_parser("monitor-remove")
    monitor_remove.add_argument("--session-id", required=True)
    monitor_remove.set_defaults(func=cmd_monitor_remove)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

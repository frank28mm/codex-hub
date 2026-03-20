#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    from ops import codex_memory, codex_models, feishu_agent, material_router, project_pause, runtime_ingestion, runtime_state
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import codex_models  # type: ignore
    import feishu_agent  # type: ignore
    import material_router  # type: ignore
    import project_pause  # type: ignore
    import runtime_ingestion  # type: ignore
    import runtime_state  # type: ignore


def workspace_root() -> Path:
    return codex_memory.WORKSPACE_ROOT


def _canonical_workspace_root() -> Path:
    current = workspace_root()
    parent = current.parent
    if parent.name == "workspace-hub-worktrees":
        sibling = parent.parent / "workspace-hub"
        if sibling.exists():
            return sibling
    return current


def _feishu_local_extension_roots() -> list[Path]:
    codex_home = Path.home() / ".codex"
    roots = [
        codex_home,
        codex_home / "skills",
        codex_home / "agents",
    ]
    for root in roots:
        root.mkdir(parents=True, exist_ok=True)
    return roots


def _feishu_writable_roots(*, include_local_extensions: bool = False) -> list[Path]:
    canonical_root = _canonical_workspace_root()
    worktrees_root = canonical_root.parent / "workspace-hub-worktrees"
    roots = [
        codex_memory.VAULT_ROOT,
        canonical_root,
        canonical_root / "projects",
        workspace_root(),
        workspace_root() / "projects",
        worktrees_root / "core-v1-0-3-to-v1-0-5",
        worktrees_root / "feishu-bridge",
        worktrees_root / "electron-console",
    ]
    if include_local_extensions:
        roots.extend(_feishu_local_extension_roots())
    unique: list[Path] = []
    seen: set[str] = set()
    for path in roots:
        if not path.exists():
            continue
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def _print(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _response(broker_action: str, *, ok: bool, **payload: Any) -> dict[str, Any]:
    return {"ok": ok, "broker_action": broker_action, **payload}


def _pause_block_response(action: str, *, project_name: str) -> dict[str, Any] | None:
    canonical_project = codex_memory.canonical_project_name(str(project_name or "").strip())
    if not canonical_project:
        return None
    pause_payload = project_pause.active_pause(project_name=canonical_project, scope="broker_execution")
    if not pause_payload.get("active"):
        return None
    return _response(
        action,
        ok=False,
        action=action,
        result_status="suppressed",
        reason="project_paused",
        error_type="project_paused",
        project_name=canonical_project,
        pause=pause_payload,
    )


def _run(command: list[str], *, cwd: Path | None = None) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=str(cwd or workspace_root()),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _command_result(action: str, payload: dict[str, Any]) -> dict[str, Any]:
    ok = payload.get("returncode", 1) == 0
    launch_context = _extract_prefixed_json(payload.get("stderr", ""), "WORKSPACE_HUB_LAUNCH_CONTEXT=")
    finalize_payload = _extract_prefixed_json(payload.get("stderr", ""), "WORKSPACE_HUB_FINALIZE_LAUNCH=")
    return _response(
        action,
        ok=ok,
        action=action,
        result_status="success" if ok else "error",
        launch_context=launch_context,
        finalize_launch=finalize_payload,
        **payload,
    )


def _extract_prefixed_json(text: str, prefix: str) -> dict[str, Any] | None:
    for line in str(text or "").splitlines():
        if not line.startswith(prefix):
            continue
        raw = line[len(prefix) :].strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def _record_feishu_operation_event(
    *,
    domain: str,
    action: str,
    payload: dict[str, Any],
    status: str,
    summary: str,
) -> dict[str, Any]:
    event = {
        "ts": codex_memory.iso_now(),
        "type": "feishu_operation",
        "domain": domain,
        "action": action,
        "status": status,
        "summary": summary,
        "source": str(payload.get("source", "")),
        "project_name": str(payload.get("project_name", "")),
        "chat_ref": str(payload.get("chat_ref", "")),
        "thread_name": str(payload.get("thread_name", "")),
    }
    codex_memory.append_ndjson(codex_memory.EVENTS_NDJSON, event)
    return event


def _summarize_feishu_result(domain: str, action: str, result: dict[str, Any]) -> str:
    inner = result.get("result", {}) if isinstance(result.get("result"), dict) else {}
    if domain == "msg" and action == "send":
        return f"sent message {inner.get('message_id', '')}".strip()
    if domain == "doc" and action == "create":
        return f"created doc {inner.get('document_id', '')}".strip()
    if domain == "table" and action == "add":
        return f"added record {inner.get('record_id', '')}".strip()
    if domain == "table" and action == "create-app":
        return f"created bitable app {inner.get('app_token', '')}".strip()
    if domain == "table" and action == "create":
        return f"created table {inner.get('table_id', '')}".strip()
    if domain == "table" and action == "create-field":
        return f"created field {inner.get('field_id', '')}".strip()
    if domain == "cal" and action == "add":
        event = inner.get("event", {}) if isinstance(inner.get("event"), dict) else {}
        return f"created calendar event {event.get('event_id', '')}".strip()
    if domain == "task" and action == "add":
        return f"created task {inner.get('task_id', '')}".strip()
    if domain == "meeting" and action == "create":
        return f"created meeting {inner.get('meeting_id', '')}".strip()
    return f"{domain} {action}"


def _codex_cli_path() -> str:
    resolved = shutil.which("codex")
    if resolved:
        return resolved
    fallback = Path.home() / ".npm-global" / "bin" / "codex"
    if fallback.exists():
        return str(fallback)
    return "codex"


def _node_cli_path() -> str:
    resolved = shutil.which("node")
    if resolved:
        return resolved
    candidates = [
        Path("/opt/homebrew/bin/node"),
        Path("/usr/local/bin/node"),
        Path.home() / ".nvm" / "current" / "bin" / "node",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return "node"


def _codex_command_prefix() -> list[str]:
    codex_path = _codex_cli_path()
    codex_target = Path(codex_path)
    if codex_target.exists():
        try:
            with codex_target.open("r", encoding="utf-8", errors="ignore") as handle:
                first_line = handle.readline().strip()
        except OSError:
            first_line = ""
        node_path = _node_cli_path()
        node_target = Path(node_path)
        if first_line.startswith("#!/usr/bin/env node") and node_target.exists():
            return [str(node_target), codex_path]
    return [codex_path]


def _codex_exec_command(
    *,
    prompt: str,
    session_id: str = "",
    execution_profile: str = "",
    model: str = "",
    reasoning_effort: str = "",
    source: str = "",
) -> list[str]:
    resolved = codex_models.resolve_runtime_settings(
        model,
        reasoning_effort,
        execution_profile=execution_profile,
        source=source,
    )
    selected_model = str(resolved.get("model", "")).strip()
    selected_reasoning = str(resolved.get("reasoning_effort", "")).strip()
    command = [*_codex_command_prefix(), "exec", "-C", str(workspace_root())]
    if selected_model:
        command.extend(["--model", selected_model])
    if selected_reasoning:
        command.extend(["-c", f'model_reasoning_effort="{selected_reasoning}"'])
    if execution_profile in {"feishu", "feishu-object-op", "feishu-local-extend"}:
        command.extend(
            [
                "--sandbox",
                "workspace-write",
                "-c",
                'approval_policy="never"',
                "-c",
                "sandbox_workspace_write.network_access=true",
            ]
        )
        for root in _feishu_writable_roots(
            include_local_extensions=execution_profile == "feishu-local-extend"
        ):
            command.extend(["--add-dir", str(root)])
    if execution_profile == "feishu-approved":
        command.extend(
            [
                "--sandbox",
                "danger-full-access",
                "-c",
                'approval_policy="never"',
            ]
        )
    if execution_profile == "electron-full-access":
        command.extend(
            [
                "--sandbox",
                "danger-full-access",
                "-c",
                'approval_policy="never"',
            ]
        )
    if session_id:
        command.extend(["resume", session_id])
        if prompt:
            command.append(prompt)
        return command
    command.append(prompt)
    return command


def _should_use_start_codex(execution_profile: str = "") -> bool:
    return execution_profile in {
        "feishu",
        "feishu-object-op",
        "feishu-local-extend",
        "feishu-approved",
        "electron",
        "electron-full-access",
    }


def _start_codex_path() -> str:
    return str(workspace_root() / "ops" / "start-codex")


def _start_codex_command(
    *,
    prompt: str,
    project_name: str = "",
    session_id: str = "",
    no_auto_resume: bool = False,
    execution_profile: str = "",
    model: str = "",
    reasoning_effort: str = "",
    source: str = "",
    chat_ref: str = "",
    thread_name: str = "",
    thread_label: str = "",
    source_message_id: str = "",
) -> list[str]:
    command = [_start_codex_path()]
    if execution_profile:
        command.extend(["--execution-profile", execution_profile])
    if model:
        command.extend(["--model", model])
    if reasoning_effort:
        command.extend(["--reasoning-effort", reasoning_effort])
    if project_name:
        command.extend(["--project", project_name])
    if session_id:
        command.extend(["--resume-session-id", session_id])
    if no_auto_resume:
        command.append("--no-auto-resume")
    forward_values = {
        "source": source,
        "chat_ref": chat_ref,
        "thread_name": thread_name,
        "thread_label": thread_label,
        "source_message_id": source_message_id,
    }
    for key, option in runtime_ingestion.start_codex_forward_options():
        value = str(forward_values.get(key, "") or "").strip()
        if value:
            command.extend([option, value])
    if prompt:
        command.extend(["--prompt", prompt])
    return command


def _project_snapshot(project_name: str = "") -> dict[str, Any]:
    facts, errors = codex_memory.project_board_facts(codex_memory.load_registry())
    selected = []
    for item in facts:
        if project_name and item["project_name"] != project_name:
            continue
        selected.append(
            {
                "project_name": item["project_name"],
                "status": item["status"],
                "priority": item["priority"],
                "updated_at": item["updated_at"],
                "next_action": item["next_action"],
                "board_path": item["board_path"],
            }
        )
    return {"projects": selected, "errors": errors}


def _health_snapshot() -> dict[str, Any]:
    payload = _run(["python3", str(workspace_root() / "ops" / "workspace_hub_health_check.py"), "status"])
    if payload["returncode"] != 0:
        return payload
    try:
        return json.loads(payload["stdout"])
    except json.JSONDecodeError:
        return payload


def _review_snapshot(project_name: str = "") -> list[dict[str, Any]]:
    try:
        from ops import review_plane
    except ImportError:  # pragma: no cover
        import review_plane  # type: ignore

    review_plane.rebuild_review_inbox(sync_runtime=True)
    return review_plane.review_items(project_name=project_name)


def _coordination_snapshot(project_name: str = "") -> list[dict[str, Any]]:
    try:
        from ops import coordination_plane
    except ImportError:  # pragma: no cover
        import coordination_plane  # type: ignore

    coordination_plane.rebuild_coordination_projection(sync_runtime=True)
    return coordination_plane.coordination_items(project_name=project_name)


def _bridge_settings_summary(bridge: str = "feishu") -> dict[str, Any]:
    payload = runtime_state.fetch_bridge_settings(bridge)
    settings = payload.get("settings", {})
    allowed_users = settings.get("allowed_users", [])
    return {
        "domain": settings.get("domain", ""),
        "group_policy": settings.get("group_policy", ""),
        "require_mention": bool(settings.get("require_mention", False)),
        "allowed_user_count": len(allowed_users) if isinstance(allowed_users, list) else 0,
        "has_app_credentials": bool(settings.get("app_id") and settings.get("app_secret")),
        "configured_keys": sorted(
            key for key, value in settings.items() if key not in {"app_secret"} and value not in ("", None, [], {})
        ),
    }


def _registered_project_names() -> set[str]:
    return {
        codex_memory.canonical_project_name(str(item.get("project_name", "")).strip())
        for item in codex_memory.load_registry()
        if str(item.get("project_name", "")).strip()
    }


def _resolve_explicit_topic_name(project_name: str, topic_name: str) -> tuple[str, list[str]]:
    target = str(topic_name or "").strip().lower()
    if not target:
        return "", []
    matches: list[str] = []
    available_topics: list[str] = []
    stem_prefix = f"{project_name}-"
    for path in codex_memory.topic_board_paths(project_name):
        topic_board = codex_memory.load_topic_board(path)
        frontmatter = topic_board["frontmatter"]
        canonical_topic_name = str(frontmatter.get("topic_name", "")).strip()
        topic_key = str(frontmatter.get("topic_key", "")).strip()
        if canonical_topic_name:
            available_topics.append(canonical_topic_name)
        stem = path.stem
        trimmed_stem = stem[:-4] if stem.endswith("-跟进板") else stem
        short_stem = trimmed_stem[len(stem_prefix) :] if trimmed_stem.startswith(stem_prefix) else trimmed_stem
        candidates = {
            canonical_topic_name.lower(),
            topic_key.lower(),
            stem.lower(),
            trimmed_stem.lower(),
            short_stem.lower(),
        }
        if target in {item for item in candidates if item}:
            matches.append(canonical_topic_name or topic_key)
    unique_matches = sorted({item for item in matches if item})
    if len(unique_matches) == 1:
        return unique_matches[0], sorted(set(available_topics))
    return "", sorted(set(available_topics))


def _user_profile_snapshot() -> dict[str, Any]:
    profile = codex_memory.load_user_profile()
    return {
        "preferred_name": profile.get("preferred_name", ""),
        "feishu_open_id": profile.get("feishu_open_id", ""),
        "alternate_names": profile.get("alternate_names", []),
        "relationship": profile.get("relationship", "workspace owner"),
        "updated_at": profile.get("updated_at", ""),
        "path": profile.get("path", ""),
        "note": profile.get("body", ""),
    }


def _codex_models_snapshot() -> dict[str, Any]:
    return codex_models.summarize_settings()


def _bridge_conversation_summary(bridge: str = "feishu") -> dict[str, int]:
    rows = runtime_state.fetch_bridge_conversations(bridge=bridge, limit=100)
    return {
        "thread_count": len(rows),
        "bound_thread_count": sum(1 for item in rows if item.get("project_name")),
        "running_thread_count": sum(1 for item in rows if item.get("execution_state") == "running"),
        "attention_thread_count": sum(1 for item in rows if item.get("needs_attention")),
    }


def _bridge_status_snapshot(bridge: str = "feishu") -> dict[str, Any]:
    connection = runtime_state.fetch_bridge_connection(bridge)
    metadata = connection.get("metadata", {}) or {}
    heartbeat_at = str(metadata.get("heartbeat_at") or "").strip()
    last_event_at = str(connection.get("last_event_at", "")).strip()
    stale_after_seconds = int(metadata.get("stale_after_seconds") or 90)
    event_idle_after_seconds = int(metadata.get("event_idle_after_seconds") or 0)
    stale = False
    event_stalled = False
    if connection.get("status") == "connected":
        reference_text = heartbeat_at or connection.get("updated_at", "")
        try:
            reference = dt.datetime.fromisoformat(reference_text.replace("Z", "+00:00"))
            reference_utc = reference.astimezone(dt.timezone.utc) if reference.tzinfo else reference.replace(tzinfo=dt.timezone.utc)
            stale = (dt.datetime.now(dt.timezone.utc) - reference_utc).total_seconds() > stale_after_seconds
        except ValueError:
            stale = False
        if event_idle_after_seconds > 0:
            event_reference_text = last_event_at or connection.get("updated_at", "")
            try:
                event_reference = dt.datetime.fromisoformat(event_reference_text.replace("Z", "+00:00"))
                event_reference_utc = (
                    event_reference.astimezone(dt.timezone.utc)
                    if event_reference.tzinfo
                    else event_reference.replace(tzinfo=dt.timezone.utc)
                )
                event_stalled = (
                    dt.datetime.now(dt.timezone.utc) - event_reference_utc
                ).total_seconds() > event_idle_after_seconds
            except ValueError:
                event_stalled = False
        stale = stale or event_stalled
    effective_status = "stale" if stale and connection.get("status") == "connected" else connection.get("status", "disconnected")
    return {
        "bridge": bridge,
        "connection_status": effective_status,
        "host_mode": connection.get("host_mode", ""),
        "transport": connection.get("transport", ""),
        "last_error": connection.get("last_error", ""),
        "last_event_at": connection.get("last_event_at", ""),
        "updated_at": connection.get("updated_at", ""),
        "heartbeat_at": heartbeat_at,
        "stale": stale,
        "event_stalled": event_stalled,
        "stale_after_seconds": stale_after_seconds,
        "event_idle_after_seconds": event_idle_after_seconds,
        "metadata": metadata,
        "settings_summary": _bridge_settings_summary(bridge),
    }


def cmd_bridge_conversations(args: argparse.Namespace) -> int:
    rows = runtime_state.fetch_bridge_conversations(bridge=args.bridge, limit=args.limit)
    protocol = runtime_state.bridge_retrieval_protocol(bridge=args.bridge, limit=args.limit)
    return _print(_response("bridge_conversations", ok=True, bridge=args.bridge, rows=rows, retrieval_protocol=protocol))


def cmd_bridge_messages(args: argparse.Namespace) -> int:
    rows = runtime_state.fetch_bridge_messages(bridge=args.bridge, chat_ref=args.chat_ref, limit=args.limit)
    normalized = []
    for item in rows:
        payload = item.get("payload") or {}
        normalized.append(
            {
                "bridge": item.get("bridge", ""),
                "direction": item.get("direction", ""),
                "message_id": item.get("message_id", ""),
                "chat_ref": item.get("chat_ref", ""),
                "project_name": item.get("project_name", ""),
                "session_id": item.get("session_id", ""),
                "status": item.get("status", ""),
                "created_at": item.get("created_at", ""),
                "updated_at": item.get("updated_at", ""),
                "text": str(payload.get("text") or "").strip(),
                "chat_type": str(payload.get("chat_type") or payload.get("reply_target_type") or "").strip(),
                "sender_ref": str(payload.get("open_id") or payload.get("user_id") or payload.get("sender_ref") or "").strip(),
                "phase": str(payload.get("phase") or "").strip(),
                "source_message_id": str(payload.get("source_message_id") or "").strip(),
            }
        )
    protocol = runtime_state.bridge_retrieval_protocol(bridge=args.bridge, chat_ref=args.chat_ref, limit=args.limit)
    return _print(
        _response(
            "bridge_messages",
            ok=True,
            bridge=args.bridge,
            chat_ref=args.chat_ref,
            rows=normalized,
            retrieval_protocol=protocol,
        )
    )


def cmd_bridge_message_detail(args: argparse.Namespace) -> int:
    detail = runtime_state.fetch_bridge_message_detail(
        bridge=args.bridge,
        message_id=args.message_id,
        direction=args.direction,
    )
    return _print(
        _response(
            "bridge_message_detail",
            ok=bool(detail.get("message_id")),
            bridge=args.bridge,
            direction=args.direction,
            message_id=args.message_id,
            detail=detail,
        )
    )


def cmd_user_profile(args: argparse.Namespace) -> int:
    if args.profile_json:
        try:
            payload = json.loads(args.profile_json)
        except json.JSONDecodeError as exc:
            return _print(_response("user_profile", ok=False, error=f"invalid profile json: {exc}"))
        preferred_name = str(payload.get("preferred_name", "")).strip()
        aliases = payload.get("alternate_names", [])
        if not isinstance(aliases, list):
            aliases = []
        profile = codex_memory.save_user_profile(
            preferred_name=preferred_name,
            alternate_names=[str(item).strip() for item in aliases if str(item).strip()],
            feishu_open_id=str(payload.get("feishu_open_id", "")).strip(),
            relationship=str(payload.get("relationship", "workspace owner")).strip() or "workspace owner",
            note=str(payload.get("note", "")).strip(),
        )
        return _print(_response("user_profile", ok=True, updated=True, profile=profile))
    return _print(_response("user_profile", ok=True, updated=False, profile=_user_profile_snapshot()))


def cmd_bridge_chat_binding(args: argparse.Namespace) -> int:
    bridge = args.bridge
    chat_ref = str(args.chat_ref or "").strip()
    if not chat_ref:
        return _print(_response("bridge_chat_binding", ok=False, bridge=bridge, error="chat_ref is required"))
    if args.binding_json:
        try:
            payload = json.loads(args.binding_json)
        except json.JSONDecodeError as exc:
            return _print(_response("bridge_chat_binding", ok=False, bridge=bridge, chat_ref=chat_ref, error=f"invalid binding json: {exc}"))
        project_name = codex_memory.canonical_project_name(str(payload.get("project_name", "")).strip())
        topic_name = str(payload.get("topic_name", "")).strip()
        if project_name and project_name not in _registered_project_names():
            return _print(
                _response(
                    "bridge_chat_binding",
                    ok=False,
                    bridge=bridge,
                    chat_ref=chat_ref,
                    error=f"unknown project_name `{project_name}`",
                )
            )
        if topic_name and not project_name:
            return _print(
                _response(
                    "bridge_chat_binding",
                    ok=False,
                    bridge=bridge,
                    chat_ref=chat_ref,
                    error="topic_name requires project_name",
                )
            )
        binding_scope = "topic" if topic_name else str(payload.get("binding_scope", "project")).strip() or "project"
        if binding_scope == "topic" and not topic_name:
            return _print(
                _response(
                    "bridge_chat_binding",
                    ok=False,
                    bridge=bridge,
                    chat_ref=chat_ref,
                    error="topic binding requires topic_name",
                )
            )
        if topic_name:
            canonical_topic_name, available_topics = _resolve_explicit_topic_name(project_name, topic_name)
            if not canonical_topic_name:
                return _print(
                    _response(
                        "bridge_chat_binding",
                        ok=False,
                        bridge=bridge,
                        chat_ref=chat_ref,
                        error=f"unknown topic_name `{topic_name}` for project `{project_name}`",
                        available_topics=available_topics,
                    )
                )
            topic_name = canonical_topic_name
            binding_scope = "topic"
        binding = runtime_state.upsert_bridge_chat_binding(
            bridge=bridge,
            chat_ref=chat_ref,
            binding_scope=binding_scope,
            project_name=project_name,
            topic_name=topic_name,
            session_id=str(payload.get("session_id", "")).strip(),
            metadata=payload.get("metadata") or {},
        )
        return _print(_response("bridge_chat_binding", ok=True, updated=True, binding=binding))
    binding = runtime_state.fetch_bridge_chat_binding(bridge=bridge, chat_ref=chat_ref)
    return _print(_response("bridge_chat_binding", ok=True, updated=False, binding=binding))


def cmd_bridge_bindings(args: argparse.Namespace) -> int:
    rows = runtime_state.fetch_bridge_chat_bindings(bridge=args.bridge, limit=args.limit)
    return _print(_response("bridge_bindings", ok=True, bridge=args.bridge, rows=rows))


def cmd_approval_token(args: argparse.Namespace) -> int:
    token = str(args.token or "").strip()
    if not token:
        return _print(_response("approval_token", ok=False, error="token is required"))
    if args.token_json:
        try:
            payload = json.loads(args.token_json)
        except json.JSONDecodeError as exc:
            return _print(_response("approval_token", ok=False, token=token, error=f"invalid token json: {exc}"))
        scope = str(payload.get("scope", "")).strip()
        status = str(payload.get("status", "")).strip()
        if not scope:
            return _print(_response("approval_token", ok=False, token=token, error="scope is required"))
        if not status:
            return _print(_response("approval_token", ok=False, token=token, error="status is required"))
        item = runtime_state.upsert_approval_token(
            token=token,
            scope=scope,
            status=status,
            project_name=str(payload.get("project_name", "")).strip(),
            session_id=str(payload.get("session_id", "")).strip(),
            expires_at=str(payload.get("expires_at", "")).strip(),
            metadata=payload.get("metadata") or {},
        )
        return _print(_response("approval_token", ok=True, updated=True, token=token, item=item))
    item = runtime_state.fetch_approval_token(token)
    return _print(_response("approval_token", ok=True, updated=False, token=token, item=item))


def cmd_approval_tokens(args: argparse.Namespace) -> int:
    rows = runtime_state.fetch_approval_tokens(status=args.status, scope=args.scope, limit=args.limit)
    return _print(
        _response(
            "approval_tokens",
            ok=True,
            status_filter=args.status,
            scope_filter=args.scope,
            rows=rows,
        )
    )


def cmd_init_db(_args: argparse.Namespace) -> int:
    payload = runtime_state.init_db()
    return _print(_response("init_db", ok=bool(payload.get("ok", True)), **payload))


def cmd_status(_args: argparse.Namespace) -> int:
    payload = _response(
        "status",
        ok=True,
        broker="local-broker",
        workspace_root=str(workspace_root()),
        runtime=runtime_state.fetch_runtime_summary(),
        capabilities={
            "codex_exec": True,
            "codex_resume": True,
            "codex_app": True,
            "project_snapshot": True,
            "review_snapshot": True,
            "coordination_snapshot": True,
            "health_snapshot": True,
            "bridge_status": True,
            "bridge_settings": True,
            "bridge_connection": True,
            "bridge_conversations": True,
            "bridge_messages": True,
            "bridge_message_detail": True,
            "bridge_chat_binding": True,
            "bridge_bindings": True,
            "approval_token": True,
            "approval_tokens": True,
            "user_profile": True,
            "material_inspect": True,
            "material_suggest": True,
            "codex_models": True,
            "feishu_op": True,
        },
        commands=[
            "init-db",
            "status",
            "codex-exec",
            "codex-resume",
            "codex-app",
            "projects",
            "review-inbox",
            "coordination-inbox",
            "health",
            "bridge-status",
            "bridge-settings",
            "bridge-connection",
            "bridge-conversations",
            "bridge-messages",
            "bridge-message-detail",
            "bridge-chat-binding",
            "bridge-bindings",
            "approval-token",
            "approval-tokens",
            "user-profile",
            "material-inspect",
            "material-suggest",
            "codex-models",
            "feishu-op",
            "panel",
            "command-center",
            "record-bridge-message",
        ],
    )
    return _print(payload)


def cmd_codex_exec(args: argparse.Namespace) -> int:
    blocked = _pause_block_response("codex_exec", project_name=getattr(args, "project_name", ""))
    if blocked:
        return _print(blocked)
    execution_profile = getattr(args, "execution_profile", "")
    command = (
        _start_codex_command(
            prompt=args.prompt,
            project_name=getattr(args, "project_name", ""),
            no_auto_resume=bool(getattr(args, "no_auto_resume", False)),
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
            chat_ref=getattr(args, "chat_ref", ""),
            thread_name=getattr(args, "thread_name", ""),
            thread_label=getattr(args, "thread_label", ""),
            source_message_id=getattr(args, "source_message_id", ""),
        )
        if _should_use_start_codex(execution_profile)
        else _codex_exec_command(
            prompt=args.prompt,
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
        )
    )
    payload = _command_result("codex_exec", _run(command))
    return _print(payload)


def cmd_codex_resume(args: argparse.Namespace) -> int:
    blocked = _pause_block_response("codex_resume", project_name=getattr(args, "project_name", ""))
    if blocked:
        return _print(blocked)
    execution_profile = getattr(args, "execution_profile", "")
    command = (
        _start_codex_command(
            prompt=args.prompt,
            project_name=getattr(args, "project_name", ""),
            session_id=args.session_id,
            no_auto_resume=bool(getattr(args, "no_auto_resume", False)),
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
            chat_ref=getattr(args, "chat_ref", ""),
            thread_name=getattr(args, "thread_name", ""),
            thread_label=getattr(args, "thread_label", ""),
            source_message_id=getattr(args, "source_message_id", ""),
        )
        if _should_use_start_codex(execution_profile)
        else _codex_exec_command(
            prompt=args.prompt,
            session_id=args.session_id,
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
        )
    )
    payload = _command_result("codex_resume", _run(command, cwd=workspace_root()))
    return _print(payload)


def cmd_codex_open_app(_args: argparse.Namespace) -> int:
    payload = _command_result("codex_app", _run([_codex_cli_path(), "app", str(workspace_root())]))
    return _print(payload)


def cmd_projects(args: argparse.Namespace) -> int:
    payload = _project_snapshot(project_name=args.project_name)
    return _print(_response("projects", ok=True, **payload))


def cmd_material_inspect(args: argparse.Namespace) -> int:
    payload = material_router.inspect_material_route(args.project_name)
    return _print(_response("material_inspect", ok=True, **payload))


def cmd_material_suggest(args: argparse.Namespace) -> int:
    payload = material_router.suggest_material_route(args.project_name, args.prompt or "")
    return _print(_response("material_suggest", ok=True, **payload))


def cmd_codex_models(args: argparse.Namespace) -> int:
    if args.settings_json:
        try:
            payload = json.loads(args.settings_json)
        except json.JSONDecodeError as exc:
            return _print(_response("codex_models", ok=False, error=f"invalid settings json: {exc}"))
        try:
            summary = codex_models.save_defaults(
                workspace=str(payload.get("workspace", "")).strip(),
                feishu=str(payload.get("feishu", "")).strip(),
                electron=str(payload.get("electron", "")).strip(),
                workspace_reasoning=str(payload.get("workspace_reasoning", "")).strip(),
                feishu_reasoning=str(payload.get("feishu_reasoning", "")).strip(),
                electron_reasoning=str(payload.get("electron_reasoning", "")).strip(),
            )
        except ValueError as exc:
            return _print(_response("codex_models", ok=False, error=str(exc)))
        return _print(_response("codex_models", ok=True, updated=True, **summary))
    return _print(_response("codex_models", ok=True, updated=False, **_codex_models_snapshot()))


def cmd_feishu_op(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json or "{}")
    except json.JSONDecodeError as exc:
        return _print(_response("feishu_op", ok=False, error=f"invalid payload json: {exc}"))
    if not isinstance(payload, dict):
        return _print(_response("feishu_op", ok=False, error="payload_json must decode to an object"))
    try:
        result = feishu_agent.perform_operation(args.domain, args.action, payload)
    except feishu_agent.FeishuAgentError as exc:
        event = _record_feishu_operation_event(
            domain=args.domain,
            action=args.action,
            payload=payload,
            status="error",
            summary=str(exc),
        )
        return _print(
            _response(
                "feishu_op",
                ok=False,
                domain=args.domain,
                action=args.action,
                result_status="error",
                error=str(exc),
                error_code=exc.code,
                details=exc.details,
                operation_event=event,
            )
        )
    summary = _summarize_feishu_result(args.domain, args.action, result)
    event = _record_feishu_operation_event(
        domain=args.domain,
        action=args.action,
        payload=payload,
        status="success",
        summary=summary,
    )
    return _print(
        _response(
            "feishu_op",
            ok=True,
            domain=args.domain,
            action=args.action,
            result_status="success",
            result=result,
            summary=summary,
            operation_event=event,
        )
    )


def cmd_review_inbox(args: argparse.Namespace) -> int:
    return _print(_response("review_inbox", ok=True, items=_review_snapshot(project_name=args.project_name)))


def cmd_coordination_inbox(args: argparse.Namespace) -> int:
    return _print(
        _response("coordination_inbox", ok=True, items=_coordination_snapshot(project_name=args.project_name))
    )


def cmd_health(_args: argparse.Namespace) -> int:
    payload = _health_snapshot()
    return _print(_response("health", ok=payload.get("returncode", 0) == 0, payload=payload))


def cmd_bridge_status(args: argparse.Namespace) -> int:
    bridge = args.bridge
    payload = _bridge_status_snapshot(bridge)
    return _print(_response("bridge_status", ok=True, **payload))


def cmd_bridge_settings(args: argparse.Namespace) -> int:
    bridge = args.bridge
    if args.settings_json:
        try:
            settings = json.loads(args.settings_json)
        except json.JSONDecodeError as exc:
            return _print(_response("bridge_settings", ok=False, bridge=bridge, error=f"invalid settings json: {exc}"))
        payload = runtime_state.upsert_bridge_settings(bridge, settings)
        return _print(_response("bridge_settings", ok=True, updated=True, **payload))
    payload = runtime_state.fetch_bridge_settings(bridge)
    return _print(_response("bridge_settings", ok=True, updated=False, **payload))


def cmd_bridge_connection(args: argparse.Namespace) -> int:
    bridge = args.bridge
    if args.connection_json:
        try:
            connection = json.loads(args.connection_json)
        except json.JSONDecodeError as exc:
            return _print(
                _response("bridge_connection", ok=False, bridge=bridge, error=f"invalid connection json: {exc}")
            )
        payload = runtime_state.upsert_bridge_connection(
            bridge,
            status=str(connection.get("status", "disconnected")),
            host_mode=str(connection.get("host_mode", "")),
            transport=str(connection.get("transport", "")),
            last_error=str(connection.get("last_error", "")),
            last_event_at=str(connection.get("last_event_at", "")),
            metadata=connection.get("metadata") or {},
        )
        return _print(_response("bridge_connection", ok=True, updated=True, **payload))
    payload = runtime_state.fetch_bridge_connection(bridge)
    return _print(_response("bridge_connection", ok=True, updated=False, **payload))


def cmd_panel(args: argparse.Namespace) -> int:
    panel = args.name
    if panel == "overview":
        project_snapshot = _project_snapshot()
        review_items = _review_snapshot()
        coordination_items = _coordination_snapshot()
        health_payload = _health_snapshot()
        bridge_summary = _bridge_conversation_summary("feishu")
        payload = {
            "cards": [
                {"label": "Active Projects", "value": str(len(project_snapshot.get("projects", [])))},
                {"label": "Pending Reviews", "value": str(len(review_items))},
                {"label": "Open Coordination", "value": str(len(coordination_items))},
                {"label": "Health Alerts", "value": str(health_payload.get("open_alert_count", 0))},
                {"label": "CoCo Threads", "value": str(bridge_summary["thread_count"])},
                {"label": "Bound Threads", "value": str(bridge_summary["bound_thread_count"])},
                {"label": "Running Threads", "value": str(bridge_summary["running_thread_count"])},
                {"label": "Threads Needing Attention", "value": str(bridge_summary["attention_thread_count"])},
            ],
            "note": "Console-first overview served from local broker projections.",
        }
        return _print(_response("panel", ok=True, panel_name=panel, **payload))
    if panel == "projects":
        return _print(
            _response(
                "panel",
                ok=True,
                panel_name=panel,
                rows=_project_snapshot(project_name=args.project_name).get("projects", []),
            )
        )
    if panel == "review":
        return _print(_response("panel", ok=True, panel_name=panel, rows=_review_snapshot(project_name=args.project_name)))
    if panel == "coordination":
        return _print(
            _response("panel", ok=True, panel_name=panel, rows=_coordination_snapshot(project_name=args.project_name))
        )
    if panel == "health":
        health_payload = _health_snapshot()
        rows = [
            {
                "title": "workspace-health",
                "summary": f"issue_count={health_payload.get('last_entry', {}).get('issue_count', 0)} open_alert_count={health_payload.get('open_alert_count', 0)}",
                "severity": "warning" if health_payload.get("open_alert_count", 0) else "info",
                "report_path": health_payload.get("latest_report", ""),
            }
        ]
        return _print(_response("panel", ok=True, panel_name=panel, rows=rows, alerts=rows))
    if panel == "bridge-conversations":
        return _print(
            _response(
                "panel",
                ok=True,
                panel_name=panel,
                rows=runtime_state.fetch_bridge_conversations(bridge="feishu", limit=50),
                note="Feishu conversations mirrored from runtime bridge messages.",
            )
        )
    if panel == "user-profile":
        return _print(_response("panel", ok=True, panel_name=panel, profile=_user_profile_snapshot()))
    return _print(_response("panel", ok=False, panel_name=panel, rows=[], error=f"unknown panel `{panel}`"))


def cmd_command_center(args: argparse.Namespace) -> int:
    action = args.action
    if action in {"codex-exec", "codex-resume"}:
        blocked = _pause_block_response("command_center", project_name=getattr(args, "project_name", ""))
        if blocked:
            blocked["action"] = action
            return _print(blocked)
    execution_profile = getattr(args, "execution_profile", "")
    payload: dict[str, Any]
    if action == "open-codex-app":
        payload = _command_result("codex_app", _run([_codex_cli_path(), "app", str(workspace_root())]))
    elif action == "codex-exec":
        command = (
        _start_codex_command(
            prompt=args.prompt,
            project_name=getattr(args, "project_name", ""),
            no_auto_resume=bool(getattr(args, "no_auto_resume", False)),
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
            chat_ref=getattr(args, "chat_ref", ""),
            thread_name=getattr(args, "thread_name", ""),
            thread_label=getattr(args, "thread_label", ""),
            source_message_id=getattr(args, "source_message_id", ""),
        )
        if _should_use_start_codex(execution_profile)
        else _codex_exec_command(
                prompt=args.prompt,
                execution_profile=execution_profile,
                model=getattr(args, "model", ""),
                reasoning_effort=getattr(args, "reasoning_effort", ""),
                source=getattr(args, "source", ""),
            )
        )
        payload = _command_result(
            "codex_exec",
            _run(command),
        )
    elif action == "codex-resume":
        command = (
        _start_codex_command(
            prompt=args.prompt,
            project_name=getattr(args, "project_name", ""),
            session_id=args.session_id,
            no_auto_resume=bool(getattr(args, "no_auto_resume", False)),
            execution_profile=execution_profile,
            model=getattr(args, "model", ""),
            reasoning_effort=getattr(args, "reasoning_effort", ""),
            source=getattr(args, "source", ""),
            chat_ref=getattr(args, "chat_ref", ""),
            thread_name=getattr(args, "thread_name", ""),
            thread_label=getattr(args, "thread_label", ""),
            source_message_id=getattr(args, "source_message_id", ""),
        )
        if _should_use_start_codex(execution_profile)
        else _codex_exec_command(
                prompt=args.prompt,
                session_id=args.session_id,
                execution_profile=execution_profile,
                model=getattr(args, "model", ""),
                reasoning_effort=getattr(args, "reasoning_effort", ""),
                source=getattr(args, "source", ""),
            )
        )
        payload = _command_result(
            "codex_resume",
            _run(command, cwd=workspace_root()),
        )
    else:
        return _print(_response("command_center", ok=False, action=action, error=f"unknown action `{action}`"))
    return _print(
        _response(
            "command_center",
            ok=payload["ok"],
            action=action,
            delegated_broker_action=payload["broker_action"],
            result_status=payload.get("result_status", ""),
            command=payload.get("command", []),
            returncode=payload.get("returncode", 1),
            stdout=payload.get("stdout", ""),
            stderr=payload.get("stderr", ""),
            launch_context=payload.get("launch_context"),
            finalize_launch=payload.get("finalize_launch"),
        )
    )


def cmd_record_bridge(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload) if args.payload else {}
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid payload json: {exc}"}), file=sys.stderr)
        return 1
    record = runtime_state.upsert_bridge_message(
        bridge=args.bridge,
        direction=args.direction,
        message_id=args.message_id,
        status=args.status,
        payload=payload,
        project_name=args.project_name,
        session_id=args.session_id,
    )
    return _print(_response("record_bridge_message", ok=True, record=record))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local broker for Codex/bridge/console integration.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db")
    init_db.set_defaults(func=cmd_init_db)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    codex_exec = subparsers.add_parser("codex-exec")
    codex_exec.add_argument("--prompt", required=True)
    codex_exec.add_argument("--execution-profile", default="")
    codex_exec.add_argument("--model", default="")
    codex_exec.add_argument("--reasoning-effort", default="")
    codex_exec.add_argument("--project-name", default="")
    codex_exec.add_argument("--source", default="")
    codex_exec.add_argument("--chat-ref", default="")
    codex_exec.add_argument("--thread-name", default="")
    codex_exec.add_argument("--thread-label", default="")
    codex_exec.add_argument("--source-message-id", default="")
    codex_exec.add_argument("--no-auto-resume", action="store_true")
    codex_exec.set_defaults(func=cmd_codex_exec)

    codex_resume = subparsers.add_parser("codex-resume")
    codex_resume.add_argument("--session-id", required=True)
    codex_resume.add_argument("--prompt", default="")
    codex_resume.add_argument("--execution-profile", default="")
    codex_resume.add_argument("--model", default="")
    codex_resume.add_argument("--reasoning-effort", default="")
    codex_resume.add_argument("--project-name", default="")
    codex_resume.add_argument("--source", default="")
    codex_resume.add_argument("--chat-ref", default="")
    codex_resume.add_argument("--thread-name", default="")
    codex_resume.add_argument("--thread-label", default="")
    codex_resume.add_argument("--source-message-id", default="")
    codex_resume.add_argument("--no-auto-resume", action="store_true")
    codex_resume.set_defaults(func=cmd_codex_resume)

    codex_app = subparsers.add_parser("codex-app")
    codex_app.set_defaults(func=cmd_codex_open_app)

    projects = subparsers.add_parser("projects")
    projects.add_argument("--project-name", default="")
    projects.set_defaults(func=cmd_projects)

    material_inspect = subparsers.add_parser("material-inspect")
    material_inspect.add_argument("--project-name", required=True)
    material_inspect.set_defaults(func=cmd_material_inspect)

    material_suggest = subparsers.add_parser("material-suggest")
    material_suggest.add_argument("--project-name", required=True)
    material_suggest.add_argument("--prompt", default="")
    material_suggest.set_defaults(func=cmd_material_suggest)

    codex_models_parser = subparsers.add_parser("codex-models")
    codex_models_parser.add_argument("--settings-json", default="")
    codex_models_parser.set_defaults(func=cmd_codex_models)

    feishu_op = subparsers.add_parser("feishu-op")
    feishu_op.add_argument("--domain", required=True)
    feishu_op.add_argument("--action", required=True)
    feishu_op.add_argument("--payload-json", default="{}")
    feishu_op.set_defaults(func=cmd_feishu_op)

    review_inbox = subparsers.add_parser("review-inbox")
    review_inbox.add_argument("--project-name", default="")
    review_inbox.set_defaults(func=cmd_review_inbox)

    coordination_inbox = subparsers.add_parser("coordination-inbox")
    coordination_inbox.add_argument("--project-name", default="")
    coordination_inbox.set_defaults(func=cmd_coordination_inbox)

    health = subparsers.add_parser("health")
    health.set_defaults(func=cmd_health)

    bridge_status = subparsers.add_parser("bridge-status")
    bridge_status.add_argument("--bridge", default="feishu")
    bridge_status.set_defaults(func=cmd_bridge_status)

    bridge_settings = subparsers.add_parser("bridge-settings")
    bridge_settings.add_argument("--bridge", default="feishu")
    bridge_settings.add_argument("--settings-json", default="")
    bridge_settings.set_defaults(func=cmd_bridge_settings)

    bridge_connection = subparsers.add_parser("bridge-connection")
    bridge_connection.add_argument("--bridge", default="feishu")
    bridge_connection.add_argument("--connection-json", default="")
    bridge_connection.set_defaults(func=cmd_bridge_connection)

    bridge_conversations = subparsers.add_parser("bridge-conversations")
    bridge_conversations.add_argument("--bridge", default="feishu")
    bridge_conversations.add_argument("--limit", type=int, default=50)
    bridge_conversations.set_defaults(func=cmd_bridge_conversations)

    bridge_messages = subparsers.add_parser("bridge-messages")
    bridge_messages.add_argument("--bridge", default="feishu")
    bridge_messages.add_argument("--chat-ref", default="")
    bridge_messages.add_argument("--limit", type=int, default=100)
    bridge_messages.set_defaults(func=cmd_bridge_messages)

    bridge_message_detail = subparsers.add_parser("bridge-message-detail")
    bridge_message_detail.add_argument("--bridge", default="feishu")
    bridge_message_detail.add_argument("--message-id", required=True)
    bridge_message_detail.add_argument("--direction", default="")
    bridge_message_detail.set_defaults(func=cmd_bridge_message_detail)

    bridge_chat_binding = subparsers.add_parser("bridge-chat-binding")
    bridge_chat_binding.add_argument("--bridge", default="feishu")
    bridge_chat_binding.add_argument("--chat-ref", required=True)
    bridge_chat_binding.add_argument("--binding-json", default="")
    bridge_chat_binding.set_defaults(func=cmd_bridge_chat_binding)

    bridge_bindings = subparsers.add_parser("bridge-bindings")
    bridge_bindings.add_argument("--bridge", default="feishu")
    bridge_bindings.add_argument("--limit", type=int, default=100)
    bridge_bindings.set_defaults(func=cmd_bridge_bindings)

    approval_token = subparsers.add_parser("approval-token")
    approval_token.add_argument("--token", required=True)
    approval_token.add_argument("--token-json", default="")
    approval_token.set_defaults(func=cmd_approval_token)

    approval_tokens = subparsers.add_parser("approval-tokens")
    approval_tokens.add_argument("--status", default="")
    approval_tokens.add_argument("--scope", default="")
    approval_tokens.add_argument("--limit", type=int, default=100)
    approval_tokens.set_defaults(func=cmd_approval_tokens)

    user_profile = subparsers.add_parser("user-profile")
    user_profile.add_argument("--profile-json", default="")
    user_profile.set_defaults(func=cmd_user_profile)

    panel = subparsers.add_parser("panel")
    panel.add_argument("--name", required=True)
    panel.add_argument("--project-name", default="")
    panel.set_defaults(func=cmd_panel)

    command_center = subparsers.add_parser("command-center")
    command_center.add_argument("--action", required=True)
    command_center.add_argument("--project-name", default="")
    command_center.add_argument("--session-id", default="")
    command_center.add_argument("--prompt", default="")
    command_center.add_argument("--execution-profile", default="")
    command_center.add_argument("--model", default="")
    command_center.add_argument("--reasoning-effort", default="")
    command_center.add_argument("--source", default="")
    command_center.add_argument("--chat-ref", default="")
    command_center.add_argument("--thread-name", default="")
    command_center.add_argument("--thread-label", default="")
    command_center.add_argument("--source-message-id", default="")
    command_center.add_argument("--no-auto-resume", action="store_true")
    command_center.set_defaults(func=cmd_command_center)

    record_bridge = subparsers.add_parser("record-bridge-message")
    record_bridge.add_argument("--bridge", required=True)
    record_bridge.add_argument("--direction", choices=["inbound", "outbound"], required=True)
    record_bridge.add_argument("--message-id", required=True)
    record_bridge.add_argument("--status", required=True)
    record_bridge.add_argument("--project-name", default="")
    record_bridge.add_argument("--session-id", default="")
    record_bridge.add_argument("--payload", default="{}")
    record_bridge.set_defaults(func=cmd_record_bridge)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

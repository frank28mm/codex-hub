#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any


class LarkCliBackendError(RuntimeError):
    def __init__(self, message: str, *, code: str = "", details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _runtime_env(env: dict[str, str] | None = None) -> dict[str, str]:
    return dict(env or os.environ)


def _lark_cli_bin(env: dict[str, str] | None = None) -> str:
    runtime = _runtime_env(env)
    return str(runtime.get("WORKSPACE_HUB_LARK_CLI_BIN") or "").strip() or "lark-cli"


def _backend_mode(env: dict[str, str] | None = None) -> str:
    value = str(_runtime_env(env).get("WORKSPACE_HUB_FEISHU_BACKEND") or "auto").strip().lower()
    return value or "auto"


def _enabled_domains(env: dict[str, str] | None = None) -> set[str]:
    value = str(
        _runtime_env(env).get("WORKSPACE_HUB_FEISHU_BACKEND_DOMAINS")
        or "im doc drive contact table task calendar vc minutes wiki sheet mail whiteboard"
    ).strip().lower()
    if not value:
        return {"im", "doc", "drive", "contact", "table", "task", "calendar", "vc", "minutes", "wiki", "sheet", "mail", "whiteboard"}
    return {item.strip() for item in value.replace(",", " ").split() if item.strip()}


def _normalize_domain(domain: str) -> str:
    raw = str(domain or "").strip().lower()
    aliases = {
        "docs": "doc",
        "base": "table",
        "bitable": "table",
        "cal": "calendar",
        "contacts": "contact",
        "sheets": "sheet",
    }
    return aliases.get(raw, raw)


def _binary_available(env: dict[str, str] | None = None) -> bool:
    return bool(shutil.which(_lark_cli_bin(env)))


def backend_enabled(domain: str, env: dict[str, str] | None = None) -> bool:
    mode = _backend_mode(env)
    if mode == "legacy":
        return False
    if mode not in {"auto", "lark-cli"}:
        return False
    if _normalize_domain(domain) not in _enabled_domains(env):
        return False
    return _binary_available(env)


def doc_backend_enabled(env: dict[str, str] | None = None) -> bool:
    return backend_enabled("doc", env)


def im_backend_enabled(env: dict[str, str] | None = None) -> bool:
    return backend_enabled("im", env)


def _parse_json_output(text: str) -> dict[str, Any]:
    payload = str(text or "").strip()
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise LarkCliBackendError(
            f"invalid lark-cli json output: {exc}",
            code="invalid_json_output",
            details={"stdout": payload},
        ) from exc
    if not isinstance(parsed, dict):
        raise LarkCliBackendError(
            "unexpected lark-cli output shape",
            code="invalid_output_shape",
            details={"stdout": payload},
        )
    return parsed


def _payload_body(payload: dict[str, Any]) -> dict[str, Any]:
    nested = payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload


def _payload_items(payload: dict[str, Any]) -> list[Any]:
    nested = payload.get("data")
    if isinstance(nested, list):
        return list(nested)
    body = _payload_body(payload)
    items = body.get("items")
    if isinstance(items, list):
        return list(items)
    results = body.get("results")
    if isinstance(results, list):
        return list(results)
    return []


def _run_lark_cli(args: list[str], *, input_text: str | None = None) -> dict[str, Any]:
    command = [_lark_cli_bin(), *args]
    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        input=input_text,
        check=False,
    )
    if result.returncode != 0:
        details: dict[str, Any] = {"args": args}
        stderr_text = str(result.stderr or "").strip()
        stdout_text = str(result.stdout or "").strip()
        if stdout_text:
            try:
                details["stdout_payload"] = json.loads(stdout_text)
            except json.JSONDecodeError:
                details["stdout"] = stdout_text
        if stderr_text:
            details["stderr"] = stderr_text
        hint = ""
        payload = details.get("stdout_payload")
        if isinstance(payload, dict):
            hint = str(payload.get("error", {}).get("hint") or "").strip()
            error_payload = payload.get("error")
            if isinstance(error_payload, dict):
                error_type = str(error_payload.get("type") or "").strip()
                if error_type:
                    details["error_type"] = error_type
                console_url = str(error_payload.get("console_url") or "").strip()
                if console_url:
                    details["console_url"] = console_url
                message = str(error_payload.get("message") or "").strip()
                if error_type == "missing_scope":
                    match = re.search(r"missing required scope\(s\):\s*(.+)$", message)
                    missing_scopes = []
                    if match:
                        missing_scopes = [
                            item.strip()
                            for item in str(match.group(1) or "").split(",")
                            if item.strip()
                        ]
                    details["missing_scopes"] = missing_scopes
                    details["authorization_hint"] = hint or (
                        f'lark-cli auth login --scope "{" ".join(missing_scopes)}"' if missing_scopes else ""
                    )
        message = hint or stderr_text or stdout_text or f"lark-cli exited with code {result.returncode}"
        raise LarkCliBackendError(message, code="command_failed", details=details)
    return _parse_json_output(result.stdout)


def _normalize_api_path(path: str) -> str:
    raw = str(path or "").strip()
    if not raw:
        raise LarkCliBackendError("api path is required", code="missing_api_path")
    if raw.startswith("/open-apis/"):
        return raw
    if raw.startswith("/"):
        return f"/open-apis{raw}"
    return f"/open-apis/{raw}"


def api_call(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "api",
        str(method or "GET").upper(),
        _normalize_api_path(path),
        "--as",
        str(identity or "user"),
    ]
    if params:
        command.extend(["--params", json.dumps(params, ensure_ascii=False)])
    if data:
        command.extend(["--data", json.dumps(data, ensure_ascii=False)])
    payload = _run_lark_cli(command)
    if "data" in payload and isinstance(payload["data"], dict):
        return payload["data"]
    return payload


def _coerce_document_id(payload: dict[str, Any]) -> str:
    body = _payload_body(payload)
    candidates = [
        body.get("document_id"),
        body.get("documentId"),
        body.get("doc_id"),
        body.get("docId"),
        body.get("token"),
        body.get("obj_token"),
    ]
    for item in candidates:
        value = str(item or "").strip()
        if value:
            return value
    url = str(body.get("url") or body.get("document_url") or body.get("document_uri") or body.get("doc_url") or "").strip()
    if url:
        match = re.search(r"/docx/([A-Za-z0-9]+)", url)
        if match:
            return str(match.group(1))
    return ""


def _coerce_document_url(payload: dict[str, Any], *, document_id: str = "") -> str:
    body = _payload_body(payload)
    candidates = [
        body.get("url"),
        body.get("document_url"),
        body.get("document_uri"),
        body.get("doc_url"),
        body.get("link"),
    ]
    for item in candidates:
        value = str(item or "").strip()
        if value:
            return value
    if document_id:
        return f"https://feishu.cn/docx/{document_id}"
    return ""


def _coerce_message_id(payload: dict[str, Any]) -> str:
    body = _payload_body(payload)
    candidates = [
        body.get("message_id"),
        body.get("messageId"),
        body.get("msg_id"),
        body.get("id"),
    ]
    for item in candidates:
        value = str(item or "").strip()
        if value:
            return value
    message = body.get("message")
    if isinstance(message, dict):
        for key in ("message_id", "messageId", "id"):
            value = str(message.get(key) or "").strip()
            if value:
                return value
    return ""


def doc_create(*, title: str, content: str = "", file_path: str = "", folder_token: str = "") -> dict[str, Any]:
    markdown = str(content or "").strip()
    if not markdown and file_path:
        markdown = Path(file_path).read_text(encoding="utf-8")
    if not markdown:
        raise LarkCliBackendError("doc create requires content or file_path", code="missing_doc_content")
    command = [
        "docs",
        "+create",
        "--as",
        "user",
        "--title",
        title,
        "--markdown",
        markdown,
    ]
    if folder_token:
        command.extend(["--folder-token", folder_token])
    payload = _run_lark_cli(command)
    document_id = _coerce_document_id(payload)
    url = _coerce_document_url(payload, document_id=document_id)
    return {
        "ok": True,
        "document_id": document_id,
        "url": url,
        "result": payload,
        "backend": "lark-cli",
    }


def doc_fetch(*, document: str) -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "docs",
            "+fetch",
            "--as",
            "user",
            "--doc",
            document,
            "--format",
            "json",
        ]
    )
    document_id = _coerce_document_id(payload) or str(document or "").strip()
    body = _payload_body(payload)
    return {
        "document_id": document_id,
        "content": body.get("markdown") or body.get("content"),
        "result": payload,
        "backend": "lark-cli",
    }


def doc_search(*, query: str, page_size: int = 15, page_token: str = "") -> dict[str, Any]:
    if not str(query or "").strip():
        raise LarkCliBackendError("doc search requires query", code="missing_query")
    command = [
        "docs",
        "+search",
        "--as",
        "user",
        "--query",
        str(query),
        "--page-size",
        str(int(page_size or 15)),
    ]
    if str(page_token or "").strip():
        command.extend(["--page-token", str(page_token).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "files": _payload_items(payload),
        "page_token": str(body.get("page_token") or body.get("next_page_token") or "").strip(),
        "has_more": bool(body.get("has_more")),
        "result": payload,
        "backend": "lark-cli",
    }


def doc_list(*, folder_token: str = "", page_size: int = 50, order_by: str = "EditedTime", direction: str = "DESC") -> dict[str, Any]:
    params: dict[str, Any] = {
        "page_size": int(page_size or 50),
        "order_by": str(order_by or "EditedTime"),
        "direction": str(direction or "DESC"),
    }
    if str(folder_token or "").strip():
        params["folder_token"] = str(folder_token).strip()
    body = api_call("GET", "/drive/v1/files", params=params, identity="user")
    return {
        "files": list(body.get("files") or []) if isinstance(body.get("files"), list) else [],
        "result": {"data": body},
        "backend": "lark-cli",
    }


def doc_insert_image(*, document: str, file_path: str) -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "docs",
            "+media-insert",
            "--as",
            "user",
            "--doc",
            document,
            "--file",
            file_path,
            "--type",
            "image",
        ]
    )
    document_id = _coerce_document_id(payload) or str(document or "").strip()
    return {
        "ok": True,
        "document_id": document_id,
        "file_path": file_path,
        "url": _coerce_document_url(payload, document_id=document_id),
        "result": payload,
        "backend": "lark-cli",
    }


def im_send(
    *,
    chat_id: str = "",
    user_id: str = "",
    msg_type: str = "text",
    text: str = "",
    content: str = "",
    markdown: str = "",
    image: str = "",
    file: str = "",
    audio: str = "",
    video: str = "",
    video_cover: str = "",
    identity: str = "bot",
) -> dict[str, Any]:
    command = ["im", "+messages-send", "--as", str(identity or "bot")]
    if str(chat_id or "").strip():
        command.extend(["--chat-id", str(chat_id).strip()])
    elif str(user_id or "").strip():
        command.extend(["--user-id", str(user_id).strip()])
    else:
        raise LarkCliBackendError("im send requires chat_id or user_id", code="missing_target")
    if str(markdown or "").strip():
        command.extend(["--markdown", str(markdown).strip()])
    elif str(image or "").strip():
        command.extend(["--image", str(image).strip()])
    elif str(file or "").strip():
        command.extend(["--file", str(file).strip()])
    elif str(audio or "").strip():
        command.extend(["--audio", str(audio).strip()])
    elif str(video or "").strip():
        if not str(video_cover or "").strip():
            raise LarkCliBackendError("im send video requires video_cover", code="missing_video_cover")
        command.extend(["--video", str(video).strip(), "--video-cover", str(video_cover).strip()])
    elif str(msg_type or "text").strip() == "text":
        if not str(text or "").strip():
            raise LarkCliBackendError("im send requires text", code="missing_text")
        command.extend(["--text", str(text)])
    else:
        if not str(content or "").strip():
            raise LarkCliBackendError("im send requires content", code="missing_content")
        command.extend(["--msg-type", str(msg_type or "text")])
        command.extend(["--content", str(content)])
    payload = _run_lark_cli(command)
    return {
        "ok": True,
        "message_id": _coerce_message_id(payload),
        "result": payload,
        "backend": "lark-cli",
    }


def im_reply(
    *,
    message_id: str,
    msg_type: str = "text",
    text: str = "",
    content: str = "",
    markdown: str = "",
    image: str = "",
    file: str = "",
    audio: str = "",
    video: str = "",
    video_cover: str = "",
    reply_in_thread: bool = False,
    identity: str = "bot",
) -> dict[str, Any]:
    command = [
        "im",
        "+messages-reply",
        "--as",
        str(identity or "bot"),
        "--message-id",
        str(message_id or "").strip(),
    ]
    if reply_in_thread:
        command.append("--reply-in-thread")
    if str(markdown or "").strip():
        command.extend(["--markdown", str(markdown).strip()])
    elif str(image or "").strip():
        command.extend(["--image", str(image).strip()])
    elif str(file or "").strip():
        command.extend(["--file", str(file).strip()])
    elif str(audio or "").strip():
        command.extend(["--audio", str(audio).strip()])
    elif str(video or "").strip():
        if not str(video_cover or "").strip():
            raise LarkCliBackendError("im reply video requires video_cover", code="missing_video_cover")
        command.extend(["--video", str(video).strip(), "--video-cover", str(video_cover).strip()])
    elif str(msg_type or "text").strip() == "text":
        if not str(text or "").strip():
            raise LarkCliBackendError("im reply requires text", code="missing_text")
        command.extend(["--text", str(text)])
    else:
        if not str(content or "").strip():
            raise LarkCliBackendError("im reply requires content", code="missing_content")
        command.extend(["--msg-type", str(msg_type or "text")])
        command.extend(["--content", str(content)])
    payload = _run_lark_cli(command)
    return {
        "ok": True,
        "message_id": _coerce_message_id(payload),
        "result": payload,
        "backend": "lark-cli",
    }


def im_chat_search(*, query: str, page_size: int = 20, identity: str = "bot") -> dict[str, Any]:
    command = [
        "im",
        "+chat-search",
        "--as",
        str(identity or "bot"),
        "--query",
        str(query or ""),
        "--page-size",
        str(int(page_size or 20)),
    ]
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "chats": list(items) if isinstance(items, list) else [],
        "result": payload,
        "backend": "lark-cli",
    }


def im_chat_messages_list(
    *,
    chat_id: str = "",
    user_id: str = "",
    page_size: int = 50,
    identity: str = "bot",
) -> dict[str, Any]:
    command = [
        "im",
        "+chat-messages-list",
        "--as",
        str(identity or "bot"),
        "--page-size",
        str(int(page_size or 50)),
    ]
    if str(chat_id or "").strip():
        command.extend(["--chat-id", str(chat_id).strip()])
    elif str(user_id or "").strip():
        command.extend(["--user-id", str(user_id).strip()])
    else:
        raise LarkCliBackendError("im history requires chat_id or user_id", code="missing_target")
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "messages": list(items) if isinstance(items, list) else [],
        "result": payload,
        "backend": "lark-cli",
    }


def im_messages_search(*, query: str, page_size: int = 20, identity: str = "user") -> dict[str, Any]:
    if not str(query or "").strip():
        raise LarkCliBackendError("im search requires query", code="missing_query")
    command = [
        "im",
        "+messages-search",
        "--as",
        str(identity or "user"),
        "--query",
        str(query),
        "--page-size",
        str(int(page_size or 20)),
    ]
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "messages": list(items) if isinstance(items, list) else [],
        "result": payload,
        "backend": "lark-cli",
    }


def im_download_resources(
    *,
    message_id: str,
    file_key: str,
    resource_type: str,
    output: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    normalized_type = str(resource_type or "").strip().lower()
    if normalized_type not in {"image", "file"}:
        raise LarkCliBackendError("resource type must be image or file", code="invalid_resource_type")
    command = [
        "im",
        "+messages-resources-download",
        "--as",
        str(identity or "user"),
        "--message-id",
        str(message_id or "").strip(),
        "--file-key",
        str(file_key or "").strip(),
        "--type",
        normalized_type,
    ]
    if str(output or "").strip():
        command.extend(["--output", str(output).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "message_id": str(message_id or "").strip(),
        "file_key": str(file_key or "").strip(),
        "type": normalized_type,
        "path": str(body.get("output") or body.get("path") or body.get("file_path") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def contact_get(*, user_id: str = "", user_id_type: str = "open_id") -> dict[str, Any]:
    command = ["contact", "+get-user", "--as", "user"]
    target = str(user_id or "").strip()
    if target:
        command.extend(["--user-id", target, "--user-id-type", str(user_id_type or "open_id")])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "user": body.get("user") or {},
        "result": payload,
        "backend": "lark-cli",
    }


def contact_search(*, query: str, page_size: int = 20, page_token: str = "") -> dict[str, Any]:
    if not str(query or "").strip():
        raise LarkCliBackendError("contact search requires query", code="missing_query")
    command = [
        "contact",
        "+search-user",
        "--as",
        "user",
        "--query",
        str(query),
        "--page-size",
        str(int(page_size or 20)),
    ]
    if str(page_token or "").strip():
        command.extend(["--page-token", str(page_token).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    items = body.get("items")
    if not isinstance(items, list):
        items = body.get("users")
    return {
        "users": items or [],
        "result": payload,
        "backend": "lark-cli",
    }


def task_list(*, query: str = "", completed: bool = False) -> dict[str, Any]:
    command = ["task", "+get-my-tasks", "--as", "user"]
    if completed:
        command.append("--complete")
    if str(query or "").strip():
        command.extend(["--query", str(query).strip()])
    payload = _run_lark_cli(command)
    return {
        "tasks": _payload_items(payload),
        "result": payload,
        "backend": "lark-cli",
    }


def task_create(*, summary: str, description: str = "", due: str = "", assignee: str = "") -> dict[str, Any]:
    if not str(summary or "").strip():
        raise LarkCliBackendError("task create requires summary", code="missing_summary")
    command = ["task", "+create", "--as", "user", "--summary", str(summary).strip()]
    if str(description or "").strip():
        command.extend(["--description", str(description).strip()])
    if str(due or "").strip():
        command.extend(["--due", str(due).strip()])
    if str(assignee or "").strip():
        command.extend(["--assignee", str(assignee).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    task = body.get("task")
    if not isinstance(task, dict):
        task = body
    return {
        "ok": True,
        "task_id": str(task.get("guid") or task.get("id") or body.get("guid") or "").strip(),
        "task": task,
        "result": payload,
        "backend": "lark-cli",
    }


def task_complete(*, task_id: str) -> dict[str, Any]:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise LarkCliBackendError("task complete requires task_id", code="missing_task_id")
    payload = _run_lark_cli(
        [
            "task",
            "+complete",
            "--as",
            "user",
            "--task-id",
            normalized_task_id,
        ]
    )
    return {
        "ok": True,
        "task_id": normalized_task_id,
        "result": payload,
        "backend": "lark-cli",
    }


def task_delete(*, task_id: str) -> dict[str, Any]:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise LarkCliBackendError("task delete requires task_id", code="missing_task_id")
    body = api_call("DELETE", f"/task/v2/tasks/{normalized_task_id}", identity="user")
    return {
        "ok": True,
        "task_id": normalized_task_id,
        "result": {"data": body},
        "backend": "lark-cli",
    }


def calendar_agenda(*, calendar_id: str = "", start: str = "", end: str = "", identity: str = "bot") -> dict[str, Any]:
    command = ["calendar", "+agenda", "--as", str(identity or "bot")]
    if str(calendar_id or "").strip():
        command.extend(["--calendar-id", str(calendar_id).strip()])
    if str(start or "").strip():
        command.extend(["--start", str(start).strip()])
    if str(end or "").strip():
        command.extend(["--end", str(end).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    data = payload.get("data")
    events = list(data) if isinstance(data, list) else _payload_items(payload)
    resolved_calendar_id = str(body.get("calendar_id") or calendar_id or "").strip()
    return {
        "calendar_id": resolved_calendar_id,
        "events": events,
        "result": payload,
        "backend": "lark-cli",
    }


def calendar_create(
    *,
    summary: str,
    start: str,
    end: str,
    calendar_id: str = "",
    description: str = "",
    attendee_ids: list[str] | None = None,
    identity: str = "bot",
) -> dict[str, Any]:
    if not str(summary or "").strip():
        raise LarkCliBackendError("calendar create requires summary", code="missing_summary")
    if not str(start or "").strip() or not str(end or "").strip():
        raise LarkCliBackendError("calendar create requires start and end", code="missing_time_range")
    command = [
        "calendar",
        "+create",
        "--as",
        str(identity or "bot"),
        "--summary",
        str(summary).strip(),
        "--start",
        str(start).strip(),
        "--end",
        str(end).strip(),
    ]
    if str(calendar_id or "").strip():
        command.extend(["--calendar-id", str(calendar_id).strip()])
    if str(description or "").strip():
        command.extend(["--description", str(description).strip()])
    attendee_values = [str(item).strip() for item in (attendee_ids or []) if str(item).strip()]
    if attendee_values:
        command.extend(["--attendee-ids", ",".join(attendee_values)])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    event = body.get("event")
    if not isinstance(event, dict):
        event = body
    return {
        "ok": True,
        "event": event,
        "event_id": str(event.get("event_id") or event.get("id") or body.get("event_id") or "").strip(),
        "calendar_id": str(body.get("calendar_id") or calendar_id or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def calendar_get(*, calendar_id: str, event_id: str, identity: str = "bot") -> dict[str, Any]:
    normalized_calendar_id = str(calendar_id or "").strip()
    normalized_event_id = str(event_id or "").strip()
    if not normalized_calendar_id:
        raise LarkCliBackendError("calendar get requires calendar_id", code="missing_calendar_id")
    if not normalized_event_id:
        raise LarkCliBackendError("calendar get requires event_id", code="missing_event_id")
    body = api_call(
        "GET",
        f"/calendar/v4/calendars/{normalized_calendar_id}/events/{normalized_event_id}",
        identity=str(identity or "bot"),
    )
    event = body.get("event")
    if not isinstance(event, dict):
        event = body
    return {
        "calendar_id": normalized_calendar_id,
        "event": event,
        "result": {"data": body},
        "backend": "lark-cli",
    }


def calendar_delete(*, calendar_id: str, event_id: str, identity: str = "bot") -> dict[str, Any]:
    normalized_calendar_id = str(calendar_id or "").strip()
    normalized_event_id = str(event_id or "").strip()
    if not normalized_calendar_id:
        raise LarkCliBackendError("calendar delete requires calendar_id", code="missing_calendar_id")
    if not normalized_event_id:
        raise LarkCliBackendError("calendar delete requires event_id", code="missing_event_id")
    body = api_call(
        "DELETE",
        f"/calendar/v4/calendars/{normalized_calendar_id}/events/{normalized_event_id}",
        identity=str(identity or "bot"),
    )
    return {
        "ok": True,
        "calendar_id": normalized_calendar_id,
        "event_id": normalized_event_id,
        "result": {"data": body},
        "backend": "lark-cli",
    }


def drive_upload(
    *,
    file_path: str,
    folder_token: str = "",
    name: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = ["drive", "+upload", "--as", str(identity or "user"), "--file", str(file_path or "").strip()]
    if str(folder_token or "").strip():
        command.extend(["--folder-token", str(folder_token).strip()])
    if str(name or "").strip():
        command.extend(["--name", str(name).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "file_token": str(body.get("file_token") or body.get("token") or body.get("obj_token") or "").strip(),
        "name": str(body.get("name") or "").strip(),
        "url": str(body.get("url") or body.get("file_url") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def drive_download(
    *,
    file_token: str,
    output: str = "",
    overwrite: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = ["drive", "+download", "--as", str(identity or "user"), "--file-token", str(file_token or "").strip()]
    if str(output or "").strip():
        command.extend(["--output", str(output).strip()])
    if overwrite:
        command.append("--overwrite")
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "file_token": str(file_token or "").strip(),
        "path": str(body.get("output") or body.get("path") or body.get("file_path") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def drive_add_comment(
    *,
    doc: str,
    content: str,
    block_id: str = "",
    selection_with_ellipsis: str = "",
    full_comment: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "drive",
        "+add-comment",
        "--as",
        str(identity or "user"),
        "--doc",
        str(doc or "").strip(),
        "--content",
        str(content or "").strip(),
    ]
    if str(block_id or "").strip():
        command.extend(["--block-id", str(block_id).strip()])
    if str(selection_with_ellipsis or "").strip():
        command.extend(["--selection-with-ellipsis", str(selection_with_ellipsis).strip()])
    if full_comment:
        command.append("--full-comment")
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "comment_id": str(body.get("comment_id") or body.get("id") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def vc_search(
    *,
    query: str = "",
    start: str = "",
    end: str = "",
    organizer_ids: list[str] | None = None,
    participant_ids: list[str] | None = None,
    room_ids: list[str] | None = None,
    page_size: int = 15,
    page_token: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = ["vc", "+search", "--as", str(identity or "user"), "--format", "json", "--page-size", str(int(page_size or 15))]
    if str(query or "").strip():
        command.extend(["--query", str(query).strip()])
    if str(start or "").strip():
        command.extend(["--start", str(start).strip()])
    if str(end or "").strip():
        command.extend(["--end", str(end).strip()])
    if organizer_ids:
        command.extend(["--organizer-ids", ",".join(str(item).strip() for item in organizer_ids if str(item).strip())])
    if participant_ids:
        command.extend(["--participant-ids", ",".join(str(item).strip() for item in participant_ids if str(item).strip())])
    if room_ids:
        command.extend(["--room-ids", ",".join(str(item).strip() for item in room_ids if str(item).strip())])
    if str(page_token or "").strip():
        command.extend(["--page-token", str(page_token).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "meetings": _payload_items(payload),
        "page_token": str(body.get("page_token") or body.get("next_page_token") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def vc_notes(
    *,
    meeting_ids: list[str] | None = None,
    minute_tokens: list[str] | None = None,
    calendar_event_ids: list[str] | None = None,
    output_dir: str = "",
    overwrite: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = ["vc", "+notes", "--as", str(identity or "user"), "--format", "json"]
    if meeting_ids:
        command.extend(["--meeting-ids", ",".join(str(item).strip() for item in meeting_ids if str(item).strip())])
    if minute_tokens:
        command.extend(["--minute-tokens", ",".join(str(item).strip() for item in minute_tokens if str(item).strip())])
    if calendar_event_ids:
        command.extend(["--calendar-event-ids", ",".join(str(item).strip() for item in calendar_event_ids if str(item).strip())])
    if str(output_dir or "").strip():
        command.extend(["--output-dir", str(output_dir).strip()])
    if overwrite:
        command.append("--overwrite")
    payload = _run_lark_cli(command)
    return {
        "notes": _payload_items(payload),
        "result": payload,
        "backend": "lark-cli",
    }


def minutes_get(*, minute_token: str, identity: str = "user") -> dict[str, Any]:
    body = api_call(
        "GET",
        "/minutes/v1/minutes",
        params={"minute_token": str(minute_token or "").strip()},
        identity=str(identity or "user"),
    )
    minutes = body.get("minutes")
    if not isinstance(minutes, dict):
        minutes = body
    return {
        "minute": minutes,
        "result": {"data": body},
        "backend": "lark-cli",
    }


def wiki_get_node(*, token: str, identity: str = "user") -> dict[str, Any]:
    body = api_call(
        "GET",
        "/wiki/v2/spaces/get_node",
        params={"token": str(token or "").strip()},
        identity=str(identity or "user"),
    )
    node = body.get("node")
    if not isinstance(node, dict):
        node = body
    return {
        "node": node,
        "result": {"data": body},
        "backend": "lark-cli",
    }


def sheet_create(
    *,
    title: str,
    headers: list[Any] | None = None,
    data: list[Any] | None = None,
    folder_token: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = ["sheets", "+create", "--as", str(identity or "user"), "--title", str(title or "").strip()]
    if headers is not None:
        command.extend(["--headers", json.dumps(headers, ensure_ascii=False)])
    if data is not None:
        command.extend(["--data", json.dumps(data, ensure_ascii=False)])
    if str(folder_token or "").strip():
        command.extend(["--folder-token", str(folder_token).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {
        "spreadsheet_token": str(body.get("spreadsheet_token") or body.get("token") or body.get("obj_token") or "").strip(),
        "url": str(body.get("url") or "").strip(),
        "result": payload,
        "backend": "lark-cli",
    }


def _sheet_locator_command(base_command: list[str], *, spreadsheet_token: str = "", url: str = "", sheet_id: str = "", range_expr: str = "") -> list[str]:
    command = list(base_command)
    if str(url or "").strip():
        command.extend(["--url", str(url).strip()])
    elif str(spreadsheet_token or "").strip():
        command.extend(["--spreadsheet-token", str(spreadsheet_token).strip()])
    if str(sheet_id or "").strip():
        command.extend(["--sheet-id", str(sheet_id).strip()])
    if str(range_expr or "").strip():
        command.extend(["--range", str(range_expr).strip()])
    return command


def sheet_info(*, spreadsheet_token: str = "", url: str = "", identity: str = "user") -> dict[str, Any]:
    command = _sheet_locator_command(["sheets", "+info", "--as", str(identity or "user")], spreadsheet_token=spreadsheet_token, url=url)
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {"spreadsheet": body, "result": payload, "backend": "lark-cli"}


def sheet_read(
    *,
    spreadsheet_token: str = "",
    url: str = "",
    sheet_id: str = "",
    range_expr: str = "",
    value_render_option: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = _sheet_locator_command(
        ["sheets", "+read", "--as", str(identity or "user")],
        spreadsheet_token=spreadsheet_token,
        url=url,
        sheet_id=sheet_id,
        range_expr=range_expr,
    )
    if str(value_render_option or "").strip():
        command.extend(["--value-render-option", str(value_render_option).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    return {"values": body.get("values") or body.get("data") or [], "result": payload, "backend": "lark-cli"}


def sheet_write(
    *,
    values: list[Any],
    spreadsheet_token: str = "",
    url: str = "",
    sheet_id: str = "",
    range_expr: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = _sheet_locator_command(
        ["sheets", "+write", "--as", str(identity or "user"), "--values", json.dumps(values, ensure_ascii=False)],
        spreadsheet_token=spreadsheet_token,
        url=url,
        sheet_id=sheet_id,
        range_expr=range_expr,
    )
    payload = _run_lark_cli(command)
    return {"ok": True, "result": payload, "backend": "lark-cli"}


def sheet_append(
    *,
    values: list[Any],
    spreadsheet_token: str = "",
    url: str = "",
    sheet_id: str = "",
    range_expr: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = _sheet_locator_command(
        ["sheets", "+append", "--as", str(identity or "user"), "--values", json.dumps(values, ensure_ascii=False)],
        spreadsheet_token=spreadsheet_token,
        url=url,
        sheet_id=sheet_id,
        range_expr=range_expr,
    )
    payload = _run_lark_cli(command)
    return {"ok": True, "result": payload, "backend": "lark-cli"}


def sheet_find(
    *,
    text: str,
    spreadsheet_token: str = "",
    url: str = "",
    sheet_id: str = "",
    range_expr: str = "",
    ignore_case: bool = False,
    include_formulas: bool = False,
    match_entire_cell: bool = False,
    search_by_regex: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = _sheet_locator_command(
        ["sheets", "+find", "--as", str(identity or "user"), "--find", str(text or "").strip()],
        spreadsheet_token=spreadsheet_token,
        url=url,
        sheet_id=sheet_id,
        range_expr=range_expr,
    )
    if ignore_case:
        command.append("--ignore-case")
    if include_formulas:
        command.append("--include-formulas")
    if match_entire_cell:
        command.append("--match-entire-cell")
    if search_by_regex:
        command.append("--search-by-regex")
    payload = _run_lark_cli(command)
    return {"matches": _payload_items(payload), "result": payload, "backend": "lark-cli"}


def mail_triage(
    *,
    query: str = "",
    filter_json: str = "",
    mailbox: str = "me",
    max_count: int = 20,
    labels: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = ["mail", "+triage", "--as", str(identity or "user"), "--format", "json", "--mailbox", str(mailbox or "me"), "--max", str(int(max_count or 20))]
    if str(query or "").strip():
        command.extend(["--query", str(query).strip()])
    if str(filter_json or "").strip():
        command.extend(["--filter", str(filter_json).strip()])
    if labels:
        command.append("--labels")
    payload = _run_lark_cli(command)
    return {"messages": _payload_items(payload), "result": payload, "backend": "lark-cli"}


def mail_send(
    *,
    to: str,
    subject: str,
    body: str,
    cc: str = "",
    bcc: str = "",
    sender: str = "",
    attach: str = "",
    inline: str = "",
    confirm_send: bool = False,
    plain_text: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "mail",
        "+send",
        "--as",
        str(identity or "user"),
        "--to",
        str(to or "").strip(),
        "--subject",
        str(subject or "").strip(),
        "--body",
        str(body or ""),
    ]
    if str(cc or "").strip():
        command.extend(["--cc", str(cc).strip()])
    if str(bcc or "").strip():
        command.extend(["--bcc", str(bcc).strip()])
    if str(sender or "").strip():
        command.extend(["--from", str(sender).strip()])
    if str(attach or "").strip():
        command.extend(["--attach", str(attach).strip()])
    if str(inline or "").strip():
        command.extend(["--inline", str(inline).strip()])
    if confirm_send:
        command.append("--confirm-send")
    if plain_text:
        command.append("--plain-text")
    payload = _run_lark_cli(command)
    return {"message": _payload_body(payload), "result": payload, "backend": "lark-cli"}


def mail_reply(
    *,
    message_id: str,
    body: str,
    to: str = "",
    cc: str = "",
    bcc: str = "",
    sender: str = "",
    attach: str = "",
    inline: str = "",
    confirm_send: bool = False,
    plain_text: bool = False,
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "mail",
        "+reply",
        "--as",
        str(identity or "user"),
        "--message-id",
        str(message_id or "").strip(),
        "--body",
        str(body or ""),
    ]
    if str(to or "").strip():
        command.extend(["--to", str(to).strip()])
    if str(cc or "").strip():
        command.extend(["--cc", str(cc).strip()])
    if str(bcc or "").strip():
        command.extend(["--bcc", str(bcc).strip()])
    if str(sender or "").strip():
        command.extend(["--from", str(sender).strip()])
    if str(attach or "").strip():
        command.extend(["--attach", str(attach).strip()])
    if str(inline or "").strip():
        command.extend(["--inline", str(inline).strip()])
    if confirm_send:
        command.append("--confirm-send")
    if plain_text:
        command.append("--plain-text")
    payload = _run_lark_cli(command)
    return {"message": _payload_body(payload), "result": payload, "backend": "lark-cli"}


def mail_message(*, message_id: str, mailbox: str = "me", html: bool = True, identity: str = "user") -> dict[str, Any]:
    command = [
        "mail",
        "+message",
        "--as",
        str(identity or "user"),
        "--message-id",
        str(message_id or "").strip(),
        "--mailbox",
        str(mailbox or "me"),
    ]
    if not html:
        command.extend(["--html=false"])
    payload = _run_lark_cli(command)
    return {"message": _payload_body(payload), "result": payload, "backend": "lark-cli"}


def mail_thread(*, thread_id: str, mailbox: str = "me", html: bool = True, identity: str = "user") -> dict[str, Any]:
    command = [
        "mail",
        "+thread",
        "--as",
        str(identity or "user"),
        "--thread-id",
        str(thread_id or "").strip(),
        "--mailbox",
        str(mailbox or "me"),
    ]
    if not html:
        command.extend(["--html=false"])
    payload = _run_lark_cli(command)
    return {"thread": _payload_body(payload), "result": payload, "backend": "lark-cli"}


def whiteboard_update(
    *,
    whiteboard_token: str,
    dsl: str,
    overwrite: bool = False,
    yes: bool = False,
    idempotent_token: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "docs",
        "+whiteboard-update",
        "--as",
        str(identity or "user"),
        "--whiteboard-token",
        str(whiteboard_token or "").strip(),
    ]
    if overwrite:
        command.append("--overwrite")
    if yes:
        command.append("--yes")
    if str(idempotent_token or "").strip():
        command.extend(["--idempotent-token", str(idempotent_token).strip()])
    payload = _run_lark_cli(command, input_text=str(dsl or ""))
    return {"whiteboard_token": str(whiteboard_token or "").strip(), "result": payload, "backend": "lark-cli"}


def base_get(*, base_token: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+base-get",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
        ]
    )
    body = _payload_body(payload)
    return {
        "base": body.get("base") or {},
        "result": payload,
        "backend": "lark-cli",
    }


def base_table_list(*, base_token: str, limit: int = 50, offset: int = 0, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+table-list",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--limit",
            str(int(limit or 50)),
            "--offset",
            str(int(offset or 0)),
        ]
    )
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "tables": list(items) if isinstance(items, list) else [],
        "total": body.get("total"),
        "count": body.get("count"),
        "limit": body.get("limit"),
        "offset": body.get("offset"),
        "result": payload,
        "backend": "lark-cli",
    }


def base_field_list(
    *,
    base_token: str,
    table_id: str,
    limit: int = 100,
    offset: int = 0,
    identity: str = "user",
) -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+field-list",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--limit",
            str(int(limit or 100)),
            "--offset",
            str(int(offset or 0)),
        ]
    )
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "fields": list(items) if isinstance(items, list) else [],
        "total": body.get("total"),
        "count": body.get("count"),
        "limit": body.get("limit"),
        "offset": body.get("offset"),
        "result": payload,
        "backend": "lark-cli",
    }


def base_view_list(
    *,
    base_token: str,
    table_id: str,
    limit: int = 100,
    offset: int = 0,
    identity: str = "user",
) -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+view-list",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--limit",
            str(int(limit or 100)),
            "--offset",
            str(int(offset or 0)),
        ]
    )
    body = _payload_body(payload)
    items = body.get("items")
    return {
        "views": list(items) if isinstance(items, list) else [],
        "total": body.get("total"),
        "count": body.get("count"),
        "limit": body.get("limit"),
        "offset": body.get("offset"),
        "result": payload,
        "backend": "lark-cli",
    }


def base_record_list(
    *,
    base_token: str,
    table_id: str,
    limit: int = 100,
    offset: int = 0,
    view_id: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "base",
        "+record-list",
        "--as",
        str(identity or "user"),
        "--base-token",
        str(base_token or "").strip(),
        "--table-id",
        str(table_id or "").strip(),
        "--limit",
        str(int(limit or 100)),
        "--offset",
        str(int(offset or 0)),
    ]
    if str(view_id or "").strip():
        command.extend(["--view-id", str(view_id).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    items = body.get("items")
    if isinstance(items, list):
        records = list(items)
    else:
        records = []
        rows = body.get("data")
        fields = body.get("fields")
        record_ids = body.get("record_id_list")
        if isinstance(rows, list) and isinstance(fields, list):
            ids = record_ids if isinstance(record_ids, list) else []
            for index, row in enumerate(rows):
                values = row if isinstance(row, list) else []
                record_fields = {str(name): values[pos] for pos, name in enumerate(fields) if pos < len(values)}
                record_id = str(ids[index] if index < len(ids) else "").strip()
                records.append({"record_id": record_id, "fields": record_fields})
    return {
        "records": records,
        "total": body.get("total") if body.get("total") is not None else len(records),
        "count": body.get("count") if body.get("count") is not None else len(records),
        "limit": body.get("limit"),
        "offset": body.get("offset"),
        "fields": list(body.get("fields") or []) if isinstance(body.get("fields"), list) else [],
        "record_id_list": list(body.get("record_id_list") or []) if isinstance(body.get("record_id_list"), list) else [],
        "result": payload,
        "backend": "lark-cli",
    }


def base_record_upsert(
    *,
    base_token: str,
    table_id: str,
    fields: dict[str, Any],
    record_id: str = "",
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "base",
        "+record-upsert",
        "--as",
        str(identity or "user"),
        "--base-token",
        str(base_token or "").strip(),
        "--table-id",
        str(table_id or "").strip(),
        "--json",
        json.dumps(fields or {}, ensure_ascii=False),
    ]
    target_record_id = str(record_id or "").strip()
    if target_record_id:
        command.extend(["--record-id", target_record_id])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    record = body.get("record")
    if not isinstance(record, dict):
        record = {"record_id": target_record_id, "fields": fields or {}}
    resolved_record_id = (
        str(record.get("record_id") or body.get("record_id") or target_record_id or "").strip()
    )
    if resolved_record_id and not str(record.get("record_id") or "").strip():
        record["record_id"] = resolved_record_id
    return {
        "record_id": resolved_record_id,
        "record": record,
        "created": bool(body.get("created")),
        "updated": bool(body.get("updated")),
        "result": payload,
        "backend": "lark-cli",
    }


def base_record_delete(*, base_token: str, table_id: str, record_id: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+record-delete",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--record-id",
            str(record_id or "").strip(),
            "--yes",
        ]
    )
    return {"ok": True, "record_id": str(record_id or "").strip(), "result": payload, "backend": "lark-cli"}


def base_app_create(*, name: str, time_zone: str = "", folder_token: str = "", identity: str = "user") -> dict[str, Any]:
    command = ["base", "+base-create", "--as", str(identity or "user"), "--name", str(name or "").strip()]
    if str(time_zone or "").strip():
        command.extend(["--time-zone", str(time_zone).strip()])
    if str(folder_token or "").strip():
        command.extend(["--folder-token", str(folder_token).strip()])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    base = body.get("base")
    if not isinstance(base, dict):
        base = body
    return {"app": base, "result": payload, "backend": "lark-cli"}


def base_table_create(
    *,
    base_token: str,
    name: str,
    fields: list[dict[str, Any]] | None = None,
    view: dict[str, Any] | list[dict[str, Any]] | None = None,
    identity: str = "user",
) -> dict[str, Any]:
    command = [
        "base",
        "+table-create",
        "--as",
        str(identity or "user"),
        "--base-token",
        str(base_token or "").strip(),
        "--name",
        str(name or "").strip(),
    ]
    if fields:
        command.extend(["--fields", json.dumps(fields, ensure_ascii=False)])
    if view is not None:
        command.extend(["--view", json.dumps(view, ensure_ascii=False)])
    payload = _run_lark_cli(command)
    body = _payload_body(payload)
    table = body.get("table")
    if not isinstance(table, dict):
        table = body
    return {"table": table, "result": payload, "backend": "lark-cli"}


def base_table_delete(*, base_token: str, table_id: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+table-delete",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--yes",
        ]
    )
    return {"ok": True, "table_id": str(table_id or "").strip(), "result": payload, "backend": "lark-cli"}


def base_field_create(*, base_token: str, table_id: str, field: dict[str, Any], identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+field-create",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--json",
            json.dumps(field, ensure_ascii=False),
        ]
    )
    body = _payload_body(payload)
    field_body = body.get("field")
    if not isinstance(field_body, dict):
        field_body = body
    return {"field": field_body, "result": payload, "backend": "lark-cli"}


def base_field_update(*, base_token: str, table_id: str, field_id: str, field: dict[str, Any], identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+field-update",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--field-id",
            str(field_id or "").strip(),
            "--json",
            json.dumps(field, ensure_ascii=False),
        ]
    )
    body = _payload_body(payload)
    field_body = body.get("field")
    if not isinstance(field_body, dict):
        field_body = body
    return {"field": field_body, "result": payload, "backend": "lark-cli"}


def base_field_delete(*, base_token: str, table_id: str, field_id: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+field-delete",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--field-id",
            str(field_id or "").strip(),
            "--yes",
        ]
    )
    return {"ok": True, "field_id": str(field_id or "").strip(), "result": payload, "backend": "lark-cli"}


def base_view_get(*, base_token: str, table_id: str, view_id: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+view-get",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--view-id",
            str(view_id or "").strip(),
        ]
    )
    body = _payload_body(payload)
    view = body.get("view")
    if not isinstance(view, dict):
        view = body
    return {"view": view, "result": payload, "backend": "lark-cli"}


def base_view_create(
    *,
    base_token: str,
    table_id: str,
    view: dict[str, Any] | list[dict[str, Any]],
    identity: str = "user",
) -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+view-create",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--json",
            json.dumps(view, ensure_ascii=False),
        ]
    )
    body = _payload_body(payload)
    created = body.get("view")
    if not isinstance(created, dict):
        items = _payload_items(payload)
        created = items[0] if items and isinstance(items[0], dict) else body
    return {"view": created, "result": payload, "backend": "lark-cli"}


def base_view_update(*, base_token: str, table_id: str, view_id: str, name: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+view-rename",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--view-id",
            str(view_id or "").strip(),
            "--name",
            str(name or "").strip(),
        ]
    )
    body = _payload_body(payload)
    view = body.get("view")
    if not isinstance(view, dict):
        view = body
    return {"view": view, "result": payload, "backend": "lark-cli"}


def base_view_delete(*, base_token: str, table_id: str, view_id: str, identity: str = "user") -> dict[str, Any]:
    payload = _run_lark_cli(
        [
            "base",
            "+view-delete",
            "--as",
            str(identity or "user"),
            "--base-token",
            str(base_token or "").strip(),
            "--table-id",
            str(table_id or "").strip(),
            "--view-id",
            str(view_id or "").strip(),
            "--yes",
        ]
    )
    return {"ok": True, "view_id": str(view_id or "").strip(), "result": payload, "backend": "lark-cli"}


__all__ = [
    "LarkCliBackendError",
    "api_call",
    "base_field_list",
    "base_get",
    "base_record_list",
    "base_record_upsert",
    "base_table_list",
    "base_view_list",
    "backend_enabled",
    "contact_get",
    "contact_search",
    "doc_list",
    "doc_search",
    "im_backend_enabled",
    "im_chat_messages_list",
    "im_chat_search",
    "im_messages_search",
    "im_reply",
    "im_send",
    "calendar_agenda",
    "calendar_create",
    "calendar_delete",
    "calendar_get",
    "drive_add_comment",
    "drive_download",
    "drive_upload",
    "doc_backend_enabled",
    "doc_create",
    "doc_fetch",
    "doc_insert_image",
    "im_download_resources",
    "task_complete",
    "task_create",
    "task_delete",
    "task_list",
    "mail_message",
    "mail_reply",
    "mail_send",
    "mail_thread",
    "mail_triage",
    "minutes_get",
    "sheet_append",
    "sheet_create",
    "sheet_find",
    "sheet_info",
    "sheet_read",
    "sheet_write",
    "vc_notes",
    "vc_search",
    "whiteboard_update",
    "wiki_get_node",
    "base_app_create",
    "base_field_create",
    "base_field_delete",
    "base_field_update",
    "base_record_delete",
    "base_table_create",
    "base_table_delete",
    "base_view_create",
    "base_view_delete",
    "base_view_get",
    "base_view_update",
]

#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEISHU_PRIVATE_TARGET = str(os.environ.get("WORKSPACE_HUB_FEISHU_PRIVATE_TARGET", "")).strip()


def _run_feishu_operation(domain: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
    command = [
        "python3",
        str(REPO_ROOT / "ops" / "local_broker.py"),
        "feishu-op",
        "--domain",
        domain,
        "--action",
        action,
        "--payload-json",
        json.dumps(payload, ensure_ascii=False),
    ]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or f"exit_code={result.returncode}")
    try:
        payload = json.loads(result.stdout) if result.stdout.strip() else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid feishu delivery response: {exc}") from exc
    if payload.get("ok") is not True:
        raise RuntimeError(str(payload.get("error") or payload.get("summary") or "feishu delivery failed"))
    result_payload = payload.get("result")
    return result_payload if isinstance(result_payload, dict) else payload


def _nested_value(payload: Any, key: str) -> str:
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


def resolve_send_target(target: str) -> str:
    raw = str(target or "").strip()
    spec = raw.split(":", 1)[1] if ":" in raw else raw
    if spec == "coco-private":
        return DEFAULT_FEISHU_PRIVATE_TARGET
    if spec.startswith("user:"):
        return spec.split(":", 1)[1]
    if spec.startswith("chat:"):
        return spec.split(":", 1)[1]
    return spec


def resolve_doc_share_target(target: str) -> str:
    raw = str(target or "").strip()
    if raw.startswith("feishu:chat:") or raw.startswith("chat:"):
        return ""
    resolved = resolve_send_target(raw)
    if resolved.startswith("chat:"):
        return ""
    return resolved


def send_message(
    target: str,
    text: str = "",
    *,
    msg_type: str = "text",
    card: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "to": resolve_send_target(target),
        "msg_type": msg_type,
    }
    if msg_type == "interactive":
        payload["card"] = card or {}
    else:
        payload["text"] = text
    result = _run_feishu_operation("msg", "send", payload)
    return {
        "ok": True,
        "kind": "message",
        "target": payload["to"],
        "receive_id_type": result.get("receive_id_type", ""),
        "msg_type": result.get("msg_type", msg_type),
        "message_id": result.get("message_id", ""),
        "result": result,
    }


def send_card(target: str, card: dict[str, Any], *, text: str = "", kind: str = "card") -> dict[str, Any]:
    result = send_message(target, text=text, msg_type="interactive", card=card)
    result["kind"] = kind
    return result


def send_approval_card(
    target: str,
    card: dict[str, Any],
    *,
    text: str = "",
    gate_token: str = "",
) -> dict[str, Any]:
    result = send_card(target, card, text=text, kind="approval_request")
    if gate_token:
        result["gate_token"] = gate_token
    return result


def create_doc(
    target: str,
    *,
    title: str,
    file_path: str = "",
    content: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": title,
    }
    if file_path:
        payload["file"] = file_path
    if content:
        payload["content"] = content
    share_to = resolve_doc_share_target(target)
    if share_to:
        payload["share_to"] = share_to
    result = _run_feishu_operation("doc", "create", payload)
    document_id = _nested_value(result, "document_id")
    url = _nested_value(result, "url")
    return {
        "ok": True,
        "kind": "doc",
        "target": target,
        "share_to": share_to,
        "title": title,
        "file_path": file_path,
        "document_id": document_id,
        "url": url,
        "result": result,
    }


def deliver_report(
    target: str,
    *,
    title: str,
    file_path: str,
    summary_text: str = "",
) -> dict[str, Any]:
    doc_result = create_doc(target, title=title, file_path=file_path)
    doc_url = str(doc_result.get("url") or "").strip()
    if not summary_text:
        summary_text = f"已生成《{title}》。"
    message_lines = [summary_text.strip()]
    if doc_url:
        message_lines.append(f"飞书文档：{doc_url}")
    delivery_result = send_message(target, text="\n".join(message_lines).strip())
    return {
        "ok": True,
        "kind": "report_delivery",
        "target": target,
        "document": doc_result,
        "delivery": delivery_result,
        "document_id": str(doc_result.get("document_id") or "").strip(),
        "url": doc_url,
        "message_id": str(delivery_result.get("message_id") or "").strip(),
    }


__all__ = [
    "create_doc",
    "deliver_report",
    "resolve_doc_share_target",
    "resolve_send_target",
    "send_approval_card",
    "send_card",
    "send_message",
]

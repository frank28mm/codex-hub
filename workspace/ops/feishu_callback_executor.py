#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

try:
    from ops import background_job_executor, feishu_agent, feishu_outbound_gateway
except ImportError:  # pragma: no cover
    import background_job_executor  # type: ignore
    import feishu_agent  # type: ignore
    import feishu_outbound_gateway  # type: ignore


@dataclass
class FeishuCallbackExecutorError(RuntimeError):
    message: str
    code: str = ""
    details: dict[str, Any] | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise FeishuCallbackExecutorError(f"{key} is required", code=f"missing_{key}")
    return value


def _report_delivery(payload: dict[str, Any]) -> dict[str, Any]:
    target = _require_string(payload, "target")
    title = _require_string(payload, "title")
    file_path = _require_string(payload, "file_path")
    summary_text = str(payload.get("summary_text") or payload.get("summary") or "").strip()

    doc_result = background_job_executor.create_feishu_doc_target(target, title=title, file_path=file_path)
    doc_url = str(doc_result.get("url") or doc_result.get("document", {}).get("url") or "").strip()
    if not summary_text:
        summary_text = f"已生成《{title}》。"
    message_lines = [summary_text]
    if doc_url:
        message_lines.append(f"飞书文档：{doc_url}")
    delivery_result = background_job_executor.deliver_feishu_target(target, text="\n".join(message_lines).strip())
    return {
        "ok": True,
        "action": "report-delivery",
        "target": target,
        "document": doc_result,
        "delivery": delivery_result,
    }


def _doc_create(payload: dict[str, Any]) -> dict[str, Any]:
    target = _require_string(payload, "target")
    title = _require_string(payload, "title")
    file_path = str(payload.get("file_path") or payload.get("file") or "").strip()
    content = str(payload.get("content") or "").strip()
    if not file_path and not content:
        raise FeishuCallbackExecutorError(
            "either file_path or content is required",
            code="missing_doc_content",
        )
    document = feishu_outbound_gateway.create_doc(
        target,
        title=title,
        file_path=file_path,
        content=content,
    )
    return {
        "ok": True,
        "action": "doc-create",
        "target": target,
        "document": document,
    }


def _bitable_writeback(payload: dict[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("mode") or payload.get("write_mode") or "").strip().lower()
    if mode not in {"add", "update", "delete"}:
        raise FeishuCallbackExecutorError(
            "mode must be one of: add, update, delete",
            code="invalid_bitable_mode",
            details={"mode": mode},
        )
    table_payload = payload.get("payload")
    if table_payload is None:
        table_payload = {key: value for key, value in payload.items() if key not in {"mode", "write_mode"}}
    if not isinstance(table_payload, dict):
        raise FeishuCallbackExecutorError("payload must be an object", code="invalid_payload")
    result = feishu_agent.perform_operation("table", mode, table_payload)
    return {
        "ok": True,
        "action": "bitable-writeback",
        "mode": mode,
        "result": result,
    }


def _approval_routed_action(payload: dict[str, Any]) -> dict[str, Any]:
    route = str(payload.get("route") or payload.get("kind") or "").strip().lower()
    if route == "background-job":
        project_name = _require_string(payload, "project_name")
        task_id = _require_string(payload, "task_id")
        approval_token = _require_string(payload, "approval_token")
        projected_job = background_job_executor.board_job_projector.project_background_job(project_name, task_id)
        result = background_job_executor.execute_projected_job(
            projected_job,
            trigger_source="feishu_callback_executor",
            approval_token=approval_token,
            dry_run=bool(payload.get("dry_run")),
        )
        return {
            "ok": True,
            "action": "approval-routed-action",
            "route": route,
            "result": result,
        }
    if route == "feishu-op":
        domain = _require_string(payload, "domain")
        action = _require_string(payload, "feishu_action")
        op_payload = payload.get("payload")
        if not isinstance(op_payload, dict):
            raise FeishuCallbackExecutorError("payload must be an object", code="invalid_payload")
        result = feishu_agent.perform_operation(domain, action, op_payload)
        return {
            "ok": True,
            "action": "approval-routed-action",
            "route": route,
            "result": result,
        }
    raise FeishuCallbackExecutorError(
        "route must be one of: background-job, feishu-op",
        code="invalid_route",
        details={"route": route},
    )


def execute_callback_action(action: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = action.strip().lower()
    data = payload or {}
    if normalized == "doc-create":
        return _doc_create(data)
    if normalized == "report-delivery":
        return _report_delivery(data)
    if normalized == "bitable-writeback":
        return _bitable_writeback(data)
    if normalized == "approval-routed-action":
        return _approval_routed_action(data)
    raise FeishuCallbackExecutorError(
        f"unknown feishu callback action: {action}",
        code="unknown_action",
        details={"action": action},
    )

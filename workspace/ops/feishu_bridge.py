#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import http.server
import hmac
import json
import os
import ssl
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

try:
    from ops import runtime_state, workspace_hub_project, feishu_projection
except ImportError:  # pragma: no cover
    import runtime_state  # type: ignore
    import workspace_hub_project  # type: ignore
    import feishu_projection  # type: ignore

try:  # pragma: no cover - optional dependency in some Python installs
    import certifi
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore


BRIDGE_NAME = "feishu"
SCHEMA_VERSION = "feishu-bridge.v1"
READ_ONLY_COMMANDS = {
    "/projects": "projects",
    "/review": "review-inbox",
    "/reviews": "review-inbox",
    "/coordination": "coordination-inbox",
    "/coord": "coordination-inbox",
    "/health": "health",
    "/status": "status",
}
PROJECT_SCOPED_COMMANDS = {"projects", "review-inbox", "coordination-inbox"}
MAINLINE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
FEISHU_OPEN_BASE = "https://open.feishu.cn/open-apis"


def bridge_contract() -> dict[str, Any]:
    runtime_contract = runtime_state.feishu_runtime_contract()
    return {
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "entry_mode": "compatibility_webhook_only",
        "default_entry_mode": "electron_long_connection",
        "host_mode": "electron",
        "transport": "sdk_websocket_plus_rest",
        "truth_source": runtime_contract["truth_source"],
        "bitable_mode": runtime_contract["bitable_mode"],
        "allowed_write_tables": runtime_contract["writable_tables"],
        "reserved_tables": runtime_contract["reserved_tables"],
        "read_only_tables": runtime_contract["read_only_tables"],
        "forbidden_capabilities": [
            "direct_vault_writes",
            "review_truth_mutation",
            "coordination_truth_mutation",
            "sidecar_receipt_mutation",
            "multi_im_expansion",
        ],
        "task_state_update_mode": "task_writeback_only",
        "shared_broker_owner": "mainline",
    }


def workspace_root() -> Path:
    configured = os.environ.get("WORKSPACE_HUB_ROOT", "").strip()
    if configured:
        candidate = Path(configured)
        if (candidate / "ops" / "local_broker.py").exists():
            return candidate
    return MAINLINE_ROOT


def broker_path() -> Path:
    return workspace_root() / "ops" / "local_broker.py"


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def broker_available() -> bool:
    return broker_path().exists()


def canonical_project_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return workspace_hub_project.canonicalize(text)


def _strip_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    return str(value).strip()


def _jsonish(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[0] not in "[{":
        return text
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _dedupe(parts: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        ordered.append(part)
    return ordered


def _collect_text_parts(value: Any) -> list[str]:
    parts: list[str] = []

    def visit(node: Any) -> None:
        node = _jsonish(node)
        if isinstance(node, str):
            text = node.strip()
            if text:
                parts.append(text)
            return
        if isinstance(node, list):
            for item in node:
                visit(item)
            return
        if isinstance(node, dict):
            text = _strip_text(node.get("text", ""))
            if text:
                parts.append(text)
            for key, child in node.items():
                if key in {"text", "tag", "user_id", "user_name", "href"}:
                    continue
                visit(child)

    visit(value)
    return _dedupe(parts)


def _extract_text(value: Any) -> str:
    if value is None:
        return ""
    decoded = _jsonish(value)
    if isinstance(decoded, str):
        return decoded.strip()
    parts = _collect_text_parts(decoded)
    return "\n".join(parts).strip()


def detect_event_type(payload: dict[str, Any]) -> str:
    header = payload.get("header", {})
    if isinstance(header, dict):
        event_type = _strip_text(header.get("event_type", ""))
        if event_type:
            return event_type
    event = payload.get("event", {})
    if isinstance(event, dict):
        event_type = _strip_text(event.get("type", ""))
        if event_type:
            return event_type
    return _strip_text(payload.get("type", ""))


def detect_payload_schema(payload: dict[str, Any]) -> str:
    if _strip_text(payload.get("challenge", "")):
        return "feishu-url-verification"
    if isinstance(payload.get("header"), dict) and isinstance(payload.get("event"), dict):
        return "feishu-webhook"
    if "event" in payload and isinstance(payload.get("event"), dict):
        return "feishu-event"
    return "bridge-cli"


def safe_text(payload: dict[str, Any]) -> str:
    direct_candidates = [
        payload.get("text", ""),
        payload.get("text_without_at_bot", ""),
    ]
    message = payload.get("message", {})
    if isinstance(message, dict):
        direct_candidates.extend([message.get("text", ""), message.get("text_without_at_bot", "")])
    event = payload.get("event", {})
    if isinstance(event, dict):
        direct_candidates.extend([event.get("text", ""), event.get("text_without_at_bot", "")])
        event_message = event.get("message", {})
        if isinstance(event_message, dict):
            direct_candidates.extend([event_message.get("text", ""), event_message.get("text_without_at_bot", "")])
    for candidate in direct_candidates:
        text = _strip_text(candidate)
        if text:
            return text

    content_candidates = [payload.get("content")]
    if isinstance(message, dict):
        content_candidates.append(message.get("content"))
    event = payload.get("event", {})
    if isinstance(event, dict):
        content_candidates.append(event.get("content"))
        event_message = event.get("message", {})
        if isinstance(event_message, dict):
            content_candidates.append(event_message.get("content"))
        body = event.get("body", {})
        if isinstance(body, dict):
            content_candidates.append(body.get("content"))
    for candidate in content_candidates:
        text = _extract_text(candidate)
        if text:
            return text
    return ""


def detect_project_name(payload: dict[str, Any]) -> str:
    for key in ("project_name", "project", "projectName"):
        value = canonical_project_name(payload.get(key, ""))
        if value:
            return value
    event = payload.get("event", {})
    if isinstance(event, dict):
        for key in ("project_name", "project", "projectName"):
            value = canonical_project_name(event.get(key, ""))
            if value:
                return value
        context = event.get("context", {})
        if isinstance(context, dict):
            for key in ("project_name", "project", "projectName"):
                value = canonical_project_name(context.get(key, ""))
                if value:
                    return value
    return ""


def detect_session_id(payload: dict[str, Any]) -> str:
    for key in ("session_id", "sessionId"):
        value = _strip_text(payload.get(key, ""))
        if value:
            return value
    event = payload.get("event", {})
    if isinstance(event, dict):
        context = event.get("context", {})
        if isinstance(context, dict):
            for key in ("session_id", "sessionId"):
                value = _strip_text(context.get(key, ""))
                if value:
                    return value
    return ""


def inbound_message_id(payload: dict[str, Any]) -> str:
    for key in ("message_id", "open_message_id", "messageId", "event_id"):
        value = _strip_text(payload.get(key, ""))
        if value:
            return value
    event = payload.get("event", {})
    if isinstance(event, dict):
        message = event.get("message", {})
        if isinstance(message, dict):
            for key in ("message_id", "messageId"):
                value = _strip_text(message.get(key, ""))
                if value:
                    return value
    header = payload.get("header", {})
    if isinstance(header, dict):
        for key in ("event_id", "message_id", "messageId"):
            value = _strip_text(header.get(key, ""))
            if value:
                return value
    return ""


def webhook_token(payload: dict[str, Any]) -> str:
    for key in ("token", "verification_token"):
        value = _strip_text(payload.get(key, ""))
        if value:
            return value
    event = payload.get("event", {})
    if isinstance(event, dict):
        for key in ("token", "verification_token"):
            value = _strip_text(event.get(key, ""))
            if value:
                return value
    return ""


def header_value(headers: dict[str, Any], key: str) -> str:
    key_l = key.lower()
    for header_key, header_value_raw in headers.items():
        if str(header_key).lower() == key_l:
            return _strip_text(header_value_raw)
    return ""


def configured_verification_token() -> str:
    return os.environ.get("FEISHU_VERIFICATION_TOKEN", "").strip()


def configured_signing_secret() -> str:
    return os.environ.get("FEISHU_SIGNING_SECRET", "").strip()


def configured_app_id() -> str:
    return os.environ.get("FEISHU_APP_ID", "").strip()


def configured_app_secret() -> str:
    return os.environ.get("FEISHU_APP_SECRET", "").strip()


def tenant_status() -> dict[str, Any]:
    verification_token_ready = bool(configured_verification_token())
    signing_secret_ready = bool(configured_signing_secret())
    app_id_ready = bool(configured_app_id())
    app_secret_ready = bool(configured_app_secret())
    webhook_ready = verification_token_ready
    reply_ready = app_id_ready and app_secret_ready
    signing_mode = "verification+signature" if signing_secret_ready else "verification_only"
    next_steps: list[str] = []
    if not verification_token_ready:
        next_steps.append("Set FEISHU_VERIFICATION_TOKEN before accepting real webhook events.")
    if not app_id_ready or not app_secret_ready:
        next_steps.append("Set FEISHU_APP_ID and FEISHU_APP_SECRET before enabling real reply send.")
    if webhook_ready and not reply_ready:
        next_steps.append("Webhook preview mode is ready; real replies remain blocked until app credentials are configured.")
    if webhook_ready and reply_ready:
        next_steps.append("Run tenant-smoke locally, then verify the same contract against the real Feishu webhook entry.")
    return {
        "ok": True,
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "workspace_root": str(workspace_root()),
        "broker_available": broker_available(),
        "verification_mode": signing_mode,
        "webhook_ready": webhook_ready,
        "reply_ready": reply_ready,
        "send_reply_ready": webhook_ready and reply_ready,
        "env": {
            "FEISHU_VERIFICATION_TOKEN": verification_token_ready,
            "FEISHU_SIGNING_SECRET": signing_secret_ready,
            "FEISHU_APP_ID": app_id_ready,
            "FEISHU_APP_SECRET": app_secret_ready,
        },
        "next_steps": next_steps,
    }


def build_webhook_headers(payload: dict[str, Any]) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    secret = configured_signing_secret()
    if not secret:
        return headers
    timestamp = str(int(dt.datetime.now(dt.timezone.utc).timestamp()))
    nonce = "workspace-hub-feishu-smoke"
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    digest = hmac.new(secret.encode("utf-8"), f"{timestamp}{nonce}{body}".encode("utf-8"), hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode("utf-8")
    headers.update(
        {
            "X-Lark-Request-Timestamp": timestamp,
            "X-Lark-Request-Nonce": nonce,
            "X-Lark-Signature": signature,
        }
    )
    return headers


def build_tenant_smoke_payload(*, text: str, project_name: str = "", session_id: str = "", message_id: str = "") -> dict[str, Any]:
    payload = {
        "schema": "2.0",
        "token": configured_verification_token(),
        "header": {"event_type": "im.message.receive_v1", "event_id": message_id or f"evt-{iso_now()}"},
        "event": {
            "message": {
                "message_id": message_id or f"msg-{iso_now()}",
                "content": json.dumps({"text": text}, ensure_ascii=False),
            }
        },
    }
    if project_name or session_id:
        payload["event"]["context"] = {}
        if project_name:
            payload["event"]["context"]["project_name"] = project_name
        if session_id:
            payload["event"]["context"]["session_id"] = session_id
    return payload


def tenant_smoke(*, text: str, project_name: str = "", session_id: str = "", send_reply: bool = False) -> dict[str, Any]:
    payload = build_tenant_smoke_payload(text=text, project_name=project_name, session_id=session_id)
    headers = build_webhook_headers(payload)
    status = tenant_status()
    route_result = route_webhook_event(payload, headers=headers, send_reply=send_reply)
    return {
        "ok": bool(route_result.get("ok")),
        "bridge": BRIDGE_NAME,
        "tenant_status": status,
        "payload_preview": payload,
        "headers_preview": headers,
        "route_result": route_result,
        "reason": route_result.get("reason", ""),
        "error_type": route_result.get("error_type", ""),
    }


def tenant_credentials_status() -> dict[str, Any]:
    status = tenant_status()
    if not status["reply_ready"]:
        return {
            "ok": False,
            "bridge": BRIDGE_NAME,
            "schema_version": SCHEMA_VERSION,
            "tenant_status": status,
            "reason": "reply_not_ready",
            "error_type": "reply_not_ready",
            "detail": "Webhook preview is ready but reply credentials are missing or incomplete.",
        }
    app_id = configured_app_id()
    app_secret = configured_app_secret()
    token_result = fetch_tenant_access_token(app_id=app_id, app_secret=app_secret)
    payload = token_result.get("payload", {}) if isinstance(token_result.get("payload"), dict) else {}
    tenant_access_token = _strip_text(payload.get("tenant_access_token", ""))
    ok = bool(token_result.get("ok") and tenant_access_token)
    detail = "" if ok else payload.get("error", token_result.get("reason") or token_result.get("error_type") or "tenant_access_token_failed")
    return {
        "ok": ok,
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "tenant_status": status,
        "tenant_token_result": token_result,
        "tenant_access_token": tenant_access_token,
        "reason": detail or ("tenant_access_token_failed" if not ok else ""),
        "error_type": token_result.get("error_type", "tenant_access_token_failed") if not ok else "",
    }


def readiness_summary() -> dict[str, Any]:
    tenant = tenant_status()
    credentials = tenant_credentials_status()
    bitable = feishu_projection.bitable_target_status()
    ready = credentials.get("ok", False) and bitable.get("ok", False)
    return {
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "ok": ready,
        "tenant_status": tenant,
        "tenant_credentials": credentials,
        "bitable_target_status": bitable,
        "mode": "fs-05-fs-06-ready",
    }


def verify_webhook_request(payload: dict[str, Any], headers: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = headers or {}
    expected_token = configured_verification_token()
    incoming_token = webhook_token(payload)
    if not expected_token:
        return {
            "ok": False,
            "mode": "verification_token",
            "reason": "verification_not_configured",
            "error_type": "verification_not_configured",
        }
    if incoming_token != expected_token:
        return {
            "ok": False,
            "mode": "verification_token",
            "reason": "verification_failed",
            "error_type": "verification_failed",
        }

    secret = configured_signing_secret()
    timestamp = header_value(headers, "X-Lark-Request-Timestamp")
    nonce = header_value(headers, "X-Lark-Request-Nonce")
    signature = header_value(headers, "X-Lark-Signature")
    if secret:
        if not timestamp or not nonce or not signature:
            return {
                "ok": False,
                "mode": "signature",
                "reason": "signature_headers_missing",
                "error_type": "signature_headers_missing",
            }
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        content = f"{timestamp}{nonce}{body}".encode("utf-8")
        digest = hmac.new(secret.encode("utf-8"), content, hashlib.sha256).digest()
        expected_signature = base64.b64encode(digest).decode("utf-8")
        if not hmac.compare_digest(signature, expected_signature):
            return {
                "ok": False,
                "mode": "signature",
                "reason": "signature_failed",
                "error_type": "signature_failed",
            }
        return {
            "ok": True,
            "mode": "signature",
            "reason": "",
            "error_type": "",
        }
    return {
        "ok": True,
        "mode": "verification_token",
        "reason": "",
        "error_type": "",
    }


def reply_text_from_route_result(route_result: dict[str, Any]) -> str:
    if route_result.get("kind") == "url_verification":
        return "URL verification acknowledged."
    if not route_result.get("ok"):
        reason = _strip_text(route_result.get("reason", "")) or _strip_text(route_result.get("error_type", "")) or "unknown_error"
        return f"请求未执行：{reason}"

    command = _strip_text(route_result.get("command", ""))
    broker_payload = route_result.get("broker_payload", {})
    if command == "projects":
        count = len((broker_payload or {}).get("projects", []))
        project_name = _strip_text(route_result.get("project_name", ""))
        suffix = f"（{project_name}）" if project_name else ""
        return f"已获取项目视图{suffix}，共 {count} 条。"
    if command == "review-inbox":
        count = len((broker_payload or {}).get("items", []))
        return f"已获取 review 视图，共 {count} 条。"
    if command == "coordination-inbox":
        count = len((broker_payload or {}).get("items", []))
        return f"已获取 coordination 视图，共 {count} 条。"
    if command == "health":
        payload = (broker_payload or {}).get("payload", {}) if isinstance((broker_payload or {}).get("payload", {}), dict) else {}
        open_alert_count = payload.get("open_alert_count", 0)
        issue_count = ((payload.get("last_entry", {}) if isinstance(payload.get("last_entry", {}), dict) else {}).get("issue_count", 0))
        return f"健康巡检已查询：issue_count={issue_count}，open_alert_count={open_alert_count}。"
    if command == "codex-resume":
        session_id = _strip_text(route_result.get("session_id", ""))
        suffix = f" `{session_id}`" if session_id else ""
        return f"已请求恢复会话{suffix}。"
    if command == "codex-exec":
        return "已接收消息并转发给 Codex 执行。"
    return f"已执行 `{command}`。"


def _http_json(url: str, *, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    ssl_context = _ssl_context()
    try:
        with urllib.request.urlopen(request, timeout=15, context=ssl_context) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body) if body.strip() else {}
            return {"ok": True, "status": getattr(response, "status", 200), "payload": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8") if exc.fp else ""
        parsed = json.loads(body) if body.strip().startswith("{") else {"raw": body}
        return {"ok": False, "status": exc.code, "payload": parsed, "reason": "http_error"}
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "status": 0, "payload": {}, "reason": str(exc)}


def _ssl_context() -> ssl.SSLContext | None:
    if certifi is None:
        return None
    cafile = certifi.where()
    if not cafile:
        return None
    return ssl.create_default_context(cafile=cafile)


def fetch_tenant_access_token(*, app_id: str, app_secret: str) -> dict[str, Any]:
    return _http_json(
        f"{FEISHU_OPEN_BASE}/auth/v3/tenant_access_token/internal",
        payload={"app_id": app_id, "app_secret": app_secret},
        headers={"Content-Type": "application/json; charset=utf-8"},
    )


def send_reply_message(*, message_id: str, text: str) -> dict[str, Any]:
    app_id = configured_app_id()
    app_secret = configured_app_secret()
    if not app_id or not app_secret:
        return {
            "ok": False,
            "reason": "reply_credentials_missing",
            "error_type": "reply_credentials_missing",
        }
    token_result = fetch_tenant_access_token(app_id=app_id, app_secret=app_secret)
    token_payload = token_result.get("payload", {})
    tenant_access_token = _strip_text(token_payload.get("tenant_access_token", ""))
    if not token_result.get("ok") or not tenant_access_token:
        return {
            "ok": False,
            "reason": "tenant_access_token_failed",
            "error_type": "tenant_access_token_failed",
            "token_result": token_result,
        }
    reply_result = _http_json(
        f"{FEISHU_OPEN_BASE}/im/v1/messages/{urllib.parse.quote(message_id, safe='')}/reply",
        payload={
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        },
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {tenant_access_token}",
        },
    )
    if not reply_result.get("ok"):
        return {
            "ok": False,
            "reason": "reply_send_failed",
            "error_type": "reply_send_failed",
            "reply_result": reply_result,
        }
    return {
        "ok": True,
        "reason": "",
        "error_type": "",
        "reply_result": reply_result,
    }


def http_response_from_route(route_result: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    request = route_result.get("request", {}) if isinstance(route_result.get("request"), dict) else {}
    route_payload = route_result.get("route_result", {}) if isinstance(route_result.get("route_result"), dict) else {}
    if request.get("payload_schema") == "feishu-url-verification" or route_payload.get("kind") == "url_verification":
        return 200, {"challenge": route_payload.get("challenge") or route_result.get("challenge", "")}
    if not route_result.get("ok", False):
        return 400, {
            "ok": False,
            "reason": route_result.get("reason", ""),
            "error_type": route_result.get("error_type", ""),
            "reply_text": route_result.get("reply_text", ""),
        }
    reply_result = route_result.get("reply_result", {}) if isinstance(route_result.get("reply_result"), dict) else {}
    route_payload = route_result.get("route_result", {}) if isinstance(route_result.get("route_result"), dict) else {}
    return 200, {
        "ok": True,
        "message_id": route_result.get("message_id", ""),
        "command": route_payload.get("command", ""),
        "project_name": route_payload.get("project_name", ""),
        "reply_status": reply_result.get("status", ""),
        "reply_text": route_result.get("reply_text", ""),
    }


def make_webhook_server(*, host: str = "127.0.0.1", port: int = 8710, send_reply: bool = False) -> http.server.ThreadingHTTPServer:
    class Handler(http.server.BaseHTTPRequestHandler):
        server_version = "WorkspaceHubFeishu/1.0"

        def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover
            return

        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            raw_body = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self._send_json(
                    400,
                    {
                        "ok": False,
                        "reason": "invalid_json",
                        "error_type": "invalid_json",
                    },
                )
                return
            headers = {key: value for key, value in self.headers.items()}
            route_result = route_webhook_event(payload, headers=headers, send_reply=send_reply)
            status_code, response_payload = http_response_from_route(route_result)
            self._send_json(status_code, response_payload)

    return http.server.ThreadingHTTPServer((host, port), Handler)


def serve_webhook(*, host: str = "127.0.0.1", port: int = 8710, send_reply: bool = False, once: bool = False) -> dict[str, Any]:
    server = make_webhook_server(host=host, port=port, send_reply=send_reply)
    actual_host, actual_port = server.server_address
    summary = {
        "ok": True,
        "bridge": BRIDGE_NAME,
        "host": actual_host,
        "port": actual_port,
        "send_reply": send_reply,
        "once": once,
        "mode": "local_webhook_server",
    }
    if once:
        worker = threading.Thread(target=server.handle_request, daemon=True)
        worker.start()
        worker.join(timeout=30)
        server.server_close()
        if worker.is_alive():
            return {
                **summary,
                "ok": False,
                "reason": "request_timeout",
                "error_type": "request_timeout",
            }
        return summary
    print(json.dumps(summary, ensure_ascii=False), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        server.server_close()
    return summary


def _run(command: list[str]) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=str(workspace_root()),
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


def call_broker(command: str, *, args: list[str] | None = None) -> dict[str, Any]:
    args = args or []
    if not broker_available():
        return {
            "ok": False,
            "reason": "broker_unavailable",
            "error_type": "broker_unavailable",
            "command": command,
            "stdout": "",
            "stderr": "",
            "returncode": None,
        }
    result = _run(["python3", str(broker_path()), command, *args])
    payload: dict[str, Any]
    invalid_json = False
    try:
        payload = json.loads(result["stdout"]) if result["stdout"].strip() else {}
    except json.JSONDecodeError:
        invalid_json = True
        payload = {"stdout": result["stdout"], "stderr": result["stderr"]}
    ok = result["returncode"] == 0 and not invalid_json
    error_type = ""
    if invalid_json:
        error_type = "invalid_broker_payload"
    elif result["returncode"] != 0:
        error_type = "broker_returncode"
    payload.update(
        {
            "ok": ok,
            "returncode": result["returncode"],
            "stderr": result["stderr"],
            "reason": payload.get("reason", error_type),
            "error_type": payload.get("error_type", error_type),
            "broker_command": command,
        }
    )
    return payload


def record_inbound(
    *,
    message_id: str,
    payload: dict[str, Any],
    status: str,
    project_name: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    runtime_state.init_db()
    return runtime_state.upsert_bridge_message(
        bridge=BRIDGE_NAME,
        direction="inbound",
        message_id=message_id,
        status=status,
        payload=payload,
        project_name=project_name,
        session_id=session_id,
    )


def record_delivery(
    *,
    delivery_key: str,
    target_ref: str,
    status: str,
    payload: dict[str, Any] | None = None,
    channel: str = "chat",
) -> dict[str, Any]:
    runtime_state.init_db()
    return runtime_state.upsert_delivery_status(
        delivery_key=delivery_key,
        bridge=BRIDGE_NAME,
        status=status,
        channel=channel,
        target_ref=target_ref,
        payload=payload or {},
    )


def classify_payload(payload: dict[str, Any]) -> dict[str, Any]:
    message_id = inbound_message_id(payload) or f"feishu-{iso_now()}"
    return {
        "schema_version": SCHEMA_VERSION,
        "payload_schema": detect_payload_schema(payload),
        "event_type": detect_event_type(payload),
        "message_id": message_id,
        "project_name": detect_project_name(payload),
        "session_id": detect_session_id(payload),
        "text": safe_text(payload),
        "challenge": _strip_text(payload.get("challenge", "")),
    }


def broker_request_for_text(text: str, *, project_name: str, session_id: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        return {"kind": "empty"}
    command_token, _, remainder = stripped.partition(" ")
    token = command_token.lower()
    trailing = remainder.strip()
    resolved_project_name = project_name
    resolved_session_id = session_id
    if token in READ_ONLY_COMMANDS:
        broker_command = READ_ONLY_COMMANDS[token]
        args: list[str] = []
        if broker_command in PROJECT_SCOPED_COMMANDS:
            resolved_project_name = canonical_project_name(trailing) or project_name
            if resolved_project_name:
                args = ["--project-name", resolved_project_name]
        return {
            "kind": "readonly_command",
            "text": stripped,
            "broker_command": broker_command,
            "args": args,
            "project_name": resolved_project_name,
            "session_id": resolved_session_id,
            "command_token": token,
        }
    if token == "/resume":
        resolved_session_id = _strip_text(trailing) or session_id
        if not resolved_session_id:
            return {
                "kind": "invalid_command",
                "error_type": "missing_session_id",
                "reason": "missing_session_id",
                "command_token": token,
            }
        return {
            "kind": "resume_command",
            "text": stripped,
            "broker_command": "codex-resume",
            "args": ["--session-id", resolved_session_id],
            "project_name": resolved_project_name,
            "session_id": resolved_session_id,
            "command_token": token,
        }
    if token.startswith("/"):
        return {
            "kind": "invalid_command",
            "error_type": "unsupported_command",
            "reason": "unsupported_command",
            "command_token": token,
        }
    return {
        "kind": "chat",
        "text": stripped,
        "broker_command": "codex-exec",
        "args": ["--prompt", stripped],
        "project_name": resolved_project_name,
        "session_id": resolved_session_id,
        "command_token": "",
    }


def inbound_record_payload(raw_payload: dict[str, Any], request: dict[str, Any], broker_request: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request": request,
        "raw": raw_payload,
    }
    if broker_request:
        payload["broker_request"] = broker_request
    return payload


def route_chat_message(payload: dict[str, Any]) -> dict[str, Any]:
    contract = bridge_contract()
    request = classify_payload(payload)
    message_id = request["message_id"]
    project_name = request["project_name"]
    session_id = request["session_id"]
    text = request["text"]
    inbound = record_inbound(
        message_id=message_id,
        payload=inbound_record_payload(payload, request),
        status="received",
        project_name=project_name,
        session_id=session_id,
    )

    if request["payload_schema"] == "feishu-url-verification" and request["challenge"]:
        inbound = record_inbound(
            message_id=message_id,
            payload=inbound_record_payload(payload, request),
            status="challenge_only",
            project_name=project_name,
            session_id=session_id,
        )
        delivery = record_delivery(
            delivery_key=f"{message_id}:outbound",
            target_ref=message_id,
            status="challenge_only",
            payload={"challenge": request["challenge"]},
            channel="webhook",
        )
        return {
            "ok": True,
            "bridge": BRIDGE_NAME,
            "contract": contract,
            "kind": "url_verification",
            "message_id": message_id,
            "request": request,
            "inbound_record": inbound,
            "delivery_record": delivery,
            "challenge": request["challenge"],
        }

    broker_request = broker_request_for_text(text, project_name=project_name, session_id=session_id)

    if broker_request["kind"] == "empty":
        inbound = record_inbound(
            message_id=message_id,
            payload=inbound_record_payload(payload, request, broker_request),
            status="ignored_empty_message",
            project_name=project_name,
            session_id=session_id,
        )
        delivery = record_delivery(
            delivery_key=f"{message_id}:outbound",
            target_ref=message_id,
            status="ignored_empty_message",
            payload={"reason": "empty_text"},
        )
        return {
            "ok": False,
            "bridge": BRIDGE_NAME,
            "contract": contract,
            "message_id": message_id,
            "request": request,
            "broker_request": broker_request,
            "inbound_record": inbound,
            "delivery_record": delivery,
            "reason": "empty_text",
            "error_type": "empty_text",
        }

    if broker_request["kind"] == "invalid_command":
        inbound = record_inbound(
            message_id=message_id,
            payload=inbound_record_payload(payload, request, broker_request),
            status="ignored_unsupported_command",
            project_name=project_name,
            session_id=session_id,
        )
        delivery = record_delivery(
            delivery_key=f"{message_id}:outbound",
            target_ref=message_id,
            status="ignored_unsupported_command",
            payload={"reason": broker_request["reason"], "error_type": broker_request["error_type"]},
        )
        return {
            "ok": False,
            "bridge": BRIDGE_NAME,
            "contract": contract,
            "message_id": message_id,
            "request": request,
            "broker_request": broker_request,
            "inbound_record": inbound,
            "delivery_record": delivery,
            "reason": broker_request["reason"],
            "error_type": broker_request["error_type"],
        }

    project_name = broker_request.get("project_name", project_name)
    session_id = broker_request.get("session_id", session_id)
    request["project_name"] = project_name
    request["session_id"] = session_id

    if not broker_available():
        inbound = record_inbound(
            message_id=message_id,
            payload=inbound_record_payload(payload, request, broker_request),
            status="blocked_no_broker",
            project_name=project_name,
            session_id=session_id,
        )
        delivery = record_delivery(
            delivery_key=f"{message_id}:outbound",
            target_ref=message_id,
            status="blocked_no_broker",
            payload={"reason": "broker_unavailable", "error_type": "broker_unavailable"},
        )
        return {
            "ok": False,
            "bridge": BRIDGE_NAME,
            "contract": contract,
            "message_id": message_id,
            "request": request,
            "broker_request": broker_request,
            "inbound_record": inbound,
            "delivery_record": delivery,
            "reason": "broker_unavailable",
            "error_type": "broker_unavailable",
        }

    command = broker_request["broker_command"]
    broker_args = broker_request["args"]
    broker_payload = call_broker(command, args=broker_args)
    inbound_status = "accepted" if broker_payload.get("ok") else "broker_failed"
    inbound = record_inbound(
        message_id=message_id,
        payload=inbound_record_payload(payload, request, broker_request),
        status=inbound_status,
        project_name=project_name,
        session_id=session_id,
    )
    delivery_status = "accepted" if broker_payload.get("ok") else "broker_failed"
    delivery = record_delivery(
        delivery_key=f"{message_id}:outbound",
        target_ref=message_id,
        status=delivery_status,
        payload={
            "broker_command": command,
            "broker_ok": broker_payload.get("ok", False),
            "broker_reason": broker_payload.get("reason", ""),
            "error_type": broker_payload.get("error_type", ""),
            "broker_returncode": broker_payload.get("returncode"),
        },
    )
    return {
        "ok": bool(broker_payload.get("ok")),
        "bridge": BRIDGE_NAME,
        "contract": contract,
        "message_id": message_id,
        "project_name": project_name,
        "session_id": session_id,
        "command": command,
        "request": request,
        "broker_request": broker_request,
        "inbound_record": inbound,
        "delivery_record": delivery,
        "broker_payload": broker_payload,
        "reason": broker_payload.get("reason", ""),
        "error_type": broker_payload.get("error_type", ""),
    }


def route_webhook_event(payload: dict[str, Any], *, headers: dict[str, Any] | None = None, send_reply: bool = False) -> dict[str, Any]:
    headers = headers or {}
    request = classify_payload(payload)
    message_id = request["message_id"]
    project_name = request["project_name"]
    session_id = request["session_id"]
    verification = verify_webhook_request(payload, headers=headers)
    if not verification["ok"]:
        inbound = record_inbound(
            message_id=message_id,
            payload={"schema_version": SCHEMA_VERSION, "request": request, "verification": verification, "raw": payload},
            status="blocked_invalid_webhook",
            project_name=project_name,
            session_id=session_id,
        )
        delivery = record_delivery(
            delivery_key=f"{message_id}:reply",
            target_ref=message_id,
            status="blocked_invalid_webhook",
            payload=verification,
            channel="reply",
        )
        return {
            "ok": False,
            "bridge": BRIDGE_NAME,
            "message_id": message_id,
            "request": request,
            "verification": verification,
            "inbound_record": inbound,
            "delivery_record": delivery,
            "reason": verification["reason"],
            "error_type": verification["error_type"],
        }

    route_result = route_chat_message(payload)
    reply_text = reply_text_from_route_result(route_result)
    reply_result: dict[str, Any]
    if request["payload_schema"] == "feishu-url-verification":
        reply_result = {
            "status": "challenge_only",
            "challenge": request["challenge"],
            "payload": {"challenge": request["challenge"]},
        }
    elif not send_reply:
        reply_record = record_delivery(
            delivery_key=f"{message_id}:reply",
            target_ref=message_id,
            status="reply_preview",
            payload={"reply_text": reply_text},
            channel="reply",
        )
        reply_result = {
            "status": "preview_only",
            "payload": {
                "msg_type": "text",
                "content": {"text": reply_text},
            },
            "delivery_record": reply_record,
        }
    else:
        send_result = send_reply_message(message_id=message_id, text=reply_text)
        reply_status = "reply_sent" if send_result.get("ok") else "reply_failed"
        reply_record = record_delivery(
            delivery_key=f"{message_id}:reply",
            target_ref=message_id,
            status=reply_status,
            payload={"reply_text": reply_text, "send_result": send_result},
            channel="reply",
        )
        reply_result = {
            "status": "sent" if send_result.get("ok") else "failed",
            "payload": {
                "msg_type": "text",
                "content": {"text": reply_text},
            },
            "send_result": send_result,
            "delivery_record": reply_record,
        }

    return {
        "ok": bool(route_result.get("ok")),
        "bridge": BRIDGE_NAME,
        "message_id": message_id,
        "request": request,
        "verification": verification,
        "route_result": route_result,
        "reply_result": reply_result,
        "reply_text": reply_text,
        "reason": route_result.get("reason", ""),
        "error_type": route_result.get("error_type", ""),
    }


def cmd_receive_chat(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid payload_json: {exc}"}), file=sys.stderr)
        return 1
    print(json.dumps(route_chat_message(payload), ensure_ascii=False))
    return 0


def cmd_webhook_event(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload_json)
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid payload_json: {exc}"}), file=sys.stderr)
        return 1
    try:
        headers = json.loads(args.headers_json) if args.headers_json else {}
    except json.JSONDecodeError as exc:
        print(json.dumps({"error": f"invalid headers_json: {exc}"}), file=sys.stderr)
        return 1
    print(json.dumps(route_webhook_event(payload, headers=headers, send_reply=args.send_reply), ensure_ascii=False))
    return 0


def cmd_delivery_status(args: argparse.Namespace) -> int:
    payload = json.loads(args.payload_json) if args.payload_json else {}
    record = record_delivery(
        delivery_key=args.delivery_key,
        target_ref=args.target_ref,
        status=args.status,
        payload=payload,
        channel=args.channel,
    )
    print(json.dumps({"ok": True, "record": record}, ensure_ascii=False))
    return 0


def cmd_broker_status(_args: argparse.Namespace) -> int:
    payload = call_broker("status")
    payload["bridge_contract"] = bridge_contract()
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_contract_status(_args: argparse.Namespace) -> int:
    print(json.dumps({"ok": True, "bridge_contract": bridge_contract()}, ensure_ascii=False))
    return 0


def cmd_tenant_status(_args: argparse.Namespace) -> int:
    print(json.dumps(tenant_status(), ensure_ascii=False))
    return 0


def cmd_tenant_smoke(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            tenant_smoke(
                text=args.text,
                project_name=args.project_name,
                session_id=args.session_id,
                send_reply=args.send_reply,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def cmd_tenant_credentials(_args: argparse.Namespace) -> int:
    print(json.dumps(tenant_credentials_status(), ensure_ascii=False))
    return 0


def cmd_readiness(_args: argparse.Namespace) -> int:
    print(json.dumps(readiness_summary(), ensure_ascii=False))
    return 0


def cmd_serve_webhook(args: argparse.Namespace) -> int:
    result = serve_webhook(
        host=args.host,
        port=args.port,
        send_reply=args.send_reply,
        once=args.once,
    )
    if args.once:
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get("ok") else 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Feishu chat bridge skeleton for workspace-hub.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    receive = subparsers.add_parser("receive-chat")
    receive.add_argument("--payload-json", required=True)
    receive.set_defaults(func=cmd_receive_chat)

    webhook = subparsers.add_parser("webhook-event")
    webhook.add_argument("--payload-json", required=True)
    webhook.add_argument("--headers-json", default="{}")
    webhook.add_argument("--send-reply", action="store_true")
    webhook.set_defaults(func=cmd_webhook_event)

    delivery = subparsers.add_parser("delivery-status")
    delivery.add_argument("--delivery-key", required=True)
    delivery.add_argument("--target-ref", required=True)
    delivery.add_argument("--status", required=True)
    delivery.add_argument("--channel", default="chat")
    delivery.add_argument("--payload-json", default="{}")
    delivery.set_defaults(func=cmd_delivery_status)

    broker = subparsers.add_parser("broker-status")
    broker.set_defaults(func=cmd_broker_status)

    contract = subparsers.add_parser("contract-status")
    contract.set_defaults(func=cmd_contract_status)

    tenant_status_cmd = subparsers.add_parser("tenant-status")
    tenant_status_cmd.set_defaults(func=cmd_tenant_status)

    tenant_smoke_cmd = subparsers.add_parser("tenant-smoke")
    tenant_smoke_cmd.add_argument("--text", default="/projects Codex Obsidian记忆与行动系统")
    tenant_smoke_cmd.add_argument("--project-name", default="")
    tenant_smoke_cmd.add_argument("--session-id", default="")
    tenant_smoke_cmd.add_argument("--send-reply", action="store_true")
    tenant_smoke_cmd.set_defaults(func=cmd_tenant_smoke)

    tenant_credentials_cmd = subparsers.add_parser("tenant-credentials")
    tenant_credentials_cmd.set_defaults(func=cmd_tenant_credentials)

    readiness_cmd = subparsers.add_parser("readiness")
    readiness_cmd.set_defaults(func=cmd_readiness)

    serve_webhook_cmd = subparsers.add_parser("serve-webhook")
    serve_webhook_cmd.add_argument("--host", default="127.0.0.1")
    serve_webhook_cmd.add_argument("--port", type=int, default=8710)
    serve_webhook_cmd.add_argument("--send-reply", action="store_true")
    serve_webhook_cmd.add_argument("--once", action="store_true")
    serve_webhook_cmd.set_defaults(func=cmd_serve_webhook)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

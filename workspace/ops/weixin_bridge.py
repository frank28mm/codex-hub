#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import random
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

try:
    from ops import codex_context, runtime_state
    from ops.codex_memory import launch_agent_loaded, launch_agent_plist_path
except ImportError:  # pragma: no cover
    import codex_context  # type: ignore
    import runtime_state  # type: ignore
    from codex_memory import launch_agent_loaded, launch_agent_plist_path  # type: ignore

try:  # pragma: no cover - optional dependency in some Python installs
    import certifi
except ImportError:  # pragma: no cover
    certifi = None  # type: ignore

try:  # pragma: no cover - optional dependency in some Python installs
    import qrcode
except ImportError:  # pragma: no cover
    qrcode = None  # type: ignore


BRIDGE_NAME = "weixin"
SCHEMA_VERSION = "weixin-bridge.v1"
DEFAULT_BASE_URL = "https://ilinkai.weixin.qq.com"
DEFAULT_BOT_TYPE = "3"
BASE_INFO_VERSION = "codex-hub-weixin-bridge/0.1"
LONG_POLL_TIMEOUT_MS = 35_000
API_TIMEOUT_MS = 15_000
CONFIG_TIMEOUT_MS = 10_000
TYPING_STATUS_START = 1
TYPING_STATUS_CANCEL = 2
LAUNCH_AGENT_NAME = "com.codexhub.weixin-bridge"
DEFAULT_POLL_INTERVAL = 2
DEFAULT_ERROR_BACKOFF = 8
WEIXIN_DM_THREAD_NAME = "CoCo 私聊"
WEIXIN_DM_THREAD_LABEL = "CoCo 私聊"
WEIXIN_TEXT_CHUNK_LIMIT = 900
CONTINUE_SESSION_PATTERNS = [
    re.compile(r"^\s*继续"),
    re.compile(r"^\s*接着"),
    re.compile(r"^\s*延续"),
    re.compile(r"^\s*继续处理"),
    re.compile(r"^\s*继续刚才"),
    re.compile(r"^\s*回到刚才"),
    re.compile(r"^\s*刚才"),
    re.compile(r"^\s*上一[个条轮]"),
]
HIGH_RISK_PATTERNS = [
    re.compile(r"\bgit\s+push\b", re.I),
    re.compile(r"\bgh\s+pr\b", re.I),
    re.compile(r"\bssh\b", re.I),
    re.compile(r"\bscp\b", re.I),
    re.compile(r"\brsync\b", re.I),
    re.compile(r"\bdeploy\b", re.I),
    re.compile(r"\bpublish\b", re.I),
    re.compile(r"\bmerge\s+pr\b", re.I),
    re.compile(r"推(?:到|送到|上)?\s*github", re.I),
    re.compile(r"发布生产"),
    re.compile(r"线上部署"),
    re.compile(r"LaunchAgents", re.I),
    re.compile(r"\blaunchctl\b", re.I),
]


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()


def broker_path() -> Path:
    return workspace_root() / "ops" / "local_broker.py"


def runtime_dir() -> Path:
    return runtime_state.runtime_root() / "weixin"


def account_store_path() -> Path:
    return runtime_dir() / "account.json"


def login_session_path() -> Path:
    return runtime_dir() / "login_session.json"


def login_qr_image_path() -> Path:
    return runtime_dir() / "login_qr.png"


def bridge_state_path() -> Path:
    return runtime_dir() / "bridge_state.json"


def log_stdout_path() -> Path:
    return workspace_root() / "logs" / "weixin-bridge.log"


def log_stderr_path() -> Path:
    return workspace_root() / "logs" / "weixin-bridge.err.log"


def bridge_contract() -> dict[str, Any]:
    runtime_contract = runtime_state.feishu_runtime_contract()
    return {
        "bridge": BRIDGE_NAME,
        "schema_version": SCHEMA_VERSION,
        "entry_mode": "python_dm_long_poll",
        "host_mode": "python",
        "transport": "http_json_long_poll",
        "chat_types": ["direct"],
        "truth_source": runtime_contract["truth_source"],
        "allowed_write_tables": runtime_contract["writable_tables"],
        "reserved_tables": runtime_contract["reserved_tables"],
        "read_only_tables": runtime_contract["read_only_tables"],
        "forbidden_capabilities": [
            "group_chat",
            "approval_cards",
            "bitable_projection",
            "doc_mirror",
            "direct_vault_writes",
        ],
    }


def _ensure_runtime_dir() -> None:
    runtime_dir().mkdir(parents=True, exist_ok=True)


def _bridge_state() -> dict[str, Any]:
    payload = _read_json(
        bridge_state_path(),
        {
            "version": 1,
            "mode": "",
            "last_loop_started_at": "",
            "last_loop_finished_at": "",
            "last_success_at": "",
            "last_error_at": "",
            "last_error": "",
            "last_message_count": 0,
            "consecutive_failures": 0,
            "updated_at": "",
        },
    )
    return payload if isinstance(payload, dict) else {}


def _save_bridge_state(updates: dict[str, Any]) -> dict[str, Any]:
    current = _bridge_state()
    current.update(updates)
    current["updated_at"] = runtime_state.iso_now()
    _write_json(bridge_state_path(), current)
    return current


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    _ensure_runtime_dir()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _render_login_qr_png(content: str) -> str:
    if qrcode is None:
        return ""
    text = str(content or "").strip()
    if not text:
        return ""
    _ensure_runtime_dir()
    image = qrcode.make(text)
    path = login_qr_image_path()
    image.save(path)
    return str(path)


def load_account() -> dict[str, Any]:
    payload = _read_json(account_store_path(), {})
    return payload if isinstance(payload, dict) else {}


def save_account(payload: dict[str, Any]) -> dict[str, Any]:
    existing = load_account()
    merged = {**existing, **payload}
    merged["saved_at"] = runtime_state.iso_now()
    _write_json(account_store_path(), merged)
    return merged


def load_login_session() -> dict[str, Any]:
    payload = _read_json(login_session_path(), {})
    return payload if isinstance(payload, dict) else {}


def save_login_session(payload: dict[str, Any]) -> dict[str, Any]:
    _write_json(login_session_path(), payload)
    return payload


def clear_login_session() -> None:
    try:
        login_session_path().unlink()
    except FileNotFoundError:
        return
    try:
        login_qr_image_path().unlink()
    except FileNotFoundError:
        return


def _base_info() -> dict[str, str]:
    return {"channel_version": BASE_INFO_VERSION}


def _ensure_trailing_slash(url: str) -> str:
    return url if url.endswith("/") else f"{url}/"


def _random_wechat_uin() -> str:
    value = str(random.randint(1, 2**32 - 1)).encode("utf-8")
    return base64.b64encode(value).decode("utf-8")


def _request_json(
    *,
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    token: str = "",
    timeout: float = API_TIMEOUT_MS / 1000,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    body = b""
    headers = {"Content-Type": "application/json", "X-WECHAT-UIN": _random_wechat_uin()}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["AuthorizationType"] = "ilink_bot_token"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if extra_headers:
        headers.update({key: value for key, value in extra_headers.items() if value})
    request = urllib.request.Request(url, data=body or None, method=method.upper())
    for key, value in headers.items():
        request.add_header(key, value)
    ssl_context = _ssl_context()
    with urllib.request.urlopen(request, timeout=timeout, context=ssl_context) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw) if raw else {}


def _ssl_context() -> ssl.SSLContext | None:
    if certifi is None:
        return None
    cafile = certifi.where()
    if not cafile:
        return None
    return ssl.create_default_context(cafile=cafile)


def _api_post(
    *,
    base_url: str,
    endpoint: str,
    payload: dict[str, Any],
    token: str = "",
    timeout_ms: int = API_TIMEOUT_MS,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    url = urllib.parse.urljoin(_ensure_trailing_slash(base_url), endpoint)
    return _request_json(
        method="POST",
        url=url,
        payload={**payload, "base_info": _base_info()},
        token=token,
        timeout=timeout_ms / 1000,
        extra_headers=extra_headers,
    )


def plist_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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


def run_launchctl(*parts: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["launchctl", *parts], text=True, capture_output=True, check=False)


def default_env() -> dict[str, str]:
    path_parts = [
        str(Path.home() / "Library" / "Python" / f"{sys.version_info.major}.{sys.version_info.minor}" / "bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    current = os.environ.get("PATH", "").strip()
    if current:
        path_parts.append(current)
    return {"PATH": ":".join(dict.fromkeys(part for part in path_parts if part)), "PYTHONUNBUFFERED": "1"}


def launch_agent_payload(*, poll_interval: int, error_backoff: int) -> dict[str, Any]:
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "Label": LAUNCH_AGENT_NAME,
        "ProgramArguments": [
            python_path,
            str(workspace_root() / "ops" / "weixin_bridge.py"),
            "daemon",
            "--poll-interval",
            str(int(poll_interval)),
            "--error-backoff",
            str(int(error_backoff)),
        ],
        "RunAtLoad": True,
        "KeepAlive": True,
        "WorkingDirectory": str(workspace_root()),
        "StandardOutPath": str(log_stdout_path()),
        "StandardErrorPath": str(log_stderr_path()),
        "EnvironmentVariables": default_env(),
    }


def start_login_qr(*, base_url: str = DEFAULT_BASE_URL, bot_type: str = DEFAULT_BOT_TYPE, account_id: str = "default") -> dict[str, Any]:
    url = urllib.parse.urljoin(
        _ensure_trailing_slash(base_url),
        f"ilink/bot/get_bot_qrcode?bot_type={urllib.parse.quote(bot_type)}",
    )
    payload = _request_json(method="GET", url=url, timeout=10)
    session = {
        "session_key": str(uuid.uuid4()),
        "account_id": account_id,
        "base_url": base_url,
        "bot_type": bot_type,
        "qrcode": str(payload.get("qrcode", "")).strip(),
        "qrcode_url": str(payload.get("qrcode_img_content", "")).strip(),
        "started_at": runtime_state.iso_now(),
    }
    qr_image_path = _render_login_qr_png(session["qrcode_url"])
    if qr_image_path:
        session["qrcode_image_path"] = qr_image_path
    save_login_session(session)
    runtime_state.upsert_bridge_settings(
        BRIDGE_NAME,
        {
            "base_url": base_url,
            "bot_type": bot_type,
            "account_id": account_id,
            "login_session_key": session["session_key"],
        },
    )
    runtime_state.upsert_bridge_connection(
        BRIDGE_NAME,
        status="setup",
        host_mode="python",
        transport="http_json_long_poll",
        metadata={"account_id": account_id, "login_session_key": session["session_key"]},
    )
    return session


def _poll_qr_status(*, base_url: str, qrcode: str) -> dict[str, Any]:
    url = urllib.parse.urljoin(
        _ensure_trailing_slash(base_url),
        f"ilink/bot/get_qrcode_status?qrcode={urllib.parse.quote(qrcode)}",
    )
    return _request_json(
        method="GET",
        url=url,
        timeout=LONG_POLL_TIMEOUT_MS / 1000,
        extra_headers={"iLink-App-ClientVersion": "1"},
    )


def wait_for_login(*, timeout_seconds: int = 180) -> dict[str, Any]:
    session = load_login_session()
    if not session.get("qrcode"):
        return {"connected": False, "error": "login_session_missing"}
    deadline = time.time() + max(1, timeout_seconds)
    while time.time() < deadline:
        status_payload = _poll_qr_status(base_url=session["base_url"], qrcode=session["qrcode"])
        status = str(status_payload.get("status", "")).strip() or "wait"
        if status == "confirmed":
            account = save_account(
                {
                    "account_id": session.get("account_id", "default"),
                    "token": str(status_payload.get("bot_token", "")).strip(),
                    "base_url": str(status_payload.get("baseurl", "")).strip() or session["base_url"],
                    "user_id": str(status_payload.get("ilink_user_id", "")).strip(),
                    "bot_id": str(status_payload.get("ilink_bot_id", "")).strip(),
                    "bot_type": session.get("bot_type", DEFAULT_BOT_TYPE),
                    "get_updates_buf": "",
                }
            )
            clear_login_session()
            runtime_state.upsert_bridge_connection(
                BRIDGE_NAME,
                status="connected",
                host_mode="python",
                transport="http_json_long_poll",
                last_event_at=runtime_state.iso_now(),
                metadata={"account_id": account.get("account_id", "default"), "user_id": account.get("user_id", "")},
            )
            return {"connected": True, "account": account, "status": status}
        if status == "expired":
            runtime_state.upsert_bridge_connection(
                BRIDGE_NAME,
                status="setup",
                host_mode="python",
                transport="http_json_long_poll",
                last_error="qrcode_expired",
                metadata={"account_id": session.get("account_id", "default")},
            )
            return {"connected": False, "status": status, "error": "qrcode_expired"}
        time.sleep(1.0)
    return {"connected": False, "status": "wait", "error": "login_timeout"}


def _account_or_error() -> tuple[dict[str, Any], dict[str, Any] | None]:
    account = load_account()
    token = str(account.get("token", "")).strip()
    base_url = str(account.get("base_url", "")).strip()
    if token and base_url:
        return account, None
    return {}, {"ok": False, "error": "weixin_account_not_configured", "bridge": BRIDGE_NAME}


def _message_analysis(message: dict[str, Any]) -> dict[str, str]:
    fallback_kind = "empty"
    for item in message.get("item_list") or []:
        if int(item.get("type", 0) or 0) == 1:
            text_item = item.get("text_item", {})
            text = str(text_item.get("text", "")).strip()
            if text:
                return {"text": text, "content_kind": "text"}
        if item.get("image_item") is not None:
            if fallback_kind == "empty":
                fallback_kind = "image"
            continue
        if int(item.get("type", 0) or 0) == 3 or item.get("voice_item") is not None:
            voice_item = item.get("voice_item", {})
            text = str(voice_item.get("text", "")).strip()
            if text:
                return {"text": text, "content_kind": "voice_text"}
            if fallback_kind == "empty":
                fallback_kind = "voice"
            continue
        if item.get("video_item") is not None:
            if fallback_kind == "empty":
                fallback_kind = "video"
            continue
        if item.get("file_item") is not None:
            if fallback_kind == "empty":
                fallback_kind = "file"
    return {"text": "", "content_kind": fallback_kind}


def _message_text(message: dict[str, Any]) -> str:
    return _message_analysis(message)["text"]


def _unsupported_input_reply(content_kind: str) -> str:
    kind = str(content_kind or "").strip()
    if kind == "image":
        return "当前微信私聊桥第一版还不能直接理解图片内容。请补一段文字说明，或等后续版本接入图片理解。"
    if kind == "voice":
        return "当前微信私聊桥第一版只支持微信侧已经转写成文字的语音。这条语音没有可用转写，请改发文字。"
    if kind == "video":
        return "当前微信私聊桥第一版还不能直接理解视频内容。请先发文字说明，或把关键信息整理成文字再发。"
    if kind == "file":
        return "当前微信私聊桥第一版还不能直接读取微信里的文件附件。请先发文字说明，或把文件放到工作区可访问位置后再让我处理。"
    return ""


def _chat_ref(account_id: str, user_id: str) -> str:
    return f"weixin:{account_id}:{user_id}"


def _message_id(message: dict[str, Any]) -> str:
    raw = str(message.get("message_id", "")).strip()
    return raw or f"weixin-{uuid.uuid4().hex[:12]}"


def _normalize_inbound(message: dict[str, Any], account: dict[str, Any]) -> dict[str, Any]:
    user_id = str(message.get("from_user_id", "")).strip()
    analysis = _message_analysis(message)
    text = analysis["text"]
    account_id = str(account.get("account_id", "default")).strip() or "default"
    chat_ref = _chat_ref(account_id, user_id)
    project_name = detect_project_name(text)
    return {
        "bridge": BRIDGE_NAME,
        "message_id": _message_id(message),
        "chat_ref": chat_ref,
        "thread_name": WEIXIN_DM_THREAD_NAME,
        "thread_label": WEIXIN_DM_THREAD_LABEL,
        "text": text,
        "content_kind": analysis["content_kind"],
        "user_id": user_id,
        "account_id": account_id,
        "context_token": str(message.get("context_token", "")).strip(),
        "project_name": project_name,
        "raw_message": message,
    }


def _registry_alias_candidates() -> list[tuple[str, str]]:
    entries = codex_context.registry_entries()
    candidates: list[tuple[str, str]] = []
    for entry in entries:
        canonical = str(entry.get("project_name", "")).strip()
        if not canonical:
            continue
        aliases = [canonical, *[str(item).strip() for item in entry.get("aliases", []) if str(item).strip()]]
        for alias in aliases:
            candidates.append((canonical, alias))
    candidates.sort(key=lambda item: len(item[1]), reverse=True)
    return candidates


def detect_project_name(text: str) -> str:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return ""
    for canonical, alias in _registry_alias_candidates():
        if alias and alias.lower() in normalized:
            return canonical
    return ""


def _broker_command(args: list[str]) -> dict[str, Any]:
    command = ["python3", str(broker_path()), *args]
    completed = subprocess.run(command, cwd=str(workspace_root()), capture_output=True, text=True, check=False)
    payload = {"ok": completed.returncode == 0, "stdout": completed.stdout, "stderr": completed.stderr, "command": command}
    if completed.stdout.strip():
        try:
            payload["response"] = json.loads(completed.stdout.strip())
        except json.JSONDecodeError:
            payload["response"] = {"raw": completed.stdout.strip()}
    return payload


def _should_block_high_risk(text: str) -> bool:
    source = str(text or "").strip()
    return any(pattern.search(source) for pattern in HIGH_RISK_PATTERNS)


def _reply_text_from_broker_result(result: dict[str, Any]) -> str:
    response = result.get("response", {}) if isinstance(result.get("response"), dict) else {}
    finalize = response.get("finalize_launch", {}) if isinstance(response.get("finalize_launch"), dict) else {}
    if finalize.get("reply_text"):
        return str(finalize.get("reply_text", "")).strip()
    if finalize.get("summary_excerpt"):
        return str(finalize.get("summary_excerpt", "")).strip()
    stdout = str(response.get("stdout", "") or response.get("raw", "") or result.get("stdout", "")).strip()
    if stdout:
        return stdout
    if response.get("ok") is True:
        return "已收到并完成处理。"
    error = str(response.get("error", "")).strip() or str(result.get("stderr", "")).strip()
    return error or "处理失败。"


def _markdown_to_weixin_text(text: str) -> str:
    source = str(text or "").strip()
    if not source:
        return ""
    result = source
    result = re.sub(r"```[^\n]*\n?([\s\S]*?)```", lambda match: match.group(1).strip(), result)
    result = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", result)
    result = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", result)
    result = re.sub(r"^\|[\s:|-]+\|$", "", result, flags=re.M)

    def _table_row(match: re.Match[str]) -> str:
        inner = match.group(1)
        cells = [cell.strip() for cell in inner.split("|")]
        return "  ".join(cell for cell in cells if cell)

    result = re.sub(r"^\|(.+)\|$", _table_row, result, flags=re.M)
    result = re.sub(r"(?m)^(#{1,6})\s*", "", result)
    result = re.sub(r"[*_~`]+", "", result)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()


def _split_reply_text(text: str, *, limit: int = WEIXIN_TEXT_CHUNK_LIMIT) -> list[str]:
    source = _markdown_to_weixin_text(text)
    if not source:
        return []
    if len(source) <= limit:
        return [source]

    chunks: list[str] = []
    current = ""
    for paragraph in source.split("\n\n"):
        part = paragraph.strip()
        if not part:
            continue
        candidate = f"{current}\n\n{part}".strip() if current else part
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(part) <= limit:
            current = part
            continue
        pieces = [piece for piece in re.split(r"(?<=[。！？!?])", part) if piece]
        for piece in pieces:
            piece = piece.strip()
            if not piece:
                continue
            candidate = f"{current}{piece}" if current else piece
            if len(candidate) <= limit:
                current = candidate
                continue
            if current:
                chunks.append(current)
                current = ""
            while len(piece) > limit:
                chunks.append(piece[:limit].rstrip())
                piece = piece[limit:].lstrip()
            current = piece
    if current:
        chunks.append(current)
    if len(chunks) <= 1:
        return chunks
    total = len(chunks)
    return [f"（{index + 1}/{total}）\n{chunk}" for index, chunk in enumerate(chunks)]


def _deliver_reply_text(
    account: dict[str, Any],
    normalized: dict[str, Any],
    *,
    reply_text: str,
    status: str,
    project_name: str,
    session_id: str,
) -> list[dict[str, Any]]:
    deliveries: list[dict[str, Any]] = []
    chunks = _split_reply_text(reply_text)
    for index, chunk in enumerate(chunks, start=1):
        delivery = send_text_message(
            account,
            user_id=normalized["user_id"],
            context_token=normalized["context_token"],
            text=chunk,
        )
        runtime_state.upsert_bridge_message(
            bridge=BRIDGE_NAME,
            direction="outbound",
            message_id=str(delivery.get("message_id", "")) or f"out-{normalized['message_id']}-{index}",
            status=status,
            payload={
                "text": chunk,
                "chat_ref": normalized["chat_ref"],
                "chunk_index": index,
                "chunk_count": len(chunks),
            },
            project_name=project_name,
            session_id=session_id,
        )
        deliveries.append(delivery)
    return deliveries


def _continuation_requested(text: str) -> bool:
    source = str(text or "").strip()
    if not source:
        return False
    return any(pattern.search(source) for pattern in CONTINUE_SESSION_PATTERNS)


def _workspace_binding_metadata(normalized: dict[str, Any]) -> dict[str, Any]:
    return {
        "thread_name": normalized["thread_name"],
        "thread_label": normalized["thread_label"],
        "account_id": normalized["account_id"],
        "user_id": normalized["user_id"],
    }


def _ensure_workspace_binding(normalized: dict[str, Any], *, session_id: str = "") -> dict[str, Any]:
    return runtime_state.upsert_bridge_chat_binding(
        bridge=BRIDGE_NAME,
        chat_ref=normalized["chat_ref"],
        binding_scope="workspace",
        project_name="",
        topic_name="",
        session_id=session_id,
        metadata=_workspace_binding_metadata(normalized),
    )


def _update_binding_from_result(
    normalized: dict[str, Any],
    result: dict[str, Any],
    *,
    explicit_project_name: str = "",
) -> dict[str, Any]:
    response = result.get("response", {}) if isinstance(result.get("response"), dict) else {}
    finalize = response.get("finalize_launch", {}) if isinstance(response.get("finalize_launch"), dict) else {}
    session_id = str(finalize.get("session_id", "")).strip()
    if explicit_project_name:
        return runtime_state.upsert_bridge_chat_binding(
            bridge=BRIDGE_NAME,
            chat_ref=normalized["chat_ref"],
            binding_scope="workspace",
            project_name="",
            topic_name="",
            session_id=session_id,
            metadata={
                **_workspace_binding_metadata(normalized),
                "last_project_name": explicit_project_name,
            },
        )
    return _ensure_workspace_binding(normalized, session_id=session_id)


def _send_typing(account: dict[str, Any], *, user_id: str, context_token: str, status: int) -> None:
    if not user_id or not context_token:
        return
    try:
        config = _api_post(
            base_url=str(account["base_url"]),
            endpoint="ilink/bot/getconfig",
            payload={"ilink_user_id": user_id, "context_token": context_token},
            token=str(account["token"]),
            timeout_ms=CONFIG_TIMEOUT_MS,
        )
        ticket = str(config.get("typing_ticket", "")).strip()
        if not ticket:
            return
        _api_post(
            base_url=str(account["base_url"]),
            endpoint="ilink/bot/sendtyping",
            payload={"ilink_user_id": user_id, "typing_ticket": ticket, "status": status},
            token=str(account["token"]),
            timeout_ms=CONFIG_TIMEOUT_MS,
        )
    except Exception:
        return


def send_text_message(account: dict[str, Any], *, user_id: str, context_token: str, text: str) -> dict[str, Any]:
    client_id = f"weixin-{uuid.uuid4().hex[:12]}"
    request_payload = {
        "msg": {
            "from_user_id": "",
            "to_user_id": user_id,
            "client_id": client_id,
            "message_type": 2,
            "message_state": 2,
            "context_token": context_token,
            "item_list": [{"type": 1, "text_item": {"text": text}}],
        }
    }
    response = _api_post(
        base_url=str(account["base_url"]),
        endpoint="ilink/bot/sendmessage",
        payload=request_payload,
        token=str(account["token"]),
    )
    return {"message_id": client_id, "result": response}


def route_private_message(message: dict[str, Any], *, send_reply: bool = True) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    normalized = _normalize_inbound(message, account)
    inbound_record = runtime_state.upsert_bridge_message(
        bridge=BRIDGE_NAME,
        direction="inbound",
        message_id=normalized["message_id"],
        status="received",
        payload=normalized,
        project_name=normalized["project_name"],
    )
    runtime_state.upsert_bridge_connection(
        BRIDGE_NAME,
        status="connected",
        host_mode="python",
        transport="http_json_long_poll",
        last_event_at=runtime_state.iso_now(),
        metadata={"account_id": normalized["account_id"], "user_id": normalized["user_id"]},
    )
    text = str(normalized["text"]).strip()
    if not text:
        reply_text = _unsupported_input_reply(str(normalized.get("content_kind", "")))
        deliveries: list[dict[str, Any]] = []
        if send_reply and normalized["context_token"] and reply_text:
            deliveries = _deliver_reply_text(
                account,
                normalized,
                reply_text=reply_text,
                status="unsupported_input",
                project_name=normalized["project_name"],
                session_id="",
            )
        return {
            "ok": False,
            "reason": "unsupported_input" if reply_text else "empty_text",
            "inbound_record": inbound_record,
            "reply_text": reply_text,
            "delivery": deliveries[0] if deliveries else {},
            "deliveries": deliveries,
        }
    if _should_block_high_risk(text):
        blocked_text = "微信私聊桥接第一版暂不执行高风险或系统级动作。请改走 Feishu 私聊 CoCo 完成授权后再执行。"
        deliveries: list[dict[str, Any]] = []
        if send_reply and normalized["context_token"]:
            deliveries = _deliver_reply_text(
                account,
                normalized,
                reply_text=blocked_text,
                status="blocked_high_risk",
                project_name=normalized["project_name"],
                session_id="",
            )
        return {
            "ok": False,
            "reason": "high_risk_not_supported",
            "reply_text": blocked_text,
            "delivery": deliveries[0] if deliveries else {},
            "deliveries": deliveries,
        }

    binding = runtime_state.fetch_bridge_chat_binding(bridge=BRIDGE_NAME, chat_ref=normalized["chat_ref"])
    if not str(binding.get("binding_scope", "")).strip():
        binding = _ensure_workspace_binding(normalized)
    explicit_project_name = str(normalized.get("project_name", "")).strip()
    continue_requested = _continuation_requested(text)
    resume_session_id = str(binding.get("session_id", "")).strip() if continue_requested else ""
    action = "codex-resume" if resume_session_id else "codex-exec"
    broker_args = [
        "command-center",
        "--action",
        action,
        "--execution-profile",
        "weixin",
        "--source",
        "weixin",
        "--chat-ref",
        normalized["chat_ref"],
        "--thread-name",
        normalized["thread_name"],
        "--thread-label",
        normalized["thread_label"],
        "--source-message-id",
        normalized["message_id"],
    ]
    if action == "codex-resume":
        broker_args.extend(["--session-id", resume_session_id])
    if explicit_project_name:
        broker_args.extend(["--project-name", explicit_project_name])
    if text:
        broker_args.extend(["--prompt", text])

    _send_typing(account, user_id=normalized["user_id"], context_token=normalized["context_token"], status=TYPING_STATUS_START)
    broker_result = _broker_command(broker_args)
    _send_typing(account, user_id=normalized["user_id"], context_token=normalized["context_token"], status=TYPING_STATUS_CANCEL)

    updated_binding = _update_binding_from_result(
        normalized,
        broker_result,
        explicit_project_name=explicit_project_name,
    )
    reply_text = _reply_text_from_broker_result(broker_result)
    deliveries: list[dict[str, Any]] = []
    if send_reply and normalized["context_token"] and reply_text:
        deliveries = _deliver_reply_text(
            account,
            normalized,
            reply_text=reply_text,
            status="sent" if broker_result.get("ok") else "error",
            project_name=str(updated_binding.get("project_name", "")).strip(),
            session_id=str(updated_binding.get("session_id", "")).strip(),
        )
    return {
        "ok": bool(broker_result.get("ok")),
        "normalized": normalized,
        "binding": updated_binding,
        "broker_result": broker_result,
        "reply_text": reply_text,
        "delivery": deliveries[0] if deliveries else {},
        "deliveries": deliveries,
    }


def run_once(*, send_reply: bool = True) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    get_updates_buf = str(account.get("get_updates_buf", "")).strip()
    response = _api_post(
        base_url=str(account["base_url"]),
        endpoint="ilink/bot/getupdates",
        payload={"get_updates_buf": get_updates_buf},
        token=str(account["token"]),
        timeout_ms=LONG_POLL_TIMEOUT_MS,
    )
    messages = response.get("msgs") or []
    if isinstance(messages, dict):
        messages = [messages]
    next_buf = str(response.get("get_updates_buf", "")).strip()
    if next_buf != get_updates_buf:
        save_account({"get_updates_buf": next_buf})
    results = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if int(message.get("message_type", 0) or 0) == 2:
            continue
        results.append(route_private_message(message, send_reply=send_reply))
    runtime_state.upsert_bridge_connection(
        BRIDGE_NAME,
        status="connected",
        host_mode="python",
        transport="http_json_long_poll",
        last_event_at=runtime_state.iso_now(),
        metadata={"account_id": account.get("account_id", "default"), "message_count": len(results)},
    )
    return {"ok": True, "bridge": BRIDGE_NAME, "message_count": len(results), "results": results}


def safe_run_once(*, send_reply: bool = True, mode: str = "manual") -> dict[str, Any]:
    started_at = runtime_state.iso_now()
    _save_bridge_state({"mode": mode, "last_loop_started_at": started_at})
    try:
        payload = run_once(send_reply=send_reply)
    except Exception as exc:  # pragma: no cover - defensive path for daemon reliability
        now = runtime_state.iso_now()
        state = _bridge_state()
        failures = int(state.get("consecutive_failures") or 0) + 1
        _save_bridge_state(
            {
                "mode": mode,
                "last_loop_finished_at": now,
                "last_error_at": now,
                "last_error": str(exc),
                "consecutive_failures": failures,
                "last_message_count": 0,
            }
        )
        runtime_state.upsert_bridge_connection(
            BRIDGE_NAME,
            status="error",
            host_mode="python",
            transport="http_json_long_poll",
            last_error=str(exc),
            metadata={"consecutive_failures": failures, "mode": mode},
        )
        return {"ok": False, "bridge": BRIDGE_NAME, "error": str(exc), "mode": mode}
    if payload.get("ok") is False:
        now = runtime_state.iso_now()
        state = _bridge_state()
        failures = int(state.get("consecutive_failures") or 0) + 1
        error_text = str(payload.get("error", "")).strip() or str(payload.get("reason", "")).strip() or "weixin_run_once_failed"
        _save_bridge_state(
            {
                "mode": mode,
                "last_loop_finished_at": now,
                "last_error_at": now,
                "last_error": error_text,
                "consecutive_failures": failures,
                "last_message_count": 0,
            }
        )
        runtime_state.upsert_bridge_connection(
            BRIDGE_NAME,
            status="setup" if error_text == "weixin_account_not_configured" else "error",
            host_mode="python",
            transport="http_json_long_poll",
            last_error=error_text,
            metadata={"consecutive_failures": failures, "mode": mode},
        )
        return payload
    now = runtime_state.iso_now()
    _save_bridge_state(
        {
            "mode": mode,
            "last_loop_finished_at": now,
            "last_success_at": now,
            "last_error_at": "",
            "last_error": "",
            "consecutive_failures": 0,
            "last_message_count": int(payload.get("message_count") or 0),
        }
    )
    return payload


def run_daemon(*, poll_interval: int = DEFAULT_POLL_INTERVAL, error_backoff: int = DEFAULT_ERROR_BACKOFF, verbose: bool = False) -> int:
    interval = max(1, int(poll_interval))
    backoff = max(interval, int(error_backoff))
    while True:
        payload = safe_run_once(mode="daemon")
        if verbose:
            print(json.dumps(payload, ensure_ascii=False), flush=True)
        if payload.get("ok"):
            time.sleep(interval)
            continue
        time.sleep(backoff)


def bridge_status() -> dict[str, Any]:
    account = load_account()
    settings = runtime_state.fetch_bridge_settings(BRIDGE_NAME)
    connection = runtime_state.fetch_bridge_connection(BRIDGE_NAME)
    state = _bridge_state()
    binding_count = len(runtime_state.fetch_bridge_chat_bindings(bridge=BRIDGE_NAME, limit=50))
    return {
        "bridge": BRIDGE_NAME,
        "contract": bridge_contract(),
        "settings": settings,
        "connection": connection,
        "installed": launch_agent_plist_path(LAUNCH_AGENT_NAME).exists(),
        "loaded": launch_agent_loaded(LAUNCH_AGENT_NAME),
        "plist": str(launch_agent_plist_path(LAUNCH_AGENT_NAME)),
        "bridge_state_path": str(bridge_state_path()),
        "log_stdout": str(log_stdout_path()),
        "log_stderr": str(log_stderr_path()),
        "configured": bool(str(account.get("token", "")).strip() and str(account.get("base_url", "")).strip()),
        "account": {
            "account_id": str(account.get("account_id", "")).strip(),
            "base_url": str(account.get("base_url", "")).strip(),
            "user_id": str(account.get("user_id", "")).strip(),
            "bot_id": str(account.get("bot_id", "")).strip(),
            "has_token": bool(str(account.get("token", "")).strip()),
            "saved_at": str(account.get("saved_at", "")).strip(),
        },
        "login_session": load_login_session(),
        "binding_count": binding_count,
        "loop_state": state,
    }


def smoke(*, text: str, user_id: str = "", dry_run: bool = False) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    if dry_run:
        return {
            "ok": True,
            "bridge": BRIDGE_NAME,
            "contract": bridge_contract(),
            "sample_payload": {
                "text": text,
                "user_id": user_id or "<wechat-user-id>",
                "chat_ref": _chat_ref(str(account.get("account_id", "default")) or "default", user_id or "<wechat-user-id>"),
            },
        }
    if not user_id:
        return {"ok": False, "error": "user_id_required_for_live_smoke"}
    reply = send_text_message(account, user_id=user_id, context_token="smoke-context-token", text=text)
    return {"ok": True, "reply": reply}


def cmd_status(_args: argparse.Namespace) -> int:
    print(json.dumps(bridge_status(), ensure_ascii=False))
    return 0


def cmd_contract(_args: argparse.Namespace) -> int:
    print(json.dumps(bridge_contract(), ensure_ascii=False))
    return 0


def cmd_login_qr_start(args: argparse.Namespace) -> int:
    payload = start_login_qr(base_url=args.base_url, bot_type=args.bot_type, account_id=args.account_id)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_login_qr_wait(args: argparse.Namespace) -> int:
    payload = wait_for_login(timeout_seconds=args.timeout)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_login(args: argparse.Namespace) -> int:
    started = start_login_qr(base_url=args.base_url, bot_type=args.bot_type, account_id=args.account_id)
    image_path = str(started.get("qrcode_image_path") or "").strip()
    opened = False
    if image_path and not args.no_open:
        try:
            subprocess.run(["open", image_path], check=False, capture_output=True, text=True)
            opened = True
        except Exception:
            opened = False
    waited = wait_for_login(timeout_seconds=args.timeout)
    payload = {
        "started": started,
        "opened_image": opened,
        "wait": waited,
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if waited.get("connected") else 1


def cmd_run_once(args: argparse.Namespace) -> int:
    payload = safe_run_once(send_reply=not args.no_reply, mode="manual")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    payload = smoke(text=args.text, user_id=args.user_id, dry_run=args.dry_run)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_daemon(args: argparse.Namespace) -> int:
    return run_daemon(poll_interval=args.poll_interval, error_backoff=args.error_backoff, verbose=args.verbose)


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(LAUNCH_AGENT_NAME)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    log_stdout_path().parent.mkdir(parents=True, exist_ok=True)
    plist_path.write_text(
        plist_dumps(launch_agent_payload(poll_interval=args.poll_interval, error_backoff=args.error_backoff)),
        encoding="utf-8",
    )
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    bootstrap = run_launchctl("bootstrap", domain, str(plist_path))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{LAUNCH_AGENT_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=sys.stderr)
        return kickstart.returncode
    print(
        json.dumps(
            {
                "installed": True,
                "loaded": True,
                "plist": str(plist_path),
                "poll_interval": int(args.poll_interval),
                "error_backoff": int(args.error_backoff),
            },
            ensure_ascii=False,
        )
    )
    return 0


def cmd_enable(args: argparse.Namespace) -> int:
    login_started = start_login_qr(base_url=args.base_url, bot_type=args.bot_type, account_id=args.account_id)
    image_path = str(login_started.get("qrcode_image_path") or "").strip()
    opened = False
    if image_path and not args.no_open:
        try:
            subprocess.run(["open", image_path], check=False, capture_output=True, text=True)
            opened = True
        except Exception:
            opened = False
    login_wait = wait_for_login(timeout_seconds=args.timeout)
    if not login_wait.get("connected"):
        print(
            json.dumps(
                {
                    "ok": False,
                    "stage": "login",
                    "started": login_started,
                    "opened_image": opened,
                    "wait": login_wait,
                },
                ensure_ascii=False,
            )
        )
        return 1
    install_payload = launch_agent_payload(poll_interval=args.poll_interval, error_backoff=args.error_backoff)
    plist_path = launch_agent_plist_path(LAUNCH_AGENT_NAME)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    log_stdout_path().parent.mkdir(parents=True, exist_ok=True)
    plist_path.write_text(plist_dumps(install_payload), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    bootstrap = run_launchctl("bootstrap", domain, str(plist_path))
    if bootstrap.returncode != 0:
        print(
            json.dumps(
                {
                    "ok": False,
                    "stage": "install-launchagent",
                    "started": login_started,
                    "opened_image": opened,
                    "wait": login_wait,
                    "stderr": bootstrap.stderr.strip(),
                },
                ensure_ascii=False,
            )
        )
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{LAUNCH_AGENT_NAME}")
    if kickstart.returncode != 0:
        print(
            json.dumps(
                {
                    "ok": False,
                    "stage": "kickstart",
                    "started": login_started,
                    "opened_image": opened,
                    "wait": login_wait,
                    "stderr": kickstart.stderr.strip(),
                },
                ensure_ascii=False,
            )
        )
        return kickstart.returncode
    payload = {
        "ok": True,
        "started": login_started,
        "opened_image": opened,
        "wait": login_wait,
        "launchagent": {
            "installed": True,
            "loaded": True,
            "poll_interval": int(args.poll_interval),
            "error_backoff": int(args.error_backoff),
            "plist": str(plist_path),
        },
        "status": bridge_status(),
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(LAUNCH_AGENT_NAME)
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    if plist_path.exists():
        plist_path.unlink()
    print(json.dumps({"installed": False, "plist": str(plist_path)}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Weixin DM bridge for Codex Hub.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    contract = subparsers.add_parser("contract")
    contract.set_defaults(func=cmd_contract)

    login_qr_start = subparsers.add_parser("login-qr-start")
    login_qr_start.add_argument("--base-url", default=DEFAULT_BASE_URL)
    login_qr_start.add_argument("--bot-type", default=DEFAULT_BOT_TYPE)
    login_qr_start.add_argument("--account-id", default="default")
    login_qr_start.set_defaults(func=cmd_login_qr_start)

    login_qr_wait = subparsers.add_parser("login-qr-wait")
    login_qr_wait.add_argument("--timeout", type=int, default=180)
    login_qr_wait.set_defaults(func=cmd_login_qr_wait)

    login_parser = subparsers.add_parser("login")
    login_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    login_parser.add_argument("--bot-type", default=DEFAULT_BOT_TYPE)
    login_parser.add_argument("--account-id", default="default")
    login_parser.add_argument("--timeout", type=int, default=180)
    login_parser.add_argument("--no-open", action="store_true")
    login_parser.set_defaults(func=cmd_login)

    run_once_parser = subparsers.add_parser("run-once")
    run_once_parser.add_argument("--no-reply", action="store_true")
    run_once_parser.set_defaults(func=cmd_run_once)

    daemon_parser = subparsers.add_parser("daemon")
    daemon_parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    daemon_parser.add_argument("--error-backoff", type=int, default=DEFAULT_ERROR_BACKOFF)
    daemon_parser.add_argument("--verbose", action="store_true")
    daemon_parser.set_defaults(func=cmd_daemon)

    install_parser = subparsers.add_parser("install-launchagent")
    install_parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    install_parser.add_argument("--error-backoff", type=int, default=DEFAULT_ERROR_BACKOFF)
    install_parser.set_defaults(func=cmd_install_launchagent)

    enable_parser = subparsers.add_parser("enable")
    enable_parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    enable_parser.add_argument("--bot-type", default=DEFAULT_BOT_TYPE)
    enable_parser.add_argument("--account-id", default="default")
    enable_parser.add_argument("--timeout", type=int, default=180)
    enable_parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    enable_parser.add_argument("--error-backoff", type=int, default=DEFAULT_ERROR_BACKOFF)
    enable_parser.add_argument("--no-open", action="store_true")
    enable_parser.set_defaults(func=cmd_enable)

    uninstall_parser = subparsers.add_parser("uninstall-launchagent")
    uninstall_parser.set_defaults(func=cmd_uninstall_launchagent)

    smoke_parser = subparsers.add_parser("smoke")
    smoke_parser.add_argument("--text", default="微信桥接 smoke")
    smoke_parser.add_argument("--user-id", default="")
    smoke_parser.add_argument("--dry-run", action="store_true")
    smoke_parser.set_defaults(func=cmd_smoke)

    return parser


def main() -> int:
    runtime_state.init_db()
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

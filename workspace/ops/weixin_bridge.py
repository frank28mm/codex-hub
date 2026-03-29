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
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

try:  # pragma: no cover - optional dependency
    from Crypto.Cipher import AES as CryptoAES
except ImportError:  # pragma: no cover
    CryptoAES = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover
    Cipher = None  # type: ignore
    algorithms = None  # type: ignore
    modes = None  # type: ignore

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
BROKER_TIMEOUT_MS = 0
TYPING_STATUS_START = 1
TYPING_STATUS_CANCEL = 2
LAUNCH_AGENT_NAME = "com.codexhub.weixin-bridge"
DEFAULT_POLL_INTERVAL = 2
DEFAULT_ERROR_BACKOFF = 8
DEFAULT_WORKER_LIMIT = 10
DEFAULT_WORKER_IDLE_INTERVAL = 1
WEIXIN_INBOUND_QUEUE = "weixin_inbound_messages"
WEIXIN_WORKER_NAME = "weixin_bridge_worker"
WEIXIN_QUEUE_LEASE_SECONDS = 300
WEIXIN_QUEUE_LEASE_RENEW_INTERVAL_SECONDS = 90
WEIXIN_CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"
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


def inbound_media_dir() -> Path:
    return runtime_dir() / "inbound-media"


def log_stdout_path() -> Path:
    return workspace_root() / "logs" / "weixin-bridge.log"


def log_stderr_path() -> Path:
    return workspace_root() / "logs" / "weixin-bridge.err.log"


def bridge_workspace_path() -> Path:
    return workspace_root() / "bridge"


def voice_transcode_helper_path() -> Path:
    return bridge_workspace_path() / "weixin_voice_to_wav.mjs"


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


def _save_bridge_state_safe(updates: dict[str, Any], *, context: str = "") -> dict[str, Any]:
    try:
        return _save_bridge_state(updates)
    except Exception as exc:  # pragma: no cover - only hit during runtime resource failures
        _bridge_log(
            "bridge_state_save_failed",
            context=context,
            error=str(exc),
        )
        state = _bridge_state()
        if isinstance(state, dict):
            state.update(updates)
            state["updated_at"] = runtime_state.iso_now()
            state["state_write_failed"] = True
            state["state_write_failure_context"] = str(context or "").strip()
            state["state_write_failure_error"] = str(exc)
        return state if isinstance(state, dict) else {}


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


def _build_cdn_download_url(encrypted_query_param: str, cdn_base_url: str = WEIXIN_CDN_BASE_URL) -> str:
    return f"{cdn_base_url}/download?encrypted_query_param={urllib.parse.quote(encrypted_query_param)}"


def _download_cdn_bytes(encrypted_query_param: str, *, label: str, cdn_base_url: str = WEIXIN_CDN_BASE_URL) -> bytes:
    url = _build_cdn_download_url(encrypted_query_param, cdn_base_url=cdn_base_url)
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=API_TIMEOUT_MS / 1000, context=_ssl_context()) as response:
            return response.read()
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else ""
        raise RuntimeError(f"{label}: cdn_download_http_{exc.code} {body}".strip()) from exc
    except URLError as exc:
        raise RuntimeError(f"{label}: cdn_download_network_error {exc.reason}") from exc


def _parse_attachment_aes_key(value: str) -> bytes:
    token = str(value or "").strip()
    if not token:
        raise ValueError("missing attachment aes key")
    if re.fullmatch(r"[0-9a-fA-F]{32}", token):
        return bytes.fromhex(token)
    decoded = base64.b64decode(token)
    if len(decoded) == 16:
        return decoded
    ascii_value = decoded.decode("ascii", errors="ignore")
    if len(decoded) == 32 and re.fullmatch(r"[0-9a-fA-F]{32}", ascii_value):
        return bytes.fromhex(ascii_value)
    raise ValueError(f"unexpected attachment aes key shape: {len(decoded)} bytes")


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if pad < 1 or pad > 16 or len(data) < pad:
        raise ValueError("invalid pkcs7 padding")
    if data[-pad:] != bytes([pad]) * pad:
        raise ValueError("invalid pkcs7 padding")
    return data[:-pad]


def _decrypt_attachment_bytes(ciphertext: bytes, key: bytes) -> bytes:
    if CryptoAES is not None:
        return _pkcs7_unpad(CryptoAES.new(key, CryptoAES.MODE_ECB).decrypt(ciphertext))
    if Cipher is not None and algorithms is not None and modes is not None:
        decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
        return _pkcs7_unpad(decryptor.update(ciphertext) + decryptor.finalize())
    raise RuntimeError("missing AES decrypt dependency")


def _image_suffix_from_bytes(payload: bytes) -> str:
    if payload.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if payload.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if payload.startswith(b"RIFF") and payload[8:12] == b"WEBP":
        return ".webp"
    if payload.startswith((b"GIF87a", b"GIF89a")):
        return ".gif"
    return ".bin"


def _attachment_suffix(attachment_type: str, payload: bytes, attachment_name: str) -> str:
    if attachment_type == "image":
        return _image_suffix_from_bytes(payload)
    if attachment_type == "voice":
        return ".silk"
    suffix = Path(str(attachment_name or "").strip()).suffix
    return suffix if suffix else ".bin"


def _attachment_download_candidate(message: dict[str, Any]) -> dict[str, Any]:
    for item in message.get("item_list") or []:
        image_item = item.get("image_item") if isinstance(item, dict) else None
        voice_item = item.get("voice_item") if isinstance(item, dict) else None
        video_item = item.get("video_item") if isinstance(item, dict) else None
        file_item = item.get("file_item") if isinstance(item, dict) else None
        if image_item is not None:
            media = image_item.get("media", {}) if isinstance(image_item, dict) else {}
            return {
                "attachment_type": "image",
                "attachment_ref": str(media.get("encrypt_query_param", "")).strip(),
                "attachment_name": "",
                "attachment_aes_key": str(image_item.get("aeskey", "")).strip() or str(media.get("aes_key", "")).strip(),
                "voice_transcript": "",
            }
        if int(item.get("type", 0) or 0) == 3 or voice_item is not None:
            media = voice_item.get("media", {}) if isinstance(voice_item, dict) else {}
            transcript = str(voice_item.get("text", "")).strip() if isinstance(voice_item, dict) else ""
            return {
                "attachment_type": "voice",
                "attachment_ref": str(media.get("encrypt_query_param", "")).strip(),
                "attachment_name": "",
                "attachment_aes_key": str(media.get("aes_key", "")).strip(),
                "voice_transcript": transcript,
            }
        if video_item is not None:
            media = video_item.get("media", {}) if isinstance(video_item, dict) else {}
            return {
                "attachment_type": "video",
                "attachment_ref": str(media.get("encrypt_query_param", "")).strip(),
                "attachment_name": "",
                "attachment_aes_key": str(media.get("aes_key", "")).strip(),
                "voice_transcript": "",
            }
        if file_item is not None:
            media = file_item.get("media", {}) if isinstance(file_item, dict) else {}
            return {
                "attachment_type": "file",
                "attachment_ref": str(media.get("encrypt_query_param", "")).strip(),
                "attachment_name": str(file_item.get("file_name", "")).strip() if isinstance(file_item, dict) else "",
                "attachment_aes_key": str(media.get("aes_key", "")).strip(),
                "voice_transcript": "",
            }
    return {
        "attachment_type": "",
        "attachment_ref": "",
        "attachment_name": "",
        "attachment_aes_key": "",
        "voice_transcript": "",
    }


def _persist_attachment_bytes(
    *,
    message_id: str,
    attachment_type: str,
    attachment_name: str,
    payload: bytes,
) -> str:
    inbound_media_dir().mkdir(parents=True, exist_ok=True)
    suffix = _attachment_suffix(attachment_type, payload, attachment_name)
    path = inbound_media_dir() / f"{message_id}-{attachment_type}{suffix}"
    path.write_bytes(payload)
    return str(path)


def _download_attachment_for_message(message: dict[str, Any], normalized: dict[str, Any]) -> dict[str, str]:
    candidate = _attachment_download_candidate(message)
    attachment_type = str(candidate.get("attachment_type", "")).strip()
    attachment_ref = str(candidate.get("attachment_ref", "")).strip()
    if not attachment_type or not attachment_ref:
        return {
            "attachment_path": "",
            "attachment_media_type": "",
            "attachment_download_error": "",
        }
    if attachment_type not in {"image", "voice"}:
        return {
            "attachment_path": "",
            "attachment_media_type": "",
            "attachment_download_error": "",
        }
    try:
        encrypted = _download_cdn_bytes(
            attachment_ref,
            label=f"{attachment_type}:{normalized.get('message_id', '')}",
        )
        aes_key = str(candidate.get("attachment_aes_key", "")).strip()
        payload = _decrypt_attachment_bytes(encrypted, _parse_attachment_aes_key(aes_key)) if aes_key else encrypted
        attachment_path = _persist_attachment_bytes(
            message_id=str(normalized.get("message_id", "")).strip(),
            attachment_type=attachment_type,
            attachment_name=str(candidate.get("attachment_name", "")).strip(),
            payload=payload,
        )
        return {
            "attachment_path": attachment_path,
            "attachment_media_type": attachment_type,
            "attachment_download_error": "",
        }
    except Exception as exc:
        return {
            "attachment_path": "",
            "attachment_media_type": attachment_type,
            "attachment_download_error": str(exc),
        }


def _voice_wav_output_path(attachment_path: str) -> str:
    path = Path(str(attachment_path or "").strip())
    if not path:
        return ""
    return str(path.with_suffix(".wav"))


def _transcode_voice_attachment_to_wav(attachment_path: str) -> tuple[str, str]:
    source = str(attachment_path or "").strip()
    if not source:
        return "", "missing_voice_attachment"
    source_path = Path(source)
    if source_path.suffix.lower() == ".wav":
        return str(source_path), ""
    output_path = _voice_wav_output_path(source)
    command = ["node", str(voice_transcode_helper_path()), source, output_path]
    completed = subprocess.run(
        command,
        cwd=str(bridge_workspace_path()),
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = str(completed.stderr or "").strip()
        stdout = str(completed.stdout or "").strip()
        return "", stderr or stdout or "voice_transcode_failed"
    if not Path(output_path).exists():
        return "", "voice_transcode_missing_output"
    return output_path, ""


def _transcribe_voice_attachment(attachment_path: str) -> tuple[str, str]:
    source = str(attachment_path or "").strip()
    if not source:
        return "", "missing_voice_attachment"
    api_key = str(os.environ.get("OPENAI_API_KEY", "")).strip()
    if not api_key:
        return "", "openai_api_key_missing"
    try:
        from openai import OpenAI
    except ImportError:
        return "", "openai_sdk_missing"
    try:
        client = OpenAI(api_key=api_key)
        with Path(source).open("rb") as audio_file:
            result = client.audio.transcriptions.create(
                model="gpt-4o-mini-transcribe",
                file=audio_file,
                response_format="text",
            )
    except Exception as exc:
        return "", f"voice_transcription_failed: {exc}"
    if isinstance(result, str):
        text = result.strip()
    else:
        text = str(getattr(result, "text", "") or result).strip()
    return (text, "") if text else ("", "voice_transcription_empty")


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


def _message_attachment(message: dict[str, Any]) -> dict[str, Any]:
    candidate = _attachment_download_candidate(message)
    return {
        "attachment_type": str(candidate.get("attachment_type", "")).strip(),
        "attachment_ref": str(candidate.get("attachment_ref", "")).strip(),
        "attachment_name": str(candidate.get("attachment_name", "")).strip(),
        "voice_transcript": str(candidate.get("voice_transcript", "")).strip(),
    }


def _unsupported_input_reply(
    content_kind: str,
    *,
    attachment_download_error: str = "",
    voice_transcription_error: str = "",
) -> str:
    kind = str(content_kind or "").strip()
    if kind == "image":
        if attachment_download_error:
            return f"当前微信私聊桥未能取回这张图片附件：{attachment_download_error}。请重试，或补一段文字说明。"
        return "当前微信私聊桥还没有拿到可用的图片附件。请重试，或补一段文字说明。"
    if kind == "voice":
        if attachment_download_error:
            return f"当前微信私聊桥未能取回这段语音附件：{attachment_download_error}。请重试，或直接改发文字。"
        if voice_transcription_error:
            if voice_transcription_error == "openai_api_key_missing":
                return "当前微信私聊桥拿到了语音附件，但本机还没有配置可用的语音转写能力。请直接改发文字。"
            if voice_transcription_error == "openai_sdk_missing":
                return "当前微信私聊桥拿到了语音附件，但本机缺少语音转写依赖。请直接改发文字。"
            return f"当前微信私聊桥拿到了语音附件，但语音转写失败：{voice_transcription_error}。请直接改发文字。"
        return "当前微信私聊桥还没有拿到可用的语音输入。请改发文字，或等后续版本接入更稳的语音解析。"
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
    attachment = _message_attachment(message)
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
        "attachment_type": str(attachment.get("attachment_type", "")).strip(),
        "attachment_ref": str(attachment.get("attachment_ref", "")).strip(),
        "attachment_name": str(attachment.get("attachment_name", "")).strip(),
        "voice_transcript": str(attachment.get("voice_transcript", "")).strip(),
        "attachment_path": "",
        "attachment_media_type": "",
        "attachment_download_error": "",
        "voice_transcription_error": "",
        "raw_message": message,
    }


def _prepare_inbound_message(message: dict[str, Any], account: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
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
    return normalized, inbound_record


def _persist_inbound_normalized(normalized: dict[str, Any], *, inbound_record: dict[str, Any]) -> dict[str, Any]:
    return runtime_state.upsert_bridge_message(
        bridge=BRIDGE_NAME,
        direction="inbound",
        message_id=str(normalized.get("message_id", "")).strip(),
        status=str(inbound_record.get("status", "")).strip() or "received",
        payload=normalized,
        project_name=str(normalized.get("project_name", "")).strip(),
        session_id=str(inbound_record.get("session_id", "")).strip(),
    )


def _hydrate_inbound_message(normalized: dict[str, Any], *, inbound_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    attachment_type = str(normalized.get("attachment_type", "")).strip()
    raw_message = normalized.get("raw_message") if isinstance(normalized.get("raw_message"), dict) else {}
    if attachment_type in {"image", "voice"} and raw_message and not str(normalized.get("attachment_path", "")).strip():
        normalized.update(_download_attachment_for_message(raw_message, normalized))
    if attachment_type == "voice" and not str(normalized.get("voice_transcript", "")).strip():
        attachment_path = str(normalized.get("attachment_path", "")).strip()
        if attachment_path and not str(normalized.get("attachment_download_error", "")).strip():
            wav_path, transcode_error = _transcode_voice_attachment_to_wav(attachment_path)
            if wav_path:
                normalized["attachment_path"] = wav_path
                normalized["attachment_media_type"] = "audio/wav"
                attachment_path = wav_path
                transcode_error = ""
            if transcode_error:
                normalized["voice_transcription_error"] = transcode_error
            if attachment_path and not str(normalized.get("voice_transcription_error", "")).strip():
                transcript, transcription_error = _transcribe_voice_attachment(attachment_path)
                if transcript:
                    normalized["voice_transcript"] = transcript
                    normalized["voice_transcription_error"] = ""
                elif transcription_error:
                    normalized["voice_transcription_error"] = transcription_error
        elif str(normalized.get("attachment_download_error", "")).strip():
            normalized["voice_transcription_error"] = ""
    return normalized, _persist_inbound_normalized(normalized, inbound_record=inbound_record)


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
    timeout_seconds = BROKER_TIMEOUT_MS / 1000 if BROKER_TIMEOUT_MS and BROKER_TIMEOUT_MS > 0 else None
    try:
        run_kwargs: dict[str, Any] = {
            "cwd": str(workspace_root()),
            "capture_output": True,
            "text": True,
            "check": False,
        }
        if timeout_seconds is not None:
            run_kwargs["timeout"] = timeout_seconds
        completed = subprocess.run(command, **run_kwargs)
    except subprocess.TimeoutExpired as exc:
        stderr = str(exc.stderr or "").strip()
        stdout = str(exc.stdout or "").strip()
        return {
            "ok": False,
            "command": command,
            "stdout": stdout,
            "stderr": stderr,
            "error": "weixin_broker_timeout",
            "reason": "session_timed_out",
            "response": {
                "ok": False,
                "error": "weixin_broker_timeout",
                "reason": "session_timed_out",
                "timeout_ms": BROKER_TIMEOUT_MS,
            },
        }
    except Exception as exc:  # pragma: no cover - defensive path
        return {
            "ok": False,
            "command": command,
            "stdout": "",
            "stderr": str(exc),
            "error": "weixin_broker_exec_failed",
            "reason": "broker_exec_failed",
            "response": {
                "ok": False,
                "error": "weixin_broker_exec_failed",
                "reason": "broker_exec_failed",
            },
        }
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
    failure_reason = str(response.get("reason", "")).strip() or str(result.get("reason", "")).strip()
    if failure_reason == "session_timed_out":
        timeout_seconds = int(response.get("timeout_ms") or BROKER_TIMEOUT_MS) // 1000
        return (
            "当前微信线程上一轮执行长时间未返回，我已经停止继续等待。\n"
            "请直接重试刚才的问题；如果再次超时，我会继续走恢复路径。\n"
            f"本轮超时阈值：{timeout_seconds} 秒。"
        )
    if failure_reason == "broker_exec_failed":
        return "当前微信桥执行链路异常中断。我已经记录失败状态，请直接重试刚才的问题。"
    if response.get("ok") is True:
        stdout = str(response.get("stdout", "") or response.get("raw", "") or result.get("stdout", "")).strip()
        if stdout:
            return stdout
        return "已收到并完成处理。"
    broker_action = (
        str(response.get("delegated_broker_action", "")).strip()
        or str(response.get("delegatedbrokeraction", "")).strip()
        or str(response.get("broker_action", "")).strip()
        or str(response.get("brokeraction", "")).strip()
    )
    error = str(response.get("error", "")).strip() or str(result.get("stderr", "")).strip()
    if not error and str(response.get("result_status", "")).strip() == "error" and broker_action:
        error = f"broker call failed: {broker_action}"
    if error:
        first_line = next((line.strip() for line in error.splitlines() if line.strip()), "")
        if first_line:
            error = first_line
    if error:
        return (
            "当前微信桥执行链路异常中断，我已经开始恢复。\n"
            f"故障摘要：{error}"
        )
    return error or "处理失败。"


def _broker_prompt_from_normalized(normalized: dict[str, Any]) -> str:
    text = str(normalized.get("text", "")).strip()
    if text:
        return text
    attachment_type = str(normalized.get("attachment_type", "")).strip()
    attachment_path = str(normalized.get("attachment_path", "")).strip()
    voice_transcript = str(normalized.get("voice_transcript", "")).strip()
    if attachment_type == "voice" and voice_transcript:
        return voice_transcript
    if attachment_type == "image" and attachment_path:
        return "用户通过微信发送了一张图片附件，请结合本地图片内容理解问题并直接回复。"
    return ""


def _bridge_log(event: str, **fields: Any) -> None:
    record = {"ts": runtime_state.iso_now(), "bridge": BRIDGE_NAME, "event": event, **fields}
    print(json.dumps(record, ensure_ascii=False, sort_keys=True), file=sys.stderr, flush=True)


def _runtime_event_lease_renewer(
    *,
    event_key: str,
    claim_token: str,
    stop_event: threading.Event,
    lease_seconds: int = WEIXIN_QUEUE_LEASE_SECONDS,
    renew_interval_seconds: float | None = None,
) -> None:
    interval = max(0.01, float(renew_interval_seconds or WEIXIN_QUEUE_LEASE_RENEW_INTERVAL_SECONDS or 1.0))
    while not stop_event.wait(interval):
        renewed = runtime_state.renew_runtime_event_lease(
            event_key,
            claim_token=claim_token,
            lease_seconds=lease_seconds,
        )
        if str(renewed.get("status", "")).strip() != "processing":
            return


def _start_runtime_event_lease_heartbeat(
    *,
    event_key: str,
    claim_token: str,
    lease_seconds: int = WEIXIN_QUEUE_LEASE_SECONDS,
    renew_interval_seconds: float | None = None,
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_runtime_event_lease_renewer,
        kwargs={
            "event_key": event_key,
            "claim_token": claim_token,
            "stop_event": stop_event,
            "lease_seconds": lease_seconds,
            "renew_interval_seconds": renew_interval_seconds,
        },
        name=f"weixin-lease-{event_key[:8]}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


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
        "session_lane": normalized["chat_ref"],
        "session_launch_source": BRIDGE_NAME,
        "session_thread_name": normalized["thread_name"],
        "session_thread_label": normalized["thread_label"],
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


def _route_normalized_message(
    normalized: dict[str, Any],
    *,
    account: dict[str, Any],
    inbound_record: dict[str, Any],
    send_reply: bool = True,
) -> dict[str, Any]:
    text = _broker_prompt_from_normalized(normalized)
    if not text:
        reply_text = _unsupported_input_reply(
            str(normalized.get("content_kind", "")),
            attachment_download_error=str(normalized.get("attachment_download_error", "")).strip(),
            voice_transcription_error=str(normalized.get("voice_transcription_error", "")).strip(),
        )
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
    binding_metadata = binding.get("metadata", {}) if isinstance(binding.get("metadata"), dict) else {}
    binding_lane = str(binding_metadata.get("session_lane", "")).strip()
    if binding and continue_requested and not binding_lane:
        binding = runtime_state.upsert_bridge_chat_binding(
            bridge=BRIDGE_NAME,
            chat_ref=normalized["chat_ref"],
            binding_scope=str(binding.get("binding_scope", "") or "workspace"),
            project_name=str(binding.get("project_name", "") or ""),
            topic_name=str(binding.get("topic_name", "") or ""),
            session_id=str(binding.get("session_id", "") or ""),
            metadata={
                **binding_metadata,
                **_workspace_binding_metadata(normalized),
                "legacy_session_lane_backfilled": True,
            },
        )
        binding_metadata = binding.get("metadata", {}) if isinstance(binding.get("metadata"), dict) else {}
        binding_lane = str(binding_metadata.get("session_lane", "")).strip()
    resume_session_id = (
        str(binding.get("session_id", "")).strip()
        if continue_requested and binding_lane == normalized["chat_ref"]
        else ""
    )
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
    else:
        # Keep Weixin DM conversations isolated from hot project resumes in other
        # surfaces. Only explicit "继续/接着" should reuse the Weixin session.
        broker_args.append("--no-auto-resume")
    if explicit_project_name:
        broker_args.extend(["--project-name", explicit_project_name])
    if text:
        broker_args.extend(["--prompt", text])
    attachment_path = str(normalized.get("attachment_path", "")).strip()
    attachment_type = str(normalized.get("attachment_type", "")).strip()
    voice_transcript = str(normalized.get("voice_transcript", "")).strip()
    if attachment_path:
        broker_args.extend(["--attachment-path", attachment_path])
    if attachment_type:
        broker_args.extend(["--attachment-type", attachment_type])
    if voice_transcript:
        broker_args.extend(["--voice-transcript", voice_transcript])

    broker_result: dict[str, Any]
    updated_binding = binding
    reply_text = ""
    _send_typing(account, user_id=normalized["user_id"], context_token=normalized["context_token"], status=TYPING_STATUS_START)
    try:
        broker_result = _broker_command(broker_args)
        updated_binding = _update_binding_from_result(
            normalized,
            broker_result,
            explicit_project_name=explicit_project_name,
        )
        reply_text = _reply_text_from_broker_result(broker_result)
    except Exception as exc:  # pragma: no cover - defensive path for silent reply failures
        broker_result = {
            "ok": False,
            "command": ["python3", str(broker_path()), *broker_args],
            "stdout": "",
            "stderr": str(exc),
            "error": "weixin_route_failed",
            "reason": "route_execution_failed",
            "response": {
                "ok": False,
                "error": "weixin_route_failed",
                "reason": "route_execution_failed",
            },
        }
        reply_text = "当前微信桥在生成回复前发生异常。我已经记录失败状态，请直接重试刚才的问题。"
        _bridge_log("route_private_message_failed", chat_ref=normalized["chat_ref"], message_id=normalized["message_id"], error=str(exc))
    finally:
        _send_typing(account, user_id=normalized["user_id"], context_token=normalized["context_token"], status=TYPING_STATUS_CANCEL)

    deliveries: list[dict[str, Any]] = []
    if send_reply and normalized["context_token"] and reply_text:
        try:
            deliveries = _deliver_reply_text(
                account,
                normalized,
                reply_text=reply_text,
                status="sent" if broker_result.get("ok") else "error",
                project_name=str(updated_binding.get("project_name", "")).strip(),
                session_id=str(updated_binding.get("session_id", "")).strip(),
            )
        except Exception as exc:  # pragma: no cover - defensive path for missing outbound trace
            broker_result = {
                **broker_result,
                "ok": False,
                "error": "weixin_delivery_failed",
                "reason": "delivery_failed",
                "response": {
                    **(broker_result.get("response", {}) if isinstance(broker_result.get("response"), dict) else {}),
                    "ok": False,
                    "error": "weixin_delivery_failed",
                    "reason": "delivery_failed",
                },
            }
            runtime_state.upsert_bridge_message(
                bridge=BRIDGE_NAME,
                direction="outbound",
                message_id=f"out-error-{normalized['message_id']}",
                status="error",
                payload={
                    "text": reply_text,
                    "chat_ref": normalized["chat_ref"],
                    "delivery_error": str(exc),
                },
                project_name=str(updated_binding.get("project_name", "")).strip(),
                session_id=str(updated_binding.get("session_id", "")).strip(),
            )
            _bridge_log("route_private_message_delivery_failed", chat_ref=normalized["chat_ref"], message_id=normalized["message_id"], error=str(exc))

    response = broker_result.get("response", {}) if isinstance(broker_result.get("response"), dict) else {}
    failure_reason = str(response.get("reason", "")).strip() or str(broker_result.get("reason", "")).strip()
    return {
        "ok": bool(broker_result.get("ok")) and (not send_reply or not reply_text or bool(deliveries)),
        "reason": failure_reason,
        "normalized": normalized,
        "inbound_record": inbound_record,
        "binding": updated_binding,
        "broker_result": broker_result,
        "reply_text": reply_text,
        "delivery": deliveries[0] if deliveries else {},
        "deliveries": deliveries,
    }


def route_private_message(message: dict[str, Any], *, send_reply: bool = True) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    normalized, inbound_record = _prepare_inbound_message(message, account)
    normalized, inbound_record = _hydrate_inbound_message(normalized, inbound_record=inbound_record)
    return _route_normalized_message(normalized, account=account, inbound_record=inbound_record, send_reply=send_reply)


def enqueue_private_message(message: dict[str, Any], *, send_reply: bool = True) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    normalized, inbound_record = _prepare_inbound_message(message, account)
    event = runtime_state.enqueue_runtime_event(
        queue_name=WEIXIN_INBOUND_QUEUE,
        event_type="private_message",
        payload={
            "normalized": normalized,
            "send_reply": bool(send_reply),
        },
        dedupe_key=normalized["message_id"],
        status="pending",
    )
    return {
        "ok": True,
        "event_key": event.get("event_key", ""),
        "normalized": normalized,
        "inbound_record": inbound_record,
        "queued": True,
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
        results.append(enqueue_private_message(message, send_reply=send_reply))
    runtime_state.upsert_bridge_connection(
        BRIDGE_NAME,
        status="connected",
        host_mode="python",
        transport="http_json_long_poll",
        last_event_at=runtime_state.iso_now(),
        metadata={"account_id": account.get("account_id", "default"), "enqueued_count": len(results)},
    )
    return {"ok": True, "bridge": BRIDGE_NAME, "message_count": len(results), "enqueued_count": len(results), "results": results}


def run_queue_once(*, limit: int = DEFAULT_WORKER_LIMIT) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    claimed = runtime_state.claim_runtime_events(
        queue_name=WEIXIN_INBOUND_QUEUE,
        claimed_by=WEIXIN_WORKER_NAME,
        limit=max(1, int(limit or DEFAULT_WORKER_LIMIT)),
        lease_seconds=WEIXIN_QUEUE_LEASE_SECONDS,
        event_types=["private_message"],
    )
    processed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for event in claimed:
        payload = dict(event.get("payload") or {})
        normalized = payload.get("normalized") if isinstance(payload.get("normalized"), dict) else {}
        if not normalized:
            updated = runtime_state.fail_runtime_event(
                event["event_key"],
                claim_token=str(event.get("claim_token", "")),
                error="missing_normalized_payload",
                retry_after_seconds=0,
                final=True,
            )
            failed.append(
                {
                    "event_key": event["event_key"],
                    "error": updated.get("last_error", ""),
                    "status": updated.get("status", ""),
                }
            )
            continue
        inbound_record = runtime_state.fetch_bridge_message_detail(
            bridge=BRIDGE_NAME,
            direction="inbound",
            message_id=str(normalized.get("message_id", "")).strip(),
        )
        if not inbound_record:
            inbound_record = runtime_state.upsert_bridge_message(
                bridge=BRIDGE_NAME,
                direction="inbound",
                message_id=str(normalized.get("message_id", "")).strip(),
                status="received",
                payload=normalized,
                project_name=str(normalized.get("project_name", "")).strip(),
            )
        lease_stop, lease_thread = _start_runtime_event_lease_heartbeat(
            event_key=str(event.get("event_key", "")).strip(),
            claim_token=str(event.get("claim_token", "")).strip(),
        )
        try:
            normalized, inbound_record = _hydrate_inbound_message(normalized, inbound_record=inbound_record)
            result = _route_normalized_message(
                normalized,
                account=account,
                inbound_record=inbound_record,
                send_reply=bool(payload.get("send_reply", True)),
            )
        except Exception as exc:
            retry_after_seconds = min(300, 30 * max(1, int(event.get("attempt_count", 1) or 1)))
            updated = runtime_state.fail_runtime_event(
                event["event_key"],
                claim_token=str(event.get("claim_token", "")),
                error=f"{type(exc).__name__}: {exc}",
                retry_after_seconds=retry_after_seconds,
                final=False,
            )
            failed.append(
                {
                    "event_key": event["event_key"],
                    "error": updated.get("last_error", ""),
                    "retry_at": updated.get("available_at", ""),
                }
            )
            _bridge_log(
                "run_queue_once_failed",
                event_key=event["event_key"],
                message_id=normalized.get("message_id", ""),
                error=str(exc),
            )
            continue
        finally:
            lease_stop.set()
            lease_thread.join(timeout=1)
        if str(result.get("reason", "")).strip() == "delivery_failed":
            retry_after_seconds = min(300, 30 * max(1, int(event.get("attempt_count", 1) or 1)))
            updated = runtime_state.fail_runtime_event(
                event["event_key"],
                claim_token=str(event.get("claim_token", "")),
                error="delivery_failed",
                retry_after_seconds=retry_after_seconds,
                final=False,
            )
            failed.append(
                {
                    "event_key": event["event_key"],
                    "error": updated.get("last_error", ""),
                    "retry_at": updated.get("available_at", ""),
                }
            )
            continue
        runtime_state.complete_runtime_event(
            event["event_key"],
            claim_token=str(event.get("claim_token", "")),
            result=result,
        )
        processed.append(
            {
                "event_key": event["event_key"],
                "message_id": normalized.get("message_id", ""),
                "ok": bool(result.get("ok")),
                "reason": str(result.get("reason", "")).strip(),
            }
        )
    return {
        "ok": not failed,
        "bridge": BRIDGE_NAME,
        "queue_name": WEIXIN_INBOUND_QUEUE,
        "claimed_count": len(claimed),
        "processed_count": len(processed),
        "failed_count": len(failed),
        "processed": processed,
        "failed": failed,
    }


def safe_run_once(*, send_reply: bool = True, mode: str = "manual") -> dict[str, Any]:
    started_at = runtime_state.iso_now()
    _save_bridge_state_safe({"mode": mode, "last_loop_started_at": started_at}, context="loop_start")
    try:
        payload = run_once(send_reply=send_reply)
    except Exception as exc:  # pragma: no cover - defensive path for daemon reliability
        now = runtime_state.iso_now()
        state = _bridge_state()
        failures = int(state.get("consecutive_failures") or 0) + 1
        _save_bridge_state_safe(
            {
                "mode": mode,
                "last_loop_finished_at": now,
                "last_error_at": now,
                "last_error": str(exc),
                "consecutive_failures": failures,
                "last_message_count": 0,
            },
            context="loop_exception",
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
        _save_bridge_state_safe(
            {
                "mode": mode,
                "last_loop_finished_at": now,
                "last_error_at": now,
                "last_error": error_text,
                "consecutive_failures": failures,
                "last_message_count": 0,
            },
            context="loop_failed_payload",
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
    _save_bridge_state_safe(
        {
            "mode": mode,
            "last_loop_finished_at": now,
            "last_success_at": now,
            "last_error_at": "",
            "last_error": "",
            "consecutive_failures": 0,
            "last_message_count": int(payload.get("message_count") or 0),
        },
        context="loop_success",
    )
    return payload


def _run_poll_daemon_loop(
    *,
    stop_event: threading.Event,
    poll_interval: int,
    error_backoff: int,
    verbose: bool = False,
) -> None:
    interval = max(1, int(poll_interval))
    backoff = max(interval, int(error_backoff))
    while not stop_event.is_set():
        payload = safe_run_once(mode="daemon_poll")
        if verbose:
            print(json.dumps({"poll": payload}, ensure_ascii=False), flush=True)
        sleep_seconds = interval if payload.get("ok") else backoff
        if stop_event.wait(sleep_seconds):
            return


def _run_worker_daemon_loop(
    *,
    stop_event: threading.Event,
    worker_limit: int,
    idle_interval: int,
    error_backoff: int,
    verbose: bool = False,
) -> None:
    idle_sleep = max(1, int(idle_interval))
    backoff = max(idle_sleep, int(error_backoff))
    while not stop_event.is_set():
        try:
            payload = run_queue_once(limit=worker_limit)
        except Exception as exc:  # pragma: no cover - daemon resilience path
            payload = {
                "ok": False,
                "bridge": BRIDGE_NAME,
                "queue_name": WEIXIN_INBOUND_QUEUE,
                "error": str(exc),
                "reason": "worker_loop_failed",
                "claimed_count": 0,
                "processed_count": 0,
                "failed_count": 0,
            }
            _bridge_log("worker_loop_failed", error=str(exc))
            runtime_state.upsert_bridge_connection(
                BRIDGE_NAME,
                status="error",
                host_mode="python",
                transport="http_json_long_poll",
                last_error=str(exc),
                metadata={"mode": "daemon_worker", "reason": "worker_loop_failed"},
            )
        if verbose:
            print(json.dumps({"worker": payload}, ensure_ascii=False), flush=True)
        has_work = any(int(payload.get(key, 0) or 0) > 0 for key in ("claimed_count", "processed_count", "failed_count"))
        sleep_seconds = 0 if has_work else (idle_sleep if payload.get("ok") else backoff)
        if sleep_seconds and stop_event.wait(sleep_seconds):
            return


def run_daemon(*, poll_interval: int = DEFAULT_POLL_INTERVAL, error_backoff: int = DEFAULT_ERROR_BACKOFF, verbose: bool = False) -> int:
    stop_event = threading.Event()
    poll_thread = threading.Thread(
        target=_run_poll_daemon_loop,
        kwargs={
            "stop_event": stop_event,
            "poll_interval": poll_interval,
            "error_backoff": error_backoff,
            "verbose": verbose,
        },
        name="weixin-bridge-poll",
        daemon=False,
    )
    worker_thread = threading.Thread(
        target=_run_worker_daemon_loop,
        kwargs={
            "stop_event": stop_event,
            "worker_limit": DEFAULT_WORKER_LIMIT,
            "idle_interval": DEFAULT_WORKER_IDLE_INTERVAL,
            "error_backoff": error_backoff,
            "verbose": verbose,
        },
        name="weixin-bridge-worker",
        daemon=False,
    )
    poll_thread.start()
    worker_thread.start()
    try:
        poll_thread.join()
        worker_thread.join()
    except KeyboardInterrupt:
        stop_event.set()
        poll_thread.join(timeout=2)
        worker_thread.join(timeout=2)
        return 130
    return 0


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
        "runtime_queue": runtime_state.fetch_runtime_queue_status(queue_name=WEIXIN_INBOUND_QUEUE),
    }


def smoke(*, text: str, user_id: str = "", dry_run: bool = True) -> dict[str, Any]:
    account, error = _account_or_error()
    if error:
        return error
    sample_payload = {
        "text": text,
        "user_id": user_id or "<wechat-user-id>",
        "chat_ref": _chat_ref(str(account.get("account_id", "default")) or "default", user_id or "<wechat-user-id>"),
    }
    if dry_run:
        return {
            "ok": True,
            "bridge": BRIDGE_NAME,
            "contract": bridge_contract(),
            "delivery_mode": "preview_only",
            "sample_payload": sample_payload,
        }
    if not user_id:
        return {"ok": False, "error": "user_id_required_for_live_smoke", "delivery_mode": "live_send"}
    reply = send_text_message(account, user_id=user_id, context_token="smoke-context-token", text=text)
    return {
        "ok": True,
        "bridge": BRIDGE_NAME,
        "delivery_mode": "live_send",
        "sample_payload": sample_payload,
        "reply": reply,
    }


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


def cmd_run_once(args: argparse.Namespace) -> int:
    payload = safe_run_once(send_reply=not args.no_reply, mode="manual")
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_run_queue_once(args: argparse.Namespace) -> int:
    payload = run_queue_once(limit=args.limit)
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload.get("failed_count", 0) == 0 else 1


def cmd_smoke(args: argparse.Namespace) -> int:
    payload = smoke(text=args.text, user_id=args.user_id, dry_run=not args.live_send)
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

    run_once_parser = subparsers.add_parser("run-once")
    run_once_parser.add_argument("--no-reply", action="store_true")
    run_once_parser.set_defaults(func=cmd_run_once)

    run_queue_parser = subparsers.add_parser("run-queue-once")
    run_queue_parser.add_argument("--limit", type=int, default=DEFAULT_WORKER_LIMIT)
    run_queue_parser.set_defaults(func=cmd_run_queue_once)

    daemon_parser = subparsers.add_parser("daemon")
    daemon_parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    daemon_parser.add_argument("--error-backoff", type=int, default=DEFAULT_ERROR_BACKOFF)
    daemon_parser.add_argument("--verbose", action="store_true")
    daemon_parser.set_defaults(func=cmd_daemon)

    install_parser = subparsers.add_parser("install-launchagent")
    install_parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL)
    install_parser.add_argument("--error-backoff", type=int, default=DEFAULT_ERROR_BACKOFF)
    install_parser.set_defaults(func=cmd_install_launchagent)

    uninstall_parser = subparsers.add_parser("uninstall-launchagent")
    uninstall_parser.set_defaults(func=cmd_uninstall_launchagent)

    smoke_parser = subparsers.add_parser("smoke")
    smoke_parser.add_argument("--text", default="微信桥接 smoke")
    smoke_parser.add_argument("--user-id", default="")
    smoke_mode_group = smoke_parser.add_mutually_exclusive_group()
    smoke_mode_group.add_argument("--dry-run", action="store_true")
    smoke_mode_group.add_argument("--live-send", action="store_true")
    smoke_parser.set_defaults(func=cmd_smoke)

    return parser


def main() -> int:
    runtime_state.init_db()
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

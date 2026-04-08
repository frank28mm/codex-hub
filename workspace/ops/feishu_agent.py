#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import plistlib
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import requests

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyYAML is required for feishu_agent") from exc

try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None

try:
    from ops import feishu_capabilities
except ImportError:  # pragma: no cover
    import feishu_capabilities  # type: ignore

try:
    from ops import lark_cli_backend
except ImportError:  # pragma: no cover
    import lark_cli_backend  # type: ignore


DEFAULT_BASE_URL = "https://open.feishu.cn/open-apis"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_AUTHORIZE_URL = "https://open.feishu.cn/open-apis/authen/v1/index"
DEFAULT_OAUTH_REDIRECT_URI = "http://127.0.0.1:14589/feishu-auth/callback"
DEFAULT_OAUTH_PORT = 14589
DEFAULT_OAUTH_PATH = "/feishu-auth/callback"
DEFAULT_TOKEN_STORE_NAME = "feishu_user_token.json"
DEFAULT_DYNAMIC_REGISTRY_NAME = "feishu_resources.dynamic.yaml"
DEFAULT_LAUNCH_AGENT_NAME = "com.codexhub.coco-feishu-bridge.plist"
DEFAULT_LARK_CLI_DOMAINS = "event,im,docs,drive,base,task,calendar,vc,minutes,contact,wiki,sheets,mail"
DEFAULT_LARK_CLI_CONFIG_PATH = Path.home() / ".lark-cli" / "config.json"


class FeishuAgentError(RuntimeError):
    def __init__(self, message: str, *, code: str = "", details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _control_root(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_CONTROL_ROOT", "")).strip()
    if explicit:
        return Path(explicit)
    return Path(__file__).resolve().parents[1] / "control"


def default_registry_path(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_FEISHU_RESOURCES_PATH", "")).strip()
    if explicit:
        return Path(explicit)
    return _control_root(source) / "feishu_resources.yaml"


def default_dynamic_registry_path(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_FEISHU_DYNAMIC_RESOURCES_PATH", "")).strip()
    if explicit:
        return Path(explicit)
    return _runtime_root(source) / DEFAULT_DYNAMIC_REGISTRY_NAME


def _workspace_root(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_ROOT", "")).strip()
    if explicit:
        return Path(explicit)
    return Path(__file__).resolve().parents[1]


def _runtime_root(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_RUNTIME_ROOT", "")).strip()
    if explicit:
        return Path(explicit)
    return _workspace_root(source) / "runtime"


def default_user_token_store_path(env: dict[str, str] | None = None) -> Path:
    source = env or os.environ
    explicit = str(source.get("WORKSPACE_HUB_FEISHU_USER_TOKEN_PATH", "")).strip()
    if explicit:
        return Path(explicit)
    return _runtime_root(source) / DEFAULT_TOKEN_STORE_NAME


def _parse_env_file(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        values[key] = value
    return values


def _load_launch_agent_env() -> dict[str, str]:
    plist_path = Path.home() / "Library" / "LaunchAgents" / DEFAULT_LAUNCH_AGENT_NAME
    if not plist_path.exists():
        return {}
    try:
        payload = plistlib.loads(plist_path.read_bytes())
    except Exception:
        return {}
    values = payload.get("EnvironmentVariables", {})
    return values if isinstance(values, dict) else {}


def _load_bootstrap_env() -> dict[str, str]:
    source = dict(os.environ)
    workspace_root = _workspace_root(source)
    values: dict[str, str] = {}
    explicit = str(source.get("WORKSPACE_HUB_FEISHU_ENV", "")).strip()
    candidates = [
        explicit,
        str(workspace_root / "ops" / "feishu_bridge.env.local"),
        str(workspace_root / ".env.feishu.local"),
    ]
    for raw_path in candidates:
        if not raw_path:
            continue
        path = Path(raw_path)
        if path.exists():
            try:
                values.update(_parse_env_file(path.read_text(encoding="utf-8")))
            except Exception:
                continue
    values.update(_load_launch_agent_env())
    return values


def _default_registry_payload() -> dict[str, Any]:
    return {
        "version": 1,
        "defaults": {
            "owner_open_id": "",
            "calendar_id": "",
            "doc_folder_token": "",
            "oauth_scopes": [
                "contact:contact.base:readonly",
                "task:task:write",
                "task:tasklist:read",
                "task:tasklist:write",
            ],
            "meeting": {
                "calendar_id": "",
                "timezone": DEFAULT_TIMEZONE,
                "duration_minutes": 30,
                "attendee_ability": "can_modify_event",
                "visibility": "default",
            },
        },
        "projection": {
            "app": {
                "alias": "codex_hub_projection",
                "name": "Codex Hub 项目任务看板",
                "app_token": "",
                "folder_token": "",
            },
            "tables": {
                "projects_overview": {
                    "alias": "codex_hub_projects_overview",
                    "name": "项目总览",
                    "table_id": "",
                    "default_view_name": "全部项目",
                },
                "tasks_current": {
                    "alias": "codex_hub_tasks_current",
                    "name": "当前任务",
                    "table_id": "",
                    "default_view_name": "全部任务",
                },
            },
            "views": {
                "projects_overview": [
                    {"name": "全部项目", "type": "grid"},
                    {"name": "按状态看板", "type": "kanban"},
                    {"name": "按优先级", "type": "grid"},
                    {"name": "最近更新", "type": "grid"},
                    {"name": "需关注项目", "type": "grid"},
                ],
                "tasks_current": [
                    {"name": "全部任务", "type": "grid"},
                    {"name": "按状态看板", "type": "kanban"},
                    {"name": "按项目分组", "type": "kanban"},
                    {"name": "阻塞项", "type": "grid"},
                    {"name": "最近更新任务", "type": "grid"},
                ],
            },
        },
        "aliases": {
            "chats": {},
            "users": {},
            "calendars": {},
            "doc_folders": {},
            "tables": {},
            "tasklists": {},
        },
    }


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _load_registry_file(target: Path, *, default_payload: dict[str, Any]) -> dict[str, Any]:
    if not target.exists():
        return _json_clone(default_payload)
    with target.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload if isinstance(payload, dict) else _json_clone(default_payload)


def _deep_merge_registry(base: Any, overlay: Any) -> Any:
    if not isinstance(base, dict) or not isinstance(overlay, dict):
        return _json_clone(overlay)
    merged = _json_clone(base)
    for key, value in overlay.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge_registry(merged[key], value)
        else:
            merged[key] = _json_clone(value)
    return merged


def load_static_registry(path: str | Path | None = None, *, env: dict[str, str] | None = None) -> dict[str, Any]:
    target = Path(path) if path else default_registry_path(env)
    return _load_registry_file(target, default_payload=_default_registry_payload())


def load_dynamic_registry(path: str | Path | None = None, *, env: dict[str, str] | None = None) -> dict[str, Any]:
    target = Path(path) if path else default_dynamic_registry_path(env)
    return _load_registry_file(target, default_payload={})


def load_registry(path: str | Path | None = None, *, env: dict[str, str] | None = None) -> dict[str, Any]:
    target = Path(path) if path else default_registry_path(env)
    static_payload = load_static_registry(target, env=env)
    default_target = default_registry_path(env)
    try:
        should_merge_dynamic = target.resolve() == default_target.resolve()
    except FileNotFoundError:
        should_merge_dynamic = str(target) == str(default_target)
    if not should_merge_dynamic:
        return static_payload
    dynamic_payload = load_dynamic_registry(env=env)
    if not dynamic_payload:
        return static_payload
    return _deep_merge_registry(static_payload, dynamic_payload)


def save_registry(payload: dict[str, Any], path: str | Path | None = None, *, env: dict[str, str] | None = None) -> Path:
    target = Path(path) if path else default_registry_path(env)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)
    return target


def save_dynamic_registry(payload: dict[str, Any], path: str | Path | None = None, *, env: dict[str, str] | None = None) -> Path:
    target = Path(path) if path else default_dynamic_registry_path(env)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)
    return target


def registry_dynamic_overlay(payload: dict[str, Any]) -> dict[str, Any]:
    projection = payload.get("projection") if isinstance(payload.get("projection"), dict) else {}
    projection_app = projection.get("app") if isinstance(projection.get("app"), dict) else {}
    projection_tables = projection.get("tables") if isinstance(projection.get("tables"), dict) else {}
    aliases = payload.get("aliases") if isinstance(payload.get("aliases"), dict) else {}
    tables_aliases = aliases.get("tables") if isinstance(aliases.get("tables"), dict) else {}
    overlay: dict[str, Any] = {"projection": {"app": {}, "tables": {}}, "aliases": {"tables": {}}}
    if str(projection_app.get("app_token") or "").strip():
        overlay["projection"]["app"]["app_token"] = str(projection_app.get("app_token") or "").strip()
    if str(projection_app.get("folder_token") or "").strip():
        overlay["projection"]["app"]["folder_token"] = str(projection_app.get("folder_token") or "").strip()
    for table_key, table_cfg in projection_tables.items():
        if not isinstance(table_cfg, dict):
            continue
        entry: dict[str, Any] = {}
        if str(table_cfg.get("table_id") or "").strip():
            entry["table_id"] = str(table_cfg.get("table_id") or "").strip()
        if entry:
            overlay["projection"]["tables"][str(table_key)] = entry
    for alias, value in tables_aliases.items():
        if not isinstance(value, dict):
            continue
        entry: dict[str, Any] = {}
        for key in ("app_token", "table_id"):
            if str(value.get(key) or "").strip():
                entry[key] = str(value.get(key) or "").strip()
        view_ids = value.get("view_ids_by_name")
        if isinstance(view_ids, dict) and view_ids:
            entry["view_ids_by_name"] = {
                str(name): str(view_id)
                for name, view_id in view_ids.items()
                if str(name).strip() and str(view_id).strip()
            }
        if entry:
            overlay["aliases"]["tables"][str(alias)] = entry
    return overlay


def _parse_dt(text: str, *, timezone: str = DEFAULT_TIMEZONE) -> dict[str, str]:
    raw = str(text or "").strip()
    if not raw:
        raise FeishuAgentError("time is required", code="missing_time")
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
            return {"timestamp": str(int(parsed.timestamp())), "timezone": timezone}
        except ValueError:
            continue
    raise FeishuAgentError(
        f"cannot parse datetime: {raw}",
        code="invalid_datetime",
        details={"value": raw},
    )


def _extract_by_patterns(value: str, patterns: list[str]) -> str:
    import re

    text = str(value or "").strip()
    if not text:
        return ""
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return str(match.group(1))
    return ""


def _ensure_dict(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    raise FeishuAgentError("payload must be a JSON object", code="invalid_payload")


def _ensure_list(payload: Any, *, code: str = "invalid_list") -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    raise FeishuAgentError("payload must be a JSON array", code=code)


def _bitable_fields_need_user_fallback(fields: list[dict[str, Any]]) -> bool:
    for field in fields:
        if not isinstance(field, dict):
            continue
        field_type = field.get("type")
        ui_type = str(field.get("ui_type") or "").strip().lower()
        is_single_select = field_type in {3, "3", "select"} or ui_type == "singleselect"
        if not is_single_select:
            continue
        options = _ensure_dict(field.get("property")).get("options")
        if isinstance(options, list) and options:
            continue
        return True
    return False


def _bool_flag(payload: dict[str, Any], key: str, *, default: bool = False) -> bool:
    value = payload.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _build_ssl_context() -> ssl.SSLContext:
    if certifi is not None:
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


def _parse_iso_timestamp(value: str) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return 0.0


def _extract_json_blob(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        payload = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _user_match_score(user: dict[str, Any], query: str) -> int:
    target = str(query or "").strip().lower()
    if not target:
        return 0
    candidates = [
        str(user.get("name") or "").strip(),
        str(user.get("nickname") or "").strip(),
        str(user.get("en_name") or "").strip(),
        str(user.get("email") or "").strip(),
    ]
    best = 0
    for raw in candidates:
        value = raw.lower()
        if not value:
            continue
        if value == target:
            best = max(best, 400)
        elif value.startswith(target):
            best = max(best, 300)
        elif target in value:
            best = max(best, 200)
    return best


class FeishuAgent:
    def __init__(
        self,
        *,
        env: dict[str, str] | None = None,
        registry_path: str | Path | None = None,
        base_url: str = DEFAULT_BASE_URL,
    ) -> None:
        runtime_env = dict(_load_bootstrap_env()) if env is None else {}
        runtime_env.update(dict(os.environ) if env is None else {})
        if env:
            runtime_env.update(env)
        self.env = runtime_env
        self.registry_path = Path(registry_path) if registry_path else default_registry_path(self.env)
        self.dynamic_registry_path = default_dynamic_registry_path(self.env)
        self.registry = load_registry(self.registry_path, env=self.env)
        self.base_url = base_url
        self.app_id = str(self.env.get("FEISHU_APP_ID", "")).strip()
        self.app_secret = str(self.env.get("FEISHU_APP_SECRET", "")).strip()
        self.user_access_token = str(self.env.get("FEISHU_USER_ACCESS_TOKEN", "")).strip()
        self.user_refresh_token = str(self.env.get("FEISHU_USER_REFRESH_TOKEN", "")).strip()
        self.owner_open_id = str(
            self.env.get("FEISHU_OWNER_OPEN_ID")
            or self.registry.get("defaults", {}).get("owner_open_id")
            or ""
        ).strip()
        self.oauth_redirect_uri = str(
            self.env.get("FEISHU_OAUTH_REDIRECT_URI", DEFAULT_OAUTH_REDIRECT_URI)
        ).strip() or DEFAULT_OAUTH_REDIRECT_URI
        suffix = (self.app_id or "default")[-8:]
        self._cache_path = Path(tempfile.gettempdir()) / f".codex_hub_feishu_tok_{suffix}.json"
        self._user_token_store_path = default_user_token_store_path(self.env)
        self._ssl_context = _build_ssl_context()
        self._calendar_cache: list[dict[str, Any]] | None = None

    def _lark_cli_available(self) -> bool:
        return shutil.which("lark-cli") is not None

    def _lark_cli_config_status(self) -> dict[str, Any]:
        if not self._lark_cli_available():
            return {"available": False, "configured": False}
        try:
            proc = subprocess.run(
                ["lark-cli", "config", "show"],
                text=True,
                capture_output=True,
                check=False,
            )
        except Exception as exc:
            return {"available": True, "configured": False, "error": str(exc)}
        payload = _extract_json_blob(proc.stdout)
        if not payload and DEFAULT_LARK_CLI_CONFIG_PATH.exists():
            try:
                config_payload = json.loads(DEFAULT_LARK_CLI_CONFIG_PATH.read_text(encoding="utf-8"))
            except Exception:
                config_payload = {}
            apps = config_payload.get("apps") if isinstance(config_payload, dict) else []
            if isinstance(apps, list) and apps:
                latest = apps[-1] if isinstance(apps[-1], dict) else {}
                if isinstance(latest, dict):
                    payload = {
                        "appId": latest.get("appId"),
                        "brand": latest.get("brand"),
                        "lang": latest.get("lang"),
                    }
        app_id = str(payload.get("appId") or "").strip()
        return {
            "available": True,
            "configured": bool(app_id),
            "app_id": app_id,
            "brand": str(payload.get("brand") or "").strip(),
            "lang": str(payload.get("lang") or "").strip(),
            "config_path": str(DEFAULT_LARK_CLI_CONFIG_PATH),
        }

    def _lark_cli_auth_status(self) -> dict[str, Any]:
        if not self._lark_cli_available():
            return {"available": False, "logged_in": False}
        try:
            proc = subprocess.run(
                ["lark-cli", "auth", "status"],
                text=True,
                capture_output=True,
                check=False,
            )
        except Exception as exc:
            return {"available": True, "logged_in": False, "error": str(exc)}
        payload = _extract_json_blob(proc.stdout)
        identity = str(payload.get("identity") or "").strip()
        user_open_id = str(payload.get("userOpenId") or "").strip()
        return {
            "available": True,
            "logged_in": bool(identity),
            "identity": identity,
            "token_status": str(payload.get("tokenStatus") or "").strip(),
            "user_name": str(payload.get("userName") or "").strip(),
            "user_open_id": user_open_id,
            "app_id": str(payload.get("appId") or "").strip(),
            "scope": str(payload.get("scope") or "").strip(),
            "expires_at": str(payload.get("expiresAt") or "").strip(),
            "refresh_expires_at": str(payload.get("refreshExpiresAt") or "").strip(),
        }

    def _lark_cli_auth_login(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self._lark_cli_available():
            raise FeishuAgentError("lark-cli is not installed", code="lark_cli_missing")
        domains = payload.get("domains") or payload.get("domain") or DEFAULT_LARK_CLI_DOMAINS
        scope = str(payload.get("scope") or "").strip()
        if not scope:
            auth_plan = feishu_capabilities.build_auth_plan()
            scope = str(auth_plan.get("requested_scope_string") or "").strip()
        command = ["lark-cli", "auth", "login"]
        if scope:
            command.extend(["--scope", scope])
        else:
            command.extend(["--domain", str(domains)])
        proc = subprocess.run(command, text=True, capture_output=True, check=False)
        if proc.returncode != 0:
            message = str(proc.stderr or proc.stdout or "lark_cli_auth_login_failed").strip()
            raise FeishuAgentError(message, code="lark_cli_auth_login_failed")
        return self._lark_cli_auth_status()

    # ---- core transport ----

    def _http(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        url = self.base_url + path
        if params:
            query = {key: value for key, value in params.items() if value is not None and value != ""}
            if query:
                url += "?" + urllib.parse.urlencode(query)
        body = json.dumps(data, ensure_ascii=False).encode("utf-8") if data is not None else b""
        headers = {"Content-Type": "application/json", "Content-Length": str(len(body))}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(url, data=body or None, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30, context=self._ssl_context) as response:
                payload = json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="ignore")
            raise FeishuAgentError(
                f"HTTP {exc.code}: {body_text[:500]}",
                code="http_error",
                details={"status": exc.code, "body": body_text[:2000]},
            ) from exc
        except Exception as exc:
            raise FeishuAgentError(
                f"request failed: {exc}",
                code="request_failed",
            ) from exc
        if payload.get("code", 0) != 0:
            raise FeishuAgentError(
                f"API error {payload.get('code')}: {payload.get('msg')}",
                code="api_error",
                details={"response": payload},
            )
        return payload.get("data", payload)

    def _http_multipart(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        files: dict[str, tuple[str, Any, str]] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        url = self.base_url + path
        headers: dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = requests.request(method, url, headers=headers, data=data, files=files, timeout=60)
        except Exception as exc:
            raise FeishuAgentError(f"request failed: {exc}", code="request_failed") from exc
        body_text = response.text
        if response.status_code >= 400:
            raise FeishuAgentError(
                f"HTTP {response.status_code}: {body_text[:500]}",
                code="http_error",
                details={"status": response.status_code, "body": body_text[:2000]},
            )
        try:
            payload = response.json()
        except Exception as exc:
            raise FeishuAgentError(
                f"invalid JSON response: {body_text[:500]}",
                code="invalid_json",
            ) from exc
        if payload.get("code", 0) != 0:
            raise FeishuAgentError(
                f"API error {payload.get('code')}: {payload.get('msg')}",
                code="api_error",
                details={"response": payload},
            )
        return payload.get("data", payload)

    def _token(self) -> str:
        if self._cache_path.exists():
            try:
                cached = json.loads(self._cache_path.read_text(encoding="utf-8"))
                if float(cached.get("expire", 0)) > time.time() + 120:
                    return str(cached["token"])
            except Exception:
                pass
        if not self.app_id or not self.app_secret:
            raise FeishuAgentError(
                "missing FEISHU_APP_ID or FEISHU_APP_SECRET",
                code="missing_credentials",
            )
        payload = self._http(
            "POST",
            "/auth/v3/tenant_access_token/internal",
            data={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        token = str(payload["tenant_access_token"])
        expire = time.time() + int(payload.get("expire", 7200))
        self._cache_path.write_text(json.dumps({"token": token, "expire": expire}), encoding="utf-8")
        return token

    def _oauth_token(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.app_id or not self.app_secret:
            raise FeishuAgentError(
                "missing FEISHU_APP_ID or FEISHU_APP_SECRET",
                code="missing_credentials",
            )
        request_payload = {
            "grant_type": payload.get("grant_type"),
            "client_id": self.app_id,
            "client_secret": self.app_secret,
        }
        for key in ("code", "refresh_token", "redirect_uri"):
            value = str(payload.get(key, "")).strip()
            if value:
                request_payload[key] = value
        return self._http("POST", "/authen/v2/oauth/token", data=request_payload)

    def _oidc_access_token(self, code: str) -> dict[str, Any]:
        target = str(code or "").strip()
        if not target:
            raise FeishuAgentError("authorization code is required", code="missing_authorization_code")
        return self._http(
            "POST",
            "/authen/v1/oidc/access_token",
            data={"grant_type": "authorization_code", "code": target},
            token=self._token(),
        )

    def _oidc_refresh_token(self, refresh_token: str) -> dict[str, Any]:
        target = str(refresh_token or "").strip()
        if not target:
            raise FeishuAgentError("refresh token is required", code="missing_refresh_token")
        return self._http(
            "POST",
            "/authen/v1/oidc/refresh_access_token",
            data={"grant_type": "refresh_token", "refresh_token": target},
            token=self._token(),
        )

    def _load_user_token_store(self) -> dict[str, Any]:
        target = self._user_token_store_path
        if not target.exists():
            return {}
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _save_user_token_store(self, payload: dict[str, Any]) -> dict[str, Any]:
        target = self._user_token_store_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            os.chmod(target, 0o600)
        except OSError:
            pass
        return payload

    def _clear_user_token_store(self) -> None:
        target = self._user_token_store_path
        if target.exists():
            target.unlink()

    def _refresh_user_token(self, refresh_token: str) -> dict[str, Any]:
        result = self._oidc_refresh_token(refresh_token)
        stored = self._normalize_user_token_payload(
            result,
            redirect_uri=self.oauth_redirect_uri,
            previous_refresh_token=refresh_token,
        )
        self._save_user_token_store(stored)
        return stored

    def _normalize_user_token_payload(
        self,
        payload: dict[str, Any],
        *,
        redirect_uri: str,
        previous_refresh_token: str = "",
        authorization_code: str = "",
    ) -> dict[str, Any]:
        now = time.time()
        access_token = str(payload.get("access_token", "")).strip()
        refresh_token = str(payload.get("refresh_token", "")).strip() or str(previous_refresh_token or "").strip()
        expires_in = int(payload.get("expires_in") or payload.get("access_token_expires_in") or 7200)
        refresh_expires_in = int(payload.get("refresh_expires_in") or payload.get("refresh_token_expires_in") or 0)
        token_type = str(payload.get("token_type", "Bearer")).strip() or "Bearer"
        scope = payload.get("scope") or payload.get("scopes") or []
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": token_type,
            "scope": scope,
            "obtained_at": datetime.fromtimestamp(now).isoformat(),
            "access_token_expire_at": datetime.fromtimestamp(now + expires_in).isoformat(),
            "refresh_token_expire_at": datetime.fromtimestamp(now + refresh_expires_in).isoformat() if refresh_expires_in else "",
            "redirect_uri": redirect_uri,
            "authorization_code": str(authorization_code or "").strip(),
            "auth_method": "oidc_v1",
            "profile": payload.get("profile") or {},
        }

    def _user_token(self) -> str:
        token = str(self.user_access_token or "").strip()
        if token:
            return token
        store = self._load_user_token_store()
        access_token = str(store.get("access_token", "")).strip()
        access_expire_at = _parse_iso_timestamp(str(store.get("access_token_expire_at") or ""))
        if access_token and access_expire_at > time.time() + 120:
            return access_token
        refresh_token = str(self.user_refresh_token or store.get("refresh_token") or "").strip()
        if refresh_token:
            try:
                refreshed = self._refresh_user_token(refresh_token)
                refreshed_token = str(refreshed.get("access_token", "")).strip()
                if refreshed_token:
                    return refreshed_token
            except FeishuAgentError:
                pass
        raise FeishuAgentError(
            "task operations require FEISHU_USER_ACCESS_TOKEN after the CoCo app has been granted user-identity task scopes",
            code="missing_user_access_token",
            details={
                "hint": "run `python3 ops/feishu_agent.py auth login` once, or configure FEISHU_USER_ACCESS_TOKEN for the CoCo service"
            },
        )

    def api(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        backend_domain = self._lark_cli_backend_domain_for_path(path)
        if backend_domain and self._can_use_lark_cli_backend(backend_domain):
            try:
                return lark_cli_backend.api_call(method, path, params=params, data=data, identity="user")
            except lark_cli_backend.LarkCliBackendError:
                pass
        return self._http(method, path, data=data, params=params, token=self._token())

    def user_api(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        backend_domain = self._lark_cli_backend_domain_for_path(path)
        if backend_domain and self._can_use_lark_cli_backend(backend_domain):
            try:
                return lark_cli_backend.api_call(method, path, params=params, data=data, identity="user")
            except lark_cli_backend.LarkCliBackendError:
                pass
        return self._http(method, path, data=data, params=params, token=self._user_token())

    # ---- registry / resource resolution ----

    def _aliases(self, key: str) -> dict[str, Any]:
        aliases = self.registry.get("aliases", {})
        value = aliases.get(key, {})
        return value if isinstance(value, dict) else {}

    def _defaults(self) -> dict[str, Any]:
        value = self.registry.get("defaults", {})
        return value if isinstance(value, dict) else {}

    def _match_alias(self, mapping: dict[str, Any], value: str) -> Any:
        text = str(value or "").strip()
        if not text:
            return None
        if text in mapping:
            return mapping[text]
        lowered = text.lower()
        for key, item in mapping.items():
            if key.lower() == lowered:
                return item
        return None

    def resolve_user_id(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            raise FeishuAgentError("user reference is required", code="missing_user")
        if target.startswith("ou_"):
            return target
        alias_value = self._match_alias(self._aliases("users"), target)
        if isinstance(alias_value, dict):
            open_id = str(alias_value.get("open_id", "")).strip()
            if open_id:
                return open_id
            email = str(alias_value.get("email", "")).strip()
            if email:
                target = email
        elif isinstance(alias_value, str) and alias_value.strip():
            target = alias_value.strip()
        if "@" in target:
            payload = self.api(
                "POST",
                "/contact/v3/users/batch_get_id",
                data={"emails": [target]},
                params={"user_id_type": "open_id"},
            )
            user_list = payload.get("user_list", [])
            if user_list:
                user_id = str(user_list[0].get("user_id") or user_list[0].get("open_id") or "").strip()
                if user_id:
                    return user_id
        matches = self._search_users_locally(target, page_size=100)
        if not matches:
            raise FeishuAgentError(f"user not found: {value}", code="user_not_found", details={"value": value})
        open_id = str(matches[0].get("open_id") or "").strip()
        if not open_id:
            raise FeishuAgentError(f"user not found: {value}", code="user_not_found", details={"value": value})
        return open_id

    def resolve_chat_id(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            raise FeishuAgentError("chat reference is required", code="missing_chat")
        if target.startswith("oc_"):
            return target
        alias_value = self._match_alias(self._aliases("chats"), target)
        if isinstance(alias_value, str) and alias_value.strip():
            return alias_value.strip()
        if self._can_use_lark_cli_backend("im"):
            try:
                payload = lark_cli_backend.im_chat_search(query=target, page_size=50, identity="bot")
                candidates = payload.get("chats", [])
                exact = None
                partial = None
                lowered = target.lower()
                for item in candidates:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or item.get("chat_name") or "").strip()
                    chat_id = str(item.get("chat_id") or item.get("id") or "").strip()
                    if not name or not chat_id:
                        continue
                    if name == target or name.lower() == lowered:
                        exact = chat_id
                        break
                    if lowered in name.lower() and partial is None:
                        partial = chat_id
                if exact:
                    return exact
                if partial:
                    return partial
            except lark_cli_backend.LarkCliBackendError:
                pass
        payload = self.api("GET", "/im/v1/chats", params={"page_size": 100})
        candidates = payload.get("items", [])
        exact = None
        partial = None
        lowered = target.lower()
        for item in candidates:
            name = str(item.get("name", "")).strip()
            chat_id = str(item.get("chat_id", "")).strip()
            if not name or not chat_id:
                continue
            if name == target or name.lower() == lowered:
                exact = chat_id
                break
            if lowered in name.lower() and partial is None:
                partial = chat_id
        if exact:
            return exact
        if partial:
            return partial
        raise FeishuAgentError(f"chat not found: {value}", code="chat_not_found", details={"value": value})

    def resolve_calendar_id(self, value: str = "") -> str:
        target = str(value or "").strip()
        if not target or target.lower() == "default":
            target = str(self._defaults().get("calendar_id", "")).strip()
        if not target:
            target = self.default_calendar_id()
        alias_value = self._match_alias(self._aliases("calendars"), target)
        if isinstance(alias_value, str) and alias_value.strip():
            return alias_value.strip()
        if target == "primary":
            return self.default_calendar_id()
        return target

    def _default_personal_calendar_id(self) -> str:
        return str(self._defaults().get("personal_calendar_id") or "").strip()

    def _default_personal_reminder_target(self) -> str:
        return str(self._defaults().get("personal_reminder_target") or "").strip().lower()

    @staticmethod
    def _is_group_calendar_id(calendar_id: str) -> bool:
        return str(calendar_id or "").strip().endswith("@group.calendar.feishu.cn")

    def _should_use_user_calendar_identity(self, calendar_id: str) -> bool:
        target = str(calendar_id or "").strip()
        personal_calendar_id = self._default_personal_calendar_id()
        return bool(personal_calendar_id and target == personal_calendar_id)

    def resolve_folder_token(self, value: str = "") -> str:
        target = str(value or "").strip()
        if not target:
            target = str(self._defaults().get("doc_folder_token", "")).strip()
        alias_value = self._match_alias(self._aliases("doc_folders"), target)
        if isinstance(alias_value, str) and alias_value.strip():
            return alias_value.strip()
        extracted = _extract_by_patterns(
            target,
            [
                r"/folder/([A-Za-z0-9]+)",
                r"folder_token=([A-Za-z0-9]+)",
                r"(fld[A-Za-z0-9]+)",
            ],
        )
        if extracted:
            return extracted
        return target if target.startswith("fld") else ""

    def resolve_document_id(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            raise FeishuAgentError("document reference is required", code="missing_document")
        extracted = _extract_by_patterns(
            target,
            [
                r"/docx/([A-Za-z0-9]+)",
                r"document_id=([A-Za-z0-9]+)",
            ],
        )
        return extracted or target

    def resolve_table_refs(self, app: str = "", table: str = "") -> tuple[str, str]:
        app_ref = str(app or "").strip()
        table_ref = str(table or "").strip()
        app_alias_value = self._match_alias(self._aliases("tables"), app_ref)
        if isinstance(app_alias_value, dict):
            app_ref = str(app_alias_value.get("app_token") or app_alias_value.get("app") or app_ref).strip()
        table_alias_value = self._match_alias(self._aliases("tables"), table_ref)
        if isinstance(table_alias_value, dict):
            app_ref = str(table_alias_value.get("app_token") or table_alias_value.get("app") or app_ref).strip()
            table_ref = str(table_alias_value.get("table_id") or table_alias_value.get("table") or table_ref).strip()
        app_token = _extract_by_patterns(
            app_ref,
            [r"/base/([A-Za-z0-9]+)", r"app_token=([A-Za-z0-9]+)"],
        ) or app_ref
        table_id = _extract_by_patterns(
            table_ref,
            [r"(tbl[A-Za-z0-9]+)", r"table=([A-Za-z0-9_]+)"],
        ) or table_ref
        if not app_token:
            raise FeishuAgentError("bitable app_token is required", code="missing_app_token")
        return app_token, table_id

    def _load_json_payload(self, raw: Any, *, default: Any, code: str) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, Path):
            return json.loads(raw.read_text(encoding="utf-8"))
        text = str(raw).strip()
        if not text:
            return default
        path = Path(text)
        if path.exists() and path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise FeishuAgentError(f"cannot decode json payload: {text}", code=code) from exc

    def save_registry(self) -> Path:
        return save_registry(self.registry, self.registry_path, env=self.env)

    def save_dynamic_registry(self, payload: dict[str, Any]) -> Path:
        return save_dynamic_registry(payload, self.dynamic_registry_path, env=self.env)

    def _normalize_bitable_fields(self, raw: Any) -> list[dict[str, Any]]:
        loaded = self._load_json_payload(raw, default=[], code="invalid_table_fields")
        fields = _ensure_list(loaded, code="invalid_table_fields")
        normalized: list[dict[str, Any]] = []
        for item in fields:
            if isinstance(item, dict):
                field = dict(item)
                if "name" in field and "field_name" not in field:
                    field["field_name"] = field.pop("name")
                field_name = str(field.get("field_name") or "").strip()
                if not field_name:
                    raise FeishuAgentError("field_name is required", code="missing_field_name")
                if "type" not in field:
                    raise FeishuAgentError(f"field type is required for {field_name}", code="missing_field_type")
                normalized.append(field)
                continue
            field_name = str(item or "").strip()
            if not field_name:
                continue
            normalized.append({"field_name": field_name, "type": 1})
        return normalized

    def _resolve_bitable_folder_token(self, payload: dict[str, Any]) -> str:
        explicit = str(payload.get("folder_token") or "").strip()
        if explicit:
            return explicit
        folder = str(payload.get("folder") or "").strip()
        if not folder:
            return ""
        try:
            return self.resolve_folder_token(folder)
        except FeishuAgentError:
            return folder

    def _default_meeting_settings(self) -> dict[str, Any]:
        defaults = self._defaults().get("meeting", {})
        if not isinstance(defaults, dict):
            defaults = {}
        merged = dict(defaults)
        if not str(merged.get("calendar_id") or "").strip():
            personal_calendar_id = self._default_personal_calendar_id()
            if personal_calendar_id:
                merged["calendar_id"] = personal_calendar_id
        if not str(merged.get("timezone") or "").strip():
            merged["timezone"] = DEFAULT_TIMEZONE
        return merged

    def _default_calendar_create_route(self) -> str:
        return str(self._defaults().get("calendar_create_default_route") or "").strip().lower()

    def list_calendars(self) -> list[dict[str, Any]]:
        if self._calendar_cache is None:
            result = self.api("GET", "/calendar/v4/calendars")
            self._calendar_cache = list(result.get("calendar_list") or [])
        return self._calendar_cache

    def default_calendar_id(self) -> str:
        calendars = self.list_calendars()
        if not calendars:
            raise FeishuAgentError("calendar_id is required", code="missing_calendar_id")
        primary = next((item for item in calendars if str(item.get("type") or "").strip() == "primary"), None)
        owner = next((item for item in calendars if str(item.get("role") or "").strip() == "owner"), None)
        chosen = primary or owner or calendars[0]
        calendar_id = str(chosen.get("calendar_id") or "").strip()
        if not calendar_id:
            raise FeishuAgentError("calendar_id is required", code="missing_calendar_id")
        return calendar_id

    def _list_users_for_search(self, *, page_size: int = 100) -> list[dict[str, Any]]:
        result = self.api(
            "GET",
            "/contact/v3/users",
            params={"page_size": int(page_size or 100), "user_id_type": "open_id"},
        )
        items = result.get("items", [])
        return items if isinstance(items, list) else []

    def _search_users_locally(self, query: str, *, page_size: int = 100) -> list[dict[str, Any]]:
        users = self._list_users_for_search(page_size=page_size)
        scored: list[tuple[int, dict[str, Any]]] = []
        for user in users:
            if not isinstance(user, dict):
                continue
            score = _user_match_score(user, query)
            if score <= 0:
                continue
            scored.append((score, user))
        scored.sort(
            key=lambda item: (
                -item[0],
                str(item[1].get("name") or ""),
                str(item[1].get("open_id") or ""),
            )
        )
        return [user for _, user in scored]

    # ---- operations ----

    def _format_cli_message_content(self, item: dict[str, Any]) -> Any:
        body = item.get("body")
        raw = ""
        if isinstance(body, dict):
            raw = str(body.get("content") or "").strip()
        elif body not in (None, ""):
            raw = str(body).strip()
        if not raw:
            raw = str(item.get("content") or "").strip()
        if not raw:
            return ""
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def _normalize_cli_message(self, item: dict[str, Any]) -> dict[str, Any]:
        ts_value = item.get("create_time") or item.get("update_time") or item.get("created_at") or 0
        try:
            ts_ms = int(str(ts_value or "0"))
        except ValueError:
            ts_ms = 0
        if 0 < ts_ms < 1_000_000_000_000:
            ts_ms *= 1000
        sender = item.get("sender")
        sender_id = ""
        if isinstance(sender, dict):
            sender_id = str(sender.get("id") or sender.get("sender_id") or sender.get("open_id") or "").strip()
        if not sender_id:
            sender_id = str(item.get("sender_id") or item.get("open_id") or "").strip()
        return {
            "id": item.get("message_id") or item.get("id"),
            "sender": sender_id,
            "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M") if ts_ms else "",
            "type": item.get("msg_type") or item.get("message_type") or item.get("type"),
            "content": self._format_cli_message_content(item),
        }

    def msg_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        target = ""
        receive_id_type = ""
        if payload.get("email"):
            target = self.resolve_user_id(str(payload["email"]))
            receive_id_type = "open_id"
        elif payload.get("chat"):
            target = self.resolve_chat_id(str(payload["chat"]))
            receive_id_type = "chat_id"
        elif payload.get("to"):
            raw = str(payload["to"]).strip()
            if raw.startswith("oc_"):
                target = raw
                receive_id_type = "chat_id"
            elif raw.startswith("ou_"):
                target = raw
                receive_id_type = "open_id"
            elif "@" in raw:
                target = self.resolve_user_id(raw)
                receive_id_type = "open_id"
            else:
                try:
                    target = self.resolve_chat_id(raw)
                    receive_id_type = "chat_id"
                except FeishuAgentError:
                    target = self.resolve_user_id(raw)
                    receive_id_type = "open_id"
        else:
            raise FeishuAgentError("specify one of to/email/chat", code="missing_target")
        msg_type = str(payload.get("msg_type") or "text").strip() or "text"
        text = ""
        markdown = str(payload.get("markdown") or "").strip()
        image = str(payload.get("image") or "").strip()
        file_attachment = str(payload.get("file") or "").strip()
        audio = str(payload.get("audio") or "").strip()
        video = str(payload.get("video") or "").strip()
        video_cover = str(payload.get("video_cover") or "").strip()
        if msg_type == "interactive":
            card_payload = payload.get("card")
            if card_payload in (None, ""):
                card_payload = payload.get("content")
            if isinstance(card_payload, str):
                try:
                    card_payload = json.loads(card_payload)
                except json.JSONDecodeError as exc:
                    raise FeishuAgentError(
                        f"invalid interactive card json: {exc}",
                        code="invalid_card",
                    ) from exc
            if not isinstance(card_payload, dict):
                raise FeishuAgentError("card is required for interactive message", code="missing_card")
            content = json.dumps(card_payload, ensure_ascii=False)
        else:
            text = str(payload.get("text") or "").strip()
            media_like = bool(markdown or image or audio or video or (msg_type in {"image", "file", "audio", "media"} and file_attachment))
            if not text and file_attachment and not media_like:
                text = Path(str(payload["file"])).read_text(encoding="utf-8")
            if not text and not media_like and msg_type == "text":
                raise FeishuAgentError("text or file is required", code="missing_text")
            content = json.dumps({"text": text}, ensure_ascii=False)
        if self._can_use_lark_cli_backend("im"):
            try:
                cli_result = lark_cli_backend.im_send(
                    chat_id=target if receive_id_type == "chat_id" else "",
                    user_id=target if receive_id_type == "open_id" else "",
                    msg_type=msg_type,
                    text=text if msg_type == "text" else "",
                    content=content if msg_type != "text" else "",
                    markdown=markdown,
                    image=image,
                    file=file_attachment if msg_type == "file" or (file_attachment and not text) else "",
                    audio=audio,
                    video=video,
                    video_cover=video_cover,
                    identity="bot",
                )
                return {
                    "ok": True,
                    "domain": "msg",
                    "action": "send",
                    "message_id": cli_result.get("message_id"),
                    "target": target,
                    "receive_id_type": receive_id_type,
                    "msg_type": msg_type if not markdown else "post",
                    "backend": "lark-cli",
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        if markdown:
            msg_type = "post"
            content = json.dumps({"text": markdown}, ensure_ascii=False)
        elif image:
            msg_type = "image"
            content = json.dumps({"image_key": image}, ensure_ascii=False)
        elif file_attachment and msg_type == "file":
            content = json.dumps({"file_key": file_attachment}, ensure_ascii=False)
        elif audio:
            msg_type = "audio"
            content = json.dumps({"file_key": audio}, ensure_ascii=False)
        elif video:
            msg_type = "media"
            content = json.dumps({"file_key": video, "image_key": video_cover}, ensure_ascii=False)
        result = self.api(
            "POST",
            "/im/v1/messages",
            data={
                "receive_id": target,
                "msg_type": msg_type,
                "content": content,
            },
            params={"receive_id_type": receive_id_type},
        )
        return {
            "ok": True,
            "domain": "msg",
            "action": "send",
            "message_id": result.get("message_id"),
            "target": target,
            "receive_id_type": receive_id_type,
            "msg_type": msg_type,
        }

    def msg_reply(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = str(payload.get("to") or payload.get("message_id") or "").strip()
        if not message_id:
            raise FeishuAgentError("message_id is required", code="missing_message_id")
        msg_type = str(payload.get("msg_type") or "text").strip() or "text"
        markdown = str(payload.get("markdown") or "").strip()
        image = str(payload.get("image") or "").strip()
        file_attachment = str(payload.get("file") or "").strip()
        audio = str(payload.get("audio") or "").strip()
        video = str(payload.get("video") or "").strip()
        video_cover = str(payload.get("video_cover") or "").strip()
        if msg_type == "interactive":
            card_payload = payload.get("card")
            if card_payload in (None, ""):
                card_payload = payload.get("content")
            if isinstance(card_payload, str):
                try:
                    card_payload = json.loads(card_payload)
                except json.JSONDecodeError as exc:
                    raise FeishuAgentError(
                        f"invalid interactive card json: {exc}",
                        code="invalid_card",
                    ) from exc
            if not isinstance(card_payload, dict):
                raise FeishuAgentError("card is required for interactive message", code="missing_card")
            content = json.dumps(card_payload, ensure_ascii=False)
            text = ""
        else:
            text = str(payload.get("text") or "").strip()
            media_like = bool(markdown or image or audio or video or (msg_type in {"image", "file", "audio", "media"} and file_attachment))
            if not text and file_attachment and not media_like:
                text = Path(str(payload["file"])).read_text(encoding="utf-8")
            if not text and not media_like and msg_type == "text":
                raise FeishuAgentError("text or file is required", code="missing_text")
            content = json.dumps({"text": text}, ensure_ascii=False)
        if self._can_use_lark_cli_backend("im"):
            try:
                cli_result = lark_cli_backend.im_reply(
                    message_id=message_id,
                    msg_type=msg_type,
                    text=text if msg_type == "text" else "",
                    content=content if msg_type != "text" else "",
                    markdown=markdown,
                    image=image,
                    file=file_attachment if msg_type == "file" or (file_attachment and not text) else "",
                    audio=audio,
                    video=video,
                    video_cover=video_cover,
                    reply_in_thread=_bool_flag(payload, "reply_in_thread"),
                    identity="bot",
                )
                return {
                    "ok": True,
                    "domain": "msg",
                    "action": "reply",
                    "message_id": cli_result.get("message_id"),
                    "msg_type": msg_type if not markdown else "post",
                    "backend": "lark-cli",
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        if markdown:
            msg_type = "post"
            content = json.dumps({"text": markdown}, ensure_ascii=False)
        elif image:
            msg_type = "image"
            content = json.dumps({"image_key": image}, ensure_ascii=False)
        elif file_attachment and msg_type == "file":
            content = json.dumps({"file_key": file_attachment}, ensure_ascii=False)
        elif audio:
            msg_type = "audio"
            content = json.dumps({"file_key": audio}, ensure_ascii=False)
        elif video:
            msg_type = "media"
            content = json.dumps({"file_key": video, "image_key": video_cover}, ensure_ascii=False)
        result = self.api(
            "POST",
            f"/im/v1/messages/{message_id}/reply",
            data={"msg_type": msg_type, "content": content},
        )
        return {"ok": True, "domain": "msg", "action": "reply", "message_id": result.get("message_id")}

    def msg_history(self, payload: dict[str, Any]) -> dict[str, Any]:
        chat_id = self.resolve_chat_id(str(payload.get("chat") or ""))
        if self._can_use_lark_cli_backend("im"):
            try:
                result = lark_cli_backend.im_chat_messages_list(
                    chat_id=chat_id,
                    page_size=int(payload.get("limit") or 20),
                    identity="bot",
                )
                return {
                    "chat_id": chat_id,
                    "messages": [self._normalize_cli_message(item) for item in result.get("messages", []) if isinstance(item, dict)],
                    "backend": "lark-cli",
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api(
            "GET",
            "/im/v1/messages",
            params={"container_id_type": "chat", "container_id": chat_id, "page_size": int(payload.get("limit") or 20)},
        )
        messages = []
        for item in result.get("items", []):
            try:
                body = json.loads(item.get("body", {}).get("content", "{}"))
            except Exception:
                body = item.get("body", {}).get("content", "")
            ts_ms = int(item.get("create_time", 0) or 0)
            messages.append(
                {
                    "id": item.get("message_id"),
                    "sender": item.get("sender", {}).get("id"),
                    "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M") if ts_ms else "",
                    "type": item.get("msg_type"),
                    "content": body,
                }
            )
        return {"chat_id": chat_id, "messages": messages}

    def msg_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = str(payload.get("query") or "").strip()
        if not query:
            raise FeishuAgentError("query is required", code="missing_query")
        if self._can_use_lark_cli_backend("im"):
            try:
                result = lark_cli_backend.im_messages_search(
                    query=query,
                    page_size=int(payload.get("limit") or 20),
                    identity="user",
                )
                return {"query": query, "messages": result.get("messages", []), "backend": "lark-cli"}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api(
            "POST",
            "/im/v1/messages/search",
            data={"query": query},
            params={"page_size": int(payload.get("limit") or 20)},
        )
        return {"query": query, "messages": result.get("items", [])}

    def msg_chats(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._can_use_lark_cli_backend("im"):
            try:
                result = lark_cli_backend.im_chat_search(
                    query=str(payload.get("query") or "").strip(),
                    page_size=int(payload.get("limit") or 50),
                    identity="bot",
                )
                chats = [
                    {
                        "id": item.get("chat_id") or item.get("id"),
                        "name": item.get("name") or item.get("chat_name"),
                        "type": item.get("chat_type") or item.get("type"),
                        "members": item.get("member_count") or item.get("members"),
                    }
                    for item in result.get("chats", [])
                    if isinstance(item, dict)
                ]
                return {"chats": chats, "backend": "lark-cli"}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("GET", "/im/v1/chats", params={"page_size": int(payload.get("limit") or 50)})
        chats = [
            {
                "id": item.get("chat_id"),
                "name": item.get("name"),
                "type": item.get("chat_type"),
                "members": item.get("member_count"),
            }
            for item in result.get("items", [])
        ]
        return {"chats": chats}

    def msg_download_resources(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("im")
        message_id = str(payload.get("message_id") or payload.get("id") or "").strip()
        file_key = str(payload.get("file_key") or payload.get("key") or "").strip()
        resource_type = str(payload.get("type") or "file").strip().lower()
        if not message_id or not file_key:
            raise FeishuAgentError("message_id and file_key are required", code="missing_resource_locator")
        try:
            return lark_cli_backend.im_download_resources(
                message_id=message_id,
                file_key=file_key,
                resource_type=resource_type,
                output=str(payload.get("output") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "msg_resource_download_failed", details=exc.details) from exc

    def user_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("email"):
            target = str(payload["email"]).strip()
            result = self.api(
                "POST",
                "/contact/v3/users/batch_get_id",
                data={"emails": [target]},
                params={"user_id_type": "open_id"},
            )
            return {"users": result.get("user_list", [])}
        user_id = self.resolve_user_id(str(payload.get("id") or payload.get("user") or ""))
        if self._can_use_lark_cli_backend("contact"):
            try:
                return lark_cli_backend.contact_get(user_id=user_id)
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("GET", f"/contact/v3/users/{user_id}", params={"user_id_type": "open_id"})
        return {"user": result.get("user")}

    def user_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = str(payload.get("name") or payload.get("query") or "").strip()
        if not query:
            raise FeishuAgentError("name is required", code="missing_name")
        limit = int(payload.get("limit") or 20)
        if self._can_use_lark_cli_backend("contact"):
            try:
                result = lark_cli_backend.contact_search(query=query, page_size=limit)
                return {"users": list(result.get("users") or [])[:limit], "backend": "lark-cli"}
            except lark_cli_backend.LarkCliBackendError:
                pass
        return {"users": self._search_users_locally(query, page_size=max(limit, 100))[:limit]}

    def drive_upload(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("drive")
        file_path = str(payload.get("file_path") or payload.get("file") or "").strip()
        if not file_path:
            raise FeishuAgentError("file_path is required", code="missing_file_path")
        folder_token = self.resolve_folder_token(str(payload.get("folder") or payload.get("folder_token") or ""))
        try:
            return lark_cli_backend.drive_upload(
                file_path=file_path,
                folder_token=folder_token,
                name=str(payload.get("name") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "drive_upload_failed", details=exc.details) from exc

    def drive_download(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("drive")
        file_token = str(payload.get("file_token") or payload.get("token") or payload.get("id") or payload.get("doc") or "").strip()
        if not file_token:
            raise FeishuAgentError("file_token is required", code="missing_file_token")
        try:
            return lark_cli_backend.drive_download(
                file_token=file_token,
                output=str(payload.get("output") or "").strip(),
                overwrite=_bool_flag(payload, "overwrite"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "drive_download_failed", details=exc.details) from exc

    def drive_add_comment(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("drive")
        doc = str(payload.get("doc") or payload.get("document") or payload.get("url") or "").strip()
        content = str(payload.get("content") or "").strip()
        if not doc or not content:
            raise FeishuAgentError("doc and content are required", code="missing_comment_target")
        try:
            return lark_cli_backend.drive_add_comment(
                doc=doc,
                content=content,
                block_id=str(payload.get("block_id") or "").strip(),
                selection_with_ellipsis=str(payload.get("selection_with_ellipsis") or "").strip(),
                full_comment=_bool_flag(payload, "full_comment"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "drive_comment_failed", details=exc.details) from exc

    def vc_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("vc")
        try:
            return lark_cli_backend.vc_search(
                query=str(payload.get("query") or "").strip(),
                start=str(payload.get("start") or "").strip(),
                end=str(payload.get("end") or "").strip(),
                organizer_ids=_ensure_list(payload.get("organizer_ids")) if payload.get("organizer_ids") is not None else [],
                participant_ids=_ensure_list(payload.get("participant_ids")) if payload.get("participant_ids") is not None else [],
                room_ids=_ensure_list(payload.get("room_ids")) if payload.get("room_ids") is not None else [],
                page_size=int(payload.get("limit") or payload.get("page_size") or 15),
                page_token=str(payload.get("page_token") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "vc_search_failed", details=exc.details) from exc

    def vc_notes(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("vc")
        try:
            return lark_cli_backend.vc_notes(
                meeting_ids=_ensure_list(payload.get("meeting_ids")) if payload.get("meeting_ids") is not None else [],
                minute_tokens=_ensure_list(payload.get("minute_tokens")) if payload.get("minute_tokens") is not None else [],
                calendar_event_ids=_ensure_list(payload.get("calendar_event_ids")) if payload.get("calendar_event_ids") is not None else [],
                output_dir=str(payload.get("output_dir") or "").strip(),
                overwrite=_bool_flag(payload, "overwrite"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "vc_notes_failed", details=exc.details) from exc

    def minutes_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("minutes")
        minute_token = self.resolve_minutes_token(str(payload.get("minute") or payload.get("minute_token") or payload.get("url") or ""))
        if not minute_token:
            raise FeishuAgentError("minute_token is required", code="missing_minute_token")
        try:
            return lark_cli_backend.minutes_get(minute_token=minute_token, identity=str(payload.get("identity") or "user"))
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "minutes_get_failed", details=exc.details) from exc

    def wiki_get_node(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("wiki")
        token = str(payload.get("token") or payload.get("wiki_token") or payload.get("id") or "").strip()
        if not token:
            raise FeishuAgentError("wiki token is required", code="missing_wiki_token")
        try:
            return lark_cli_backend.wiki_get_node(token=token, identity=str(payload.get("identity") or "user"))
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "wiki_get_node_failed", details=exc.details) from exc

    def sheet_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        title = str(payload.get("title") or payload.get("name") or "").strip()
        if not title:
            raise FeishuAgentError("sheet title is required", code="missing_sheet_title")
        try:
            return lark_cli_backend.sheet_create(
                title=title,
                headers=_ensure_list(payload.get("headers")) if payload.get("headers") is not None else None,
                data=_ensure_list(payload.get("data")) if payload.get("data") is not None else None,
                folder_token=self.resolve_folder_token(str(payload.get("folder") or payload.get("folder_token") or "")),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_create_failed", details=exc.details) from exc

    def sheet_info(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        try:
            return lark_cli_backend.sheet_info(
                spreadsheet_token=self.resolve_sheet_token(str(payload.get("spreadsheet") or payload.get("spreadsheet_token") or "")),
                url=str(payload.get("url") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_info_failed", details=exc.details) from exc

    def sheet_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        try:
            return lark_cli_backend.sheet_read(
                spreadsheet_token=self.resolve_sheet_token(str(payload.get("spreadsheet") or payload.get("spreadsheet_token") or "")),
                url=str(payload.get("url") or "").strip(),
                sheet_id=str(payload.get("sheet_id") or "").strip(),
                range_expr=str(payload.get("range") or "").strip(),
                value_render_option=str(payload.get("value_render_option") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_read_failed", details=exc.details) from exc

    def sheet_write(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        values = _ensure_list(payload.get("values"), code="invalid_sheet_values")
        try:
            return lark_cli_backend.sheet_write(
                values=values,
                spreadsheet_token=self.resolve_sheet_token(str(payload.get("spreadsheet") or payload.get("spreadsheet_token") or "")),
                url=str(payload.get("url") or "").strip(),
                sheet_id=str(payload.get("sheet_id") or "").strip(),
                range_expr=str(payload.get("range") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_write_failed", details=exc.details) from exc

    def sheet_append(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        values = _ensure_list(payload.get("values"), code="invalid_sheet_values")
        try:
            return lark_cli_backend.sheet_append(
                values=values,
                spreadsheet_token=self.resolve_sheet_token(str(payload.get("spreadsheet") or payload.get("spreadsheet_token") or "")),
                url=str(payload.get("url") or "").strip(),
                sheet_id=str(payload.get("sheet_id") or "").strip(),
                range_expr=str(payload.get("range") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_append_failed", details=exc.details) from exc

    def sheet_find(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("sheet")
        text = str(payload.get("text") or payload.get("query") or payload.get("find") or "").strip()
        if not text:
            raise FeishuAgentError("find text is required", code="missing_sheet_find_text")
        try:
            return lark_cli_backend.sheet_find(
                text=text,
                spreadsheet_token=self.resolve_sheet_token(str(payload.get("spreadsheet") or payload.get("spreadsheet_token") or "")),
                url=str(payload.get("url") or "").strip(),
                sheet_id=str(payload.get("sheet_id") or "").strip(),
                range_expr=str(payload.get("range") or "").strip(),
                ignore_case=_bool_flag(payload, "ignore_case"),
                include_formulas=_bool_flag(payload, "include_formulas"),
                match_entire_cell=_bool_flag(payload, "match_entire_cell"),
                search_by_regex=_bool_flag(payload, "search_by_regex"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "sheet_find_failed", details=exc.details) from exc

    def mail_triage(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("mail")
        try:
            return lark_cli_backend.mail_triage(
                query=str(payload.get("query") or "").strip(),
                filter_json=json.dumps(payload.get("filter"), ensure_ascii=False) if isinstance(payload.get("filter"), dict) else str(payload.get("filter") or "").strip(),
                mailbox=str(payload.get("mailbox") or "me").strip() or "me",
                max_count=int(payload.get("limit") or payload.get("max") or 20),
                labels=_bool_flag(payload, "labels"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "mail_triage_failed", details=exc.details) from exc

    def mail_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("mail")
        to = str(payload.get("to") or "").strip()
        subject = str(payload.get("subject") or "").strip()
        body = str(payload.get("body") or "").strip()
        if not to or not subject or not body:
            raise FeishuAgentError("to, subject and body are required", code="missing_mail_fields")
        try:
            return lark_cli_backend.mail_send(
                to=to,
                subject=subject,
                body=body,
                cc=str(payload.get("cc") or "").strip(),
                bcc=str(payload.get("bcc") or "").strip(),
                sender=str(payload.get("from") or "").strip(),
                attach=str(payload.get("attach") or "").strip(),
                inline=str(payload.get("inline") or "").strip(),
                confirm_send=_bool_flag(payload, "confirm_send"),
                plain_text=_bool_flag(payload, "plain_text"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "mail_send_failed", details=exc.details) from exc

    def mail_reply(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("mail")
        message_id = str(payload.get("message_id") or payload.get("id") or "").strip()
        body = str(payload.get("body") or "").strip()
        if not message_id or not body:
            raise FeishuAgentError("message_id and body are required", code="missing_mail_reply_fields")
        try:
            return lark_cli_backend.mail_reply(
                message_id=message_id,
                body=body,
                to=str(payload.get("to") or "").strip(),
                cc=str(payload.get("cc") or "").strip(),
                bcc=str(payload.get("bcc") or "").strip(),
                sender=str(payload.get("from") or "").strip(),
                attach=str(payload.get("attach") or "").strip(),
                inline=str(payload.get("inline") or "").strip(),
                confirm_send=_bool_flag(payload, "confirm_send"),
                plain_text=_bool_flag(payload, "plain_text"),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "mail_reply_failed", details=exc.details) from exc

    def mail_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("mail")
        message_id = str(payload.get("message_id") or payload.get("id") or "").strip()
        if not message_id:
            raise FeishuAgentError("message_id is required", code="missing_mail_message_id")
        try:
            return lark_cli_backend.mail_message(
                message_id=message_id,
                mailbox=str(payload.get("mailbox") or "me").strip() or "me",
                html=_bool_flag(payload, "html", default=True),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "mail_message_failed", details=exc.details) from exc

    def mail_thread(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("mail")
        thread_id = str(payload.get("thread_id") or payload.get("id") or "").strip()
        if not thread_id:
            raise FeishuAgentError("thread_id is required", code="missing_mail_thread_id")
        try:
            return lark_cli_backend.mail_thread(
                thread_id=thread_id,
                mailbox=str(payload.get("mailbox") or "me").strip() or "me",
                html=_bool_flag(payload, "html", default=True),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "mail_thread_failed", details=exc.details) from exc

    def whiteboard_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_lark_cli_backend("whiteboard")
        whiteboard_token = self.resolve_whiteboard_token(str(payload.get("whiteboard") or payload.get("whiteboard_token") or payload.get("id") or ""))
        if not whiteboard_token:
            raise FeishuAgentError("whiteboard token is required", code="missing_whiteboard_token")
        dsl = str(payload.get("dsl") or payload.get("content") or "").strip()
        if not dsl and payload.get("file"):
            dsl = Path(str(payload["file"])).read_text(encoding="utf-8")
        if not dsl:
            raise FeishuAgentError("dsl/content is required", code="missing_whiteboard_dsl")
        try:
            return lark_cli_backend.whiteboard_update(
                whiteboard_token=whiteboard_token,
                dsl=dsl,
                overwrite=_bool_flag(payload, "overwrite"),
                yes=_bool_flag(payload, "yes"),
                idempotent_token=str(payload.get("idempotent_token") or "").strip(),
                identity=str(payload.get("identity") or "user"),
            )
        except lark_cli_backend.LarkCliBackendError as exc:
            raise FeishuAgentError(str(exc), code=exc.code or "whiteboard_update_failed", details=exc.details) from exc

    def auth_status(self, _payload: dict[str, Any]) -> dict[str, Any]:
        store = self._load_user_token_store()
        access_token = str(self.user_access_token or store.get("access_token") or "").strip()
        refresh_token = str(self.user_refresh_token or store.get("refresh_token") or "").strip()
        access_expire_at = str(store.get("access_token_expire_at") or "").strip()
        refresh_expire_at = str(store.get("refresh_token_expire_at") or "").strip()
        auth_method = str(store.get("auth_method") or "").strip()
        lark_cli_config = self._lark_cli_config_status()
        lark_cli_auth = self._lark_cli_auth_status()
        lark_cli_user_logged_in = bool(
            lark_cli_config.get("configured")
            and lark_cli_auth.get("logged_in")
            and str(lark_cli_auth.get("identity") or "").strip() == "user"
        )
        effective_user_auth_ready = bool(access_token or lark_cli_user_logged_in)
        bridge_credentials_ready = bool(self.app_id and self.app_secret)
        object_ops_ready = bool(lark_cli_config.get("configured") and effective_user_auth_ready)
        capability_state = feishu_capabilities.evaluate_capabilities(
            granted_scopes=lark_cli_auth.get("scope"),
            lark_cli_configured=bool(lark_cli_config.get("configured")),
            user_auth_ready=effective_user_auth_ready,
            bridge_credentials_ready=bridge_credentials_ready,
        )
        next_steps: list[str] = []
        if not lark_cli_config.get("configured"):
            next_steps.append("Run `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app`.")
        elif not effective_user_auth_ready:
            next_steps.append("Complete the Feishu user login flow until `object_ops_ready=true`.")
        if object_ops_ready and not bridge_credentials_ready:
            next_steps.append("Sync `FEISHU_APP_SECRET` into `ops/feishu_bridge.env.local` and rerun setup.")
        if capability_state["missing_requested_scopes"]:
            next_steps.append(
                "Core Feishu capability scopes are still missing. Run `lark-cli auth login --scope \""
                + " ".join(capability_state["missing_requested_scopes"])
                + "\"`."
            )
        for capability_id in capability_state["feature_specific_pending_capabilities"][:3]:
            capability_payload = capability_state["capabilities"].get(capability_id) or {}
            auth_command = str(capability_payload.get("auth_command") or "").strip()
            if auth_command:
                next_steps.append(f"{capability_payload.get('label')}: run `{auth_command}`.")
        return {
            "configured": bridge_credentials_ready,
            "bridge_credentials_ready": bridge_credentials_ready,
            "has_user_access_token": bool(access_token),
            "has_refresh_token": bool(refresh_token),
            "auto_refresh_ready": bool(refresh_token),
            "auth_method": auth_method,
            "redirect_uri": str(store.get("redirect_uri") or self.oauth_redirect_uri),
            "token_store_path": str(self._user_token_store_path),
            "access_token_expire_at": access_expire_at,
            "refresh_token_expire_at": refresh_expire_at,
            "profile": store.get("profile") or {},
            "lark_cli": lark_cli_config,
            "lark_cli_auth": lark_cli_auth,
            "lark_cli_user_logged_in": lark_cli_user_logged_in,
            "effective_user_auth_ready": effective_user_auth_ready,
            "object_ops_ready": object_ops_ready,
            "coco_bridge_ready": bridge_credentials_ready,
            "full_ready": bool(object_ops_ready and bridge_credentials_ready),
            "auth_plan": capability_state["auth_plan"],
            "auth_plan_ready": capability_state["auth_plan_ready"],
            "missing_requested_scopes": capability_state["missing_requested_scopes"],
            "capabilities": capability_state["capabilities"],
            "feature_specific_pending_capabilities": capability_state["feature_specific_pending_capabilities"],
            "next_steps": next_steps,
        }

    def auth_clear(self, _payload: dict[str, Any]) -> dict[str, Any]:
        self._clear_user_token_store()
        return {"ok": True, "token_store_path": str(self._user_token_store_path)}

    def _fetch_user_info(self, access_token: str) -> dict[str, Any]:
        payload = self._http("GET", "/authen/v1/user_info", token=access_token)
        return payload if isinstance(payload, dict) else {}

    def auth_login(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not self.app_id or not self.app_secret:
            status = self._lark_cli_auth_login(payload)
            return {
                "ok": True,
                "auth_method": "lark-cli",
                "identity": status.get("identity"),
                "user_open_id": status.get("user_open_id"),
                "user_name": status.get("user_name"),
                "status": self.auth_status({}),
            }
        redirect_uri = str(payload.get("redirect_uri") or self.oauth_redirect_uri or DEFAULT_OAUTH_REDIRECT_URI).strip()
        parsed_redirect = urllib.parse.urlparse(redirect_uri)
        if parsed_redirect.scheme not in {"http", "https"}:
            raise FeishuAgentError("redirect_uri must be http/https", code="invalid_redirect_uri")
        host = parsed_redirect.hostname or "127.0.0.1"
        port = parsed_redirect.port or DEFAULT_OAUTH_PORT
        callback_path = parsed_redirect.path or DEFAULT_OAUTH_PATH
        timeout_seconds = int(payload.get("timeout") or 180)
        should_open_browser = str(payload.get("open_browser", "true")).strip().lower() != "false"
        state = str(payload.get("state") or f"codex-hub-{int(time.time())}").strip()
        event = threading.Event()
        auth_result: dict[str, str] = {}
        auth_plan = feishu_capabilities.build_auth_plan()
        configured_scopes = (
            payload.get("scope")
            or payload.get("scopes")
            or auth_plan.get("requested_scopes")
            or self._defaults().get("oauth_scopes")
            or []
        )
        if isinstance(configured_scopes, str):
            scopes = [item.strip() for item in configured_scopes.replace(",", " ").split() if item.strip()]
        elif isinstance(configured_scopes, list):
            scopes = [str(item).strip() for item in configured_scopes if str(item).strip()]
        else:
            scopes = []

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                parts = urllib.parse.urlparse(self.path)
                if parts.path != callback_path:
                    self.send_response(404)
                    self.end_headers()
                    return
                params = urllib.parse.parse_qs(parts.query)
                auth_result["code"] = str((params.get("code") or [""])[0]).strip()
                auth_result["state"] = str((params.get("state") or [""])[0]).strip()
                auth_result["error"] = str((params.get("error") or [""])[0]).strip()
                auth_result["error_description"] = str((params.get("error_description") or [""])[0]).strip()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    "<html><body><h3>CoCo Feishu 授权已收到</h3><p>可以关闭此页面，返回 Codex。</p></body></html>".encode(
                        "utf-8"
                    )
                )
                event.set()

            def log_message(self, _format: str, *_args) -> None:  # noqa: A003
                return

        server = HTTPServer((host, port), CallbackHandler)
        worker = threading.Thread(target=server.handle_request, daemon=True)
        worker.start()
        authorize_query = urllib.parse.urlencode(
            {
                "app_id": self.app_id,
                "redirect_uri": redirect_uri,
                "state": state,
                **({"scope": " ".join(scopes)} if scopes else {}),
            }
        )
        authorize_url = f"{DEFAULT_AUTHORIZE_URL}?{authorize_query}"
        if should_open_browser:
            webbrowser.open(authorize_url)
        event.wait(timeout_seconds)
        server.server_close()
        if not event.is_set():
            raise FeishuAgentError(
                "timed out waiting for Feishu OAuth callback",
                code="oauth_timeout",
                details={"authorize_url": authorize_url, "redirect_uri": redirect_uri},
            )
        if auth_result.get("error"):
            raise FeishuAgentError(
                auth_result.get("error_description") or auth_result["error"],
                code="oauth_error",
                details={"authorize_url": authorize_url, "redirect_uri": redirect_uri},
            )
        code = auth_result.get("code", "")
        if not code:
            raise FeishuAgentError(
                "Feishu OAuth callback missing code",
                code="oauth_missing_code",
                details={"authorize_url": authorize_url, "redirect_uri": redirect_uri},
            )
        token_payload = self._oidc_access_token(code)
        access_token = str(token_payload.get("access_token", "")).strip()
        if not access_token:
            raise FeishuAgentError("OAuth token response missing access_token", code="oauth_invalid_response")
        profile = self._fetch_user_info(access_token)
        stored = self._normalize_user_token_payload(
            {**token_payload, "profile": profile},
            redirect_uri=redirect_uri,
            authorization_code=code,
        )
        self._save_user_token_store(stored)
        return {
            "ok": True,
            "auth_method": "codex-hub-oauth",
            "authorize_url": authorize_url,
            "redirect_uri": redirect_uri,
            "scopes": scopes,
            "auth_plan": auth_plan,
            "token_store_path": str(self._user_token_store_path),
            "profile": profile,
            "access_token_expire_at": stored.get("access_token_expire_at", ""),
            "refresh_token_expire_at": stored.get("refresh_token_expire_at", ""),
            "status": self.auth_status({}),
        }

    def _md_to_blocks(self, text: str) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        for line in text.splitlines():
            item = line.rstrip()
            if item.startswith("### "):
                blocks.append({"block_type": 5, "heading3": {"style": {}, "elements": [{"text_run": {"content": item[4:], "text_element_style": {}}}]}})
            elif item.startswith("## "):
                blocks.append({"block_type": 4, "heading2": {"style": {}, "elements": [{"text_run": {"content": item[3:], "text_element_style": {}}}]}})
            elif item.startswith("# "):
                blocks.append({"block_type": 3, "heading1": {"style": {}, "elements": [{"text_run": {"content": item[2:], "text_element_style": {}}}]}})
            elif item.startswith("- ") or item.startswith("* "):
                blocks.append({"block_type": 12, "bullet": {"style": {}, "elements": [{"text_run": {"content": item[2:], "text_element_style": {}}}]}})
            elif item and item[0].isdigit() and ". " in item[:4]:
                blocks.append({"block_type": 13, "ordered": {"style": {}, "elements": [{"text_run": {"content": item.split('. ', 1)[1], "text_element_style": {}}}]}})
            elif item:
                blocks.append({"block_type": 2, "text": {"style": {}, "elements": [{"text_run": {"content": item, "text_element_style": {}}}]}})
        return blocks

    def _write_doc_blocks(self, document_id: str, blocks: list[dict[str, Any]]) -> None:
        for index in range(0, len(blocks), 50):
            self.api(
                "POST",
                f"/docx/v1/documents/{document_id}/blocks/{document_id}/children",
                data={"children": blocks[index : index + 50], "index": index},
            )

    def _create_doc_image_block(self, document_id: str, *, index: int = 0) -> dict[str, Any]:
        result = self.api(
            "POST",
            f"/docx/v1/documents/{document_id}/blocks/{document_id}/children",
            data={"children": [{"block_type": 27, "image": {}}], "index": index},
        )
        children = _ensure_list(result.get("children"))
        if not children:
            raise FeishuAgentError("doc image block creation returned no children", code="doc_image_block_missing")
        return _ensure_dict(children[0])

    def _upload_doc_image(self, block_id: str, file_path: Path) -> str:
        mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        with file_path.open("rb") as handle:
            result = self._http_multipart(
                "POST",
                "/drive/v1/medias/upload_all",
                token=self._token(),
                data={
                    "file_name": file_path.name,
                    "parent_type": "docx_image",
                    "parent_node": block_id,
                    "size": str(file_path.stat().st_size),
                },
                files={"file": (file_path.name, handle, mime_type)},
            )
        file_token = str(result.get("file_token") or "").strip()
        if not file_token:
            raise FeishuAgentError("doc image upload missing file_token", code="doc_image_upload_failed")
        return file_token

    def _replace_doc_image(self, document_id: str, block_id: str, file_token: str) -> dict[str, Any]:
        result = self.api(
            "PATCH",
            f"/docx/v1/documents/{document_id}/blocks/{block_id}",
            data={"replace_image": {"token": file_token}},
        )
        return _ensure_dict(result.get("block"))

    def _grant_doc_permission(self, document_id: str, open_id: str) -> None:
        if not open_id:
            return
        try:
            self.api(
                "POST",
                f"/drive/v1/permissions/{document_id}/members",
                data={"member_type": "openid", "member_id": open_id, "perm": "full_access"},
                params={"type": "docx", "need_notification": "false"},
            )
        except FeishuAgentError:
            return

    def _resolved_share_target(self, share_to: str) -> str:
        raw = str(share_to or "").strip()
        if not raw:
            return ""
        try:
            return self.resolve_user_id(raw)
        except FeishuAgentError:
            return raw

    def _can_use_lark_cli_doc_backend(self, *, share_to: str = "") -> bool:
        if not lark_cli_backend.doc_backend_enabled(self.env):
            return False
        normalized_share = self._resolved_share_target(share_to)
        if not normalized_share:
            return True
        return normalized_share == self.owner_open_id

    def _can_use_lark_cli_backend(self, domain: str) -> bool:
        return lark_cli_backend.backend_enabled(domain, self.env)

    def _lark_cli_backend_domain_for_path(self, path: str) -> str:
        target = str(path or "").strip()
        if target.startswith("/bitable/"):
            return "table"
        if target.startswith("/task/"):
            return "task"
        if target.startswith("/calendar/"):
            return "calendar"
        return ""

    def _require_lark_cli_backend(self, domain: str) -> None:
        if not self._can_use_lark_cli_backend(domain):
            raise FeishuAgentError(
                f"{domain} operations require the official lark-cli backend",
                code=f"{domain}_backend_unavailable",
            )

    def resolve_minutes_token(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            return ""
        extracted = _extract_by_patterns(
            target,
            [
                r"/minutes/([A-Za-z0-9_-]+)",
                r"minute[_-]?token=([A-Za-z0-9_-]+)",
            ],
        )
        return extracted or target

    def resolve_whiteboard_token(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            return ""
        extracted = _extract_by_patterns(
            target,
            [
                r"/whiteboard/([A-Za-z0-9_-]+)",
                r"whiteboard[_-]?token=([A-Za-z0-9_-]+)",
            ],
        )
        return extracted or target

    def resolve_sheet_token(self, value: str) -> str:
        target = str(value or "").strip()
        if not target:
            return ""
        extracted = _extract_by_patterns(
            target,
            [
                r"/sheets/([A-Za-z0-9_-]+)",
                r"spreadsheet[_-]?token=([A-Za-z0-9_-]+)",
            ],
        )
        return extracted or target

    def doc_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        folder_token = self.resolve_folder_token(str(payload.get("folder") or ""))
        create_payload: dict[str, Any] = {"title": str(payload.get("title") or "新文档")}
        if folder_token:
            create_payload["folder_token"] = folder_token
        content = str(payload.get("content") or "").strip()
        if not content and payload.get("file"):
            content = Path(str(payload["file"])).read_text(encoding="utf-8")
        share_to = str(payload.get("share_to") or self.owner_open_id or "").strip()
        if self._can_use_lark_cli_doc_backend(share_to=share_to):
            try:
                return lark_cli_backend.doc_create(
                    title=str(create_payload.get("title") or "新文档"),
                    content=content,
                    file_path=str(payload.get("file") or "").strip(),
                    folder_token=folder_token,
                )
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("POST", "/docx/v1/documents", data=create_payload)
        document = result.get("document", {})
        document_id = str(document.get("document_id") or "").strip()
        if content and document_id:
            blocks = self._md_to_blocks(content)
            if blocks:
                self._write_doc_blocks(document_id, blocks)
        if document_id and share_to:
            target_user = self.resolve_user_id(share_to)
            self._grant_doc_permission(document_id, target_user)
        return {
            "ok": True,
            "document_id": document_id,
            "url": document.get("document_uri") or f"https://feishu.cn/docx/{document_id}",
        }

    def doc_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        document_id = self.resolve_document_id(str(payload.get("id") or payload.get("document") or payload.get("url") or ""))
        if self._can_use_lark_cli_doc_backend():
            try:
                return lark_cli_backend.doc_fetch(document=document_id)
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("GET", f"/docx/v1/documents/{document_id}/raw_content", params={"lang": 0})
        return {"document_id": document_id, "content": result.get("content")}

    def doc_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        folder_token = self.resolve_folder_token(str(payload.get("folder") or ""))
        if self._can_use_lark_cli_doc_backend():
            try:
                result = lark_cli_backend.doc_list(
                    folder_token=folder_token,
                    page_size=int(payload.get("limit") or 50),
                )
                return {
                    "files": list(result.get("files") or []),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        params: dict[str, Any] = {
            "page_size": int(payload.get("limit") or 50),
            "order_by": "EditedTime",
            "direction": "DESC",
        }
        if folder_token:
            params["folder_token"] = folder_token
        result = self.api("GET", "/drive/v1/files", params=params)
        return {"files": result.get("files", [])}

    def doc_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = str(payload.get("query") or payload.get("name") or "").strip()
        if not query:
            raise FeishuAgentError("query is required", code="missing_query")
        limit = int(payload.get("limit") or 15)
        if self._can_use_lark_cli_doc_backend():
            try:
                result = lark_cli_backend.doc_search(
                    query=query,
                    page_size=limit,
                    page_token=str(payload.get("page_token") or "").strip(),
                )
                return {
                    "files": list(result.get("files") or [])[:limit],
                    "page_token": result.get("page_token") or "",
                    "has_more": bool(result.get("has_more")),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        raise FeishuAgentError(
            "doc search requires the official lark-cli user-auth backend",
            code="doc_search_unavailable",
            details={"query": query},
        )

    def doc_insert_image(self, payload: dict[str, Any]) -> dict[str, Any]:
        document_id = self.resolve_document_id(str(payload.get("id") or payload.get("document") or payload.get("url") or ""))
        raw_file_path = str(payload.get("file_path") or payload.get("file") or "").strip()
        if not raw_file_path:
            raise FeishuAgentError("file_path is required", code="missing_file_path")
        file_path = Path(raw_file_path)
        if not file_path.exists():
            raise FeishuAgentError("image file does not exist", code="missing_image_file", details={"file_path": str(file_path)})
        if self._can_use_lark_cli_doc_backend():
            try:
                return lark_cli_backend.doc_insert_image(document=document_id, file_path=str(file_path))
            except lark_cli_backend.LarkCliBackendError:
                pass
        index = int(payload.get("index") or 0)
        block = self._create_doc_image_block(document_id, index=index)
        block_id = str(block.get("block_id") or "").strip()
        if not block_id:
            raise FeishuAgentError("doc image block missing block_id", code="doc_image_block_missing")
        file_token = self._upload_doc_image(block_id, file_path)
        updated = self._replace_doc_image(document_id, block_id, file_token)
        return {
            "ok": True,
            "document_id": document_id,
            "block_id": block_id,
            "file_token": file_token,
            "file_path": str(file_path),
            "url": f"https://feishu.cn/docx/{document_id}",
            "block": updated,
        }

    def table_records(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        if self._can_use_lark_cli_backend("table") and not payload.get("filter") and not payload.get("sort"):
            raw_offset = payload.get("offset")
            if raw_offset is None:
                raw_offset = payload.get("page_token")
            try:
                offset = int(raw_offset or 0)
            except (TypeError, ValueError):
                offset = 0
            view_id = str(payload.get("view") or payload.get("view_id") or "").strip()
            try:
                result = lark_cli_backend.base_record_list(
                    base_token=app_token,
                    table_id=table_id,
                    limit=int(payload.get("limit") or 100),
                    offset=offset,
                    view_id=view_id,
                )
                return {
                    "app_token": app_token,
                    "table_id": table_id,
                    "records": list(result.get("records") or []),
                    "total": result.get("total"),
                    "fields": list(result.get("fields") or []),
                    "record_id_list": list(result.get("record_id_list") or []),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        params: dict[str, Any] = {"page_size": int(payload.get("limit") or 100)}
        if payload.get("filter"):
            params["filter"] = payload["filter"]
        if payload.get("sort"):
            params["sort"] = payload["sort"]
        if payload.get("page_token"):
            params["page_token"] = str(payload["page_token"])
        result = self.api("GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records", params=params)
        return {
            "app_token": app_token,
            "table_id": table_id,
            "records": list(result.get("items") or []),
            "total": result.get("total"),
            "has_more": bool(result.get("has_more", False)),
            "page_token": result.get("page_token") or "",
        }

    def table_add(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        raw = payload.get("data")
        if raw is None and payload.get("file"):
            raw = json.loads(Path(str(payload["file"])).read_text(encoding="utf-8"))
        fields = raw if isinstance(raw, dict) else json.loads(str(raw or "{}"))
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_record_upsert(
                    base_token=app_token,
                    table_id=table_id,
                    fields=fields,
                )
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table_id,
                    "record_id": result.get("record_id"),
                    "record": result.get("record"),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("POST", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records", data={"fields": fields})
        return {"ok": True, "app_token": app_token, "table_id": table_id, "record_id": result.get("record", {}).get("record_id")}

    def table_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        record_id = str(payload.get("record") or payload.get("record_id") or "").strip()
        if not table_id or not record_id:
            raise FeishuAgentError("table and record are required", code="missing_table_or_record")
        raw = payload.get("data")
        fields = raw if isinstance(raw, dict) else json.loads(str(raw or "{}"))
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_record_upsert(
                    base_token=app_token,
                    table_id=table_id,
                    record_id=record_id,
                    fields=fields,
                )
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table_id,
                    "record": result.get("record"),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("PUT", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}", data={"fields": fields})
        return {"ok": True, "record": result.get("record")}

    def table_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        record_id = str(payload.get("record") or payload.get("record_id") or "").strip()
        if not table_id or not record_id:
            raise FeishuAgentError("table and record are required", code="missing_table_or_record")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_record_delete(
                    base_token=app_token,
                    table_id=table_id,
                    record_id=record_id,
                )
                return {"ok": True, "record_id": record_id, "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}")
        return {"ok": True}

    def table_tables(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, _table_id = self.resolve_table_refs(str(payload.get("app") or ""), "")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_table_list(
                    base_token=app_token,
                    limit=int(payload.get("limit") or 50),
                    offset=int(payload.get("offset") or 0),
                )
                return {
                    "app_token": app_token,
                    "tables": list(result.get("tables") or []),
                    "total": result.get("total"),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("GET", f"/bitable/v1/apps/{app_token}/tables", params={"page_size": 50})
        return {"app_token": app_token, "tables": list(result.get("items") or [])}

    def table_get_app(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, _table_id = self.resolve_table_refs(str(payload.get("app") or ""), "")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_get(base_token=app_token)
                return {"app_token": app_token, "app": _ensure_dict(result.get("base")), "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api("GET", f"/bitable/v1/apps/{app_token}")
        return {"app_token": app_token, "app": _ensure_dict(result.get("app"))}

    def table_delete_table(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_table_delete(base_token=app_token, table_id=table_id)
                return {"ok": True, "app_token": app_token, "table_id": table_id, "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}")
        return {"ok": True, "app_token": app_token, "table_id": table_id}

    def table_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_field_list(
                    base_token=app_token,
                    table_id=table_id,
                    limit=int(payload.get("limit") or 100),
                    offset=int(payload.get("offset") or 0),
                )
                fields = list(result.get("fields") or [])
                if not _bitable_fields_need_user_fallback(fields):
                    return {
                        "app_token": app_token,
                        "table_id": table_id,
                        "fields": fields,
                        "total": result.get("total"),
                        "backend": result.get("backend"),
                    }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.api(
            "GET",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields",
            params={"page_size": 100},
        )
        return {"app_token": app_token, "table_id": table_id, "fields": list(result.get("items") or [])}

    def table_update_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        field_id = str(payload.get("field_id") or payload.get("field") or "").strip()
        if not table_id or not field_id:
            raise FeishuAgentError("table and field are required", code="missing_table_or_field")
        data: dict[str, Any] = {}
        if payload.get("field_name") or payload.get("name"):
            data["field_name"] = str(payload.get("field_name") or payload.get("name") or "").strip()
        if payload.get("property") is not None:
            data["property"] = _json_clone(payload.get("property"))
        if payload.get("description") is not None:
            data["description"] = str(payload.get("description") or "")
        if not data:
            raise FeishuAgentError("field update payload is empty", code="missing_field_update")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_field_update(
                    base_token=app_token,
                    table_id=table_id,
                    field_id=field_id,
                    field=data,
                )
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table_id,
                    "field": _ensure_dict(result.get("field")),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api(
            "PUT",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}",
            data=data,
        )
        return {"ok": True, "app_token": app_token, "table_id": table_id, "field": _ensure_dict(result.get("field"))}

    def table_delete_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        field_id = str(payload.get("field_id") or payload.get("field") or "").strip()
        if not table_id or not field_id:
            raise FeishuAgentError("table and field are required", code="missing_table_or_field")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_field_delete(base_token=app_token, table_id=table_id, field_id=field_id)
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table_id,
                    "field_id": field_id,
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}")
        return {"ok": True, "app_token": app_token, "table_id": table_id, "field_id": field_id}

    def table_views(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_view_list(
                    base_token=app_token,
                    table_id=table_id,
                    limit=int(payload.get("limit") or 100),
                    offset=int(payload.get("offset") or 0),
                )
                return {
                    "app_token": app_token,
                    "table_id": table_id,
                    "views": list(result.get("views") or []),
                    "total": result.get("total"),
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api(
            "GET",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/views",
            params={"page_size": int(payload.get("limit") or 100)},
        )
        return {"app_token": app_token, "table_id": table_id, "views": list(result.get("items") or [])}

    def table_get_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        view_id = str(payload.get("view") or payload.get("view_id") or "").strip()
        if not table_id or not view_id:
            raise FeishuAgentError("table and view are required", code="missing_table_or_view")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_view_get(base_token=app_token, table_id=table_id, view_id=view_id)
                return {"app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view")), "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("GET", f"/bitable/v1/apps/{app_token}/tables/{table_id}/views/{view_id}")
        return {"app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view"))}

    def table_create_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        view_name = str(payload.get("name") or payload.get("view_name") or "").strip()
        if not view_name:
            raise FeishuAgentError("view name is required", code="missing_view_name")
        view_type = str(payload.get("type") or payload.get("view_type") or "grid").strip() or "grid"
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_view_create(
                    base_token=app_token,
                    table_id=table_id,
                    view={"view_name": view_name, "view_type": view_type},
                )
                return {"ok": True, "app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view")), "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api(
            "POST",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/views",
            data={"view_name": view_name, "view_type": view_type},
        )
        return {"ok": True, "app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view"))}

    def table_update_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        view_id = str(payload.get("view") or payload.get("view_id") or "").strip()
        if not table_id or not view_id:
            raise FeishuAgentError("table and view are required", code="missing_table_or_view")
        data: dict[str, Any] = {}
        if payload.get("name") or payload.get("view_name"):
            data["view_name"] = str(payload.get("name") or payload.get("view_name") or "").strip()
        if payload.get("type") or payload.get("view_type"):
            data["view_type"] = str(payload.get("type") or payload.get("view_type") or "").strip()
        if payload.get("property") is not None:
            data["property"] = _json_clone(payload.get("property"))
        if not data:
            raise FeishuAgentError("view update payload is empty", code="missing_view_update")
        if self._can_use_lark_cli_backend("table") and set(data.keys()).issubset({"view_name"}):
            try:
                result = lark_cli_backend.base_view_update(
                    base_token=app_token,
                    table_id=table_id,
                    view_id=view_id,
                    name=str(data.get("view_name") or "").strip(),
                )
                return {"ok": True, "app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view")), "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api(
            "PATCH",
            f"/bitable/v1/apps/{app_token}/tables/{table_id}/views/{view_id}",
            data=data,
        )
        return {"ok": True, "app_token": app_token, "table_id": table_id, "view": _ensure_dict(result.get("view"))}

    def table_delete_view(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        view_id = str(payload.get("view") or payload.get("view_id") or "").strip()
        if not table_id or not view_id:
            raise FeishuAgentError("table and view are required", code="missing_table_or_view")
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_view_delete(base_token=app_token, table_id=table_id, view_id=view_id)
                return {"ok": True, "app_token": app_token, "table_id": table_id, "view_id": view_id, "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}/views/{view_id}")
        return {"ok": True, "app_token": app_token, "table_id": table_id, "view_id": view_id}

    def table_create_app(self, payload: dict[str, Any]) -> dict[str, Any]:
        name = str(payload.get("name") or payload.get("title") or "").strip()
        if not name:
            raise FeishuAgentError("app name is required", code="missing_app_name")
        data: dict[str, Any] = {"name": name}
        timezone = str(payload.get("timezone") or DEFAULT_TIMEZONE).strip()
        if timezone:
            data["time_zone"] = timezone
        folder_token = self._resolve_bitable_folder_token(payload)
        if folder_token:
            data["folder_token"] = folder_token
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_app_create(
                    name=name,
                    time_zone=timezone,
                    folder_token=folder_token,
                )
                app = _ensure_dict(result.get("app"))
                response: dict[str, Any] = {
                    "ok": True,
                    "app_token": app.get("app_token"),
                    "default_table_id": app.get("default_table_id"),
                    "url": app.get("url"),
                    "app": app,
                    "backend": result.get("backend"),
                }
                table_name = str(payload.get("table_name") or payload.get("table") or "").strip() or "表1"
                response["table"] = {
                    "table_id": app.get("default_table_id"),
                    "name": table_name,
                    "default_view_name": str(payload.get("default_view_name") or "").strip() or "默认视图",
                    "fields": self._normalize_bitable_fields(payload.get("fields")) if payload.get("fields") is not None else [],
                }
                return response
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("POST", "/bitable/v1/apps", data=data)
        app = _ensure_dict(result.get("app"))
        table_name = str(payload.get("table_name") or payload.get("table") or "").strip() or "表1"
        response: dict[str, Any] = {
            "ok": True,
            "app_token": app.get("app_token"),
            "default_table_id": app.get("default_table_id"),
            "url": app.get("url"),
            "app": app,
        }
        response["table"] = {
            "table_id": app.get("default_table_id"),
            "name": table_name,
            "default_view_name": str(payload.get("default_view_name") or "").strip() or "默认视图",
            "fields": self._normalize_bitable_fields(payload.get("fields")) if payload.get("fields") is not None else [],
        }
        return response

    def table_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, _table_id = self.resolve_table_refs(str(payload.get("app") or ""), "")
        name = str(payload.get("name") or payload.get("table_name") or "").strip()
        if not name:
            raise FeishuAgentError("table name is required", code="missing_table_name")
        table_data: dict[str, Any] = {"name": name}
        default_view_name = str(payload.get("default_view_name") or "").strip()
        if default_view_name:
            table_data["default_view_name"] = default_view_name
        fields_spec = payload.get("fields")
        if fields_spec is not None:
            normalized_fields = self._normalize_bitable_fields(fields_spec)
            if normalized_fields:
                table_data["fields"] = normalized_fields
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_table_create(
                    base_token=app_token,
                    name=name,
                    fields=table_data.get("fields"),
                    view={"view_name": default_view_name} if default_view_name else None,
                )
                table = _ensure_dict(result.get("table"))
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table.get("table_id"),
                    "table": table,
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("POST", f"/bitable/v1/apps/{app_token}/tables", data={"table": table_data})
        table = _ensure_dict(result.get("table"))
        if not table:
            table = {
                "table_id": result.get("table_id"),
                "default_view_id": result.get("default_view_id"),
                "field_id_list": result.get("field_id_list") or [],
                "name": name,
            }
        return {"ok": True, "app_token": app_token, "table_id": table.get("table_id"), "table": table}

    def table_create_field(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        raw_field = payload.get("field")
        if raw_field is None:
            raw_field = {
                "field_name": payload.get("field_name") or payload.get("name") or "",
                "type": payload.get("type"),
                "ui_type": payload.get("ui_type"),
                "property": payload.get("property"),
                "description": payload.get("description"),
            }
        field_candidates = self._normalize_bitable_fields([raw_field])
        if not field_candidates:
            raise FeishuAgentError("field definition is required", code="missing_field_definition")
        field_data = field_candidates[0]
        if field_data.get("ui_type") in {"", None}:
            field_data.pop("ui_type", None)
        if field_data.get("description") in {"", None}:
            field_data.pop("description", None)
        property_value = field_data.get("property")
        if property_value is None or property_value == "" or property_value == {}:
            field_data.pop("property", None)
        if self._can_use_lark_cli_backend("table"):
            try:
                result = lark_cli_backend.base_field_create(
                    base_token=app_token,
                    table_id=table_id,
                    field=field_data,
                )
                field = _ensure_dict(result.get("field"))
                return {
                    "ok": True,
                    "app_token": app_token,
                    "table_id": table_id,
                    "field_id": field.get("field_id"),
                    "field": field,
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        result = self.user_api("POST", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields", data=field_data)
        field = _ensure_dict(result.get("field"))
        return {"ok": True, "app_token": app_token, "table_id": table_id, "field_id": field.get("field_id"), "field": field}

    def _normalize_attendees(self, attendees: list[Any] | None) -> list[dict[str, str]]:
        normalized: list[dict[str, str]] = []
        for item in attendees or []:
            if isinstance(item, dict):
                attendee_type = str(item.get("type") or "").strip() or "user"
                attendee_id = str(item.get("id") or item.get("user_id") or item.get("email") or "").strip()
                if not attendee_id:
                    continue
                if attendee_type == "user" and not attendee_id.startswith("ou_"):
                    attendee_id = self.resolve_user_id(attendee_id)
                elif attendee_type == "chat" and not attendee_id.startswith("oc_"):
                    attendee_id = self.resolve_chat_id(attendee_id)
                normalized.append({"type": attendee_type, "id": attendee_id})
                continue
            raw = str(item or "").strip()
            if not raw:
                continue
            if raw.startswith("oc_"):
                normalized.append({"type": "chat", "id": raw})
            elif raw.startswith("ou_"):
                normalized.append({"type": "user", "id": raw})
            elif "@" in raw:
                try:
                    normalized.append({"type": "user", "id": self.resolve_user_id(raw)})
                except FeishuAgentError:
                    normalized.append({"type": "third_party", "id": raw})
            else:
                normalized.append({"type": "user", "id": self.resolve_user_id(raw)})
        return normalized

    def _create_calendar_event(self, payload: dict[str, Any], *, with_vchat: bool = False) -> dict[str, Any]:
        title = str(payload.get("title") or payload.get("summary") or "").strip()
        if not title:
            raise FeishuAgentError("title is required", code="missing_title")
        timezone = str(payload.get("timezone") or self._default_meeting_settings().get("timezone") or DEFAULT_TIMEZONE)
        start = _parse_dt(str(payload.get("start") or payload.get("start_time") or ""), timezone=timezone)
        end_value = str(payload.get("end") or payload.get("end_time") or "").strip()
        if not end_value:
            duration_minutes = int(
                payload.get("duration_minutes")
                or self._default_meeting_settings().get("duration_minutes")
                or 30
            )
            end_dt = datetime.fromtimestamp(int(start["timestamp"])) + timedelta(minutes=duration_minutes)
            end = {"timestamp": str(int(end_dt.timestamp())), "timezone": timezone}
        else:
            end = _parse_dt(end_value, timezone=timezone)
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        use_user_identity = self._should_use_user_calendar_identity(calendar_id)
        event_data: dict[str, Any] = {
            "summary": title,
            "start_time": start,
            "end_time": end,
            "need_notification": True,
            "attendee_ability": str(
                payload.get("attendee_ability")
                or self._default_meeting_settings().get("attendee_ability")
                or "can_modify_event"
            ),
        }
        if payload.get("desc") or payload.get("description"):
            event_data["description"] = str(payload.get("desc") or payload.get("description"))
        if payload.get("visibility") or self._default_meeting_settings().get("visibility"):
            event_data["visibility"] = str(payload.get("visibility") or self._default_meeting_settings().get("visibility"))
        if payload.get("free_busy_status"):
            event_data["free_busy_status"] = str(payload["free_busy_status"])
        if payload.get("location"):
            location = payload["location"]
            if isinstance(location, dict):
                event_data["location"] = _json_clone(location)
            else:
                event_data["location"] = {"name": str(location)}
        reminders = payload.get("reminders")
        if reminders:
            event_data["reminders"] = _json_clone(reminders)
        recurrence = payload.get("recurrence")
        if recurrence:
            event_data["recurrence"] = _json_clone(recurrence)
        if with_vchat or payload.get("vchat"):
            vchat = _ensure_dict(payload.get("vchat"))
            if not vchat:
                vchat = {"vc_type": "vc"}
            if not vchat.get("vc_type"):
                vchat["vc_type"] = "vc"
            event_data["vchat"] = _json_clone(vchat)
        if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
            try:
                result = lark_cli_backend.api_call(
                    "POST",
                    f"/calendar/v4/calendars/{calendar_id}/events",
                    data=event_data,
                    identity="bot",
                )
            except lark_cli_backend.LarkCliBackendError:
                result = self._http("POST", f"/calendar/v4/calendars/{calendar_id}/events", data=event_data, token=self._token())
        else:
            if use_user_identity:
                result = self.user_api("POST", f"/calendar/v4/calendars/{calendar_id}/events", data=event_data)
            else:
                result = self.api("POST", f"/calendar/v4/calendars/{calendar_id}/events", data=event_data)
        event = _ensure_dict(result.get("event"))
        event_id = str(event.get("event_id") or "").strip()
        attendees = self._normalize_attendees(list(payload.get("attendees") or []))
        if (
            with_vchat
            and self.owner_open_id
            and not any(item["type"] == "user" and item["id"] == self.owner_open_id for item in attendees)
        ):
            attendees.append({"type": "user", "id": self.owner_open_id})
        attendee_warning = ""
        if attendees and event_id:
            operate_id = next((item["id"] for item in attendees if item["type"] == "user"), "")
            attendee_data = []
            for item in attendees:
                row: dict[str, Any] = {"type": item["type"], "operate_id": operate_id or item["id"]}
                if item["type"] == "user":
                    row["user_id"] = item["id"]
                elif item["type"] == "chat":
                    row["chat_id"] = item["id"]
                elif item["type"] == "resource":
                    row["room_id"] = item["id"]
                elif item["type"] == "third_party":
                    row["third_party_email"] = item["id"]
                attendee_data.append(row)
            try:
                if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
                    lark_cli_backend.api_call(
                        "POST",
                        f"/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                        data={"attendees": attendee_data, "need_notification": True},
                        params={"user_id_type": "open_id"},
                        identity="bot",
                    )
                else:
                    if use_user_identity:
                        self.user_api(
                            "POST",
                            f"/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                            data={"attendees": attendee_data, "need_notification": True},
                            params={"user_id_type": "open_id"},
                        )
                    else:
                        self.api(
                            "POST",
                            f"/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                            data={"attendees": attendee_data, "need_notification": True},
                            params={"user_id_type": "open_id"},
                        )
            except FeishuAgentError as exc:
                attendee_warning = str(exc)
            except lark_cli_backend.LarkCliBackendError as exc:
                attendee_warning = str(exc)
        safe_event = {
            "event_id": event_id,
            "summary": event.get("summary") or title,
            "app_link": event.get("app_link"),
            "start_time": event.get("start_time") or start,
            "end_time": event.get("end_time") or end,
            "vchat": event.get("vchat") or event_data.get("vchat") or {},
        }
        response = {"ok": True, "calendar_id": calendar_id, "event": safe_event, "attendees": attendees}
        if attendee_warning:
            response["warning"] = attendee_warning
        return response

    def cal_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        use_user_identity = self._should_use_user_calendar_identity(calendar_id)
        now_ts = int(time.time())
        days = int(payload.get("days") or 7)
        if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
            start_iso = datetime.fromtimestamp(now_ts, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            end_iso = datetime.fromtimestamp(now_ts + days * 86400, UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            try:
                result = lark_cli_backend.calendar_agenda(
                    calendar_id=calendar_id,
                    start=start_iso,
                    end=end_iso,
                    identity="bot",
                )
                events = []
                for event in result.get("events", []):
                    safe_event = _ensure_dict(event)
                    events.append(
                        {
                            "id": safe_event.get("event_id") or safe_event.get("id"),
                            "title": safe_event.get("summary") or safe_event.get("title"),
                            "start": safe_event.get("start_time") or safe_event.get("start"),
                            "end": safe_event.get("end_time") or safe_event.get("end"),
                            "location": _ensure_dict(safe_event.get("location")).get("name") or safe_event.get("location"),
                            "description": safe_event.get("description"),
                            "vchat": safe_event.get("vchat") or {},
                        }
                    )
                return {
                    "calendar_id": result.get("calendar_id") or calendar_id,
                    "events": events,
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        if use_user_identity:
            result = self.user_api(
                "GET",
                f"/calendar/v4/calendars/{calendar_id}/events",
                params={"start_time": str(now_ts), "end_time": str(now_ts + days * 86400), "page_size": 50},
            )
        else:
            result = self.api(
                "GET",
                f"/calendar/v4/calendars/{calendar_id}/events",
                params={"start_time": str(now_ts), "end_time": str(now_ts + days * 86400), "page_size": 50},
            )
        events = []
        for event in result.get("items", []):
            events.append(
                {
                    "id": event.get("event_id"),
                    "title": event.get("summary"),
                    "start": event.get("start_time"),
                    "end": event.get("end_time"),
                    "location": _ensure_dict(event.get("location")).get("name"),
                    "description": event.get("description"),
                    "vchat": event.get("vchat") or {},
                }
            )
        return {"calendar_id": calendar_id, "events": events}

    def cal_add(self, payload: dict[str, Any]) -> dict[str, Any]:
        explicit_calendar = str(payload.get("calendar") or payload.get("calendar_id") or "").strip()
        if not explicit_calendar and self._default_calendar_create_route() in {
            "invite",
            "meeting",
            "invite_meeting",
            "meeting_invite",
        }:
            merged = dict(payload)
            defaults = self._default_meeting_settings()
            if not merged.get("calendar") and defaults.get("calendar_id"):
                merged["calendar"] = defaults.get("calendar_id")
            if not merged.get("timezone") and defaults.get("timezone"):
                merged["timezone"] = defaults.get("timezone")
            if merged.get("attendee_ability") is None and defaults.get("attendee_ability"):
                merged["attendee_ability"] = defaults.get("attendee_ability")
            if merged.get("visibility") is None and defaults.get("visibility"):
                merged["visibility"] = defaults.get("visibility")
            result = self._create_calendar_event(merged, with_vchat=True)
            response = {
                "ok": True,
                "calendar_id": result.get("calendar_id"),
                "event": result.get("event", {}),
                "attendees": result.get("attendees", []),
                "route": "invite_meeting",
            }
            if result.get("warning"):
                response["warning"] = result["warning"]
            return response
        if not explicit_calendar:
            personal_calendar_id = self._default_personal_calendar_id()
            if personal_calendar_id:
                payload = dict(payload)
                payload["calendar_id"] = personal_calendar_id
            else:
                default_calendar_id = self.resolve_calendar_id("")
                if (
                    self._default_personal_reminder_target() == "task"
                    and self._is_group_calendar_id(default_calendar_id)
                ):
                    raise FeishuAgentError(
                        "implicit personal reminder calendar is not configured; refusing to use group calendar default",
                        code="implicit_group_calendar_blocked",
                        details={"calendar_id": default_calendar_id, "suggested_target": "task"},
                    )
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        use_user_identity = self._should_use_user_calendar_identity(calendar_id)
        if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
            timezone = str(payload.get("timezone") or DEFAULT_TIMEZONE)
            try:
                start = _parse_dt(str(payload.get("start") or payload.get("start_time") or ""), timezone=timezone)
                end_value = str(payload.get("end") or payload.get("end_time") or "").strip()
                if end_value:
                    end = _parse_dt(end_value, timezone=timezone)
                else:
                    duration_minutes = int(payload.get("duration_minutes") or 60)
                    end = {"timestamp": str(int(start["timestamp"]) + duration_minutes * 60), "timezone": timezone}
                result = lark_cli_backend.calendar_create(
                    summary=str(payload.get("title") or payload.get("summary") or "").strip(),
                    start=datetime.fromtimestamp(int(start["timestamp"]), UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    end=datetime.fromtimestamp(int(end["timestamp"]), UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    calendar_id=calendar_id,
                    description=str(payload.get("note") or payload.get("description") or "").strip(),
                    attendee_ids=[],
                    identity="bot",
                )
                return {
                    "ok": True,
                    "calendar_id": result.get("calendar_id") or calendar_id,
                    "event": result.get("event") or {},
                    "backend": result.get("backend"),
                }
            except (FeishuAgentError, lark_cli_backend.LarkCliBackendError):
                pass
        return self._create_calendar_event(payload, with_vchat=False)

    def cal_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        use_user_identity = self._should_use_user_calendar_identity(calendar_id)
        event_id = str(payload.get("id") or payload.get("event_id") or "").strip()
        if not event_id:
            raise FeishuAgentError("event id is required", code="missing_event_id")
        if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
            try:
                result = lark_cli_backend.calendar_delete(
                    calendar_id=calendar_id,
                    event_id=event_id,
                    identity="bot",
                )
                return {
                    "ok": True,
                    "calendar_id": calendar_id,
                    "event_id": event_id,
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        if use_user_identity:
            self.user_api("DELETE", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}")
        else:
            self.api("DELETE", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}")
        return {"ok": True, "calendar_id": calendar_id, "event_id": event_id}

    def task_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._can_use_lark_cli_backend("task"):
            try:
                result = lark_cli_backend.task_list(
                    query=str(payload.get("query") or "").strip(),
                    completed=bool(payload.get("completed")),
                )
                return {"tasks": list(result.get("tasks") or []), "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        params: dict[str, Any] = {"page_size": int(payload.get("limit") or 50), "user_id_type": "open_id"}
        if payload.get("completed"):
            params["completed"] = "true"
        result = self.user_api("GET", "/task/v2/tasks", params=params)
        return {"tasks": result.get("items", [])}

    def task_add(self, payload: dict[str, Any]) -> dict[str, Any]:
        title = str(payload.get("title") or payload.get("summary") or "").strip()
        if not title:
            raise FeishuAgentError("title is required", code="missing_title")
        if self._can_use_lark_cli_backend("task"):
            try:
                result = lark_cli_backend.task_create(
                    summary=title,
                    description=str(payload.get("note") or payload.get("description") or "").strip(),
                    due=str(payload.get("due") or "").strip(),
                    assignee=self.owner_open_id,
                )
                return {
                    "ok": True,
                    "task_id": result.get("task_id"),
                    "task": result.get("task") or {},
                    "backend": result.get("backend"),
                }
            except lark_cli_backend.LarkCliBackendError:
                pass
        data: dict[str, Any] = {"summary": title}
        due = str(payload.get("due") or "").strip()
        if due:
            ts = _parse_dt(due)["timestamp"]
            data["due"] = {"timestamp": str(int(ts) * 1000), "is_all_day": ":" not in due}
        if payload.get("note") or payload.get("description"):
            data["description"] = str(payload.get("note") or payload.get("description"))
        if self.owner_open_id:
            data["members"] = [{"id": self.owner_open_id, "type": "user", "role": "assignee"}]
        result = self.user_api("POST", "/task/v2/tasks", data=data, params={"user_id_type": "open_id"})
        return {"ok": True, "task_id": result.get("task", {}).get("guid"), "task": result.get("task", {})}

    def task_done(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_id = str(payload.get("id") or payload.get("task_id") or "").strip()
        if not task_id:
            raise FeishuAgentError("task id is required", code="missing_task_id")
        if self._can_use_lark_cli_backend("task"):
            try:
                result = lark_cli_backend.task_complete(task_id=task_id)
                return {"ok": True, "task_id": task_id, "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api(
            "PATCH",
            f"/task/v2/tasks/{task_id}",
            data={"task": {"completed_at": str(int(time.time() * 1000))}, "update_fields": ["completed_at"]},
        )
        return {"ok": True, "task_id": task_id}

    def task_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        task_id = str(payload.get("id") or payload.get("task_id") or "").strip()
        if not task_id:
            raise FeishuAgentError("task id is required", code="missing_task_id")
        if self._can_use_lark_cli_backend("task"):
            try:
                result = lark_cli_backend.task_delete(task_id=task_id)
                return {"ok": True, "task_id": task_id, "backend": result.get("backend")}
            except lark_cli_backend.LarkCliBackendError:
                pass
        self.user_api("DELETE", f"/task/v2/tasks/{task_id}")
        return {"ok": True, "task_id": task_id}

    def meeting_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        merged = _json_clone(payload)
        defaults = self._default_meeting_settings()
        if not merged.get("calendar") and defaults.get("calendar_id"):
            merged["calendar"] = defaults.get("calendar_id")
        if not merged.get("timezone") and defaults.get("timezone"):
            merged["timezone"] = defaults.get("timezone")
        if merged.get("attendee_ability") is None and defaults.get("attendee_ability"):
            merged["attendee_ability"] = defaults.get("attendee_ability")
        if merged.get("visibility") is None and defaults.get("visibility"):
            merged["visibility"] = defaults.get("visibility")
        result = self._create_calendar_event(merged, with_vchat=True)
        event = result.get("event", {})
        return {
            "ok": True,
            "meeting_id": event.get("event_id"),
            "calendar_id": result.get("calendar_id"),
            "meeting": {
                "event_id": event.get("event_id"),
                "title": event.get("summary"),
                "join_url": _ensure_dict(event.get("vchat")).get("meeting_url") or event.get("app_link"),
                "app_link": event.get("app_link"),
                "vchat": event.get("vchat") or {},
                "start_time": event.get("start_time"),
                "end_time": event.get("end_time"),
            },
            "attendees": result.get("attendees", []),
            **({"warning": result["warning"]} if result.get("warning") else {}),
        }

    def _meeting_event(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        use_user_identity = self._should_use_user_calendar_identity(calendar_id)
        event_id = str(payload.get("id") or payload.get("meeting_id") or payload.get("event_id") or "").strip()
        if not event_id:
            raise FeishuAgentError("meeting id is required", code="missing_meeting_id")
        if self._can_use_lark_cli_backend("calendar") and not use_user_identity:
            try:
                result = lark_cli_backend.calendar_get(
                    calendar_id=calendar_id,
                    event_id=event_id,
                    identity="bot",
                )
            except lark_cli_backend.LarkCliBackendError:
                result = {"event": self._http("GET", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}", token=self._token()).get("event")}
        else:
            if use_user_identity:
                result = self.user_api("GET", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}")
            else:
                result = self.api("GET", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}")
        return calendar_id, _ensure_dict(result.get("event"))

    def meeting_get(self, payload: dict[str, Any]) -> dict[str, Any]:
        calendar_id, event = self._meeting_event(payload)
        return {"calendar_id": calendar_id, "meeting": {"event_id": event.get("event_id"), "title": event.get("summary"), "join_url": _ensure_dict(event.get("vchat")).get("meeting_url") or event.get("app_link"), "app_link": event.get("app_link"), "vchat": event.get("vchat") or {}, "start_time": event.get("start_time"), "end_time": event.get("end_time")}}

    def meeting_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self.cal_list(payload)
        meetings = []
        for item in data.get("events", []):
            vchat = _ensure_dict(item.get("vchat"))
            vc_type = str(vchat.get("vc_type") or "").strip()
            if (vc_type and vc_type != "no_meeting") or item.get("location") == "视频会议":
                meetings.append({"event_id": item.get("id"), "title": item.get("title"), "join_url": vchat.get("meeting_url") or item.get("app_link"), "vchat": vchat, "start_time": item.get("start"), "end_time": item.get("end")})
        return {"calendar_id": data.get("calendar_id"), "meetings": meetings}

    def meeting_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.cal_delete(payload)
        return {"ok": True, "meeting_id": result.get("event_id"), "calendar_id": result.get("calendar_id")}

    def meeting_cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.meeting_delete(payload)

    def perform(self, domain: str, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        key = f"{domain.strip().lower()} {action.strip().lower()}"
        handlers = {
            "msg send": self.msg_send,
            "msg reply": self.msg_reply,
            "msg history": self.msg_history,
            "msg search": self.msg_search,
            "msg chats": self.msg_chats,
            "msg download-resources": self.msg_download_resources,
            "auth status": self.auth_status,
            "auth login": self.auth_login,
            "auth clear": self.auth_clear,
            "user get": self.user_get,
            "user search": self.user_search,
            "drive upload": self.drive_upload,
            "drive download": self.drive_download,
            "drive add-comment": self.drive_add_comment,
            "doc create": self.doc_create,
            "doc get": self.doc_get,
            "doc insert-image": self.doc_insert_image,
            "doc list": self.doc_list,
            "doc search": self.doc_search,
            "table records": self.table_records,
            "table add": self.table_add,
            "table update": self.table_update,
            "table delete": self.table_delete,
            "table get-app": self.table_get_app,
            "table tables": self.table_tables,
            "table delete-table": self.table_delete_table,
            "table fields": self.table_fields,
            "table update-field": self.table_update_field,
            "table delete-field": self.table_delete_field,
            "table views": self.table_views,
            "table get-view": self.table_get_view,
            "table create-view": self.table_create_view,
            "table update-view": self.table_update_view,
            "table delete-view": self.table_delete_view,
            "table create-app": self.table_create_app,
            "table create": self.table_create,
            "table create-field": self.table_create_field,
            "cal list": self.cal_list,
            "cal add": self.cal_add,
            "cal delete": self.cal_delete,
            "task list": self.task_list,
            "task add": self.task_add,
            "task done": self.task_done,
            "task delete": self.task_delete,
            "vc search": self.vc_search,
            "vc notes": self.vc_notes,
            "minutes get": self.minutes_get,
            "wiki get-node": self.wiki_get_node,
            "sheet create": self.sheet_create,
            "sheet info": self.sheet_info,
            "sheet read": self.sheet_read,
            "sheet write": self.sheet_write,
            "sheet append": self.sheet_append,
            "sheet find": self.sheet_find,
            "mail triage": self.mail_triage,
            "mail send": self.mail_send,
            "mail reply": self.mail_reply,
            "mail message": self.mail_message,
            "mail thread": self.mail_thread,
            "whiteboard update": self.whiteboard_update,
            "meeting create": self.meeting_create,
            "meeting get": self.meeting_get,
            "meeting list": self.meeting_list,
            "meeting delete": self.meeting_delete,
            "meeting cancel": self.meeting_cancel,
        }
        if key not in handlers:
            raise FeishuAgentError(f"unknown feishu operation: {key}", code="unknown_operation")
        result = handlers[key](_ensure_dict(payload))
        return {"ok": True, "domain": domain, "action": action, "result": result}


def _parse_cli_payload(args: list[str]) -> tuple[str, str, dict[str, Any]]:
    if len(args) < 2:
        raise FeishuAgentError("usage: feishu_agent.py <domain> <action> [--key value ...]", code="usage")
    domain, action = args[0], args[1]
    payload: dict[str, Any] = {}
    rest = args[2:]
    index = 0
    while index < len(rest):
        item = rest[index]
        if not item.startswith("--"):
            index += 1
            continue
        key = item[2:].replace("-", "_")
        if index + 1 < len(rest) and not rest[index + 1].startswith("--"):
            payload[key] = rest[index + 1]
            index += 2
        else:
            payload[key] = True
            index += 1
    if payload.get("payload_json"):
        extra = json.loads(str(payload["payload_json"]))
        payload.pop("payload_json", None)
        if not isinstance(extra, dict):
            raise FeishuAgentError("payload_json must decode to an object", code="invalid_payload")
        payload.update(extra)
    return domain, action, payload


def perform_operation(
    domain: str,
    action: str,
    payload: dict[str, Any] | None = None,
    *,
    registry_path: str | Path | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    agent = FeishuAgent(env=env, registry_path=registry_path)
    return agent.perform(domain, action, payload or {})


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else os.sys.argv[1:])
    if not args:
        print("Usage: feishu_agent.py <domain> <action> [--key value ...]")
        return 0
    try:
        domain, action, payload = _parse_cli_payload(args)
        result = perform_operation(domain, action, payload)
    except FeishuAgentError as exc:
        print(
            json.dumps(
                {"ok": False, "error": str(exc), "error_code": exc.code, "details": exc.details},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

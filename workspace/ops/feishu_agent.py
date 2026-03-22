#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import plistlib
import ssl
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyYAML is required for feishu_agent") from exc

try:
    import certifi
except ImportError:  # pragma: no cover
    certifi = None


DEFAULT_BASE_URL = "https://open.feishu.cn/open-apis"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_AUTHORIZE_URL = "https://open.feishu.cn/open-apis/authen/v1/index"
DEFAULT_OAUTH_REDIRECT_URI = "http://127.0.0.1:14589/feishu-auth/callback"
DEFAULT_OAUTH_PORT = 14589
DEFAULT_OAUTH_PATH = "/feishu-auth/callback"
DEFAULT_TOKEN_STORE_NAME = "feishu_user_token.json"
DEFAULT_LAUNCH_AGENT_NAME = "com.codexhub.coco-feishu-bridge.plist"


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


def load_registry(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path else default_registry_path()
    if not target.exists():
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
    with target.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload


def save_registry(payload: dict[str, Any], path: str | Path | None = None) -> Path:
    target = Path(path) if path else default_registry_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)
    return target


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


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
        self.registry = load_registry(self.registry_path)
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
                "hint": "run `python3 <workspace>/ops/feishu_agent.py auth login` once, or configure FEISHU_USER_ACCESS_TOKEN for the CoCo service"
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
        return self._http(method, path, data=data, params=params, token=self._token())

    def user_api(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
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
            ],
        )
        return extracted or target

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
        alias_value = self._match_alias(self._aliases("tables"), table_ref or app_ref)
        if isinstance(alias_value, dict):
            app_ref = str(alias_value.get("app_token") or alias_value.get("app") or app_ref).strip()
            table_ref = str(alias_value.get("table_id") or alias_value.get("table") or table_ref).strip()
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
        return save_registry(self.registry, self.registry_path)

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
        return defaults if isinstance(defaults, dict) else {}

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
        text = str(payload.get("text") or "").strip()
        if not text and payload.get("file"):
            text = Path(str(payload["file"])).read_text(encoding="utf-8")
        if not text:
            raise FeishuAgentError("text or file is required", code="missing_text")
        result = self.api(
            "POST",
            "/im/v1/messages",
            data={
                "receive_id": target,
                "msg_type": "text",
                "content": json.dumps({"text": text}, ensure_ascii=False),
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
        }

    def msg_reply(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = str(payload.get("to") or payload.get("message_id") or "").strip()
        if not message_id:
            raise FeishuAgentError("message_id is required", code="missing_message_id")
        text = str(payload.get("text") or "").strip()
        if not text and payload.get("file"):
            text = Path(str(payload["file"])).read_text(encoding="utf-8")
        if not text:
            raise FeishuAgentError("text or file is required", code="missing_text")
        result = self.api(
            "POST",
            f"/im/v1/messages/{message_id}/reply",
            data={"msg_type": "text", "content": json.dumps({"text": text}, ensure_ascii=False)},
        )
        return {"ok": True, "domain": "msg", "action": "reply", "message_id": result.get("message_id")}

    def msg_history(self, payload: dict[str, Any]) -> dict[str, Any]:
        chat_id = self.resolve_chat_id(str(payload.get("chat") or ""))
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
        result = self.api(
            "POST",
            "/im/v1/messages/search",
            data={"query": query},
            params={"page_size": int(payload.get("limit") or 20)},
        )
        return {"query": query, "messages": result.get("items", [])}

    def msg_chats(self, payload: dict[str, Any]) -> dict[str, Any]:
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
        result = self.api("GET", f"/contact/v3/users/{user_id}", params={"user_id_type": "open_id"})
        return {"user": result.get("user")}

    def user_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        query = str(payload.get("name") or payload.get("query") or "").strip()
        if not query:
            raise FeishuAgentError("name is required", code="missing_name")
        limit = int(payload.get("limit") or 20)
        return {"users": self._search_users_locally(query, page_size=max(limit, 100))[:limit]}

    def auth_status(self, _payload: dict[str, Any]) -> dict[str, Any]:
        store = self._load_user_token_store()
        access_token = str(self.user_access_token or store.get("access_token") or "").strip()
        refresh_token = str(self.user_refresh_token or store.get("refresh_token") or "").strip()
        access_expire_at = str(store.get("access_token_expire_at") or "").strip()
        refresh_expire_at = str(store.get("refresh_token_expire_at") or "").strip()
        auth_method = str(store.get("auth_method") or "").strip()
        return {
            "configured": bool(self.app_id and self.app_secret),
            "has_user_access_token": bool(access_token),
            "has_refresh_token": bool(refresh_token),
            "auto_refresh_ready": bool(refresh_token),
            "auth_method": auth_method,
            "redirect_uri": str(store.get("redirect_uri") or self.oauth_redirect_uri),
            "token_store_path": str(self._user_token_store_path),
            "access_token_expire_at": access_expire_at,
            "refresh_token_expire_at": refresh_expire_at,
            "profile": store.get("profile") or {},
        }

    def auth_clear(self, _payload: dict[str, Any]) -> dict[str, Any]:
        self._clear_user_token_store()
        return {"ok": True, "token_store_path": str(self._user_token_store_path)}

    def _fetch_user_info(self, access_token: str) -> dict[str, Any]:
        payload = self._http("GET", "/authen/v1/user_info", token=access_token)
        return payload if isinstance(payload, dict) else {}

    def auth_login(self, payload: dict[str, Any]) -> dict[str, Any]:
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
        configured_scopes = payload.get("scopes") or self._defaults().get("oauth_scopes") or []
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
            "authorize_url": authorize_url,
            "redirect_uri": redirect_uri,
            "scopes": scopes,
            "token_store_path": str(self._user_token_store_path),
            "profile": profile,
            "access_token_expire_at": stored.get("access_token_expire_at", ""),
            "refresh_token_expire_at": stored.get("refresh_token_expire_at", ""),
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

    def doc_create(self, payload: dict[str, Any]) -> dict[str, Any]:
        folder_token = self.resolve_folder_token(str(payload.get("folder") or ""))
        create_payload: dict[str, Any] = {"title": str(payload.get("title") or "新文档")}
        if folder_token:
            create_payload["folder_token"] = folder_token
        result = self.api("POST", "/docx/v1/documents", data=create_payload)
        document = result.get("document", {})
        document_id = str(document.get("document_id") or "").strip()
        content = str(payload.get("content") or "").strip()
        if not content and payload.get("file"):
            content = Path(str(payload["file"])).read_text(encoding="utf-8")
        if content and document_id:
            blocks = self._md_to_blocks(content)
            if blocks:
                self._write_doc_blocks(document_id, blocks)
        share_to = str(payload.get("share_to") or self.owner_open_id or "").strip()
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
        result = self.api("GET", f"/docx/v1/documents/{document_id}/raw_content", params={"lang": 0})
        return {"document_id": document_id, "content": result.get("content")}

    def doc_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        folder_token = self.resolve_folder_token(str(payload.get("folder") or ""))
        params: dict[str, Any] = {
            "page_size": int(payload.get("limit") or 50),
            "order_by": "EditedTime",
            "direction": "DESC",
        }
        if folder_token:
            params["folder_token"] = folder_token
        result = self.api("GET", "/drive/v1/files", params=params)
        return {"files": result.get("files", [])}

    def table_records(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
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
        result = self.user_api("POST", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records", data={"fields": fields})
        return {"ok": True, "app_token": app_token, "table_id": table_id, "record_id": result.get("record", {}).get("record_id")}

    def table_update(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        record_id = str(payload.get("record") or payload.get("record_id") or "").strip()
        if not table_id or not record_id:
            raise FeishuAgentError("table and record are required", code="missing_table_or_record")
        raw = payload.get("data")
        fields = raw if isinstance(raw, dict) else json.loads(str(raw or "{}"))
        result = self.user_api("PUT", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}", data={"fields": fields})
        return {"ok": True, "record": result.get("record")}

    def table_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        record_id = str(payload.get("record") or payload.get("record_id") or "").strip()
        if not table_id or not record_id:
            raise FeishuAgentError("table and record are required", code="missing_table_or_record")
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}")
        return {"ok": True}

    def table_tables(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, _table_id = self.resolve_table_refs(str(payload.get("app") or ""), "")
        result = self.api("GET", f"/bitable/v1/apps/{app_token}/tables", params={"page_size": 50})
        return {"app_token": app_token, "tables": list(result.get("items") or [])}

    def table_get_app(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, _table_id = self.resolve_table_refs(str(payload.get("app") or ""), "")
        result = self.api("GET", f"/bitable/v1/apps/{app_token}")
        return {"app_token": app_token, "app": _ensure_dict(result.get("app"))}

    def table_delete_table(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}")
        return {"ok": True, "app_token": app_token, "table_id": table_id}

    def table_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
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
        self.user_api("DELETE", f"/bitable/v1/apps/{app_token}/tables/{table_id}/fields/{field_id}")
        return {"ok": True, "app_token": app_token, "table_id": table_id, "field_id": field_id}

    def table_views(self, payload: dict[str, Any]) -> dict[str, Any]:
        app_token, table_id = self.resolve_table_refs(str(payload.get("app") or ""), str(payload.get("table") or ""))
        if not table_id:
            raise FeishuAgentError("table id is required", code="missing_table_id")
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
        result = self.api("POST", f"/calendar/v4/calendars/{calendar_id}/events", data=event_data)
        event = _ensure_dict(result.get("event"))
        event_id = str(event.get("event_id") or "").strip()
        attendees = self._normalize_attendees(list(payload.get("attendees") or []))
        if self.owner_open_id and not any(item["type"] == "user" and item["id"] == self.owner_open_id for item in attendees):
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
                self.api(
                    "POST",
                    f"/calendar/v4/calendars/{calendar_id}/events/{event_id}/attendees",
                    data={"attendees": attendee_data, "need_notification": True},
                    params={"user_id_type": "open_id"},
                )
            except FeishuAgentError as exc:
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
        now_ts = int(time.time())
        days = int(payload.get("days") or 7)
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
        return self._create_calendar_event(payload, with_vchat=False)

    def cal_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        calendar_id = self.resolve_calendar_id(str(payload.get("calendar") or payload.get("calendar_id") or ""))
        event_id = str(payload.get("id") or payload.get("event_id") or "").strip()
        if not event_id:
            raise FeishuAgentError("event id is required", code="missing_event_id")
        self.api("DELETE", f"/calendar/v4/calendars/{calendar_id}/events/{event_id}")
        return {"ok": True, "calendar_id": calendar_id, "event_id": event_id}

    def task_list(self, payload: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {"page_size": int(payload.get("limit") or 50), "user_id_type": "open_id"}
        if payload.get("completed"):
            params["completed"] = "true"
        result = self.user_api("GET", "/task/v2/tasks", params=params)
        return {"tasks": result.get("items", [])}

    def task_add(self, payload: dict[str, Any]) -> dict[str, Any]:
        title = str(payload.get("title") or payload.get("summary") or "").strip()
        if not title:
            raise FeishuAgentError("title is required", code="missing_title")
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
        event_id = str(payload.get("id") or payload.get("meeting_id") or payload.get("event_id") or "").strip()
        if not event_id:
            raise FeishuAgentError("meeting id is required", code="missing_meeting_id")
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
            "auth status": self.auth_status,
            "auth login": self.auth_login,
            "auth clear": self.auth_clear,
            "user get": self.user_get,
            "user search": self.user_search,
            "doc create": self.doc_create,
            "doc get": self.doc_get,
            "doc list": self.doc_list,
            "table records": self.table_records,
            "table add": self.table_add,
            "table update": self.table_update,
            "table delete": self.table_delete,
            "table tables": self.table_tables,
            "table get-app": self.table_get_app,
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

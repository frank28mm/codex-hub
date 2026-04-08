#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = WORKSPACE_ROOT / "control" / "feishu_capabilities.yaml"


def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload, ensure_ascii=False))


def _normalize_string_list(values: Any) -> list[str]:
    if isinstance(values, str):
        return [item.strip() for item in values.replace(",", " ").split() if item.strip()]
    if isinstance(values, list):
        result: list[str] = []
        for item in values:
            text = str(item or "").strip()
            if text:
                result.append(text)
        return result
    return []


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def load_manifest(path: Path | None = None) -> dict[str, Any]:
    target = path or MANIFEST_PATH
    if yaml is None or not target.exists():
        return {"version": 1, "auth_plan": {"label": "Feishu core authorization bundle", "capability_ids": [], "extra_scopes": []}, "capabilities": {}}
    payload = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {"version": 1, "auth_plan": {"label": "Feishu core authorization bundle", "capability_ids": [], "extra_scopes": []}, "capabilities": {}}


def capability_items(manifest: dict[str, Any] | None = None) -> list[tuple[str, dict[str, Any]]]:
    source = manifest or load_manifest()
    raw = source.get("capabilities")
    if not isinstance(raw, dict):
        return []
    items: list[tuple[str, dict[str, Any]]] = []
    for capability_id, value in raw.items():
        if not isinstance(value, dict):
            continue
        items.append((str(capability_id), dict(value)))
    return items


def build_auth_plan(manifest: dict[str, Any] | None = None) -> dict[str, Any]:
    source = manifest or load_manifest()
    auth_plan = source.get("auth_plan") if isinstance(source.get("auth_plan"), dict) else {}
    capability_id_list = _normalize_string_list(auth_plan.get("capability_ids"))
    capability_by_id = {capability_id: payload for capability_id, payload in capability_items(source)}
    selected_items: list[tuple[str, dict[str, Any]]] = []
    for capability_id in capability_id_list:
        payload = capability_by_id.get(capability_id)
        if payload is not None:
            selected_items.append((capability_id, payload))
    requested_scopes = _dedupe_keep_order(
        _normalize_string_list(auth_plan.get("extra_scopes"))
        + [
            scope
            for _capability_id, payload in selected_items
            for scope in _normalize_string_list(payload.get("required_scopes"))
        ]
    )
    requested_domains = _dedupe_keep_order(
        [
            domain
            for _capability_id, payload in selected_items
            for domain in _normalize_string_list(payload.get("domains"))
        ]
    )
    return {
        "label": str(auth_plan.get("label") or "Feishu core authorization bundle").strip(),
        "capability_ids": [capability_id for capability_id, _payload in selected_items],
        "capabilities": [
            {
                "id": capability_id,
                "label": str(payload.get("label") or capability_id).strip(),
                "description": str(payload.get("description") or "").strip(),
                "feature_specific": bool(payload.get("feature_specific")),
                "requires_bridge_credentials": bool(payload.get("requires_bridge_credentials")),
                "required_scopes": _normalize_string_list(payload.get("required_scopes")),
                "extra_dependencies": _normalize_string_list(payload.get("extra_dependencies")),
                "notes": _normalize_string_list(payload.get("notes")),
            }
            for capability_id, payload in selected_items
        ],
        "requested_scopes": requested_scopes,
        "requested_scope_string": " ".join(requested_scopes),
        "requested_domains": requested_domains,
        "requested_domain_string": ",".join(requested_domains),
    }


def granted_scope_set(value: Any) -> set[str]:
    return set(_normalize_string_list(value))


def evaluate_capabilities(
    *,
    manifest: dict[str, Any] | None = None,
    granted_scopes: Any,
    lark_cli_configured: bool,
    user_auth_ready: bool,
    bridge_credentials_ready: bool,
) -> dict[str, Any]:
    source = manifest or load_manifest()
    auth_plan = build_auth_plan(source)
    granted = granted_scope_set(granted_scopes)
    capability_status: dict[str, dict[str, Any]] = {}
    feature_specific_pending: list[str] = []
    for capability_id, payload in capability_items(source):
        required_scopes = _normalize_string_list(payload.get("required_scopes"))
        missing_scopes = [scope for scope in required_scopes if scope not in granted]
        requires_user_auth = bool(payload.get("user_auth_required", True))
        requires_bridge_credentials = bool(payload.get("requires_bridge_credentials"))
        blocking_requirements: list[str] = []
        if requires_user_auth and not lark_cli_configured:
            blocking_requirements.append("lark_cli_config")
        if requires_user_auth and not user_auth_ready:
            blocking_requirements.append("user_auth")
        if requires_bridge_credentials and not bridge_credentials_ready:
            blocking_requirements.append("bridge_credentials")
        ready = not missing_scopes and not blocking_requirements
        if bool(payload.get("feature_specific")) and not ready:
            feature_specific_pending.append(capability_id)
        capability_status[capability_id] = {
            "label": str(payload.get("label") or capability_id).strip(),
            "description": str(payload.get("description") or "").strip(),
            "feature_specific": bool(payload.get("feature_specific")),
            "bootstrap_core": bool(payload.get("bootstrap_core")),
            "requires_bridge_credentials": requires_bridge_credentials,
            "required_scopes": required_scopes,
            "missing_scopes": missing_scopes,
            "missing_scope_count": len(missing_scopes),
            "blocking_requirements": blocking_requirements,
            "ready": ready,
            "notes": _normalize_string_list(payload.get("notes")),
            "extra_dependencies": _normalize_string_list(payload.get("extra_dependencies")),
            "auth_command": (
                f'lark-cli auth login --scope "{" ".join(missing_scopes)}"' if missing_scopes else ""
            ),
        }
    missing_requested_scopes = [scope for scope in auth_plan["requested_scopes"] if scope not in granted]
    return {
        "auth_plan": auth_plan,
        "auth_plan_ready": bool(lark_cli_configured and user_auth_ready and not missing_requested_scopes),
        "missing_requested_scopes": missing_requested_scopes,
        "capabilities": capability_status,
        "feature_specific_pending_capabilities": feature_specific_pending,
    }

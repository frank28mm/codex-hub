#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


def workspace_root() -> Path:
    explicit = os.environ.get("WORKSPACE_HUB_ROOT", "").strip()
    if explicit:
        return Path(explicit).resolve()
    return Path(__file__).resolve().parents[1]


def runtime_root() -> Path:
    explicit = os.environ.get("WORKSPACE_HUB_RUNTIME_ROOT", "").strip()
    if explicit:
        return Path(explicit).resolve()
    return workspace_root() / "runtime"


def cache_root() -> Path:
    return runtime_root() / "result-cache"


def namespace_root(namespace: str) -> Path:
    normalized = str(namespace or "").strip().replace("/", "-")
    return cache_root() / (normalized or "default")


def stable_key(identity: Any) -> str:
    payload = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def entry_path(namespace: str, key: str) -> Path:
    normalized_key = str(key or "").strip()
    return namespace_root(namespace) / f"{normalized_key}.json"


def cache_contract() -> dict[str, Any]:
    return {
        "schema_version": "codex-hub.result-cache.v1",
        "root": str(cache_root()),
        "fields": [
            "namespace",
            "key",
            "identity",
            "value",
            "metadata",
            "created_at",
            "updated_at",
        ],
    }


def load_entry(namespace: str, key: str, *, max_age_seconds: int | None = None) -> dict[str, Any] | None:
    path = entry_path(namespace, key)
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    if max_age_seconds is not None:
        updated_at = float(payload.get("updated_at_epoch", 0) or 0)
        if updated_at and (time.time() - updated_at) > max(0, int(max_age_seconds)):
            return None
    return payload


def save_entry(
    namespace: str,
    key: str,
    *,
    identity: Any,
    value: Any,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = time.time()
    payload = {
        "namespace": str(namespace or "").strip(),
        "key": str(key or "").strip(),
        "identity": identity,
        "value": value,
        "metadata": dict(metadata or {}),
        "created_at_epoch": now,
        "updated_at_epoch": now,
    }
    path = entry_path(namespace, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def remember(namespace: str, identity: Any, *, value: Any, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    key = stable_key(identity)
    return save_entry(namespace, key, identity=identity, value=value, metadata=metadata)


def recall(namespace: str, identity: Any, *, max_age_seconds: int | None = None) -> dict[str, Any] | None:
    key = stable_key(identity)
    return load_entry(namespace, key, max_age_seconds=max_age_seconds)

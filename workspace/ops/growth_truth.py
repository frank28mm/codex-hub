#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyYAML is required for growth_truth") from exc

try:
    from ops import codex_memory
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore


try:
    from ops import workspace_hub_project
except ImportError:  # pragma: no cover
    import workspace_hub_project  # type: ignore


DEFAULT_WORKSPACE_ROOT = workspace_hub_project.DEFAULT_WORKSPACE_ROOT


def workspace_root() -> Path:
    explicit = str(os.environ.get("WORKSPACE_HUB_ROOT", "")).strip()
    return Path(explicit) if explicit else DEFAULT_WORKSPACE_ROOT


def control_root() -> Path:
    explicit = str(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", "")).strip()
    if explicit:
        return Path(explicit)
    return workspace_root() / "control"


def load_growth_control() -> dict[str, Any]:
    path = control_root() / "codex_growth_system.yaml"
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload if isinstance(payload, dict) else {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def object_names() -> list[str]:
    control = load_growth_control()
    objects = control.get("objects") or {}
    if not isinstance(objects, dict):
        return []
    return [str(name).strip() for name in objects if str(name).strip()]


def object_spec(object_name: str) -> dict[str, Any]:
    control = load_growth_control()
    objects = control.get("objects") or {}
    if not isinstance(objects, dict):
        raise KeyError(f"unknown growth object `{object_name}`")
    spec = objects.get(object_name)
    if not isinstance(spec, dict):
        raise KeyError(f"unknown growth object `{object_name}`")
    return spec


def object_headers(object_name: str) -> list[str]:
    fields = object_spec(object_name).get("fields") or []
    return [str(item).strip() for item in fields if str(item).strip()]


def object_key_field(object_name: str) -> str:
    headers = object_headers(object_name)
    if not headers:
        raise RuntimeError(f"missing fields for growth object `{object_name}`")
    return headers[0]


def object_path(object_name: str) -> Path:
    raw = _text(object_spec(object_name).get("table_path"))
    if not raw:
        raise RuntimeError(f"missing table_path for growth object `{object_name}`")
    return Path(raw)


def default_frontmatter(object_name: str) -> dict[str, Any]:
    control = load_growth_control()
    return {
        "board_type": "growth_object",
        "project_name": _text(control.get("project_name")) or "Growth System",
        "object_name": object_name,
        "updated_at": dt.date.today().isoformat(),
        "purpose": f"Codex Growth System 的 {object_name} 真相主表。",
    }


def render_growth_frontmatter(data: dict[str, Any]) -> str:
    order = [
        "board_type",
        "project_name",
        "object_name",
        "status",
        "priority",
        "updated_at",
        "purpose",
        "next_action",
        "summary",
    ]
    lines = ["---"]
    seen: set[str] = set()
    for key in order:
        if key not in data:
            continue
        value = data.get(key)
        seen.add(key)
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {_text(item)}")
            continue
        if value is not None:
            lines.append(f"{key}: {_text(value)}")
    for key in sorted(data):
        if key in seen:
            continue
        value = data.get(key)
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {_text(item)}")
            continue
        if value is not None:
            lines.append(f"{key}: {_text(value)}")
    lines.append("---")
    return "\n".join(lines)


def render_object_file(object_name: str, rows: list[dict[str, str]], *, frontmatter: dict[str, Any] | None = None) -> str:
    meta = dict(default_frontmatter(object_name))
    meta.update(frontmatter or {})
    meta["object_name"] = object_name
    meta["updated_at"] = dt.date.today().isoformat()
    title = f"# {_text(meta.get('project_name')) or 'Growth System'}｜Growth System｜{object_name}"
    headers = object_headers(object_name)
    body = "\n".join([title, "", *codex_memory.markdown_table_lines(headers, rows)]).rstrip() + "\n"
    return f"{render_growth_frontmatter(meta)}\n\n{body}"


def ensure_object_file(object_name: str) -> Path:
    path = object_path(object_name)
    return ensure_object_file_at(object_name, path)


def ensure_object_file_at(object_name: str, path: Path, *, project_name: str = "") -> Path:
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    frontmatter = {"project_name": project_name} if _text(project_name) else None
    path.write_text(render_object_file(object_name, [], frontmatter=frontmatter), encoding="utf-8")
    return path


def load_rows(object_name: str) -> list[dict[str, str]]:
    path = ensure_object_file(object_name)
    return load_rows_at(object_name, path)


def load_rows_at(object_name: str, path: Path, *, project_name: str = "") -> list[dict[str, str]]:
    path = ensure_object_file_at(object_name, path, project_name=project_name)
    text = codex_memory.read_text(path)
    _frontmatter, body = codex_memory.parse_frontmatter(text)
    return codex_memory.parse_markdown_table(body, object_headers(object_name), allow_missing=True)


def save_rows(object_name: str, rows: list[dict[str, str]]) -> Path:
    path = ensure_object_file(object_name)
    return save_rows_at(object_name, path, rows)


def save_rows_at(object_name: str, path: Path, rows: list[dict[str, str]], *, project_name: str = "") -> Path:
    path = ensure_object_file_at(object_name, path, project_name=project_name)
    text = codex_memory.read_text(path)
    frontmatter, _body = codex_memory.parse_frontmatter(text)
    if _text(project_name) and not _text(frontmatter.get("project_name")):
        frontmatter["project_name"] = _text(project_name)
    normalized_rows = [{header: _text(row.get(header, "")) for header in object_headers(object_name)} for row in rows]
    codex_memory.write_text(path, render_object_file(object_name, normalized_rows, frontmatter=frontmatter))
    return path


def upsert_rows(object_name: str, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    return upsert_rows_at(object_name, object_path(object_name), rows)


def upsert_rows_at(object_name: str, path: Path, rows: list[dict[str, Any]], *, project_name: str = "") -> list[dict[str, str]]:
    key_field = object_key_field(object_name)
    existing = load_rows_at(object_name, path, project_name=project_name)
    by_key = {_text(row.get(key_field)): dict(row) for row in existing if _text(row.get(key_field))}
    for candidate in rows:
        key = _text(candidate.get(key_field))
        if not key:
            raise ValueError(f"missing `{key_field}` for growth object `{object_name}`")
        row = dict(by_key.get(key, {header: "" for header in object_headers(object_name)}))
        for header in object_headers(object_name):
            if header in candidate:
                row[header] = _text(candidate.get(header))
        by_key[key] = row
    merged = list(by_key.values())
    merged.sort(key=lambda item: _text(item.get(key_field)))
    save_rows_at(object_name, path, merged, project_name=project_name)
    return merged


def delete_rows(object_name: str, keys: list[str]) -> list[dict[str, str]]:
    key_field = object_key_field(object_name)
    target_keys = {_text(item) for item in keys if _text(item)}
    remaining = [row for row in load_rows(object_name) if _text(row.get(key_field)) not in target_keys]
    save_rows(object_name, remaining)
    return remaining


def snapshot() -> dict[str, Any]:
    return {
        "ok": True,
        "objects": {
            object_name: {
                "path": str(object_path(object_name)),
                "key_field": object_key_field(object_name),
                "row_count": len(load_rows(object_name)),
            }
            for object_name in object_names()
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Codex Growth System truth tables")
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("snapshot")

    upsert = subparsers.add_parser("upsert")
    upsert.add_argument("--object", required=True)
    upsert.add_argument("--payload-json", required=True)

    delete = subparsers.add_parser("delete")
    delete.add_argument("--object", required=True)
    delete.add_argument("--keys-json", required=True)

    args = parser.parse_args()
    if args.action == "snapshot":
        print(json.dumps(snapshot(), ensure_ascii=False, indent=2))
        return
    if args.action == "upsert":
        payload = json.loads(args.payload_json)
        rows = payload if isinstance(payload, list) else [payload]
        print(json.dumps({"ok": True, "rows": upsert_rows(args.object, rows)}, ensure_ascii=False, indent=2))
        return
    if args.action == "delete":
        payload = json.loads(args.keys_json)
        keys = payload if isinstance(payload, list) else [payload]
        print(json.dumps({"ok": True, "rows": delete_rows(args.object, [str(item) for item in keys])}, ensure_ascii=False, indent=2))
        return
    raise SystemExit(2)


if __name__ == "__main__":
    main()

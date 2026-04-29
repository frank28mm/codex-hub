#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import codex_memory, workspace_hub_project
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import workspace_hub_project  # type: ignore


PROJECT_NAME = "增长与营销"
TABLE_SPECS: dict[str, dict[str, Any]] = {
    "asset": {
        "title": f"# {PROJECT_NAME}｜内容资产主表",
        "path_name": f"{PROJECT_NAME}-内容资产主表.md",
        "purpose": f"{PROJECT_NAME} 项目级内容资产 canonical 主表。",
        "headers": [
            "asset_id",
            "asset_type",
            "project_name",
            "product_or_service",
            "channel",
            "topic",
            "source_bucket",
            "source_path",
            "checksum",
            "status",
            "created_at",
            "updated_at",
        ],
    },
    "publish": {
        "title": f"# {PROJECT_NAME}｜已发布记录",
        "path_name": f"{PROJECT_NAME}-已发布记录.md",
        "purpose": f"{PROJECT_NAME} 的已发布内容 canonical 主表。",
        "headers": [
            "publish_id",
            "asset_id",
            "project_name",
            "product_or_service",
            "channel",
            "publish_date",
            "publish_time",
            "visible_time_text",
            "location",
            "title",
            "body",
            "content_kind",
            "topic_tags",
            "like_count",
            "comment_count",
            "dm_count",
            "qualified_lead_count",
            "status",
            "next_action",
            "source_path",
        ],
    },
    "feedback": {
        "title": f"# {PROJECT_NAME}｜反馈线索记录",
        "path_name": f"{PROJECT_NAME}-反馈线索记录.md",
        "purpose": f"{PROJECT_NAME} 的反馈线索 canonical 主表。",
        "headers": [
            "feedback_id",
            "publish_id",
            "asset_id",
            "project_name",
            "product_or_service",
            "channel",
            "feedback_date",
            "feedback_time",
            "signal_summary",
            "like_count",
            "comment_count",
            "dm_count",
            "qualified_lead_count",
            "followup_status",
            "next_action",
            "source_path",
        ],
    },
}


def vault_root() -> Path:
    raw = str(os.environ.get("WORKSPACE_HUB_VAULT_ROOT", "")).strip()
    if raw:
        return Path(raw)
    return workspace_hub_project.DEFAULT_LOCAL_VAULT_ROOT


def working_root() -> Path:
    return vault_root() / "01_working"


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _today_text() -> str:
    return dt.date.today().isoformat()


def _table_spec(table_kind: str) -> dict[str, Any]:
    normalized = str(table_kind).strip().lower()
    spec = TABLE_SPECS.get(normalized)
    if not isinstance(spec, dict):
        raise KeyError(f"unknown table kind `{table_kind}`")
    return spec


def table_headers(table_kind: str) -> list[str]:
    return list(_table_spec(table_kind)["headers"])


def table_path(table_kind: str) -> Path:
    return working_root() / str(_table_spec(table_kind)["path_name"])


def default_frontmatter(table_kind: str) -> dict[str, Any]:
    spec = _table_spec(table_kind)
    return {
        "board_type": "growth_content_object",
        "project_name": PROJECT_NAME,
        "content_table": str(table_kind).strip().lower(),
        "updated_at": _today_text(),
        "purpose": _text(spec.get("purpose")),
    }


def render_frontmatter(data: dict[str, Any]) -> str:
    order = ["board_type", "project_name", "content_table", "status", "priority", "updated_at", "purpose"]
    lines = ["---"]
    seen: set[str] = set()
    for key in order:
        if key not in data:
            continue
        seen.add(key)
        lines.append(f"{key}: {_text(data.get(key))}")
    for key in sorted(data):
        if key in seen:
            continue
        lines.append(f"{key}: {_text(data.get(key))}")
    lines.append("---")
    return "\n".join(lines)


def render_table_file(table_kind: str, rows: list[dict[str, str]], *, frontmatter: dict[str, Any] | None = None) -> str:
    spec = _table_spec(table_kind)
    meta = dict(default_frontmatter(table_kind))
    meta.update(frontmatter or {})
    meta["content_table"] = str(table_kind).strip().lower()
    meta["updated_at"] = _today_text()
    body = "\n".join(
        [
            render_frontmatter(meta),
            "",
            str(spec["title"]),
            "",
            *codex_memory.markdown_table_lines(table_headers(table_kind), rows),
            "",
        ]
    )
    return body


def ensure_table_file(table_kind: str) -> Path:
    path = table_path(table_kind)
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_table_file(table_kind, []), encoding="utf-8")
    return path


def load_rows(table_kind: str) -> list[dict[str, str]]:
    path = ensure_table_file(table_kind)
    text = codex_memory.read_text(path)
    _frontmatter, body = codex_memory.parse_frontmatter(text)
    return codex_memory.parse_markdown_table(body, table_headers(table_kind), allow_missing=True)


def save_rows(table_kind: str, rows: list[dict[str, Any]]) -> Path:
    path = ensure_table_file(table_kind)
    text = codex_memory.read_text(path)
    frontmatter, _body = codex_memory.parse_frontmatter(text)
    normalized = [{header: _text(row.get(header, "")) for header in table_headers(table_kind)} for row in rows]
    codex_memory.write_text(path, render_table_file(table_kind, normalized, frontmatter=frontmatter))
    return path


def upsert_rows(table_kind: str, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    headers = table_headers(table_kind)
    key_field = headers[0]
    existing = load_rows(table_kind)
    by_key = {_text(row.get(key_field)): dict(row) for row in existing if _text(row.get(key_field))}
    for candidate in rows:
        key = _text(candidate.get(key_field))
        if not key:
            raise ValueError(f"missing `{key_field}` for table `{table_kind}`")
        row = dict(by_key.get(key, {header: "" for header in headers}))
        for header in headers:
            if header in candidate:
                row[header] = _text(candidate.get(header))
        if "project_name" in headers and not row.get("project_name"):
            row["project_name"] = PROJECT_NAME
        by_key[key] = row
    merged = list(by_key.values())
    merged.sort(key=lambda item: _text(item.get(key_field)))
    save_rows(table_kind, merged)
    return merged


def snapshot() -> dict[str, Any]:
    payload: dict[str, Any] = {"ok": True, "tables": {}}
    for table_kind in TABLE_SPECS:
        rows = load_rows(table_kind)
        payload["tables"][table_kind] = {
            "path": str(table_path(table_kind)),
            "row_count": len(rows),
            "headers": table_headers(table_kind),
        }
    return payload


def build_asset_row(
    *,
    asset_id: str,
    asset_type: str,
    product_or_service: str = "",
    channel: str = "",
    topic: str = "",
    source_bucket: str = "",
    source_path: str = "",
    checksum: str = "",
    status: str = "",
    created_at: str = "",
    updated_at: str = "",
) -> dict[str, str]:
    return {
        "asset_id": _text(asset_id),
        "asset_type": _text(asset_type),
        "project_name": PROJECT_NAME,
        "product_or_service": _text(product_or_service),
        "channel": _text(channel),
        "topic": _text(topic),
        "source_bucket": _text(source_bucket),
        "source_path": _text(source_path),
        "checksum": _text(checksum),
        "status": _text(status),
        "created_at": _text(created_at),
        "updated_at": _text(updated_at) or _today_text(),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage 增长与营销 canonical content truth tables")
    subparsers = parser.add_subparsers(dest="action", required=True)

    subparsers.add_parser("snapshot")

    upsert = subparsers.add_parser("upsert")
    upsert.add_argument("--table", required=True, choices=sorted(TABLE_SPECS))
    upsert.add_argument("--payload-json", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action == "snapshot":
        print(json.dumps(snapshot(), ensure_ascii=False, indent=2))
        return 0
    payload = json.loads(args.payload_json)
    rows = payload if isinstance(payload, list) else [payload]
    print(json.dumps({"ok": True, "rows": upsert_rows(args.table, rows)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

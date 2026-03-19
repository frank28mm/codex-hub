#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

try:
    from ops import codex_memory, runtime_state
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import runtime_state  # type: ignore


COORDINATION_HEADERS = [
    "coordination_id",
    "from_project",
    "to_project",
    "source_ref",
    "requested_action",
    "status",
    "assignee",
    "receipt_ref",
    "due_at",
    "updated_at",
]
OPEN_COORDINATION_STATUSES = {"pending", "acknowledged", "in_progress"}


def ensure_coordination_file() -> None:
    path = codex_memory.COORDINATION_MD
    if path.exists():
        return
    text = (
        "# COORDINATION\n\n"
        "跨项目协同真相源。\n\n"
        "## Coordination Table\n\n"
        "<!-- AUTO_COORDINATION_TABLE_START -->\n"
        "| coordination_id | from_project | to_project | source_ref | requested_action | status | assignee | receipt_ref | due_at | updated_at |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "<!-- AUTO_COORDINATION_TABLE_END -->\n"
    )
    codex_memory.write_text(path, text)


def load_rows() -> list[dict[str, str]]:
    ensure_coordination_file()
    text = codex_memory.read_text(codex_memory.COORDINATION_MD)
    return codex_memory.parse_markdown_table(
        codex_memory.extract_marked_block(text, ("<!-- AUTO_COORDINATION_TABLE_START -->", "<!-- AUTO_COORDINATION_TABLE_END -->")),
        COORDINATION_HEADERS,
        allow_missing=True,
    )


def save_rows(rows: list[dict[str, str]]) -> None:
    ensure_coordination_file()
    text = codex_memory.read_text(codex_memory.COORDINATION_MD)
    text = codex_memory.replace_or_append_marked_section(
        text,
        "## Coordination Table",
        ("<!-- AUTO_COORDINATION_TABLE_START -->", "<!-- AUTO_COORDINATION_TABLE_END -->"),
        codex_memory.markdown_table_lines(COORDINATION_HEADERS, rows),
    )
    codex_memory.write_text(codex_memory.COORDINATION_MD, text)


def coordination_items(*, project_name: str = "") -> list[dict[str, Any]]:
    rows = load_rows()
    items: list[dict[str, Any]] = []
    current_project = codex_memory.canonical_project_name(project_name) if project_name else ""
    for row in rows:
        if current_project and current_project not in {row.get("from_project", ""), row.get("to_project", "")}:
            continue
        items.append(
            {
                "coordination_id": row.get("coordination_id", ""),
                "from_project": row.get("from_project", ""),
                "to_project": row.get("to_project", ""),
                "source_ref": row.get("source_ref", ""),
                "requested_action": row.get("requested_action", ""),
                "status": row.get("status", ""),
                "assignee": row.get("assignee", ""),
                "receipt_ref": row.get("receipt_ref", ""),
                "due_at": row.get("due_at", ""),
                "updated_at": row.get("updated_at", ""),
                "metadata": {},
            }
        )
    items.sort(key=lambda item: (item["status"], item.get("updated_at", ""), item["coordination_id"]))
    return items


def rebuild_coordination_projection(*, sync_runtime: bool = True) -> dict[str, Any]:
    ensure_coordination_file()
    items = coordination_items()
    if sync_runtime:
        runtime_state.init_db()
        runtime_state.replace_coordination_items(items)
    return {"path": str(codex_memory.COORDINATION_MD), "item_count": len(items)}


def create_coordination(
    *,
    coordination_id: str,
    from_project: str,
    to_project: str,
    source_ref: str,
    requested_action: str,
    assignee: str = "",
    due_at: str = "",
) -> dict[str, str]:
    rows = load_rows()
    if any(row.get("coordination_id") == coordination_id for row in rows):
        raise ValueError(f"coordination_id already exists: {coordination_id}")
    row = {
        "coordination_id": coordination_id,
        "from_project": codex_memory.canonical_project_name(from_project),
        "to_project": codex_memory.canonical_project_name(to_project),
        "source_ref": source_ref,
        "requested_action": requested_action,
        "status": "pending",
        "assignee": assignee,
        "receipt_ref": "",
        "due_at": due_at,
        "updated_at": codex_memory.iso_now(),
    }
    rows.append(row)
    save_rows(rows)
    rebuild_coordination_projection(sync_runtime=True)
    codex_memory.update_now_and_next_actions(codex_memory.load_bindings().get("bindings", []))
    codex_memory.trigger_dashboard_sync_once()
    return row


def update_coordination(
    coordination_id: str,
    *,
    status: str,
    assignee: str = "",
    receipt_ref: str = "",
) -> dict[str, str]:
    rows = load_rows()
    target = next((row for row in rows if row.get("coordination_id") == coordination_id), None)
    if not target:
        raise KeyError(f"unknown coordination_id `{coordination_id}`")
    target["status"] = status
    if assignee:
        target["assignee"] = assignee
    if receipt_ref:
        target["receipt_ref"] = receipt_ref
    target["updated_at"] = codex_memory.iso_now()
    save_rows(rows)
    rebuild_coordination_projection(sync_runtime=True)
    codex_memory.update_now_and_next_actions(codex_memory.load_bindings().get("bindings", []))
    codex_memory.trigger_dashboard_sync_once()
    return target


def followup_lines(*, limit: int = 10) -> list[str]:
    lines: list[str] = []
    for item in coordination_items():
        if item["status"] not in OPEN_COORDINATION_STATUSES:
            continue
        lines.append(
            f"- [ ] 协同 `{item['from_project']}` -> `{item['to_project']}` `{item['coordination_id']}` | 状态：{item['status']} | 事项：{item['requested_action']}"
        )
        if len(lines) >= limit:
            break
    return lines


def cmd_create(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            create_coordination(
                coordination_id=args.coordination_id,
                from_project=args.from_project,
                to_project=args.to_project,
                source_ref=args.source_ref,
                requested_action=args.requested_action,
                assignee=args.assignee,
                due_at=args.due_at,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            update_coordination(
                args.coordination_id,
                status=args.status,
                assignee=args.assignee,
                receipt_ref=args.receipt_ref,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    print(json.dumps({"items": coordination_items(project_name=args.project_name)}, ensure_ascii=False))
    return 0


def cmd_rebuild(_args: argparse.Namespace) -> int:
    print(json.dumps(rebuild_coordination_projection(sync_runtime=True), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub coordination plane")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--coordination-id", required=True)
    create.add_argument("--from-project", required=True)
    create.add_argument("--to-project", required=True)
    create.add_argument("--source-ref", default="")
    create.add_argument("--requested-action", required=True)
    create.add_argument("--assignee", default="")
    create.add_argument("--due-at", default="")
    create.set_defaults(func=cmd_create)

    update = subparsers.add_parser("update")
    update.add_argument("--coordination-id", required=True)
    update.add_argument("--status", required=True)
    update.add_argument("--assignee", default="")
    update.add_argument("--receipt-ref", default="")
    update.set_defaults(func=cmd_update)

    list_cmd = subparsers.add_parser("list")
    list_cmd.add_argument("--project-name", default="")
    list_cmd.set_defaults(func=cmd_list)

    rebuild = subparsers.add_parser("rebuild")
    rebuild.set_defaults(func=cmd_rebuild)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

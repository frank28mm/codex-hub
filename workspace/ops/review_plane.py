#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from ops import codex_memory, runtime_state
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import runtime_state  # type: ignore


OPEN_REVIEW_STATUSES = {"pending_review", "changes_requested", "rejected"}
INBOX_HEADERS = ["任务", "项目", "板面", "审核状态", "审核人", "交付物", "审核结论", "审核时间"]


def iso_now() -> str:
    return codex_memory.iso_now()


def ensure_review_inbox() -> Path:
    path = codex_memory.REVIEW_INBOX_MD
    if path.exists():
        return path
    text = (
        "# REVIEW INBOX\n\n"
        "派生视图：只展示待处理或需要继续判断的审核项。\n\n"
        "## Auto Review Inbox\n\n"
        "<!-- AUTO_REVIEW_INBOX_START -->\n"
        "| 任务 | 项目 | 板面 | 审核状态 | 审核人 | 交付物 | 审核结论 | 审核时间 |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "<!-- AUTO_REVIEW_INBOX_END -->\n"
    )
    codex_memory.write_text(path, text)
    return path


def parse_table() -> list[dict[str, str]]:
    text = codex_memory.read_text(ensure_review_inbox())
    return codex_memory.parse_markdown_table(
        codex_memory.extract_marked_block(text, ("<!-- AUTO_REVIEW_INBOX_START -->", "<!-- AUTO_REVIEW_INBOX_END -->")),
        INBOX_HEADERS,
        allow_missing=True,
    )


def _task_refs(project_name: str) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    project_board = codex_memory.load_project_board(project_name)
    for index, row in enumerate(project_board["project_rows"]):
        refs.append(
            {
                "task_id": row.get("ID", ""),
                "project_name": project_name,
                "source_type": "project",
                "source_path": project_board["path"],
                "row_group": "project_rows",
                "rows": project_board["project_rows"],
                "index": index,
                "row": row,
            }
        )
    for topic_path in codex_memory.topic_board_paths(project_name):
        topic_board = codex_memory.load_topic_board(topic_path)
        for index, row in enumerate(topic_board["rows"]):
            refs.append(
                {
                    "task_id": row.get("ID", ""),
                    "project_name": project_name,
                    "source_type": "topic",
                    "source_path": topic_path,
                    "row_group": "topic_rows",
                    "rows": topic_board["rows"],
                    "index": index,
                    "row": row,
                }
            )
    return refs


def find_task_ref(project_name: str, task_id: str) -> dict[str, Any]:
    for item in _task_refs(codex_memory.canonical_project_name(project_name)):
        if item["task_id"] == task_id:
            return item
    raise KeyError(f"unknown task id `{task_id}` for project `{project_name}`")


def _persist_task_ref(ref: dict[str, Any]) -> None:
    project_name = ref["project_name"]
    if ref["source_type"] == "topic":
        topic_board = codex_memory.load_topic_board(ref["source_path"])
        codex_memory.save_topic_board(topic_board["path"], topic_board["frontmatter"], topic_board["body"], ref["rows"])
        codex_memory.refresh_project_rollups(project_name)
    else:
        project_board = codex_memory.load_project_board(project_name)
        if ref["source_path"] == project_board["path"]:
            project_rows = ref["rows"] if ref.get("row_group") == "project_rows" else project_board["project_rows"]
            rollup_rows = ref["rows"] if ref.get("row_group") == "rollup_rows" else project_board["rollup_rows"]
            codex_memory.save_project_board(
                project_board["path"],
                project_board["frontmatter"],
                project_board["body"],
                project_rows,
                rollup_rows,
            )


def submit_review(project_name: str, task_id: str, *, deliverable_ref: str, reviewer: str) -> dict[str, Any]:
    ref = find_task_ref(project_name, task_id)
    row = ref["rows"][ref["index"]]
    row["交付物"] = deliverable_ref
    row["审核状态"] = "pending_review"
    row["审核人"] = reviewer
    row["审核结论"] = ""
    row["审核时间"] = iso_now()
    _persist_task_ref(ref)
    rebuild_review_inbox(sync_runtime=True)
    codex_memory.update_now_and_next_actions(codex_memory.load_bindings().get("bindings", []))
    codex_memory.trigger_dashboard_sync_once()
    return row


def decide_review(
    project_name: str,
    task_id: str,
    *,
    review_status: str,
    reviewer: str,
    decision_note: str,
) -> dict[str, Any]:
    if review_status not in codex_memory.ALLOWED_REVIEW_STATUSES or review_status in {"", "draft"}:
        raise ValueError(f"invalid review status: {review_status}")
    ref = find_task_ref(project_name, task_id)
    row = ref["rows"][ref["index"]]
    row["审核状态"] = review_status
    row["审核人"] = reviewer
    row["审核结论"] = decision_note
    row["审核时间"] = iso_now()
    _persist_task_ref(ref)
    rebuild_review_inbox(sync_runtime=True)
    codex_memory.update_now_and_next_actions(codex_memory.load_bindings().get("bindings", []))
    codex_memory.trigger_dashboard_sync_once()
    return row


def review_items(*, project_name: str = "") -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    entries = codex_memory.load_registry()
    for entry in entries:
        current_project = entry["project_name"]
        if project_name and current_project != codex_memory.canonical_project_name(project_name):
            continue
        for ref in _task_refs(current_project):
            row = ref["row"]
            review_status = row.get("审核状态", "")
            if review_status not in OPEN_REVIEW_STATUSES:
                continue
            items.append(
                {
                    "task_ref": f"{current_project}:{row.get('ID', '')}",
                    "task_id": row.get("ID", ""),
                    "project_name": current_project,
                    "source_path": str(ref["source_path"]),
                    "review_status": review_status,
                    "reviewer": row.get("审核人", ""),
                    "deliverable_ref": row.get("交付物", ""),
                    "decision_note": row.get("审核结论", ""),
                    "decided_at": row.get("审核时间", ""),
                    "metadata": {
                        "task_name": row.get("事项", ""),
                        "source_type": ref["source_type"],
                    },
                }
            )
    items.sort(key=lambda item: (item["review_status"], item.get("decided_at", ""), item["task_ref"]))
    return items


def rebuild_review_inbox(*, sync_runtime: bool = True) -> dict[str, Any]:
    path = ensure_review_inbox()
    items = review_items()
    rows = [
        {
            "任务": f"`{item['task_id']}` {item['metadata'].get('task_name', '')}",
            "项目": item["project_name"],
            "板面": item["source_path"],
            "审核状态": item["review_status"],
            "审核人": item["reviewer"],
            "交付物": item["deliverable_ref"],
            "审核结论": item["decision_note"],
            "审核时间": codex_memory.display_timestamp(item["decided_at"]) if item["decided_at"] else "",
        }
        for item in items
    ]
    body = codex_memory.replace_or_append_marked_section(
        codex_memory.read_text(path),
        "## Auto Review Inbox",
        ("<!-- AUTO_REVIEW_INBOX_START -->", "<!-- AUTO_REVIEW_INBOX_END -->"),
        codex_memory.markdown_table_lines(INBOX_HEADERS, rows),
    )
    codex_memory.write_text(path, body)
    if sync_runtime:
        runtime_state.init_db()
        runtime_state.replace_review_items(items)
    return {"path": str(path), "item_count": len(items)}


def followup_lines(*, limit: int = 10) -> list[str]:
    lines: list[str] = []
    for item in review_items()[:limit]:
        lines.append(
            f"- [ ] 审核 `{item['project_name']}` {item['task_id']} | 状态：{item['review_status']} | 交付物：{item['deliverable_ref'] or '待补充'}"
        )
    return lines


def cmd_submit(args: argparse.Namespace) -> int:
    print(json.dumps(submit_review(args.project_name, args.task_id, deliverable_ref=args.deliverable, reviewer=args.reviewer), ensure_ascii=False))
    return 0


def cmd_decide(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            decide_review(
                args.project_name,
                args.task_id,
                review_status=args.review_status,
                reviewer=args.reviewer,
                decision_note=args.decision_note,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def cmd_rebuild(_args: argparse.Namespace) -> int:
    print(json.dumps(rebuild_review_inbox(sync_runtime=True), ensure_ascii=False))
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    print(json.dumps({"items": review_items(project_name=args.project_name)}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub review plane")
    subparsers = parser.add_subparsers(dest="command", required=True)

    submit = subparsers.add_parser("submit")
    submit.add_argument("--project-name", required=True)
    submit.add_argument("--task-id", required=True)
    submit.add_argument("--deliverable", required=True)
    submit.add_argument("--reviewer", required=True)
    submit.set_defaults(func=cmd_submit)

    decide = subparsers.add_parser("decide")
    decide.add_argument("--project-name", required=True)
    decide.add_argument("--task-id", required=True)
    decide.add_argument("--review-status", required=True)
    decide.add_argument("--reviewer", required=True)
    decide.add_argument("--decision-note", default="")
    decide.set_defaults(func=cmd_decide)

    rebuild = subparsers.add_parser("rebuild-inbox")
    rebuild.set_defaults(func=cmd_rebuild)

    list_cmd = subparsers.add_parser("list")
    list_cmd.add_argument("--project-name", default="")
    list_cmd.set_defaults(func=cmd_list)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

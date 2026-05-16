#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any

try:
    from ops import runtime_state
except ImportError:  # pragma: no cover
    import runtime_state  # type: ignore


MAX_QUEUE_ITEMS = 10


def runtime_dir() -> Path:
    return runtime_state.runtime_root() / "wechat-gui-send"


def queue_dir() -> Path:
    return runtime_dir() / "queues"


def queue_path(queue_id: str) -> Path:
    return queue_dir() / f"{queue_id}.json"


def _write_queue(payload: dict[str, Any]) -> None:
    path = queue_path(payload["queue_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def load_queue(queue_id: str) -> dict[str, Any]:
    return json.loads(queue_path(queue_id).read_text(encoding="utf-8"))


def _normalize_item(item: dict[str, Any], ordinal: int) -> dict[str, Any]:
    recipient_name = str(item.get("recipient_name", "")).strip()
    message_text = str(item.get("message_text", ""))
    if not recipient_name:
        raise ValueError("recipient_name_required")
    if not message_text.strip():
        raise ValueError("message_text_required")
    timestamp = runtime_state.iso_now()
    return {
        "recipient_name": recipient_name,
        "message_text": message_text,
        "ordinal": ordinal,
        "status": "prepared",
        "error": "",
        "prepared_at": timestamp,
        "sent_at": "",
    }


def prepare_queue(
    items: list[dict[str, Any]],
    *,
    thread_scope: str = "",
) -> dict[str, Any]:
    if not items:
        raise ValueError("queue_items_required")
    if len(items) > MAX_QUEUE_ITEMS:
        raise ValueError("queue_item_limit_exceeded")
    queue_id = f"wgq_{uuid.uuid4().hex[:12]}"
    payload = {
        "queue_id": queue_id,
        "thread_scope": str(thread_scope or "").strip(),
        "status": "prepared",
        "created_at": runtime_state.iso_now(),
        "confirmed_at": "",
        "items": [_normalize_item(item, index + 1) for index, item in enumerate(items)],
        "execution_report": {},
    }
    _write_queue(payload)
    return payload


def review_queue(queue_id: str) -> dict[str, Any]:
    payload = load_queue(queue_id)
    return {
        "queue_id": payload["queue_id"],
        "status": payload["status"],
        "total_items": len(payload["items"]),
        "items": [
            {
                "ordinal": item["ordinal"],
                "recipient_name": item["recipient_name"],
                "message_preview": item["message_text"][:80],
                "status": item["status"],
            }
            for item in payload["items"]
        ],
    }


def confirm_queue(queue_id: str, *, confirmed: bool) -> dict[str, Any]:
    if not confirmed:
        return {
            "ok": False,
            "queue_id": queue_id,
            "error": "confirmation_required",
        }
    payload = load_queue(queue_id)
    payload["status"] = "confirmed"
    payload["confirmed_at"] = runtime_state.iso_now()
    _write_queue(payload)
    return payload


def record_execution_result(
    queue_id: str,
    *,
    sent: int,
    failed: int,
    skipped: int,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = load_queue(queue_id)
    payload["status"] = "executed"
    payload["execution_report"] = {
        "sent": sent,
        "failed": failed,
        "skipped": skipped,
        "items": items,
        "updated_at": runtime_state.iso_now(),
    }
    _write_queue(payload)
    return payload


def queue_status(queue_id: str) -> dict[str, Any]:
    payload = load_queue(queue_id)
    report = dict(payload.get("execution_report") or {})
    return {
        "queue_id": payload["queue_id"],
        "status": payload["status"],
        "confirmed_at": payload.get("confirmed_at", ""),
        "total_items": len(payload.get("items") or []),
        "execution_report": report,
    }


def cmd_prepare(args: argparse.Namespace) -> int:
    payload = prepare_queue(
        json.loads(args.items_json),
        thread_scope=args.thread_scope,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    print(json.dumps(review_queue(args.queue_id), ensure_ascii=False, indent=2))
    return 0


def cmd_confirm(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            confirm_queue(args.queue_id, confirmed=args.confirmed),
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    print(json.dumps(queue_status(args.queue_id), ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare and track WeChat GUI send queues for Computer Use-driven delivery."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--items-json", required=True)
    prepare.add_argument("--thread-scope", default="")
    prepare.set_defaults(func=cmd_prepare)

    review = subparsers.add_parser("review")
    review.add_argument("--queue-id", required=True)
    review.set_defaults(func=cmd_review)

    confirm = subparsers.add_parser("confirm")
    confirm.add_argument("--queue-id", required=True)
    confirm.add_argument("--confirmed", action="store_true")
    confirm.set_defaults(func=cmd_confirm)

    status = subparsers.add_parser("status")
    status.add_argument("--queue-id", required=True)
    status.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())

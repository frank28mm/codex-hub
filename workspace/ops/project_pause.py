#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import yaml


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1])))


def control_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", str(workspace_root() / "control")))


def pause_config_path() -> Path:
    return control_root() / "project-pauses.yaml"


def canonical_project_name(project_name: str) -> str:
    raw = str(project_name or "").strip()
    if not raw:
        return ""
    try:
        from ops import codex_memory
    except ImportError:  # pragma: no cover
        try:
            import codex_memory  # type: ignore
        except ImportError:
            return raw
    return str(codex_memory.canonical_project_name(raw)).strip() or raw


def local_today() -> dt.date:
    return dt.datetime.now().astimezone().date()


def parse_date(text: Any) -> dt.date | None:
    value = str(text or "").strip()
    if not value:
        return None
    try:
        return dt.date.fromisoformat(value)
    except ValueError:
        return None


def read_config() -> dict[str, Any]:
    path = pause_config_path()
    if not path.exists():
        return {"version": 1, "entries": []}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must decode to a mapping")
    entries = data.get("entries", [])
    if not isinstance(entries, list):
        raise ValueError(f"{path} entries must be a list")
    return {"version": int(data.get("version", 1) or 1), "entries": entries}


def normalize_entry(raw: dict[str, Any], *, index: int) -> dict[str, Any]:
    entry_id = str(raw.get("id", "")).strip() or f"pause-{index}"
    project_name = canonical_project_name(raw.get("project_name", ""))
    bridge = str(raw.get("bridge", "")).strip()
    chat_ref = str(raw.get("chat_ref", "")).strip()
    start_date = parse_date(raw.get("start_date", ""))
    end_date = parse_date(raw.get("end_date", "")) or start_date
    scopes = raw.get("scopes", [])
    if not isinstance(scopes, list):
        scopes = []
    normalized_scopes = [str(item).strip() for item in scopes if str(item).strip()]
    return {
        "id": entry_id,
        "project_name": project_name,
        "bridge": bridge,
        "chat_ref": chat_ref,
        "start_date": start_date.isoformat() if start_date else "",
        "end_date": end_date.isoformat() if end_date else "",
        "reason": str(raw.get("reason", "")).strip(),
        "requested_by": str(raw.get("requested_by", "")).strip(),
        "requested_at": str(raw.get("requested_at", "")).strip(),
        "scopes": normalized_scopes,
    }


def entry_matches(
    entry: dict[str, Any],
    *,
    project_name: str = "",
    bridge: str = "",
    chat_ref: str = "",
    scope: str = "",
    on_date: dt.date,
) -> bool:
    start_date = parse_date(entry.get("start_date", ""))
    end_date = parse_date(entry.get("end_date", "")) or start_date
    if start_date and on_date < start_date:
        return False
    if end_date and on_date > end_date:
        return False
    entry_project = canonical_project_name(entry.get("project_name", ""))
    if entry_project:
        if not project_name:
            return False
        if canonical_project_name(project_name) != entry_project:
            return False
    entry_bridge = str(entry.get("bridge", "")).strip()
    if entry_bridge and str(bridge or "").strip() != entry_bridge:
        return False
    entry_chat_ref = str(entry.get("chat_ref", "")).strip()
    if entry_chat_ref and str(chat_ref or "").strip() != entry_chat_ref:
        return False
    scopes = [str(item).strip() for item in entry.get("scopes", []) if str(item).strip()]
    if scope and scopes and "*" not in scopes and scope not in scopes:
        return False
    return True


def active_pause(
    *,
    project_name: str = "",
    bridge: str = "",
    chat_ref: str = "",
    scope: str = "",
    on_date: dt.date | None = None,
) -> dict[str, Any]:
    target_date = on_date or local_today()
    config = read_config()
    for index, raw in enumerate(config.get("entries", []), start=1):
        if not isinstance(raw, dict):
            continue
        entry = normalize_entry(raw, index=index)
        if entry_matches(
            entry,
            project_name=project_name,
            bridge=bridge,
            chat_ref=chat_ref,
            scope=scope,
            on_date=target_date,
        ):
            return {
                "active": True,
                "date": target_date.isoformat(),
                "scope": scope,
                "entry": entry,
            }
    return {
        "active": False,
        "date": target_date.isoformat(),
        "scope": scope,
        "entry": {},
    }


def pause_summary(payload: dict[str, Any]) -> str:
    if not payload.get("active"):
        return ""
    entry = payload.get("entry", {}) if isinstance(payload.get("entry", {}), dict) else {}
    project_name = str(entry.get("project_name", "")).strip()
    start_date = str(entry.get("start_date", "")).strip()
    end_date = str(entry.get("end_date", "")).strip()
    reason = str(entry.get("reason", "")).strip()
    parts = []
    if project_name:
        parts.append(f"`{project_name}`")
    if start_date and end_date:
        if start_date == end_date:
            parts.append(start_date)
        else:
            parts.append(f"{start_date} to {end_date}")
    if reason:
        parts.append(reason)
    return " | ".join(parts)


def cmd_status(args: argparse.Namespace) -> int:
    payload = active_pause(
        project_name=args.project_name,
        bridge=args.bridge,
        chat_ref=args.chat_ref,
        scope=args.scope,
        on_date=parse_date(args.date) or None,
    )
    payload["summary"] = pause_summary(payload)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub project pause utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    status.add_argument("--project-name", default="")
    status.add_argument("--bridge", default="")
    status.add_argument("--chat-ref", default="")
    status.add_argument("--scope", default="")
    status.add_argument("--date", default="")
    status.set_defaults(func=cmd_status)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

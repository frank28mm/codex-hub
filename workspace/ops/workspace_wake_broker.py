#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import codex_memory, workspace_hub_project


DEFAULT_WORKSPACE_ROOT = workspace_hub_project.DEFAULT_WORKSPACE_ROOT
RUNNING_STALE_SECONDS = 30 * 60
DEFAULT_REASON_PRIORITY = 5
REASON_PRIORITIES = {
    "manual_wake": 40,
    "wake_now": 40,
    "project_writeback": 30,
    "wake_catchup": 20,
    "interval": 10,
    "launchd": 10,
}


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(DEFAULT_WORKSPACE_ROOT)))


def runtime_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_RUNTIME_ROOT", str(workspace_root() / "runtime")))


def state_path() -> Path:
    return runtime_root() / "wake-broker.json"


def iso_now_local() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def parse_timestamp(text: str) -> dt.datetime | None:
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": "",
        "jobs": {},
    }


def load_state() -> dict[str, Any]:
    state = codex_memory.load_json(state_path(), empty_state())
    if not isinstance(state, dict):
        return empty_state()
    state.setdefault("version", 1)
    state.setdefault("jobs", {})
    return state


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = iso_now_local()
    codex_memory.dump_json(state_path(), state)


def normalize_reason(reason: str) -> str:
    normalized = reason.strip().lower().replace(" ", "_")
    return normalized or "unspecified"


def normalize_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    if not metadata:
        return {}
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if value in (None, ""):
            continue
        try:
            json.dumps(value, ensure_ascii=False)
        except TypeError:
            normalized[str(key)] = str(value)
            continue
        normalized[str(key)] = value
    return normalized


def resolve_priority(reason: str) -> int:
    return REASON_PRIORITIES.get(normalize_reason(reason), DEFAULT_REASON_PRIORITY)


def request_wake(
    job_name: str,
    *,
    reason: str,
    metadata: dict[str, Any] | None = None,
    requested_at: str = "",
) -> dict[str, Any]:
    normalized_reason = normalize_reason(reason)
    requested_at_text = requested_at or iso_now_local()
    requested = {
        "reason": normalized_reason,
        "priority": resolve_priority(normalized_reason),
        "requested_at": requested_at_text,
        "metadata": normalize_metadata(metadata),
    }
    with codex_memory.workspace_lock():
        state = load_state()
        jobs = state.setdefault("jobs", {})
        job_state = jobs.setdefault(job_name, {})
        pending = job_state.get("pending") or {}
        accepted = True
        if pending:
            pending_priority = int(pending.get("priority", DEFAULT_REASON_PRIORITY) or DEFAULT_REASON_PRIORITY)
            if pending_priority > requested["priority"]:
                accepted = False
            elif pending_priority == requested["priority"] and str(pending.get("requested_at", "")) > requested_at_text:
                accepted = False
        if accepted:
            job_state["pending"] = requested
            save_state(state)
        return {
            "accepted": accepted,
            "job_name": job_name,
            "pending": job_state.get("pending") or pending,
            "running": job_state.get("running") or {},
        }


def claim_wake(
    job_name: str,
    *,
    now: dt.datetime | None = None,
    stale_after_seconds: int = RUNNING_STALE_SECONDS,
) -> dict[str, Any]:
    current_time = now or dt.datetime.now().astimezone()
    claimed_at = current_time.isoformat(timespec="seconds")
    with codex_memory.workspace_lock():
        state = load_state()
        jobs = state.setdefault("jobs", {})
        job_state = jobs.setdefault(job_name, {})
        running = job_state.get("running") or {}
        if running:
            running_started_at = parse_timestamp(str(running.get("claimed_at", "")))
            if running_started_at is not None:
                age_seconds = (current_time - running_started_at).total_seconds()
                if age_seconds < stale_after_seconds:
                    return {
                        "claimed": False,
                        "reason": "wake_in_flight",
                        "running": running,
                        "pending": job_state.get("pending") or {},
                    }
            job_state["last_abandoned"] = running
            job_state["running"] = {}
        pending = job_state.get("pending") or {}
        if not pending:
            save_state(state)
            return {
                "claimed": False,
                "reason": "no_pending",
                "pending": {},
                "running": job_state.get("running") or {},
            }
        wake = {
            **pending,
            "wake_id": f"wake-{uuid.uuid4().hex[:10]}",
            "claimed_at": claimed_at,
        }
        job_state["pending"] = {}
        job_state["running"] = wake
        save_state(state)
        return {
            "claimed": True,
            "wake": wake,
        }


def complete_wake(
    job_name: str,
    *,
    wake_id: str,
    status: str,
    result: dict[str, Any] | None = None,
    completed_at: str = "",
) -> dict[str, Any]:
    completed_at_text = completed_at or iso_now_local()
    with codex_memory.workspace_lock():
        state = load_state()
        jobs = state.setdefault("jobs", {})
        job_state = jobs.setdefault(job_name, {})
        running = job_state.get("running") or {}
        if str(running.get("wake_id", "")) != wake_id:
            return {
                "completed": False,
                "reason": "wake_mismatch",
                "running": running,
            }
        entry = {
            **running,
            "status": status,
            "completed_at": completed_at_text,
            "result": normalize_metadata(result),
        }
        job_state["running"] = {}
        job_state["last_completed"] = entry
        save_state(state)
        return {
            "completed": True,
            "entry": entry,
        }


def job_status(job_name: str) -> dict[str, Any]:
    state = load_state()
    job_state = state.get("jobs", {}).get(job_name, {})
    return {
        "job_name": job_name,
        "pending": job_state.get("pending") or {},
        "running": job_state.get("running") or {},
        "last_completed": job_state.get("last_completed") or {},
        "last_abandoned": job_state.get("last_abandoned") or {},
        "state_path": str(state_path()),
    }


def cmd_request(args: argparse.Namespace) -> int:
    payload = request_wake(args.job_name, reason=args.reason)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    payload = claim_wake(args.job_name, stale_after_seconds=args.stale_after_seconds)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_complete(args: argparse.Namespace) -> int:
    payload = complete_wake(args.job_name, wake_id=args.wake_id, status=args.status)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    payload = job_status(args.job_name)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Workspace wake broker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    request = subparsers.add_parser("request")
    request.add_argument("--job-name", required=True)
    request.add_argument("--reason", required=True)
    request.set_defaults(func=cmd_request)

    claim = subparsers.add_parser("claim")
    claim.add_argument("--job-name", required=True)
    claim.add_argument("--stale-after-seconds", type=int, default=RUNNING_STALE_SECONDS)
    claim.set_defaults(func=cmd_claim)

    complete = subparsers.add_parser("complete")
    complete.add_argument("--job-name", required=True)
    complete.add_argument("--wake-id", required=True)
    complete.add_argument("--status", required=True)
    complete.set_defaults(func=cmd_complete)

    status = subparsers.add_parser("status")
    status.add_argument("--job-name", required=True)
    status.set_defaults(func=cmd_status)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import codex_memory, runtime_state, workspace_job_schema
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import runtime_state  # type: ignore
    import workspace_job_schema  # type: ignore


TERMINAL_LEASE_STATES = {"completed", "failed", "aborted", "released"}


def workspace_root() -> Path:
    return runtime_state.workspace_root()


def vault_root() -> Path:
    return codex_memory.VAULT_ROOT


def normalize_engine_name(value: str) -> str:
    text = str(value or "").strip().lower()
    if text in {"", "codex"}:
        return "codex"
    if text in {"claude", "claude-code", "claude_hub"}:
        return "claude"
    return re.sub(r"[^a-z0-9._-]+", "-", text).strip("-") or "codex"


def normalize_entry_surface(value: str, *, engine_name: str) -> str:
    text = re.sub(r"[^a-z0-9._-]+", "-", str(value or "").strip().lower()).strip("-")
    if text:
        return text
    return "claude-hub" if normalize_engine_name(engine_name) == "claude" else "codex-app"


def engine_runtime_root(*, engine_name: str, entry_surface: str = "") -> Path:
    normalized_engine = normalize_engine_name(engine_name)
    normalized_surface = normalize_entry_surface(entry_surface, engine_name=normalized_engine)
    return runtime_state.runtime_root() / "engines" / normalized_engine / normalized_surface


def slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value or "").strip()).strip("-")
    return text or "general"


def lease_scope_key(
    *,
    project_name: str = "",
    binding_scope: str = "",
    binding_board_path: str = "",
    topic_name: str = "",
    source_chat_ref: str = "",
) -> str:
    project = str(project_name or "").strip() or "general"
    scope = str(binding_scope or "").strip() or "chat"
    pointer = str(topic_name or "").strip()
    if not pointer and binding_board_path:
        pointer = Path(str(binding_board_path)).stem
    if not pointer:
        pointer = str(source_chat_ref or "").strip() or "workspace"
    basis = {
        "project_name": project,
        "binding_scope": scope,
        "pointer": pointer,
    }
    digest = hashlib.sha1(json.dumps(basis, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    return f"{slugify(project)}:{slugify(scope)}:{slugify(pointer)}:{digest}"


def default_approval_scope(*, engine_name: str, entry_surface: str) -> str:
    return f"{normalize_engine_name(engine_name)}_{normalize_entry_surface(entry_surface, engine_name=engine_name)}_runtime"


def adapter_manifest(*, engine_name: str, entry_surface: str, launch_source: str = "") -> dict[str, Any]:
    normalized_engine = normalize_engine_name(engine_name)
    normalized_surface = normalize_entry_surface(entry_surface, engine_name=normalized_engine)
    runtime_root = engine_runtime_root(engine_name=normalized_engine, entry_surface=normalized_surface)
    launcher_path = workspace_root() / "ops" / ("start-claude-hub" if normalized_engine == "claude" else "start-codex")
    broker_actions = ["codex-exec", "codex-resume"] if normalized_engine == "codex" else ["engine-exec", "engine-resume"]
    return workspace_job_schema.EngineAdapterManifest(
        adapter_id=f"{normalized_engine}:{normalized_surface}",
        engine_name=normalized_engine,
        entry_surface=normalized_surface,
        launcher_path=str(launcher_path),
        shared_memory_roots=[str(workspace_root()), str(vault_root())],
        isolated_runtime_root=str(runtime_root),
        session_namespace=f"{normalized_engine}.sessions",
        lease_namespace="workspace-hub.engine-session-leases",
        approval_namespace=default_approval_scope(engine_name=normalized_engine, entry_surface=normalized_surface),
        supports_resume=True,
        broker_actions=broker_actions,
        metadata={
            "launch_source": str(launch_source or "").strip(),
            "projection_root": str(runtime_root / "projection"),
            "scratch_root": str(runtime_root / "scratch"),
        },
    ).to_dict()


def session_contract(
    *,
    engine_name: str,
    entry_surface: str,
    project_name: str = "",
    binding_scope: str = "",
    binding_board_path: str = "",
    topic_name: str = "",
    launch_source: str = "",
    source_chat_ref: str = "",
    session_id: str = "",
    execution_profile: str = "",
    approval_state: str = "",
    lease_status: str = "",
) -> dict[str, Any]:
    normalized_engine = normalize_engine_name(engine_name)
    normalized_surface = normalize_entry_surface(entry_surface, engine_name=normalized_engine)
    normalized_session_id = str(session_id or "").strip() or str(uuid.uuid4())
    runtime_root = engine_runtime_root(engine_name=normalized_engine, entry_surface=normalized_surface)
    lease_key = lease_scope_key(
        project_name=project_name,
        binding_scope=binding_scope,
        binding_board_path=binding_board_path,
        topic_name=topic_name,
        source_chat_ref=source_chat_ref,
    )
    project_ref = str(project_name or "").strip() or "general"
    pointer = str(topic_name or "").strip() or Path(str(binding_board_path or "")).stem or str(source_chat_ref or "").strip() or "workspace"
    workspace_session_ref = f"{normalized_engine}:{slugify(project_ref)}:{slugify(pointer)}"
    return workspace_job_schema.EngineSessionContract(
        engine_name=normalized_engine,
        entry_surface=normalized_surface,
        launch_source=str(launch_source or "").strip(),
        project_name=str(project_name or "").strip(),
        binding_scope=str(binding_scope or "").strip() or "chat",
        binding_board_path=str(binding_board_path or "").strip(),
        topic_name=str(topic_name or "").strip(),
        workspace_session_ref=workspace_session_ref,
        engine_session_id=normalized_session_id,
        lease_key=lease_key,
        lease_status=str(lease_status or "").strip(),
        approval_scope=default_approval_scope(engine_name=normalized_engine, entry_surface=normalized_surface),
        approval_state=str(approval_state or "").strip(),
        shared_memory_roots=[str(workspace_root()), str(vault_root())],
        isolated_runtime_root=str(runtime_root),
        session_runtime_root=str(runtime_root / "sessions" / normalized_session_id),
        projection_root=str(runtime_root / "projection"),
        scratch_root=str(runtime_root / "scratch"),
        metadata={
            "execution_profile": str(execution_profile or "").strip(),
            "source_chat_ref": str(source_chat_ref or "").strip(),
        },
    ).to_dict()


def claim_engine_session_lease(
    *,
    engine_name: str,
    entry_surface: str,
    project_name: str = "",
    binding_scope: str = "",
    binding_board_path: str = "",
    topic_name: str = "",
    launch_source: str = "",
    source_chat_ref: str = "",
    session_id: str = "",
    execution_profile: str = "",
    approval_state: str = "",
    state: str = "running",
) -> dict[str, Any]:
    contract = session_contract(
        engine_name=engine_name,
        entry_surface=entry_surface,
        project_name=project_name,
        binding_scope=binding_scope,
        binding_board_path=binding_board_path,
        topic_name=topic_name,
        launch_source=launch_source,
        source_chat_ref=source_chat_ref,
        session_id=session_id,
        execution_profile=execution_profile,
        approval_state=approval_state,
        lease_status=state,
    )
    existing = runtime_state.fetch_engine_session_lease(lease_key=contract["lease_key"])
    existing_engine = str(existing.get("engine_name") or "").strip()
    existing_session = str(existing.get("session_id") or "").strip()
    existing_state = str(existing.get("state") or "").strip()
    conflict = bool(
        existing_engine
        and existing_state
        and existing_state not in TERMINAL_LEASE_STATES
        and (existing_engine != contract["engine_name"] or existing_session != contract["engine_session_id"])
    )
    if conflict:
        return {
            "ok": False,
            "conflict": True,
            "lease": existing,
            "contract": contract,
            "existing": existing,
            "error": "engine_session_lease_conflict",
        }
    lease = runtime_state.upsert_engine_session_lease(
        lease_key=contract["lease_key"],
        project_name=str(project_name or "").strip(),
        binding_scope=str(contract.get("binding_scope") or "").strip(),
        binding_board_path=str(contract.get("binding_board_path") or "").strip(),
        topic_name=str(contract.get("topic_name") or "").strip(),
        engine_name=str(contract.get("engine_name") or "").strip(),
        entry_surface=str(contract.get("entry_surface") or "").strip(),
        launch_source=str(launch_source or "").strip(),
        session_id=str(contract.get("engine_session_id") or "").strip(),
        workspace_session_ref=str(contract.get("workspace_session_ref") or "").strip(),
        state=str(state or "").strip() or "running",
        approval_scope=str(contract.get("approval_scope") or "").strip(),
        approval_state=str(approval_state or "").strip(),
        runtime_root=str(contract.get("isolated_runtime_root") or "").strip(),
        session_runtime_root=str(contract.get("session_runtime_root") or "").strip(),
        metadata={
            "execution_profile": str(execution_profile or "").strip(),
            "source_chat_ref": str(source_chat_ref or "").strip(),
        },
    )
    return {
        "ok": True,
        "conflict": False,
        "lease": lease,
        "contract": contract,
        "existing": existing,
    }


def release_engine_session_lease(
    *,
    lease_key: str,
    state: str,
    session_id: str = "",
    summary: str = "",
) -> dict[str, Any]:
    existing = runtime_state.fetch_engine_session_lease(lease_key=lease_key)
    if not str(existing.get("lease_key") or "").strip():
        return {"ok": False, "error": "lease_not_found", "lease": existing}
    metadata = dict(existing.get("metadata") or {})
    if summary:
        metadata["summary_excerpt"] = str(summary).strip()[:400]
    lease = runtime_state.upsert_engine_session_lease(
        lease_key=lease_key,
        project_name=str(existing.get("project_name") or "").strip(),
        binding_scope=str(existing.get("binding_scope") or "").strip(),
        binding_board_path=str(existing.get("binding_board_path") or "").strip(),
        topic_name=str(existing.get("topic_name") or "").strip(),
        engine_name=str(existing.get("engine_name") or "").strip(),
        entry_surface=str(existing.get("entry_surface") or "").strip(),
        launch_source=str(existing.get("launch_source") or "").strip(),
        session_id=str(session_id or existing.get("session_id") or "").strip(),
        workspace_session_ref=str(existing.get("workspace_session_ref") or "").strip(),
        state=str(state or "").strip(),
        approval_scope=str(existing.get("approval_scope") or "").strip(),
        approval_state=str(existing.get("approval_state") or "").strip(),
        runtime_root=str(existing.get("runtime_root") or "").strip(),
        session_runtime_root=str(existing.get("session_runtime_root") or "").strip(),
        metadata=metadata,
    )
    return {"ok": True, "lease": lease}


def _print(payload: dict[str, Any]) -> int:
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_contract(args: argparse.Namespace) -> int:
    manifest = adapter_manifest(
        engine_name=args.engine_name,
        entry_surface=args.entry_surface,
        launch_source=args.launch_source,
    )
    contract = session_contract(
        engine_name=args.engine_name,
        entry_surface=args.entry_surface,
        project_name=args.project_name,
        binding_scope=args.binding_scope,
        binding_board_path=args.binding_board_path,
        topic_name=args.topic_name,
        launch_source=args.launch_source,
        source_chat_ref=args.source_chat_ref,
        session_id=args.session_id,
        execution_profile=args.execution_profile,
        approval_state=args.approval_state,
        lease_status=args.lease_status,
    )
    return _print({"ok": True, "manifest": manifest, "contract": contract})


def cmd_claim_lease(args: argparse.Namespace) -> int:
    payload = claim_engine_session_lease(
        engine_name=args.engine_name,
        entry_surface=args.entry_surface,
        project_name=args.project_name,
        binding_scope=args.binding_scope,
        binding_board_path=args.binding_board_path,
        topic_name=args.topic_name,
        launch_source=args.launch_source,
        source_chat_ref=args.source_chat_ref,
        session_id=args.session_id,
        execution_profile=args.execution_profile,
        approval_state=args.approval_state,
        state=args.state,
    )
    return _print(payload)


def cmd_release_lease(args: argparse.Namespace) -> int:
    payload = release_engine_session_lease(
        lease_key=args.lease_key,
        state=args.state,
        session_id=args.session_id,
        summary=args.summary,
    )
    return _print(payload)


def cmd_fetch_lease(args: argparse.Namespace) -> int:
    return _print({"ok": True, "lease": runtime_state.fetch_engine_session_lease(lease_key=args.lease_key)})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Workspace Hub engine adapter contracts")
    subparsers = parser.add_subparsers(dest="command", required=True)

    contract = subparsers.add_parser("contract")
    contract.add_argument("--engine-name", default="codex")
    contract.add_argument("--entry-surface", default="")
    contract.add_argument("--project-name", default="")
    contract.add_argument("--binding-scope", default="chat")
    contract.add_argument("--binding-board-path", default="")
    contract.add_argument("--topic-name", default="")
    contract.add_argument("--launch-source", default="")
    contract.add_argument("--source-chat-ref", default="")
    contract.add_argument("--session-id", default="")
    contract.add_argument("--execution-profile", default="")
    contract.add_argument("--approval-state", default="")
    contract.add_argument("--lease-status", default="")
    contract.set_defaults(func=cmd_contract)

    claim = subparsers.add_parser("claim-lease")
    claim.add_argument("--engine-name", default="codex")
    claim.add_argument("--entry-surface", default="")
    claim.add_argument("--project-name", default="")
    claim.add_argument("--binding-scope", default="chat")
    claim.add_argument("--binding-board-path", default="")
    claim.add_argument("--topic-name", default="")
    claim.add_argument("--launch-source", default="")
    claim.add_argument("--source-chat-ref", default="")
    claim.add_argument("--session-id", default="")
    claim.add_argument("--execution-profile", default="")
    claim.add_argument("--approval-state", default="")
    claim.add_argument("--state", default="running")
    claim.set_defaults(func=cmd_claim_lease)

    release = subparsers.add_parser("release-lease")
    release.add_argument("--lease-key", required=True)
    release.add_argument("--session-id", default="")
    release.add_argument("--state", default="completed")
    release.add_argument("--summary", default="")
    release.set_defaults(func=cmd_release_lease)

    fetch = subparsers.add_parser("fetch-lease")
    fetch.add_argument("--lease-key", required=True)
    fetch.set_defaults(func=cmd_fetch_lease)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

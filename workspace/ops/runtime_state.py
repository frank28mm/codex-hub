#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import hashlib
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
import datetime as dt
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import workspace_job_schema

try:
    from ops import workspace_hub_project
except ImportError:  # pragma: no cover
    import workspace_hub_project  # type: ignore


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1])))


def runtime_root() -> Path:
    explicit = os.environ.get("WORKSPACE_HUB_RUNTIME_ROOT", "").strip()
    if explicit:
        return Path(explicit)
    current_root = workspace_root()
    current_runtime = current_root / "runtime"
    worktrees_root = current_root.parent
    if worktrees_root.name == "workspace-hub-worktrees":
        canonical_runtime = workspace_hub_project.DEFAULT_WORKSPACE_ROOT / "runtime"
        if canonical_runtime.exists():
            return canonical_runtime
    return current_runtime


def runtime_state_dir() -> Path:
    return runtime_root() / "state"


def runtime_db_path() -> Path:
    return runtime_state_dir() / "workspace-hub.db"


RUNTIME_OWNERSHIP: dict[str, dict[str, Any]] = {
    "bridge_messages": {
        "owner": "bridge/broker",
        "feishu_mode": "writable",
        "purpose": "Inbound and outbound bridge message records.",
    },
    "delivery_status": {
        "owner": "bridge/broker",
        "feishu_mode": "writable",
        "purpose": "Delivery attempts, reply status, and broker handoff results.",
    },
    "approval_tokens": {
        "owner": "control_layer_reserved",
        "feishu_mode": "reserved",
        "purpose": "Approval and confirmation tokens owned by the shared control layer.",
    },
    "review_items": {
        "owner": "review_plane_rebuild",
        "feishu_mode": "read_only",
        "purpose": "Read model rebuilt from Vault review truth.",
    },
    "coordination_items": {
        "owner": "coordination_plane_rebuild",
        "feishu_mode": "read_only",
        "purpose": "Read model rebuilt from Vault coordination truth.",
    },
    "sidecar_receipts": {
        "owner": "v1_0_6_reserved",
        "feishu_mode": "reserved",
        "purpose": "Future sidecar receipt state reserved for v1.0.6 and later.",
    },
    "gflow_runs": {
        "owner": "gflow_runtime",
        "feishu_mode": "read_only",
        "purpose": "Persisted GFlow workflow runs, gates, and current stage state.",
    },
    "gflow_stage_results": {
        "owner": "gflow_runtime",
        "feishu_mode": "read_only",
        "purpose": "Persisted per-stage results and evidence handoff for GFlow runs.",
    },
    "bridge_execution_leases": {
        "owner": "bridge/broker",
        "feishu_mode": "writable",
        "purpose": "Per-conversation execution lease truth for bridge-level stale detection.",
    },
    "growth_action_attempts": {
        "owner": "growth_runtime",
        "feishu_mode": "read_only",
        "purpose": "Growth platform write attempts, idempotency, frequency, and failure tracking.",
    },
}

STALE_THREAD_ATTENTION_AFTER_SECONDS = 12 * 60 * 60
RESPONSE_DELAY_ATTENTION_AFTER_SECONDS = 2 * 60
PROGRESS_STALL_ATTENTION_AFTER_SECONDS = 3 * 60


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_timestamp(value: str) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def age_seconds(value: str) -> int | None:
    parsed = parse_iso_timestamp(value)
    if parsed is None:
        return None
    return max(0, int((dt.datetime.now(dt.timezone.utc) - parsed).total_seconds()))


class ManagedConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_value, traceback):  # type: ignore[override]
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect() -> sqlite3.Connection:
    path = runtime_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, factory=ManagedConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS bridge_messages (
        bridge TEXT NOT NULL,
        direction TEXT NOT NULL,
        message_id TEXT NOT NULL,
        project_name TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (bridge, direction, message_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS delivery_status (
        delivery_key TEXT PRIMARY KEY,
        bridge TEXT NOT NULL,
        channel TEXT NOT NULL DEFAULT '',
        target_ref TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS approval_tokens (
        token TEXT PRIMARY KEY,
        scope TEXT NOT NULL,
        status TEXT NOT NULL,
        project_name TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        expires_at TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS review_items (
        task_ref TEXT PRIMARY KEY,
        project_name TEXT NOT NULL,
        source_path TEXT NOT NULL,
        review_status TEXT NOT NULL,
        reviewer TEXT NOT NULL DEFAULT '',
        deliverable_ref TEXT NOT NULL DEFAULT '',
        decision_note TEXT NOT NULL DEFAULT '',
        decided_at TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS coordination_items (
        coordination_id TEXT PRIMARY KEY,
        from_project TEXT NOT NULL,
        to_project TEXT NOT NULL,
        status TEXT NOT NULL,
        assignee TEXT NOT NULL DEFAULT '',
        due_at TEXT NOT NULL DEFAULT '',
        receipt_ref TEXT NOT NULL DEFAULT '',
        source_ref TEXT NOT NULL DEFAULT '',
        requested_action TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sidecar_receipts (
        task_id TEXT PRIMARY KEY,
        companion TEXT NOT NULL,
        status TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '',
        artifacts_json TEXT NOT NULL DEFAULT '[]',
        started_at TEXT NOT NULL DEFAULT '',
        finished_at TEXT NOT NULL DEFAULT '',
        audit_ref TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_settings (
        bridge TEXT PRIMARY KEY,
        settings_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_connections (
        bridge TEXT PRIMARY KEY,
        host_mode TEXT NOT NULL DEFAULT '',
        transport TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'disconnected',
        last_error TEXT NOT NULL DEFAULT '',
        last_event_at TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_chat_bindings (
        bridge TEXT NOT NULL,
        chat_ref TEXT NOT NULL,
        binding_scope TEXT NOT NULL DEFAULT 'project',
        project_name TEXT NOT NULL DEFAULT '',
        topic_name TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (bridge, chat_ref)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_execution_leases (
        bridge TEXT NOT NULL,
        conversation_key TEXT NOT NULL,
        session_id TEXT NOT NULL DEFAULT '',
        project_name TEXT NOT NULL DEFAULT '',
        topic_name TEXT NOT NULL DEFAULT '',
        state TEXT NOT NULL DEFAULT '',
        started_at TEXT NOT NULL DEFAULT '',
        last_progress_at TEXT NOT NULL DEFAULT '',
        completed_at TEXT NOT NULL DEFAULT '',
        stale_after_seconds INTEGER NOT NULL DEFAULT 0,
        last_delivery_phase TEXT NOT NULL DEFAULT '',
        last_error TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (bridge, conversation_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runtime_events (
        event_key TEXT PRIMARY KEY,
        queue_name TEXT NOT NULL,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        result_json TEXT NOT NULL DEFAULT '{}',
        available_at TEXT NOT NULL,
        claimed_by TEXT NOT NULL DEFAULT '',
        claim_token TEXT NOT NULL DEFAULT '',
        leased_at TEXT NOT NULL DEFAULT '',
        lease_expires_at TEXT NOT NULL DEFAULT '',
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gflow_runs (
        run_id TEXT PRIMARY KEY,
        project_name TEXT NOT NULL DEFAULT '',
        session_id TEXT NOT NULL DEFAULT '',
        invocation_mode TEXT NOT NULL,
        status TEXT NOT NULL,
        current_stage_id TEXT NOT NULL DEFAULT '',
        current_stage_skill TEXT NOT NULL DEFAULT '',
        gate_type TEXT NOT NULL DEFAULT '',
        gate_reason TEXT NOT NULL DEFAULT '',
        gate_token TEXT NOT NULL DEFAULT '',
        freeze_scope TEXT NOT NULL DEFAULT '',
        latest_summary TEXT NOT NULL DEFAULT '',
        latest_next_action TEXT NOT NULL DEFAULT '',
        workflow_plan_json TEXT NOT NULL DEFAULT '{}',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS gflow_stage_results (
        run_id TEXT NOT NULL,
        stage_id TEXT NOT NULL,
        skill TEXT NOT NULL,
        position INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL,
        summary TEXT NOT NULL DEFAULT '',
        next_action TEXT NOT NULL DEFAULT '',
        evidence_json TEXT NOT NULL DEFAULT '[]',
        handoff_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (run_id, stage_id),
        FOREIGN KEY (run_id) REFERENCES gflow_runs(run_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS growth_action_attempts (
        idempotency_key TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        command TEXT NOT NULL,
        action_status TEXT NOT NULL,
        payload_fingerprint TEXT NOT NULL DEFAULT '',
        risk_level TEXT NOT NULL DEFAULT '',
        error TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
]


def init_db() -> dict[str, Any]:
    with connect() as conn:
        for statement in SCHEMA:
            conn.execute(statement)
        conn.commit()
        counts = {
            "bridge_messages": scalar(conn, "SELECT COUNT(*) FROM bridge_messages"),
            "delivery_status": scalar(conn, "SELECT COUNT(*) FROM delivery_status"),
            "approval_tokens": scalar(conn, "SELECT COUNT(*) FROM approval_tokens"),
            "review_items": scalar(conn, "SELECT COUNT(*) FROM review_items"),
            "coordination_items": scalar(conn, "SELECT COUNT(*) FROM coordination_items"),
            "sidecar_receipts": scalar(conn, "SELECT COUNT(*) FROM sidecar_receipts"),
            "gflow_runs": scalar(conn, "SELECT COUNT(*) FROM gflow_runs"),
            "gflow_stage_results": scalar(conn, "SELECT COUNT(*) FROM gflow_stage_results"),
            "bridge_settings": scalar(conn, "SELECT COUNT(*) FROM bridge_settings"),
            "bridge_connections": scalar(conn, "SELECT COUNT(*) FROM bridge_connections"),
            "bridge_chat_bindings": scalar(conn, "SELECT COUNT(*) FROM bridge_chat_bindings"),
            "runtime_events": scalar(conn, "SELECT COUNT(*) FROM runtime_events"),
            "growth_action_attempts": scalar(conn, "SELECT COUNT(*) FROM growth_action_attempts"),
        }
    return {"db_path": str(runtime_db_path()), "initialized": True, "counts": counts}


def scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _queue_event_key(queue_name: str, event_type: str, dedupe_key: str, payload: dict[str, Any]) -> str:
    identity = {
        "queue_name": str(queue_name or "").strip(),
        "event_type": str(event_type or "").strip(),
        "dedupe_key": str(dedupe_key or "").strip(),
        "payload": payload if not dedupe_key else {},
    }
    return hashlib.sha1(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _decode_runtime_event(row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any]:
    payload = row_to_dict(row) if isinstance(row, sqlite3.Row) or row is None else dict(row)
    if not payload:
        return {
            "event_key": "",
            "queue_name": "",
            "event_type": "",
            "status": "",
            "payload": {},
            "result": {},
            "available_at": "",
            "claimed_by": "",
            "claim_token": "",
            "leased_at": "",
            "lease_expires_at": "",
            "attempt_count": 0,
            "last_error": "",
            "created_at": "",
            "updated_at": "",
        }
    return {
        "event_key": payload.get("event_key", ""),
        "queue_name": payload.get("queue_name", ""),
        "event_type": payload.get("event_type", ""),
        "status": payload.get("status", ""),
        "payload": json.loads(payload.get("payload_json") or "{}"),
        "result": json.loads(payload.get("result_json") or "{}"),
        "available_at": payload.get("available_at", ""),
        "claimed_by": payload.get("claimed_by", ""),
        "claim_token": payload.get("claim_token", ""),
        "leased_at": payload.get("leased_at", ""),
        "lease_expires_at": payload.get("lease_expires_at", ""),
        "attempt_count": int(payload.get("attempt_count", 0) or 0),
        "last_error": payload.get("last_error", ""),
        "created_at": payload.get("created_at", ""),
        "updated_at": payload.get("updated_at", ""),
    }


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    conn = connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def json_text(value: Any) -> str:
    return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True)


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(_json_safe(key)): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _payload_fingerprint(value: Any) -> str:
    return hashlib.sha1(json_text(value).encode("utf-8")).hexdigest()


def record_growth_action_attempt(
    *,
    idempotency_key: str,
    platform: str,
    command: str,
    action_status: str,
    payload: dict[str, Any] | None = None,
    risk_level: str = "",
    error: str = "",
) -> dict[str, Any]:
    init_db()
    normalized_key = str(idempotency_key or "").strip()
    if not normalized_key:
        raise ValueError("idempotency_key is required")
    now = iso_now()
    fingerprint = _payload_fingerprint(payload or {})
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO growth_action_attempts (
                idempotency_key, platform, command, action_status, payload_fingerprint, risk_level, error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                action_status = excluded.action_status,
                payload_fingerprint = excluded.payload_fingerprint,
                risk_level = excluded.risk_level,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            (
                normalized_key,
                str(platform or "").strip(),
                str(command or "").strip(),
                str(action_status or "").strip(),
                fingerprint,
                str(risk_level or "").strip(),
                str(error or "").strip(),
                now,
                now,
            ),
        )
    return fetch_growth_action_attempt(normalized_key)


def fetch_growth_action_attempt(idempotency_key: str) -> dict[str, Any]:
    init_db()
    normalized_key = str(idempotency_key or "").strip()
    if not normalized_key:
        return {
            "idempotency_key": "",
            "platform": "",
            "command": "",
            "action_status": "",
            "payload_fingerprint": "",
            "risk_level": "",
            "error": "",
            "created_at": "",
            "updated_at": "",
        }
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM growth_action_attempts WHERE idempotency_key = ?",
            (normalized_key,),
        ).fetchone()
    payload = row_to_dict(row)
    return payload if payload else {
        "idempotency_key": normalized_key,
        "platform": "",
        "command": "",
        "action_status": "",
        "payload_fingerprint": "",
        "risk_level": "",
        "error": "",
        "created_at": "",
        "updated_at": "",
    }


def growth_action_recent_count(*, platform: str, since_seconds: int = 3600) -> int:
    init_db()
    since = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=max(0, int(since_seconds or 0)))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with connect() as conn:
        return scalar(
            conn,
            "SELECT COUNT(*) FROM growth_action_attempts WHERE platform = ? AND updated_at >= ?",
            (str(platform or "").strip(), since),
        )


def growth_action_consecutive_failures(*, platform: str, command: str, limit: int = 10) -> int:
    init_db()
    query_limit = max(1, min(int(limit or 10), 50))
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT action_status
            FROM growth_action_attempts
            WHERE platform = ? AND command = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (str(platform or "").strip(), str(command or "").strip(), query_limit),
        ).fetchall()
    count = 0
    for row in rows:
        if str(row["action_status"] or "").strip() == "failed":
            count += 1
            continue
        break
    return count


def upsert_bridge_message(
    *,
    bridge: str,
    direction: str,
    message_id: str,
    status: str,
    payload: dict[str, Any],
    project_name: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    init_db()
    now = iso_now()
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO bridge_messages (
                bridge, direction, message_id, project_name, session_id,
                status, payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bridge, direction, message_id) DO UPDATE SET
                project_name=excluded.project_name,
                session_id=excluded.session_id,
                status=excluded.status,
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at
            """,
            (
                bridge,
                direction,
                message_id,
                project_name,
                session_id,
                status,
                json_text(payload),
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM bridge_messages WHERE bridge = ? AND direction = ? AND message_id = ?",
            (bridge, direction, message_id),
        ).fetchone()
    record = row_to_dict(row)
    enqueue_runtime_event(
        queue_name="bridge_message_log",
        event_type="bridge_message",
        payload={
            "bridge": bridge,
            "direction": direction,
            "message_id": message_id,
            "project_name": project_name,
            "session_id": session_id,
            "status": status,
            "record": {
                "created_at": record.get("created_at", ""),
                "updated_at": record.get("updated_at", ""),
                "payload": payload,
            },
        },
        dedupe_key=f"{bridge}:{direction}:{message_id}:{status}",
        status="completed",
    )
    return record


def upsert_delivery_status(
    *,
    delivery_key: str,
    bridge: str,
    status: str,
    channel: str = "",
    target_ref: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = iso_now()
    payload = payload or {}
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO delivery_status (
                delivery_key, bridge, channel, target_ref, status,
                payload_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(delivery_key) DO UPDATE SET
                bridge=excluded.bridge,
                channel=excluded.channel,
                target_ref=excluded.target_ref,
                status=excluded.status,
                payload_json=excluded.payload_json,
                updated_at=excluded.updated_at
            """,
            (
                delivery_key,
                bridge,
                channel,
                target_ref,
                status,
                json_text(payload),
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM delivery_status WHERE delivery_key = ?",
            (delivery_key,),
        ).fetchone()
    record = row_to_dict(row)
    enqueue_runtime_event(
        queue_name="delivery_status_log",
        event_type="delivery_status",
        payload={
            "delivery_key": delivery_key,
            "bridge": bridge,
            "channel": channel,
            "target_ref": target_ref,
            "status": status,
            "record": {
                "created_at": record.get("created_at", ""),
                "updated_at": record.get("updated_at", ""),
                "payload": payload,
            },
        },
        dedupe_key=f"{delivery_key}:{status}",
        status="completed",
    )
    return record


def enqueue_runtime_event(
    *,
    queue_name: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    dedupe_key: str = "",
    event_key: str = "",
    status: str = "pending",
    available_at: str = "",
    result: dict[str, Any] | None = None,
    claimed_by: str = "",
    claim_token: str = "",
    leased_at: str = "",
    lease_expires_at: str = "",
    attempt_count: int = 0,
    last_error: str = "",
) -> dict[str, Any]:
    init_db()
    normalized_queue = str(queue_name or "").strip()
    normalized_type = str(event_type or "").strip()
    if not normalized_queue:
        raise ValueError("queue_name is required")
    if not normalized_type:
        raise ValueError("event_type is required")
    payload = payload or {}
    result = result or {}
    now = iso_now()
    normalized_key = str(event_key or "").strip() or _queue_event_key(normalized_queue, normalized_type, dedupe_key, payload)
    normalized_status = str(status or "pending").strip() or "pending"
    normalized_available_at = str(available_at or "").strip() or now
    with transaction() as conn:
        existing = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO runtime_events (
                    event_key, queue_name, event_type, status, payload_json, result_json,
                    available_at, claimed_by, claim_token, leased_at, lease_expires_at,
                    attempt_count, last_error, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_key,
                    normalized_queue,
                    normalized_type,
                    normalized_status,
                    json_text(payload),
                    json_text(result),
                    normalized_available_at,
                    claimed_by,
                    claim_token,
                    leased_at,
                    lease_expires_at,
                    max(0, int(attempt_count or 0)),
                    str(last_error or ""),
                    now,
                    now,
                ),
            )
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
    return _decode_runtime_event(row)


def fetch_runtime_event(event_key: str) -> dict[str, Any]:
    init_db()
    normalized_key = str(event_key or "").strip()
    if not normalized_key:
        return _decode_runtime_event(None)
    with connect() as conn:
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
    return _decode_runtime_event(row)


def fetch_runtime_events(
    *,
    queue_name: str = "",
    statuses: list[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    init_db()
    query_limit = max(1, min(int(limit or 100), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if queue_name:
        clauses.append("queue_name = ?")
        params.append(str(queue_name))
    normalized_statuses = [str(item).strip() for item in (statuses or []) if str(item).strip()]
    if normalized_statuses:
        clauses.append("status IN ({})".format(", ".join("?" for _ in normalized_statuses)))
        params.extend(normalized_statuses)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM runtime_events
            {where_sql}
            ORDER BY created_at DESC, event_key ASC
            LIMIT ?
            """,
            (*params, query_limit),
        ).fetchall()
    return [_decode_runtime_event(row) for row in rows]


def fetch_runtime_queue_status(*, queue_name: str = "") -> dict[str, Any]:
    init_db()
    clauses: list[str] = []
    params: list[Any] = []
    if queue_name:
        clauses.append("queue_name = ?")
        params.append(str(queue_name))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    now = iso_now()
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT queue_name, status, COUNT(*) AS count
            FROM runtime_events
            {where_sql}
            GROUP BY queue_name, status
            ORDER BY queue_name ASC, status ASC
            """,
            tuple(params),
        ).fetchall()
        stale_processing = conn.execute(
            f"""
            SELECT COUNT(*) FROM runtime_events
            {where_sql}{' AND ' if where_sql else 'WHERE '}status = 'processing' AND lease_expires_at != '' AND lease_expires_at < ?
            """,
            (*params, now),
        ).fetchone()
        latest_created_at_row = conn.execute(
            f"""
            SELECT MAX(created_at) FROM runtime_events
            {where_sql}
            """,
            tuple(params),
        ).fetchone()
    counts: dict[str, dict[str, int]] = {}
    for row in rows_to_dicts(rows):
        queue = str(row.get("queue_name") or "").strip() or "default"
        status_name = str(row.get("status") or "").strip() or "unknown"
        counts.setdefault(queue, {})
        counts[queue][status_name] = int(row.get("count", 0) or 0)
    aggregate = {
        "pending": 0,
        "processing": 0,
        "failed": 0,
        "completed": 0,
    }
    for queue_counts in counts.values():
        for status_name, value in queue_counts.items():
            aggregate[status_name] = aggregate.get(status_name, 0) + int(value or 0)
    aggregate["stale_processing"] = int(stale_processing[0] if stale_processing else 0)
    return {
        "queue_name": queue_name,
        "counts": counts,
        "aggregate": aggregate,
        "latest_created_at": latest_created_at_row[0] if latest_created_at_row else "",
    }


def claim_runtime_events(
    *,
    queue_name: str,
    claimed_by: str,
    limit: int = 20,
    lease_seconds: int = 300,
    event_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    init_db()
    normalized_queue = str(queue_name or "").strip()
    normalized_consumer = str(claimed_by or "").strip()
    if not normalized_queue:
        raise ValueError("queue_name is required")
    if not normalized_consumer:
        raise ValueError("claimed_by is required")
    query_limit = max(1, min(int(limit or 20), 500))
    now = iso_now()
    lease_until = (
        dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=max(30, int(lease_seconds or 300)))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    normalized_types = [str(item).strip() for item in (event_types or []) if str(item).strip()]
    with transaction() as conn:
        type_clause = ""
        type_params: list[Any] = []
        if normalized_types:
            type_clause = " AND event_type IN ({})".format(", ".join("?" for _ in normalized_types))
            type_params.extend(normalized_types)
        conn.execute(
            f"""
            UPDATE runtime_events
            SET status = 'pending',
                claimed_by = '',
                claim_token = '',
                leased_at = '',
                lease_expires_at = '',
                updated_at = ?
            WHERE queue_name = ?
              AND status = 'processing'
              AND lease_expires_at != ''
              AND lease_expires_at < ?
              {type_clause}
            """,
            (now, normalized_queue, now, *type_params),
        )
        candidate_rows = conn.execute(
            f"""
            SELECT event_key FROM runtime_events
            WHERE queue_name = ?
              AND status IN ('pending', 'failed')
              AND available_at <= ?
              {type_clause}
            ORDER BY available_at ASC, created_at ASC, event_key ASC
            LIMIT ?
            """,
            (normalized_queue, now, *type_params, query_limit),
        ).fetchall()
        claimed_keys: list[str] = []
        for row in candidate_rows:
            event_key = str(row["event_key"]).strip()
            token = str(uuid.uuid4())
            conn.execute(
                """
                UPDATE runtime_events
                SET status = 'processing',
                    claimed_by = ?,
                    claim_token = ?,
                    leased_at = ?,
                    lease_expires_at = ?,
                    attempt_count = attempt_count + 1,
                    updated_at = ?
                WHERE event_key = ?
                """,
                (normalized_consumer, token, now, lease_until, now, event_key),
            )
            claimed_keys.append(event_key)
        if not claimed_keys:
            return []
        rows = conn.execute(
            "SELECT * FROM runtime_events WHERE event_key IN ({}) ORDER BY created_at ASC, event_key ASC".format(
                ", ".join("?" for _ in claimed_keys)
            ),
            tuple(claimed_keys),
        ).fetchall()
    return [_decode_runtime_event(row) for row in rows]


def complete_runtime_event(
    event_key: str,
    *,
    claim_token: str = "",
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    normalized_key = str(event_key or "").strip()
    if not normalized_key:
        return _decode_runtime_event(None)
    result = result or {}
    now = iso_now()
    with transaction() as conn:
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
        if row is None:
            return _decode_runtime_event(None)
        if claim_token and str(row["claim_token"] or "").strip() != claim_token:
            return _decode_runtime_event(row)
        conn.execute(
            """
            UPDATE runtime_events
            SET status = 'completed',
                result_json = ?,
                claimed_by = '',
                claim_token = '',
                leased_at = '',
                lease_expires_at = '',
                last_error = '',
                updated_at = ?
            WHERE event_key = ?
            """,
            (json_text(result), now, normalized_key),
        )
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
    return _decode_runtime_event(row)


def renew_runtime_event_lease(
    event_key: str,
    *,
    claim_token: str = "",
    lease_seconds: int = 300,
) -> dict[str, Any]:
    init_db()
    normalized_key = str(event_key or "").strip()
    if not normalized_key:
        return _decode_runtime_event(None)
    now_dt = dt.datetime.now(dt.timezone.utc)
    now = now_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    lease_until = (
        now_dt + dt.timedelta(seconds=max(30, int(lease_seconds or 300)))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with transaction() as conn:
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
        if row is None:
            return _decode_runtime_event(None)
        if claim_token and str(row["claim_token"] or "").strip() != claim_token:
            return _decode_runtime_event(row)
        if str(row["status"] or "").strip() != "processing":
            return _decode_runtime_event(row)
        conn.execute(
            """
            UPDATE runtime_events
            SET leased_at = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE event_key = ?
            """,
            (now, lease_until, now, normalized_key),
        )
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
    return _decode_runtime_event(row)


def fail_runtime_event(
    event_key: str,
    *,
    claim_token: str = "",
    error: str = "",
    retry_after_seconds: int = 0,
    final: bool = False,
) -> dict[str, Any]:
    init_db()
    normalized_key = str(event_key or "").strip()
    if not normalized_key:
        return _decode_runtime_event(None)
    now_dt = dt.datetime.now(dt.timezone.utc)
    now = now_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    available_at = (
        now_dt + dt.timedelta(seconds=max(0, int(retry_after_seconds or 0)))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    next_status = "failed" if final else "pending"
    with transaction() as conn:
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
        if row is None:
            return _decode_runtime_event(None)
        if claim_token and str(row["claim_token"] or "").strip() != claim_token:
            return _decode_runtime_event(row)
        conn.execute(
            """
            UPDATE runtime_events
            SET status = ?,
                available_at = ?,
                claimed_by = '',
                claim_token = '',
                leased_at = '',
                lease_expires_at = '',
                last_error = ?,
                updated_at = ?
            WHERE event_key = ?
            """,
            (next_status, available_at, str(error or "").strip(), now, normalized_key),
        )
        row = conn.execute("SELECT * FROM runtime_events WHERE event_key = ?", (normalized_key,)).fetchone()
    return _decode_runtime_event(row)


def fetch_approval_token(token: str) -> dict[str, Any]:
    init_db()
    token = str(token or "").strip()
    if not token:
        return {
            "token": "",
            "scope": "",
            "status": "",
            "project_name": "",
            "session_id": "",
            "expires_at": "",
            "metadata": {},
            "created_at": "",
            "updated_at": "",
        }
    with connect() as conn:
        row = conn.execute("SELECT * FROM approval_tokens WHERE token = ?", (token,)).fetchone()
    payload = row_to_dict(row)
    if not payload:
        return {
            "token": token,
            "scope": "",
            "status": "",
            "project_name": "",
            "session_id": "",
            "expires_at": "",
            "metadata": {},
            "created_at": "",
            "updated_at": "",
        }
    return {
        "token": payload["token"],
        "scope": payload["scope"],
        "status": payload["status"],
        "project_name": payload["project_name"],
        "session_id": payload["session_id"],
        "expires_at": payload["expires_at"],
        "metadata": json.loads(payload["metadata_json"]),
        "created_at": payload["created_at"],
        "updated_at": payload["updated_at"],
    }


def fetch_approval_tokens(*, status: str = "", scope: str = "", limit: int = 100) -> list[dict[str, Any]]:
    init_db()
    query_limit = max(1, min(int(limit or 100), 500))
    clauses = []
    params: list[Any] = []
    if status:
        clauses.append("status = ?")
        params.append(str(status))
    if scope:
        clauses.append("scope = ?")
        params.append(str(scope))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM approval_tokens
            {where_sql}
            ORDER BY updated_at DESC, created_at DESC, token ASC
            LIMIT ?
            """,
            (*params, query_limit),
        ).fetchall()
    payloads = []
    for row in rows_to_dicts(rows):
        payloads.append(
            {
                "token": row["token"],
                "scope": row["scope"],
                "status": row["status"],
                "project_name": row["project_name"],
                "session_id": row["session_id"],
                "expires_at": row["expires_at"],
                "metadata": json.loads(row["metadata_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return payloads


def approval_token_is_pending(item: dict[str, Any]) -> bool:
    if str(item.get("status") or "").strip() != "pending":
        return False
    expires_at = parse_iso_timestamp(str(item.get("expires_at") or "").strip())
    if expires_at is None:
        return True
    return expires_at > dt.datetime.now(dt.timezone.utc)


def upsert_approval_token(
    *,
    token: str,
    scope: str,
    status: str,
    project_name: str = "",
    session_id: str = "",
    expires_at: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    now = iso_now()
    metadata = metadata or {}
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO approval_tokens (
                token, scope, status, project_name, session_id,
                expires_at, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(token) DO UPDATE SET
                scope=excluded.scope,
                status=excluded.status,
                project_name=excluded.project_name,
                session_id=excluded.session_id,
                expires_at=excluded.expires_at,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (
                token,
                scope,
                status,
                project_name,
                session_id,
                expires_at,
                json_text(metadata),
                now,
                now,
            ),
        )
    item = fetch_approval_token(token)
    enqueue_runtime_event(
        queue_name="approval_token_log",
        event_type="approval_token",
        payload=item,
        dedupe_key=f"{token}:{status}:{expires_at}",
        status="completed",
    )
    return item


def fetch_bridge_messages(
    *,
    bridge: str = "feishu",
    chat_ref: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    init_db()
    query_limit = max(1, min(int(limit or 100), 500))
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM bridge_messages
            WHERE bridge = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?
            """,
            (bridge, query_limit),
        ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows_to_dicts(rows):
        payload = json.loads(row.get("payload_json") or "{}")
        record = {
            "bridge": row["bridge"],
            "direction": row["direction"],
            "message_id": row["message_id"],
            "project_name": row.get("project_name", ""),
            "session_id": row.get("session_id", ""),
            "status": row.get("status", ""),
            "payload": payload,
            "created_at": row.get("created_at", ""),
            "updated_at": row.get("updated_at", ""),
        }
        derived_chat_ref = str(
            payload.get("chat_id")
            or payload.get("chat_ref")
            or payload.get("open_id")
            or payload.get("user_id")
            or payload.get("reply_target")
            or ""
        ).strip()
        if chat_ref and derived_chat_ref != chat_ref:
            continue
        record["chat_ref"] = derived_chat_ref
        items.append(record)
    return items


def fetch_bridge_message_activity(bridge: str = "feishu") -> dict[str, Any]:
    init_db()
    with connect() as conn:
        inbound = conn.execute(
            """
            SELECT message_id, status, payload_json, created_at, updated_at
            FROM bridge_messages
            WHERE bridge = ? AND direction = 'inbound'
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (bridge,),
        ).fetchone()
        outbound = conn.execute(
            """
            SELECT message_id, status, payload_json, created_at, updated_at
            FROM bridge_messages
            WHERE bridge = ? AND direction = 'outbound'
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            (bridge,),
        ).fetchone()

    def normalize(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {
                "message_id": "",
                "status": "",
                "created_at": "",
                "updated_at": "",
                "activity_at": "",
                "cursor_at": "",
                "cursor_kind": "",
                "text": "",
                "sender_ref": "",
                "phase": "",
            }
        payload = json.loads(row["payload_json"] or "{}")
        created_at = str(row["created_at"] or "").strip()
        updated_at = str(row["updated_at"] or "").strip()
        cursor_at = str(payload.get("event_observed_at") or payload.get("event_created_at") or updated_at or created_at).strip()
        return {
            "message_id": str(row["message_id"] or "").strip(),
            "status": str(row["status"] or "").strip(),
            "created_at": created_at,
            "updated_at": updated_at,
            "activity_at": updated_at or created_at,
            "cursor_at": cursor_at,
            "cursor_kind": str(payload.get("cursor_kind") or "").strip(),
            "text": str(payload.get("text") or "").strip(),
            "sender_ref": str(
                payload.get("open_id")
                or payload.get("user_id")
                or payload.get("sender_ref")
                or ""
            ).strip(),
            "phase": str(payload.get("phase") or "").strip(),
        }

    return {
        "bridge": bridge,
        "inbound": normalize(inbound),
        "outbound": normalize(outbound),
    }


def fetch_bridge_message_detail(
    *,
    bridge: str = "feishu",
    message_id: str,
    direction: str = "",
) -> dict[str, Any]:
    init_db()
    normalized_message_id = str(message_id or "").strip()
    normalized_direction = str(direction or "").strip()
    if not normalized_message_id:
        return {
            "bridge": bridge,
            "direction": normalized_direction,
            "message_id": "",
            "chat_ref": "",
            "project_name": "",
            "session_id": "",
            "status": "",
            "payload": {},
            "created_at": "",
            "updated_at": "",
        }
    clauses = ["bridge = ?", "message_id = ?"]
    params: list[Any] = [bridge, normalized_message_id]
    if normalized_direction:
        clauses.append("direction = ?")
        params.append(normalized_direction)
    with connect() as conn:
        row = conn.execute(
            f"""
            SELECT * FROM bridge_messages
            WHERE {' AND '.join(clauses)}
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    record = row_to_dict(row)
    if not record:
        return {
            "bridge": bridge,
            "direction": normalized_direction,
            "message_id": normalized_message_id,
            "chat_ref": "",
            "project_name": "",
            "session_id": "",
            "status": "",
            "payload": {},
            "created_at": "",
            "updated_at": "",
        }
    payload = json.loads(record.get("payload_json") or "{}")
    chat_ref = str(
        payload.get("chat_id")
        or payload.get("chat_ref")
        or payload.get("open_id")
        or payload.get("user_id")
        or payload.get("reply_target")
        or ""
    ).strip()
    return {
        "bridge": record.get("bridge", ""),
        "direction": record.get("direction", ""),
        "message_id": record.get("message_id", ""),
        "chat_ref": chat_ref,
        "project_name": record.get("project_name", ""),
        "session_id": record.get("session_id", ""),
        "status": record.get("status", ""),
        "payload": payload,
        "created_at": record.get("created_at", ""),
        "updated_at": record.get("updated_at", ""),
    }


def bridge_retrieval_protocol(*, bridge: str = "feishu", chat_ref: str = "", limit: int = 50) -> dict[str, Any]:
    conversations = fetch_bridge_conversations(bridge=bridge, limit=limit)
    if chat_ref:
        messages = fetch_bridge_messages(bridge=bridge, chat_ref=chat_ref, limit=max(20, limit))
        detail_message_ids = [
            str(item.get("message_id", "")).strip()
            for item in messages
            if str(item.get("message_id", "")).strip()
        ]
        return {
            "name": "search-timeline-detail",
            "steps": ["search", "timeline", "detail"],
            "next_step": "detail" if detail_message_ids else "search",
            "search_candidate_count": len(conversations),
            "timeline_candidate_count": len(messages),
            "detail_candidate_count": len(detail_message_ids),
            "timeline_refs": [chat_ref],
            "detail_refs": detail_message_ids[: max(1, min(limit, 20))],
        }
    return {
        "name": "search-timeline-detail",
        "steps": ["search", "timeline", "detail"],
        "next_step": "timeline" if conversations else "search",
        "search_candidate_count": len(conversations),
        "timeline_candidate_count": len(conversations),
        "detail_candidate_count": 0,
        "timeline_refs": [
            str(item.get("chat_ref", "")).strip()
            for item in conversations
            if str(item.get("chat_ref", "")).strip()
        ][: max(1, min(limit, 20))],
        "detail_refs": [],
    }


def fetch_bridge_execution_lease(*, bridge: str = "feishu", conversation_key: str) -> dict[str, Any]:
    init_db()
    normalized_key = str(conversation_key or "").strip()
    if not normalized_key:
        return {
            "bridge": bridge,
            "conversation_key": "",
            "session_id": "",
            "project_name": "",
            "topic_name": "",
            "state": "",
            "started_at": "",
            "last_progress_at": "",
            "completed_at": "",
            "stale_after_seconds": 0,
            "last_delivery_phase": "",
            "last_error": "",
            "metadata": {},
            "created_at": "",
            "updated_at": "",
        }
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM bridge_execution_leases WHERE bridge = ? AND conversation_key = ?",
            (bridge, normalized_key),
        ).fetchone()
    payload = row_to_dict(row)
    if not payload:
        return {
            "bridge": bridge,
            "conversation_key": normalized_key,
            "session_id": "",
            "project_name": "",
            "topic_name": "",
            "state": "",
            "started_at": "",
            "last_progress_at": "",
            "completed_at": "",
            "stale_after_seconds": 0,
            "last_delivery_phase": "",
            "last_error": "",
            "metadata": {},
            "created_at": "",
            "updated_at": "",
        }
    return {
        "bridge": payload["bridge"],
        "conversation_key": payload["conversation_key"],
        "session_id": payload["session_id"],
        "project_name": payload["project_name"],
        "topic_name": payload["topic_name"],
        "state": payload["state"],
        "started_at": payload["started_at"],
        "last_progress_at": payload["last_progress_at"],
        "completed_at": payload["completed_at"],
        "stale_after_seconds": int(payload.get("stale_after_seconds") or 0),
        "last_delivery_phase": payload["last_delivery_phase"],
        "last_error": payload["last_error"],
        "metadata": json.loads(payload["metadata_json"]),
        "created_at": payload["created_at"],
        "updated_at": payload["updated_at"],
    }


def fetch_bridge_execution_leases(*, bridge: str = "feishu", limit: int = 100) -> list[dict[str, Any]]:
    init_db()
    query_limit = max(1, min(int(limit or 100), 500))
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM bridge_execution_leases
            WHERE bridge = ?
            ORDER BY updated_at DESC, conversation_key ASC
            LIMIT ?
            """,
            (bridge, query_limit),
        ).fetchall()
    payloads = []
    for row in rows_to_dicts(rows):
        payloads.append(
            {
                "bridge": row["bridge"],
                "conversation_key": row["conversation_key"],
                "session_id": row["session_id"],
                "project_name": row["project_name"],
                "topic_name": row["topic_name"],
                "state": row["state"],
                "started_at": row["started_at"],
                "last_progress_at": row["last_progress_at"],
                "completed_at": row["completed_at"],
                "stale_after_seconds": int(row.get("stale_after_seconds") or 0),
                "last_delivery_phase": row["last_delivery_phase"],
                "last_error": row["last_error"],
                "metadata": json.loads(row["metadata_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return payloads


def upsert_bridge_execution_lease(
    *,
    bridge: str,
    conversation_key: str,
    state: str,
    session_id: str = "",
    project_name: str = "",
    topic_name: str = "",
    started_at: str = "",
    last_progress_at: str = "",
    completed_at: str = "",
    stale_after_seconds: int = 0,
    last_delivery_phase: str = "",
    last_error: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    normalized_key = str(conversation_key or "").strip()
    if not normalized_key:
        raise ValueError("conversation_key is required")
    metadata = metadata or {}
    now = iso_now()
    existing = fetch_bridge_execution_lease(bridge=bridge, conversation_key=normalized_key)
    normalized_started_at = str(started_at or "").strip() or str(existing.get("started_at") or "").strip() or now
    normalized_progress_at = str(last_progress_at or "").strip() or now
    normalized_completed_at = str(completed_at or "").strip()
    if str(state or "").strip() in {"reported", "failed", "completed"} and not normalized_completed_at:
        normalized_completed_at = now
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO bridge_execution_leases (
                bridge, conversation_key, session_id, project_name, topic_name, state,
                started_at, last_progress_at, completed_at, stale_after_seconds,
                last_delivery_phase, last_error, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bridge, conversation_key) DO UPDATE SET
                session_id=excluded.session_id,
                project_name=excluded.project_name,
                topic_name=excluded.topic_name,
                state=excluded.state,
                started_at=excluded.started_at,
                last_progress_at=excluded.last_progress_at,
                completed_at=excluded.completed_at,
                stale_after_seconds=excluded.stale_after_seconds,
                last_delivery_phase=excluded.last_delivery_phase,
                last_error=excluded.last_error,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (
                bridge,
                normalized_key,
                session_id,
                project_name,
                topic_name,
                state,
                normalized_started_at,
                normalized_progress_at,
                normalized_completed_at,
                max(0, int(stale_after_seconds or 0)),
                last_delivery_phase,
                last_error,
                json_text(metadata),
                str(existing.get("created_at") or "").strip() or now,
                now,
            ),
        )
    return fetch_bridge_execution_lease(bridge=bridge, conversation_key=normalized_key)


def fetch_bridge_conversations(*, bridge: str = "feishu", limit: int = 50) -> list[dict[str, Any]]:
    messages = fetch_bridge_messages(bridge=bridge, limit=max(200, limit * 8))
    bindings = {row["chat_ref"]: row for row in fetch_bridge_chat_bindings(bridge=bridge, limit=max(100, limit * 4))}
    execution_leases = {
        row["conversation_key"]: row
        for row in fetch_bridge_execution_leases(bridge=bridge, limit=max(100, limit * 4))
    }
    approval_rows = fetch_approval_tokens(limit=max(100, limit * 4))
    approvals_by_chat: dict[str, list[dict[str, Any]]] = {}
    for item in approval_rows:
        metadata = item.get("metadata") or {}
        chat_ref = str(metadata.get("chat_id") or metadata.get("chat_ref") or "").strip()
        if not chat_ref:
            continue
        approvals_by_chat.setdefault(chat_ref, []).append(item)
    terminal_phases = {"direct", "error", "final", "reply", "report"}
    ordered_messages = sorted(
        messages,
        key=lambda item: (
            item.get("updated_at") or "",
            item.get("created_at") or "",
            1 if item.get("direction") == "outbound" else 0,
            item.get("message_id") or "",
        ),
    )
    grouped: dict[str, dict[str, Any]] = {}
    for item in ordered_messages:
        payload = item.get("payload") or {}
        chat_ref = str(item.get("chat_ref") or "").strip()
        if not chat_ref:
            continue
        row = grouped.setdefault(
            chat_ref,
            {
                "chat_ref": chat_ref,
                "chat_type": str(payload.get("chat_type") or payload.get("reply_target_type") or "").strip(),
                "participants": set(),
                "message_count": 0,
                "last_message_at": "",
                "last_message_preview": "",
                "last_direction": "",
                "last_user_request": "",
                "last_user_request_at": "",
                "last_report": "",
                "last_report_at": "",
                "last_error": "",
                "last_error_at": "",
                "project_name": item.get("project_name", ""),
                "binding_scope": "",
                "topic_name": "",
                "session_id": item.get("session_id", ""),
                "last_delivery_phase": "",
                "reporting_status": "",
                "last_ack_at": "",
                "last_terminal_at": "",
                "last_terminal_phase": "",
            },
        )
        row["message_count"] += 1
        sender_ref = str(payload.get("open_id") or payload.get("user_id") or payload.get("sender_ref") or "").strip()
        if sender_ref:
            row["participants"].add(sender_ref)
        stamp = item.get("updated_at") or item.get("created_at") or ""
        if stamp >= row["last_message_at"]:
            row["last_message_at"] = stamp
            row["last_message_preview"] = str(payload.get("text") or "").strip()[:180]
            row["last_direction"] = item.get("direction", "")
            row["project_name"] = item.get("project_name", "")
            row["session_id"] = item.get("session_id", "")
            row["last_delivery_phase"] = str(payload.get("phase") or "").strip()
            row["reporting_status"] = item.get("status", "") or row["last_delivery_phase"]
        if item.get("direction") == "inbound" and stamp >= row["last_user_request_at"]:
            row["last_user_request_at"] = stamp
            row["last_user_request"] = str(payload.get("text") or "").strip()[:180]
        if item.get("direction") == "outbound" and stamp >= row["last_report_at"]:
            row["last_report_at"] = stamp
            row["last_report"] = str(payload.get("text") or "").strip()[:180]
            row["reporting_status"] = item.get("status", "") or row["last_delivery_phase"]
        phase = str(payload.get("phase") or "").strip()
        if item.get("direction") == "outbound" and phase == "ack" and stamp >= row["last_ack_at"]:
            row["last_ack_at"] = stamp
        if item.get("direction") == "outbound" and phase in terminal_phases and stamp >= row["last_terminal_at"]:
            row["last_terminal_at"] = stamp
            row["last_terminal_phase"] = phase
        if item.get("direction") == "outbound" and phase == "error" and stamp >= row["last_error_at"]:
            row["last_error_at"] = stamp
            row["last_error"] = str(payload.get("text") or "").strip()[:180]
    for chat_ref, row in grouped.items():
        binding = bindings.get(chat_ref, {})
        lease = execution_leases.get(chat_ref, {})
        if binding:
            row["binding_scope"] = binding.get("binding_scope", "")
            row["topic_name"] = binding.get("topic_name", "")
            row["project_name"] = binding.get("project_name", "") or row.get("project_name", "")
            row["session_id"] = binding.get("session_id", "") or row.get("session_id", "")
        if lease:
            row["lease_state"] = str(lease.get("state") or "").strip()
            row["lease_started_at"] = str(lease.get("started_at") or "").strip()
            row["lease_last_progress_at"] = str(lease.get("last_progress_at") or "").strip()
            row["lease_completed_at"] = str(lease.get("completed_at") or "").strip()
            row["lease_stale_after_seconds"] = int(lease.get("stale_after_seconds") or 0)
            row["lease_last_delivery_phase"] = str(lease.get("last_delivery_phase") or "").strip()
            row["lease_last_error"] = str(lease.get("last_error") or "").strip()
            row["project_name"] = str(lease.get("project_name") or row.get("project_name") or "").strip()
            row["topic_name"] = str(lease.get("topic_name") or row.get("topic_name") or "").strip()
            row["session_id"] = str(lease.get("session_id") or row.get("session_id") or "").strip()
    rows = []
    for row in grouped.values():
        ack_at = str(row.get("last_ack_at") or "").strip()
        terminal_at = str(row.get("last_terminal_at") or "").strip()
        user_request_at = str(row.get("last_user_request_at") or "").strip()
        chat_ref = str(row.get("chat_ref") or "").strip()
        conversation_approvals = approvals_by_chat.get(chat_ref, [])
        pending_approvals = [item for item in conversation_approvals if approval_token_is_pending(item)]
        last_report_at = str(row.get("last_report_at") or "").strip()
        lease_state = str(row.get("lease_state") or "").strip()
        lease_started_at = str(row.get("lease_started_at") or "").strip()
        lease_last_progress_at = str(row.get("lease_last_progress_at") or "").strip()
        lease_completed_at = str(row.get("lease_completed_at") or "").strip()
        lease_stale_after_seconds = max(0, int(row.get("lease_stale_after_seconds") or 0))
        pending_request = bool(user_request_at and (not terminal_at or terminal_at < user_request_at))
        execution_state = "idle"
        if lease_state in {"running", "approval_pending", "reported", "failed", "completed"}:
            execution_state = "reported" if lease_state == "completed" else lease_state
        elif row.get("last_terminal_phase") == "error":
            execution_state = "failed"
        elif row.get("last_terminal_phase"):
            execution_state = "reported"
        elif pending_request and last_report_at and last_report_at >= user_request_at:
            execution_state = "running"
        elif pending_request:
            execution_state = "pending"
        binding_required = bool(row.get("chat_type") == "group" and not row.get("project_name"))
        ack_pending = bool(
            execution_state == "running"
            and str(row.get("lease_last_delivery_phase") or "").strip() == "ack"
        ) or bool(pending_request and ack_at and ack_at >= user_request_at)
        awaiting_report = execution_state == "running" or bool(
            pending_request and last_report_at and last_report_at >= user_request_at
        )
        last_user_request_age_seconds = age_seconds(user_request_at)
        last_ack_age_seconds = age_seconds(ack_at)
        last_report_age_seconds = age_seconds(str(row.get("last_report_at") or "").strip())
        lease_last_progress_age_seconds = age_seconds(lease_last_progress_at or lease_started_at)
        attention_reason = ""
        if execution_state == "failed":
            attention_reason = "last_execution_failed"
        elif binding_required:
            attention_reason = "binding_required"
        elif execution_state == "approval_pending" or pending_approvals:
            attention_reason = "approval_pending"
        elif (
            execution_state == "running"
            and lease_last_progress_age_seconds is not None
            and lease_stale_after_seconds > 0
            and lease_last_progress_age_seconds >= lease_stale_after_seconds
        ):
            attention_reason = "progress_stalled"
        elif awaiting_report and (last_report_age_seconds or 0) >= PROGRESS_STALL_ATTENTION_AFTER_SECONDS:
            attention_reason = "progress_stalled"
        elif pending_request and (last_user_request_age_seconds or 0) >= RESPONSE_DELAY_ATTENTION_AFTER_SECONDS:
            attention_reason = "response_delayed"
        stale_thread = bool(
            attention_reason in {"binding_required", "response_delayed", "progress_stalled"}
            and (last_user_request_age_seconds or 0) >= STALE_THREAD_ATTENTION_AFTER_SECONDS
        )
        if stale_thread:
            attention_reason = ""
        binding_scope = str(row.get("binding_scope") or "").strip() or ("project" if row.get("project_name") else "chat")
        project_name = str(row.get("project_name") or "").strip()
        topic_name = str(row.get("topic_name") or "").strip()
        binding_label = "unbound"
        if project_name and topic_name:
            binding_label = f"{project_name} / {topic_name}"
        elif project_name:
            binding_label = project_name
        thread_label = binding_label if binding_label != "unbound" else chat_ref
        pending_token = pending_approvals[0] if pending_approvals else {}
        pending_metadata = pending_token.get("metadata") or {}
        rows.append(
            {
                **row,
                "binding_scope": binding_scope,
                "binding_label": binding_label,
                "thread_label": thread_label,
                "participant_count": len(row["participants"]),
                "participants": sorted(row["participants"]),
                "execution_state": execution_state,
                "binding_required": binding_required,
                "pending_request": pending_request,
                "ack_pending": ack_pending,
                "awaiting_report": awaiting_report,
                "last_user_request_age_seconds": last_user_request_age_seconds,
                "last_ack_age_seconds": last_ack_age_seconds,
                "last_report_age_seconds": last_report_age_seconds,
                "lease_state": lease_state,
                "lease_started_at": lease_started_at,
                "lease_last_progress_at": lease_last_progress_at,
                "lease_last_progress_age_seconds": lease_last_progress_age_seconds,
                "lease_completed_at": lease_completed_at,
                "lease_stale_after_seconds": lease_stale_after_seconds,
                "lease_last_delivery_phase": str(row.get("lease_last_delivery_phase") or "").strip(),
                "lease_last_error": str(row.get("lease_last_error") or "").strip(),
                "approval_pending": bool(pending_approvals),
                "pending_approval_count": len(pending_approvals),
                "pending_approval_token": str(pending_token.get("token") or "").strip(),
                "pending_approval_expires_at": str(pending_token.get("expires_at") or "").strip(),
                "pending_approval_action": str(
                    pending_metadata.get("requested_action") or pending_metadata.get("requested_text") or ""
                ).strip(),
                "stale_thread": stale_thread,
                "needs_attention": bool(attention_reason),
                "attention_reason": attention_reason,
            }
        )
    rows.sort(key=lambda item: item.get("last_message_at", ""), reverse=True)
    return rows[: max(1, min(int(limit or 50), 200))]


def _binding_label(binding: dict[str, Any]) -> str:
    project_name = str(binding.get("project_name") or "").strip()
    topic_name = str(binding.get("topic_name") or "").strip()
    if project_name and topic_name:
        return f"{project_name} / {topic_name}"
    if project_name:
        return project_name
    return "unbound"


def fetch_bridge_continuity_status(*, bridge: str = "feishu", limit: int = 50) -> dict[str, Any]:
    bindings = fetch_bridge_chat_bindings(bridge=bridge, limit=max(100, limit * 4))
    conversations = fetch_bridge_conversations(bridge=bridge, limit=limit)
    issues: list[dict[str, Any]] = []
    shared_session_count = 0
    response_delayed_count = 0
    progress_stalled_count = 0

    bindings_by_session: dict[str, list[dict[str, Any]]] = {}
    for binding in bindings:
        session_id = str(binding.get("session_id") or "").strip()
        if not session_id:
            continue
        bindings_by_session.setdefault(session_id, []).append(binding)

    for session_id, rows in sorted(bindings_by_session.items()):
        unique_rows = []
        seen_chat_refs: set[str] = set()
        for row in rows:
            chat_ref = str(row.get("chat_ref") or "").strip()
            if not chat_ref or chat_ref in seen_chat_refs:
                continue
            seen_chat_refs.add(chat_ref)
            unique_rows.append(row)
        if len(unique_rows) <= 1:
            continue
        shared_session_count += 1
        bindings_payload = [
            {
                "chat_ref": str(item.get("chat_ref") or "").strip(),
                "binding_scope": str(item.get("binding_scope") or "").strip(),
                "project_name": str(item.get("project_name") or "").strip(),
                "topic_name": str(item.get("topic_name") or "").strip(),
                "binding_label": _binding_label(item),
                "updated_at": str(item.get("updated_at") or "").strip(),
            }
            for item in sorted(unique_rows, key=lambda value: str(value.get("chat_ref") or ""))
        ]
        issues.append(
            {
                "issue_type": "shared_session_across_chats",
                "severity": "warning",
                "session_id": session_id,
                "chat_refs": [item["chat_ref"] for item in bindings_payload],
                "bindings": bindings_payload,
                "summary": f"session `{session_id}` 同时绑定到 {len(bindings_payload)} 个 chat",
            }
        )

    for row in conversations:
        attention_reason = str(row.get("attention_reason") or "").strip()
        if attention_reason not in {"response_delayed", "progress_stalled"}:
            continue
        if row.get("stale_thread"):
            continue
        if attention_reason == "response_delayed":
            response_delayed_count += 1
        else:
            progress_stalled_count += 1
        issues.append(
            {
                "issue_type": attention_reason,
                "severity": "warning",
                "chat_ref": str(row.get("chat_ref") or "").strip(),
                "project_name": str(row.get("project_name") or "").strip(),
                "topic_name": str(row.get("topic_name") or "").strip(),
                "session_id": str(row.get("session_id") or "").strip(),
                "thread_label": str(row.get("thread_label") or "").strip(),
                "last_user_request_at": str(row.get("last_user_request_at") or "").strip(),
                "last_ack_at": str(row.get("last_ack_at") or "").strip(),
                "last_report_at": str(row.get("last_report_at") or "").strip(),
                "summary": (
                    f"chat `{row.get('chat_ref', '')}` {attention_reason} "
                    f"(thread={row.get('thread_label', '')}, session={row.get('session_id', '')})"
                ).strip(),
            }
        )

    return {
        "bridge": bridge,
        "ok": not issues,
        "issue_count": len(issues),
        "shared_session_count": shared_session_count,
        "ack_delayed_count": 0,
        "awaiting_report_count": progress_stalled_count,
        "response_delayed_count": response_delayed_count,
        "progress_stalled_count": progress_stalled_count,
        "issues": issues,
    }


def replace_review_items(items: list[dict[str, Any]]) -> int:
    now = iso_now()
    with transaction() as conn:
        conn.execute("DELETE FROM review_items")
        for item in items:
            conn.execute(
                """
                INSERT INTO review_items (
                    task_ref, project_name, source_path, review_status, reviewer,
                    deliverable_ref, decision_note, decided_at, metadata_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["task_ref"],
                    item["project_name"],
                    item["source_path"],
                    item["review_status"],
                    item.get("reviewer", ""),
                    item.get("deliverable_ref", ""),
                    item.get("decision_note", ""),
                    item.get("decided_at", ""),
                    json_text(item.get("metadata", {})),
                    now,
                ),
            )
    return len(items)


def replace_coordination_items(items: list[dict[str, Any]]) -> int:
    now = iso_now()
    with transaction() as conn:
        conn.execute("DELETE FROM coordination_items")
        for item in items:
            conn.execute(
                """
                INSERT INTO coordination_items (
                    coordination_id, from_project, to_project, status, assignee,
                    due_at, receipt_ref, source_ref, requested_action, metadata_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["coordination_id"],
                    item["from_project"],
                    item["to_project"],
                    item["status"],
                    item.get("assignee", ""),
                    item.get("due_at", ""),
                    item.get("receipt_ref", ""),
                    item.get("source_ref", ""),
                    item.get("requested_action", ""),
                    json_text(item.get("metadata", {})),
                    now,
                ),
            )
    return len(items)


def fetch_review_items(*, project_name: str = "", statuses: list[str] | None = None) -> list[dict[str, Any]]:
    statuses = statuses or []
    with connect() as conn:
        sql = "SELECT * FROM review_items"
        clauses: list[str] = []
        params: list[Any] = []
        if project_name:
            clauses.append("project_name = ?")
            params.append(project_name)
        if statuses:
            clauses.append("review_status IN ({})".format(", ".join("?" for _ in statuses)))
            params.extend(statuses)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC, task_ref ASC"
        rows = conn.execute(sql, tuple(params)).fetchall()
    return rows_to_dicts(rows)


def fetch_coordination_items(*, project_name: str = "", statuses: list[str] | None = None) -> list[dict[str, Any]]:
    statuses = statuses or []
    with connect() as conn:
        sql = "SELECT * FROM coordination_items"
        clauses: list[str] = []
        params: list[Any] = []
        if project_name:
            clauses.append("(from_project = ? OR to_project = ?)")
            params.extend([project_name, project_name])
        if statuses:
            clauses.append("status IN ({})".format(", ".join("?" for _ in statuses)))
            params.extend(statuses)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC, coordination_id ASC"
        rows = conn.execute(sql, tuple(params)).fetchall()
    return rows_to_dicts(rows)


def fetch_runtime_summary() -> dict[str, Any]:
    init_db()
    queue_status = fetch_runtime_queue_status()
    with connect() as conn:
        return {
            "db_path": str(runtime_db_path()),
            "bridge_message_count": scalar(conn, "SELECT COUNT(*) FROM bridge_messages"),
            "delivery_count": scalar(conn, "SELECT COUNT(*) FROM delivery_status"),
            "approval_token_count": scalar(conn, "SELECT COUNT(*) FROM approval_tokens"),
            "review_item_count": scalar(conn, "SELECT COUNT(*) FROM review_items"),
            "coordination_item_count": scalar(conn, "SELECT COUNT(*) FROM coordination_items"),
            "sidecar_receipt_count": scalar(conn, "SELECT COUNT(*) FROM sidecar_receipts"),
            "bridge_settings_count": scalar(conn, "SELECT COUNT(*) FROM bridge_settings"),
            "bridge_connection_count": scalar(conn, "SELECT COUNT(*) FROM bridge_connections"),
            "bridge_chat_binding_count": scalar(conn, "SELECT COUNT(*) FROM bridge_chat_bindings"),
            "bridge_execution_lease_count": scalar(conn, "SELECT COUNT(*) FROM bridge_execution_leases"),
            "runtime_event_count": scalar(conn, "SELECT COUNT(*) FROM runtime_events"),
            "growth_action_attempt_count": scalar(conn, "SELECT COUNT(*) FROM growth_action_attempts"),
            "runtime_queue": queue_status,
        }


def fetch_bridge_chat_binding(*, bridge: str = "feishu", chat_ref: str) -> dict[str, Any]:
    init_db()
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM bridge_chat_bindings WHERE bridge = ? AND chat_ref = ?",
            (bridge, chat_ref),
        ).fetchone()
    payload = row_to_dict(row)
    if not payload:
        return {
            "bridge": bridge,
            "chat_ref": chat_ref,
            "binding_scope": "",
            "project_name": "",
            "topic_name": "",
            "session_id": "",
            "metadata": {},
            "created_at": "",
            "updated_at": "",
        }
    return {
        "bridge": payload["bridge"],
        "chat_ref": payload["chat_ref"],
        "binding_scope": payload["binding_scope"],
        "project_name": payload["project_name"],
        "topic_name": payload["topic_name"],
        "session_id": payload["session_id"],
        "metadata": json.loads(payload["metadata_json"]),
        "created_at": payload["created_at"],
        "updated_at": payload["updated_at"],
    }


def fetch_bridge_chat_bindings(*, bridge: str = "feishu", limit: int = 100) -> list[dict[str, Any]]:
    init_db()
    query_limit = max(1, min(int(limit or 100), 500))
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM bridge_chat_bindings
            WHERE bridge = ?
            ORDER BY updated_at DESC, chat_ref ASC
            LIMIT ?
            """,
            (bridge, query_limit),
        ).fetchall()
    payloads = []
    for row in rows_to_dicts(rows):
        payloads.append(
            {
                "bridge": row["bridge"],
                "chat_ref": row["chat_ref"],
                "binding_scope": row["binding_scope"],
                "project_name": row["project_name"],
                "topic_name": row["topic_name"],
                "session_id": row["session_id"],
                "metadata": json.loads(row["metadata_json"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return payloads


def upsert_bridge_chat_binding(
    *,
    bridge: str,
    chat_ref: str,
    binding_scope: str,
    project_name: str,
    topic_name: str = "",
    session_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    now = iso_now()
    metadata = metadata or {}
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO bridge_chat_bindings (
                bridge, chat_ref, binding_scope, project_name, topic_name,
                session_id, metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bridge, chat_ref) DO UPDATE SET
                binding_scope=excluded.binding_scope,
                project_name=excluded.project_name,
                topic_name=excluded.topic_name,
                session_id=excluded.session_id,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (
                bridge,
                chat_ref,
                binding_scope,
                project_name,
                topic_name,
                session_id,
                json_text(metadata),
                now,
                now,
            ),
        )
    return fetch_bridge_chat_binding(bridge=bridge, chat_ref=chat_ref)


def fetch_bridge_settings(bridge: str = "feishu") -> dict[str, Any]:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT * FROM bridge_settings WHERE bridge = ?", (bridge,)).fetchone()
    payload = row_to_dict(row)
    if not payload:
        return {"bridge": bridge, "settings": {}, "updated_at": ""}
    return {
        "bridge": payload["bridge"],
        "settings": json.loads(payload["settings_json"]),
        "updated_at": payload["updated_at"],
    }


def upsert_bridge_settings(bridge: str, settings: dict[str, Any]) -> dict[str, Any]:
    init_db()
    now = iso_now()
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO bridge_settings (bridge, settings_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(bridge) DO UPDATE SET
                settings_json=excluded.settings_json,
                updated_at=excluded.updated_at
            """,
            (bridge, json_text(settings), now),
        )
    return fetch_bridge_settings(bridge)


def fetch_bridge_connection(bridge: str = "feishu") -> dict[str, Any]:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT * FROM bridge_connections WHERE bridge = ?", (bridge,)).fetchone()
    payload = row_to_dict(row)
    if not payload:
        return {
            "bridge": bridge,
            "host_mode": "",
            "transport": "",
            "status": "disconnected",
            "last_error": "",
            "last_event_at": "",
            "metadata": {},
            "updated_at": "",
        }
    return {
        "bridge": payload["bridge"],
        "host_mode": payload["host_mode"],
        "transport": payload["transport"],
        "status": payload["status"],
        "last_error": payload["last_error"],
        "last_event_at": payload["last_event_at"],
        "metadata": json.loads(payload["metadata_json"]),
        "updated_at": payload["updated_at"],
    }


def _prefer_latest_iso(current: str, candidate: str) -> str:
    current_dt = parse_iso_timestamp(str(current or "").strip())
    candidate_dt = parse_iso_timestamp(str(candidate or "").strip())
    if current_dt is None:
        return str(candidate or "").strip()
    if candidate_dt is None:
        return str(current or "").strip()
    return str(candidate if candidate_dt >= current_dt else current).strip()


def bridge_runtime_snapshot(*, bridge: str = "feishu") -> dict[str, Any]:
    connection = fetch_bridge_connection(bridge)
    activity = fetch_bridge_message_activity(bridge)
    continuity = fetch_bridge_continuity_status(bridge=bridge, limit=20)
    inbound = dict(activity.get("inbound", {}) or {})
    outbound = dict(activity.get("outbound", {}) or {})
    metadata = dict(connection.get("metadata", {}) or {})
    metadata.update(
        {
            "host_mode": str(connection.get("host_mode", "")).strip(),
            "updated_at": str(connection.get("updated_at", "")).strip(),
            "continuity_ok": bool(continuity.get("ok", False)),
            "shared_session_count": int(continuity.get("shared_session_count", 0) or 0),
            "response_delayed_count": int(continuity.get("response_delayed_count", 0) or 0),
            "progress_stalled_count": int(continuity.get("progress_stalled_count", 0) or 0),
        }
    )
    last_event_at = _prefer_latest_iso(
        str(connection.get("last_event_at", "")).strip(),
        str(inbound.get("activity_at") or inbound.get("cursor_at") or "").strip(),
    )
    if last_event_at == str(inbound.get("activity_at") or inbound.get("cursor_at") or "").strip():
        metadata["last_message_preview"] = str(inbound.get("text") or metadata.get("last_message_preview") or "").strip()
        metadata["last_sender_ref"] = str(inbound.get("sender_ref") or metadata.get("last_sender_ref") or "").strip()
    metadata["last_delivery_at"] = _prefer_latest_iso(
        str(metadata.get("last_delivery_at") or "").strip(),
        str(outbound.get("activity_at") or outbound.get("cursor_at") or "").strip(),
    )
    if metadata["last_delivery_at"] == str(outbound.get("activity_at") or outbound.get("cursor_at") or "").strip():
        metadata["last_delivery_phase"] = str(outbound.get("phase") or metadata.get("last_delivery_phase") or "").strip()
    return workspace_job_schema.BridgeRuntimeSnapshot(
        bridge=bridge,
        status=str(connection.get("status", "")).strip(),
        transport=str(connection.get("transport", "")).strip(),
        last_event_at=last_event_at,
        last_error=str(connection.get("last_error", "")).strip(),
        inbound_message_id=str(inbound.get("message_id", "")).strip(),
        inbound_cursor_at=str(inbound.get("cursor_at", "")).strip(),
        outbound_message_id=str(outbound.get("message_id", "")).strip(),
        outbound_cursor_at=str(outbound.get("cursor_at", "")).strip(),
        continuity_issue_count=int(continuity.get("issue_count", 0) or 0),
        metadata=metadata,
    ).to_dict()


def bridge_status_surface(*, bridge: str = "feishu", settings_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = bridge_runtime_snapshot(bridge=bridge)
    return workspace_job_schema.bridge_status_surface(snapshot, settings_summary=settings_summary)


def upsert_bridge_connection(
    bridge: str,
    *,
    status: str,
    host_mode: str = "",
    transport: str = "",
    last_error: str = "",
    last_event_at: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_db()
    now = iso_now()
    metadata = metadata or {}
    with transaction() as conn:
        conn.execute(
            """
            INSERT INTO bridge_connections (
                bridge, host_mode, transport, status,
                last_error, last_event_at, metadata_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bridge) DO UPDATE SET
                host_mode=excluded.host_mode,
                transport=excluded.transport,
                status=excluded.status,
                last_error=excluded.last_error,
                last_event_at=excluded.last_event_at,
                metadata_json=excluded.metadata_json,
                updated_at=excluded.updated_at
            """,
            (bridge, host_mode, transport, status, last_error, last_event_at, json_text(metadata), now),
        )
    return fetch_bridge_connection(bridge)


def feishu_runtime_contract() -> dict[str, Any]:
    return {
        "truth_source": "obsidian_vault",
        "bitable_mode": "read_only_projection",
        "writable_tables": ["bridge_messages", "delivery_status", "bridge_execution_leases"],
        "reserved_tables": ["approval_tokens", "sidecar_receipts"],
        "read_only_tables": ["review_items", "coordination_items"],
        "queue_tables": ["runtime_events"],
        "durable_queues": [
            "retrieval_sync",
            "dashboard_sync",
            "feishu_projection_sync",
            "growth_feishu_projection_sync",
            "bridge_message_log",
            "delivery_status_log",
            "approval_token_log",
        ],
        "ownership": RUNTIME_OWNERSHIP,
        "runtime_db_path": str(runtime_db_path()),
    }

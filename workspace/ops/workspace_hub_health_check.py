#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import subprocess
import sys
import tomllib
import uuid
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import (
    codex_memory,
    project_pause,
    result_cache,
    runtime_state,
    workspace_job_schema,
    workspace_hub_project,
    workspace_hub_route_check,
    workspace_wake_broker,
)


HEALTH_AGENT_NAME = "com.codexhub.workspace-hub-health-check"
HEALTH_AUTOMATION_ID = "workspace-health"
OFFICIAL_SCHEDULER_ID = HEALTH_AGENT_NAME
STATUS_CACHE_NAMESPACE = "workspace-health-status"
ALERT_CONFIRMATION_PASSES = 2
HEALTH_INTERVAL_SECONDS = 4 * 3600
WAKE_CATCHUP_GRACE_SECONDS = 30 * 60
DASHBOARD_REBUILD_TIMEOUT_SECONDS = 30
PROJECT_NAME = workspace_hub_project.PROJECT_NAME
SEVERITY_ORDER = {"info": 0, "warning": 1, "critical": 2}
DEFAULT_WORKSPACE_ROOT = workspace_hub_project.DEFAULT_WORKSPACE_ROOT
DEFAULT_VAULT_ROOT = workspace_hub_project.DEFAULT_LOCAL_VAULT_ROOT


def fixture_mode() -> bool:
    return os.environ.get("WORKSPACE_HUB_FIXTURE_MODE", "").strip() == "1" or "PYTEST_CURRENT_TEST" in os.environ


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(DEFAULT_WORKSPACE_ROOT)))


def code_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CODE_ROOT", str(workspace_root())))


def expected_workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_EXPECTED_WORKSPACE_ROOT", str(code_root())))


def expected_vault_root() -> Path:
    if fixture_mode():
        return Path(os.environ.get("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", str(DEFAULT_VAULT_ROOT)))
    return DEFAULT_VAULT_ROOT


def expected_projects_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_EXPECTED_PROJECTS_ROOT", str(expected_workspace_root() / "projects")))


def looks_like_vault_root(path: Path) -> bool:
    return (path / "PROJECT_REGISTRY.md").exists() and (path / "01_working").exists()


def vault_root() -> Path:
    if not fixture_mode():
        return DEFAULT_VAULT_ROOT
    candidate = Path(os.environ.get("WORKSPACE_HUB_VAULT_ROOT", str(expected_vault_root())))
    if looks_like_vault_root(candidate):
        return candidate
    fallback = expected_vault_root()
    if looks_like_vault_root(fallback):
        return fallback
    return candidate


def reports_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_REPORTS_ROOT", str(workspace_root() / "reports")))


def health_reports_root() -> Path:
    return reports_root() / "ops" / "workspace-hub-health"


def history_path() -> Path:
    return health_reports_root() / "history.ndjson"


def alerts_path() -> Path:
    return health_reports_root() / "alerts.ndjson"


def latest_report_path() -> Path:
    return health_reports_root() / "latest.md"


def archive_report_path() -> Path:
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return health_reports_root() / f"health-{stamp}.md"


def health_topic_board_path() -> Path:
    return vault_root() / "01_working" / f"{PROJECT_NAME}-运维巡检-跟进板.md"


def project_board_path() -> Path:
    return vault_root() / "01_working" / f"{PROJECT_NAME}-项目板.md"


def launch_agent_plist_path() -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{HEALTH_AGENT_NAME}.plist"


def log_stdout_path() -> Path:
    return workspace_root() / "logs" / "workspace-hub-health-check.log"


def log_stderr_path() -> Path:
    return workspace_root() / "logs" / "workspace-hub-health-check.err.log"


def codex_home() -> Path:
    return Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))


def automation_config_path() -> Path:
    return codex_home() / "automations" / HEALTH_AUTOMATION_ID / "automation.toml"


def automation_db_path() -> Path:
    return codex_home() / "sqlite" / "codex-dev.db"


def command_env() -> dict[str, str]:
    env = os.environ.copy()
    env["WORKSPACE_HUB_CODE_ROOT"] = str(code_root())
    env["WORKSPACE_HUB_ROOT"] = str(expected_workspace_root())
    env["WORKSPACE_HUB_VAULT_ROOT"] = str(vault_root())
    env["WORKSPACE_HUB_PROJECTS_ROOT"] = str(expected_projects_root())
    return env


def iso_now_local() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def iso_from_millis(value: Any) -> str:
    try:
        raw = int(value)
    except (TypeError, ValueError):
        return ""
    return dt.datetime.fromtimestamp(raw / 1000, tz=dt.timezone.utc).astimezone().isoformat(timespec="seconds")


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


def dashboard_paths() -> list[str]:
    return [
        str(codex_memory.HOME_DASHBOARD_MD),
        str(codex_memory.PROJECTS_DASHBOARD_MD),
        str(codex_memory.ACTIONS_DASHBOARD_MD),
        str(codex_memory.MEMORY_HEALTH_MD),
    ]


def board_paths() -> list[str]:
    return [
        str(health_topic_board_path()),
        str(project_board_path()),
        str(codex_memory.NEXT_ACTIONS_MD),
    ]


def base_related_board_paths() -> list[str]:
    return board_paths() + dashboard_paths()


def run_json_command(command: list[str]) -> tuple[dict[str, Any], int]:
    result = subprocess.run(
        command,
        cwd=code_root(),
        env=command_env(),
        text=True,
        capture_output=True,
        check=False,
    )
    payload = json.loads(result.stdout) if result.stdout.strip() else {}
    return payload, result.returncode


def load_run_ledger_entries() -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    path = history_path()
    if not path.exists():
        return entries
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        entries.append(payload)
    return entries


def official_scheduler_run_count() -> int:
    count = 0
    for item in load_run_ledger_entries():
        if item.get("trigger_source") == "launchd" or item.get("scheduler_id") == OFFICIAL_SCHEDULER_ID:
            count += 1
    return count


def direct_scheduler_run_count() -> int:
    count = 0
    for item in load_run_ledger_entries():
        if item.get("trigger_source") == "launchd":
            count += 1
    return count


def latest_run_record() -> dict[str, Any]:
    entries = load_run_ledger_entries()
    return entries[-1] if entries else {}


def default_health_interval_seconds() -> int:
    return int(os.environ.get("WORKSPACE_HUB_HEALTH_INTERVAL_SECONDS", str(HEALTH_INTERVAL_SECONDS)))


def default_catchup_grace_seconds() -> int:
    return int(os.environ.get("WORKSPACE_HUB_HEALTH_CATCHUP_GRACE_SECONDS", str(WAKE_CATCHUP_GRACE_SECONDS)))


def request_health_wake(
    *,
    reason: str,
    trigger_source: str = "",
    scheduled_for: str = "",
    automation_run_id: str = "",
    scheduler_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "trigger_source": trigger_source or reason,
        "scheduled_for": scheduled_for,
        "automation_run_id": automation_run_id,
        "scheduler_id": scheduler_id,
    }
    if metadata:
        payload.update(metadata)
    return workspace_wake_broker.request_wake(
        HEALTH_AUTOMATION_ID,
        reason=reason,
        metadata=payload,
    )


def run_requested_health_wake() -> dict[str, Any]:
    claimed = workspace_wake_broker.claim_wake(HEALTH_AUTOMATION_ID)
    if not claimed.get("claimed"):
        return {
            "executed": False,
            "reason": claimed.get("reason", ""),
            "pending": claimed.get("pending", {}),
            "running": claimed.get("running", {}),
        }
    wake = claimed["wake"]
    metadata = wake.get("metadata", {}) or {}
    try:
        payload = run_health_check(
            trigger_source=str(metadata.get("trigger_source") or wake.get("reason") or "wake_broker"),
            scheduled_for=str(metadata.get("scheduled_for") or ""),
            automation_run_id=str(metadata.get("automation_run_id") or ""),
            scheduler_id=str(metadata.get("scheduler_id") or ""),
        )
    except Exception as exc:
        workspace_wake_broker.complete_wake(
            HEALTH_AUTOMATION_ID,
            wake_id=str(wake.get("wake_id", "")),
            status="failed",
            result={"error": f"{type(exc).__name__}: {exc}"},
        )
        raise

    workspace_wake_broker.complete_wake(
        HEALTH_AUTOMATION_ID,
        wake_id=str(wake.get("wake_id", "")),
        status="succeeded" if payload.get("ok") else "failed",
        result={
            "ok": bool(payload.get("ok")),
            "run_id": str(payload.get("run_record", {}).get("run_id", "")),
            "issue_count": int(payload.get("run_record", {}).get("issue_count", 0) or 0),
        },
    )
    return {
        "executed": True,
        "reason": str(wake.get("reason", "")),
        "wake": wake,
        "payload": payload,
    }


def compute_catchup_status(
    *,
    now: dt.datetime | None = None,
    scheduler_status: dict[str, Any] | None = None,
    interval_seconds: int | None = None,
    grace_seconds: int | None = None,
) -> dict[str, Any]:
    current_time = now.astimezone() if now else dt.datetime.now().astimezone()
    scheduler = scheduler_status or load_official_scheduler_status()
    interval = int(interval_seconds or default_health_interval_seconds())
    grace = int(grace_seconds or default_catchup_grace_seconds())
    latest = latest_run_record()
    last_finished_at = parse_timestamp(str(latest.get("finished_at") or latest.get("checked_at") or ""))
    next_run_at = parse_timestamp(str(scheduler.get("next_run_at", "")))
    last_due_at = last_finished_at + dt.timedelta(seconds=interval + grace) if last_finished_at else None
    scheduled_from_last = last_finished_at + dt.timedelta(seconds=interval) if last_finished_at else None
    next_due_at = next_run_at + dt.timedelta(seconds=grace) if next_run_at else None
    next_anchor_relevant = bool(next_run_at) and (not last_finished_at or next_run_at >= last_finished_at)

    def pack(
        *,
        should_run: bool,
        reason: str,
        due_at: dt.datetime | None,
        scheduled_for: dt.datetime | None,
    ) -> dict[str, Any]:
        overdue_seconds = 0
        if should_run and due_at:
            overdue_seconds = max(0, int((current_time - due_at).total_seconds()))
        return {
            "should_run": should_run,
            "reason": reason,
            "due_at": due_at.isoformat(timespec="seconds") if due_at else "",
            "scheduled_for": scheduled_for.isoformat(timespec="seconds") if scheduled_for else "",
            "overdue_seconds": overdue_seconds,
            "last_finished_at": last_finished_at.isoformat(timespec="seconds") if last_finished_at else "",
            "next_run_at": next_run_at.isoformat(timespec="seconds") if next_run_at else "",
        }

    if not scheduler.get("active"):
        return pack(should_run=False, reason="scheduler_inactive", due_at=None, scheduled_for=None)

    if last_due_at and current_time > last_due_at:
        return pack(
            should_run=True,
            reason="stale_after_sleep_or_missed_window",
            due_at=last_due_at,
            scheduled_for=scheduled_from_last,
        )

    if next_anchor_relevant and next_due_at and current_time > next_due_at:
        return pack(
            should_run=True,
            reason="stale_after_sleep_or_missed_window",
            due_at=next_due_at,
            scheduled_for=next_run_at,
        )

    fresh_due_candidates = [item for item in [last_due_at, next_due_at if next_anchor_relevant else None] if item]
    fresh_scheduled_candidates = [item for item in [scheduled_from_last, next_run_at if next_anchor_relevant else None] if item]
    if not fresh_due_candidates:
        return pack(should_run=False, reason="no_schedule_anchor", due_at=None, scheduled_for=None)
    return pack(
        should_run=False,
        reason="fresh",
        due_at=min(fresh_due_candidates),
        scheduled_for=min(fresh_scheduled_candidates) if fresh_scheduled_candidates else None,
    )


def load_codex_automation_status() -> dict[str, Any]:
    info: dict[str, Any] = {
        "id": HEALTH_AUTOMATION_ID,
        "configured": False,
        "config_status": "",
        "runtime_status": "",
        "prompt": "",
        "cwds": [],
        "cwd_matches": False,
        "required_cwds": [],
        "missing_cwds": [],
        "last_run_at": "",
        "next_run_at": "",
        "run_count": 0,
        "verified_run_count": official_scheduler_run_count(),
        "direct_run_count": direct_scheduler_run_count(),
    }
    config_path = automation_config_path()
    if config_path.exists():
        info["configured"] = True
        payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
        info["config_status"] = payload.get("status", "")
        info["prompt"] = payload.get("prompt", "")
        info["cwds"] = [str(item) for item in payload.get("cwds", [])]
    required = {str(expected_workspace_root()), str(expected_vault_root())}
    configured = set(info["cwds"])
    info["required_cwds"] = sorted(required)
    info["missing_cwds"] = sorted(path for path in required if path not in configured)
    info["cwd_matches"] = not info["missing_cwds"]
    db_path = automation_db_path()
    if not db_path.exists():
        info["active"] = info["configured"] and info["config_status"] == "ACTIVE" and info["cwd_matches"]
        return info
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "select status, last_run_at, next_run_at from automations where id = ?",
                (HEALTH_AUTOMATION_ID,),
            ).fetchone()
            if row:
                info["runtime_status"] = row[0] or ""
                info["last_run_at"] = iso_from_millis(row[1])
                info["next_run_at"] = iso_from_millis(row[2])
            run_row = conn.execute(
                "select count(*) from automation_runs where automation_id = ?",
                (HEALTH_AUTOMATION_ID,),
            ).fetchone()
            if run_row:
                info["run_count"] = int(run_row[0] or 0)
    except sqlite3.Error:
        pass
    info["active"] = (
        info["configured"]
        and info["config_status"] == "ACTIVE"
        and info["runtime_status"] == "ACTIVE"
        and info["cwd_matches"]
    )
    return info


def load_launchagent_interval_seconds() -> int:
    path = launch_agent_plist_path()
    if not path.exists():
        return default_health_interval_seconds()
    try:
        import plistlib

        with path.open("rb") as fh:
            payload = plistlib.load(fh)
    except Exception:
        return default_health_interval_seconds()
    try:
        return int(payload.get("StartInterval") or default_health_interval_seconds())
    except (TypeError, ValueError):
        return default_health_interval_seconds()


def load_official_scheduler_status() -> dict[str, Any]:
    installed = launch_agent_plist_path().exists()
    loaded = codex_memory.launch_agent_loaded(HEALTH_AGENT_NAME)
    interval = load_launchagent_interval_seconds()
    latest = latest_run_record()
    last_finished_at = parse_timestamp(str(latest.get("finished_at") or latest.get("checked_at") or ""))
    next_run_at = ""
    if last_finished_at:
        next_run_at = (last_finished_at + dt.timedelta(seconds=interval)).isoformat(timespec="seconds")
    return {
        "id": OFFICIAL_SCHEDULER_ID,
        "type": "launchagent",
        "configured": installed,
        "config_status": "ACTIVE" if installed else "INACTIVE",
        "runtime_status": "ACTIVE" if loaded else "INACTIVE",
        "prompt": "",
        "cwds": [str(expected_workspace_root()), str(expected_vault_root())],
        "cwd_matches": True,
        "required_cwds": [str(expected_workspace_root()), str(expected_vault_root())],
        "missing_cwds": [],
        "last_run_at": str(latest.get("finished_at") or latest.get("checked_at") or ""),
        "next_run_at": next_run_at,
        "run_count": official_scheduler_run_count(),
        "verified_run_count": official_scheduler_run_count(),
        "direct_run_count": direct_scheduler_run_count(),
        "active": installed and loaded,
        "interval_seconds": interval,
        "plist": str(launch_agent_plist_path()),
    }


def trigger_dashboard_rebuild() -> subprocess.CompletedProcess[str] | None:
    sync_script = code_root() / "ops" / "codex_dashboard_sync.py"
    if not sync_script.exists():
        return None
    try:
        result = subprocess.run(
            ["python3", str(sync_script), "rebuild-all"],
            cwd=code_root(),
            env=command_env(),
            text=True,
            capture_output=True,
            check=False,
            timeout=DASHBOARD_REBUILD_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        print(
            f"[workspace_hub_health_check] dashboard rebuild timed out after {DASHBOARD_REBUILD_TIMEOUT_SECONDS}s",
            file=sys.stderr,
        )
        return subprocess.CompletedProcess(
            exc.cmd,
            124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or f"timeout after {DASHBOARD_REBUILD_TIMEOUT_SECONDS}s",
        )
    if result.returncode != 0:
        print(
            f"[workspace_hub_health_check] dashboard rebuild failed: {result.stderr.strip() or result.stdout.strip()}",
            file=sys.stderr,
        )
    return result


def collect_refresh_chain_status() -> dict[str, Any]:
    pre_consistency, pre_consistency_code = run_json_command(
        ["python3", str(code_root() / "ops" / "codex_dashboard_sync.py"), "verify-consistency"]
    )
    refresh_index, refresh_index_code = run_json_command(
        ["python3", str(code_root() / "ops" / "codex_memory.py"), "refresh-index"]
    )
    try:
        rebuild_result = subprocess.run(
            ["python3", str(code_root() / "ops" / "codex_dashboard_sync.py"), "rebuild-all"],
            cwd=code_root(),
            env=command_env(),
            text=True,
            capture_output=True,
            check=False,
            timeout=DASHBOARD_REBUILD_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        rebuild_result = subprocess.CompletedProcess(
            exc.cmd,
            124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or f"timeout after {DASHBOARD_REBUILD_TIMEOUT_SECONDS}s",
        )
    try:
        rebuild_payload = json.loads(rebuild_result.stdout) if rebuild_result.stdout.strip() else {}
    except json.JSONDecodeError:
        rebuild_payload = {"raw_stdout": rebuild_result.stdout}
    post_consistency, post_consistency_code = run_json_command(
        ["python3", str(code_root() / "ops" / "codex_dashboard_sync.py"), "verify-consistency"]
    )
    pre_consistency["exit_code"] = pre_consistency_code
    refresh_index["exit_code"] = refresh_index_code
    rebuild_payload["exit_code"] = rebuild_result.returncode
    rebuild_payload.setdefault("stderr", rebuild_result.stderr)
    post_consistency["exit_code"] = post_consistency_code
    return {
        "consistency_pre_refresh": pre_consistency,
        "refresh_index": refresh_index,
        "rebuild_all": rebuild_payload,
        "consistency": post_consistency,
    }


def resolve_run_context(
    *,
    trigger_source: str = "",
    scheduled_for: str = "",
    automation_run_id: str = "",
    scheduler_id: str = "",
) -> dict[str, str]:
    started_at = iso_now_local()
    resolved_trigger = trigger_source or os.environ.get("WORKSPACE_HUB_HEALTH_TRIGGER_SOURCE", "").strip() or "manual_cli"
    resolved_schedule = scheduled_for or os.environ.get("WORKSPACE_HUB_HEALTH_SCHEDULED_FOR", "").strip() or started_at
    resolved_run_id = automation_run_id or os.environ.get("WORKSPACE_HUB_HEALTH_AUTOMATION_RUN_ID", "").strip()
    resolved_scheduler_id = scheduler_id or os.environ.get("WORKSPACE_HUB_HEALTH_SCHEDULER", "").strip()
    return {
        "run_id": f"whc-{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}",
        "trigger_source": resolved_trigger,
        "scheduled_for": resolved_schedule,
        "automation_run_id": resolved_run_id,
        "scheduler_id": resolved_scheduler_id,
        "started_at": started_at,
    }


def collect_checks(run_context: dict[str, str]) -> dict[str, Any]:
    scheduler_status = load_official_scheduler_status()
    codex_automation_status = load_codex_automation_status()
    watcher, _ = run_json_command(["python3", str(code_root() / "ops" / "codex_session_watcher.py"), "status"])
    dashboard, _ = run_json_command(["python3", str(code_root() / "ops" / "codex_dashboard_sync.py"), "status"])
    refresh_chain = collect_refresh_chain_status()
    route = workspace_hub_route_check.run_checks()
    bridge_continuity = runtime_state.fetch_bridge_continuity_status(bridge="feishu", limit=100)
    return {
        "checked_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "watcher": watcher,
        "dashboard_sync": dashboard,
        **refresh_chain,
        "routing": route,
        "bridge_continuity": bridge_continuity,
        "official_scheduler": scheduler_status,
        "health_launchagent": scheduler_status,
        "codex_automation": codex_automation_status,
        "catchup_status": compute_catchup_status(scheduler_status=scheduler_status),
        "run_context": run_context,
    }


def active_health_pause() -> dict[str, Any]:
    return project_pause.active_pause(project_name=PROJECT_NAME, scope="automation")


def escalate_severity(*values: str) -> str:
    chosen = "info"
    for item in values:
        if SEVERITY_ORDER.get(item, -1) > SEVERITY_ORDER.get(chosen, -1):
            chosen = item
    return chosen


def should_uproll_alert(
    *,
    requires_action: bool,
    impacts_core: bool,
    requires_manager_attention: bool,
    occurrence_count: int = 1,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if requires_action:
        reasons.append("requires_action")
    if impacts_core:
        reasons.append("impacts_core_chain")
    if requires_manager_attention:
        reasons.append("requires_manager_attention")
    if occurrence_count >= 2:
        reasons.append("repeated")
    return (bool(reasons), reasons)


def build_alert(
    *,
    alert_key: str,
    category: str,
    severity: str,
    summary: str,
    requires_manager_attention: bool,
    affects_core: bool = True,
    occurrence_count: int = 1,
) -> dict[str, Any]:
    uproll, reasons = should_uproll_alert(
        requires_action=True,
        impacts_core=affects_core,
        requires_manager_attention=requires_manager_attention,
        occurrence_count=occurrence_count,
    )
    return {
        "alert_key": alert_key,
        "category": category,
        "severity": severity,
        "current_summary": summary,
        "requires_manager_attention": requires_manager_attention,
        "affected_targets": ["运维巡检专题板", "一级项目板", "NEXT_ACTIONS", "07_dashboards"],
        "related_board_paths": base_related_board_paths(),
        "status": "open",
        "uproll": uproll,
        "uproll_reasons": reasons,
    }


def evaluate_checks(checks: dict[str, Any]) -> dict[str, Any]:
    alerts: list[dict[str, Any]] = []
    watcher_ok = bool(checks["watcher"].get("installed")) and bool(checks["watcher"].get("loaded"))
    dashboard_ok = bool(checks["dashboard_sync"].get("installed")) and bool(checks["dashboard_sync"].get("loaded"))
    refresh_index_ok = checks.get("refresh_index", {}).get("changed") is True or checks.get("refresh_index", {}).get("exit_code", 0) == 0
    rebuild_ok = checks.get("rebuild_all", {}).get("status", "ok") == "ok" and checks.get("rebuild_all", {}).get("exit_code", 0) == 0
    consistency_pre_refresh = checks.get("consistency_pre_refresh", {})
    consistency_ok = bool(checks["consistency"].get("ok"))
    routing_ok = bool(checks["routing"].get("ok"))
    bridge_continuity = checks.get(
        "bridge_continuity",
        {
            "ok": True,
            "issue_count": 0,
            "shared_session_count": 0,
            "ack_delayed_count": 0,
            "awaiting_report_count": 0,
            "response_delayed_count": 0,
            "progress_stalled_count": 0,
            "issues": [],
        },
    )
    bridge_continuity_ok = bool(bridge_continuity.get("ok", True))
    scheduler = checks["official_scheduler"]
    scheduler_ok = bool(scheduler.get("configured")) and bool(scheduler.get("active"))
    scheduler_verified = scheduler_ok and (
        int(scheduler.get("verified_run_count", 0)) > 0
        or checks["run_context"].get("scheduler_id") == OFFICIAL_SCHEDULER_ID
        or checks["run_context"].get("trigger_source") == "launchd"
    )
    codex_automation = checks.get("codex_automation", {})
    codex_automation_ok = not bool(codex_automation.get("active"))

    if not watcher_ok:
        alerts.append(
            build_alert(
                alert_key="health.watcher.launchagent",
                category="watcher",
                severity="critical",
                summary="watcher 未安装或未加载",
                requires_manager_attention=True,
            )
        )
    if not dashboard_ok:
        alerts.append(
            build_alert(
                alert_key="health.dashboard-sync.launchagent",
                category="dashboard_sync",
                severity="critical",
                summary="dashboard sync 未安装或未加载",
                requires_manager_attention=True,
            )
        )
    if not refresh_index_ok or not rebuild_ok:
        refresh_detail_parts: list[str] = []
        if not refresh_index_ok:
            refresh_detail_parts.append(
                f"refresh-index failed: {checks.get('refresh_index', {}).get('stderr') or checks.get('refresh_index', {}).get('exit_code')}"
            )
        if not rebuild_ok:
            refresh_detail_parts.append(
                f"rebuild-all failed: {checks.get('rebuild_all', {}).get('stderr') or checks.get('rebuild_all', {}).get('exit_code')}"
            )
        alerts.append(
            build_alert(
                alert_key="health.dashboard.refresh-chain",
                category="dashboard_refresh",
                severity="critical",
                summary="自动刷新链路未完成：" + "; ".join(refresh_detail_parts),
                requires_manager_attention=True,
            )
        )
    if not consistency_ok:
        detail = "; ".join(checks["consistency"].get("issues", []) or ["verify-consistency 未通过"])
        alerts.append(
            build_alert(
                alert_key="health.dashboard.consistency",
                category="consistency",
                severity="critical",
                summary=f"verify-consistency 未通过：{detail}",
                requires_manager_attention=True,
            )
        )
    if not routing_ok:
        route_issues: list[str] = []
        for item in checks["routing"].get("results", []):
            for issue in item.get("issues", []):
                route_issues.append(f"{item['name']}: {issue}")
        detail = "; ".join(route_issues) or "route-check 未通过"
        alerts.append(
            build_alert(
                alert_key="health.routing.binding",
                category="routing",
                severity="critical",
                summary=f"route-check 未通过：{detail}",
                requires_manager_attention=True,
            )
        )
    if not bridge_continuity_ok:
        continuity_parts: list[str] = []
        if int(bridge_continuity.get("shared_session_count", 0) or 0) > 0:
            continuity_parts.append(f"shared_session={bridge_continuity.get('shared_session_count', 0)}")
        if int(bridge_continuity.get("response_delayed_count", 0) or 0) > 0:
            continuity_parts.append(f"response_delayed={bridge_continuity.get('response_delayed_count', 0)}")
        if int(bridge_continuity.get("progress_stalled_count", 0) or 0) > 0:
            continuity_parts.append(f"progress_stalled={bridge_continuity.get('progress_stalled_count', 0)}")
        detail = ", ".join(continuity_parts) or f"issue_count={bridge_continuity.get('issue_count', 0)}"
        alerts.append(
            build_alert(
                alert_key="health.bridge.continuity",
                category="bridge_continuity",
                severity="warning",
                summary=f"Feishu continuity 存在异常：{detail}",
                requires_manager_attention=False,
            )
        )
    if not scheduler_ok:
        scheduler_detail = "health LaunchAgent 缺失、未加载或未完成本地定时调度绑定"
        alerts.append(
            build_alert(
                alert_key="health.scheduler.official",
                category="scheduler",
                severity="critical",
                summary=scheduler_detail,
                requires_manager_attention=True,
            )
        )
    elif not scheduler_verified:
        alerts.append(
            build_alert(
                alert_key="health.scheduler.first-run",
                category="scheduler",
                severity="warning",
                summary="官方 health LaunchAgent 已存在，但尚未完成首轮真实定时运行验收",
                requires_manager_attention=False,
            )
        )
    if not codex_automation_ok:
        alerts.append(
            build_alert(
                alert_key="health.scheduler.codex-automation-conflict",
                category="scheduler",
                severity="critical",
                summary="Codex automation `workspace-health` 仍处于启用态，存在双重调度风险",
                requires_manager_attention=True,
            )
        )

    rows = [
        build_row(
            row_id="WH-HC-01",
            module="守护进程",
            task="watcher launchagent 保持 installed + loaded",
            ok=watcher_ok,
            next_ok="继续由自动化巡检观察 watcher 状态",
            next_fail="检查 watcher launchagent 是否被卸载、bootout 或异常退出",
            dependency="" if watcher_ok else "watcher 未安装或未加载",
            parent_id="WH-OPS-02",
        ),
        build_row(
            row_id="WH-HC-02",
            module="守护进程",
            task="dashboard sync launchagent 保持 installed + loaded",
            ok=dashboard_ok,
            next_ok="继续由自动化巡检观察 dashboard sync 状态",
            next_fail="检查 dashboard sync launchagent 是否被卸载、bootout 或异常退出",
            dependency="" if dashboard_ok else "dashboard sync 未安装或未加载",
            parent_id="WH-OPS-02",
        ),
        build_row(
            row_id="WH-HC-03",
            module="一致性",
            task="`verify-consistency` 持续通过",
            ok=consistency_ok,
            next_ok="继续按自动巡检确认总板与事实源一致",
            next_fail="先修复项目板、总板或 dashboard 的一致性问题",
            dependency="" if consistency_ok else "存在一致性错误",
            parent_id="WH-OPS-02",
        ),
        build_row(
            row_id="WH-HC-04",
            module="路由",
            task="app 直开协议与 `start-codex --dry-run` 的绑定逻辑保持一致",
            ok=routing_ok,
            next_ok="后续将路由验证纳入自动巡检",
            next_fail="检查 board binding 解析或 `start-codex` 输出是否漂移",
            dependency="" if routing_ok else "存在路由绑定不一致",
            parent_id="WH-OPS-01",
        ),
        build_row(
            row_id="WH-HC-07",
            module="连续性",
            task="Feishu chat binding 不出现 shared session / ack drift / report drift",
            ok=bridge_continuity_ok,
            next_ok="继续由自动巡检观察 Feishu continuity 和 chat binding 状态",
            next_fail="检查 chat binding 与 session 复用是否漂移，并修复 ack/report 卡滞线程",
            dependency=(
                ""
                if bridge_continuity_ok
                else (
                    f"shared_session={bridge_continuity.get('shared_session_count', 0)}, "
                    f"response_delayed={bridge_continuity.get('response_delayed_count', 0)}, "
                    f"progress_stalled={bridge_continuity.get('progress_stalled_count', 0)}"
                )
            ),
            parent_id="WH-OPS-01",
            status_when_fail="doing",
        ),
        build_row(
            row_id="WH-HC-05",
            module="调度",
            task="`workspace_hub_health_check.py` LaunchAgent 作为官方调度入口并完成真实定时运行验收",
            ok=scheduler_verified,
            next_ok="继续由 health LaunchAgent 定时巡检，并保留 watcher 的 wake catch-up 作为休眠补偿入口",
            next_fail=(
                "安装并加载 health LaunchAgent，确保本地定时巡检可用"
                if not scheduler_ok
                else "等待下一次 LaunchAgent 定时窗口；若因休眠错过，则由 watcher 触发 wake catch-up 完成补跑验收"
            ),
            dependency=(
                ""
                if scheduler_verified
                else (
                    "health LaunchAgent 缺失或未加载"
                    if not scheduler_ok
                    else "尚无真实定时运行记录"
                )
            ),
            parent_id="WH-OPS-02",
            status_when_fail="blocked" if not scheduler_ok else "doing",
        ),
        build_row(
            row_id="WH-HC-06",
            module="调度",
            task="`workspace-health` Codex automation 保持停用，避免双重调度",
            ok=codex_automation_ok,
            next_ok="继续让 Codex automation 保持停用，仅由本地 LaunchAgent 承担生产巡检",
            next_fail="停用 Codex automation `workspace-health`，避免与 LaunchAgent 双重调度",
            dependency="" if codex_automation_ok else "检测到 Codex automation 仍处于 ACTIVE",
            parent_id="WH-OPS-02",
        ),
    ]
    issues = [item["current_summary"] for item in alerts]
    return {
        "checked_at": checks["checked_at"],
        "ok": not alerts,
        "issues": issues,
        "alerts": alerts,
        "checks": checks,
        "rows": rows,
    }


def build_row(
    *,
    row_id: str,
    module: str,
    task: str,
    ok: bool,
    next_ok: str,
    next_fail: str,
    dependency: str,
    parent_id: str,
    status_when_fail: str = "blocked",
) -> dict[str, str]:
    now = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    return {
        "ID": row_id,
        "模块": module,
        "事项": task,
        "状态": "done" if ok else status_when_fail,
        "下一步": next_ok if ok else next_fail,
        "更新时间": now,
        "阻塞/依赖": dependency,
        "上卷ID": parent_id,
    }


def ensure_health_topic_board() -> Path:
    path = health_topic_board_path()
    if path.exists():
        return path
    project_board = codex_memory.ensure_project_board(PROJECT_NAME)
    frontmatter = codex_memory.render_frontmatter(
        {
            "board_type": "topic",
            "project_name": PROJECT_NAME,
            "topic_name": "运维巡检",
            "topic_key": "ops-health",
            "rollup_target": str(project_board),
            "updated_at": dt.date.today().isoformat(),
            "purpose": f"作为 {PROJECT_NAME} 的运维巡检专题板，自动维护健康检查日志对应的结构化告警事实。",
        }
    )
    body = (
        f"{frontmatter}\n\n"
        f"# {PROJECT_NAME}｜运维巡检跟进板\n\n"
        "## 使用说明\n\n"
        f"- 本页是 `{PROJECT_NAME}` 运维巡检的专题执行事实源。\n"
        "- 健康巡检脚本会自动更新本页状态，并回卷到一级项目板和总板。\n"
        f"- 巡检日志保存在 `{health_reports_root()}/`。\n\n"
        "## 任务主表\n\n"
        f"{codex_memory.AUTO_TASK_TABLE_MARKERS[0]}\n"
        + "\n".join(codex_memory.markdown_table_lines(codex_memory.TOPIC_BOARD_HEADERS, []))
        + f"\n{codex_memory.AUTO_TASK_TABLE_MARKERS[1]}\n\n"
        "## 人工补充\n\n"
        "- 当前无\n"
    )
    codex_memory.write_text(path, body)
    return path


def sync_health_topic_board(result: dict[str, Any], *, trigger_followup_syncs: bool = True) -> list[str]:
    topic_path = ensure_health_topic_board()
    topic_board = codex_memory.load_topic_board(topic_path)
    codex_memory.save_topic_board(topic_path, topic_board["frontmatter"], topic_board["body"], result["rows"])
    project_path = codex_memory.refresh_project_rollups(PROJECT_NAME, topic_path=topic_path)
    codex_memory.refresh_active_projects(codex_memory.load_registry())
    codex_memory.refresh_next_actions_rollup()
    if trigger_followup_syncs:
        codex_memory.trigger_retrieval_sync_once()
        codex_memory.trigger_dashboard_sync_once()
    return [str(topic_path), str(project_path), str(codex_memory.NEXT_ACTIONS_MD), *dashboard_paths()]


def compute_script_version() -> str:
    try:
        commit = subprocess.run(
            ["git", "-C", str(code_root()), "rev-parse", "--short", "HEAD"],
            text=True,
            capture_output=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    dirty = subprocess.run(
        ["git", "-C", str(code_root()), "status", "--short", "--untracked-files=no"],
        text=True,
        capture_output=True,
        check=False,
    ).stdout.strip()
    return f"git:{commit}{'-dirty' if dirty else ''}"


def load_latest_alert_states() -> dict[str, dict[str, Any]]:
    states: dict[str, dict[str, Any]] = {}
    path = alerts_path()
    if not path.exists():
        return states
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        alert_key = str(payload.get("alert_key", "")).strip()
        if alert_key:
            states[alert_key] = payload
    return states


def append_ndjson(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def execution_summary(*, ok: bool, issue_count: int) -> str:
    if ok:
        return f"`{HEALTH_AUTOMATION_ID}` execution passed with {issue_count} issues"
    return f"`{HEALTH_AUTOMATION_ID}` execution detected {issue_count} issues"


def build_health_execution_outcome(
    *,
    ok: bool,
    issue_count: int,
    alert_count: int,
    rows: list[dict[str, str]],
    trigger_source: str,
) -> workspace_job_schema.JobExecutionOutcome:
    return workspace_job_schema.JobExecutionOutcome(
        status="ok" if ok else "error",
        summary=execution_summary(ok=ok, issue_count=issue_count),
        issue_count=issue_count,
        alert_count=alert_count,
        metadata={
            "trigger_source": trigger_source,
            "row_count": len(rows),
            "row_ids": [row["ID"] for row in rows],
        },
    )


def build_delivery_outcome(
    *,
    delivery_id: str,
    status: str,
    requested: bool = True,
    summary: str = "",
    error: str = "",
    targets: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> workspace_job_schema.JobDeliveryOutcome:
    return workspace_job_schema.JobDeliveryOutcome(
        delivery_id=delivery_id,
        status=status,
        requested=requested,
        summary=summary,
        error=error,
        targets=list(targets or []),
        metadata=dict(metadata or {}),
    )


def update_alert_ledger(
    result: dict[str, Any],
    *,
    run_id: str,
    checked_at: str,
    changed_targets: list[str],
) -> dict[str, Any]:
    alerts_path().parent.mkdir(parents=True, exist_ok=True)
    alerts_path().touch(exist_ok=True)
    previous = load_latest_alert_states()
    current = {item["alert_key"]: item for item in result["alerts"]}
    updates: list[dict[str, Any]] = []
    latest_states = dict(previous)
    open_states: list[dict[str, Any]] = []
    pending_states: list[dict[str, Any]] = []
    resolved_states: list[dict[str, Any]] = []

    for alert_key, alert in current.items():
        previous_state = previous.get(alert_key, {})
        update = {
            "alert_key": alert_key,
            "category": alert["category"],
            "severity": escalate_severity(str(previous_state.get("severity", "")), alert["severity"]),
            "status": "open",
            "first_seen_at": previous_state.get("first_seen_at", checked_at),
            "last_seen_at": checked_at,
            "occurrence_count": int(previous_state.get("occurrence_count", 0) or 0) + 1,
            "current_summary": alert["current_summary"],
            "affected_targets": alert["affected_targets"],
            "requires_manager_attention": bool(alert["requires_manager_attention"]),
            "related_board_paths": changed_targets or alert["related_board_paths"],
            "last_run_id": run_id,
            "uproll": bool(alert.get("uproll", False)),
            "uproll_reasons": list(alert.get("uproll_reasons", [])),
            "status_updated_at": checked_at,
            "confirmation_passes": 0,
        }
        updates.append(update)
        latest_states[alert_key] = update
        open_states.append(update)

    for alert_key, previous_state in previous.items():
        if alert_key in current:
            continue
        previous_status = str(previous_state.get("status", ""))
        if previous_status not in {"open", "resolved_pending_confirmation"}:
            continue
        confirmation_passes = int(previous_state.get("confirmation_passes", 0) or 0) + 1
        status = "resolved" if confirmation_passes >= ALERT_CONFIRMATION_PASSES else "resolved_pending_confirmation"
        update = {
            **previous_state,
            "status": status,
            "last_run_id": run_id,
            "status_updated_at": checked_at,
            "confirmation_passes": confirmation_passes,
        }
        if status == "resolved_pending_confirmation":
            update["current_summary"] = f"待确认关闭：{previous_state.get('current_summary', '')}"
            pending_states.append(update)
        else:
            update["current_summary"] = f"已关闭：{previous_state.get('current_summary', '')}"
            resolved_states.append(update)
        updates.append(update)
        latest_states[alert_key] = update

    for item in updates:
        append_ndjson(alerts_path(), item)

    return {
        "updates": updates,
        "open_alerts": open_states,
        "pending_alerts": pending_states,
        "resolved_alerts": resolved_states,
        "latest_states": latest_states,
    }


def render_health_report(
    result: dict[str, Any],
    *,
    run_record: dict[str, Any],
    alert_summary: dict[str, Any],
    execution_outcome: dict[str, Any],
    delivery_outcomes: list[dict[str, Any]],
) -> str:
    checks = result["checks"]
    catchup = checks.get("catchup_status", {})
    lines = [
        f"# {PROJECT_NAME} 健康巡检日志",
        "",
        f"- run_id：`{run_record['run_id']}`",
        f"- 时间：{result['checked_at']}",
        f"- trigger_source：`{run_record['trigger_source']}`",
        f"- scheduled_for：`{run_record['scheduled_for']}`",
        f"- started_at：`{run_record['started_at']}`",
        f"- finished_at：`{run_record['finished_at']}`",
        f"- script_version：`{run_record['script_version']}`",
        f"- execution_status：`{execution_outcome['status']}`",
        f"- delivery_status：`{run_record['delivery_status']}`",
        f"- overall_ok：`{run_record['ok']}`",
        "",
        "## 执行结果",
        "",
        f"- summary：{execution_outcome.get('summary', '')}",
        f"- issue_count：`{execution_outcome.get('issue_count', 0)}`",
        f"- alert_count：`{execution_outcome.get('alert_count', 0)}`",
        "",
        "## 检查项",
        "",
        f"- watcher：installed=`{checks['watcher'].get('installed')}` loaded=`{checks['watcher'].get('loaded')}`",
        f"- dashboard sync：installed=`{checks['dashboard_sync'].get('installed')}` loaded=`{checks['dashboard_sync'].get('loaded')}` pending=`{checks['dashboard_sync'].get('pending_events')}`",
        f"- verify-consistency（pre-refresh）：ok=`{checks.get('consistency_pre_refresh', {}).get('ok')}` issue_count=`{checks.get('consistency_pre_refresh', {}).get('issue_count', 0)}`",
        f"- refresh-index：changed=`{checks.get('refresh_index', {}).get('changed')}` exit_code=`{checks.get('refresh_index', {}).get('exit_code', 0)}`",
        f"- rebuild-all：status=`{checks.get('rebuild_all', {}).get('status', '')}` exit_code=`{checks.get('rebuild_all', {}).get('exit_code', 0)}`",
        f"- verify-consistency（post-refresh）：ok=`{checks['consistency'].get('ok')}` issue_count=`{checks['consistency'].get('issue_count', 0)}`",
        f"- route-check：ok=`{checks['routing'].get('ok')}` case_count=`{checks['routing'].get('case_count', 0)}`",
        (
            "- bridge continuity："
            f"ok=`{checks.get('bridge_continuity', {}).get('ok', True)}` "
            f"issue_count=`{checks.get('bridge_continuity', {}).get('issue_count', 0)}` "
            f"shared_session=`{checks.get('bridge_continuity', {}).get('shared_session_count', 0)}` "
            f"response_delayed=`{checks.get('bridge_continuity', {}).get('response_delayed_count', 0)}` "
            f"progress_stalled=`{checks.get('bridge_continuity', {}).get('progress_stalled_count', 0)}`"
        ),
        f"- official scheduler：type=`{checks['official_scheduler'].get('type')}` configured=`{checks['official_scheduler'].get('configured')}` active=`{checks['official_scheduler'].get('active')}` run_count=`{checks['official_scheduler'].get('run_count')}` direct_run_count=`{checks['official_scheduler'].get('direct_run_count')}` verified_run_count=`{checks['official_scheduler'].get('verified_run_count')}` last_run_at=`{checks['official_scheduler'].get('last_run_at')}` next_run_at=`{checks['official_scheduler'].get('next_run_at')}`",
        f"- Codex automation：configured=`{checks.get('codex_automation', {}).get('configured')}` active=`{checks.get('codex_automation', {}).get('active')}` runtime_status=`{checks.get('codex_automation', {}).get('runtime_status')}`",
        f"- wake catch-up：should_run=`{catchup.get('should_run')}` reason=`{catchup.get('reason')}` due_at=`{catchup.get('due_at')}` overdue_seconds=`{catchup.get('overdue_seconds')}`",
        "",
    ]
    if alert_summary["open_alerts"]:
        lines.extend(["## 告警", ""])
        for alert in alert_summary["open_alerts"]:
            lines.append(
                f"- `{alert['severity']}` `{alert['alert_key']}` {alert['current_summary']} | 上卷：`{alert.get('uproll', False)}` {','.join(alert.get('uproll_reasons', []))}"
            )
        lines.append("")
    else:
        lines.extend(["## 告警", "", "- 无", ""])
    if alert_summary["pending_alerts"]:
        lines.extend(["## 待确认关闭", ""])
        for alert in alert_summary["pending_alerts"]:
            lines.append(
                f"- `{alert['alert_key']}` confirmation_passes=`{alert.get('confirmation_passes', 0)}` {alert['current_summary']}"
            )
        lines.append("")
    lines.extend(["## Delivery", ""])
    for item in delivery_outcomes:
        suffix = f" | error={item['error']}" if item.get("error") else ""
        lines.append(
            f"- `{item['delivery_id']}` requested=`{item.get('requested', True)}` status=`{item.get('status', '')}` {item.get('summary', '')}{suffix}"
        )
    lines.append("")
    lines.extend(["## 板面回写", ""])
    for path in run_record["writeback_targets"]:
        lines.append(f"- {path}")
    lines.extend(["", "## 结构化状态", ""])
    for row in result["rows"]:
        lines.append(f"- `{row['ID']}` `{row['状态']}` {row['事项']} | 下一步：{row['下一步']}")
    lines.append("")
    return "\n".join(lines)


def write_health_logs(
    result: dict[str, Any],
    *,
    run_record: dict[str, Any],
    alert_summary: dict[str, Any],
    execution_outcome: dict[str, Any],
    delivery_outcomes: list[dict[str, Any]],
) -> dict[str, str]:
    root = health_reports_root()
    root.mkdir(parents=True, exist_ok=True)
    archive_path = Path(run_record["report_path"])
    latest_path = latest_report_path()
    report_text = render_health_report(
        result,
        run_record=run_record,
        alert_summary=alert_summary,
        execution_outcome=execution_outcome,
        delivery_outcomes=delivery_outcomes,
    )
    archive_path.write_text(report_text, encoding="utf-8")
    latest_path.write_text(report_text, encoding="utf-8")
    return {"archive_path": str(archive_path), "latest_path": str(latest_path)}


def write_run_ledger(run_record: dict[str, Any]) -> None:
    append_ndjson(history_path(), run_record)


def run_health_check(
    *,
    checks: dict[str, Any] | None = None,
    trigger_source: str = "",
    scheduled_for: str = "",
    automation_run_id: str = "",
    scheduler_id: str = "",
    ) -> dict[str, Any]:
    pause_payload = active_health_pause()
    if pause_payload.get("active"):
        return {
            "ok": True,
            "skipped": True,
            "reason": "project_paused",
            "pause": pause_payload,
            "changed_targets": [],
            "log_paths": {},
            "run_record": {},
            "alert_summary": {},
            "issues": [],
            "alerts": [],
            "checked_at": iso_now_local(),
        }
    run_context = resolve_run_context(
        trigger_source=trigger_source,
        scheduled_for=scheduled_for,
        automation_run_id=automation_run_id,
        scheduler_id=scheduler_id,
    )
    collected = checks or collect_checks(run_context)
    result = evaluate_checks(collected)
    execution_outcome = build_health_execution_outcome(
        ok=result["ok"],
        issue_count=len(result["issues"]),
        alert_count=len(result["alerts"]),
        rows=result["rows"],
        trigger_source=run_context["trigger_source"],
    )
    overall_ok = bool(result["ok"])
    changed_targets: list[str] = []
    writeback_error = ""
    delivery_outcomes: list[workspace_job_schema.JobDeliveryOutcome] = []

    with codex_memory.workspace_lock():
        try:
            changed_targets = sync_health_topic_board(result, trigger_followup_syncs=False)
            delivery_outcomes.append(
                build_delivery_outcome(
                    delivery_id="board-writeback",
                    status="delivered",
                    summary=f"wrote {len(changed_targets)} board targets",
                    targets=changed_targets,
                    metadata={"target_count": len(changed_targets)},
                )
            )
        except OSError as exc:
            writeback_error = f"{exc.__class__.__name__}: {exc}"
            overall_ok = False
            result["ok"] = False
            result["issues"] = [*result["issues"], f"真实 Vault 写回失败：{writeback_error}"]
            result["alerts"] = [
                *result["alerts"],
                build_alert(
                    alert_key="health.vault.writeback",
                    category="vault_writeback",
                    severity="critical",
                    summary=f"真实 Vault 写回失败：{writeback_error}",
                    requires_manager_attention=True,
                ),
            ]
            delivery_outcomes.append(
                build_delivery_outcome(
                    delivery_id="board-writeback",
                    status="not-delivered",
                    summary="health board writeback failed",
                    error=writeback_error,
                    targets=base_related_board_paths(),
                )
            )

        alert_summary = update_alert_ledger(
            result,
            run_id=run_context["run_id"],
            checked_at=result["checked_at"],
            changed_targets=changed_targets,
        )
        delivery_outcomes.append(
            build_delivery_outcome(
                delivery_id="alert-ledger",
                status="delivered",
                summary=f"updated {len(alert_summary['updates'])} alert ledger rows",
                targets=[str(alerts_path())],
                metadata={"update_count": len(alert_summary["updates"])},
            )
        )

    retrieval_error = ""
    try:
        codex_memory.trigger_retrieval_sync_once()
        delivery_outcomes.append(
            build_delivery_outcome(
                delivery_id="retrieval-sync",
                status="delivered",
                summary="requested retrieval sync",
            )
        )
    except Exception as exc:
        retrieval_error = f"{exc.__class__.__name__}: {exc}"
        delivery_outcomes.append(
            build_delivery_outcome(
                delivery_id="retrieval-sync",
                status="not-delivered",
                summary="retrieval sync request failed",
                error=retrieval_error,
            )
        )

    rebuild_result = trigger_dashboard_rebuild()
    if rebuild_result is None or rebuild_result.returncode == 0:
        delivery_outcomes.append(
            build_delivery_outcome(
                delivery_id="dashboard-sync",
                status="delivered",
                summary="dashboard rebuild completed",
                targets=dashboard_paths(),
                metadata={"exit_code": 0 if rebuild_result is None else rebuild_result.returncode},
            )
        )
    else:
        overall_ok = False
        delivery_outcomes.append(
            build_delivery_outcome(
                delivery_id="dashboard-sync",
                status="not-delivered",
                summary="dashboard rebuild failed",
                error=rebuild_result.stderr.strip() or rebuild_result.stdout.strip() or f"exit_code={rebuild_result.returncode}",
                targets=dashboard_paths(),
                metadata={"exit_code": rebuild_result.returncode},
            )
        )

    delivery_outcomes.append(
        build_delivery_outcome(
            delivery_id="feishu-notify",
            status="not-requested",
            requested=False,
            summary="workspace-health does not notify Feishu by default",
        )
    )

    finished_at = iso_now_local()
    log_paths = {
        "archive_path": str(archive_report_path()),
        "latest_path": str(latest_report_path()),
    }
    run_record = workspace_job_schema.build_run_ledger_entry(
        job_id=HEALTH_AUTOMATION_ID,
        run_id=run_context["run_id"],
        started_at=run_context["started_at"],
        finished_at=finished_at,
        trigger_source=run_context["trigger_source"],
        scheduled_for=run_context["scheduled_for"],
        automation_run_id=run_context["automation_run_id"],
        scheduler_id=run_context["scheduler_id"],
        script_version=compute_script_version(),
        report_path=log_paths["archive_path"],
        latest_report_path=log_paths["latest_path"],
        writeback_targets=changed_targets,
        execution_outcome=execution_outcome,
        delivery_outcomes=delivery_outcomes,
        overall_ok=overall_ok,
        artifacts={
            "run_ledger_path": str(history_path()),
            "alert_ledger_path": str(alerts_path()),
            "report_path": log_paths["archive_path"],
            "latest_report_path": log_paths["latest_path"],
        },
        metadata={
            "writeback_error": writeback_error,
            "retrieval_error": retrieval_error,
        },
    )

    with codex_memory.workspace_lock():
        log_paths = write_health_logs(
            result,
            run_record=run_record,
            alert_summary=alert_summary,
            execution_outcome=run_record["execution_outcome"],
            delivery_outcomes=run_record["delivery_outcomes"],
        )
        run_record["report_path"] = log_paths["archive_path"]
        run_record["latest_report_path"] = log_paths["latest_path"]
        run_record["artifacts"]["report_path"] = log_paths["archive_path"]
        run_record["artifacts"]["latest_report_path"] = log_paths["latest_path"]
        write_run_ledger(run_record)
    return {
        **result,
        "ok": overall_ok,
        "execution_ok": run_record["execution_outcome"]["status"] == "ok",
        "delivery_status": run_record["delivery_status"],
        "delivery_outcomes": run_record["delivery_outcomes"],
        "changed_targets": changed_targets,
        "log_paths": log_paths,
        "run_record": run_record,
        "execution_outcome": run_record["execution_outcome"],
        "alert_summary": alert_summary,
    }


def run_catchup_if_stale(
    *,
    now: dt.datetime | None = None,
    interval_seconds: int | None = None,
    grace_seconds: int | None = None,
) -> dict[str, Any]:
    pause_payload = active_health_pause()
    if pause_payload.get("active"):
        return {
            "executed": False,
            "decision": {
                "should_run": False,
                "reason": "project_paused",
                "scheduled_for": "",
                "due_at": "",
                "overdue_seconds": 0,
            },
            "pause": pause_payload,
        }
    decision = compute_catchup_status(
        now=now,
        interval_seconds=interval_seconds,
        grace_seconds=grace_seconds,
    )
    if not decision["should_run"]:
        return {
            "executed": False,
            "decision": decision,
        }
    request = request_health_wake(
        reason="wake_catchup",
        trigger_source="wake_catchup",
        scheduled_for=decision["scheduled_for"],
        scheduler_id=OFFICIAL_SCHEDULER_ID,
    )
    execution = run_requested_health_wake()
    return {
        "executed": bool(execution.get("executed")),
        "decision": decision,
        "request": request,
        **(
            {"payload": execution.get("payload", {}), "wake": execution.get("wake", {})}
            if execution.get("executed")
            else {"reason": execution.get("reason", "")}
        ),
    }


def run_launchctl(*parts: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["launchctl", *parts],
        text=True,
        capture_output=True,
        check=False,
    )


def plist_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def plist_value(value: Any, indent: str = "    ") -> str:
    if isinstance(value, bool):
        return f"{indent}<{str(value).lower()}/>"
    if isinstance(value, str):
        return f"{indent}<string>{plist_escape(value)}</string>"
    if isinstance(value, list):
        lines = [f"{indent}<array>"]
        for item in value:
            lines.append(plist_value(item, indent + "  "))
        lines.append(f"{indent}</array>")
        return "\n".join(lines)
    if isinstance(value, dict):
        lines = [f"{indent}<dict>"]
        for key, item in value.items():
            lines.append(f"{indent}  <key>{plist_escape(str(key))}</key>")
            lines.append(plist_value(item, indent + "  "))
        lines.append(f"{indent}</dict>")
        return "\n".join(lines)
    return f"{indent}<string>{plist_escape(str(value))}</string>"


def plist_dumps(payload: dict[str, Any]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">',
        "  <dict>",
    ]
    for key, value in payload.items():
        lines.append(f"    <key>{plist_escape(str(key))}</key>")
        lines.append(plist_value(value, "    "))
    lines.extend(["  </dict>", "</plist>"])
    return "\n".join(lines) + "\n"


def launch_agent_payload(interval: int) -> dict[str, Any]:
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    return {
        "Label": HEALTH_AGENT_NAME,
        "ProgramArguments": [
            python_path,
            str(code_root() / "ops" / "workspace_hub_health_check.py"),
            "wake-now",
            "--reason",
            "interval",
            "--trigger-source",
            "launchd",
            "--scheduler-id",
            OFFICIAL_SCHEDULER_ID,
        ],
        "RunAtLoad": True,
        "StartInterval": int(interval),
        "WorkingDirectory": str(workspace_root()),
        "StandardOutPath": str(log_stdout_path()),
        "StandardErrorPath": str(log_stderr_path()),
        "EnvironmentVariables": {
            "PYTHONUNBUFFERED": "1",
            "WORKSPACE_HUB_CODE_ROOT": str(code_root()),
            "WORKSPACE_HUB_ROOT": str(expected_workspace_root()),
            "WORKSPACE_HUB_VAULT_ROOT": str(vault_root()),
            "WORKSPACE_HUB_REPORTS_ROOT": str(reports_root()),
        },
    }


def cmd_run_once(_args: argparse.Namespace) -> int:
    payload = run_health_check(
        trigger_source=_args.trigger_source,
        scheduled_for=_args.scheduled_for,
        automation_run_id=_args.automation_run_id,
        scheduler_id=_args.scheduler_id,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


def cmd_wake_now(args: argparse.Namespace) -> int:
    request = request_health_wake(
        reason=args.reason,
        trigger_source=args.trigger_source or args.reason,
        scheduled_for=args.scheduled_for,
        automation_run_id=args.automation_run_id,
        scheduler_id=args.scheduler_id,
    )
    execution = run_requested_health_wake()
    payload = {
        "requested": request,
        **execution,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not execution.get("executed"):
        return 0
    return 0 if execution.get("payload", {}).get("ok") else 1


def cmd_catch_up_if_stale(args: argparse.Namespace) -> int:
    payload = run_catchup_if_stale(
        interval_seconds=args.interval_seconds,
        grace_seconds=args.grace_seconds,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["executed"]:
        return 0
    return 0 if payload["payload"]["ok"] else 1


def cmd_status(_args: argparse.Namespace) -> int:
    payload = load_status_payload()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_status_cache_identity() -> dict[str, Any]:
    latest = latest_report_path()
    history = history_path()
    alerts = alerts_path()
    official_scheduler = load_official_scheduler_status()
    codex_automation = load_codex_automation_status()
    wake_status = workspace_wake_broker.job_status(HEALTH_AUTOMATION_ID)
    pause = active_health_pause()
    return {
        "installed": launch_agent_plist_path().exists(),
        "loaded": codex_memory.launch_agent_loaded(HEALTH_AGENT_NAME),
        "latest_report_mtime": latest.stat().st_mtime if latest.exists() else 0,
        "history_mtime": history.stat().st_mtime if history.exists() else 0,
        "alerts_mtime": alerts.stat().st_mtime if alerts.exists() else 0,
        "official_scheduler": official_scheduler,
        "codex_automation": codex_automation,
        "wake_broker": wake_status,
        "pause": pause,
    }


def build_status_payload() -> dict[str, Any]:
    latest = latest_report_path()
    history = history_path()
    last_entry: dict[str, Any] = {}
    if history.exists():
        lines = history.read_text(encoding="utf-8").splitlines()
        if lines:
            last_entry = json.loads(lines[-1])
    latest_alerts = load_latest_alert_states()
    open_alerts = [item for item in latest_alerts.values() if item.get("status") == "open"]
    official_scheduler = load_official_scheduler_status()
    codex_automation = load_codex_automation_status()
    return {
        "installed": launch_agent_plist_path().exists(),
        "loaded": codex_memory.launch_agent_loaded(HEALTH_AGENT_NAME),
        "plist": str(launch_agent_plist_path()),
        "latest_report": str(latest) if latest.exists() else "",
        "history_path": str(history),
        "alerts_path": str(alerts_path()),
        "last_entry": last_entry,
        "open_alert_count": len(open_alerts),
        "official_scheduler": official_scheduler,
        "codex_automation": codex_automation,
        "catchup_status": compute_catchup_status(scheduler_status=official_scheduler),
        "wake_broker": workspace_wake_broker.job_status(HEALTH_AUTOMATION_ID),
        "pause": active_health_pause(),
    }


def load_status_payload() -> dict[str, Any]:
    identity = build_status_cache_identity()
    cached = result_cache.recall(STATUS_CACHE_NAMESPACE, identity)
    if cached and isinstance(cached.get("value"), dict):
        payload = dict(cached["value"])
        payload["cache"] = {
            "hit": True,
            "namespace": STATUS_CACHE_NAMESPACE,
            "key": str(cached.get("key", "")).strip(),
        }
        return payload
    payload = build_status_payload()
    cache_entry = result_cache.remember(
        STATUS_CACHE_NAMESPACE,
        identity,
        value=payload,
        metadata={"open_alert_count": int(payload.get("open_alert_count", 0) or 0)},
    )
    payload["cache"] = {
        "hit": False,
        "namespace": STATUS_CACHE_NAMESPACE,
        "key": str(cache_entry.get("key", "")).strip(),
    }
    return payload


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    launch_agent_plist_path().parent.mkdir(parents=True, exist_ok=True)
    log_stdout_path().parent.mkdir(parents=True, exist_ok=True)
    launch_agent_plist_path().write_text(plist_dumps(launch_agent_payload(args.interval)), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(launch_agent_plist_path()))
    bootstrap = run_launchctl("bootstrap", domain, str(launch_agent_plist_path()))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=os.sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{HEALTH_AGENT_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=os.sys.stderr)
        return kickstart.returncode
    print(
        json.dumps(
            {
                "installed": True,
                "loaded": True,
                "plist": str(launch_agent_plist_path()),
                "interval": int(args.interval),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(launch_agent_plist_path()))
    if launch_agent_plist_path().exists():
        launch_agent_plist_path().unlink()
    print(json.dumps({"installed": False, "plist": str(launch_agent_plist_path())}, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Run and automate {PROJECT_NAME} health checks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_once = subparsers.add_parser("run-once")
    run_once.add_argument("--trigger-source", default="")
    run_once.add_argument("--scheduled-for", default="")
    run_once.add_argument("--automation-run-id", default="")
    run_once.add_argument("--scheduler-id", default="")
    run_once.set_defaults(func=cmd_run_once)

    wake_now = subparsers.add_parser("wake-now")
    wake_now.add_argument("--reason", default="manual_wake")
    wake_now.add_argument("--trigger-source", default="")
    wake_now.add_argument("--scheduled-for", default="")
    wake_now.add_argument("--automation-run-id", default="")
    wake_now.add_argument("--scheduler-id", default="")
    wake_now.set_defaults(func=cmd_wake_now)

    catch_up = subparsers.add_parser("catch-up-if-stale")
    catch_up.add_argument("--interval-seconds", type=int, default=default_health_interval_seconds())
    catch_up.add_argument("--grace-seconds", type=int, default=default_catchup_grace_seconds())
    catch_up.set_defaults(func=cmd_catch_up_if_stale)

    status = subparsers.add_parser("status")
    status.set_defaults(func=cmd_status)

    install = subparsers.add_parser("install-launchagent")
    install.add_argument("--interval", type=int, default=default_health_interval_seconds())
    install.set_defaults(func=cmd_install_launchagent)

    uninstall = subparsers.add_parser("uninstall-launchagent")
    uninstall.set_defaults(func=cmd_uninstall_launchagent)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

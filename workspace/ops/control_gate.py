#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

try:
    from ops import workspace_hub_project
except ImportError:  # pragma: no cover
    import workspace_hub_project  # type: ignore


ALLOWED_TARGET_CLASSES = {"public", "owned-low", "owned-high", "partner", "restricted"}
ALLOWED_ACTION_CLASSES = {
    "read",
    "session-establish",
    "reversible-write-business",
    "reversible-write-system",
    "privileged-or-irreversible",
}
ALLOWED_EXECUTION_PROFILES = {"interactive", "noninteractive", "dry-run-capable"}
ALLOWED_DECISIONS = {"allow", "confirm", "deny"}


def workspace_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1])))


def control_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", str(workspace_root() / "control")))


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


def control_decisions_path() -> Path:
    return runtime_root() / "control-decisions.ndjson"


def network_audit_path() -> Path:
    return runtime_root() / "network-audit.ndjson"


def iso_now() -> str:
    import datetime as dt

    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must decode to a mapping")
    return data


def load_policy_bundle(root: Path | None = None) -> dict[str, Any]:
    root = root or control_root()
    bundle = {
        "targets": read_yaml(root / "targets.yaml"),
        "action_policy": read_yaml(root / "action-policy.yaml"),
        "credential_policy": read_yaml(root / "credential-policy.yaml"),
        "execution_profiles": read_yaml(root / "execution-profiles.yaml"),
    }
    validate_policy_bundle(bundle)
    return bundle


def validate_policy_bundle(bundle: dict[str, Any]) -> None:
    targets_cfg = bundle["targets"]
    action_cfg = bundle["action_policy"]
    exec_cfg = bundle["execution_profiles"]

    defaults = targets_cfg.get("defaults", {})
    if defaults.get("fallback_web_target_class") not in ALLOWED_TARGET_CLASSES:
        raise ValueError("targets.yaml defaults.fallback_web_target_class is invalid")
    if defaults.get("fallback_nonweb_target_class") not in ALLOWED_TARGET_CLASSES:
        raise ValueError("targets.yaml defaults.fallback_nonweb_target_class is invalid")
    for item in targets_cfg.get("targets", []):
        if item.get("target_class") not in ALLOWED_TARGET_CLASSES:
            raise ValueError(f"targets.yaml target_class invalid for {item.get('id', '<unknown>')}")

    action_classes = action_cfg.get("action_classes", {})
    for name in action_classes:
        if name not in ALLOWED_ACTION_CLASSES:
            raise ValueError(f"action-policy.yaml unknown action class: {name}")
    for profile in exec_cfg.get("profiles", {}):
        if profile not in ALLOWED_EXECUTION_PROFILES:
            raise ValueError(f"execution-profiles.yaml unknown profile: {profile}")
    for rule in action_cfg.get("matrix", []):
        if rule.get("decision") not in ALLOWED_DECISIONS:
            raise ValueError(f"action-policy.yaml invalid decision in {rule.get('id', '<unknown>')}")
        for item in rule.get("target_classes", []):
            if item != "*" and item not in ALLOWED_TARGET_CLASSES:
                raise ValueError(f"action-policy.yaml invalid target class {item}")
        for item in rule.get("action_classes", []):
            if item != "*" and item not in ALLOWED_ACTION_CLASSES:
                raise ValueError(f"action-policy.yaml invalid action class {item}")
        for item in rule.get("execution_profiles", []):
            if item != "*" and item not in ALLOWED_EXECUTION_PROFILES:
                raise ValueError(f"action-policy.yaml invalid execution profile {item}")


def append_ndjson(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def count_ndjson_records(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
    except FileNotFoundError:
        return 0


def extract_host_and_scheme(target: str) -> tuple[str, str]:
    parsed = urlparse(target)
    if parsed.scheme:
        return (parsed.hostname or "").lower(), parsed.scheme.lower()
    return "", ""


def rule_matches_target(rule: dict[str, Any], target: str, host: str, scheme: str) -> bool:
    target_l = target.lower()
    if any(target_l.startswith(prefix.lower()) for prefix in rule.get("target_prefixes", [])):
        return True
    if host and host in {item.lower() for item in rule.get("hosts", [])}:
        return True
    if host and any(host.endswith(item.lower()) for item in rule.get("host_suffixes", [])):
        return True
    if host and any(item.lower() in host for item in rule.get("host_contains", [])):
        return True
    if scheme and scheme in {item.lower() for item in rule.get("schemes", [])}:
        return True
    return False


def classify_target(target: str, bundle: dict[str, Any] | None = None) -> tuple[str, str]:
    bundle = bundle or load_policy_bundle()
    targets_cfg = bundle["targets"]
    host, scheme = extract_host_and_scheme(target)
    for rule in targets_cfg.get("targets", []):
        if rule_matches_target(rule, target, host, scheme):
            return str(rule["target_class"]), str(rule.get("id", "matched-rule"))
    defaults = targets_cfg.get("defaults", {})
    if scheme in {item.lower() for item in defaults.get("web_schemes", ["http", "https"])}:
        return str(defaults["fallback_web_target_class"]), "fallback-web"
    return str(defaults["fallback_nonweb_target_class"]), "fallback-nonweb"


def classify_action(action: str, bundle: dict[str, Any] | None = None) -> tuple[str, str]:
    bundle = bundle or load_policy_bundle()
    action_cfg = bundle["action_policy"].get("action_classes", {})
    action_l = action.strip().lower()
    for action_class, meta in action_cfg.items():
        aliases = {action_class.lower(), *(item.lower() for item in meta.get("aliases", []))}
        if action_l in aliases:
            return action_class, action_l
    raise ValueError(f"Unknown action: {action}")


def _matches_field(value: str, allowed: list[str]) -> bool:
    return "*" in allowed or value in allowed


def decide_action(
    *,
    target: str,
    action: str,
    execution_context: str,
    dry_run: bool = False,
    session_authority: str = "explicit",
    data_sensitivity: str = "internal-data",
    bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if execution_context not in ALLOWED_EXECUTION_PROFILES:
        raise ValueError(f"Unknown execution context: {execution_context}")
    bundle = bundle or load_policy_bundle()
    target_class, target_rule = classify_target(target, bundle)
    action_class, action_alias = classify_action(action, bundle)
    action_cfg = bundle["action_policy"]
    defaults = action_cfg.get("defaults", {})
    selected: dict[str, Any] | None = None
    for rule in action_cfg.get("matrix", []):
        if not _matches_field(target_class, list(rule.get("target_classes", []))):
            continue
        if not _matches_field(action_class, list(rule.get("action_classes", []))):
            continue
        if not _matches_field(execution_context, list(rule.get("execution_profiles", []))):
            continue
        selected = rule
        break
    if not selected:
        selected = {
            "id": "default",
            "decision": defaults.get("decision", "deny"),
            "audit_required": bool(defaults.get("audit_required", True)),
            "requires_dry_run": bool(defaults.get("requires_dry_run", False)),
            "reason_code": defaults.get("reason_code", "no_policy_match"),
        }
    decision = str(selected["decision"])
    requires_dry_run = bool(selected.get("requires_dry_run", False))
    reason_code = str(selected.get("reason_code", "unknown"))
    if requires_dry_run and not dry_run and decision == "allow":
        decision = "confirm"
        reason_code = "dry_run_required"
    return {
        "decision": decision,
        "target_class": target_class,
        "target_rule": target_rule,
        "action_class": action_class,
        "action_alias": action_alias,
        "execution_profile": execution_context,
        "audit_required": bool(selected.get("audit_required", True)),
        "requires_dry_run": requires_dry_run,
        "reason_code": reason_code,
        "session_authority": session_authority,
        "data_sensitivity": data_sensitivity,
        "matched_rule": str(selected.get("id", "default")),
        "dry_run": dry_run,
    }


def record_decision(
    *,
    target: str,
    action: str,
    execution_context: str,
    result: dict[str, Any],
    project_name: str = "",
    session_id: str = "",
    request_id: str = "",
) -> dict[str, Any]:
    event = {
        "timestamp": iso_now(),
        "request_id": request_id,
        "target": target,
        "target_class": result["target_class"],
        "action": action,
        "action_class": result["action_class"],
        "execution_context": execution_context,
        "decision": result["decision"],
        "audit_required": result["audit_required"],
        "requires_dry_run": result["requires_dry_run"],
        "reason_code": result["reason_code"],
        "project_name": project_name,
        "session_id": session_id,
    }
    append_ndjson(control_decisions_path(), event)
    return event


def audit_event(
    *,
    target: str,
    action: str,
    result: str,
    target_class: str = "",
    action_class: str = "",
    execution_context: str = "",
    project_name: str = "",
    session_id: str = "",
    request_id: str = "",
    audit_ref: str = "",
) -> dict[str, Any]:
    derived_audit_ref = audit_ref or (
        f"audit:{request_id}" if request_id else hashlib.sha1(f"{target}|{action}|{result}|{iso_now()}".encode("utf-8")).hexdigest()
    )
    event = {
        "timestamp": iso_now(),
        "audit_ref": derived_audit_ref,
        "request_id": request_id,
        "target": target,
        "target_class": target_class,
        "action": action,
        "action_class": action_class,
        "execution_context": execution_context,
        "result": result,
        "project_name": project_name,
        "session_id": session_id,
    }
    append_ndjson(network_audit_path(), event)
    return event


def status_summary(root: Path | None = None) -> dict[str, Any]:
    root = root or control_root()
    bundle = load_policy_bundle(root)
    return {
        "control_root": str(root),
        "config_loaded": True,
        "target_rule_count": len(bundle["targets"].get("targets", [])),
        "action_rule_count": len(bundle["action_policy"].get("matrix", [])),
        "execution_profile_count": len(bundle["execution_profiles"].get("profiles", {})),
        "control_decisions_path": str(control_decisions_path()),
        "network_audit_path": str(network_audit_path()),
        "control_decision_count": count_ndjson_records(control_decisions_path()),
        "network_audit_count": count_ndjson_records(network_audit_path()),
    }


def decide_then_run(
    command: list[str],
    *,
    target: str,
    action: str,
    execution_context: str,
    dry_run: bool = False,
    session_authority: str = "explicit",
    data_sensitivity: str = "internal-data",
    project_name: str = "",
    session_id: str = "",
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    check: bool = False,
    text: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = decide_action(
        target=target,
        action=action,
        execution_context=execution_context,
        dry_run=dry_run,
        session_authority=session_authority,
        data_sensitivity=data_sensitivity,
    )
    record_decision(
        target=target,
        action=action,
        execution_context=execution_context,
        result=result,
        project_name=project_name,
        session_id=session_id,
    )
    if result["decision"] == "deny":
        raise PermissionError(f"Denied by control policy: {result['reason_code']}")
    if result["decision"] == "confirm":
        raise PermissionError(f"Confirmation required by control policy: {result['reason_code']}")
    completed = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=text,
        capture_output=capture_output,
        check=check,
    )
    audit_event(
        target=target,
        action=action,
        result="success" if completed.returncode == 0 else f"returncode:{completed.returncode}",
        target_class=result["target_class"],
        action_class=result["action_class"],
        execution_context=execution_context,
        project_name=project_name,
        session_id=session_id,
    )
    return completed

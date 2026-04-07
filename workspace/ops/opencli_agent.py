#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyYAML is required for opencli_agent") from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import growth_closed_loop, runtime_state
except ImportError:  # pragma: no cover
    growth_closed_loop = None  # type: ignore[assignment]
    from ops import runtime_state


DOCTOR_LINE_RE = re.compile(r"^\[(?P<status>[A-Z]+)\]\s+(?P<label>[^:]+):\s*(?P<detail>.*)$")
MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
ITEM_LINK_RE = re.compile(
    r"(?:^|\n)\[\s*(?P<body>.*?)\s*\]\((?P<url>https://www\.goofish\.com/item\?id=[^)]+)\)",
    re.S,
)
INQUIRY_LINK_RE = re.compile(
    r"(?:^|\n)\[\s*(?P<body>.*?)\s*\]\((?P<url>https://www\.goofish\.com/im\?[^)]+)\)",
    re.S,
)
PRICE_RE = re.compile(r"¥\s*([0-9]+(?:\.[0-9]+)?(?:\s*万)?)")
WANTS_RE = re.compile(r"(\d+)\s*人想要")
XIAOHONGSHU_SESSION_WARMUP_COMMANDS = {
    "creator-profile",
    "creator-notes",
    "creator-notes-summary",
    "creator-note-detail",
    "creator-stats",
    "publish",
    "comment-send",
    "dm-send",
}
XIAOHONGSHU_WRITE_COMMANDS = {"publish", "comment-send", "dm-send"}
BROWSER_BRIDGE_EXTENSION_ID = "njdpninihpgnnlafppcpabknniodbhpf"
BROWSER_BRIDGE_WAKE_URL = f"chrome-extension://{BROWSER_BRIDGE_EXTENSION_ID}/popup.html"


class OpenCLIAgentError(RuntimeError):
    def __init__(self, message: str, *, code: str = "", details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details or {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def control_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_CONTROL_ROOT", str(REPO_ROOT / "control")))


def _growth_control() -> dict[str, Any]:
    path = control_root() / "codex_growth_system.yaml"
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _growth_risk_controls() -> dict[str, Any]:
    payload = _growth_control().get("risk_controls", {})
    return payload if isinstance(payload, dict) else {}


def _growth_platform_policy(site: str) -> dict[str, Any]:
    policies = _growth_control().get("platform_policies", {})
    if not isinstance(policies, dict):
        return {}
    item = policies.get(str(site or "").strip().lower(), {})
    return item if isinstance(item, dict) else {}


def _growth_auto_commands(site: str) -> set[str]:
    item = _growth_platform_policy(site)
    commands = item.get("auto_commands", [])
    if not isinstance(commands, list):
        return set()
    return {str(value).strip().lower() for value in commands if str(value).strip()}


def _human_gate_override(payload: dict[str, Any]) -> bool:
    return any(bool(payload.get(key)) for key in ("human_gate_approved", "manual_gate_override", "manual_execution"))


def _enforce_growth_platform_gate(site: str, command: str, payload: dict[str, Any]) -> None:
    normalized_site = str(site or "").strip().lower()
    normalized_command = str(command or "").strip().lower()
    if normalized_site != "xiaohongshu" or normalized_command not in XIAOHONGSHU_WRITE_COMMANDS:
        return
    if normalized_command in _growth_auto_commands(normalized_site):
        return
    if _human_gate_override(payload):
        return
    policy = _growth_platform_policy(normalized_site)
    raise OpenCLIAgentError(
        "xiaohongshu write commands require a human gate in the current growth system policy",
        code="human_gate_required",
        details={
            "site": normalized_site,
            "command": normalized_command,
            "role": str(policy.get("role", "")).strip(),
            "allowed_auto_commands": sorted(_growth_auto_commands(normalized_site)),
        },
    )


def _default_idempotency_key(site: str, command: str, payload: dict[str, Any]) -> str:
    identity = {
        "site": str(site or "").strip().lower(),
        "command": str(command or "").strip().lower(),
        "payload": payload,
    }
    return hashlib.sha1(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _write_text_blob(payload: dict[str, Any]) -> str:
    options = payload.get("options", {})
    positional = payload.get("positional", [])
    fields: list[str] = []
    if isinstance(positional, list):
        fields.extend(str(item) for item in positional if str(item).strip())
    if isinstance(options, dict):
        for key in ("content", "title", "message", "text"):
            value = str(options.get(key, "")).strip()
            if value:
                fields.append(value)
    return "\n".join(fields).strip()


def _write_guard(site: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
    controls = _growth_risk_controls()
    max_actions = int(
        payload.get("max_actions_per_hour")
        or (controls.get("max_actions_per_hour", {}) or {}).get(str(site or "").strip().lower(), 0)
        or 0
    )
    max_failures = int(
        payload.get("max_failures_before_trip")
        or (controls.get("max_failures_before_trip", {}) or {}).get(str(site or "").strip().lower(), 0)
        or 0
    )
    blocked_keywords = [str(item).strip().lower() for item in controls.get("blocked_keywords", []) if str(item).strip()]
    idempotency_key = str(payload.get("idempotency_key", "")).strip() or _default_idempotency_key(site, command, payload)
    existing = runtime_state.fetch_growth_action_attempt(idempotency_key)
    if str(existing.get("action_status", "")).strip() in {"running", "done", "gated"}:
        return {
            "gated": True,
            "reason": "idempotency_hit",
            "idempotency_key": idempotency_key,
            "existing_status": existing.get("action_status", ""),
        }
    body = _write_text_blob(payload).lower()
    matched_keyword = next((item for item in blocked_keywords if item in body), "")
    if matched_keyword:
        return {
            "gated": True,
            "reason": "blocked_keyword",
            "idempotency_key": idempotency_key,
            "keyword": matched_keyword,
        }
    if max_actions and runtime_state.growth_action_recent_count(platform=site, since_seconds=3600) >= max_actions:
        return {
            "gated": True,
            "reason": "hourly_limit",
            "idempotency_key": idempotency_key,
            "max_actions_per_hour": max_actions,
        }
    if max_failures and runtime_state.growth_action_consecutive_failures(platform=site, command=command) >= max_failures:
        return {
            "gated": True,
            "reason": "circuit_open",
            "idempotency_key": idempotency_key,
            "max_failures_before_trip": max_failures,
        }
    return {
        "gated": False,
        "reason": "",
        "idempotency_key": idempotency_key,
        "max_actions_per_hour": max_actions,
        "max_failures_before_trip": max_failures,
    }


def opencli_bin() -> str:
    return str(os.environ.get("WORKSPACE_HUB_OPENCLI_BIN", "")).strip() or "opencli"


def _run_command(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            argv,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:  # pragma: no cover
        raise OpenCLIAgentError(
            f"opencli binary not found: {opencli_bin()}",
            code="binary_not_found",
            details={"argv": argv},
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise OpenCLIAgentError(
            f"opencli command timed out after {timeout_seconds}s",
            code="timeout",
            details={"argv": argv, "timeout_seconds": timeout_seconds},
        ) from exc


def _parse_doctor_result(result: subprocess.CompletedProcess[str], *, version: str) -> dict[str, Any]:
    raw = str(result.stdout or "").strip()
    checks: dict[str, dict[str, str]] = {}
    ok = result.returncode == 0
    for line in raw.splitlines():
        match = DOCTOR_LINE_RE.match(line.strip())
        if not match:
            continue
        label = match.group("label").strip().lower().replace(" ", "_")
        status = match.group("status").strip().lower()
        detail = match.group("detail").strip()
        checks[label] = {"status": status, "detail": detail}
        if status not in {"ok", "pass", "skip"}:
            ok = False
    return {
        "ok": ok,
        "version": version,
        "checks": checks,
        "raw": raw,
        "stderr": str(result.stderr or "").strip(),
        "returncode": result.returncode,
    }


def _wake_browser_bridge() -> dict[str, Any]:
    script = f'tell application "Google Chrome" to open location "{BROWSER_BRIDGE_WAKE_URL}"'
    attempts: list[dict[str, Any]] = []
    commands = [
        ("osascript", ["osascript", "-e", script]),
        ("open", ["open", "-a", "Google Chrome", BROWSER_BRIDGE_WAKE_URL]),
    ]
    result: subprocess.CompletedProcess[str] | None = None
    method = ""
    for label, argv in commands:
        current = _run_command(argv, timeout_seconds=15)
        attempts.append(
            {
                "method": label,
                "argv": argv,
                "returncode": current.returncode,
                "stdout": str(current.stdout or "").strip(),
                "stderr": str(current.stderr or "").strip(),
            }
        )
        if current.returncode == 0:
            result = current
            method = label
            break
    if result is None:
        raise OpenCLIAgentError(
            "failed to wake OpenCLI Browser Bridge",
            code="browser_bridge_wake_failed",
            details={"attempts": attempts},
        )
    return {
        "ok": True,
        "method": method,
        "wake_url": BROWSER_BRIDGE_WAKE_URL,
        "stdout": str(result.stdout or "").strip(),
        "stderr": str(result.stderr or "").strip(),
        "attempts": attempts,
    }


def _ensure_browser_bridge_connected() -> dict[str, Any]:
    version = opencli_version()
    preflight = _run_command([opencli_bin(), "doctor", "--no-live"], timeout_seconds=20)
    initial = _parse_doctor_result(preflight, version=version)
    if initial.get("checks", {}).get("extension", {}).get("status") == "ok":
        return {"ok": True, "woke": False, "doctor": initial}
    wake = _wake_browser_bridge()
    healed = _run_command([opencli_bin(), "doctor"], timeout_seconds=30)
    healed_snapshot = _parse_doctor_result(healed, version=version)
    if healed_snapshot.get("checks", {}).get("extension", {}).get("status") != "ok":
        raise OpenCLIAgentError(
            "OpenCLI Browser Bridge is still disconnected after wake",
            code="browser_bridge_unavailable",
            details={"before": initial, "after": healed_snapshot, "wake": wake},
        )
    return {"ok": True, "woke": True, "doctor": healed_snapshot, "wake": wake}


def _run_session_warmup(site: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized_site = str(site or "").strip().lower()
    normalized_command = str(command or "").strip().lower()
    if normalized_site != "xiaohongshu" or normalized_command not in XIAOHONGSHU_SESSION_WARMUP_COMMANDS:
        return {
            "ok": True,
            "site": normalized_site,
            "command": normalized_command,
            "warmed": False,
            "skipped": True,
            "reason": "no_warmup_required",
        }
    helper = REPO_ROOT / "ops" / "opencli_session_warmup.mjs"
    argv = [
        "node",
        str(helper),
        "--site",
        normalized_site,
        "--command",
        normalized_command,
        "--payload-json",
        json.dumps(payload, ensure_ascii=False),
    ]
    result = _run_command(argv, timeout_seconds=int(payload.get("warmup_timeout_seconds", 90) or 90))
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    try:
        parsed = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError as exc:
        raise OpenCLIAgentError(
            "opencli session warmup returned invalid json",
            code="invalid_warmup_payload",
            details={"site": normalized_site, "command": normalized_command, "argv": argv, "stdout": stdout, "stderr": stderr},
        ) from exc
    if result.returncode != 0 or not isinstance(parsed, dict) or not parsed.get("ok"):
        raise OpenCLIAgentError(
            f"opencli session warmup {normalized_site} {normalized_command} failed",
            code=str(parsed.get("error_code") or "warmup_failed"),
            details={
                "site": normalized_site,
                "command": normalized_command,
                "argv": argv,
                "stdout": stdout,
                "stderr": stderr,
                "warmup_result": parsed,
            },
        )
    return parsed


def _run_write_helper(site: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
    risk_level = str(payload.get("risk_level", "")).strip() or "side_effect_or_private_data"
    bridge = _ensure_browser_bridge_connected()
    guard_state = _write_guard(site, command, payload)
    if guard_state.get("gated"):
        runtime_state.record_growth_action_attempt(
            idempotency_key=str(guard_state.get("idempotency_key", "")),
            platform=site,
            command=command,
            action_status="gated",
            payload=payload,
            risk_level=risk_level,
            error=str(guard_state.get("reason", "")),
        )
        raise OpenCLIAgentError(
            f"opencli helper {site} {command} gated",
            code="action_gated",
            details={"site": site, "command": command, "gate_state": guard_state},
        )
    runtime_state.record_growth_action_attempt(
        idempotency_key=str(guard_state.get("idempotency_key", "")),
        platform=site,
        command=command,
        action_status="running",
        payload=payload,
        risk_level=risk_level,
    )
    try:
        warmup = _run_session_warmup(site, command, payload)
    except OpenCLIAgentError as exc:
        runtime_state.record_growth_action_attempt(
            idempotency_key=str(guard_state.get("idempotency_key", "")),
            platform=site,
            command=command,
            action_status="failed",
            payload=payload,
            risk_level=risk_level,
            error=str(exc.code or "warmup_failed"),
        )
        raise
    helper = REPO_ROOT / "ops" / "opencli_write_helper.mjs"
    argv = [
        "node",
        str(helper),
        "--site",
        site,
        "--command",
        command,
        "--payload-json",
        json.dumps(payload, ensure_ascii=False),
    ]
    try:
        result = _run_command(argv, timeout_seconds=int(payload.get("timeout_seconds", 180) or 180))
    except OpenCLIAgentError as exc:
        runtime_state.record_growth_action_attempt(
            idempotency_key=str(guard_state.get("idempotency_key", "")),
            platform=site,
            command=command,
            action_status="failed",
            payload=payload,
            risk_level=risk_level,
            error=str(exc),
        )
        raise
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    try:
        parsed = json.loads(stdout) if stdout else {}
    except json.JSONDecodeError as exc:
        runtime_state.record_growth_action_attempt(
            idempotency_key=str(guard_state.get("idempotency_key", "")),
            platform=site,
            command=command,
            action_status="failed",
            payload=payload,
            risk_level=risk_level,
            error="invalid_helper_payload",
        )
        raise OpenCLIAgentError(
            "opencli write helper returned invalid json",
            code="invalid_helper_payload",
            details={"site": site, "command": command, "stdout": stdout, "stderr": stderr},
        ) from exc
    if result.returncode != 0 or not isinstance(parsed, dict) or not parsed.get("ok"):
        runtime_state.record_growth_action_attempt(
            idempotency_key=str(guard_state.get("idempotency_key", "")),
            platform=site,
            command=command,
            action_status="failed",
            payload=payload,
            risk_level=risk_level,
            error=str(parsed.get("error_code") or "helper_failed"),
        )
        raise OpenCLIAgentError(
            f"opencli helper {site} {command} failed",
            code=str(parsed.get("error_code") or "helper_failed"),
            details={
                "site": site,
                "command": command,
                "argv": argv,
                "stdout": stdout,
                "stderr": stderr,
                "helper_result": parsed,
            },
        )
    version = opencli_version()
    runtime_state.record_growth_action_attempt(
        idempotency_key=str(guard_state.get("idempotency_key", "")),
        platform=site,
        command=command,
        action_status="done",
        payload=payload,
        risk_level=risk_level,
    )
    growth_cycle_result: dict[str, Any] = {}
    growth_cycle_error: dict[str, Any] = {}
    growth_cycle_payload = payload.get("growth_cycle")
    if isinstance(growth_cycle_payload, dict) and growth_cycle_payload and growth_closed_loop is not None:
        try:
            growth_cycle_result = growth_closed_loop.record_cycle(growth_cycle_payload)
        except Exception as exc:
            growth_cycle_error = {
                "ok": False,
                "code": "growth_cycle_writeback_failed",
                "message": f"{type(exc).__name__}: {exc}",
            }
    elif isinstance(growth_cycle_payload, dict) and growth_cycle_payload:
        growth_cycle_error = {
            "ok": False,
            "code": "growth_cycle_unavailable",
            "message": "growth_closed_loop is not installed in this deployment",
        }
    return {
        "ok": True,
        "version": version,
        "site": site,
        "command": command,
        "bridge": bridge,
        "warmup": warmup,
        "argv": argv,
        "idempotency_key": str(guard_state.get("idempotency_key", "")),
        "gate_state": guard_state,
        "result": parsed.get("result") or {},
        "growth_cycle": growth_cycle_result,
        "growth_cycle_error": growth_cycle_error,
        "stdout": stdout,
        "stderr": stderr,
    }


def opencli_version() -> str:
    result = _run_command([opencli_bin(), "--version"], timeout_seconds=15)
    version = str(result.stdout or "").strip()
    if result.returncode != 0 or not version:
        raise OpenCLIAgentError(
            "failed to read opencli version",
            code="version_failed",
            details={"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr},
        )
    return version


def _doctor_snapshot() -> dict[str, Any]:
    version = opencli_version()
    result = _run_command([opencli_bin(), "doctor"], timeout_seconds=30)
    return _parse_doctor_result(result, version=version)


def _list_commands(payload: dict[str, Any]) -> dict[str, Any]:
    version = opencli_version()
    result = _run_command([opencli_bin(), "list", "-f", "yaml"], timeout_seconds=30)
    if result.returncode != 0:
        raise OpenCLIAgentError(
            "opencli list failed",
            code="list_failed",
            details={"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr},
        )
    rows = yaml.safe_load(result.stdout or "") or []
    if not isinstance(rows, list):
        raise OpenCLIAgentError(
            "opencli list returned unexpected payload",
            code="invalid_list_payload",
            details={"stdout": result.stdout},
        )
    site_filter = str(payload.get("site", "")).strip().lower()
    strategy_filter = str(payload.get("strategy", "")).strip().lower()
    filtered: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        site = str(item.get("site", "")).strip().lower()
        strategy = str(item.get("strategy", "")).strip().lower()
        if site_filter and site != site_filter:
            continue
        if strategy_filter and strategy != strategy_filter:
            continue
        filtered.append(item)
    return {
        "ok": True,
        "version": version,
        "count": len(filtered),
        "commands": filtered,
    }


def _coerce_option_name(key: str) -> str:
    return f"--{str(key).strip().replace('_', '-')}"


def _normalize_payload(payload: dict[str, Any]) -> tuple[list[str], dict[str, Any], bool]:
    positional = payload.get("positional", [])
    if positional is None:
        positional = []
    if not isinstance(positional, list):
        raise OpenCLIAgentError("payload.positional must be a list", code="invalid_payload")
    options = payload.get("options", {})
    if options is None:
        options = {}
    if not isinstance(options, dict):
        raise OpenCLIAgentError("payload.options must be an object", code="invalid_payload")
    expect_json = bool(payload.get("expect_json", True))
    return [str(item) for item in positional], options, expect_json


def _command_argv(site: str, command: str, payload: dict[str, Any]) -> tuple[list[str], bool]:
    positional, options, expect_json = _normalize_payload(payload)
    argv = [opencli_bin(), site, command, *positional]
    has_format = any(str(key).replace("_", "-") == "format" for key in options)
    for key, value in options.items():
        if value in (None, "", False):
            continue
        option = _coerce_option_name(key)
        if value is True:
            argv.append(option)
            continue
        if isinstance(value, list):
            for item in value:
                argv.extend([option, str(item)])
            continue
        argv.extend([option, str(value)])
    if expect_json and not has_format:
        argv.extend(["-f", "json"])
    return argv, expect_json or has_format


def _run_site_command(site: str, command: str, payload: dict[str, Any]) -> dict[str, Any]:
    version = opencli_version()
    bridge = _ensure_browser_bridge_connected()
    warmup = _run_session_warmup(site, command, payload)
    argv, parse_structured = _command_argv(site, command, payload)
    timeout_seconds = int(payload.get("timeout_seconds", 90) or 90)
    result = _run_command(argv, timeout_seconds=timeout_seconds)
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0:
        raise OpenCLIAgentError(
            f"opencli {site} {command} failed",
            code="command_failed",
            details={
                "site": site,
                "command": command,
                "argv": argv,
                "returncode": result.returncode,
                "stdout": stdout,
                "stderr": stderr,
            },
        )
    parsed: Any = stdout
    if parse_structured and stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = stdout
    return {
        "ok": True,
        "version": version,
        "site": site,
        "command": command,
        "bridge": bridge,
        "warmup": warmup,
        "argv": argv,
        "result": parsed,
        "stdout": stdout,
        "stderr": stderr,
    }


def _coerce_query(payload: dict[str, Any]) -> str:
    positional, options, _ = _normalize_payload(payload)
    if positional:
        return str(positional[0]).strip()
    query = str(options.get("query", "")).strip()
    if query:
        return query
    raise OpenCLIAgentError("xianyu search requires a query", code="usage")


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _strip_markdown_artifacts(value: str) -> str:
    text = MARKDOWN_IMAGE_RE.sub(" ", value or "")
    text = text.replace("[", " ").replace("]", " ")
    text = re.sub(r"\((?:https?:)?//[^)\s]+\)", " ", text)
    text = re.sub(r"\\([_()\\])", r"\1", text)
    return _normalize_whitespace(text)


def _read_single_markdown(output_dir: Path) -> tuple[Path, str]:
    candidates = sorted(output_dir.rglob("*.md"))
    if not candidates:
        raise OpenCLIAgentError(
            "opencli web read did not produce a markdown artifact",
            code="missing_markdown_artifact",
            details={"output_dir": str(output_dir)},
        )
    path = candidates[0]
    return path, path.read_text(encoding="utf-8")


def _run_web_read_capture(url: str, *, wait_seconds: int = 3) -> dict[str, Any]:
    version = opencli_version()
    output_dir = Path(tempfile.mkdtemp(prefix="workspace-hub-opencli-"))
    argv = [
        opencli_bin(),
        "web",
        "read",
        "--url",
        url,
        "--output",
        str(output_dir),
        "--download-images",
        "false",
        "--wait",
        str(wait_seconds),
        "-f",
        "json",
    ]
    result = _run_command(argv, timeout_seconds=max(90, wait_seconds + 30))
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0:
        raise OpenCLIAgentError(
            "opencli web read failed",
            code="web_read_failed",
            details={"url": url, "argv": argv, "returncode": result.returncode, "stdout": stdout, "stderr": stderr},
        )
    try:
        index_payload = json.loads(stdout) if stdout else []
    except json.JSONDecodeError:
        index_payload = stdout
    markdown_path, markdown = _read_single_markdown(output_dir)
    return {
        "ok": True,
        "version": version,
        "argv": argv,
        "url": url,
        "output_dir": str(output_dir),
        "index": index_payload,
        "markdown_path": str(markdown_path),
        "markdown": markdown,
        "stdout": stdout,
        "stderr": stderr,
    }


def _parse_xianyu_personal_summary(markdown: str) -> dict[str, Any]:
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    title = ""
    location = ""
    for idx, line in enumerate(lines):
        if line.startswith("# "):
            title = line.removeprefix("# ").replace("_闲鱼", "").strip()
        if line == "Preview" and idx + 2 < len(lines):
            maybe_title = lines[idx + 1]
            if maybe_title:
                title = maybe_title.split("![](")[0].strip() or title
            maybe_location = lines[idx + 2]
            if maybe_location and "粉丝" not in maybe_location:
                location = maybe_location
    normalized = _normalize_whitespace(markdown)
    followers = int(re.search(r"(\d+)\s*粉丝", normalized).group(1)) if re.search(r"(\d+)\s*粉丝", normalized) else 0
    following = int(re.search(r"(\d+)\s*关注", normalized).group(1)) if re.search(r"(\d+)\s*关注", normalized) else 0
    listings = int(re.search(r"宝贝\s+宝贝\s+(\d+)", normalized).group(1)) if re.search(r"宝贝\s+宝贝\s+(\d+)", normalized) else 0
    reputation = int(re.search(r"信用及评价\s+信用及评价\s+(\d+)", normalized).group(1)) if re.search(r"信用及评价\s+信用及评价\s+(\d+)", normalized) else 0
    return {
        "account": title.replace("\\_", "_"),
        "location": location,
        "followers": followers,
        "following": following,
        "listings": listings,
        "reputation": reputation,
    }


def _xianyu_query_value(url: str, key: str) -> str:
    try:
        payload = parse_qs(urlsplit(url).query)
    except ValueError:
        return ""
    values = payload.get(key) or []
    if not values:
        return ""
    return _text(values[0])


def _xianyu_thread_id(url: str) -> str:
    item_id = _xianyu_query_value(url, "itemId")
    peer_user_id = _xianyu_query_value(url, "peerUserId")
    if item_id or peer_user_id:
        return f"xianyu-{item_id or 'item'}-{peer_user_id or 'peer'}"
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"xianyu-{digest}"


def _xianyu_context_lines(markdown: str, url: str, *, window: int = 5) -> list[str]:
    lines = markdown.splitlines()
    for index, line in enumerate(lines):
        if url not in line:
            continue
        start = max(0, index - window)
        end = min(len(lines), index + window + 1)
        context: list[str] = []
        for candidate in lines[start:end]:
            cleaned = _strip_markdown_artifacts(candidate)
            if cleaned:
                context.append(cleaned)
        return context
    return []


def _pick_xianyu_candidate_line(lines: list[str], *, exclude: set[str] | None = None) -> str:
    blocked = {
        "消息",
        "聊天",
        "尚未选择任何联系人",
        "快点左侧列表聊起来吧~",
        "发闲置",
        "反馈",
        "客服",
        "回顶部",
        "APP",
        "登录",
        "搜索",
    }
    if exclude:
        blocked.update({_normalize_whitespace(item) for item in exclude if _normalize_whitespace(item)})
    for line in lines:
        cleaned = _normalize_whitespace(line)
        if not cleaned or cleaned in blocked:
            continue
        if cleaned.startswith("https://"):
            continue
        if "goofish.com" in cleaned:
            continue
        if cleaned.startswith("原文链接"):
            continue
        return cleaned
    return ""


def _parse_xianyu_inquiries(markdown: str, *, limit: int | None = None) -> list[dict[str, Any]]:
    inquiries: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for match in INQUIRY_LINK_RE.finditer(markdown):
        url = _text(match.group("url"))
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        body = _strip_markdown_artifacts(match.group("body"))
        body_lines = [line for line in body.splitlines() if _normalize_whitespace(line)]
        context_lines = _xianyu_context_lines(markdown, url)
        candidate_lines = [
            _normalize_whitespace(line)
            for line in [*body_lines, *context_lines]
            if _normalize_whitespace(line)
        ]
        item_id = _xianyu_query_value(url, "itemId")
        peer_user_id = _xianyu_query_value(url, "peerUserId")
        if not item_id and not peer_user_id:
            continue
        thread_id = _xianyu_thread_id(url)
        listing_title = _pick_xianyu_candidate_line(candidate_lines)
        remaining = [line for line in candidate_lines if line != listing_title]
        buyer_name = ""
        for line in remaining:
            if re.fullmatch(r"[A-Za-z0-9_一-龥\*]{2,40}", line):
                buyer_name = line
                break
        last_message = _pick_xianyu_candidate_line(remaining, exclude={buyer_name, listing_title})
        unread = "yes" if any("未读" in line for line in candidate_lines) else "no"
        timestamp = ""
        for line in remaining:
            if re.search(r"(刚刚|\d+分钟前|\d+小时前|昨天|今天|\d{1,2}:\d{2})", line):
                timestamp = line
                break
        inquiries.append(
            {
                "thread_id": thread_id,
                "listing_id": item_id,
                "listing_title": listing_title,
                "buyer_name": buyer_name,
                "peer_user_id": peer_user_id,
                "source_ref": url,
                "last_message": last_message,
                "last_message_at": timestamp,
                "unread": unread,
                "thread_url": url,
            }
        )
        if limit and len(inquiries) >= limit:
            break
    return inquiries


def _parse_xianyu_inquiry_thread(markdown: str, *, url: str) -> dict[str, Any]:
    item_id = _xianyu_query_value(url, "itemId")
    peer_user_id = _xianyu_query_value(url, "peerUserId")
    empty = "尚未选择任何联系人" in markdown
    lines = [
        _normalize_whitespace(_strip_markdown_artifacts(line))
        for line in markdown.splitlines()
        if _normalize_whitespace(_strip_markdown_artifacts(line))
    ]
    lines = [
        line for line in lines
        if line not in {"聊天_闲鱼", "聊天", "消息", "登录", "搜索", "发闲置", "反馈", "客服", "回顶部", "APP"}
        and not line.startswith("> 原文链接")
        and line != "---"
        and not line.startswith("(javascript:void")
        and not line.startswith("- ")
    ]
    listing_title = ""
    for line in lines:
        if "工作流" in line or "诊断" in line or (item_id and item_id in line):
            listing_title = line
            break
    return {
        "thread_id": _xianyu_thread_id(url),
        "item_id": item_id,
        "peer_user_id": peer_user_id,
        "thread_url": url,
        "empty": empty,
        "listing_title": listing_title,
        "observed_lines": lines[:20],
        "line_count": len(lines),
    }


def _clean_xianyu_title_segment(value: str) -> str:
    text = value or ""
    for marker in ("猜你喜欢", "小闲鱼没有找到你想要的宝贝~", "减少筛选内容试试", "网页版发闲置功能又升级啦！"):
        if marker in text:
            text = text.split(marker)[-1]
    text = re.sub(r"^\)+", " ", text)
    text = re.sub(r"\s*Apple/苹果\s*", " ", text)
    text = re.sub(r"\s*(全新|几乎全新|明显使用痕迹|无法正常使用|卖家信用优秀|卖家信用极好)\s*", " ", text)
    return _normalize_whitespace(text)


def _parse_xianyu_item_blocks(markdown: str, *, limit: int | None = None, seller_filter: str = "") -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    normalized_seller_filter = _normalize_whitespace(_strip_markdown_artifacts(seller_filter)).lower()
    for match in ITEM_LINK_RE.finditer(markdown):
        url = match.group("url").strip()
        if url in seen_urls:
            continue
        seen_urls.add(url)
        body = _strip_markdown_artifacts(match.group("body"))
        if not body:
            continue
        price_match = PRICE_RE.search(body)
        price = price_match.group(1).replace(" ", "") if price_match else ""
        wants_match = WANTS_RE.search(body)
        wants = int(wants_match.group(1)) if wants_match else 0
        title_text = body
        if price_match:
            title_text = body[: price_match.start()]
        title_text = _clean_xianyu_title_segment(title_text)
        if not title_text:
            continue
        seller = ""
        seller_matches = re.findall(r"([A-Za-z0-9_一-龥\*]+)\s*$", body)
        if seller_matches:
            seller = seller_matches[-1]
        normalized_seller = _normalize_whitespace(seller).lower()
        normalized_body = body.lower()
        if (
            normalized_seller_filter
            and normalized_seller != normalized_seller_filter
            and normalized_seller_filter not in normalized_body
        ):
            continue
        items.append(
            {
                "title": title_text,
                "price": price,
                "wants": wants,
                "seller": seller,
                "url": url,
            }
        )
        if limit and len(items) >= limit:
            break
    return items


def _run_xianyu_command(command: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized = str(command or "").strip().lower()
    limit = int((payload.get("options") or {}).get("limit", 10) or 10) if isinstance(payload.get("options"), dict) else 10
    if normalized in {"publish", "inquiry-reply"}:
        return _run_write_helper("xianyu", normalized, payload)
    bridge = _ensure_browser_bridge_connected()
    if normalized == "personal-summary":
        capture = _run_web_read_capture("https://www.goofish.com/personal")
        return {
            "ok": True,
            "version": capture["version"],
            "site": "xianyu",
            "command": normalized,
            "bridge": bridge,
            "argv": capture["argv"],
            "result": _parse_xianyu_personal_summary(capture["markdown"]),
            "artifacts": {"markdown_path": capture["markdown_path"], "source_url": capture["url"]},
            "stdout": capture["stdout"],
            "stderr": capture["stderr"],
        }
    if normalized == "my-listings":
        capture = _run_web_read_capture("https://www.goofish.com/personal")
        summary = _parse_xianyu_personal_summary(capture["markdown"])
        account = summary["account"]
        if not account or account == "闲鱼":
            raise OpenCLIAgentError(
                "xianyu personal page did not resolve to an account-specific view",
                code="unstable_personal_page",
                details={"source_url": capture["url"], "markdown_path": capture["markdown_path"]},
            )
        items = _parse_xianyu_item_blocks(capture["markdown"], limit=limit, seller_filter=account)
        if not items:
            raise OpenCLIAgentError(
                "xianyu my-listings could not isolate account-owned listings from the current page shape",
                code="listings_not_isolated",
                details={"account": account, "source_url": capture["url"], "markdown_path": capture["markdown_path"]},
            )
        return {
            "ok": True,
            "version": capture["version"],
            "site": "xianyu",
            "command": normalized,
            "bridge": bridge,
            "argv": capture["argv"],
            "result": items,
            "artifacts": {"markdown_path": capture["markdown_path"], "source_url": capture["url"]},
            "stdout": capture["stdout"],
            "stderr": capture["stderr"],
        }
    if normalized == "search":
        query = _coerce_query(payload)
        capture = _run_web_read_capture(f"https://www.goofish.com/search?keyword={quote(query)}")
        return {
            "ok": True,
            "version": capture["version"],
            "site": "xianyu",
            "command": normalized,
            "bridge": bridge,
            "argv": capture["argv"],
            "result": _parse_xianyu_item_blocks(capture["markdown"], limit=limit),
            "artifacts": {"markdown_path": capture["markdown_path"], "source_url": capture["url"], "query": query},
            "stdout": capture["stdout"],
            "stderr": capture["stderr"],
        }
    if normalized == "inquiries":
        capture = _run_web_read_capture("https://www.goofish.com/im")
        return {
            "ok": True,
            "version": capture["version"],
            "site": "xianyu",
            "command": normalized,
            "bridge": bridge,
            "argv": capture["argv"],
            "result": _parse_xianyu_inquiries(capture["markdown"], limit=limit),
            "artifacts": {"markdown_path": capture["markdown_path"], "source_url": capture["url"]},
            "stdout": capture["stdout"],
            "stderr": capture["stderr"],
        }
    if normalized == "inquiry-thread-read":
        options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
        url = _text(payload.get("url")) or _text(options.get("url"))
        if not url:
            item_id = _text(options.get("item_id"))
            peer_user_id = _text(options.get("peer_user_id"))
            if item_id and peer_user_id:
                url = f"https://www.goofish.com/im?itemId={quote(item_id)}&peerUserId={quote(peer_user_id)}"
        if not url:
            raise OpenCLIAgentError("xianyu inquiry-thread-read requires options.url or item_id+peer_user_id", code="usage")
        capture = _run_web_read_capture(url)
        return {
            "ok": True,
            "version": capture["version"],
            "site": "xianyu",
            "command": normalized,
            "bridge": bridge,
            "argv": capture["argv"],
            "result": _parse_xianyu_inquiry_thread(capture["markdown"], url=url),
            "artifacts": {"markdown_path": capture["markdown_path"], "source_url": capture["url"]},
            "stdout": capture["stdout"],
            "stderr": capture["stderr"],
        }
    raise OpenCLIAgentError(
        f"unsupported xianyu command: {command}",
        code="unsupported_command",
        details={"site": "xianyu", "command": command},
    )


def perform_operation(site: str, command: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized_site = str(site or "").strip().lower()
    normalized_command = str(command or "").strip().lower()
    request = payload or {}
    if not normalized_site:
        raise OpenCLIAgentError("site is required", code="usage")
    if not normalized_command:
        raise OpenCLIAgentError("command is required", code="usage")
    if normalized_site == "system" and normalized_command == "doctor":
        return _doctor_snapshot()
    if normalized_site == "system" and normalized_command == "list":
        return _list_commands(request)
    _enforce_growth_platform_gate(normalized_site, normalized_command, request)
    if normalized_site == "xiaohongshu" and normalized_command in {"publish", "comment-send", "dm-send"}:
        return _run_write_helper("xiaohongshu", normalized_command, request)
    if normalized_site == "xianyu":
        return _run_xianyu_command(normalized_command, request)
    return _run_site_command(normalized_site, normalized_command, request)


def _parse_cli_payload(text: str) -> dict[str, Any]:
    if not text.strip():
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise OpenCLIAgentError(f"invalid payload json: {exc}", code="invalid_payload") from exc
    if not isinstance(payload, dict):
        raise OpenCLIAgentError("payload must decode to an object", code="invalid_payload")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run OpenCLI operations from Codex Hub")
    parser.add_argument("--site", required=True)
    parser.add_argument("--command", required=True)
    parser.add_argument("--payload-json", default="{}")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = perform_operation(args.site, args.command, _parse_cli_payload(args.payload_json))
    except OpenCLIAgentError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "site": args.site,
                    "command": args.command,
                    "error": str(exc),
                    "error_code": exc.code,
                    "details": exc.details,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

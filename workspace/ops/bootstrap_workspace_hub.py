#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - bootstrap must be able to install its own deps
    yaml = None  # type: ignore


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
SITE_CONFIG_PATH = WORKSPACE_ROOT / "control" / "site.yaml"
BOOTSTRAP_STATUS_PATH = WORKSPACE_ROOT / "runtime" / "bootstrap-status.json"
CODEX_CONFIG_PATH = WORKSPACE_ROOT / ".codex" / "config.toml"
REQUIREMENTS_PATH = WORKSPACE_ROOT / "requirements.txt"
MEMORY_TEMPLATE_ROOT = (WORKSPACE_ROOT.parent / "memory").resolve()
DEFAULT_MEMORY_ROOT = (WORKSPACE_ROOT.parent / "memory.local").resolve()
PYTHON_DEPENDENCIES = (
    ("yaml", "PyYAML"),
    ("docx", "python-docx"),
    ("openpyxl", "openpyxl"),
    ("pypdf", "pypdf"),
    ("requests", "requests"),
    ("bs4", "beautifulsoup4"),
    ("qrcode", "qrcode[pil]"),
    ("certifi", "certifi"),
    ("cryptography", "cryptography"),
    ("openai", "openai"),
)
FEATURE_TOOL_GROUPS = {
    "knowledge_base_pdf_ocr": {
        "label": "Knowledge Base PDF / OCR ingestion",
        "commands": ("tesseract", "ocrmypdf", "pdftoppm"),
        "install_hint": "python3 ops/bootstrap_workspace_hub.py install-feature --feature knowledge-base",
    },
    "opencli_browser": {
        "label": "OpenCLI browser execution",
        "commands": ("opencli",),
        "apps": ("Google Chrome",),
        "install_hint": "python3 ops/bootstrap_workspace_hub.py install-feature --feature opencli",
    },
}
SUPPORTED_PACKAGE_MANAGERS = ("brew", "apt", "winget", "choco")
SYSTEM_PACKAGE_GROUPS: dict[str, dict[str, dict[str, list[str]]]] = {
    "node_runtime": {
        "brew": {"formula": ["node"], "cask": []},
        "apt": {"packages": ["nodejs", "npm"]},
        "winget": {"ids": ["OpenJS.NodeJS.LTS"]},
        "choco": {"packages": ["nodejs-lts"]},
    },
    "knowledge_base_pdf_ocr": {
        "brew": {"formula": ["tesseract", "ocrmypdf", "poppler"], "cask": []},
        "apt": {"packages": ["tesseract-ocr", "ocrmypdf", "poppler-utils"]},
        "winget": {"ids": ["UB-Mannheim.TesseractOCR", "OCRmyPDF.OCRMYPDF", "oschwartz10612.Poppler"]},
        "choco": {"packages": ["tesseract", "ocrmypdf", "poppler"]},
    },
    "opencli_browser": {
        "brew": {"formula": [], "cask": ["google-chrome"]},
        "apt": {"packages": ["chromium-browser"]},
        "winget": {"ids": ["Google.Chrome"]},
        "choco": {"packages": ["googlechrome"]},
    },
}
FEATURE_SURFACES = {
    "feishu": {
        "label": "Feishu object ops and bridge",
        "installer": "setup-feishu-cli",
    },
    "knowledge-base": {
        "label": "Knowledge Base intake and OCR",
        "system_group": "knowledge_base_pdf_ocr",
        "installer": "install-feature",
    },
    "opencli": {
        "label": "OpenCLI browser execution",
        "system_group": "opencli_browser",
        "installer": "install-feature",
    },
    "weixin": {
        "label": "Weixin bridge",
        "installer": "install-feature",
    },
    "electron": {
        "label": "Electron desktop shell",
        "installer": "install-feature",
    },
}
LARK_CLI_PACKAGE = "@larksuite/cli"
LARK_CLI_SKILLS_REPO = "https://github.com/larksuite/cli"
LARK_CLI_CONFIG_PATH = Path.home() / ".lark-cli" / "config.json"
LARK_CLI_SKILLS_ROOT = Path.home() / ".agents" / "skills"
OPENCLI_NPM_PACKAGE = "@jackwener/opencli"
DEFAULT_FEISHU_CLI_DOMAINS = "event,im,docs,drive,base,task,calendar,vc,minutes,contact,wiki,sheets,mail"
FEISHU_BRIDGE_ENV_PATH = WORKSPACE_ROOT / "ops" / "feishu_bridge.env.local"
FEISHU_BRIDGE_ENV_EXAMPLE_PATH = WORKSPACE_ROOT / "ops" / "feishu_bridge.env.example"
LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS = 45
HOMEBREW_INSTALL_TIMEOUT_SECONDS = 1800
HOMEBREW_INSTALL_SHELL = 'NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
DEFAULT_COMMAND_PATH_HINTS = (
    "/opt/homebrew/bin",
    "/opt/homebrew/sbin",
    "/usr/local/bin",
    "/usr/local/sbin",
    str(Path.home() / ".local" / "bin"),
)
MACOS_AUTO_INSTALL_FEATURES = ("feishu", "knowledge-base", "opencli", "weixin", "electron")


@dataclass
class SiteConfig:
    product_name: str
    workspace_root: Path
    memory_root: Path
    operator_name: str
    timezone: str
    launchagent_prefix: str
    feishu_enabled: bool
    electron_enabled: bool


def app_candidates(name: str) -> list[Path]:
    return [
        Path("/Applications") / f"{name}.app",
        Path.home() / "Applications" / f"{name}.app",
    ]


def app_installed(name: str) -> bool:
    return any(candidate.exists() for candidate in app_candidates(name))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def host_platform() -> str:
    if sys.platform == "darwin":
        return "macos"
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform.startswith("win"):
        return "windows"
    return sys.platform


def is_macos() -> bool:
    return host_platform() == "macos"


def resolve_root(raw: object, default: Path) -> Path:
    if raw in (None, "", "auto"):
        return default.resolve()
    path = Path(str(raw)).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (WORKSPACE_ROOT / path).resolve()


def ensure_yaml_available() -> None:
    if yaml is not None:
        return
    raise SystemExit(
        "PyYAML is required to read control/site.yaml. "
        "Run `python3 ops/bootstrap_workspace_hub.py install-python-deps` "
        "or `python3 ops/bootstrap_workspace_hub.py setup` first."
    )


def default_site_config() -> SiteConfig:
    return SiteConfig(
        product_name="Codex Hub",
        workspace_root=WORKSPACE_ROOT.resolve(),
        memory_root=DEFAULT_MEMORY_ROOT,
        operator_name="",
        timezone="Asia/Shanghai",
        launchagent_prefix="com.codexhub",
        feishu_enabled=False,
        electron_enabled=True,
    )


def load_site_config() -> SiteConfig:
    if yaml is None:
        return default_site_config()
    if not SITE_CONFIG_PATH.exists():
        return default_site_config()
    raw = yaml.safe_load(SITE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    site = raw.get("site") or {}
    return SiteConfig(
        product_name=str(site.get("product_name") or default_site_config().product_name),
        workspace_root=resolve_root(site.get("workspace_root"), default_site_config().workspace_root),
        memory_root=resolve_root(site.get("memory_root"), default_site_config().memory_root),
        operator_name=str(site.get("operator_name") or ""),
        timezone=str(site.get("timezone") or default_site_config().timezone),
        launchagent_prefix=str(site.get("launchagent_prefix") or default_site_config().launchagent_prefix),
        feishu_enabled=bool(site.get("feishu_enabled", default_site_config().feishu_enabled)),
        electron_enabled=bool(site.get("electron_enabled", default_site_config().electron_enabled)),
    )


def required_workspace_dirs(workspace_root: Path) -> list[Path]:
    return [
        workspace_root / "runtime",
        workspace_root / "logs",
        workspace_root / "reports" / "system",
        workspace_root / "reports" / "ops",
        workspace_root / "projects",
    ]


def required_memory_dirs(memory_root: Path) -> list[Path]:
    return [
        memory_root / "01_working",
        memory_root / "02_episodic" / "daily",
        memory_root / "03_semantic" / "projects",
        memory_root / "03_semantic" / "systems",
        memory_root / "07_dashboards",
        memory_root / "07_dashboards" / "materials",
    ]


def ensure_dirs(paths: list[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def seed_memory_template(memory_root: Path) -> dict[str, object]:
    if memory_root.resolve() == MEMORY_TEMPLATE_ROOT:
        return {
            "seeded": False,
            "skipped": True,
            "reason": "runtime_uses_template_root",
            "template_root": str(MEMORY_TEMPLATE_ROOT),
            "memory_root": str(memory_root),
        }
    if not MEMORY_TEMPLATE_ROOT.exists():
        return {
            "seeded": False,
            "skipped": True,
            "reason": "template_root_missing",
            "template_root": str(MEMORY_TEMPLATE_ROOT),
            "memory_root": str(memory_root),
        }
    copied: list[str] = []
    for source in sorted(MEMORY_TEMPLATE_ROOT.rglob("*")):
        if source.name == ".DS_Store":
            continue
        relative = source.relative_to(MEMORY_TEMPLATE_ROOT)
        destination = memory_root / relative
        if source.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            continue
        shutil.copy2(source, destination)
        copied.append(str(destination))
    return {
        "seeded": bool(copied),
        "copied_count": len(copied),
        "template_root": str(MEMORY_TEMPLATE_ROOT),
        "memory_root": str(memory_root),
        "copied": copied,
    }


def write_codex_config(site: SiteConfig) -> None:
    writable_roots = [
        site.workspace_root,
        site.memory_root,
        site.workspace_root / "projects",
    ]
    rendered = "\n".join(
        [
            "#:schema https://developers.openai.com/codex/config-schema.json",
            "# This file is generated by `python3 ops/bootstrap_workspace_hub.py init`.",
            "",
            'approval_policy = "on-request"',
            'sandbox_mode = "workspace-write"',
            'web_search = "cached"',
            "allow_login_shell = true",
            'personality = "pragmatic"',
            "",
            "[sandbox_workspace_write]",
            "network_access = true",
            "writable_roots = [",
            *[f'  "{path}",' for path in writable_roots],
            "]",
            "exclude_tmpdir_env_var = false",
            "exclude_slash_tmp = false",
            "",
            "[features]",
            "shell_snapshot = true",
            "sqlite = true",
            "unified_exec = true",
            "",
        ]
    )
    CODEX_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CODEX_CONFIG_PATH.write_text(rendered, encoding="utf-8")


def command_available(name: str) -> bool:
    return shutil.which(name, path=command_env()["PATH"]) is not None


def command_env() -> dict[str, str]:
    env = dict(os.environ)
    existing = [segment for segment in str(env.get("PATH") or "").split(os.pathsep) if segment]
    merged: list[str] = []
    for segment in [*DEFAULT_COMMAND_PATH_HINTS, *existing]:
        if segment not in merged:
            merged.append(segment)
    env["PATH"] = os.pathsep.join(merged)
    return env


def detect_system_package_manager() -> str:
    if command_available("brew"):
        return "brew"
    if command_available("apt-get"):
        return "apt"
    if command_available("winget"):
        return "winget"
    if command_available("choco"):
        return "choco"
    return ""


def codex_auth_path() -> Path:
    return Path.home() / ".codex" / "auth.json"


def codex_auth_ready() -> bool:
    path = codex_auth_path()
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return isinstance(payload, dict) and bool(payload)


def lark_cli_skills_installed() -> bool:
    if not LARK_CLI_SKILLS_ROOT.exists():
        return False
    return any(path.name.startswith("lark-") for path in LARK_CLI_SKILLS_ROOT.iterdir())


def python_module_status() -> dict[str, bool]:
    return {module: importlib.util.find_spec(module) is not None for module, _ in PYTHON_DEPENDENCIES}


def missing_python_packages() -> list[str]:
    status = python_module_status()
    return [package for module, package in PYTHON_DEPENDENCIES if not status.get(module, False)]


def node_runtime_ready() -> bool:
    return all(command_available(name) for name in ("node", "npm", "npx"))


def feature_tool_status() -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, item in FEATURE_TOOL_GROUPS.items():
        commands = {name: command_available(name) for name in item.get("commands", ())}
        apps = {name: app_installed(name) for name in item.get("apps", ())}
        ready = all(commands.values()) and all(apps.values())
        payload[key] = {
            "label": str(item.get("label", "")).strip(),
            "commands": commands,
            "apps": apps,
            "ready": ready,
            "install_hint": str(item.get("install_hint", "")).strip(),
        }
    return payload


def build_system_install_command(group_id: str, *, manager: str) -> list[str]:
    group = SYSTEM_PACKAGE_GROUPS.get(group_id, {})
    payload = group.get(manager, {})
    if manager == "brew":
        formula = list(payload.get("formula", []))
        cask = list(payload.get("cask", []))
        commands: list[list[str]] = []
        if formula:
            commands.append(["brew", "install", *formula])
        if cask:
            commands.append(["brew", "install", "--cask", *cask])
        flattened: list[str] = []
        for command in commands:
            if flattened:
                flattened.extend(["&&"])
            flattened.extend(command)
        return flattened
    if manager == "apt":
        packages = list(payload.get("packages", []))
        return ["sudo", "apt-get", "install", "-y", *packages] if packages else []
    if manager == "winget":
        ids = list(payload.get("ids", []))
        if not ids:
            return []
        # Emit one command line flattened for display; execution helper will split into multiple installs.
        flattened: list[str] = []
        for package_id in ids:
            if flattened:
                flattened.extend(["&&"])
            flattened.extend(["winget", "install", "--id", package_id, "--exact", "--accept-package-agreements", "--accept-source-agreements"])
        return flattened
    if manager == "choco":
        packages = list(payload.get("packages", []))
        return ["choco", "install", "-y", *packages] if packages else []
    return []


def install_homebrew(*, dry_run: bool = False) -> dict[str, Any]:
    if not is_macos():
        return {"ok": False, "skipped": True, "reason": "not_macos"}
    if command_available("brew"):
        return {"ok": True, "installed": False, "skipped": True, "manager": "brew"}
    command = ["/bin/bash", "-lc", HOMEBREW_INSTALL_SHELL]
    if dry_run:
        return {"ok": True, "installed": False, "dry_run": True, "manager": "brew", "command": command}
    result = run_command_with_timeout(command, WORKSPACE_ROOT, timeout_seconds=HOMEBREW_INSTALL_TIMEOUT_SECONDS)
    ok = int(result.get("returncode") or 0) == 0 and command_available("brew")
    return {"ok": ok, "installed": ok, "manager": "brew", "result": result}


def execute_system_install(group_id: str, *, manager: str, dry_run: bool = False) -> dict[str, Any]:
    group = SYSTEM_PACKAGE_GROUPS.get(group_id, {})
    payload = group.get(manager, {})
    if not payload:
        return {"ok": False, "group_id": group_id, "manager": manager, "reason": "manager_not_supported"}
    if dry_run:
        return {
            "ok": True,
            "group_id": group_id,
            "manager": manager,
            "dry_run": True,
            "command": build_system_install_command(group_id, manager=manager),
        }
    results: list[dict[str, object]] = []
    if manager == "brew":
        for command in filter(None, [payload.get("formula"), payload.get("cask")]):
            if command == payload.get("formula"):
                cmd = ["brew", "install", *list(command)]
            else:
                cmd = ["brew", "install", "--cask", *list(command)]
            if len(cmd) > 2:
                results.append(run_command(cmd, WORKSPACE_ROOT))
    elif manager == "apt":
        cmd = ["sudo", "apt-get", "install", "-y", *list(payload.get("packages", []))]
        results.append(run_command(cmd, WORKSPACE_ROOT))
    elif manager == "winget":
        for package_id in payload.get("ids", []):
            cmd = ["winget", "install", "--id", package_id, "--exact", "--accept-package-agreements", "--accept-source-agreements"]
            results.append(run_command(cmd, WORKSPACE_ROOT))
    elif manager == "choco":
        cmd = ["choco", "install", "-y", *list(payload.get("packages", []))]
        results.append(run_command(cmd, WORKSPACE_ROOT))
    ok = all(int(item.get("returncode") or 0) == 0 for item in results)
    return {"ok": ok, "group_id": group_id, "manager": manager, "results": results}


def install_system_group(group_id: str, *, manager: str = "", dry_run: bool = False) -> dict[str, Any]:
    if group_id == "node_runtime" and not dry_run and node_runtime_ready():
        return {"ok": True, "group_id": group_id, "manager": str(manager or detect_system_package_manager()).strip(), "skipped": True}
    resolved_manager = str(manager or detect_system_package_manager()).strip()
    bootstrap_result: dict[str, Any] = {"skipped": True}
    if not resolved_manager and is_macos():
        bootstrap_result = install_homebrew(dry_run=dry_run)
        if bool(bootstrap_result.get("ok")):
            resolved_manager = "brew"
    if not resolved_manager:
        return {
            "ok": False,
            "group_id": group_id,
            "manager": "",
            "reason": "package_manager_unavailable",
            "package_manager_bootstrap": bootstrap_result,
        }
    payload = execute_system_install(group_id, manager=resolved_manager, dry_run=dry_run)
    payload["package_manager_bootstrap"] = bootstrap_result
    return payload


def install_npm_global_package(package_name: str, *, dry_run: bool = False) -> dict[str, Any]:
    command = ["npm", "install", "-g", package_name]
    if dry_run:
        return {"ok": True, "dry_run": True, "command": command, "package": package_name}
    result = run_command(command, WORKSPACE_ROOT)
    return {
        "ok": int(result.get("returncode") or 0) == 0,
        "package": package_name,
        "result": result,
    }


def feature_doctor(feature: str, site: SiteConfig) -> dict[str, Any]:
    normalized = str(feature or "").strip().lower()
    if normalized not in FEATURE_SURFACES:
        raise SystemExit(f"unknown feature: {feature}")
    status = current_bootstrap_status(site)
    surface = FEATURE_SURFACES[normalized]
    checks: list[dict[str, object]] = []
    ready = True
    install_actions: list[str] = []

    if normalized == "feishu":
        summary = status.get("feishu_setup") if isinstance(status.get("feishu_setup"), dict) else {}
        checks.extend(
            [
                {"name": "site_feishu_enabled", "ok": bool(site.feishu_enabled), "note": "site.yaml feishu_enabled"},
                {"name": "lark_cli", "ok": bool(status["commands"].get("lark_cli")), "note": "official CLI installed"},
                {"name": "lark_cli_configured", "ok": bool((status.get("feishu_cli") or {}).get("configured")), "note": "CLI config ready"},
                {"name": "object_ops_ready", "ok": bool(summary.get("object_ops_ready")), "note": "Feishu object operations ready"},
                {"name": "coco_bridge_ready", "ok": bool(summary.get("coco_bridge_ready")), "note": "assistant bridge credentials synced"},
                {"name": "full_ready", "ok": bool(summary.get("full_ready")), "note": "full Feishu path ready"},
            ]
        )
        install_actions.append("python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app")
    elif normalized == "knowledge-base":
        tools = (status.get("feature_tools") or {}).get("knowledge_base_pdf_ocr", {})
        checks.append({"name": "ocr_tools_ready", "ok": bool(tools.get("ready")), "note": "tesseract / ocrmypdf / pdftoppm"})
        kb = status.get("knowledge_base") if isinstance(status.get("knowledge_base"), dict) else {}
        checks.append(
            {
                "name": "knowledge_bootstrap",
                "ok": int(((kb.get("knowledge_bootstrap") or {}).get("returncode")) or 0) == 0,
                "note": "knowledge intake bootstrap",
            }
        )
        install_actions.append("python3 ops/bootstrap_workspace_hub.py install-feature --feature knowledge-base")
    elif normalized == "opencli":
        tools = (status.get("feature_tools") or {}).get("opencli_browser", {})
        checks.append({"name": "browser_ready", "ok": bool(tools.get("ready")), "note": "opencli + Google Chrome runtime"})
        install_actions.append("python3 ops/bootstrap_workspace_hub.py install-feature --feature opencli")
    elif normalized == "weixin":
        checks.append({"name": "node", "ok": bool(status["commands"].get("node")), "note": "node available"})
        checks.append({"name": "npm", "ok": bool(status["commands"].get("npm")), "note": "npm available"})
        checks.append({"name": "voice_helper", "ok": (WORKSPACE_ROOT / "bridge" / "weixin_voice_to_wav.mjs").exists(), "note": "voice helper present"})
        checks.append({"name": "bridge_dependencies", "ok": (WORKSPACE_ROOT / "bridge" / "node_modules").exists(), "note": "bridge npm dependencies installed"})
        checks.append({"name": "openai_sdk", "ok": bool(status["python_modules"].get("openai")), "note": "voice transcription SDK"})
        install_actions.append("python3 ops/bootstrap_workspace_hub.py install-feature --feature weixin")
    elif normalized == "electron":
        checks.append({"name": "node", "ok": bool(status["commands"].get("node")), "note": "node available"})
        checks.append({"name": "npm", "ok": bool(status["commands"].get("npm")), "note": "npm available"})
        checks.append({"name": "package_json", "ok": (WORKSPACE_ROOT / "apps" / "electron-console" / "package.json").exists(), "note": "electron package present"})
        checks.append({"name": "electron_dependencies", "ok": (WORKSPACE_ROOT / "apps" / "electron-console" / "node_modules").exists(), "note": "electron npm dependencies installed"})
        install_actions.append("python3 ops/bootstrap_workspace_hub.py install-feature --feature electron")

    ready = all(bool(item.get("ok")) for item in checks)
    return {
        "feature": normalized,
        "label": surface["label"],
        "ready": ready,
        "checks": checks,
        "installer": surface.get("installer", ""),
        "install_actions": install_actions,
        "package_manager": detect_system_package_manager(),
    }


def install_feature(feature: str, site: SiteConfig, *, dry_run: bool = False) -> dict[str, Any]:
    normalized = str(feature or "").strip().lower()
    if normalized not in FEATURE_SURFACES:
        raise SystemExit(f"unknown feature: {feature}")
    manager = detect_system_package_manager()
    if normalized == "feishu":
        runtime_result = install_system_group("node_runtime", manager=manager, dry_run=dry_run)
        if not bool(runtime_result.get("ok")):
            return {"feature": normalized, "installer": "install-feishu-cli", "runtime_result": runtime_result, "ok": False}
        if dry_run:
            return {
                "feature": normalized,
                "dry_run": True,
                "installer": "setup-feishu-cli",
                "runtime_result": runtime_result,
                "command": ["python3", "ops/bootstrap_workspace_hub.py", "setup-feishu-cli", "--create-feishu-app"],
                "ok": True,
            }
        payload = install_feishu_cli_tooling(force=False, install_skills=True)
        ok = not result_failed(payload.get("cli")) and not result_failed(payload.get("skills"))
        return {"feature": normalized, "installer": "install-feishu-cli", "runtime_result": runtime_result, "result": payload, "ok": ok}
    if normalized == "knowledge-base":
        tools = (feature_tool_status().get("knowledge_base_pdf_ocr") or {}) if not dry_run else {}
        if bool(tools.get("ready")):
            return {"feature": normalized, "installer": "system-deps", "skipped": True, "ok": True}
        system_result = install_system_group("knowledge_base_pdf_ocr", manager=manager, dry_run=dry_run)
        return {"feature": normalized, "installer": "system-deps", "system_result": system_result, "ok": bool(system_result.get("ok", False))}
    if normalized == "opencli":
        tools = (feature_tool_status().get("opencli_browser") or {}) if not dry_run else {}
        if bool(tools.get("ready")):
            return {"feature": normalized, "installer": "opencli-runtime", "skipped": True, "ok": True}
        runtime_result = install_system_group("node_runtime", manager=manager, dry_run=dry_run)
        browser_result = install_system_group("opencli_browser", manager=manager or str(runtime_result.get("manager") or ""), dry_run=dry_run)
        cli_result = install_npm_global_package(OPENCLI_NPM_PACKAGE, dry_run=dry_run) if runtime_result.get("ok") else {"ok": False, "reason": "node_runtime_failed"}
        ok = bool(runtime_result.get("ok")) and bool(browser_result.get("ok")) and bool(cli_result.get("ok"))
        return {
            "feature": normalized,
            "installer": "opencli-runtime",
            "runtime_result": runtime_result,
            "browser_result": browser_result,
            "cli_result": cli_result,
            "ok": ok,
        }
    if normalized == "weixin":
        command = ["npm", "install"]
        if not dry_run and (WORKSPACE_ROOT / "bridge" / "node_modules").exists():
            return {"feature": normalized, "installer": "npm-install-bridge", "skipped": True, "ok": True}
        runtime_result = install_system_group("node_runtime", manager=manager, dry_run=dry_run)
        if not bool(runtime_result.get("ok")):
            return {"feature": normalized, "installer": "npm-install-bridge", "runtime_result": runtime_result, "ok": False}
        if dry_run:
            return {
                "feature": normalized,
                "dry_run": True,
                "command": command,
                "cwd": str(WORKSPACE_ROOT / "bridge"),
                "runtime_result": runtime_result,
                "ok": True,
            }
        result = run_command(command, WORKSPACE_ROOT / "bridge")
        return {"feature": normalized, "installer": "npm-install-bridge", "runtime_result": runtime_result, "result": result, "ok": int(result.get("returncode") or 0) == 0}
    if normalized == "electron":
        command = ["npm", "install"]
        cwd = WORKSPACE_ROOT / "apps" / "electron-console"
        if not dry_run and (cwd / "node_modules").exists():
            return {"feature": normalized, "installer": "npm-install-electron", "skipped": True, "ok": True}
        runtime_result = install_system_group("node_runtime", manager=manager, dry_run=dry_run)
        if not bool(runtime_result.get("ok")):
            return {"feature": normalized, "installer": "npm-install-electron", "runtime_result": runtime_result, "ok": False}
        if dry_run:
            return {
                "feature": normalized,
                "dry_run": True,
                "command": command,
                "cwd": str(cwd),
                "runtime_result": runtime_result,
                "ok": True,
            }
        result = run_command(command, cwd)
        return {"feature": normalized, "installer": "npm-install-electron", "runtime_result": runtime_result, "result": result, "ok": int(result.get("returncode") or 0) == 0}
    return {"feature": normalized, "ok": False, "reason": "unsupported_feature"}


def maybe_auto_install_macos_features(site: SiteConfig, *, install: bool) -> dict[str, Any]:
    if not install:
        return {"installed": False, "skipped": True}
    if not is_macos():
        return {"installed": False, "skipped": True, "reason": "not_macos"}
    results: dict[str, Any] = {}
    for feature in MACOS_AUTO_INSTALL_FEATURES:
        results[feature] = install_feature(feature, site, dry_run=False)
    ok = all(bool((results.get(feature) or {}).get("ok")) for feature in MACOS_AUTO_INSTALL_FEATURES)
    return {"installed": ok, "results": results, "features": list(MACOS_AUTO_INSTALL_FEATURES)}


def run_command(cmd: list[str], cwd: Path) -> dict[str, object]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        env=command_env(),
    )
    return {
        "command": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
    }


def run_command_with_timeout(cmd: list[str], cwd: Path, *, timeout_seconds: int) -> dict[str, object]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
            env=command_env(),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = str(exc.stdout or "").strip()
        stderr = str(exc.stderr or "").strip()
        message = stderr or stdout or f"timed out after {timeout_seconds}s"
        return {
            "command": cmd,
            "returncode": 124,
            "stdout": stdout,
            "stderr": message,
            "timed_out": True,
            "timeout_seconds": timeout_seconds,
        }
    return {
        "command": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "timeout_seconds": timeout_seconds,
    }


def result_failed(result: object) -> bool:
    if not isinstance(result, dict):
        return False
    if result.get("skipped"):
        return False
    return int(result.get("returncode") or 0) != 0


def _extract_json_blob(text: str) -> dict[str, object]:
    raw = str(text or "").strip()
    if not raw:
        return {}
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        payload = json.loads(raw[start : end + 1])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_env_text(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in str(text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _upsert_env_value(text: str, key: str, value: str) -> str:
    line = f"{key}={value}"
    pattern = re.compile(rf"(?m)^{re.escape(key)}=.*$")
    if pattern.search(text):
        return pattern.sub(line, text)
    prefix = text if text.endswith("\n") or not text else text + "\n"
    return f"{prefix}{line}\n"


def _current_lark_cli_config() -> dict[str, object]:
    if not command_available("lark-cli"):
        return {"available": False, "configured": False}
    result = run_command(["lark-cli", "config", "show"], WORKSPACE_ROOT)
    payload = _extract_json_blob(str(result.get("stdout") or ""))
    app_id = str(payload.get("appId") or "").strip()
    return {
        "available": True,
        "configured": bool(app_id),
        "app_id": app_id,
        "brand": str(payload.get("brand") or "").strip(),
        "lang": str(payload.get("lang") or "").strip(),
        "source": str(LARK_CLI_CONFIG_PATH),
        "raw": payload,
    }


def _parse_created_app_credentials(*outputs: str) -> dict[str, str]:
    combined = "\n".join(str(item or "") for item in outputs if item)
    app_id_match = re.search(r"\b(cli_[a-z0-9]+)\b", combined)
    secret_match = re.search(r"(?im)(?:app[_ ]secret|appsecret)\s*[:=]\s*([A-Za-z0-9._-]{8,})", combined)
    values: dict[str, str] = {}
    if app_id_match:
        values["app_id"] = str(app_id_match.group(1))
    if secret_match:
        candidate = str(secret_match.group(1)).strip()
        if "*" not in candidate:
            values["app_secret"] = candidate
    return values


def _sync_feishu_bridge_credentials(config_init_result: dict[str, object] | None = None) -> dict[str, object]:
    current = _current_lark_cli_config()
    app_id = str(current.get("app_id") or "").strip()
    parsed = _parse_created_app_credentials(
        str((config_init_result or {}).get("stdout") or ""),
        str((config_init_result or {}).get("stderr") or ""),
    )
    app_secret_from_create = str(parsed.get("app_secret") or "").strip()
    existing_text = ""
    if FEISHU_BRIDGE_ENV_PATH.exists():
        existing_text = FEISHU_BRIDGE_ENV_PATH.read_text(encoding="utf-8")
    elif FEISHU_BRIDGE_ENV_EXAMPLE_PATH.exists():
        existing_text = FEISHU_BRIDGE_ENV_EXAMPLE_PATH.read_text(encoding="utf-8")
    existing_values = _parse_env_text(existing_text)
    existing_secret = str(existing_values.get("FEISHU_APP_SECRET") or "").strip()
    effective_secret = existing_secret or app_secret_from_create
    rendered = existing_text
    changed = False
    if app_id and str(existing_values.get("FEISHU_APP_ID") or "").strip() != app_id:
        rendered = _upsert_env_value(rendered, "FEISHU_APP_ID", app_id)
        changed = True
    if effective_secret and str(existing_values.get("FEISHU_APP_SECRET") or "").strip() != effective_secret:
        rendered = _upsert_env_value(rendered, "FEISHU_APP_SECRET", effective_secret)
        changed = True
    created = bool(app_id and not FEISHU_BRIDGE_ENV_PATH.exists())
    if changed or created:
        FEISHU_BRIDGE_ENV_PATH.parent.mkdir(parents=True, exist_ok=True)
        FEISHU_BRIDGE_ENV_PATH.write_text(rendered, encoding="utf-8")
    return {
        "env_path": str(FEISHU_BRIDGE_ENV_PATH),
        "app_id": app_id,
        "app_id_synced": bool(app_id),
        "app_secret_synced": bool(effective_secret),
        "app_secret_source": "existing_env" if existing_secret else ("config_init_output" if app_secret_from_create else ""),
        "bridge_credentials_ready": bool(app_id and effective_secret),
        "changed": changed or created,
    }


def _run_feishu_auth_status() -> dict[str, object]:
    result = run_command([sys.executable, "ops/feishu_agent.py", "auth", "status"], WORKSPACE_ROOT)
    payload = _extract_json_blob(str(result.get("stdout") or ""))
    result["status"] = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    return result


def _write_bootstrap_status(payload: dict[str, object]) -> None:
    BOOTSTRAP_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BOOTSTRAP_STATUS_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _read_bootstrap_status() -> dict[str, object]:
    if not BOOTSTRAP_STATUS_PATH.exists():
        return {}
    try:
        payload = json.loads(BOOTSTRAP_STATUS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _infer_local_ready(site: SiteConfig, payload: dict[str, object] | None = None) -> bool:
    candidate = payload or {}
    explicit = candidate.get("local_ready")
    phase = str(candidate.get("setup_phase") or "").strip()
    if explicit is not None and (bool(explicit) or phase):
        return bool(explicit)
    required = [
        CODEX_CONFIG_PATH,
        site.workspace_root / "runtime",
        site.workspace_root / "logs",
        site.workspace_root / "reports",
        site.memory_root / "PROJECT_REGISTRY.md",
        site.memory_root / "ACTIVE_PROJECTS.md",
        site.memory_root / "NEXT_ACTIONS.md",
    ]
    return all(path.exists() for path in required)


def _refresh_bootstrap_status(site: SiteConfig, payload: dict[str, object]) -> dict[str, object]:
    refreshed = dict(payload)
    dynamic = bootstrap_status_payload(site)
    for key in ("commands", "auth", "python_modules", "feature_tools", "apps", "files", "feishu_cli", "feishu_setup"):
        refreshed[key] = dynamic.get(key, refreshed.get(key))
    refreshed["manual_actions"] = build_manual_actions(site, refreshed)
    refreshed["checked_at"] = utc_now()
    refreshed["local_ready"] = _infer_local_ready(site, refreshed)
    refreshed["feishu_ready"] = bool(
        isinstance(refreshed.get("feishu_setup"), dict) and refreshed["feishu_setup"].get("full_ready")
    )
    if not refreshed.get("setup_phase") and refreshed["local_ready"]:
        refreshed["setup_phase"] = "complete"
    return refreshed


def current_bootstrap_status(site: SiteConfig) -> dict[str, object]:
    existing_payload = _read_bootstrap_status()
    if existing_payload:
        return _refresh_bootstrap_status(site, existing_payload)
    payload = bootstrap_status_payload(site)
    payload["manual_actions"] = build_manual_actions(site, payload)
    return payload


def _ensure_site_feishu_enabled() -> dict[str, object]:
    if not SITE_CONFIG_PATH.exists():
        return {"changed": False, "feishu_enabled": False, "path": str(SITE_CONFIG_PATH), "reason": "site_config_missing"}
    text = SITE_CONFIG_PATH.read_text(encoding="utf-8")
    if "feishu_enabled: true" in text:
        return {"changed": False, "feishu_enabled": True, "path": str(SITE_CONFIG_PATH)}
    if yaml is not None:
        payload = yaml.safe_load(text) or {}
        site = payload.get("site") or {}
        site["feishu_enabled"] = True
        payload["site"] = site
        SITE_CONFIG_PATH.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        return {"changed": True, "feishu_enabled": True, "path": str(SITE_CONFIG_PATH)}
    updated = re.sub(r"(?m)^(\s*feishu_enabled:\s*)false\s*$", r"\1true", text, count=1)
    if updated == text:
        updated = text.rstrip("\n") + "\n  feishu_enabled: true\n"
    SITE_CONFIG_PATH.write_text(updated, encoding="utf-8")
    return {"changed": True, "feishu_enabled": True, "path": str(SITE_CONFIG_PATH)}


def _launchagent_results_ready(results: dict[str, object]) -> bool:
    if not isinstance(results, dict):
        return False
    for item in results.values():
        if not isinstance(item, dict):
            continue
        if int(item.get("returncode") or 0) != 0:
            return False
    return True


def bootstrap_status_payload(site: SiteConfig) -> dict[str, object]:
    feishu_cli_status = _current_lark_cli_config()
    feishu_auth_status = _run_feishu_auth_status() if site.feishu_enabled and command_available("python3") else {"status": {}}
    return {
        "generated_at": utc_now(),
        "product_name": site.product_name,
        "workspace_root": str(site.workspace_root),
        "memory_root": str(site.memory_root),
        "memory_template_root": str(MEMORY_TEMPLATE_ROOT),
        "operator_name": site.operator_name,
        "timezone": site.timezone,
        "host_platform": host_platform(),
        "launchagent_prefix": site.launchagent_prefix,
        "feishu_enabled": site.feishu_enabled,
        "electron_enabled": site.electron_enabled,
        "commands": {
            "python3": command_available("python3"),
            "node": command_available("node"),
            "npm": command_available("npm"),
            "npx": command_available("npx"),
            "codex": command_available("codex"),
            "lark_cli": command_available("lark-cli"),
        },
        "auth": {
            "codex_cli_logged_in": codex_auth_ready(),
            "codex_auth_path": str(codex_auth_path()),
        },
        "python_modules": python_module_status(),
        "feature_tools": feature_tool_status(),
        "apps": {
            "obsidian": app_installed("Obsidian"),
            "codex_desktop": app_installed("Codex"),
            "google_chrome": app_installed("Google Chrome"),
        },
        "files": {
            "site_config": SITE_CONFIG_PATH.exists(),
            "codex_config": CODEX_CONFIG_PATH.exists(),
            "feishu_resources": (site.workspace_root / "control" / "feishu_resources.yaml").exists(),
            "feishu_bridge_env_example": (site.workspace_root / "ops" / "feishu_bridge.env.example").exists(),
            "lark_cli_config": LARK_CLI_CONFIG_PATH.exists(),
            "memory_template_root": MEMORY_TEMPLATE_ROOT.exists(),
        },
        "manual_actions": [],
        "local_ready": False,
        "feishu_ready": False,
        "setup_phase": "pending",
        "sync_results": {},
        "launchagents": {
            "installed": False,
        },
        "feishu_bridge": {
            "installed": False,
        },
        "feishu_cli": {
            "installed": command_available("lark-cli"),
            "configured": bool(feishu_cli_status.get("configured")),
            "skills_installed": lark_cli_skills_installed(),
            "app_id": str(feishu_cli_status.get("app_id") or ""),
            "brand": str(feishu_cli_status.get("brand") or ""),
        },
        "feishu_setup": (feishu_auth_status or {}).get("status") or {},
    }


def build_manual_actions(site: SiteConfig, payload: dict[str, object]) -> list[str]:
    actions: list[str] = []
    commands = payload.get("commands", {})
    module_status = payload.get("python_modules", {})
    apps = payload.get("apps", {})
    missing_packages = [package for module, package in PYTHON_DEPENDENCIES if not module_status.get(module, False)]
    if missing_packages:
        actions.append(
            "Install Python dependencies with `python3 ops/bootstrap_workspace_hub.py install-python-deps`."
        )
    if not commands.get("codex"):
        actions.append("Install Codex CLI and complete `codex login`.")
    elif not bool((payload.get("auth") or {}).get("codex_cli_logged_in")):
        actions.append("Run `codex login` once if this machine has not authenticated yet.")
    if not apps.get("codex_desktop"):
        actions.append("Optional but recommended: install the Codex desktop app for direct-open workspace sessions.")
    if not apps.get("obsidian"):
        actions.append("Optional but strongly recommended: install Obsidian for full Vault browsing and `obsidian://` deep-link support.")
    feature_tools = payload.get("feature_tools") if isinstance(payload.get("feature_tools"), dict) else {}
    for key, item in feature_tools.items():
        if not isinstance(item, dict) or item.get("ready"):
            continue
        label = str(item.get("label", key)).strip()
        install_hint = str(item.get("install_hint", "")).strip()
        if install_hint:
            actions.append(f"Optional for `{label}`: {install_hint}.")
    if is_macos():
        macos_auto_installs = payload.get("macos_auto_installs") if isinstance(payload.get("macos_auto_installs"), dict) else {}
        if macos_auto_installs and not macos_auto_installs.get("installed"):
            actions.append("macOS automatic dependency install did not fully finish; rerun `python3 ops/bootstrap_workspace_hub.py setup` or use `doctor-feature` to inspect the missing surface.")
    if site.feishu_enabled:
        feishu_setup = payload.get("feishu_setup") if isinstance(payload.get("feishu_setup"), dict) else {}
        actions.append("Fill `control/feishu_resources.yaml` with your app, calendar, table, and alias defaults.")
        if not feishu_setup.get("full_ready"):
            actions.append(
                "Run `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app` to install the official Feishu CLI, create/configure the app, sync bridge credentials, and complete login."
            )
        if feishu_setup.get("object_ops_ready") and not feishu_setup.get("coco_bridge_ready"):
            actions.append(
                "Feishu object operations are ready, but the assistant bridge credentials are still incomplete. Finish the bridge app secret sync before enabling the live Feishu bridge."
            )
        elif not feishu_setup.get("object_ops_ready"):
            actions.append("Complete the Feishu login flow until `object_ops_ready=true`.")
        actions.extend(
            [
                "Ensure your workspace assistant Feishu app scopes are approved and published.",
                "Optionally install the Feishu bridge launch agent with `python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge` after Feishu reports `full_ready=true`.",
            ]
        )
    else:
        if not commands.get("lark_cli"):
            actions.append(
                "If you want Feishu later, first install the official tooling with `python3 ops/bootstrap_workspace_hub.py install-feishu-cli`."
            )
        actions.append(
            "When you are ready to connect Feishu, run `python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app` so Codex can guide app creation, credential sync, and login."
        )
    return actions


def maybe_sync(site: SiteConfig, skip_sync: bool) -> dict[str, object]:
    if skip_sync:
        return {"skipped": True}
    commands = {
        "refresh_index": ["python3", "ops/codex_memory.py", "refresh-index"],
        "rebuild_dashboards": ["python3", "ops/codex_dashboard_sync.py", "rebuild-all"],
        "verify_consistency": ["python3", "ops/codex_dashboard_sync.py", "verify-consistency"],
    }
    return {name: run_command(cmd, site.workspace_root) for name, cmd in commands.items()}


def maybe_bootstrap_knowledge_base(site: SiteConfig) -> dict[str, object]:
    commands = {
        "knowledge_bootstrap": ["python3", "ops/knowledge_intake.py", "bootstrap"],
        "discover_projects": ["python3", "ops/codex_memory.py", "discover-projects"],
    }
    return {name: run_command(cmd, site.workspace_root) for name, cmd in commands.items()}


def install_python_dependencies(*, force: bool = False) -> dict[str, object]:
    missing = missing_python_packages()
    if not missing and not force:
        return {
            "installed": False,
            "skipped": True,
            "missing_packages": [],
            "requirements_path": str(REQUIREMENTS_PATH),
        }
    if REQUIREMENTS_PATH.exists():
        command = [sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_PATH)]
    else:
        command = [sys.executable, "-m", "pip", "install", *missing]
    result = run_command(command, WORKSPACE_ROOT)
    result["installed"] = result.get("returncode") == 0
    result["missing_packages"] = missing
    result["requirements_path"] = str(REQUIREMENTS_PATH)
    result["python_modules_after"] = python_module_status()
    return result


def install_feishu_cli_tooling(*, force: bool = False, install_skills: bool = True) -> dict[str, object]:
    results: dict[str, object] = {
        "cli": {"installed": False, "skipped": True},
        "skills": {"installed": False, "skipped": not install_skills},
    }
    if force or not command_available("lark-cli"):
        results["cli"] = run_command(["npm", "install", "-g", LARK_CLI_PACKAGE], WORKSPACE_ROOT)
        results["cli"]["installed"] = results["cli"].get("returncode") == 0
        results["cli"]["skipped"] = False
    else:
        results["cli"] = {"installed": True, "skipped": True}
    if install_skills:
        if force or not lark_cli_skills_installed():
            results["skills"] = run_command(
                ["npx", "skills", "add", LARK_CLI_SKILLS_REPO, "-y", "-g"],
                WORKSPACE_ROOT,
            )
            results["skills"]["installed"] = results["skills"].get("returncode") == 0
            results["skills"]["skipped"] = False
        else:
            results["skills"] = {"installed": True, "skipped": True}
    return results


def setup_feishu_cli(
    *,
    create_app: bool,
    install: bool,
    install_skills: bool,
    login_user: bool = True,
    run_doctor: bool = False,
) -> dict[str, object]:
    results: dict[str, object] = {
        "install": {"skipped": True},
        "config_init": {"skipped": True},
        "credentials_sync": {"skipped": True},
        "auth_login": {"skipped": not login_user},
        "auth_status": {"skipped": True},
        "doctor": {"skipped": True},
    }
    if install:
        results["install"] = install_feishu_cli_tooling(force=False, install_skills=install_skills)
        cli_result = (results["install"] or {}).get("cli", {})
        if isinstance(cli_result, dict) and int(cli_result.get("returncode") or 0) != 0:
            return results
    if create_app or not LARK_CLI_CONFIG_PATH.exists():
        config_cmd = ["lark-cli", "config", "init"]
        if create_app:
            config_cmd.append("--new")
        results["config_init"] = run_command(config_cmd, WORKSPACE_ROOT)
        if int(results["config_init"].get("returncode") or 0) != 0:
            return results
    results["credentials_sync"] = _sync_feishu_bridge_credentials(
        results["config_init"] if isinstance(results.get("config_init"), dict) else None
    )
    if login_user:
        auth_cmd = [sys.executable, "ops/feishu_agent.py", "auth", "login"]
        results["auth_login"] = run_command(auth_cmd, WORKSPACE_ROOT)
        if int(results["auth_login"].get("returncode") or 0) != 0:
            results["auth_status"] = _run_feishu_auth_status()
            return results
    results["auth_status"] = _run_feishu_auth_status()
    if run_doctor:
        results["doctor"] = run_command(["lark-cli", "doctor"], WORKSPACE_ROOT)
    status = results.get("auth_status")
    auth_payload = status.get("status") if isinstance(status, dict) else {}
    results["summary"] = auth_payload if isinstance(auth_payload, dict) else {}
    return results


def install_feishu_cli_only(*, install_skills: bool) -> dict[str, object]:
    results: dict[str, object] = {
        "install": install_feishu_cli_tooling(force=False, install_skills=install_skills),
        "config_init": {"skipped": True},
        "credentials_sync": {"skipped": True},
        "auth_login": {"skipped": True},
        "auth_status": {"skipped": True},
        "doctor": {"skipped": True},
    }
    status = _run_feishu_auth_status()
    results["auth_status"] = status
    auth_payload = status.get("status") if isinstance(status, dict) else {}
    results["summary"] = auth_payload if isinstance(auth_payload, dict) else {}
    return results


def maybe_install_launchagents(site: SiteConfig, install: bool) -> dict[str, object]:
    if not install:
        return {"installed": False, "skipped": True}
    results = {
        "watcher": run_command_with_timeout(
            ["python3", "ops/codex_session_watcher.py", "install-launchagent", "--poll-interval", "300"],
            site.workspace_root,
            timeout_seconds=LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS,
        ),
        "dashboard_sync": run_command_with_timeout(
            ["python3", "ops/codex_dashboard_sync.py", "install-launchagent", "--interval", "900"],
            site.workspace_root,
            timeout_seconds=LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS,
        ),
        "health_check": run_command_with_timeout(
            ["python3", "ops/workspace_hub_health_check.py", "install-launchagent", "--interval", "14400"],
            site.workspace_root,
            timeout_seconds=LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS,
        ),
        "feishu_projection": run_command_with_timeout(
            ["python3", "ops/feishu_projection.py", "install-launchagent", "--interval", "900"],
            site.workspace_root,
            timeout_seconds=LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS,
        ),
        "knowledge_intake": run_command_with_timeout(
            ["python3", "ops/knowledge_intake.py", "install-launchagent", "--hour", "4", "--minute", "0"],
            site.workspace_root,
            timeout_seconds=LAUNCHAGENT_INSTALL_TIMEOUT_SECONDS,
        ),
    }
    return {"installed": _launchagent_results_ready(results), "results": results}


def maybe_install_feishu_bridge(site: SiteConfig, install: bool) -> dict[str, object]:
    if not install:
        return {"installed": False, "skipped": True}
    if not site.feishu_enabled:
        return {"installed": False, "skipped": True, "reason": "feishu_disabled"}
    console_root = site.workspace_root / "apps" / "electron-console"
    package_json = console_root / "package.json"
    if not package_json.exists():
        return {"installed": False, "skipped": True, "reason": "electron_console_missing"}
    install_result = run_command(["npm", "install"], console_root)
    bridge_result = run_command(["node", "coco-bridge-service.js", "install-launchagent"], console_root)
    return {
        "installed": install_result.get("returncode") == 0 and bridge_result.get("returncode") == 0,
        "results": {
            "npm_install": install_result,
            "bridge_install": bridge_result,
        },
    }


def perform_init(site: SiteConfig, args: argparse.Namespace) -> dict[str, object]:
    feishu_requested = bool(getattr(args, "setup_feishu_cli", False) or getattr(args, "create_feishu_app", False))
    if feishu_requested:
        site_update = _ensure_site_feishu_enabled()
        if site_update.get("feishu_enabled"):
            site = replace(site, feishu_enabled=True)
    else:
        site_update = {"changed": False, "feishu_enabled": site.feishu_enabled, "path": str(SITE_CONFIG_PATH)}
    ensure_dirs(required_workspace_dirs(site.workspace_root))
    ensure_dirs(required_memory_dirs(site.memory_root))
    memory_template = seed_memory_template(site.memory_root)
    write_codex_config(site)

    payload = bootstrap_status_payload(site)
    payload["memory_template"] = memory_template
    payload["site_updates"] = {"feishu_enabled": site_update}
    payload["setup_phase"] = "initializing"
    _write_bootstrap_status(payload)
    payload["macos_auto_installs"] = maybe_auto_install_macos_features(
        site,
        install=not bool(getattr(args, "skip_macos_auto_install", False)),
    )
    payload["setup_phase"] = "macos_dependency_install_complete"
    _write_bootstrap_status(payload)
    payload["knowledge_base"] = maybe_bootstrap_knowledge_base(site)
    payload["setup_phase"] = "knowledge_bootstrap_complete"
    _write_bootstrap_status(payload)
    payload["sync_results"] = maybe_sync(site, skip_sync=args.skip_sync)
    payload["local_ready"] = True
    payload["setup_phase"] = "local_runtime_ready"
    _write_bootstrap_status(payload)
    payload["launchagents"] = maybe_install_launchagents(site, install=args.install_launchagents)
    payload["setup_phase"] = "launchagent_install_complete"
    _write_bootstrap_status(payload)
    if bool(getattr(args, "setup_feishu_cli", False) or getattr(args, "create_feishu_app", False)):
        payload["feishu_cli"] = setup_feishu_cli(
            create_app=getattr(args, "create_feishu_app", False),
            install=True,
            install_skills=True,
            login_user=True,
        )
        payload["feishu_ready"] = bool(
            isinstance(payload.get("feishu_cli"), dict)
            and isinstance(payload["feishu_cli"].get("summary"), dict)
            and payload["feishu_cli"]["summary"].get("full_ready")
        )
        payload["setup_phase"] = "feishu_setup_complete"
    elif bool(getattr(args, "install_feishu_cli", False)):
        payload["feishu_cli"] = install_feishu_cli_only(install_skills=True)
        payload["feishu_ready"] = bool(
            isinstance(payload.get("feishu_cli"), dict)
            and isinstance(payload["feishu_cli"].get("summary"), dict)
            and payload["feishu_cli"]["summary"].get("full_ready")
        )
        payload["setup_phase"] = "feishu_tooling_install_complete"
    else:
        payload["feishu_cli"] = {
            "install": {"skipped": True},
            "config_init": {"skipped": True},
            "credentials_sync": {"skipped": True},
            "auth_login": {"skipped": True},
            "auth_status": {"skipped": True},
            "doctor": {"skipped": True},
            "summary": {},
        }
        payload["feishu_ready"] = False
        payload["setup_phase"] = "local_runtime_ready"
    _write_bootstrap_status(payload)
    payload["feishu_bridge"] = maybe_install_feishu_bridge(site, install=args.install_feishu_bridge)
    payload["setup_phase"] = "complete"
    payload["manual_actions"] = build_manual_actions(site, payload)
    _write_bootstrap_status(payload)
    return payload


def cmd_init(args: argparse.Namespace) -> int:
    site = load_site_config()
    payload = perform_init(site, args)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_install_python_deps(args: argparse.Namespace) -> int:
    payload = install_python_dependencies(force=args.force)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload.get("installed", True) else 0


def cmd_setup(args: argparse.Namespace) -> int:
    dependency_result = {"installed": False, "skipped": True}
    if not args.skip_python_deps:
        dependency_result = install_python_dependencies(force=args.force_python_deps)
        if dependency_result.get("returncode", 0) != 0:
            print(
                json.dumps(
                    {"ok": False, "stage": "install-python-deps", "python_dependencies": dependency_result},
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return int(dependency_result.get("returncode") or 1)
    site = load_site_config()
    bootstrap_result = perform_init(site, args)
    acceptance_result = {"skipped": True}
    acceptance_rc = 0
    if not args.skip_acceptance:
        acceptance_result = run_command([sys.executable, "ops/accept_product.py", "run"], site.workspace_root)
        acceptance_rc = int(acceptance_result.get("returncode") or 0)
    feishu_cli_result = bootstrap_result.get("feishu_cli")
    feishu_cli_ok = True
    if isinstance(feishu_cli_result, dict):
        install_payload = feishu_cli_result.get("install")
        if isinstance(install_payload, dict):
            feishu_cli_ok = feishu_cli_ok and not result_failed(install_payload.get("cli"))
            feishu_cli_ok = feishu_cli_ok and not result_failed(install_payload.get("skills"))
        if bool(getattr(args, "setup_feishu_cli", False) or getattr(args, "create_feishu_app", False)):
            feishu_cli_ok = feishu_cli_ok and not result_failed(feishu_cli_result.get("config_init"))
            feishu_cli_ok = feishu_cli_ok and not result_failed(feishu_cli_result.get("credentials_sync"))
            feishu_cli_ok = feishu_cli_ok and not result_failed(feishu_cli_result.get("auth_login"))
            feishu_cli_ok = feishu_cli_ok and not result_failed(feishu_cli_result.get("doctor"))
            summary = feishu_cli_result.get("summary")
            if isinstance(summary, dict):
                feishu_cli_ok = feishu_cli_ok and bool(summary.get("full_ready"))
    payload = {
        "ok": acceptance_rc == 0 and dependency_result.get("returncode", 0) == 0 and feishu_cli_ok,
        "python_dependencies": dependency_result,
        "bootstrap": bootstrap_result,
        "acceptance": acceptance_result,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


def cmd_status(_: argparse.Namespace) -> int:
    site = load_site_config()
    payload = current_bootstrap_status(site)
    if BOOTSTRAP_STATUS_PATH.exists():
        _write_bootstrap_status(payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_install_feishu_cli(args: argparse.Namespace) -> int:
    payload = install_feishu_cli_tooling(force=args.force, install_skills=not args.skip_skills)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    cli_rc = int(((payload.get("cli") or {}).get("returncode")) or 0)
    skills_rc = int(((payload.get("skills") or {}).get("returncode")) or 0)
    return 0 if cli_rc == 0 and skills_rc == 0 else 1


def cmd_setup_feishu_cli(args: argparse.Namespace) -> int:
    site_update = _ensure_site_feishu_enabled()
    payload = setup_feishu_cli(
        create_app=args.create_feishu_app,
        install=not args.skip_install,
        install_skills=not args.skip_skills,
        login_user=not bool(getattr(args, "skip_login", False)),
        run_doctor=bool(getattr(args, "run_lark_cli_doctor", False)),
    )
    site = load_site_config()
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    status_payload = bootstrap_status_payload(site)
    status_payload["site_updates"] = {"feishu_enabled": site_update}
    existing_payload = {}
    if BOOTSTRAP_STATUS_PATH.exists():
        try:
            existing_payload = json.loads(BOOTSTRAP_STATUS_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing_payload = {}
    status_payload["local_ready"] = _infer_local_ready(site, existing_payload if isinstance(existing_payload, dict) else {})
    status_payload["setup_phase"] = "feishu_setup_complete"
    status_payload["feishu_cli"] = payload.get("install") if isinstance(payload.get("install"), dict) else status_payload.get("feishu_cli", {})
    status_payload["feishu_setup"] = summary if isinstance(summary, dict) else {}
    status_payload["feishu_ready"] = bool(isinstance(summary, dict) and summary.get("full_ready"))
    status_payload["manual_actions"] = build_manual_actions(site, status_payload)
    _write_bootstrap_status(status_payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    for key in ("config_init", "credentials_sync", "auth_login", "doctor"):
        result = payload.get(key)
        if isinstance(result, dict) and not result.get("skipped"):
            if int(result.get("returncode") or 0) != 0:
                return 1
    install_payload = payload.get("install")
    if isinstance(install_payload, dict):
        for key in ("cli", "skills"):
            result = install_payload.get(key)
            if isinstance(result, dict) and not result.get("skipped"):
                if int(result.get("returncode") or 0) != 0:
                    return 1
    if isinstance(summary, dict) and not summary.get("full_ready"):
        return 1
    return 0


def cmd_doctor_feature(args: argparse.Namespace) -> int:
    site = load_site_config()
    payload = feature_doctor(args.feature, site)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if bool(payload.get("ready")) else 1


def cmd_install_system_deps(args: argparse.Namespace) -> int:
    detected_manager = detect_system_package_manager()
    manager = str(args.manager or detected_manager).strip()
    payload = execute_system_install(args.group, manager=manager, dry_run=args.dry_run)
    payload["detected_manager"] = detected_manager
    payload["resolved_manager"] = manager
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if bool(payload.get("ok")) else 1


def cmd_install_feature(args: argparse.Namespace) -> int:
    site = load_site_config()
    payload = install_feature(args.feature, site, dry_run=args.dry_run)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if bool(payload.get("ok")) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap the portable Codex Hub product workspace.")
    sub = parser.add_subparsers(dest="command", required=True)

    install_deps_parser = sub.add_parser(
        "install-python-deps",
        help="Install the Python packages required by bootstrap, acceptance, and bridge helpers.",
    )
    install_deps_parser.add_argument("--force", action="store_true", help="Run pip install even if the modules already exist.")
    install_deps_parser.set_defaults(func=cmd_install_python_deps)

    install_feishu_cli_parser = sub.add_parser(
        "install-feishu-cli",
        help="Install the official Feishu CLI and official Lark skills used by the public Codex Hub build.",
    )
    install_feishu_cli_parser.add_argument("--force", action="store_true", help="Reinstall lark-cli even if it already exists.")
    install_feishu_cli_parser.add_argument("--skip-skills", action="store_true", help="Skip installing the official Lark skills bundle.")
    install_feishu_cli_parser.set_defaults(func=cmd_install_feishu_cli)

    doctor_feature_parser = sub.add_parser(
        "doctor-feature",
        help="Check whether a specific feature surface is ready and show the next install action if it is not.",
    )
    doctor_feature_parser.add_argument("--feature", choices=sorted(FEATURE_SURFACES), required=True, help="Feature surface to inspect.")
    doctor_feature_parser.set_defaults(func=cmd_doctor_feature)

    install_system_deps_parser = sub.add_parser(
        "install-system-deps",
        help="Install the system package bundle behind a feature surface, with auto-detected package manager support.",
    )
    install_system_deps_parser.add_argument("--group", choices=sorted(SYSTEM_PACKAGE_GROUPS), required=True, help="System dependency group to install.")
    install_system_deps_parser.add_argument("--manager", choices=SUPPORTED_PACKAGE_MANAGERS, help="Override the detected package manager.")
    install_system_deps_parser.add_argument("--dry-run", action="store_true", help="Show the resolved install command without executing it.")
    install_system_deps_parser.set_defaults(func=cmd_install_system_deps)

    install_feature_parser = sub.add_parser(
        "install-feature",
        help="Install the primary prerequisites for a feature surface without running the full product bootstrap.",
    )
    install_feature_parser.add_argument("--feature", choices=sorted(FEATURE_SURFACES), required=True, help="Feature surface to install.")
    install_feature_parser.add_argument("--dry-run", action="store_true", help="Show the resolved install action without executing it.")
    install_feature_parser.set_defaults(func=cmd_install_feature)

    setup_feishu_cli_parser = sub.add_parser(
        "setup-feishu-cli",
        help="Install lark-cli, create/configure the Feishu app, sync bridge credentials, and complete the public Feishu login flow.",
    )
    setup_feishu_cli_parser.add_argument("--create-feishu-app", action="store_true", help="Create a new Feishu app in the browser before configuration.")
    setup_feishu_cli_parser.add_argument("--skip-install", action="store_true", help="Skip the lark-cli and official skills installation stage.")
    setup_feishu_cli_parser.add_argument("--skip-skills", action="store_true", help="Skip installing the official Lark skills bundle.")
    setup_feishu_cli_parser.add_argument(
        "--login-lark-cli-user",
        action="store_true",
        help="Deprecated compatibility flag. The default setup now runs the unified Codex Hub Feishu login flow automatically.",
    )
    setup_feishu_cli_parser.add_argument(
        "--run-lark-cli-doctor",
        action="store_true",
        help="Optional advanced step: run `lark-cli doctor` after setup for raw CLI diagnostics.",
    )
    setup_feishu_cli_parser.add_argument(
        "--skip-login",
        action="store_true",
        help="Skip the final Feishu login step. This leaves the setup incomplete and is intended only for debugging.",
    )
    setup_feishu_cli_parser.set_defaults(func=cmd_setup_feishu_cli)

    init_parser = sub.add_parser("init", help="Initialize runtime folders, generated config, and optional launchagents.")
    init_parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip refresh-index / rebuild-all / verify-consistency during bootstrap.",
    )
    init_parser.add_argument(
        "--skip-macos-auto-install",
        action="store_true",
        help="On macOS, skip the default Homebrew/npm dependency auto-install stage.",
    )
    init_parser.add_argument(
        "--install-launchagents",
        action="store_true",
        help="Install launchd tasks for watcher, dashboard sync, health check, and Feishu projection.",
    )
    init_parser.add_argument(
        "--install-feishu-bridge",
        action="store_true",
        help="Run npm install for Electron and install the Feishu bridge launch agent.",
    )
    init_parser.add_argument(
        "--install-feishu-cli",
        action="store_true",
        help="Install the official Feishu CLI and official Lark skills during bootstrap without configuring or logging into Feishu.",
    )
    init_parser.add_argument(
        "--setup-feishu-cli",
        action="store_true",
        help="Install and configure the official Feishu CLI during bootstrap, then run the unified Feishu login flow.",
    )
    init_parser.add_argument(
        "--create-feishu-app",
        action="store_true",
        help="When setting up the official Feishu CLI, create a new app in the browser first.",
    )
    init_parser.set_defaults(func=cmd_init)

    setup_parser = sub.add_parser(
        "setup",
        help="Install Python dependencies, run bootstrap, and then run acceptance in one command.",
    )
    setup_parser.add_argument(
        "--skip-python-deps",
        action="store_true",
        help="Skip the Python dependency installation stage.",
    )
    setup_parser.add_argument(
        "--force-python-deps",
        action="store_true",
        help="Force reinstall Python dependencies before bootstrap.",
    )
    setup_parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip refresh-index / rebuild-all / verify-consistency during bootstrap.",
    )
    setup_parser.add_argument(
        "--skip-macos-auto-install",
        action="store_true",
        help="On macOS, skip the default Homebrew/npm dependency auto-install stage.",
    )
    setup_parser.add_argument(
        "--install-launchagents",
        action="store_true",
        help="Install launchd tasks for watcher, dashboard sync, health check, and Feishu projection.",
    )
    setup_parser.add_argument(
        "--install-feishu-bridge",
        action="store_true",
        help="Run npm install for Electron and install the Feishu bridge launch agent.",
    )
    setup_parser.add_argument(
        "--install-feishu-cli",
        action="store_true",
        help="Install the official Feishu CLI and official Lark skills during setup without configuring or logging into Feishu.",
    )
    setup_parser.add_argument(
        "--setup-feishu-cli",
        action="store_true",
        help="Install and configure the official Feishu CLI during setup, then run the unified Feishu login flow.",
    )
    setup_parser.add_argument(
        "--create-feishu-app",
        action="store_true",
        help="When setting up the official Feishu CLI, create a new app in the browser first.",
    )
    setup_parser.add_argument(
        "--skip-acceptance",
        action="store_true",
        help="Skip the final acceptance run after bootstrap.",
    )
    setup_parser.set_defaults(func=cmd_setup)

    status_parser = sub.add_parser("status", help="Show the latest bootstrap status snapshot.")
    status_parser.set_defaults(func=cmd_status)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - acceptance should report missing deps, not crash before reporting
    yaml = None  # type: ignore


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
SITE_CONFIG_PATH = WORKSPACE_ROOT / "control" / "site.yaml"
BOOTSTRAP_STATUS_PATH = WORKSPACE_ROOT / "runtime" / "bootstrap-status.json"
REPORT_PATH = WORKSPACE_ROOT / "reports" / "system" / "product-acceptance-latest.md"
DEFAULT_MEMORY_ROOT = (WORKSPACE_ROOT.parent / "memory.local").resolve()
REQUIRED_PYTHON_MODULES = (
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
    },
    "opencli_browser": {
        "label": "OpenCLI browser execution",
        "apps": ("Google Chrome",),
    },
}
REQUIRED_BOOTSTRAP_COMMANDS = (
    "doctor-feature",
    "install-system-deps",
    "install-feature",
)
REQUIRED_FEATURE_SURFACES = (
    "feishu",
    "knowledge-base",
    "opencli",
    "weixin",
    "electron",
)
REQUIRED_SYSTEM_GROUPS = (
    "knowledge_base_pdf_ocr",
    "opencli_browser",
)

FORBIDDEN_PATTERNS = [
    "/workspace-hub-data/Codex-Workspace-Memory",
    "/workspace-hub-worktrees/",
    "/Users/" + Path.home().name + "/workspace-hub",
    "/Users/" + Path.home().name + "/Codex Hub",
    "com." + Path.home().name + ".",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def codex_auth_path() -> Path:
    return Path.home() / ".codex" / "auth.json"


def load_site() -> tuple[Path, Path, bool]:
    if yaml is None:
        return WORKSPACE_ROOT.resolve(), DEFAULT_MEMORY_ROOT, False
    raw = yaml.safe_load(SITE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    site = raw.get("site") or {}
    workspace_root = site.get("workspace_root")
    memory_root = site.get("memory_root")
    feishu_enabled = bool(site.get("feishu_enabled", False))
    if workspace_root in (None, "", "auto"):
        workspace = WORKSPACE_ROOT
    else:
        workspace = Path(str(workspace_root)).expanduser()
    if memory_root in (None, "", "auto"):
        memory = DEFAULT_MEMORY_ROOT
    else:
        memory = Path(str(memory_root)).expanduser()
    return workspace.resolve(), memory.resolve(), feishu_enabled


def check_paths(workspace_root: Path, memory_root: Path, *, feishu_enabled: bool) -> list[tuple[str, bool, str]]:
    required = [
        workspace_root / "README.md",
        workspace_root / "AGENTS.md",
        workspace_root / "MEMORY_SYSTEM.md",
        workspace_root / "ops" / "bootstrap_workspace_hub.py",
        workspace_root / "ops" / "accept_product.py",
        workspace_root / "ops" / "lark_cli_backend.py",
        workspace_root / "ops" / "knowledge_intake.py",
        workspace_root / "ops" / "opencli_agent.py",
        workspace_root / "ops" / "opencli_policy.py",
        workspace_root / "ops" / "opencli_session_warmup.mjs",
        workspace_root / "ops" / "background_job_executor.py",
        workspace_root / "ops" / "board_job_projector.py",
        workspace_root / "ops" / "workspace_job_schema.py",
        workspace_root / "ops" / "workspace_wake_broker.py",
        workspace_root / "ops" / "feishu_outbound_gateway.py",
        workspace_root / "ops" / "weixin_bridge.py",
        workspace_root / "ops" / "growth_truth.py",
        workspace_root / "ops" / "start-codex",
        workspace_root / "control" / "site.yaml",
        workspace_root / "control" / "obsidian_web_clipper_templates" / "knowledge_base" / "README.md",
        workspace_root / ".codex" / "config.toml",
        workspace_root / "apps" / "electron-console" / "coco-bridge-service.js",
        workspace_root / "bridge" / "feishu" / "gateway.js",
        workspace_root / "bridge" / "weixin_voice_to_wav.mjs",
        memory_root / "PROJECT_REGISTRY.md",
        memory_root / "ACTIVE_PROJECTS.md",
        memory_root / "NEXT_ACTIONS.md",
        memory_root / "07_dashboards" / "HOME.md",
    ]
    if feishu_enabled:
        required.extend(
            [
                workspace_root / "ops" / "feishu_bridge.env.example",
                workspace_root / "control" / "feishu_resources.yaml",
            ]
        )
    return [(str(path), path.exists(), "required path") for path in required]


def check_commands(*, feishu_enabled: bool) -> list[tuple[str, bool, str]]:
    checks = [
        ("python3", shutil.which("python3") is not None, "required command"),
        ("node", shutil.which("node") is not None, "required command"),
        ("npm", shutil.which("npm") is not None, "required command"),
        ("npx", shutil.which("npx") is not None, "required command"),
        ("codex", shutil.which("codex") is not None, "required command"),
    ]
    if feishu_enabled:
        checks.append(("lark-cli", shutil.which("lark-cli") is not None, "required when Feishu is enabled"))
    return checks


def check_apps() -> list[tuple[str, bool, str]]:
    return [
        ("Codex.app", (Path("/Applications") / "Codex.app").exists(), "recommended desktop app"),
        ("Obsidian.app", (Path("/Applications") / "Obsidian.app").exists(), "recommended for full Vault browsing"),
        ("Google Chrome.app", (Path("/Applications") / "Google Chrome.app").exists(), "optional for OpenCLI browser execution"),
    ]


def check_python_modules() -> list[tuple[str, bool, str]]:
    return [
        (package, importlib.util.find_spec(module) is not None, f"required Python package ({module})")
        for module, package in REQUIRED_PYTHON_MODULES
    ]


def check_feature_tools() -> list[tuple[str, bool, str]]:
    checks: list[tuple[str, bool, str]] = []
    for key, item in FEATURE_TOOL_GROUPS.items():
        label = str(item.get("label", key)).strip()
        for command in item.get("commands", ()):
            checks.append((command, shutil.which(command) is not None, f"optional for {label}"))
        for app in item.get("apps", ()):
            checks.append((app, (Path("/Applications") / f"{app}.app").exists(), f"optional for {label}"))
    return checks


def check_bootstrap_cli_contract() -> list[tuple[str, bool, str]]:
    bootstrap_path = WORKSPACE_ROOT / "ops" / "bootstrap_workspace_hub.py"
    spec = importlib.util.spec_from_file_location("_codex_hub_bootstrap_contract", bootstrap_path)
    if spec is None or spec.loader is None:
        return [("bootstrap-import", False, "unable to load bootstrap contract from file path")]
    bootstrap_module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = bootstrap_module
    try:
        spec.loader.exec_module(bootstrap_module)
    except Exception as exc:
        sys.modules.pop(spec.name, None)
        return [("bootstrap-import", False, f"unable to import bootstrap contract: {type(exc).__name__}")]
    finally:
        sys.modules.pop(spec.name, None)

    parser = bootstrap_module.build_parser()
    commands: set[str] = set()
    for action in getattr(parser, "_actions", []):
        choices = getattr(action, "choices", None)
        if isinstance(choices, dict):
            commands.update(str(name) for name in choices)
    feature_surfaces = set(getattr(bootstrap_module, "FEATURE_SURFACES", {}).keys())
    system_groups = set(getattr(bootstrap_module, "SYSTEM_PACKAGE_GROUPS", {}).keys())

    checks: list[tuple[str, bool, str]] = []
    for command in REQUIRED_BOOTSTRAP_COMMANDS:
        checks.append((command, command in commands, "required bootstrap CLI command"))
    for feature in REQUIRED_FEATURE_SURFACES:
        checks.append((f"feature:{feature}", feature in feature_surfaces, "required feature-specific surface"))
    for group in REQUIRED_SYSTEM_GROUPS:
        checks.append((f"group:{group}", group in system_groups, "required system dependency bundle"))
    return checks


def read_codex_auth_status() -> dict[str, object]:
    path = codex_auth_path()
    if not path.exists():
        return {"ready": False, "path": str(path), "reason": "missing"}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"ready": False, "path": str(path), "reason": f"invalid-json:{type(exc).__name__}"}
    return {"ready": isinstance(payload, dict) and bool(payload), "path": str(path), "reason": "ok"}


def scan_forbidden(root: Path) -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path == Path(__file__).resolve():
            continue
        if path == REPORT_PATH.resolve():
            continue
        if any(
            part in {"node_modules", "__pycache__", ".pytest_cache", ".mypy_cache", ".next", "runtime", "logs", ".codex"}
            for part in path.parts
        ):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for pattern in FORBIDDEN_PATTERNS:
            if pattern in text:
                hits.append((str(path), pattern))
    return hits


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


def read_feishu_auth_status(workspace_root: Path) -> dict[str, object]:
    proc = subprocess.run(
        [sys.executable, "ops/feishu_agent.py", "auth", "status"],
        cwd=str(workspace_root),
        text=True,
        capture_output=True,
        check=False,
    )
    payload = _extract_json_blob(proc.stdout)
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "status": result if isinstance(result, dict) else {},
    }


def infer_bootstrap_local_ready(workspace_root: Path, memory_root: Path, payload: dict[str, object] | None = None) -> bool:
    candidate = payload or {}
    explicit = candidate.get("local_ready")
    phase = str(candidate.get("setup_phase") or "").strip()
    if explicit is not None and (bool(explicit) or phase):
        return bool(explicit)
    required = [
        workspace_root / ".codex" / "config.toml",
        workspace_root / "runtime",
        workspace_root / "logs",
        workspace_root / "reports",
        memory_root / "PROJECT_REGISTRY.md",
        memory_root / "ACTIVE_PROJECTS.md",
        memory_root / "NEXT_ACTIONS.md",
    ]
    return all(path.exists() for path in required)


def write_report(results: dict[str, object]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Codex Hub 产品化备份验收",
        "",
        f"- 生成时间：{results['generated_at']}",
        f"- 结果：{'PASS' if results['passed'] else 'FAIL'}",
        "",
        "## 路径检查",
        "",
    ]
    for item in results["path_checks"]:
        path, ok, note = item
        lines.append(f"- {'OK' if ok else 'FAIL'} `{path}`：{note}")
    lines.extend(["", "## 命令检查", ""])
    for item in results["command_checks"]:
        name, ok, note = item
        lines.append(f"- {'OK' if ok else 'FAIL'} `{name}`：{note}")
    lines.extend(["", "## Auth Checks", ""])
    auth = results.get("codex_auth_status") or {}
    if isinstance(auth, dict):
        lines.append(
            f"- {'OK' if auth.get('ready') else 'FAIL'} Codex 登录：`{auth.get('path', '')}` | reason=`{auth.get('reason', '')}`"
        )
    lines.extend(["", "## Python Package Checks", ""])
    for item in results["python_module_checks"]:
        name, ok, note = item
        lines.append(f"- {'OK' if ok else 'FAIL'} `{name}`：{note}")
    lines.extend(["", "## App Checks", ""])
    for item in results.get("app_checks", []):
        name, ok, note = item
        lines.append(f"- {'OK' if ok else 'MISS'} `{name}`：{note}")
    lines.extend(["", "## Feature Tool Checks", ""])
    for item in results.get("feature_tool_checks", []):
        name, ok, note = item
        lines.append(f"- {'OK' if ok else 'MISS'} `{name}`：{note}")
    lines.extend(["", "## Bootstrap CLI Contract", ""])
    for item in results.get("bootstrap_cli_checks", []):
        name, ok, note = item
        lines.append(f"- {'OK' if ok else 'FAIL'} `{name}`：{note}")
    lines.extend(["", "## Forbidden Pattern Scan", ""])
    forbidden_hits = results["forbidden_hits"]
    if forbidden_hits:
        for path, pattern in forbidden_hits:
            lines.append(f"- FAIL `{path}` 包含 `{pattern}`")
    else:
        lines.append("- OK 未发现个人现网路径或个人邮箱痕迹")
    lines.extend(["", "## Bootstrap 状态", ""])
    bootstrap_ok = results["bootstrap_status_exists"]
    lines.append(f"- {'OK' if bootstrap_ok else 'FAIL'} bootstrap 状态文件：`{BOOTSTRAP_STATUS_PATH}`")
    lines.append(
        f"- {'OK' if results.get('bootstrap_local_ready') else 'FAIL'} 本地初始化完成：local_ready=`{results.get('bootstrap_local_ready')}` phase=`{results.get('bootstrap_phase')}`"
    )
    if results.get("feishu_enabled"):
        lines.extend(["", "## Feishu CLI 状态", ""])
        lines.append(
            f"- {'OK' if results['lark_cli_configured'] else 'FAIL'} lark-cli 配置：`{results['lark_cli_config_path']}`"
        )
        feishu_auth = results.get("feishu_auth_status") or {}
        if isinstance(feishu_auth, dict):
            status = feishu_auth.get("status") or {}
            if isinstance(status, dict):
                lines.append(
                    f"- {'OK' if status.get('object_ops_ready') else 'FAIL'} Feishu 对象能力登录：object_ops_ready=`{status.get('object_ops_ready')}`"
                )
                lines.append(
                    f"- {'OK' if status.get('coco_bridge_ready') else 'FAIL'} assistant bridge 凭据同步：coco_bridge_ready=`{status.get('coco_bridge_ready')}`"
                )
                lines.append(
                    f"- {'OK' if status.get('full_ready') else 'FAIL'} Feishu 完整可用：full_ready=`{status.get('full_ready')}`"
                )
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def cmd_run(_: argparse.Namespace) -> int:
    workspace_root, memory_root, feishu_enabled = load_site()
    lark_cli_config_path = Path.home() / ".lark-cli" / "config.json"
    path_checks = check_paths(workspace_root, memory_root, feishu_enabled=feishu_enabled)
    command_checks = check_commands(feishu_enabled=feishu_enabled)
    app_checks = check_apps()
    python_module_checks = check_python_modules()
    feature_tool_checks = check_feature_tools()
    bootstrap_cli_checks = check_bootstrap_cli_contract()
    forbidden_hits = scan_forbidden(workspace_root)
    codex_auth_status = read_codex_auth_status()
    feishu_auth_status = read_feishu_auth_status(workspace_root) if feishu_enabled else {"status": {}}
    feishu_status = feishu_auth_status.get("status") if isinstance(feishu_auth_status, dict) else {}
    feishu_full_ready = bool(isinstance(feishu_status, dict) and feishu_status.get("full_ready"))
    bootstrap_status = {}
    bootstrap_local_ready = False
    bootstrap_phase = ""
    if BOOTSTRAP_STATUS_PATH.exists():
        try:
            bootstrap_status = json.loads(BOOTSTRAP_STATUS_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            bootstrap_status = {}
        if isinstance(bootstrap_status, dict):
            bootstrap_local_ready = infer_bootstrap_local_ready(workspace_root, memory_root, bootstrap_status)
            bootstrap_phase = str(bootstrap_status.get("setup_phase") or "")
    passed = (
        all(ok for _, ok, _ in path_checks)
        and all(ok for _, ok, _ in command_checks)
        and bool(codex_auth_status.get("ready"))
        and all(ok for _, ok, _ in python_module_checks)
        and all(ok for _, ok, _ in bootstrap_cli_checks)
        and not forbidden_hits
        and BOOTSTRAP_STATUS_PATH.exists()
        and bootstrap_local_ready
        and (not feishu_enabled or (lark_cli_config_path.exists() and feishu_full_ready))
    )
    results = {
        "generated_at": utc_now(),
        "workspace_root": str(workspace_root),
        "memory_root": str(memory_root),
        "feishu_enabled": feishu_enabled,
        "path_checks": path_checks,
        "command_checks": command_checks,
        "app_checks": app_checks,
        "codex_auth_status": codex_auth_status,
        "python_module_checks": python_module_checks,
        "feature_tool_checks": feature_tool_checks,
        "bootstrap_cli_checks": bootstrap_cli_checks,
        "forbidden_hits": forbidden_hits,
        "bootstrap_status_exists": BOOTSTRAP_STATUS_PATH.exists(),
        "bootstrap_local_ready": bootstrap_local_ready,
        "bootstrap_phase": bootstrap_phase,
        "lark_cli_config_path": str(lark_cli_config_path),
        "lark_cli_configured": lark_cli_config_path.exists(),
        "feishu_auth_status": feishu_auth_status,
        "passed": passed,
    }
    write_report(results)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run acceptance checks for the portable Codex Hub product backup.")
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run", help="Run product acceptance checks.")
    run_parser.set_defaults(func=cmd_run)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

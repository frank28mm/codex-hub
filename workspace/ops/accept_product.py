#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import shutil
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
REQUIRED_PYTHON_MODULES = (
    ("yaml", "PyYAML"),
    ("docx", "python-docx"),
    ("openpyxl", "openpyxl"),
    ("pypdf", "pypdf"),
    ("qrcode", "qrcode[pil]"),
    ("certifi", "certifi"),
)

FORBIDDEN_PATTERNS = [
    "/Users/" + "frank" + "/workspace-hub",
    "workspace-hub-data/" + "Codex-Workspace-Memory",
    "com." + "frank" + ".",
    "frank" + "@example.com",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_site() -> tuple[Path, Path]:
    if yaml is None:
        return WORKSPACE_ROOT.resolve(), (WORKSPACE_ROOT.parent / "memory").resolve()
    raw = yaml.safe_load(SITE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    site = raw.get("site") or {}
    workspace_root = site.get("workspace_root")
    memory_root = site.get("memory_root")
    if workspace_root in (None, "", "auto"):
        workspace = WORKSPACE_ROOT
    else:
        workspace = Path(str(workspace_root)).expanduser()
    if memory_root in (None, "", "auto"):
        memory = WORKSPACE_ROOT.parent / "memory"
    else:
        memory = Path(str(memory_root)).expanduser()
    return workspace.resolve(), memory.resolve()


def check_paths(workspace_root: Path, memory_root: Path) -> list[tuple[str, bool, str]]:
    required = [
        workspace_root / "README.md",
        workspace_root / "AGENTS.md",
        workspace_root / "MEMORY_SYSTEM.md",
        workspace_root / "ops" / "bootstrap_workspace_hub.py",
        workspace_root / "ops" / "accept_product.py",
        workspace_root / "ops" / "start-codex",
        workspace_root / "control" / "site.yaml",
        workspace_root / ".codex" / "config.toml",
        memory_root / "PROJECT_REGISTRY.md",
        memory_root / "ACTIVE_PROJECTS.md",
        memory_root / "NEXT_ACTIONS.md",
        memory_root / "07_dashboards" / "HOME.md",
    ]
    return [(str(path), path.exists(), "required path") for path in required]


def check_commands() -> list[tuple[str, bool, str]]:
    return [
        ("python3", shutil.which("python3") is not None, "required command"),
        ("node", shutil.which("node") is not None, "required command"),
        ("codex", shutil.which("codex") is not None, "required command"),
    ]


def check_python_modules() -> list[tuple[str, bool, str]]:
    return [
        (package, importlib.util.find_spec(module) is not None, f"required Python package ({module})")
        for module, package in REQUIRED_PYTHON_MODULES
    ]


def scan_forbidden(root: Path) -> list[tuple[str, str]]:
    hits: list[tuple[str, str]] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path == Path(__file__).resolve():
            continue
        if path == REPORT_PATH.resolve():
            continue
        if any(part in {"node_modules", "__pycache__", ".pytest_cache", ".mypy_cache", ".next"} for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        for pattern in FORBIDDEN_PATTERNS:
            if pattern in text:
                hits.append((str(path), pattern))
    return hits


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
    lines.extend(["", "## Python Package Checks", ""])
    for item in results["python_module_checks"]:
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
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def cmd_run(_: argparse.Namespace) -> int:
    workspace_root, memory_root = load_site()
    path_checks = check_paths(workspace_root, memory_root)
    command_checks = check_commands()
    python_module_checks = check_python_modules()
    forbidden_hits = scan_forbidden(workspace_root)
    passed = (
        all(ok for _, ok, _ in path_checks)
        and all(ok for _, ok, _ in command_checks)
        and all(ok for _, ok, _ in python_module_checks)
        and not forbidden_hits
        and BOOTSTRAP_STATUS_PATH.exists()
    )
    results = {
        "generated_at": utc_now(),
        "workspace_root": str(workspace_root),
        "memory_root": str(memory_root),
        "path_checks": path_checks,
        "command_checks": command_checks,
        "python_module_checks": python_module_checks,
        "forbidden_hits": forbidden_hits,
        "bootstrap_status_exists": BOOTSTRAP_STATUS_PATH.exists(),
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

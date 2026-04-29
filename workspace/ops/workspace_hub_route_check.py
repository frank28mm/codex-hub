#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    from ops import workspace_hub_project
except ImportError:  # pragma: no cover
    import workspace_hub_project  # type: ignore

PROJECT_NAME = workspace_hub_project.PROJECT_NAME
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


def command_env() -> dict[str, str]:
    env = os.environ.copy()
    env["WORKSPACE_HUB_CODE_ROOT"] = str(code_root())
    env["WORKSPACE_HUB_ROOT"] = str(expected_workspace_root())
    env["WORKSPACE_HUB_VAULT_ROOT"] = str(expected_vault_root())
    env["WORKSPACE_HUB_PROJECTS_ROOT"] = str(expected_projects_root())
    return env


def reports_root() -> Path:
    return Path(os.environ.get("WORKSPACE_HUB_REPORTS_ROOT", str(workspace_root() / "reports")))


def route_reports_root() -> Path:
    return reports_root() / "ops" / "workspace-hub-routing"


@dataclass(frozen=True)
class RouteCase:
    name: str
    project_name: str
    prompt: str
    expected_scope: str
    expected_board_suffix: str


def default_cases() -> list[RouteCase]:
    # Public route checks stay product-facing; prefix-collision regressions live in unit tests.
    return [
        RouteCase(
            name="workspace-system-project",
            project_name=PROJECT_NAME,
            prompt=f"我们继续聊 {PROJECT_NAME} 项目",
            expected_scope="project",
            expected_board_suffix=f"{PROJECT_NAME}-项目板.md",
        ),
        RouteCase(
            name="workspace-system-legacy-alias",
            project_name="workspace-hub",
            prompt="我们继续聊 workspace-hub 项目",
            expected_scope="project",
            expected_board_suffix=f"{PROJECT_NAME}-项目板.md",
        ),
    ]


def run_json_command(command: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=code_root(),
        env=env or command_env(),
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(result.stdout)


def parse_key_value_output(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def run_start_codex_dry_run(case: RouteCase) -> dict[str, str]:
    env = command_env()
    result = subprocess.run(
        [
            "bash",
            str(code_root() / "ops" / "start-codex"),
            "--project",
            case.project_name,
            "--prompt",
            case.prompt,
            "--dry-run",
        ],
        cwd=code_root(),
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    parsed = parse_key_value_output(result.stdout)
    return {
        "mode": parsed.get("mode", ""),
        "project_name": parsed.get("project", ""),
        "binding_scope": parsed.get("context_scope", ""),
        "board_path": parsed.get("context_board", ""),
    }


def run_app_direct_protocol(case: RouteCase) -> dict[str, str]:
    binding = run_json_command(
        [
            "python3",
            str(code_root() / "ops" / "codex_memory.py"),
            "resolve-board-binding",
            "--project-name",
            case.project_name,
            "--prompt",
            case.prompt,
        ]
    )
    suggestion = run_json_command(
        [
            "python3",
            str(code_root() / "ops" / "codex_context.py"),
            "suggest",
            "--project-name",
            case.project_name,
            "--prompt",
            case.prompt,
        ]
    )
    return {
        "project_name": str(suggestion.get("project_name", case.project_name)),
        "binding_scope": str(binding.get("binding_scope", "")),
        "board_path": str(suggestion.get("board_path", binding.get("binding_board_path", ""))),
    }


def evaluate_case(case: RouteCase) -> dict[str, Any]:
    dry_run = run_start_codex_dry_run(case)
    app_direct = run_app_direct_protocol(case)
    issues: list[str] = []
    expected_project_name = app_direct.get("project_name", case.project_name)

    if dry_run.get("project_name") != expected_project_name:
        issues.append(
            f"start-codex project mismatch: expected `{expected_project_name}` got `{dry_run.get('project_name', '')}`"
        )
    if dry_run.get("binding_scope") != app_direct.get("binding_scope"):
        issues.append(
            f"binding_scope mismatch: start-codex=`{dry_run.get('binding_scope', '')}` app-direct=`{app_direct.get('binding_scope', '')}`"
        )
    if dry_run.get("board_path") != app_direct.get("board_path"):
        issues.append(
            f"board_path mismatch: start-codex=`{dry_run.get('board_path', '')}` app-direct=`{app_direct.get('board_path', '')}`"
        )
    if app_direct.get("binding_scope") != case.expected_scope:
        issues.append(
            f"expected scope `{case.expected_scope}` but got `{app_direct.get('binding_scope', '')}`"
        )
    if not str(app_direct.get("board_path", "")).endswith(case.expected_board_suffix):
        issues.append(
            f"expected board suffix `{case.expected_board_suffix}` but got `{app_direct.get('board_path', '')}`"
        )

    return {
        "name": case.name,
        "project_name": case.project_name,
        "prompt": case.prompt,
        "expected_scope": case.expected_scope,
        "expected_board_suffix": case.expected_board_suffix,
        "start_codex": dry_run,
        "app_direct": app_direct,
        "ok": not issues,
        "issues": issues,
    }


def run_checks(cases: list[RouteCase] | None = None) -> dict[str, Any]:
    selected = cases or default_cases()
    results = [evaluate_case(case) for case in selected]
    return {
        "checked_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "ok": all(item["ok"] for item in results),
        "case_count": len(results),
        "results": results,
    }


def render_report(payload: dict[str, Any]) -> str:
    lines = [
        f"# {PROJECT_NAME} 路由验证报告",
        "",
        f"- 时间：{payload.get('checked_at', '')}",
        f"- 结果：`{'通过' if payload.get('ok') else '失败'}`",
        f"- 用途：验证 `start-codex --dry-run` 与 app 直开协议使用的绑定逻辑是否一致",
        "",
    ]
    for item in payload.get("results", []):
        lines.extend(
            [
                f"## {item['name']}",
                "",
                f"- 项目：`{item['project_name']}`",
                f"- prompt：{item['prompt']}",
                f"- 期望：`{item['expected_scope']}` / `{item['expected_board_suffix']}`",
                f"- `start-codex`：scope=`{item['start_codex'].get('binding_scope', '')}` board=`{item['start_codex'].get('board_path', '')}`",
                f"- app 直开协议：scope=`{item['app_direct'].get('binding_scope', '')}` board=`{item['app_direct'].get('board_path', '')}`",
            ]
        )
        if item["ok"]:
            lines.append("- 结论：通过")
        else:
            lines.append("- 结论：失败")
            for issue in item.get("issues", []):
                lines.append(f"  - {issue}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_report(payload: dict[str, Any]) -> str:
    root = route_reports_root()
    root.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    report_path = root / f"route-check-{stamp}.md"
    latest_path = root / "latest.md"
    text = render_report(payload)
    report_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return str(report_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"Validate {PROJECT_NAME} routing consistency")
    parser.add_argument("--write-report", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = run_checks()
    if args.write_report:
        payload["report_path"] = write_report(payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

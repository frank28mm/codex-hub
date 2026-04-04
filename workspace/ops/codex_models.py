#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import yaml

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover
        tomllib = None  # type: ignore[assignment]


WORKSPACE_ROOT = Path(
    os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))
).resolve()
DEFAULT_SETTINGS_PATH = WORKSPACE_ROOT / "control" / "codex-models.yaml"
USER_CONFIG_PATH = Path.home() / ".codex" / "config.toml"


def _default_choice_catalog() -> list[dict[str, str]]:
    return [
        {"id": "gpt-5-codex", "label": "GPT-5 Codex", "note": "当前 CLI 默认模型"},
        {"id": "gpt-5.4", "label": "GPT-5.4", "note": "与 Codex App 常用模型对齐"},
    ]


def _default_reasoning_catalog() -> list[dict[str, str]]:
    return [
        {"id": "low", "label": "低", "note": "更快，适合直接查询和轻任务"},
        {"id": "medium", "label": "中", "note": "平衡速度与推理深度"},
        {"id": "high", "label": "高", "note": "更深推理，适合复杂任务"},
        {"id": "xhigh", "label": "超高", "note": "最深推理，适合复杂分析和长链任务"},
    ]


def _load_user_config() -> dict[str, Any]:
    if not USER_CONFIG_PATH.exists() or tomllib is None:
        return {}
    try:
        return tomllib.loads(USER_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_settings_file(path: Path | None = None) -> dict[str, Any]:
    target = path or DEFAULT_SETTINGS_PATH
    if not target.exists():
        return {}
    try:
        data = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _normalize_choices(raw_choices: Any, cli_default_model: str) -> list[dict[str, str]]:
    catalog = {item["id"]: dict(item) for item in _default_choice_catalog()}
    if isinstance(raw_choices, list):
        for item in raw_choices:
            if isinstance(item, str):
                catalog.setdefault(item.strip(), {"id": item.strip(), "label": item.strip(), "note": ""})
            elif isinstance(item, dict):
                item_id = str(item.get("id", "")).strip()
                if not item_id:
                    continue
                catalog[item_id] = {
                    "id": item_id,
                    "label": str(item.get("label", item_id)).strip() or item_id,
                    "note": str(item.get("note", "")).strip(),
                }
    if cli_default_model:
        catalog.setdefault(
            cli_default_model,
            {"id": cli_default_model, "label": cli_default_model, "note": "当前 CLI 配置默认模型"},
        )
    return list(catalog.values())


def _normalize_reasoning_choices(raw_choices: Any, cli_default_reasoning: str) -> list[dict[str, str]]:
    catalog = {item["id"]: dict(item) for item in _default_reasoning_catalog()}
    if isinstance(raw_choices, list):
        for item in raw_choices:
            if isinstance(item, str):
                value = item.strip()
                if not value:
                    continue
                catalog.setdefault(value, {"id": value, "label": value, "note": ""})
            elif isinstance(item, dict):
                item_id = str(item.get("id", "")).strip()
                if not item_id:
                    continue
                catalog[item_id] = {
                    "id": item_id,
                    "label": str(item.get("label", item_id)).strip() or item_id,
                    "note": str(item.get("note", "")).strip(),
                }
    if cli_default_reasoning:
        catalog.setdefault(
            cli_default_reasoning,
            {"id": cli_default_reasoning, "label": cli_default_reasoning, "note": "当前 CLI 配置默认推理强度"},
        )
    return list(catalog.values())


def _normalize_default_entry(raw_defaults: dict[str, Any], key: str, *, cli_default_model: str, cli_default_reasoning: str) -> dict[str, str]:
    value = raw_defaults.get(key, "")
    if isinstance(value, dict):
        model = str(value.get("model", "")).strip() or cli_default_model
        reasoning = str(value.get("reasoning_effort", "")).strip() or cli_default_reasoning
        return {"model": model, "reasoning_effort": reasoning}
    if isinstance(value, str):
        return {"model": value.strip() or cli_default_model, "reasoning_effort": cli_default_reasoning}
    return {"model": cli_default_model, "reasoning_effort": cli_default_reasoning}


def summarize_settings(path: Path | None = None) -> dict[str, Any]:
    user_config = _load_user_config()
    settings = _load_settings_file(path)
    cli_default_model = str(user_config.get("model", "")).strip()
    cli_default_reasoning = str(user_config.get("model_reasoning_effort", "")).strip()
    cli_default_provider = str(user_config.get("model_provider", "")).strip()
    defaults = settings.get("defaults", {}) if isinstance(settings.get("defaults"), dict) else {}
    workspace_defaults = _normalize_default_entry(
        defaults,
        "workspace",
        cli_default_model=cli_default_model,
        cli_default_reasoning=cli_default_reasoning,
    )
    feishu_defaults = _normalize_default_entry(
        defaults,
        "feishu",
        cli_default_model=workspace_defaults["model"],
        cli_default_reasoning=workspace_defaults["reasoning_effort"],
    )
    electron_defaults = _normalize_default_entry(
        defaults,
        "electron",
        cli_default_model=workspace_defaults["model"],
        cli_default_reasoning=workspace_defaults["reasoning_effort"],
    )
    choices = _normalize_choices(settings.get("choices"), cli_default_model)
    reasoning_choices = _normalize_reasoning_choices(settings.get("reasoning_choices"), cli_default_reasoning)
    return {
        "settings_path": str((path or DEFAULT_SETTINGS_PATH).resolve()),
        "settings_present": bool(settings),
        "cli_default_model": cli_default_model,
        "cli_default_reasoning_effort": cli_default_reasoning,
        "cli_default_provider": cli_default_provider,
        "defaults": {
            "workspace": workspace_defaults["model"],
            "feishu": feishu_defaults["model"],
            "electron": electron_defaults["model"],
        },
        "reasoning_defaults": {
            "workspace": workspace_defaults["reasoning_effort"],
            "feishu": feishu_defaults["reasoning_effort"],
            "electron": electron_defaults["reasoning_effort"],
        },
        "choices": choices,
        "reasoning_choices": reasoning_choices,
    }


def _entrypoint_for(execution_profile: str = "", source: str = "") -> str:
    profile = str(execution_profile or "").strip()
    source_name = str(source or "").strip()
    if profile.startswith("feishu") or source_name in {"feishu", "weixin"} or profile.startswith("weixin"):
        return "feishu"
    if profile.startswith("electron") or source_name == "electron":
        return "electron"
    return "workspace"


def resolve_runtime_settings(
    explicit_model: str = "",
    explicit_reasoning_effort: str = "",
    *,
    execution_profile: str = "",
    source: str = "",
    path: Path | None = None,
) -> dict[str, str]:
    summary = summarize_settings(path)
    entrypoint = _entrypoint_for(execution_profile=execution_profile, source=source)
    return {
        "model": str(explicit_model or "").strip() or str(summary.get("defaults", {}).get(entrypoint, "")).strip(),
        "reasoning_effort": str(explicit_reasoning_effort or "").strip()
        or str(summary.get("reasoning_defaults", {}).get(entrypoint, "")).strip(),
    }


def resolve_model(explicit_model: str = "", *, execution_profile: str = "", source: str = "", path: Path | None = None) -> str:
    return resolve_runtime_settings(
        explicit_model,
        "",
        execution_profile=execution_profile,
        source=source,
        path=path,
    )["model"]


def resolve_reasoning_effort(
    explicit_reasoning_effort: str = "",
    *,
    execution_profile: str = "",
    source: str = "",
    path: Path | None = None,
) -> str:
    return resolve_runtime_settings(
        "",
        explicit_reasoning_effort,
        execution_profile=execution_profile,
        source=source,
        path=path,
    )["reasoning_effort"]


def save_defaults(
    *,
    workspace: str = "",
    feishu: str = "",
    electron: str = "",
    workspace_reasoning: str = "",
    feishu_reasoning: str = "",
    electron_reasoning: str = "",
    path: Path | None = None,
) -> dict[str, Any]:
    target = path or DEFAULT_SETTINGS_PATH
    existing = _load_settings_file(target)
    summary = summarize_settings(target)
    model_choices = summary.get("choices", [])
    reasoning_choices = summary.get("reasoning_choices", [])
    allowed_model_ids = {item["id"] for item in model_choices if item.get("id")}
    allowed_reasoning_ids = {item["id"] for item in reasoning_choices if item.get("id")}
    desired_models = {
        "workspace": str(workspace or "").strip() or str(summary["defaults"]["workspace"]),
        "feishu": str(feishu or "").strip() or str(summary["defaults"]["feishu"]),
        "electron": str(electron or "").strip() or str(summary["defaults"]["electron"]),
    }
    desired_reasoning = {
        "workspace": str(workspace_reasoning or "").strip() or str(summary["reasoning_defaults"]["workspace"]),
        "feishu": str(feishu_reasoning or "").strip() or str(summary["reasoning_defaults"]["feishu"]),
        "electron": str(electron_reasoning or "").strip() or str(summary["reasoning_defaults"]["electron"]),
    }
    for key, value in desired_models.items():
        if value and allowed_model_ids and value not in allowed_model_ids:
            raise ValueError(f"unknown model `{value}` for {key}")
    for key, value in desired_reasoning.items():
        if value and allowed_reasoning_ids and value not in allowed_reasoning_ids:
            raise ValueError(f"unknown reasoning_effort `{value}` for {key}")
    payload = {
        "version": int(existing.get("version", 1) or 1),
        "defaults": {
            "workspace": {"model": desired_models["workspace"], "reasoning_effort": desired_reasoning["workspace"]},
            "feishu": {"model": desired_models["feishu"], "reasoning_effort": desired_reasoning["feishu"]},
            "electron": {"model": desired_models["electron"], "reasoning_effort": desired_reasoning["electron"]},
        },
        "choices": existing.get("choices") or model_choices,
        "reasoning_choices": existing.get("reasoning_choices") or reasoning_choices,
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return summarize_settings(target)


def _cmd_summary(_args: argparse.Namespace) -> int:
    print(json.dumps(summarize_settings(), ensure_ascii=False))
    return 0


def _cmd_resolve(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            resolve_runtime_settings(
                args.model,
                args.reasoning_effort,
                execution_profile=args.execution_profile,
                source=args.source,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def _cmd_save(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            save_defaults(
                workspace=args.workspace,
                feishu=args.feishu,
                electron=args.electron,
                workspace_reasoning=args.workspace_reasoning,
                feishu_reasoning=args.feishu_reasoning,
                electron_reasoning=args.electron_reasoning,
            ),
            ensure_ascii=False,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex model defaults and selection helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    summary = subparsers.add_parser("summary")
    summary.set_defaults(func=_cmd_summary)

    resolve = subparsers.add_parser("resolve")
    resolve.add_argument("--model", default="")
    resolve.add_argument("--reasoning-effort", default="")
    resolve.add_argument("--execution-profile", default="")
    resolve.add_argument("--source", default="")
    resolve.set_defaults(func=_cmd_resolve)

    save = subparsers.add_parser("save-defaults")
    save.add_argument("--workspace", default="")
    save.add_argument("--feishu", default="")
    save.add_argument("--electron", default="")
    save.add_argument("--workspace-reasoning", default="")
    save.add_argument("--feishu-reasoning", default="")
    save.add_argument("--electron-reasoning", default="")
    save.set_defaults(func=_cmd_save)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
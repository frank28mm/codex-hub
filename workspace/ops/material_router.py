#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import codex_context, codex_retrieval
except ImportError:  # pragma: no cover
    import codex_context  # type: ignore
    import codex_retrieval  # type: ignore


CONFIG_BLOCK_RE = re.compile(
    r"<!-- MATERIAL_ROUTE_CONFIG_START -->\s*```json\s*(.*?)\s*```\s*<!-- MATERIAL_ROUTE_CONFIG_END -->",
    re.S,
)
DEFAULT_GROUP_LIMIT = 6


def vault_root() -> Path:
    return codex_retrieval.vault_root()


def material_routes_root() -> Path:
    return vault_root() / "03_semantic" / "material_routes"


def canonical_project_name(project_name: str) -> str:
    return codex_context.canonical_project_name(project_name)


def project_root(project_name: str) -> Path | None:
    target = canonical_project_name(project_name)
    for entry in codex_context.registry_entries():
        if str(entry.get("project_name", "")).strip() != target:
            continue
        raw_path = str(entry.get("path", "")).strip()
        if raw_path:
            return Path(raw_path).expanduser()
    candidate = codex_retrieval.projects_root() / target
    return candidate if candidate.exists() else None


def material_route_path(project_name: str) -> Path:
    return material_routes_root() / f"{canonical_project_name(project_name)}.md"


def parse_config_block(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    match = CONFIG_BLOCK_RE.search(text)
    if not match:
        raise ValueError(f"Material route config block not found: {path}")
    payload = json.loads(match.group(1))
    if not isinstance(payload, dict):
        raise ValueError(f"Material route config must be an object: {path}")
    return payload


def normalize_path(value: str, *, project_dir: Path | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    path = Path(raw).expanduser()
    if not path.is_absolute():
        if project_dir is not None:
            path = project_dir / path
        else:
            path = vault_root() / raw
    return str(path.resolve(strict=False))


def normalize_path_list(values: Any, *, project_dir: Path | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values if isinstance(values, list) else []:
        path = normalize_path(str(item), project_dir=project_dir)
        if not path or path in seen:
            continue
        seen.add(path)
        normalized.append(path)
    return normalized


def normalize_string_list(values: Any) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for item in values if isinstance(values, list) else []:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def is_within(child: str, parent: str) -> bool:
    child_path = Path(child).resolve(strict=False)
    parent_path = Path(parent).resolve(strict=False)
    try:
        child_path.relative_to(parent_path)
        return True
    except ValueError:
        return False


def any_within(path: str, roots: list[str]) -> bool:
    return any(is_within(path, root) for root in roots)


def classify_hit(path: str, config: dict[str, Any]) -> str:
    if any_within(path, config["report_roots"]):
        return "report"
    if any_within(path, config["deliverable_roots"]):
        return "deliverable"
    if any_within(path, config["project_material_roots"]):
        return "project-material"
    return "other"


def load_material_route(project_name: str) -> dict[str, Any]:
    target = canonical_project_name(project_name)
    route_path = material_route_path(target)
    project_dir = project_root(target)
    payload: dict[str, Any] = {
        "project_name": target,
        "config_path": str(route_path),
        "config_present": route_path.exists(),
        "config_valid": False,
        "project_root": str(project_dir.resolve(strict=False)) if project_dir is not None else "",
        "project_material_roots": [],
        "report_roots": [],
        "deliverable_roots": [],
        "hotset_paths": [],
        "ignore_paths": [],
        "preferred_queries": [],
        "allow_roots": [],
        "issues": [],
        "complete": False,
    }
    if not route_path.exists():
        payload["issues"].append("missing_material_route_config")
        return payload

    try:
        raw = parse_config_block(route_path)
    except Exception as exc:  # pragma: no cover - surfaced in inspect output
        payload["issues"].append(f"invalid_material_route_config:{exc}")
        return payload

    payload["config_valid"] = True
    payload["project_material_roots"] = normalize_path_list(raw.get("project_material_roots"), project_dir=project_dir)
    payload["report_roots"] = normalize_path_list(raw.get("report_roots"), project_dir=project_dir)
    payload["deliverable_roots"] = normalize_path_list(raw.get("deliverable_roots"), project_dir=project_dir)
    payload["hotset_paths"] = normalize_path_list(raw.get("hotset_paths"), project_dir=project_dir)
    payload["ignore_paths"] = normalize_path_list(raw.get("ignore_paths"), project_dir=project_dir)
    payload["preferred_queries"] = normalize_string_list(raw.get("preferred_queries"))
    payload["allow_roots"] = normalize_path_list(raw.get("allow_roots"), project_dir=project_dir)

    auto_allow = payload["project_material_roots"] + payload["report_roots"] + payload["deliverable_roots"]
    if not payload["allow_roots"]:
        payload["allow_roots"] = list(dict.fromkeys(auto_allow))

    if not payload["project_material_roots"]:
        payload["issues"].append("missing_project_material_roots")
    if not payload["allow_roots"]:
        payload["issues"].append("missing_allow_roots")

    for root in payload["project_material_roots"] + payload["report_roots"] + payload["deliverable_roots"]:
        if not any_within(root, payload["allow_roots"]):
            payload["issues"].append(f"root_outside_allow_roots:{root}")

    for path_value in payload["hotset_paths"] + payload["ignore_paths"]:
        if not any_within(path_value, payload["allow_roots"]):
            payload["issues"].append(f"path_outside_allow_roots:{path_value}")

    payload["complete"] = payload["config_valid"] and not payload["issues"]
    return payload


def search_material_hits(project_name: str, prompt: str, config: dict[str, Any], *, limit: int = DEFAULT_GROUP_LIMIT) -> dict[str, list[dict[str, Any]]]:
    queries: list[str] = []
    if prompt.strip():
        queries.append(prompt.strip())
    queries.extend(config["preferred_queries"])
    if project_name:
        queries.append(project_name)

    seen: set[str] = set()
    grouped = {
        "material_hits": [],
        "report_hits": [],
        "deliverable_hits": [],
        "hotset_hits": [],
    }

    for query in queries:
        for item in codex_retrieval.search_index(query, hotset_paths=config["hotset_paths"], limit=20):
            path = str(item.get("path", "")).strip()
            if not path or path in seen:
                continue
            if config["allow_roots"] and not any_within(path, config["allow_roots"]):
                continue
            if config["ignore_paths"] and any_within(path, config["ignore_paths"]):
                continue
            seen.add(path)
            group = classify_hit(path, config)
            hit = codex_context.build_retrieval_hit(item, route_group=group)
            if hit["is_hotset"] and len(grouped["hotset_hits"]) < limit:
                grouped["hotset_hits"].append(hit)
            if group == "report" and len(grouped["report_hits"]) < limit:
                grouped["report_hits"].append(hit)
            elif group == "deliverable" and len(grouped["deliverable_hits"]) < limit:
                grouped["deliverable_hits"].append(hit)
            elif group == "project-material" and len(grouped["material_hits"]) < limit:
                grouped["material_hits"].append(hit)
        if all(len(items) >= limit for items in grouped.values()):
            break
    return grouped


def build_retrieval_protocol(payload: dict[str, Any]) -> dict[str, Any]:
    timeline_hits = [
        *payload.get("hotset_hits", []),
        *payload.get("material_hits", []),
        *payload.get("report_hits", []),
        *payload.get("deliverable_hits", []),
    ]
    detail_hits = sorted(
        payload.get("hotset_hits") or timeline_hits,
        key=codex_context.retrieval_detail_priority,
    )[:DEFAULT_GROUP_LIMIT]
    return codex_context.build_retrieval_protocol(
        search_hits=payload.get("search_hits", []),
        timeline_hits=timeline_hits[:DEFAULT_GROUP_LIMIT],
        detail_hits=detail_hits,
        limit=DEFAULT_GROUP_LIMIT,
    )


def inspect_material_route(project_name: str) -> dict[str, Any]:
    return load_material_route(project_name)


def suggest_material_route(project_name: str, prompt: str, launch_source: str = "") -> dict[str, Any]:
    target = canonical_project_name(project_name)
    context = codex_context.suggest_context(project_name=target, prompt=prompt, launch_source=launch_source)
    config = load_material_route(target)
    payload: dict[str, Any] = {
        **config,
        "binding_scope": context.get("binding_scope", "general"),
        "board_path": context.get("board_path", ""),
        "recommended_files": context.get("recommended_files", []),
        "search_hits": context.get("search_hits", []),
        "timeline_hits": context.get("timeline_hits", []),
        "detail_hits": context.get("detail_hits", []),
        "reasoning_tags": context.get("reasoning_tags", []),
        "workflow_recommendation": context.get("workflow_recommendation", {}),
        "gflow_recommendation": context.get("gflow_recommendation", {}),
        "gflow_runtime_summary": context.get("gflow_runtime_summary", {}),
        "project_runtime_snapshot": context.get("project_runtime_snapshot", {}),
        "bridge_runtime_snapshot": context.get("bridge_runtime_snapshot", {}),
        "hot_window_summary": context.get("hot_window_summary", {}),
        "fallback_used": not config["config_present"],
        "material_hits": [],
        "report_hits": [],
        "deliverable_hits": [],
        "hotset_hits": [],
        "retrieval_protocol": {},
    }
    if not config["config_present"] or not config["config_valid"]:
        payload["retrieval_protocol"] = build_retrieval_protocol(payload)
        return payload
    payload.update(search_material_hits(target, prompt, config))
    payload["timeline_hits"] = [
        *payload.get("hotset_hits", []),
        *payload.get("material_hits", []),
        *payload.get("report_hits", []),
        *payload.get("deliverable_hits", []),
    ][:DEFAULT_GROUP_LIMIT]
    payload["detail_hits"] = sorted(
        payload.get("hotset_hits") or payload["timeline_hits"],
        key=codex_context.retrieval_detail_priority,
    )[:DEFAULT_GROUP_LIMIT]
    if payload["hotset_hits"]:
        payload["reasoning_tags"] = [*payload["reasoning_tags"], "material-hotset"]
    if payload["report_hits"] or payload["deliverable_hits"] or payload["material_hits"]:
        payload["reasoning_tags"] = [*payload["reasoning_tags"], "material-routing"]
    payload["retrieval_protocol"] = build_retrieval_protocol(payload)
    return payload


def cmd_inspect(args: argparse.Namespace) -> int:
    print(json.dumps(inspect_material_route(args.project_name), ensure_ascii=False))
    return 0


def cmd_suggest(args: argparse.Namespace) -> int:
    print(json.dumps(suggest_material_route(args.project_name, args.prompt or "", args.launch_source or ""), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub material routing helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_cmd = subparsers.add_parser("inspect")
    inspect_cmd.add_argument("--project-name", required=True)
    inspect_cmd.set_defaults(func=cmd_inspect)

    suggest_cmd = subparsers.add_parser("suggest")
    suggest_cmd.add_argument("--project-name", required=True)
    suggest_cmd.add_argument("--prompt", default="")
    suggest_cmd.add_argument("--launch-source", default="")
    suggest_cmd.set_defaults(func=cmd_suggest)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

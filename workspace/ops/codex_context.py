#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import codex_retrieval, gstack_automation, gstack_phase1_entry, runtime_state, workspace_hub_project
except ImportError:  # pragma: no cover
    import codex_retrieval  # type: ignore
    import gstack_automation  # type: ignore
    import gstack_phase1_entry  # type: ignore
    import runtime_state  # type: ignore
    import workspace_hub_project  # type: ignore


GLOBAL_RECOMMENDED = [
    ("PROJECT_REGISTRY.md", "项目注册入口"),
    ("ACTIVE_PROJECTS.md", "当前活跃项目入口"),
    ("NEXT_ACTIONS.md", "全局动作入口"),
]
DEFAULT_RETRIEVAL_LIMIT = 6
FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n(.*)\Z", re.S)
REGISTRY_RE = re.compile(
    r"<!-- PROJECT_REGISTRY_DATA_START -->\s*```json\s*(.*?)\s*```\s*<!-- PROJECT_REGISTRY_DATA_END -->",
    re.S,
)
def safe_search(query: str, *, project_name: str = "", topic_name: str = "", limit: int = 6) -> list[dict[str, Any]]:
    try:
        return codex_retrieval.search_index(query, project_name=project_name, topic_name=topic_name, limit=limit)
    except Exception:
        return []


def doc_title(path: str) -> str:
    try:
        payload = codex_retrieval.get_document(path)
        title = str(payload.get("title", "")).strip()
        if title:
            return title
    except Exception:
        pass
    return Path(path).stem


def add_recommendation(recommendations: list[dict[str, str]], seen: set[str], path: str, reason: str) -> None:
    if not path or path in seen:
        return
    seen.add(path)
    recommendations.append({"path": path, "title": doc_title(path), "reason": reason})


def build_retrieval_hit(item: dict[str, Any], *, route_group: str = "search") -> dict[str, Any]:
    return {
        "path": str(item.get("path", "")).strip(),
        "title": str(item.get("title", "")).strip() or doc_title(str(item.get("path", ""))),
        "doc_type": str(item.get("doc_type", "")).strip(),
        "project_name": str(item.get("project_name", "")).strip(),
        "topic_name": str(item.get("topic_name", "")).strip(),
        "excerpt": str(item.get("excerpt", "")).strip(),
        "score": item.get("score", 0),
        "source_group": str(item.get("source_group", "other")).strip() or "other",
        "route_group": route_group,
        "heading": str(item.get("heading", "")).strip(),
        "line_start": int(item.get("line_start", 0) or 0),
        "line_end": int(item.get("line_end", 0) or 0),
        "is_hotset": bool(item.get("is_hotset", False)),
        "pin_reason": str(item.get("pin_reason", "")).strip(),
    }


def retrieval_detail_priority(item: dict[str, Any]) -> tuple[int, int, int, float]:
    source_group = str(item.get("source_group", "other")).strip()
    doc_type = str(item.get("doc_type", "")).strip()
    return (
        0 if bool(item.get("is_hotset", False)) else 1,
        codex_retrieval.SOURCE_GROUP_WEIGHT.get(source_group, 99),
        codex_retrieval.DOC_TYPE_WEIGHT.get(doc_type, 99),
        -float(item.get("score", 0) or 0),
    )


def build_retrieval_protocol(
    *,
    search_hits: list[dict[str, Any]],
    timeline_hits: list[dict[str, Any]] | None = None,
    detail_hits: list[dict[str, Any]] | None = None,
    limit: int = DEFAULT_RETRIEVAL_LIMIT,
) -> dict[str, Any]:
    timeline_rows = (timeline_hits or search_hits)[:limit]
    detail_rows = (detail_hits or timeline_rows)[:limit]
    next_step = "search"
    if detail_rows:
        next_step = "detail"
    elif timeline_rows:
        next_step = "timeline"
    return {
        "name": "search-timeline-detail",
        "steps": ["search", "timeline", "detail"],
        "next_step": next_step,
        "search_candidate_count": len(search_hits),
        "timeline_candidate_count": len(timeline_rows),
        "detail_candidate_count": len(detail_rows),
        "timeline_paths": [
            str(item.get("path", "")).strip()
            for item in timeline_rows
            if str(item.get("path", "")).strip()
        ],
        "detail_paths": [
            str(item.get("path", "")).strip()
            for item in detail_rows
            if str(item.get("path", "")).strip()
        ],
    }


def global_paths() -> list[tuple[str, str]]:
    root = codex_retrieval.vault_root()
    return [(str(root / name), reason) for name, reason in GLOBAL_RECOMMENDED]


def working_root() -> Path:
    return codex_retrieval.vault_root() / "01_working"


def registry_entries() -> list[dict[str, Any]]:
    path = codex_retrieval.vault_root() / "PROJECT_REGISTRY.md"
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    match = REGISTRY_RE.search(text)
    if not match:
        return []
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return []


def canonical_project_name(project_name: str) -> str:
    name = project_name.strip()
    if not name:
        return ""
    lowered = name.lower()
    for entry in registry_entries():
        candidates = [str(entry.get("project_name", "")).strip()]
        candidates.extend(str(item).strip() for item in entry.get("aliases", []) if str(item).strip())
        if any(candidate.lower() == lowered for candidate in candidates if candidate):
            return str(entry.get("project_name", name)).strip() or name
    return workspace_hub_project.canonicalize(name)


def project_summary_path(project_name: str) -> Path:
    project_name = canonical_project_name(project_name)
    return codex_retrieval.vault_root() / "03_semantic" / "projects" / f"{project_name}.md"


def parse_frontmatter(text: str) -> dict[str, str]:
    match = FRONTMATTER_RE.match(text)
    if not match:
        return {}
    data: dict[str, str] = {}
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip()
    return data


def project_board_path(project_name: str) -> Path:
    project_name = canonical_project_name(project_name)
    return working_root() / f"{project_name}-项目板.md"


def topic_board_paths(project_name: str) -> list[Path]:
    project_name = canonical_project_name(project_name)
    paths: list[Path] = []
    for path in sorted(working_root().glob(f"{project_name}-*跟进板.md")):
        if not path.is_file():
            continue
        frontmatter = parse_frontmatter(path.read_text(encoding="utf-8"))
        if frontmatter.get("board_type") == "topic":
            paths.append(path)
    return paths


def resolve_board_binding(project_name: str, prompt: str = "") -> dict[str, str]:
    project_name = canonical_project_name(project_name)
    project_path = project_board_path(project_name)
    result = {
        "binding_scope": "project",
        "binding_board_path": str(project_path),
        "topic_name": "",
        "rollup_target": str(project_path),
    }
    prompt_l = prompt.lower().strip()
    if not prompt_l:
        return result
    matches: list[dict[str, str]] = []
    for path in topic_board_paths(project_name):
        frontmatter = parse_frontmatter(path.read_text(encoding="utf-8"))
        topic_name = str(frontmatter.get("topic_name", "")).strip()
        topic_key = str(frontmatter.get("topic_key", "")).strip()
        haystacks = [topic_name, topic_key, path.stem]
        if any(item and item.lower() in prompt_l for item in haystacks):
            matches.append(
                {
                    "binding_scope": "topic",
                    "binding_board_path": str(path),
                    "topic_name": topic_name,
                    "rollup_target": str(frontmatter.get("rollup_target", project_path)),
                }
            )
    return matches[0] if len(matches) == 1 else result


def build_workflow_recommendation(prompt: str) -> dict[str, Any]:
    text = prompt.strip()
    if not text:
        return {}
    try:
        recommendation = gstack_phase1_entry.detect_workflow_path(text)
    except Exception:
        return {}
    if recommendation.get("status") != "workflow-recommended":
        return {}
    suggestion = {
        "recognized_stage": recommendation.get("recognized_stage", ""),
        "suggested_path": list(recommendation.get("suggested_path", [])),
        "assistant_message": str(recommendation.get("assistant_message", "")).strip(),
        "initial_action_plan": list(recommendation.get("initial_action_plan", [])),
    }
    second_opinion = gstack_phase1_entry.suggest_second_opinion_skill_from_path(
        suggestion["suggested_path"]
    )
    if second_opinion:
        package = gstack_phase1_entry.build_second_opinion_package(
            second_opinion["skill"],
            prompt=text,
            trigger_path=suggestion["suggested_path"],
            source=second_opinion.get("source", "manual"),
            workflow_detection=recommendation,
        )
        execution = gstack_phase1_entry.build_second_opinion_main_thread_execution(
            skill=second_opinion["skill"],
            packaged_request=package,
            prompt=text,
            trigger_path=suggestion["suggested_path"],
        )
        second_opinion["package_template_id"] = package["template_id"]
        second_opinion["material_source"] = package.get("material_source", "template")
        second_opinion["packaged_request"] = {
            "autofilled_fields": list(package.get("autofilled_fields", [])),
            "request": dict(package.get("request", {})),
        }
        second_opinion["main_thread_execution"] = execution
        suggestion["second_opinion"] = second_opinion
    return suggestion


def build_gflow_recommendation(prompt: str) -> dict[str, Any]:
    text = prompt.strip()
    if not text:
        return {}
    try:
        payload = gstack_automation.build_workflow_preview(text)
    except Exception as exc:
        try:
            trigger = gstack_automation.detect_gflow_trigger(text)
        except Exception:
            trigger = {}
        if not trigger.get("matched"):
            return {}
        runtime_state.enqueue_runtime_event(
            queue_name="gflow_run_log",
            event_type="gflow_recommendation_failed",
            payload={
                "prompt": text,
                "entry_prompt": str(trigger.get("entry_prompt", "")).strip(),
            },
            result={
                "error": f"{type(exc).__name__}: {exc}",
            },
            dedupe_key=(
                "gflow-recommendation-failed:"
                + hashlib.sha1(text.encode("utf-8")).hexdigest()
            ),
            status="completed",
        )
        return {
            "status": "gflow-recommendation-error",
            "invocation_mode": str(trigger.get("invocation_mode", "")).strip(),
            "trigger_token": str(trigger.get("trigger_token", "")).strip(),
            "entry_prompt": str(trigger.get("entry_prompt", "")).strip(),
            "error": f"{type(exc).__name__}: {exc}",
        }
    workflow_plan = dict(payload.get("workflow_plan", {}))
    main_thread_handoff = str(payload.get("main_thread_handoff", "")).strip()
    return {
        "status": str(payload.get("status", "")).strip(),
        "invocation_mode": str(payload.get("invocation_mode", "")).strip(),
        "trigger_token": str(payload.get("trigger_token", "")).strip(),
        "entry_prompt": str(payload.get("entry_prompt", "")).strip(),
        "path_source": str(payload.get("path_source", "")).strip(),
        "template_id": str(payload.get("template_id", "")).strip(),
        "template_label": str(payload.get("template_label", "")).strip(),
        "project_scope_id": str(payload.get("project_scope_id", "")).strip(),
        "project_scope_label": str(payload.get("project_scope_label", "")).strip(),
        "recognized_stage": str(
            (payload.get("workflow_detection") or {}).get("recognized_stage", "")
        ).strip(),
        "suggested_path": list(payload.get("suggested_path", [])),
        "initial_stage": str(payload.get("initial_stage", "")).strip(),
        "initial_action_plan": list(workflow_plan.get("initial_action_plan", [])),
        "project_scope": dict(workflow_plan.get("project_scope") or {}),
        "workflow_plan": workflow_plan,
        "main_thread_handoff": main_thread_handoff,
        "handoff_preview": "\n".join(main_thread_handoff.splitlines()[:5]).strip(),
    }


def build_gflow_runtime_summary(project_name: str) -> dict[str, Any]:
    normalized_project = canonical_project_name(project_name)
    if not normalized_project:
        return {}
    try:
        summary = gstack_automation.latest_project_workflow_summary(normalized_project)
    except Exception:
        return {}
    run_id = str(summary.get("run_id", "")).strip()
    if not run_id:
        return summary
    try:
        from ops import codex_memory
    except ImportError:  # pragma: no cover
        try:
            import codex_memory  # type: ignore
        except ImportError:
            return summary
    try:
        board = codex_memory.load_project_board(normalized_project)
    except Exception:
        return summary
    gflow_rows = board.get("gflow_rows", [])
    if any(str(row.get("ID", "")).strip() == run_id for row in gflow_rows if isinstance(row, dict)):
        return summary
    try:
        codex_memory.sync_gflow_project_layers(normalized_project)
    except Exception:
        return summary
    return summary


def suggest_context(project_name: str = "", prompt: str = "") -> dict[str, Any]:
    project_name = canonical_project_name(project_name) if project_name else ""
    payload: dict[str, Any] = {
        "project_name": project_name or "",
        "binding_scope": "general",
        "board_path": "",
        "recommended_files": [],
        "search_hits": [],
        "timeline_hits": [],
        "detail_hits": [],
        "retrieval_protocol": {},
        "reasoning_tags": [],
        "workflow_recommendation": {},
        "gflow_recommendation": {},
        "gflow_runtime_summary": {},
    }
    recommendations: list[dict[str, str]] = []
    seen_paths: set[str] = set()

    for path, reason in global_paths():
        add_recommendation(recommendations, seen_paths, path, reason)

    binding: dict[str, str] | None = None
    topic_name = ""
    if project_name:
        binding = resolve_board_binding(project_name, prompt)
        payload["binding_scope"] = binding.get("binding_scope", "project")
        payload["board_path"] = binding.get("binding_board_path", "")
        topic_name = binding.get("topic_name", "")
        add_recommendation(recommendations, seen_paths, payload["board_path"], "当前绑定板面")
        summary_path = project_summary_path(project_name)
        if summary_path.exists():
            add_recommendation(recommendations, seen_paths, str(summary_path), "项目长期背景")
        if topic_name:
            payload["reasoning_tags"].append("topic-bound")
        else:
            payload["reasoning_tags"].append("project-bound")
        if workspace_hub_project.is_workspace_hub_project(project_name):
            system_page = codex_retrieval.vault_root() / "03_semantic" / "systems" / "workspace-hub.md"
            if system_page.exists():
                add_recommendation(recommendations, seen_paths, str(system_page), "系统运行规则")
        try:
            from ops import review_plane
        except ImportError:  # pragma: no cover
            try:
                import review_plane  # type: ignore
            except ImportError:
                review_plane = None  # type: ignore
        if review_plane is not None:
            try:
                review_items = review_plane.review_items(project_name=project_name)
            except Exception:
                review_items = []
            if review_items:
                add_recommendation(
                    recommendations,
                    seen_paths,
                    str(codex_retrieval.vault_root() / "04_review" / "INBOX.md"),
                    "当前项目待审事项",
                )
                payload["reasoning_tags"].append("review-pending")
        try:
            from ops import coordination_plane
        except ImportError:  # pragma: no cover
            try:
                import coordination_plane  # type: ignore
            except ImportError:
                coordination_plane = None  # type: ignore
        if coordination_plane is not None:
            try:
                coordination_items = coordination_plane.coordination_items(project_name=project_name)
            except Exception:
                coordination_items = []
            open_items = [item for item in coordination_items if item.get("status") in {"pending", "acknowledged", "in_progress"}]
            if open_items:
                add_recommendation(
                    recommendations,
                    seen_paths,
                    str(codex_retrieval.vault_root() / "04_coordination" / "COORDINATION.md"),
                    "当前项目协同事项",
                )
                payload["reasoning_tags"].append("coordination-open")
        runtime_summary = build_gflow_runtime_summary(project_name)
        if runtime_summary:
            payload["gflow_runtime_summary"] = runtime_summary
            payload["reasoning_tags"].append("gflow-runtime-active")
    else:
        payload["reasoning_tags"].append("general-mode")

    queries: list[tuple[str, dict[str, str]]] = []
    if prompt.strip():
        queries.append((prompt.strip(), {"project_name": project_name, "topic_name": topic_name}))
    if topic_name:
        queries.append((topic_name, {"project_name": project_name, "topic_name": topic_name}))
    if project_name:
        queries.append((project_name, {"project_name": project_name, "topic_name": ""}))
    elif prompt.strip():
        queries.append((prompt.strip(), {"project_name": "", "topic_name": ""}))

    search_hits: list[dict[str, Any]] = []
    hit_paths: set[str] = set()
    for query, filters in queries:
        for item in safe_search(query, project_name=filters["project_name"], topic_name=filters["topic_name"], limit=DEFAULT_RETRIEVAL_LIMIT):
            path = str(item.get("path", "")).strip()
            if not path or path in hit_paths:
                continue
            hit_paths.add(path)
            search_hits.append(build_retrieval_hit(item))
            if len(search_hits) >= DEFAULT_RETRIEVAL_LIMIT:
                break
        if len(search_hits) >= DEFAULT_RETRIEVAL_LIMIT:
            break

    for hit in search_hits[:3]:
        add_recommendation(recommendations, seen_paths, hit["path"], "检索候选入口")

    if search_hits:
        payload["reasoning_tags"].append("retrieval-hit")
    if prompt.strip():
        payload["reasoning_tags"].append("prompt-aware")
        gflow_recommendation = build_gflow_recommendation(prompt)
        workflow_prompt = (
            str(gflow_recommendation.get("entry_prompt", "")).strip()
            if gflow_recommendation
            else prompt
        )
        if gflow_recommendation:
            payload["gflow_recommendation"] = gflow_recommendation
            payload["reasoning_tags"].append("gflow-explicit")
        workflow_recommendation = build_workflow_recommendation(workflow_prompt)
        if workflow_recommendation:
            payload["workflow_recommendation"] = workflow_recommendation
            payload["reasoning_tags"].append("workflow-recommended")
            if workflow_recommendation.get("second_opinion"):
                payload["reasoning_tags"].append("second-opinion-ready")

    payload["recommended_files"] = recommendations[:8]
    payload["search_hits"] = search_hits
    timeline_hits = search_hits[:DEFAULT_RETRIEVAL_LIMIT]
    detail_hits = sorted(search_hits, key=retrieval_detail_priority)[:DEFAULT_RETRIEVAL_LIMIT]
    payload["timeline_hits"] = timeline_hits
    payload["detail_hits"] = detail_hits
    payload["retrieval_protocol"] = build_retrieval_protocol(
        search_hits=search_hits,
        timeline_hits=timeline_hits,
        detail_hits=detail_hits,
    )
    return payload


def cmd_suggest(args: argparse.Namespace) -> int:
    print(json.dumps(suggest_context(project_name=args.project_name or "", prompt=args.prompt or ""), ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="workspace-hub context suggestion utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    suggest = subparsers.add_parser("suggest")
    suggest.add_argument("--project-name", default="")
    suggest.add_argument("--prompt", default="")
    suggest.set_defaults(func=cmd_suggest)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

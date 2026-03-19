from __future__ import annotations

from pathlib import Path

from ops import codex_retrieval, material_router


def test_material_router_inspect_reads_independent_config(sample_env) -> None:
    payload = material_router.inspect_material_route("SampleProj")
    assert payload["config_present"] is True
    assert payload["config_valid"] is True
    assert payload["complete"] is True
    assert payload["project_material_roots"] == [str(sample_env["sample_project"].resolve())]
    assert payload["report_roots"] == [str(sample_env["reports_root"].resolve())]
    assert any(path.endswith("guide.md") for path in payload["hotset_paths"])


def test_material_router_suggest_combines_context_and_material_hits(sample_env) -> None:
    codex_retrieval.build_index()
    payload = material_router.suggest_material_route(
        "SampleProj",
        "请查看 Project Document Marker 和 System Report Marker",
    )
    assert payload["binding_scope"] == "project"
    assert payload["fallback_used"] is False
    assert any(item["path"].endswith("guide.md") for item in payload["material_hits"])
    assert any(item["path"].endswith("system-overview.md") for item in payload["report_hits"])
    assert any(item["path"].endswith("guide.md") for item in payload["hotset_hits"])
    hotset_hit = next(item for item in payload["hotset_hits"] if item["path"].endswith("guide.md"))
    assert hotset_hit["source_group"] == "project-doc"
    assert hotset_hit["route_group"] == "project-material"
    assert hotset_hit["heading"] == "Guide"
    assert hotset_hit["is_hotset"] is True
    assert "material-routing" in payload["reasoning_tags"]
    assert payload["timeline_hits"]
    assert payload["detail_hits"]
    assert payload["retrieval_protocol"]["name"] == "search-timeline-detail"
    assert payload["retrieval_protocol"]["next_step"] == "detail"
    assert any(path.endswith("guide.md") for path in payload["retrieval_protocol"]["timeline_paths"])
    assert any(path.endswith("guide.md") for path in payload["retrieval_protocol"]["detail_paths"])


def test_material_router_suggest_falls_back_without_config(sample_env) -> None:
    route_file = sample_env["vault_root"] / "03_semantic" / "material_routes" / "SampleProj.md"
    route_file.unlink()
    codex_retrieval.build_index()
    payload = material_router.suggest_material_route("SampleProj", "Topic Retrieval Marker")
    assert payload["config_present"] is False
    assert payload["fallback_used"] is True
    assert payload["material_hits"] == []
    assert any(item["path"].endswith("SampleProj-需求-跟进板.md") for item in payload["search_hits"])
    assert payload["timeline_hits"]
    assert payload["detail_hits"]
    assert payload["retrieval_protocol"]["next_step"] in {"timeline", "detail"}


def test_material_router_flags_paths_outside_allow_roots(sample_env) -> None:
    route_file = sample_env["vault_root"] / "03_semantic" / "material_routes" / "SampleProj.md"
    route_file.write_text(
        (
            "# SampleProj 材料路由\n\n"
            "<!-- MATERIAL_ROUTE_CONFIG_START -->\n"
            "```json\n"
            "{\n"
            f"  \"project_material_roots\": [\"{(sample_env['sample_project'] / 'guide.md').as_posix()}\"],\n"
            "  \"allow_roots\": [\"/tmp/nowhere\"]\n"
            "}\n"
            "```\n"
            "<!-- MATERIAL_ROUTE_CONFIG_END -->\n"
        ),
        encoding="utf-8",
    )
    payload = material_router.inspect_material_route("SampleProj")
    assert payload["complete"] is False
    assert any(issue.startswith("root_outside_allow_roots:") for issue in payload["issues"])

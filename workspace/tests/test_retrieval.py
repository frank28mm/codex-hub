from __future__ import annotations

import json
from pathlib import Path

from ops import codex_retrieval


def test_build_index_and_search_structured_docs(sample_env) -> None:
    state = codex_retrieval.build_index()
    assert state["doc_count"] >= 8
    results = codex_retrieval.search_index("Alpha Board", limit=5)
    assert results
    assert results[0]["doc_type"] == "project-board"
    assert results[0]["source_group"] == "truth"
    assert results[0]["heading"] == "Sample Board"
    assert results[0]["line_start"] >= 1
    assert results[0]["line_end"] >= results[0]["line_start"]
    assert results[0]["path"].endswith("SampleProj-项目板.md")


def test_rich_documents_are_indexed(sample_env) -> None:
    codex_retrieval.build_index()
    pdf_results = codex_retrieval.search_index("PDF Fixture Marker", limit=5)
    assert any(item["path"].endswith("slides.pdf") for item in pdf_results)
    docx_results = codex_retrieval.search_index("Docx Fixture Marker", limit=5)
    assert any(item["path"].endswith("guide.docx") for item in docx_results)
    xlsx_results = codex_retrieval.search_index("XLSX Fixture Marker", limit=5)
    assert any(item["path"].endswith("budget.xlsx") for item in xlsx_results)
    csv_results = codex_retrieval.search_index("CSV Fixture Marker", limit=5)
    assert any(item["path"].endswith("items.csv") for item in csv_results)


def test_sync_index_handles_modification_and_deletion(sample_env) -> None:
    codex_retrieval.build_index()
    guide = sample_env["sample_project"] / "guide.md"
    guide.write_text("# Guide\n\nUpdated Retrieval Marker\n", encoding="utf-8")
    removed = sample_env["sample_project"] / "items.csv"
    removed.unlink()
    state = codex_retrieval.sync_index()
    assert state["changed_count"] >= 1
    results = codex_retrieval.search_index("Updated Retrieval Marker", limit=5)
    assert any(item["path"].endswith("guide.md") for item in results)
    missing = codex_retrieval.search_index("CSV Fixture Marker", limit=5)
    assert not any(item["path"].endswith("items.csv") for item in missing)


def test_code_files_are_not_indexed(sample_env) -> None:
    codex_retrieval.build_index()
    results = codex_retrieval.search_index("SECRET_CODE_ONLY", limit=5)
    assert results == []


def test_office_lock_files_are_not_indexed(sample_env) -> None:
    temp_csv = sample_env["sample_project"] / ".~temporary-export.csv"
    temp_csv.write_text("marker,value\nLOCK_FILE_ONLY,1\n", encoding="utf-8")
    codex_retrieval.build_index()
    results = codex_retrieval.search_index("LOCK_FILE_ONLY", limit=5)
    assert results == []


def test_get_and_status(sample_env) -> None:
    codex_retrieval.build_index()
    guide = sample_env["sample_project"] / "guide.md"
    payload = codex_retrieval.get_document(str(guide))
    assert payload["doc_type"] == "project-doc"
    assert payload["source_group"] == "project-doc"
    assert payload["heading"] == "Guide"
    assert payload["line_start"] == 1
    assert payload["line_end"] >= 3
    assert "Project Document Marker" in payload["content"]
    status = codex_retrieval.status()
    assert status["db_exists"] is True
    assert status["doc_count"] >= 1


def test_search_index_marks_hotset_and_preserves_metadata(sample_env) -> None:
    codex_retrieval.build_index()
    guide = sample_env["sample_project"] / "guide.md"
    results = codex_retrieval.search_index(
        "Project Document Marker",
        hotset_paths=[str(guide.resolve())],
        limit=5,
    )
    assert results
    assert results[0]["path"].endswith("guide.md")
    assert results[0]["is_hotset"] is True
    assert results[0]["pin_reason"] == "hotset_path"
    assert results[0]["heading"] == "Guide"


def test_search_index_prefers_truth_then_system_then_report_then_project_doc(sample_env) -> None:
    shared_term = "Shared Ranking Marker"
    (sample_env["vault_root"] / "01_working" / "SampleProj-项目板.md").write_text(
        "---\nboard_type: project\nproject_name: SampleProj\nstatus: active\npriority: high\nupdated_at: 2026-03-11\npurpose: sample project board\n---\n\n# Sample Board\n\nShared Ranking Marker\n",
        encoding="utf-8",
    )
    (sample_env["vault_root"] / "03_semantic" / "systems" / "workspace-hub.md").write_text(
        "# workspace-hub\n\nShared Ranking Marker\n",
        encoding="utf-8",
    )
    (sample_env["workspace_root"] / "reports" / "system-overview.md").write_text(
        "# Report\n\nShared Ranking Marker\n",
        encoding="utf-8",
    )
    (sample_env["sample_project"] / "guide.md").write_text(
        "# Guide\n\nShared Ranking Marker\n",
        encoding="utf-8",
    )
    codex_retrieval.build_index()
    results = codex_retrieval.search_index(shared_term, limit=10)
    groups = [item["source_group"] for item in results[:4]]
    assert groups == ["truth", "system-doc", "report", "project-doc"]


def test_search_index_returns_best_section_heading_and_excerpt(sample_env) -> None:
    guide = sample_env["sample_project"] / "guide.md"
    guide.write_text(
        "# Guide\n\nIntro paragraph.\n\n## Deployment Checklist\n\nSpecial Deploy Marker lives in this section.\n\n## Notes\n\nOther note.\n",
        encoding="utf-8",
    )
    codex_retrieval.build_index()
    results = codex_retrieval.search_index("Special Deploy Marker", limit=5)
    guide_result = next(item for item in results if item["path"].endswith("guide.md"))
    assert guide_result["heading"] == "Deployment Checklist"
    assert "Special Deploy Marker" in guide_result["excerpt"]
    assert guide_result["line_start"] >= 5
    assert guide_result["line_end"] < 11


def test_search_index_uses_sliding_window_for_large_sections(sample_env) -> None:
    guide = sample_env["sample_project"] / "guide.md"
    large_section = "\n".join(f"line {index}: filler content" for index in range(1, 180))
    guide.write_text(
        "# Guide\n\n## Deep Dive\n\n"
        + large_section
        + "\n\nline 181: Tail Marker For Retrieval\n",
        encoding="utf-8",
    )
    codex_retrieval.build_index()
    results = codex_retrieval.search_index("Tail Marker For Retrieval", limit=5)
    guide_result = next(item for item in results if item["path"].endswith("guide.md"))
    assert guide_result["heading"] == "Deep Dive"
    assert "Tail Marker For Retrieval" in guide_result["excerpt"]
    assert guide_result["line_start"] > 100

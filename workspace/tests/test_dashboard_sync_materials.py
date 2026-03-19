from __future__ import annotations

import importlib


def test_rebuild_dashboards_generates_material_inspection_page(sample_env) -> None:
    from ops import codex_dashboard_sync, codex_memory, codex_retrieval

    codex_dashboard_sync = importlib.reload(codex_dashboard_sync)
    codex_memory = importlib.reload(codex_memory)
    codex_retrieval = importlib.reload(codex_retrieval)

    codex_retrieval.build_index()
    state = codex_dashboard_sync.load_state()
    result = codex_dashboard_sync.rebuild_dashboards(state=state, full=True, registry=codex_memory.load_registry())

    assert "SampleProj" in result["projects"]

    page = codex_memory.materials_dashboard_path("SampleProj")
    text = codex_memory.read_text(page)

    assert "# SampleProj｜材料检查" in text
    assert "truth board" in text
    assert "guide.md" in text
    assert "system-overview.md" in text
    assert "dirty count" in text

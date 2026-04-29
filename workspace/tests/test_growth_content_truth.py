from __future__ import annotations

import importlib
from pathlib import Path


def test_growth_content_truth_upsert_creates_tables(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", str(tmp_path / "vault"))
    from ops import growth_content_truth as module

    growth_content_truth = importlib.reload(module)
    rows = growth_content_truth.upsert_rows(
        "asset",
        [
            growth_content_truth.build_asset_row(
                asset_id="GC-ASSET-TEST-001",
                asset_type="snapshot",
                product_or_service="Codex Hub",
                channel="朋友圈",
                topic="测试快照",
                source_bucket="已发布",
                source_path="/tmp/test.png",
                checksum="abc",
                status="captured",
                created_at="2026-04-13T00:00:00+08:00",
                updated_at="2026-04-13T00:00:00+08:00",
            )
        ],
    )
    assert rows[0]["asset_id"] == "GC-ASSET-TEST-001"
    assert growth_content_truth.table_path("asset").exists()


def test_growth_content_truth_snapshot_lists_all_tables(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", str(tmp_path / "vault"))
    from ops import growth_content_truth as module

    growth_content_truth = importlib.reload(module)
    payload = growth_content_truth.snapshot()
    assert payload["ok"] is True
    assert set(payload["tables"]) == {"asset", "publish", "feedback"}


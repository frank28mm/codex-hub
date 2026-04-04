from __future__ import annotations

import importlib

import yaml


def test_growth_truth_upsert_rows_at_preserves_project_name(sample_env) -> None:
    from ops import growth_truth

    growth_truth = importlib.reload(growth_truth)
    path = sample_env["reports_root"] / "ops" / "growth" / "Action.md"
    (sample_env["control_root"] / "codex_growth_system.yaml").write_text(
        yaml.safe_dump(
            {
                "project_name": "增长与营销",
                "objects": {
                    "Action": {
                        "table_path": str(path),
                        "fields": [
                            "action_id",
                            "platform",
                            "command",
                            "target_type",
                            "target_id",
                            "status",
                            "risk_level",
                            "run_id",
                            "error",
                            "executed_at",
                        ],
                    }
                },
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    rows = growth_truth.upsert_rows_at(
        "Action",
        path,
        [
            {
                "action_id": "ACT-1",
                "platform": "xiaohongshu",
                "command": "comment-send",
                "target_type": "note",
                "target_id": "note-1",
                "status": "queued",
            }
        ],
        project_name="增长与营销",
    )

    text = path.read_text(encoding="utf-8")

    assert rows[0]["action_id"] == "ACT-1"
    assert rows[0]["command"] == "comment-send"
    assert "project_name: 增长与营销" in text

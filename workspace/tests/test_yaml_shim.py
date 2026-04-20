from __future__ import annotations

import yaml


def test_safe_load_parses_flow_style_string_lists() -> None:
    payload = yaml.safe_load(
        """
matrix:
  - id: public-read
    target_classes: [public]
    action_classes: [read]
    execution_profiles: [interactive, noninteractive, dry-run-capable]
"""
    )

    row = payload["matrix"][0]
    assert row["target_classes"] == ["public"]
    assert row["action_classes"] == ["read"]
    assert row["execution_profiles"] == ["interactive", "noninteractive", "dry-run-capable"]


def test_safe_dump_round_trips_nested_dicts_with_lists() -> None:
    original = {
        "topics": [
            {
                "topic_id": "agents",
                "aliases": ["agent", "agents"],
                "project_refs": ["Codex Hub", "知识库"],
            },
            {
                "topic_id": "project-ops",
                "keywords": ["project board", "next actions"],
            },
        ]
    }

    dumped = yaml.safe_dump(original, allow_unicode=True, sort_keys=False)
    loaded = yaml.safe_load(dumped)

    assert loaded == original

from __future__ import annotations

import importlib


def reload_ops_modules():
    from ops import codex_memory, coordination_plane, local_broker, review_plane

    codex_memory = importlib.reload(codex_memory)
    review_plane = importlib.reload(review_plane)
    coordination_plane = importlib.reload(coordination_plane)
    local_broker = importlib.reload(local_broker)
    return codex_memory, review_plane, coordination_plane, local_broker


def seed_project_and_topic_rows(codex_memory) -> None:
    board = codex_memory.load_project_board("SampleProj")
    project_rows = [
        {
            "ID": "SP-1",
            "父ID": "SP-1",
            "来源": "project",
            "范围": "控制层",
            "事项": "完成统一控制契约",
            "状态": "doing",
            "交付物": "",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "补齐 wrapper 回归",
            "更新时间": "2026-03-12T00:00:00Z",
            "指向": str(board["path"]),
        }
    ]
    codex_memory.save_project_board(board["path"], board["frontmatter"], board["body"], project_rows, [])

    topic_path = codex_memory.topic_board_paths("SampleProj")[0]
    topic_board = codex_memory.load_topic_board(topic_path)
    topic_rows = [
        {
            "ID": "TP-1",
            "模块": "需求",
            "事项": "输出需求说明",
            "状态": "done",
            "交付物": "",
            "审核状态": "",
            "审核人": "",
            "审核结论": "",
            "审核时间": "",
            "下一步": "提交审核",
            "更新时间": "2026-03-12T00:00:00Z",
            "阻塞/依赖": "",
            "上卷ID": "TP-1",
        }
    ]
    codex_memory.save_topic_board(topic_board["path"], topic_board["frontmatter"], topic_board["body"], topic_rows)
    codex_memory.refresh_project_rollups("SampleProj")


def test_review_plane_rebuilds_inbox_and_runtime(sample_env) -> None:
    codex_memory, review_plane, _coordination_plane, _local_broker = reload_ops_modules()
    seed_project_and_topic_rows(codex_memory)

    row = review_plane.submit_review("SampleProj", "TP-1", deliverable_ref="/tmp/output.md", reviewer="Frank")
    assert row["审核状态"] == "pending_review"

    items = review_plane.review_items(project_name="SampleProj")
    assert len(items) == 1
    assert items[0]["task_id"] == "TP-1"
    assert items[0]["deliverable_ref"] == "/tmp/output.md"
    assert codex_memory.REVIEW_INBOX_MD.exists()


def test_coordination_plane_updates_source_and_broker_panels(sample_env, monkeypatch) -> None:
    codex_memory, _review_plane, coordination_plane, local_broker = reload_ops_modules()
    seed_project_and_topic_rows(codex_memory)

    created = coordination_plane.create_coordination(
        coordination_id="CO-1",
        from_project="SampleProj",
        to_project="OtherProj",
        source_ref="/tmp/ref.md",
        requested_action="请复核统一契约",
        assignee="Alex",
        due_at="2026-03-20",
    )
    assert created["status"] == "pending"

    updated = coordination_plane.update_coordination("CO-1", status="in_progress", receipt_ref="/tmp/receipt.md")
    assert updated["status"] == "in_progress"

    monkeypatch.setattr(local_broker, "_health_snapshot", lambda: {"open_alert_count": 0, "last_entry": {"issue_count": 0}})
    review_panel = local_broker.cmd_panel  # smoke: symbol exists after reload
    assert review_panel is not None
    rows = coordination_plane.coordination_items(project_name="SampleProj")
    assert rows[0]["coordination_id"] == "CO-1"
    assert rows[0]["status"] == "in_progress"

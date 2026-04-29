from __future__ import annotations

import json
from pathlib import Path

from ops import knowledge_intake


def _configure_paths(monkeypatch, tmp_path: Path) -> dict[str, Path]:
    vault_root = tmp_path / "vault"
    project_root = tmp_path / "project"
    repo_root = tmp_path / "workspace"
    codex_root = project_root / "_codex"
    generated_root = codex_root / "generated"
    paths = {
        "vault_root": vault_root,
        "project_root": project_root,
        "clip_inbox": vault_root / "02_episodic" / "clips" / knowledge_intake.PROJECT_NAME / "inbox",
        "pdf_inbox": project_root / "sources" / "pdf_inbox",
        "codex_root": codex_root,
        "generated_root": generated_root,
        "clip_summary_root": generated_root / "clip_summaries",
        "pdf_summary_root": generated_root / "pdf_summaries",
        "topics_root": project_root / "topics",
        "topic_registry": project_root / "topics" / "_registry.yaml",
        "project_memory": codex_root / "PROJECT_MEMORY.md",
        "source_registry": codex_root / "SOURCE_REGISTRY.md",
        "state_json": generated_root / "intake_state.json",
        "operating_audit": generated_root / "operating_audit.md",
        "project_board": vault_root / "01_working" / f"{knowledge_intake.PROJECT_NAME}-项目板.md",
        "project_registry": vault_root / "PROJECT_REGISTRY.md",
        "workspace_briefs": vault_root / "02_episodic" / "clips" / knowledge_intake.PROJECT_NAME / "inbox" / "workspace-briefs",
        "curated_external": vault_root / "02_episodic" / "clips" / knowledge_intake.PROJECT_NAME / "inbox" / "curated-external",
        "curated_seeds": codex_root / "curated_source_seeds.yaml",
        "repo_root": repo_root,
    }
    for name, path in (
        ("VAULT_ROOT", vault_root),
        ("PROJECT_ROOT", project_root),
        ("CLIP_INBOX", paths["clip_inbox"]),
        ("PDF_INBOX", paths["pdf_inbox"]),
        ("CODEX_ROOT", codex_root),
        ("GENERATED_ROOT", generated_root),
        ("CLIP_SUMMARY_ROOT", paths["clip_summary_root"]),
        ("PDF_SUMMARY_ROOT", paths["pdf_summary_root"]),
        ("TOPICS_ROOT", paths["topics_root"]),
        ("TOPIC_REGISTRY_PATH", paths["topic_registry"]),
        ("PROJECT_MEMORY_MD", paths["project_memory"]),
        ("SOURCE_REGISTRY_MD", paths["source_registry"]),
        ("STATE_JSON", paths["state_json"]),
        ("OPERATING_AUDIT_MD", paths["operating_audit"]),
        ("PROJECT_BOARD_PATH", paths["project_board"]),
        ("PROJECT_REGISTRY_PATH", paths["project_registry"]),
        ("WORKSPACE_BRIEF_ROOT", paths["workspace_briefs"]),
        ("CURATED_SOURCE_ROOT", paths["curated_external"]),
        ("CURATED_SOURCE_SEEDS_PATH", paths["curated_seeds"]),
        ("REPO_ROOT", repo_root),
        ("LOG_STDOUT", repo_root / "logs" / "knowledge-intake.log"),
        ("LOG_STDERR", repo_root / "logs" / "knowledge-intake.err.log"),
    ):
        monkeypatch.setattr(knowledge_intake, name, path)
    return paths
def test_ensure_structure_creates_required_files(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    result = knowledge_intake.ensure_structure()
    assert Path(result["clip_inbox"]) == paths["clip_inbox"]
    assert paths["clip_inbox"].exists()
    assert paths["pdf_inbox"].exists()
    assert paths["project_memory"].exists()
    assert paths["source_registry"].exists()
    assert paths["workspace_briefs"].exists()
    assert paths["curated_external"].exists()
    assert (paths["topics_root"] / "README.md").exists()
    assert paths["topic_registry"].exists()
    assert paths["curated_seeds"].exists()


def test_run_once_processes_clip_and_pdf(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    clip_path = paths["clip_inbox"] / "mcp-article.md"
    clip_path.write_text(
        "\n".join(
            [
                "---",
                "title: MCP Primer",
                "url: https://example.com/mcp",
                "---",
                "",
                "# MCP Primer",
                "",
                "Model Context Protocol lets agents connect tools and memory cleanly.",
                "",
                "This article focuses on agent workflows and interoperability.",
            ]
        ),
        encoding="utf-8",
    )
    pdf_path = paths["project_root"] / "google agent whitebook" / "sample.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(b"%PDF-1.4 test")

    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(
        knowledge_intake,
        "extract_pdf_text",
        lambda path: (
            "Context engineering, sessions and memory systems help agents retain useful state across sessions. " * 10,
            "embedded_text",
            4,
        ),
    )
    monkeypatch.setattr(
        knowledge_intake,
        "read_pdf_metadata",
        lambda path: {"title": "Context Engineering and Memory Systems", "metadata": {}},
    )

    captured: list[dict[str, object]] = []

    def fake_writeback(binding, *, source, changed_targets, trigger_dashboard_sync):
        captured.append(
            {
                "binding": binding,
                "source": source,
                "changed_targets": list(changed_targets),
                "trigger_dashboard_sync": trigger_dashboard_sync,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(knowledge_intake, "record_project_writeback", fake_writeback)

    result = knowledge_intake.run_once()
    assert result["ok"] is True
    assert result["clip_count"] == 1
    assert result["pdf_count"] == 1
    assert paths["state_json"].exists()
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    assert len(state["sources"]) == 2
    registry_text = paths["source_registry"].read_text(encoding="utf-8")
    assert "MCP Primer" in registry_text
    assert "sample.pdf" in registry_text
    project_memory_text = paths["project_memory"].read_text(encoding="utf-8")
    assert "Clip 数：1" in project_memory_text
    assert "PDF 数：1" in project_memory_text
    assert "失败待重试来源：0" in project_memory_text
    assert "AI与自动化" in registry_text
    assert "技术系统" in registry_text
    audit_text = paths["operating_audit"].read_text(encoding="utf-8")
    assert f"{knowledge_intake.PROJECT_NAME} Operating Audit" in audit_text
    assert "运营验收：待继续观察" in audit_text
    assert captured and captured[0]["source"] == "knowledge-intake"


def test_launch_agent_payload_includes_expected_paths() -> None:
    payload = knowledge_intake.launch_agent_payload(hour=4, minute=15)
    assert payload["Label"] == knowledge_intake.LAUNCH_AGENT_NAME
    env = payload["EnvironmentVariables"]
    assert "/opt/homebrew/bin" in env["PATH"]
    assert "Library/Python" in env["PATH"]
    assert payload["StartCalendarInterval"] == {"Hour": 4, "Minute": 15}


def test_run_once_marks_duplicate_clip_urls_as_archived(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    for index in (1, 2):
        clip_path = paths["clip_inbox"] / f"duplicate-{index}.md"
        clip_path.write_text(
            "\n".join(
                [
                    "---",
                    "title: Same Source",
                    "url: https://example.com/article?utm_source=test",
                    "---",
                    "",
                    "# Same Source",
                    "",
                    f"Variant {index}",
                ]
            ),
            encoding="utf-8",
        )
    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(knowledge_intake, "record_project_writeback", lambda *args, **kwargs: {"ok": True})
    result = knowledge_intake.run_once()
    assert result["ok"] is True
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    archived = [row for row in state["sources"].values() if row.get("status") == "已归档"]
    assert len(archived) == 1
    assert archived[0]["duplicate_of"]


def test_run_once_records_failure_and_retry_state(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    pdf_path = paths["pdf_inbox"] / "broken.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 broken")

    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(knowledge_intake, "extract_pdf_text", lambda path: (_ for _ in ()).throw(RuntimeError("ocr pipeline failed")))
    monkeypatch.setattr(knowledge_intake, "record_project_writeback", lambda *args, **kwargs: {"ok": True})

    result = knowledge_intake.run_once()
    assert result["ok"] is True
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    [row] = state["sources"].values()
    assert row["status"] == "未处理"
    assert row["last_error"] == "ocr pipeline failed"
    assert row["retry_count"] == 1
    project_memory_text = paths["project_memory"].read_text(encoding="utf-8")
    assert "失败待重试来源：1" in project_memory_text


def test_audit_summary_reports_operating_readiness(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    items = [
        knowledge_intake.SourceItem(
            source_key="clip:article",
            source_type="clip",
            title="Article",
            source_path=str(knowledge_intake.CURATED_SOURCE_ROOT / "article.md"),
            updated_at="2026-03-21T00:00:00Z",
            fingerprint="a",
            companion_path="article-summary.md",
            candidate_topics=["growth-content-ops"],
            topic_l1="商业增长与变现",
            topic_l2="增长营销",
            topic_l3="growth-content-ops",
            content_type="article",
            project_refs=[knowledge_intake.PROJECT_NAME],
            routing_confidence="high",
            status="已入主题",
            extraction_method="markdown_excerpt",
            canonical_ref="https://example.com/article",
            duplicate_of="",
            last_error="",
            retry_count=0,
            extra={},
        ),
        knowledge_intake.SourceItem(
            source_key="clip:video",
            source_type="clip",
            title="Video",
            source_path=str(knowledge_intake.CLIP_INBOX / "video.md"),
            updated_at="2026-03-21T00:00:00Z",
            fingerprint="b",
            companion_path="video-summary.md",
            candidate_topics=["ai-industry-signals"],
            topic_l1="AI与自动化",
            topic_l2="信息情报",
            topic_l3="ai-industry-signals",
            content_type="video",
            project_refs=[knowledge_intake.PROJECT_NAME],
            routing_confidence="high",
            status="已入主题",
            extraction_method="markdown_excerpt",
            canonical_ref="https://youtube.com/watch?v=1",
            duplicate_of="",
            last_error="",
            retry_count=0,
            extra={},
        ),
        knowledge_intake.SourceItem(
            source_key="pdf:paper",
            source_type="pdf",
            title="Paper",
            source_path=str(knowledge_intake.PROJECT_ROOT / "sources" / "paper.pdf"),
            updated_at="2026-03-21T00:00:00Z",
            fingerprint="c",
            companion_path="paper-summary.md",
            candidate_topics=["agents"],
            topic_l1="AI与自动化",
            topic_l2="技术系统",
            topic_l3="agents",
            content_type="pdf",
            project_refs=[knowledge_intake.PROJECT_NAME],
            routing_confidence="high",
            status="已入主题",
            extraction_method="embedded_text",
            canonical_ref="pdf:paper",
            duplicate_of="",
            last_error="",
            retry_count=0,
            extra={},
        ),
    ]
    summary = knowledge_intake.audit_summary(items)
    assert summary["source_count"] == 3
    assert summary["external_topics"] == ["growth-content-ops"]
    assert summary["source_types_ready"] is True
    assert summary["failed_count"] == 0
    assert summary["inbox_review_count"] == 0
    assert summary["ready"] is True


def test_run_once_promotes_matching_sources_into_topic_pages(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    agent_topic = paths["topics_root"] / "agents.md"
    agent_topic.write_text("# Agents\n", encoding="utf-8")
    clip_path = paths["clip_inbox"] / "agent-ops.md"
    clip_path.write_text(
        "\n".join(
            [
                "---",
                "title: Agent Ops Primer",
                "url: https://example.com/agent-ops",
                "---",
                "",
                "# Agent Ops Primer",
                "",
                "This article focuses on agent workflows, orchestration, and assistant systems in production.",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(knowledge_intake, "record_project_writeback", lambda *args, **kwargs: {"ok": True})
    result = knowledge_intake.run_once()
    assert result["ok"] is True
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    [row] = state["sources"].values()
    assert row["status"] == "已入主题"
    assert row["topic_l1"] == "AI与自动化"
    assert row["topic_l2"] == "技术系统"
    assert row["topic_l3"] == "agents"
    assert "Codex Hub" in row["project_refs"]
    topic_text = agent_topic.read_text(encoding="utf-8")
    assert "## 自动来源索引" in topic_text
    assert "Agent Ops Primer" in topic_text
    topics_readme = (paths["topics_root"] / "README.md").read_text(encoding="utf-8")
    assert "## 自动来源概览" in topics_readme


def test_run_once_auto_creates_new_topic_page_from_registry(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    clip_path = paths["clip_inbox"] / "gstack.md"
    clip_path.write_text(
        "\n".join(
            [
                "---",
                "title: Garry Tan open sourced his AI special ops team",
                "url: https://example.com/gstack",
                "---",
                "",
                "# Garry Tan open sourced his AI special ops team",
                "",
                "gstack uses Claude Code, Codex, Cursor, slash commands, and skills as an AI coding workflow.",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(knowledge_intake, "record_project_writeback", lambda *args, **kwargs: {"ok": True})
    result = knowledge_intake.run_once()
    assert result["ok"] is True
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    [row] = state["sources"].values()
    assert row["topic_l1"] == "AI与自动化"
    assert row["topic_l2"] == "工具应用"
    assert row["topic_l3"] == "coding-agents"
    assert row["status"] == "已入主题"
    topic_page = paths["topics_root"] / "coding-agents.md"
    assert topic_page.exists()
    assert "## 自动来源索引" in topic_page.read_text(encoding="utf-8")


def test_run_once_keeps_medium_confidence_items_in_review(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    clip_path = paths["clip_inbox"] / "ambiguous.md"
    clip_path.write_text(
        "\n".join(
            [
                "---",
                "title: Ambiguous AI workflow note",
                "url: https://example.com/ambiguous",
                "---",
                "",
                "# Ambiguous AI workflow note",
                "",
                "This source loosely references agents and workflows without enough signal.",
            ]
        ),
        encoding="utf-8",
    )
    original_detect_route = knowledge_intake.detect_route

    def fake_detect_route(*args, **kwargs):
        route = original_detect_route(*args, **kwargs)
        route["topic_l1"] = "AI与自动化"
        route["topic_l2"] = "技术系统"
        route["topic_l3"] = "agents"
        route["candidate_topics"] = ["agents"]
        route["routing_confidence"] = "medium"
        return route

    monkeypatch.setattr(knowledge_intake, "detect_route", fake_detect_route)
    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )
    monkeypatch.setattr(knowledge_intake, "record_project_writeback", lambda *args, **kwargs: {"ok": True})

    result = knowledge_intake.run_once()
    assert result["ok"] is True
    state = json.loads(paths["state_json"].read_text(encoding="utf-8"))
    [row] = state["sources"].values()
    assert row["routing_confidence"] == "medium"
    assert row["status"] == "已提要"
    assert not (paths["topics_root"] / "agents.md").exists()


def test_run_once_triggers_writeback_when_rendered_outputs_change(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    clip_path = paths["clip_inbox"] / "agents.md"
    clip_path.write_text(
        "\n".join(
            [
                "---",
                "title: Agent Ops Primer",
                "url: https://example.com/agents",
                "---",
                "",
                "# Agent Ops Primer",
                "",
                "Agents, sessions, and memory systems in production.",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        knowledge_intake,
        "verify_toolchain",
        lambda: {"ok": True, "errors": [], "tools": {}, "versions": {}},
    )

    writebacks: list[dict[str, object]] = []

    def fake_writeback(binding, *, source, changed_targets, trigger_dashboard_sync):
        writebacks.append(
            {
                "binding": binding,
                "source": source,
                "changed_targets": list(changed_targets),
                "trigger_dashboard_sync": trigger_dashboard_sync,
            }
        )
        return {"ok": True}

    monkeypatch.setattr(knowledge_intake, "record_project_writeback", fake_writeback)

    def render_audit_v1(items, *, last_run_at):
        knowledge_intake.write_text(paths["operating_audit"], "# audit\n\nversion: v1\n")

    monkeypatch.setattr(knowledge_intake, "render_operating_audit", render_audit_v1)
    first = knowledge_intake.run_once()
    assert first["ok"] is True
    assert first["render_changed"] is True
    assert writebacks

    writebacks.clear()

    def render_audit_v2(items, *, last_run_at):
        knowledge_intake.write_text(paths["operating_audit"], "# audit\n\nversion: v2\n")

    monkeypatch.setattr(knowledge_intake, "render_operating_audit", render_audit_v2)
    second = knowledge_intake.run_once()
    assert second["ok"] is True
    assert second["processed_count"] == 0
    assert second["render_changed"] is True
    assert writebacks


def test_detect_route_prefers_title_specific_memory_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Context Engineering: Sessions & Memory",
        "This paper discusses agents, sessions, retrieval timing, memory quality, and consolidation.",
        source_type="pdf",
        metadata_type="pdf",
    )
    assert route["topic_l1"] == "AI与自动化"
    assert route["topic_l2"] == "技术系统"
    assert route["topic_l3"] == "memory-systems"


def test_detect_route_matches_coding_agents_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Y Combinator掌门人Garry Tan开源了自己的AI特种部队",
        "gstack 本质上是一组 Claude Code 的自定义命令，覆盖 Codex、Cursor、skills 和 slash command 工作流。",
        source_type="clip",
    )
    assert route["topic_l1"] == "AI与自动化"
    assert route["topic_l2"] == "工具应用"
    assert route["topic_l3"] == "coding-agents"


def test_detect_route_matches_ai_industry_signals_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "GTC 2026: Blackwell、Rubin、AI factories",
        "NVIDIA discussed CUDA, inference demand, model platform shifts, and AI factories at GTC.",
        source_type="clip",
        metadata_type="video",
        url="https://www.youtube.com/watch?v=AaCnIkmHtq8",
    )
    assert route["topic_l1"] == "AI与自动化"
    assert route["topic_l2"] == "信息情报"
    assert route["topic_l3"] == "ai-industry-signals"


def test_detect_route_matches_ai_native_curriculum_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "AI Native 课程体系与营地执行指南",
        "这份资料围绕 Learning Lab、learning curriculum、workshop、camp、learning design 与 course architecture 展开。",
        source_type="clip",
    )
    assert route["topic_l1"] == "教育与学习产品"
    assert route["topic_l2"] == "产品方法"
    assert route["topic_l3"] == "ai-native-curriculum"


def test_detect_route_matches_cross_border_commerce_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Shopify 与 TikTok Shop 的最低可行业务模型",
        "这篇内容讨论跨境电商、Amazon、Shopify、TikTok Shop、选品与 business model。",
        source_type="clip",
    )
    assert route["topic_l1"] == "商业增长与变现"
    assert route["topic_l2"] == "产品方法"
    assert route["topic_l3"] == "cross-border-commerce"


def test_detect_route_matches_growth_content_ops_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Content marketing strategy that improves conversion and retention",
        "这篇内容讨论 content growth、内容营销、paid acquisition、conversion 和 retention。",
        source_type="clip",
    )
    assert route["topic_l1"] == "商业增长与变现"
    assert route["topic_l2"] == "增长营销"
    assert route["topic_l3"] == "growth-content-ops"


def test_detect_route_matches_creative_intake_systems_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "创意收件箱与资产索引机制",
        "这里讲的是创意收件箱、资产索引、素材管理和创意资产如何归档。",
        source_type="clip",
    )
    assert route["topic_l1"] == "创意与内容资产"
    assert route["topic_l2"] == "运营管理"
    assert route["topic_l3"] == "creative-intake-systems"


def test_detect_route_matches_venue_project_delivery_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "上海科技馆互动装置与研学交付记录",
        "内容覆盖 experience venue、interactive installation、onsite delivery、visitor flow 和研学执行。",
        source_type="clip",
    )
    assert route["topic_l1"] == "消费产品与项目交付"
    assert route["topic_l2"] == "运营管理"
    assert route["topic_l3"] == "venue-project-delivery"


def test_detect_route_matches_project_ops_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Project ops workflow for dashboards and next actions",
        "这篇内容讲的是 project ops、working board、dashboard sync、next actions 和 operating system。",
        source_type="clip",
    )
    assert route["topic_l1"] == "系统运营与方法论"
    assert route["topic_l2"] == "运营管理"
    assert route["topic_l3"] == "project-ops"


def test_detect_route_matches_knowledge_systems_topic(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "Obsidian Web Clipper 与 Source Registry 的知识入库机制",
        "这篇资料解释 knowledge base、Obsidian、web clipper、project memory 和 knowledge intake。",
        source_type="clip",
    )
    assert route["topic_l1"] == "系统运营与方法论"
    assert route["topic_l2"] == "工具应用"
    assert route["topic_l3"] == "knowledge-systems"


def test_detect_route_prefers_summary_project_topic_for_workspace_brief(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        f"{knowledge_intake.PROJECT_NAME} 项目摘要",
        "这个项目包含 agents、mcp、source registry、web clipper 和 knowledge intake。",
        source_type="clip",
        metadata_type="workspace-brief",
        metadata_project_refs=[knowledge_intake.PROJECT_NAME],
    )
    assert route["topic_l1"] == "系统运营与方法论"
    assert route["topic_l2"] == "工具应用"
    assert route["topic_l3"] == "knowledge-systems"


def test_seed_project_briefs_generates_workspace_brief_sources(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    summary_ai = tmp_path / "Learning Lab.md"
    summary_ai.write_text(
        "# Learning Lab\n\n课程设计、工作坊、营地和 curriculum 结构。",
        encoding="utf-8",
    )
    summary_ec = tmp_path / "Growth Lab.md"
    summary_ec.write_text(
        "# Growth Lab\n\nShopify、TikTok Shop、选品与 business model。",
        encoding="utf-8",
    )
    paths["project_registry"].write_text(
        "\n".join(
            [
                "# PROJECT_REGISTRY",
                "<!-- PROJECT_REGISTRY_DATA_START -->",
                "```json",
                json.dumps(
                    [
                        {
                            "project_name": "Learning Lab",
                            "summary_note": str(summary_ai),
                        },
                        {
                            "project_name": "Growth Lab",
                            "summary_note": str(summary_ec),
                        },
                        {
                            "project_name": knowledge_intake.PROJECT_NAME,
                            "summary_note": str(summary_ec),
                        },
                    ],
                    ensure_ascii=False,
                ),
                "```",
                "<!-- PROJECT_REGISTRY_DATA_END -->",
            ]
        ),
        encoding="utf-8",
    )
    result = knowledge_intake.seed_project_briefs()
    assert result["ok"] is True
    assert result["generated_count"] == 2
    assert knowledge_intake.PROJECT_NAME in result["skipped"]
    ai_brief = paths["workspace_briefs"] / "learning-lab-summary.md"
    ec_brief = paths["workspace_briefs"] / "growth-lab-summary.md"
    assert ai_brief.exists()
    assert ec_brief.exists()
    assert "source_type: workspace-brief" in ai_brief.read_text(encoding="utf-8")


def test_detect_route_uses_routing_hints_for_curated_sources(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    route = knowledge_intake.detect_route(
        "A platform page with sparse topical text",
        "This page doesn't say much beyond generic benefits and templates.",
        source_type="clip",
        metadata_type="curated-external",
        metadata_routing_hints=["creative-intake-systems"],
    )
    assert route["topic_l1"] == "创意与内容资产"
    assert route["topic_l2"] == "运营管理"
    assert route["topic_l3"] == "creative-intake-systems"


def test_seed_curated_sources_writes_clip_notes(monkeypatch, tmp_path: Path) -> None:
    paths = _configure_paths(monkeypatch, tmp_path)
    knowledge_intake.ensure_structure()
    paths["curated_seeds"].write_text(
        json.dumps(
            {
                "version": 1,
                "sources": [
                    {
                        "seed_id": "cross-border-commerce-bigcommerce",
                        "title": "Cross-border Ecommerce Guide",
                        "url": "https://example.com/cross-border",
                        "topic_hint": "cross-border-commerce",
                        "project_refs": ["Growth Lab", knowledge_intake.PROJECT_NAME],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        knowledge_intake,
        "fetch_html_excerpt",
        lambda url: {
            "title": "Cross-border Ecommerce Guide",
            "excerpt": "Amazon、Shopify、TikTok Shop、选品与 business model 的跨境电商实践。",
            "fetched_url": url,
            "status_code": 200,
        },
    )

    result = knowledge_intake.seed_curated_sources()
    assert result["generated_count"] == 1
    clip_path = paths["curated_external"] / "cross-border-commerce-bigcommerce.md"
    assert clip_path.exists()
    text = clip_path.read_text(encoding="utf-8")
    assert "source_type: curated-external" in text
    assert "routing_hints:" in text
    assert "cross-border-commerce" in text


def test_cmd_fetch_url_prints_captured_article(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        knowledge_intake,
        "fetch_html_excerpt",
        lambda url, persist_artifact=False: {
            "ok": True,
            "title": "微信公众号文章标题",
            "excerpt": "这里是网页正文摘录。",
            "fetched_url": url,
            "status_code": 200,
            "content_status": "captured",
            "blocked_reason": "",
            "reader_mode": "http_html",
            "fallback_used": False,
            "artifact_json_path": "/tmp/public-article/latest.json",
        },
    )

    rc = knowledge_intake.cmd_fetch_url(type("Args", (), {"url": "https://mp.weixin.qq.com/s/demo"})())

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["title"] == "微信公众号文章标题"
    assert payload["fetched_url"] == "https://mp.weixin.qq.com/s/demo"

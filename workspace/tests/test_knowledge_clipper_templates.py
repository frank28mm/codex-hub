from __future__ import annotations

import json
from pathlib import Path


TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "control" / "obsidian_web_clipper_templates" / "knowledge_base"
EXPECTED_PATH = "02_episodic/clips/Knowledge Base/inbox"
EXPECTED_VAULT = "memory"
EXPECTED_FILES = {
    "knowledge-base-general-article-clipper.json",
    "knowledge-base-official-docs-clipper.json",
    "knowledge-base-research-web-clipper.json",
    "knowledge-base-highlight-clipper.json",
    "knowledge-base-youtube-video-clipper.json",
}
IMPORT_BUNDLE = TEMPLATE_ROOT / "knowledge-base-clipper-import.json"
REQUIRED_PROPERTIES = {
    "title",
    "url",
    "domain",
    "clipped_at",
    "source_type",
    "processing_status",
    "topic_candidates",
}


def load_template(name: str) -> dict:
    return json.loads((TEMPLATE_ROOT / name).read_text(encoding="utf-8-sig"))


def test_template_pack_contains_expected_files() -> None:
    existing = {path.name for path in TEMPLATE_ROOT.glob("*.json")}
    assert EXPECTED_FILES <= existing


def test_templates_use_fixed_knowledge_clip_inbox() -> None:
    for name in EXPECTED_FILES:
        payload = load_template(name)
        assert payload["schemaVersion"] == "0.1.0"
        assert payload["behavior"] == "create"
        assert payload["path"] == EXPECTED_PATH
        assert payload["vault"] == EXPECTED_VAULT
        assert payload["triggers"]
        if name == "knowledge-base-youtube-video-clipper.json":
            assert payload["noteContentFormat"].startswith("# {{schema:@VideoObject:name}}")
        else:
            assert payload["noteContentFormat"].startswith("# {{title}}")


def test_templates_expose_required_frontmatter_properties() -> None:
    for name in EXPECTED_FILES:
        payload = load_template(name)
        names = {row["name"] for row in payload["properties"]}
        assert REQUIRED_PROPERTIES <= names


def test_article_templates_render_body_and_images_without_context_placeholder() -> None:
    for name in {
        "knowledge-base-general-article-clipper.json",
        "knowledge-base-official-docs-clipper.json",
        "knowledge-base-research-web-clipper.json",
    }:
        payload = load_template(name)
        note_body = payload["noteContentFormat"]
        assert "{{context}}" not in note_body
        assert "{{contentHtml|" in note_body
        assert "{{image}}" in note_body


def test_youtube_template_captures_thumbnail_and_transcript() -> None:
    payload = load_template("knowledge-base-youtube-video-clipper.json")
    note_body = payload["noteContentFormat"]
    assert payload["name"] == "Knowledge Base - YouTube Video"
    assert payload["triggers"] == ["https://www.youtube.com/watch"]
    assert "{{schema:@VideoObject:thumbnailUrl|first}}" in note_body
    assert "selector:transcript-segment-view-model > .yt-core-attributed-string" in note_body
    assert "Show transcript" in note_body


def test_import_bundle_contains_all_templates() -> None:
    payload = json.loads(IMPORT_BUNDLE.read_text(encoding="utf-8-sig"))
    assert payload["vaults"] == [EXPECTED_VAULT]
    assert set(payload["template_list"]) == {
        "kb-general-article",
        "kb-official-docs",
        "kb-research-web",
        "kb-highlight",
        "kb-youtube-video",
    }
    for template_id in payload["template_list"]:
        template = payload[f"template_{template_id}"]
        assert template["path"] == EXPECTED_PATH
        assert template["vault"] == EXPECTED_VAULT

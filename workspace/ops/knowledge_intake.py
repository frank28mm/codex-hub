#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from pypdf import PdfReader

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops.codex_memory import (
        PROJECTS_ROOT,
        VAULT_ROOT,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        record_project_writeback,
        replace_or_append_marked_section,
        write_text,
    )
    from ops import public_article_reader
except ImportError:  # pragma: no cover
    from codex_memory import (  # type: ignore
        PROJECTS_ROOT,
        VAULT_ROOT,
        iso_now,
        launch_agent_loaded,
        launch_agent_plist_path,
        record_project_writeback,
        replace_or_append_marked_section,
        write_text,
    )
    import public_article_reader  # type: ignore

SITE_CONFIG_PATH = REPO_ROOT / "control" / "site.yaml"


@lru_cache(maxsize=1)
def load_site_settings() -> dict[str, Any]:
    if not SITE_CONFIG_PATH.exists():
        return {}
    payload = yaml.safe_load(SITE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    site = payload.get("site") if isinstance(payload, dict) else {}
    return site if isinstance(site, dict) else {}


def site_value(key: str, default: str) -> str:
    value = load_site_settings().get(key)
    text = str(value or "").strip()
    return text or default


PROJECT_NAME = site_value("knowledge_base_project_name", "Knowledge Base")
PROJECT_ROOT = PROJECTS_ROOT / PROJECT_NAME
CLIP_INBOX = VAULT_ROOT / "02_episodic" / "clips" / PROJECT_NAME / "inbox"
PDF_INBOX = PROJECT_ROOT / "sources" / "pdf_inbox"
CODEX_ROOT = PROJECT_ROOT / "_codex"
GENERATED_ROOT = CODEX_ROOT / "generated"
CLIP_SUMMARY_ROOT = GENERATED_ROOT / "clip_summaries"
PDF_SUMMARY_ROOT = GENERATED_ROOT / "pdf_summaries"
TOPICS_ROOT = PROJECT_ROOT / "topics"
TOPIC_REGISTRY_PATH = TOPICS_ROOT / "_registry.yaml"
PROJECT_MEMORY_MD = CODEX_ROOT / "PROJECT_MEMORY.md"
SOURCE_REGISTRY_MD = CODEX_ROOT / "SOURCE_REGISTRY.md"
STATE_JSON = GENERATED_ROOT / "intake_state.json"
OPERATING_AUDIT_MD = GENERATED_ROOT / "operating_audit.md"
PROJECT_BOARD_PATH = VAULT_ROOT / "01_working" / f"{PROJECT_NAME}-项目板.md"
PROJECT_REGISTRY_PATH = VAULT_ROOT / "PROJECT_REGISTRY.md"
WORKSPACE_BRIEF_ROOT = CLIP_INBOX / "workspace-briefs"
CURATED_SOURCE_ROOT = CLIP_INBOX / "curated-external"
CURATED_SOURCE_SEEDS_PATH = CODEX_ROOT / "curated_source_seeds.yaml"

LAUNCH_AGENT_NAME = f"{site_value('launchagent_prefix', 'com.codexhub')}.knowledge-intake"
LOG_STDOUT = REPO_ROOT / "logs" / "knowledge-intake.log"
LOG_STDERR = REPO_ROOT / "logs" / "knowledge-intake.err.log"

SOURCE_REGISTRY_MARKERS = ("<!-- AUTO_SOURCE_REGISTRY:START -->", "<!-- AUTO_SOURCE_REGISTRY:END -->")
PROJECT_MEMORY_MARKERS = ("<!-- AUTO_KB_MEMORY:START -->", "<!-- AUTO_KB_MEMORY:END -->")
TOPICS_OVERVIEW_MARKERS = ("<!-- AUTO_TOPICS_OVERVIEW:START -->", "<!-- AUTO_TOPICS_OVERVIEW:END -->")
TOPIC_SOURCE_MARKERS = ("<!-- AUTO_TOPIC_SOURCES:START -->", "<!-- AUTO_TOPIC_SOURCES:END -->")
DEFAULT_STATUS = "已提要"
STATUSES = {"未处理", "已提要", "已入主题", "已归档"}

PUBLIC_PROJECT_NAME_MAP = {
    "Learning Lab": "Learning Lab",
    "Growth Lab": "Growth Lab",
    "Creative Studio": "Creative Studio",
    "Experience Delivery": "Experience Delivery",
    "Knowledge Base": PROJECT_NAME,
    "knowledge-base": PROJECT_NAME,
}


@dataclass(frozen=True)
class SourceItem:
    source_key: str
    source_type: str
    title: str
    source_path: str
    updated_at: str
    fingerprint: str
    companion_path: str
    candidate_topics: list[str]
    topic_l1: str
    topic_l2: str
    topic_l3: str
    content_type: str
    project_refs: list[str]
    routing_confidence: str
    status: str
    extraction_method: str
    canonical_ref: str
    duplicate_of: str
    last_error: str
    retry_count: int
    extra: dict[str, Any]


def local_now() -> dt.datetime:
    return dt.datetime.now().astimezone()


def home_python_bin() -> Path:
    return Path.home() / "Library" / "Python" / f"{sys.version_info.major}.{sys.version_info.minor}" / "bin"


def default_env() -> dict[str, str]:
    path_parts = [
        str(home_python_bin()),
        "/opt/homebrew/bin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
    ]
    current = os.environ.get("PATH", "").strip()
    if current:
        path_parts.append(current)
    return {"PATH": ":".join(dict.fromkeys(part for part in path_parts if part)), "PYTHONUNBUFFERED": "1"}


def ensure_structure() -> dict[str, str]:
    CLIP_INBOX.mkdir(parents=True, exist_ok=True)
    WORKSPACE_BRIEF_ROOT.mkdir(parents=True, exist_ok=True)
    CURATED_SOURCE_ROOT.mkdir(parents=True, exist_ok=True)
    PDF_INBOX.mkdir(parents=True, exist_ok=True)
    CLIP_SUMMARY_ROOT.mkdir(parents=True, exist_ok=True)
    PDF_SUMMARY_ROOT.mkdir(parents=True, exist_ok=True)
    TOPICS_ROOT.mkdir(parents=True, exist_ok=True)
    if not PROJECT_MEMORY_MD.exists():
        write_text(
            PROJECT_MEMORY_MD,
            "\n".join(
                [
                    f"# {PROJECT_NAME} PROJECT_MEMORY",
                    "",
                    "## 自动更新",
                    "",
                    "<!-- AUTO_KB_MEMORY:START -->",
                    "- 最近 intake 运行：待初始化",
                    "- Clip 数：0",
                    "- PDF 数：0",
                    "- 待处理来源：0",
                    "- 失败待重试来源：0",
                    "- 最近候选主题：无",
                    "<!-- AUTO_KB_MEMORY:END -->",
                    "",
                ]
            ),
        )
    if not SOURCE_REGISTRY_MD.exists():
        write_text(
            SOURCE_REGISTRY_MD,
            "\n".join(
                [
                    f"# {PROJECT_NAME} SOURCE_REGISTRY",
                    "",
                    "## 自动登记",
                    "",
                    "<!-- AUTO_SOURCE_REGISTRY:START -->",
                    "| 来源键 | 类型 | 标题 | 状态 | 一级主题 | 二级主题 | 三级主题 | 内容类型 | 关联项目 | 提取方式 | 归口置信度 | 更新时间 | 来源路径 | 伴随笔记 | 错误 |",
                    "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
                    "<!-- AUTO_SOURCE_REGISTRY:END -->",
                    "",
                ]
            ),
        )
    topics_readme = TOPICS_ROOT / "README.md"
    if not topics_readme.exists():
        write_text(
            topics_readme,
            "\n".join(
                [
                    f"# {PROJECT_NAME} Topics",
                    "",
                    "这里用于承载已经稳定下来的主题知识页与知识沉淀入口。",
                    "",
                    "第一版 intake 自动化不会直接改写现有主题页，只会登记来源并生成伴随笔记。",
                    "",
                ]
            ),
        )
    if not TOPIC_REGISTRY_PATH.exists():
        write_text(
            TOPIC_REGISTRY_PATH,
            yaml.safe_dump(default_topic_registry(), allow_unicode=True, sort_keys=False),
        )
    if not CURATED_SOURCE_SEEDS_PATH.exists():
        write_text(
            CURATED_SOURCE_SEEDS_PATH,
            yaml.safe_dump(default_curated_source_seeds(), allow_unicode=True, sort_keys=False),
        )
    load_topic_registry.cache_clear()
    return {
        "clip_inbox": str(CLIP_INBOX),
        "workspace_briefs": str(WORKSPACE_BRIEF_ROOT),
        "curated_external": str(CURATED_SOURCE_ROOT),
        "pdf_inbox": str(PDF_INBOX),
        "project_memory": str(PROJECT_MEMORY_MD),
        "source_registry": str(SOURCE_REGISTRY_MD),
        "topics_root": str(TOPICS_ROOT),
        "topic_registry": str(TOPIC_REGISTRY_PATH),
        "curated_source_seeds": str(CURATED_SOURCE_SEEDS_PATH),
    }


def default_topic_registry() -> dict[str, Any]:
    payload = {
        "version": 1,
        "description": f"{PROJECT_NAME} 三层主题注册表。一级按业务域，二级按知识视角/能力域，三级按具体主题。",
        "domains": [
            {"id": "ai-automation", "name": "AI与自动化"},
            {"id": "education-learning", "name": "教育与学习产品"},
            {"id": "commercial-growth", "name": "商业增长与变现"},
            {"id": "creative-content", "name": "创意与内容资产"},
            {"id": "consumer-delivery", "name": "消费产品与项目交付"},
            {"id": "systems-methods", "name": "系统运营与方法论"},
        ],
        "lanes": [
            {"id": "info-intel", "name": "信息情报"},
            {"id": "tech-systems", "name": "技术系统"},
            {"id": "tool-application", "name": "工具应用"},
            {"id": "product-method", "name": "产品方法"},
            {"id": "growth-marketing", "name": "增长营销"},
            {"id": "operations-management", "name": "运营管理"},
            {"id": "case-review", "name": "案例复盘"},
        ],
        "topics": [
            {
                "topic_id": "agents",
                "display_name": "agents",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "agents.md",
                "aliases": ["agent", "agents", "assistant systems"],
                "keywords": ["agentic system", "multi-agent", "orchestration", "assistant workflow"],
                "project_refs": ["Codex Hub", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "memory-systems",
                "display_name": "memory-systems",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "memory-systems.md",
                "aliases": ["memory systems", "session memory", "sessions and memory", "sessions & memory"],
                "keywords": ["context engineering", "retrieval timing", "memory quality", "consolidation", "provenance", "session management"],
                "project_refs": ["Codex Hub", PROJECT_NAME],
            },
            {
                "topic_id": "mcp",
                "display_name": "mcp",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "mcp.md",
                "aliases": ["mcp", "model context protocol"],
                "keywords": ["tool interoperability", "client host server", "json-rpc", "tool protocol"],
                "project_refs": ["Codex Hub", PROJECT_NAME],
            },
            {
                "topic_id": "evaluation",
                "display_name": "evaluation",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "evaluation.md",
                "aliases": ["evaluation", "agent quality"],
                "keywords": ["benchmark", "judge model", "llm as a judge", "hitl", "quality gate"],
                "project_refs": ["Codex Hub", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "production-systems",
                "display_name": "production-systems",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "production-systems.md",
                "aliases": ["production systems", "agentops"],
                "keywords": ["deployment", "rollout", "observability", "operations", "scaling"],
                "project_refs": ["Codex Hub", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "coding-agents",
                "display_name": "coding-agents",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "tool-application",
                "lane_name": "工具应用",
                "page": "coding-agents.md",
                "summary_project": "Codex Hub",
                "aliases": ["coding agent", "coding agents", "ai coding workflow", "gstack", "explicit gears"],
                "keywords": ["claude code", "codex", "cursor", "slash command", "skill", "skills", "ai 特种部队", "ai工作流", "ai native coding"],
                "project_refs": ["Codex Hub", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "ai-industry-signals",
                "display_name": "ai-industry-signals",
                "domain_id": "ai-automation",
                "domain_name": "AI与自动化",
                "lane_id": "info-intel",
                "lane_name": "信息情报",
                "page": "ai-industry-signals.md",
                "aliases": ["ai industry", "ai signals", "model release", "platform shift"],
                "keywords": ["nvidia", "gtc", "blackwell", "rubin", "ai factories", "inference", "cuda", "model platform", "ai native"],
                "project_refs": ["Codex Hub", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "ai-native-curriculum",
                "display_name": "ai-native-curriculum",
                "domain_id": "education-learning",
                "domain_name": "教育与学习产品",
                "lane_id": "product-method",
                "lane_name": "产品方法",
                "page": "ai-native-curriculum.md",
                "summary_project": "Learning Lab",
                "aliases": ["learning curriculum", "ai native curriculum", "vibe coding course"],
                "keywords": ["curriculum", "workshop", "camp", "learning design", "course architecture"],
                "project_refs": ["Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "tutoring-products",
                "display_name": "tutoring-products",
                "domain_id": "education-learning",
                "domain_name": "教育与学习产品",
                "lane_id": "tech-systems",
                "lane_name": "技术系统",
                "page": "tutoring-products.md",
                "summary_project": "Learning Lab",
                "aliases": ["ai tutor", "tutoring product", "learning copilot"],
                "keywords": ["learning report", "feedback loop", "guided tutoring", "student support"],
                "project_refs": ["Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "cross-border-commerce",
                "display_name": "cross-border-commerce",
                "domain_id": "commercial-growth",
                "domain_name": "商业增长与变现",
                "lane_id": "product-method",
                "lane_name": "产品方法",
                "page": "cross-border-commerce.md",
                "summary_project": "Growth Lab",
                "aliases": ["cross border commerce", "market expansion", "global ecommerce"],
                "keywords": ["amazon", "shopify", "tiktok shop", "business model", "market expansion"],
                "project_refs": ["Growth Lab", PROJECT_NAME],
            },
            {
                "topic_id": "growth-content-ops",
                "display_name": "growth-content-ops",
                "domain_id": "commercial-growth",
                "domain_name": "商业增长与变现",
                "lane_id": "growth-marketing",
                "lane_name": "增长营销",
                "page": "growth-content-ops.md",
                "aliases": ["growth content", "content ops"],
                "keywords": ["content growth", "paid acquisition", "conversion", "retention", "广告投放", "内容营销"],
                "project_refs": ["Growth Lab", "Learning Lab", PROJECT_NAME],
            },
            {
                "topic_id": "creative-intake-systems",
                "display_name": "creative-intake-systems",
                "domain_id": "creative-content",
                "domain_name": "创意与内容资产",
                "lane_id": "operations-management",
                "lane_name": "运营管理",
                "page": "creative-intake-systems.md",
                "summary_project": "Creative Studio",
                "aliases": ["creative intake", "idea inbox"],
                "keywords": ["创意收件箱", "资产索引", "创意资产", "素材管理"],
                "project_refs": ["Creative Studio", PROJECT_NAME],
            },
            {
                "topic_id": "venue-project-delivery",
                "display_name": "venue-project-delivery",
                "domain_id": "consumer-delivery",
                "domain_name": "消费产品与项目交付",
                "lane_id": "operations-management",
                "lane_name": "运营管理",
                "page": "venue-project-delivery.md",
                "summary_project": "Experience Delivery",
                "aliases": ["venue delivery", "project delivery"],
                "keywords": ["experience venue", "interactive installation", "onsite delivery", "visitor flow"],
                "project_refs": ["Experience Delivery", PROJECT_NAME],
            },
            {
                "topic_id": "knowledge-systems",
                "display_name": "knowledge-systems",
                "domain_id": "systems-methods",
                "domain_name": "系统运营与方法论",
                "lane_id": "tool-application",
                "lane_name": "工具应用",
                "page": "knowledge-systems.md",
                "summary_project": PROJECT_NAME,
                "aliases": ["knowledge system", "knowledge base"],
                "keywords": ["obsidian", "web clipper", "source registry", "project memory", "knowledge intake"],
                "project_refs": [PROJECT_NAME, "Codex Hub"],
            },
            {
                "topic_id": "project-ops",
                "display_name": "project-ops",
                "domain_id": "systems-methods",
                "domain_name": "系统运营与方法论",
                "lane_id": "operations-management",
                "lane_name": "运营管理",
                "page": "project-ops.md",
                "aliases": ["project ops", "operating system"],
                "keywords": ["project board", "next actions", "dashboard sync", "working board", "工作流"],
                "project_refs": ["Codex Hub", PROJECT_NAME],
            },
        ],
    }
    return sanitize_topic_registry(payload)


def default_curated_source_seeds() -> dict[str, Any]:
    payload = {
        "version": 1,
        "description": f"{PROJECT_NAME} 外部来源种子。用于把真实网页来源按受控方式引入 clip inbox，验证非 AI 分支的归口能力。",
        "sources": [
            {
                "seed_id": "ai-native-curriculum-teachable",
                "title": "AI Curriculum Generator | Teachable",
                "url": "https://www.teachable.com/ai-curriculum-generator",
                "topic_hint": "ai-native-curriculum",
                "project_refs": ["Learning Lab", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "tutoring-products-khan-labs",
                "title": "Khan Labs | Khan Academy",
                "url": "https://www.khanacademy.org/khan-labs",
                "topic_hint": "tutoring-products",
                "project_refs": ["Learning Lab", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "cross-border-commerce-bigcommerce",
                "title": "Cross-border Ecommerce: Global Selling Guide | BigCommerce",
                "url": "https://www.bigcommerce.com/articles/ecommerce/cross-border-ecommerce/",
                "topic_hint": "cross-border-commerce",
                "project_refs": ["Growth Lab", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "growth-content-ops-ahrefs",
                "title": "How to Create a Winning Content Marketing Strategy (+ Template) | Ahrefs",
                "url": "https://ahrefs.com/blog/content-marketing-strategy/",
                "topic_hint": "growth-content-ops",
                "project_refs": ["Growth Lab", "Learning Lab", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "creative-intake-milanote",
                "title": "Advertising Brief Template & Example | Milanote",
                "url": "https://milanote.com/templates/creative-brief/advertising-brief",
                "topic_hint": "creative-intake-systems",
                "project_refs": ["Creative Studio", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "project-ops-atlassian",
                "title": "What is Workflow Management? | Atlassian",
                "url": "https://www.atlassian.com/agile/project-management/workflow-management",
                "topic_hint": "project-ops",
                "project_refs": ["Codex Hub", PROJECT_NAME],
                "content_type": "article",
            },
            {
                "seed_id": "venue-project-delivery-blooloop",
                "title": "Tinker imagineers designs exhibition for New Media Museum of Sound & Vision | blooloop",
                "url": "https://blooloop.com/museum/news/tinker-imagineers-new-media-museum/",
                "topic_hint": "venue-project-delivery",
                "project_refs": ["Experience Delivery", PROJECT_NAME],
                "content_type": "article",
            },
        ],
    }
    return sanitize_curated_source_seeds(payload)


def sanitize_project_ref(name: str) -> str:
    value = str(name or "").strip()
    if not value:
        return ""
    return PUBLIC_PROJECT_NAME_MAP.get(value, value)


def sanitize_topic_registry(payload: dict[str, Any]) -> dict[str, Any]:
    topics = payload.get("topics", [])
    if not isinstance(topics, list):
        return payload
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        refs = [sanitize_project_ref(item) for item in topic.get("project_refs", [])]
        deduped = []
        for item in refs:
            if item and item not in deduped:
                deduped.append(item)
        topic["project_refs"] = deduped or [PROJECT_NAME]
        summary_project = sanitize_project_ref(topic.get("summary_project", ""))
        if summary_project:
            topic["summary_project"] = summary_project
        elif "summary_project" in topic:
            topic.pop("summary_project", None)
    return payload


def sanitize_curated_source_seeds(payload: dict[str, Any]) -> dict[str, Any]:
    rows = payload.get("sources", [])
    if not isinstance(rows, list):
        return payload
    for row in rows:
        if not isinstance(row, dict):
            continue
        refs = [sanitize_project_ref(item) for item in row.get("project_refs", [])]
        deduped = []
        for item in refs:
            if item and item not in deduped:
                deduped.append(item)
        row["project_refs"] = deduped or [PROJECT_NAME]
    return payload


@lru_cache(maxsize=1)
def load_topic_registry() -> dict[str, Any]:
    if not TOPIC_REGISTRY_PATH.exists():
        ensure_structure()
    payload = yaml.safe_load(TOPIC_REGISTRY_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return default_topic_registry()
    return payload


def topic_registry_rows() -> list[dict[str, Any]]:
    topics = load_topic_registry().get("topics", [])
    return topics if isinstance(topics, list) else []


def curated_source_seed_rows() -> list[dict[str, Any]]:
    if not CURATED_SOURCE_SEEDS_PATH.exists():
        ensure_structure()
    payload = yaml.safe_load(CURATED_SOURCE_SEEDS_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        payload = default_curated_source_seeds()
    rows = payload.get("sources", [])
    return rows if isinstance(rows, list) else []


def topic_definition(topic_id: str) -> dict[str, Any]:
    for topic in topic_registry_rows():
        if str(topic.get("topic_id", "")) == topic_id:
            return topic
    return {}


def topic_page_path(topic_id: str) -> Path | None:
    topic = topic_definition(topic_id)
    if not topic:
        return None
    page_name = str(topic.get("page", "")).strip() or f"{topic_id}.md"
    return TOPICS_ROOT / page_name


def ensure_topic_page(topic_id: str) -> Path | None:
    if not topic_id or topic_id == "inbox-review":
        return None
    path = topic_page_path(topic_id)
    topic = topic_definition(topic_id)
    if not path or not topic:
        return None
    if path.exists():
        return path
    display_name = str(topic.get("display_name", topic_id)).strip() or topic_id
    project_refs = [str(item) for item in topic.get("project_refs", []) if str(item).strip()]
    lines = [
        f"# {display_name}",
        "",
        "## 主题定位",
        "",
        f"- 业务域：`{topic.get('domain_name', '待定')}`",
        f"- 知识视角：`{topic.get('lane_name', '待定')}`",
        f"- 主题：`{display_name}`",
    ]
    if project_refs:
        lines.append(f"- 关联项目：{', '.join(project_refs)}")
    lines.extend(
        [
            "",
            "## 当前稳定结论",
            "",
            "- 待首批来源沉淀后自动补充。",
            "",
            "## 自动来源索引",
            "",
            "<!-- AUTO_TOPIC_SOURCES:START -->",
            "- 当前还没有自动沉淀到本主题的来源。",
            "<!-- AUTO_TOPIC_SOURCES:END -->",
            "",
        ]
    )
    write_text(path, "\n".join(lines))
    return path


def load_state() -> dict[str, Any]:
    if not STATE_JSON.exists():
        return {"version": 1, "updated_at": "", "last_run_at": "", "sources": {}}
    try:
        return json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": 1, "updated_at": "", "last_run_at": "", "sources": {}}


def save_state(payload: dict[str, Any]) -> None:
    payload["updated_at"] = iso_now()
    write_text(STATE_JSON, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def tool_command(name: str) -> str:
    return shutil.which(name, path=default_env()["PATH"]) or ""


def tool_status() -> dict[str, dict[str, Any]]:
    tools = {"tesseract": {}, "ocrmypdf": {}, "pdftoppm": {}}
    for name in tools:
        path = tool_command(name)
        tools[name] = {"installed": bool(path), "path": path}
    return tools


def run_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, **default_env()},
    )


def verify_toolchain() -> dict[str, Any]:
    status = tool_status()
    versions: dict[str, str] = {}
    errors: list[str] = []
    commands = {
        "tesseract": ["tesseract", "--version"],
        "ocrmypdf": ["ocrmypdf", "--version"],
        "pdftoppm": ["pdftoppm", "-v"],
    }
    for name, command in commands.items():
        path = status[name]["path"]
        if not path:
            errors.append(f"{name} missing")
            continue
        result = run_command(command)
        output = (result.stdout or result.stderr).strip().splitlines()
        versions[name] = output[0].strip() if output else ""
        if result.returncode != 0:
            errors.append(f"{name} version check failed")
    return {"tools": status, "versions": versions, "ok": not errors, "errors": errors}


def slugify(value: str) -> str:
    text = re.sub(r"[^\w\s-]", " ", str(value or "").strip(), flags=re.U)
    text = re.sub(r"[\s_-]+", "-", text).strip("-").lower()
    return text or "untitled"


def markdown_text_excerpt(text: str, *, max_chars: int = 1200) -> str:
    cleaned = re.sub(r"^---\n.*?\n---\n", "", text, flags=re.S)
    cleaned = re.sub(r"`{3}.*?`{3}", " ", cleaned, flags=re.S)
    cleaned = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", cleaned)
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
    cleaned = re.sub(r"^#+\s*", "", cleaned, flags=re.M)
    cleaned = re.sub(r"\n{2,}", "\n\n", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = cleaned.strip()
    return cleaned[:max_chars].strip()


def parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    match = re.match(r"^---\n(.*?)\n---\n?", text, flags=re.S)
    if not match:
        return {}, text
    raw = match.group(1)
    body = text[match.end():]
    try:
        parsed = yaml.safe_load(raw) or {}
        if not isinstance(parsed, dict):
            parsed = {}
    except yaml.YAMLError:
        parsed = {}
    return parsed, body


def title_from_markdown(path: Path, body: str, metadata: dict[str, Any]) -> str:
    title = str(metadata.get("title", "")).strip()
    if title:
        return title
    for line in body.splitlines():
        candidate = line.strip()
        if candidate.startswith("#"):
            return candidate.lstrip("#").strip()
        if candidate:
            return candidate[:120]
    return path.stem


def content_type_for_source(source_type: str, *, metadata_type: str = "", url: str = "") -> str:
    typed = metadata_type.strip().lower()
    if typed:
        if "youtube" in typed or "video" in typed:
            return "video"
        if "highlight" in typed:
            return "highlight"
        if "pdf" in typed:
            return "pdf"
    if source_type == "pdf":
        return "pdf"
    if "youtube.com" in url or "youtu.be" in url:
        return "video"
    return "article"


def detect_route(
    *chunks: str,
    source_type: str,
    metadata_type: str = "",
    url: str = "",
    metadata_project_refs: list[str] | None = None,
    metadata_routing_hints: list[str] | None = None,
) -> dict[str, Any]:
    title_haystack = str(chunks[0] if chunks else "").lower().strip()
    haystack = " ".join(chunk.lower() for chunk in chunks if chunk).strip()
    primary_project = str((metadata_project_refs or [""])[0]).strip()
    is_workspace_brief = metadata_type.strip().lower() == "workspace-brief"
    hint_tokens = [str(item).strip().lower() for item in (metadata_routing_hints or []) if str(item).strip()]
    best_score = 0
    best_title_hits = 0
    best_topic: dict[str, Any] | None = None
    for topic in topic_registry_rows():
        score = 0
        title_hits = 0
        if is_workspace_brief and primary_project and str(topic.get("summary_project", "")).strip() == primary_project:
            score += 6
            title_hits += 2
        topic_tokens = {
            str(topic.get("topic_id", "")).strip().lower(),
            str(topic.get("display_name", "")).strip().lower(),
            *(str(item).strip().lower() for item in topic.get("aliases", []) if str(item).strip()),
        }
        if any(token in topic_tokens for token in hint_tokens):
            score += 5
            title_hits += 2
        for token in [*topic.get("aliases", []), *topic.get("keywords", []), topic.get("display_name", ""), topic.get("topic_id", "")]:
            normalized = str(token).strip().lower()
            if not normalized:
                continue
            if normalized in haystack:
                score += 1
            if normalized in title_haystack:
                score += 2
                title_hits += 1
        if score > best_score or (score == best_score and title_hits > best_title_hits):
            best_score = score
            best_title_hits = title_hits
            best_topic = topic
    content_type = content_type_for_source(source_type, metadata_type=metadata_type, url=url)
    if not best_topic:
        return {
            "candidate_topics": ["inbox-review"],
            "topic_l1": "",
            "topic_l2": "",
            "topic_l3": "inbox-review",
            "content_type": content_type,
            "project_refs": [],
            "routing_confidence": "low",
        }
    confidence = "high" if best_score >= 3 else "medium"
    return {
        "candidate_topics": [str(best_topic.get("topic_id", "inbox-review"))],
        "topic_l1": str(best_topic.get("domain_name", "")),
        "topic_l2": str(best_topic.get("lane_name", "")),
        "topic_l3": str(best_topic.get("topic_id", "inbox-review")),
        "content_type": content_type,
        "project_refs": [str(item) for item in best_topic.get("project_refs", [])],
        "routing_confidence": confidence,
    }


def fingerprint_for_path(path: Path) -> str:
    stat = path.stat()
    payload = f"{path.resolve()}::{stat.st_size}::{int(stat.st_mtime)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def file_content_signature(path: Path, *, sample_bytes: int = 262144) -> str:
    digest = hashlib.sha1()
    size = path.stat().st_size
    digest.update(str(size).encode("utf-8"))
    with path.open("rb") as handle:
        head = handle.read(sample_bytes)
        digest.update(head)
        if size > sample_bytes:
            handle.seek(max(size - sample_bytes, 0))
            digest.update(handle.read(sample_bytes))
    return digest.hexdigest()


def normalize_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    query_parts: list[str] = []
    if parsed.query:
        for chunk in parsed.query.split("&"):
            if not chunk:
                continue
            key = chunk.split("=", 1)[0].lower()
            if key.startswith("utm_"):
                continue
            query_parts.append(chunk)
    normalized = parsed._replace(query="&".join(query_parts), fragment="", path=parsed.path or "/")
    return normalized.geturl()


def make_source_key(source_type: str, path: Path) -> str:
    relative = ""
    try:
        if source_type == "clip":
            relative = str(path.resolve().relative_to(VAULT_ROOT.resolve()))
        else:
            relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        relative = path.name
    digest = hashlib.sha1(f"{source_type}:{relative}".encode("utf-8")).hexdigest()[:10]
    return f"{source_type}:{digest}"


def clip_summary_path(path: Path) -> Path:
    return CLIP_SUMMARY_ROOT / f"{slugify(path.stem)}.md"


def pdf_summary_path(path: Path) -> Path:
    return PDF_SUMMARY_ROOT / f"{slugify(path.stem)}.md"


def clip_sources() -> list[Path]:
    if not CLIP_INBOX.exists():
        return []
    return sorted(path for path in CLIP_INBOX.rglob("*.md") if path.is_file())


def registered_projects() -> list[dict[str, Any]]:
    if not PROJECT_REGISTRY_PATH.exists():
        return []
    text = PROJECT_REGISTRY_PATH.read_text(encoding="utf-8", errors="ignore")
    match = re.search(
        r"<!-- PROJECT_REGISTRY_DATA_START -->\s*```json\s*(.*?)\s*```\s*<!-- PROJECT_REGISTRY_DATA_END -->",
        text,
        flags=re.S,
    )
    if not match:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def workspace_brief_path(project_name: str) -> Path:
    return WORKSPACE_BRIEF_ROOT / f"{slugify(project_name)}-summary.md"


def curated_source_path(seed_id: str, title: str) -> Path:
    base = slugify(seed_id or title)
    return CURATED_SOURCE_ROOT / f"{base}.md"


def fetch_html_excerpt(url: str, *, persist_artifact: bool = False) -> dict[str, Any]:
    return public_article_reader.read_url(url, persist_artifact_result=persist_artifact)


def seed_curated_sources(*, seed_ids: list[str] | None = None) -> dict[str, Any]:
    ensure_structure()
    wanted = {item.strip() for item in (seed_ids or []) if item.strip()}
    generated: list[str] = []
    updated: list[str] = []
    failed: list[dict[str, str]] = []
    for row in curated_source_seed_rows():
        seed_id = str(row.get("seed_id", "")).strip()
        if not seed_id:
            continue
        if wanted and seed_id not in wanted:
            continue
        url = str(row.get("url", "")).strip()
        if not url:
            failed.append({"seed_id": seed_id, "error": "missing url"})
            continue
        try:
            captured = fetch_html_excerpt(url)
        except Exception as exc:
            failed.append({"seed_id": seed_id, "error": str(exc)})
            continue
        note_path = curated_source_path(seed_id, str(row.get("title", "")).strip() or captured["title"])
        existed = note_path.exists()
        frontmatter = {
            "title": str(row.get("title", "")).strip() or str(captured["title"]).strip(),
            "url": str(captured["fetched_url"]).strip() or url,
            "source_type": "curated-external",
            "project_refs": [str(item) for item in row.get("project_refs", []) if str(item).strip()],
            "routing_hints": [str(row.get("topic_hint", "")).strip()] if str(row.get("topic_hint", "")).strip() else [],
            "content_type": str(row.get("content_type", "article")).strip() or "article",
            "seed_id": seed_id,
            "fetched_at": iso_now(),
        }
        lines = [
            "---",
            yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False).strip(),
            "---",
            "",
            f"# {frontmatter['title']}",
            "",
            f"> Source: {frontmatter['url']}",
            "",
            "## Captured content",
            "",
            captured["excerpt"] or "（未提取到足够正文，请后续人工检查原网页。）",
            "",
        ]
        write_text(note_path, "\n".join(lines))
        (updated if existed else generated).append(seed_id)
    return {
        "ok": not failed,
        "generated_count": len(generated),
        "updated_count": len(updated),
        "failed_count": len(failed),
        "generated": generated,
        "updated": updated,
        "failed": failed,
        "curated_root": str(CURATED_SOURCE_ROOT),
    }


def seed_project_briefs(*, include_self: bool = False) -> dict[str, Any]:
    ensure_structure()
    generated: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []
    for row in registered_projects():
        project_name = str(row.get("project_name", "")).strip()
        summary_note = Path(str(row.get("summary_note", "")).strip())
        if not project_name or not summary_note.exists():
            continue
        if not include_self and project_name == PROJECT_NAME:
            skipped.append(project_name)
            continue
        text = summary_note.read_text(encoding="utf-8", errors="ignore")
        metadata, body = parse_frontmatter(text)
        excerpt = markdown_text_excerpt(body, max_chars=2200)
        note_path = workspace_brief_path(project_name)
        existed = note_path.exists()
        frontmatter = {
            "title": f"{project_name} 项目摘要",
            "source_type": "workspace-brief",
            "project_refs": [project_name, PROJECT_NAME],
            "origin_summary_note": str(summary_note),
            "generated_at": iso_now(),
        }
        lines = [
            "---",
            yaml.safe_dump(frontmatter, allow_unicode=True, sort_keys=False).strip(),
            "---",
            "",
            f"# {project_name} 项目摘要",
            "",
            f"> Source: `{summary_note}`",
            "",
            "## Captured content",
            "",
            excerpt or "（原始摘要为空，请回源项目摘要页。）",
            "",
        ]
        write_text(note_path, "\n".join(lines))
        (updated if existed else generated).append(project_name)
    return {
        "ok": True,
        "generated_count": len(generated),
        "updated_count": len(updated),
        "skipped_count": len(skipped),
        "generated": generated,
        "updated": updated,
        "skipped": skipped,
        "workspace_brief_root": str(WORKSPACE_BRIEF_ROOT),
    }


def pdf_sources() -> list[Path]:
    if not PROJECT_ROOT.exists():
        return []
    rows: list[Path] = []
    for path in PROJECT_ROOT.rglob("*.pdf"):
        if not path.is_file():
            continue
        parts = {part.lower() for part in path.parts}
        if "_codex" in parts or ".git" in parts:
            continue
        rows.append(path)
    return sorted(rows)


def known_topic_pages() -> dict[str, Path]:
    pages: dict[str, Path] = {}
    if not TOPICS_ROOT.exists():
        return pages
    for path in TOPICS_ROOT.glob("*.md"):
        if path.name == "README.md":
            continue
        topic_id = path.stem
        for topic in topic_registry_rows():
            if str(topic.get("page", "")).strip() == path.name:
                topic_id = str(topic.get("topic_id", path.stem))
                break
        pages[topic_id] = path
    return pages


def extract_pdf_text(path: Path) -> tuple[str, str, int]:
    page_count = 0
    direct_texts: list[str] = []
    try:
        reader = PdfReader(str(path))
        page_count = len(reader.pages)
        for page in reader.pages[: min(10, len(reader.pages))]:
            try:
                text = (page.extract_text() or "").strip()
            except Exception:
                text = ""
            if text:
                direct_texts.append(text)
    except Exception:
        reader = None
    direct_text = "\n\n".join(direct_texts).strip()
    if len(direct_text) >= 200:
        return direct_text, "embedded_text", page_count
    ocrmypdf = tool_command("ocrmypdf")
    if ocrmypdf:
        with tempfile.TemporaryDirectory(prefix="knowledge-intake-ocr-") as tempdir:
            sidecar = Path(tempdir) / "sidecar.txt"
            output_pdf = Path(tempdir) / "ocr.pdf"
            result = run_command(
                [
                    ocrmypdf,
                    "--skip-text",
                    "--sidecar",
                    str(sidecar),
                    str(path),
                    str(output_pdf),
                ]
            )
            if result.returncode == 0 and sidecar.exists():
                text = sidecar.read_text(encoding="utf-8", errors="ignore").strip()
                if text:
                    return text, "ocrmypdf_sidecar", page_count
    return direct_text, "no_text_extracted", page_count


def read_pdf_metadata(path: Path) -> dict[str, Any]:
    title = path.stem
    metadata: dict[str, Any] = {}
    try:
        reader = PdfReader(str(path))
        pdf_meta = reader.metadata or {}
        maybe_title = str(pdf_meta.get("/Title", "")).strip()
        if maybe_title:
            title = maybe_title
        metadata = {key.lstrip("/"): str(value) for key, value in pdf_meta.items()}
    except Exception:
        metadata = {}
    return {"title": title, "metadata": metadata}


def build_clip_item(path: Path, state_row: dict[str, Any]) -> SourceItem:
    text = path.read_text(encoding="utf-8", errors="ignore")
    metadata, body = parse_frontmatter(text)
    title = title_from_markdown(path, body, metadata)
    url = str(metadata.get("url", metadata.get("source", ""))).strip()
    domain = urlparse(url).netloc if url else ""
    canonical_ref = normalize_url(url) or f"clip-path:{path.resolve()}"
    excerpt = markdown_text_excerpt(body)
    metadata_project_refs = metadata.get("project_refs", [])
    if not isinstance(metadata_project_refs, list):
        metadata_project_refs = [metadata_project_refs] if metadata_project_refs else []
    metadata_routing_hints = metadata.get("routing_hints", [])
    if not isinstance(metadata_routing_hints, list):
        metadata_routing_hints = [metadata_routing_hints] if metadata_routing_hints else []
    route = detect_route(
        title,
        excerpt,
        url,
        domain,
        source_type="clip",
        metadata_type=str(metadata.get("source_type", "")),
        url=url,
        metadata_project_refs=[str(item) for item in metadata_project_refs if str(item).strip()],
        metadata_routing_hints=[str(item) for item in metadata_routing_hints if str(item).strip()],
    )
    summary_path = clip_summary_path(path)
    summary_lines = [
        f"# {title}",
        "",
        "## 来源",
        "",
        f"- 类型：clip",
        f"- 原始路径：`{path}`",
        f"- URL：{url or '未知'}",
        f"- 域名：{domain or '未知'}",
        f"- 最近更新时间：{dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds')}",
        f"- 一级主题：{route['topic_l1'] or '待定'}",
        f"- 二级主题：{route['topic_l2'] or '待定'}",
        f"- 三级主题：{route['topic_l3'] or '待定'}",
        f"- 关联项目：{', '.join(route['project_refs']) or '待定'}",
        f"- 内容类型：{route['content_type']}",
        f"- 归口置信度：{route['routing_confidence']}",
        "",
        "## 结构化摘录",
        "",
        excerpt or "（当前未提取到可用文本，请人工检查原始 clip。）",
        "",
    ]
    write_text(summary_path, "\n".join(summary_lines))
    status = state_row.get("status", DEFAULT_STATUS)
    if status not in STATUSES:
        status = DEFAULT_STATUS
    retry_count = int(state_row.get("retry_count") or 0)
    return SourceItem(
        source_key=make_source_key("clip", path),
        source_type="clip",
        title=title,
        source_path=str(path),
        updated_at=dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
        fingerprint=fingerprint_for_path(path),
        companion_path=str(summary_path),
        candidate_topics=route["candidate_topics"],
        topic_l1=route["topic_l1"],
        topic_l2=route["topic_l2"],
        topic_l3=route["topic_l3"],
        content_type=route["content_type"],
        project_refs=route["project_refs"],
        routing_confidence=route["routing_confidence"],
        status=status if status in {"已入主题", "已归档"} else DEFAULT_STATUS,
        extraction_method="markdown_excerpt",
        canonical_ref=canonical_ref,
        duplicate_of=str(state_row.get("duplicate_of") or ""),
        last_error=str(state_row.get("last_error") or ""),
        retry_count=retry_count,
        extra={"url": url, "domain": domain},
    )


def build_pdf_item(path: Path, state_row: dict[str, Any]) -> SourceItem:
    meta = read_pdf_metadata(path)
    extracted_text, extraction_method, page_count = extract_pdf_text(path)
    title = meta["title"]
    excerpt = markdown_text_excerpt(extracted_text, max_chars=2400)
    route = detect_route(
        title,
        excerpt,
        json.dumps(meta["metadata"], ensure_ascii=False),
        source_type="pdf",
        metadata_type="pdf",
    )
    canonical_ref = f"pdf:{file_content_signature(path)}"
    summary_path = pdf_summary_path(path)
    summary_lines = [
        f"# {title}",
        "",
        "## 来源",
        "",
        "- 类型：pdf",
        f"- 原始路径：`{path}`",
        f"- 页数：{page_count}",
        f"- 提取方式：{extraction_method}",
        f"- 最近更新时间：{dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec='seconds')}",
        f"- 一级主题：{route['topic_l1'] or '待定'}",
        f"- 二级主题：{route['topic_l2'] or '待定'}",
        f"- 三级主题：{route['topic_l3'] or '待定'}",
        f"- 关联项目：{', '.join(route['project_refs']) or '待定'}",
        f"- 内容类型：{route['content_type']}",
        f"- 归口置信度：{route['routing_confidence']}",
        "",
        "## 元数据",
        "",
        "```json",
        json.dumps(meta["metadata"], ensure_ascii=False, indent=2),
        "```",
        "",
        "## 结构化摘录",
        "",
        excerpt or "（未提取到可用文本；已保留原 PDF，后续可继续增强 OCR。）",
        "",
    ]
    write_text(summary_path, "\n".join(summary_lines))
    status = state_row.get("status", DEFAULT_STATUS)
    if status not in STATUSES:
        status = DEFAULT_STATUS
    retry_count = int(state_row.get("retry_count") or 0)
    return SourceItem(
        source_key=make_source_key("pdf", path),
        source_type="pdf",
        title=title,
        source_path=str(path),
        updated_at=dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
        fingerprint=fingerprint_for_path(path),
        companion_path=str(summary_path),
        candidate_topics=route["candidate_topics"],
        topic_l1=route["topic_l1"],
        topic_l2=route["topic_l2"],
        topic_l3=route["topic_l3"],
        content_type=route["content_type"],
        project_refs=route["project_refs"],
        routing_confidence=route["routing_confidence"],
        status=status if status in {"已入主题", "已归档"} else DEFAULT_STATUS,
        extraction_method=extraction_method,
        canonical_ref=canonical_ref,
        duplicate_of=str(state_row.get("duplicate_of") or ""),
        last_error=str(state_row.get("last_error") or ""),
        retry_count=retry_count,
        extra={"page_count": page_count},
    )


def build_failed_item(source_type: str, path: Path, state_row: dict[str, Any], error: str) -> SourceItem:
    summary_path = clip_summary_path(path) if source_type == "clip" else pdf_summary_path(path)
    retry_count = int(state_row.get("retry_count") or 0) + 1
    title = path.stem
    write_text(
        summary_path,
        "\n".join(
            [
                f"# {title}",
                "",
                "## 处理状态",
                "",
                f"- 类型：{source_type}",
                f"- 原始路径：`{path}`",
                f"- 最近错误：{error}",
                f"- 重试次数：{retry_count}",
                "",
                "当前来源处理失败，已保留原始文件，等待下一轮自动重试或人工检查。",
                "",
            ]
        ),
    )
    return SourceItem(
        source_key=make_source_key(source_type, path),
        source_type=source_type,
        title=title,
        source_path=str(path),
        updated_at=dt.datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds"),
        fingerprint=fingerprint_for_path(path),
        companion_path=str(summary_path),
        candidate_topics=["inbox-review"],
        topic_l1="",
        topic_l2="",
        topic_l3="inbox-review",
        content_type=content_type_for_source(source_type),
        project_refs=[],
        routing_confidence="low",
        status="未处理",
        extraction_method="failed",
        canonical_ref=f"failed:{source_type}:{path.resolve()}",
        duplicate_of="",
        last_error=error,
        retry_count=retry_count,
        extra={},
    )


def resolve_duplicates(items: list[SourceItem]) -> list[SourceItem]:
    primary_by_ref: dict[str, SourceItem] = {}
    rows: list[SourceItem] = []
    ordered = sorted(items, key=lambda item: (item.canonical_ref, item.updated_at, item.source_key), reverse=True)
    for item in ordered:
        if not item.canonical_ref:
            rows.append(item)
            continue
        primary = primary_by_ref.get(item.canonical_ref)
        if primary is None:
            primary_by_ref[item.canonical_ref] = item
            rows.append(item)
            continue
        rows.append(
            SourceItem(
                source_key=item.source_key,
                source_type=item.source_type,
                title=item.title,
                source_path=item.source_path,
                updated_at=item.updated_at,
                fingerprint=item.fingerprint,
                companion_path=item.companion_path,
                candidate_topics=item.candidate_topics,
                topic_l1=item.topic_l1,
                topic_l2=item.topic_l2,
                topic_l3=item.topic_l3,
                content_type=item.content_type,
                project_refs=item.project_refs,
                routing_confidence=item.routing_confidence,
                status="已归档",
                extraction_method=item.extraction_method,
                canonical_ref=item.canonical_ref,
                duplicate_of=primary.source_key,
                last_error=item.last_error,
                retry_count=item.retry_count,
                extra={**item.extra, "duplicate": True},
            )
        )
    return sorted(rows, key=lambda item: (item.updated_at, item.source_key), reverse=True)


def render_source_registry(items: list[SourceItem]) -> None:
    rows = sorted(items, key=lambda item: (item.updated_at, item.source_key), reverse=True)
    lines = [
        "| 来源键 | 类型 | 标题 | 状态 | 一级主题 | 二级主题 | 三级主题 | 内容类型 | 关联项目 | 提取方式 | 归口置信度 | 更新时间 | 来源路径 | 伴随笔记 | 错误 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in rows:
        lines.append(
            "| {source_key} | {source_type} | {title} | {status} | {topic_l1} | {topic_l2} | {topic_l3} | {content_type} | {project_refs} | {extraction_method} | {routing_confidence} | {updated_at} | `{source_path}` | `{companion}` | {error} |".format(
                source_key=item.source_key,
                source_type=item.source_type,
                title=item.title.replace("|", "｜"),
                status=item.status,
                topic_l1=(item.topic_l1 or "待定").replace("|", "｜"),
                topic_l2=(item.topic_l2 or "待定").replace("|", "｜"),
                topic_l3=(item.topic_l3 or "待定").replace("|", "｜"),
                content_type=item.content_type,
                project_refs=", ".join(item.project_refs).replace("|", "｜") or "待定",
                extraction_method=item.extraction_method,
                routing_confidence=item.routing_confidence,
                updated_at=item.updated_at,
                source_path=item.source_path,
                companion=item.companion_path,
                error=(item.last_error or "").replace("|", "｜"),
            )
        )
    text = SOURCE_REGISTRY_MD.read_text(encoding="utf-8") if SOURCE_REGISTRY_MD.exists() else f"# {PROJECT_NAME} SOURCE_REGISTRY\n"
    updated = replace_or_append_marked_section(text, "## 自动登记", SOURCE_REGISTRY_MARKERS, lines)
    write_text(SOURCE_REGISTRY_MD, updated)


def render_project_memory(items: list[SourceItem], *, last_run_at: str) -> None:
    clip_count = sum(1 for item in items if item.source_type == "clip")
    pdf_count = sum(1 for item in items if item.source_type == "pdf")
    pending_count = sum(1 for item in items if item.status in {"未处理", "已提要"})
    failed_count = sum(1 for item in items if item.last_error)
    topic_counter: dict[str, int] = {}
    for item in items:
        topic = item.topic_l3 or "inbox-review"
        topic_counter[topic] = topic_counter.get(topic, 0) + 1
    top_topics = sorted(topic_counter.items(), key=lambda pair: (-pair[1], pair[0]))
    topic_text = "、".join(f"{name}({count})" for name, count in top_topics[:5]) if top_topics else "无"
    lines = [
        f"- 最近 intake 运行：{last_run_at or '未运行'}",
        f"- Clip 数：{clip_count}",
        f"- PDF 数：{pdf_count}",
        f"- 待处理来源：{pending_count}",
        f"- 失败待重试来源：{failed_count}",
        f"- 最近候选主题：{topic_text}",
    ]
    text = PROJECT_MEMORY_MD.read_text(encoding="utf-8") if PROJECT_MEMORY_MD.exists() else f"# {PROJECT_NAME} PROJECT_MEMORY\n"
    updated = replace_or_append_marked_section(text, "## 自动更新", PROJECT_MEMORY_MARKERS, lines)
    write_text(PROJECT_MEMORY_MD, updated)


def promote_topic_items(items: list[SourceItem]) -> list[SourceItem]:
    known_topics: set[str] = set()
    for item in items:
        if item.topic_l3 and item.topic_l3 != "inbox-review" and item.routing_confidence == "high":
            path = ensure_topic_page(item.topic_l3)
            if path and path.exists():
                known_topics.add(item.topic_l3)
    known_topics.update(known_topic_pages())
    rows: list[SourceItem] = []
    for item in items:
        if item.status == "已归档" or item.last_error:
            rows.append(item)
            continue
        if item.topic_l3 in known_topics and item.status in {"未处理", "已提要"}:
            rows.append(replace(item, status="已入主题"))
        else:
            rows.append(item)
    return rows


def _normalize_rendered_output(path: Path, text: str) -> str:
    lines = text.splitlines()
    normalized: list[str] = []
    for line in lines:
        if path == PROJECT_MEMORY_MD and line.startswith("- 最近 intake 运行："):
            normalized.append("- 最近 intake 运行：<normalized>")
            continue
        if path == OPERATING_AUDIT_MD and line.startswith("- 最近 intake 运行："):
            normalized.append("- 最近 intake 运行：<normalized>")
            continue
        normalized.append(line)
    return "\n".join(normalized)


def rendered_output_signature() -> str:
    digest = hashlib.sha1()
    candidates = [
        SOURCE_REGISTRY_MD,
        PROJECT_MEMORY_MD,
        TOPICS_ROOT / "README.md",
        OPERATING_AUDIT_MD,
        *sorted(
            path
            for path in TOPICS_ROOT.glob("*.md")
            if path.name not in {"README.md"} and path.name != "_registry.yaml"
        ),
    ]
    seen: set[str] = set()
    for path in candidates:
        if not path.exists():
            continue
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        digest.update(key.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_normalize_rendered_output(path, path.read_text(encoding="utf-8")).encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def render_topics_readme(items: list[SourceItem]) -> None:
    topic_counts: dict[str, int] = {}
    grouped: dict[str, dict[str, list[tuple[str, int]]]] = {}
    inbox_review = 0
    for item in items:
        if item.status == "已归档":
            continue
        if item.topic_l3 == "inbox-review" and item.status != "已入主题":
            inbox_review += 1
        if item.topic_l3 and item.topic_l3 != "inbox-review":
            topic_counts[item.topic_l3] = topic_counts.get(item.topic_l3, 0) + 1
    for topic_id, count in sorted(topic_counts.items(), key=lambda pair: (-pair[1], pair[0])):
        topic = topic_definition(topic_id)
        domain = str(topic.get("domain_name", "") or "待定")
        lane = str(topic.get("lane_name", "") or "待定")
        grouped.setdefault(domain, {}).setdefault(lane, []).append((topic_id, count))
    lines = []
    for domain, lanes in grouped.items():
        lines.append(f"### {domain}")
        lines.append("")
        for lane, topics in lanes.items():
            lines.append(f"- {lane}")
            for topic_id, count in topics:
                lines.append(f"  - `{topic_id}`：{count} 条已关联来源")
        lines.append("")
    lines.append(f"- `inbox-review`：{inbox_review} 条待人工判断来源")
    readme_path = TOPICS_ROOT / "README.md"
    text = readme_path.read_text(encoding="utf-8") if readme_path.exists() else f"# {PROJECT_NAME} Topics\n"
    updated = replace_or_append_marked_section(text, "## 自动来源概览", TOPICS_OVERVIEW_MARKERS, lines)
    write_text(readme_path, updated)


def render_topic_pages(items: list[SourceItem]) -> None:
    rendered_topics: set[str] = set()
    for topic in topic_registry_rows():
        topic_id = str(topic.get("topic_id", "")).strip()
        if not topic_id:
            continue
        matching = [
            item
            for item in items
            if item.topic_l3 == topic_id and item.status == "已入主题" and not item.last_error and not item.duplicate_of
        ]
        path = topic_page_path(topic_id)
        if not path:
            continue
        if matching and not path.exists():
            ensure_topic_page(topic_id)
        if not path.exists():
            continue
        rendered_topics.add(topic_id)
        matching = [
            item
            for item in items
            if item.topic_l3 == topic_id and item.status == "已入主题" and not item.last_error and not item.duplicate_of
        ]
        lines = [f"- 已关联来源数：{len(matching)}", ""]
        if matching:
            lines.append("### 已沉淀来源")
            lines.append("")
            for item in matching:
                lines.append(
                    f"- {item.title} | {item.source_type} | {item.extraction_method} | `{item.companion_path}`"
                )
        else:
            lines.append("- 当前还没有自动沉淀到本主题的来源。")
        text = path.read_text(encoding="utf-8")
        updated = replace_or_append_marked_section(text, "## 自动来源索引", TOPIC_SOURCE_MARKERS, lines)
        write_text(path, updated)
    for topic_id, path in known_topic_pages().items():
        if topic_id in rendered_topics:
            continue
        text = path.read_text(encoding="utf-8")
        updated = replace_or_append_marked_section(
            text,
            "## 自动来源索引",
            TOPIC_SOURCE_MARKERS,
            ["- 已关联来源数：0", "", "- 当前还没有自动沉淀到本主题的来源。"],
        )
        write_text(path, updated)


def audit_summary(items: list[SourceItem]) -> dict[str, Any]:
    active = [item for item in items if item.status != "已归档"]
    content_type_counts: dict[str, int] = {}
    topic_counts: dict[str, int] = {}
    external_topics: set[str] = set()
    failed_count = 0
    inbox_review_count = 0
    for item in active:
        content_type_counts[item.content_type] = content_type_counts.get(item.content_type, 0) + 1
        if item.last_error:
            failed_count += 1
        if item.topic_l3 == "inbox-review":
            inbox_review_count += 1
        if item.topic_l3 and item.topic_l3 != "inbox-review":
            topic_counts[item.topic_l3] = topic_counts.get(item.topic_l3, 0) + 1
        source_path = Path(item.source_path)
        if item.source_type == "clip" and CURATED_SOURCE_ROOT in source_path.parents:
            external_topics.add(item.topic_l3)
    source_types_ready = all(content_type_counts.get(kind, 0) > 0 for kind in ("article", "pdf", "video"))
    ready = failed_count == 0 and inbox_review_count == 0 and source_types_ready
    return {
        "source_count": len(active),
        "content_type_counts": content_type_counts,
        "topic_counts": dict(sorted(topic_counts.items(), key=lambda pair: (-pair[1], pair[0]))),
        "external_topics": sorted(topic for topic in external_topics if topic),
        "failed_count": failed_count,
        "inbox_review_count": inbox_review_count,
        "launchagent_installed": launch_agent_plist_path(LAUNCH_AGENT_NAME).exists(),
        "launchagent_loaded": launch_agent_loaded(LAUNCH_AGENT_NAME),
        "source_types_ready": source_types_ready,
        "ready": ready,
    }


def render_operating_audit(items: list[SourceItem], *, last_run_at: str) -> None:
    summary = audit_summary(items)
    lines = [
        f"# {PROJECT_NAME} Operating Audit",
        "",
        "## 当前结论",
        "",
        f"- 最近 intake 运行：{last_run_at}",
        f"- 运营验收：{'通过' if summary['ready'] else '待继续观察'}",
        f"- 来源总数：{summary['source_count']}",
        f"- 来源类型覆盖：article={summary['content_type_counts'].get('article', 0)} / pdf={summary['content_type_counts'].get('pdf', 0)} / video={summary['content_type_counts'].get('video', 0)}",
        f"- inbox-review：{summary['inbox_review_count']}",
        f"- 失败来源：{summary['failed_count']}",
        f"- launch agent：installed={summary['launchagent_installed']} loaded={summary['launchagent_loaded']}",
        "",
        "## 已有真实外部来源的主题",
        "",
    ]
    if summary["external_topics"]:
        lines.extend(f"- `{topic}`" for topic in summary["external_topics"])
    else:
        lines.append("- 暂无")
    lines.extend(
        [
            "",
            "## 当前主题覆盖",
            "",
        ]
    )
    for topic_id, count in summary["topic_counts"].items():
        lines.append(f"- `{topic_id}`：{count} 条活跃来源")
    lines.append("")
    write_text(OPERATING_AUDIT_MD, "\n".join(lines))


def project_binding() -> dict[str, Any]:
    return {
        "project_name": PROJECT_NAME,
        "session_id": "",
        "binding_scope": "project",
        "binding_board_path": str(PROJECT_BOARD_PATH),
        "topic_name": "",
        "rollup_target": str(PROJECT_BOARD_PATH),
    }


def run_once() -> dict[str, Any]:
    ensure_structure()
    toolchain = verify_toolchain()
    if not toolchain["ok"]:
        return {"ok": False, "error": "missing_toolchain", "toolchain": toolchain}

    state = load_state()
    sources_state = state.setdefault("sources", {})
    previous_render_signature = str(state.get("last_render_signature", "") or "")
    processed = 0
    seen_keys: set[str] = set()
    collected_items: list[SourceItem] = []

    for source_type, paths, builder in (
        ("clip", clip_sources(), build_clip_item),
        ("pdf", pdf_sources(), build_pdf_item),
    ):
        for path in paths:
            source_key = make_source_key(source_type, path)
            state_row = sources_state.get(source_key, {})
            try:
                item = builder(path, state_row)
            except Exception as exc:
                item = build_failed_item(source_type, path, state_row, str(exc))
            seen_keys.add(item.source_key)
            previous = sources_state.get(item.source_key, {})
            if previous.get("fingerprint") != item.fingerprint or previous.get("last_error") != item.last_error:
                processed += 1
            collected_items.append(item)

    items = promote_topic_items(resolve_duplicates(collected_items))

    for item in items:
        sources_state[item.source_key] = {
            "source_type": item.source_type,
            "title": item.title,
            "source_path": item.source_path,
            "updated_at": item.updated_at,
            "fingerprint": item.fingerprint,
            "companion_path": item.companion_path,
            "candidate_topics": item.candidate_topics,
            "topic_l1": item.topic_l1,
            "topic_l2": item.topic_l2,
            "topic_l3": item.topic_l3,
            "content_type": item.content_type,
            "project_refs": item.project_refs,
            "routing_confidence": item.routing_confidence,
            "status": item.status,
            "extraction_method": item.extraction_method,
            "canonical_ref": item.canonical_ref,
            "duplicate_of": item.duplicate_of,
            "last_error": item.last_error,
            "retry_count": item.retry_count,
            "extra": item.extra,
        }

    for source_key in list(sources_state.keys()):
        if source_key not in seen_keys:
            sources_state.pop(source_key, None)
            processed += 1

    last_run_at = iso_now()
    state["last_run_at"] = last_run_at
    render_source_registry(items)
    render_project_memory(items, last_run_at=last_run_at)
    render_topics_readme(items)
    render_topic_pages(items)
    render_operating_audit(items, last_run_at=last_run_at)
    current_render_signature = rendered_output_signature()
    render_changed = current_render_signature != previous_render_signature
    state["last_render_signature"] = current_render_signature
    save_state(state)

    if processed or render_changed:
        record_project_writeback(
            project_binding(),
            source="knowledge-intake",
            changed_targets=["project_memory", "source_registry", "knowledge_topics", "knowledge_intake"],
            trigger_dashboard_sync=True,
        )
    return {
        "ok": True,
        "last_run_at": last_run_at,
        "processed_count": processed,
        "render_changed": render_changed,
        "clip_count": sum(1 for item in items if item.source_type == "clip"),
        "pdf_count": sum(1 for item in items if item.source_type == "pdf"),
        "items": [item.__dict__ for item in items],
        "toolchain": toolchain,
    }


def plist_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def plist_value(value: Any, indent: str = "    ") -> list[str]:
    if isinstance(value, bool):
        return [f"{indent}<{str(value).lower()}/>" ]
    if isinstance(value, int):
        return [f"{indent}<integer>{value}</integer>"]
    if isinstance(value, dict):
        lines = [f"{indent}<dict>"]
        for key, item in value.items():
            lines.append(f"{indent}  <key>{plist_escape(str(key))}</key>")
            lines.extend(plist_value(item, indent + "  "))
        lines.append(f"{indent}</dict>")
        return lines
    if isinstance(value, (list, tuple)):
        lines = [f"{indent}<array>"]
        for item in value:
            lines.extend(plist_value(item, indent + "  "))
        lines.append(f"{indent}</array>")
        return lines
    return [f"{indent}<string>{plist_escape(str(value))}</string>"]


def plist_dumps(payload: dict[str, Any]) -> str:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">',
        "  <dict>",
    ]
    for key, value in payload.items():
        lines.append(f"    <key>{plist_escape(str(key))}</key>")
        lines.extend(plist_value(value, "    "))
    lines.extend(["  </dict>", "</plist>"])
    return "\n".join(lines) + "\n"


def run_launchctl(*parts: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["launchctl", *parts], text=True, capture_output=True, check=False)


def cmd_bootstrap(_args: argparse.Namespace) -> int:
    print(json.dumps({"ok": True, "structure": ensure_structure()}, ensure_ascii=False, indent=2))
    return 0


def cmd_run_once(_args: argparse.Namespace) -> int:
    result = run_once()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") else 1


def cmd_status(_args: argparse.Namespace) -> int:
    ensure_structure()
    state = load_state()
    items = state.get("sources", {})
    queue = {
        "installed": launch_agent_plist_path(LAUNCH_AGENT_NAME).exists(),
        "loaded": launch_agent_loaded(LAUNCH_AGENT_NAME),
        "plist": str(launch_agent_plist_path(LAUNCH_AGENT_NAME)),
    }
    print(
        json.dumps(
            {
                "project_name": PROJECT_NAME,
                "last_run_at": state.get("last_run_at", ""),
                "source_count": len(items),
                "clip_inbox": str(CLIP_INBOX),
                "workspace_briefs": str(WORKSPACE_BRIEF_ROOT),
                "curated_external": str(CURATED_SOURCE_ROOT),
                "curated_source_seeds": str(CURATED_SOURCE_SEEDS_PATH),
                "pdf_inbox": str(PDF_INBOX),
                "project_memory": str(PROJECT_MEMORY_MD),
                "source_registry": str(SOURCE_REGISTRY_MD),
                "operating_audit": str(OPERATING_AUDIT_MD),
                "state_path": str(STATE_JSON),
                "launchagent": queue,
                "toolchain": verify_toolchain(),
                "audit": audit_summary(
                    [
                        SourceItem(
                            source_key=key,
                            source_type=str(row.get("source_type", "")),
                            title=str(row.get("title", "")),
                            source_path=str(row.get("source_path", "")),
                            updated_at=str(row.get("updated_at", "")),
                            fingerprint=str(row.get("fingerprint", "")),
                            companion_path=str(row.get("companion_path", "")),
                            candidate_topics=[str(item) for item in row.get("candidate_topics", [])],
                            topic_l1=str(row.get("topic_l1", "")),
                            topic_l2=str(row.get("topic_l2", "")),
                            topic_l3=str(row.get("topic_l3", "")),
                            content_type=str(row.get("content_type", "")),
                            project_refs=[str(item) for item in row.get("project_refs", [])],
                            routing_confidence=str(row.get("routing_confidence", "")),
                            status=str(row.get("status", "")),
                            extraction_method=str(row.get("extraction_method", "")),
                            canonical_ref=str(row.get("canonical_ref", "")),
                            duplicate_of=str(row.get("duplicate_of", "")),
                            last_error=str(row.get("last_error", "")),
                            retry_count=int(row.get("retry_count") or 0),
                            extra=row.get("extra", {}) if isinstance(row.get("extra", {}), dict) else {},
                        )
                        for key, row in items.items()
                    ]
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def launch_agent_payload(*, hour: int, minute: int) -> dict[str, Any]:
    python_path = subprocess.run(
        ["python3", "-c", "import sys; print(sys.executable)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "Label": LAUNCH_AGENT_NAME,
        "ProgramArguments": [python_path, str(REPO_ROOT / "ops" / "knowledge_intake.py"), "run-once"],
        "RunAtLoad": True,
        "StartCalendarInterval": {"Hour": int(hour), "Minute": int(minute)},
        "StandardOutPath": str(LOG_STDOUT),
        "StandardErrorPath": str(LOG_STDERR),
        "WorkingDirectory": str(REPO_ROOT),
        "EnvironmentVariables": default_env(),
    }


def cmd_install_launchagent(args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(LAUNCH_AGENT_NAME)
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    LOG_STDOUT.parent.mkdir(parents=True, exist_ok=True)
    plist_path.write_text(plist_dumps(launch_agent_payload(hour=args.hour, minute=args.minute)), encoding="utf-8")
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    bootstrap = run_launchctl("bootstrap", domain, str(plist_path))
    if bootstrap.returncode != 0:
        print(bootstrap.stderr.strip(), file=sys.stderr)
        return bootstrap.returncode
    kickstart = run_launchctl("kickstart", "-k", f"{domain}/{LAUNCH_AGENT_NAME}")
    if kickstart.returncode != 0:
        print(kickstart.stderr.strip(), file=sys.stderr)
        return kickstart.returncode
    print(json.dumps({"installed": True, "plist": str(plist_path), "hour": args.hour, "minute": args.minute}, ensure_ascii=False))
    return 0


def cmd_uninstall_launchagent(_args: argparse.Namespace) -> int:
    plist_path = launch_agent_plist_path(LAUNCH_AGENT_NAME)
    domain = f"gui/{os.getuid()}"
    run_launchctl("bootout", domain, str(plist_path))
    if plist_path.exists():
        plist_path.unlink()
    print(json.dumps({"installed": False, "plist": str(plist_path)}, ensure_ascii=False))
    return 0


def cmd_seed_project_briefs(args: argparse.Namespace) -> int:
    print(json.dumps(seed_project_briefs(include_self=args.include_self), ensure_ascii=False, indent=2))
    return 0


def cmd_seed_curated_sources(args: argparse.Namespace) -> int:
    print(json.dumps(seed_curated_sources(seed_ids=args.seed_id or []), ensure_ascii=False, indent=2))
    return 0


def cmd_fetch_url(args: argparse.Namespace) -> int:
    payload = fetch_html_excerpt(str(args.url or "").strip(), persist_artifact=True)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Daily knowledge intake for clips and PDFs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_cmd = subparsers.add_parser("bootstrap")
    bootstrap_cmd.set_defaults(func=cmd_bootstrap)

    run_once_cmd = subparsers.add_parser("run-once")
    run_once_cmd.set_defaults(func=cmd_run_once)

    status_cmd = subparsers.add_parser("status")
    status_cmd.set_defaults(func=cmd_status)

    install_cmd = subparsers.add_parser("install-launchagent")
    install_cmd.add_argument("--hour", type=int, default=4)
    install_cmd.add_argument("--minute", type=int, default=0)
    install_cmd.set_defaults(func=cmd_install_launchagent)

    uninstall_cmd = subparsers.add_parser("uninstall-launchagent")
    uninstall_cmd.set_defaults(func=cmd_uninstall_launchagent)

    seed_cmd = subparsers.add_parser("seed-project-briefs")
    seed_cmd.add_argument("--include-self", action="store_true")
    seed_cmd.set_defaults(func=cmd_seed_project_briefs)

    curated_cmd = subparsers.add_parser("seed-curated-sources")
    curated_cmd.add_argument("--seed-id", action="append", default=[])
    curated_cmd.set_defaults(func=cmd_seed_curated_sources)

    fetch_url_cmd = subparsers.add_parser("fetch-url")
    fetch_url_cmd.add_argument("--url", required=True)
    fetch_url_cmd.set_defaults(func=cmd_fetch_url)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

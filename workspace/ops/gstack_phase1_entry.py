#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import claude_code_runner
except ImportError:  # pragma: no cover
    import claude_code_runner  # type: ignore


ENTRY_ORDER = ["office-hours", "plan-ceo-review", "plan-eng-review"]
EXECUTION_ORDER = ["investigate", "review", "qa", "browse"]
DELIVERY_ORDER = ["document-release", "retro", "ship"]
POSTURE_ORDER = ["careful", "freeze", "unfreeze"]
SECOND_OPINION_ORDER = ["claude-review", "claude-challenge", "claude-consult"]

DIRECT_DO_TERMS = [
    "直接做",
    "不要分析",
    "别分析",
    "不用分析",
    "直接改",
    "直接实现",
    "just do",
    "don't analyze",
]

ENTRY_RULES: dict[str, dict[str, Any]] = {
    "office-hours": {
        "stage": "entry",
        "reason": "问题仍然分散，先做问题重构比直接给方案更稳。",
        "keywords": [
            "梳理",
            "想清楚",
            "切题",
            "还没想清楚",
            "先决定做不做",
            "怎么切",
            "这个想法",
            "这个新项目",
            "reframe",
            "frame this",
        ],
        "initial_action_plan": [
            "先重构问题和目标",
            "拆出范围、约束和开放问题",
            "给出最小可继续推进的下一步",
        ],
    },
    "plan-ceo-review": {
        "stage": "entry",
        "reason": "当前更像产品判断问题，核心是值不值得做、优先级和机会成本。",
        "keywords": [
            "值不值得做",
            "值不值得现在做",
            "产品角度",
            "优先级",
            "机会成本",
            "现在做吗",
            "worth doing",
            "priority",
            "opportunity cost",
        ],
        "initial_action_plan": [
            "先明确用户价值和业务收益",
            "再判断现在做的优先级和机会成本",
            "输出明确的产品判断与下一步建议",
        ],
    },
    "plan-eng-review": {
        "stage": "entry",
        "reason": "当前更像技术可行性判断问题，重点在架构、风险和验证面。",
        "keywords": [
            "技术上",
            "技术风险",
            "架构",
            "可行性",
            "维护",
            "怎么落地",
            "测试",
            "feasible",
            "architecture",
            "technical risk",
            "maintain",
        ],
        "initial_action_plan": [
            "先明确要改动的系统边界",
            "识别主要失败路径和耦合点",
            "给出最小安全落地路径和验证建议",
        ],
    },
}

EXECUTION_RULES: dict[str, dict[str, Any]] = {
    "investigate": {
        "stage": "execution",
        "reason": "当前更像排查问题或定位根因，不适合直接跳到修改或验收。",
        "keywords": [
            "为什么会这样",
            "为什么失败",
            "行为不对",
            "哪里出问题",
            "根因",
            "排查",
            "定位",
            "调查",
            "bug",
            "报错",
            "失败",
            "root cause",
            "why did this fail",
            "diagnose",
        ],
        "initial_action_plan": [
            "先明确预期和实际行为",
            "收集最小必要证据并缩小边界",
            "给出最可能根因和下一步修复建议",
        ],
    },
    "review": {
        "stage": "execution",
        "reason": "当前更像对现有改动、方案或 diff 的风险审查。",
        "keywords": [
            "帮我审一下",
            "review",
            "看有没有问题",
            "这次改动",
            "这份 diff",
            "这个 pr",
            "regression",
            "merge risk",
        ],
        "initial_action_plan": [
            "先界定 review 范围和风险面",
            "找出具体 findings、回归风险和缺失验证",
            "给出是否适合继续或合并的建议",
        ],
    },
    "qa": {
        "stage": "execution",
        "reason": "当前更像实现后的验证与验收问题，不该再停留在计划层。",
        "keywords": [
            "测一下",
            "验收",
            "验证",
            "跑一下",
            "确认能发",
            "确认可用",
            "test it",
            "validate",
            "qa",
            "smoke",
        ],
        "initial_action_plan": [
            "先定义最小验证面和高风险路径",
            "跑最小必要的测试、smoke 或负路径检查",
            "给出通过、失败和未验证部分的结论",
        ],
    },
    "browse": {
        "stage": "execution",
        "reason": "当前更像真实页面、UI 流程或浏览器行为验证问题，需要 live evidence。",
        "keywords": [
            "页面",
            "网页",
            "浏览器",
            "前端",
            "ui",
            "交互",
            "按钮",
            "表单",
            "真实页面",
            "真实浏览器",
            "浏览一下",
            "页面流程",
            "browser",
            "playwright",
            "screenshot",
            "frontend flow",
        ],
        "initial_action_plan": [
            "先界定要验证的页面或用户路径",
            "用真实浏览器复现关键步骤并收集证据",
            "输出已验证行为、失败点和下一步建议",
        ],
    },
}

DELIVERY_RULES: dict[str, dict[str, Any]] = {
    "document-release": {
        "stage": "delivery",
        "reason": "当前更像文档或发布说明同步问题，应从已验证变更出发收口。",
        "keywords": [
            "发布说明",
            "release note",
            "changelog",
            "同步文档",
            "更新文档",
            "使用说明",
            "文档说明",
            "release notes",
            "update docs",
            "write docs",
        ],
        "initial_action_plan": [
            "先界定要同步的文档或受众范围",
            "只提取已验证的变更和结论",
            "输出可发布的文档更新与剩余 caveats",
        ],
    },
    "retro": {
        "stage": "delivery",
        "reason": "当前更像一个阶段结束后的复盘问题，不是继续实现或审查。",
        "keywords": [
            "复盘",
            "回顾",
            "总结这一轮",
            "retrospective",
            "retro",
            "postmortem",
            "lessons learned",
        ],
        "initial_action_plan": [
            "先固定复盘范围和时间盒",
            "区分事实结果、摩擦点和有效做法",
            "输出少量可执行的下轮改进项",
        ],
    },
    "ship": {
        "stage": "delivery",
        "reason": "当前更像发布、提交、交付或正式 handoff 前的 readiness 判断问题。",
        "keywords": [
            "发版",
            "上线",
            "交付",
            "提交发布",
            "准备发",
            "准备上线",
            "准备交付",
            "ship it",
            "ready to ship",
            "handoff",
            "submit",
        ],
        "initial_action_plan": [
            "先界定要发布或交付的范围",
            "检查 readiness、caveats 和 rollback 预期",
            "输出最小安全交付路径与是否可现在 ship 的判断",
        ],
    },
}

POSTURE_RULES: dict[str, dict[str, Any]] = {
    "careful": {
        "stage": "posture",
        "reason": "当前任务可以继续，但风险、模糊性或外部影响要求更窄、更显式的谨慎姿态。",
        "keywords": [
            "谨慎一点",
            "小心一点",
            "高风险",
            "风险高",
            "先保守",
            "先稳一点",
            "careful",
            "be careful",
            "safer path",
        ],
        "initial_action_plan": [
            "先说明当前风险和为什么要收紧姿态",
            "缩小允许动作范围并保留 dry-run / preview",
            "给出谨慎姿态下可继续推进的下一步",
        ],
    },
    "freeze": {
        "stage": "posture",
        "reason": "当前更适合先冻结写操作或发布活动，再继续只读排查或等待明确 gate。",
        "keywords": [
            "先冻结",
            "先别动",
            "暂停发布",
            "不要写",
            "先停下来",
            "freeze",
            "read-only first",
            "stop changes",
        ],
        "initial_action_plan": [
            "先界定冻结范围和被阻断的动作",
            "保留只读排查或验证准备等允许动作",
            "写清楚解除冻结所需的 gate",
        ],
    },
    "unfreeze": {
        "stage": "posture",
        "reason": "当前更像判断先前 freeze 条件是否满足，以及能否安全恢复执行。",
        "keywords": [
            "解除冻结",
            "可以继续了吗",
            "现在能放开吗",
            "unfreeze",
            "resume changes",
            "lift the freeze",
        ],
        "initial_action_plan": [
            "先重述原 freeze 范围和解除条件",
            "检查 gate 是否真的满足",
            "给出恢复后的最小安全下一步",
        ],
    },
}

SECOND_OPINION_RULES: dict[str, dict[str, Any]] = {
    "claude-review": {
        "stage": "second-opinion",
        "reason": "当前已经有具体方案、改动或判断，适合再加一轮独立复审来降低盲区。",
        "keywords": [
            "claude-review",
            "再审一下",
            "复审",
            "再给一版 review",
            "换个模型再审",
        ],
        "initial_action_plan": [
            "先固定需要二次复审的对象和问题",
            "区分主判断、第二意见的认同点和分歧点",
            "把第二意见回收进主线程的最终判断",
        ],
    },
    "claude-challenge": {
        "stage": "second-opinion",
        "reason": "当前更需要最强反方和压力测试，而不是再来一版平衡式 review。",
        "keywords": [
            "claude-challenge",
            "挑战一下",
            "站在反方",
            "唱反调",
            "挑刺",
            "最强反对",
            "pressure test",
            "devil's advocate",
        ],
        "initial_action_plan": [
            "先写清要挑战的主张、方案或上线判断",
            "收拢最强反方论点和最危险假设",
            "明确什么证据会推翻当前判断",
        ],
    },
    "claude-consult": {
        "stage": "second-opinion",
        "reason": "当前更像要一个轻量顾问式视角，用来重构 framing、tradeoff 或备选路径。",
        "keywords": [
            "claude-consult",
            "顾问意见",
            "咨询一下",
            "再给个建议",
            "专家意见",
            "consult",
            "advisory opinion",
        ],
        "initial_action_plan": [
            "先固定要咨询的决策问题",
            "明确当前 framing 和候选方案",
            "吸收补充视角后再调整主线程建议",
        ],
    },
}

SECOND_OPINION_MARKERS = [
    "claude",
    "第二意见",
    "second opinion",
    "另一个模型",
    "另一种意见",
]

SECOND_OPINION_REVIEW_TERMS = [
    "review",
    "审一下",
    "复审",
    "再看一遍",
    "re-review",
]

SECOND_OPINION_CHALLENGE_TERMS = [
    "challenge",
    "挑战一下",
    "站在反方",
    "唱反调",
    "挑刺",
    "pressure test",
    "devil's advocate",
]

SECOND_OPINION_CONSULT_TERMS = [
    "consult",
    "咨询一下",
    "顾问意见",
    "专家意见",
    "再给个建议",
    "advisory",
]

SECOND_OPINION_MODE_MAP = {
    "claude-review": "review",
    "claude-challenge": "challenge",
    "claude-consult": "consult",
}
SECOND_OPINION_REQUIRED_FIELDS = ["question", "artifact", "current_judgment", "extra_context"]
SECOND_OPINION_REQUEST_SCHEMA_VERSION = "codex-hub.second-opinion.request.v1"
SECOND_OPINION_RESPONSE_SCHEMA_VERSION = "codex-hub.second-opinion.response.v1"
SECOND_OPINION_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "question_or_focus": {"type": "string"},
        "key_judgment": {"type": "string"},
        "difference_from_current_judgment": {"type": "string"},
        "recommended_next_step": {"type": "string"},
        "evidence_needed": {"type": "string"},
    },
    "required": [
        "status",
        "question_or_focus",
        "key_judgment",
        "difference_from_current_judgment",
        "recommended_next_step",
    ],
    "additionalProperties": False,
}
SECOND_OPINION_PACKAGE_TEMPLATES: dict[str, dict[str, str]] = {
    "claude-review": {
        "template_id": "review-risk-scan",
        "default_question": "这个改动或方案最大的回归风险、盲区或缺失验证是什么？",
        "artifact_prefix": "待复审对象",
        "artifact_fallback": "请主线程补充 diff、方案摘要或待复审对象。",
        "default_current_judgment": "当前主线程判断：这项改动或方案看起来可以继续，但需要独立复审来找盲区。",
        "default_extra_context": "请重点看回归风险、边界条件、缺失验证和发布影响。",
    },
    "claude-challenge": {
        "template_id": "challenge-pressure-test",
        "default_question": "当前判断最强的反对意见或不上线理由是什么？",
        "artifact_prefix": "待挑战的判断或发布决定",
        "artifact_fallback": "请主线程补充待挑战的判断、发布决定或交付对象。",
        "default_current_judgment": "当前主线程判断：这个判断暂时可接受，但需要反方压力测试。",
        "default_extra_context": "请重点给出最强反对意见、失败路径、被忽略的前置条件和推翻当前判断所需证据。",
    },
    "claude-consult": {
        "template_id": "consult-tradeoff-check",
        "default_question": "这个方案最大的 tradeoff、framing 偏差或备选路径是什么？",
        "artifact_prefix": "待咨询的方案或决策",
        "artifact_fallback": "请主线程补充待咨询的方案、计划或决策摘要。",
        "default_current_judgment": "当前主线程判断：方向可以继续，但希望补一轮顾问式视角。",
        "default_extra_context": "请重点看 framing、tradeoff、替代路径和执行上的隐性代价。",
    },
}
SECOND_OPINION_MAX_STATUS_LINES = 8
SECOND_OPINION_MAX_PLAN_STEPS = 5
SECOND_OPINION_MAX_CHANGED_FILES = 12
SECOND_OPINION_MAX_DIFF_LINES = 18
SECOND_OPINION_MAX_ARTIFACT_PREVIEW_LINES = 5


def _run_text_command(
    command: list[str], *, cwd: Path = REPO_ROOT, timeout_seconds: float = 2.0
) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _trim_nonempty_lines(text: str, limit: int) -> list[str]:
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    return lines[:limit]


def _trim_diff_excerpt(text: str, limit: int) -> list[str]:
    kept: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            continue
        if line.startswith(("diff --git ", "@@", "--- ", "+++ ", "+", "-")):
            kept.append(line)
        if len(kept) >= limit:
            break
    return kept


def _categorize_changed_files(changed_files: list[str]) -> dict[str, list[str]]:
    buckets = {"source": [], "tests": [], "docs": [], "config": []}
    for entry in changed_files:
        value = entry.strip()
        if not value:
            continue
        lower = value.lower()
        name = Path(lower).name
        if (
            lower.startswith("tests/")
            or "/tests/" in f"/{lower}"
            or name.startswith("test_")
            or name.endswith("_test.py")
            or ".spec." in name
            or ".test." in name
        ):
            buckets["tests"].append(value)
        elif (
            lower.startswith("docs/")
            or "/docs/" in f"/{lower}"
            or lower.endswith((".md", ".rst", ".txt", ".adoc"))
        ):
            buckets["docs"].append(value)
        elif lower.endswith((".yaml", ".yml", ".json", ".toml", ".ini", ".cfg", ".lock")):
            buckets["config"].append(value)
        else:
            buckets["source"].append(value)
    return buckets


def _preview_multiline(text: str, limit: int = SECOND_OPINION_MAX_ARTIFACT_PREVIEW_LINES) -> str:
    return "\n".join(_trim_nonempty_lines(text, limit))


def detect_git_repo_root(repo_root: Path = REPO_ROOT) -> Path | None:
    resolved = _run_text_command(["git", "rev-parse", "--show-toplevel"], cwd=repo_root)
    if not resolved:
        return None
    return Path(resolved).resolve()


def collect_git_worktree_snapshot(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    git_root = detect_git_repo_root(repo_root)
    if not git_root:
        return {}

    branch = _run_text_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=git_root)
    head_commit = _run_text_command(["git", "log", "-1", "--pretty=format:%h %s"], cwd=git_root)
    status_lines = _trim_nonempty_lines(
        _run_text_command(["git", "status", "--short"], cwd=git_root),
        SECOND_OPINION_MAX_STATUS_LINES,
    )
    staged_stat = _run_text_command(["git", "diff", "--shortstat", "--cached"], cwd=git_root)
    unstaged_stat = _run_text_command(["git", "diff", "--shortstat"], cwd=git_root)
    staged_diff_excerpt = _trim_diff_excerpt(
        _run_text_command(
            ["git", "diff", "--cached", "--unified=0", "--no-color"],
            cwd=git_root,
            timeout_seconds=4.0,
        ),
        SECOND_OPINION_MAX_DIFF_LINES,
    )
    unstaged_diff_excerpt = _trim_diff_excerpt(
        _run_text_command(
            ["git", "diff", "--unified=0", "--no-color"],
            cwd=git_root,
            timeout_seconds=4.0,
        ),
        SECOND_OPINION_MAX_DIFF_LINES,
    )

    changed_files: list[str] = []
    for command in (
        ["git", "diff", "--name-only", "--cached"],
        ["git", "diff", "--name-only"],
    ):
        for entry in _trim_nonempty_lines(
            _run_text_command(command, cwd=git_root), SECOND_OPINION_MAX_CHANGED_FILES
        ):
            if entry not in changed_files:
                changed_files.append(entry)
    file_buckets = _categorize_changed_files(changed_files)

    return {
        "git_root": str(git_root),
        "branch": branch or "unknown",
        "head_commit": head_commit,
        "status_lines": status_lines,
        "staged_stat": staged_stat,
        "unstaged_stat": unstaged_stat,
        "staged_diff_excerpt": staged_diff_excerpt,
        "unstaged_diff_excerpt": unstaged_diff_excerpt,
        "changed_files": changed_files,
        "file_buckets": file_buckets,
        "dirty": bool(status_lines or staged_stat or unstaged_stat),
    }


def build_review_second_opinion_materials(*, prompt: str = "", repo_root: Path = REPO_ROOT) -> dict[str, str]:
    snapshot = collect_git_worktree_snapshot(repo_root)
    if not snapshot:
        return {}

    buckets = snapshot.get("file_buckets", {})
    source_files = list(buckets.get("source", []))
    test_files = list(buckets.get("tests", []))
    doc_files = list(buckets.get("docs", []))
    config_files = list(buckets.get("config", []))
    lines = [
        "当前 review 证据包",
        f"- 仓库: {snapshot['git_root']}",
        f"- 分支: {snapshot['branch']}",
    ]
    if snapshot.get("head_commit"):
        lines.append(f"- 最近提交: {snapshot['head_commit']}")
    lines.append(f"- 工作树状态: {'dirty' if snapshot['dirty'] else 'clean'}")
    if prompt.strip():
        lines.append(f"- 原始请求: {prompt.strip()}")
    lines.append(
        "- 变更分类: "
        f"source={len(source_files)} test={len(test_files)} doc={len(doc_files)} config={len(config_files)}"
    )
    if snapshot["status_lines"]:
        lines.append("- 当前变更:")
        lines.extend(f"  - {line}" for line in snapshot["status_lines"])
    if snapshot.get("staged_stat"):
        lines.append(f"- staged diff summary: {snapshot['staged_stat']}")
    if snapshot.get("unstaged_stat"):
        lines.append(f"- unstaged diff summary: {snapshot['unstaged_stat']}")
    if source_files:
        lines.append("- 代码文件:")
        lines.extend(f"  - {line}" for line in source_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if test_files:
        lines.append("- 测试文件:")
        lines.extend(f"  - {line}" for line in test_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if doc_files:
        lines.append("- 文档文件:")
        lines.extend(f"  - {line}" for line in doc_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if config_files:
        lines.append("- 配置文件:")
        lines.extend(f"  - {line}" for line in config_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if snapshot.get("staged_diff_excerpt"):
        lines.append("- staged diff 片段:")
        lines.extend(f"  {line}" for line in snapshot["staged_diff_excerpt"])
    if snapshot.get("unstaged_diff_excerpt"):
        lines.append("- unstaged diff 片段:")
        lines.extend(f"  {line}" for line in snapshot["unstaged_diff_excerpt"])
    if snapshot["changed_files"] and not any((source_files, test_files, doc_files, config_files)):
        lines.append("- 相关文件:")
        lines.extend(f"  - {line}" for line in snapshot["changed_files"])
    if not snapshot["dirty"]:
        lines.append("- 当前工作树干净；如需更细 review/ship 证据，请补充具体 commit、diff 或发布对象。")

    if source_files and not test_files:
        judgment = (
            "当前主线程判断：存在代码改动，但没有看到对应测试文件改动；复审时应优先检查回归面和缺失验证。"
        )
    elif snapshot["dirty"]:
        judgment = "当前主线程判断：工作树仍有未收口改动；复审时要先限定这次 review 的真实范围。"
    else:
        judgment = (
            "当前主线程判断：当前改动值得继续推进，但 second-opinion 应按真实 diff 片段、变更分类和测试覆盖信号来找盲区。"
        )
    extra_context = (
        "自动取材来源：git review evidence packet。请重点检查 diff 片段、代码/测试/文档分布，以及是否存在缺失验证或越界改动。"
    )

    return {
        "artifact": "\n".join(lines),
        "current_judgment": judgment,
        "extra_context": extra_context,
    }


def build_ship_second_opinion_materials(*, prompt: str = "", repo_root: Path = REPO_ROOT) -> dict[str, str]:
    snapshot = collect_git_worktree_snapshot(repo_root)
    if not snapshot:
        return {}

    buckets = snapshot.get("file_buckets", {})
    source_files = list(buckets.get("source", []))
    test_files = list(buckets.get("tests", []))
    doc_files = list(buckets.get("docs", []))
    config_files = list(buckets.get("config", []))
    lines = [
        "当前 ship 证据包",
        f"- 仓库: {snapshot['git_root']}",
        f"- 分支: {snapshot['branch']}",
    ]
    if snapshot.get("head_commit"):
        lines.append(f"- rollback anchor: {snapshot['head_commit']}")
    lines.append(f"- 工作树状态: {'dirty' if snapshot['dirty'] else 'clean'}")
    if prompt.strip():
        lines.append(f"- 原始请求: {prompt.strip()}")
    lines.append("- 发布 gate 摘要:")
    lines.append(f"  - 工作树干净: {'否' if snapshot['dirty'] else '是'}")
    lines.append(f"  - 代码文件: {len(source_files)}")
    lines.append(f"  - 测试文件: {len(test_files)}")
    lines.append(f"  - 文档文件: {len(doc_files)}")
    lines.append(f"  - 配置文件: {len(config_files)}")
    if snapshot["status_lines"]:
        lines.append("- 当前变更:")
        lines.extend(f"  - {line}" for line in snapshot["status_lines"])
    if snapshot.get("staged_stat"):
        lines.append(f"- staged diff summary: {snapshot['staged_stat']}")
    if snapshot.get("unstaged_stat"):
        lines.append(f"- unstaged diff summary: {snapshot['unstaged_stat']}")
    if source_files:
        lines.append("- 交付涉及代码文件:")
        lines.extend(f"  - {line}" for line in source_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if test_files:
        lines.append("- 交付涉及测试文件:")
        lines.extend(f"  - {line}" for line in test_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if doc_files:
        lines.append("- 交付涉及文档文件:")
        lines.extend(f"  - {line}" for line in doc_files[:SECOND_OPINION_MAX_CHANGED_FILES])
    if snapshot.get("staged_diff_excerpt"):
        lines.append("- staged diff 片段:")
        lines.extend(f"  {line}" for line in snapshot["staged_diff_excerpt"])
    if snapshot.get("unstaged_diff_excerpt"):
        lines.append("- unstaged diff 片段:")
        lines.extend(f"  {line}" for line in snapshot["unstaged_diff_excerpt"])

    if snapshot["dirty"]:
        judgment = (
            "当前主线程判断：候选发布对象所在工作树仍不干净，ship 之前要先确认未收口改动是否属于本次范围。"
        )
    elif source_files and not test_files:
        judgment = "当前主线程判断：候选发布对象涉及代码改动，但没有看到对应测试文件改动；ship 前应补强 QA 证据。"
    elif source_files and not doc_files:
        judgment = "当前主线程判断：候选发布对象涉及代码改动，但没有看到明显文档/发布说明改动；ship 前要确认对外说明是否足够。"
    else:
        judgment = "当前主线程判断：候选发布对象已具备基本 ship 证据，但仍需要反方压力测试确认 release gate 是否真的足够。"

    extra_context = (
        "自动取材来源：git ship evidence packet。请重点检查 release gate、rollback anchor、测试/文档覆盖和 diff 片段里的潜在发版阻塞点。"
    )

    return {
        "artifact": "\n".join(lines),
        "current_judgment": judgment,
        "extra_context": extra_context,
    }


def build_plan_second_opinion_materials(
    skill: str,
    *,
    prompt: str = "",
    trigger_path: list[str] | None = None,
    workflow_detection: dict[str, Any] | None = None,
) -> dict[str, str]:
    workflow = workflow_detection or {}
    suggested_path = [
        str(item).strip()
        for item in (workflow.get("suggested_path") or trigger_path or [])
        if str(item).strip()
    ]
    initial_action_plan = [
        str(item).strip()
        for item in (workflow.get("initial_action_plan") or [])
        if str(item).strip()
    ][:SECOND_OPINION_MAX_PLAN_STEPS]
    lines = ["当前方案摘要"]
    if prompt.strip():
        lines.append(f"- 原始请求: {prompt.strip()}")
    if workflow.get("recognized_stage"):
        lines.append(f"- 识别层级: {workflow['recognized_stage']}")
    if suggested_path:
        lines.append(f"- 识别路径: {' -> '.join(suggested_path)}")
    if workflow.get("assistant_message"):
        lines.append(f"- 当前系统判断: {str(workflow['assistant_message']).strip()}")
    if initial_action_plan:
        lines.append("- 当前建议行动:")
        lines.extend(f"  {idx}. {step}" for idx, step in enumerate(initial_action_plan, start=1))

    return {
        "artifact": "\n".join(lines),
        "current_judgment": (
            "当前主线程判断：方向可以继续，但希望先对 framing、tradeoff 和替代路径再补一轮顾问式 second opinion。"
        ),
        "extra_context": (
            "自动取材来源：workflow detection summary。请重点检查路径选择、机会成本、隐藏 tradeoff 和更窄的下一步。"
        ),
    }


def extract_second_opinion_autofill_materials(
    skill: str,
    *,
    prompt: str = "",
    trigger_path: list[str] | None = None,
    workflow_detection: dict[str, Any] | None = None,
    repo_root: Path = REPO_ROOT,
) -> dict[str, str]:
    if skill == "claude-review":
        return build_review_second_opinion_materials(prompt=prompt, repo_root=repo_root)
    if skill == "claude-challenge":
        return build_ship_second_opinion_materials(prompt=prompt, repo_root=repo_root)
    if skill == "claude-consult":
        return build_plan_second_opinion_materials(
            skill,
            prompt=prompt,
            trigger_path=trigger_path,
            workflow_detection=workflow_detection,
        )
    return {}


def normalize(text: str) -> str:
    return " ".join(text.strip().lower().split())


def contains_any(text: str, terms: list[str]) -> bool:
    lowered = normalize(text)
    return any(term.lower() in lowered for term in terms)


def build_chain_plan(skills: list[str]) -> list[str]:
    plan: list[str] = []
    for skill in skills:
        rule = (
            ENTRY_RULES.get(skill)
            or EXECUTION_RULES.get(skill)
            or DELIVERY_RULES.get(skill)
            or POSTURE_RULES.get(skill)
            or SECOND_OPINION_RULES.get(skill)
            or {}
        )
        for step in rule.get("initial_action_plan", []):
            if isinstance(step, str):
                plan.append(step)
    return plan


def matches_second_opinion_skill(skill: str, text: str) -> bool:
    rule = SECOND_OPINION_RULES[skill]
    if contains_any(text, rule["keywords"]):
        return True

    if skill == "claude-review":
        return contains_any(text, SECOND_OPINION_MARKERS) and (
            contains_any(text, SECOND_OPINION_REVIEW_TERMS)
            or (
                not contains_any(text, SECOND_OPINION_CHALLENGE_TERMS)
                and not contains_any(text, SECOND_OPINION_CONSULT_TERMS)
            )
        )

    if skill == "claude-challenge":
        return contains_any(text, SECOND_OPINION_MARKERS) and contains_any(
            text, SECOND_OPINION_CHALLENGE_TERMS
        )

    if skill == "claude-consult":
        return contains_any(text, SECOND_OPINION_MARKERS) and contains_any(
            text, SECOND_OPINION_CONSULT_TERMS
        )

    return False


def suggest_second_opinion_skill_from_path(suggested_path: list[str]) -> dict[str, Any]:
    cleaned_path = [str(item).strip() for item in suggested_path if str(item).strip()]
    explicit_skill = next((skill for skill in cleaned_path if skill in SECOND_OPINION_ORDER), "")
    if explicit_skill:
        return {
            "skill": explicit_skill,
            "source": "explicit",
            "rationale": "当前请求已经显式命中第二意见层。",
            "required_fields": list(SECOND_OPINION_REQUIRED_FIELDS),
            "request_schema_version": SECOND_OPINION_REQUEST_SCHEMA_VERSION,
            "response_schema_version": SECOND_OPINION_RESPONSE_SCHEMA_VERSION,
        }
    if "ship" in cleaned_path:
        skill = "claude-challenge"
        rationale = "发版/交付判断更适合先走反方压力测试。"
    elif "review" in cleaned_path:
        skill = "claude-review"
        rationale = "现有方案或改动审查更适合先走独立复审。"
    elif "plan-eng-review" in cleaned_path or "plan-ceo-review" in cleaned_path:
        skill = "claude-consult"
        rationale = "方案评审问题更适合补一轮顾问式 second opinion。"
    else:
        return {}
    return {
        "skill": skill,
        "source": "followup",
        "rationale": rationale,
        "required_fields": list(SECOND_OPINION_REQUIRED_FIELDS),
        "request_schema_version": SECOND_OPINION_REQUEST_SCHEMA_VERSION,
        "response_schema_version": SECOND_OPINION_RESPONSE_SCHEMA_VERSION,
    }


def build_second_opinion_package(
    skill: str,
    *,
    prompt: str = "",
    question: str = "",
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    trigger_path: list[str] | None = None,
    source: str = "manual",
    workflow_detection: dict[str, Any] | None = None,
    repo_root: Path = REPO_ROOT,
) -> dict[str, Any]:
    if skill not in SECOND_OPINION_PACKAGE_TEMPLATES:
        raise ValueError(f"Unsupported second-opinion packaging skill: {skill}")

    template = SECOND_OPINION_PACKAGE_TEMPLATES[skill]
    prompt_text = prompt.strip()
    trigger = [str(item).strip() for item in (trigger_path or []) if str(item).strip()]
    autofilled_fields: list[str] = []
    extracted_materials = extract_second_opinion_autofill_materials(
        skill,
        prompt=prompt_text,
        trigger_path=trigger,
        workflow_detection=workflow_detection,
        repo_root=repo_root,
    )

    resolved_question = question.strip()
    if not resolved_question:
        resolved_question = template["default_question"]
        autofilled_fields.append("question")

    resolved_artifact = artifact.strip() or str(extracted_materials.get("artifact", "")).strip()
    if not resolved_artifact:
        artifact_body = prompt_text or template["artifact_fallback"]
        resolved_artifact = f"{template['artifact_prefix']}：{artifact_body}"
    if not artifact.strip():
        autofilled_fields.append("artifact")

    resolved_current_judgment = current_judgment.strip() or str(
        extracted_materials.get("current_judgment", "")
    ).strip()
    if not resolved_current_judgment:
        resolved_current_judgment = template["default_current_judgment"]
    if not current_judgment.strip():
        autofilled_fields.append("current_judgment")

    resolved_extra_context = extra_context.strip() or str(
        extracted_materials.get("extra_context", "")
    ).strip()
    if not resolved_extra_context:
        parts = [template["default_extra_context"]]
        if trigger:
            parts.append(f"当前来源工作流：{' -> '.join(trigger)}。")
        if prompt_text:
            parts.append(f"原始请求：{prompt_text}")
        resolved_extra_context = " ".join(parts)
    if not extra_context.strip():
        autofilled_fields.append("extra_context")

    return {
        "template_id": template["template_id"],
        "skill": skill,
        "source": source,
        "trigger_path": trigger,
        "autofilled_fields": autofilled_fields,
        "material_source": "extractor" if extracted_materials else "template",
        "request": {
            "question": resolved_question,
            "artifact": resolved_artifact,
            "current_judgment": resolved_current_judgment,
            "extra_context": resolved_extra_context,
        },
    }


def build_second_opinion_main_thread_execution(
    *,
    skill: str,
    packaged_request: dict[str, Any],
    prompt: str = "",
    trigger_path: list[str] | None = None,
) -> dict[str, str]:
    cleaned_prompt = prompt.strip() or packaged_request.get("request", {}).get("question", "")
    trigger = [str(item).strip() for item in (trigger_path or []) if str(item).strip()]
    source = str(packaged_request.get("source", "")).strip()
    entrypoint = "workflow-second-opinion" if source == "followup" or trigger else "second-opinion-from-prompt"
    command_parts = [
        "python3",
        str(REPO_ROOT / "ops" / "gstack_phase1_entry.py"),
        entrypoint,
        "--prompt",
        cleaned_prompt,
    ]
    focus_question = str(packaged_request.get("request", {}).get("question", "")).strip()
    if focus_question:
        command_parts.extend(["--question", focus_question])
    artifact_preview = _preview_multiline(
        str(packaged_request.get("request", {}).get("artifact", "")),
        SECOND_OPINION_MAX_ARTIFACT_PREVIEW_LINES,
    )
    return {
        "entrypoint": entrypoint,
        "focus_question": focus_question,
        "current_judgment": str(
            packaged_request.get("request", {}).get("current_judgment", "")
        ).strip(),
        "artifact_preview": artifact_preview,
        "material_source": str(packaged_request.get("material_source", "")).strip(),
        "suggested_command": " ".join(shlex.quote(part) for part in command_parts if part),
    }


def build_second_opinion_prompt(
    skill: str,
    *,
    question: str,
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    trigger_path: list[str] | None = None,
    source_prompt: str = "",
) -> str:
    envelope = build_second_opinion_request(
        skill,
        question=question,
        artifact=artifact,
        current_judgment=current_judgment,
        extra_context=extra_context,
        trigger_path=trigger_path,
        source_prompt=source_prompt,
    )
    return (
        "Second-opinion request envelope:\n"
        f"{json.dumps(envelope, ensure_ascii=False, indent=2)}\n\n"
        "Return a JSON object that matches the provided JSON schema exactly."
    )


def build_second_opinion_request(
    skill: str,
    *,
    question: str,
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    trigger_path: list[str] | None = None,
    source_prompt: str = "",
) -> dict[str, Any]:
    return {
        "schema_version": SECOND_OPINION_REQUEST_SCHEMA_VERSION,
        "stage": "second-opinion",
        "skill": skill,
        "mode": SECOND_OPINION_MODE_MAP[skill],
        "trigger_path": list(trigger_path or []),
        "source_prompt": source_prompt.strip(),
        "request": {
            "question": question.strip(),
            "artifact": artifact.strip(),
            "current_judgment": current_judgment.strip(),
            "extra_context": extra_context.strip(),
        },
        "response_contract": {
            "schema_version": SECOND_OPINION_RESPONSE_SCHEMA_VERSION,
            "required_fields": list(SECOND_OPINION_RESPONSE_SCHEMA["required"]),
        },
    }


def build_second_opinion_main_thread_handoff(
    *,
    skill: str,
    request_envelope: dict[str, Any],
    structured_output: dict[str, Any] | None,
    stdout: str = "",
    stderr: str = "",
) -> str:
    trigger_path = request_envelope.get("trigger_path") or []
    lines = ["第二意见回收"]
    lines.append(f"- 模式: `{skill}`")
    if trigger_path:
        lines.append(f"- 触发路径: `{' -> '.join(trigger_path)}`")

    if structured_output:
        lines.append(f"- 状态: `{structured_output.get('status', 'unknown')}`")
        question = structured_output.get("question_or_focus") or request_envelope["request"].get(
            "question", ""
        )
        lines.append(f"- 焦点问题: {question}")
        lines.append(f"- 核心判断: {structured_output.get('key_judgment', '')}")
        lines.append(
            f"- 与主判断的分歧: {structured_output.get('difference_from_current_judgment', '')}"
        )
        lines.append(f"- 建议下一步: {structured_output.get('recommended_next_step', '')}")
        evidence_needed = structured_output.get("evidence_needed", "")
        if evidence_needed:
            lines.append(f"- 还需要的证据: {evidence_needed}")
        return "\n".join(lines)

    lines.append("- 状态: `runner-returned-no-structured-output`")
    lines.append(
        f"- 焦点问题: {request_envelope['request'].get('question', '') or '未提供焦点问题'}"
    )
    if stdout:
        lines.append(f"- 原始输出: {stdout}")
    if stderr:
        lines.append(f"- runner stderr: {stderr}")
    lines.append("- 建议下一步: 先检查 Claude runner 返回内容是否继续符合 response schema。")
    return "\n".join(lines)


def run_second_opinion(
    skill: str,
    *,
    question: str,
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    trigger_path: list[str] | None = None,
    source_prompt: str = "",
    model: str = claude_code_runner.DEFAULT_MODEL,
    settings_path: Path = claude_code_runner.CLAUDE_SETTINGS_PATH,
) -> dict[str, Any]:
    if skill not in SECOND_OPINION_MODE_MAP:
        raise ValueError(f"Unsupported second-opinion skill: {skill}")

    request_envelope = build_second_opinion_request(
        skill,
        question=question,
        artifact=artifact,
        current_judgment=current_judgment,
        extra_context=extra_context,
        trigger_path=trigger_path,
        source_prompt=source_prompt,
    )
    prompt = build_second_opinion_prompt(
        skill,
        question=question,
        artifact=artifact,
        current_judgment=current_judgment,
        extra_context=extra_context,
        trigger_path=trigger_path,
        source_prompt=source_prompt,
    )
    runner_payload = claude_code_runner.run_claude(
        mode=SECOND_OPINION_MODE_MAP[skill],
        prompt=prompt,
        model=model,
        settings_path=settings_path,
        json_schema=SECOND_OPINION_RESPONSE_SCHEMA,
    )
    main_thread_handoff = build_second_opinion_main_thread_handoff(
        skill=skill,
        request_envelope=request_envelope,
        structured_output=runner_payload.get("structured_output"),
        stdout=str(runner_payload.get("stdout", "")),
        stderr=str(runner_payload.get("stderr", "")),
    )
    return {
        "skill": skill,
        "stage": "second-opinion",
        "runner": "claude_code_runner",
        "request_envelope": request_envelope,
        "response_contract": {
            "schema_version": SECOND_OPINION_RESPONSE_SCHEMA_VERSION,
            "required_fields": list(SECOND_OPINION_RESPONSE_SCHEMA["required"]),
        },
        "main_thread_handoff": main_thread_handoff,
        **runner_payload,
    }


def run_second_opinion_from_prompt(
    *,
    prompt: str,
    question: str = "",
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    model: str = claude_code_runner.DEFAULT_MODEL,
    settings_path: Path = claude_code_runner.CLAUDE_SETTINGS_PATH,
) -> dict[str, Any]:
    workflow = detect_workflow_path(prompt)
    second_opinion = suggest_second_opinion_skill_from_path(workflow.get("suggested_path", []))
    if not second_opinion:
        raise ValueError("Prompt did not resolve to a second-opinion skill")
    packaged_request = build_second_opinion_package(
        second_opinion["skill"],
        prompt=prompt,
        question=question,
        artifact=artifact,
        current_judgment=current_judgment,
        extra_context=extra_context,
        trigger_path=workflow.get("suggested_path", []),
        source=second_opinion.get("source", "manual"),
        workflow_detection=workflow,
    )
    payload = run_second_opinion(
        second_opinion["skill"],
        question=packaged_request["request"]["question"],
        artifact=packaged_request["request"]["artifact"],
        current_judgment=packaged_request["request"]["current_judgment"],
        extra_context=packaged_request["request"]["extra_context"],
        trigger_path=workflow.get("suggested_path", []),
        source_prompt=prompt,
        model=model,
        settings_path=settings_path,
    )
    payload["workflow_detection"] = workflow
    payload["second_opinion_candidate"] = second_opinion
    payload["packaged_request"] = packaged_request
    return payload


def run_second_opinion_for_workflow(
    *,
    prompt: str,
    question: str = "",
    artifact: str = "",
    current_judgment: str = "",
    extra_context: str = "",
    model: str = claude_code_runner.DEFAULT_MODEL,
    settings_path: Path = claude_code_runner.CLAUDE_SETTINGS_PATH,
) -> dict[str, Any]:
    workflow = detect_workflow_path(prompt)
    second_opinion = suggest_second_opinion_skill_from_path(workflow.get("suggested_path", []))
    if not second_opinion:
        raise ValueError("Workflow path did not resolve to a second-opinion skill")
    packaged_request = build_second_opinion_package(
        second_opinion["skill"],
        prompt=prompt,
        question=question,
        artifact=artifact,
        current_judgment=current_judgment,
        extra_context=extra_context,
        trigger_path=workflow.get("suggested_path", []),
        source=second_opinion.get("source", "manual"),
        workflow_detection=workflow,
    )
    payload = run_second_opinion(
        second_opinion["skill"],
        question=packaged_request["request"]["question"],
        artifact=packaged_request["request"]["artifact"],
        current_judgment=packaged_request["request"]["current_judgment"],
        extra_context=packaged_request["request"]["extra_context"],
        trigger_path=workflow.get("suggested_path", []),
        source_prompt=prompt,
        model=model,
        settings_path=settings_path,
    )
    payload["workflow_detection"] = workflow
    payload["second_opinion_candidate"] = second_opinion
    payload["packaged_request"] = packaged_request
    return payload


def detect_workflow_path(prompt: str) -> dict[str, Any]:
    text = prompt.strip()
    lowered = normalize(text)
    matched_entry: list[str] = []
    matched_execution: list[str] = []
    matched_delivery: list[str] = []
    matched_posture: list[str] = []
    matched_second_opinion: list[str] = []

    if not text:
        return {
            "status": "needs-input",
            "recognized_stage": "",
            "matched_skills": [],
            "suggested_path": [],
            "assistant_message": "当前没有足够输入，无法判断是否适合进入 gstack 工作流。",
            "initial_action_plan": [],
        }

    for skill in ENTRY_ORDER:
        rule = ENTRY_RULES[skill]
        if contains_any(lowered, rule["keywords"]):
            matched_entry.append(skill)

    for skill in EXECUTION_ORDER:
        rule = EXECUTION_RULES[skill]
        if contains_any(lowered, rule["keywords"]):
            matched_execution.append(skill)

    for skill in DELIVERY_ORDER:
        rule = DELIVERY_RULES[skill]
        if contains_any(lowered, rule["keywords"]):
            matched_delivery.append(skill)

    for skill in POSTURE_ORDER:
        rule = POSTURE_RULES[skill]
        if contains_any(lowered, rule["keywords"]):
            matched_posture.append(skill)

    for skill in SECOND_OPINION_ORDER:
        if matches_second_opinion_skill(skill, lowered):
            matched_second_opinion.append(skill)

    if (
        not matched_entry
        and not matched_execution
        and not matched_delivery
        and not matched_posture
        and not matched_second_opinion
        and contains_any(lowered, DIRECT_DO_TERMS)
    ):
        return {
            "status": "direct-execution-ok",
            "recognized_stage": "",
            "matched_skills": [],
            "suggested_path": [],
            "assistant_message": "这条请求更适合直接执行，不默认触发 gstack 工作流。",
            "initial_action_plan": ["按用户要求直接执行，不额外进入 gstack 工作流。"],
        }

    if (
        not matched_entry
        and not matched_execution
        and not matched_delivery
        and not matched_posture
        and not matched_second_opinion
    ):
        return {
            "status": "no-workflow-recommended",
            "recognized_stage": "",
            "matched_skills": [],
            "suggested_path": [],
            "assistant_message": "当前没有命中已落地的 gstack 工作流层。",
            "initial_action_plan": [],
        }

    suggested_path = [skill for skill in ENTRY_ORDER if skill in matched_entry]
    suggested_path.extend(skill for skill in EXECUTION_ORDER if skill in matched_execution)
    suggested_path.extend(skill for skill in DELIVERY_ORDER if skill in matched_delivery)
    suggested_path.extend(skill for skill in POSTURE_ORDER if skill in matched_posture)
    suggested_path.extend(
        skill for skill in SECOND_OPINION_ORDER if skill in matched_second_opinion
    )

    if (
        sum(
            bool(matches)
            for matches in (
                matched_entry,
                matched_execution,
                matched_delivery,
                matched_posture,
                matched_second_opinion,
            )
        )
        > 1
    ):
        message = (
            "这条请求同时命中了多个工作流层。"
            "我建议按识别到的层级顺序先厘清问题，再进入对应的执行或交付环节。"
        )
        stage = "multi-stage"
        plan = build_chain_plan(suggested_path)
    elif matched_execution and not matched_entry and not matched_delivery:
        stage = "execution"
        if len(suggested_path) == 1:
            primary = suggested_path[0]
            rule = EXECUTION_RULES[primary]
            message = (
                f"这条请求更适合先走 `{primary}`。"
                f"{rule['reason']}如果你同意，我先按这条执行路径给出一版初始判断。"
            )
            plan = list(rule["initial_action_plan"])
        else:
            message = (
                "这条请求同时命中了执行层的多个环节。"
                "我建议按识别到的执行顺序先排查/审查，再进入验证。"
            )
            plan = build_chain_plan(suggested_path)
    elif matched_delivery and not matched_entry and not matched_execution and not matched_posture:
        stage = "delivery"
        if len(suggested_path) == 1:
            primary = suggested_path[0]
            rule = DELIVERY_RULES[primary]
            message = (
                f"这条请求更适合先走 `{primary}`。"
                f"{rule['reason']}如果你同意，我先按这条执行路径给出一版初始判断。"
            )
            plan = list(rule["initial_action_plan"])
        else:
            message = (
                "这条请求同时命中了交付层的多个环节。"
                "我建议先同步文档或结论，再进入复盘收口。"
            )
            plan = build_chain_plan(suggested_path)
    elif matched_posture and not matched_entry and not matched_execution and not matched_delivery:
        stage = "posture"
        if len(suggested_path) == 1:
            primary = suggested_path[0]
            rule = POSTURE_RULES[primary]
            message = (
                f"这条请求更适合先走 `{primary}`。"
                f"{rule['reason']}如果你同意，我先按这条执行路径给出一版初始判断。"
            )
            plan = list(rule["initial_action_plan"])
        else:
            message = (
                "这条请求同时命中了姿态层的多个判断。"
                "我建议先明确风险边界，再决定是保持谨慎、冻结还是解除冻结。"
            )
            plan = build_chain_plan(suggested_path)
    elif (
        matched_second_opinion
        and not matched_entry
        and not matched_execution
        and not matched_delivery
        and not matched_posture
    ):
        stage = "second-opinion"
        if len(suggested_path) == 1:
            primary = suggested_path[0]
            rule = SECOND_OPINION_RULES[primary]
            message = (
                f"这条请求更适合先走 `{primary}`。"
                f"{rule['reason']}如果你同意，我先按这条执行路径给出一版初始判断。"
            )
            plan = list(rule["initial_action_plan"])
        else:
            message = (
                "这条请求同时命中了第二意见层的多个模式。"
                "我建议先明确你要的是复审、挑战还是顾问式咨询，再安排第二意见。"
            )
            plan = build_chain_plan(suggested_path)
    elif len(suggested_path) == 1:
        primary = suggested_path[0]
        rule = ENTRY_RULES[primary]
        message = (
            f"这条请求更适合先走 `{primary}`。"
            f"{rule['reason']}如果你同意，我先按这条路径给出一版初始判断。"
        )
        plan = list(rule["initial_action_plan"])
        stage = "entry"
    else:
        message = (
            "这条请求同时命中了入口层的多个判断维度。"
            "我建议按 `office-hours -> plan-ceo-review -> plan-eng-review` 这条链先把问题重构清楚，"
            "再给产品和技术两层判断。"
        )
        plan = build_chain_plan(suggested_path)
        stage = "entry"

    return {
        "status": "workflow-recommended",
        "recognized_stage": stage,
        "matched_skills": suggested_path,
        "suggested_path": suggested_path,
        "assistant_message": message,
        "initial_action_plan": plan,
    }


def detect_entry_path(prompt: str) -> dict[str, Any]:
    """Backward-compatible alias for earlier entry-only callers."""
    return detect_workflow_path(prompt)


def cmd_suggest(args: argparse.Namespace) -> int:
    payload = detect_workflow_path(args.prompt)
    payload["prompt"] = args.prompt
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def cmd_second_opinion(args: argparse.Namespace) -> int:
    payload = run_second_opinion(
        args.skill,
        question=args.question,
        artifact=args.artifact,
        current_judgment=args.current_judgment,
        extra_context=args.extra_context,
        model=args.model,
        settings_path=Path(args.settings_path),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        if payload.get("main_thread_handoff"):
            print(payload["main_thread_handoff"])
        elif payload.get("structured_output"):
            print(json.dumps(payload["structured_output"], ensure_ascii=False))
        elif payload.get("stdout"):
            print(payload["stdout"])
        elif payload.get("stderr"):
            print(payload["stderr"])
    return int(payload["returncode"])


def cmd_second_opinion_from_prompt(args: argparse.Namespace) -> int:
    payload = run_second_opinion_from_prompt(
        prompt=args.prompt,
        question=args.question,
        artifact=args.artifact,
        current_judgment=args.current_judgment,
        extra_context=args.extra_context,
        model=args.model,
        settings_path=Path(args.settings_path),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        if payload.get("main_thread_handoff"):
            print(payload["main_thread_handoff"])
        elif payload.get("structured_output"):
            print(json.dumps(payload["structured_output"], ensure_ascii=False))
        elif payload.get("stdout"):
            print(payload["stdout"])
        elif payload.get("stderr"):
            print(payload["stderr"])
    return int(payload["returncode"])


def cmd_workflow_second_opinion(args: argparse.Namespace) -> int:
    payload = run_second_opinion_for_workflow(
        prompt=args.prompt,
        question=args.question,
        artifact=args.artifact,
        current_judgment=args.current_judgment,
        extra_context=args.extra_context,
        model=args.model,
        settings_path=Path(args.settings_path),
    )
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        if payload.get("main_thread_handoff"):
            print(payload["main_thread_handoff"])
        elif payload.get("structured_output"):
            print(json.dumps(payload["structured_output"], ensure_ascii=False))
        elif payload.get("stdout"):
            print(payload["stdout"])
        elif payload.get("stderr"):
            print(payload["stderr"])
    return int(payload["returncode"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="gstack workflow assistant helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    suggest = subparsers.add_parser("suggest")
    suggest.add_argument("--prompt", required=True)
    suggest.set_defaults(func=cmd_suggest)

    second_opinion = subparsers.add_parser("second-opinion")
    second_opinion.add_argument("--skill", choices=SECOND_OPINION_ORDER, required=True)
    second_opinion.add_argument("--question", required=True)
    second_opinion.add_argument("--artifact", default="")
    second_opinion.add_argument("--current-judgment", default="")
    second_opinion.add_argument("--extra-context", default="")
    second_opinion.add_argument("--model", default=claude_code_runner.DEFAULT_MODEL)
    second_opinion.add_argument("--settings-path", default=str(claude_code_runner.CLAUDE_SETTINGS_PATH))
    second_opinion.add_argument("--json", action="store_true")
    second_opinion.set_defaults(func=cmd_second_opinion)

    second_opinion_from_prompt = subparsers.add_parser("second-opinion-from-prompt")
    second_opinion_from_prompt.add_argument("--prompt", required=True)
    second_opinion_from_prompt.add_argument("--question", default="")
    second_opinion_from_prompt.add_argument("--artifact", default="")
    second_opinion_from_prompt.add_argument("--current-judgment", default="")
    second_opinion_from_prompt.add_argument("--extra-context", default="")
    second_opinion_from_prompt.add_argument("--model", default=claude_code_runner.DEFAULT_MODEL)
    second_opinion_from_prompt.add_argument(
        "--settings-path", default=str(claude_code_runner.CLAUDE_SETTINGS_PATH)
    )
    second_opinion_from_prompt.add_argument("--json", action="store_true")
    second_opinion_from_prompt.set_defaults(func=cmd_second_opinion_from_prompt)

    workflow_second_opinion = subparsers.add_parser("workflow-second-opinion")
    workflow_second_opinion.add_argument("--prompt", required=True)
    workflow_second_opinion.add_argument("--question", default="")
    workflow_second_opinion.add_argument("--artifact", default="")
    workflow_second_opinion.add_argument("--current-judgment", default="")
    workflow_second_opinion.add_argument("--extra-context", default="")
    workflow_second_opinion.add_argument("--model", default=claude_code_runner.DEFAULT_MODEL)
    workflow_second_opinion.add_argument(
        "--settings-path", default=str(claude_code_runner.CLAUDE_SETTINGS_PATH)
    )
    workflow_second_opinion.add_argument("--json", action="store_true")
    workflow_second_opinion.set_defaults(func=cmd_workflow_second_opinion)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

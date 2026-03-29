#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import gstack_phase1_entry, runtime_state
except ImportError:  # pragma: no cover
    import gstack_phase1_entry  # type: ignore
    import runtime_state  # type: ignore


WORKFLOW_PLAN_SCHEMA_VERSION = "codex-hub.workflow.plan.v1"
WORKFLOW_STAGE_RESULT_SCHEMA_VERSION = "codex-hub.workflow.stage-result.v1"
WORKFLOW_RUN_SUMMARY_SCHEMA_VERSION = "codex-hub.workflow.run-summary.v1"

INVOCATION_MODE_ADVISORY = "advisory"
INVOCATION_MODE_GFLOW_EXPLICIT = "gflow-explicit"

RUN_STATUS_PLANNED = "planned"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_PAUSED = "paused"
RUN_STATUS_AWAITING_APPROVAL = "awaiting_approval"
RUN_STATUS_FROZEN = "frozen"
RUN_STATUS_COMPLETED = "completed"

RUN_STATUSES = {
    RUN_STATUS_PLANNED,
    RUN_STATUS_RUNNING,
    RUN_STATUS_PAUSED,
    RUN_STATUS_AWAITING_APPROVAL,
    RUN_STATUS_FROZEN,
    RUN_STATUS_COMPLETED,
}

STAGE_STATUS_PENDING = "pending"
STAGE_STATUS_RUNNING = "running"
STAGE_STATUS_PAUSED = "paused"
STAGE_STATUS_AWAITING_APPROVAL = "awaiting_approval"
STAGE_STATUS_FROZEN = "frozen"
STAGE_STATUS_COMPLETED = "completed"
STAGE_STATUS_SKIPPED = "skipped"

STAGE_STATUSES = {
    STAGE_STATUS_PENDING,
    STAGE_STATUS_RUNNING,
    STAGE_STATUS_PAUSED,
    STAGE_STATUS_AWAITING_APPROVAL,
    STAGE_STATUS_FROZEN,
    STAGE_STATUS_COMPLETED,
    STAGE_STATUS_SKIPPED,
}

GATE_TYPE_NONE = ""
GATE_TYPE_USER = "user"
GATE_TYPE_APPROVAL = "approval"
GATE_TYPE_FREEZE = "freeze"
GATE_TYPES = {
    GATE_TYPE_NONE,
    GATE_TYPE_USER,
    GATE_TYPE_APPROVAL,
    GATE_TYPE_FREEZE,
}
AUTO_EXECUTABLE_STAGE_SKILLS = {"fix", "qa", "writeback"}
STOP_CONDITION_GATE_MAP = {
    "needs_context": GATE_TYPE_USER,
    "approval_required": GATE_TYPE_APPROVAL,
    "freeze": GATE_TYPE_FREEZE,
    "blocked": GATE_TYPE_USER,
    "human_choice_required": GATE_TYPE_USER,
}
STAGE_STOP_RE = re.compile(
    r"GFLOW_STOP:\s*(?P<condition>[a-z_]+)(?:\s*[-|:：]\s*(?P<reason>.+))?",
    re.IGNORECASE,
)

DEFAULT_READONLY_PATH = ["office-hours", "plan-eng-review"]
DEFAULT_HANDOFF_TITLE = "GFlow explicit mode handoff"
RUNTIME_HANDOFF_TITLE = "GFlow lightweight runtime handoff"
DIRECT_REQUEST_MARKER = "用户这次的直接请求是："
LEADING_TRIGGER_RE = re.compile(r"^\s*(?P<trigger>gflow)\b(?P<suffix>.*)$", re.IGNORECASE | re.DOTALL)
COMMAND_TRIGGER_RE = re.compile(
    r"^\s*(?P<prefix>(?:请|请你|用|走|进入|启动|调用)\s*)"
    r"(?P<trigger>gflow)\b"
    r"(?:\s*(?:工作流|workflow))?"
    r"(?:\s*(?:来|去|先))?"
    r"(?:\s*(?:处理|跑|做|推进))?"
    r"(?:\s*(?:这件事|这个任务|这个需求|这个工作))?"
    r"(?P<suffix>.*)$",
    re.IGNORECASE | re.DOTALL,
)
SEPARATOR_PREFIX_RE = re.compile(r"^[\s:：,，;；.!！？\-]+")
NON_TRIGGER_REMAINDER_PREFIXES = (
    "的",
    "系统",
    "方案",
    "workflow",
    "是什么",
    "有几个",
    "如何",
    "怎么",
)
META_QUESTION_PREFIXES = (
    "是什么",
    "有几个",
    "如何",
    "怎么",
    "有哪些",
    "为什么",
    "区别",
    "原理",
    "规则",
    "阶段",
)
ACTION_INVOCATION_TERMS = [
    "直接",
    "直接做",
    "帮我",
    "请",
    "先",
    "处理",
    "推进",
    "执行",
    "review",
    "审核",
    "审查",
    "检查",
    "修",
    "修复",
    "qa",
    "验证",
    "ship",
    "同步",
    "写回",
]
QUOTED_SUBJECT_RE = re.compile(r"[「“\"'](?P<subject>[^」”\"']+)[」”\"']")
SCOPE_SUBJECT_RE = re.compile(
    r"(?:帮我review一下|帮我审核一下|帮我审核|审核一下|审核|评审|审查|检查|看看|review)\s*"
    r"(?:[「“\"'](?P<quoted>[^」”\"']+)[」”\"']|(?P<plain>.+?))"
    r"(?:\s*(?:这个|这条|这套))?\s*(?:workflow|项目|模块|链路|代码|工程|实现)",
    re.IGNORECASE,
)

WORKFLOW_TEMPLATE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "review-fix-qa-writeback": {
        "label": "Review -> Fix -> QA -> Writeback",
        "description": "先审查问题，再完成修复，随后验证并把结果写回项目事实层。",
        "path": ["review", "fix", "qa", "writeback"],
        "success_criteria": [
            "review 已形成明确 findings、风险和修复面",
            "fix 已完成最小必要修复，不再停在 findings",
            "qa 已验证修复结果和回归风险",
            "writeback 已把结论、下一步和活跃 run 状态写回项目板",
        ],
        "gate_policy": [
            "review 若只得到问题但没有明确修复面，不得停下，必须进入 fix 或显式说明 blocker",
            "fix 后必须进入 QA，不能跳过验证",
            "只有命中 needs_context / approval_required / freeze / blocked / human_choice_required 才允许暂停",
        ],
        "auto_continue": True,
        "stop_conditions": [
            "needs_context",
            "approval_required",
            "freeze",
            "blocked",
            "human_choice_required",
        ],
        "stage_contracts": [
            {
                "skill": "review",
                "deliverable": "明确 findings、受影响文件和最小修复面",
                "required_evidence": ["目标对象", "当前实现或 diff", "风险上下文"],
                "gate_rule": "如果目标对象不明确或证据不足，才暂停补上下文；否则直接进入 fix。",
            },
            {
                "skill": "fix",
                "deliverable": "完成最小必要修复并记录修复摘要",
                "required_evidence": ["review findings", "目标代码面"],
                "gate_rule": "只有命中 stop_conditions 才暂停；否则修完后直接进入 QA。",
            },
            {
                "skill": "qa",
                "deliverable": "验证修复结果与回归风险，给出通过/失败结论",
                "required_evidence": ["修复后的实现", "最小必要测试或 smoke"],
                "gate_rule": "关键验证失败时暂停；否则继续 writeback。",
            },
            {
                "skill": "writeback",
                "deliverable": "把 workflow 结论、下一步和运行态写回项目板和摘要",
                "required_evidence": ["review 结论", "fix 摘要", "qa 结论"],
                "gate_rule": "写回失败时暂停并明确剩余风险。",
            },
        ],
    },
    "review-claude-review": {
        "label": "Review -> Claude Review",
        "description": "先完成主线程 review，再用 Claude 做独立第二意见复审。",
        "path": ["review", "claude-review"],
        "success_criteria": [
            "主线程 review 已产出明确 findings 或通过判断",
            "第二意见已回收并写清与主判断的认同点或分歧点",
            "下一步已明确是继续修复、继续验证还是准备交付",
        ],
        "gate_policy": [
            "若主线程 review 证据不足，则先补 diff / 方案摘要 / 风险对象，再进入 Claude review",
            "若第二意见给出新的高风险结论，则暂停并要求主线程重评",
        ],
        "auto_continue": True,
        "stop_conditions": [
            "needs_context",
            "approval_required",
            "freeze",
            "blocked",
            "human_choice_required",
        ],
        "stage_contracts": [
            {
                "skill": "review",
                "deliverable": "主线程 review 结论、回归风险与证据包",
                "required_evidence": ["diff 或方案摘要", "当前主线程判断"],
                "gate_rule": "缺少复审对象时暂停补证据",
            },
            {
                "skill": "claude-review",
                "deliverable": "独立第二意见与分歧点回收",
                "required_evidence": ["question", "artifact", "current_judgment", "extra_context"],
                "gate_rule": "第二意见发现高风险时暂停给主线程重评",
            },
        ],
    },
    "review-qa-ship": {
        "label": "Review -> QA -> Ship",
        "description": "先审查风险，再做验证，最后进入发布准备判断。",
        "path": ["review", "qa", "ship"],
        "success_criteria": [
            "review 已收口主要风险与缺口",
            "QA 已覆盖最小高风险路径并给出通过/失败结论",
            "ship 已输出最小安全交付路径与 remaining risks",
        ],
        "gate_policy": [
            "review 未收口主要风险前不得进入 QA",
            "QA 未形成通过或失败结论前不得进入 ship",
            "ship 若命中高风险 caveat，需要暂停给用户或审批 gate",
        ],
        "auto_continue": True,
        "stop_conditions": [
            "needs_context",
            "approval_required",
            "freeze",
            "blocked",
            "human_choice_required",
        ],
        "stage_contracts": [
            {
                "skill": "review",
                "deliverable": "风险审查结论与缺失验证清单",
                "required_evidence": ["待审对象", "当前变更范围"],
                "gate_rule": "重大风险未解释前不得推进到 QA",
            },
            {
                "skill": "qa",
                "deliverable": "最小验证结果与未验证面",
                "required_evidence": ["测试结果", "smoke 或负路径证据"],
                "gate_rule": "关键验证失败或未覆盖关键路径时暂停",
            },
            {
                "skill": "ship",
                "deliverable": "是否可交付的 readiness 判断",
                "required_evidence": ["review 结论", "QA 结论", "交付范围"],
                "gate_rule": "命中高风险发布动作时暂停给审批或用户确认",
            },
        ],
    },
    "office-hours-plan-ceo-plan-eng": {
        "label": "Office Hours -> CEO Review -> Eng Review",
        "description": "先重构问题，再给产品判断，最后给技术落地判断。",
        "path": ["office-hours", "plan-ceo-review", "plan-eng-review"],
        "success_criteria": [
            "问题 framing、范围和约束已被重构清楚",
            "产品层已给出值不值得做与优先级判断",
            "技术层已给出最小安全落地路径和主要风险",
        ],
        "gate_policy": [
            "入口问题仍散乱时不得跳过 office-hours",
            "产品判断不明确时不得直接承诺技术方案",
            "技术方案存在关键外部依赖时需显式标出阻塞",
        ],
        "auto_continue": True,
        "stop_conditions": [
            "needs_context",
            "approval_required",
            "freeze",
            "blocked",
            "human_choice_required",
        ],
        "stage_contracts": [
            {
                "skill": "office-hours",
                "deliverable": "问题 framing、范围、约束与开放问题",
                "required_evidence": ["用户目标", "当前上下文"],
                "gate_rule": "问题范围仍模糊时暂停补背景",
            },
            {
                "skill": "plan-ceo-review",
                "deliverable": "产品价值、优先级与机会成本判断",
                "required_evidence": ["framing 结果", "目标用户/业务价值"],
                "gate_rule": "核心价值主张不成立时暂停，不进入技术评估",
            },
            {
                "skill": "plan-eng-review",
                "deliverable": "技术可行性、风险与验证路径",
                "required_evidence": ["产品判断", "系统边界"],
                "gate_rule": "关键技术前提未满足时显式标 blocker",
            },
        ],
    },
    "document-release-ship": {
        "label": "Document Release -> Ship",
        "description": "先同步发布说明或文档，再判断交付是否可正式推出。",
        "path": ["document-release", "ship"],
        "success_criteria": [
            "文档或 release note 已同步到位",
            "ship 已明确当前是否可交付及剩余 caveat",
        ],
        "gate_policy": [
            "文档依据不完整时先补已验证事实，不直接进入 ship",
            "ship 命中高风险 caveat 时暂停给用户确认",
        ],
        "auto_continue": True,
        "stop_conditions": [
            "needs_context",
            "approval_required",
            "freeze",
            "blocked",
            "human_choice_required",
        ],
        "stage_contracts": [
            {
                "skill": "document-release",
                "deliverable": "面向目标受众的文档/发布说明",
                "required_evidence": ["已验证变更", "受众范围"],
                "gate_rule": "没有已验证事实时暂停，不生成发布说明",
            },
            {
                "skill": "ship",
                "deliverable": "交付 readiness 判断与最小安全路径",
                "required_evidence": ["发布说明或文档更新", "剩余风险"],
                "gate_rule": "命中高风险外部动作时暂停给用户或审批",
            },
        ],
    },
}

PHASE4_SHIP_HINTS = [
    "ship",
    "能不能 ship",
    "可不可以 ship",
    "是否可以 ship",
    "准备 ship",
]

GFLOW_PROJECT_SCOPE_REVIEW_FILES = [
    str(REPO_ROOT / "ops" / "gstack_automation.py"),
    str(REPO_ROOT / "ops" / "gstack_phase1_entry.py"),
    str(REPO_ROOT / "ops" / "codex_context.py"),
    str(REPO_ROOT / "ops" / "material_router.py"),
    str(REPO_ROOT / "ops" / "codex_memory.py"),
    str(REPO_ROOT / "ops" / "runtime_state.py"),
    str(REPO_ROOT / "ops" / "start-codex"),
    str(REPO_ROOT / "tests" / "test_gstack_automation.py"),
    str(REPO_ROOT / "tests" / "test_v1_0_1.py"),
]

WORKFLOW_SCOPE_TARGET_DEFINITIONS: dict[str, dict[str, Any]] = {
    "gflow-codebase-review": {
        "label": "GFlow 项目代码",
        "reason": "当前请求显式要求 review GFlow 项目代码，默认从 GFlow 主代码面出发，并扩展相邻执行链与测试。",
        "aliases": [
            "gflow",
            "g flow",
            "gstack automation workflow",
            "gflow workflow",
        ],
        "seed_files": list(GFLOW_PROJECT_SCOPE_REVIEW_FILES),
        "seed_terms": ["gflow", "gstack", "workflow", "phase"],
        "coverage": [
            "workflow runtime",
            "workflow path detection",
            "launcher/context wiring",
            "project writeback",
            "runtime persistence",
            "phase 1~4 regression tests",
        ],
    }
}

GFLOW_SCOPE_REVIEW_TERMS = [
    "review",
    "审一下",
    "审核",
    "审核一下",
    "审查",
    "检查",
    "看看",
]
GFLOW_SCOPE_CODE_TERMS = [
    "代码",
    "code",
    "codebase",
    "项目",
    "工程",
    "实现",
    "runtime",
    "phase",
]
GFLOW_SCOPE_PROJECT_TERMS = [
    "gflow",
    "g flow",
    "gstack automation workflow",
]
SCOPE_ALL_CODE_TERMS = [
    "所有代码",
    "全部代码",
    "完整代码",
    "整个项目",
    "整个workflow",
    "all code",
    "whole codebase",
]
LOCAL_IMPORT_FROM_RE = re.compile(r"^\s*from\s+ops\s+import\s+([A-Za-z0-9_, \t]+)$", re.MULTILINE)
LOCAL_IMPORT_RE = re.compile(r"^\s*import\s+([A-Za-z0-9_, \t]+)$", re.MULTILINE)
LOCAL_OPS_PATH_RE = re.compile(r"ops/([A-Za-z0-9_\-]+)(?:\.py)?")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _timestamp_sort_value(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _strip_separator(text: str) -> str:
    return SEPARATOR_PREFIX_RE.sub("", text or "").strip()


def _extract_direct_request_text(prompt: str) -> str:
    text = prompt.strip()
    if DIRECT_REQUEST_MARKER not in text:
        return text
    return text.rsplit(DIRECT_REQUEST_MARKER, 1)[-1].strip()


def _looks_like_reference_not_invocation(entry_prompt: str) -> bool:
    text = _strip_separator(entry_prompt)
    if not text:
        return False
    lowered = gstack_phase1_entry.normalize(text)
    return lowered.startswith(NON_TRIGGER_REMAINDER_PREFIXES)


def _looks_like_meta_question(entry_prompt: str) -> bool:
    text = _strip_separator(entry_prompt)
    if not text:
        return False
    lowered = gstack_phase1_entry.normalize(text)
    if lowered.startswith(META_QUESTION_PREFIXES):
        return True
    if "?" in text or "？" in text:
        return lowered.startswith(META_QUESTION_PREFIXES) or not _contains_any_term(text, ACTION_INVOCATION_TERMS)
    return False


def _looks_like_action_invocation(entry_prompt: str) -> bool:
    text = _strip_separator(entry_prompt)
    if not text or _looks_like_meta_question(text):
        return False
    if _contains_any_term(text, ACTION_INVOCATION_TERMS):
        return True
    return bool(SCOPE_SUBJECT_RE.search(text))


def _normalize_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _normalize_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _normalize_text(text: str) -> str:
    return gstack_phase1_entry.normalize(text or "")


def _contains_any_term(text: str, terms: list[str]) -> bool:
    lowered = _normalize_text(text)
    return any(str(term).strip().lower() in lowered for term in terms if str(term).strip())


def _path_key(path: list[str]) -> tuple[str, ...]:
    return tuple(str(item).strip() for item in path if str(item).strip())


def _lookup_workflow_template(suggested_path: list[str]) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    path_key = _path_key(suggested_path)
    for template_id, definition in WORKFLOW_TEMPLATE_DEFINITIONS.items():
        if _path_key(definition.get("path", [])) == path_key:
            return template_id, dict(definition)
    return None, None


def _template_payload(template_id: str, definition: dict[str, Any]) -> dict[str, Any]:
    return {
        "template_id": template_id,
        "label": str(definition.get("label", "")).strip(),
        "description": str(definition.get("description", "")).strip(),
        "path": [str(item).strip() for item in definition.get("path", []) if str(item).strip()],
        "success_criteria": [
            str(item).strip() for item in definition.get("success_criteria", []) if str(item).strip()
        ],
        "gate_policy": [
            str(item).strip() for item in definition.get("gate_policy", []) if str(item).strip()
        ],
        "auto_continue": bool(definition.get("auto_continue", True)),
        "stop_conditions": [
            str(item).strip() for item in definition.get("stop_conditions", []) if str(item).strip()
        ],
        "stage_contracts": [
            {
                "skill": str(item.get("skill", "")).strip(),
                "deliverable": str(item.get("deliverable", "")).strip(),
                "required_evidence": [
                    str(evidence).strip()
                    for evidence in item.get("required_evidence", [])
                    if str(evidence).strip()
                ],
                "gate_rule": str(item.get("gate_rule", "")).strip(),
            }
            for item in definition.get("stage_contracts", [])
            if isinstance(item, dict)
        ],
    }


def _normalize_scope_subject(text: str) -> str:
    lowered = _normalize_text(text)
    lowered = lowered.replace("workflow", " ").replace("代码", " ").replace("项目", " ")
    lowered = lowered.replace("工程", " ").replace("模块", " ").replace("链路", " ").replace("实现", " ")
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _scope_tokens(subject: str) -> list[str]:
    normalized = _normalize_scope_subject(subject)
    if not normalized:
        return []
    tokens = [token.strip() for token in re.split(r"[\s/_\-]+", normalized) if token.strip()]
    if tokens:
        return tokens
    return [normalized]


def _extract_scope_subject(entry_prompt: str) -> str:
    quoted = QUOTED_SUBJECT_RE.search(entry_prompt)
    if quoted:
        return str(quoted.group("subject") or "").strip()
    match = SCOPE_SUBJECT_RE.search(entry_prompt)
    if not match:
        return ""
    return str(match.group("quoted") or match.group("plain") or "").strip()


def _match_scope_target_definition(subject: str) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    normalized_subject = _normalize_scope_subject(subject)
    if not normalized_subject:
        return None, None
    for scope_id, definition in WORKFLOW_SCOPE_TARGET_DEFINITIONS.items():
        aliases = [
            _normalize_scope_subject(str(item).strip())
            for item in definition.get("aliases", [])
            if str(item).strip()
        ]
        if normalized_subject in aliases:
            return scope_id, dict(definition)
    return None, None


def _scope_requests_all_code(entry_prompt: str) -> bool:
    return _contains_any_term(entry_prompt, SCOPE_ALL_CODE_TERMS)


def _module_to_path(module_name: str) -> str:
    normalized = str(module_name or "").strip()
    if not normalized:
        return ""
    candidate = REPO_ROOT / "ops" / f"{normalized}.py"
    if candidate.exists():
        return str(candidate)
    candidate = REPO_ROOT / "ops" / normalized
    if candidate.exists():
        return str(candidate)
    return ""


def _related_test_paths(path_text: str) -> list[str]:
    path = Path(path_text)
    if path.parent.name == "ops" and path.suffix == ".py":
        candidate = REPO_ROOT / "tests" / f"test_{path.stem}.py"
        if candidate.exists():
            return [str(candidate)]
    if path.parent.name == "tests" and path.name.startswith("test_") and path.suffix == ".py":
        stem = path.stem.removeprefix("test_")
        candidate = REPO_ROOT / "ops" / f"{stem}.py"
        if candidate.exists():
            return [str(candidate)]
    return []


def _extract_local_import_paths(path_text: str) -> list[str]:
    path = Path(path_text)
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    discovered: list[str] = []
    if path.suffix == ".py":
        for match in LOCAL_IMPORT_FROM_RE.finditer(text):
            for name in match.group(1).split(","):
                candidate = _module_to_path(name.strip())
                if candidate:
                    discovered.append(candidate)
        for match in LOCAL_IMPORT_RE.finditer(text):
            for name in match.group(1).split(","):
                candidate = _module_to_path(name.strip())
                if candidate:
                    discovered.append(candidate)
    for match in LOCAL_OPS_PATH_RE.finditer(text):
        candidate = _module_to_path(match.group(1).strip())
        if candidate:
            discovered.append(candidate)
    return discovered


def _expand_scope_files(
    *,
    seed_files: list[str],
    subject: str,
    seed_terms: list[str] | None = None,
    include_all_code: bool = False,
) -> list[str]:
    max_depth = 2 if include_all_code else 1
    queue: list[tuple[str, int]] = [(item, 0) for item in seed_files if str(item).strip()]
    seen: set[str] = set()
    ordered: list[str] = []
    while queue:
        current, depth = queue.pop(0)
        normalized = str(current).strip()
        if not normalized or normalized in seen:
            continue
        if not Path(normalized).exists():
            continue
        seen.add(normalized)
        ordered.append(normalized)
        for related in _related_test_paths(normalized):
            if related not in seen:
                queue.append((related, depth))
        if depth >= max_depth:
            continue
        for imported in _extract_local_import_paths(normalized):
            if imported not in seen:
                queue.append((imported, depth + 1))
    if include_all_code:
        terms = [term for term in (seed_terms or _scope_tokens(subject)) if str(term).strip()]
        for root in [REPO_ROOT / "ops", REPO_ROOT / "tests"]:
            if not root.exists():
                continue
            for candidate in sorted(root.rglob("*")):
                if not candidate.is_file():
                    continue
                normalized_path = _normalize_scope_subject(str(candidate.relative_to(REPO_ROOT)))
                if all(str(term).strip().lower() in normalized_path for term in terms):
                    text_path = str(candidate)
                    if text_path not in seen:
                        seen.add(text_path)
                        ordered.append(text_path)
    return ordered


def _fallback_scope_files(subject: str) -> list[str]:
    tokens = _scope_tokens(subject)
    if not tokens:
        return []
    search_roots = [
        REPO_ROOT / "ops",
        REPO_ROOT / "tests",
        REPO_ROOT / "skills",
    ]
    matches: list[str] = []
    for root in search_roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            normalized_path = _normalize_scope_subject(str(path.relative_to(REPO_ROOT)))
            if all(token in normalized_path for token in tokens):
                matches.append(str(path))
            if len(matches) >= 12:
                return matches
    return matches


def _build_scope_payload(
    *,
    scope_id: str,
    label: str,
    reason: str,
    files: list[str],
    coverage: list[str] | None = None,
    subject: str = "",
) -> dict[str, Any]:
    payload = {
        "scope_id": str(scope_id or "").strip(),
        "label": str(label or "").strip(),
        "reason": str(reason or "").strip(),
        "files": [str(item).strip() for item in files if str(item).strip()],
        "coverage": [str(item).strip() for item in (coverage or []) if str(item).strip()],
    }
    if subject:
        payload["subject"] = str(subject).strip()
    return payload


def _resolve_review_scope(entry_prompt: str, suggested_path: list[str]) -> dict[str, Any]:
    if not suggested_path or str(suggested_path[0]).strip() != "review":
        return {}
    if not _contains_any_term(entry_prompt, GFLOW_SCOPE_REVIEW_TERMS):
        return {}
    subject = _extract_scope_subject(entry_prompt)
    if not subject:
        return {}
    if not _contains_any_term(entry_prompt, GFLOW_SCOPE_CODE_TERMS):
        return {}
    include_all_code = _scope_requests_all_code(entry_prompt)

    scope_id, definition = _match_scope_target_definition(subject)
    if definition:
        files = _expand_scope_files(
            seed_files=[str(item).strip() for item in definition.get("seed_files", []) if str(item).strip()],
            subject=subject,
            seed_terms=[str(item).strip() for item in definition.get("seed_terms", []) if str(item).strip()],
            include_all_code=include_all_code,
        )
        return _build_scope_payload(
            scope_id=scope_id or "",
            label=str(definition.get("label", "")).strip(),
            reason=str(definition.get("reason", "")).strip(),
            files=files,
            coverage=[str(item).strip() for item in definition.get("coverage", []) if str(item).strip()],
            subject=subject,
        )

    files = _expand_scope_files(
        seed_files=_fallback_scope_files(subject),
        subject=subject,
        include_all_code=include_all_code,
    )
    if not files:
        return {}
    return _build_scope_payload(
        scope_id=f"review-scope-{re.sub(r'[^a-z0-9]+', '-', _normalize_scope_subject(subject)).strip('-') or 'target'}",
        label=f"{subject} 代码范围",
        reason=f"当前请求显式要求 review `{subject}` 的代码，已按仓内文件名与路径相似度扩展范围。",
        files=files,
        coverage=["matched repo files", "adjacent tests when available"],
        subject=subject,
    )


def _apply_phase4_template_detection(
    entry_prompt: str,
    suggestion: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(suggestion)
    normalized_prompt = _normalize_text(entry_prompt)
    suggested_path = [
        str(item).strip() for item in updated.get("suggested_path", []) if str(item).strip()
    ]

    template_id = ""
    template_path: list[str] = []
    if suggested_path[:1] == ["review"] and _contains_any_term(normalized_prompt, GFLOW_SCOPE_CODE_TERMS):
        template_id = "review-fix-qa-writeback"
        template_path = list(WORKFLOW_TEMPLATE_DEFINITIONS[template_id]["path"])
    elif suggested_path == ["review", "claude-review"]:
        template_id = "review-claude-review"
        template_path = list(WORKFLOW_TEMPLATE_DEFINITIONS[template_id]["path"])
    elif (
        suggested_path[:2] == ["review", "qa"]
        and _contains_any_term(
            normalized_prompt,
            gstack_phase1_entry.DELIVERY_RULES["ship"]["keywords"] + PHASE4_SHIP_HINTS,
        )
    ):
        template_id = "review-qa-ship"
        template_path = list(WORKFLOW_TEMPLATE_DEFINITIONS[template_id]["path"])
    elif (
        suggested_path[:1] == ["office-hours"]
        and _contains_any_term(normalized_prompt, gstack_phase1_entry.ENTRY_RULES["plan-ceo-review"]["keywords"])
        and _contains_any_term(normalized_prompt, gstack_phase1_entry.ENTRY_RULES["plan-eng-review"]["keywords"])
    ):
        template_id = "office-hours-plan-ceo-plan-eng"
        template_path = list(WORKFLOW_TEMPLATE_DEFINITIONS[template_id]["path"])
    elif (
        suggested_path[:1] == ["document-release"]
        and _contains_any_term(
            normalized_prompt,
            gstack_phase1_entry.DELIVERY_RULES["ship"]["keywords"] + PHASE4_SHIP_HINTS,
        )
    ):
        template_id = "document-release-ship"
        template_path = list(WORKFLOW_TEMPLATE_DEFINITIONS[template_id]["path"])

    if not template_id:
        return updated

    template = _template_payload(template_id, WORKFLOW_TEMPLATE_DEFINITIONS[template_id])
    updated["template"] = template
    updated["template_id"] = template_id
    updated["template_label"] = template["label"]
    updated["template_description"] = template["description"]
    updated["suggested_path"] = list(template_path)
    updated["path_source"] = "workflow-template"
    updated["recognized_stage"] = "template-workflow"
    updated["assistant_message"] = (
        f"这条请求已命中 Phase 4 高价值模板 `{template['label']}`，"
        f"建议按 `{' -> '.join(template_path)}` 的标准路径推进。"
    )
    updated["initial_action_plan"] = gstack_phase1_entry.build_chain_plan(template_path)
    return updated


def _validate_run_status(status: str) -> str:
    normalized = str(status or "").strip()
    if normalized not in RUN_STATUSES:
        raise ValueError(f"run status is invalid: {status}")
    return normalized


def _validate_stage_status(status: str) -> str:
    normalized = str(status or "").strip()
    if normalized not in STAGE_STATUSES:
        raise ValueError(f"stage status is invalid: {status}")
    return normalized


def _validate_gate_type(gate_type: str) -> str:
    normalized = str(gate_type or "").strip()
    if normalized not in GATE_TYPES:
        raise ValueError(f"gate_type is invalid: {gate_type}")
    return normalized


def detect_gflow_trigger(prompt: str) -> dict[str, Any]:
    text = _extract_direct_request_text(prompt)
    if not text:
        return {
            "matched": False,
            "invocation_mode": "",
            "trigger_token": "",
            "entry_prompt": "",
        }

    leading_match = LEADING_TRIGGER_RE.match(text)
    if leading_match:
        entry_prompt = _strip_separator(leading_match.group("suffix"))
        if entry_prompt and not _looks_like_reference_not_invocation(entry_prompt) and _looks_like_action_invocation(entry_prompt):
            return {
                "matched": True,
                "invocation_mode": INVOCATION_MODE_GFLOW_EXPLICIT,
                "trigger_token": leading_match.group("trigger"),
                "entry_prompt": entry_prompt,
            }

    command_match = COMMAND_TRIGGER_RE.search(text)
    if command_match:
        entry_prompt = _strip_separator(command_match.group("suffix"))
        if entry_prompt and not _looks_like_reference_not_invocation(entry_prompt) and _looks_like_action_invocation(entry_prompt):
            return {
                "matched": True,
                "invocation_mode": INVOCATION_MODE_GFLOW_EXPLICIT,
                "trigger_token": command_match.group("trigger"),
                "entry_prompt": entry_prompt,
            }

    return {
        "matched": False,
        "invocation_mode": "",
        "trigger_token": "",
        "entry_prompt": "",
    }


def _default_detection(entry_prompt: str) -> dict[str, Any]:
    return {
        "status": "workflow-recommended",
        "recognized_stage": "multi-stage",
        "matched_skills": list(DEFAULT_READONLY_PATH),
        "suggested_path": list(DEFAULT_READONLY_PATH),
        "assistant_message": (
            "这是一次显式 `GFlow` 调用，但剩余 prompt 没有稳定命中现有主动路由规则。"
            "先回退到保守的只读链 `office-hours -> plan-eng-review`。"
        ),
        "initial_action_plan": gstack_phase1_entry.build_chain_plan(DEFAULT_READONLY_PATH),
        "entry_prompt": entry_prompt,
        "path_source": "default-readonly-template",
    }


def _resolve_detection(entry_prompt: str) -> dict[str, Any]:
    detection = gstack_phase1_entry.detect_workflow_path(entry_prompt)
    if detection.get("status") != "workflow-recommended":
        return _apply_phase4_template_detection(entry_prompt, _default_detection(entry_prompt))
    suggestion = dict(detection)
    suggestion["entry_prompt"] = entry_prompt
    suggestion["path_source"] = "workflow-detection"
    return _apply_phase4_template_detection(entry_prompt, suggestion)


def _build_stage_descriptors(suggested_path: list[str]) -> list[dict[str, Any]]:
    stages: list[dict[str, Any]] = []
    for index, skill in enumerate(suggested_path, start=1):
        stages.append(
            {
                "stage_id": f"stage-{index}",
                "skill": skill,
                "status": STAGE_STATUS_PENDING,
                "position": index,
                "phase_execution_mode": "main-thread-handoff",
            }
        )
    return stages


def build_workflow_plan(
    *,
    run_id: str,
    invocation_mode: str,
    trigger_token: str,
    trigger_prompt: str,
    entry_prompt: str,
    workflow_detection: dict[str, Any],
) -> dict[str, Any]:
    if not run_id.strip():
        raise ValueError("run_id is required")
    if invocation_mode not in {INVOCATION_MODE_ADVISORY, INVOCATION_MODE_GFLOW_EXPLICIT}:
        raise ValueError("invocation_mode is invalid")
    if not entry_prompt.strip():
        raise ValueError("entry_prompt is required")

    suggested_path = [
        str(item).strip()
        for item in workflow_detection.get("suggested_path", [])
        if str(item).strip()
    ]
    if not suggested_path:
        raise ValueError("workflow_detection.suggested_path is required")

    stages = _build_stage_descriptors(suggested_path)
    template_id, template_definition = _lookup_workflow_template(suggested_path)
    template = _template_payload(template_id, template_definition) if template_id and template_definition else {}
    project_scope = _resolve_review_scope(entry_prompt, suggested_path)
    if template:
        stage_contracts = {
            str(item.get("skill", "")).strip(): dict(item)
            for item in template.get("stage_contracts", [])
            if str(item.get("skill", "")).strip()
        }
        for stage in stages:
            contract = stage_contracts.get(str(stage.get("skill", "")).strip())
            if contract:
                stage["template_contract"] = dict(contract)
    plan = {
        "schema_version": WORKFLOW_PLAN_SCHEMA_VERSION,
        "run_id": run_id,
        "created_at": _utc_now(),
        "invocation_mode": invocation_mode,
        "trigger_token": trigger_token,
        "trigger_prompt": trigger_prompt,
        "entry_prompt": entry_prompt,
        "recognized_stage": str(workflow_detection.get("recognized_stage", "")).strip(),
        "path_source": str(workflow_detection.get("path_source", "workflow-detection")).strip(),
        "assistant_message": str(workflow_detection.get("assistant_message", "")).strip(),
        "suggested_path": suggested_path,
        "initial_stage": stages[0]["skill"],
        "initial_action_plan": [
            str(item).strip()
            for item in workflow_detection.get("initial_action_plan", [])
            if str(item).strip()
        ],
        "stages": stages,
        "runtime_mode": "phase1-main-thread",
        "default_safety_profile": "auto-safe",
    }
    if template:
        plan["template"] = dict(template)
        plan["template_id"] = str(template.get("template_id", "")).strip()
        plan["template_label"] = str(template.get("label", "")).strip()
        plan["template_description"] = str(template.get("description", "")).strip()
    if project_scope:
        plan["project_scope"] = dict(project_scope)
        plan["project_scope_id"] = str(project_scope.get("scope_id", "")).strip()
        plan["project_scope_label"] = str(project_scope.get("label", "")).strip()
    return plan


def build_workflow_preview(prompt: str) -> dict[str, Any]:
    trigger = detect_gflow_trigger(prompt)
    if not trigger["matched"]:
        raise ValueError("prompt does not explicitly invoke GFlow")
    entry_prompt = str(trigger.get("entry_prompt", "")).strip()
    if not entry_prompt:
        raise ValueError("GFlow trigger matched but entry_prompt is empty")
    workflow_detection = _resolve_detection(entry_prompt)
    suggested_path = [
        str(item).strip()
        for item in workflow_detection.get("suggested_path", [])
        if str(item).strip()
    ]
    if not suggested_path:
        raise ValueError("workflow_detection.suggested_path is required")
    template_id, template_definition = _lookup_workflow_template(suggested_path)
    template = _template_payload(template_id, template_definition) if template_id and template_definition else {}
    project_scope = _resolve_review_scope(entry_prompt, suggested_path)
    workflow_plan = {
        "schema_version": WORKFLOW_PLAN_SCHEMA_VERSION,
        "run_id": "",
        "created_at": "",
        "invocation_mode": INVOCATION_MODE_GFLOW_EXPLICIT,
        "trigger_token": str(trigger.get("trigger_token", "GFlow")).strip() or "GFlow",
        "trigger_prompt": prompt.strip(),
        "entry_prompt": entry_prompt,
        "recognized_stage": str(workflow_detection.get("recognized_stage", "")).strip(),
        "path_source": str(workflow_detection.get("path_source", "workflow-detection")).strip(),
        "assistant_message": str(workflow_detection.get("assistant_message", "")).strip(),
        "suggested_path": suggested_path,
        "initial_stage": suggested_path[0],
        "initial_action_plan": [
            str(item).strip()
            for item in workflow_detection.get("initial_action_plan", [])
            if str(item).strip()
        ],
        "stages": _build_stage_descriptors(suggested_path),
        "runtime_mode": "phase1-preview",
        "default_safety_profile": "auto-safe",
    }
    if template:
        workflow_plan["template"] = dict(template)
        workflow_plan["template_id"] = str(template.get("template_id", "")).strip()
        workflow_plan["template_label"] = str(template.get("label", "")).strip()
        workflow_plan["template_description"] = str(template.get("description", "")).strip()
    if project_scope:
        workflow_plan["project_scope"] = dict(project_scope)
        workflow_plan["project_scope_id"] = str(project_scope.get("scope_id", "")).strip()
        workflow_plan["project_scope_label"] = str(project_scope.get("label", "")).strip()
    return {
        "status": "gflow-preview-ready",
        "matched": True,
        "invocation_mode": INVOCATION_MODE_GFLOW_EXPLICIT,
        "trigger_token": str(trigger.get("trigger_token", "")).strip(),
        "entry_prompt": entry_prompt,
        "path_source": workflow_plan["path_source"],
        "suggested_path": list(workflow_plan["suggested_path"]),
        "initial_stage": workflow_plan["initial_stage"],
        "template_id": str(workflow_plan.get("template_id", "")).strip(),
        "template_label": str(workflow_plan.get("template_label", "")).strip(),
        "project_scope_id": str(workflow_plan.get("project_scope_id", "")).strip(),
        "project_scope_label": str(workflow_plan.get("project_scope_label", "")).strip(),
        "workflow_detection": workflow_detection,
        "workflow_plan": workflow_plan,
        "main_thread_handoff": build_main_thread_handoff(workflow_plan, include_run_id=False),
    }


def build_stage_result(
    *,
    run_id: str,
    stage_id: str,
    skill: str,
    status: str,
    summary: str,
    next_action: str = "",
    evidence: list[str] | None = None,
) -> dict[str, Any]:
    if not run_id.strip():
        raise ValueError("run_id is required")
    if not stage_id.strip():
        raise ValueError("stage_id is required")
    if not skill.strip():
        raise ValueError("skill is required")
    normalized_status = _validate_stage_status(status)
    return {
        "schema_version": WORKFLOW_STAGE_RESULT_SCHEMA_VERSION,
        "run_id": run_id,
        "stage_id": stage_id,
        "skill": skill.strip(),
        "status": normalized_status,
        "summary": summary.strip(),
        "next_action": next_action.strip(),
        "evidence": [str(item).strip() for item in (evidence or []) if str(item).strip()],
        "updated_at": _utc_now(),
    }


def build_run_summary(
    *,
    run_id: str,
    status: str,
    current_stage: str,
    summary: str,
    completed_stages: list[str] | None = None,
    next_action: str = "",
) -> dict[str, Any]:
    if not run_id.strip():
        raise ValueError("run_id is required")
    normalized_status = _validate_run_status(status)
    if not current_stage.strip():
        raise ValueError("current_stage is required")
    return {
        "schema_version": WORKFLOW_RUN_SUMMARY_SCHEMA_VERSION,
        "run_id": run_id,
        "status": normalized_status,
        "current_stage": current_stage.strip(),
        "summary": summary.strip(),
        "completed_stages": [
            str(item).strip() for item in (completed_stages or []) if str(item).strip()
        ],
        "next_action": next_action.strip(),
        "updated_at": _utc_now(),
    }


def build_main_thread_handoff(workflow_plan: dict[str, Any], *, include_run_id: bool = True) -> str:
    suggested_path = " -> ".join(workflow_plan.get("suggested_path", []))
    template = dict(workflow_plan.get("template") or {})
    template_id = str(template.get("template_id", "") or workflow_plan.get("template_id", "")).strip()
    template_label = str(template.get("label", "") or workflow_plan.get("template_label", "")).strip()
    lines = [
        DEFAULT_HANDOFF_TITLE,
        f"- schema_version: {workflow_plan['schema_version']}",
        f"- invocation_mode: {workflow_plan['invocation_mode']}",
        f"- entry_prompt: {workflow_plan['entry_prompt']}",
        f"- suggested_path: {suggested_path}",
        f"- template: {template_id} | {template_label}" if template_id or template_label else "",
        f"- initial_stage: {workflow_plan['initial_stage']}",
        "- initial_action_plan:",
    ]
    if include_run_id:
        lines.insert(2, f"- run_id: {workflow_plan['run_id']}")
    lines = [line for line in lines if line]
    for step in workflow_plan.get("initial_action_plan", []):
        lines.append(f"  - {step}")
    if template:
        description = str(template.get("description", "")).strip()
        if description:
            lines.append(f"- template_description: {description}")
        stage_contract = next(
            (
                item
                for item in template.get("stage_contracts", [])
                if str(item.get("skill", "")).strip()
                == str(workflow_plan.get("initial_stage", "")).strip()
            ),
            {},
        )
        deliverable = str(stage_contract.get("deliverable", "")).strip()
        if deliverable:
            lines.append(f"- initial_stage_deliverable: {deliverable}")
        gate_rule = str(stage_contract.get("gate_rule", "")).strip()
        if gate_rule:
            lines.append(f"- initial_stage_gate_rule: {gate_rule}")
        auto_continue = bool(template.get("auto_continue", False))
        lines.append(f"- auto_continue: {'true' if auto_continue else 'false'}")
        stop_conditions = [
            str(item).strip() for item in template.get("stop_conditions", []) if str(item).strip()
        ]
        if stop_conditions:
            lines.append(f"- stop_conditions: {', '.join(stop_conditions)}")
    project_scope = dict(workflow_plan.get("project_scope") or {})
    if project_scope:
        lines.append(f"- project_scope: {project_scope.get('scope_id', '')} | {project_scope.get('label', '')}")
        reason = str(project_scope.get("reason", "")).strip()
        if reason:
            lines.append(f"- project_scope_reason: {reason}")
        files = [str(item).strip() for item in project_scope.get("files", []) if str(item).strip()]
        if files:
            lines.append("- project_scope_files:")
            for path in files[:8]:
                lines.append(f"  - {path}")
    lines.append(
        "请按 suggested_path 从首阶段开始推进。若模板声明 `auto_continue: true`，"
        "则在当前阶段完成且未命中 stop_conditions 时，继续推进到下一阶段，不要停在单一 review 结果上。"
    )
    return "\n".join(lines)


def _stage_result_from_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": WORKFLOW_STAGE_RESULT_SCHEMA_VERSION,
        "run_id": str(row.get("run_id", "")).strip(),
        "stage_id": str(row.get("stage_id", "")).strip(),
        "skill": str(row.get("skill", "")).strip(),
        "status": str(row.get("status", "")).strip(),
        "summary": str(row.get("summary", "")).strip(),
        "next_action": str(row.get("next_action", "")).strip(),
        "evidence": [
            str(item).strip()
            for item in _normalize_json_list(row.get("evidence_json", "[]"))
            if str(item).strip()
        ],
        "updated_at": str(row.get("updated_at", "")).strip(),
    }


def _next_stage_descriptor(
    stages: list[dict[str, Any]],
    current_stage_id: str,
) -> dict[str, Any] | None:
    seen_current = False
    for stage in stages:
        if str(stage.get("stage_id", "")).strip() == current_stage_id:
            seen_current = True
            continue
        if seen_current:
            return dict(stage)
    return None


def _stage_descriptor(stages: list[dict[str, Any]], stage_id: str) -> dict[str, Any] | None:
    for stage in stages:
        if str(stage.get("stage_id", "")).strip() == stage_id:
            return dict(stage)
    return None


def _template_stage_contract(workflow_plan: dict[str, Any], skill: str) -> dict[str, Any]:
    template = dict(workflow_plan.get("template") or {})
    for item in template.get("stage_contracts", []):
        if str(item.get("skill", "")).strip() == str(skill or "").strip():
            return dict(item)
    return {}


def _default_run_summary_text(run_row: dict[str, Any]) -> str:
    status = str(run_row.get("status", "")).strip()
    current_stage_skill = str(run_row.get("current_stage_skill", "")).strip()
    gate_reason = str(run_row.get("gate_reason", "")).strip()
    if status == RUN_STATUS_RUNNING:
        return f"GFlow 显式流程记录正在 `{current_stage_skill}` 阶段执行。".strip()
    if status == RUN_STATUS_PAUSED:
        return f"GFlow 显式流程记录在 `{current_stage_skill}` 阶段暂停。{gate_reason}".strip()
    if status == RUN_STATUS_AWAITING_APPROVAL:
        return f"GFlow 显式流程记录在 `{current_stage_skill}` 阶段等待批准。{gate_reason}".strip()
    if status == RUN_STATUS_FROZEN:
        return f"GFlow 显式流程记录在 `{current_stage_skill}` 阶段冻结。{gate_reason}".strip()
    if status == RUN_STATUS_COMPLETED:
        return "GFlow 显式流程记录已完成全部阶段。"
    return "GFlow 显式流程记录已创建。"


def _run_summary_from_row(run_row: dict[str, Any], stage_rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed_stages = [
        str(row.get("skill", "")).strip()
        for row in stage_rows
        if str(row.get("status", "")).strip() == STAGE_STATUS_COMPLETED
    ]
    return {
        "schema_version": WORKFLOW_RUN_SUMMARY_SCHEMA_VERSION,
        "run_id": str(run_row.get("run_id", "")).strip(),
        "status": str(run_row.get("status", "")).strip(),
        "current_stage": str(run_row.get("current_stage_skill", "")).strip(),
        "summary": str(run_row.get("latest_summary", "")).strip() or _default_run_summary_text(run_row),
        "completed_stages": completed_stages,
        "next_action": str(run_row.get("latest_next_action", "")).strip(),
        "updated_at": str(run_row.get("updated_at", "")).strip(),
    }


def _gate_payload_from_row(run_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": str(run_row.get("gate_type", "")).strip(),
        "reason": str(run_row.get("gate_reason", "")).strip(),
        "token": str(run_row.get("gate_token", "")).strip(),
        "freeze_scope": str(run_row.get("freeze_scope", "")).strip(),
    }


def _run_status_priority(status: str) -> int:
    normalized = str(status or "").strip()
    order = {
        RUN_STATUS_RUNNING: 0,
        RUN_STATUS_AWAITING_APPROVAL: 1,
        RUN_STATUS_PAUSED: 2,
        RUN_STATUS_FROZEN: 3,
        RUN_STATUS_PLANNED: 4,
        RUN_STATUS_COMPLETED: 5,
    }
    return order.get(normalized, 99)


def _sort_run_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        payloads,
        key=lambda payload: (
            _run_status_priority(str(payload.get("run_status", "")).strip()),
            -_timestamp_sort_value(str(payload.get("updated_at", "")).strip()),
            -_timestamp_sort_value(str(payload.get("created_at", "")).strip()),
            str(payload.get("run_id", "")).strip(),
        ),
    )


def _sql_run_status_priority() -> str:
    return (
        "CASE status "
        "WHEN 'running' THEN 0 "
        "WHEN 'awaiting_approval' THEN 1 "
        "WHEN 'paused' THEN 2 "
        "WHEN 'frozen' THEN 3 "
        "WHEN 'planned' THEN 4 "
        "WHEN 'completed' THEN 5 "
        "ELSE 99 END"
    )


def build_runtime_handoff(payload: dict[str, Any]) -> str:
    run_summary = dict(payload.get("run_summary", {}))
    workflow_plan = dict(payload.get("workflow_plan", {}))
    gate = dict(payload.get("gate", {}))
    suggested_path = " -> ".join(
        [str(item).strip() for item in workflow_plan.get("suggested_path", []) if str(item).strip()]
    )
    template = dict(workflow_plan.get("template") or {})
    template_id = str(template.get("template_id", "") or workflow_plan.get("template_id", "")).strip()
    template_label = str(template.get("label", "") or workflow_plan.get("template_label", "")).strip()
    lines = [
        RUNTIME_HANDOFF_TITLE,
        f"- run_id: {payload.get('run_id', '')}",
        f"- run_status: {run_summary.get('status', '')}",
        f"- current_stage: {run_summary.get('current_stage', '')}",
        f"- entry_prompt: {workflow_plan.get('entry_prompt', '')}",
    ]
    if suggested_path:
        lines.append(f"- suggested_path: {suggested_path}")
    if template_id or template_label:
        lines.append(f"- template: {template_id} | {template_label}")
    if template:
        lines.append(f"- auto_continue: {'true' if bool(template.get('auto_continue', False)) else 'false'}")
        stop_conditions = [
            str(item).strip() for item in template.get("stop_conditions", []) if str(item).strip()
        ]
        if stop_conditions:
            lines.append(f"- stop_conditions: {', '.join(stop_conditions)}")
    project_scope = dict(workflow_plan.get("project_scope") or {})
    if project_scope:
        lines.append(
            f"- project_scope: {project_scope.get('scope_id', '')} | {project_scope.get('label', '')}"
        )
    summary = str(run_summary.get("summary", "")).strip()
    if summary:
        lines.append(f"- summary: {summary}")
    next_action = str(run_summary.get("next_action", "")).strip()
    if next_action:
        lines.append(f"- next_action: {next_action}")
    gate_type = str(gate.get("type", "")).strip()
    if gate_type:
        lines.append(f"- gate_type: {gate_type}")
    gate_reason = str(gate.get("reason", "")).strip()
    if gate_reason:
        lines.append(f"- gate_reason: {gate_reason}")
    gate_token = str(gate.get("token", "")).strip()
    if gate_token:
        lines.append(f"- gate_token: {gate_token}")
    freeze_scope = str(gate.get("freeze_scope", "")).strip()
    if freeze_scope:
        lines.append(f"- freeze_scope: {freeze_scope}")
    stage_results = payload.get("stage_results", [])
    if stage_results:
        current_stage = str(run_summary.get("current_stage", "")).strip()
        current_result = next(
            (
                item
                for item in stage_results
                if str(item.get("skill", "")).strip() == current_stage
                or str(item.get("stage_id", "")).strip() == str(payload.get("current_stage_id", "")).strip()
            ),
            {},
        )
        evidence = [
            str(item).strip()
            for item in current_result.get("evidence", [])
            if str(item).strip()
        ]
        if evidence:
            lines.append("- current_stage_evidence:")
            for item in evidence[:5]:
                lines.append(f"  - {item}")
    lines.append(
        "若当前阶段完成且未命中 gate / stop_conditions，则继续推进到下一阶段，不要把单阶段结果当成 workflow 终点。"
    )
    return "\n".join(lines)


def build_runtime_execution_contract(payload: dict[str, Any]) -> str:
    run_id = str(payload.get("run_id", "")).strip()
    workflow_plan = dict(payload.get("workflow_plan") or {})
    template = dict(workflow_plan.get("template") or {})
    if not run_id or not template:
        return ""
    path = [str(item).strip() for item in workflow_plan.get("suggested_path", []) if str(item).strip()]
    stop_conditions = [str(item).strip() for item in template.get("stop_conditions", []) if str(item).strip()]
    lines = [
        "GFlow 显式流程契约",
        f"- run_id: {run_id}",
        f"- template: {str(template.get('template_id', '')).strip()} | {str(template.get('label', '')).strip()}",
        f"- stages: {' -> '.join(path)}",
        f"- auto_continue: {'true' if bool(template.get('auto_continue', False)) else 'false'}",
    ]
    if stop_conditions:
        lines.append(f"- stop_conditions: {', '.join(stop_conditions)}")
    lines.extend(
        [
            "这次请求不是普通单阶段任务。你必须按这条显式流程链连续推进，不能在 review findings 后直接停下。",
            "每完成一个阶段后，立即更新轻量记录并继续下一阶段；只有命中 stop_conditions 才允许暂停。",
            f"推进命令：python3 {REPO_ROOT / 'ops' / 'gstack_automation.py'} advance --run-id {run_id} --summary '<阶段摘要>' --next-action '<下一步>'",
            f"暂停命令：python3 {REPO_ROOT / 'ops' / 'gstack_automation.py'} pause --run-id {run_id} --reason '<暂停原因>' --gate-type user",
        ]
    )
    return "\n".join(lines)


def _template_stop_conditions(workflow_plan: dict[str, Any]) -> set[str]:
    template = dict(workflow_plan.get("template") or {})
    return {
        str(item).strip()
        for item in template.get("stop_conditions", [])
        if str(item).strip()
    }


def _template_auto_continue_enabled(workflow_plan: dict[str, Any]) -> bool:
    template = dict(workflow_plan.get("template") or {})
    return bool(template.get("auto_continue", False))


def _extract_finalize_launch_payload(text: str) -> dict[str, Any]:
    for line in reversed(str(text or "").splitlines()):
        if not line.startswith("WORKSPACE_HUB_FINALIZE_LAUNCH="):
            continue
        raw = line.split("=", 1)[1].strip()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(payload) if isinstance(payload, dict) else {}
    return {}


def _parse_stage_stop_signal(text: str) -> tuple[str, str]:
    match = STAGE_STOP_RE.search(str(text or ""))
    if not match:
        return "", ""
    condition = str(match.group("condition") or "").strip().lower()
    reason = str(match.group("reason") or "").strip()
    return condition, reason


def _normalize_stage_execution_result(skill: str, payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "summary": str(payload.get("summary", "")).strip(),
        "next_action": str(payload.get("next_action", "")).strip(),
        "evidence": [str(item).strip() for item in payload.get("evidence", []) if str(item).strip()],
        "stop_condition": str(payload.get("stop_condition", "")).strip(),
        "stop_reason": str(payload.get("stop_reason", "")).strip(),
        "gate_type": str(payload.get("gate_type", "")).strip(),
        "freeze_scope": str(payload.get("freeze_scope", "")).strip(),
    }
    if not normalized["summary"]:
        normalized["summary"] = f"`{skill}` 阶段已完成。"
    if not normalized["next_action"]:
        if skill == "fix":
            normalized["next_action"] = "进入 `qa` 阶段。"
        elif skill == "qa":
            normalized["next_action"] = "进入 `writeback` 阶段。"
        elif skill == "writeback":
            normalized["next_action"] = "已完成，无需继续。"
        else:
            normalized["next_action"] = "继续下一阶段。"
    return normalized


def _fixture_stage_execution_result(skill: str) -> dict[str, Any] | None:
    if str(os.environ.get("WORKSPACE_HUB_GFLOW_STAGE_EXECUTOR", "")).strip() != "fixture":
        return None
    raw = str(os.environ.get("WORKSPACE_HUB_GFLOW_STAGE_RESPONSES", "")).strip()
    payload = _normalize_json_dict(raw)
    if not payload:
        return _normalize_stage_execution_result(skill, {})
    stage_payload = payload.get(skill)
    if not isinstance(stage_payload, dict):
        return _normalize_stage_execution_result(skill, {})
    return _normalize_stage_execution_result(skill, dict(stage_payload))


def _build_stage_execution_prompt(payload: dict[str, Any], stage_skill: str) -> str:
    workflow_plan = dict(payload.get("workflow_plan") or {})
    template = dict(workflow_plan.get("template") or {})
    stage_contract = _template_stage_contract(workflow_plan, stage_skill)
    project_scope = dict(workflow_plan.get("project_scope") or {})
    stage_results = [dict(item) for item in payload.get("stage_results", []) if isinstance(item, dict)]
    previous_stage = next(
        (
            item
            for item in reversed(stage_results)
            if str(item.get("skill", "")).strip() != stage_skill
            and str(item.get("status", "")).strip() == STAGE_STATUS_COMPLETED
        ),
        {},
    )
    lines = [
        f"当前在一条既有 GFlow 显式流程记录的 `{stage_skill}` 阶段内执行，不要创建新的 workflow，也不要停在说明层。",
        f"run_id: {str(payload.get('run_id', '')).strip()}",
        f"project_name: {str(payload.get('project_name', '')).strip()}",
        f"entry_prompt: {str(payload.get('entry_prompt', '')).strip()}",
        f"template: {str(template.get('template_id', '')).strip()} | {str(template.get('label', '')).strip()}",
        f"current_stage: {stage_skill}",
    ]
    deliverable = str(stage_contract.get("deliverable", "")).strip()
    if deliverable:
        lines.append(f"当前交付目标: {deliverable}")
    gate_rule = str(stage_contract.get("gate_rule", "")).strip()
    if gate_rule:
        lines.append(f"当前 gate 规则: {gate_rule}")
    previous_summary = str(previous_stage.get("summary", "")).strip()
    if previous_summary:
        lines.append(f"上一阶段摘要: {previous_summary}")
    previous_evidence = [str(item).strip() for item in previous_stage.get("evidence", []) if str(item).strip()]
    if previous_evidence:
        lines.append("上一阶段 evidence:")
        for item in previous_evidence[:6]:
            lines.append(f"- {item}")
    scope_files = [str(item).strip() for item in project_scope.get("files", []) if str(item).strip()]
    if scope_files:
        lines.append("当前代码范围：")
        for path in scope_files[:16]:
            lines.append(f"- {path}")
    lines.extend(
        [
            "执行要求：",
            f"1. 直接完成 `{stage_skill}` 阶段该做的工作，不要只复述计划。",
            "2. 如果命中 needs_context / approval_required / freeze / blocked / human_choice_required，"
            "请在回复里单独写一行 `GFLOW_STOP:<condition> - <reason>`。",
            "3. 如果没有命中停止条件，请给出阶段结果摘要、关键证据和下一步。",
        ]
    )
    return "\n".join(lines)


def _run_stage_via_start_codex(payload: dict[str, Any], stage_skill: str) -> dict[str, Any]:
    if stage_skill == "writeback":
        run_summary = dict(payload.get("run_summary") or {})
        summary = (
            f"GFlow 运行结论已写回项目板和摘要。"
            f" 当前状态：{str(run_summary.get('status', '')).strip() or 'running'}。"
        )
        evidence = []
        latest_summary = str(payload.get("latest_summary", "")).strip()
        if latest_summary:
            evidence.append(latest_summary)
        return _normalize_stage_execution_result(
            stage_skill,
            {
                "summary": summary,
                "next_action": "已完成，无需继续。",
                "evidence": evidence,
            },
        )

    project_name = str(payload.get("project_name", "")).strip()
    if not project_name:
        return _normalize_stage_execution_result(
            stage_skill,
            {
                "summary": f"`{stage_skill}` 阶段缺少 project_name，无法继续自动执行。",
                "next_action": "补齐项目上下文后重试。",
                "stop_condition": "blocked",
                "stop_reason": "workflow run 缺少 project_name，轻量记录无法自动拉起下一阶段。",
            },
        )
    prompt = _build_stage_execution_prompt(payload, stage_skill)
    command = [
        "bash",
        str(REPO_ROOT / "ops" / "start-codex"),
        "--project",
        project_name,
        "--prompt",
        prompt,
        "--no-open-obsidian",
        "--no-auto-resume",
        "--source",
        "gflow",
        "--thread-name",
        f"GFlow {str(payload.get('run_id', '')).strip()} {stage_skill}",
    ]
    env = os.environ.copy()
    env.setdefault("WORKSPACE_HUB_SKIP_DISCOVERY", "1")
    completed = subprocess.run(
        command,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    finalize_payload = _extract_finalize_launch_payload(completed.stderr)
    reply_text = str(finalize_payload.get("reply_text", "")).strip()
    summary_excerpt = str(finalize_payload.get("summary_excerpt", "")).strip()
    stop_condition, stop_reason = _parse_stage_stop_signal(reply_text or summary_excerpt)
    summary_text = reply_text or summary_excerpt or completed.stdout.strip()
    if completed.returncode != 0 and not stop_condition:
        error_line = ""
        combined = "\n".join(
            item for item in [completed.stderr.strip(), completed.stdout.strip(), summary_text] if item
        ).strip()
        if combined:
            error_line = combined.splitlines()[-1].strip()
        stop_condition = "blocked"
        stop_reason = error_line or f"`{stage_skill}` 阶段执行失败。"
    evidence = []
    if summary_excerpt and summary_excerpt != summary_text:
        evidence.append(summary_excerpt)
    if reply_text and reply_text not in evidence:
        evidence.append(reply_text)
    return _normalize_stage_execution_result(
        stage_skill,
        {
            "summary": summary_text or f"`{stage_skill}` 阶段已完成。",
            "next_action": "",
            "evidence": evidence,
            "stop_condition": stop_condition,
            "stop_reason": stop_reason,
        },
    )


def _execute_auto_stage(payload: dict[str, Any]) -> dict[str, Any]:
    current_stage = str(payload.get("current_stage", "")).strip()
    fixture_payload = _fixture_stage_execution_result(current_stage)
    if fixture_payload is not None:
        return fixture_payload
    return _run_stage_via_start_codex(payload, current_stage)


def _maybe_auto_continue_run(run_id: str) -> dict[str, Any] | None:
    payload = fetch_workflow_run(run_id)
    workflow_plan = dict(payload.get("workflow_plan") or {})
    if not _template_auto_continue_enabled(workflow_plan):
        return None
    current_stage = str(payload.get("current_stage", "")).strip()
    if current_stage not in AUTO_EXECUTABLE_STAGE_SKILLS:
        return None
    stage_result = _execute_auto_stage(payload)
    stop_condition = str(stage_result.get("stop_condition", "")).strip()
    if stop_condition:
        stop_conditions = _template_stop_conditions(workflow_plan)
        if stop_condition in stop_conditions:
            gate_type = str(stage_result.get("gate_type", "")).strip() or STOP_CONDITION_GATE_MAP.get(
                stop_condition,
                GATE_TYPE_USER,
            )
            return pause_workflow_run(
                run_id,
                reason=str(stage_result.get("stop_reason", "")).strip() or str(stage_result.get("summary", "")).strip(),
                gate_type=gate_type,
                next_action=str(stage_result.get("next_action", "")).strip(),
                evidence=[str(item).strip() for item in stage_result.get("evidence", []) if str(item).strip()],
                freeze_scope=str(stage_result.get("freeze_scope", "")).strip(),
            )
    return advance_workflow_run(
        run_id,
        summary=str(stage_result.get("summary", "")).strip(),
        next_action=str(stage_result.get("next_action", "")).strip(),
        evidence=[str(item).strip() for item in stage_result.get("evidence", []) if str(item).strip()],
        allow_auto_execute=True,
    )


def create_workflow_run_from_prompt(prompt: str) -> dict[str, Any]:
    trigger = detect_gflow_trigger(prompt)
    if not trigger["matched"]:
        raise ValueError("prompt does not explicitly invoke GFlow")

    entry_prompt = str(trigger.get("entry_prompt", "")).strip()
    if not entry_prompt:
        raise ValueError("GFlow trigger matched but entry_prompt is empty")

    workflow_detection = _resolve_detection(entry_prompt)
    run_id = f"gflow-{uuid.uuid4().hex[:12]}"
    workflow_plan = build_workflow_plan(
        run_id=run_id,
        invocation_mode=INVOCATION_MODE_GFLOW_EXPLICIT,
        trigger_token=str(trigger.get("trigger_token", "GFlow")).strip() or "GFlow",
        trigger_prompt=prompt.strip(),
        entry_prompt=entry_prompt,
        workflow_detection=workflow_detection,
    )
    initial_stage_descriptor = dict(workflow_plan["stages"][0])
    initial_stage_result = build_stage_result(
        run_id=run_id,
        stage_id=initial_stage_descriptor["stage_id"],
        skill=initial_stage_descriptor["skill"],
        status=STAGE_STATUS_PENDING,
        summary="已创建显式 GFlow 流程记录，等待 Codex 主线程进入首阶段。",
        next_action=f"进入 `{initial_stage_descriptor['skill']}` 并按 handoff 计划推进。",
    )
    run_summary = build_run_summary(
        run_id=run_id,
        status=RUN_STATUS_PLANNED,
        current_stage=initial_stage_descriptor["skill"],
        summary="已生成显式 GFlow 流程链与轻量记录，尚未进入活跃运行态。",
        completed_stages=[],
        next_action=f"由 Codex 主线程从 `{initial_stage_descriptor['skill']}` 开始执行。",
    )
    main_thread_handoff = build_main_thread_handoff(workflow_plan)
    return {
        "status": "gflow-run-created",
        "matched": True,
        "invocation_mode": INVOCATION_MODE_GFLOW_EXPLICIT,
        "trigger_token": trigger["trigger_token"],
        "entry_prompt": entry_prompt,
        "path_source": workflow_plan["path_source"],
        "suggested_path": list(workflow_plan["suggested_path"]),
        "initial_stage": initial_stage_descriptor["skill"],
        "template_id": str(workflow_plan.get("template_id", "")).strip(),
        "template_label": str(workflow_plan.get("template_label", "")).strip(),
        "project_scope_id": str(workflow_plan.get("project_scope_id", "")).strip(),
        "project_scope_label": str(workflow_plan.get("project_scope_label", "")).strip(),
        "workflow_detection": workflow_detection,
        "workflow_plan": workflow_plan,
        "initial_stage_result": initial_stage_result,
        "run_summary": run_summary,
        "main_thread_handoff": main_thread_handoff,
    }


def _stage_rows_for_runtime_start(workflow_plan: dict[str, Any]) -> list[dict[str, Any]]:
    stage_rows: list[dict[str, Any]] = []
    for stage in workflow_plan.get("stages", []):
        descriptor = dict(stage)
        stage_id = str(descriptor.get("stage_id", "")).strip()
        skill = str(descriptor.get("skill", "")).strip()
        status = STAGE_STATUS_RUNNING if descriptor.get("position") == 1 else STAGE_STATUS_PENDING
        summary = (
            "已进入首阶段，等待 Codex 主线程按 handoff 推进。"
            if status == STAGE_STATUS_RUNNING
            else ""
        )
        next_action = f"继续 `{skill}`。" if status == STAGE_STATUS_RUNNING else ""
        stage_rows.append(
            {
                "run_id": str(workflow_plan.get("run_id", "")).strip(),
                "stage_id": stage_id,
                "skill": skill,
                "position": int(descriptor.get("position", len(stage_rows) + 1) or len(stage_rows) + 1),
                "status": status,
                "summary": summary,
                "next_action": next_action,
                "evidence_json": "[]",
                "handoff_json": "{}",
                "updated_at": _utc_now(),
            }
        )
        descriptor["status"] = status
    return stage_rows


def _insert_workflow_run(
    *,
    workflow_plan: dict[str, Any],
    project_name: str,
    session_id: str,
    stage_rows: list[dict[str, Any]],
) -> None:
    runtime_state.init_db()
    initial_stage = dict(workflow_plan.get("stages", [{}])[0])
    latest_summary = f"GFlow 已进入 `{initial_stage.get('skill', '')}` 阶段。".strip()
    latest_next_action = f"按 handoff 推进 `{initial_stage.get('skill', '')}`。".strip()
    now = _utc_now()
    metadata = {
        "path_source": workflow_plan.get("path_source", ""),
        "default_safety_profile": workflow_plan.get("default_safety_profile", "auto-safe"),
    }
    with runtime_state.transaction() as conn:
        conn.execute(
            """
            INSERT INTO gflow_runs (
                run_id, project_name, session_id, invocation_mode, status,
                current_stage_id, current_stage_skill, gate_type, gate_reason, gate_token,
                freeze_scope, latest_summary, latest_next_action, workflow_plan_json,
                metadata_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, '', '', '', '', ?, ?, ?, ?, ?, ?)
            """,
            (
                workflow_plan["run_id"],
                project_name,
                session_id,
                workflow_plan["invocation_mode"],
                RUN_STATUS_RUNNING,
                initial_stage.get("stage_id", ""),
                initial_stage.get("skill", ""),
                latest_summary,
                latest_next_action,
                runtime_state.json_text(workflow_plan),
                runtime_state.json_text(metadata),
                now,
                now,
            ),
        )
        for row in stage_rows:
            conn.execute(
                """
                INSERT INTO gflow_stage_results (
                    run_id, stage_id, skill, position, status, summary,
                    next_action, evidence_json, handoff_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["run_id"],
                    row["stage_id"],
                    row["skill"],
                    row["position"],
                    row["status"],
                    row["summary"],
                    row["next_action"],
                    row["evidence_json"],
                    row["handoff_json"],
                    row["updated_at"],
                ),
            )
    runtime_state.enqueue_runtime_event(
        queue_name="gflow_run_log",
        event_type="gflow_run_started",
        payload={
            "run_id": workflow_plan["run_id"],
            "project_name": project_name,
            "session_id": session_id,
            "current_stage": initial_stage.get("skill", ""),
        },
        dedupe_key=f"{workflow_plan['run_id']}:started",
        status="completed",
    )


def _fetch_run_row(run_id: str) -> dict[str, Any]:
    runtime_state.init_db()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return {}
    with runtime_state.connect() as conn:
        row = conn.execute("SELECT * FROM gflow_runs WHERE run_id = ?", (normalized_run_id,)).fetchone()
    return runtime_state.row_to_dict(row)


def _fetch_stage_rows(run_id: str) -> list[dict[str, Any]]:
    runtime_state.init_db()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return []
    with runtime_state.connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM gflow_stage_results
            WHERE run_id = ?
            ORDER BY position ASC, stage_id ASC
            """,
            (normalized_run_id,),
        ).fetchall()
    return runtime_state.rows_to_dicts(rows)


def _payload_from_runtime_rows(
    run_row: dict[str, Any],
    stage_rows: list[dict[str, Any]],
    *,
    status_label: str,
) -> dict[str, Any]:
    if not run_row:
        raise ValueError("run not found")
    workflow_plan = _normalize_json_dict(run_row.get("workflow_plan_json", "{}"))
    stage_map = {str(row.get("stage_id", "")).strip(): row for row in stage_rows}
    hydrated_stages: list[dict[str, Any]] = []
    for descriptor in workflow_plan.get("stages", []):
        stage_descriptor = dict(descriptor)
        stage_id = str(stage_descriptor.get("stage_id", "")).strip()
        row = stage_map.get(stage_id)
        if row:
            stage_descriptor["status"] = str(row.get("status", "")).strip()
        hydrated_stages.append(stage_descriptor)
    workflow_plan["stages"] = hydrated_stages
    workflow_plan["runtime_mode"] = "phase2-runtime"
    run_summary = _run_summary_from_row(run_row, stage_rows)
    payload = {
        "status": status_label,
        "run_status": str(run_row.get("status", "")).strip(),
        "run_id": str(run_row.get("run_id", "")).strip(),
        "project_name": str(run_row.get("project_name", "")).strip(),
        "session_id": str(run_row.get("session_id", "")).strip(),
        "invocation_mode": str(run_row.get("invocation_mode", "")).strip(),
        "entry_prompt": str(workflow_plan.get("entry_prompt", "")).strip(),
        "path_source": str(workflow_plan.get("path_source", "")).strip(),
        "suggested_path": [
            str(item).strip() for item in workflow_plan.get("suggested_path", []) if str(item).strip()
        ],
        "template_id": str(workflow_plan.get("template_id", "")).strip(),
        "template_label": str(workflow_plan.get("template_label", "")).strip(),
        "project_scope_id": str(workflow_plan.get("project_scope_id", "")).strip(),
        "project_scope_label": str(workflow_plan.get("project_scope_label", "")).strip(),
        "initial_stage": str(workflow_plan.get("initial_stage", "")).strip(),
        "current_stage_id": str(run_row.get("current_stage_id", "")).strip(),
        "current_stage": str(run_row.get("current_stage_skill", "")).strip(),
        "latest_summary": str(run_row.get("latest_summary", "")).strip(),
        "latest_next_action": str(run_row.get("latest_next_action", "")).strip(),
        "updated_at": str(run_row.get("updated_at", "")).strip(),
        "created_at": str(run_row.get("created_at", "")).strip(),
        "gate": _gate_payload_from_row(run_row),
        "workflow_plan": workflow_plan,
        "stage_results": [_stage_result_from_row(row) for row in stage_rows],
        "run_summary": run_summary,
    }
    payload["main_thread_handoff"] = build_runtime_handoff(payload)
    return payload


def latest_project_workflow_summary(project_name: str) -> dict[str, Any]:
    normalized_project = str(project_name or "").strip()
    if not normalized_project:
        return {}
    runs = list_workflow_runs(project_name=normalized_project, limit=50)
    if not runs:
        return {}
    payload = runs[0]
    run_summary = payload.get("run_summary") or {}
    return {
        "run_id": str(payload.get("run_id", "")).strip(),
        "run_status": str(payload.get("run_status", "")).strip(),
        "current_stage": str(payload.get("current_stage", "")).strip(),
        "template_id": str(payload.get("template_id", "")).strip(),
        "template_label": str(payload.get("template_label", "")).strip(),
        "latest_summary": str(payload.get("latest_summary", "")).strip() or str(run_summary.get("summary", "")).strip(),
        "latest_next_action": str(payload.get("latest_next_action", "")).strip() or str(run_summary.get("next_action", "")).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip() or str(run_summary.get("updated_at", "")).strip(),
        "gate": dict(payload.get("gate") or {}),
        "suggested_path": list(payload.get("suggested_path", [])),
    }


def _sync_phase3_writeback(payload: dict[str, Any]) -> None:
    project_name = str(payload.get("project_name", "")).strip()
    if not project_name:
        return
    try:
        from ops import codex_memory
    except ImportError:  # pragma: no cover
        import codex_memory  # type: ignore

    sync_result = codex_memory.sync_gflow_project_layers(project_name)
    binding = {
        "project_name": project_name,
        "session_id": str(payload.get("session_id", "")).strip(),
        "binding_scope": "project",
        "binding_board_path": str(sync_result.get("board_path", "")).strip(),
        "topic_name": "",
        "rollup_target": str(sync_result.get("board_path", "")).strip(),
        "started_at": str(payload.get("created_at", "")).strip() or _utc_now(),
        "last_active_at": str(payload.get("updated_at", "")).strip() or _utc_now(),
        "thread_name": f"GFlow {str(payload.get('run_id', '')).strip()}",
        "prompt": str(payload.get("entry_prompt", "")).strip(),
    }
    summary_text = str(sync_result.get("summary_text", "")).strip()
    if summary_text:
        codex_memory.update_summary_note(project_name, binding, summary_text)
    changed_targets = [str(sync_result.get("board_path", "")).strip()]
    if summary_text:
        changed_targets.append("summary_note")
    changed_targets.append("next_actions")
    codex_memory.record_project_writeback(
        binding,
        source="gflow-runtime",
        changed_targets=[item for item in changed_targets if item],
        trigger_dashboard_sync=False,
    )
    codex_memory.trigger_retrieval_sync_once()
    codex_memory.trigger_dashboard_sync_once()


def fetch_workflow_run(run_id: str) -> dict[str, Any]:
    run_row = _fetch_run_row(run_id)
    if not run_row:
        raise ValueError(f"run not found: {run_id}")
    stage_rows = _fetch_stage_rows(run_id)
    return _payload_from_runtime_rows(run_row, stage_rows, status_label="gflow-run-status")


def list_workflow_runs(*, project_name: str = "", limit: int = 20) -> list[dict[str, Any]]:
    runtime_state.init_db()
    clauses: list[str] = []
    params: list[Any] = []
    normalized_project = str(project_name or "").strip()
    if normalized_project:
        clauses.append("project_name = ?")
        params.append(normalized_project)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with runtime_state.connect() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM gflow_runs
            {where_sql}
            ORDER BY {_sql_run_status_priority()} ASC, updated_at DESC, created_at DESC, run_id ASC
            LIMIT ?
            """,
            (*params, max(1, min(int(limit or 20), 200))),
        ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in runtime_state.rows_to_dicts(rows):
        stage_rows = _fetch_stage_rows(str(row.get("run_id", "")).strip())
        payloads.append(_payload_from_runtime_rows(row, stage_rows, status_label="gflow-run-status"))
    return _sort_run_payloads(payloads)[: max(1, min(int(limit or 20), 200))]


def start_workflow_run_from_prompt(
    prompt: str,
    *,
    project_name: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    payload = create_workflow_run_from_prompt(prompt)
    workflow_plan = dict(payload["workflow_plan"])
    workflow_plan["runtime_mode"] = "phase2-runtime"
    stage_rows = _stage_rows_for_runtime_start(workflow_plan)
    _insert_workflow_run(
        workflow_plan=workflow_plan,
        project_name=str(project_name or "").strip(),
        session_id=str(session_id or "").strip(),
        stage_rows=stage_rows,
    )
    result = fetch_workflow_run(workflow_plan["run_id"]) | {"status": "gflow-run-started"}
    _sync_phase3_writeback(result)
    return result


def start_explicit_workflow_run_if_requested(
    prompt: str,
    *,
    project_name: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    trigger = detect_gflow_trigger(prompt)
    if not trigger.get("matched"):
        return {"started": False, "reason": "not-gflow"}
    payload = start_workflow_run_from_prompt(
        prompt,
        project_name=project_name,
        session_id=session_id,
    )
    payload["started"] = True
    return payload


def _append_evidence(existing_json: Any, evidence: list[str] | None) -> str:
    existing = [
        str(item).strip()
        for item in _normalize_json_list(existing_json)
        if str(item).strip()
    ]
    for item in evidence or []:
        normalized = str(item).strip()
        if normalized and normalized not in existing:
            existing.append(normalized)
    return runtime_state.json_text(existing)


def pause_workflow_run(
    run_id: str,
    *,
    reason: str,
    gate_type: str = GATE_TYPE_USER,
    next_action: str = "",
    evidence: list[str] | None = None,
    freeze_scope: str = "",
) -> dict[str, Any]:
    normalized_reason = str(reason or "").strip()
    if not normalized_reason:
        raise ValueError("reason is required")
    normalized_gate_type = _validate_gate_type(gate_type or GATE_TYPE_USER)
    if normalized_gate_type == GATE_TYPE_NONE:
        normalized_gate_type = GATE_TYPE_USER
    next_status = {
        GATE_TYPE_USER: RUN_STATUS_PAUSED,
        GATE_TYPE_APPROVAL: RUN_STATUS_AWAITING_APPROVAL,
        GATE_TYPE_FREEZE: RUN_STATUS_FROZEN,
    }[normalized_gate_type]
    next_stage_status = {
        GATE_TYPE_USER: STAGE_STATUS_PAUSED,
        GATE_TYPE_APPROVAL: STAGE_STATUS_AWAITING_APPROVAL,
        GATE_TYPE_FREEZE: STAGE_STATUS_FROZEN,
    }[normalized_gate_type]

    run_row = _fetch_run_row(run_id)
    if not run_row:
        raise ValueError(f"run not found: {run_id}")
    current_stage_id = str(run_row.get("current_stage_id", "")).strip()
    if not current_stage_id:
        raise ValueError(f"run has no current stage: {run_id}")
    current_status = str(run_row.get("status", "")).strip()
    if current_status == RUN_STATUS_COMPLETED:
        raise ValueError("completed run cannot be paused")

    gate_token = ""
    if normalized_gate_type == GATE_TYPE_APPROVAL:
        gate_token = f"gflow-{uuid.uuid4().hex[:12]}"
        runtime_state.upsert_approval_token(
            token=gate_token,
            scope="gflow_run_gate",
            status="pending",
            project_name=str(run_row.get("project_name", "")).strip(),
            session_id=str(run_row.get("session_id", "")).strip(),
            metadata={
                "run_id": run_id,
                "stage_id": current_stage_id,
                "gate_type": normalized_gate_type,
                "gate_reason": normalized_reason,
            },
        )

    now = _utc_now()
    with runtime_state.transaction() as conn:
        current_stage_row = conn.execute(
            "SELECT * FROM gflow_stage_results WHERE run_id = ? AND stage_id = ?",
            (run_id, current_stage_id),
        ).fetchone()
        if current_stage_row is None:
            raise ValueError(f"stage not found: {current_stage_id}")
        handoff = _normalize_json_dict(current_stage_row["handoff_json"])
        handoff["gate"] = {
            "type": normalized_gate_type,
            "reason": normalized_reason,
            "token": gate_token,
            "freeze_scope": str(freeze_scope or "").strip(),
        }
        conn.execute(
            """
            UPDATE gflow_stage_results
            SET status = ?, summary = ?, next_action = ?, evidence_json = ?, handoff_json = ?, updated_at = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                next_stage_status,
                normalized_reason,
                str(next_action or "").strip(),
                _append_evidence(current_stage_row["evidence_json"], evidence),
                runtime_state.json_text(handoff),
                now,
                run_id,
                current_stage_id,
            ),
        )
        conn.execute(
            """
            UPDATE gflow_runs
            SET status = ?, gate_type = ?, gate_reason = ?, gate_token = ?, freeze_scope = ?,
                latest_summary = ?, latest_next_action = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (
                next_status,
                normalized_gate_type,
                normalized_reason,
                gate_token,
                str(freeze_scope or "").strip(),
                f"`{run_row.get('current_stage_skill', '')}` 阶段已进入 gate：{normalized_reason}",
                str(next_action or "").strip(),
                now,
                run_id,
            ),
        )
    runtime_state.enqueue_runtime_event(
        queue_name="gflow_run_log",
        event_type="gflow_run_paused",
        payload={
            "run_id": run_id,
            "gate_type": normalized_gate_type,
            "reason": normalized_reason,
            "gate_token": gate_token,
        },
        dedupe_key=f"{run_id}:paused:{normalized_gate_type}:{normalized_reason}",
        status="completed",
    )
    result = fetch_workflow_run(run_id) | {"status": "gflow-run-paused"}
    _sync_phase3_writeback(result)
    return result


def resume_workflow_run(
    run_id: str,
    *,
    note: str = "",
    approval_token: str = "",
    evidence: list[str] | None = None,
    allow_auto_execute: bool = True,
) -> dict[str, Any]:
    run_row = _fetch_run_row(run_id)
    if not run_row:
        raise ValueError(f"run not found: {run_id}")
    current_status = str(run_row.get("status", "")).strip()
    if current_status not in {RUN_STATUS_PAUSED, RUN_STATUS_AWAITING_APPROVAL, RUN_STATUS_FROZEN}:
        raise ValueError("run is not paused and cannot be resumed")
    current_stage_id = str(run_row.get("current_stage_id", "")).strip()
    if not current_stage_id:
        raise ValueError(f"run has no current stage: {run_id}")
    gate_type = str(run_row.get("gate_type", "")).strip()
    gate_token = str(run_row.get("gate_token", "")).strip()
    if current_status == RUN_STATUS_AWAITING_APPROVAL:
        normalized_token = str(approval_token or "").strip()
        if not normalized_token:
            raise ValueError("approval_token is required to resume awaiting_approval run")
        if gate_token and normalized_token != gate_token:
            raise ValueError("approval_token does not match current gate token")
        item = runtime_state.fetch_approval_token(normalized_token)
        if str(item.get("status", "")).strip() != "approved":
            raise ValueError("approval_token is not approved")

    now = _utc_now()
    note_text = str(note or "").strip() or "恢复执行。"
    with runtime_state.transaction() as conn:
        current_stage_row = conn.execute(
            "SELECT * FROM gflow_stage_results WHERE run_id = ? AND stage_id = ?",
            (run_id, current_stage_id),
        ).fetchone()
        if current_stage_row is None:
            raise ValueError(f"stage not found: {current_stage_id}")
        handoff = _normalize_json_dict(current_stage_row["handoff_json"])
        handoff["resume"] = {
            "note": note_text,
            "from_gate_type": gate_type,
            "approval_token": str(approval_token or "").strip(),
            "resumed_at": now,
        }
        conn.execute(
            """
            UPDATE gflow_stage_results
            SET status = ?, summary = ?, next_action = ?, evidence_json = ?, handoff_json = ?, updated_at = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                STAGE_STATUS_RUNNING,
                note_text,
                "继续当前阶段执行。",
                _append_evidence(current_stage_row["evidence_json"], evidence),
                runtime_state.json_text(handoff),
                now,
                run_id,
                current_stage_id,
            ),
        )
        conn.execute(
            """
            UPDATE gflow_runs
            SET status = ?, gate_type = '', gate_reason = '', gate_token = '', freeze_scope = '',
                latest_summary = ?, latest_next_action = ?, updated_at = ?
            WHERE run_id = ?
            """,
            (
                RUN_STATUS_RUNNING,
                f"`{run_row.get('current_stage_skill', '')}` 已恢复执行。",
                "继续当前阶段。",
                now,
                run_id,
            ),
        )
    runtime_state.enqueue_runtime_event(
        queue_name="gflow_run_log",
        event_type="gflow_run_resumed",
        payload={
            "run_id": run_id,
            "from_status": current_status,
            "approval_token": str(approval_token or "").strip(),
        },
        dedupe_key=f"{run_id}:resumed:{now}",
        status="completed",
    )
    result = fetch_workflow_run(run_id) | {"status": "gflow-run-resumed"}
    _sync_phase3_writeback(result)
    if allow_auto_execute:
        auto_result = _maybe_auto_continue_run(run_id)
        if auto_result is not None:
            return auto_result
    return result


def advance_workflow_run(
    run_id: str,
    *,
    summary: str,
    next_action: str = "",
    evidence: list[str] | None = None,
    allow_auto_execute: bool = True,
) -> dict[str, Any]:
    normalized_summary = str(summary or "").strip()
    if not normalized_summary:
        raise ValueError("summary is required")
    run_row = _fetch_run_row(run_id)
    if not run_row:
        raise ValueError(f"run not found: {run_id}")
    if str(run_row.get("status", "")).strip() != RUN_STATUS_RUNNING:
        raise ValueError("run must be running before advancing")
    stage_rows = _fetch_stage_rows(run_id)
    workflow_plan = _normalize_json_dict(run_row.get("workflow_plan_json", "{}"))
    stages = list(workflow_plan.get("stages", []))
    current_stage_id = str(run_row.get("current_stage_id", "")).strip()
    current_stage = _stage_descriptor(stages, current_stage_id)
    if current_stage is None:
        raise ValueError(f"current stage descriptor not found: {current_stage_id}")
    next_stage = _next_stage_descriptor(stages, current_stage_id)
    now = _utc_now()

    with runtime_state.transaction() as conn:
        current_stage_row = conn.execute(
            "SELECT * FROM gflow_stage_results WHERE run_id = ? AND stage_id = ?",
            (run_id, current_stage_id),
        ).fetchone()
        if current_stage_row is None:
            raise ValueError(f"stage not found: {current_stage_id}")
        current_handoff = _normalize_json_dict(current_stage_row["handoff_json"])
        if next_stage is not None:
            current_handoff["next_stage"] = {
                "stage_id": next_stage["stage_id"],
                "skill": next_stage["skill"],
                "handoff_summary": normalized_summary,
            }
        conn.execute(
            """
            UPDATE gflow_stage_results
            SET status = ?, summary = ?, next_action = ?, evidence_json = ?, handoff_json = ?, updated_at = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                STAGE_STATUS_COMPLETED,
                normalized_summary,
                str(next_action or "").strip(),
                _append_evidence(current_stage_row["evidence_json"], evidence),
                runtime_state.json_text(current_handoff),
                now,
                run_id,
                current_stage_id,
            ),
        )
        if next_stage is not None:
            next_stage_row = conn.execute(
                "SELECT * FROM gflow_stage_results WHERE run_id = ? AND stage_id = ?",
                (run_id, next_stage["stage_id"]),
            ).fetchone()
            if next_stage_row is None:
                raise ValueError(f"next stage not found: {next_stage['stage_id']}")
            next_stage_handoff = _normalize_json_dict(next_stage_row["handoff_json"])
            next_stage_contract = _template_stage_contract(workflow_plan, str(next_stage.get("skill", "")).strip())
            next_stage_handoff["previous_stage"] = {
                "stage_id": current_stage_id,
                "skill": current_stage["skill"],
                "summary": normalized_summary,
            }
            if next_stage_contract:
                next_stage_handoff["template_contract"] = next_stage_contract
            next_stage_summary = f"已接手来自 `{current_stage['skill']}` 的 handoff。"
            next_stage_action = f"继续 `{next_stage['skill']}`。"
            deliverable = str(next_stage_contract.get("deliverable", "")).strip()
            gate_rule = str(next_stage_contract.get("gate_rule", "")).strip()
            if deliverable:
                next_stage_summary = f"{next_stage_summary} 当前交付目标：{deliverable}"
            if gate_rule:
                next_stage_action = f"{next_stage_action} Gate 规则：{gate_rule}"
            conn.execute(
                """
                UPDATE gflow_stage_results
                SET status = ?, summary = ?, next_action = ?, handoff_json = ?, updated_at = ?
                WHERE run_id = ? AND stage_id = ?
                """,
                (
                    STAGE_STATUS_RUNNING,
                    next_stage_summary,
                    next_stage_action,
                    runtime_state.json_text(next_stage_handoff),
                    now,
                    run_id,
                    next_stage["stage_id"],
                ),
            )
            conn.execute(
                """
                UPDATE gflow_runs
                SET current_stage_id = ?, current_stage_skill = ?, latest_summary = ?, latest_next_action = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    next_stage["stage_id"],
                    next_stage["skill"],
                    f"`{current_stage['skill']}` 已完成，已进入 `{next_stage['skill']}`。",
                    next_stage_action,
                    now,
                    run_id,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE gflow_runs
                SET status = ?, latest_summary = ?, latest_next_action = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    RUN_STATUS_COMPLETED,
                    "GFlow 显式流程记录已完成全部阶段。",
                    "已完成，无需继续。",
                    now,
                    run_id,
                ),
            )
    runtime_state.enqueue_runtime_event(
        queue_name="gflow_run_log",
        event_type="gflow_stage_advanced",
        payload={
            "run_id": run_id,
            "from_stage": current_stage["skill"],
            "to_stage": str(next_stage.get("skill", "")).strip() if next_stage else "",
        },
        dedupe_key=f"{run_id}:advanced:{current_stage_id}:{now}",
        status="completed",
    )
    result_status = "gflow-run-completed" if next_stage is None else "gflow-stage-advanced"
    result = fetch_workflow_run(run_id) | {"status": result_status}
    _sync_phase3_writeback(result)
    if allow_auto_execute and next_stage is not None:
        auto_result = _maybe_auto_continue_run(run_id)
        if auto_result is not None:
            return auto_result
    return result


def cmd_plan(args: argparse.Namespace) -> int:
    try:
        payload = create_workflow_run_from_prompt(args.prompt)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    output = payload["workflow_plan"] if args.json else json.dumps(payload["workflow_plan"], ensure_ascii=False)
    print(output if isinstance(output, str) else json.dumps(output, ensure_ascii=False))
    return 0


def cmd_from_prompt(args: argparse.Namespace) -> int:
    try:
        payload = create_workflow_run_from_prompt(args.prompt)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    try:
        payload = start_workflow_run_from_prompt(
            args.prompt,
            project_name=args.project_name,
            session_id=args.session_id,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    try:
        payload = fetch_workflow_run(args.run_id)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_pause(args: argparse.Namespace) -> int:
    try:
        payload = pause_workflow_run(
            args.run_id,
            reason=args.reason,
            gate_type=args.gate_type,
            next_action=args.next_action,
            evidence=args.evidence,
            freeze_scope=args.freeze_scope,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    try:
        payload = resume_workflow_run(
            args.run_id,
            note=args.note,
            approval_token=args.approval_token,
            evidence=args.evidence,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_advance(args: argparse.Namespace) -> int:
    try:
        payload = advance_workflow_run(
            args.run_id,
            summary=args.summary,
            next_action=args.next_action,
            evidence=args.evidence,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print(payload["main_thread_handoff"])
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    payload = list_workflow_runs(project_name=args.project_name, limit=args.limit)
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex Hub GFlow explicit-mode helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan")
    plan.add_argument("--prompt", required=True)
    plan.add_argument("--json", action="store_true")
    plan.set_defaults(func=cmd_plan)

    from_prompt = subparsers.add_parser("from-prompt")
    from_prompt.add_argument("--prompt", required=True)
    from_prompt.add_argument("--json", action="store_true")
    from_prompt.set_defaults(func=cmd_from_prompt)

    start = subparsers.add_parser("start")
    start.add_argument("--prompt", required=True)
    start.add_argument("--project-name", default="")
    start.add_argument("--session-id", default="")
    start.add_argument("--json", action="store_true")
    start.set_defaults(func=cmd_start)

    status = subparsers.add_parser("status")
    status.add_argument("--run-id", required=True)
    status.add_argument("--json", action="store_true")
    status.set_defaults(func=cmd_status)

    pause = subparsers.add_parser("pause")
    pause.add_argument("--run-id", required=True)
    pause.add_argument("--reason", required=True)
    pause.add_argument("--gate-type", choices=["user", "approval", "freeze"], default="user")
    pause.add_argument("--next-action", default="")
    pause.add_argument("--freeze-scope", default="")
    pause.add_argument("--evidence", action="append", default=[])
    pause.add_argument("--json", action="store_true")
    pause.set_defaults(func=cmd_pause)

    resume = subparsers.add_parser("resume")
    resume.add_argument("--run-id", required=True)
    resume.add_argument("--note", default="")
    resume.add_argument("--approval-token", default="")
    resume.add_argument("--evidence", action="append", default=[])
    resume.add_argument("--json", action="store_true")
    resume.set_defaults(func=cmd_resume)

    advance = subparsers.add_parser("advance")
    advance.add_argument("--run-id", required=True)
    advance.add_argument("--summary", required=True)
    advance.add_argument("--next-action", default="")
    advance.add_argument("--evidence", action="append", default=[])
    advance.add_argument("--json", action="store_true")
    advance.set_defaults(func=cmd_advance)

    run_list = subparsers.add_parser("list")
    run_list.add_argument("--project-name", default="")
    run_list.add_argument("--limit", type=int, default=20)
    run_list.set_defaults(func=cmd_list)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

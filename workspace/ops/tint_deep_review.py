#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
FRONTEND_REPO = WORKSPACE_ROOT / "projects" / "TINT" / "TINT前端(web)"
BACKEND_REPO = WORKSPACE_ROOT / "projects" / "TINT" / "TINT服务器"
STATE_PATH = WORKSPACE_ROOT / "runtime" / "tint-deep-review-state.json"
REPORT_ROOT = WORKSPACE_ROOT / "reports" / "projects" / "TINT" / "code-review"
FRONTEND_REPORT_DIR = REPORT_ROOT / "frontend"
BACKEND_REPORT_DIR = REPORT_ROOT / "backend"
SUMMARY_REPORT_DIR = REPORT_ROOT / "summary"
INDEX_DIR = REPORT_ROOT / "indexes"
MANIFEST_PATH = INDEX_DIR / "manifest.json"

FRONTEND_APP_ROUTER = FRONTEND_REPO / "src" / "AppRouter.tsx"
FRONTEND_RESULT_SUMMARY = FRONTEND_REPO / "src" / "pages" / "ResultSummary.tsx"
FRONTEND_QUESTION_DETAIL = FRONTEND_REPO / "src" / "pages" / "QuestionDetail.tsx"
FRONTEND_REVIEW_FLOW = FRONTEND_REPO / "src" / "pages" / "ReviewFlow.tsx"
FRONTEND_QUESTION_TEXT = FRONTEND_REPO / "src" / "components" / "ui" / "QuestionText.tsx"
FRONTEND_TYPES = FRONTEND_REPO / "src" / "services" / "types.ts"
FRONTEND_SUBMISSION_TEST = FRONTEND_REPO / "src" / "__tests__" / "pages" / "submissionPages.test.tsx"
FRONTEND_QUESTION_TEXT_TEST = FRONTEND_REPO / "src" / "components" / "ui" / "QuestionText.test.tsx"
FRONTEND_API = FRONTEND_REPO / "src" / "services" / "api.ts"

BACKEND_ROUTES = BACKEND_REPO / "homework_agent" / "api" / "routes.py"
BACKEND_MISTAKES_API = BACKEND_REPO / "homework_agent" / "api" / "mistakes.py"
BACKEND_SUBMISSIONS_API = BACKEND_REPO / "homework_agent" / "api" / "submissions.py"
BACKEND_USER_OVERRIDES = BACKEND_REPO / "homework_agent" / "core" / "user_overrides.py"
BACKEND_MISTAKES_SERVICE = BACKEND_REPO / "homework_agent" / "services" / "mistakes_service.py"
BACKEND_SUBMISSIONS_TEST = BACKEND_REPO / "homework_agent" / "tests" / "test_submissions_api.py"
BACKEND_MISTAKES_TEST = BACKEND_REPO / "homework_agent" / "tests" / "test_mistakes_api.py"

SIDE_CONFIG: dict[str, dict[str, Any]] = {
    "frontend": {
        "repo": FRONTEND_REPO,
        "report_dir": FRONTEND_REPORT_DIR,
        "index_path": INDEX_DIR / "frontend-index.md",
        "archive_prefix": "frontend-review",
        "index_prefix": "frontend-index",
    },
    "backend": {
        "repo": BACKEND_REPO,
        "report_dir": BACKEND_REPORT_DIR,
        "index_path": INDEX_DIR / "backend-index.md",
        "archive_prefix": "backend-review",
        "index_prefix": "backend-index",
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def stamp_now() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def line_number_contains(path: Path, needle: str) -> int | None:
    try:
        for index, line in enumerate(read_text(path).splitlines(), start=1):
            if needle in line:
                return index
    except Exception:
        return None
    return None


def line_number_regex(path: Path, pattern: str) -> int | None:
    try:
        compiled = re.compile(pattern)
        for index, line in enumerate(read_text(path).splitlines(), start=1):
            if compiled.search(line):
                return index
    except Exception:
        return None
    return None


def rel_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(WORKSPACE_ROOT))
    except Exception:
        return str(path.resolve())


def path_ref(path: Path, line: int | None = None) -> str:
    abs_path = str(path.resolve())
    return f"`{abs_path}:{line}`" if line else f"`{abs_path}`"


def run_command(args: list[str], *, cwd: Path) -> dict[str, Any]:
    started = time.time()
    try:
        completed = subprocess.run(
            args,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
        )
        exit_code = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    except FileNotFoundError as exc:
        exit_code = 127
        stdout = ""
        stderr = str(exc)
    duration = round(time.time() - started, 3)
    return {
        "args": args,
        "cwd": str(cwd),
        "exit_code": exit_code,
        "ok": exit_code == 0,
        "stdout": stdout,
        "stderr": stderr,
        "duration_sec": duration,
    }


def summarize_output(value: str, *, limit: int = 2400) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    keep = max(200, limit // 2)
    return f"{text[:keep]}\n...\n{text[-keep:]}"


def git_output(repo: Path, *args: str) -> str:
    result = run_command(["git", *args], cwd=repo)
    if result["exit_code"] != 0:
        raise RuntimeError(result["stderr"] or result["stdout"] or f"git {' '.join(args)} failed")
    return str(result["stdout"]).strip()


def git_lines(repo: Path, *args: str) -> list[str]:
    output = git_output(repo, *args)
    return [line for line in output.splitlines() if line.strip()]


def git_head(repo: Path) -> str:
    return git_output(repo, "rev-parse", "HEAD")


def git_commit_window(repo: Path, limit: int = 15) -> list[dict[str, str]]:
    lines = git_lines(repo, "log", f"-{limit}", "--date=short", "--pretty=format:%H%x09%h%x09%cs%x09%s")
    commits: list[dict[str, str]] = []
    for line in lines:
        parts = line.split("\t", 3)
        if len(parts) != 4:
            continue
        full_hash, short_hash, commit_date, subject = parts
        commits.append(
            {
                "hash": full_hash,
                "short": short_hash,
                "date": commit_date,
                "subject": subject,
            }
        )
    return commits


def git_diff_range_for_window(repo: Path, limit: int = 15) -> str | None:
    hashes = git_lines(repo, "rev-list", "--max-count", str(limit), "HEAD")
    if not hashes:
        return None
    oldest = hashes[-1]
    parent = run_command(["git", "rev-parse", f"{oldest}^"], cwd=repo)
    if parent["exit_code"] == 0:
        return f"{str(parent['stdout']).strip()}..HEAD"
    return f"{oldest}..HEAD"


def git_diff_hotspots(repo: Path, limit: int = 15, top_n: int = 8) -> list[dict[str, Any]]:
    diff_range = git_diff_range_for_window(repo, limit=limit)
    if not diff_range:
        return []
    result = run_command(["git", "diff", "--numstat", diff_range], cwd=repo)
    if result["exit_code"] != 0:
        return []
    hotspots: list[dict[str, Any]] = []
    for line in str(result["stdout"]).splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        added_raw, removed_raw, path = parts
        try:
            added = int(added_raw)
        except Exception:
            added = 0
        try:
            removed = int(removed_raw)
        except Exception:
            removed = 0
        hotspots.append(
            {
                "path": path,
                "added": added,
                "removed": removed,
                "total": added + removed,
            }
        )
    hotspots.sort(key=lambda item: (-int(item["total"]), str(item["path"])))
    return hotspots[:top_n]


def infer_themes(commits: list[dict[str, str]], *, side: str) -> list[str]:
    subjects = [str(item.get("subject") or "").lower() for item in commits]
    themes: list[str] = []
    if side == "frontend":
        if any(token in subject for subject in subjects for token in ("brand", "logo", "favicon", "title")):
            themes.append("品牌统一与入口文案收口")
        if any(token in subject for subject in subjects for token in ("summary", "review-flow", "question-detail", "submission", "exclude")):
            themes.append("结果页、题目详情和复习流围绕排除题/手动判定做一致性修正")
        if any(token in subject for subject in subjects for token in ("deduplicate", "questiontext", "option", "handwritten", "ocr")):
            themes.append("OCR 题干/选项渲染去噪与重复内容折叠")
        if any(token in subject for subject in subjects for token in ("build", "ci", "artifact")):
            themes.append("CI 与前端构建稳定性修补")
    else:
        if any(token in subject for subject in subjects for token in ("override", "verdict", "exclusion", "regrade")):
            themes.append("submission / mistakes / user overrides 真源统一")
        if any(token in subject for subject in subjects for token in ("qbank", "question", "pollution", "dedupe", "full details")):
            themes.append("题目明细恢复、OCR 污染清理与历史回填")
        if any(token in subject for subject in subjects for token in ("layout", "exif", "slice", "visual")):
            themes.append("视觉题切片与图像布局稳定性修正")
        if any(token in subject for subject in subjects for token in ("deploy", "ci", "artifact")):
            themes.append("部署链路与 CI 配额/依赖稳定性处理")
    return themes or ["近期提交分散，未形成单一主线"]


def extract_frontend_routes(path: Path) -> list[dict[str, str]]:
    pattern = re.compile(r'<Route path="([^"]+)" element={<RouteTransition><([A-Za-z0-9_]+) />')
    routes: list[dict[str, str]] = []
    for line in read_text(path).splitlines():
        matched = pattern.search(line)
        if not matched:
            continue
        route_path, component = matched.groups()
        routes.append({"path": route_path, "component": component})
    return routes


def collect_api_usage(root: Path) -> list[dict[str, Any]]:
    call_pattern = re.compile(
        r"apiClient\.(get|post|put|patch|delete)(?:<[^>]+>)?\(\s*([`'\"])(.+?)\2",
        re.DOTALL,
    )
    usages: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*.ts*")):
        if "node_modules" in path.parts or "dist" in path.parts:
            continue
        text = read_text(path)
        entries: list[dict[str, str]] = []
        for matched in call_pattern.finditer(text):
            method, _, endpoint = matched.groups()
            entries.append(
                {
                    "method": method.upper(),
                    "endpoint": " ".join(str(endpoint).split()),
                }
            )
        if not entries:
            continue
        usages.append(
            {
                "path": rel_path(path),
                "entries": entries,
            }
        )
    return usages


def list_test_files(root: Path) -> list[str]:
    files = []
    for path in sorted(root.rglob("*test.ts*")):
        if "node_modules" in path.parts or "dist" in path.parts:
            continue
        files.append(rel_path(path))
    return files


def find_frontend_context_files() -> list[str]:
    candidates = [
        FRONTEND_API,
        FRONTEND_RESULT_SUMMARY,
        FRONTEND_QUESTION_DETAIL,
        FRONTEND_REVIEW_FLOW,
        FRONTEND_REPO / "src" / "pages" / "History.tsx",
        FRONTEND_REPO / "src" / "contexts" / "ProfileContext.tsx",
    ]
    out: list[str] = []
    for path in candidates:
        if path.exists():
            out.append(rel_path(path))
    return out


def build_frontend_index() -> tuple[str, list[dict[str, Any]]]:
    routes = extract_frontend_routes(FRONTEND_APP_ROUTER)
    api_usage = collect_api_usage(FRONTEND_REPO / "src")
    tests = list_test_files(FRONTEND_REPO / "src")
    hotspots = [rel_path(path) for path in [FRONTEND_RESULT_SUMMARY, FRONTEND_QUESTION_DETAIL, FRONTEND_REVIEW_FLOW, FRONTEND_QUESTION_TEXT]]

    manifest: list[dict[str, Any]] = [
        {
            "side": "frontend",
            "module_name": "router",
            "kind": "route-map",
            "path": rel_path(FRONTEND_APP_ROUTER),
            "entry_points": [item["path"] for item in routes],
            "keywords": ["routing", "pages", "navigation"],
            "dependencies": ["react-router-dom", "framer-motion"],
            "risk_tags": ["entrypoint"],
        }
    ]

    for usage in api_usage:
        manifest.append(
            {
                "side": "frontend",
                "module_name": Path(str(usage["path"])).stem,
                "kind": "api-consumer",
                "path": usage["path"],
                "entry_points": [f"{entry['method']} {entry['endpoint']}" for entry in usage["entries"]],
                "keywords": ["apiClient", "submission", "mistakes", "profile"],
                "dependencies": ["src/services/api.ts"],
                "risk_tags": ["network"],
            }
        )

    lines = [
        "# TINT Frontend Code Index",
        "",
        "## 路由入口",
    ]
    for item in routes:
        lines.append(f"- `{item['path']}` -> `{item['component']}`")

    lines.extend(["", "## API 消费入口"])
    for usage in api_usage[:16]:
        calls = ", ".join(f"{entry['method']} {entry['endpoint']}" for entry in usage["entries"][:4])
        lines.append(f"- `{usage['path']}` | {calls}")

    lines.extend(
        [
            "",
            "## Profile / Auth 相关链路",
            *[f"- `{path}`" for path in find_frontend_context_files()],
            "",
            "## 风险热点",
            *[f"- `{path}`" for path in hotspots],
            "",
            "## 测试入口",
            *[f"- `{path}`" for path in tests],
        ]
    )
    return "\n".join(lines) + "\n", manifest


def parse_backend_route_modules() -> list[str]:
    text = read_text(BACKEND_ROUTES)
    pattern = re.compile(r"from homework_agent\.api import ([a-zA-Z0-9_]+) as ([a-zA-Z0-9_]+)_api")
    modules = []
    for matched in pattern.finditer(text):
        modules.append(matched.group(1))
    return modules


def parse_api_endpoints(path: Path) -> list[str]:
    pattern = re.compile(r'@router\.(get|post|put|patch|delete)\("([^"]+)"')
    endpoints: list[str] = []
    for line in read_text(path).splitlines():
        matched = pattern.search(line)
        if not matched:
            continue
        method, route = matched.groups()
        endpoints.append(f"{method.upper()} {route}")
    return endpoints


def list_backend_tests() -> list[str]:
    tests_dir = BACKEND_REPO / "homework_agent" / "tests"
    tests = []
    for path in sorted(tests_dir.glob("test_*.py")):
        tests.append(rel_path(path))
    return tests


def build_backend_index() -> tuple[str, list[dict[str, Any]]]:
    modules = parse_backend_route_modules()
    manifest: list[dict[str, Any]] = []
    lines = [
        "# TINT Backend Code Index",
        "",
        "## FastAPI 路由聚合",
    ]
    for module in modules:
        module_path = BACKEND_REPO / "homework_agent" / "api" / f"{module}.py"
        endpoints = parse_api_endpoints(module_path) if module_path.exists() else []
        lines.append(f"- `{rel_path(module_path)}` | {', '.join(endpoints[:6]) if endpoints else 'no direct endpoints parsed'}")
        manifest.append(
            {
                "side": "backend",
                "module_name": module,
                "kind": "api-module",
                "path": rel_path(module_path),
                "entry_points": endpoints,
                "keywords": ["fastapi", "router", module],
                "dependencies": ["homework_agent/api/routes.py"],
                "risk_tags": ["api-surface"],
            }
        )

    hotspot_paths = [
        BACKEND_MISTAKES_API,
        BACKEND_SUBMISSIONS_API,
        BACKEND_USER_OVERRIDES,
        BACKEND_MISTAKES_SERVICE,
        BACKEND_REPO / "homework_agent" / "workers" / "grade_worker.py",
        BACKEND_REPO / "homework_agent" / "utils" / "submission_store.py",
    ]
    lines.extend(
        [
            "",
            "## 状态真源热点",
            *[f"- `{rel_path(path)}`" for path in hotspot_paths if path.exists()],
            "",
            "## Service / Worker 关系",
            f"- `submission -> {rel_path(BACKEND_SUBMISSIONS_API)} -> {rel_path(BACKEND_MISTAKES_SERVICE)} -> homework_agent/workers/grade_worker.py`",
            f"- `mistake exclusion -> {rel_path(BACKEND_MISTAKES_API)} -> {rel_path(BACKEND_MISTAKES_SERVICE)} -> grade_result.user_overrides`",
            "",
            "## 测试入口",
            *[f"- `{path}`" for path in list_backend_tests()],
        ]
    )
    return "\n".join(lines) + "\n", manifest


def make_location(path: Path, line: int | None) -> dict[str, Any]:
    return {"path": str(path.resolve()), "line": line}


def frontend_findings() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    findings: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    coverage_gaps: list[str] = []

    if (
        "blank_count" in read_text(FRONTEND_TYPES)
        and "const correct = Math.max(0, totalItems - wrong - uncertain);" in read_text(FRONTEND_RESULT_SUMMARY)
    ):
        findings.append(
            {
                "severity": "P1",
                "title": "空题被结果页计入正确数和正确率",
                "impact": "只要 summary 含有 blank_count，结果页顶部百分比和“正确”数量都会被抬高，用户会看到比实际更好的批改结果。",
                "evidence": "前端类型保留了 blank_count，但结果页正确数公式仍是 total - wrong - uncertain，没有把 blank_count 扣除。",
                "locations": [
                    make_location(FRONTEND_RESULT_SUMMARY, line_number_contains(FRONTEND_RESULT_SUMMARY, "const correct = Math.max(0, totalItems - wrong - uncertain);")),
                    make_location(FRONTEND_RESULT_SUMMARY, line_number_contains(FRONTEND_RESULT_SUMMARY, "Math.round((counts.correct / Math.max(1, counts.total)) * 100)")),
                    make_location(FRONTEND_TYPES, line_number_contains(FRONTEND_TYPES, "blank_count?: number;")),
                ],
            }
        )

    if "apiClient.get<SubmissionDetail>(`/submissions/${sid}`);" in read_text(FRONTEND_RESULT_SUMMARY) and "apiClient.get<SubmissionDetail>(`/submissions/${submissionId}`);" in read_text(FRONTEND_QUESTION_DETAIL):
        findings.append(
            {
                "severity": "P1",
                "title": "多孩子场景下提交详情首刷仍可能打错 profile 头",
                "impact": "用户切换到其他孩子后从历史/深链进入结果页或题目详情，首刷请求仍可能命中当前 active profile，直接出现 submission not found。",
                "evidence": "ResultSummary 与 QuestionDetail 的第一次 /submissions/:id 请求都没带 headers，只有后续 refetch 才使用 submission.profile_id 纠偏。",
                "locations": [
                    make_location(FRONTEND_RESULT_SUMMARY, line_number_contains(FRONTEND_RESULT_SUMMARY, "apiClient.get<SubmissionDetail>(`/submissions/${sid}`);")),
                    make_location(FRONTEND_QUESTION_DETAIL, line_number_contains(FRONTEND_QUESTION_DETAIL, "apiClient.get<SubmissionDetail>(`/submissions/${submissionId}`);")),
                    make_location(FRONTEND_QUESTION_DETAIL, line_number_contains(FRONTEND_QUESTION_DETAIL, "Using submission.profile_id avoids \"submission not found\"")),
                ],
            }
        )

    if "localStorage.removeItem(key);" in read_text(FRONTEND_RESULT_SUMMARY):
        findings.append(
            {
                "severity": "P2",
                "title": "dirty refresh 标记在刷新失败时也会被清掉",
                "impact": "QuestionDetail / ReviewFlow 写入的 submission_dirty 标记会在 ResultSummary 刷新失败时丢失，用户只能靠硬刷新才能再拉到最新统计。",
                "evidence": "refreshIfDirty 在 finally 中无条件移除 submission_dirty_${sid}，没有区分拉取成功还是失败。",
                "locations": [
                    make_location(FRONTEND_RESULT_SUMMARY, line_number_contains(FRONTEND_RESULT_SUMMARY, "localStorage.removeItem(key);")),
                    make_location(FRONTEND_QUESTION_DETAIL, line_number_contains(FRONTEND_QUESTION_DETAIL, "localStorage.setItem(`submission_dirty_${sid}`")),
                    make_location(FRONTEND_REVIEW_FLOW, line_number_contains(FRONTEND_REVIEW_FLOW, "localStorage.setItem(`submission_dirty_${item.submission_id}`")),
                ],
            }
        )

    if "await apiClient.post(" in read_text(FRONTEND_QUESTION_DETAIL) and "const updated = await refetchSubmission();" in read_text(FRONTEND_QUESTION_DETAIL):
        observations.append(
            {
                "title": "错题排除成功后若刷新失败，前端仍会统一报操作失败",
                "detail": "handleExclude 把 POST /mistakes/exclusions 和 refetchSubmission 放在同一个 try 中，后续 GET 失败时会覆盖已经成功的排除动作，用户侧会得到误导性失败提示。",
                "locations": [
                    make_location(FRONTEND_QUESTION_DETAIL, line_number_contains(FRONTEND_QUESTION_DETAIL, "await apiClient.post(")),
                    make_location(FRONTEND_QUESTION_DETAIL, line_number_contains(FRONTEND_QUESTION_DETAIL, "const updated = await refetchSubmission();")),
                ],
            }
        )

    if "i += cycleLen * 2;" in read_text(FRONTEND_QUESTION_TEXT) and "three" not in read_text(FRONTEND_QUESTION_TEXT_TEST).lower():
        observations.append(
            {
                "title": "QuestionText 的重复选项折叠仍偏向双循环场景",
                "detail": "当前 dedupe 算法一次只吞掉一对重复 cycle，三连重复 OCR 选项仍可能漏掉最后一轮；现有测试只覆盖双重复和手写噪声。",
                "locations": [
                    make_location(FRONTEND_QUESTION_TEXT, line_number_contains(FRONTEND_QUESTION_TEXT, "i += cycleLen * 2;")),
                    make_location(FRONTEND_QUESTION_TEXT_TEST, 1),
                ],
            }
        )

    submission_test_text = read_text(FRONTEND_SUBMISSION_TEST)
    if "blank_count: 0" in submission_test_text and "blank_count: 1" not in submission_test_text:
        coverage_gaps.append(
            "结果页测试只覆盖 `blank_count: 0` 的 summary，没有覆盖空题进入统计后的正确率和正确数。"
        )
    if "{ headers: { 'X-Profile-Id': 'p1' } }" in submission_test_text and "submission not found" not in submission_test_text:
        coverage_gaps.append(
            "题目详情测试只验证了后续 refresh 使用 `X-Profile-Id`，没有覆盖首刷请求在多孩子场景下的 profile 归属。"
        )
    if "QuestionText" in read_text(FRONTEND_QUESTION_TEXT_TEST) and "three identical cycles" not in read_text(FRONTEND_QUESTION_TEXT_TEST):
        coverage_gaps.append(
            "QuestionText 测试没有覆盖三连重复选项 cycle，OCR 极端噪声仍缺回归用例。"
        )
    return findings, observations, coverage_gaps


def backend_findings() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    findings: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    coverage_gaps: list[str] = []

    overrides_text = read_text(BACKEND_USER_OVERRIDES)
    if "resolve_visible_item_id(normalized_item_id, [existing_item_id])" in overrides_text:
        findings.append(
            {
                "severity": "P1",
                "title": "user_overrides 清理逻辑会误删重复题号的兄弟 override",
                "impact": "当同一 submission 里存在重复题号或 occurrence 后缀时，set/clear 可能借由单候选 token 匹配把另一题的 override 一起删掉，导致状态串题。",
                "evidence": "set_user_override / clear_user_override 在循环中用单个 existing_item_id 调用 resolve_visible_item_id，会把 token 相同的另一 occurrence 也视作匹配目标。",
                "locations": [
                    make_location(BACKEND_USER_OVERRIDES, line_number_contains(BACKEND_USER_OVERRIDES, "resolve_visible_item_id(normalized_item_id, [existing_item_id])")),
                    make_location(BACKEND_USER_OVERRIDES, line_number_contains(BACKEND_USER_OVERRIDES, "for existing_item_id, payload in overrides.items():")),
                ],
            }
        )

    mistakes_api_text = read_text(BACKEND_MISTAKES_API)
    if "exclude_mistake(" in mistakes_api_text and "_sync_submission_user_override(" in mistakes_api_text:
        findings.append(
            {
                "severity": "P1",
                "title": "mistake exclusion 是两次独立写入，失败时会留下部分提交状态",
                "impact": "exclusion row 与 grade_result.user_overrides 不是原子提交，第二次写入失败时，接口会返回 503，但用户真实状态已经半更新，前台展示可能继续不一致。",
                "evidence": "post_mistake_exclusion / delete_mistake_exclusion 先写 mistake_exclusions，再单独调用 _sync_submission_user_override 同步 grade_result。",
                "locations": [
                    make_location(BACKEND_MISTAKES_API, line_number_contains(BACKEND_MISTAKES_API, "exclude_mistake(")),
                    make_location(BACKEND_MISTAKES_API, line_number_contains(BACKEND_MISTAKES_API, "_sync_submission_user_override(")),
                    make_location(BACKEND_MISTAKES_API, line_number_contains(BACKEND_MISTAKES_API, "failed to sync exclusion override")),
                ],
            }
        )

    submissions_text = read_text(BACKEND_SUBMISSIONS_API)
    if "\"grade_result\": grade_result" in submissions_text and "restore_mistake(" in submissions_text:
        findings.append(
            {
                "severity": "P2",
                "title": "手动改判先落 grade_result 再清 exclusion，仍存在部分提交窗口",
                "impact": "update_question_verdict 若在 restore_mistake 阶段失败，会向前端返回 502，但新的 verdict 和 user_overrides 已经落库，后续重试与用户提示会失真。",
                "evidence": "submissions.update 发生在 restore_mistake 之前，两步写入之间没有回滚或事务封装。",
                "locations": [
                    make_location(BACKEND_SUBMISSIONS_API, line_number_contains(BACKEND_SUBMISSIONS_API, "\"grade_result\": grade_result")),
                    make_location(BACKEND_SUBMISSIONS_API, line_number_contains(BACKEND_SUBMISSIONS_API, "restore_mistake(")),
                ],
            }
        )

    if "manual_" in submissions_text and "summary = display.get(\"summary\")" in submissions_text:
        observations.append(
            {
                "title": "submission detail / mistake list 当前高度依赖 overlay 读时修正",
                "detail": "这一轮修正把 verdict、exclusion、manual overrides 汇总到 display overlay，短期内能统一展示，但真实持久化边界仍复杂，后续更适合抽到单一状态服务或事务边界里。",
                "locations": [
                    make_location(BACKEND_SUBMISSIONS_API, line_number_contains(BACKEND_SUBMISSIONS_API, "display = _build_submission_display_view(")),
                    make_location(BACKEND_MISTAKES_SERVICE, line_number_contains(BACKEND_MISTAKES_SERVICE, "resolve_user_override_index(")),
                ],
            }
        )

    if "regrade_submission.py" in read_text(BACKEND_SUBMISSIONS_TEST) or "test_regrade_submission.py" in "\n".join(list_backend_tests()):
        observations.append(
            {
                "title": "regrade / backfill 脚本链路已增重，需要继续关注历史数据迁移成本",
                "detail": "近期提交新增了 regrade 与 backfill 脚本配套测试，说明历史数据兼容正在变成显式负担；后续修复应优先保持脚本幂等和可恢复。",
                "locations": [
                    make_location(BACKEND_REPO / "scripts" / "regrade_submission.py", 1),
                    make_location(BACKEND_REPO / "scripts" / "backfill_submission_full_details.py", 1),
                ],
            }
        )

    submissions_test_text = read_text(BACKEND_SUBMISSIONS_TEST)
    mistakes_test_text = read_text(BACKEND_MISTAKES_TEST)
    if "question not found in submission" not in submissions_test_text and "#2" not in submissions_test_text:
        coverage_gaps.append(
            "submission verdict / override 测试没有覆盖重复题号或 occurrence 场景，兄弟题 override 串删风险缺少回归。"
        )
    if "failed to sync exclusion override" not in mistakes_test_text:
        coverage_gaps.append(
            "mistakes API 测试没有覆盖 exclusion row 已写成功、grade_result 同步失败的部分提交路径。"
        )
    if "restore_mistake" not in submissions_test_text or "502" not in submissions_test_text:
        coverage_gaps.append(
            "manual verdict 测试主要覆盖成功路径，没有覆盖落库成功但 restore_mistake 失败时的返回和持久化状态。"
        )
    return findings, observations, coverage_gaps


def render_check_block(checks: list[dict[str, Any]]) -> list[str]:
    lines = []
    for item in checks:
        cmd = " ".join(item["args"])
        status = "PASS" if item["ok"] else "FAIL"
        lines.append(f"- `{status}` `{cmd}` (`{item['duration_sec']}s`)")
        output = summarize_output(item["stderr"] or item["stdout"])
        if output:
            lines.append("")
            lines.append("```text")
            lines.append(output)
            lines.append("```")
    return lines


def render_findings_block(title: str, items: list[dict[str, Any]], *, include_severity: bool) -> list[str]:
    lines = [f"## {title}"]
    if not items:
        lines.extend(["", "- 无"])
        return lines
    for index, item in enumerate(items, start=1):
        heading = f"{index}. "
        if include_severity:
            heading += f"[{item['severity']}] "
        heading += str(item["title"])
        lines.extend(["", heading])
        body_key = "impact" if include_severity else "detail"
        lines.append(f"- 说明：{item[body_key]}")
        if include_severity:
            lines.append(f"- 依据：{item['evidence']}")
        locations = [path_ref(Path(loc["path"]), loc.get("line")) for loc in item.get("locations", []) if loc.get("path")]
        if locations:
            lines.append(f"- 位置：{', '.join(locations)}")
    return lines


def render_coverage_block(gaps: list[str]) -> list[str]:
    lines = ["## Coverage Gaps"]
    if not gaps:
        lines.extend(["", "- 无"])
        return lines
    for gap in gaps:
        lines.append(f"- {gap}")
    return lines


def render_commits_block(commits: list[dict[str, str]]) -> list[str]:
    lines = ["## Recent Commits"]
    for item in commits:
        lines.append(f"- `{item['short']}` `{item['date']}` {item['subject']}")
    return lines


def render_hotspots_block(hotspots: list[dict[str, Any]]) -> list[str]:
    lines = ["## Diff Hotspots"]
    if not hotspots:
        lines.extend(["", "- 无"])
        return lines
    for item in hotspots:
        lines.append(f"- `{item['path']}` | `+{item['added']} / -{item['removed']}`")
    return lines


def write_report_files(side: str, *, report_text: str, index_text: str, manifest_entries: list[dict[str, Any]]) -> dict[str, str]:
    stamp = stamp_now()
    config = SIDE_CONFIG[side]
    report_dir: Path = config["report_dir"]
    latest_report = report_dir / "latest.md"
    archived_report = report_dir / f"{config['archive_prefix']}-{stamp}.md"
    latest_index: Path = config["index_path"]
    archived_index = INDEX_DIR / f"{config['index_prefix']}-{stamp}.md"

    write_text(archived_report, report_text)
    write_text(latest_report, report_text)
    write_text(archived_index, index_text)
    write_text(latest_index, index_text)

    manifest = read_json(MANIFEST_PATH)
    entries = manifest.get("entries")
    merged = [entry for entry in entries if isinstance(entry, dict) and entry.get("side") != side] if isinstance(entries, list) else []
    merged.extend(manifest_entries)
    write_json(
        MANIFEST_PATH,
        {
            "project": "TINT",
            "updated_at": iso_now(),
            "entries": merged,
        },
    )
    return {
        "latest_report": str(latest_report.resolve()),
        "archived_report": str(archived_report.resolve()),
        "latest_index": str(latest_index.resolve()),
        "archived_index": str(archived_index.resolve()),
        "manifest": str(MANIFEST_PATH.resolve()),
    }


def build_frontend_phase() -> dict[str, Any]:
    commits = git_commit_window(FRONTEND_REPO, limit=15)
    hotspots = git_diff_hotspots(FRONTEND_REPO, limit=15)
    checks = [
        run_command(
            ["npm", "test", "--", "--run", "src/__tests__/pages/submissionPages.test.tsx", "src/components/ui/QuestionText.test.tsx"],
            cwd=FRONTEND_REPO,
        ),
        run_command(["npm", "run", "build"], cwd=FRONTEND_REPO),
        run_command(["npm", "run", "lint"], cwd=FRONTEND_REPO),
    ]
    findings, observations, coverage_gaps = frontend_findings()
    index_text, manifest_entries = build_frontend_index()
    themes = infer_themes(commits, side="frontend")

    lines = [
        "# TINT Frontend Deep Review",
        "",
        f"- Timestamp: `{iso_now()}`",
        f"- Repo HEAD: `{git_head(FRONTEND_REPO)}`",
        f"- Report scope: `latest 15 commits + current tree static review`",
        "",
        "## Recent Work Themes",
        *[f"- {item}" for item in themes],
        "",
        *render_commits_block(commits),
        "",
        *render_hotspots_block(hotspots),
        "",
        "## Executed Checks",
        *render_check_block(checks),
        "",
        *render_findings_block("Confirmed Findings", findings, include_severity=True),
        "",
        *render_findings_block("Observations", observations, include_severity=False),
        "",
        *render_coverage_block(coverage_gaps),
        "",
        "## Index Links",
        f"- Frontend index: `{str((INDEX_DIR / 'frontend-index.md').resolve())}`",
        f"- Manifest: `{str(MANIFEST_PATH.resolve())}`",
    ]
    report_text = "\n".join(lines).strip() + "\n"
    paths = write_report_files("frontend", report_text=report_text, index_text=index_text, manifest_entries=manifest_entries)
    return {
        "side": "frontend",
        "status": "done",
        "completed_at": iso_now(),
        "head": git_head(FRONTEND_REPO),
        "confirmed_count": len(findings),
        "observation_count": len(observations),
        "findings": findings,
        "observations": observations,
        "coverage_gaps": coverage_gaps,
        "checks": checks,
        "recent_commits": commits,
        "themes": themes,
        "hotspots": hotspots,
        "paths": paths,
    }


def build_backend_phase() -> dict[str, Any]:
    commits = git_commit_window(BACKEND_REPO, limit=15)
    hotspots = git_diff_hotspots(BACKEND_REPO, limit=15)
    checks = [
        run_command(["python3", "-m", "pytest", "-q", "homework_agent/tests/test_submissions_api.py"], cwd=BACKEND_REPO),
        run_command(["python3", "-m", "pytest", "-q", "homework_agent/tests/test_mistakes_api.py"], cwd=BACKEND_REPO),
        run_command(["python3", "-m", "pytest", "-q"], cwd=BACKEND_REPO),
    ]
    findings, observations, coverage_gaps = backend_findings()
    index_text, manifest_entries = build_backend_index()
    themes = infer_themes(commits, side="backend")

    lines = [
        "# TINT Backend Deep Review",
        "",
        f"- Timestamp: `{iso_now()}`",
        f"- Repo HEAD: `{git_head(BACKEND_REPO)}`",
        f"- Report scope: `latest 15 commits + current tree static review`",
        "",
        "## Recent Work Themes",
        *[f"- {item}" for item in themes],
        "",
        *render_commits_block(commits),
        "",
        *render_hotspots_block(hotspots),
        "",
        "## Executed Checks",
        *render_check_block(checks),
        "",
        *render_findings_block("Confirmed Findings", findings, include_severity=True),
        "",
        *render_findings_block("Observations", observations, include_severity=False),
        "",
        *render_coverage_block(coverage_gaps),
        "",
        "## Index Links",
        f"- Backend index: `{str((INDEX_DIR / 'backend-index.md').resolve())}`",
        f"- Manifest: `{str(MANIFEST_PATH.resolve())}`",
    ]
    report_text = "\n".join(lines).strip() + "\n"
    paths = write_report_files("backend", report_text=report_text, index_text=index_text, manifest_entries=manifest_entries)
    return {
        "side": "backend",
        "status": "done",
        "completed_at": iso_now(),
        "head": git_head(BACKEND_REPO),
        "confirmed_count": len(findings),
        "observation_count": len(observations),
        "findings": findings,
        "observations": observations,
        "coverage_gaps": coverage_gaps,
        "checks": checks,
        "recent_commits": commits,
        "themes": themes,
        "hotspots": hotspots,
        "paths": paths,
    }


def summarize_priority(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    items = []
    for finding in findings:
        item = dict(finding)
        items.append(item)
    items.sort(key=lambda item: (order.get(str(item.get("severity")), 9), str(item.get("title"))))
    return items


def build_summary_phase(state: dict[str, Any]) -> dict[str, Any]:
    frontend = state.get("phases", {}).get("frontend") or {}
    backend = state.get("phases", {}).get("backend") or {}
    if str(frontend.get("status")) != "done" or str(backend.get("status")) != "done":
        raise RuntimeError("summary phase requires completed frontend and backend phases")

    confirmed = summarize_priority(
        [*frontend.get("findings", []), *backend.get("findings", [])]
    )
    observations = [*frontend.get("observations", []), *backend.get("observations", [])]

    summary_lines = [
        "# TINT Deep Review Summary",
        "",
        f"- Timestamp: `{iso_now()}`",
        f"- Frontend report: `{frontend.get('paths', {}).get('latest_report', '')}`",
        f"- Backend report: `{backend.get('paths', {}).get('latest_report', '')}`",
        "",
        "## Priority Queue",
    ]
    if confirmed:
        for item in confirmed:
            summary_lines.append(f"- `{item['severity']}` {item['title']}")
    else:
        summary_lines.append("- 无确认问题")

    summary_lines.extend(
        [
            "",
            "## Direct Fix Queue",
        ]
    )
    if confirmed:
        for item in confirmed:
            summary_lines.append(f"- `{item['severity']}` {item['title']} | 下一步：进入修复与 PR 队列")
    else:
        summary_lines.append("- 无")

    summary_lines.extend(
        [
            "",
            "## Needs Verification",
        ]
    )
    if observations:
        for item in observations:
            summary_lines.append(f"- {item['title']}")
    else:
        summary_lines.append("- 无")

    summary_lines.extend(
        [
            "",
            "## Side Snapshots",
            f"- Frontend confirmed / observations: `{frontend.get('confirmed_count', 0)} / {frontend.get('observation_count', 0)}`",
            f"- Backend confirmed / observations: `{backend.get('confirmed_count', 0)} / {backend.get('observation_count', 0)}`",
            "",
            "## Linked Artifacts",
            f"- Frontend latest: `{frontend.get('paths', {}).get('latest_report', '')}`",
            f"- Backend latest: `{backend.get('paths', {}).get('latest_report', '')}`",
            f"- Frontend index: `{frontend.get('paths', {}).get('latest_index', '')}`",
            f"- Backend index: `{backend.get('paths', {}).get('latest_index', '')}`",
            f"- Manifest: `{str(MANIFEST_PATH.resolve())}`",
        ]
    )
    report_text = "\n".join(summary_lines).strip() + "\n"

    stamp = stamp_now()
    latest_report = SUMMARY_REPORT_DIR / "latest.md"
    archived_report = SUMMARY_REPORT_DIR / f"summary-review-{stamp}.md"
    write_text(archived_report, report_text)
    write_text(latest_report, report_text)

    return {
        "side": "summary",
        "status": "done",
        "completed_at": iso_now(),
        "based_on_heads": {
            "frontend": frontend.get("head"),
            "backend": backend.get("head"),
        },
        "confirmed_count": len(confirmed),
        "observation_count": len(observations),
        "findings": confirmed,
        "observations": observations,
        "paths": {
            "latest_report": str(latest_report.resolve()),
            "archived_report": str(archived_report.resolve()),
        },
    }


def load_state() -> dict[str, Any]:
    state = read_json(STATE_PATH)
    phases = state.get("phases")
    if not isinstance(phases, dict):
        state["phases"] = {}
    return state


def phase_is_fresh(phase: dict[str, Any], *, side: str, heads: dict[str, str]) -> bool:
    if str(phase.get("status")) != "done":
        return False
    return str(phase.get("head") or "") == str(heads.get(side) or "")


def summary_is_fresh(phase: dict[str, Any], *, heads: dict[str, str]) -> bool:
    if str(phase.get("status")) != "done":
        return False
    based_on = phase.get("based_on_heads")
    if not isinstance(based_on, dict):
        return False
    return str(based_on.get("frontend") or "") == heads["frontend"] and str(based_on.get("backend") or "") == heads["backend"]


def sync_state(state: dict[str, Any]) -> dict[str, Any]:
    heads = {
        "frontend": git_head(FRONTEND_REPO),
        "backend": git_head(BACKEND_REPO),
    }
    phases = state.setdefault("phases", {})
    frontend_phase = phases.get("frontend") if isinstance(phases.get("frontend"), dict) else {}
    backend_phase = phases.get("backend") if isinstance(phases.get("backend"), dict) else {}
    summary_phase = phases.get("summary") if isinstance(phases.get("summary"), dict) else {}

    frontend_fresh = phase_is_fresh(frontend_phase or {}, side="frontend", heads=heads)
    backend_fresh = phase_is_fresh(backend_phase or {}, side="backend", heads=heads)
    summary_fresh = summary_is_fresh(summary_phase or {}, heads=heads)

    if frontend_phase and not frontend_fresh:
        frontend_phase["status"] = "stale"
    if backend_phase and not backend_fresh:
        backend_phase["status"] = "stale"
    if summary_phase and not summary_fresh:
        summary_phase["status"] = "stale"

    phases["frontend"] = frontend_phase
    phases["backend"] = backend_phase
    phases["summary"] = summary_phase

    next_phase = "frontend"
    if frontend_fresh:
        next_phase = "backend"
    if frontend_fresh and backend_fresh:
        next_phase = "summary"
    if frontend_fresh and backend_fresh and summary_fresh:
        next_phase = "none"

    state["project"] = "TINT"
    state["updated_at"] = iso_now()
    state["heads"] = heads
    state["next_phase"] = next_phase
    return state


def board_snapshot(state: dict[str, Any], executed_phase: str | None = None) -> dict[str, Any]:
    phases = state.get("phases", {})
    frontend = phases.get("frontend", {}) if isinstance(phases.get("frontend"), dict) else {}
    backend = phases.get("backend", {}) if isinstance(phases.get("backend"), dict) else {}
    summary = phases.get("summary", {}) if isinstance(phases.get("summary"), dict) else {}
    executed = executed_phase or "none"
    current_phase = state.get("next_phase", "frontend")
    if executed != "none":
        current_phase = f"{executed} done -> {state.get('next_phase', 'none')} pending"
    last_run_at = (
        state.get("last_run_at")
        or summary.get("completed_at")
        or backend.get("completed_at")
        or frontend.get("completed_at")
        or state.get("updated_at")
    )
    return {
        "current_phase": current_phase,
        "last_run_at": last_run_at,
        "automation_name": "TINT deep review campaign",
        "frontend_report": frontend.get("paths", {}).get("latest_report"),
        "backend_report": backend.get("paths", {}).get("latest_report"),
        "summary_report": summary.get("paths", {}).get("latest_report"),
        "frontend_index": frontend.get("paths", {}).get("latest_index"),
        "backend_index": backend.get("paths", {}).get("latest_index"),
        "manifest": str(MANIFEST_PATH.resolve()),
        "confirmed_count": int(frontend.get("confirmed_count", 0) or 0) + int(backend.get("confirmed_count", 0) or 0),
        "observation_count": int(frontend.get("observation_count", 0) or 0) + int(backend.get("observation_count", 0) or 0),
        "next_phase": state.get("next_phase"),
    }


def run_phase(phase: str, state: dict[str, Any]) -> dict[str, Any]:
    if phase == "frontend":
        result = build_frontend_phase()
    elif phase == "backend":
        result = build_backend_phase()
    elif phase == "summary":
        result = build_summary_phase(state)
    else:
        raise RuntimeError(f"unsupported phase: {phase}")

    state.setdefault("phases", {})[phase] = result
    now = iso_now()
    state["updated_at"] = now
    state["last_run_at"] = now
    return state


def save_state(state: dict[str, Any]) -> None:
    write_json(STATE_PATH, state)


def cmd_status(_args: argparse.Namespace) -> dict[str, Any]:
    state = sync_state(load_state())
    save_state(state)
    return {
        "project": "TINT",
        "state_path": str(STATE_PATH.resolve()),
        "heads": state.get("heads", {}),
        "next_phase": state.get("next_phase"),
        "phases": state.get("phases", {}),
        "board_snapshot": board_snapshot(state),
    }


def cmd_phase(args: argparse.Namespace, phase: str) -> dict[str, Any]:
    state = sync_state(load_state())
    state = run_phase(phase, state)
    state = sync_state(state)
    save_state(state)
    return {
        "project": "TINT",
        "executed_phase": phase,
        "next_phase": state.get("next_phase"),
        "state_path": str(STATE_PATH.resolve()),
        "phase_result": state.get("phases", {}).get(phase, {}),
        "board_snapshot": board_snapshot(state, executed_phase=phase),
    }


def cmd_campaign(_args: argparse.Namespace) -> dict[str, Any]:
    state = sync_state(load_state())
    next_phase = str(state.get("next_phase") or "frontend")
    executed_phase = "none"
    if next_phase in {"frontend", "backend", "summary"}:
        state = run_phase(next_phase, state)
        executed_phase = next_phase
        state = sync_state(state)
    save_state(state)
    return {
        "project": "TINT",
        "executed_phase": executed_phase,
        "next_phase": state.get("next_phase"),
        "state_path": str(STATE_PATH.resolve()),
        "heads": state.get("heads", {}),
        "phases": state.get("phases", {}),
        "board_snapshot": board_snapshot(state, executed_phase=executed_phase if executed_phase != "none" else None),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage-based deep code review orchestrator for TINT.")
    parser.add_argument("--json", action="store_true", help="Print JSON payload to stdout.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "frontend", "backend", "summary", "campaign"):
        child = subparsers.add_parser(name)
        child.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "status":
            payload = cmd_status(args)
        elif args.command == "frontend":
            payload = cmd_phase(args, "frontend")
        elif args.command == "backend":
            payload = cmd_phase(args, "backend")
        elif args.command == "summary":
            payload = cmd_phase(args, "summary")
        else:
            payload = cmd_campaign(args)
    except Exception as exc:
        error_payload = {
            "project": "TINT",
            "error": str(exc),
        }
        if args.json:
            print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        else:
            print(str(exc), file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        executed = payload.get("executed_phase") or args.command
        print(f"TINT deep review: {executed} -> next {payload.get('next_phase')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

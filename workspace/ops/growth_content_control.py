from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from ops import lark_cli_backend
except ImportError:  # pragma: no cover
    import lark_cli_backend  # type: ignore


PROJECT_NAME = "增长与营销"
DEFAULT_IDENTITY = "user"
DEFAULT_BASE_TOKEN = str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_BASE_TOKEN", "")).strip()
DEFAULT_BASE_NAME = str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_BASE_NAME", f"{PROJECT_NAME}全局内容中控")).strip()
DEFAULT_FOLDER_TOKEN = str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_FOLDER_TOKEN", "")).strip()


def runtime_root() -> Path:
    raw = str(os.environ.get("WORKSPACE_HUB_RUNTIME_ROOT", "")).strip()
    if raw:
        return Path(raw)
    return REPO_ROOT / "runtime"


STATE_PATH = runtime_root() / "growth-content-control.json"
DEFAULT_TABLE_IDS = {
    "asset": str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_ASSET_TABLE_ID", "")).strip(),
    "publish": str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_PUBLISH_TABLE_ID", "")).strip(),
    "feedback": str(os.environ.get("WORKSPACE_HUB_GROWTH_CONTENT_FEEDBACK_TABLE_ID", "")).strip(),
}
TABLE_NAMES = {
    "asset": "内容资产主表",
    "publish": "已发布记录",
    "feedback": "反馈线索记录",
}
DASHBOARD_NAME = f"{PROJECT_NAME}内容中控后台"
DASHBOARD_THEME_STYLE = "SimpleBlue"
TABLE_VIEW_SPECS: dict[str, list[dict[str, Any]]] = {
    "asset": [
        {
            "name": "内容资产总览",
            "type": "grid",
            "sort_fields": [],
            "filter": {"logic": "and", "conditions": []},
        }
    ],
    "publish": [
        {
            "name": "最近发布",
            "type": "grid",
            "sort_fields": [
                {"field_name": "发布日期", "desc": True},
                {"field_name": "发布时间", "desc": True},
            ],
            "filter": {"logic": "and", "conditions": []},
        },
        {
            "name": "高互动发布",
            "type": "grid",
            "sort_fields": [
                {"field_name": "有效销售线索", "desc": True},
                {"field_name": "评论条数", "desc": True},
                {"field_name": "点赞数", "desc": True},
            ],
            "filter": {
                "logic": "or",
                "conditions": [
                    {"field_name": "评论条数", "operator": "isGreater", "value": 0},
                    {"field_name": "私聊数", "operator": "isGreater", "value": 0},
                    {"field_name": "有效销售线索", "operator": "isGreater", "value": 0},
                ],
            },
        },
        {
            "name": "高线索发布",
            "type": "grid",
            "sort_fields": [
                {"field_name": "有效销售线索", "desc": True},
                {"field_name": "发布时间", "desc": True},
            ],
            "filter": {
                "logic": "and",
                "conditions": [
                    {"field_name": "有效销售线索", "operator": "isGreater", "value": 0},
                ],
            },
        },
    ],
    "feedback": [
        {
            "name": "全部反馈",
            "type": "grid",
            "sort_fields": [
                {"field_name": "反馈日期", "desc": True},
                {"field_name": "反馈时间", "desc": True},
            ],
            "filter": {"logic": "and", "conditions": []},
        },
        {
            "name": "待跟进反馈",
            "type": "grid",
            "sort_fields": [
                {"field_name": "有效销售线索", "desc": True},
                {"field_name": "反馈时间", "desc": True},
            ],
            "filter": {
                "logic": "and",
                "conditions": [
                    {"field_name": "跟进状态", "operator": "isNot", "value": "done"},
                ],
            },
        },
        {
            "name": "高意向线索",
            "type": "grid",
            "sort_fields": [
                {"field_name": "有效销售线索", "desc": True},
                {"field_name": "反馈时间", "desc": True},
            ],
            "filter": {
                "logic": "and",
                "conditions": [
                    {"field_name": "有效销售线索", "operator": "isGreater", "value": 0},
                ],
            },
        },
    ],
}
DASHBOARD_BLOCK_SPECS: list[dict[str, Any]] = [
    {
        "name": "已发布总数",
        "type": "statistics",
        "data_config": {"table_name": TABLE_NAMES["publish"], "count_all": True},
    },
    {
        "name": "反馈总数",
        "type": "statistics",
        "data_config": {"table_name": TABLE_NAMES["feedback"], "count_all": True},
    },
    {
        "name": "有效线索总数",
        "type": "statistics",
        "data_config": {
            "table_name": TABLE_NAMES["feedback"],
            "series": [{"field_name": "有效销售线索", "rollup": "SUM"}],
        },
    },
    {
        "name": "发布渠道分布",
        "type": "ring",
        "data_config": {
            "table_name": TABLE_NAMES["publish"],
            "count_all": True,
            "group_by": [{"field_name": "渠道", "mode": "integrated"}],
        },
    },
    {
        "name": "产品线发布分布",
        "type": "pie",
        "data_config": {
            "table_name": TABLE_NAMES["publish"],
            "count_all": True,
            "group_by": [{"field_name": "产品/服务", "mode": "integrated"}],
        },
    },
    {
        "name": "产品线线索分布",
        "type": "bar",
        "data_config": {
            "table_name": TABLE_NAMES["feedback"],
            "series": [{"field_name": "有效销售线索", "rollup": "SUM"}],
            "group_by": [{"field_name": "产品/服务", "mode": "integrated"}],
        },
    },
    {
        "name": "发布日期趋势",
        "type": "column",
        "data_config": {
            "table_name": TABLE_NAMES["publish"],
            "count_all": True,
            "group_by": [
                {
                    "field_name": "发布日期",
                    "mode": "integrated",
                    "sort": {"type": "group", "order": "asc"},
                }
            ],
        },
    },
    {
        "name": "跟进状态分布",
        "type": "bar",
        "data_config": {
            "table_name": TABLE_NAMES["feedback"],
            "count_all": True,
            "group_by": [{"field_name": "跟进状态", "mode": "integrated"}],
        },
    },
]
TABLE_FIELD_SPECS: dict[str, list[dict[str, Any]]] = {
    "asset": [
        {"field_name": "项目", "type": "text"},
        {"field_name": "产品/服务", "type": "text"},
        {"field_name": "本地记录ID", "type": "text"},
        {"field_name": "内容标题", "type": "text"},
        {"field_name": "文案正文", "type": "text"},
        {"field_name": "渠道", "type": "text"},
        {"field_name": "内容类型", "type": "text"},
        {"field_name": "状态", "type": "text"},
        {"field_name": "优先级", "type": "text"},
        {"field_name": "统一入口", "type": "text"},
        {"field_name": "图片素材路径", "type": "text"},
        {"field_name": "视频素材路径", "type": "text"},
        {"field_name": "图片附件", "type": "attachment"},
        {"field_name": "视频附件", "type": "attachment"},
        {"field_name": "配图说明", "type": "text"},
        {"field_name": "备注", "type": "text"},
        {"field_name": "生成来源", "type": "text"},
        {"field_name": "任务来源", "type": "text"},
        {"field_name": "媒体状态", "type": "text"},
        {"field_name": "媒体备注", "type": "text"},
        {"field_name": "本地路径", "type": "text"},
        {"field_name": "记录类型", "type": "text"},
    ],
    "publish": [
        {"field_name": "项目", "type": "text"},
        {"field_name": "产品/服务", "type": "text"},
        {"field_name": "渠道", "type": "text"},
        {"field_name": "发布日期", "type": "text"},
        {"field_name": "发布时间", "type": "text"},
        {"field_name": "截图时间原文", "type": "text"},
        {"field_name": "位置", "type": "text"},
        {"field_name": "内容标题", "type": "text"},
        {"field_name": "文案正文", "type": "text"},
        {"field_name": "内容形式", "type": "text"},
        {"field_name": "主题标签", "type": "text"},
        {"field_name": "截图路径", "type": "text"},
        {"field_name": "点赞数", "type": "number"},
        {"field_name": "评论条数", "type": "number"},
        {"field_name": "私聊数", "type": "number"},
        {"field_name": "有效销售线索", "type": "number"},
        {"field_name": "状态", "type": "text"},
        {"field_name": "下一步", "type": "text"},
        {"field_name": "生成来源", "type": "text"},
        {"field_name": "任务来源", "type": "text"},
        {"field_name": "本地记录ID", "type": "text"},
        {"field_name": "本地资产ID", "type": "text"},
    ],
    "feedback": [
        {"field_name": "项目", "type": "text"},
        {"field_name": "产品/服务", "type": "text"},
        {"field_name": "渠道", "type": "text"},
        {"field_name": "反馈日期", "type": "text"},
        {"field_name": "反馈时间", "type": "text"},
        {"field_name": "关键信息", "type": "text"},
        {"field_name": "截图路径", "type": "text"},
        {"field_name": "点赞数", "type": "number"},
        {"field_name": "评论条数", "type": "number"},
        {"field_name": "私聊数", "type": "number"},
        {"field_name": "有效销售线索", "type": "number"},
        {"field_name": "跟进状态", "type": "text"},
        {"field_name": "下一步", "type": "text"},
        {"field_name": "关联发布ID", "type": "text"},
        {"field_name": "本地记录ID", "type": "text"},
        {"field_name": "本地资产ID", "type": "text"},
    ],
}


class GrowthContentControlError(RuntimeError):
    pass


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _run_lark_json(argv: list[str], *, cwd: str | None = None) -> dict[str, Any]:
    result = subprocess.run(argv, capture_output=True, text=True, check=False, cwd=cwd)
    if result.returncode != 0:
        raise GrowthContentControlError(
            f"lark command failed: {' '.join(argv)}\nstdout={result.stdout}\nstderr={result.stderr}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise GrowthContentControlError(
            f"lark command returned invalid json: {' '.join(argv)}\nstdout={result.stdout}"
        ) from exc
    if not payload.get("ok", False):
        raise GrowthContentControlError(f"lark command returned not ok payload: {payload}")
    return payload


def _duplicate_field_error(exc: Exception) -> bool:
    text = str(exc)
    return "800010205" in text or "validation_error" in text and "Existing field" in text


def _field_create_limited(exc: Exception) -> bool:
    return "800004135" in str(exc)


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return len(value) > 0
    return True


def _load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_state(payload: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def resolved_base_token() -> str:
    state = _load_state()
    return _text(state.get("base_token")) or DEFAULT_BASE_TOKEN


def resolved_table_id(table_kind: str) -> str:
    state = _load_state()
    tables = state.get("tables") if isinstance(state.get("tables"), dict) else {}
    table = tables.get(table_kind) if isinstance(tables, dict) else {}
    return _text((table or {}).get("table_id")) or _text(DEFAULT_TABLE_IDS.get(table_kind))


def schema_snapshot() -> dict[str, Any]:
    state = _load_state()
    tables = state.get("tables") if isinstance(state.get("tables"), dict) else {}
    return {
        "ok": True,
        "project_name": PROJECT_NAME,
        "base_token": _text(state.get("base_token")) or DEFAULT_BASE_TOKEN,
        "tables": {
            table_kind: {
                "name": TABLE_NAMES[table_kind],
                "table_id": _text((tables.get(table_kind) or {}).get("table_id")) or DEFAULT_TABLE_IDS.get(table_kind, ""),
                "fields": TABLE_FIELD_SPECS[table_kind],
            }
            for table_kind in ("asset", "publish", "feedback")
        },
    }


def _list_views(*, table_id: str, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> list[dict[str, Any]]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id)
    if not current_base_token or not current_table_id:
        return []
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+view-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--table-id",
            current_table_id,
            "--offset",
            "0",
            "--limit",
            "200",
        ]
    )
    return list((payload.get("data") or {}).get("items") or [])


def _field_id_map(*, table_id: str, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, str]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id)
    if not current_base_token or not current_table_id:
        return {}
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+field-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--table-id",
            current_table_id,
            "--offset",
            "0",
            "--limit",
            "200",
        ]
    )
    return {
        _text(item.get("field_name")): _text(item.get("field_id"))
        for item in (payload.get("data") or {}).get("items", [])
        if _text(item.get("field_name")) and _text(item.get("field_id"))
    }


def _normalize_filter_conditions(
    conditions: list[dict[str, Any]],
    *,
    field_ids: dict[str, str],
) -> list[list[Any]]:
    operator_map = {
        "is": "==",
        "isNot": "!=",
        "isGreater": ">",
        "isGreaterEqual": ">=",
        "isLess": "<",
        "isLessEqual": "<=",
        "isEmpty": "empty",
        "isNotEmpty": "non_empty",
    }
    normalized: list[list[Any]] = []
    for condition in conditions:
        field_name = _text(condition.get("field_name"))
        field_ref = field_ids.get(field_name) or field_name
        operator = operator_map.get(_text(condition.get("operator")), _text(condition.get("operator")))
        if not (field_ref and operator):
            continue
        if "value" not in condition:
            normalized.append([field_ref, operator])
            continue
        normalized.append([field_ref, operator, condition.get("value")])
    return normalized


def _list_dashboards(*, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> list[dict[str, Any]]:
    current_base_token = _text(base_token) or resolved_base_token()
    if not current_base_token:
        return []
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+dashboard-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
        ]
    )
    return list((payload.get("data") or {}).get("items") or [])


def _list_dashboard_blocks(
    *,
    dashboard_id: str,
    base_token: str = DEFAULT_BASE_TOKEN,
    identity: str = DEFAULT_IDENTITY,
) -> list[dict[str, Any]]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_dashboard_id = _text(dashboard_id)
    if not current_base_token or not current_dashboard_id:
        return []
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+dashboard-block-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--dashboard-id",
            current_dashboard_id,
        ]
    )
    return list((payload.get("data") or {}).get("items") or [])


def bootstrap_live_base(
    *,
    base_name: str = DEFAULT_BASE_NAME,
    folder_token: str = DEFAULT_FOLDER_TOKEN,
    identity: str = DEFAULT_IDENTITY,
) -> dict[str, Any]:
    app_payload = lark_cli_backend.base_app_create(name=base_name, folder_token=folder_token, identity=identity)
    app = dict(app_payload.get("app") or {})
    base_token = _text(app.get("app_token") or app.get("base_token"))
    if not base_token:
        raise GrowthContentControlError(f"failed to resolve base token from payload: {app_payload}")
    created_tables: dict[str, Any] = {}
    for table_kind in ("asset", "publish", "feedback"):
        table_payload = lark_cli_backend.base_table_create(
            base_token=base_token,
            name=TABLE_NAMES[table_kind],
            identity=identity,
        )
        table = dict(table_payload.get("table") or {})
        table_id = _text(table.get("table_id") or table.get("id"))
        ensure_result = ensure_fields(
            table_kind=table_kind,
            table_id=table_id,
            base_token=base_token,
            identity=identity,
        )
        created_tables[table_kind] = {
            "name": TABLE_NAMES[table_kind],
            "table_id": table_id,
            "fields": TABLE_FIELD_SPECS[table_kind],
            "ensure_result": ensure_result,
        }
        time.sleep(1)
    payload = {
        "ok": True,
        "project_name": PROJECT_NAME,
        "base_name": base_name,
        "base_token": base_token,
        "tables": created_tables,
    }
    _save_state(payload)
    return payload


def get_record(
    *,
    record_id: str,
    base_token: str = DEFAULT_BASE_TOKEN,
    table_id: str = "",
    identity: str = DEFAULT_IDENTITY,
) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id) or resolved_table_id("asset")
    if not (current_base_token and current_table_id):
        return {}
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+record-get",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--table-id",
            current_table_id,
            "--record-id",
            record_id,
        ]
    )
    return dict(payload.get("data", {}).get("record") or {})


def ensure_fields(*, table_kind: str, table_id: str, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id) or resolved_table_id(table_kind)
    if not current_base_token:
        raise GrowthContentControlError("missing growth content base token")
    if not current_table_id:
        raise GrowthContentControlError("missing growth content table id")
    desired = list(TABLE_FIELD_SPECS[table_kind])
    existing_payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+field-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--table-id",
            current_table_id,
            "--offset",
            "0",
            "--limit",
            "200",
        ]
    )
    existing_names = {str(item.get("field_name", "")).strip() for item in (existing_payload.get("data", {}).get("items") or [])}
    created: list[str] = []
    skipped_existing: list[str] = []
    for spec in desired:
        field_name = _text(spec.get("field_name"))
        if not field_name or field_name in existing_names:
            continue
        attempt_error: Exception | None = None
        for attempt in range(3):
            try:
                _run_lark_json(
                    [
                        "lark-cli",
                        "base",
                        "+field-create",
                        "--as",
                        identity,
                        "--base-token",
                        current_base_token,
                        "--table-id",
                        current_table_id,
                        "--json",
                        json.dumps(spec, ensure_ascii=False),
                    ]
                )
                created.append(field_name)
                attempt_error = None
                break
            except Exception as exc:
                if _duplicate_field_error(exc):
                    skipped_existing.append(field_name)
                    attempt_error = None
                    break
                attempt_error = exc
                if _field_create_limited(exc) and attempt < 2:
                    time.sleep(3 * (attempt + 1))
                    continue
                raise
        if attempt_error is not None:
            raise attempt_error
        time.sleep(1)
    return {
        "ok": True,
        "table_kind": table_kind,
        "table_id": current_table_id,
        "created_fields": created,
        "skipped_existing": skipped_existing,
        "field_count": len(desired),
    }


def ensure_views(*, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    created: list[dict[str, str]] = []
    updated: list[dict[str, str]] = []
    for table_kind, specs in TABLE_VIEW_SPECS.items():
        table_id = resolved_table_id(table_kind)
        if not table_id:
            continue
        field_ids = _field_id_map(table_id=table_id, base_token=current_base_token, identity=identity)
        existing_views = _list_views(table_id=table_id, base_token=current_base_token, identity=identity)
        for spec in specs:
            view_name = _text(spec.get("name"))
            view_id = ""
            for item in existing_views:
                if _text(item.get("view_name")) == view_name:
                    view_id = _text(item.get("view_id"))
                    break
            if not view_id:
                _run_lark_json(
                    [
                        "lark-cli",
                        "base",
                        "+view-create",
                        "--as",
                        identity,
                        "--base-token",
                        current_base_token,
                        "--table-id",
                        table_id,
                        "--json",
                        json.dumps({"name": view_name, "type": _text(spec.get("type")) or "grid"}, ensure_ascii=False),
                    ]
                )
                existing_views = _list_views(table_id=table_id, base_token=current_base_token, identity=identity)
                for item in existing_views:
                    if _text(item.get("view_name")) == view_name:
                        view_id = _text(item.get("view_id"))
                        break
                created.append({"table_kind": table_kind, "view_name": view_name})
            filter_spec = dict(spec.get("filter") or {"logic": "and", "conditions": []})
            normalized_filter = {
                "logic": _text(filter_spec.get("logic")) or "and",
                "conditions": _normalize_filter_conditions(
                    list(filter_spec.get("conditions") or []),
                    field_ids=field_ids,
                ),
            }
            _run_lark_json(
                [
                    "lark-cli",
                    "base",
                    "+view-set-filter",
                    "--as",
                    identity,
                    "--base-token",
                    current_base_token,
                    "--table-id",
                    table_id,
                    "--view-id",
                    view_id or view_name,
                    "--json",
                    json.dumps(normalized_filter, ensure_ascii=False),
                ]
            )
            sort_spec = []
            for item in spec.get("sort_fields") or []:
                field_name = _text(item.get("field_name"))
                sort_spec.append(
                    {
                        "field": field_ids.get(field_name) or field_name,
                        "desc": bool(item.get("desc", False)),
                    }
                )
            _run_lark_json(
                [
                    "lark-cli",
                    "base",
                    "+view-set-sort",
                    "--as",
                    identity,
                    "--base-token",
                    current_base_token,
                    "--table-id",
                    table_id,
                    "--view-id",
                    view_id or view_name,
                    "--json",
                    json.dumps(sort_spec, ensure_ascii=False),
                ]
            )
            updated.append({"table_kind": table_kind, "view_name": view_name})
    return {
        "ok": True,
        "created": created,
        "updated": updated,
    }


def ensure_dashboard(*, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_dashboard_id = ""
    dashboards = _list_dashboards(base_token=current_base_token, identity=identity)
    for item in dashboards:
        if _text(item.get("name")) == DASHBOARD_NAME:
            current_dashboard_id = _text(item.get("dashboard_id"))
            break
    created_dashboard = False
    if not current_dashboard_id:
        _run_lark_json(
            [
                "lark-cli",
                "base",
                "+dashboard-create",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--name",
                DASHBOARD_NAME,
                "--theme-style",
                DASHBOARD_THEME_STYLE,
            ]
        )
        dashboards = _list_dashboards(base_token=current_base_token, identity=identity)
        for item in dashboards:
            if _text(item.get("name")) == DASHBOARD_NAME:
                current_dashboard_id = _text(item.get("dashboard_id"))
                break
        created_dashboard = True
    else:
        _run_lark_json(
            [
                "lark-cli",
                "base",
                "+dashboard-update",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--dashboard-id",
                current_dashboard_id,
                "--name",
                DASHBOARD_NAME,
                "--theme-style",
                DASHBOARD_THEME_STYLE,
            ]
        )
    existing_blocks = _list_dashboard_blocks(dashboard_id=current_dashboard_id, base_token=current_base_token, identity=identity)
    existing_signature = sorted(
        (_text(item.get("name")) or _text(item.get("block_name")), _text(item.get("type")) or _text(item.get("block_type")))
        for item in existing_blocks
        if (_text(item.get("name")) or _text(item.get("block_name")))
    )
    desired_signature = sorted((_text(item.get("name")), _text(item.get("type"))) for item in DASHBOARD_BLOCK_SPECS)
    if existing_signature == desired_signature and len(existing_signature) == len(desired_signature):
        return {
            "ok": True,
            "dashboard_id": current_dashboard_id,
            "dashboard_name": DASHBOARD_NAME,
            "created_dashboard": created_dashboard,
            "deleted_block_count": 0,
            "created_blocks": [{"name": name, "type": type_name} for name, type_name in existing_signature],
            "reused_existing_blocks": True,
        }
    deleted_blocks: list[str] = []
    for block in existing_blocks:
        block_id = _text(block.get("block_id"))
        if not block_id:
            continue
        _run_lark_json(
            [
                "lark-cli",
                "base",
                "+dashboard-block-delete",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--dashboard-id",
                current_dashboard_id,
                "--block-id",
                block_id,
                "--yes",
            ]
        )
        deleted_blocks.append(block_id)
    created_blocks: list[dict[str, str]] = []
    for spec in DASHBOARD_BLOCK_SPECS:
        _run_lark_json(
            [
                "lark-cli",
                "base",
                "+dashboard-block-create",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--dashboard-id",
                current_dashboard_id,
                "--name",
                _text(spec.get("name")),
                "--type",
                _text(spec.get("type")),
                "--data-config",
                json.dumps(spec.get("data_config") or {}, ensure_ascii=False),
            ]
        )
        created_blocks.append({"name": _text(spec.get("name")), "type": _text(spec.get("type"))})
    return {
        "ok": True,
        "dashboard_id": current_dashboard_id,
        "dashboard_name": DASHBOARD_NAME,
        "created_dashboard": created_dashboard,
        "deleted_block_count": len(deleted_blocks),
        "created_blocks": created_blocks,
    }


def ensure_surface(*, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    return {
        "ok": True,
        "views": ensure_views(base_token=base_token, identity=identity),
        "dashboard": ensure_dashboard(base_token=base_token, identity=identity),
    }


def available_field_names(*, table_id: str, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> set[str]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id)
    if not current_base_token or not current_table_id:
        return set()
    payload = _run_lark_json(
        [
            "lark-cli",
            "base",
            "+field-list",
            "--as",
            identity,
            "--base-token",
            current_base_token,
            "--table-id",
            current_table_id,
            "--offset",
            "0",
            "--limit",
            "200",
        ]
    )
    return {
        _text(item.get("field_name"))
        for item in (payload.get("data", {}).get("items") or [])
        if _text(item.get("field_name"))
    }


def filter_payload_for_table(payload: dict[str, Any], *, table_id: str, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    fields = available_field_names(table_id=table_id, base_token=base_token, identity=identity)
    if not fields:
        return dict(payload)
    return {key: value for key, value in payload.items() if key in fields}


def build_asset_payload(
    *,
    local_record_id: str = "",
    product_or_service: str = "",
    title: str = "",
    body: str = "",
    channels: list[str] | None = None,
    content_type: str = "",
    status: str = "",
    priority: str = "",
    entry: str = "",
    image_paths: list[str] | None = None,
    video_paths: list[str] | None = None,
    visual_note: str = "",
    note: str = "",
    source: str = "",
    task_source: str = "",
    media_status: str = "",
    media_note: str = "",
    local_path: str = "",
) -> dict[str, Any]:
    payload = {
        "项目": PROJECT_NAME,
        "产品/服务": product_or_service,
        "本地记录ID": local_record_id,
        "内容标题": title,
        "文案正文": body,
        "渠道": " / ".join(_text(item) for item in (channels or []) if _text(item)),
        "内容类型": content_type,
        "状态": status,
        "优先级": priority,
        "统一入口": entry,
        "图片素材路径": "\n".join(item for item in (image_paths or []) if _text(item)),
        "视频素材路径": "\n".join(item for item in (video_paths or []) if _text(item)),
        "配图说明": visual_note,
        "备注": note,
        "生成来源": source,
        "任务来源": task_source,
        "媒体状态": media_status,
        "媒体备注": media_note,
        "本地路径": local_path,
        "记录类型": "asset",
    }
    return {key: value for key, value in payload.items() if _has_value(value)}


def build_publish_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "项目": PROJECT_NAME,
        "产品/服务": _text(row.get("product_or_service")),
        "渠道": _text(row.get("channel")),
        "发布日期": _text(row.get("publish_date")),
        "发布时间": _text(row.get("publish_time")),
        "截图时间原文": _text(row.get("visible_time_text")),
        "位置": _text(row.get("location")),
        "内容标题": _text(row.get("title")),
        "文案正文": _text(row.get("body")),
        "内容形式": _text(row.get("content_kind")),
        "主题标签": _text(row.get("topic_tags")),
        "截图路径": _text(row.get("source_path")),
        "点赞数": _text(row.get("like_count")),
        "评论条数": _text(row.get("comment_count")),
        "私聊数": _text(row.get("dm_count")),
        "有效销售线索": _text(row.get("qualified_lead_count")),
        "状态": _text(row.get("status")),
        "下一步": _text(row.get("next_action")),
        "生成来源": _text(row.get("generation_source") or row.get("source")),
        "任务来源": _text(row.get("task_source")),
        "本地记录ID": _text(row.get("publish_id")),
        "本地资产ID": _text(row.get("asset_id")),
    }
    return {key: value for key, value in payload.items() if _has_value(value)}


def build_feedback_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "项目": PROJECT_NAME,
        "产品/服务": _text(row.get("product_or_service")),
        "渠道": _text(row.get("channel")),
        "反馈日期": _text(row.get("feedback_date")),
        "反馈时间": _text(row.get("feedback_time")),
        "关键信息": _text(row.get("signal_summary")),
        "截图路径": _text(row.get("source_path")),
        "点赞数": _text(row.get("like_count")),
        "评论条数": _text(row.get("comment_count")),
        "私聊数": _text(row.get("dm_count")),
        "有效销售线索": _text(row.get("qualified_lead_count")),
        "跟进状态": _text(row.get("followup_status")),
        "下一步": _text(row.get("next_action")),
        "关联发布ID": _text(row.get("publish_id")),
        "本地记录ID": _text(row.get("feedback_id")),
        "本地资产ID": _text(row.get("asset_id")),
    }
    return {key: value for key, value in payload.items() if _has_value(value)}


def _record_upsert(*, table_kind: str, payload: dict[str, Any], record_id: str = "", base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = resolved_table_id(table_kind)
    response = {
        "ok": True,
        "mode": "preview" if not (current_base_token and current_table_id) else "live",
        "table_kind": table_kind,
        "table_id": current_table_id,
        "payload": payload,
    }
    if not (current_base_token and current_table_id):
        return response
    response["field_sync_result"] = ensure_fields(
        table_kind=table_kind,
        table_id=current_table_id,
        base_token=current_base_token,
        identity=identity,
    )
    resolved_record_id = _text(record_id)
    local_record_key = _text(payload.get("本地记录ID"))
    if not resolved_record_id and local_record_key:
        try:
            listing = lark_cli_backend.base_record_list(
                base_token=current_base_token,
                table_id=current_table_id,
                identity=identity,
            )
            for item in listing.get("records", []):
                fields = item.get("fields") if isinstance(item, dict) else {}
                if isinstance(fields, dict) and _text(fields.get("本地记录ID")) == local_record_key:
                    resolved_record_id = _text(item.get("record_id"))
                    break
        except Exception:
            resolved_record_id = ""
    argv = [
        "lark-cli",
        "base",
        "+record-upsert",
        "--as",
        identity,
        "--base-token",
        current_base_token,
        "--table-id",
        current_table_id,
    ]
    if resolved_record_id:
        argv.extend(["--record-id", resolved_record_id])
    argv.extend(["--json", json.dumps(payload, ensure_ascii=False)])
    response["record_result"] = _run_lark_json(argv)
    response["record_id"] = resolved_record_id or _text((response["record_result"].get("data") or {}).get("record_id"))
    return response


def upsert_publish_record(row: dict[str, Any], *, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    return _record_upsert(
        table_kind="publish",
        payload=build_publish_payload(row),
        record_id="",
        base_token=base_token,
        identity=identity,
    )


def upsert_feedback_record(row: dict[str, Any], *, base_token: str = DEFAULT_BASE_TOKEN, identity: str = DEFAULT_IDENTITY) -> dict[str, Any]:
    return _record_upsert(
        table_kind="feedback",
        payload=build_feedback_payload(row),
        record_id="",
        base_token=base_token,
        identity=identity,
    )


def upsert_content_record(
    *,
    base_token: str = DEFAULT_BASE_TOKEN,
    table_id: str = "",
    identity: str = DEFAULT_IDENTITY,
    record_id: str | None = None,
    title: str | None = None,
    body: str | None = None,
    channels: list[str] | None = None,
    content_type: str | None = None,
    status: str | None = None,
    priority: str | None = None,
    entry: str | None = None,
    image_paths: list[str] | None = None,
    video_paths: list[str] | None = None,
    visual_note: str | None = None,
    note: str | None = None,
    source: str | None = None,
    task_source: str | None = None,
    media_status: str | None = None,
    media_note: str | None = None,
    image_files: list[str] | None = None,
    video_files: list[str] | None = None,
    ensure_fields_enabled: bool = True,
    product_or_service: str | None = None,
    local_record_id: str | None = None,
    local_path: str | None = None,
) -> dict[str, Any]:
    current_base_token = _text(base_token) or resolved_base_token()
    current_table_id = _text(table_id) or resolved_table_id("asset")
    payload = build_asset_payload(
        local_record_id=_text(local_record_id or record_id),
        product_or_service=_text(product_or_service),
        title=_text(title),
        body=_text(body),
        channels=channels,
        content_type=_text(content_type),
        status=_text(status),
        priority=_text(priority),
        entry=_text(entry),
        image_paths=image_paths,
        video_paths=video_paths,
        visual_note=_text(visual_note),
        note=_text(note),
        source=_text(source),
        task_source=_text(task_source),
        media_status=_text(media_status),
        media_note=_text(media_note),
        local_path=_text(local_path),
    )
    response: dict[str, Any] = {
        "ok": True,
        "mode": "preview" if not (current_base_token and current_table_id) else "live",
        "table_kind": "asset",
        "table_id": current_table_id,
        "payload": payload,
        "uploaded": [],
        "upload_errors": [],
    }
    if not (current_base_token and current_table_id):
        return response
    if ensure_fields_enabled:
        response["field_sync_result"] = ensure_fields(
            table_kind="asset",
            table_id=current_table_id,
            base_token=current_base_token,
            identity=identity,
        )
    filtered_payload = filter_payload_for_table(
        payload,
        table_id=current_table_id,
        base_token=current_base_token,
        identity=identity,
    )
    response["filtered_payload"] = filtered_payload
    argv = [
        "lark-cli",
        "base",
        "+record-upsert",
        "--as",
        identity,
        "--base-token",
        current_base_token,
        "--table-id",
        current_table_id,
    ]
    if _text(record_id):
        argv.extend(["--record-id", _text(record_id)])
    argv.extend(["--json", json.dumps(filtered_payload, ensure_ascii=False)])
    record_response = _run_lark_json(argv)
    resolved_record_id = _text((record_response.get("data") or {}).get("record_id"))
    response["record_id"] = resolved_record_id
    for file_path in image_files or []:
        attachment_path = Path(file_path).expanduser().resolve()
        upload_payload = _run_lark_json(
            [
                "lark-cli",
                "base",
                "+record-upload-attachment",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--table-id",
                current_table_id,
                "--record-id",
                resolved_record_id,
                "--field-id",
                "图片附件",
                "--file",
                f"./{attachment_path.name}",
            ],
            cwd=str(attachment_path.parent),
        )
        response["uploaded"].append({"file": str(attachment_path), "result": upload_payload.get("data")})
    for file_path in video_files or []:
        attachment_path = Path(file_path).expanduser().resolve()
        upload_payload = _run_lark_json(
            [
                "lark-cli",
                "base",
                "+record-upload-attachment",
                "--as",
                identity,
                "--base-token",
                current_base_token,
                "--table-id",
                current_table_id,
                "--record-id",
                resolved_record_id,
                "--field-id",
                "视频附件",
                "--file",
                f"./{attachment_path.name}",
            ],
            cwd=str(attachment_path.parent),
        )
        response["uploaded"].append({"file": str(attachment_path), "result": upload_payload.get("data")})
    return response


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Growth content projection helpers")
    subparsers = parser.add_subparsers(dest="action", required=True)

    subparsers.add_parser("schema")
    bootstrap = subparsers.add_parser("bootstrap-live")
    bootstrap.add_argument("--base-name", default=DEFAULT_BASE_NAME)
    bootstrap.add_argument("--folder-token", default=DEFAULT_FOLDER_TOKEN)
    bootstrap.add_argument("--identity", default=DEFAULT_IDENTITY)

    ensure = subparsers.add_parser("ensure-fields")
    ensure.add_argument("--table-kind", required=True, choices=["asset", "publish", "feedback"])
    ensure.add_argument("--table-id", required=True)
    ensure.add_argument("--base-token", default=DEFAULT_BASE_TOKEN)
    ensure.add_argument("--identity", default=DEFAULT_IDENTITY)
    ensure_surface_parser = subparsers.add_parser("ensure-surface")
    ensure_surface_parser.add_argument("--base-token", default=DEFAULT_BASE_TOKEN)
    ensure_surface_parser.add_argument("--identity", default=DEFAULT_IDENTITY)
    subparsers.add_parser("state")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action == "schema":
        print(json.dumps(schema_snapshot(), ensure_ascii=False, indent=2))
        return 0
    if args.action == "bootstrap-live":
        print(
            json.dumps(
                bootstrap_live_base(
                    base_name=args.base_name,
                    folder_token=args.folder_token,
                    identity=args.identity,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.action == "state":
        print(json.dumps(_load_state(), ensure_ascii=False, indent=2))
        return 0
    if args.action == "ensure-surface":
        print(
            json.dumps(
                ensure_surface(
                    base_token=args.base_token,
                    identity=args.identity,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    payload = ensure_fields(
        table_kind=args.table_kind,
        table_id=args.table_id,
        base_token=args.base_token,
        identity=args.identity,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

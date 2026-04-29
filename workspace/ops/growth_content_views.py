#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from ops import codex_memory, growth_content_truth
except ImportError:  # pragma: no cover
    import codex_memory  # type: ignore
    import growth_content_truth  # type: ignore


PROJECT_NAME = "增长与营销"
DASHBOARD_PATH_NAME = f"{PROJECT_NAME}-内容中控.md"
DETAIL_DIR_NAME = f"{PROJECT_NAME}-内容详情"
DETAIL_INDEX_NAME = "INDEX.md"


def vault_root() -> Path:
    return growth_content_truth.vault_root()


def detail_root() -> Path:
    return vault_root() / "01_working" / DETAIL_DIR_NAME


def detail_index_path() -> Path:
    return detail_root() / DETAIL_INDEX_NAME


def dashboard_path() -> Path:
    return vault_root() / "07_dashboards" / DASHBOARD_PATH_NAME


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int(value: Any) -> int:
    text = _text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _iso_today() -> str:
    return dt.date.today().isoformat()


def _iso_now() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def _date_from_text(value: str) -> dt.date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _sort_rows_by_date(rows: list[dict[str, str]], *, date_key: str, time_key: str) -> list[dict[str, str]]:
    return sorted(
        rows,
        key=lambda row: (
            _text(row.get(date_key)),
            _text(row.get(time_key)),
            _text(row.get(next(iter(row.keys())), "")),
        ),
        reverse=True,
    )


def _render_frontmatter(data: dict[str, Any]) -> str:
    order = ["project_name", "note_type", "asset_id", "publish_id", "updated_at", "purpose"]
    lines = ["---"]
    seen: set[str] = set()
    for key in order:
        if key not in data:
            continue
        seen.add(key)
        lines.append(f"{key}: {_text(data.get(key))}")
    for key in sorted(data):
        if key in seen:
            continue
        lines.append(f"{key}: {_text(data.get(key))}")
    lines.append("---")
    return "\n".join(lines)


def _markdown_link(label: str, path: str | Path) -> str:
    return f"[{label}]({_text(path)})"


def load_view_rows() -> dict[str, list[dict[str, str]]]:
    return {
        "asset": growth_content_truth.load_rows("asset"),
        "publish": growth_content_truth.load_rows("publish"),
        "feedback": growth_content_truth.load_rows("feedback"),
    }


def _detail_path_for_publish(publish_id: str) -> Path:
    return detail_root() / f"{_text(publish_id)}.md"


def _detail_path_for_asset(asset_id: str) -> Path:
    return detail_root() / f"{_text(asset_id)}.md"


def _render_feedback_table(rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return ["- 当前无反馈记录"]
    headers = ["feedback_id", "feedback_time", "signal_summary", "qualified_lead_count", "followup_status", "next_action"]
    table_rows = [{header: _text(row.get(header)) for header in headers} for row in rows]
    return codex_memory.markdown_table_lines(headers, table_rows)


def render_publish_detail_page(
    publish: dict[str, str],
    *,
    asset: dict[str, str] | None,
    feedback_rows: list[dict[str, str]],
) -> str:
    publish_id = _text(publish.get("publish_id"))
    asset_id = _text(publish.get("asset_id"))
    title = _text(publish.get("title")) or _text((asset or {}).get("topic")) or publish_id
    source_path = _text(publish.get("source_path")) or _text((asset or {}).get("source_path"))
    frontmatter = _render_frontmatter(
        {
            "project_name": PROJECT_NAME,
            "note_type": "growth_content_detail",
            "asset_id": asset_id,
            "publish_id": publish_id,
            "updated_at": _iso_now(),
            "purpose": "单条内容详情页，承接文案、媒体、反馈、跟进与复盘。",
        }
    )
    lines = [
        frontmatter,
        "",
        f"# {PROJECT_NAME}｜内容详情｜{title}",
        "",
        "## 基本信息",
        "",
        f"- 发布ID：`{publish_id}`",
        f"- 资产ID：`{asset_id}`",
        f"- 产品/服务：`{_text(publish.get('product_or_service'))}`",
        f"- 渠道：`{_text(publish.get('channel'))}`",
        f"- 发布时间：`{_text(publish.get('publish_date'))} {_text(publish.get('publish_time'))}`",
        f"- 内容形式：`{_text(publish.get('content_kind'))}`",
        f"- 主题标签：`{_text(publish.get('topic_tags'))}`",
        f"- 主表记录：{_markdown_link('已发布记录', growth_content_truth.table_path('publish'))}",
        "",
        "## 文案正文",
        "",
        _text(publish.get("body")) or "待补正文。",
        "",
        "## 媒体与证据",
        "",
        f"- 原始截图：{_markdown_link(Path(source_path).name if source_path else '原始截图', source_path) if source_path else '待补截图'}",
        f"- 位置/时间原文：`{_text(publish.get('location'))}` / `{_text(publish.get('visible_time_text'))}`",
        f"- 点赞/评论/私聊/有效线索：`{_int(publish.get('like_count'))} / {_int(publish.get('comment_count'))} / {_int(publish.get('dm_count'))} / {_int(publish.get('qualified_lead_count'))}`",
        "",
        "## 反馈与线索",
        "",
        *_render_feedback_table(feedback_rows),
        "",
        "## 跟进与复盘",
        "",
        f"- 当前状态：`{_text(publish.get('status')) or '待补充'}`",
        f"- 下一步：{_text(publish.get('next_action')) or '待补充'}",
        "- 复盘判断：待补充",
        "- 可复用结论：待补充",
        "",
    ]
    return "\n".join(lines)


def render_asset_detail_page(asset: dict[str, str]) -> str:
    asset_id = _text(asset.get("asset_id"))
    title = _text(asset.get("topic")) or asset_id
    source_path = _text(asset.get("source_path"))
    frontmatter = _render_frontmatter(
        {
            "project_name": PROJECT_NAME,
            "note_type": "growth_content_detail",
            "asset_id": asset_id,
            "updated_at": _iso_now(),
            "purpose": "内容资产详情页，承接尚未形成发布记录的资产上下文。",
        }
    )
    lines = [
        frontmatter,
        "",
        f"# {PROJECT_NAME}｜内容资产详情｜{title}",
        "",
        "## 基本信息",
        "",
        f"- 资产ID：`{asset_id}`",
        f"- 资产类型：`{_text(asset.get('asset_type'))}`",
        f"- 产品/服务：`{_text(asset.get('product_or_service'))}`",
        f"- 渠道：`{_text(asset.get('channel'))}`",
        f"- 状态：`{_text(asset.get('status'))}`",
        f"- 主表记录：{_markdown_link('内容资产主表', growth_content_truth.table_path('asset'))}",
        "",
        "## 来源与素材",
        "",
        f"- 来源桶：`{_text(asset.get('source_bucket'))}`",
        f"- 原始文件：{_markdown_link(Path(source_path).name if source_path else '原始文件', source_path) if source_path else '待补文件路径'}",
        f"- 校验值：`{_text(asset.get('checksum'))}`",
        "",
        "## 说明",
        "",
        "- 当前还没有关联的发布记录。",
        "- 如后续发布，优先转为对应的发布详情页继续沉淀。",
        "",
    ]
    return "\n".join(lines)


def render_detail_index(
    publish_rows: list[dict[str, str]],
    asset_rows: list[dict[str, str]],
    *,
    generated_paths: dict[str, Path],
) -> str:
    frontmatter = _render_frontmatter(
        {
            "project_name": PROJECT_NAME,
            "note_type": "growth_content_detail_index",
            "updated_at": _iso_now(),
            "purpose": "增长与营销内容详情页索引。",
        }
    )
    rows: list[dict[str, str]] = []
    linked_asset_ids = {_text(row.get("asset_id")) for row in publish_rows if _text(row.get("asset_id"))}
    for publish in publish_rows:
        publish_id = _text(publish.get("publish_id"))
        rows.append(
            {
                "detail_id": publish_id,
                "kind": "publish",
                "product_or_service": _text(publish.get("product_or_service")),
                "channel": _text(publish.get("channel")),
                "title": _text(publish.get("title")),
                "detail_path": _text(generated_paths.get(publish_id)),
            }
        )
    for asset in asset_rows:
        asset_id = _text(asset.get("asset_id"))
        if asset_id in linked_asset_ids:
            continue
        rows.append(
            {
                "detail_id": asset_id,
                "kind": "asset",
                "product_or_service": _text(asset.get("product_or_service")),
                "channel": _text(asset.get("channel")),
                "title": _text(asset.get("topic")),
                "detail_path": _text(generated_paths.get(asset_id)),
            }
        )
    lines = [
        frontmatter,
        "",
        f"# {PROJECT_NAME}｜内容详情索引",
        "",
        "## 说明",
        "",
        "- 这里是单条详情页索引，不是事实主表。",
        "- 事实源继续以 `内容资产主表 / 已发布记录 / 反馈线索记录` 为准。",
        "",
        *codex_memory.markdown_table_lines(
            ["detail_id", "kind", "product_or_service", "channel", "title", "detail_path"],
            rows,
        ),
        "",
    ]
    return "\n".join(lines)


def _metric_window(publish_rows: list[dict[str, str]], feedback_rows: list[dict[str, str]], *, days: int) -> dict[str, int]:
    today = dt.date.today()
    publish_window = [
        row
        for row in publish_rows
        if (_date_from_text(_text(row.get("publish_date"))) and (today - _date_from_text(_text(row.get("publish_date")))).days < days)
    ]
    feedback_window = [
        row
        for row in feedback_rows
        if (_date_from_text(_text(row.get("feedback_date"))) and (today - _date_from_text(_text(row.get("feedback_date")))).days < days)
    ]
    return {
        "publish_count": len(publish_window),
        "like_count": sum(_int(row.get("like_count")) for row in publish_window),
        "comment_count": sum(_int(row.get("comment_count")) for row in publish_window),
        "dm_count": sum(_int(row.get("dm_count")) for row in publish_window),
        "qualified_lead_count": sum(_int(row.get("qualified_lead_count")) for row in feedback_window or publish_window),
        "feedback_count": len(feedback_window),
    }


def _group_summary(
    publish_rows: list[dict[str, str]],
    feedback_rows: list[dict[str, str]],
    *,
    key: str,
) -> list[dict[str, str]]:
    feedback_by_publish: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in feedback_rows:
        feedback_by_publish[_text(row.get("publish_id"))].append(row)
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"publish_count": 0, "feedback_count": 0, "qualified_lead_count": 0})
    for publish in publish_rows:
        group_key = _text(publish.get(key)) or "未标注"
        grouped[group_key]["publish_count"] += 1
        grouped[group_key]["qualified_lead_count"] += _int(publish.get("qualified_lead_count"))
        grouped[group_key]["feedback_count"] += len(feedback_by_publish.get(_text(publish.get("publish_id")), []))
    rows = []
    for group_key, stats in sorted(grouped.items(), key=lambda item: (-item[1]["qualified_lead_count"], -item[1]["publish_count"], item[0])):
        rows.append(
            {
                key: group_key,
                "publish_count": str(stats["publish_count"]),
                "feedback_count": str(stats["feedback_count"]),
                "qualified_lead_count": str(stats["qualified_lead_count"]),
            }
        )
    return rows


def render_dashboard(
    asset_rows: list[dict[str, str]],
    publish_rows: list[dict[str, str]],
    feedback_rows: list[dict[str, str]],
    *,
    generated_paths: dict[str, Path],
) -> str:
    today_metrics = _metric_window(publish_rows, feedback_rows, days=1)
    seven_day_metrics = _metric_window(publish_rows, feedback_rows, days=7)
    product_rows = _group_summary(publish_rows, feedback_rows, key="product_or_service")
    channel_rows = _group_summary(publish_rows, feedback_rows, key="channel")
    recent_publish_rows = []
    for publish in publish_rows[:10]:
        publish_id = _text(publish.get("publish_id"))
        recent_publish_rows.append(
            {
                "publish_id": publish_id,
                "product_or_service": _text(publish.get("product_or_service")),
                "channel": _text(publish.get("channel")),
                "publish_date": _text(publish.get("publish_date")),
                "title": _text(publish.get("title")),
                "like_count": str(_int(publish.get("like_count"))),
                "comment_count": str(_int(publish.get("comment_count"))),
                "qualified_lead_count": str(_int(publish.get("qualified_lead_count"))),
                "detail_path": _text(generated_paths.get(publish_id)),
            }
        )
    followup_rows = []
    for feedback in _sort_rows_by_date(feedback_rows, date_key="feedback_date", time_key="feedback_time"):
        followup_status = _text(feedback.get("followup_status")).lower()
        if followup_status in {"done", "closed", "archived"}:
            continue
        publish_id = _text(feedback.get("publish_id"))
        followup_rows.append(
            {
                "feedback_id": _text(feedback.get("feedback_id")),
                "publish_id": publish_id,
                "product_or_service": _text(feedback.get("product_or_service")),
                "channel": _text(feedback.get("channel")),
                "qualified_lead_count": str(_int(feedback.get("qualified_lead_count"))),
                "followup_status": _text(feedback.get("followup_status")) or "待跟进",
                "next_action": _text(feedback.get("next_action")),
                "detail_path": _text(generated_paths.get(publish_id)),
            }
        )

    frontmatter = _render_frontmatter(
        {
            "project_name": PROJECT_NAME,
            "note_type": "growth_content_dashboard",
            "updated_at": _iso_now(),
            "purpose": "只读展示页，面向内容发布、反馈、线索和跟进的经营观察。",
        }
    )
    lines = [
        frontmatter,
        "",
        f"# {PROJECT_NAME}｜内容中控",
        "",
        "## 定位",
        "",
        "- 本页属于 `07_dashboards/` 展示层。",
        "- 事实源仍然是 `内容资产主表 / 已发布记录 / 反馈线索记录 / 单条详情页`。",
        "- 本页只做经营观察，不在这里手写事实。",
        "",
        "## 汇总规则",
        "",
        "- `主表索引` 负责高频筛选、高频聚合和高频关联。",
        "- `单条详情页` 负责承接完整文案、原始截图、反馈原话和复盘判断。",
        "- `展示页` 负责看趋势、看漏斗、看待跟进。",
        "",
        "## Auto Overview",
        "<!-- AUTO_GROWTH_CONTENT_OVERVIEW_START -->",
        f"- 资产总数：`{len(asset_rows)}`",
        f"- 发布总数：`{len(publish_rows)}`",
        f"- 反馈总数：`{len(feedback_rows)}`",
        f"- 今日发布/反馈/有效线索：`{today_metrics['publish_count']} / {today_metrics['feedback_count']} / {today_metrics['qualified_lead_count']}`",
        f"- 近 7 天发布/点赞/评论/私聊/有效线索：`{seven_day_metrics['publish_count']} / {seven_day_metrics['like_count']} / {seven_day_metrics['comment_count']} / {seven_day_metrics['dm_count']} / {seven_day_metrics['qualified_lead_count']}`",
        f"- 详情页索引：{_markdown_link('内容详情索引', detail_index_path())}",
        "<!-- AUTO_GROWTH_CONTENT_OVERVIEW_END -->",
        "",
        "## Auto Product Summary",
        "<!-- AUTO_GROWTH_CONTENT_PRODUCT_START -->",
        *codex_memory.markdown_table_lines(
            ["product_or_service", "publish_count", "feedback_count", "qualified_lead_count"],
            product_rows,
        ),
        "<!-- AUTO_GROWTH_CONTENT_PRODUCT_END -->",
        "",
        "## Auto Channel Summary",
        "<!-- AUTO_GROWTH_CONTENT_CHANNEL_START -->",
        *codex_memory.markdown_table_lines(
            ["channel", "publish_count", "feedback_count", "qualified_lead_count"],
            channel_rows,
        ),
        "<!-- AUTO_GROWTH_CONTENT_CHANNEL_END -->",
        "",
        "## Auto Recent Publishes",
        "<!-- AUTO_GROWTH_CONTENT_PUBLISH_START -->",
        *codex_memory.markdown_table_lines(
            ["publish_id", "product_or_service", "channel", "publish_date", "title", "like_count", "comment_count", "qualified_lead_count", "detail_path"],
            recent_publish_rows,
        ),
        "<!-- AUTO_GROWTH_CONTENT_PUBLISH_END -->",
        "",
        "## Auto Followup Queue",
        "<!-- AUTO_GROWTH_CONTENT_FOLLOWUP_START -->",
        *codex_memory.markdown_table_lines(
            ["feedback_id", "publish_id", "product_or_service", "channel", "qualified_lead_count", "followup_status", "next_action", "detail_path"],
            followup_rows,
        ),
        "<!-- AUTO_GROWTH_CONTENT_FOLLOWUP_END -->",
        "",
    ]
    return "\n".join(lines)


def refresh_views() -> dict[str, Any]:
    rows = load_view_rows()
    asset_rows = _sort_rows_by_date(rows["asset"], date_key="updated_at", time_key="created_at")
    publish_rows = _sort_rows_by_date(rows["publish"], date_key="publish_date", time_key="publish_time")
    feedback_rows = _sort_rows_by_date(rows["feedback"], date_key="feedback_date", time_key="feedback_time")
    asset_by_id = {_text(row.get("asset_id")): row for row in asset_rows}
    feedback_by_publish: dict[str, list[dict[str, str]]] = defaultdict(list)
    for feedback in feedback_rows:
        feedback_by_publish[_text(feedback.get("publish_id"))].append(feedback)

    generated_paths: dict[str, Path] = {}
    detail_root().mkdir(parents=True, exist_ok=True)
    for publish in publish_rows:
        publish_id = _text(publish.get("publish_id"))
        path = _detail_path_for_publish(publish_id)
        generated_paths[publish_id] = path
        codex_memory.write_text(
            path,
            render_publish_detail_page(
                publish,
                asset=asset_by_id.get(_text(publish.get("asset_id"))),
                feedback_rows=feedback_by_publish.get(publish_id, []),
            ),
        )

    published_asset_ids = {_text(row.get("asset_id")) for row in publish_rows if _text(row.get("asset_id"))}
    for asset in asset_rows:
        asset_id = _text(asset.get("asset_id"))
        if asset_id in published_asset_ids:
            continue
        path = _detail_path_for_asset(asset_id)
        generated_paths[asset_id] = path
        codex_memory.write_text(path, render_asset_detail_page(asset))

    codex_memory.write_text(
        detail_index_path(),
        render_detail_index(publish_rows, asset_rows, generated_paths=generated_paths),
    )
    codex_memory.write_text(
        dashboard_path(),
        render_dashboard(asset_rows, publish_rows, feedback_rows, generated_paths=generated_paths),
    )
    return {
        "ok": True,
        "detail_root": str(detail_root()),
        "detail_index_path": str(detail_index_path()),
        "dashboard_path": str(dashboard_path()),
        "detail_page_count": len(generated_paths),
        "asset_count": len(asset_rows),
        "publish_count": len(publish_rows),
        "feedback_count": len(feedback_rows),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Render 增长与营销 content detail pages and dashboard views")
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("refresh")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.action == "refresh":
        print(json.dumps(refresh_views(), ensure_ascii=False, indent=2))
        return 0
    raise SystemExit(f"unknown action: {args.action}")


if __name__ == "__main__":
    raise SystemExit(main())

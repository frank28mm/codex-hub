#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ops import growth_content_control, growth_content_truth, growth_content_views


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("curation payload must be a JSON object")
    return payload


def _metadata(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("metadata")
    return dict(value) if isinstance(value, dict) else {}


def _resolve_generation_source(row: dict[str, Any], *, metadata: dict[str, Any]) -> str:
    return (
        _text(row.get("generation_source"))
        or _text(row.get("source"))
        or _text(metadata.get("execution_tool"))
        or "Codex vision"
    )


def _resolve_task_source(row: dict[str, Any], *, metadata: dict[str, Any]) -> str:
    return (
        _text(row.get("task_source"))
        or _text(row.get("source_bucket"))
        or _text(metadata.get("task_source"))
        or "growth-content-curation"
    )


def apply_batch(payload: dict[str, Any], *, write_projection_live: bool = True) -> dict[str, Any]:
    assets = list(payload.get("assets") or [])
    publishes = list(payload.get("publishes") or [])
    feedbacks = list(payload.get("feedbacks") or [])
    metadata = _metadata(payload)

    if assets:
        growth_content_truth.upsert_rows("asset", assets)
    if publishes:
        growth_content_truth.upsert_rows("publish", publishes)
    if feedbacks:
        growth_content_truth.upsert_rows("feedback", feedbacks)
    local_surface_result = growth_content_views.refresh_views()

    live_results: list[dict[str, Any]] = []
    if write_projection_live:
        for row in assets:
            generation_source = _resolve_generation_source(row, metadata=metadata)
            task_source = _resolve_task_source(row, metadata=metadata)
            live_results.append(
                growth_content_control.upsert_content_record(
                    local_record_id=_text(row.get("asset_id")),
                    product_or_service=_text(row.get("product_or_service")),
                    title=_text(row.get("live_title")) or _text(row.get("topic")),
                    body=_text(row.get("live_body")),
                    channels=[_text(row.get("channel"))] if _text(row.get("channel")) else [],
                    status=_text(row.get("status")),
                    local_path=_text(row.get("source_path")),
                    source=generation_source,
                    task_source=task_source,
                    ensure_fields_enabled=False,
                )
            )
        for row in publishes:
            live_results.append(
                growth_content_control.upsert_publish_record(
                    {
                        **row,
                        "generation_source": _resolve_generation_source(row, metadata=metadata),
                        "task_source": _resolve_task_source(row, metadata=metadata),
                    }
                )
            )
        for row in feedbacks:
            live_results.append(growth_content_control.upsert_feedback_record(row))

    return {
        "ok": True,
        "asset_count": len(assets),
        "publish_count": len(publishes),
        "feedback_count": len(feedbacks),
        "local_surface_result": local_surface_result,
        "live_results": live_results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply Codex-curated growth content records to local truth and Feishu projection")
    parser.add_argument("--json-file", required=True)
    parser.add_argument("--skip-live-projection", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = _load_json(args.json_file)
    result = apply_batch(payload, write_projection_live=not args.skip_live_projection)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

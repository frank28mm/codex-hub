#!/usr/bin/env python3
from __future__ import annotations

from typing import Any


LAUNCH_CONTEXT_FIELDS: tuple[str, ...] = (
    "project_name",
    "binding_scope",
    "binding_board_path",
    "topic_name",
    "rollup_target",
    "launch_source",
    "source_chat_ref",
    "source_thread_name",
    "source_thread_label",
    "attachment_path",
    "attachment_type",
    "voice_transcript",
    "execution_profile",
    "model",
    "reasoning_effort",
)

LAUNCH_CONTEXT_SCHEMA_KEYS: tuple[str, ...] = ("mode", *LAUNCH_CONTEXT_FIELDS, "session_id")

TRANSCRIPT_SHARED_FIELD_PATHS: dict[str, list[str]] = {
    field: [
        f"payload.launch_context.{field}",
        f"payload.context.{field}",
        f"payload.{field}",
        f"meta.launch.{field}",
    ]
    for field in LAUNCH_CONTEXT_FIELDS
}

START_CODEX_FORWARD_OPTIONS: tuple[tuple[str, str], ...] = (
    ("source", "--source"),
    ("chat_ref", "--chat-ref"),
    ("thread_name", "--thread-name"),
    ("thread_label", "--thread-label"),
    ("source_message_id", "--source-message-id"),
    ("attachment_path", "--attachment-path"),
    ("attachment_type", "--attachment-type"),
    ("voice_transcript", "--voice-transcript"),
)


def transcript_shared_fields() -> dict[str, list[str]]:
    return {key: list(value) for key, value in TRANSCRIPT_SHARED_FIELD_PATHS.items()}


def start_codex_forward_options() -> list[tuple[str, str]]:
    return list(START_CODEX_FORWARD_OPTIONS)


def launch_context_contract() -> dict[str, Any]:
    return {
        "launch_context_keys": list(LAUNCH_CONTEXT_SCHEMA_KEYS),
        "launch_context_fields": list(LAUNCH_CONTEXT_FIELDS),
        "transcript_shared_fields": transcript_shared_fields(),
        "start_codex_forward_options": start_codex_forward_options(),
    }

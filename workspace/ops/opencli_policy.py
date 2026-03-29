#!/usr/bin/env python3
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


XIAOHONGSHU_AUTO_COMMANDS = {
    "search",
    "feed",
    "user",
    "creator-note-detail",
    "creator-notes",
    "creator-notes-summary",
    "creator-profile",
    "creator-stats",
}

XIAOHONGSHU_GATED_COMMANDS = {
    "publish",
    "comment-send",
    "dm-send",
    "download",
    "notifications",
}

XIANYU_AUTO_COMMANDS = {
    "personal-summary",
    "my-listings",
    "search",
    "inquiries",
    "inquiry-thread-read",
}

XIANYU_GATED_COMMANDS = {
    "publish",
}


CONTROL_ROOT = Path(__file__).resolve().parents[1] / "control"
OPENCLI_AUTHORIZATION_PATH = CONTROL_ROOT / "opencli-authorization.yaml"


@lru_cache(maxsize=1)
def _authorization_config() -> dict[str, Any]:
    if not OPENCLI_AUTHORIZATION_PATH.exists():
        return {}
    payload = yaml.safe_load(OPENCLI_AUTHORIZATION_PATH.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _preauthorized(site: str, command: str) -> bool:
    config = _authorization_config()
    site_rules = config.get("site_authorizations", {})
    if not isinstance(site_rules, dict):
        return False
    item = site_rules.get(site, {})
    if not isinstance(item, dict):
        return False
    commands = item.get("preauthorized_commands", [])
    if not isinstance(commands, list):
        return False
    return str(command or "").strip().lower() in {str(value).strip().lower() for value in commands}


def command_policy(site: str, command: str) -> dict[str, Any]:
    normalized_site = str(site or "").strip().lower()
    normalized_command = str(command or "").strip().lower()
    if normalized_site == "system":
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "auto",
            "risk": "diagnostic",
            "reason": "system commands are read-only runtime diagnostics",
        }
    if normalized_site == "xiaohongshu" and normalized_command in XIAOHONGSHU_AUTO_COMMANDS:
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "auto",
            "risk": "read_only",
            "reason": "low-risk read/search/creator analytics command in the approved Xiaohongshu pilot scope",
        }
    if normalized_site == "xiaohongshu" and _preauthorized(normalized_site, normalized_command):
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "preauthorized",
            "risk": "side_effect_or_private_data",
            "reason": "this Xiaohongshu command is currently covered by the persisted platform-level user authorization",
        }
    if normalized_site == "xiaohongshu" and normalized_command in XIAOHONGSHU_GATED_COMMANDS:
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "approval_required",
            "risk": "side_effect_or_private_data",
            "reason": "this Xiaohongshu command mutates content, exports local files, or accesses personal notification surfaces",
        }
    if normalized_site == "xianyu" and normalized_command in XIANYU_AUTO_COMMANDS:
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "auto",
            "risk": "read_only",
            "reason": "low-risk Xianyu read command implemented through OpenCLI-backed browser read and kept inside the approved pilot scope",
        }
    if normalized_site == "xianyu" and _preauthorized(normalized_site, normalized_command):
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "preauthorized",
            "risk": "side_effect_or_private_data",
            "reason": "this Xianyu command is currently covered by the persisted platform-level user authorization",
        }
    if normalized_site == "xianyu" and normalized_command in XIANYU_GATED_COMMANDS:
        return {
            "site": normalized_site,
            "command": normalized_command,
            "mode": "approval_required",
            "risk": "side_effect_or_private_data",
            "reason": "Xianyu publish or other write paths stay behind approval until the adapter surface is explicitly hardened",
        }
    return {
        "site": normalized_site,
        "command": normalized_command,
        "mode": "approval_required",
        "risk": "unclassified",
        "reason": "command is outside the currently approved OpenCLI pilot scope and must stay behind an approval gate",
    }

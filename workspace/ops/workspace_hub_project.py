from __future__ import annotations

import os
from pathlib import Path

PROJECT_NAME = "Codex Hub"
PROJECT_ALIASES = (
    "workspace-hub",
    "workspace hub",
    "Codex Obsidian记忆与行动系统",
)
DEFAULT_WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
DEFAULT_LOCAL_VAULT_ROOT = Path(
    os.environ.get("WORKSPACE_HUB_VAULT_ROOT", str(DEFAULT_WORKSPACE_ROOT.parent / "memory"))
).resolve()
LEGACY_ICLOUD_VAULT_ROOT = DEFAULT_LOCAL_VAULT_ROOT


def is_workspace_hub_project(name: str) -> bool:
    normalized = name.strip().lower()
    if not normalized:
        return False
    return normalized == PROJECT_NAME.lower() or normalized in {alias.lower() for alias in PROJECT_ALIASES}


def canonicalize(name: str) -> str:
    return PROJECT_NAME if is_workspace_hub_project(name) else name.strip()

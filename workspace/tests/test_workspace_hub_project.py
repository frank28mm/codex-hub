from __future__ import annotations

from ops.workspace_hub_project import PROJECT_NAME, canonicalize, is_workspace_hub_project


def test_canonicalize_legacy_workspace_project_names() -> None:
    assert PROJECT_NAME == "Codex Hub"
    assert is_workspace_hub_project("workspace-hub") is True
    assert is_workspace_hub_project("Codex Obsidian记忆与行动系统") is True
    assert canonicalize("workspace hub") == PROJECT_NAME
    assert canonicalize("Codex Obsidian记忆与行动系统") == PROJECT_NAME

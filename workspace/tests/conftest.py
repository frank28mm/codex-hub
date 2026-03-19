from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from tests.fixture_builder import build_sample_environment, write_sample_feishu_resources


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def sample_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    env = build_sample_environment(tmp_path)
    shutil.copytree(REPO_ROOT / "control", env["control_root"], dirs_exist_ok=True)
    write_sample_feishu_resources(env["control_root"])
    monkeypatch.setenv("WORKSPACE_HUB_ROOT", str(env["workspace_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_CODE_ROOT", str(REPO_ROOT))
    monkeypatch.setenv("WORKSPACE_HUB_VAULT_ROOT", str(env["vault_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_WORKSPACE_ROOT", str(REPO_ROOT))
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_VAULT_ROOT", str(env["vault_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_EXPECTED_PROJECTS_ROOT", str(env["projects_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_PROJECTS_ROOT", str(env["projects_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_REPORTS_ROOT", str(env["reports_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_RUNTIME_ROOT", str(env["runtime_root"]))
    monkeypatch.setenv("WORKSPACE_HUB_CONTROL_ROOT", str(env["control_root"]))
    return env

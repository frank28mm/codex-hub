from __future__ import annotations

import importlib
from pathlib import Path


def test_read_codex_auth_status_reports_missing_file(monkeypatch, tmp_path: Path) -> None:
    from ops import accept_product as accept_module

    accept_product = importlib.reload(accept_module)
    missing = tmp_path / "auth.json"
    monkeypatch.setattr(accept_product, "codex_auth_path", lambda: missing)

    payload = accept_product.read_codex_auth_status()

    assert payload["ready"] is False
    assert payload["reason"] == "missing"


def test_check_feature_tools_reports_optional_dependencies(monkeypatch) -> None:
    from ops import accept_product as accept_module

    accept_product = importlib.reload(accept_module)
    monkeypatch.setattr(
        accept_product,
        "FEATURE_TOOL_GROUPS",
        {
            "sample": {
                "label": "Sample Feature",
                "commands": ("sample-cmd",),
            }
        },
    )
    monkeypatch.setattr(accept_product.shutil, "which", lambda name: None)

    checks = accept_product.check_feature_tools()

    assert checks == [("sample-cmd", False, "optional for Sample Feature")]


def test_check_bootstrap_cli_contract_requires_new_feature_surfaces() -> None:
    from ops import accept_product as accept_module

    accept_product = importlib.reload(accept_module)

    checks = accept_product.check_bootstrap_cli_contract()
    check_map = {name: ok for name, ok, _note in checks}

    assert check_map["doctor-feature"] is True
    assert check_map["install-system-deps"] is True
    assert check_map["install-feature"] is True
    assert check_map["feature:feishu"] is True
    assert check_map["feature:knowledge-base"] is True
    assert check_map["feature:opencli"] is True
    assert check_map["feature:weixin"] is True
    assert check_map["feature:electron"] is True
    assert check_map["group:knowledge_base_pdf_ocr"] is True
    assert check_map["group:opencli_browser"] is True

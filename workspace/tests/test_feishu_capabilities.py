from __future__ import annotations

import importlib


def test_build_auth_plan_includes_minutes_artifact_scopes() -> None:
    from ops import feishu_capabilities as feishu_capabilities_module

    feishu_capabilities = importlib.reload(feishu_capabilities_module)

    payload = feishu_capabilities.build_auth_plan()

    assert "minutes_artifacts" in payload["capability_ids"]
    assert "minutes:minutes.artifacts:read" in payload["requested_scopes"]
    assert "minutes:minutes.transcript:export" in payload["requested_scopes"]
    assert "offline_access" in payload["requested_scopes"]


def test_evaluate_capabilities_marks_minutes_artifacts_as_missing_scope() -> None:
    from ops import feishu_capabilities as feishu_capabilities_module

    feishu_capabilities = importlib.reload(feishu_capabilities_module)

    payload = feishu_capabilities.evaluate_capabilities(
        granted_scopes="minutes:minutes:readonly vc:note:read",
        lark_cli_configured=True,
        user_auth_ready=True,
        bridge_credentials_ready=True,
    )

    minutes_basic = payload["capabilities"]["minutes_basic"]
    minutes_artifacts = payload["capabilities"]["minutes_artifacts"]

    assert minutes_basic["ready"] is True
    assert minutes_artifacts["ready"] is False
    assert minutes_artifacts["missing_scopes"] == [
        "minutes:minutes.artifacts:read",
        "minutes:minutes.transcript:export",
    ]
    assert payload["auth_plan_ready"] is False

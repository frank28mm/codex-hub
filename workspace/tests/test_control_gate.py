from __future__ import annotations

import json

import pytest

from ops import control_gate
from ops.workspace_hub_project import PROJECT_NAME


def test_decision_matrix_public_read_allow_and_record(sample_env) -> None:
    result = control_gate.decide_action(
        target="https://example.com",
        action="read",
        execution_context="noninteractive",
    )
    assert result["decision"] == "allow"
    assert result["target_class"] == "public"
    record = control_gate.record_decision(
        target="https://example.com",
        action="read",
        execution_context="noninteractive",
        result=result,
        project_name=PROJECT_NAME,
        session_id="sess-1",
    )
    lines = control_gate.control_decisions_path().read_text(encoding="utf-8").splitlines()
    assert lines
    payload = json.loads(lines[-1])
    assert payload["decision"] == "allow"
    assert payload["project_name"] == PROJECT_NAME
    assert record["reason_code"] == "public_read_allowed"


def test_reversible_system_requires_confirmation_and_dry_run(sample_env) -> None:
    result = control_gate.decide_action(
        target="ssh://prod.internal",
        action="restart-service",
        execution_context="interactive",
        dry_run=False,
    )
    assert result["decision"] == "confirm"
    assert result["requires_dry_run"] is True


def test_privileged_action_denied(sample_env) -> None:
    result = control_gate.decide_action(
        target="https://console.aliyun.com",
        action="change-iam",
        execution_context="interactive",
    )
    assert result["decision"] == "deny"
    assert result["reason_code"] == "privileged_or_irreversible_denied"


def test_status_summary_counts_files(sample_env) -> None:
    control_gate.audit_event(
        target="https://github.com",
        action="read",
        result="success",
        target_class="owned-low",
        action_class="read",
        execution_context="interactive",
    )
    summary = control_gate.status_summary()
    assert summary["config_loaded"] is True
    assert summary["action_rule_count"] >= 1
    assert summary["network_audit_count"] == 1


def test_invalid_execution_context_raises(sample_env) -> None:
    with pytest.raises(ValueError):
        control_gate.decide_action(
            target="https://example.com",
            action="read",
            execution_context="unknown-context",
        )

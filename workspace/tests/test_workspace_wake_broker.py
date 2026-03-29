from __future__ import annotations

import importlib


def test_wake_broker_coalesces_to_higher_priority_reason(sample_env) -> None:
    from ops import workspace_wake_broker as wake_broker_module

    wake_broker = importlib.reload(wake_broker_module)
    wake_broker.request_wake("workspace-health", reason="interval")
    payload = wake_broker.request_wake("workspace-health", reason="project_writeback")

    assert payload["accepted"] is True
    assert payload["pending"]["reason"] == "project_writeback"
    assert payload["pending"]["priority"] > wake_broker.resolve_priority("interval")


def test_wake_broker_claim_and_complete_cycle(sample_env) -> None:
    from ops import workspace_wake_broker as wake_broker_module

    wake_broker = importlib.reload(wake_broker_module)
    wake_broker.request_wake("workspace-health", reason="manual_wake", metadata={"trigger_source": "manual_cli"})

    claimed = wake_broker.claim_wake("workspace-health")
    assert claimed["claimed"] is True
    wake_id = claimed["wake"]["wake_id"]

    completed = wake_broker.complete_wake(
        "workspace-health",
        wake_id=wake_id,
        status="succeeded",
        result={"run_id": "whc-test", "ok": True},
    )
    assert completed["completed"] is True

    status = wake_broker.job_status("workspace-health")
    assert status["pending"] == {}
    assert status["running"] == {}
    assert status["last_completed"]["status"] == "succeeded"
    assert status["last_completed"]["result"]["run_id"] == "whc-test"

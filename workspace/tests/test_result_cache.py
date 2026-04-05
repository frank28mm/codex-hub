from __future__ import annotations

from ops import result_cache


def test_result_cache_round_trip(sample_env) -> None:
    identity = {"job": "health-check", "project": "Codex Hub"}
    payload = result_cache.remember(
        "automation",
        identity,
        value={"status": "ok", "issue_count": 0},
        metadata={"source": "test"},
    )

    recalled = result_cache.recall("automation", identity)

    assert payload["key"] == result_cache.stable_key(identity)
    assert recalled is not None
    assert recalled["value"]["status"] == "ok"
    assert recalled["metadata"]["source"] == "test"


def test_result_cache_contract_exposes_runtime_root(sample_env) -> None:
    contract = result_cache.cache_contract()

    assert contract["schema_version"] == "codex-hub.result-cache.v1"
    assert contract["root"].endswith("/runtime/result-cache")
    assert "namespace" in contract["fields"]
    assert "value" in contract["fields"]

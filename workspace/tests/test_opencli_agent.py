from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import yaml


def test_opencli_doctor_parses_missing_extension(monkeypatch) -> None:
    from ops import opencli_agent

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[1] == "--version":
            return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")
        assert argv[1] == "doctor"
        stdout = """opencli v1.4.1 doctor

[OK] Daemon: running on port 19825
[MISSING] Extension: not connected
[FAIL] Connectivity: failed (Daemon is running but the Browser Extension is not connected.)
"""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)
    result = opencli_agent.perform_operation("system", "doctor", {})
    assert result["ok"] is False
    assert result["version"] == "1.4.1"
    assert result["checks"]["daemon"]["status"] == "ok"
    assert result["checks"]["extension"]["status"] == "missing"
    assert result["checks"]["connectivity"]["status"] == "fail"


def test_ensure_browser_bridge_connected_wakes_extension_when_preflight_is_missing(monkeypatch) -> None:
    from ops import opencli_agent

    events: list[tuple[str, tuple[str, ...]]] = []

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        events.append(("run", tuple(argv)))
        if argv[1] == "--version":
            return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")
        if argv[:3] == ["opencli", "doctor", "--no-live"]:
            stdout = """opencli v1.4.1 doctor

[OK] Daemon: running on port 19825
[MISSING] Extension: not connected
"""
            return subprocess.CompletedProcess(argv, 0, stdout, "")
        if argv[0] == "osascript":
            return subprocess.CompletedProcess(argv, 0, "", "")
        if argv[:2] == ["opencli", "doctor"]:
            stdout = """opencli v1.4.1 doctor

[OK] Daemon: running on port 19825
[OK] Extension: connected
[OK] Connectivity: connected in 0.3s
"""
            return subprocess.CompletedProcess(argv, 0, stdout, "")
        raise AssertionError(argv)

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)

    result = opencli_agent._ensure_browser_bridge_connected()

    assert result["ok"] is True
    assert result["woke"] is True
    assert result["doctor"]["checks"]["extension"]["status"] == "ok"
    assert any(call[1][0] == "osascript" for call in events)


def test_ensure_browser_bridge_connected_returns_without_wake_when_extension_is_present(monkeypatch) -> None:
    from ops import opencli_agent

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[1] == "--version":
            return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")
        assert argv[:3] == ["opencli", "doctor", "--no-live"]
        stdout = """opencli v1.4.1 doctor

[OK] Daemon: running on port 19825
[OK] Extension: connected
"""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)

    result = opencli_agent._ensure_browser_bridge_connected()

    assert result["ok"] is True
    assert result["woke"] is False


def test_wake_browser_bridge_falls_back_to_open_when_osascript_fails(monkeypatch) -> None:
    from ops import opencli_agent

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[0] == "osascript":
            return subprocess.CompletedProcess(argv, 1, "", "Apple event timed out")
        if argv[:3] == ["open", "-a", "Google Chrome"]:
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise AssertionError(argv)

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)

    result = opencli_agent._wake_browser_bridge()

    assert result["ok"] is True
    assert result["method"] == "open"
    assert result["attempts"][0]["returncode"] == 1
    assert result["attempts"][1]["returncode"] == 0


def test_opencli_list_filters_site(monkeypatch) -> None:
    from ops import opencli_agent

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[1] == "--version":
            return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")
        assert argv[1:4] == ["list", "-f", "yaml"]
        stdout = """
- command: xiaohongshu/search
  site: xiaohongshu
  name: search
  strategy: cookie
- command: bilibili/hot
  site: bilibili
  name: hot
  strategy: cookie
"""
        return subprocess.CompletedProcess(argv, 0, stdout, "")

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)
    result = opencli_agent.perform_operation("system", "list", {"site": "xiaohongshu"})
    assert result["ok"] is True
    assert result["count"] == 1
    assert result["commands"][0]["command"] == "xiaohongshu/search"


def test_broker_opencli_op_returns_structured_result(monkeypatch, capsys) -> None:
    from ops import local_broker

    monkeypatch.setattr(
        local_broker.opencli_agent,
        "perform_operation",
        lambda site, command, payload: {
            "ok": True,
            "site": site,
            "command": command,
            "result": {"commands": [{"command": "xiaohongshu/search"}]},
        },
    )
    exit_code = local_broker.cmd_opencli_op(
        argparse.Namespace(site="system", command="list", payload_json=json.dumps({"site": "xiaohongshu"}))
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is True
    assert payload["broker_action"] == "opencli_op"
    assert payload["site"] == "system"
    assert payload["command"] == "list"
    assert payload["result"]["result"]["commands"][0]["command"] == "xiaohongshu/search"


def test_local_broker_parser_registers_opencli_op() -> None:
    from ops import local_broker

    parser = local_broker.build_parser()
    action = next(item for item in parser._actions if isinstance(item, argparse._SubParsersAction))
    assert "opencli-op" in action.choices


def test_broker_opencli_op_blocks_xianyu_publish_without_token_under_public_policy(monkeypatch, capsys) -> None:
    from ops import local_broker

    monkeypatch.setattr(
        local_broker.opencli_agent,
        "perform_operation",
        lambda site, command, payload: {"ok": True, "site": site, "command": command, "result": {"status": "drafted"}},
    )
    exit_code = local_broker.cmd_opencli_op(
        argparse.Namespace(site="xianyu", command="publish", payload_json="{}", approval_token="")
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is False
    assert payload["error"] == "approval_token_required"
    assert payload["policy"]["mode"] == "approval_required"


def test_broker_opencli_op_keeps_working_when_a_legacy_approved_token_is_still_supplied(sample_env, monkeypatch, capsys) -> None:
    from ops import local_broker, runtime_state

    runtime_state.upsert_approval_token(
        token="opencli-approved",
        scope=local_broker.OPENCLI_APPROVAL_SCOPE,
        status="approved",
        metadata={"approved_site": "xiaohongshu", "approved_command": "publish"},
    )
    monkeypatch.setattr(
        local_broker.opencli_agent,
        "perform_operation",
        lambda site, command, payload: {"ok": True, "site": site, "command": command, "result": {"status": "drafted"}},
    )
    exit_code = local_broker.cmd_opencli_op(
        argparse.Namespace(
            site="xiaohongshu",
            command="publish",
            payload_json=json.dumps({"positional": ["内容"], "options": {"title": "标题", "draft": True}}),
            approval_token="opencli-approved",
        )
    )
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["ok"] is True
    assert payload["policy"]["mode"] == "approval_required"
    assert payload["result"]["site"] == "xiaohongshu"
    assert payload["result"]["result"]["status"] == "drafted"


def test_xianyu_personal_summary_parses_structured_fields(monkeypatch) -> None:
    from ops import opencli_agent

    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/personal.md",
            "markdown": """# TB_51538397_闲鱼

Preview
TB_51538397
上海市
30粉丝
9关注
宝贝
宝贝
27
信用及评价
信用及评价
46
""",
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation("xianyu", "personal-summary", {})
    assert result["ok"] is True
    assert result["result"]["account"] == "TB_51538397"
    assert result["result"]["location"] == "上海市"
    assert result["result"]["followers"] == 30
    assert result["result"]["following"] == 9
    assert result["result"]["listings"] == 27
    assert result["result"]["reputation"] == 46


def test_xianyu_search_returns_structured_items(monkeypatch) -> None:
    from ops import opencli_agent

    markdown = """# _闲鱼

[ 商品A ¥50 12人想要 卖家甲 ](https://www.goofish.com/item?id=1&categoryId=2)
[ 商品B ¥80 3人想要 卖家乙 ](https://www.goofish.com/item?id=3&categoryId=4)
"""
    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/search.md",
            "markdown": markdown,
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation("xianyu", "search", {"positional": ["AI"], "options": {"limit": 1}})
    assert result["ok"] is True
    assert result["artifacts"]["query"] == "AI"
    assert len(result["result"]) == 1
    assert result["result"][0]["title"] == "商品A"
    assert result["result"][0]["price"] == "50"
    assert result["result"][0]["wants"] == 12


def test_xianyu_my_listings_filters_to_current_seller(monkeypatch) -> None:
    from ops import opencli_agent

    markdown = """# TB_51538397_闲鱼

Preview
TB_51538397
上海市
30粉丝
9关注

[ 自己的商品A ¥50 12人想要 TB_51538397 ](https://www.goofish.com/item?id=1&categoryId=2)
[ 别人的商品B ¥80 3人想要 卖家乙 ](https://www.goofish.com/item?id=3&categoryId=4)
"""
    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/personal.md",
            "markdown": markdown,
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation("xianyu", "my-listings", {"options": {"limit": 10}})
    assert result["ok"] is True
    assert len(result["result"]) == 1
    assert result["result"][0]["title"] == "自己的商品A"


def test_xianyu_inquiries_extracts_thread_links(monkeypatch) -> None:
    from ops import opencli_agent

    markdown = """
# 聊天_闲鱼

[ 买家甲 AI 工作流诊断｜帮你找出效率卡点和自动化机会 想了解一下怎么做 3分钟前未读 ](https://www.goofish.com/im?itemId=1036239638047&peerUserId=88766817)
""".strip()
    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/im.md",
            "markdown": markdown,
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation("xianyu", "inquiries", {"options": {"limit": 5}})
    assert result["ok"] is True
    assert len(result["result"]) == 1
    assert result["result"][0]["listing_id"] == "1036239638047"
    assert result["result"][0]["peer_user_id"] == "88766817"
    assert result["result"][0]["thread_url"].startswith("https://www.goofish.com/im?")


def test_xianyu_inquiry_thread_read_reports_empty_state(monkeypatch) -> None:
    from ops import opencli_agent

    markdown = """
# 聊天_闲鱼

尚未选择任何联系人

快点左侧列表聊起来吧~
""".strip()
    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/im-thread.md",
            "markdown": markdown,
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation(
        "xianyu",
        "inquiry-thread-read",
        {"options": {"url": "https://www.goofish.com/im?itemId=1036239638047&peerUserId=88766817"}},
    )
    assert result["ok"] is True
    assert result["result"]["empty"] is True
    assert result["result"]["item_id"] == "1036239638047"


def test_xiaohongshu_comment_send_routes_to_write_helper(monkeypatch) -> None:
    from ops import opencli_agent

    monkeypatch.setattr(opencli_agent, "_run_session_warmup", lambda site, command, payload: {"ok": True, "warmed": True})
    monkeypatch.setattr(
        opencli_agent,
        "_run_write_helper",
        lambda site, command, payload: {
            "ok": True,
            "site": site,
            "command": command,
            "result": {"status": "sent", "url": payload["options"]["url"]},
        },
    )
    result = opencli_agent.perform_operation(
        "xiaohongshu",
        "comment-send",
        {
            "human_gate_approved": True,
            "positional": ["测试评论"],
            "options": {"url": "https://www.xiaohongshu.com/explore/abc"},
        },
    )
    assert result["ok"] is True
    assert result["site"] == "xiaohongshu"
    assert result["command"] == "comment-send"
    assert result["result"]["status"] == "sent"


def test_opencli_write_helper_can_write_growth_closed_loop(sample_env, monkeypatch) -> None:
    from ops import growth_truth, opencli_agent

    root = sample_env["vault_root"] / "01_working"
    (sample_env["control_root"] / "codex_growth_system.yaml").write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "project_name": "增长与营销",
                "system_name": "Codex Growth System",
                "objects": {
                    "Lead": {
                        "table_path": str(root / "Growth-Lead.md"),
                        "fields": [
                            "lead_id",
                            "platform",
                            "source_type",
                            "source_ref",
                            "problem",
                            "score",
                            "status",
                            "owner",
                            "handoff_required",
                            "updated_at",
                        ],
                    },
                    "Conversation": {
                        "table_path": str(root / "Growth-Conversation.md"),
                        "fields": [
                            "conversation_id",
                            "lead_id",
                            "channel",
                            "last_message_excerpt",
                            "round_index",
                            "next_action",
                            "status",
                        ],
                    },
                    "Action": {
                        "table_path": str(root / "Growth-Action.md"),
                        "fields": [
                            "action_id",
                            "platform",
                            "command",
                            "target_type",
                            "target_id",
                            "status",
                            "risk_level",
                            "run_id",
                            "error",
                            "executed_at",
                        ],
                    },
                    "Evidence": {
                        "table_path": str(root / "Growth-Evidence.md"),
                        "fields": [
                            "evidence_id",
                            "source_type",
                            "source_id",
                            "signal_type",
                            "content",
                            "decision",
                            "merged_into",
                            "created_at",
                        ],
                    },
                },
                "risk_controls": {},
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[0] == "node":
            return subprocess.CompletedProcess(argv, 0, json.dumps({"ok": True, "result": {"status": "sent"}}), "")
        assert argv[1] == "--version"
        return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")

    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)
    monkeypatch.setattr(opencli_agent, "_run_session_warmup", lambda site, command, payload: {"ok": True, "warmed": True})
    monkeypatch.setattr(opencli_agent, "_ensure_browser_bridge_connected", lambda: {"ok": True, "woke": False})
    result = opencli_agent.perform_operation(
        "xiaohongshu",
        "comment-send",
        {
            "human_gate_approved": True,
            "positional": ["测试评论"],
            "options": {"url": "https://www.xiaohongshu.com/explore/abc"},
            "growth_cycle": {
                "lead": {
                    "platform": "xiaohongshu",
                    "source_type": "comment",
                    "source_ref": "xhs-abc",
                    "problem": "想知道工作台如何搭建",
                    "score": 88,
                    "status": "qualified",
                },
                "conversation": {
                    "channel": "comment",
                    "message": "可以先做一次诊断。",
                    "round_index": 1,
                },
                "action": {
                    "command": "comment-send",
                    "target_type": "lead",
                    "risk_level": "medium",
                },
                "evidence": {
                    "signal_type": "comment_reply",
                    "content": "公开评论后形成高意向线索。",
                },
            },
        },
    )

    assert result["ok"] is True
    if result["growth_cycle"]:
        assert result["growth_cycle_error"] == {}
        assert growth_truth.load_rows("Lead")[0]["handoff_required"] == "yes"
        assert growth_truth.load_rows("Conversation")[0]["channel"] == "comment"
        assert growth_truth.load_rows("Action")[0]["command"] == "comment-send"
        assert growth_truth.load_rows("Evidence")[0]["signal_type"] == "comment_reply"
    else:
        assert result["growth_cycle_error"]["code"] == "growth_cycle_unavailable"


def test_write_guard_blocks_duplicate_idempotency(sample_env) -> None:
    from ops import opencli_agent, runtime_state

    runtime_state.record_growth_action_attempt(
        idempotency_key="dup-key",
        platform="xiaohongshu",
        command="comment-send",
        action_status="done",
        payload={"options": {"url": "https://example.com"}},
    )
    guard = opencli_agent._write_guard(
        "xiaohongshu",
        "comment-send",
        {"idempotency_key": "dup-key", "options": {"url": "https://example.com"}, "positional": ["hello"]},
    )
    assert guard["gated"] is True
    assert guard["reason"] == "idempotency_hit"


def test_xianyu_my_listings_handles_personal_page_wrapper_and_escaped_seller(monkeypatch) -> None:
    from ops import opencli_agent

    markdown = """# TB_51538397_闲鱼

> 原文链接: https://www.goofish.com/personal

![](//img.alicdn.com/banner.webp)[ 网页版发闲置功能又升级啦！ ](https://www.goofish.com/changelog)

Preview

TB\\_51538397

上海市

30粉丝

9关注

[

![](//img.alicdn.com/item-a.webp)

百植萃蓝精灵防晒霜，SPF50+，40g装，24年4月淘宝买

¥50

TB\\_51538397

](https://www.goofish.com/item?id=1008059815001&categoryId=50025435)

[

![](//img.alicdn.com/item-b.webp)

别人的商品B

¥80

卖家乙

](https://www.goofish.com/item?id=1008059815002&categoryId=50025435)
"""
    monkeypatch.setattr(
        opencli_agent,
        "_run_web_read_capture",
        lambda url, wait_seconds=3: {
            "version": "1.4.1",
            "argv": ["opencli", "web", "read"],
            "url": url,
            "markdown_path": "/tmp/personal-wrapped.md",
            "markdown": markdown,
            "stdout": "[]",
            "stderr": "",
        },
    )
    monkeypatch.setattr(
        opencli_agent,
        "_ensure_browser_bridge_connected",
        lambda: {"ok": True, "woke": False, "doctor": {"checks": {"extension": {"status": "ok"}}}},
    )
    result = opencli_agent.perform_operation("xianyu", "my-listings", {"options": {"limit": 10}})
    assert result["ok"] is True
    assert len(result["result"]) == 1
    assert result["result"][0]["seller"] == "TB_51538397"
    assert result["result"][0]["title"] == "百植萃蓝精灵防晒霜，SPF50+，40g装，24年4月淘宝买"


def test_xianyu_policy_is_auto_for_read_only_surface() -> None:
    from ops import opencli_policy

    policy = opencli_policy.command_policy("xianyu", "my-listings")
    assert policy["mode"] == "auto"
    assert policy["risk"] == "read_only"
    assert opencli_policy.command_policy("xianyu", "inquiries")["mode"] == "auto"


def test_opencli_policy_keeps_public_write_commands_approval_gated_by_default() -> None:
    from ops import opencli_policy

    xhs = opencli_policy.command_policy("xiaohongshu", "publish")
    xhs_comment = opencli_policy.command_policy("xiaohongshu", "comment-send")
    xianyu = opencli_policy.command_policy("xianyu", "publish")
    assert xhs["mode"] == "approval_required"
    assert xhs_comment["mode"] == "approval_required"
    assert xianyu["mode"] == "approval_required"


def test_xiaohongshu_publish_dispatches_to_local_write_helper(monkeypatch) -> None:
    from ops import opencli_agent

    monkeypatch.setattr(opencli_agent, "_run_session_warmup", lambda site, command, payload: {"ok": True, "warmed": True})
    monkeypatch.setattr(
        opencli_agent,
        "_run_write_helper",
        lambda site, command, payload: {"ok": True, "site": site, "command": command, "result": {"status": "published"}},
    )
    result = opencli_agent.perform_operation(
        "xiaohongshu",
        "publish",
        {
            "human_gate_approved": True,
            "positional": ["测试正文"],
            "options": {"title": "测试标题", "images": "/tmp/demo.png"},
        },
    )
    assert result["ok"] is True
    assert result["site"] == "xiaohongshu"
    assert result["command"] == "publish"
    assert result["result"]["status"] == "published"


def test_xiaohongshu_write_requires_human_gate_by_default() -> None:
    from ops import opencli_agent

    try:
        opencli_agent.perform_operation(
            "xiaohongshu",
            "comment-send",
            {"positional": ["你好"], "options": {"url": "https://www.xiaohongshu.com/explore/demo"}},
        )
    except opencli_agent.OpenCLIAgentError as exc:
        assert exc.code == "human_gate_required"
        assert exc.details["site"] == "xiaohongshu"
        assert exc.details["command"] == "comment-send"
    else:  # pragma: no cover
        raise AssertionError("expected xiaohongshu write to require a human gate")


def test_xiaohongshu_creator_profile_runs_session_warmup_before_cli(monkeypatch) -> None:
    from ops import opencli_agent

    events: list[tuple[str, object]] = []

    def fake_warm(site: str, command: str, payload: dict[str, object]) -> dict[str, object]:
        events.append(("warm", (site, command)))
        return {"ok": True, "site": site, "command": command, "warmed": True}

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[1] == "--version":
            events.append(("version", tuple(argv)))
            return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")
        events.append(("command", tuple(argv)))
        assert argv[:5] == ["opencli", "xiaohongshu", "creator-profile", "-f", "json"]
        return subprocess.CompletedProcess(argv, 0, json.dumps([{"field": "Name", "value": "测试账号"}]), "")

    monkeypatch.setattr(opencli_agent, "_run_session_warmup", fake_warm)
    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)
    monkeypatch.setattr(opencli_agent, "_ensure_browser_bridge_connected", lambda: {"ok": True, "woke": False})

    result = opencli_agent.perform_operation("xiaohongshu", "creator-profile", {})

    assert result["ok"] is True
    assert result["warmup"]["warmed"] is True
    assert result["result"][0]["value"] == "测试账号"
    assert events[0][0] == "version"
    assert events[1] == ("warm", ("xiaohongshu", "creator-profile"))
    assert events[2][0] == "command"


def test_xiaohongshu_comment_send_runs_session_warmup_before_helper(sample_env, monkeypatch) -> None:
    from ops import opencli_agent

    events: list[tuple[str, object]] = []

    def fake_warm(site: str, command: str, payload: dict[str, object]) -> dict[str, object]:
        events.append(("warm", (site, command)))
        return {"ok": True, "site": site, "command": command, "warmed": True}

    def fake_run(argv: list[str], *, timeout_seconds: int = 90) -> subprocess.CompletedProcess[str]:
        if argv[0] == "node":
            events.append(("helper", tuple(argv)))
            return subprocess.CompletedProcess(argv, 0, json.dumps({"ok": True, "result": {"status": "sent"}}), "")
        events.append(("version", tuple(argv)))
        return subprocess.CompletedProcess(argv, 0, "1.4.1\n", "")

    monkeypatch.setattr(opencli_agent, "_run_session_warmup", fake_warm)
    monkeypatch.setattr(opencli_agent, "_run_command", fake_run)
    monkeypatch.setattr(opencli_agent, "_ensure_browser_bridge_connected", lambda: {"ok": True, "woke": False})

    result = opencli_agent.perform_operation(
        "xiaohongshu",
        "comment-send",
        {
            "human_gate_approved": True,
            "positional": ["你好"],
            "options": {"url": "https://www.xiaohongshu.com/explore/demo"},
        },
    )

    assert result["ok"] is True
    assert result["warmup"]["warmed"] is True
    assert result["result"]["status"] == "sent"
    assert events[0] == ("warm", ("xiaohongshu", "comment-send"))
    assert events[1][0] == "helper"
    assert events[2][0] == "version"


def test_xianyu_publish_dispatches_to_local_write_helper(monkeypatch) -> None:
    from ops import opencli_agent

    monkeypatch.setattr(
        opencli_agent,
        "_run_write_helper",
        lambda site, command, payload: {"ok": True, "site": site, "command": command, "result": {"status": "published"}},
    )
    result = opencli_agent.perform_operation(
        "xianyu",
        "publish",
        {"positional": ["测试描述"], "options": {"images": "/tmp/demo.png", "price": "1"}},
    )
    assert result["ok"] is True
    assert result["site"] == "xianyu"
    assert result["command"] == "publish"
    assert result["result"]["status"] == "published"

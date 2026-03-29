from __future__ import annotations

from pathlib import Path

from ops import gstack_phase1_entry


def test_office_hours_prompt_gets_entry_recommendation() -> None:
    payload = gstack_phase1_entry.detect_entry_path("帮我梳理一下这个想法，我还没想清楚要不要做。")
    assert payload["status"] == "workflow-recommended"
    assert payload["recognized_stage"] == "entry"
    assert payload["suggested_path"] == ["office-hours"]
    assert "我先按 `office-hours` 接管" in payload["assistant_message"]
    assert "初始判断" in payload["assistant_message"]


def test_plan_ceo_review_prompt_gets_product_recommendation() -> None:
    payload = gstack_phase1_entry.detect_entry_path("这个方向值不值得现在做？从产品角度看呢？")
    assert payload["suggested_path"] == ["plan-ceo-review"]
    assert payload["matched_skills"] == ["plan-ceo-review"]


def test_plan_eng_review_prompt_gets_technical_recommendation() -> None:
    payload = gstack_phase1_entry.detect_entry_path("这个方案技术上怎么落地，会不会很难维护？")
    assert payload["suggested_path"] == ["plan-eng-review"]
    assert payload["matched_skills"] == ["plan-eng-review"]


def test_mixed_prompt_gets_full_entry_chain() -> None:
    payload = gstack_phase1_entry.detect_entry_path(
        "我有个新项目想法，还没想清楚值不值得做，技术上怎么落地也没把握。"
    )
    assert payload["suggested_path"] == [
        "office-hours",
        "plan-ceo-review",
        "plan-eng-review",
    ]
    assert len(payload["initial_action_plan"]) >= 3
    assert payload["initial_action_plan"][0] == "先重构问题和目标"


def test_direct_do_prompt_skips_entry_workflow() -> None:
    payload = gstack_phase1_entry.detect_entry_path("这个小改动不要分析，直接做。")
    assert payload["status"] == "direct-execution-ok"
    assert payload["suggested_path"] == []


def test_document_release_like_prompt_no_longer_counts_as_unmatched() -> None:
    payload = gstack_phase1_entry.detect_entry_path("帮我整理一下发布说明。")
    assert payload["status"] == "workflow-recommended"
    assert payload["recognized_stage"] == "delivery"
    assert payload["suggested_path"] == ["document-release"]


def test_investigate_prompt_gets_execution_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("这个 bug 为什么会这样，先帮我定位根因。")
    assert payload["status"] == "workflow-recommended"
    assert payload["recognized_stage"] == "execution"
    assert payload["suggested_path"] == ["investigate"]


def test_review_prompt_gets_execution_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("帮我审一下这次改动有没有问题。")
    assert payload["recognized_stage"] == "execution"
    assert payload["suggested_path"] == ["review"]


def test_qa_prompt_gets_execution_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("已经改好了，帮我测一下并验收。")
    assert payload["recognized_stage"] == "execution"
    assert payload["suggested_path"] == ["qa"]


def test_cross_stage_prompt_gets_entry_then_execution_chain() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "我这个想法还没梳理清楚，技术上怎么落地也没想好，最后做完后还要帮我验收。"
    )
    assert payload["recognized_stage"] == "multi-stage"
    assert payload["suggested_path"] == [
        "office-hours",
        "plan-eng-review",
        "qa",
    ]


def test_review_then_browse_then_qa_prompt_gets_canonical_execution_chain() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "帮我 review 这个改动，再看页面流转，再做 QA。"
    )

    assert payload["recognized_stage"] == "execution"
    assert payload["suggested_path"] == ["review", "browse", "qa"]
    assert "我先按 `review -> browse -> qa` 这条链推进" in payload["assistant_message"]


def test_browse_prompt_gets_execution_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "帮我用真实浏览器看一下这个页面流程和按钮交互。"
    )
    assert payload["recognized_stage"] == "execution"
    assert payload["suggested_path"] == ["browse"]


def test_document_release_prompt_gets_delivery_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "帮我把这次变更同步成发布说明和更新文档。"
    )
    assert payload["recognized_stage"] == "delivery"
    assert payload["suggested_path"] == ["document-release"]


def test_retro_prompt_gets_delivery_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("这一轮做完了，帮我做个复盘。")
    assert payload["recognized_stage"] == "delivery"
    assert payload["suggested_path"] == ["retro"]


def test_ship_prompt_gets_delivery_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("这个版本准备发版了，帮我判断是不是能发。")
    assert payload["recognized_stage"] == "delivery"
    assert payload["suggested_path"] == ["ship"]


def test_page_ready_to_ship_prompt_gets_browse_qa_ship_chain() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("这个页面修好没，能不能准备 ship。")

    assert payload["recognized_stage"] == "multi-stage"
    assert payload["suggested_path"] == ["browse", "qa", "ship"]
    assert "我先按 `browse -> qa -> ship` 这条链推进" in payload["assistant_message"]


def test_careful_prompt_gets_posture_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("这次变更风险很高，先谨慎一点推进。")
    assert payload["recognized_stage"] == "posture"
    assert payload["suggested_path"] == ["careful"]


def test_freeze_prompt_gets_posture_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("先冻结所有写操作，等我确认后再继续。")
    assert payload["recognized_stage"] == "posture"
    assert payload["suggested_path"] == ["freeze"]


def test_unfreeze_prompt_gets_posture_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path("现在可以解除冻结继续推进了吗？")
    assert payload["recognized_stage"] == "posture"
    assert payload["suggested_path"] == ["unfreeze"]


def test_cross_stage_prompt_can_reach_delivery_layer() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "这个想法还没梳理清楚，技术上怎么落地也要判断，做完后还要同步发布说明。"
    )
    assert payload["recognized_stage"] == "multi-stage"
    assert payload["suggested_path"] == [
        "office-hours",
        "plan-eng-review",
        "document-release",
    ]


def test_cross_stage_prompt_can_reach_posture_layer() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "帮我审一下这次改动，准备发版前先谨慎一点推进。"
    )
    assert payload["recognized_stage"] == "multi-stage"
    assert payload["suggested_path"] == ["review", "ship", "careful"]


def test_claude_review_prompt_gets_second_opinion_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "请让 Claude 再审一下这个方案，给我一个第二意见。"
    )
    assert payload["recognized_stage"] == "second-opinion"
    assert payload["suggested_path"] == ["claude-review"]


def test_claude_review_example_prompt_from_agents_matches() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "这个方案请再给我一个 Claude 风格的第二意见。"
    )
    assert payload["recognized_stage"] == "second-opinion"
    assert payload["suggested_path"] == ["claude-review"]


def test_claude_challenge_prompt_gets_second_opinion_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "你站在反方挑战一下这个发布判断，做一次 Claude second opinion。"
    )
    assert payload["recognized_stage"] == "second-opinion"
    assert payload["suggested_path"] == ["claude-challenge"]


def test_claude_consult_prompt_gets_second_opinion_recommendation() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "我想让 Claude 给一个顾问意见，帮我看看这个取舍。"
    )
    assert payload["recognized_stage"] == "second-opinion"
    assert payload["suggested_path"] == ["claude-consult"]


def test_cross_stage_prompt_can_reach_second_opinion_layer() -> None:
    payload = gstack_phase1_entry.detect_workflow_path(
        "先帮我审一下这次改动，然后再让 Claude 给一个第二意见。"
    )
    assert payload["recognized_stage"] == "multi-stage"
    assert payload["suggested_path"] == ["review", "claude-review"]


def test_suggest_second_opinion_skill_from_path_prefers_explicit_skill() -> None:
    payload = gstack_phase1_entry.suggest_second_opinion_skill_from_path(
        ["review", "claude-review"]
    )

    assert payload["skill"] == "claude-review"
    assert payload["source"] == "explicit"


def test_suggest_second_opinion_skill_from_path_derives_followup_for_review_ship_and_plan() -> None:
    review_payload = gstack_phase1_entry.suggest_second_opinion_skill_from_path(["review"])
    ship_payload = gstack_phase1_entry.suggest_second_opinion_skill_from_path(["ship"])
    plan_payload = gstack_phase1_entry.suggest_second_opinion_skill_from_path(
        ["plan-eng-review"]
    )

    assert review_payload["skill"] == "claude-review"
    assert review_payload["source"] == "followup"
    assert ship_payload["skill"] == "claude-challenge"
    assert ship_payload["source"] == "followup"
    assert plan_payload["skill"] == "claude-consult"
    assert plan_payload["source"] == "followup"


def test_build_second_opinion_prompt_keeps_question_judgment_and_context() -> None:
    prompt = gstack_phase1_entry.build_second_opinion_prompt(
        "claude-review",
        question="Should we ship this rollout?",
        artifact="release decision",
        current_judgment="Ship after one more smoke check.",
        extra_context="The risky area is migration rollback.",
        trigger_path=["review", "claude-review"],
        source_prompt="帮我审一下这次改动，然后再给我一个第二意见。",
    )
    assert '"skill": "claude-review"' in prompt
    assert "Should we ship this rollout?" in prompt
    assert "Ship after one more smoke check." in prompt
    assert "The risky area is migration rollback." in prompt
    assert '"trigger_path": [' in prompt
    assert "review" in prompt
    assert "claude-review" in prompt
    assert "帮我审一下这次改动，然后再给我一个第二意见。" in prompt
    assert gstack_phase1_entry.SECOND_OPINION_REQUEST_SCHEMA_VERSION in prompt


def test_build_second_opinion_package_autofills_review_template_from_prompt() -> None:
    payload = gstack_phase1_entry.build_second_opinion_package(
        "claude-review",
        prompt="帮我审一下这次改动有没有问题。",
        trigger_path=["review"],
        source="followup",
    )

    assert payload["template_id"] == "review-risk-scan"
    assert payload["source"] == "followup"
    assert payload["material_source"] == "extractor"
    assert payload["autofilled_fields"] == [
        "question",
        "artifact",
        "current_judgment",
        "extra_context",
    ]
    assert payload["request"]["question"] == "这个改动或方案最大的回归风险、盲区或缺失验证是什么？"
    assert "当前 review 证据包" in payload["request"]["artifact"]
    assert "帮我审一下这次改动有没有问题。" in payload["request"]["artifact"]
    assert "git review evidence packet" in payload["request"]["extra_context"]


def test_build_review_second_opinion_materials_include_diff_and_file_signals(monkeypatch) -> None:
    monkeypatch.setattr(
        gstack_phase1_entry,
        "collect_git_worktree_snapshot",
        lambda repo_root=None: {  # type: ignore[no-untyped-def]
            "git_root": "/tmp/sample",
            "branch": "main",
            "head_commit": "abc123 sample commit",
            "status_lines": ["M ops/foo.py", "A tests/test_foo.py"],
            "staged_stat": "1 file changed, 8 insertions(+)",
            "unstaged_stat": "1 file changed, 2 deletions(-)",
            "staged_diff_excerpt": ["diff --git a/ops/foo.py b/ops/foo.py", "@@ -1 +1 @@", "+new line"],
            "unstaged_diff_excerpt": ["diff --git a/tests/test_foo.py b/tests/test_foo.py", "@@ -2 +2 @@"],
            "changed_files": ["ops/foo.py", "tests/test_foo.py"],
            "file_buckets": {
                "source": ["ops/foo.py"],
                "tests": ["tests/test_foo.py"],
                "docs": [],
                "config": [],
            },
            "dirty": True,
        },
    )

    payload = gstack_phase1_entry.build_review_second_opinion_materials(prompt="帮我审一下这次改动。")

    assert "当前 review 证据包" in payload["artifact"]
    assert "变更分类: source=1 test=1 doc=0 config=0" in payload["artifact"]
    assert "staged diff 片段:" in payload["artifact"]
    assert "unstaged diff 片段:" in payload["artifact"]
    assert "ops/foo.py" in payload["artifact"]
    assert "tests/test_foo.py" in payload["artifact"]
    assert "git review evidence packet" in payload["extra_context"]


def test_build_ship_second_opinion_materials_include_release_gate_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(
        gstack_phase1_entry,
        "collect_git_worktree_snapshot",
        lambda repo_root=None: {  # type: ignore[no-untyped-def]
            "git_root": "/tmp/sample",
            "branch": "release/1.2.3",
            "head_commit": "def456 release commit",
            "status_lines": ["M app/main.py"],
            "staged_stat": "1 file changed, 4 insertions(+)",
            "unstaged_stat": "",
            "staged_diff_excerpt": ["diff --git a/app/main.py b/app/main.py", "@@ -5 +5 @@"],
            "unstaged_diff_excerpt": [],
            "changed_files": ["app/main.py"],
            "file_buckets": {
                "source": ["app/main.py"],
                "tests": [],
                "docs": [],
                "config": [],
            },
            "dirty": True,
        },
    )

    payload = gstack_phase1_entry.build_ship_second_opinion_materials(prompt="这个版本准备发版了。")

    assert "当前 ship 证据包" in payload["artifact"]
    assert "rollback anchor: def456 release commit" in payload["artifact"]
    assert "发布 gate 摘要:" in payload["artifact"]
    assert "工作树干净: 否" in payload["artifact"]
    assert "git ship evidence packet" in payload["extra_context"]


def test_build_second_opinion_package_uses_extracted_materials_before_template_fallback(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        gstack_phase1_entry,
        "extract_second_opinion_autofill_materials",
        lambda *args, **kwargs: {  # type: ignore[no-untyped-def]
            "artifact": "REAL_ARTIFACT",
            "current_judgment": "REAL_JUDGMENT",
            "extra_context": "REAL_CONTEXT",
        },
    )

    payload = gstack_phase1_entry.build_second_opinion_package(
        "claude-challenge",
        prompt="这个版本准备发版了，帮我判断是不是能发。",
        trigger_path=["ship"],
        source="followup",
    )

    assert payload["material_source"] == "extractor"
    assert payload["request"]["artifact"] == "REAL_ARTIFACT"
    assert payload["request"]["current_judgment"] == "REAL_JUDGMENT"
    assert payload["request"]["extra_context"] == "REAL_CONTEXT"


def test_build_second_opinion_package_builds_consult_materials_from_workflow_detection() -> None:
    workflow = gstack_phase1_entry.detect_workflow_path("这个方向值不值得做？从产品角度看呢？")

    payload = gstack_phase1_entry.build_second_opinion_package(
        "claude-consult",
        prompt="这个方向值不值得做？从产品角度看呢？",
        trigger_path=workflow["suggested_path"],
        source="followup",
        workflow_detection=workflow,
    )

    assert payload["material_source"] == "extractor"
    assert payload["template_id"] == "consult-tradeoff-check"
    assert "当前方案摘要" in payload["request"]["artifact"]
    assert "识别路径: plan-ceo-review" in payload["request"]["artifact"]
    assert "当前建议行动" in payload["request"]["artifact"]
    assert "workflow detection summary" in payload["request"]["extra_context"]


def test_build_second_opinion_main_thread_execution_prefers_workflow_entrypoint() -> None:
    packaged_request = gstack_phase1_entry.build_second_opinion_package(
        "claude-review",
        prompt="帮我审一下这次改动有没有问题。",
        trigger_path=["review"],
        source="followup",
    )

    execution = gstack_phase1_entry.build_second_opinion_main_thread_execution(
        skill="claude-review",
        packaged_request=packaged_request,
        prompt="帮我审一下这次改动有没有问题。",
        trigger_path=["review"],
    )

    assert execution["entrypoint"] == "workflow-second-opinion"
    assert execution["focus_question"] == packaged_request["request"]["question"]
    assert "workflow-second-opinion" in execution["suggested_command"]
    assert "帮我审一下这次改动有没有问题。" in execution["suggested_command"]


def test_run_second_opinion_routes_to_claude_runner(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run_claude(*, mode, prompt, model, settings_path, json_schema):  # type: ignore[no-untyped-def]
        captured["mode"] = mode
        captured["prompt"] = prompt
        captured["model"] = model
        captured["settings_path"] = settings_path
        captured["json_schema"] = json_schema
        return {
            "ok": True,
            "mode": mode,
            "model": model,
            "command": ["claude", "<prompt>"],
            "temp_home": "/tmp/fake-home",
            "env_keys": ["ANTHROPIC_AUTH_TOKEN"],
            "stdout": "READY",
            "structured_output": {
                "status": "ok",
                "question_or_focus": "focus",
                "key_judgment": "judgment",
                "difference_from_current_judgment": "delta",
                "recommended_next_step": "next",
            },
            "stderr": "",
            "returncode": 0,
        }

    monkeypatch.setattr(gstack_phase1_entry.claude_code_runner, "run_claude", fake_run_claude)

    payload = gstack_phase1_entry.run_second_opinion(
        "claude-challenge",
        question="What is the strongest counterargument against this release?",
        artifact="release decision",
        current_judgment="Ship tonight.",
        extra_context="The migration is hard to roll back.",
        trigger_path=["ship", "claude-challenge"],
        source_prompt="这个版本准备发版了，请你站在反方挑战一下。",
        model="sonnet",
        settings_path=tmp_path / "settings.json",
    )

    assert payload["ok"] is True
    assert payload["skill"] == "claude-challenge"
    assert payload["stage"] == "second-opinion"
    assert payload["runner"] == "claude_code_runner"
    assert payload["stdout"] == "READY"
    assert payload["request_envelope"]["schema_version"] == gstack_phase1_entry.SECOND_OPINION_REQUEST_SCHEMA_VERSION
    assert payload["response_contract"]["schema_version"] == gstack_phase1_entry.SECOND_OPINION_RESPONSE_SCHEMA_VERSION
    assert payload["structured_output"]["key_judgment"] == "judgment"
    assert "第二意见回收" in payload["main_thread_handoff"]
    assert "claude-challenge" in payload["main_thread_handoff"]
    assert captured["mode"] == "challenge"
    assert captured["model"] == "sonnet"
    assert captured["settings_path"] == tmp_path / "settings.json"
    assert captured["json_schema"]["required"] == gstack_phase1_entry.SECOND_OPINION_RESPONSE_SCHEMA["required"]
    assert "Ship tonight." in str(captured["prompt"])
    assert "ship" in str(captured["prompt"])
    assert "claude-challenge" in str(captured["prompt"])
    assert "这个版本准备发版了，请你站在反方挑战一下。" in str(captured["prompt"])


def test_run_second_opinion_from_prompt_reuses_detected_second_opinion_path(
    monkeypatch, tmp_path: Path
) -> None:
    def fake_run_second_opinion(
        skill, *, question, artifact, current_judgment, extra_context, trigger_path, source_prompt, model, settings_path
    ):  # type: ignore[no-untyped-def]
        return {
            "skill": skill,
            "ok": True,
            "returncode": 0,
            "structured_output": {
                "status": "ok",
                "question_or_focus": question,
                "key_judgment": "use claude-review",
                "difference_from_current_judgment": "no change",
                "recommended_next_step": "continue",
            },
            "request_envelope": {
                "trigger_path": trigger_path,
                "source_prompt": source_prompt,
            },
            "main_thread_handoff": f"第二意见回收\n- 触发路径: `{' -> '.join(trigger_path)}`",
        }

    monkeypatch.setattr(gstack_phase1_entry, "run_second_opinion", fake_run_second_opinion)

    payload = gstack_phase1_entry.run_second_opinion_from_prompt(
        prompt="先帮我审一下这次改动，然后再让 Claude 给一个第二意见。",
        question="Should we keep this rollout narrow?",
        artifact="rollout decision",
        current_judgment="Keep it narrow.",
        extra_context="UI-only change.",
        settings_path=tmp_path / "settings.json",
    )

    assert payload["skill"] == "claude-review"
    assert payload["workflow_detection"]["suggested_path"] == ["review", "claude-review"]
    assert payload["request_envelope"]["trigger_path"] == ["review", "claude-review"]
    assert "review -> claude-review" in payload["main_thread_handoff"]


def test_run_second_opinion_from_prompt_can_follow_up_from_review_path(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    def fake_run_second_opinion(
        skill, *, question, artifact, current_judgment, extra_context, trigger_path, source_prompt, model, settings_path
    ):  # type: ignore[no-untyped-def]
        captured["question"] = question
        captured["artifact"] = artifact
        captured["current_judgment"] = current_judgment
        captured["extra_context"] = extra_context
        return {
            "skill": skill,
            "ok": True,
            "returncode": 0,
            "structured_output": {
                "status": "ok",
                "question_or_focus": question,
                "key_judgment": "use claude-review as follow-up",
                "difference_from_current_judgment": "adds extra caution",
                "recommended_next_step": "continue review",
            },
            "request_envelope": {
                "trigger_path": trigger_path,
                "source_prompt": source_prompt,
            },
            "main_thread_handoff": f"第二意见回收\n- 触发路径: `{' -> '.join(trigger_path)}`",
        }

    monkeypatch.setattr(gstack_phase1_entry, "run_second_opinion", fake_run_second_opinion)

    payload = gstack_phase1_entry.run_second_opinion_from_prompt(
        prompt="帮我审一下这次改动有没有问题。",
        settings_path=tmp_path / "settings.json",
    )

    assert payload["skill"] == "claude-review"
    assert payload["workflow_detection"]["suggested_path"] == ["review"]
    assert payload["second_opinion_candidate"]["skill"] == "claude-review"
    assert payload["second_opinion_candidate"]["source"] == "followup"
    assert payload["packaged_request"]["template_id"] == "review-risk-scan"
    assert payload["packaged_request"]["material_source"] == "extractor"
    assert payload["packaged_request"]["autofilled_fields"] == [
        "question",
        "artifact",
        "current_judgment",
        "extra_context",
    ]
    assert payload["request_envelope"]["trigger_path"] == ["review"]
    assert captured["question"] == "这个改动或方案最大的回归风险、盲区或缺失验证是什么？"
    assert "当前 review 证据包" in str(captured["artifact"])
    assert "帮我审一下这次改动有没有问题。" in str(captured["artifact"])
    assert "缺失验证" in str(captured["current_judgment"]) or "工作树" in str(captured["current_judgment"])
    assert "git review evidence packet" in str(captured["extra_context"])
    assert "review" in payload["main_thread_handoff"]


def test_run_second_opinion_for_workflow_uses_followup_mapping(
    monkeypatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    def fake_run_second_opinion(
        skill, *, question, artifact, current_judgment, extra_context, trigger_path, source_prompt, model, settings_path
    ):  # type: ignore[no-untyped-def]
        captured["question"] = question
        captured["artifact"] = artifact
        captured["current_judgment"] = current_judgment
        captured["extra_context"] = extra_context
        return {
            "skill": skill,
            "ok": True,
            "returncode": 0,
            "structured_output": {
                "status": "ok",
                "question_or_focus": question,
                "key_judgment": "challenge the ship call",
                "difference_from_current_judgment": "needs more rollback evidence",
                "recommended_next_step": "delay ship",
            },
            "request_envelope": {
                "trigger_path": trigger_path,
                "source_prompt": source_prompt,
            },
            "main_thread_handoff": f"第二意见回收\n- 触发路径: `{' -> '.join(trigger_path)}`",
        }

    monkeypatch.setattr(gstack_phase1_entry, "run_second_opinion", fake_run_second_opinion)

    payload = gstack_phase1_entry.run_second_opinion_for_workflow(
        prompt="这个版本准备发版了，帮我判断是不是能发。",
        settings_path=tmp_path / "settings.json",
    )

    assert payload["skill"] == "claude-challenge"
    assert payload["workflow_detection"]["suggested_path"] == ["ship"]
    assert payload["second_opinion_candidate"]["skill"] == "claude-challenge"
    assert payload["second_opinion_candidate"]["source"] == "followup"
    assert payload["packaged_request"]["template_id"] == "challenge-pressure-test"
    assert payload["packaged_request"]["material_source"] == "extractor"
    assert payload["request_envelope"]["trigger_path"] == ["ship"]
    assert captured["question"] == "当前判断最强的反对意见或不上线理由是什么？"
    assert "当前 ship 证据包" in str(captured["artifact"])
    assert "这个版本准备发版了，帮我判断是不是能发。" in str(captured["artifact"])
    assert "候选发布对象" in str(captured["current_judgment"])
    assert "git ship evidence packet" in str(captured["extra_context"])


def test_build_second_opinion_main_thread_handoff_falls_back_when_runner_returns_no_structured_output() -> None:
    handoff = gstack_phase1_entry.build_second_opinion_main_thread_handoff(
        skill="claude-review",
        request_envelope=gstack_phase1_entry.build_second_opinion_request(
            "claude-review",
            question="Should we ship?",
            trigger_path=["review", "claude-review"],
        ),
        structured_output=None,
        stdout="RAW",
        stderr="",
    )

    assert "runner-returned-no-structured-output" in handoff
    assert "Should we ship?" in handoff
    assert "RAW" in handoff

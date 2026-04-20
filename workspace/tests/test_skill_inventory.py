from pathlib import Path
import re


SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"
EXPECTED_SKILLS = {
    "browse",
    "careful",
    "claude-challenge",
    "claude-consult",
    "claude-review",
    "document-release",
    "freeze",
    "retro",
    "ship",
    "unfreeze",
    "investigate",
    "review",
    "qa",
    "guard",
    "office-hours",
    "plan-ceo-review",
    "plan-eng-review",
    "wechat-gui-send",
}


def _read_skill(name: str) -> str:
    return (SKILL_ROOT / name / "SKILL.md").read_text(encoding="utf-8")


def _frontmatter(text: str) -> str:
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    assert match, "skill must start with YAML frontmatter"
    return match.group(1)


def test_expected_repo_skills_exist() -> None:
    present = {path.parent.name for path in SKILL_ROOT.glob("*/SKILL.md")}
    assert EXPECTED_SKILLS.issubset(present)


def test_gstack_repo_skills_have_required_metadata() -> None:
    for name in EXPECTED_SKILLS:
        text = _read_skill(name)
        frontmatter = _frontmatter(text)
        assert f"name: {name}" in frontmatter
        assert "description:" in frontmatter
        assert len(text.splitlines()) >= 20


def test_investigate_skill_is_root_cause_first() -> None:
    text = _read_skill("investigate")
    assert "root-cause" in text or "root cause" in text
    assert "facts" in text
    assert "inferences" in text
    assert "unknowns" in text


def test_review_qa_guard_skills_cover_their_core_postures() -> None:
    review = _read_skill("review")
    qa = _read_skill("qa")
    guard = _read_skill("guard")
    assert "Findings" in review
    assert "severity" in review
    assert "verified" in qa
    assert "passed" in qa
    assert "Risk classification" in guard
    assert "approval" in guard.lower()


def test_phase1_shared_protocol_exists_and_covers_core_contracts() -> None:
    text = (SKILL_ROOT / "_shared" / "gstack_phase1_protocols.md").read_text(
        encoding="utf-8"
    )
    assert "Ask-User Protocol" in text
    assert "Completion Status Protocol" in text
    assert "Escalation Protocol" in text
    assert "Output Contract" in text
    assert "DONE_WITH_CONCERNS" in text
    assert "NEEDS_CONTEXT" in text


def test_phase2_shared_protocol_exists_and_covers_core_contracts() -> None:
    text = (SKILL_ROOT / "_shared" / "gstack_phase2_protocols.md").read_text(
        encoding="utf-8"
    )
    assert "Verification Evidence Protocol" in text
    assert "Delivery Sync Protocol" in text
    assert "Retrospective Protocol" in text
    assert "Output Contract" in text


def test_phase3_shared_protocol_exists_and_covers_core_contracts() -> None:
    text = (SKILL_ROOT / "_shared" / "gstack_phase3_protocols.md").read_text(
        encoding="utf-8"
    )
    assert "Release Readiness Protocol" in text
    assert "Safety Posture Protocol" in text
    assert "Approval Boundary Protocol" in text
    assert "Output Contract" in text


def test_phase4_shared_protocol_exists_and_covers_core_contracts() -> None:
    text = (SKILL_ROOT / "_shared" / "gstack_phase4_protocols.md").read_text(
        encoding="utf-8"
    )
    assert "Second Opinion Trigger Protocol" in text
    assert "Disagreement Framing Protocol" in text
    assert "Challenge Protocol" in text
    assert "Consultation Protocol" in text
    assert "Output Contract" in text


def test_phase1_entry_skills_reference_shared_protocols() -> None:
    for name in ("office-hours", "plan-ceo-review", "plan-eng-review"):
        text = _read_skill(name)
        assert "../_shared/gstack_phase1_protocols.md" in text


def test_phase1_entry_skills_cover_distinct_roles() -> None:
    office_hours = _read_skill("office-hours")
    ceo_review = _read_skill("plan-ceo-review")
    eng_review = _read_skill("plan-eng-review")

    assert "Reframed problem" in office_hours
    assert "underspecified" in office_hours

    assert "Product judgment" in ceo_review
    assert "opportunity cost" in ceo_review

    assert "Engineering judgment" in eng_review
    assert "failure modes" in eng_review


def test_core_handoff_skills_share_common_output_slots() -> None:
    office_hours = _read_skill("office-hours")
    ceo_review = _read_skill("plan-ceo-review")
    eng_review = _read_skill("plan-eng-review")
    review = _read_skill("review")
    qa = _read_skill("qa")
    ship = _read_skill("ship")

    for text in (office_hours, ceo_review, eng_review, review, qa, ship):
        assert "Current stage" in text
        assert "Next input needed" in text
        assert "Recommended next step" in text

    assert "Review judgment" in review
    assert "QA judgment" in qa
    assert "Readiness judgment" in ship


def test_phase2_skills_reference_shared_protocols() -> None:
    for name in ("browse", "document-release", "retro"):
        text = _read_skill(name)
        assert "../_shared/gstack_phase2_protocols.md" in text


def test_phase3_skills_reference_shared_protocols() -> None:
    for name in ("ship", "careful", "freeze", "unfreeze"):
        text = _read_skill(name)
        assert "../_shared/gstack_phase3_protocols.md" in text


def test_phase4_skills_reference_shared_protocols() -> None:
    for name in ("claude-review", "claude-challenge", "claude-consult"):
        text = _read_skill(name)
        assert "../_shared/gstack_phase4_protocols.md" in text


def test_phase2_skills_cover_distinct_roles() -> None:
    browse = _read_skill("browse")
    document_release = _read_skill("document-release")
    retro = _read_skill("retro")

    assert "Playwright" in browse
    assert "Evidence collected" in browse

    assert "release-facing" in document_release or "release-facing" in document_release.lower()
    assert "Verified changes to communicate" in document_release

    assert "retrospective" in retro.lower()
    assert "What went well" in retro


def test_phase3_skills_cover_distinct_roles() -> None:
    ship = _read_skill("ship")
    careful = _read_skill("careful")
    freeze = _read_skill("freeze")
    unfreeze = _read_skill("unfreeze")

    assert "Readiness judgment" in ship
    assert "release-readiness" in ship or "ready to go out" in ship

    assert "Risk summary" in careful
    assert "Careful posture boundaries" in careful

    assert "Freeze scope" in freeze
    assert "Unfreeze condition" in freeze

    assert "Unfreeze judgment" in unfreeze
    assert "Freeze gate reviewed" in unfreeze


def test_phase4_skills_cover_distinct_roles() -> None:
    claude_review = _read_skill("claude-review")
    claude_challenge = _read_skill("claude-challenge")
    claude_consult = _read_skill("claude-consult")

    assert "Claude assessment" in claude_review
    assert "Agreement or disagreement" in claude_review

    assert "Strongest counterargument" in claude_challenge
    assert "Evidence needed to resolve the challenge" in claude_challenge

    assert "Claude perspective" in claude_consult
    assert "What changed versus the current framing" in claude_consult


def test_wechat_gui_send_skill_mentions_prepare_then_confirm_runtime() -> None:
    skill = _read_skill("wechat-gui-send")

    assert "prepare -> confirm send" in skill
    assert "Computer Use" in skill
    assert "Use the WeChat search field to paste the recipient name for fast targeting." in skill

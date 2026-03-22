---
name: claude-review
description: Use when an existing plan, diff, judgment, or decision would benefit from a focused Claude-style second opinion before the main Codex thread commits to it.
---

# Claude Review

Use this skill when the main thread already has a concrete judgment, plan, or change list, and another review pass would reduce blind spots.

Before using this skill, read `../_shared/gstack_phase4_protocols.md`.

## Goal

Collect a focused second-opinion review without giving away ownership of the final decision.

## Workflow

1. Name the exact thing being re-reviewed:
   - plan
   - diff
   - design judgment
   - release decision
2. State the question the second opinion should answer.
3. Separate:
   - current Codex judgment
   - what the second opinion agrees with
   - what it disputes or adds
4. Reconcile the result back into the parent thread's judgment.

## Rules

1. Do not use this skill before there is something concrete to review.
2. Keep the question narrow enough that disagreement is meaningful.
3. Treat the second opinion as evidence, not as the final owner.
4. Be explicit about changed confidence or changed recommendation.

## Output Contract

Return in this order:

1. Status
2. Question handed to the second opinion
3. Claude assessment
4. Agreement or disagreement with the current judgment
5. Recommended next step

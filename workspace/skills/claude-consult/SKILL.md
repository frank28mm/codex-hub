---
name: claude-consult
description: Use when the main thread needs a lightweight Claude-style advisory perspective, alternative framing, or tradeoff consult without asking for a full re-review.
---

# Claude Consult

Use this skill when the task would benefit from a short advisory consult rather than a full review or adversarial challenge.

Before using this skill, read `../_shared/gstack_phase4_protocols.md`.

## Goal

Bring in a second-opinion consult that reframes the problem, sharpens tradeoffs, or suggests a better option shape.

## Workflow

1. State the advisory question.
2. Name the current frame or option set.
3. Ask for:
   - alternative framing
   - tradeoff clarification
   - missing option
   - advisory recommendation
4. Compare the consult result with the current plan.
5. Decide whether the parent thread changes course, scope, or sequencing.

## Rules

1. Keep the consult lightweight and decision-oriented.
2. Prefer option shaping over restating the same plan.
3. Do not use this skill as a substitute for root-cause work or full review.
4. End with what changed in the main thread's recommendation.

## Output Contract

Return in this order:

1. Status
2. Consult question
3. Claude perspective
4. What changed versus the current framing
5. Recommended next step

---
name: office-hours
description: Use when a request is still scattered or underspecified and Codex should reframe the problem before planning or implementation, especially for new ideas, ambiguous requests, or tasks that need clearer goals, scope, and success criteria.
---

# Office Hours

Use this skill when the user is pointing at a real task, but the task is not yet framed tightly enough to judge or execute well.

Before using this skill, read `../_shared/gstack_phase1_protocols.md`.

## Goal

Turn a vague or overloaded request into a clear problem statement, a manageable decision surface, and a concrete next move.

## Workflow

1. Reframe the request:
   - what the user is actually trying to achieve
   - what is still ambiguous
   - what constraints are already visible
2. Separate:
   - objective
   - scope
   - assumptions
   - open questions
3. Reduce the problem:
   - identify the smallest useful framing
   - cut tangents and premature solutioning
4. If there are multiple plausible paths, present a short option set with tradeoffs.
5. Recommend the most sensible next path:
   - stay in `office-hours`
   - hand off to `plan-ceo-review`
   - hand off to `plan-eng-review`
   - or proceed directly if the problem is now clear enough

## Rules

1. Do not jump into execution while the task is still underspecified.
2. Prefer clarity over completeness.
3. Ask only the smallest clarification that materially changes the framing.
4. If the user mixes product, business, and engineering concerns, separate them explicitly.
5. End with a concrete next move, not just a cleaner description.

## Output Contract

Return in this order:

1. Status
2. What I understood
3. Reframed problem
4. Open questions or constraints
5. Recommended next step

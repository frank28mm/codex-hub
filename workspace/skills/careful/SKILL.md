---
name: careful
description: Use when a task can proceed but the current risk, fragility, or ambiguity calls for a slower, narrower, more explicit execution posture than the normal flow.
---

# Careful

Use this skill when the task should keep moving, but only under tighter boundaries and more explicit checkpoints.

Before using this skill, read `../_shared/gstack_phase3_protocols.md`.

## Goal

Shift a task into a cautious execution posture without fully freezing it.

## Workflow

1. Name the current risk:
   - destructive potential
   - ambiguity
   - missing verification
   - external visibility
2. Narrow the work:
   - smaller scope
   - preview or dry-run first
   - explicit checkpoints
3. State what is allowed and what is deferred.
4. Recommend the next safe move under this posture.

## Rules

1. Prefer reversible steps.
2. Make boundaries concrete, not emotional.
3. If a full freeze is better, say so instead of pretending "careful" is enough.
4. Keep the posture temporary and goal-directed.

## Output Contract

Return in this order:

1. Status
2. Risk summary
3. Careful posture boundaries
4. What is safe to do now
5. Recommended next step

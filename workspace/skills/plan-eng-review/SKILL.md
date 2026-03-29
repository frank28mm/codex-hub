---
name: plan-eng-review
description: Use when the main question is technical feasibility, architecture, delivery risk, or testability, and Codex should review a plan from an engineering-lead perspective before implementation or commitment.
---

# Plan Eng Review

Use this skill when the user needs an engineering-lead style review of how a proposal would be built, where it can fail, and what would make it safe to execute.

Before using this skill, read `../_shared/gstack_phase1_protocols.md`.

## Goal

Produce a technical judgment about feasibility, architecture boundaries, risk, and the minimum safe delivery path.

## Workflow

1. Clarify the technical target:
   - what needs to be built or changed
   - what systems are involved
   - what constraints already exist
2. Review the plan through four lenses:
   - architecture fit
   - failure modes
   - test and verification surface
   - rollout or operational risk
3. Identify the narrowest safe implementation shape:
   - what can be staged
   - what should be isolated
   - what must be validated first
4. Distinguish:
   - technically straightforward
   - feasible but risky
   - blocked by missing context or infrastructure
5. Recommend the next engineering move:
   - proceed directly
   - split scope
   - investigate first
   - add guardrails
   - reject current plan shape

## Rules

1. Prefer concrete failure modes over abstract architecture language.
2. Call out hidden coupling and irreversible choices explicitly.
3. Treat testability and rollout safety as first-class concerns.
4. If the plan is feasible only with strong assumptions, list them clearly.
5. End with the smallest technically credible next step.

## Output Contract

Return in this order:

1. Current stage
2. What I understood
3. Engineering judgment
4. Main risks or architectural concerns
5. Stage conclusion
6. Why this can move to execution, investigate first, or stop here
7. Next input needed
8. Recommended next step

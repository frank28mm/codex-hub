---
name: claude-challenge
description: Use when the user wants a Claude-style adversarial second opinion that pressure-tests a plan, argument, or release decision with the strongest counterargument.
---

# Claude Challenge

Use this skill when the right next move is not another balanced review, but a deliberate challenge to the current plan or claim.

Before using this skill, read `../_shared/gstack_phase4_protocols.md`.

## Goal

Pressure-test the current judgment by surfacing the strongest plausible counterargument and the evidence that would overturn it.

## Workflow

1. State the claim or plan to challenge.
2. State why this challenge matters now:
   - high downside
   - weak assumptions
   - release pressure
   - hidden tradeoffs
3. Ask for the strongest counter-case.
4. Distill:
   - the strongest counterargument
   - the most dangerous assumption
   - the evidence that would settle it
5. Feed the result back into the parent thread's decision.

## Rules

1. Do not ask for a polite summary; ask for the sharpest useful challenge.
2. Keep the challenge tied to evidence, not rhetoric.
3. Name what would actually change the current decision.
4. End with a concrete recommendation, not just a warning.

## Output Contract

Return in this order:

1. Status
2. Claim being challenged
3. Strongest counterargument
4. Evidence needed to resolve the challenge
5. Recommended next step

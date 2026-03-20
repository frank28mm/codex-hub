---
name: review
description: Use when the task is to review code, a diff, a feature branch, or a change list and Codex should focus on findings, regressions, risks, and missing validation rather than implementation.
---

# Review

Use this skill for review work, not for feature building.

## Goal

Judge whether a change is safe, coherent, and ready to merge or ship.

## Workflow

1. Establish review scope:
   - files changed
   - stated intent
   - risk surface
2. Inspect behavior, not just syntax:
   - regressions
   - state transitions
   - edge cases
   - missing tests
   - contract drift
3. Verify the most important paths with targeted tests or static checks when practical.
4. Produce findings ordered by severity.

## Rules

1. Findings come first.
2. Each finding should explain:
   - what is wrong
   - why it matters
   - where it is
3. Prefer concrete behavioral language over style opinions.
4. If there are no findings, state that explicitly and still mention residual risk or test gaps.

## Output Contract

Return:

1. Findings
2. Open questions or assumptions
3. Brief change summary only if useful


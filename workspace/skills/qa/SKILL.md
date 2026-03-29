---
name: qa
description: Use when implementation is done or nearly done and Codex should verify behavior with targeted tests, smoke checks, negative cases, and acceptance-oriented validation before sign-off.
---

# QA

Use this skill after changes exist and need verification.

## Goal

Confirm whether the change works as intended, and identify what is still unverified.

When the task involves a real page, UI flow, or browser path, `qa` should treat `browse` output as formal evidence, not as optional background color.

## Workflow

1. Define the acceptance surface:
   - primary success path
   - likely failure path
   - regression-sensitive areas
2. Run the smallest useful validation set:
   - unit tests
   - smoke checks
   - compile or lint checks
   - negative-path checks
   - browser evidence from `browse`, when the change touches UI, front-end flow, or page behavior
3. Record actual results, not assumed results.
4. Call out untested areas explicitly.

## Rules

1. Prefer targeted verification over broad expensive suites unless the risk justifies it.
2. Distinguish:
   - passed
   - failed
   - not run
3. If a failure is environmental rather than product logic, say that clearly.
4. Do not describe verification as complete when important paths were skipped.
5. For front-end or page-flow acceptance, do not mark QA complete if browser evidence is missing or stale.
6. When `browse` already ran, explicitly reuse its snapshot, screenshot, console, and network findings in the QA conclusion.

## Output Contract

Return:

1. Current stage
2. What was verified
3. What passed
4. What failed
5. What was not verified
6. Browser evidence used
7. QA judgment
8. Why this can move to `ship`, return to `fix`, or needs more verification
9. Next input needed
10. Recommended next step
11. Release or merge recommendation

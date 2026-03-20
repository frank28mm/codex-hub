---
name: qa
description: Use when implementation is done or nearly done and Codex should verify behavior with targeted tests, smoke checks, negative cases, and acceptance-oriented validation before sign-off.
---

# QA

Use this skill after changes exist and need verification.

## Goal

Confirm whether the change works as intended, and identify what is still unverified.

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

## Output Contract

Return:

1. What was verified
2. What passed
3. What failed
4. What was not verified
5. Release or merge recommendation


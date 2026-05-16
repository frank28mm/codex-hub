---
name: quality-gate
description: Use when generated creator-workflow content must be checked for CTA drift, unsupported claims, provenance leaks, evidence mismatch, and other release-blocking issues before writeback or delivery.
---

# Quality Gate

Use this skill after draft content exists and before it is written back, projected, or delivered.

## Goal

Decide whether the generated content is safe enough to move forward, and explain any blocking issues clearly.

## Inputs

- creator brief
- generated draft
- creator truth / product truth / transaction truth constraints

## Workflow

1. Check CTA alignment.
2. Check unsupported claims or over-promising language.
3. Check provenance leaks:
   - tool names
   - internal field names
   - schema ids
4. Check evidence presence and mismatch.
5. Return a pass/fail result with explicit issue list.

## Rules

1. Findings come before polish.
2. If the draft fails, say exactly why it should not move forward.
3. Do not silently fix the draft inside this skill.
4. This skill is a gate, not a rewriting stage.

## Output Contract

Return:

1. Current stage
2. Draft status
3. What passed
4. What failed
5. Blocking issues
6. Why it can or cannot move to writeback
7. Recommended next step

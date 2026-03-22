---
name: guard
description: Use when work involves risky or potentially destructive actions and Codex should stay in a constrained, approval-aware posture, including read-only triage, dry-run-first execution, and explicit risk framing.
---

# Guard

Use this skill when safety posture matters as much as task completion.

## Goal

Reduce accidental damage while still moving the task forward.

## Workflow

1. Classify the action:
   - read-only
   - low-risk mutation
   - destructive or externally visible mutation
2. Prefer the safest equivalent path first:
   - inspection
   - preview
   - dry run
   - targeted mutation
3. State the exact boundary:
   - what will change
   - what will not change
   - what approval is needed
4. After approval, execute only the approved scope.

## Rules

1. Do not blur diagnosis and mutation.
2. For risky actions, state the command or effect before execution.
3. Prefer reversible operations over irreversible ones.
4. If the environment blocks an action, explain the missing permission and ask for the next authorization step instead of treating it as task failure.

## Output Contract

Return:

1. Risk classification
2. Proposed safe path
3. Required approval, if any
4. Executed scope
5. Residual risk


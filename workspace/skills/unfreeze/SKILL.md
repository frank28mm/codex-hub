---
name: unfreeze
description: Use when a task or release was previously frozen and Codex should judge whether the freeze condition has been satisfied and what the next safe mutation step is.
---

# Unfreeze

Use this skill when a prior freeze may be lifted and the task needs a safe path back into active execution.

Before using this skill, read `../_shared/gstack_phase3_protocols.md`.

## Goal

Confirm whether the freeze condition has actually been met and define the next safe step after unfreezing.

## Workflow

1. Restate the original freeze scope and gate.
2. Check whether the gate is now satisfied:
   - approval received
   - evidence collected
   - verification passed
   - risk narrowed
3. Distinguish:
   - safe to unfreeze now
   - still frozen
   - partially unfreeze with constraints
4. Recommend the next concrete step after unfreeze.

## Rules

1. Do not unfreeze on hope or momentum.
2. If only part of the gate is satisfied, say what remains frozen.
3. Keep the first post-unfreeze step small and explicit.
4. If another posture is more appropriate, say so.

## Output Contract

Return in this order:

1. Status
2. Freeze gate reviewed
3. Unfreeze judgment
4. Remaining limits or caveats
5. Recommended next step

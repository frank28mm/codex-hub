---
name: freeze
description: Use when the task should temporarily stop writes, releases, or risky changes until a condition, approval, or missing evidence is resolved.
---

# Freeze

Use this skill when the right move is to stop mutation or release activity until a clear gate is satisfied.

Before using this skill, read `../_shared/gstack_phase3_protocols.md`.

## Goal

Put the work into a controlled no-mutation posture and make the unblock condition explicit.

## Workflow

1. Name what is being frozen:
   - writes
   - release activity
   - deployment
   - a specific risky surface
2. State why freezing is safer than continuing.
3. State what remains allowed:
   - inspection
   - diagnosis
   - planning
   - verification prep
4. Define the exact unfreeze condition.

## Rules

1. Be explicit about what is blocked.
2. Do not use freeze as a vague delay tactic.
3. Keep read-only work available whenever possible.
4. End with a concrete unfreeze condition.

## Output Contract

Return in this order:

1. Status
2. Freeze scope
3. Why the freeze is necessary
4. Allowed activity during freeze
5. Unfreeze condition

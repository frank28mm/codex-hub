---
name: document-release
description: Use when implementation or review is done and Codex should synchronize release notes, user-facing docs, or internal delivery documentation from verified changes without inventing unsupported claims.
---

# Document Release

Use this skill when the work is already implemented or verified and the next job is to bring documentation or release-facing material up to date.

Before using this skill, read `../_shared/gstack_phase2_protocols.md`.

## Goal

Turn verified work into clear release-facing or internal documentation that matches reality.

## Workflow

1. Clarify the audience:
   - internal operators
   - users
   - reviewers
   - release consumers
2. Gather the verified source of truth:
   - accepted code changes
   - tests
   - reports
   - validated behavior
3. Sync the smallest necessary documentation surface:
   - release note
   - usage guide
   - operator instruction
   - change summary
4. Separate:
   - confirmed changes
   - caveats or incomplete validation
   - follow-up items that should not be disguised as done
5. Recommend the next move:
   - publish
   - request missing validation
   - hand off to `ship`

## Rules

1. Never write beyond what has been verified.
2. Prefer a sharp, truthful summary over exhaustive prose.
3. If the right source of truth is unclear, stop and name the missing source.
4. Do not let release copy drift away from actual behavior.
5. End with what is ready to communicate now.

## Output Contract

Return in this order:

1. Status
2. Scope of documentation sync
3. Verified changes to communicate
4. Caveats or missing validation
5. Recommended next step

---
name: browse
description: Use when the task requires verifying a real page, UI flow, browser behavior, or front-end path with live evidence, especially when screenshots, interaction traces, or observed behavior matter more than code inspection alone.
---

# Browse

Use this skill when the user needs a real browser pass through a page or flow, not just a static code read.

Before using this skill, read:

- `../_shared/gstack_phase2_protocols.md`
- `$CODEX_HOME/skills/playwright/SKILL.md`

## Goal

Verify a real browser path, collect concrete evidence, and separate confirmed behavior from assumptions.

For front-end or page-flow work, `browse` is normally the first half of a `browse -> qa` chain, not a standalone finish line.

## Workflow

1. Clarify the target:
   - what page or flow matters
   - what the expected behavior is
   - what should count as success or failure
2. Use the Playwright skill to reproduce the smallest useful path in a real browser.
3. Capture evidence:
   - snapshot
   - screenshot
   - console or network signal
   - trace when useful
4. Distinguish:
   - verified behavior
   - broken behavior
   - still-unknown areas
5. Package the browser evidence so the next stage can reuse it:
   - page snapshot
   - screenshot
   - console or network findings
   - exact path reproduced
   - unresolved gaps
6. Recommend the next move:
   - hand off to `qa`
   - hand off to `review`
   - hand off to `document-release`
   - or stop if the needed behavior is already verified

## Rules

1. Prefer a narrow browser path over broad exploratory clicking.
2. Do not claim a UI issue is fixed without live evidence.
3. If refs go stale or the page changes significantly, re-snapshot before continuing.
4. Use headed browser mode when visual confirmation matters.
5. For front-end verification, prefer handing off to `qa` instead of declaring the work accepted inside `browse`.
6. End with evidence and a concrete next step.

## Output Contract

Return in this order:

1. Status
2. What I checked
3. Verified behavior and failures
4. Evidence collected
5. QA-ready evidence handoff
6. Recommended next step

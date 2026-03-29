---
name: ship
description: Use when work is believed to be ready for release, submission, handoff, or formal delivery, and Codex should judge readiness, surface missing gates, and define the smallest safe path to ship.
---

# Ship

Use this skill when the question is no longer "how do we build it?" but "is it actually ready to go out?"

Before using this skill, read:

- `../_shared/gstack_phase2_protocols.md`
- `../_shared/gstack_phase3_protocols.md`

## Goal

Turn a nearly-finished change into a clear release-readiness judgment and the smallest safe shipping path.

## Workflow

1. Clarify the release scope:
   - what is shipping
   - to whom
   - through what channel
2. Check the readiness surface:
   - implementation status
   - review or QA status
   - known caveats
   - rollback or recovery expectations
3. Distinguish:
   - ready now
   - ready with concerns
   - not ready
4. If not ready, say what gate is still missing.
5. Recommend the next move:
   - ship now
   - gate on one missing validation
   - freeze changes first
   - return to `qa` or `review`

## Rules

1. Do not let enthusiasm replace release evidence.
2. Prefer a narrow shipping path over a broad launch story.
3. Call out externally visible risk explicitly.
4. Treat rollback confidence as part of readiness.
5. End with a single clearest shipping recommendation.

## Output Contract

Return in this order:

1. Current stage
2. Release scope
3. Readiness judgment
4. Remaining gates or caveats
5. Why this can ship now, should return to `qa` / `review`, or should stop
6. Next input needed
7. Recommended next step

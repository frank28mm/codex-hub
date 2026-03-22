# Gstack Phase 2 Shared Protocols

These protocols are shared by the Phase 2 workflow skills:

- `browse`
- `document-release`
- `retro`

Use them to keep verification, delivery sync, and retrospective work consistent.

## Verification Evidence Protocol

When a skill claims a page, flow, or behavior was verified:

1. Say what surface was checked.
2. Say what method was used:
   - browser reproduction
   - smoke check
   - screenshot or trace
   - log or console inspection
3. Separate:
   - verified behavior
   - failed behavior
   - unknown behavior
4. Do not claim something is verified if it was only inferred from code or prior discussion.

## Delivery Sync Protocol

When syncing documentation or release-facing material:

1. Treat code, tests, and accepted reports as the source of truth.
2. Do not invent shipped behavior that was not verified.
3. Distinguish:
   - confirmed changes
   - caveats or limitations
   - open items that still need validation
4. Prefer the smallest doc or release update that keeps downstream readers aligned.

## Retrospective Protocol

When writing a retrospective:

1. Keep the timebox or scope explicit.
2. Separate outcome facts from interpretation.
3. Capture:
   - what worked
   - what created friction
   - what should change next time
4. End with a small number of concrete follow-up changes, not a vague summary.

## Output Contract

Each Phase 2 skill should return:

1. `Status`
2. `Scope reviewed or synchronized`
3. `Verified facts or evidence`
4. `Gaps, caveats, or lessons`
5. `Recommended next step`

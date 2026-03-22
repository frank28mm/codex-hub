# Gstack Phase 3 Shared Protocols

These protocols are shared by the Phase 3 workflow skills:

- `ship`
- `careful`
- `freeze`
- `unfreeze`

Use them to keep high-risk delivery and posture control consistent.

## Release Readiness Protocol

Before a skill claims something is ready to ship:

1. Name the exact scope being released or handed off.
2. State the supporting evidence:
   - tests
   - review or QA status
   - known caveats
3. Separate:
   - ready now
   - ready with concerns
   - not ready
4. Do not hide missing validation behind optimistic wording.

## Safety Posture Protocol

When a skill changes posture:

1. State the posture explicitly:
   - careful
   - freeze
   - unfreeze
2. State what is allowed.
3. State what is blocked.
4. State what condition or approval changes the posture.

## Approval Boundary Protocol

For risky or externally visible actions:

1. Say what will change.
2. Say what external surface is touched:
   - git remote
   - release target
   - deployment target
   - user-facing output
3. Ask for or confirm approval before mutation if the current posture requires it.
4. Keep the parent thread as the final control point.

## Output Contract

Each Phase 3 skill should return:

1. `Status`
2. `Current posture or release scope`
3. `Readiness or safety judgment`
4. `Open risks, caveats, or blocked actions`
5. `Recommended next step`

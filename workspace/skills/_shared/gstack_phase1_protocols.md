# Gstack Phase 1 Shared Protocols

These protocols are shared by the first Phase 1 workflow skills:

- `office-hours`
- `plan-ceo-review`
- `plan-eng-review`

Use them to keep the three skills compatible and predictable.

## Ask-User Protocol

Ask the user only when the missing information changes the decision materially.

Use this order:

1. State what is missing.
2. Explain why it changes the recommendation.
3. Ask for the smallest useful clarification.
4. If a reasonable assumption exists, offer it first.

Default categories:

- `must ask`
  - target user or buyer is unclear
  - success metric is unclear
  - hard deadline or hard constraint is unclear
  - the user must choose between materially different paths
- `may assume`
  - naming
  - draft structure
  - illustrative examples
  - lightweight prioritization when stakes are low

## Completion Status Protocol

Every result should end in exactly one status:

- `DONE`
  - the requested review or reframing is complete
  - a recommendation and next step are clear
- `DONE_WITH_CONCERNS`
  - a useful result exists, but risks or missing evidence remain
- `BLOCKED`
  - the next useful move depends on missing information or external input
- `NEEDS_CONTEXT`
  - the request is too underspecified to frame responsibly without more context

## Escalation Protocol

Escalate when:

1. The recommendation would commit the user to a high-cost or hard-to-reverse direction.
2. The analysis reveals policy, legal, payment, security, or production-risk implications.
3. The user appears to be mixing product, business, and engineering decisions that must be separated.
4. The current phase should hand off to another workflow skill.

Escalation should say:

1. what changed
2. why the current layer is no longer enough
3. which next layer is recommended

## Output Contract

Each Phase 1 skill should return:

1. `Status`
2. `What I understood`
3. `Decision or reframing result`
4. `Key risks or concerns`
5. `Recommended next step`

Optional sections may be added when clearly useful, but these five anchors should remain stable.

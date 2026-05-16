---
name: channel-brief
description: Use when Codex needs to convert a strategy packet into a channel-specific creator brief, including content type, structure hint, tool assignment, and draft constraints.
---

# Channel Brief

Use this skill after creator truth, product truth, transaction truth, audience stage, and claim/evidence are already resolved.

## Goal

Assemble a channel-ready brief that can be handed to `claude-writing`, an image/video provider, or another generation tool without re-deciding business truth.

## Inputs

- strategy packet
- `control/creator_brief.schema.json`
- `skills/claude-writing/references/platform-style-profiles.yaml`

## Workflow

1. Resolve the channel-specific content type.
2. Pull the matching style profile if one exists.
3. Assign:
   - writing tool
   - media tool
   - writeback target
4. Produce a complete brief that already conforms to the schema.

## Rules

1. Do not rewrite creator or transaction truth here.
2. Do not skip required schema fields.
3. Prefer the smallest useful brief over an overstuffed prompt packet.
4. Treat the brief as a contract, not prose.

## Output Contract

Return:

1. Current stage
2. Content type
3. Structure hint
4. Tool assignment
5. Required schema fields filled
6. Channel-specific constraints
7. Recommended next step

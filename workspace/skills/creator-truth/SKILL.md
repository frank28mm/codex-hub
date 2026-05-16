---
name: creator-truth
description: Use when Codex needs to resolve who is speaking in the creator workflow, what tone and identity stack should be used, what credibility sources are allowed, and what public boundary must not be crossed.
---

# Creator Truth

Use this skill inside the Codex Hub creator workflow when the system needs to decide the speaking identity before any product claim, CTA, or channel rewrite begins.

## Goal

Resolve the creator-side truth layer so downstream strategy, writing, and review steps all inherit the same identity, tone, and public-safety boundary.

## Inputs

- `control/creator_workflow.yaml`
- `creator_truth_id`
- optional `persona_version`

## Workflow

1. Read the matching `creator_truth` object from `control/creator_workflow.yaml`.
2. Extract:
   - primary role
   - secondary roles
   - tone traits
   - working beliefs
   - credibility sources
   - public boundaries
3. Normalize the output so later stages can reuse it without reinterpretation.
4. If a requested tone or identity conflicts with the truth layer, return the truth-layer version rather than inventing a new one.

## Rules

1. Do not invent credentials, life history, or case evidence.
2. Do not rewrite product value or CTA here.
3. If the request would leak private or unapproved evidence, surface the boundary explicitly.
4. The output of this skill is a truth-resolution step, not final copy.

## Output Contract

Return in this order:

1. Current stage
2. Resolved creator truth id
3. Persona version
4. Identity stack
5. Tone traits
6. Credibility sources allowed
7. Public boundary notes
8. Recommended next step

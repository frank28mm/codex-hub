---
name: product-truth
description: Use when Codex needs to resolve which product is being communicated in the creator workflow, its canonical positioning, current business goal, and the allowed evidence sources for that product.
---

# Product Truth

Use this skill when the creator workflow must decide what is being sold or explained before writing starts.

## Goal

Resolve the product-side truth layer so later steps inherit the same positioning, goals, claims, and evidence boundaries.

## Inputs

- `control/creator_workflow.yaml`
- `product`

## Workflow

1. Read the matching `product_truths` entry.
2. Extract:
   - display name
   - canonical positioning
   - primary audiences
   - current business goal
   - core claims
   - evidence sources
   - default transaction truth id
3. Reduce these into a stable product truth packet for the strategy layer.

## Rules

1. Do not invent new products or ad hoc aliases.
2. Do not override transaction truth here.
3. If multiple claims exist, keep them as options rather than silently picking a different one.
4. Treat evidence lists as hard sources, not inspiration.

## Output Contract

Return:

1. Current stage
2. Resolved product id
3. Display name
4. Canonical positioning
5. Current business goal
6. Core claim options
7. Evidence sources allowed
8. Recommended next step

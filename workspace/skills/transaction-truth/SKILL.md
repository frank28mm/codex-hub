---
name: transaction-truth
description: Use when Codex needs to resolve the active transaction contract for a product in the creator workflow, including price, refund boundary, validation period, CTA, and audience gate.
---

# Transaction Truth

Use this skill when the creator workflow must decide how an offer is presented, not just what the product is.

## Goal

Freeze the transaction-side truth so price, refund language, validation period, CTA, and audience gate stay consistent across channels.

## Inputs

- `control/creator_workflow.yaml`
- `transaction_truth_id` or `product`

## Workflow

1. Resolve the transaction truth by explicit id, or by the product’s default transaction truth id.
2. Extract:
   - price or quote state
   - validation period
   - refund policy
   - CTA text
   - relationship entry
   - audience gate
3. Return a stable transaction packet for the brief builder.

## Rules

1. Never invent pricing when the truth says `not_fixed` or `quote_required`.
2. Do not weaken refund or validation boundary language.
3. Do not turn transaction truth into final copy by itself.
4. If the current truth is still draft, surface that clearly.

## Output Contract

Return:

1. Current stage
2. Resolved transaction truth id
3. Product id
4. Price state or price text
5. Validation period
6. Refund policy
7. CTA text
8. Recommended next step

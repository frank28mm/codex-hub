---
name: audience-stage
description: Use when Codex needs to resolve who the current content is for and what the relationship stage is, so the creator workflow can choose the right goal, format, and next move.
---

# Audience Stage

Use this skill when the creator workflow needs a stable answer to “给谁看” and “现在处于什么关系阶段”.

## Goal

Map product context and signal context into a stable audience + relationship-stage decision.

## Inputs

- `control/creator_workflow.yaml`
- `product`
- optional current signal context
- optional explicit audience or relationship-stage hint

## Workflow

1. Start from product primary audiences.
2. If explicit signal context exists, decide the most plausible relationship stage.
3. Reduce the decision into:
   - audience
   - relationship stage
   - problem frame
4. Return the decision as a strategy input, not final messaging.

## Rules

1. Do not infer intimate trust if the evidence only supports awareness.
2. Do not decide CTA here.
3. Treat `institutional` as a distinct stage rather than folding it into consumer trust stages.
4. Keep the result simple and reusable.

## Output Contract

Return:

1. Current stage
2. Product
3. Resolved audience
4. Resolved relationship stage
5. Problem frame
6. Why this stage is appropriate
7. Recommended next step

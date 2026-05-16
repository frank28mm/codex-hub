---
name: claim-evidence
description: Use when Codex needs to decide what core claim to lead with, what evidence to cite, and what emotional tone to use in the creator workflow without inventing unsupported proof.
---

# Claim Evidence

Use this skill when the creator workflow needs to decide “这轮打什么、用什么证据、带什么情绪”.

## Goal

Choose the smallest credible claim and the evidence anchors that can safely support it.

## Inputs

- creator truth packet
- product truth packet
- transaction truth packet
- audience + relationship stage

## Workflow

1. Pick the core claim that best matches the current goal and relationship stage.
2. Select the evidence anchors that are both real and relevant.
3. Pick the intended emotion:
   - trust
   - clarity
   - urgency
   - hope
   - confidence
   - reflection
4. Return a strategy packet that later writing tools can consume directly.

## Rules

1. Do not fabricate proof.
2. Do not use private evidence that violates creator boundary rules.
3. Prefer specific evidence anchors over broad slogans.
4. If evidence is thin, reduce the claim instead of stretching it.

## Output Contract

Return:

1. Current stage
2. Core claim
3. Evidence to use
4. Evidence to avoid
5. Emotion target
6. Why this combination is credible
7. Recommended next step

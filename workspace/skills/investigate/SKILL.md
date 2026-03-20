---
name: investigate
description: Use when behavior is wrong or unclear and Codex needs a root-cause-first diagnosis before changing code, including flaky bugs, outages, regressions, data mismatches, failed automations, and unclear runtime states.
---

# Investigate

Use this skill when the right next step is diagnosis, not implementation.

## Goal

Produce a clear root-cause-oriented investigation result before proposing or applying fixes.

## Workflow

1. Frame the problem:
   - what is expected
   - what is actually happening
   - where it happens
   - how severe it is
2. Collect hard evidence before patching:
   - repro steps
   - logs, runtime state, requests, traces, or screenshots
   - recent code/config changes
   - targeted tests or smoke checks
3. Separate:
   - facts
   - inferences
   - unknowns
4. Build and rank a short hypothesis set.
5. Try to eliminate hypotheses with the cheapest decisive checks first.
6. State the most likely root cause and confidence level.
7. Only then propose the smallest fix or next capture point.

## Rules

1. Do not jump to code edits before you have evidence, unless the user explicitly asks for a speculative fix.
2. Prefer read-only inspection first.
3. Prefer targeted commands and narrow test runs over broad scans.
4. If the issue is not reproducible, say so explicitly and define what signal must be captured next time.
5. If multiple systems are involved, identify the failing boundary instead of blaming the nearest symptom.

## Output Contract

Return the result in this order:

1. Symptom
2. Scope
3. Evidence
4. Root cause or leading hypothesis
5. Confidence
6. Recommended next step


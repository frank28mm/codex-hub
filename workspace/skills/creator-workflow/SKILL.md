---
name: creator-workflow
description: Use when the user wants to operate the Codex Hub creator workflow in natural language, including refreshing strategy, generating a sample, marking a sample as sent, writing feedback back into the workflow, or checking current sample status.
---

# Creator Workflow

Use this skill when the user is not asking for a generic piece of copy, but for an operational action inside the `creator workflow`.

Typical requests:

- `给产品 A 出一条今天的样本`
- `这条已经发出去了`
- `把这条反馈记进去`
- `看看当前进度`
- `刷新一下策略`

## Goal

Turn natural-language creator workflow requests into the existing runner contract without creating a second execution path.

## Entry Points

Prefer these two entry points when the public runtime has installed the optional creator workflow runner:

1. Direct runner:
   - `python3 ops/creator_workflow_runner.py intent-from-prompt --prompt '<request>'`
   - `python3 ops/creator_workflow_runner.py from-prompt --prompt '<request>'`
   - `python3 ops/creator_workflow_runner.py from-prompt --prompt '<request>' --confirm`
2. Broker-facing entry:
   - `python3 ops/local_broker.py command-center --action creator-workflow --prompt '<request>'`
   - add `--confirm` only when the write action should really execute

If those entry points are not present, use this skill as the public contract for implementing the same flow in a local project.

## Rules

1. Start with prompt interpretation, not direct execution.
2. If the result says `requires_confirmation=true`, do not silently execute the write action.
3. Reuse the existing runner actions:
   - `refresh_strategy`
   - `run_suggested_sample`
   - `promote_sample_live`
   - `record_external_send`
   - `ingest_feedback_sample`
   - `sample_state`
4. If the prompt is ambiguous, return the detection result rather than guessing a write action.
5. Do not bypass project pause or confirmation rules just because the request is phrased naturally.

## Output Contract

Return in this order:

1. Current stage
2. Detected creator workflow action
3. Parsed arguments
4. Whether confirmation is required
5. Execution result
6. Recommended next step

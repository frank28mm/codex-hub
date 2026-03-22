# Gstack Phase 4 Shared Protocols

These protocols are shared by the Phase 4 second-opinion skills:

- `claude-review`
- `claude-challenge`
- `claude-consult`

Use them to keep optional second-opinion work consistent without handing final judgment away from the main Codex thread.

## Architecture Contract

- `Feishu / Electron / broker` only invoke the main `Codex` thread.
- They do not directly invoke `Claude Code`.
- `Codex` decides when a Phase 4 second-opinion path is warranted.
- When warranted, `Codex` internally calls `Claude Code` through `ops/claude_code_runner.py`.
- Installing `claude-review / claude-challenge / claude-consult` into `~/.codex/skills/` is a local discovery enhancement, not a prerequisite for the Feishu or Electron path.

## Current Execution Path

Preferred official entry:

- `python3 ops/gstack_phase1_entry.py second-opinion --skill claude-review --question "<question>"`
- `python3 ops/gstack_phase1_entry.py second-opinion --skill claude-challenge --question "<question>"`
- `python3 ops/gstack_phase1_entry.py second-opinion --skill claude-consult --question "<question>"`
- `python3 ops/gstack_phase1_entry.py second-opinion-from-prompt --prompt "<natural language>" --question "<question>"`
- `python3 ops/gstack_phase1_entry.py workflow-second-opinion --prompt "<natural language>"`
- This official entry now runs in strict safety mode:
  - `--permission-mode plan`
  - `--tools ""`
  - append-only advisory system prompt
- This official entry also fixes a versioned request/response contract:
  - request envelope: `codex-hub.second-opinion.request.v1`
  - response contract: `codex-hub.second-opinion.response.v1`
  - normalized request fields: `question / artifact / current_judgment / extra_context`
  - normalized response fields: `status / question_or_focus / key_judgment / difference_from_current_judgment / recommended_next_step / evidence_needed`
  - 对 `second-opinion-from-prompt / workflow-second-opinion` 两条入口，`question / artifact / current_judgment / extra_context` 现在允许按工作流模板自动补齐
  - 当前默认模板：
    - `claude-review` -> `review-risk-scan`
    - `claude-challenge` -> `challenge-pressure-test`
    - `claude-consult` -> `consult-tradeoff-check`

Low-level runner entry:

- `python3 ops/claude_code_runner.py --mode review "<prompt>"`
- `python3 ops/claude_code_runner.py --mode challenge "<prompt>"`
- `python3 ops/claude_code_runner.py --mode consult "<prompt>"`

This runner intentionally:

- keeps Codex as the main owner
- uses `Claude Code` only for second opinion
- runs in `plan` permission mode with tools disabled
- runs with a temporary writable `HOME` under `/tmp`
- injects the existing local Claude proxy/auth env from `~/.claude/settings.json`

## Second Opinion Trigger Protocol

Before using a Phase 4 skill:

1. State the exact artifact, plan, judgment, or decision that needs a second opinion.
2. State why another opinion is useful:
   - disagreement risk
   - blind-spot check
   - pressure test
   - alternative framing
3. Keep the parent Codex thread as the owner of the final judgment.
4. Do not ask for a second opinion when the task is still too vague to review or challenge cleanly.

## Disagreement Framing Protocol

When the second opinion differs from the current judgment:

1. Separate:
   - what still agrees
   - what changes
   - what remains unknown
2. Do not flatten disagreement into a vague "needs more thought".
3. Name the concrete evidence that would resolve the disagreement.
4. End with a clear next move for the parent thread.

## Challenge Protocol

When using `claude-challenge`:

1. State the claim, plan, or assumption being challenged.
2. Ask for the strongest counterargument, not a balanced summary.
3. Name what evidence would overturn the current plan.
4. Keep the output actionable instead of rhetorical.

## Consultation Protocol

When using `claude-consult`:

1. Frame the ask as a focused advisory question.
2. Prefer alternative framing, tradeoff clarification, or option shaping.
3. Keep the consult lightweight; this is not a full re-review.
4. End with what changed, if anything, in the parent thread's view.

## Output Contract

Each Phase 4 skill should return:

1. `Status`
2. `Question or judgment under review`
3. `Second-opinion result`
4. `Agreement, disagreement, or changed confidence`
5. `Recommended next step`

In the current official runtime path, that contract is returned as the normalized `structured_output` object, not the raw provider envelope.
同时官方入口还会产出统一的 `main_thread_handoff` 文本模板，供 `Codex` 主线程直接回收与整合，而不需要每个入口各自重新拼装结果说明。

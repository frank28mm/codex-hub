---
name: claude-writing
description: Use when the user explicitly wants Claude Code to act as a dedicated writing specialist for rewriting, polishing, structuring, or sharpening prose without changing factual intent.
---

# Claude Writing

Use this skill when the main thread wants a dedicated Claude-style writing pass rather than a review, challenge, or consult.

Before using this skill, read:

- `references/platform-style-library.md`
- `references/platform-style-profiles.yaml`

## Goal

Bring in a writing specialist that improves clarity, tone, flow, and structure while keeping factual claims anchored to the source material the main thread provides.

## When To Use

- Rewrite or polish an existing draft
- Turn rough notes into cleaner prose
- Adjust tone for a target audience
- Improve structure, headings, or flow
- Produce a stronger version of copy, memo, brief, or article text

## Workflow

1. State the writing task clearly:
   - rewrite
   - polish
   - restructure
   - tighten
   - expand carefully
2. State the target audience and tone.
3. Separate:
   - source facts that must remain true
   - style changes that are allowed
   - content changes that are not allowed
4. Ask for a revised draft, not just advice about the draft.
5. Reconcile the rewritten output back into the parent thread's final delivery.
6. If the user specifies a platform, lock to the matching platform profile before writing.
7. If the user does not specify a platform, ask only when the platform materially changes the output; otherwise default to a neutral high-clarity draft and say the platform is still unset.

## Rules

1. Do not invent facts, metrics, quotes, or commitments that were not provided.
2. Preserve the user's actual intent before improving style.
3. If the source is too thin, note the missing inputs instead of padding with made-up detail.
4. Prefer concrete prose changes over abstract writing commentary.
5. When useful, ask for one strong version first, then optional variants.
6. Do not mix platform voices by default:
   - 小红书图文 != 公众号长文
   - 公众号推送文案 != 朋友圈文案

## Official Entry

Preferred low-level runner:

`python3 ops/claude_code_runner.py --mode writing "<prompt>"`

## Output Contract

Return in this order:

1. Status
2. Writing brief
3. Revised draft
4. Notable changes to voice or structure
5. Recommended next step

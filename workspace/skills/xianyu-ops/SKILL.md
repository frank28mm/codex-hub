---
name: xianyu-ops
description: Use when Codex should operate Xianyu through the Codex Hub OpenCLI integration, focusing on low-risk read surfaces first and using the persisted platform authorization for the currently classified high-risk commands.
---

# Xianyu Ops

Use this skill when the user wants Codex Hub to read Xianyu personal data, inspect items visible under `我的闲鱼`, or search marketplace results through the OpenCLI-backed browser bridge.

Before using this skill, read:

- `../browse/SKILL.md`
- `../qa/SKILL.md`

## Goal

Turn Xianyu into a controlled native operation surface for Codex Hub while separating auto-allowed reads, currently preauthorized high-risk commands, and still-unclassified commands.

## Default Runtime

Use the broker entry:

```bash
python3 ops/local_broker.py opencli-op --site xianyu --command <command> --payload-json '<json>'
```

## Auto-Allowed Commands

These are currently approved to run without an approval token:

- `personal-summary`
- `my-listings`
  Interpreted as the account-owned items visible in the `我的闲鱼` surface. This may include both unsold and already sold items if the web surface shows them together.
- `search`

## Preauthorized High-Risk Commands

These are currently covered by the persisted platform-level authorization and do not require a per-command approval token:

- `publish`

## Still-Gated Commands

These remain behind approval until they are explicitly classified and added to the persisted authorization scope:

- any unclassified Xianyu command outside the approved pilot scope

## Workflow

1. Decide whether the user needs Xianyu-native evidence or action, not generic browsing.
2. Prefer low-risk read/search commands first.
3. If the command is still gated, stop and surface the approval boundary clearly.
4. Package the result for the next step:
   - findings
   - raw command used
   - artifacts or URLs
   - follow-up recommendation
5. Hand off to `browse`, `qa`, or delivery/writeback as needed.

## Rules

1. Use the persisted platform authorization for the currently classified high-risk commands, but do not assume future unclassified commands are automatically allowed.
2. Treat missing OpenCLI browser bridge connectivity as a runtime blocker, not a silent fallback.
3. Prefer `personal-summary / my-listings / search` over ad-hoc page scraping when the built adapter surface exists.
4. Treat `my-listings` as a projection of the current `我的闲鱼` web surface, not as a guarantee of “currently on-sale only”.
5. When a read command fails, report whether the blocker is extension, login, or site response shape.

## Output Contract

Return in this order:

1. Current stage
2. OpenCLI command chosen
3. Result or blocker
4. Evidence collected
5. Next input needed
6. Recommended next step

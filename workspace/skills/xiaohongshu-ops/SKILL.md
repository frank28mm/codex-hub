---
name: xiaohongshu-ops
description: Use when Codex should operate Xiaohongshu through OpenCLI in the Codex Hub workspace, with low-risk read/search commands allowed directly and the currently classified high-risk commands covered by the persisted platform authorization.
---

# Xiaohongshu Ops

Use this skill when the user wants Codex Hub to read Xiaohongshu creator data, search notes, inspect creator analytics, or run the currently authorized Xiaohongshu write paths through OpenCLI.

Before using this skill, read:

- `../browse/SKILL.md`
- `../qa/SKILL.md`

## Goal

Turn Xiaohongshu into a controlled native operation surface for Codex Hub while preserving a clear distinction between auto-allowed reads, currently preauthorized high-risk commands, and still-unclassified commands.

## Default Runtime

Use the broker entry:

```bash
python3 ops/local_broker.py opencli-op --site xiaohongshu --command <command> --payload-json '<json>'
```

## Auto-Allowed Commands

These are currently approved to run without an approval token:

- `search`
- `feed`
- `user`
- `creator-note-detail`
- `creator-notes`
- `creator-notes-summary`
- `creator-profile`
- `creator-stats`

## Preauthorized High-Risk Commands

These are currently covered by the persisted platform-level authorization and do not require a per-command approval token:

- `publish`
- `download`
- `notifications`

## Still-Gated Commands

These remain behind approval until they are explicitly classified and added to the persisted authorization scope:

- any unclassified Xiaohongshu command outside the approved pilot scope

## Workflow

1. Decide whether the user needs Xiaohongshu-native evidence or action, not generic browsing.
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
3. Prefer creator-side analytics commands over brittle page scraping when both exist.
4. When a read command fails, report whether the blocker is extension, login, or site response shape.

## Output Contract

Return in this order:

1. Current stage
2. OpenCLI command chosen
3. Result or blocker
4. Evidence collected
5. Next input needed
6. Recommended next step

---
name: wechat-gui-send
description: Use when Codex should prepare and send WeChat direct messages through the Computer Use plugin operating the macOS WeChat desktop app, with an explicit prepare-then-confirm flow and fail-closed contact verification.
---

# WeChat GUI Send

Use this skill when the user wants WeChat private messages sent through the local `WeChat` desktop GUI rather than through a bridge, API, or background sender.

Before using this skill, read:

- `../qa/SKILL.md`
- `references/wechat-workflow.md`
- `references/failure-modes.md`

## Goal

Prepare a local send queue first, preview it for the user, and only after explicit confirmation use `Computer Use` to operate `WeChat`.

This skill follows a strict `prepare -> confirm send` flow.

## Workflow

1. Parse the user request into queue items with `recipient_name` and `message_text`.
2. Call `ops.wechat_gui_send.prepare_queue(...)`.
3. Show the queue preview from `ops.wechat_gui_send.review_queue(...)`.
4. Wait for explicit confirmation such as `确认发送`.
5. Only after confirmation, use `get_app_state(app="WeChat")` before each meaningful GUI step.
6. Use the WeChat search field to paste the recipient name for fast targeting.
7. Inspect the search result, open the target chat, and stop if the match is ambiguous.
8. Verify the active chat header matches the intended recipient.
9. Focus the message input.
10. Paste the message text from the system clipboard instead of typing into the WeChat GUI.
11. Press `Return` to send the message.
12. Record the send result with `ops.wechat_gui_send.record_execution_result(...)`.

## Rules

1. Direct chats only.
2. Text only.
3. No send during preparation.
4. No implicit confirmation.
5. Do not guess when multiple contacts match.
6. Prefer the search box over manual list navigation whenever possible.
7. Use the system clipboard for recipient names and message text instead of GUI typing.
8. Treat `Return` as the default send action.
9. Prefer accessibility-tree controls over raw coordinates.
10. Stop on missing controls, login screens, modals, or layout drift.
11. Keep the first version fail-closed: one hard failure stops the remaining batch.

## Output Contract

Return in this order:

1. Current stage
2. Queue id
3. Result or blocker
4. Evidence collected
5. Next input needed
6. Recommended next step

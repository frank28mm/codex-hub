# WeChat Workflow

## Execution Anchors

1. `get_app_state(app="WeChat")`
2. Focus the search field.
3. Paste the recipient name from the system clipboard into the search field.
4. Inspect the search result for ambiguity.
5. Open the intended chat.
6. Verify the active chat header.
7. Focus the message input.
8. Paste the message text from the system clipboard.
9. Press `Return` to send.
10. Verify the input no longer contains the full unsent text.

## Notes

- Refresh app state before each major step so the model can re-anchor.
- Treat unexpected modals, permission prompts, and login screens as blockers.
- Prefer clipboard paste over GUI typing for both recipient names and message text.
- Use coordinates only when accessibility elements are missing or unusable.

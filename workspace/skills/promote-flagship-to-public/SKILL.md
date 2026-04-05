---
name: promote-flagship-to-public
description: Use when Codex should migrate pending private flagship commits into the public Codex Hub repo with strict anchor-based accounting, private-boundary filtering, parity verification, and fail-closed delivery.
---

# Promote Flagship To Public

Use this skill when the goal is to move already-committed private flagship Codex Hub changes into the open-source Codex Hub repo without leaking personal or private information.

## Goal

Treat public promotion as an accounted migration batch, not a “latest commit” sync.

The result must:

1. use a pending commit range anchored by `last_promoted_private_sha`
2. classify every pending commit/path as accounted or unaccounted
3. fail closed if any item is still `needs-decision` or `needs-redaction`
4. only advance the anchor after apply, verify, commit, and push all succeed

## Default Runtime

Primary entrypoint:

```bash
python3 ops/flagship_public_promotion.py status
python3 ops/flagship_public_promotion.py run
```

If the ledger has not been initialized yet, set the anchor explicitly first:

```bash
python3 ops/flagship_public_promotion.py set-anchor --private-sha <sha> --public-commit-sha <sha>
```

## Workflow

1. Read the promotion status and confirm the pending range is based on the anchor, not just the latest commit.
2. Check whether the private head is already pushed to the tracked GitHub branch.
3. Review the batch accounting:
   - `public-safe`
   - `already-public-safe`
   - `private-only`
   - `needs-redaction`
   - `needs-decision`
4. If any pending item is unaccounted, stop and report exactly what blocked anchor advancement.
5. If the batch is fully accounted, run the full migration:
   - apply public-safe delta
   - run parity verification
   - commit to the public repo
   - push the public branch
   - advance the anchor
6. Write back the batch report, commit coverage, and final public commit id.

## Rules

1. Never treat “latest commit” as the migration unit; always use the full pending range since the last promoted anchor.
2. Never advance the anchor if any commit/path in the pending range is still unaccounted.
3. `private-only` still counts as accounted, but it must be explicit in the batch report.
4. `manual-template`, `manual-port`, `rewrite-required`, `unmatched`, and `unmanaged_shared_paths` are not silently skippable.
5. If the public repo is dirty before apply, stop instead of mixing this batch with unrelated changes.
6. Preserve intentionally better public-safe variants instead of forcing private file parity.
7. The target is parity of capability, UX, runtime contract, setup, and verification surface, not blind file sameness.

## Output Contract

Return in this order:

1. Current anchor and pending private range
2. Batch accounting summary
3. Whether the batch is fully accounted
4. What was applied or why the batch was blocked
5. Verification result
6. Public commit / branch / push result
7. Whether the anchor advanced

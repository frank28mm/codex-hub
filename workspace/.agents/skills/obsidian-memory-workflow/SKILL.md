---
name: obsidian-memory-workflow
description: Use when working inside the current Codex Hub workspace on the Obsidian-backed memory system, including project routing, progressive memory loading, project discovery, session writeback, and Vault updates.
---

# Obsidian Memory Workflow

Use this skill whenever the task touches the `Codex Hub workspace + sibling memory` system.

## Fixed Paths

1. Workspace root: the current Codex Hub `workspace/` directory
2. Vault root: the sibling `memory/` directory
3. Standard launcher: `ops/start-codex`
4. Memory utility: `ops/codex_memory.py`

## Core Rules

1. Do not require the user to manually open Obsidian before starting work.
2. Treat direct file I/O against the Vault as the source of truth; Obsidian GUI is optional.
3. Never bulk-read the entire Vault at startup.
4. Use progressive disclosure:
   - `T0`: `AGENTS.md`, `MEMORY_SYSTEM.md`
   - `T1`: `PROJECT_REGISTRY.md`, `ACTIVE_PROJECTS.md`, `NEXT_ACTIONS.md`
   - `T2`: current project board, and current topic board if the prompt clearly targets a topic
   - `T2.5`: `python3 ops/codex_context.py suggest`
   - `T2.8`: current project summary note when long-term background is needed
   - `T3`: semantic expansion only when needed
   - `T4`: episodic logs only when needed
5. Do not commit `projects/` contents to the Codex Hub product repo.
6. Treat project summary pages as long-term memory only; do not use them as the current task fact source.
7. Current fact-source chain is `topic board -> project board -> NEXT_ACTIONS -> dashboards`.
8. Do not overwrite human-maintained `summary` with the latest session output; write recent session metadata into `last_writeback_*` and `## 自动写回`.
9. The workspace itself is a formal project; system-evolution work should bind to its project board instead of living only in reports or chat.

## Direct Codex App Mode

If Codex is opened directly in the app with the current product workspace root:

1. Follow the same workflow as `ops/start-codex` as closely as possible.
2. Before substantial project work, run:
   - `python3 ops/codex_memory.py discover-projects`
3. Prefer keeping the session watcher installed:
   - `python3 ops/codex_session_watcher.py status`
   - `python3 ops/codex_session_watcher.py install-launchagent`
4. Route memory reads through `PROJECT_REGISTRY.md`, `ACTIVE_PROJECTS.md`, `NEXT_ACTIONS.md`, the bound project board, and the topic board if one is targeted.
5. After binding a project or topic, prefer running `python3 ops/codex_context.py suggest` before reading deeper.
6. When watcher is installed, task-complete writeback is handled automatically from local Codex session files.
7. If watcher is unavailable, fall back to the explicit launcher or update writeback targets manually before finishing a project-scoped task.

## Utility Commands

1. Discover new local projects:
   - `python3 ops/codex_memory.py discover-projects`
2. Rebuild machine-generated index pages:
   - `python3 ops/codex_memory.py refresh-index`
3. Generate context suggestions after binding:
   - `python3 ops/codex_context.py suggest --project-name <name> --prompt "<prompt>"`

## Writeback Targets

For project sessions, writeback should land in:

1. `01_working/NOW.md`
2. `NEXT_ACTIONS.md`
3. `03_semantic/projects/<project>.md`
4. `02_episodic/daily/YYYY-MM-DD.md`
5. `runtime/project-bindings.json`
6. `runtime/session-router.json`
7. `runtime/events.ndjson`
8. `runtime/retrieval/index.sqlite`
9. `runtime/retrieval/state.json`
10. `07_dashboards/HOME.md`
11. `07_dashboards/PROJECTS.md`
12. `07_dashboards/ACTIONS.md`
13. `07_dashboards/MEMORY_HEALTH.md`

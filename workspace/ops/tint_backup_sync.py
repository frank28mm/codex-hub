#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    from ops.controlled_git import run_git_command
except ImportError:  # pragma: no cover
    from controlled_git import run_git_command


WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_HUB_ROOT", str(Path(__file__).resolve().parents[1]))).resolve()
DEFAULT_REPO = WORKSPACE_ROOT / "projects" / "TINT" / "TINT服务器"
DEFAULT_REPORT = WORKSPACE_ROOT / "runtime" / "tint-backup-sync-report.md"


@dataclass
class SyncState:
    repo: str
    remote: str
    branch: str
    current_branch: str
    dirty: bool
    action: str
    ahead: int
    behind: int
    unexpected_remotes: list[str]
    dirty_files: list[str]
    incoming_files: list[str]
    outgoing_files: list[str]
    timestamp: str


def run_git(repo: Path, *args: str, remote: str = "", check: bool = True) -> str:
    payload, exit_code = run_git_command(
        repo=repo,
        git_args=list(args),
        execution_context="noninteractive",
        dry_run=False,
        explicit_remote=remote,
        project_name="TINT",
        session_id="",
    )
    stdout = str(payload.get("stdout", "")).strip()
    stderr = str(payload.get("stderr", "")).strip()
    if check and exit_code != 0:
        raise RuntimeError(stderr or stdout or f"git {' '.join(args)} failed with code {exit_code}")
    return stdout


def git_lines(repo: Path, *args: str, remote: str = "") -> list[str]:
    output = run_git(repo, *args, remote=remote)
    return [line for line in output.splitlines() if line.strip()]


def repo_state(repo: Path, remote: str, branch: str) -> SyncState:
    run_git(repo, "fetch", remote, "--prune", remote=remote)
    current_branch = run_git(repo, "rev-parse", "--abbrev-ref", "HEAD", remote=remote)
    dirty_files = git_lines(repo, "status", "--short", remote=remote)
    dirty = bool(dirty_files)
    remotes = git_lines(repo, "remote", remote=remote)
    unexpected_remotes = [name for name in remotes if name != remote]

    ahead_text, behind_text = run_git(
        repo,
        "rev-list",
        "--left-right",
        "--count",
        f"HEAD...{remote}/{branch}",
        remote=remote,
    ).split()
    ahead = int(ahead_text)
    behind = int(behind_text)

    incoming_files = git_lines(repo, "diff", "--name-status", f"HEAD..{remote}/{branch}", remote=remote)
    outgoing_files = git_lines(repo, "diff", "--name-status", f"{remote}/{branch}..HEAD", remote=remote)

    action = "checked"
    if current_branch != branch:
        action = "blocked-wrong-branch"
    elif dirty:
        action = "blocked-dirty"
    elif ahead == 0 and behind > 0:
        run_git(repo, "pull", "--ff-only", remote, branch, remote=remote)
        action = "fast-forwarded"
        ahead = 0
        behind = 0
        incoming_files = []
        outgoing_files = []
    elif ahead == 0 and behind == 0:
        action = "up-to-date"
    elif ahead > 0 and behind == 0:
        action = "ahead-local"
    else:
        action = "diverged"

    return SyncState(
        repo=str(repo),
        remote=remote,
        branch=branch,
        current_branch=current_branch,
        dirty=dirty,
        action=action,
        ahead=ahead,
        behind=behind,
        unexpected_remotes=unexpected_remotes,
        dirty_files=dirty_files,
        incoming_files=incoming_files,
        outgoing_files=outgoing_files,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def write_report(path: Path, state: SyncState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# TINT Backup Sync Report",
        "",
        f"- Timestamp: `{state.timestamp}`",
        f"- Repo: `{state.repo}`",
        f"- Target: `{state.remote}/{state.branch}`",
        f"- Current branch: `{state.current_branch}`",
        f"- Action: `{state.action}`",
        f"- Dirty: `{'yes' if state.dirty else 'no'}`",
        f"- Ahead / Behind: `{state.ahead} / {state.behind}`",
    ]

    if state.unexpected_remotes:
        lines.extend(
            [
                "",
                "## Unexpected Remotes",
                *[f"- `{name}`" for name in state.unexpected_remotes],
            ]
        )

    if state.dirty_files:
        lines.extend(
            [
                "",
                "## Dirty Files",
                *[f"- `{line}`" for line in state.dirty_files],
            ]
        )

    if state.incoming_files:
        lines.extend(
            [
                "",
                "## Incoming Diff",
                *[f"- `{line}`" for line in state.incoming_files],
            ]
        )

    if state.outgoing_files:
        lines.extend(
            [
                "",
                "## Outgoing Diff",
                *[f"- `{line}`" for line in state.outgoing_files],
            ]
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safely keep the TINT backup repo aligned with GitHub.")
    parser.add_argument("--repo", type=Path, default=DEFAULT_REPO, help="Path to the backup repository.")
    parser.add_argument("--remote", default="origin", help="Remote name to track.")
    parser.add_argument("--branch", default="main", help="Remote branch to track.")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT, help="Markdown report output path.")
    parser.add_argument("--json", action="store_true", help="Also print JSON state to stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = args.repo.resolve()
    if not (repo / ".git").exists():
        print(f"Repository not found: {repo}", file=sys.stderr)
        return 2

    try:
        state = repo_state(repo, args.remote, args.branch)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    write_report(args.report.resolve(), state)

    if args.json:
        print(json.dumps(asdict(state), ensure_ascii=False, indent=2))
    else:
        print(f"{state.action}: {state.repo} -> {state.remote}/{state.branch}")

    if state.action in {"blocked-dirty", "blocked-wrong-branch", "diverged"}:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

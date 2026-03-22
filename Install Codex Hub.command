#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_DIR="$ROOT_DIR/workspace"

cd "$WORKSPACE_DIR"

echo "==> Bootstrapping Codex Hub..."
python3 ops/bootstrap_workspace_hub.py setup --install-launchagents

echo
echo "Setup finished."
echo "If you want Feishu chat entry as well, enable Feishu in workspace/control/site.yaml"
echo "and then run:"
echo "  python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge"
echo
echo "Next recommended step:"
echo "  double-click 'Validate Codex Hub.command'"

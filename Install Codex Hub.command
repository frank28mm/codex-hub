#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_DIR="$ROOT_DIR/workspace"

cd "$WORKSPACE_DIR"

echo "==> Bootstrapping Codex Hub..."
python3 ops/bootstrap_workspace_hub.py setup --install-launchagents

echo
echo "Setup finished."
echo "If you want Feishu chat entry as well, run:"
echo "  python3 ops/bootstrap_workspace_hub.py setup-feishu-cli --create-feishu-app"
echo
echo "That guided setup will:"
echo "  1. open the Feishu app creation/config page"
echo "  2. if needed, open the app baseinfo page and prompt once for App Secret"
echo "  3. open the Feishu authorization page and continue after you approve"
echo "Ignore repeated macOS Keychain popups during this flow; they are not part of the standard public setup path."
echo "  python3 ops/bootstrap_workspace_hub.py init --install-feishu-bridge"
echo
echo "Next recommended step:"
echo "  double-click 'Validate Codex Hub.command'"

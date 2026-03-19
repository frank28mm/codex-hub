#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_DIR="$ROOT_DIR/workspace"

cd "$WORKSPACE_DIR"

echo "==> Running Codex Hub acceptance..."
python3 ops/accept_product.py run

echo
echo "Acceptance finished."
echo "Review the latest report at:"
echo "  $WORKSPACE_DIR/reports/system/product-acceptance-latest.md"

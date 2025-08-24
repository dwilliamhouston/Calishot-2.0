#!/usr/bin/env bash
set -euo pipefail
# Cross-platform installer launcher (macOS/Linux)
# This script intentionally does NOT copy any repo data/ or books/ files.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$SCRIPT_DIR/install.py" "$@"

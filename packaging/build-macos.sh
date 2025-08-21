#!/usr/bin/env bash
set -euo pipefail

# Build standalone macOS executable with PyInstaller
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

python3 -m venv .venv-build
source .venv-build/bin/activate
python -m pip install --upgrade pip
python -m pip install .[dev]

# Clean previous dist/build
rm -rf dist build

# PyInstaller requires ':' as the add-data separator on Unix
pyinstaller \
  --name Calishot-Web \
  --onefile \
  --noconfirm \
  --clean \
  --add-data "calishot_web/templates:calishot_web/templates" \
  --add-data "calishot_web/static:calishot_web/static" \
  --add-data "data:data" \
  -p . \
  calishot_web/cli.py

echo "\nBuilt app at: dist/Calishot-Web"

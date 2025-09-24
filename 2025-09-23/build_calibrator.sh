#!/usr/bin/env bash
set -euo pipefail

# Build standalone app for mhz19c_calibrate.py using PyInstaller
# Usage: ./build_calibrator.sh

APP_NAME="MHZ19C-Cal"

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "PyInstaller not found. Install with: python3 -m pip install pyinstaller" >&2
  exit 1
fi

rm -rf build dist "${APP_NAME}.spec" || true

pyinstaller \
  --noconsole \
  --windowed \
  --onefile \
  --name "${APP_NAME}" \
  --hidden-import matplotlib.backends.backend_tkagg \
  mhz19c_calibrate.py

echo "Build complete: dist/${APP_NAME}"


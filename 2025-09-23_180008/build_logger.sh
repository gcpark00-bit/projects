#!/usr/bin/env bash
set -euo pipefail

# Build standalone app for multichannel_usb_logger.py using PyInstaller
# Usage: ./build_logger.sh

APP_NAME="CO2Logger"

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "PyInstaller not found. Install with: python3 -m pip install pyinstaller" >&2
  exit 1
fi

# Clean previous build artifacts
rm -rf build dist "${APP_NAME}.spec" || true

# On macOS/Linux, --add-data uses ':' separator
pyinstaller \
  --noconsole \
  --windowed \
  --onefile \
  --name "${APP_NAME}" \
  --hidden-import matplotlib.backends.backend_tkagg \
  --add-data "ports_config.json:." \
  multichannel_usb_logger.py

echo "Build complete: dist/${APP_NAME}" 


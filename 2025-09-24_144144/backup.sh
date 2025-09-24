#!/usr/bin/env bash
set -euo pipefail
TS="${1:-$(date +%F_%H%M%S)}"
DST="$TS"
mkdir -p "$DST"
rsync -a \
  --exclude ".venv/" \
  --exclude "$DST/" \
  --exclude '20??-??-??*/' \
  ./ "$DST"/
# Try zip, fallback to tar.gz
if command -v zip >/dev/null 2>&1; then
  zip -r "backup_${TS}.zip" "$DST" >/dev/null
else
  tar -czf "backup_${TS}.tar.gz" "$DST"
fi
echo "Backup complete: $DST and archive created."

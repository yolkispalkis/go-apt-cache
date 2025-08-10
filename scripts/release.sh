#!/usr/bin/env bash
set -euo pipefail
# Если HEAD точно на теге vX.Y.Z -> релиз; иначе снапшот
if git describe --tags --exact-match >/dev/null 2>&1; then
  gbp dch --ignore-branch --meta --release
else
  gbp dch --ignore-branch --meta --snapshot
fi
gbp buildpackage --git-ignore-new -us -uc -b
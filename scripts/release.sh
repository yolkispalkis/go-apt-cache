#!/usr/bin/env bash
set -euo pipefail
export DEBFULLNAME="${DEBFULLNAME:-$(git config --get user.name)}"
export DEBEMAIL="${DEBEMAIL:-$(git config --get user.email)}"
DIST="${DIST:-noble}"

OPTS=(--ignore-branch --meta --distribution "${DIST}" --upstream-tag='%(version)s')
if git describe --tags --exact-match >/dev/null 2>&1; then
  gbp dch "${OPTS[@]}" --release
else
  gbp dch "${OPTS[@]}" --snapshot
fi
gbp buildpackage --git-ignore-new -us -uc -b
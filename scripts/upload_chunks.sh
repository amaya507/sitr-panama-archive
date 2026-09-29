#!/usr/bin/env bash
# Upload every chunk in $1 to its day's GitHub release (raw-YYYY-MM-DD, UTC),
# creating the release on first use. Retries; exits non-zero only if a chunk
# could not be uploaded after all attempts.
set -uo pipefail
dir="${1:?usage: upload_chunks.sh DIR}"
status=0
shopt -s nullglob
for f in "$dir"/sitr_*.tar.gz; do
  name=$(basename "$f")
  day=$(echo "$name" | sed -E 's/^sitr_([0-9]{4})([0-9]{2})([0-9]{2})T.*/\1-\2-\3/')
  tag="raw-$day"
  ok=0
  for attempt in 1 2 3 4 5; do
    if ! gh release view "$tag" >/dev/null 2>&1; then
      gh release create "$tag" --title "Raw SITR responses $day (UTC)" --prerelease --latest=false \
        --notes "Raw, unmodified responses from https://sitr.cnd.com.pa/m/pub/data/ fetched on $day (UTC). One .tar.gz per fetch: meta.json + raw/<endpoint>.json. See README 'Raw archive'." \
        >/dev/null 2>&1 || true   # may race with another job; re-checked below
    fi
    if gh release upload "$tag" "$f" --clobber; then
      echo "uploaded $name -> $tag"
      ok=1
      break
    fi
    echo "upload attempt $attempt failed for $name" >&2
    sleep $((attempt * 5))
  done
  [ $ok -eq 1 ] || { echo "::error::could not upload $name"; status=1; }
done
exit $status

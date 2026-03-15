#!/usr/bin/env bash
set -euo pipefail

MAX_TRACKS="${1:-1000}"
MAX_UNENRICHED="${2:-10000}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYTHON_BIN="python"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Error: python or python3 is required." >&2
  exit 1
fi

export DBT_PROFILES_DIR="$REPO_ROOT/profiles"

get_max_added_at() {
  local value
  if value="$($PYTHON_BIN ./scripts/get_max_added_at.py 2>/dev/null)"; then
    value="${value//$'\r'/}"
    value="${value//$'\n'/}"
    echo "Found Max: $value" >&2
    printf '%s' "$value"
  else
    echo "Error Max: unable to query max added_at. Defaulting to full ingestion." >&2
    printf ''
  fi
}

run_spotify_ingest() {
  local cutoff="$1"
  local args=()

  if [[ "$MAX_TRACKS" =~ ^-?[0-9]+$ ]] && (( MAX_TRACKS > 0 )); then
    args+=("--max-tracks" "$MAX_TRACKS")
  fi

  if [[ -n "$cutoff" ]]; then
    args+=("--stop-before-added-at" "$cutoff")
  fi

  "$PYTHON_BIN" ./scripts/ingest_spotify_library.py "${args[@]}"
}

run_musicbrainz_enrichment() {
  echo "Running progressive MusicBrainz enrichment for up to ${MAX_UNENRICHED} unenriched ISRC values..."
  "$PYTHON_BIN" ./scripts/enrich_musicbrainz.py --max-unenriched "$MAX_UNENRICHED"
}

run_dbt_build_and_test() {
  dbt run
  dbt test
}

main() {
  local max_added_at
  max_added_at="$(get_max_added_at)"

  run_spotify_ingest "$max_added_at"
  run_musicbrainz_enrichment
  run_dbt_build_and_test

  echo "Pipeline complete. Query analytics.mart_playlist_ready_tracks for playlist candidates."
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

API_ENDPOINT="${API_ENDPOINT:-https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context}"
API_KEY="${API_KEY:-}"
REQUEST_COUNT="${REQUEST_COUNT:-20}"

SAMPLES=(
  "epl-arsenal-man-city:b193b51f-cbed-4398-a297-237dd3322607"
  "wc26-usa-mexico:b42fe447-b2b1-485f-ae6d-1559ee2b57c7"
  "nba-lakers-celtics:d6742e61-2457-43fd-aa3f-e61f6a76c7af"
  "wc26-england-brazil:c94d7e01-333d-41cd-a67d-cc0285fa7f28"
)

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing dependency: curl" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq" >&2
  exit 1
fi

if [[ -z "$API_KEY" ]]; then
  echo "Set API_KEY before running this script." >&2
  echo "Example: API_KEY=your_key ./scripts/stability-test.sh" >&2
  exit 1
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/sportsync-stability.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

normalize_response() {
  local input_file="$1"
  jq -S '
    del(
      .metadata.generated_at,
      .metadata.response_time_ms,
      .metadata.data_freshness
    )
  ' "$input_file"
}

run_match_test() {
  local label="$1"
  local match_id="$2"
  local match_dir="$TMP_ROOT/$label"
  local baseline_norm="$match_dir/baseline.normalized.json"
  local pass=true

  mkdir -p "$match_dir"
  echo "Testing $label ($match_id)"

  for attempt in $(seq 1 "$REQUEST_COUNT"); do
    local raw_file="$match_dir/response-$attempt.raw.json"
    local norm_file="$match_dir/response-$attempt.normalized.json"
    local http_code

    http_code="$(curl -sS \
      -H "x-api-key: $API_KEY" \
      -o "$raw_file" \
      -w '%{http_code}' \
      "$API_ENDPOINT?match_id=$match_id")"

    if [[ "$http_code" != "200" ]]; then
      echo "  FAIL call $attempt returned HTTP $http_code" >&2
      pass=false
      continue
    fi

    if ! jq -e '
      has("match") and
      has("teams") and
      has("market") and
      has("opening_lines") and
      has("injuries") and
      has("intel") and
      has("metadata")
    ' "$raw_file" >/dev/null; then
      echo "  FAIL call $attempt missing one or more mandatory top-level sections" >&2
      pass=false
      continue
    fi

    normalize_response "$raw_file" > "$norm_file"

    if [[ "$attempt" == "1" ]]; then
      cp "$norm_file" "$baseline_norm"
      continue
    fi

    if ! diff -u "$baseline_norm" "$norm_file" > "$match_dir/diff-$attempt.patch"; then
      echo "  FAIL call $attempt differs from baseline after normalization" >&2
      pass=false
    else
      rm -f "$match_dir/diff-$attempt.patch"
    fi
  done

  if [[ "$pass" == true ]]; then
    echo "  PASS $label"
    return 0
  fi

  echo "  FAIL $label" >&2
  echo "  Artifacts: $match_dir" >&2
  return 1
}

overall_pass=true

for pair in "${SAMPLES[@]}"; do
  label="${pair%%:*}"
  match_id="${pair##*:}"
  run_match_test "$label" "$match_id" || overall_pass=false
done

if [[ "$overall_pass" == true ]]; then
  echo "OVERALL PASS"
  exit 0
fi

echo "OVERALL FAIL" >&2
exit 1

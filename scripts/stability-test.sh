#!/usr/bin/env bash
set -euo pipefail

API_ENDPOINT="${API_ENDPOINT:-https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context}"
API_KEY="${API_KEY:-sk_test_sportsync_beta_001}"
REQUEST_COUNT="${REQUEST_COUNT:-20}"

USA_MEXICO_MATCH_ID="${USA_MEXICO_MATCH_ID:-b42fe447-b2b1-485f-ae6d-1559ee2b57c7}"
EPL_MATCH_ID="${EPL_MATCH_ID:-}"
NBA_MATCH_ID="${NBA_MATCH_ID:-}"

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing dependency: curl" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq" >&2
  exit 1
fi

if [[ -z "$EPL_MATCH_ID" || -z "$NBA_MATCH_ID" ]]; then
  cat >&2 <<'EOF'
Set EPL_MATCH_ID and NBA_MATCH_ID before running this script.
The seed SQL files generate UUIDs with gen_random_uuid() and print each match_id via NOTICE when applied.

Example:
  EPL_MATCH_ID=<uuid> NBA_MATCH_ID=<uuid> ./scripts/stability-test.sh
EOF
  exit 1
fi

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/sportsync-stability.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

normalize_response() {
  local input_file="$1"
  jq -S '
    del(
      .metadata.generated_at,
      .metadata.response_time_ms
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
      has("form") and
      has("h2h") and
      has("injuries") and
      has("players") and
      has("trends") and
      has("live_state") and
      has("metadata")
    ' "$raw_file" >/dev/null; then
      echo "  FAIL call $attempt is missing one or more mandatory top-level sections" >&2
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

run_match_test "usa-mexico" "$USA_MEXICO_MATCH_ID" || overall_pass=false
run_match_test "epl-arsenal-man-city" "$EPL_MATCH_ID" || overall_pass=false
run_match_test "nba-lakers-celtics" "$NBA_MATCH_ID" || overall_pass=false

if [[ "$overall_pass" == true ]]; then
  echo "OVERALL PASS"
  exit 0
fi

echo "OVERALL FAIL" >&2
exit 1

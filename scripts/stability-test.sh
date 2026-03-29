#!/usr/bin/env bash
set -euo pipefail

API_ENDPOINT="${API_ENDPOINT:-https://sportsync-api.vercel.app/api/match-context}"
API_KEY="${API_KEY:-}"
REQUEST_COUNT="${REQUEST_COUNT:-20}"
CURL_RETRY_COUNT="${CURL_RETRY_COUNT:-3}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-1}"
CURL_MAX_TIME="${CURL_MAX_TIME:-20}"

USA_MEXICO_MATCH_ID="${USA_MEXICO_MATCH_ID:-b42fe447-b2b1-485f-ae6d-1559ee2b57c7}"
EPL_MATCH_ID="${EPL_MATCH_ID:-b193b51f-cbed-4398-a297-237dd3322607}"
NBA_MATCH_ID="${NBA_MATCH_ID:-d6742e61-2457-43fd-aa3f-e61f6a76c7af}"
ENGLAND_BRAZIL_MATCH_ID="${ENGLAND_BRAZIL_MATCH_ID:-c94d7e01-333d-41cd-a67d-cc0285fa7f28}"

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing dependency: curl" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq" >&2
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
      .metadata.data_freshness.match.last_updated,
      .metadata.data_freshness.intel.last_updated,
      .metadata.data_freshness.injuries.last_updated
    )
  ' "$input_file"
}

curl_with_retries() {
  curl \
    --silent \
    --show-error \
    --retry "$CURL_RETRY_COUNT" \
    --retry-delay "$CURL_RETRY_DELAY" \
    --retry-all-errors \
    --max-time "$CURL_MAX_TIME" \
    "$@"
}

required_sections_expr() {
  jq -r '
    .required
    | map("has(\"" + . + "\")")
    | join(" and ")
  ' "$(dirname "$0")/../schema/v1-response-contract.json"
}

run_match_test() {
  local label="$1"
  local match_id="$2"
  local match_dir="$TMP_ROOT/$label"
  local baseline_norm="$match_dir/baseline.normalized.json"
  local pass=true

  mkdir -p "$match_dir"
  echo "Testing $label ($match_id)"
  local section_assertion
  section_assertion="$(required_sections_expr)"

  for attempt in $(seq 1 "$REQUEST_COUNT"); do
    local raw_file="$match_dir/response-$attempt.raw.json"
    local norm_file="$match_dir/response-$attempt.normalized.json"
    local http_code

    if [[ -n "$API_KEY" ]]; then
      http_code="$(curl_with_retries \
        -H "x-api-key: $API_KEY" \
        -o "$raw_file" \
        -w '%{http_code}' \
        "$API_ENDPOINT?match_id=$match_id")"
    else
      http_code="$(curl_with_retries \
        -o "$raw_file" \
        -w '%{http_code}' \
        "$API_ENDPOINT?match_id=$match_id")"
    fi

    if [[ "$http_code" != "200" ]]; then
      echo "  FAIL call $attempt returned HTTP $http_code" >&2
      pass=false
      continue
    fi

    if ! jq -e "$section_assertion" "$raw_file" >/dev/null; then
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
run_match_test "england-brazil" "$ENGLAND_BRAZIL_MATCH_ID" || overall_pass=false

if [[ "$overall_pass" == true ]]; then
  echo "OVERALL PASS"
  exit 0
fi

echo "OVERALL FAIL" >&2
exit 1

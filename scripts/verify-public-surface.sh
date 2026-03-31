#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
API_ENDPOINT="${API_ENDPOINT:-https://sportsync-api.vercel.app/api/match-context}"
API_KEY="${API_KEY:-}"
CURL_RETRY_COUNT="${CURL_RETRY_COUNT:-3}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-1}"
CURL_MAX_TIME="${CURL_MAX_TIME:-20}"

SAMPLE_IDS=(
  "b193b51f-cbed-4398-a297-237dd3322607"
  "b42fe447-b2b1-485f-ae6d-1559ee2b57c7"
  "d6742e61-2457-43fd-aa3f-e61f6a76c7af"
  "c94d7e01-333d-41cd-a67d-cc0285fa7f28"
)

for cmd in curl jq rg sort mktemp diff; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing dependency: $cmd" >&2
    exit 1
  fi
done

failures=0
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/sportsync-verify.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

report_failure() {
  failures=$((failures + 1))
  echo "FAIL: $1" >&2
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

echo "Checking public sample IDs against live endpoint..."
required_sections=()
while IFS= read -r section; do
  required_sections+=("$section")
done < <(jq -r '.required[]' "$ROOT_DIR/schema/v1-response-contract.json")

for match_id in "${SAMPLE_IDS[@]}"; do
  response_file="$tmp_dir/${match_id}.json"
  if [[ -n "$API_KEY" ]]; then
    status_code="$(curl_with_retries -H "x-api-key: $API_KEY" -o "$response_file" -w '%{http_code}' "$API_ENDPOINT?match_id=$match_id")"
  else
    status_code="$(curl_with_retries -o "$response_file" -w '%{http_code}' "$API_ENDPOINT?match_id=$match_id")"
  fi
  if [[ "$status_code" != "200" ]]; then
    report_failure "sample $match_id returned HTTP $status_code"
    continue
  fi

  for section in "${required_sections[@]}"; do
    if ! jq -e --arg section "$section" 'has($section)' "$response_file" >/dev/null; then
      report_failure "sample $match_id missing required top-level section: $section"
    fi
  done
done

echo "Checking docs + demo sample ID consistency..."
expected_file="$tmp_dir/expected_ids.txt"
docs_file="$tmp_dir/docs_ids.txt"
demo_file="$tmp_dir/demo_ids.txt"
printf '%s\n' "${SAMPLE_IDS[@]}" | sort -u > "$expected_file"
rg -o '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' "$ROOT_DIR/docs/index.html" | sort -u > "$docs_file"
rg -o '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' "$ROOT_DIR/api/demo-matches.js" | sort -u > "$demo_file"

if ! diff -u "$expected_file" "$docs_file" >/dev/null; then
  report_failure "docs sample IDs do not match expected published set"
fi

if ! diff -u "$expected_file" "$demo_file" >/dev/null; then
  report_failure "api/demo-matches sample IDs do not match expected published set"
fi

echo "Checking for browser-exposed integration keys..."
if rg -n 'SUPABASE_ANON_KEY|supabase\.createClient|qffzvrnbzabcokqqrwbv|eyJhbGciOiJIUzI1Ni' "$ROOT_DIR/demo/index.html" >/dev/null; then
  report_failure "demo/index.html still contains browser-exposed Supabase credentials or client wiring"
fi

if rg -n 'sk_test_sportsync' "$ROOT_DIR/demo/index.html" "$ROOT_DIR/api/match-context.js" "$ROOT_DIR/api/demo-matches.js" >/dev/null; then
  report_failure "demo/API source still contains hardcoded SportsSync keys"
fi

echo "Checking for dead CTA placeholders..."
if rg -n 'href="#"\s*' "$ROOT_DIR/index.html" "$ROOT_DIR/docs/index.html" "$ROOT_DIR/demo/index.html" "$ROOT_DIR/trends/index.html" >/dev/null; then
  report_failure "one or more public pages still contain dead href=\"#\" links"
fi

if [[ "$failures" -gt 0 ]]; then
  echo "verify-public-surface: FAILED ($failures issues)" >&2
  exit 1
fi

echo "verify-public-surface: PASS"

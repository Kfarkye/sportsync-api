#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VERCEL_CONFIG="$ROOT_DIR/vercel.json"

for cmd in jq rg; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing dependency: $cmd" >&2
    exit 1
  fi
done

if ! jq empty "$VERCEL_CONFIG" >/dev/null; then
  echo "FAIL: vercel.json is not valid JSON" >&2
  exit 1
fi

required_global_headers=(
  "Strict-Transport-Security"
  "X-Content-Type-Options"
  "X-Frame-Options"
  "Referrer-Policy"
  "Permissions-Policy"
)

for header in "${required_global_headers[@]}"; do
  if ! jq -e --arg header "$header" '.headers[] | select(.source == "/(.*)") | .headers[] | select(.key == $header)' "$VERCEL_CONFIG" >/dev/null; then
    echo "FAIL: missing global security header in vercel.json: $header" >&2
    exit 1
  fi
done

if ! jq -e '.headers[] | select(.source == "/api/match-context") | .headers[] | select(.key == "Cache-Control" and .value == "no-store, max-age=0")' "$VERCEL_CONFIG" >/dev/null; then
  echo "FAIL: /api/match-context cache policy missing or incorrect" >&2
  exit 1
fi

if ! jq -e '.headers[] | select(.source == "/api/trends") | .headers[] | select(.key == "Cache-Control")' "$VERCEL_CONFIG" >/dev/null; then
  echo "FAIL: /api/trends cache policy missing" >&2
  exit 1
fi

if ! jq -e '.headers[] | select(.source == "/api/demo-matches") | .headers[] | select(.key == "Cache-Control")' "$VERCEL_CONFIG" >/dev/null; then
  echo "FAIL: /api/demo-matches cache policy missing" >&2
  exit 1
fi

api_files=(
  "$ROOT_DIR/api/match-context.js"
  "$ROOT_DIR/api/trends.js"
  "$ROOT_DIR/api/demo-matches.js"
)

for file in "${api_files[@]}"; do
  if ! rg -q 'handleOptions\(' "$file"; then
    echo "FAIL: $file does not handle OPTIONS preflight" >&2
    exit 1
  fi

  if ! rg -q 'setApiHeaders\(' "$file"; then
    echo "FAIL: $file does not set API security headers" >&2
    exit 1
  fi
done

echo "check-headers: PASS"

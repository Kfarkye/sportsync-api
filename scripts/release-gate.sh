#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

for cmd in bash node jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing dependency: $cmd" >&2
    exit 1
  fi
done

echo "[1/6] Static syntax and schema checks"
node --check "$ROOT_DIR/api/_lib/http.js"
node --check "$ROOT_DIR/api/match-context.js"
node --check "$ROOT_DIR/api/demo-matches.js"
node --check "$ROOT_DIR/api/trends.js"
jq empty "$ROOT_DIR/schema/v1-response-contract.json" >/dev/null
jq empty "$ROOT_DIR/schema/sample-response-v1.json" >/dev/null
jq empty "$ROOT_DIR/vercel.json" >/dev/null

echo "[2/6] Header policy checks"
bash "$ROOT_DIR/scripts/check-headers.sh"

echo "[3/6] Error envelope consistency checks"
bash "$ROOT_DIR/scripts/check-error-shape.sh"

echo "[4/6] Public surface contract checks"
bash "$ROOT_DIR/scripts/verify-public-surface.sh"

echo "[5/6] Stability checks"
REQUEST_COUNT="${REQUEST_COUNT:-3}" bash "$ROOT_DIR/scripts/stability-test.sh"

echo "[6/6] Release gate status"
echo "release-gate: PASS"

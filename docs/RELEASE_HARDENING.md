# Release Hardening (Launch Gate + SLO)

## Launch gate policy

### Blocking gates (must pass)
1. `bash ./scripts/release-gate.sh`
2. `bash ./scripts/check-headers.sh`
3. `bash ./scripts/check-error-shape.sh`
4. `bash ./scripts/verify-public-surface.sh`
5. `REQUEST_COUNT=3 bash ./scripts/stability-test.sh`

### Advisory gates (do not block deploy, but must be reviewed)
1. 24-hour `api_request_logs` scan for `5xx`, `UPSTREAM_TIMEOUT`, and `RATE_LIMITED` spikes.
2. p95 latency trend for `/api/match-context` and `/api/trends` versus SLO targets.
3. Top 10 error-code distribution review (to catch new error families before users do).

## Header policy (locked)

### Security headers (all routes)
Applied in `vercel.json` and reinforced by API handlers:
- `Strict-Transport-Security: max-age=63072000; includeSubDomains; preload`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Referrer-Policy: strict-origin-when-cross-origin`
- `Permissions-Policy: camera=(), microphone=(), geolocation=()`

### Cache policy
- `/api/match-context`: `Cache-Control: no-store, max-age=0`
- `/api/trends`: `Cache-Control: public, max-age=30, s-maxage=60, stale-while-revalidate=120`
- `/api/demo-matches`: `Cache-Control: public, max-age=60, s-maxage=300`

### CORS policy
- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: GET, OPTIONS`
- `Access-Control-Allow-Headers: Content-Type, x-api-key`
- `Access-Control-Max-Age: 86400`

### Error response consistency
All API errors must return:
```json
{
  "error": {
    "code": "UPPER_SNAKE_CASE",
    "message": "Human-readable explanation"
  }
}
```

## SLO and error budget

### SLO targets (monthly)
1. Availability:
   - `/api/match-context`: 99.5%
   - `/api/trends`: 99.5%
   - `/api/demo-matches`: 99.9%
2. Latency:
   - `/api/match-context`: p95 <= 1500 ms
   - `/api/trends`: p95 <= 1200 ms
   - `/api/demo-matches`: p95 <= 300 ms
3. Correctness:
   - 100% of published sample IDs return HTTP 200 and all required schema sections.

### Error budget
- 99.5% monthly availability budget = 216 minutes downtime/month.
- 99.9% monthly availability budget = 43.2 minutes downtime/month.

### Burn policy
1. Fast burn (page immediately): >2% budget consumed in 1 hour.
2. Slow burn (freeze new non-critical changes): >25% budget consumed in 7 days.
3. Exhausted budget (only reliability work): >100% budget consumed in month.

## CI wiring
- Workflow: `.github/workflows/release-gate.yml`
- Single blocking command: `bash ./scripts/release-gate.sh`
- Optional secret for protected checks: `SPORTSYNC_PUBLIC_CHECK_API_KEY`

# AGENTS.md — AI Match Context API (sportsync-api)

## What This Project Is

A standalone API product that delivers pre-joined match intelligence payloads in a single call. Buyers are AI builders, content automators, and betting tool developers who need structured match context (odds, form, injuries, H2H, trends, player props) without building their own data pipeline.

**One-line promise:** "Get a full pre-joined match intelligence payload in one call for AI previews, betting tools, and content generation."

## Task Format

When you receive a task, it will be in one of two formats:

### Product Brief (short form)
```
Brief: <name>
What: <plain language description>
Why: <urgency / context>
Done when: <observable proof>
Don't break: <blast radius>
Priority: now | soon | later
```

When you receive a Product Brief, expand it into a full Task Batch before executing. Map fields as follows:
- "What" → Objective + Requirements
- "Why" → Context
- "Done when" → Acceptance Criteria (binary pass/fail, each with verification)
- "Don't break" → Constraints
- "Priority" → P0 (now, blocking) | P1 (now) | P2 (soon) | P3 (later)

### Task Batch (full spec)
If the task is already a full Task Batch with Objective, Scope, Requirements, Acceptance Criteria, Constraints, Validation, and Delivery sections — execute directly against the spec.

## Stack

- **Backend**: Supabase (Postgres + Edge Functions)
- **Supabase Project Ref**: `hylnixnuabtnmjcdnujm`
- **Region**: `us-east-1`
- **Plan**: Pro ($10/mo, Workflow org)
- **Endpoint**: `https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context`
- **Frontend**: Static HTML pages (no framework, no build step)
- **Auth**: API key via `x-api-key` header, SHA-256 hashed, validated against `api_keys` table

## Database (18 Tables — All With RLS)

| Table | Purpose |
|---|---|
| `leagues` | Canonical league registry |
| `teams` | Normalized team identities |
| `team_mappings` | Entity resolution (provider → canonical) |
| `venues` | Match locations |
| `matches` | Core match registry |
| `market_odds` | Betting market snapshots (time-series) |
| `team_form` | Rolling performance context |
| `head_to_head` | H2H history cache |
| `injury_reports` | Per-team injury reports |
| `team_injury_impact` | Aggregate injury impact scoring |
| `player_context` | Key player stats + prop lines |
| `team_trends` | Statistical/situational trends |
| `prediction_markets` | Polymarket data |
| `live_state` | Real-time game state |
| `valuation` | Model-derived fair values |
| `api_keys` | Customer auth (SHA-256 hashed keys) |
| `api_request_logs` | Full request observability |
| `rate_limit_buckets` | Sliding window rate limiting |

### Database Conventions

- Rate limiting is handled via the `check_rate_limit` RPC function (sliding window, per-minute + daily caps).
- All functions use `SET search_path = public` (security advisory fixed).
- `pg_trgm` extension lives in `extensions` schema, not `public`.
- Entity resolution uses `team_mappings` for provider → canonical team ID lookups.
- Market odds are time-series: each snapshot is a new row, not an upsert.

## V1 Response Contract (FROZEN)

The v1 response schema is defined in `schema/v1-response-contract.json`. It has 10 mandatory sections:

```
match, teams, market, form, h2h, injuries, players, trends, live_state, metadata
```

**Rules:**
- Missing sections return `{ "available": false, "reason": "..." }` — never null, never omitted.
- Every section includes a `freshness` timestamp.
- `metadata.schema_version` must always be `"1.0"`.
- A sample valid response is in `schema/sample-response-v1.json`.

## Edge Function: `match-context`

The core endpoint. Accepts:
- `GET /match-context?match_id={uuid}&league={optional}`
- Header: `x-api-key: <key>`

Response flow:
1. Validate API key (SHA-256 hash lookup in `api_keys`)
2. Check rate limit (`check_rate_limit` RPC)
3. Fetch match from `matches` table
4. Hydrate all 10 sections from respective tables
5. Return full payload with per-section freshness
6. Log request to `api_request_logs`

Error responses are structured JSON:
| Scenario | HTTP | Code |
|---|---|---|
| No API key | 401 | `MISSING_API_KEY` |
| Invalid API key | 401 | `INVALID_API_KEY` |
| Missing match_id | 400 | `MISSING_MATCH_ID` |
| Match not found | 404 | `MATCH_NOT_FOUND` |
| Rate limited | 429 | `RATE_LIMITED` |

## File Structure

```
sportsync-api/
├── AGENTS.md                           # This file
├── index.html                          # Landing page
├── demo/index.html                     # Interactive API demo
├── docs/index.html                     # API documentation
└── schema/
    ├── v1-response-contract.json       # Frozen v1 JSON Schema
    └── sample-response-v1.json         # Example API response
```

## Test Credentials

| Key | Value |
|---|---|
| **API Key** | `sk_test_sportsync_beta_001` |
| **Test Match** | `b42fe447-b2b1-485f-ae6d-1559ee2b57c7` |

### Smoke Test
```bash
curl -H "x-api-key: sk_test_sportsync_beta_001" \
  "https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context?match_id=b42fe447-b2b1-485f-ae6d-1559ee2b57c7"
```

Expected: HTTP 200 with all 10 sections populated.

## Code Standards

- **No stubs, TODOs, placeholders, or mock logic.** Every function complete, every error path handled.
- **No modification to the v1 response schema** without explicit approval. The contract is frozen.
- **No changes to auth logic** (API key validation, SHA-256 hashing) without explicit approval.
- **No changes to rate limiting logic** without explicit approval.
- **Graceful degradation only** — missing data returns `available: false`, never crashes the response.

## What NOT to Touch

Unless the task explicitly says otherwise:
- Do not modify `schema/v1-response-contract.json` (frozen).
- Do not change the API key validation flow.
- Do not alter the `check_rate_limit` RPC or rate limiting tables.
- Do not remove or rename any of the 10 response sections.
- Do not change Supabase project settings or RLS policies without explicit approval.

## Validation Checklist

After every task, before committing:
1. Smoke test passes (curl command above returns HTTP 200).
2. Error responses match the table above (401/400/404/429).
3. If DB changes: verify with SELECT that data looks correct.
4. If Edge Function changes: check Supabase logs for successful execution.
5. If schema changes: verify `sample-response-v1.json` still validates against `v1-response-contract.json`.
6. If HTML changes: visually confirm the affected page renders correctly.

## Sprint Context (21-Day Plan)

**Current: Day 1 complete. Scorecard: 5/10.**

### Remaining Day 2–7 (Stability Gate)
- [ ] 20 repeat calls, verify no schema drift
- [ ] Seed EPL + NBA matches for multi-sport proof
- [ ] Verify 3 sample payloads stable
- [ ] Error logs + freshness timestamps verified

### Week 2 (Package — Days 8–14)
- [ ] Docs verified against real output
- [ ] AI-generated match preview proof artifact
- [ ] Before/After comparison page
- [ ] Landing page: pricing tiers, CTA, payload explorer
- [ ] External person can make first call without help

### Week 3 (Sell Beta — Days 15–21)
- [ ] 20-target outreach list
- [ ] 1-page product brief
- [ ] Outreach messages
- [ ] Beta onboarding
- [ ] First pricing feedback

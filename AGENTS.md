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
- **API Project Ref**: `hylnixnuabtnmjcdnujm` (dedicated clean surface for the API product)
- **Main Data Project Ref**: `qffzvrnbzabcokqqrwbv` (source of truth for live sports data — The Drip platform)
- **Region**: `us-east-1`
- **Plan**: Pro ($10/mo, Workflow org)
- **Endpoint**: `https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context`
- **Frontend**: Static HTML pages (no framework, no build step)
- **Auth**: API key via `x-api-key` header, SHA-256 hashed, validated against `api_keys` table

## Database Schema (18 Tables — All With RLS)

**CRITICAL: Use ONLY the exact column names listed below. Do NOT guess or invent column names.**

### `leagues` — Canonical league registry
| Column | Type | Notes |
|---|---|---|
| `id` | text | PK. Slug-style: `'eng.1'`, `'nba'`, `'fifa.wc'` |
| `name` | text | Display name |
| `sport` | text | CHECK: soccer, basketball, football, hockey, baseball, tennis |
| `country` | text? | Nullable |
| `tier` | integer? | Default 1 |
| `active` | boolean? | Default true |
| `created_at` | timestamptz? | Default now() |
| `updated_at` | timestamptz? | Default now() |

### `teams` — Normalized team identities
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK, default gen_random_uuid() |
| `canonical_name` | text | UNIQUE. The authoritative name |
| `code` | text? | UNIQUE. Short code: `'ARS'`, `'LAL'` |
| `display_name` | text | For UI rendering |
| `league_id` | text? | FK → leagues.id |
| `conference` | text? | e.g. `'West'`, `'East'` |
| `division` | text? | e.g. `'Pacific'`, `'Atlantic'` |
| `logo_url` | text? | |
| `ranking` | integer? | |
| `wins` | integer? | Default 0 |
| `losses` | integer? | Default 0 |
| `draws` | integer? | Default 0 |
| `created_at` | timestamptz? | Default now() |
| `updated_at` | timestamptz? | Default now() |

### `team_mappings` — Entity resolution (provider → canonical)
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `provider` | text | e.g. `'espn'`, `'odds_api'`, `'sofascore'` |
| `external_name` | text | The provider's name for the team |
| `canonical_name` | text | FK → teams.canonical_name |
| `league_id` | text? | FK → leagues.id |
| `confidence` | numeric? | Default 1.0 |
| `auto_healed` | boolean? | Default false |
| `created_at` | timestamptz? | Default now() |

⚠️ **No `team_id` column.** Links via `canonical_name`, not UUID.

### `venues` — Match locations
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `name` | text | |
| `city` | text | |
| `country` | text | |
| `state` | text? | |
| `capacity` | integer? | |
| `latitude` | numeric? | |
| `longitude` | numeric? | |
| `timezone` | text? | |
| `created_at` | timestamptz? | Default now() |

⚠️ **No `surface` or `indoor` columns.**

### `matches` — Core match registry
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `league_id` | text | FK → leagues.id |
| `sport` | text | |
| `season` | text | Default `'2025-26'` |
| `matchday` | integer? | |
| `stage` | text? | Default `'regular_season'` |
| `status` | text | CHECK: scheduled, live, halftime, finished, postponed, cancelled |
| `home_team_id` | uuid | FK → teams.id |
| `away_team_id` | uuid | FK → teams.id |
| `venue_id` | uuid? | FK → venues.id |
| `start_time` | timestamptz | |
| `home_score` | integer? | |
| `away_score` | integer? | |
| `espn_id` | text? | Separate column, NOT inside a jsonb blob |
| `odds_api_id` | text? | Separate column |
| `sofascore_id` | text? | Separate column |
| `created_at` | timestamptz? | Default now() |
| `updated_at` | timestamptz? | Default now() |

⚠️ **No `external_ids` jsonb column.** External IDs are separate text columns: `espn_id`, `odds_api_id`, `sofascore_id`.

### `market_odds` — Betting market snapshots (time-series)
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `match_id` | uuid | FK → matches.id |
| `source` | text | e.g. `'consensus'`, `'odds_api'` |
| `market_type` | text | Default `'full_game'` |
| `spread` | numeric? | |
| `spread_juice` | numeric? | |
| `total` | numeric? | |
| `total_juice` | numeric? | |
| `home_ml` | integer? | |
| `away_ml` | integer? | |
| `draw_ml` | integer? | NULL for NBA |
| `home_implied_prob` | numeric? | |
| `away_implied_prob` | numeric? | |
| `fetched_at` | timestamptz | Default now() |
| `created_at` | timestamptz? | Default now() |

⚠️ **No `snapshot_at` or `book_odds` columns.** Timestamp column is `fetched_at`.

### `team_form` — Rolling performance context
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `team_id` | uuid | FK → teams.id |
| `match_id` | uuid | FK → matches.id |
| `last_5` | text? | e.g. `'WWDWW'` |
| `last_10_record` | text? | e.g. `'7-2-1'` |
| `ats_last_10` | numeric? | |
| `ats_season` | numeric? | |
| `over_under_pct` | numeric? | |
| `avg_points_scored` | numeric? | |
| `avg_points_allowed` | numeric? | |
| `home_record` | text? | |
| `away_record` | text? | |
| `rest_days` | integer? | |
| `fatigue_score` | numeric? | Default 0 |
| `situation` | text? | Default `'Normal'` |
| `computed_at` | timestamptz? | Default now() |

⚠️ **No `snapshot_at` or `created_at`.** Timestamp column is `computed_at`.

### `head_to_head` — H2H history cache
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `team_a_id` | uuid | FK → teams.id |
| `team_b_id` | uuid | FK → teams.id |
| `league_id` | text? | FK → leagues.id |
| `total_meetings` | integer? | Default 0 |
| `team_a_wins` | integer? | Default 0 |
| `team_b_wins` | integer? | Default 0 |
| `draws` | integer? | Default 0 |
| `recent_matches` | jsonb? | Default `'[]'` |
| `computed_at` | timestamptz? | Default now() |

⚠️ **No `match_id`, `home_team_id`, `away_team_id`, `home_wins`, `away_wins`.** Uses `team_a_id`/`team_b_id` and `team_a_wins`/`team_b_wins`.

### `injury_reports` — Per-team injury reports
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `team_id` | uuid | FK → teams.id |
| `player_name` | text | |
| `position` | text? | |
| `status` | text | CHECK: out, doubtful, questionable, probable, day-to-day |
| `injury` | text? | |
| `impact` | text? | CHECK: high, medium, low |
| `source` | text? | e.g. `'espn'` |
| `reported_at` | timestamptz? | Default now() |
| `created_at` | timestamptz? | Default now() |

⚠️ **No `match_id`, `expected_return`, or `notes` columns.** Linked to teams only, not matches.

### `team_injury_impact` — Aggregate injury impact scoring
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `team_id` | uuid | FK → teams.id |
| `match_id` | uuid | FK → matches.id |
| `impact_score` | numeric? | Default 0 |
| `summary` | text? | |
| `computed_at` | timestamptz? | Default now() |

⚠️ **No `generated_at`.** Timestamp column is `computed_at`.

### `player_context` — Key player stats + prop lines
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `match_id` | uuid | FK → matches.id |
| `team_id` | uuid | FK → teams.id |
| `player_name` | text | |
| `position` | text? | |
| `stats_season` | jsonb? | Default `'{}'`. Season-long stats |
| `stats_recent` | jsonb? | Default `'{}'`. Last-N-game stats |
| `prop_lines` | jsonb? | Default `'[]'`. Array of prop objects |
| `created_at` | timestamptz? | Default now() |

⚠️ **No `status`, `stats`, `prop_market`, `prop_line`, `over_price`, `under_price`, `notes`, or `snapshot_at`.** Stats split into `stats_season`/`stats_recent`. Props go in `prop_lines` jsonb array: `[{"market": "points", "line": 29.5, "over_price": -110, "under_price": -110}]`.

### `team_trends` — Statistical/situational trends
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `team_id` | uuid | FK → teams.id |
| `match_id` | uuid | FK → matches.id |
| `offensive_rating` | numeric? | |
| `defensive_rating` | numeric? | |
| `pace` | numeric? | |
| `scoring_splits` | jsonb? | Default `'{}'` |
| `situational` | jsonb? | Default `'[]'` |
| `computed_at` | timestamptz? | Default now() |

⚠️ **No `summary`, `trend_values`, or `generated_at`.** Use `scoring_splits` (jsonb object) and `situational` (jsonb array of `{"label": "...", "value": ...}` objects).

### `prediction_markets` — Polymarket data
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `match_id` | uuid | FK → matches.id |
| `source` | text | Default `'polymarket'` |
| `home_win_prob` | numeric? | |
| `draw_prob` | numeric? | NULL for NBA |
| `away_win_prob` | numeric? | |
| `volume_usd` | numeric? | |
| `fetched_at` | timestamptz | Default now() |
| `created_at` | timestamptz? | Default now() |

### `live_state` — Real-time game state
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `match_id` | uuid | FK → matches.id, UNIQUE |
| `clock` | text? | |
| `period` | text? | |
| `home_score` | integer? | Default 0 |
| `away_score` | integer? | Default 0 |
| `possession` | text? | CHECK: home, away |
| `momentum` | text? | CHECK: home, away, neutral |
| `key_events` | jsonb? | Default `'[]'` |
| `updated_at` | timestamptz? | Default now() |

### `valuation` — Model-derived fair values
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `match_id` | uuid | FK → matches.id |
| `fair_line` | numeric? | |
| `market_line` | numeric? | |
| `delta` | numeric? | |
| `has_model` | boolean? | Default false |
| `model_version` | text? | e.g. `'sportsync-v1-soccer'` |
| `computed_at` | timestamptz? | Default now() |

⚠️ **No `model_name` or `generated_at`.** Model identifier is `model_version`. Timestamp is `computed_at`.

### `api_keys` — Customer auth
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `key_hash` | text | UNIQUE. SHA-256 of raw key |
| `key_prefix` | text | First 8 chars for identification |
| `name` | text | Customer/app name |
| `email` | text? | |
| `tier` | text | Default `'builder'`. CHECK: builder, pro, operator |
| `rate_limit_per_minute` | integer | Default 30 |
| `rate_limit_per_day` | integer | Default 1000 |
| `monthly_request_cap` | integer? | |
| `active` | boolean? | Default true |
| `expires_at` | timestamptz? | |
| `created_at` | timestamptz? | Default now() |
| `last_used_at` | timestamptz? | |

### `api_request_logs` — Full request observability
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `api_key_id` | uuid? | FK → api_keys.id |
| `endpoint` | text | Default `'/match-context'` |
| `match_id` | uuid? | |
| `league_id` | text? | |
| `sport` | text? | |
| `status_code` | integer | |
| `response_time_ms` | integer? | |
| `error_code` | text? | |
| `error_message` | text? | |
| `sections_available` | jsonb? | Default `'{}'` |
| `ip_address` | inet? | |
| `user_agent` | text? | |
| `created_at` | timestamptz? | Default now() |

### `rate_limit_buckets` — Sliding window rate limiting
| Column | Type | Notes |
|---|---|---|
| `id` | uuid | PK |
| `api_key_id` | uuid | FK → api_keys.id |
| `window_start` | timestamptz | |
| `window_type` | text | CHECK: minute, day |
| `request_count` | integer? | Default 0 |

### Database Conventions

- Rate limiting is handled via the `check_rate_limit` RPC function (sliding window, per-minute + daily caps).
- All functions use `SET search_path = public` (security advisory fixed).
- `pg_trgm` extension lives in `extensions` schema, not `public`.
- Entity resolution uses `team_mappings` linking `external_name` → `canonical_name` (FK to teams), NOT by team UUID.
- Market odds are time-series: each snapshot is a new row, not an upsert.
- `leagues.id` is a text slug (`'eng.1'`, `'nba'`), NOT a UUID.
- `head_to_head` has NO `match_id` — it's a team-pair cache, not per-match.
- `injury_reports` has NO `match_id` — injuries are linked to teams only.

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
├── scripts/
│   └── stability-test.sh              # 20-call stability gate per match
└── schema/
    ├── v1-response-contract.json       # Frozen v1 JSON Schema
    └── sample-response-v1.json         # Example API response
```

## Test Credentials & Seeded Matches

| Key | Value |
|---|---|
| **API Key** | `sk_test_sportsync_beta_001` |

### Seeded Matches (all return HTTP 200)
| Match | ID | Sport | Sections |
|---|---|---|---|
| 🇺🇸 USA vs Mexico | `b42fe447-b2b1-485f-ae6d-1559ee2b57c7` | soccer (FIFA WC) | market ✅ form ✅ injuries ✅ (h2h/players/trends not seeded) |
| ⚽ Arsenal vs Man City | `b193b51f-cbed-4398-a297-237dd3322607` | soccer (EPL) | ALL sections ✅ |
| 🏀 Lakers vs Celtics | `d6742e61-2457-43fd-aa3f-e61f6a76c7af` | basketball (NBA) | ALL sections ✅ |
| 🇬🇧 England vs Brazil | `c94d7e01-333d-41cd-a67d-cc0285fa7f28` | soccer (FIFA WC) | Match only (no supporting data) |

### Smoke Test
```bash
curl -H "x-api-key: sk_test_sportsync_beta_001" \
  "https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context?match_id=b193b51f-cbed-4398-a297-237dd3322607"
```

Expected: HTTP 200 with all 10 sections populated.

## Code Standards

- **No stubs, TODOs, placeholders, or mock logic.** Every function complete, every error path handled.
- **No modification to the v1 response schema** without explicit approval. The contract is frozen.
- **No changes to auth logic** (API key validation, SHA-256 hashing) without explicit approval.
- **No changes to rate limiting logic** without explicit approval.
- **Graceful degradation only** — missing data returns `available: false`, never crashes the response.
- **Use exact column names from the schema above.** Do NOT guess or invent column names. If unsure, query the database first.

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

**Current: Day 2 in progress. Scorecard: 5/10.**

### Day 2 Status
- [x] EPL + NBA matches seeded with full supporting data
- [ ] 20 repeat calls stability test (script exists at `scripts/stability-test.sh`)
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

# Kalshi Sports Discovery (2026-03-15 UTC)

Source endpoint:
- `GET https://api.elections.kalshi.com/trade-api/v2/series`

Results:
- Total series returned: `9019`
- Sports series (`category = Sports`): `1549`
- Full snapshot: `kalshi-sports-series-20260315.tsv`

Target game-winner series verified for this backfill:
- `KXNBAGAME` (NBA game winner)
- `KXNHLGAME` (NHL game winner)
- `KXNFLGAME` (NFL game winner)
- `KXNCAAMBGAME` (Men's college basketball game winner; used as NCAAB equivalent)

Settled date ranges discovered:
- `KXNBAGAME`: `2025-04-15` to `2026-03-14`
- `KXNHLGAME`: `2025-04-19` to `2026-03-14`
- `KXNFLGAME`: `2025-07-31` to `2026-01-25`
- `KXNCAAMBGAME`: `2025-11-03` to `2026-03-14`

Earliest settled date across target series:
- `2025-04-15` (NBA)

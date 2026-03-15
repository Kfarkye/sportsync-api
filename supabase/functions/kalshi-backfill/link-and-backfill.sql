-- Step 4: Link Kalshi winner-side settlements to existing matches.
-- Uses league + team mapping + date proximity window to handle timezone cutovers.
with candidate as (
  select distinct on (ks.market_ticker)
    ks.market_ticker,
    m.id as match_id,
    abs((m.start_time::date - ks.game_date)) as day_delta,
    m.start_time
  from kalshi_settlements ks
  join kalshi_team_map ktm
    on ktm.league = ks.league
   and ktm.kalshi_name = ks.team_name
   and ktm.kalshi_abbrev = split_part(ks.market_ticker, '-', array_length(string_to_array(ks.market_ticker, '-'), 1))
  join matches m
    on m.league_id = case
      when ks.league = 'ncaab' then 'mens-college-basketball'
      else ks.league
    end
   and (m.home_team = ktm.espn_name or m.away_team = ktm.espn_name)
   and m.start_time::date between ks.game_date - interval '1 day' and ks.game_date + interval '1 day'
  where ks.match_id is null
    and ks.result = 'yes'
  order by ks.market_ticker, day_delta asc, m.start_time asc
)
update kalshi_settlements ks
set match_id = candidate.match_id
from candidate
where ks.market_ticker = candidate.market_ticker;

-- Step 5: Fill closing_lines for matched games that do not already have one.
-- Converts implied probabilities to American ML odds, one row per match.
with home_side as (
  select
    ks.match_id,
    ks.league,
    ks.game_date,
    ks.closing_price as home_prob_raw,
    opp.closing_price as away_prob_raw
  from kalshi_settlements ks
  join kalshi_settlements opp
    on opp.event_ticker = ks.event_ticker
   and opp.market_ticker <> ks.market_ticker
  where ks.match_id is not null
    and ks.is_home_team = true
),
normalized as (
  select
    match_id,
    case when league = 'ncaab' then 'mens-college-basketball' else league end as league_id,
    game_date,
    greatest(0.01, least(0.99,
      coalesce(home_prob_raw, case when away_prob_raw is not null then 1 - away_prob_raw else null end)
    )) as home_prob,
    greatest(0.01, least(0.99,
      coalesce(away_prob_raw, case when home_prob_raw is not null then 1 - home_prob_raw else null end)
    )) as away_prob
  from home_side
),
dedup as (
  select distinct on (match_id)
    match_id, league_id, game_date, home_prob, away_prob
  from normalized
  where home_prob is not null
    and away_prob is not null
  order by match_id, game_date desc
)
insert into closing_lines (id, match_id, total, home_spread, away_spread, home_ml, away_ml, league_id, created_at)
select
  gen_random_uuid(),
  d.match_id,
  null,
  null,
  null,
  case
    when d.home_prob >= 0.5 then round(-100 * d.home_prob / nullif(1 - d.home_prob, 0))::int
    else round(100 * (1 - d.home_prob) / nullif(d.home_prob, 0))::int
  end as home_ml,
  case
    when d.away_prob >= 0.5 then round(-100 * d.away_prob / nullif(1 - d.away_prob, 0))::int
    else round(100 * (1 - d.away_prob) / nullif(d.away_prob, 0))::int
  end as away_ml,
  d.league_id,
  d.game_date::timestamptz
from dedup d
where not exists (
  select 1 from closing_lines cl where cl.match_id = d.match_id
);

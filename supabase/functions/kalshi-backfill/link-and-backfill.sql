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

-- Step 6: Backfill spread/total fields from Kalshi line markets.
-- Primary anchor: matched winner-side settlements.
-- Fallback anchor: direct title-team mapping against matches (covers events missing winner links).
with winner_matched_events as (
  select
    lower(league) as league,
    split_part(event_ticker, '-', 2) as event_key,
    max(match_id) as match_id
  from kalshi_settlements
  where match_id is not null
    and result = 'yes'
  group by 1,2
),
title_teams as (
  select distinct
    lower(lm.league) as league,
    split_part(lm.event_ticker, '-', 2) as event_key,
    lm.game_date,
    trim(split_part(split_part(coalesce(lm.title, ''), ':', 1), ' at ', 1)) as away_kalshi_name,
    trim(split_part(split_part(coalesce(lm.title, ''), ':', 1), ' at ', 2)) as home_kalshi_name
  from kalshi_line_markets lm
  where split_part(split_part(coalesce(lm.title, ''), ':', 1), ' at ', 2) <> ''
),
direct_matched_events as (
  select distinct on (tt.league, tt.event_key)
    tt.league,
    tt.event_key,
    m.id as match_id,
    abs((m.start_time::date - tt.game_date)) as day_delta,
    m.start_time
  from title_teams tt
  join kalshi_team_map away_map
    on away_map.league = tt.league
   and away_map.kalshi_name = tt.away_kalshi_name
  join kalshi_team_map home_map
    on home_map.league = tt.league
   and home_map.kalshi_name = tt.home_kalshi_name
  join matches m
    on m.league_id = case
      when tt.league = 'ncaab' then 'mens-college-basketball'
      else tt.league
    end
   and m.away_team = away_map.espn_name
   and m.home_team = home_map.espn_name
   and m.start_time::date between tt.game_date - interval '1 day' and tt.game_date + interval '1 day'
  order by tt.league, tt.event_key, day_delta asc, m.start_time asc
),
matched_events as (
  select distinct on (league, event_key)
    league,
    event_key,
    match_id
  from (
    select league, event_key, match_id, 1 as priority
    from winner_matched_events
    union all
    select league, event_key, match_id, 2 as priority
    from direct_matched_events
  ) ranked
  order by league, event_key, priority
),
line_join as (
  select
    lm.*,
    me.match_id,
    regexp_replace(lower(trim(split_part(split_part(coalesce(lm.title, ''), ':', 1), ' at ', 1))), '[^a-z0-9]+', '', 'g') as away_key,
    regexp_replace(lower(trim(split_part(split_part(coalesce(lm.title, ''), ':', 1), ' at ', 2))), '[^a-z0-9]+', '', 'g') as home_key,
    regexp_replace(lower(trim(regexp_replace(coalesce(lm.line_side, ''), '[[:space:]]+wins[[:space:]]+by[[:space:]]+over.*$', '', 'i'))), '[^a-z0-9]+', '', 'g') as side_key
  from kalshi_line_markets lm
  join matched_events me
    on me.league = lower(lm.league)
   and me.event_key = split_part(lm.event_ticker, '-', 2)
),
ranked_totals as (
  select
    match_id,
    line_value,
    row_number() over (
      partition by match_id
      order by coalesce(volume, 0) desc, coalesce(open_interest, 0) desc, line_value asc
    ) as rn
  from line_join
  where market_kind = 'total'
    and line_value is not null
),
selected_totals as (
  select match_id, line_value as total
  from ranked_totals
  where rn = 1
),
ranked_spreads as (
  select
    match_id,
    line_value,
    case
      when side_key <> '' and side_key = home_key then -abs(line_value)
      when side_key <> '' and side_key = away_key then abs(line_value)
      else null
    end as home_spread,
    case
      when side_key <> '' and side_key = home_key then abs(line_value)
      when side_key <> '' and side_key = away_key then -abs(line_value)
      else null
    end as away_spread,
    row_number() over (
      partition by match_id
      order by coalesce(volume, 0) desc, coalesce(open_interest, 0) desc, line_value asc
    ) as rn
  from line_join
  where market_kind = 'spread'
    and line_value is not null
),
selected_spreads as (
  select match_id, home_spread, away_spread
  from ranked_spreads
  where rn = 1
    and home_spread is not null
    and away_spread is not null
),
merged_lines as (
  select
    coalesce(t.match_id, s.match_id) as match_id,
    t.total,
    s.home_spread,
    s.away_spread
  from selected_totals t
  full join selected_spreads s on s.match_id = t.match_id
)
update closing_lines cl
set
  total = coalesce(cl.total, ml.total),
  home_spread = coalesce(cl.home_spread, ml.home_spread),
  away_spread = coalesce(cl.away_spread, ml.away_spread),
  league_id = coalesce(cl.league_id, m.league_id)
from merged_lines ml
join matches m on m.id = ml.match_id
where cl.match_id = ml.match_id
  and (
    (cl.total is null and ml.total is not null)
    or (cl.home_spread is null and ml.home_spread is not null)
    or (cl.away_spread is null and ml.away_spread is not null)
    or (cl.league_id is null and m.league_id is not null)
  );

insert into closing_lines (
  id,
  match_id,
  total,
  home_spread,
  away_spread,
  home_ml,
  away_ml,
  league_id,
  created_at
)
select
  gen_random_uuid(),
  ml.match_id,
  ml.total,
  ml.home_spread,
  ml.away_spread,
  null,
  null,
  m.league_id,
  now()
from merged_lines ml
join matches m on m.id = ml.match_id
where not exists (
  select 1
  from closing_lines cl
  where cl.match_id = ml.match_id
);

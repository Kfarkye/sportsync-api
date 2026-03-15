-- NBA totals diagnostic + post-fix assertion
-- Snapshot (2026-03-15):
-- - total nba totals rows: 11,079
-- - parse failures (line_value is null): 0
-- - linkage failures before fallback (winner-event anchor only): 44
-- - linkage failures after fallback (title+team-map direct join): 0

with nba_totals as (
  select
    lm.event_ticker,
    lm.market_ticker,
    lm.title,
    lm.line_value,
    lm.game_date,
    split_part(lm.event_ticker, '-', 2) as event_key,
    lower(lm.league) as league
  from kalshi_line_markets lm
  where lower(lm.league) = 'nba'
    and lm.market_kind = 'total'
),
winner_links as (
  select
    lower(ks.league) as league,
    split_part(ks.event_ticker, '-', 2) as event_key,
    max(ks.match_id) as match_id
  from kalshi_settlements ks
  where lower(ks.league) = 'nba'
    and ks.result = 'yes'
    and ks.match_id is not null
  group by 1,2
),
title_teams as (
  select distinct
    nt.league,
    nt.event_key,
    nt.game_date,
    trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 1)) as away_kalshi_name,
    trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2)) as home_kalshi_name
  from nba_totals nt
  where split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2) <> ''
),
direct_links as (
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
    on m.league_id = tt.league
   and m.away_team = away_map.espn_name
   and m.home_team = home_map.espn_name
   and m.start_time::date between tt.game_date - interval '1 day' and tt.game_date + interval '1 day'
  order by tt.league, tt.event_key, day_delta asc, m.start_time asc
),
classified as (
  select
    nt.*,
    wl.match_id as winner_match_id,
    dl.match_id as fallback_match_id,
    case
      when nt.line_value is null then 'parse_failure'
      when wl.match_id is null then 'game_match_failure_before_fallback'
      else 'ok'
    end as before_status,
    case
      when nt.line_value is null then 'parse_failure'
      when coalesce(wl.match_id, dl.match_id) is null then 'game_match_failure'
      else 'ok'
    end as after_status
  from nba_totals nt
  left join winner_links wl
    on wl.league = nt.league
   and wl.event_key = nt.event_key
  left join direct_links dl
    on dl.league = nt.league
   and dl.event_key = nt.event_key
)
select
  before_status as status,
  count(*) as cnt,
  array_remove(array_agg(distinct title order by title), null)[1:10] as sample_titles
from classified
group by before_status
order by cnt desc;

with nba_totals as (
  select
    lm.event_ticker,
    lm.market_ticker,
    lm.title,
    lm.line_value,
    lm.game_date,
    split_part(lm.event_ticker, '-', 2) as event_key,
    lower(lm.league) as league
  from kalshi_line_markets lm
  where lower(lm.league) = 'nba'
    and lm.market_kind = 'total'
),
winner_links as (
  select
    lower(ks.league) as league,
    split_part(ks.event_ticker, '-', 2) as event_key,
    max(ks.match_id) as match_id
  from kalshi_settlements ks
  where lower(ks.league) = 'nba'
    and ks.result = 'yes'
    and ks.match_id is not null
  group by 1,2
),
title_teams as (
  select distinct
    nt.league,
    nt.event_key,
    nt.game_date,
    trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 1)) as away_kalshi_name,
    trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2)) as home_kalshi_name
  from nba_totals nt
  where split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2) <> ''
),
direct_links as (
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
    on m.league_id = tt.league
   and m.away_team = away_map.espn_name
   and m.home_team = home_map.espn_name
   and m.start_time::date between tt.game_date - interval '1 day' and tt.game_date + interval '1 day'
  order by tt.league, tt.event_key, day_delta asc, m.start_time asc
),
classified as (
  select
    nt.*,
    coalesce(wl.match_id, dl.match_id) as resolved_match_id,
    case
      when nt.line_value is null then 'parse_failure'
      when coalesce(wl.match_id, dl.match_id) is null then 'game_match_failure'
      else 'ok'
    end as status
  from nba_totals nt
  left join winner_links wl
    on wl.league = nt.league
   and wl.event_key = nt.event_key
  left join direct_links dl
    on dl.league = nt.league
   and dl.event_key = nt.event_key
)
select
  status,
  count(*) as cnt,
  array_remove(array_agg(distinct title order by title), null)[1:10] as sample_titles
from classified
group by status
order by cnt desc;

-- Post-fix gate: unresolved rows should be < 10.
do $$
declare
  unresolved_count integer;
begin
  with nba_totals as (
    select
      lm.event_ticker,
      lm.line_value,
      lm.game_date,
      split_part(lm.event_ticker, '-', 2) as event_key,
      lower(lm.league) as league,
      lm.title
    from kalshi_line_markets lm
    where lower(lm.league) = 'nba'
      and lm.market_kind = 'total'
  ),
  winner_links as (
    select
      lower(ks.league) as league,
      split_part(ks.event_ticker, '-', 2) as event_key,
      max(ks.match_id) as match_id
    from kalshi_settlements ks
    where lower(ks.league) = 'nba'
      and ks.result = 'yes'
      and ks.match_id is not null
    group by 1,2
  ),
  title_teams as (
    select distinct
      nt.league,
      nt.event_key,
      nt.game_date,
      trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 1)) as away_kalshi_name,
      trim(split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2)) as home_kalshi_name
    from nba_totals nt
    where split_part(split_part(coalesce(nt.title, ''), ':', 1), ' at ', 2) <> ''
  ),
  direct_links as (
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
      on m.league_id = tt.league
     and m.away_team = away_map.espn_name
     and m.home_team = home_map.espn_name
     and m.start_time::date between tt.game_date - interval '1 day' and tt.game_date + interval '1 day'
    order by tt.league, tt.event_key, day_delta asc, m.start_time asc
  )
  select count(*)
  into unresolved_count
  from nba_totals nt
  left join winner_links wl
    on wl.league = nt.league
   and wl.event_key = nt.event_key
  left join direct_links dl
    on dl.league = nt.league
   and dl.event_key = nt.event_key
  where nt.line_value is null
     or coalesce(wl.match_id, dl.match_id) is null;

  if unresolved_count >= 10 then
    raise exception 'NBA totals unresolved count is % (expected < 10)', unresolved_count;
  end if;

  raise notice 'NBA totals unresolved count = % (PASS)', unresolved_count;
end
$$;

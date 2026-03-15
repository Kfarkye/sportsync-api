insert into public.kalshi_team_map (
  kalshi_name,
  espn_name,
  league,
  kalshi_abbrev,
  updated_at
)
values
  ('Arizona Diamondbacks', 'Arizona Diamondbacks', 'mlb', 'AZ', now()),
  ('Athletics', 'Athletics', 'mlb', 'ATH', now()),
  ('Atlanta Braves', 'Atlanta Braves', 'mlb', 'ATL', now()),
  ('Baltimore Orioles', 'Baltimore Orioles', 'mlb', 'BAL', now()),
  ('Boston Red Sox', 'Boston Red Sox', 'mlb', 'BOS', now()),
  ('Chicago Cubs', 'Chicago Cubs', 'mlb', 'CHC', now()),
  ('Chicago White Sox', 'Chicago White Sox', 'mlb', 'CWS', now()),
  ('Cincinnati Reds', 'Cincinnati Reds', 'mlb', 'CIN', now()),
  ('Cleveland Guardians', 'Cleveland Guardians', 'mlb', 'CLE', now()),
  ('Colorado Rockies', 'Colorado Rockies', 'mlb', 'COL', now()),
  ('Detroit Tigers', 'Detroit Tigers', 'mlb', 'DET', now()),
  ('Houston Astros', 'Houston Astros', 'mlb', 'HOU', now()),
  ('Kansas City Royals', 'Kansas City Royals', 'mlb', 'KC', now()),
  ('Los Angeles Angels', 'Los Angeles Angels', 'mlb', 'LAA', now()),
  ('Los Angeles Dodgers', 'Los Angeles Dodgers', 'mlb', 'LAD', now()),
  ('Miami Marlins', 'Miami Marlins', 'mlb', 'MIA', now()),
  ('Milwaukee Brewers', 'Milwaukee Brewers', 'mlb', 'MIL', now()),
  ('Minnesota Twins', 'Minnesota Twins', 'mlb', 'MIN', now()),
  ('New York Mets', 'New York Mets', 'mlb', 'NYM', now()),
  ('New York Yankees', 'New York Yankees', 'mlb', 'NYY', now()),
  ('Philadelphia Phillies', 'Philadelphia Phillies', 'mlb', 'PHI', now()),
  ('Pittsburgh Pirates', 'Pittsburgh Pirates', 'mlb', 'PIT', now()),
  ('San Diego Padres', 'San Diego Padres', 'mlb', 'SD', now()),
  ('San Francisco Giants', 'San Francisco Giants', 'mlb', 'SF', now()),
  ('Seattle Mariners', 'Seattle Mariners', 'mlb', 'SEA', now()),
  ('St. Louis Cardinals', 'St. Louis Cardinals', 'mlb', 'STL', now()),
  ('Tampa Bay Rays', 'Tampa Bay Rays', 'mlb', 'TB', now()),
  ('Texas Rangers', 'Texas Rangers', 'mlb', 'TEX', now()),
  ('Toronto Blue Jays', 'Toronto Blue Jays', 'mlb', 'TOR', now()),
  ('Washington Nationals', 'Washington Nationals', 'mlb', 'WSH', now())
on conflict (league, kalshi_name, kalshi_abbrev)
do update set
  espn_name = excluded.espn_name,
  updated_at = now();

create or replace function public.get_mlb_scoring_trends(p_league text default 'mlb')
returns jsonb
language sql
security definer
set search_path = public
as $$
  with team_abbr_lookup as (
    select
      team as team_name,
      max(team_abbr) as team_abbr
    from public.mlb_pitcher_game_logs
    where team is not null
      and team_abbr is not null
    group by team
  ),
  team_games as (
    select
      mp.id as match_id,
      coalesce(mp.start_time::date, current_date) as game_date,
      extract(year from coalesce(mp.start_time, now()))::integer as season_year,
      coalesce(mp.season_type, 'regular') as season_type,
      mp.home_team as team_name,
      mp.away_team as opponent_name,
      coalesce(mp.home_score, 0)::numeric as runs_for,
      coalesce(mp.away_score, 0)::numeric as runs_allowed,
      mp.home_ops::numeric as ops,
      mp.home_era::numeric as era,
      mp.home_whip::numeric as whip
    from public.mlb_postgame mp
    where mp.home_team is not null and mp.away_team is not null

    union all

    select
      mp.id as match_id,
      coalesce(mp.start_time::date, current_date) as game_date,
      extract(year from coalesce(mp.start_time, now()))::integer as season_year,
      coalesce(mp.season_type, 'regular') as season_type,
      mp.away_team as team_name,
      mp.home_team as opponent_name,
      coalesce(mp.away_score, 0)::numeric as runs_for,
      coalesce(mp.home_score, 0)::numeric as runs_allowed,
      mp.away_ops::numeric as ops,
      mp.away_era::numeric as era,
      mp.away_whip::numeric as whip
    from public.mlb_postgame mp
    where mp.home_team is not null and mp.away_team is not null
  ),
  current_season as (
    select max(season_year) as season_year
    from team_games
  ),
  team_directory as (
    select
      tg.team_name,
      max(tal.team_abbr) as team_abbr
    from team_games tg
    left join team_abbr_lookup tal
      on lower(tal.team_name) = lower(tg.team_name)
    group by tg.team_name
  ),
  current_team_stats as (
    select
      team_name,
      season_year,
      count(*) as games,
      round(avg(runs_for)::numeric, 2) as runs_per_game,
      round(avg(runs_allowed)::numeric, 2) as runs_allowed_per_game,
      round(avg(ops)::numeric, 3) as ops,
      round(avg(era)::numeric, 2) as era,
      round(avg(whip)::numeric, 2) as whip
    from team_games
    where season_year = (select season_year from current_season)
    group by team_name, season_year
  ),
  prior_team_stats as (
    select
      team_name,
      season_year,
      round(avg(runs_for)::numeric, 2) as runs_per_game,
      round(avg(runs_allowed)::numeric, 2) as runs_allowed_per_game,
      round(avg(ops)::numeric, 3) as ops,
      round(avg(era)::numeric, 2) as era,
      round(avg(whip)::numeric, 2) as whip
    from team_games
    where season_year = (select season_year - 1 from current_season)
    group by team_name, season_year
  ),
  current_season_averages as (
    select
      round(avg(runs_per_game)::numeric, 2) as runs_per_game,
      round(avg(runs_allowed_per_game)::numeric, 2) as runs_allowed_per_game,
      round(avg(ops)::numeric, 3) as ops,
      round(avg(era)::numeric, 2) as era,
      round(avg(whip)::numeric, 2) as whip
    from current_team_stats
  )
  select jsonb_build_object(
    'league', lower(coalesce(p_league, 'mlb')),
    'season_year', (select season_year from current_season),
    'generated_at', now(),
    'season_averages', (
      select jsonb_build_object(
        'runs_per_game', runs_per_game,
        'runs_allowed_per_game', runs_allowed_per_game,
        'ops', ops,
        'era', era,
        'whip', whip
      )
      from current_season_averages
    ),
    'teams', coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'team_name', td.team_name,
          'team_abbr', td.team_abbr,
          'games', cts.games,
          'runs_per_game', cts.runs_per_game,
          'runs_allowed_per_game', cts.runs_allowed_per_game,
          'OPS', cts.ops,
          'ERA', cts.era,
          'WHIP', cts.whip,
          'delta_vs_prior', case
            when pts.team_name is null then null
            else jsonb_build_object(
              'runs_per_game', round((cts.runs_per_game - pts.runs_per_game)::numeric, 2),
              'runs_allowed_per_game', round((cts.runs_allowed_per_game - pts.runs_allowed_per_game)::numeric, 2),
              'OPS', round((cts.ops - pts.ops)::numeric, 3),
              'ERA', round((cts.era - pts.era)::numeric, 2),
              'WHIP', round((cts.whip - pts.whip)::numeric, 2)
            )
          end
        )
        order by td.team_name
      )
      from team_directory td
      left join current_team_stats cts
        on lower(cts.team_name) = lower(td.team_name)
      left join prior_team_stats pts
        on lower(pts.team_name) = lower(td.team_name)
    ), '[]'::jsonb)
  );
$$;

drop materialized view if exists public.mv_mlb_team_f5_scoring;
create materialized view public.mv_mlb_team_f5_scoring as
with team_abbr_lookup as (
  select
    team as team_name,
    max(team_abbr) as team_abbr
  from public.mlb_pitcher_game_logs
  where team is not null
    and team_abbr is not null
  group by team
),
odds_by_match as (
  select
    match_id,
    avg(point)::numeric as posted_f5_total
  from public.match_halftime_odds
  where league = 'mlb'
    and market = 'totals_h1'
    and point is not null
  group by match_id
),
team_rows as (
  select
    mis.match_id,
    mis.game_date,
    mis.season_type,
    'home'::text as location,
    mis.home_team as team_name,
    coalesce(mis.home_f5_runs, 0)::numeric as f5_runs,
    coalesce(mis.home_l4_runs, 0)::numeric as l4_runs,
    coalesce(mis.home_first_inning_runs, 0)::numeric as first_inning_runs,
    (coalesce(mis.home_f5_runs, 0) + coalesce(mis.away_f5_runs, 0))::numeric as combined_f5_runs
  from public.mlb_inning_scores mis

  union all

  select
    mis.match_id,
    mis.game_date,
    mis.season_type,
    'away'::text as location,
    mis.away_team as team_name,
    coalesce(mis.away_f5_runs, 0)::numeric as f5_runs,
    coalesce(mis.away_l4_runs, 0)::numeric as l4_runs,
    coalesce(mis.away_first_inning_runs, 0)::numeric as first_inning_runs,
    (coalesce(mis.home_f5_runs, 0) + coalesce(mis.away_f5_runs, 0))::numeric as combined_f5_runs
  from public.mlb_inning_scores mis
)
select
  tr.team_name,
  tal.team_abbr,
  tr.location,
  count(*) as games,
  round(avg(tr.f5_runs)::numeric, 2) as avg_f5_runs,
  round(avg(tr.l4_runs)::numeric, 2) as avg_l4_runs,
  round(avg(case when obm.posted_f5_total is not null then obm.posted_f5_total end)::numeric, 2) as avg_posted_total,
  round(avg(case when obm.posted_f5_total is not null then (tr.combined_f5_runs > obm.posted_f5_total)::int end)::numeric, 3) as f5_over_rate,
  round(avg((tr.first_inning_runs > 0)::int)::numeric, 3) as pct_scoring_first_inning,
  round(avg(tr.first_inning_runs)::numeric, 2) as avg_first_inning_runs
from team_rows tr
left join team_abbr_lookup tal
  on lower(tal.team_name) = lower(tr.team_name)
left join odds_by_match obm
  on obm.match_id = tr.match_id
group by tr.team_name, tal.team_abbr, tr.location;

create unique index if not exists idx_mv_mlb_team_f5_scoring_unique
  on public.mv_mlb_team_f5_scoring (team_name, location);

drop materialized view if exists public.mv_mlb_starter_profiles;
create materialized view public.mv_mlb_starter_profiles as
with ranked as (
  select
    mpgl.*,
    row_number() over (partition by mpgl.athlete_id order by mpgl.game_date desc, mpgl.match_id desc, mpgl.pitch_order asc) as rn
  from public.mlb_pitcher_game_logs mpgl
  where coalesce(mpgl.is_starter, false) = true
    and mpgl.athlete_id is not null
    and mpgl.innings_outs is not null
)
select
  athlete_id,
  max(athlete_name) as athlete_name,
  max(team) filter (where rn = 1) as team,
  max(team_abbr) filter (where rn = 1) as team_abbr,
  count(*) as starts,
  round(public.baseball_outs_to_ip(avg(innings_outs))::numeric, 2) as avg_ip,
  round(avg(strikeouts)::numeric, 2) as avg_k,
  round(avg(walks)::numeric, 2) as avg_bb,
  round(avg(earned_runs)::numeric, 2) as avg_er,
  round(case when sum(innings_outs) > 0 then (sum(earned_runs) * 27.0) / sum(innings_outs) end::numeric, 2) as era,
  round(case when sum(innings_outs) > 0 then ((sum(walks) + sum(hits_allowed)) * 3.0) / sum(innings_outs) end::numeric, 2) as whip,
  round(avg(pitches_thrown)::numeric, 2) as avg_pitches,
  round(public.baseball_outs_to_ip((avg(innings_outs) filter (where home_away = 'home')))::numeric, 2) as home_avg_ip,
  round(public.baseball_outs_to_ip((avg(innings_outs) filter (where home_away = 'away')))::numeric, 2) as road_avg_ip,
  round(case when (sum(innings_outs) filter (where home_away = 'home')) > 0 then ((sum(earned_runs) filter (where home_away = 'home')) * 27.0) / (sum(innings_outs) filter (where home_away = 'home')) end::numeric, 2) as home_era,
  round(case when (sum(innings_outs) filter (where home_away = 'away')) > 0 then ((sum(earned_runs) filter (where home_away = 'away')) * 27.0) / (sum(innings_outs) filter (where home_away = 'away')) end::numeric, 2) as road_era,
  round(case when sum(innings_outs) > 0 then (sum(strikeouts) * 27.0) / sum(innings_outs) end::numeric, 2) as k_rate,
  round(case when sum(innings_outs) > 0 then (sum(walks) * 27.0) / sum(innings_outs) end::numeric, 2) as bb_rate,
  round(case when sum(innings_outs) > 0 then (sum(home_runs_allowed) * 27.0) / sum(innings_outs) end::numeric, 2) as hr_rate,
  round(public.baseball_outs_to_ip((avg(innings_outs) filter (where rn <= 5)))::numeric, 2) as last5_avg_ip,
  round((avg(strikeouts) filter (where rn <= 5))::numeric, 2) as last5_avg_k,
  round((avg(walks) filter (where rn <= 5))::numeric, 2) as last5_avg_bb,
  round((avg(earned_runs) filter (where rn <= 5))::numeric, 2) as last5_avg_er,
  round(case when (sum(innings_outs) filter (where rn <= 5)) > 0 then ((sum(earned_runs) filter (where rn <= 5)) * 27.0) / (sum(innings_outs) filter (where rn <= 5)) end::numeric, 2) as last5_era,
  round(case when (sum(innings_outs) filter (where rn <= 5)) > 0 then (((sum(walks) filter (where rn <= 5)) + (sum(hits_allowed) filter (where rn <= 5))) * 3.0) / (sum(innings_outs) filter (where rn <= 5)) end::numeric, 2) as last5_whip,
  round((avg(pitches_thrown) filter (where rn <= 5))::numeric, 2) as last5_avg_pitches,
  max(game_date) as last_game_date
from ranked
group by athlete_id;

create unique index if not exists idx_mv_mlb_starter_profiles_unique
  on public.mv_mlb_starter_profiles (athlete_id);

drop materialized view if exists public.mv_mlb_team_bullpen_profile;
create materialized view public.mv_mlb_team_bullpen_profile as
with game_level as (
  select
    match_id,
    game_date,
    team,
    max(team_abbr) as team_abbr,
    sum(innings_outs) as bullpen_outs,
    sum(hits_allowed) as hits_allowed,
    sum(earned_runs) as earned_runs,
    sum(walks) as walks,
    sum(strikeouts) as strikeouts
  from public.mlb_pitcher_game_logs
  where coalesce(is_starter, false) = false
    and team is not null
    and innings_outs is not null
  group by match_id, game_date, team
),
ranked as (
  select
    game_level.*,
    row_number() over (partition by team order by game_date desc, match_id desc) as rn
  from game_level
)
select
  ranked.team as team_name,
  max(ranked.team_abbr) as team_abbr,
  count(*) as games,
  round(case when sum(bullpen_outs) > 0 then (sum(earned_runs) * 27.0) / sum(bullpen_outs) end::numeric, 2) as bullpen_era,
  round(case when sum(bullpen_outs) > 0 then ((sum(walks) + sum(hits_allowed)) * 3.0) / sum(bullpen_outs) end::numeric, 2) as bullpen_whip,
  round(case when sum(bullpen_outs) > 0 then (sum(strikeouts) * 27.0) / sum(bullpen_outs) end::numeric, 2) as bullpen_k_per_9,
  round(case when sum(bullpen_outs) > 0 then (sum(walks) * 27.0) / sum(bullpen_outs) end::numeric, 2) as bullpen_bb_per_9,
  round(public.baseball_outs_to_ip(avg(bullpen_outs))::numeric, 2) as avg_bullpen_innings_per_game,
  round(case when (sum(bullpen_outs) filter (where rn <= 10)) > 0 then ((sum(earned_runs) filter (where rn <= 10)) * 27.0) / (sum(bullpen_outs) filter (where rn <= 10)) end::numeric, 2) as rolling10_bullpen_era,
  round(case when (sum(bullpen_outs) filter (where rn <= 10)) > 0 then (((sum(walks) filter (where rn <= 10)) + (sum(hits_allowed) filter (where rn <= 10))) * 3.0) / (sum(bullpen_outs) filter (where rn <= 10)) end::numeric, 2) as rolling10_bullpen_whip,
  round(case when (sum(bullpen_outs) filter (where rn <= 20)) > 0 then ((sum(earned_runs) filter (where rn <= 20)) * 27.0) / (sum(bullpen_outs) filter (where rn <= 20)) end::numeric, 2) as rolling20_bullpen_era,
  round(case when (sum(bullpen_outs) filter (where rn <= 20)) > 0 then (((sum(walks) filter (where rn <= 20)) + (sum(hits_allowed) filter (where rn <= 20))) * 3.0) / (sum(bullpen_outs) filter (where rn <= 20)) end::numeric, 2) as rolling20_bullpen_whip
from ranked
group by ranked.team;

create unique index if not exists idx_mv_mlb_team_bullpen_profile_unique
  on public.mv_mlb_team_bullpen_profile (team_name);

drop materialized view if exists public.mv_mlb_venue_weather_factors;
create materialized view public.mv_mlb_venue_weather_factors as
with venue_games as (
  select
    venue,
    venue_city,
    venue_state,
    weather_temp,
    (coalesce(home_score, 0) + coalesce(away_score, 0))::numeric as total_runs,
    (coalesce(home_home_runs, 0) + coalesce(away_home_runs, 0))::numeric as total_home_runs,
    case
      when weather_temp is null then 'unknown'
      when weather_temp < 55 then 'cold'
      when weather_temp < 75 then 'mild'
      when weather_temp < 90 then 'warm'
      else 'hot'
    end as temp_bucket
  from public.mlb_postgame
  where venue is not null
),
venue_baseline as (
  select
    venue,
    avg(total_runs) as venue_avg_total_runs
  from venue_games
  group by venue
)
select
  vg.venue,
  max(vg.venue_city) as venue_city,
  max(vg.venue_state) as venue_state,
  count(*) as games,
  round(avg(vg.total_runs)::numeric, 2) as avg_total_runs,
  round(avg(vg.total_home_runs)::numeric, 2) as avg_hr_per_game,
  count(*) filter (where vg.temp_bucket = 'cold') as cold_games,
  round((avg(vg.total_runs) filter (where vg.temp_bucket = 'cold'))::numeric, 2) as cold_avg_total_runs,
  round((avg((vg.total_runs > vb.venue_avg_total_runs)::int) filter (where vg.temp_bucket = 'cold'))::numeric, 3) as cold_over_rate_vs_average,
  count(*) filter (where vg.temp_bucket = 'mild') as mild_games,
  round((avg(vg.total_runs) filter (where vg.temp_bucket = 'mild'))::numeric, 2) as mild_avg_total_runs,
  round((avg((vg.total_runs > vb.venue_avg_total_runs)::int) filter (where vg.temp_bucket = 'mild'))::numeric, 3) as mild_over_rate_vs_average,
  count(*) filter (where vg.temp_bucket = 'warm') as warm_games,
  round((avg(vg.total_runs) filter (where vg.temp_bucket = 'warm'))::numeric, 2) as warm_avg_total_runs,
  round((avg((vg.total_runs > vb.venue_avg_total_runs)::int) filter (where vg.temp_bucket = 'warm'))::numeric, 3) as warm_over_rate_vs_average,
  count(*) filter (where vg.temp_bucket = 'hot') as hot_games,
  round((avg(vg.total_runs) filter (where vg.temp_bucket = 'hot'))::numeric, 2) as hot_avg_total_runs,
  round((avg((vg.total_runs > vb.venue_avg_total_runs)::int) filter (where vg.temp_bucket = 'hot'))::numeric, 3) as hot_over_rate_vs_average,
  count(*) filter (where vg.temp_bucket = 'unknown') as unknown_temp_games
from venue_games vg
join venue_baseline vb
  on vb.venue = vg.venue
group by vg.venue;

create unique index if not exists idx_mv_mlb_venue_weather_factors_unique
  on public.mv_mlb_venue_weather_factors (venue);

do $$
declare
  job_name text;
begin
  foreach job_name in array array[
    'refresh-mlb-team-f5-scoring-daily',
    'refresh-mlb-starter-profiles-daily',
    'refresh-mlb-team-bullpen-profile-daily',
    'refresh-mlb-venue-weather-factors-daily'
  ]
  loop
    perform cron.unschedule(jobid)
    from cron.job
    where jobname = job_name;
  end loop;

  perform cron.schedule(
    'refresh-mlb-team-f5-scoring-daily',
    '0 6 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_team_f5_scoring;'
  );

  perform cron.schedule(
    'refresh-mlb-starter-profiles-daily',
    '5 6 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_starter_profiles;'
  );

  perform cron.schedule(
    'refresh-mlb-team-bullpen-profile-daily',
    '10 6 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_team_bullpen_profile;'
  );

  perform cron.schedule(
    'refresh-mlb-venue-weather-factors-daily',
    '15 6 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_venue_weather_factors;'
  );
exception
  when undefined_table or invalid_schema_name then
    null;
end $$;

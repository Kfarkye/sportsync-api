create or replace function public.baseball_runs_through_inning(inning_values integer[], inning_number integer)
returns integer
language sql
immutable
as $$
  select case
    when inning_values is null or inning_number is null or inning_number <= 0 then 0
    else coalesce((
      select sum(v)
      from unnest(inning_values[1:inning_number]) as v
    ), 0)
  end
$$;

create or replace function public.baseball_runs_through_inning(inning_values jsonb, inning_number integer)
returns integer
language sql
immutable
as $$
  select case
    when inning_values is null
      or jsonb_typeof(inning_values) <> 'array'
      or inning_number is null
      or inning_number <= 0 then 0
    else coalesce((
      select sum(
        case
          when jsonb_typeof(entry.value) in ('number', 'string')
            and regexp_replace(entry.value #>> '{}', '\s+', '', 'g') ~ '^[+-]?\d+$'
            then (regexp_replace(entry.value #>> '{}', '\s+', '', 'g'))::integer
          else 0
        end
      )
      from jsonb_array_elements(inning_values) with ordinality as entry(value, ordinality)
      where entry.ordinality <= inning_number
    ), 0)
  end
$$;

create or replace function public.baseball_safe_numeric(input text)
returns numeric
language sql
immutable
as $$
  select case
    when input is null or btrim(input) = '' then null
    when upper(btrim(input)) in ('EVEN', 'EV') then 100
    when upper(btrim(input)) in ('PK', 'PICK', 'PICKEM', 'PICK''EM') then 0
    when btrim(input) ~ '^[+-]?(\d+(\.\d+)?|\.\d+)$' then btrim(input)::numeric
    else null
  end
$$;

drop materialized view if exists public.mv_mlb_bullpen_availability;
create materialized view public.mv_mlb_bullpen_availability as
with scheduled_games as (
  select
    m.id as match_id,
    m.start_time,
    m.home_team as team_name,
    m.away_team as opponent_team,
    'home'::text as home_away
  from public.matches m
  where m.league_id = 'mlb'
    and coalesce(m.status, 'STATUS_SCHEDULED') = 'STATUS_SCHEDULED'
    and m.start_time >= now()

  union all

  select
    m.id as match_id,
    m.start_time,
    m.away_team as team_name,
    m.home_team as opponent_team,
    'away'::text as home_away
  from public.matches m
  where m.league_id = 'mlb'
    and coalesce(m.status, 'STATUS_SCHEDULED') = 'STATUS_SCHEDULED'
    and m.start_time >= now()
),
next_games as (
  select
    scheduled_games.*,
    row_number() over (
      partition by scheduled_games.team_name
      order by scheduled_games.start_time asc, scheduled_games.match_id asc
    ) as rn
  from scheduled_games
),
focus_games as (
  select
    ng.match_id,
    ng.start_time,
    ng.team_name,
    ng.opponent_team,
    ng.home_away
  from next_games ng
  where ng.rn = 1
),
reliever_usage as (
  select
    fg.match_id as next_match_id,
    fg.start_time as next_game_time,
    fg.team_name,
    fg.opponent_team,
    fg.home_away,
    mpgl.athlete_id,
    max(mpgl.athlete_name) as athlete_name,
    max(mpgl.team_abbr) as team_abbr,
    count(*) filter (
      where mpgl.game_date >= fg.start_time::date - 2
        and mpgl.game_date < fg.start_time::date
    ) as appearances_last3d,
    count(*) filter (
      where mpgl.game_date >= fg.start_time::date - 4
        and mpgl.game_date < fg.start_time::date
    ) as appearances_last5d,
    count(*) filter (
      where mpgl.game_date >= fg.start_time::date - 6
        and mpgl.game_date < fg.start_time::date
    ) as appearances_last7d,
    count(*) filter (
      where mpgl.game_date >= fg.start_time::date - 13
        and mpgl.game_date < fg.start_time::date
    ) as appearances_last14d,
    coalesce(sum(mpgl.innings_outs) filter (
      where mpgl.game_date >= fg.start_time::date - 2
        and mpgl.game_date < fg.start_time::date
    ), 0) as outs_last3d,
    coalesce(sum(mpgl.innings_outs) filter (
      where mpgl.game_date >= fg.start_time::date - 4
        and mpgl.game_date < fg.start_time::date
    ), 0) as outs_last5d,
    coalesce(sum(mpgl.innings_outs) filter (
      where mpgl.game_date >= fg.start_time::date - 6
        and mpgl.game_date < fg.start_time::date
    ), 0) as outs_last7d,
    coalesce(sum(mpgl.pitches_thrown) filter (
      where mpgl.game_date >= fg.start_time::date - 2
        and mpgl.game_date < fg.start_time::date
    ), 0) as pitches_last3d,
    coalesce(sum(mpgl.pitches_thrown) filter (
      where mpgl.game_date >= fg.start_time::date - 4
        and mpgl.game_date < fg.start_time::date
    ), 0) as pitches_last5d,
    coalesce(sum(mpgl.pitches_thrown) filter (
      where mpgl.game_date >= fg.start_time::date - 6
        and mpgl.game_date < fg.start_time::date
    ), 0) as pitches_last7d,
    count(*) filter (
      where mpgl.game_date = fg.start_time::date - 1
    ) as pitched_yesterday,
    count(*) filter (
      where mpgl.game_date = fg.start_time::date - 2
    ) as pitched_two_days_ago,
    max(mpgl.game_date) as last_game_date,
    round(
      case
        when sum(mpgl.innings_outs) > 0 then (sum(mpgl.earned_runs) * 27.0) / sum(mpgl.innings_outs)
        else null
      end::numeric,
      2
    ) as recent_relief_era
  from focus_games fg
  join public.mlb_pitcher_game_logs mpgl
    on lower(mpgl.team) = lower(fg.team_name)
   and coalesce(mpgl.is_starter, false) = false
   and mpgl.innings_outs is not null
   and mpgl.game_date < fg.start_time::date
   and mpgl.game_date >= fg.start_time::date - 13
  group by
    fg.match_id,
    fg.start_time,
    fg.team_name,
    fg.opponent_team,
    fg.home_away,
    mpgl.athlete_id
),
ranked_relievers as (
  select
    ru.*,
    row_number() over (
      partition by ru.team_name
      order by ru.appearances_last14d desc, ru.pitches_last7d desc, ru.last_game_date desc, ru.athlete_name asc
    ) as leverage_rank
  from reliever_usage ru
),
injury_detail as (
  select
    fg.match_id as next_match_id,
    fg.team_name,
    coalesce(
      injury->'athlete'->>'fullName',
      injury->'athlete'->>'displayName',
      injury->'athlete'->>'shortName'
    ) as athlete_name,
    upper(coalesce(injury->'athlete'->'position'->>'abbreviation', '')) as position_abbr,
    coalesce(
      injury->>'status',
      injury->'type'->>'description',
      injury->'type'->>'name'
    ) as injury_status
  from focus_games fg
  join public.espn_enrichment ee
    on ee.id = fg.match_id
  cross join lateral jsonb_array_elements(coalesce(ee.summary_raw->'injuries', '[]'::jsonb)) as team_block
  cross join lateral jsonb_array_elements(coalesce(team_block->'injuries', '[]'::jsonb)) as injury
  where lower(coalesce(team_block->'team'->>'displayName', '')) = lower(fg.team_name)
),
injury_summary as (
  select
    id.next_match_id,
    id.team_name,
    count(*) filter (where id.position_abbr in ('RP', 'CP')) as injured_relief_pitchers,
    coalesce(
      jsonb_agg(id.athlete_name order by id.athlete_name) filter (where id.position_abbr in ('RP', 'CP')),
      '[]'::jsonb
    ) as injured_relief_pitcher_names
  from injury_detail id
  group by id.next_match_id, id.team_name
),
high_leverage_injury_summary as (
  select
    rr.next_match_id,
    rr.team_name,
    count(distinct rr.athlete_id) as injured_high_leverage_relief_pitchers,
    coalesce(
      jsonb_agg(rr.athlete_name order by rr.leverage_rank),
      '[]'::jsonb
    ) as injured_high_leverage_reliever_names
  from ranked_relievers rr
  join injury_detail id
    on id.next_match_id = rr.next_match_id
   and lower(id.team_name) = lower(rr.team_name)
   and lower(id.athlete_name) = lower(rr.athlete_name)
   and id.position_abbr in ('RP', 'CP')
  where rr.leverage_rank <= 3
  group by rr.next_match_id, rr.team_name
)
select
  fg.match_id as next_match_id,
  fg.start_time as next_game_time,
  fg.team_name,
  max(rr.team_abbr) as team_abbr,
  fg.home_away,
  fg.opponent_team,
  count(distinct rr.athlete_id) as tracked_relievers,
  count(distinct rr.athlete_id) filter (where rr.appearances_last3d > 0) as relievers_used_last3d,
  count(distinct rr.athlete_id) filter (where rr.appearances_last5d > 0) as relievers_used_last5d,
  count(distinct rr.athlete_id) filter (where rr.appearances_last7d > 0) as relievers_used_last7d,
  count(distinct rr.athlete_id) filter (where rr.pitched_yesterday > 0) as relievers_used_yesterday,
  count(distinct rr.athlete_id) filter (
    where rr.pitched_yesterday > 0
      and rr.pitched_two_days_ago > 0
  ) as relievers_back_to_back,
  count(distinct rr.athlete_id) filter (where rr.leverage_rank <= 3) as high_leverage_relievers,
  count(distinct rr.athlete_id) filter (
    where rr.leverage_rank <= 3
      and rr.pitched_yesterday > 0
  ) as high_leverage_used_yesterday,
  count(distinct rr.athlete_id) filter (
    where rr.leverage_rank <= 3
      and rr.pitched_yesterday > 0
      and rr.pitched_two_days_ago > 0
  ) as high_leverage_back_to_back,
  round(public.baseball_outs_to_ip(coalesce(sum(rr.outs_last3d), 0))::numeric, 2) as bullpen_ip_last3d,
  round(public.baseball_outs_to_ip(coalesce(sum(rr.outs_last5d), 0))::numeric, 2) as bullpen_ip_last5d,
  round(public.baseball_outs_to_ip(coalesce(sum(rr.outs_last7d), 0))::numeric, 2) as bullpen_ip_last7d,
  coalesce(sum(rr.pitches_last3d), 0) as bullpen_pitches_last3d,
  coalesce(sum(rr.pitches_last5d), 0) as bullpen_pitches_last5d,
  coalesce(sum(rr.pitches_last7d), 0) as bullpen_pitches_last7d,
  round(avg(rr.recent_relief_era)::numeric, 2) as avg_recent_relief_era,
  coalesce(isum.injured_relief_pitchers, 0) as injured_relief_pitchers,
  coalesce(hli.injured_high_leverage_relief_pitchers, 0) as injured_high_leverage_relief_pitchers,
  coalesce(isum.injured_relief_pitcher_names, '[]'::jsonb) as injured_relief_pitcher_names,
  coalesce(hli.injured_high_leverage_reliever_names, '[]'::jsonb) as injured_high_leverage_reliever_names,
  coalesce(
    jsonb_agg(rr.athlete_name order by rr.leverage_rank) filter (where rr.leverage_rank <= 3),
    '[]'::jsonb
  ) as high_leverage_reliever_names,
  coalesce(
    jsonb_agg(rr.athlete_name order by rr.athlete_name) filter (
      where rr.pitched_yesterday > 0
        and rr.pitched_two_days_ago > 0
    ),
    '[]'::jsonb
  ) as back_to_back_reliever_names,
  round((
    public.baseball_outs_to_ip(coalesce(sum(rr.outs_last3d), 0)) * 1.75
    + public.baseball_outs_to_ip(coalesce(sum(rr.outs_last5d), 0)) * 0.65
    + (coalesce(sum(rr.pitches_last3d), 0) / 40.0)
    + (count(distinct rr.athlete_id) filter (
      where rr.pitched_yesterday > 0
        and rr.pitched_two_days_ago > 0
    ) * 2.5)
    + (count(distinct rr.athlete_id) filter (
      where rr.leverage_rank <= 3
        and rr.pitched_yesterday > 0
        and rr.pitched_two_days_ago > 0
    ) * 4.0)
    + (coalesce(isum.injured_relief_pitchers, 0) * 1.5)
    + (coalesce(hli.injured_high_leverage_relief_pitchers, 0) * 2.5)
  )::numeric, 2) as bullpen_stress_score
from focus_games fg
left join ranked_relievers rr
  on rr.next_match_id = fg.match_id
 and lower(rr.team_name) = lower(fg.team_name)
left join injury_summary isum
  on isum.next_match_id = fg.match_id
 and lower(isum.team_name) = lower(fg.team_name)
left join high_leverage_injury_summary hli
  on hli.next_match_id = fg.match_id
 and lower(hli.team_name) = lower(fg.team_name)
group by
  fg.match_id,
  fg.start_time,
  fg.team_name,
  fg.home_away,
  fg.opponent_team,
  isum.injured_relief_pitchers,
  hli.injured_high_leverage_relief_pitchers,
  isum.injured_relief_pitcher_names,
  hli.injured_high_leverage_reliever_names;

create unique index if not exists idx_mv_mlb_bullpen_availability_unique
  on public.mv_mlb_bullpen_availability (team_name);

drop materialized view if exists public.mv_mlb_lead_protection;
create materialized view public.mv_mlb_lead_protection as
with team_abbr_lookup as (
  select
    team as team_name,
    max(team_abbr) as team_abbr
  from public.mlb_pitcher_game_logs
  where team is not null
    and team_abbr is not null
  group by team
),
team_rows as (
  select
    mis.match_id,
    mis.game_date,
    mis.season_type,
    mis.total_innings,
    mis.home_team as team_name,
    coalesce(mis.home_score, 0) as final_runs_for,
    coalesce(mis.away_score, 0) as final_runs_against,
    public.baseball_runs_through_inning(mis.home_innings, 5) as runs_after5,
    public.baseball_runs_through_inning(mis.away_innings, 5) as opp_runs_after5,
    public.baseball_runs_through_inning(mis.home_innings, 7) as runs_after7,
    public.baseball_runs_through_inning(mis.away_innings, 7) as opp_runs_after7
  from public.mlb_inning_scores mis

  union all

  select
    mis.match_id,
    mis.game_date,
    mis.season_type,
    mis.total_innings,
    mis.away_team as team_name,
    coalesce(mis.away_score, 0) as final_runs_for,
    coalesce(mis.home_score, 0) as final_runs_against,
    public.baseball_runs_through_inning(mis.away_innings, 5) as runs_after5,
    public.baseball_runs_through_inning(mis.home_innings, 5) as opp_runs_after5,
    public.baseball_runs_through_inning(mis.away_innings, 7) as runs_after7,
    public.baseball_runs_through_inning(mis.home_innings, 7) as opp_runs_after7
  from public.mlb_inning_scores mis
),
scored as (
  select
    tr.*,
    (tr.final_runs_for - tr.final_runs_against) as final_margin,
    (tr.runs_after5 - tr.opp_runs_after5) as margin_after5,
    case
      when tr.total_innings >= 7 then (tr.runs_after7 - tr.opp_runs_after7)
      else null
    end as margin_after7
  from team_rows tr
)
select
  s.team_name,
  tal.team_abbr,
  count(*) as games,
  count(*) filter (where s.margin_after5 > 0) as leads_after5,
  count(*) filter (where s.margin_after5 > 0 and s.final_margin > 0) as wins_when_leading_after5,
  round((avg((s.final_margin > 0)::int) filter (where s.margin_after5 > 0))::numeric, 3) as lead_conversion_after5,
  count(*) filter (where s.margin_after5 > 0 and s.final_margin <= 0) as blown_leads_after5,
  count(*) filter (where s.margin_after5 = 0) as tied_after5,
  round((avg((s.final_margin > 0)::int) filter (where s.margin_after5 = 0))::numeric, 3) as win_rate_when_tied_after5,
  count(*) filter (where s.margin_after5 < 0) as trailing_after5,
  count(*) filter (where s.margin_after5 < 0 and s.final_margin > 0) as comeback_wins_after5,
  round(avg((s.final_margin - s.margin_after5)::numeric), 2) as avg_margin_change_after5,
  count(*) filter (where s.total_innings >= 7 and s.margin_after7 > 0) as leads_after7,
  count(*) filter (where s.total_innings >= 7 and s.margin_after7 > 0 and s.final_margin > 0) as wins_when_leading_after7,
  round((avg((s.final_margin > 0)::int) filter (where s.total_innings >= 7 and s.margin_after7 > 0))::numeric, 3) as lead_conversion_after7,
  count(*) filter (where s.total_innings >= 7 and s.margin_after7 > 0 and s.final_margin <= 0) as blown_leads_after7,
  round((avg((s.final_margin - s.margin_after7)::numeric) filter (where s.total_innings >= 7))::numeric, 2) as avg_margin_change_after7,
  count(*) filter (where s.margin_after5 = 1) as one_run_leads_after5,
  round((avg((s.final_margin > 0)::int) filter (where s.margin_after5 = 1))::numeric, 3) as one_run_lead_hold_rate_after5
from scored s
left join team_abbr_lookup tal
  on lower(tal.team_name) = lower(s.team_name)
group by s.team_name, tal.team_abbr;

create unique index if not exists idx_mv_mlb_lead_protection_unique
  on public.mv_mlb_lead_protection (team_name);

drop materialized view if exists public.mv_mlb_matchup_betting_kit;
create materialized view public.mv_mlb_matchup_betting_kit as
with upcoming_games as (
  select
    m.id as match_id,
    m.start_time,
    m.home_team,
    m.away_team,
    ee.summary_raw
  from public.matches m
  left join public.espn_enrichment ee
    on ee.id = m.id
  where m.league_id = 'mlb'
    and coalesce(m.status, 'STATUS_SCHEDULED') = 'STATUS_SCHEDULED'
    and m.start_time >= now()
),
probables as (
  select
    ug.match_id,
    competitor->>'homeAway' as home_away,
    coalesce(
      probable->'athlete'->>'id',
      probable->>'playerId'
    ) as athlete_id,
    coalesce(
      probable->'athlete'->>'displayName',
      probable->'athlete'->>'fullName',
      probable->>'displayName'
    ) as athlete_name
  from upcoming_games ug
  cross join lateral jsonb_array_elements(coalesce(ug.summary_raw #> '{header,competitions,0,competitors}', '[]'::jsonb)) as competitor
  left join lateral (
    select probable
    from jsonb_array_elements(coalesce(competitor->'probables', '[]'::jsonb)) as probable
    limit 1
  ) prob on true
),
probable_summary as (
  select
    p.match_id,
    max(p.athlete_id) filter (where p.home_away = 'home') as home_probable_starter_id,
    max(p.athlete_name) filter (where p.home_away = 'home') as home_probable_starter_name,
    max(p.athlete_id) filter (where p.home_away = 'away') as away_probable_starter_id,
    max(p.athlete_name) filter (where p.home_away = 'away') as away_probable_starter_name
  from probables p
  group by p.match_id
),
last_five as (
  select
    ug.match_id,
    coalesce(team_block->'team'->>'displayName', '') as team_name,
    count(*) filter (where event->>'gameResult' = 'W') as wins,
    count(*) filter (where event->>'gameResult' = 'L') as losses,
    count(*) filter (where event->>'gameResult' = 'T') as ties
  from upcoming_games ug
  cross join lateral jsonb_array_elements(coalesce(ug.summary_raw->'lastFiveGames', '[]'::jsonb)) as team_block
  cross join lateral jsonb_array_elements(coalesce(team_block->'events', '[]'::jsonb)) as event
  group by ug.match_id, coalesce(team_block->'team'->>'displayName', '')
),
ats_summary as (
  select
    ug.match_id,
    coalesce(team_block->'team'->>'displayName', '') as team_name,
    coalesce(
      jsonb_agg(record order by coalesce(record->>'name', record->>'summary', record->>'description'))
        filter (where record is not null),
      '[]'::jsonb
    ) as ats_records,
    max(nullif(coalesce(record->>'summary', record->>'displayValue', record->>'description', record->>'name'), '')) as ats_summary
  from upcoming_games ug
  cross join lateral jsonb_array_elements(coalesce(ug.summary_raw->'againstTheSpread', '[]'::jsonb)) as team_block
  left join lateral jsonb_array_elements(coalesce(team_block->'records', '[]'::jsonb)) as record on true
  group by ug.match_id, coalesce(team_block->'team'->>'displayName', '')
),
injury_summary as (
  select
    ug.match_id,
    coalesce(team_block->'team'->>'displayName', '') as team_name,
    count(*) as total_injuries,
    count(*) filter (
      where upper(coalesce(injury->'athlete'->'position'->>'abbreviation', '')) in ('RP', 'CP')
    ) as bullpen_injuries,
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'athlete_name', coalesce(injury->'athlete'->>'fullName', injury->'athlete'->>'displayName'),
          'position', upper(coalesce(injury->'athlete'->'position'->>'abbreviation', '')),
          'status', coalesce(injury->>'status', injury->'type'->>'description', injury->'type'->>'name')
        )
        order by coalesce(injury->'athlete'->>'fullName', injury->'athlete'->>'displayName')
      ),
      '[]'::jsonb
    ) as injury_details
  from upcoming_games ug
  cross join lateral jsonb_array_elements(coalesce(ug.summary_raw->'injuries', '[]'::jsonb)) as team_block
  cross join lateral jsonb_array_elements(coalesce(team_block->'injuries', '[]'::jsonb)) as injury
  group by ug.match_id, coalesce(team_block->'team'->>'displayName', '')
),
series_summary as (
  select
    ug.match_id,
    max(coalesce(series_item->>'summary', series_item->>'description', series_item->>'title')) as series_summary
  from upcoming_games ug
  left join lateral jsonb_array_elements(coalesce(ug.summary_raw->'seasonseries', '[]'::jsonb)) as series_item on true
  group by ug.match_id
)
select
  ug.match_id,
  ug.start_time,
  ug.home_team,
  ug.away_team,
  ps.home_probable_starter_id,
  ps.home_probable_starter_name,
  ps.away_probable_starter_id,
  ps.away_probable_starter_name,
  hsp.team_abbr as home_team_abbr,
  asp.team_abbr as away_team_abbr,
  hsp.last5_era as home_starter_last5_era,
  asp.last5_era as away_starter_last5_era,
  hsp.last5_whip as home_starter_last5_whip,
  asp.last5_whip as away_starter_last5_whip,
  hsp.last5_avg_ip as home_starter_last5_avg_ip,
  asp.last5_avg_ip as away_starter_last5_avg_ip,
  hsp.last5_avg_pitches as home_starter_last5_avg_pitches,
  asp.last5_avg_pitches as away_starter_last5_avg_pitches,
  hsp.k_rate as home_starter_k_per_9,
  asp.k_rate as away_starter_k_per_9,
  hsp.bb_rate as home_starter_bb_per_9,
  asp.bb_rate as away_starter_bb_per_9,
  hbp.bullpen_era as home_bullpen_era,
  abp.bullpen_era as away_bullpen_era,
  hbp.rolling10_bullpen_era as home_bullpen_rolling10_era,
  abp.rolling10_bullpen_era as away_bullpen_rolling10_era,
  hbp.bullpen_whip as home_bullpen_whip,
  abp.bullpen_whip as away_bullpen_whip,
  hba.bullpen_stress_score as home_bullpen_stress_score,
  aba.bullpen_stress_score as away_bullpen_stress_score,
  hba.bullpen_ip_last3d as home_bullpen_ip_last3d,
  aba.bullpen_ip_last3d as away_bullpen_ip_last3d,
  hba.bullpen_pitches_last3d as home_bullpen_pitches_last3d,
  aba.bullpen_pitches_last3d as away_bullpen_pitches_last3d,
  hba.high_leverage_back_to_back as home_high_leverage_back_to_back,
  aba.high_leverage_back_to_back as away_high_leverage_back_to_back,
  hba.injured_relief_pitchers as home_injured_relief_pitchers,
  aba.injured_relief_pitchers as away_injured_relief_pitchers,
  hba.injured_high_leverage_relief_pitchers as home_injured_high_leverage_relief_pitchers,
  aba.injured_high_leverage_relief_pitchers as away_injured_high_leverage_relief_pitchers,
  hba.high_leverage_reliever_names as home_high_leverage_relievers,
  aba.high_leverage_reliever_names as away_high_leverage_relievers,
  hba.back_to_back_reliever_names as home_back_to_back_relievers,
  aba.back_to_back_reliever_names as away_back_to_back_relievers,
  hlp.lead_conversion_after5 as home_lead_conversion_after5,
  alp.lead_conversion_after5 as away_lead_conversion_after5,
  hlp.one_run_lead_hold_rate_after5 as home_one_run_hold_rate_after5,
  alp.one_run_lead_hold_rate_after5 as away_one_run_hold_rate_after5,
  hf5.avg_f5_runs as home_avg_f5_runs_home,
  af5.avg_f5_runs as away_avg_f5_runs_away,
  hf5.pct_scoring_first_inning as home_pct_scoring_first_inning_home,
  af5.pct_scoring_first_inning as away_pct_scoring_first_inning_away,
  case
    when h5.wins is null then null
    when coalesce(h5.ties, 0) > 0 then h5.wins::text || '-' || h5.losses::text || '-' || h5.ties::text
    else h5.wins::text || '-' || h5.losses::text
  end as home_last5_record,
  case
    when a5.wins is null then null
    when coalesce(a5.ties, 0) > 0 then a5.wins::text || '-' || a5.losses::text || '-' || a5.ties::text
    else a5.wins::text || '-' || a5.losses::text
  end as away_last5_record,
  round((h5.wins::numeric / nullif(h5.wins + h5.losses + h5.ties, 0)), 3) as home_last5_win_pct,
  round((a5.wins::numeric / nullif(a5.wins + a5.losses + a5.ties, 0)), 3) as away_last5_win_pct,
  hats.ats_summary as home_ats_summary,
  aats.ats_summary as away_ats_summary,
  coalesce(hats.ats_records, '[]'::jsonb) as home_ats_records,
  coalesce(aats.ats_records, '[]'::jsonb) as away_ats_records,
  coalesce(hinj.total_injuries, 0) as home_injury_count,
  coalesce(ainj.total_injuries, 0) as away_injury_count,
  coalesce(hinj.bullpen_injuries, 0) as home_bullpen_injury_count,
  coalesce(ainj.bullpen_injuries, 0) as away_bullpen_injury_count,
  coalesce(hinj.injury_details, '[]'::jsonb) as home_injury_details,
  coalesce(ainj.injury_details, '[]'::jsonb) as away_injury_details,
  ss.series_summary,
  ug.summary_raw #>> '{gameInfo,venue,fullName}' as venue,
  coalesce(
    ug.summary_raw #>> '{gameInfo,venue,address,city}',
    ug.summary_raw #>> '{gameInfo,venue,city}'
  ) as venue_city,
  coalesce(
    ug.summary_raw #>> '{gameInfo,venue,address,state}',
    ug.summary_raw #>> '{gameInfo,venue,state}'
  ) as venue_state,
  public.baseball_safe_numeric(ug.summary_raw #>> '{gameInfo,weather,temperature}') as weather_temp,
  public.baseball_safe_numeric(ug.summary_raw #>> '{gameInfo,weather,gust}') as weather_gust,
  public.baseball_safe_numeric(ug.summary_raw #>> '{gameInfo,weather,precipitation}') as weather_precipitation,
  public.baseball_safe_numeric(ug.summary_raw #>> '{gameInfo,weather,conditionId}') as weather_condition_id,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,homeTeamOdds,moneyLine}') as dk_home_moneyline,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,awayTeamOdds,moneyLine}') as dk_away_moneyline,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,spread}') as dk_spread,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,overUnder}') as dk_total,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,overOdds}') as dk_over_price,
  public.baseball_safe_numeric(ug.summary_raw #>> '{pickcenter,0,underOdds}') as dk_under_price,
  round(coalesce(aba.bullpen_stress_score, 0) - coalesce(hba.bullpen_stress_score, 0), 2) as bullpen_stress_delta_favors_home,
  round(
    coalesce(abp.rolling10_bullpen_era, abp.bullpen_era) - coalesce(hbp.rolling10_bullpen_era, hbp.bullpen_era),
    2
  ) as bullpen_quality_delta_favors_home,
  round(coalesce(hlp.lead_conversion_after5, 0) - coalesce(alp.lead_conversion_after5, 0), 3) as lead_protection_delta_favors_home,
  round(coalesce(hf5.avg_f5_runs, 0) - coalesce(af5.avg_f5_runs, 0), 2) as f5_scoring_delta_favors_home,
  round(coalesce(asp.last5_era, asp.era) - coalesce(hsp.last5_era, hsp.era), 2) as starter_era_delta_favors_home
from upcoming_games ug
left join probable_summary ps
  on ps.match_id = ug.match_id
left join lateral (
  select sp.*
  from public.mv_mlb_starter_profiles sp
  where (
      ps.home_probable_starter_id is not null
      and sp.athlete_id = ps.home_probable_starter_id
    )
    or (
      ps.home_probable_starter_id is null
      and ps.home_probable_starter_name is not null
      and lower(sp.athlete_name) = lower(ps.home_probable_starter_name)
      and lower(sp.team) = lower(ug.home_team)
    )
  order by
    case when sp.athlete_id = ps.home_probable_starter_id then 0 else 1 end,
    sp.last_game_date desc nulls last,
    sp.starts desc,
    sp.athlete_name asc
  limit 1
) hsp on true
left join lateral (
  select sp.*
  from public.mv_mlb_starter_profiles sp
  where (
      ps.away_probable_starter_id is not null
      and sp.athlete_id = ps.away_probable_starter_id
    )
    or (
      ps.away_probable_starter_id is null
      and ps.away_probable_starter_name is not null
      and lower(sp.athlete_name) = lower(ps.away_probable_starter_name)
      and lower(sp.team) = lower(ug.away_team)
    )
  order by
    case when sp.athlete_id = ps.away_probable_starter_id then 0 else 1 end,
    sp.last_game_date desc nulls last,
    sp.starts desc,
    sp.athlete_name asc
  limit 1
) asp on true
left join public.mv_mlb_team_bullpen_profile hbp
  on lower(hbp.team_name) = lower(ug.home_team)
left join public.mv_mlb_team_bullpen_profile abp
  on lower(abp.team_name) = lower(ug.away_team)
left join public.mv_mlb_bullpen_availability hba
  on hba.next_match_id = ug.match_id
 and lower(hba.team_name) = lower(ug.home_team)
left join public.mv_mlb_bullpen_availability aba
  on aba.next_match_id = ug.match_id
 and lower(aba.team_name) = lower(ug.away_team)
left join public.mv_mlb_lead_protection hlp
  on lower(hlp.team_name) = lower(ug.home_team)
left join public.mv_mlb_lead_protection alp
  on lower(alp.team_name) = lower(ug.away_team)
left join public.mv_mlb_team_f5_scoring hf5
  on lower(hf5.team_name) = lower(ug.home_team)
 and hf5.location = 'home'
left join public.mv_mlb_team_f5_scoring af5
  on lower(af5.team_name) = lower(ug.away_team)
 and af5.location = 'away'
left join last_five h5
  on h5.match_id = ug.match_id
 and lower(h5.team_name) = lower(ug.home_team)
left join last_five a5
  on a5.match_id = ug.match_id
 and lower(a5.team_name) = lower(ug.away_team)
left join ats_summary hats
  on hats.match_id = ug.match_id
 and lower(hats.team_name) = lower(ug.home_team)
left join ats_summary aats
  on aats.match_id = ug.match_id
 and lower(aats.team_name) = lower(ug.away_team)
left join injury_summary hinj
  on hinj.match_id = ug.match_id
 and lower(hinj.team_name) = lower(ug.home_team)
left join injury_summary ainj
  on ainj.match_id = ug.match_id
 and lower(ainj.team_name) = lower(ug.away_team)
left join series_summary ss
  on ss.match_id = ug.match_id;

create unique index if not exists idx_mv_mlb_matchup_betting_kit_unique
  on public.mv_mlb_matchup_betting_kit (match_id);

do $$
declare
  job_name text;
begin
  foreach job_name in array array[
    'refresh-mlb-bullpen-availability-2h',
    'refresh-mlb-lead-protection-2h',
    'refresh-mlb-matchup-betting-kit-2h'
  ]
  loop
    perform cron.unschedule(jobid)
    from cron.job
    where jobname = job_name;
  end loop;

  perform cron.schedule(
    'refresh-mlb-bullpen-availability-2h',
    '20 */2 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_bullpen_availability;'
  );

  perform cron.schedule(
    'refresh-mlb-lead-protection-2h',
    '25 */2 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_lead_protection;'
  );

  perform cron.schedule(
    'refresh-mlb-matchup-betting-kit-2h',
    '30 */2 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_matchup_betting_kit;'
  );
exception
  when undefined_table or invalid_schema_name then
    null;
end $$;

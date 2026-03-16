drop materialized view if exists public.mv_mlb_daily_betting_board;
create materialized view public.mv_mlb_daily_betting_board as
with base as (
  select
    mk.*,
    mk.start_time::date as board_date,
    coalesce(mk.home_last5_win_pct, 0.500) as home_form_win_pct,
    coalesce(mk.away_last5_win_pct, 0.500) as away_form_win_pct,
    coalesce(mk.home_one_run_hold_rate_after5, mk.home_lead_conversion_after5, 0.500) as home_hold_rate_after5,
    coalesce(mk.away_one_run_hold_rate_after5, mk.away_lead_conversion_after5, 0.500) as away_hold_rate_after5,
    coalesce(mk.home_bullpen_rolling10_era, mk.home_bullpen_era, 4.25) as home_recent_bullpen_era,
    coalesce(mk.away_bullpen_rolling10_era, mk.away_bullpen_era, 4.25) as away_recent_bullpen_era,
    coalesce(mk.home_bullpen_stress_score, 0) + coalesce(mk.away_bullpen_stress_score, 0) as combined_bullpen_stress,
    coalesce(mk.home_bullpen_injury_count, 0) + coalesce(mk.away_bullpen_injury_count, 0) as combined_bullpen_injuries,
    coalesce(mk.home_high_leverage_back_to_back, 0) + coalesce(mk.away_high_leverage_back_to_back, 0) as combined_high_leverage_back_to_back,
    coalesce(mk.home_avg_f5_runs_home, 0) + coalesce(mk.away_avg_f5_runs_away, 0) as combined_f5_scoring
  from public.mv_mlb_matchup_betting_kit mk
  where mk.start_time >= date_trunc('day', now())
    and mk.start_time < date_trunc('day', now()) + interval '3 days'
),
side_components as (
  select
    base.*,
    greatest(-18.0, least(18.0, coalesce(base.bullpen_stress_delta_favors_home, 0) * 0.75)) as home_stress_component,
    greatest(-18.0, least(18.0, coalesce(base.bullpen_quality_delta_favors_home, 0) * 4.50)) as home_bullpen_quality_component,
    greatest(-12.0, least(12.0, coalesce(base.lead_protection_delta_favors_home, 0) * 24.0)) as home_lead_component,
    greatest(
      -10.0,
      least(
        10.0,
        (coalesce(base.home_hold_rate_after5, 0.500) - coalesce(base.away_hold_rate_after5, 0.500)) * 20.0
      )
    ) as home_hold_component,
    greatest(
      -8.0,
      least(
        8.0,
        (coalesce(base.away_bullpen_injury_count, 0) - coalesce(base.home_bullpen_injury_count, 0)) * 2.0
      )
    ) as home_injury_component,
    greatest(-8.0, least(8.0, coalesce(base.starter_era_delta_favors_home, 0) * 2.50)) as home_starter_component,
    greatest(
      -6.0,
      least(
        6.0,
        (coalesce(base.home_form_win_pct, 0.500) - coalesce(base.away_form_win_pct, 0.500)) * 20.0
      )
    ) as home_form_component
  from base
),
side_scored as (
  select
    sc.*,
    round((
      coalesce(sc.home_stress_component, 0)
      + coalesce(sc.home_bullpen_quality_component, 0)
      + coalesce(sc.home_lead_component, 0)
      + coalesce(sc.home_hold_component, 0)
      + coalesce(sc.home_injury_component, 0)
      + coalesce(sc.home_starter_component, 0)
      + coalesce(sc.home_form_component, 0)
    )::numeric, 2) as home_side_raw
  from side_components sc
),
side_candidates as (
  select
    ss.board_date,
    ss.match_id,
    ss.start_time,
    ss.home_team,
    ss.away_team,
    'BULLPEN_SIDE'::text as play_type,
    'bullpen_fade_side'::text as edge_family,
    'Pregame / full game'::text as entry_window,
    case when ss.home_side_raw >= 0 then 'home' else 'away' end as target_side,
    case when ss.home_side_raw >= 0 then ss.home_team else ss.away_team end as bet_target,
    case when ss.home_side_raw >= 0 then ss.away_team else ss.home_team end as fade_team,
    case when ss.home_side_raw >= 0 then ss.dk_home_moneyline else ss.dk_away_moneyline end as dk_moneyline,
    ss.dk_spread,
    ss.dk_total,
    ss.home_probable_starter_name,
    ss.away_probable_starter_name,
    round(least(99.0, greatest(0.0, 50.0 + abs(ss.home_side_raw)))::numeric, 2) as signal_strength,
    case
      when abs(ss.home_side_raw) >= 24 then 'A'
      when abs(ss.home_side_raw) >= 16 then 'B'
      else 'C'
    end as signal_bucket,
    round(
      (case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_stress_delta_favors_home, 0) else -coalesce(ss.bullpen_stress_delta_favors_home, 0) end)::numeric,
      2
    ) as bullpen_stress_edge,
    round(
      (case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_quality_delta_favors_home, 0) else -coalesce(ss.bullpen_quality_delta_favors_home, 0) end)::numeric,
      2
    ) as bullpen_quality_edge,
    round(
      (case when ss.home_side_raw >= 0 then coalesce(ss.lead_protection_delta_favors_home, 0) else -coalesce(ss.lead_protection_delta_favors_home, 0) end)::numeric,
      3
    ) as lead_protection_edge,
    round(
      (
        case
          when ss.home_side_raw >= 0
            then coalesce(ss.home_hold_rate_after5, 0.500) - coalesce(ss.away_hold_rate_after5, 0.500)
          else coalesce(ss.away_hold_rate_after5, 0.500) - coalesce(ss.home_hold_rate_after5, 0.500)
        end
      )::numeric,
      3
    ) as hold_rate_edge,
    case
      when ss.home_side_raw >= 0
        then coalesce(ss.away_bullpen_injury_count, 0) - coalesce(ss.home_bullpen_injury_count, 0)
      else coalesce(ss.home_bullpen_injury_count, 0) - coalesce(ss.away_bullpen_injury_count, 0)
    end as bullpen_injury_edge,
    round(
      (case when ss.home_side_raw >= 0 then coalesce(ss.starter_era_delta_favors_home, 0) else -coalesce(ss.starter_era_delta_favors_home, 0) end)::numeric,
      2
    ) as starter_era_edge,
    round(
      (
        case
          when ss.home_side_raw >= 0
            then coalesce(ss.home_form_win_pct, 0.500) - coalesce(ss.away_form_win_pct, 0.500)
          else coalesce(ss.away_form_win_pct, 0.500) - coalesce(ss.home_form_win_pct, 0.500)
        end
      )::numeric,
      3
    ) as recent_form_edge,
    jsonb_build_object(
      'bullpen_stress_edge', round(
        (case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_stress_delta_favors_home, 0) else -coalesce(ss.bullpen_stress_delta_favors_home, 0) end)::numeric,
        2
      ),
      'bullpen_quality_edge', round(
        (case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_quality_delta_favors_home, 0) else -coalesce(ss.bullpen_quality_delta_favors_home, 0) end)::numeric,
        2
      ),
      'lead_protection_edge', round(
        (case when ss.home_side_raw >= 0 then coalesce(ss.lead_protection_delta_favors_home, 0) else -coalesce(ss.lead_protection_delta_favors_home, 0) end)::numeric,
        3
      ),
      'hold_rate_edge', round(
        (
          case
            when ss.home_side_raw >= 0
              then coalesce(ss.home_hold_rate_after5, 0.500) - coalesce(ss.away_hold_rate_after5, 0.500)
            else coalesce(ss.away_hold_rate_after5, 0.500) - coalesce(ss.home_hold_rate_after5, 0.500)
          end
        )::numeric,
        3
      ),
      'bullpen_injury_edge', case
        when ss.home_side_raw >= 0
          then coalesce(ss.away_bullpen_injury_count, 0) - coalesce(ss.home_bullpen_injury_count, 0)
        else coalesce(ss.home_bullpen_injury_count, 0) - coalesce(ss.away_bullpen_injury_count, 0)
      end,
      'starter_era_edge', round(
        (case when ss.home_side_raw >= 0 then coalesce(ss.starter_era_delta_favors_home, 0) else -coalesce(ss.starter_era_delta_favors_home, 0) end)::numeric,
        2
      ),
      'recent_form_edge', round(
        (
          case
            when ss.home_side_raw >= 0
              then coalesce(ss.home_form_win_pct, 0.500) - coalesce(ss.away_form_win_pct, 0.500)
            else coalesce(ss.away_form_win_pct, 0.500) - coalesce(ss.home_form_win_pct, 0.500)
          end
        )::numeric,
        3
      )
    ) as signal_components,
    concat_ws(
      '; ',
      case
        when abs(case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_stress_delta_favors_home, 0) else -coalesce(ss.bullpen_stress_delta_favors_home, 0) end) >= 5
          then (case when ss.home_side_raw >= 0 then ss.home_team else ss.away_team end)
            || ' owns the fresher bullpen'
        else null
      end,
      case
        when abs(case when ss.home_side_raw >= 0 then coalesce(ss.bullpen_quality_delta_favors_home, 0) else -coalesce(ss.bullpen_quality_delta_favors_home, 0) end) >= 0.75
          then (case when ss.home_side_raw >= 0 then ss.home_team else ss.away_team end)
            || ' has the stronger recent bullpen quality'
        else null
      end,
      case
        when abs(case when ss.home_side_raw >= 0 then coalesce(ss.lead_protection_delta_favors_home, 0) else -coalesce(ss.lead_protection_delta_favors_home, 0) end) >= 0.08
          then (case when ss.home_side_raw >= 0 then ss.home_team else ss.away_team end)
            || ' closes late leads better'
        else null
      end,
      case
        when case
          when ss.home_side_raw >= 0
            then coalesce(ss.away_bullpen_injury_count, 0) - coalesce(ss.home_bullpen_injury_count, 0)
          else coalesce(ss.home_bullpen_injury_count, 0) - coalesce(ss.away_bullpen_injury_count, 0)
        end >= 2
          then (case when ss.home_side_raw >= 0 then ss.away_team else ss.home_team end)
            || ' is carrying bullpen injuries'
        else null
      end
    ) as signal_summary,
    'Use when the bullpen edge is strong enough to matter more than the starter tax.'::text as entry_trigger
  from side_scored ss
  where least(99.0, greatest(0.0, 50.0 + abs(ss.home_side_raw))) >= 58.0
),
over_components as (
  select
    base.*,
    greatest(0.0, least(24.0, (coalesce(base.combined_bullpen_stress, 0) - 58.0) * 0.55)) as stress_component,
    greatest(
      0.0,
      least(
        20.0,
        ((((coalesce(base.home_recent_bullpen_era, 4.25) + coalesce(base.away_recent_bullpen_era, 4.25)) / 2.0) - 4.10) * 6.0)
      )
    ) as quality_component,
    greatest(0.0, least(12.0, coalesce(base.combined_high_leverage_back_to_back, 0) * 4.0)) as leverage_component,
    greatest(0.0, least(10.0, coalesce(base.combined_bullpen_injuries, 0) * 1.50)) as injury_component,
    greatest(0.0, least(8.0, (coalesce(base.combined_f5_scoring, 0) - 4.50) * 2.0)) as run_env_component,
    greatest(0.0, least(6.0, (coalesce(base.weather_temp, 72) - 72.0) * 0.35)) as weather_component,
    greatest(
      0.0,
      least(
        8.0,
        ((1.45 - (coalesce(base.home_lead_conversion_after5, 0.72) + coalesce(base.away_lead_conversion_after5, 0.72))) * 10.0)
      )
    ) as volatility_component,
    greatest(
      -6.0,
      least(
        6.0,
        case
          when base.dk_total is null then 0.0
          else (9.0 - base.dk_total) * 1.50
        end
      )
    ) as market_component
  from base
),
over_candidates as (
  select
    oc.board_date,
    oc.match_id,
    oc.start_time,
    oc.home_team,
    oc.away_team,
    'LATE_OVER'::text as play_type,
    'late_innings_total'::text as edge_family,
    'Live after 5 innings'::text as entry_window,
    'over'::text as target_side,
    case
      when oc.dk_total is null then 'Live Over'
      else 'Over ' || trim(to_char(oc.dk_total, 'FM999990.0'))
    end as bet_target,
    null::text as fade_team,
    null::numeric as dk_moneyline,
    oc.dk_spread,
    oc.dk_total,
    oc.home_probable_starter_name,
    oc.away_probable_starter_name,
    round(
      least(
        99.0,
        greatest(
          0.0,
          45.0
          + coalesce(oc.stress_component, 0)
          + coalesce(oc.quality_component, 0)
          + coalesce(oc.leverage_component, 0)
          + coalesce(oc.injury_component, 0)
          + coalesce(oc.run_env_component, 0)
          + coalesce(oc.weather_component, 0)
          + coalesce(oc.volatility_component, 0)
          + coalesce(oc.market_component, 0)
        )
      )::numeric,
      2
    ) as signal_strength,
    case
      when (
        45.0
        + coalesce(oc.stress_component, 0)
        + coalesce(oc.quality_component, 0)
        + coalesce(oc.leverage_component, 0)
        + coalesce(oc.injury_component, 0)
        + coalesce(oc.run_env_component, 0)
        + coalesce(oc.weather_component, 0)
        + coalesce(oc.volatility_component, 0)
        + coalesce(oc.market_component, 0)
      ) >= 78 then 'A'
      when (
        45.0
        + coalesce(oc.stress_component, 0)
        + coalesce(oc.quality_component, 0)
        + coalesce(oc.leverage_component, 0)
        + coalesce(oc.injury_component, 0)
        + coalesce(oc.run_env_component, 0)
        + coalesce(oc.weather_component, 0)
        + coalesce(oc.volatility_component, 0)
        + coalesce(oc.market_component, 0)
      ) >= 68 then 'B'
      else 'C'
    end as signal_bucket,
    round(oc.combined_bullpen_stress::numeric, 2) as bullpen_stress_edge,
    round((((oc.home_recent_bullpen_era + oc.away_recent_bullpen_era) / 2.0))::numeric, 2) as bullpen_quality_edge,
    round(
      ((1.45 - (coalesce(oc.home_lead_conversion_after5, 0.72) + coalesce(oc.away_lead_conversion_after5, 0.72))))::numeric,
      3
    ) as lead_protection_edge,
    null::numeric as hold_rate_edge,
    oc.combined_bullpen_injuries as bullpen_injury_edge,
    null::numeric as starter_era_edge,
    null::numeric as recent_form_edge,
    jsonb_build_object(
      'combined_bullpen_stress', round(oc.combined_bullpen_stress::numeric, 2),
      'avg_recent_bullpen_era', round((((oc.home_recent_bullpen_era + oc.away_recent_bullpen_era) / 2.0))::numeric, 2),
      'combined_high_leverage_back_to_back', oc.combined_high_leverage_back_to_back,
      'combined_bullpen_injuries', oc.combined_bullpen_injuries,
      'combined_f5_scoring', round(oc.combined_f5_scoring::numeric, 2),
      'weather_temp', oc.weather_temp,
      'dk_total', oc.dk_total
    ) as signal_components,
    concat_ws(
      '; ',
      case when oc.combined_bullpen_stress >= 80 then 'Both bullpens are heavily taxed' else null end,
      case when ((oc.home_recent_bullpen_era + oc.away_recent_bullpen_era) / 2.0) >= 5.00 then 'Recent bullpen run prevention is weak on both sides' else null end,
      case when oc.combined_high_leverage_back_to_back >= 2 then 'High-leverage arms are working on short rest' else null end,
      case when oc.combined_bullpen_injuries >= 3 then 'Bullpen injuries are thinning late-inning options' else null end,
      case when oc.dk_total is not null and oc.dk_total <= 9.0 then 'The full-game total is still in a playable range' else null end
    ) as signal_summary,
    'Look to enter once the starters are near done or if the game is within three runs after five innings.'::text as entry_trigger
  from over_components oc
  where least(
    99.0,
    greatest(
      0.0,
      45.0
      + coalesce(oc.stress_component, 0)
      + coalesce(oc.quality_component, 0)
      + coalesce(oc.leverage_component, 0)
      + coalesce(oc.injury_component, 0)
      + coalesce(oc.run_env_component, 0)
      + coalesce(oc.weather_component, 0)
      + coalesce(oc.volatility_component, 0)
      + coalesce(oc.market_component, 0)
    )
  ) >= 60.0
),
board_rows as (
  select * from side_candidates
  union all
  select * from over_candidates
)
select
  board_rows.board_date,
  row_number() over (
    partition by board_rows.board_date
    order by board_rows.signal_strength desc, board_rows.start_time asc, board_rows.match_id asc, board_rows.play_type asc
  ) as board_rank,
  board_rows.match_id,
  board_rows.start_time,
  board_rows.home_team,
  board_rows.away_team,
  board_rows.play_type,
  board_rows.edge_family,
  board_rows.entry_window,
  board_rows.target_side,
  board_rows.bet_target,
  board_rows.fade_team,
  board_rows.dk_moneyline,
  board_rows.dk_spread,
  board_rows.dk_total,
  board_rows.home_probable_starter_name,
  board_rows.away_probable_starter_name,
  board_rows.signal_strength,
  board_rows.signal_bucket,
  board_rows.bullpen_stress_edge,
  board_rows.bullpen_quality_edge,
  board_rows.lead_protection_edge,
  board_rows.hold_rate_edge,
  board_rows.bullpen_injury_edge,
  board_rows.starter_era_edge,
  board_rows.recent_form_edge,
  board_rows.signal_components,
  board_rows.signal_summary,
  board_rows.entry_trigger
from board_rows;

create unique index if not exists idx_mv_mlb_daily_betting_board_unique
  on public.mv_mlb_daily_betting_board (match_id, play_type);

do $$
declare
  job_name text;
begin
  foreach job_name in array array[
    'refresh-mlb-daily-betting-board-2h'
  ]
  loop
    perform cron.unschedule(jobid)
    from cron.job
    where jobname = job_name;
  end loop;

  perform cron.schedule(
    'refresh-mlb-daily-betting-board-2h',
    '35 */2 * * *',
    'REFRESH MATERIALIZED VIEW CONCURRENTLY public.mv_mlb_daily_betting_board;'
  );
exception
  when undefined_table or invalid_schema_name then
    null;
end $$;

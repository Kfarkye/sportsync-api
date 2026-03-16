-- MLB bullpen-driven daily board.
-- Safe to run read-only in the Supabase SQL editor.

select
  board_date,
  board_rank,
  start_time,
  play_type,
  edge_family,
  bet_target,
  fade_team,
  signal_strength,
  signal_bucket,
  home_team,
  away_team,
  dk_moneyline,
  dk_spread,
  dk_total,
  signal_summary,
  entry_trigger
from public.mv_mlb_daily_betting_board
where board_date between current_date and current_date + 2
order by board_date asc, board_rank asc;

-- Quick split: best bullpen-side plays.
select
  board_date,
  board_rank,
  start_time,
  bet_target,
  fade_team,
  signal_strength,
  bullpen_stress_edge,
  bullpen_quality_edge,
  lead_protection_edge,
  starter_era_edge,
  signal_summary
from public.mv_mlb_daily_betting_board
where play_type = 'BULLPEN_SIDE'
  and board_date between current_date and current_date + 2
order by board_date asc, signal_strength desc, start_time asc;

-- Quick split: best live late-over setups.
select
  board_date,
  board_rank,
  start_time,
  home_team,
  away_team,
  bet_target,
  signal_strength,
  bullpen_stress_edge as combined_bullpen_stress,
  bullpen_quality_edge as avg_recent_bullpen_era,
  bullpen_injury_edge as combined_bullpen_injuries,
  dk_total,
  signal_summary,
  entry_trigger
from public.mv_mlb_daily_betting_board
where play_type = 'LATE_OVER'
  and board_date between current_date and current_date + 2
order by board_date asc, signal_strength desc, start_time asc;

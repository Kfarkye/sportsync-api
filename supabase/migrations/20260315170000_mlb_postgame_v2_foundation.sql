alter table if exists public.mlb_postgame
  add column if not exists venue_city text,
  add column if not exists venue_state text,
  add column if not exists venue_indoor boolean,
  add column if not exists weather_temp integer,
  add column if not exists weather_condition text,
  add column if not exists weather_gust integer,
  add column if not exists weather_precipitation integer,
  add column if not exists home_starter_name text,
  add column if not exists away_starter_name text,
  add column if not exists home_starter_id text,
  add column if not exists away_starter_id text,
  add column if not exists total_innings integer,
  add column if not exists is_extra_innings boolean,
  add column if not exists season_type text;

create table if not exists public.mlb_inning_scores (
  id bigserial primary key,
  match_id text not null,
  espn_event_id text,
  game_date date,
  season_type text,
  home_team text,
  away_team text,
  home_score integer,
  away_score integer,
  home_innings integer[] not null default '{}',
  away_innings integer[] not null default '{}',
  home_first_inning_runs integer,
  away_first_inning_runs integer,
  home_f5_runs integer,
  away_f5_runs integer,
  home_l4_runs integer,
  away_l4_runs integer,
  total_innings integer,
  is_extra_innings boolean,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists public.mlb_inning_scores
  add column if not exists match_id text,
  add column if not exists espn_event_id text,
  add column if not exists game_date date,
  add column if not exists season_type text,
  add column if not exists home_team text,
  add column if not exists away_team text,
  add column if not exists home_score integer,
  add column if not exists away_score integer,
  add column if not exists home_innings integer[] not null default '{}',
  add column if not exists away_innings integer[] not null default '{}',
  add column if not exists home_first_inning_runs integer,
  add column if not exists away_first_inning_runs integer,
  add column if not exists home_f5_runs integer,
  add column if not exists away_f5_runs integer,
  add column if not exists home_l4_runs integer,
  add column if not exists away_l4_runs integer,
  add column if not exists total_innings integer,
  add column if not exists is_extra_innings boolean,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create unique index if not exists idx_mlb_inning_scores_match_id
  on public.mlb_inning_scores (match_id);

create index if not exists idx_mlb_inning_scores_game_date
  on public.mlb_inning_scores (game_date desc);

create table if not exists public.mlb_pitcher_game_logs (
  id bigserial primary key,
  match_id text not null,
  espn_event_id text,
  game_date date,
  season_type text,
  team text,
  team_abbr text,
  home_away text,
  opponent_team text,
  athlete_id text not null,
  athlete_name text not null,
  is_starter boolean not null default false,
  pitch_order integer,
  innings_pitched text,
  innings_outs integer,
  hits_allowed integer,
  runs_allowed integer,
  earned_runs integer,
  walks integer,
  strikeouts integer,
  home_runs_allowed integer,
  pitches_thrown integer,
  strikes_thrown integer,
  era numeric,
  whip numeric,
  decision text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table if exists public.mlb_pitcher_game_logs
  add column if not exists match_id text,
  add column if not exists espn_event_id text,
  add column if not exists game_date date,
  add column if not exists season_type text,
  add column if not exists team text,
  add column if not exists team_abbr text,
  add column if not exists home_away text,
  add column if not exists opponent_team text,
  add column if not exists athlete_id text,
  add column if not exists athlete_name text,
  add column if not exists is_starter boolean not null default false,
  add column if not exists pitch_order integer,
  add column if not exists innings_pitched text,
  add column if not exists innings_outs integer,
  add column if not exists hits_allowed integer,
  add column if not exists runs_allowed integer,
  add column if not exists earned_runs integer,
  add column if not exists walks integer,
  add column if not exists strikeouts integer,
  add column if not exists home_runs_allowed integer,
  add column if not exists pitches_thrown integer,
  add column if not exists strikes_thrown integer,
  add column if not exists era numeric,
  add column if not exists whip numeric,
  add column if not exists decision text,
  add column if not exists created_at timestamptz not null default now(),
  add column if not exists updated_at timestamptz not null default now();

create unique index if not exists idx_mlb_pitcher_logs_match_athlete_order
  on public.mlb_pitcher_game_logs (match_id, athlete_id, pitch_order);

create index if not exists idx_mlb_pitcher_logs_athlete
  on public.mlb_pitcher_game_logs (athlete_id, game_date desc);

create index if not exists idx_mlb_pitcher_logs_team
  on public.mlb_pitcher_game_logs (team, game_date desc);

create or replace function public.baseball_ip_to_outs(ip_text text)
returns integer
language sql
immutable
as $$
  select case
    when ip_text is null or btrim(ip_text) = '' then null
    when btrim(ip_text) ~ '^\d+$' then btrim(ip_text)::integer * 3
    when btrim(ip_text) ~ '^\d+\.[0-2]$' then
      split_part(btrim(ip_text), '.', 1)::integer * 3
      + split_part(btrim(ip_text), '.', 2)::integer
    else null
  end
$$;

create or replace function public.baseball_outs_to_ip(outs numeric)
returns numeric
language sql
immutable
as $$
  select case
    when outs is null then null
    else trunc(outs / 3) + ((outs::integer % 3)::numeric / 10)
  end
$$;

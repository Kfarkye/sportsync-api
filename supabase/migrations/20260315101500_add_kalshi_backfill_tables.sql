create table if not exists public.kalshi_settlements (
  id uuid primary key default gen_random_uuid(),
  event_ticker text not null,
  series_ticker text not null,
  market_ticker text not null unique,
  sport text,
  league text,
  title text,
  subtitle text,
  team_name text,
  opponent_name text,
  is_home_team boolean,
  game_date date not null,
  closing_price numeric,
  settlement_price numeric,
  settlement_value numeric,
  result text,
  volume numeric,
  open_interest numeric,
  status text,
  match_id text,
  raw_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_kalshi_game_date on public.kalshi_settlements(game_date);
create index if not exists idx_kalshi_league on public.kalshi_settlements(league);
create index if not exists idx_kalshi_match_id on public.kalshi_settlements(match_id);
create index if not exists idx_kalshi_series on public.kalshi_settlements(series_ticker);

create table if not exists public.kalshi_team_map (
  id bigserial primary key,
  kalshi_name text not null,
  espn_name text not null,
  league text not null,
  kalshi_abbrev text not null default '',
  espn_team_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists idx_kalshi_team_map_unique
  on public.kalshi_team_map (league, kalshi_name, kalshi_abbrev);

create index if not exists idx_kalshi_team_map_espn_name
  on public.kalshi_team_map (league, espn_name);

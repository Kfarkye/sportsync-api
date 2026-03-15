create table if not exists public.kalshi_line_markets (
  id uuid primary key default gen_random_uuid(),
  event_ticker text not null,
  series_ticker text not null,
  market_ticker text not null unique,
  sport text,
  league text,
  market_kind text,
  title text,
  subtitle text,
  team_name text,
  opponent_name text,
  is_home_team boolean,
  line_value numeric,
  line_side text,
  game_date date not null,
  closing_price numeric,
  settlement_price numeric,
  settlement_value numeric,
  result text,
  volume numeric,
  open_interest numeric,
  status text,
  raw_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_kalshi_line_markets_game_date on public.kalshi_line_markets(game_date);
create index if not exists idx_kalshi_line_markets_league on public.kalshi_line_markets(league);
create index if not exists idx_kalshi_line_markets_kind on public.kalshi_line_markets(market_kind);
create index if not exists idx_kalshi_line_markets_series on public.kalshi_line_markets(series_ticker);

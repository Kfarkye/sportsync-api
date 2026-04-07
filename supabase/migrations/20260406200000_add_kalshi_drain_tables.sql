-- Migration: codify the two drain-kalshi-orderbook tables + closing-price RPC
-- These existed live but had no migration file, breaking fresh-env deploys.

-- 1. kalshi_events_active (discovery phase output)
create table if not exists public.kalshi_events_active (
  event_ticker text primary key,
  sport text,
  league text,
  title text,
  home_team text,
  away_team text,
  game_date date,
  market_count integer,
  market_tickers text[] default '{}',
  status text not null default 'active',
  discovered_at timestamptz not null default now(),
  last_snapshot_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists idx_kalshi_events_active_status on public.kalshi_events_active(status);
create index if not exists idx_kalshi_events_active_game_date on public.kalshi_events_active(game_date);
create index if not exists idx_kalshi_events_active_league on public.kalshi_events_active(league);

-- 2. kalshi_orderbook_snapshots (snapshot phase output)
create table if not exists public.kalshi_orderbook_snapshots (
  id uuid primary key default gen_random_uuid(),
  market_ticker text not null,
  event_ticker text not null,
  sport text,
  league text,

  snapshot_type text not null,
  market_type text,
  market_label text,
  line_value numeric,
  line_side text,

  yes_best_bid numeric,
  yes_best_bid_qty integer,
  yes_total_bid_qty integer,
  yes_depth_levels jsonb,

  no_best_bid numeric,
  no_best_bid_qty integer,
  no_total_bid_qty integer,
  no_depth_levels jsonb,

  mid_price numeric,
  spread numeric,
  spread_width numeric,
  yes_no_imbalance numeric,

  recent_trade_count integer,
  recent_yes_volume integer,
  recent_no_volume integer,
  recent_volume_imbalance numeric,
  last_trade_price numeric,
  last_trade_side text,
  last_trade_at timestamptz,

  volume integer,
  open_interest integer,
  yes_price numeric,
  no_price numeric,

  captured_at timestamptz not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_kalshi_ob_snap_market on public.kalshi_orderbook_snapshots(market_ticker);
create index if not exists idx_kalshi_ob_snap_event on public.kalshi_orderbook_snapshots(event_ticker);
create index if not exists idx_kalshi_ob_snap_captured on public.kalshi_orderbook_snapshots(captured_at);
create index if not exists idx_kalshi_ob_snap_type on public.kalshi_orderbook_snapshots(snapshot_type);

-- 3. apply_kalshi_closing_prices_from_snapshots RPC
create or replace function public.apply_kalshi_closing_prices_from_snapshots(p_rows jsonb)
returns integer
language plpgsql
security definer
set search_path = 'public'
as $function$
declare
  v_updated integer := 0;
begin
  if to_regclass('public.kalshi_line_markets') is null then
    return 0;
  end if;

  with incoming as (
    select
      trim(coalesce(value->>'market_ticker', '')) as market_ticker,
      case
        when coalesce(value->>'closing_price', '') ~ '^[+-]?[0-9]+(\.[0-9]+)?$' then (value->>'closing_price')::numeric
        else null
      end as closing_price,
      case
        when coalesce(value->>'captured_at', '') = '' then null
        else (value->>'captured_at')::timestamptz
      end as captured_at
    from jsonb_array_elements(coalesce(p_rows, '[]'::jsonb)) as t(value)
  ), dedup as (
    select distinct on (i.market_ticker)
      i.market_ticker,
      i.closing_price
    from incoming i
    where i.market_ticker <> ''
      and i.closing_price is not null
    order by i.market_ticker, i.captured_at desc nulls last
  ), updates as (
    update public.kalshi_line_markets lm
    set closing_price = d.closing_price
    from dedup d
    where lm.market_ticker = d.market_ticker
      and lm.closing_price is null
    returning 1
  )
  select count(*)::integer into v_updated from updates;

  return coalesce(v_updated, 0);
end;
$function$;

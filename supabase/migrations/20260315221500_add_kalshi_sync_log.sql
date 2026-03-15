create table if not exists public.kalshi_sync_log (
  id bigint generated always as identity primary key,
  run_at timestamptz not null default now(),
  league text not null default 'all',
  rows_processed integer not null default 0,
  rows_sanitized integer not null default 0,
  sanitizer_rate numeric(6,4) not null default 0,
  notes text
);

create index if not exists idx_kalshi_sync_log_run_at
  on public.kalshi_sync_log (run_at desc);

create index if not exists idx_kalshi_sync_log_league
  on public.kalshi_sync_log (league);

alter table public.kalshi_sync_log enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'kalshi_sync_log'
      and policyname = 'kalshi_sync_log_service_role_rw'
  ) then
    create policy kalshi_sync_log_service_role_rw
      on public.kalshi_sync_log
      for all
      to service_role
      using (true)
      with check (true);
  end if;
end
$$;

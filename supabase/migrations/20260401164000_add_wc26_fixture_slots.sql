-- Parallel slot-label mapping for WC26 knockout fixtures.
-- Keeps wc26_fixtures FK constraints intact while preserving bracket semantics.

create table if not exists public.wc26_fixture_slots (
  fixture_id text primary key
    references public.wc26_fixtures(fixture_id)
    on delete cascade,
  home_slot_label text,
  away_slot_label text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint wc26_fixture_slots_nonempty_labels
    check (home_slot_label is not null or away_slot_label is not null)
);

create index if not exists idx_wc26_fixture_slots_home_label
  on public.wc26_fixture_slots (home_slot_label);

create index if not exists idx_wc26_fixture_slots_away_label
  on public.wc26_fixture_slots (away_slot_label);

create index if not exists idx_wc26_fixture_slots_updated_at
  on public.wc26_fixture_slots (updated_at desc);

alter table public.wc26_fixture_slots enable row level security;

do $$
begin
  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'wc26_fixture_slots'
      and policyname = 'wc26 fixture slots read'
  ) then
    create policy "wc26 fixture slots read"
      on public.wc26_fixture_slots
      for select
      using (true);
  end if;

  if not exists (
    select 1
    from pg_policies
    where schemaname = 'public'
      and tablename = 'wc26_fixture_slots'
      and policyname = 'wc26 fixture slots service write'
  ) then
    create policy "wc26 fixture slots service write"
      on public.wc26_fixture_slots
      for all
      using (auth.role() = 'service_role')
      with check (auth.role() = 'service_role');
  end if;
end
$$;

create table if not exists public.subscriptions (
  id uuid primary key default gen_random_uuid(),
  stripe_subscription_id text not null unique,
  customer_id uuid not null references public.customers(id) on delete cascade,
  tier text not null default 'free',
  status text not null default 'active',
  current_period_end timestamptz,
  grace_ends_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_subscriptions_customer_id on public.subscriptions(customer_id);
create index if not exists idx_subscriptions_status on public.subscriptions(status);
create index if not exists idx_subscriptions_grace_ends_at on public.subscriptions(grace_ends_at);

alter table public.subscriptions enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public' and tablename='subscriptions' and policyname='subscriptions_service_role_rw'
  ) then
    create policy subscriptions_service_role_rw
      on public.subscriptions
      for all
      to service_role
      using (true)
      with check (true);
  end if;
end
$$;

create table if not exists public.stripe_webhook_events (
  id bigint generated always as identity primary key,
  stripe_event_id text not null unique,
  event_type text not null,
  payload jsonb not null default '{}'::jsonb,
  status text not null default 'processing',
  error_message text,
  processed_at timestamptz not null default now(),
  created_at timestamptz not null default now()
);

create index if not exists idx_stripe_webhook_events_status on public.stripe_webhook_events(status);
create index if not exists idx_stripe_webhook_events_created_at on public.stripe_webhook_events(created_at desc);

alter table public.stripe_webhook_events enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public' and tablename='stripe_webhook_events' and policyname='stripe_webhook_events_service_role_rw'
  ) then
    create policy stripe_webhook_events_service_role_rw
      on public.stripe_webhook_events
      for all
      to service_role
      using (true)
      with check (true);
  end if;
end
$$;

alter table public.api_keys add column if not exists revoked_at timestamptz;
alter table public.api_keys add column if not exists rotated_from_key_id uuid;

do $$
begin
  if not exists (
    select 1
    from information_schema.table_constraints
    where table_schema='public'
      and table_name='api_keys'
      and constraint_name='api_keys_rotated_from_key_id_fkey'
  ) then
    alter table public.api_keys
      add constraint api_keys_rotated_from_key_id_fkey
      foreign key (rotated_from_key_id) references public.api_keys(id) on delete set null;
  end if;
end
$$;

create table if not exists public.billing_config (
  id boolean primary key default true,
  legacy_aliases_until timestamptz not null default (now() + interval '14 days'),
  updated_at timestamptz not null default now()
);

insert into public.billing_config (id)
values (true)
on conflict (id) do nothing;

alter table public.billing_config enable row level security;

do $$
begin
  if not exists (
    select 1 from pg_policies
    where schemaname='public' and tablename='billing_config' and policyname='billing_config_service_role_rw'
  ) then
    create policy billing_config_service_role_rw
      on public.billing_config
      for all
      to service_role
      using (true)
      with check (true);
  end if;
end
$$;

create or replace function public.resolve_billing_tier(raw_tier text)
returns text
language plpgsql
stable
set search_path = public
as $$
declare
  normalized text := lower(coalesce(trim(raw_tier), ''));
  aliases_active boolean := false;
begin
  select now() <= legacy_aliases_until into aliases_active
  from public.billing_config
  where id = true;

  if normalized in ('free', 'builder', 'pro') then
    return normalized;
  end if;

  if aliases_active then
    if normalized = 'sandbox' then return 'free'; end if;
    if normalized = 'production' then return 'builder'; end if;
    if normalized = 'enterprise' then return 'pro'; end if;
  end if;

  return 'free';
end;
$$;

do $$
declare
  has_plan_entitlements boolean;
  has_plan_column boolean;
  has_tier_column boolean;
begin
  select exists(
    select 1 from information_schema.tables
    where table_schema='public' and table_name='plan_entitlements'
  ) into has_plan_entitlements;

  if has_plan_entitlements then
    select exists(
      select 1 from information_schema.columns
      where table_schema='public' and table_name='plan_entitlements' and column_name='plan'
    ) into has_plan_column;

    select exists(
      select 1 from information_schema.columns
      where table_schema='public' and table_name='plan_entitlements' and column_name='tier'
    ) into has_tier_column;

    if has_plan_column then
      execute $$update public.plan_entitlements set plan='free' where lower(plan)='sandbox'$$;
      execute $$update public.plan_entitlements set plan='builder' where lower(plan)='production'$$;
      execute $$update public.plan_entitlements set plan='pro' where lower(plan)='enterprise'$$;
    elsif has_tier_column then
      execute $$update public.plan_entitlements set tier='free' where lower(tier)='sandbox'$$;
      execute $$update public.plan_entitlements set tier='builder' where lower(tier)='production'$$;
      execute $$update public.plan_entitlements set tier='pro' where lower(tier)='enterprise'$$;
    end if;

    if exists(select 1 from information_schema.columns where table_schema='public' and table_name='plan_entitlements' and column_name='rate_limit_per_minute') then
      if has_plan_column then
        execute $$update public.plan_entitlements set rate_limit_per_minute=10 where lower(plan)='free'$$;
        execute $$update public.plan_entitlements set rate_limit_per_minute=60 where lower(plan)='builder'$$;
        execute $$update public.plan_entitlements set rate_limit_per_minute=180 where lower(plan)='pro'$$;
      elsif has_tier_column then
        execute $$update public.plan_entitlements set rate_limit_per_minute=10 where lower(tier)='free'$$;
        execute $$update public.plan_entitlements set rate_limit_per_minute=60 where lower(tier)='builder'$$;
        execute $$update public.plan_entitlements set rate_limit_per_minute=180 where lower(tier)='pro'$$;
      end if;
    end if;

    if exists(select 1 from information_schema.columns where table_schema='public' and table_name='plan_entitlements' and column_name='rate_limit_per_day') then
      if has_plan_column then
        execute $$update public.plan_entitlements set rate_limit_per_day=500 where lower(plan)='free'$$;
        execute $$update public.plan_entitlements set rate_limit_per_day=10000 where lower(plan)='builder'$$;
        execute $$update public.plan_entitlements set rate_limit_per_day=50000 where lower(plan)='pro'$$;
      elsif has_tier_column then
        execute $$update public.plan_entitlements set rate_limit_per_day=500 where lower(tier)='free'$$;
        execute $$update public.plan_entitlements set rate_limit_per_day=10000 where lower(tier)='builder'$$;
        execute $$update public.plan_entitlements set rate_limit_per_day=50000 where lower(tier)='pro'$$;
      end if;
    end if;

    if exists(select 1 from information_schema.columns where table_schema='public' and table_name='plan_entitlements' and column_name='monthly_request_cap') then
      if has_plan_column then
        execute $$update public.plan_entitlements set monthly_request_cap=10000 where lower(plan)='free'$$;
        execute $$update public.plan_entitlements set monthly_request_cap=300000 where lower(plan)='builder'$$;
        execute $$update public.plan_entitlements set monthly_request_cap=2000000 where lower(plan)='pro'$$;
      elsif has_tier_column then
        execute $$update public.plan_entitlements set monthly_request_cap=10000 where lower(tier)='free'$$;
        execute $$update public.plan_entitlements set monthly_request_cap=300000 where lower(tier)='builder'$$;
        execute $$update public.plan_entitlements set monthly_request_cap=2000000 where lower(tier)='pro'$$;
      end if;
    end if;
  end if;
end
$$;

update public.customers
set plan = public.resolve_billing_tier(plan)
where plan is not null;

update public.api_keys
set tier = public.resolve_billing_tier(tier)
where tier is not null;

alter table public.api_request_logs add column if not exists billing_tier text;
alter table public.api_request_logs add column if not exists billing_mode text;
alter table public.api_request_logs add column if not exists billing_decision text;

create or replace function public.get_api_key_billing_state(raw_hash text)
returns table (
  api_key_id uuid,
  customer_id uuid,
  key_active boolean,
  key_revoked_at timestamptz,
  effective_tier text,
  rate_limit_per_minute integer,
  rate_limit_per_day integer,
  monthly_request_cap integer
)
language sql
stable
set search_path = public
as $$
  select
    k.id as api_key_id,
    k.customer_id,
    coalesce(k.active, false) as key_active,
    k.revoked_at as key_revoked_at,
    public.resolve_billing_tier(k.tier) as effective_tier,
    k.rate_limit_per_minute,
    k.rate_limit_per_day,
    k.monthly_request_cap
  from public.api_keys k
  where k.key_hash = raw_hash
  limit 1;
$$;

create or replace function public.run_billing_downgrade_worker()
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  affected_customer_ids uuid[];
  downgraded_subscriptions integer := 0;
  downgraded_customers integer := 0;
  downgraded_keys integer := 0;
  free_rpm integer := 10;
  free_day integer := 500;
  free_month integer := 10000;
begin
  with candidates as (
    select id, customer_id
    from public.subscriptions
    where grace_ends_at is not null
      and grace_ends_at < now()
      and lower(status) in ('past_due', 'unpaid', 'incomplete', 'incomplete_expired')
      and lower(tier) <> 'free'
  ),
  updated_subscriptions as (
    update public.subscriptions s
      set tier = 'free',
          status = 'past_due_downgraded',
          grace_ends_at = null,
          updated_at = now()
    from candidates c
    where s.id = c.id
    returning s.customer_id
  ),
  updated_customers as (
    update public.customers c
      set plan = 'free',
          status = 'past_due_downgraded'
    where c.id in (select customer_id from updated_subscriptions)
    returning c.id
  ),
  updated_keys as (
    update public.api_keys k
      set tier = 'free',
          rate_limit_per_minute = free_rpm,
          rate_limit_per_day = free_day,
          monthly_request_cap = free_month
    where k.customer_id in (select id from updated_customers)
      and k.active = true
    returning k.id
  )
  select
    (select count(*) from updated_subscriptions),
    (select count(*) from updated_customers),
    (select count(*) from updated_keys),
    (select array_agg(id) from updated_customers)
  into downgraded_subscriptions, downgraded_customers, downgraded_keys, affected_customer_ids;

  return jsonb_build_object(
    'downgraded_subscriptions', coalesce(downgraded_subscriptions, 0),
    'downgraded_customers', coalesce(downgraded_customers, 0),
    'downgraded_keys', coalesce(downgraded_keys, 0),
    'customer_ids', coalesce(affected_customer_ids, array[]::uuid[])
  );
end;
$$;

do $$
declare
  existing_job_id integer;
begin
  if exists(select 1 from pg_namespace where nspname = 'cron') then
    select jobid
    into existing_job_id
    from cron.job
    where jobname = 'billing-downgrade-hourly'
    limit 1;

    if existing_job_id is not null then
      perform cron.unschedule(existing_job_id);
    end if;

    perform cron.schedule(
      'billing-downgrade-hourly',
      '0 * * * *',
      $$select public.run_billing_downgrade_worker();$$
    );
  end if;
end
$$;

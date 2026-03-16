update public.billing_config
set legacy_aliases_until = now() - interval '1 second',
    updated_at = now()
where id = true;

update public.customers
set plan = public.resolve_billing_tier(plan)
where plan is not null;

update public.api_keys
set tier = public.resolve_billing_tier(tier)
where tier is not null;

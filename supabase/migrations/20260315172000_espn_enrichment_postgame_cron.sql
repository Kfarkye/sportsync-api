do $$
declare
  enrichment_job_id bigint;
  v_key text := current_setting('app.settings.service_role_key', true);
begin
  select jobid
  into enrichment_job_id
  from cron.job
  where jobname = 'espn-enrichment-postgame-4h';

  if enrichment_job_id is not null then
    perform cron.unschedule(enrichment_job_id);
  end if;

  if coalesce(v_key, '') = '' then
    return;
  end if;

  perform cron.schedule(
    'espn-enrichment-postgame-4h',
    '25 */4 * * *',
    $cron$
    select net.http_post(
      url := 'https://qffzvrnbzabcokqqrwbv.supabase.co/functions/v1/espn-enrichment-drain?mode=postgame&league=nba,nfl,nhl,mlb&days=5',
      headers := jsonb_build_object(
        'Authorization', 'Bearer ' || v_key,
        'apikey', v_key,
        'Content-Type', 'application/json'
      ),
      body := '{}'::jsonb,
      timeout_milliseconds := 60000
    );
    $cron$
  );
exception
  when undefined_table or invalid_schema_name then
    null;
end $$;

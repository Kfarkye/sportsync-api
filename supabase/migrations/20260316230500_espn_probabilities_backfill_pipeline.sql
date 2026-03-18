-- ESPN probabilities backfill pipeline (NBA/NCAAB/NFL/MLB)

CREATE TABLE IF NOT EXISTS public.espn_probabilities (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  match_id text NOT NULL,
  league_id text NOT NULL,
  sport text NOT NULL,
  espn_event_id text NOT NULL,
  play_id text,
  sequence_number integer NOT NULL,
  home_win_pct numeric(6,4),
  away_win_pct numeric(6,4),
  tie_pct numeric(6,4),
  spread_cover_prob_home numeric(6,4),
  spread_push_prob numeric(6,4),
  total_over_prob numeric(6,4),
  total_push_prob numeric(6,4),
  seconds_left integer,
  last_modified timestamptz,
  source_id text,
  source_state text,
  created_at timestamptz DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_espn_probabilities_match_seq
  ON public.espn_probabilities (match_id, sequence_number);
CREATE INDEX IF NOT EXISTS idx_espn_prob_match
  ON public.espn_probabilities (match_id);
CREATE INDEX IF NOT EXISTS idx_espn_prob_league
  ON public.espn_probabilities (league_id);
CREATE INDEX IF NOT EXISTS idx_espn_prob_total_over
  ON public.espn_probabilities (match_id, total_over_prob);
CREATE INDEX IF NOT EXISTS idx_espn_prob_sequence
  ON public.espn_probabilities (match_id, sequence_number);

ALTER TABLE public.espn_probabilities ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'espn_probabilities'
      AND policyname = 'espn_probabilities_anon_read'
  ) THEN
    CREATE POLICY espn_probabilities_anon_read
      ON public.espn_probabilities
      FOR SELECT
      TO anon
      USING (true);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public._prob_backfill_queue (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  match_id text NOT NULL,
  league_id text NOT NULL,
  sport text NOT NULL,
  espn_event_id text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  request_id bigint,
  entries_inserted integer,
  error_msg text,
  created_at timestamptz DEFAULT now(),
  completed_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_prob_backfill_queue_match
  ON public._prob_backfill_queue (match_id);
CREATE INDEX IF NOT EXISTS idx_prob_queue_status
  ON public._prob_backfill_queue (status);
CREATE INDEX IF NOT EXISTS idx_prob_queue_league
  ON public._prob_backfill_queue (league_id);

CREATE OR REPLACE FUNCTION public._prob_seed_queue()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  inserted_rows integer;
BEGIN
  INSERT INTO public._prob_backfill_queue (match_id, league_id, sport, espn_event_id, status)
  SELECT
    m.id,
    m.league_id,
    CASE m.league_id
      WHEN 'nba' THEN 'basketball'
      WHEN 'mens-college-basketball' THEN 'basketball'
      WHEN 'nfl' THEN 'football'
      WHEN 'mlb' THEN 'baseball'
    END AS sport,
    regexp_replace(m.id, '_[^_]+$', '') AS espn_event_id,
    'pending' AS status
  FROM public.matches m
  WHERE m.league_id IN ('nba', 'mens-college-basketball', 'nfl', 'mlb')
    AND m.status IN ('STATUS_FINAL', 'STATUS_FULL_TIME')
    AND NOT EXISTS (
      SELECT 1
      FROM public._prob_backfill_queue q
      WHERE q.match_id = m.id
    )
  ON CONFLICT (match_id) DO NOTHING;

  GET DIAGNOSTICS inserted_rows = ROW_COUNT;
  RETURN inserted_rows;
END;
$$;

CREATE OR REPLACE FUNCTION public._prob_fire_batch(batch_size integer DEFAULT 3)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, extensions
AS $$
DECLARE
  rec record;
  req_id bigint;
  fired integer := 0;
BEGIN
  FOR rec IN
    SELECT id, match_id, league_id, sport, espn_event_id
    FROM public._prob_backfill_queue
    WHERE status = 'pending'
    ORDER BY
      CASE league_id
        WHEN 'nfl' THEN 1
        WHEN 'nba' THEN 2
        WHEN 'mlb' THEN 3
        WHEN 'mens-college-basketball' THEN 4
        ELSE 5
      END,
      id ASC
    LIMIT batch_size
    FOR UPDATE SKIP LOCKED
  LOOP
    SELECT net.http_get(
      url := format(
        'https://sports.core.api.espn.com/v2/sports/%s/leagues/%s/events/%s/competitions/%s/probabilities?limit=500',
        rec.sport, rec.league_id, rec.espn_event_id, rec.espn_event_id
      )
    ) INTO req_id;

    UPDATE public._prob_backfill_queue
    SET status = 'fetching',
        request_id = req_id
    WHERE id = rec.id;

    fired := fired + 1;
  END LOOP;

  RETURN fired;
END;
$$;

CREATE OR REPLACE FUNCTION public._prob_process_responses()
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  rec record;
  resp record;
  body jsonb;
  items jsonb;
  entry jsonb;
  idx integer;
  processed integer := 0;
  inserted_entries integer;
  page_count integer;
BEGIN
  FOR rec IN
    SELECT q.id, q.match_id, q.league_id, q.sport, q.espn_event_id, q.request_id
    FROM public._prob_backfill_queue q
    WHERE q.status = 'fetching'
      AND q.request_id IS NOT NULL
    LIMIT 100
  LOOP
    SELECT r.status_code, r.content, r.error_msg
    INTO resp
    FROM net._http_response r
    WHERE r.id = rec.request_id;

    IF resp IS NULL THEN
      CONTINUE;
    END IF;

    IF resp.status_code IS NULL THEN
      CONTINUE;
    END IF;

    IF resp.status_code = 429 OR resp.status_code = 503 THEN
      UPDATE public._prob_backfill_queue
      SET status = 'pending',
          request_id = NULL,
          error_msg = 'retryable HTTP ' || resp.status_code
      WHERE id = rec.id;
      processed := processed + 1;
      CONTINUE;
    END IF;

    IF resp.status_code != 200 THEN
      UPDATE public._prob_backfill_queue
      SET status = 'error',
          error_msg = COALESCE(resp.error_msg, 'HTTP ' || resp.status_code),
          completed_at = now()
      WHERE id = rec.id;
      processed := processed + 1;
      CONTINUE;
    END IF;

    BEGIN
      body := resp.content::jsonb;
      items := body->'items';
      page_count := COALESCE(NULLIF(body->>'pageCount', '')::integer, 1);
    EXCEPTION WHEN OTHERS THEN
      UPDATE public._prob_backfill_queue
      SET status = 'error',
          error_msg = 'JSON parse error',
          completed_at = now()
      WHERE id = rec.id;
      processed := processed + 1;
      CONTINUE;
    END;

    IF items IS NULL OR jsonb_typeof(items) <> 'array' OR jsonb_array_length(items) = 0 THEN
      UPDATE public._prob_backfill_queue
      SET status = 'done',
          entries_inserted = 0,
          completed_at = now(),
          error_msg = CASE WHEN page_count > 1 THEN 'warning: pageCount=' || page_count ELSE NULL END
      WHERE id = rec.id;
      processed := processed + 1;
      CONTINUE;
    END IF;

    inserted_entries := 0;
    FOR idx IN 0..jsonb_array_length(items) - 1 LOOP
      entry := items->idx;
      BEGIN
        INSERT INTO public.espn_probabilities (
          match_id,
          league_id,
          sport,
          espn_event_id,
          play_id,
          sequence_number,
          home_win_pct,
          away_win_pct,
          tie_pct,
          spread_cover_prob_home,
          spread_push_prob,
          total_over_prob,
          total_push_prob,
          seconds_left,
          last_modified,
          source_id,
          source_state
        ) VALUES (
          rec.match_id,
          rec.league_id,
          rec.sport,
          rec.espn_event_id,
          CASE
            WHEN entry->'play'->>'$ref' IS NULL THEN NULL
            ELSE regexp_replace(entry->'play'->>'$ref', '.*/plays/', '')
          END,
          NULLIF(entry->>'sequenceNumber', '')::integer,
          NULLIF(entry->>'homeWinPercentage', '')::numeric,
          NULLIF(entry->>'awayWinPercentage', '')::numeric,
          NULLIF(entry->>'tiePercentage', '')::numeric,
          NULLIF(entry->>'spreadCoverProbHome', '')::numeric,
          NULLIF(entry->>'spreadPushProb', '')::numeric,
          NULLIF(entry->>'totalOverProb', '')::numeric,
          NULLIF(entry->>'totalPushProb', '')::numeric,
          NULLIF(entry->>'secondsLeft', '')::integer,
          NULLIF(entry->>'lastModified', '')::timestamptz,
          entry->'source'->>'id',
          entry->'source'->>'state'
        )
        ON CONFLICT (match_id, sequence_number) DO NOTHING;

        inserted_entries := inserted_entries + 1;
      EXCEPTION WHEN OTHERS THEN
        CONTINUE;
      END;
    END LOOP;

    UPDATE public._prob_backfill_queue
    SET status = 'done',
        entries_inserted = inserted_entries,
        completed_at = now(),
        error_msg = CASE WHEN page_count > 1 THEN 'warning: pageCount=' || page_count ELSE NULL END
    WHERE id = rec.id;

    processed := processed + 1;
  END LOOP;

  RETURN processed;
END;
$$;

CREATE OR REPLACE FUNCTION public._prob_backfill_tick()
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  processed integer;
  fired integer;
  in_flight integer;
  pending_count integer;
BEGIN
  UPDATE public._prob_backfill_queue
  SET status = 'pending',
      request_id = NULL
  WHERE status = 'fetching'
    AND request_id IN (
      SELECT q.request_id
      FROM public._prob_backfill_queue q
      JOIN net._http_response r ON r.id = q.request_id
      WHERE q.status = 'fetching'
        AND r.error_msg IS NOT NULL
    );

  SELECT public._prob_process_responses() INTO processed;
  SELECT count(*) INTO in_flight FROM public._prob_backfill_queue WHERE status = 'fetching';

  IF in_flight < 10 THEN
    SELECT public._prob_fire_batch(LEAST(5, 10 - in_flight)) INTO fired;
  ELSE
    fired := 0;
  END IF;

  SELECT count(*) INTO pending_count FROM public._prob_backfill_queue WHERE status = 'pending';
  RETURN format('p=%s f=%s fly=%s q=%s', processed, fired, in_flight, pending_count);
END;
$$;

DO $$
DECLARE
  existing_job_id integer;
BEGIN
  BEGIN
    SELECT jobid
    INTO existing_job_id
    FROM cron.job
    WHERE jobname = 'prob-backfill-sql';

    IF existing_job_id IS NOT NULL THEN
      PERFORM cron.unschedule(existing_job_id);
    END IF;

    PERFORM cron.schedule(
      'prob-backfill-sql',
      '* * * * *',
      $cron$select public._prob_backfill_tick();$cron$
    );
  EXCEPTION WHEN undefined_table THEN
    NULL;
  END;
END $$;

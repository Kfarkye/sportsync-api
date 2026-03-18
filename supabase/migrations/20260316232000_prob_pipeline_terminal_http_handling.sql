-- Treat terminal 400/404 probability responses as done (no-data), not error.

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

    IF resp IS NULL OR resp.status_code IS NULL THEN
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

    IF resp.status_code = 400 OR resp.status_code = 404 THEN
      UPDATE public._prob_backfill_queue
      SET status = 'done',
          entries_inserted = 0,
          error_msg = 'HTTP ' || resp.status_code,
          completed_at = now()
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

UPDATE public._prob_backfill_queue
SET status = 'done',
    entries_inserted = COALESCE(entries_inserted, 0),
    completed_at = COALESCE(completed_at, now())
WHERE status = 'error'
  AND error_msg IN ('HTTP 400', 'HTTP 404');

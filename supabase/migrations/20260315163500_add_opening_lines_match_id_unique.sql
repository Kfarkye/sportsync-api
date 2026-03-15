DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'opening_lines_match_id_key'
      AND conrelid = 'public.opening_lines'::regclass
  ) THEN
    ALTER TABLE public.opening_lines
      ADD CONSTRAINT opening_lines_match_id_key UNIQUE (match_id);
  END IF;
END
$$;

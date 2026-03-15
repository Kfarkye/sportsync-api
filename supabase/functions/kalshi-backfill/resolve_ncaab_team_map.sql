-- NCAAB Kalshi team mapping resolver
-- Purpose: produce reviewable mapping tiers (confident/review/unresolved) without mutating kalshi_team_map.
-- Run in Supabase SQL editor or psql. Outputs are select result sets only.

create extension if not exists pg_trgm;

drop table if exists ncaab_unmatched_names;
create temporary table ncaab_unmatched_names as
select
  ks.team_name as kalshi_name,
  count(*)::int as event_count
from kalshi_settlements ks
left join kalshi_team_map ktm
  on lower(ktm.league) = 'ncaab'
 and lower(ktm.kalshi_name) = lower(ks.team_name)
where lower(ks.league) = 'ncaab'
  and ktm.kalshi_name is null
group by ks.team_name;

drop table if exists ncaab_espn_names;
create temporary table ncaab_espn_names as
select distinct team_name as espn_name
from (
  select home_team as team_name
  from matches
  where league_id = 'mens-college-basketball'
  union
  select away_team as team_name
  from matches
  where league_id = 'mens-college-basketball'
) teams
where team_name is not null
  and trim(team_name) <> '';

drop table if exists ncaab_norm_unmatched;
create temporary table ncaab_norm_unmatched as
select
  u.kalshi_name,
  u.event_count,
  lower(
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          regexp_replace(u.kalshi_name, '&', ' and ', 'gi'),
                          '\\bst\\.?\\b', 'saint ', 'gi'
                        ),
                        '\\buc\\s+', 'university california ', 'gi'
                      ),
                      '\\bunc\\b', 'north carolina', 'gi'
                    ),
                    '\\busc\\b', 'southern california', 'gi'
                  ),
                  '\\bstate\\b', 'state', 'gi'
                ),
                '[^a-zA-Z0-9 ]+', ' ', 'g'
              ),
              '\\s+', ' ', 'g'
            ),
            '^the\\s+', '', 'gi'
          ),
          '\\buniversity\\b', '', 'gi'
        ),
        '\\s+', ' ', 'g'
      )
    )
  ) as norm_name
from ncaab_unmatched_names u;

drop table if exists ncaab_norm_espn;
create temporary table ncaab_norm_espn as
select
  e.espn_name,
  lower(
    trim(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              regexp_replace(
                regexp_replace(
                  regexp_replace(
                    regexp_replace(
                      regexp_replace(
                        regexp_replace(
                          regexp_replace(e.espn_name, '&', ' and ', 'gi'),
                          '\\bst\\.?\\b', 'saint ', 'gi'
                        ),
                        '\\buc\\s+', 'university california ', 'gi'
                      ),
                      '\\bunc\\b', 'north carolina', 'gi'
                    ),
                    '\\busc\\b', 'southern california', 'gi'
                  ),
                  '\\bstate\\b', 'state', 'gi'
                ),
                '[^a-zA-Z0-9 ]+', ' ', 'g'
              ),
              '\\s+', ' ', 'g'
            ),
            '^the\\s+', '', 'gi'
          ),
          '\\buniversity\\b', '', 'gi'
        ),
        '\\s+', ' ', 'g'
      )
    )
  ) as norm_name
from ncaab_espn_names e;

drop table if exists ncaab_exact_matches;
create temporary table ncaab_exact_matches as
select
  u.kalshi_name,
  e.espn_name,
  u.event_count,
  1.0::numeric as similarity,
  'exact_normalized'::text as match_method
from ncaab_norm_unmatched u
join ncaab_norm_espn e
  on u.norm_name = e.norm_name;

drop table if exists ncaab_remaining;
create temporary table ncaab_remaining as
select u.*
from ncaab_norm_unmatched u
left join ncaab_exact_matches em
  on em.kalshi_name = u.kalshi_name
where em.kalshi_name is null;

drop table if exists ncaab_fuzzy_ranked;
create temporary table ncaab_fuzzy_ranked as
select
  r.kalshi_name,
  e.espn_name,
  r.event_count,
  similarity(r.norm_name, e.norm_name)::numeric(6,4) as similarity,
  row_number() over (
    partition by r.kalshi_name
    order by similarity(r.norm_name, e.norm_name) desc, e.espn_name asc
  ) as rn
from ncaab_remaining r
join ncaab_norm_espn e
  on similarity(r.norm_name, e.norm_name) >= 0.4;

drop table if exists ncaab_best_fuzzy;
create temporary table ncaab_best_fuzzy as
select
  kalshi_name,
  espn_name,
  event_count,
  similarity,
  'fuzzy_pg_trgm'::text as match_method
from ncaab_fuzzy_ranked
where rn = 1;

drop table if exists ncaab_confident_matches;
create temporary table ncaab_confident_matches as
select *
from ncaab_exact_matches
union all
select *
from ncaab_best_fuzzy
where similarity >= 0.7;

drop table if exists ncaab_review_matches;
create temporary table ncaab_review_matches as
select *
from ncaab_best_fuzzy
where similarity >= 0.4
  and similarity < 0.7;

drop table if exists ncaab_unresolved;
create temporary table ncaab_unresolved as
select
  r.kalshi_name,
  r.event_count
from ncaab_remaining r
left join ncaab_best_fuzzy bf
  on bf.kalshi_name = r.kalshi_name
where bf.kalshi_name is null;

-- Output 1: confident inserts (ready to run after review)
select
  format(
    'insert into kalshi_team_map (kalshi_name, espn_name, league, kalshi_abbrev, espn_team_id) values (%L, %L, %L, %L, %s);',
    kalshi_name,
    espn_name,
    'ncaab',
    '',
    'null'
  ) as insert_sql,
  similarity,
  event_count,
  match_method
from ncaab_confident_matches
order by event_count desc, similarity desc, kalshi_name;

-- Output 2: review inserts (commented out with similarity)
select
  format(
    '-- sim=%s events=%s method=%s\n-- insert into kalshi_team_map (kalshi_name, espn_name, league, kalshi_abbrev, espn_team_id) values (%L, %L, %L, %L, %s);',
    to_char(similarity, 'FM0.0000'),
    event_count,
    match_method,
    kalshi_name,
    espn_name,
    'ncaab',
    '',
    'null'
  ) as review_sql
from ncaab_review_matches
order by event_count desc, similarity desc, kalshi_name;

-- Output 3: unresolved names (CSV-friendly)
select
  kalshi_name,
  event_count
from ncaab_unresolved
order by event_count desc, kalshi_name;

-- Validation
select 'confident' as tier, count(*) from ncaab_confident_matches
union all
select 'review', count(*) from ncaab_review_matches
union all
select 'unresolved', count(*) from ncaab_unresolved;

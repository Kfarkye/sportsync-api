import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4'

declare const Deno: any;

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ESPN Stats Drain v4
// v4: Correct gamelog parsing — metadata in top-level events,
//     stats in seasonTypes[].categories[].events[], joined by eventId.
//     Also: error isolation so one 500 doesn't kill the drain.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-internal-secret',
};
const INTERNAL_JOB_SECRET = (Deno.env.get('INTERNAL_JOB_SECRET') ?? '').trim();

function readRequestSecret(req: Request): string {
  const headerSecret = req.headers.get('x-internal-secret')?.trim();
  if (headerSecret) return headerSecret;

  const authHeader = req.headers.get('authorization') ?? '';
  if (authHeader.toLowerCase().startsWith('bearer ')) {
    return authHeader.slice(7).trim();
  }

  return '';
}

const L = {
  info: (e: string, d: Record<string, any> = {}) => console.log(JSON.stringify({ level: 'INFO', ts: new Date().toISOString(), fn: 'espn-stats-drain', event: e, ...d })),
  warn: (e: string, d: Record<string, any> = {}) => console.warn(JSON.stringify({ level: 'WARN', ts: new Date().toISOString(), fn: 'espn-stats-drain', event: e, ...d })),
  error: (e: string, d: Record<string, any> = {}) => console.error(JSON.stringify({ level: 'ERROR', ts: new Date().toISOString(), fn: 'espn-stats-drain', event: e, ...d })),
};

interface LeagueDef {
  sport: string;
  espnSlug: string;
  suffix: string;
  coreLeague: string;
  season: number;
}

const LEAGUES: Record<string, LeagueDef> = {
  nba:        { sport: 'basketball', espnSlug: 'nba',                      suffix: '_nba',        coreLeague: 'nba',                      season: 2026 },
  nfl:        { sport: 'football',   espnSlug: 'nfl',                      suffix: '_nfl',        coreLeague: 'nfl',                      season: 2025 },
  nhl:        { sport: 'hockey',     espnSlug: 'nhl',                      suffix: '_nhl',        coreLeague: 'nhl',                      season: 2026 },
  mlb:        { sport: 'baseball',   espnSlug: 'mlb',                      suffix: '_mlb',        coreLeague: 'mlb',                      season: 2025 },
  ncaaf:      { sport: 'football',   espnSlug: 'college-football',         suffix: '_ncaaf',      coreLeague: 'college-football',         season: 2025 },
  ncaab:      { sport: 'basketball', espnSlug: 'mens-college-basketball',  suffix: '_ncaab',      coreLeague: 'mens-college-basketball',  season: 2026 },
  epl:        { sport: 'soccer',     espnSlug: 'eng.1',                    suffix: '_epl',        coreLeague: 'eng.1',                    season: 2025 },
  laliga:     { sport: 'soccer',     espnSlug: 'esp.1',                    suffix: '_laliga',     coreLeague: 'esp.1',                    season: 2025 },
  seriea:     { sport: 'soccer',     espnSlug: 'ita.1',                    suffix: '_seriea',     coreLeague: 'ita.1',                    season: 2025 },
  bundesliga: { sport: 'soccer',     espnSlug: 'ger.1',                    suffix: '_bundesliga', coreLeague: 'ger.1',                    season: 2025 },
  ligue1:     { sport: 'soccer',     espnSlug: 'fra.1',                    suffix: '_ligue1',     coreLeague: 'fra.1',                    season: 2025 },
  mls:        { sport: 'soccer',     espnSlug: 'usa.1',                    suffix: '_mls',        coreLeague: 'usa.1',                    season: 2025 },
  ucl:        { sport: 'soccer',     espnSlug: 'uefa.champions',           suffix: '_ucl',        coreLeague: 'uefa.champions',           season: 2025 },
  fifawc:     { sport: 'soccer',     espnSlug: 'fifa.world',               suffix: '_fifawc',     coreLeague: 'fifa.world',               season: 2026 },
  'ned.1':    { sport: 'soccer',     espnSlug: 'ned.1',                    suffix: '_ned.1',      coreLeague: 'ned.1',                    season: 2025 },
  'por.1':    { sport: 'soccer',     espnSlug: 'por.1',                    suffix: '_por.1',      coreLeague: 'por.1',                    season: 2025 },
  'bel.1':    { sport: 'soccer',     espnSlug: 'bel.1',                    suffix: '_bel.1',      coreLeague: 'bel.1',                    season: 2025 },
  'tur.1':    { sport: 'soccer',     espnSlug: 'tur.1',                    suffix: '_tur.1',      coreLeague: 'tur.1',                    season: 2025 },
  'bra.1':    { sport: 'soccer',     espnSlug: 'bra.1',                    suffix: '_bra.1',      coreLeague: 'bra.1',                    season: 2025 },
  'arg.1':    { sport: 'soccer',     espnSlug: 'arg.1',                    suffix: '_arg.1',      coreLeague: 'arg.1',                    season: 2025 },
  'sco.1':    { sport: 'soccer',     espnSlug: 'sco.1',                    suffix: '_sco.1',      coreLeague: 'sco.1',                    season: 2025 },
};

const ESPN_SITE = 'https://site.api.espn.com/apis/site/v2/sports';
const ESPN_WEB  = 'https://site.web.api.espn.com/apis/common/v3/sports';
const ESPN_CORE = 'https://sports.core.api.espn.com/v2/sports';
const ESPN_LEADERS = 'https://site.api.espn.com/apis/site/v3/sports';
const FETCH_TIMEOUT = 15000;
const MAX_CONCURRENT = 3;
const INTER_BATCH_DELAY_MS = 300;

async function safeFetch(url: string, label: string): Promise<{ data: any; ok: boolean; error?: string }> {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) return { data: null, ok: false, error: `${label}: HTTP ${res.status}` };
    const data = await res.json();
    return { data, ok: true };
  } catch (e: any) {
    return { data: null, ok: false, error: `${label}: ${e.message}` };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function parallelLimit<T>(tasks: (() => Promise<T>)[], limit: number, delayMs = INTER_BATCH_DELAY_MS): Promise<T[]> {
  const results: T[] = [];
  for (let i = 0; i < tasks.length; i += limit) {
    const batch = tasks.slice(i, i + limit);
    const batchResults = await Promise.all(batch.map(fn => fn()));
    for (let j = 0; j < batchResults.length; j++) {
      results[i + j] = batchResults[j];
    }
    if (i + limit < tasks.length && delayMs > 0) {
      await sleep(delayMs);
    }
  }
  return results;
}

async function batchUpsert(supabase: any, table: string, rows: any[], conflict: string, batchSize = 25): Promise<{ upserted: number; errors: string[] }> {
  let upserted = 0;
  const errors: string[] = [];
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await supabase.from(table).upsert(batch, { onConflict: conflict });
    if (error) {
      errors.push(`${table} batch ${Math.floor(i / batchSize)}: ${error.message}`);
    } else {
      upserted += batch.length;
    }
  }
  return { upserted, errors };
}

function extractEspnTeamId(teamId: string, league: LeagueDef, leagueKey: string): string {
  const suffixPattern = new RegExp(`(${league.suffix}|_${leagueKey}|_${league.espnSlug.replace(/\./g, '\\.')})$`);
  return teamId.replace(suffixPattern, '');
}

function deduplicateTeams(teams: any[], league: LeagueDef, leagueKey: string): any[] {
  const seen = new Map<string, any>();
  for (const team of teams) {
    const espnId = extractEspnTeamId(team.id, league, leagueKey);
    const existing = seen.get(espnId);
    if (!existing || (team.id.includes('_') && !existing.id.includes('_'))) {
      seen.set(espnId, { ...team, _espnNumericId: espnId });
    }
  }
  return Array.from(seen.values());
}

// ━━━ DRAIN: League Leaders ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function drainLeaders(supabase: any, leagueKey: string, league: LeagueDef): Promise<{ rows: any[]; errors: string[] }> {
  const errors: string[] = [];
  const rows: any[] = [];

  const url = `${ESPN_LEADERS}/${league.sport}/${league.espnSlug}/leaders`;
  let result = await safeFetch(url, 'leaders-v3');

  if (!result.ok) {
    const coreUrl = `${ESPN_CORE}/${league.sport}/leagues/${league.coreLeague}/seasons/${league.season}/types/2/leaders`;
    result = await safeFetch(coreUrl, 'leaders-core');
    if (!result.ok) {
      errors.push(result.error || 'Leaders fetch failed');
      return { rows, errors };
    }
  }

  const data = result.data;
  let categories: any[] = [];
  if (Array.isArray(data?.leaders)) categories = data.leaders;
  else if (data?.leaders?.categories && Array.isArray(data.leaders.categories)) categories = data.leaders.categories;
  else if (Array.isArray(data?.categories)) categories = data.categories;

  for (const cat of categories) {
    const categoryName = cat.name || cat.abbreviation || 'unknown';
    const categoryDisplay = cat.displayName || cat.shortDisplayName || categoryName;
    const leaders = cat.leaders || cat.entries || [];

    for (let rank = 0; rank < Math.min(leaders.length, 25); rank++) {
      const leader = leaders[rank];
      const athlete = leader.athlete || leader.leaders?.[0]?.athlete || {};
      const team = athlete.team || leader.team || {};
      const statValue = leader.value ?? leader.displayValue ?? leader.leaders?.[0]?.value;
      const statDisplay = leader.displayValue ?? leader.leaders?.[0]?.displayValue ?? String(statValue ?? '');
      if (!athlete.id && !leader.athleteId) continue;

      rows.push({
        id: `${leagueKey}_${categoryName}_${rank + 1}`,
        league_id: leagueKey, sport: league.sport, season: league.season, season_type: 2,
        category: categoryName, category_display: categoryDisplay, rank: rank + 1,
        espn_athlete_id: String(athlete.id || leader.athleteId || ''),
        athlete_name: athlete.displayName || athlete.fullName || leader.displayName || 'Unknown',
        athlete_headshot: athlete.headshot?.href || athlete.headshot || null,
        team_id: team.id ? `${team.id}${league.suffix}` : null,
        team_name: team.displayName || team.name || null,
        team_abbr: team.abbreviation || null,
        stat_value: statValue != null ? parseFloat(String(statValue)) : null,
        stat_display: statDisplay,
        leaders_raw: leader,
        last_drained_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      });
    }
  }
  return { rows, errors };
}

// ━━━ DRAIN: Team Season Stats ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function drainTeamStats(supabase: any, leagueKey: string, league: LeagueDef, includeSeasonQuery = false): Promise<{ rows: any[]; errors: string[] }> {
  const errors: string[] = [];
  const rows: any[] = [];

  const { data: rawTeams, error: teamsErr } = await supabase
    .from('teams').select('id, name, abbreviation, league_id')
    .or(`league_id.eq.${leagueKey},league_id.eq.${league.espnSlug}`);

  if (teamsErr || !rawTeams?.length) {
    errors.push(`No teams for ${leagueKey}: ${teamsErr?.message || 'empty'}`);
    return { rows, errors };
  }

  const teams = deduplicateTeams(rawTeams, league, leagueKey);
  L.info('TEAM_STATS_START', { league: leagueKey, raw: rawTeams.length, deduped: teams.length });

  const tasks = teams.map((team: any) => async () => {
    const eid = team._espnNumericId;
    const seasonQuery = includeSeasonQuery ? `?season=${league.season}` : '';
    const [statsRes, recordRes] = await Promise.all([
      safeFetch(`${ESPN_SITE}/${league.sport}/${league.espnSlug}/teams/${eid}/statistics${seasonQuery}`, `stats-${eid}`),
      safeFetch(`${ESPN_SITE}/${league.sport}/${league.espnSlug}/teams/${eid}/record${seasonQuery}`, `rec-${eid}`),
    ]);

    let wins = 0, losses = 0, draws = 0, winPct = 0;
    const recItems = recordRes.data?.items || recordRes.data?.record?.items || [];
    const overall = recItems.find((i: any) => i.type === 'total' || i.description === 'Overall' || i.id === '0') || recItems[0];
    if (overall?.stats) {
      for (const s of overall.stats) {
        if (s.name === 'wins' || s.name === 'gamesWon') wins = s.value || 0;
        if (s.name === 'losses' || s.name === 'gamesLost') losses = s.value || 0;
        if (s.name === 'ties' || s.name === 'draws' || s.name === 'gamesDrawn') draws = s.value || 0;
        if (s.name === 'winPercent' || s.name === 'winPct') winPct = s.value || 0;
      }
    }

    const offensive: Record<string, any> = {}, defensive: Record<string, any> = {}, misc: Record<string, any> = {}, rankings: Record<string, any> = {};
    const cats = statsRes.data?.results?.stats?.categories || statsRes.data?.stats?.categories ||
      statsRes.data?.statistics?.splits?.categories || statsRes.data?.splits?.categories || [];
    for (const cat of cats) {
      const cn = (cat.name || '').toLowerCase();
      const t = cn.includes('defense') || cn.includes('opponent') ? defensive : cn.includes('misc') || cn.includes('special') ? misc : offensive;
      for (const s of (cat.stats || [])) {
        const k = s.name || s.abbreviation;
        if (!k) continue;
        t[k] = s.displayValue ?? s.value;
        if (s.rank != null) rankings[`${k}_rank`] = s.rank;
      }
    }

    return {
      id: `${team.id}_${league.season}_2`, team_id: team.id, espn_team_id: eid,
      league_id: leagueKey, sport: league.sport, season: league.season, season_type: 2,
      team_name: team.name, team_abbr: team.abbreviation,
      wins, losses, draws, win_pct: winPct || (wins + losses > 0 ? +(wins / (wins + losses)).toFixed(3) : 0),
      offensive_stats: offensive, defensive_stats: defensive, misc_stats: misc, stat_rankings: rankings,
      stats_raw: statsRes.data || {}, record_raw: recordRes.data || {},
      last_drained_at: new Date().toISOString(), updated_at: new Date().toISOString(),
    };
  });

  const results = await parallelLimit(tasks, MAX_CONCURRENT);
  for (const r of results) if (r) rows.push(r);
  return { rows, errors };
}

// ━━━ DRAIN: Athletes ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async function drainAthletes(supabase: any, leagueKey: string, league: LeagueDef): Promise<{ rows: any[]; errors: string[] }> {
  const errors: string[] = [];
  const rows: any[] = [];

  const { data: rawTeams } = await supabase
    .from('teams').select('id, name, league_id')
    .or(`league_id.eq.${leagueKey},league_id.eq.${league.espnSlug}`);

  if (!rawTeams?.length) { errors.push(`No teams for ${leagueKey}`); return { rows, errors }; }

  const teams = deduplicateTeams(rawTeams, league, leagueKey);
  L.info('ATHLETES_START', { league: leagueKey, raw: rawTeams.length, deduped: teams.length });

  const rosterTasks = teams.map((team: any) => async () => {
    const eid = team._espnNumericId;
    const result = await safeFetch(`${ESPN_SITE}/${league.sport}/${league.espnSlug}/teams/${eid}/roster`, `roster-${eid}`);
    return { team, eid, result };
  });

  const rosterResults = await parallelLimit(rosterTasks, MAX_CONCURRENT);

  for (const { team, eid, result } of rosterResults) {
    if (!result.ok) { errors.push(result.error || `Roster failed ${eid}`); continue; }
    const groups = result.data?.athletes || [];
    for (const group of groups) {
      const items = group.items || group.athletes || (group.id ? [group] : []);
      for (const a of items) {
        if (!a.id) continue;
        const pos = a.position || {};
        const inj = (a.injuries || [])[0] || {};
        rows.push({
          id: `${a.id}${league.suffix}`, espn_athlete_id: String(a.id),
          league_id: leagueKey, sport: league.sport, team_id: team.id, team_name: team.name,
          full_name: a.fullName || a.displayName || `${a.firstName || ''} ${a.lastName || ''}`.trim() || 'Unknown',
          first_name: a.firstName || null, last_name: a.lastName || null,
          display_name: a.displayName || null, short_name: a.shortName || null,
          jersey: a.jersey || null, position_name: pos.displayName || null, position_abbr: pos.abbreviation || null,
          height: a.displayHeight || null, weight: a.displayWeight || null,
          age: a.age || null, date_of_birth: a.dateOfBirth || null,
          birth_place: a.birthPlace?.displayText || [a.birthPlace?.city, a.birthPlace?.state, a.birthPlace?.country].filter(Boolean).join(', ') || null,
          headshot_url: a.headshot?.href || null,
          is_active: a.status?.type !== 'inactive', status: a.status?.type || 'active',
          injury_type: inj.type || null, injury_detail: inj.detail?.detail || inj.longComment || null,
          season_stats: a.statistics || {}, overview_raw: a,
          last_drained_at: new Date().toISOString(), updated_at: new Date().toISOString(),
        });
      }
    }
  }
  L.info('ATHLETES_PARSED', { league: leagueKey, total: rows.length });
  return { rows, errors };
}

// ━━━ DRAIN: Game Logs (v4 — correct ESPN structure) ━━━━━━━━━━━━━━
// ESPN gamelog response structure (confirmed via live probe):
// {
//   "labels": ["MIN","FG","FG%","3PT","3P%","FT","FT%","REB","AST","BLK","STL","PF","TO","PTS"],
//   "events": {
//     "401810721": { atVs, gameDate, score, gameResult, opponent: {id, displayName, abbreviation}, ... }
//   },
//   "seasonTypes": [{
//     "displayName": "2025-26 Regular Season",
//     "categories": [{
//       "displayName": "february",
//       "splitType": "february",
//       "events": [
//         { "eventId": "401810721", "stats": ["29","9-17","52.9",...] }
//       ],
//       "totals": [...]
//     }]
//   }]
// }
// Key insight: metadata lives in top-level events{}, stats live in seasonTypes[].categories[].events[]
// They join on eventId.
async function drainGameLogs(supabase: any, leagueKey: string, league: LeagueDef, limit = 5): Promise<{ rows: any[]; errors: string[] }> {
  const errors: string[] = [];
  const rows: any[] = [];

  const { data: athletes } = await supabase
    .from('espn_athletes')
    .select('id, espn_athlete_id, team_id, full_name')
    .eq('league_id', leagueKey)
    .eq('is_active', true)
    .limit(500);

  if (!athletes?.length) {
    errors.push(`No athletes in DB for ${leagueKey}. Run athletes drain first.`);
    return { rows, errors };
  }

  // Pick top N athletes per team
  const teamMap: Record<string, any[]> = {};
  for (const a of athletes) {
    const key = a.team_id || 'unknown';
    if (!teamMap[key]) teamMap[key] = [];
    if (teamMap[key].length < limit) teamMap[key].push(a);
  }
  const selected = Object.values(teamMap).flat();
  L.info('GAME_LOGS_START', { league: leagueKey, athletes: selected.length, teams: Object.keys(teamMap).length });

  const tasks = selected.map((athlete: any) => async () => {
    try {
      const url = `${ESPN_WEB}/${league.sport}/${league.espnSlug}/athletes/${athlete.espn_athlete_id}/gamelog`;
      const result = await safeFetch(url, `gamelog-${athlete.espn_athlete_id}`);

      if (!result.ok) {
        return { rows: [] as any[], error: result.error };
      }

      const data = result.data;
      const logRows: any[] = [];

      // 1. Get stat labels from top level
      const labels: string[] = data?.labels || [];
      if (labels.length === 0) {
        return { rows: [], error: `gamelog-${athlete.espn_athlete_id}: no labels` };
      }

      // 2. Build event metadata lookup from top-level events{}
      const eventMeta: Record<string, any> = {};
      const topEvents = data?.events || {};
      for (const [eventId, meta] of Object.entries(topEvents)) {
        eventMeta[eventId] = meta as any;
      }

      // 3. Extract stats from seasonTypes[].categories[].events[]
      const seasonTypes = data?.seasonTypes || [];
      for (const st of seasonTypes) {
        const categories = st.categories || [];
        for (const cat of categories) {
          const catEvents = cat.events || [];
          for (const ev of catEvents) {
            const eventId = ev.eventId || ev.id;
            if (!eventId) continue;

            const statsArr = ev.stats || [];
            if (!Array.isArray(statsArr) || statsArr.length === 0) continue;

            // Map stats array to labels
            const stats: Record<string, any> = {};
            for (let si = 0; si < statsArr.length && si < labels.length; si++) {
              if (statsArr[si] !== undefined && statsArr[si] !== null && statsArr[si] !== '') {
                stats[labels[si]] = statsArr[si];
              }
            }

            // Get metadata from top-level events
            const meta = eventMeta[String(eventId)] || {};
            const gameDate = meta.gameDate || meta.eventDate || meta.date;
            if (!gameDate) continue; // Skip events with no date

            // Determine home/away
            let homeAway: string | null = null;
            if (meta.homeAway) {
              homeAway = meta.homeAway;
            } else if (typeof meta.atVs === 'string') {
              homeAway = meta.atVs === 'vs' ? 'home' : meta.atVs === '@' ? 'away' : null;
            }

            logRows.push({
              id: `${athlete.espn_athlete_id}_${eventId}${league.suffix}`,
              athlete_id: athlete.id,
              espn_athlete_id: athlete.espn_athlete_id,
              espn_event_id: String(eventId),
              league_id: leagueKey,
              sport: league.sport,
              game_date: gameDate.split('T')[0],
              opponent_id: meta.opponent?.id ? `${meta.opponent.id}${league.suffix}` : null,
              opponent_name: meta.opponent?.displayName || meta.opponent?.abbreviation || null,
              home_away: homeAway,
              result: meta.gameResult || meta.result || null,
              score: meta.score || null,
              stats,
            });
          }
        }
      }

      return { rows: logRows };
    } catch (e: any) {
      // v4: Error isolation — one athlete crash never kills the drain
      return { rows: [] as any[], error: `gamelog-${athlete.espn_athlete_id}: ${e.message}` };
    }
  });

  const results = await parallelLimit(tasks, MAX_CONCURRENT);
  for (const r of results) {
    if (r && 'rows' in r) rows.push(...r.rows);
    if (r && 'error' in r && r.error) errors.push(r.error as string);
  }

  L.info('GAME_LOGS_PARSED', { league: leagueKey, totalGames: rows.length, errors: errors.length });
  return { rows, errors };
}

// ━━━ Main Handler ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });

  if (!INTERNAL_JOB_SECRET) {
    return new Response(JSON.stringify({ error: 'Missing INTERNAL_JOB_SECRET' }), {
      status: 500, headers: { ...CORS, 'Content-Type': 'application/json' },
    });
  }

  if (readRequestSecret(req) !== INTERNAL_JOB_SECRET) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), {
      status: 401, headers: { ...CORS, 'Content-Type': 'application/json' },
    });
  }

  const supabase = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
  );

  const url = new URL(req.url);
  const drainType = url.searchParams.get('type') || 'leaders';
  const leagueParam = url.searchParams.get('league') || 'nba';
  const seasonParam = url.searchParams.get('season');
  const dryRun = url.searchParams.get('dry') === 'true';
  const perTeamLimit = parseInt(url.searchParams.get('per_team') || '5');
  const seasonOverride = seasonParam !== null ? parseInt(seasonParam, 10) : null;

  if (seasonParam !== null && (!Number.isInteger(seasonOverride) || seasonOverride < 1900 || seasonOverride > 3000)) {
    return new Response(JSON.stringify({ error: `Invalid season param: ${seasonParam}` }), {
      status: 400, headers: { ...CORS, 'Content-Type': 'application/json' },
    });
  }

  const leagueKeys = leagueParam.split(',').map(l => l.trim().toLowerCase());
  const drainTypes = drainType === 'all' ? ['leaders', 'team_stats', 'athletes', 'game_logs'] : [drainType];

  const t0 = Date.now();
  const summary: Record<string, any> = { success: true, version: 'v4', dryRun, leagues: leagueKeys, seasonOverride, drains: {} };

  try {
    for (const dtype of drainTypes) {
      const dr: any = { itemsFound: 0, itemsUpserted: 0, errors: [] };

      for (const leagueKey of leagueKeys) {
        const baseLeague = LEAGUES[leagueKey];
        if (!baseLeague) { dr.errors.push(`Unknown league: ${leagueKey}`); continue; }
        const applySeasonOverride = dtype === 'team_stats' && seasonOverride !== null;
        const league: LeagueDef = applySeasonOverride ? { ...baseLeague, season: seasonOverride } : baseLeague;

        L.info('DRAIN_START', { type: dtype, league: leagueKey, version: 'v4', season: league.season, seasonOverride: applySeasonOverride ? seasonOverride : null });

        let result: { rows: any[]; errors: string[] };
        switch (dtype) {
          case 'leaders': result = await drainLeaders(supabase, leagueKey, league); break;
          case 'team_stats': result = await drainTeamStats(supabase, leagueKey, league, applySeasonOverride); break;
          case 'athletes': result = await drainAthletes(supabase, leagueKey, league); break;
          case 'game_logs': result = await drainGameLogs(supabase, leagueKey, league, perTeamLimit); break;
          default: result = { rows: [], errors: [`Unknown drain type: ${dtype}`] };
        }

        dr.itemsFound += result.rows.length;
        dr.errors.push(...result.errors);

        if (!dryRun && result.rows.length > 0) {
          const table = dtype === 'leaders' ? 'espn_league_leaders'
            : dtype === 'team_stats' ? 'espn_team_season_stats'
            : dtype === 'athletes' ? 'espn_athletes' : 'espn_game_logs';
          const { upserted, errors: ue } = await batchUpsert(supabase, table, result.rows, 'id');
          dr.itemsUpserted += upserted;
          dr.errors.push(...ue);
        }
      }

      if (!dryRun) {
        const drainLogErrors = dtype === 'team_stats' && seasonOverride !== null
          ? [`INFO season_override=${seasonOverride}`, ...dr.errors]
          : dr.errors;
        await supabase.from('espn_stats_drain_log').insert({
          drain_type: dtype, leagues_queried: leagueKeys,
          items_found: dr.itemsFound, items_upserted: dr.itemsUpserted,
          errors: drainLogErrors.slice(0, 50), duration_ms: Date.now() - t0,
          drain_version: 'v4',
          status: dr.errors.length === 0 ? 'success' : dr.itemsUpserted > 0 ? 'partial' : 'failure',
        });
      }

      summary.drains[dtype] = {
        itemsFound: dr.itemsFound, itemsUpserted: dr.itemsUpserted,
        errorsCount: dr.errors.length, errors: dr.errors.slice(0, 10),
      };
    }

    summary.durationMs = Date.now() - t0;
    return new Response(JSON.stringify(summary, null, 2), {
      headers: { ...CORS, 'Content-Type': 'application/json' },
    });
  } catch (err: any) {
    L.error('FATAL', { error: err.message, stack: err.stack?.substring(0, 500) });
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500, headers: { ...CORS, 'Content-Type': 'application/json' },
    });
  }
});

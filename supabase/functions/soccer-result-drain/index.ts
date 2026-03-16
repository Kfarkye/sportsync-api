import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4'

declare const Deno: any;

// ═══════════════════════════════════════════════════════════════════════════
// soccer-result-drain
// Extracts rich in-game data from completed soccer matches into
// soccer_match_result for structured frontend consumption.
//
// Data extracted per match:
//   • 28 team-level stats (possession, shots, passes, tackles, etc.)
//   • Key events timeline (goals, cards, subs with minute + player)
//   • Full lineups with individual player stats (14 stats per player)
//   • Commentary (play-by-play text, 80-120 entries per match)
//   • Match context (venue, attendance, referee)
//   • Derived signals (possession delta, shot dominance, xG proxy)
//
// Usage:
//   GET /soccer-result-drain?days=7&league=epl,laliga&backfill=true
//   GET /soccer-result-drain?date=2026-02-22&league=epl
// ═══════════════════════════════════════════════════════════════════════════

const CORS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// ─── League Configuration ────────────────────────────────────────────────
interface LeagueConfig {
  espn: string;    // ESPN league slug
  short: string;   // Our short ID
  label: string;   // Display name
}

const LEAGUES: Record<string, LeagueConfig> = {
  epl:        { espn: 'eng.1',           short: 'epl',        label: 'Premier League' },
  laliga:     { espn: 'esp.1',           short: 'laliga',     label: 'La Liga' },
  seriea:     { espn: 'ita.1',           short: 'seriea',     label: 'Serie A' },
  bundesliga: { espn: 'ger.1',           short: 'bundesliga', label: 'Bundesliga' },
  ligue1:     { espn: 'fra.1',           short: 'ligue1',     label: 'Ligue 1' },
  mls:        { espn: 'usa.1',           short: 'mls',        label: 'MLS' },
  ucl:        { espn: 'uefa.champions',  short: 'ucl',        label: 'Champions League' },
  uel:        { espn: 'uefa.europa',     short: 'uel',        label: 'Europa League' },
  'arg.1':    { espn: 'arg.1',           short: 'arg.1',      label: 'Argentina Primera' },
  'bra.1':    { espn: 'bra.1',           short: 'bra.1',      label: 'Brasileirao' },
  'ned.1':    { espn: 'ned.1',           short: 'ned.1',      label: 'Eredivisie' },
  'tur.1':    { espn: 'tur.1',           short: 'tur.1',      label: 'Super Lig' },
  'por.1':    { espn: 'por.1',           short: 'por.1',      label: 'Primeira Liga' },
  'bel.1':    { espn: 'bel.1',           short: 'bel.1',      label: 'Belgian Pro League' },
  'sco.1':    { espn: 'sco.1',           short: 'sco.1',      label: 'Scottish Premiership' },
};

const DEFAULT_LEAGUES =
  'epl,laliga,seriea,bundesliga,ligue1,mls,ucl,uel,arg.1,bra.1,ned.1,tur.1,por.1,bel.1,sco.1';

// ─── Rate Limiting ───────────────────────────────────────────────────────
const MAX_CONCURRENT = 3;
const INTER_BATCH_DELAY_MS = 500;
const FETCH_TIMEOUT = 18000;

// ─── Helpers ─────────────────────────────────────────────────────────────
function dateStr(d: Date): string {
  return d.toISOString().slice(0, 10).replace(/-/g, '');
}

function safeNum(v: any): number | null {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return isNaN(n) ? null : n;
}

function safeInt(v: any): number | null {
  const n = safeNum(v);
  return n !== null ? Math.round(n) : null;
}

function pctToDecimal(v: any): number | null {
  const n = safeNum(v);
  if (n === null) return null;
  // ESPN returns 0.7 for 70%, or 39.9 for possession
  return n > 1 ? n : n * 100;
}

async function fetchWithTimeout(url: string, ms: number, attempts = 4): Promise<any> {
  let lastError: any = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), ms);
    try {
      const res = await fetch(url, { signal: controller.signal });
      if (!res.ok) {
        if ((res.status === 429 || res.status === 503) && attempt < attempts) {
          const retryAfter = Number(res.headers.get('retry-after'));
          const waitMs = Number.isFinite(retryAfter) && retryAfter > 0
            ? retryAfter * 1000
            : Math.min(8000, 500 * Math.pow(2, attempt - 1));
          await new Promise((resolve) => setTimeout(resolve, waitMs));
          continue;
        }
        throw new Error(`HTTP ${res.status}`);
      }
      return await res.json();
    } catch (error: any) {
      lastError = error;
      if (attempt >= attempts) throw error;
      const waitMs = Math.min(8000, 500 * Math.pow(2, attempt - 1));
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError ?? new Error('ESPN fetch failed');
}

async function runBatch<T, R>(items: T[], concurrency: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.allSettled(batch.map(fn));
    for (const r of batchResults) {
      if (r.status === 'fulfilled') results.push(r.value);
    }
    if (i + concurrency < items.length) {
      await new Promise(r => setTimeout(r, INTER_BATCH_DELAY_MS));
    }
  }
  return results;
}

// ─── Stat Extraction ─────────────────────────────────────────────────────
function extractTeamStat(stats: any[], name: string): string | null {
  const s = stats?.find((st: any) => st.name === name);
  return s?.displayValue ?? null;
}

function extractTeamStats(teamData: any): Record<string, any> {
  const stats = teamData?.statistics || [];
  return {
    possession:        safeNum(extractTeamStat(stats, 'possessionPct')),
    shots:             safeInt(extractTeamStat(stats, 'totalShots')),
    shots_on_target:   safeInt(extractTeamStat(stats, 'shotsOnTarget')),
    shot_accuracy:     safeNum(extractTeamStat(stats, 'shotPct')),
    corners:           safeInt(extractTeamStat(stats, 'wonCorners')),
    fouls:             safeInt(extractTeamStat(stats, 'foulsCommitted')),
    yellow_cards:      safeInt(extractTeamStat(stats, 'yellowCards')),
    red_cards:         safeInt(extractTeamStat(stats, 'redCards')),
    offsides:          safeInt(extractTeamStat(stats, 'offsides')),
    saves:             safeInt(extractTeamStat(stats, 'saves')),
    passes:            safeInt(extractTeamStat(stats, 'totalPasses')),
    accurate_passes:   safeInt(extractTeamStat(stats, 'accuratePasses')),
    pass_pct:          safeNum(extractTeamStat(stats, 'passPct')),
    crosses:           safeInt(extractTeamStat(stats, 'totalCrosses')),
    accurate_crosses:  safeInt(extractTeamStat(stats, 'accurateCrosses')),
    cross_pct:         safeNum(extractTeamStat(stats, 'crossPct')),
    long_balls:        safeInt(extractTeamStat(stats, 'totalLongBalls')),
    accurate_long_balls: safeInt(extractTeamStat(stats, 'accurateLongBalls')),
    long_ball_pct:     safeNum(extractTeamStat(stats, 'longballPct')),
    tackles:           safeInt(extractTeamStat(stats, 'totalTackles')),
    effective_tackles: safeInt(extractTeamStat(stats, 'effectiveTackles')),
    tackle_pct:        safeNum(extractTeamStat(stats, 'tacklePct')),
    interceptions:     safeInt(extractTeamStat(stats, 'interceptions')),
    clearances:        safeInt(extractTeamStat(stats, 'totalClearance')),
    effective_clearances: safeInt(extractTeamStat(stats, 'effectiveClearance')),
    blocked_shots:     safeInt(extractTeamStat(stats, 'blockedShots')),
    penalty_goals:     safeInt(extractTeamStat(stats, 'penaltyKickGoals')),
    penalty_attempts:  safeInt(extractTeamStat(stats, 'penaltyKickShots')),
  };
}

// ─── Key Events Extraction ───────────────────────────────────────────────
function extractKeyEvents(data: any): any[] {
  const events = data.keyEvents || [];
  return events.map((ev: any) => {
    const type = ev.type?.text || 'Unknown';
    const isGoal = type.toLowerCase().includes('goal') || ev.scoringPlay === true;
    const isCard = type.toLowerCase().includes('card');
    const isSub = type.toLowerCase().includes('substitution');
    return {
      minute: ev.clock?.displayValue || null,
      type,
      team: ev.team?.displayName || null,
      player: ev.participants?.[0]?.athlete?.displayName || null,
      assist: ev.participants?.[1]?.athlete?.displayName || null,
      description: ev.text || null,
      is_goal: isGoal,
      is_card: isCard,
      is_sub: isSub,
      penalty: type.toLowerCase().includes('penalty') || false,
      own_goal: type.toLowerCase().includes('own goal') || false,
      goal_type: isGoal ? type.replace('Goal - ', '').replace('Goal', 'Normal') : null,
    };
  });
}

// ─── Lineup Extraction ───────────────────────────────────────────────────
function extractLineup(rosterData: any): any[] {
  const players = rosterData?.roster || [];
  return players.map((p: any) => {
    const stats = p.stats || [];
    const getStat = (name: string) => {
      const s = stats.find((st: any) => st.name === name);
      return s ? safeInt(s.displayValue) : 0;
    };
    return {
      player: p.athlete?.displayName || 'Unknown',
      player_id: p.athlete?.id || null,
      position: p.position?.displayName || null,
      jersey: p.jersey || null,
      starter: p.starter || false,
      subbed_in: p.subbedIn || false,
      subbed_out: p.subbedOut || false,
      stats: {
        goals: getStat('totalGoals'),
        assists: getStat('goalAssists'),
        shots: getStat('totalShots'),
        shots_on_target: getStat('shotsOnTarget'),
        saves: getStat('saves'),
        goals_conceded: getStat('goalsConceded'),
        fouls: getStat('foulsCommitted'),
        fouls_suffered: getStat('foulsSuffered'),
        yellow_cards: getStat('yellowCards'),
        red_cards: getStat('redCards'),
        own_goals: getStat('ownGoals'),
      },
    };
  });
}

// ─── Commentary Extraction ───────────────────────────────────────────────
function extractCommentary(data: any): any[] {
  const commentary = data.commentary || [];
  if (!Array.isArray(commentary)) return [];
  return commentary.map((c: any) => ({
    minute: c.time?.displayValue || null,
    text: c.text || null,
  })).filter((c: any) => c.text);
}

// ─── Full Match Parsing ──────────────────────────────────────────────────
function parseMatchResult(data: any, eventId: string, leagueShort: string): any {
  const boxTeams = data.boxscore?.teams || [];
  const homeIdx = boxTeams.findIndex((t: any) => t.homeAway === 'home');
  const awayIdx = boxTeams.findIndex((t: any) => t.homeAway === 'away');
  
  // Fallback if homeAway not set
  const hIdx = homeIdx >= 0 ? homeIdx : 0;
  const aIdx = awayIdx >= 0 ? awayIdx : 1;
  
  const homeTeam = boxTeams[hIdx]?.team || {};
  const awayTeam = boxTeams[aIdx]?.team || {};
  const homeStats = extractTeamStats(boxTeams[hIdx]);
  const awayStats = extractTeamStats(boxTeams[aIdx]);

  // Score from header
  const competitors = data.header?.competitions?.[0]?.competitors || [];
  const homeSide = competitors.find((c: any) => c.homeAway === 'home');
  const awaySide = competitors.find((c: any) => c.homeAway === 'away');
  const homeScore = safeInt(homeSide?.score) ?? safeInt(competitors[0]?.score);
  const awayScore = safeInt(awaySide?.score) ?? safeInt(competitors[1]?.score);

  // Status
  const matchStatus = data.header?.competitions?.[0]?.status?.type?.name || 'STATUS_FINAL';

  // Game info
  const gameInfo = data.gameInfo || {};
  const venue = gameInfo.venue?.fullName || null;
  const venueCity = gameInfo.venue?.address?.city || null;
  const attendance = safeInt(gameInfo.attendance);
  const referee = gameInfo.officials?.[0]?.fullName || null;

  // Start time from header
  const startTime = data.header?.competitions?.[0]?.date || null;

  // Match ID derivation
  const matchId = `${eventId}_${leagueShort}`;
  
  // Rosters
  const rosters = data.rosters || [];
  const homeRoster = rosters.find((r: any) => r.homeAway === 'home') || rosters[0];
  const awayRoster = rosters.find((r: any) => r.homeAway === 'away') || rosters[1];

  // Derived signals
  const possessionDelta = (homeStats.possession !== null && awayStats.possession !== null)
    ? homeStats.possession - awayStats.possession : null;
  const shotDominance = (homeStats.shots && awayStats.shots)
    ? Math.round((homeStats.shots / awayStats.shots) * 100) / 100 : null;
  const homeXgProxy = (homeStats.shots_on_target !== null && homeStats.shot_accuracy !== null)
    ? Math.round(homeStats.shots_on_target * (homeStats.shot_accuracy > 1 ? homeStats.shot_accuracy / 100 : homeStats.shot_accuracy) * 100) / 100
    : null;
  const awayXgProxy = (awayStats.shots_on_target !== null && awayStats.shot_accuracy !== null)
    ? Math.round(awayStats.shots_on_target * (awayStats.shot_accuracy > 1 ? awayStats.shot_accuracy / 100 : awayStats.shot_accuracy) * 100) / 100
    : null;

  return {
    id: matchId,
    match_id: matchId, // FK attempt — may not match if matches table uses different ID
    espn_event_id: eventId,
    league_id: leagueShort,
    start_time: startTime,
    home_team_id: homeTeam.id || null,
    home_team_name: homeTeam.displayName || homeSide?.team?.displayName || null,
    home_team_abbr: homeTeam.abbreviation || null,
    home_team_logo: homeTeam.logo || null,
    away_team_id: awayTeam.id || null,
    away_team_name: awayTeam.displayName || awaySide?.team?.displayName || null,
    away_team_abbr: awayTeam.abbreviation || null,
    away_team_logo: awayTeam.logo || null,
    home_score: homeScore,
    away_score: awayScore,
    match_status: matchStatus,
    venue,
    venue_city: venueCity,
    attendance,
    referee,
    // Home stats
    home_possession: homeStats.possession,
    home_shots: homeStats.shots,
    home_shots_on_target: homeStats.shots_on_target,
    home_shot_accuracy: homeStats.shot_accuracy,
    home_corners: homeStats.corners,
    home_fouls: homeStats.fouls,
    home_yellow_cards: homeStats.yellow_cards,
    home_red_cards: homeStats.red_cards,
    home_offsides: homeStats.offsides,
    home_saves: homeStats.saves,
    home_passes: homeStats.passes,
    home_accurate_passes: homeStats.accurate_passes,
    home_pass_pct: homeStats.pass_pct,
    home_crosses: homeStats.crosses,
    home_accurate_crosses: homeStats.accurate_crosses,
    home_cross_pct: homeStats.cross_pct,
    home_long_balls: homeStats.long_balls,
    home_accurate_long_balls: homeStats.accurate_long_balls,
    home_long_ball_pct: homeStats.long_ball_pct,
    home_tackles: homeStats.tackles,
    home_effective_tackles: homeStats.effective_tackles,
    home_tackle_pct: homeStats.tackle_pct,
    home_interceptions: homeStats.interceptions,
    home_clearances: homeStats.clearances,
    home_effective_clearances: homeStats.effective_clearances,
    home_blocked_shots: homeStats.blocked_shots,
    home_penalty_goals: homeStats.penalty_goals,
    home_penalty_attempts: homeStats.penalty_attempts,
    // Away stats
    away_possession: awayStats.possession,
    away_shots: awayStats.shots,
    away_shots_on_target: awayStats.shots_on_target,
    away_shot_accuracy: awayStats.shot_accuracy,
    away_corners: awayStats.corners,
    away_fouls: awayStats.fouls,
    away_yellow_cards: awayStats.yellow_cards,
    away_red_cards: awayStats.red_cards,
    away_offsides: awayStats.offsides,
    away_saves: awayStats.saves,
    away_passes: awayStats.passes,
    away_accurate_passes: awayStats.accurate_passes,
    away_pass_pct: awayStats.pass_pct,
    away_crosses: awayStats.crosses,
    away_accurate_crosses: awayStats.accurate_crosses,
    away_cross_pct: awayStats.cross_pct,
    away_long_balls: awayStats.long_balls,
    away_accurate_long_balls: awayStats.accurate_long_balls,
    away_long_ball_pct: awayStats.long_ball_pct,
    away_tackles: awayStats.tackles,
    away_effective_tackles: awayStats.effective_tackles,
    away_tackle_pct: awayStats.tackle_pct,
    away_interceptions: awayStats.interceptions,
    away_clearances: awayStats.clearances,
    away_effective_clearances: awayStats.effective_clearances,
    away_blocked_shots: awayStats.blocked_shots,
    away_penalty_goals: awayStats.penalty_goals,
    away_penalty_attempts: awayStats.penalty_attempts,
    // JSONB
    key_events: extractKeyEvents(data),
    home_lineup: extractLineup(homeRoster),
    away_lineup: extractLineup(awayRoster),
    commentary: extractCommentary(data),
    // Derived
    home_xg_proxy: homeXgProxy,
    away_xg_proxy: awayXgProxy,
    possession_delta: possessionDelta,
    shot_dominance: shotDominance,
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// Main Handler
// ═══════════════════════════════════════════════════════════════════════════
Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });

  const t0 = Date.now();
  const url = new URL(req.url);
  const supabase = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!,
  );

  // ─── Parameters ──────────────────────────────────────────────
  const leagueParam = url.searchParams.get('league') || DEFAULT_LEAGUES;
  const requestedLeagues = leagueParam.split(',').map(l => l.trim().toLowerCase());
  const daysBack = parseInt(url.searchParams.get('days') || '7', 10);
  const specificDate = url.searchParams.get('date');  // YYYY-MM-DD
  const dry = url.searchParams.get('dry') === 'true';
  const forceRefresh = url.searchParams.get('force') === 'true';

  const errors: string[] = [];
  const results: Record<string, any> = {};
  let totalDrained = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  // ─── Build date range ────────────────────────────────────────
  const dates: string[] = [];
  if (specificDate) {
    dates.push(specificDate.replace(/-/g, ''));
  } else {
    const now = new Date();
    for (let i = 0; i < daysBack; i++) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      dates.push(dateStr(d));
    }
  }

  // ─── Process each league ─────────────────────────────────────
  for (const leagueKey of requestedLeagues) {
    const cfg = LEAGUES[leagueKey];
    if (!cfg) {
      errors.push(`Unknown league: ${leagueKey}`);
      continue;
    }

    const leagueResult = { eventsFound: 0, drained: 0, skipped: 0, errors: 0 };

    // Fetch scoreboard for each date
    const allEvents: any[] = [];
    for (const d of dates) {
      try {
        const sbUrl = `https://site.api.espn.com/apis/site/v2/sports/soccer/${cfg.espn}/scoreboard?dates=${d}`;
        const sb = await fetchWithTimeout(sbUrl, FETCH_TIMEOUT);
        const events = sb.events || [];
        for (const ev of events) {
          const status = ev.status?.type?.name;
          // Only completed matches
          if (status === 'STATUS_FINAL' || status === 'STATUS_FULL_TIME') {
            allEvents.push({ id: ev.id, date: d });
          }
        }
      } catch (e: any) {
        errors.push(`Scoreboard ${cfg.short}/${d}: ${e.message}`);
      }
    }

    leagueResult.eventsFound = allEvents.length;

    // Check which we already have (skip unless force)
    let existingIds = new Set<string>();
    if (!forceRefresh && allEvents.length > 0) {
      const ids = allEvents.map(e => `${e.id}_${cfg.short}`);
      const { data: existing } = await supabase
        .from('soccer_match_result')
        .select('id')
        .in('id', ids);
      if (existing) {
        existingIds = new Set(existing.map((r: any) => r.id));
      }
    }

    // Fetch summaries for new events
    const toFetch = allEvents.filter(e => !existingIds.has(`${e.id}_${cfg.short}`));
    leagueResult.skipped = allEvents.length - toFetch.length;
    totalSkipped += leagueResult.skipped;

    if (dry) {
      leagueResult.drained = 0;
      results[leagueKey] = { ...leagueResult, wouldDrain: toFetch.length };
      continue;
    }

    // Fetch + parse + upsert
    const parsed = await runBatch(toFetch, MAX_CONCURRENT, async (ev: any) => {
      const summaryUrl = `https://site.api.espn.com/apis/site/v2/sports/soccer/${cfg.espn}/summary?event=${ev.id}`;
      const data = await fetchWithTimeout(summaryUrl, FETCH_TIMEOUT);
      return parseMatchResult(data, ev.id, cfg.short);
    });

    // Upsert in batches of 10
    for (let i = 0; i < parsed.length; i += 10) {
      const batch = parsed.slice(i, i + 10);
      const { error } = await supabase
        .from('soccer_match_result')
        .upsert(batch, { onConflict: 'id' });
      if (error) {
        errors.push(`Upsert ${cfg.short}: ${error.message}`);
        totalErrors += batch.length;
        leagueResult.errors += batch.length;
      } else {
        totalDrained += batch.length;
        leagueResult.drained += batch.length;
      }
    }

    // Also update matches table status for completed games
    for (const p of parsed) {
      await supabase
        .from('matches')
        .update({ status: 'STATUS_FINAL' })
        .eq('id', p.match_id)
        .catch(() => {});
    }

    results[leagueKey] = leagueResult;
  }

  // ─── Sample output ───────────────────────────────────────────
  let sample: any = null;
  if (totalDrained > 0) {
    const { data: sampleRow } = await supabase
      .from('soccer_match_result')
      .select('id, home_team_name, away_team_name, home_score, away_score, home_possession, away_possession, home_shots, away_shots, venue, attendance, referee')
      .order('created_at', { ascending: false })
      .limit(1)
      .single();
    sample = sampleRow;
  }

  const body = {
    success: totalErrors === 0,
    version: 'v1',
    dryRun: dry,
    dateRange: dates.length > 1 ? `${dates[dates.length - 1]} → ${dates[0]}` : dates[0],
    leagues: requestedLeagues,
    totalFound: Object.values(results).reduce((s: number, r: any) => s + (r.eventsFound || 0), 0),
    totalDrained: totalDrained,
    totalSkipped: totalSkipped,
    totalErrors: totalErrors,
    durationMs: Date.now() - t0,
    perLeague: results,
    sample,
    errors: errors.length > 0 ? errors : undefined,
  };

  return new Response(JSON.stringify(body, null, 2), {
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
});

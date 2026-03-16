import { createClient } from 'https://esm.sh/@supabase/supabase-js@2.45.4'

declare const Deno: any;

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const LEAGUE_MAP: Record<string, { espn: string; suffix: string; display: string }> = {
  epl:        { espn: 'eng.1',            suffix: 'epl',        display: 'Premier League' },
  laliga:     { espn: 'esp.1',            suffix: 'laliga',     display: 'La Liga' },
  seriea:     { espn: 'ita.1',            suffix: 'seriea',     display: 'Serie A' },
  bundesliga: { espn: 'ger.1',            suffix: 'bundesliga', display: 'Bundesliga' },
  ligue1:     { espn: 'fra.1',            suffix: 'ligue1',     display: 'Ligue 1' },
  mls:        { espn: 'usa.1',            suffix: 'mls',        display: 'MLS' },
  ucl:        { espn: 'uefa.champions',   suffix: 'ucl',        display: 'Champions League' },
  uel:        { espn: 'uefa.europa',      suffix: 'uel',        display: 'Europa League' },
  'ned.1':    { espn: 'ned.1',            suffix: 'ned.1',      display: 'Eredivisie' },
  'por.1':    { espn: 'por.1',            suffix: 'por.1',      display: 'Primeira Liga' },
  'bel.1':    { espn: 'bel.1',            suffix: 'bel.1',      display: 'Belgian Pro League' },
  'tur.1':    { espn: 'tur.1',            suffix: 'tur.1',      display: 'Super Lig' },
  'bra.1':    { espn: 'bra.1',            suffix: 'bra.1',      display: 'Brasileirao' },
  'arg.1':    { espn: 'arg.1',            suffix: 'arg.1',      display: 'Argentina Primera' },
  'sco.1':    { espn: 'sco.1',            suffix: 'sco.1',      display: 'Scottish Premiership' },
};

const MAX_CONCURRENT   = 2;
const INTER_BATCH_MS   = 500;
const FETCH_TIMEOUT_MS = 15_000;
const DRAIN_VERSION    = 'v6.3';
const DEFAULT_LEAGUES =
  'epl,laliga,seriea,bundesliga,ligue1,mls,ucl,uel,arg.1,bra.1,ned.1,tur.1,por.1,bel.1,sco.1';

async function fetchWithTimeout(url: string, ms: number): Promise<Response> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  try { return await fetch(url, { signal: ctrl.signal }); }
  finally { clearTimeout(timer); }
}

async function fetchWithRetry(url: string, attempts = 4): Promise<Response> {
  let lastError: any = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      const res = await fetchWithTimeout(url, FETCH_TIMEOUT_MS);
      if (res.ok) return res;
      if ((res.status === 429 || res.status === 503) && attempt < attempts) {
        const retryAfter = Number(res.headers.get('retry-after'));
        const waitMs = Number.isFinite(retryAfter) && retryAfter > 0
          ? retryAfter * 1000
          : Math.min(8000, 500 * Math.pow(2, attempt - 1));
        await new Promise((resolve) => setTimeout(resolve, waitMs));
        continue;
      }
      return res;
    } catch (error: any) {
      lastError = error;
      if (attempt >= attempts) throw error;
      const waitMs = Math.min(8000, 500 * Math.pow(2, attempt - 1));
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }
  throw lastError ?? new Error('ESPN fetch failed');
}

function statInt(stats: any[], name: string): number | null {
  const s = stats?.find((x: any) => x.name === name);
  if (!s) return null;
  const v = parseFloat(s.displayValue);
  return isNaN(v) ? null : Math.round(v);
}

function statNum(stats: any[], name: string): number | null {
  const s = stats?.find((x: any) => x.name === name);
  if (!s) return null;
  const v = parseFloat(s.displayValue);
  return isNaN(v) ? null : v;
}

function playerStat(playerStats: any[], name: string): number {
  const s = playerStats?.find((x: any) => x.name === name);
  return s ? parseInt(s.displayValue) || 0 : 0;
}

function buildScorerStrings(goals: any[]): { home: string[]; away: string[] } {
  const home: Record<string, string[]> = {};
  const away: Record<string, string[]> = {};
  for (const g of goals) {
    const bucket = g.side === 'home' ? home : away;
    const name = g.scorer || 'Unknown';
    if (!bucket[name]) bucket[name] = [];
    bucket[name].push(g.minute || '?');
  }
  const format = (b: Record<string, string[]>) =>
    Object.entries(b).map(([name, mins]) => `${name} ${mins.join(', ')}`);
  return { home: format(home), away: format(away) };
}

function slugify(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function normalizeName(name: string): string {
  return name
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/\u00f8/g, 'o').replace(/\u00d8/g, 'O')
    .replace(/\u00e6/g, 'ae').replace(/\u00c6/g, 'AE')
    .replace(/\u0153/g, 'oe').replace(/\u0152/g, 'OE')
    .replace(/\u00df/g, 'ss')
    .replace(/\u0111/g, 'd').replace(/\u0110/g, 'D')
    .replace(/\u0142/g, 'l').replace(/\u0141/g, 'L')
    .toLowerCase()
    .trim();
}

function buildNameKeys(name: string): string[] {
  const base = normalizeName(name);
  const keys = [base];
  const parts = base.split(/[\s-]+/).filter(Boolean);
  if (parts.length >= 3) {
    keys.push(parts.slice(1).join(' '));
    keys.push(parts[0] + ' ' + parts[parts.length - 1]);
  }
  if (name.includes('-')) {
    keys.push(base.replace(/-/g, ' '));
    keys.push(base.replace(/-/g, ''));
  }
  return [...new Set(keys)];
}

const safeInt = (v: any): number | null => {
  if (v === null || v === undefined) return null;
  const n = parseInt(String(v));
  return isNaN(n) ? null : n;
};

const safeFloat = (v: any): number | null => {
  if (v === null || v === undefined) return null;
  const n = parseFloat(String(v));
  return isNaN(n) ? null : n;
};

function parseGoalMinute(raw: string | undefined): number | null {
  if (!raw) return null;
  const cleaned = raw.replace(/'/g, '').trim();
  const addedMatch = cleaned.match(/^(\d+)\+(\d+)$/);
  if (addedMatch) return parseInt(addedMatch[1]) + parseInt(addedMatch[2]);
  const stdMatch = cleaned.match(/^(\d+)$/);
  if (stdMatch) return parseInt(stdMatch[1]);
  return null;
}

function fracToDecimal(frac: string | null | undefined): number | null {
  if (!frac || typeof frac !== 'string') return null;
  const parts = frac.split('/');
  if (parts.length !== 2) return null;
  const num = parseFloat(parts[0]);
  const den = parseFloat(parts[1]);
  if (isNaN(num) || isNaN(den) || den === 0) return null;
  return Math.round((num / den + 1) * 100) / 100;
}

function extractOdds(data: any): Record<string, number | null> {
  const pickcenter = data.pickcenter;
  if (!Array.isArray(pickcenter) || pickcenter.length === 0) {
    return { dk_home_ml: null, dk_away_ml: null, dk_draw_ml: null, dk_spread: null, dk_home_spread_price: null, dk_away_spread_price: null, dk_total: null, dk_over_price: null, dk_under_price: null };
  }
  let pc = pickcenter.find((p: any) => p.provider?.name?.toLowerCase().includes('draftkings') || p.provider?.name?.toLowerCase().includes('draft kings'));
  if (!pc) pc = pickcenter[0];
  let dk_home_ml = safeInt(pc.homeTeamOdds?.moneyLine);
  let dk_away_ml = safeInt(pc.awayTeamOdds?.moneyLine);
  let dk_draw_ml: number | null = null;
  if (pc.drawOdds?.moneyLine !== undefined) dk_draw_ml = safeInt(pc.drawOdds.moneyLine);
  if (dk_draw_ml === null && pc.drawOdds !== undefined && typeof pc.drawOdds !== 'object') dk_draw_ml = safeInt(pc.drawOdds);
  if (dk_draw_ml === null && pc.drawTeamOdds?.moneyLine !== undefined) dk_draw_ml = safeInt(pc.drawTeamOdds.moneyLine);
  if (dk_draw_ml === null && pc.tieOdds !== undefined) dk_draw_ml = safeInt(pc.tieOdds);
  if (dk_draw_ml === null && pc.drawLine !== undefined) dk_draw_ml = safeInt(pc.drawLine);
  return { dk_home_ml, dk_away_ml, dk_draw_ml, dk_spread: safeFloat(pc.spread), dk_home_spread_price: safeFloat(pc.homeTeamOdds?.spreadOdds), dk_away_spread_price: safeFloat(pc.awayTeamOdds?.spreadOdds), dk_total: safeFloat(pc.overUnder), dk_over_price: safeFloat(pc.overOdds), dk_under_price: safeFloat(pc.underOdds) };
}

function extractGameFlow(data: any, goals: any[], homeScore: number, awayScore: number, penAttempts: number): Record<string, any> {
  const parsedGoals = goals.map(g => ({ side: g.side as string, minute: parseGoalMinute(g.minute) })).filter(g => g.minute !== null) as { side: string; minute: number }[];
  parsedGoals.sort((a, b) => a.minute - b.minute);
  const home1h = parsedGoals.filter(g => g.side === 'home' && g.minute <= 45).length;
  const away1h = parsedGoals.filter(g => g.side === 'away' && g.minute <= 45).length;
  const home2h = parsedGoals.filter(g => g.side === 'home' && g.minute > 45).length;
  const away2h = parsedGoals.filter(g => g.side === 'away' && g.minute > 45).length;
  const totalGoals = homeScore + awayScore; const goals1h = home1h + away1h;
  const htResult = home1h > away1h ? 'H' : home1h < away1h ? 'A' : 'D';
  const ftResult = homeScore > awayScore ? 'H' : homeScore < awayScore ? 'A' : 'D';
  const firstGoal = parsedGoals.length > 0 ? parsedGoals[0] : null;
  const lastGoal = parsedGoals.length > 0 ? parsedGoals[parsedGoals.length - 1] : null;
  let firstGoalInterval: string | null = null;
  if (firstGoal) { const m = firstGoal.minute; if (m <= 15) firstGoalInterval = '1-15'; else if (m <= 30) firstGoalInterval = '16-30'; else if (m <= 45) firstGoalInterval = '31-45'; else if (m <= 60) firstGoalInterval = '46-60'; else if (m <= 75) firstGoalInterval = '61-75'; else firstGoalInterval = '76-90'; }
  let firstGoalMinute: number | null = null;
  const keyEvents = data.keyEvents;
  if (Array.isArray(keyEvents)) { for (const ev of keyEvents) { const typeText = (ev.type?.text || '').toLowerCase(); if (!typeText.includes('goal')) continue; if (typeText.includes('penalty kick') && !typeText.includes('penalty kick goal')) continue; const m = parseGoalMinute(ev.clock?.displayValue); if (m !== null && (firstGoalMinute === null || m < firstGoalMinute)) firstGoalMinute = m; } }
  if (firstGoalMinute === null && firstGoal) firstGoalMinute = firstGoal.minute;
  return { first_goal_minute: firstGoalMinute, home_goals_1h: home1h, away_goals_1h: away1h, home_goals_2h: home2h, away_goals_2h: away2h, ht_result: htResult, ft_result: ftResult, ht_ft_result: `${htResult}/${ftResult}`, btts: homeScore > 0 && awayScore > 0, btts_1h: home1h > 0 && away1h > 0, btts_2h: home2h > 0 && away2h > 0, home_scored_both_halves: home1h > 0 && home2h > 0, away_scored_both_halves: away1h > 0 && away2h > 0, first_goal_team: firstGoal?.side || null, first_goal_interval: firstGoalInterval, last_goal_minute: lastGoal?.minute || null, last_goal_team: lastGoal?.side || null, late_goals: parsedGoals.filter(g => g.minute >= 85).length, stoppage_time_goals: parsedGoals.filter(g => g.minute >= 90).length, penalty_awarded: penAttempts > 0, total_penalties: penAttempts, scoreless: homeScore === 0 && awayScore === 0, goals_1h_pct: totalGoals > 0 ? Math.round(goals1h / totalGoals * 1000) / 10 : null };
}

function findBet365Provider(data: any): any | null {
  const sources = [data.pickcenter, data.odds];
  for (const source of sources) { if (!Array.isArray(source)) continue; const bet365 = source.find((p: any) => { const name = (p.provider?.name || '').toLowerCase(); return name.includes('bet 365') || name.includes('bet365'); }); if (bet365) return bet365; }
  return null;
}

interface PlayerOddRow { id: string; match_id: string; espn_event_id: string; league_id: string; player_name: string; team: string | null; team_name: string | null; pool: string; odds_fractional: string; odds_decimal: number | null; implied_prob: number | null; odd_id: string | null; scored: boolean; goals_scored: number; first_goal: boolean; last_goal: boolean; goal_minutes: string[]; result: string | null; profit_decimal: number | null; match_date: string | null; home_team: string | null; away_team: string | null; captured_at: string; drain_version: string; }

function extractPlayerOdds(bet365: any, eventId: string, matchId: string, leagueKey: string, goals: any[], matchDate: string | null, homeTeam: string | null, awayTeam: string | null): PlayerOddRow[] {
  const bettingOdds = bet365?.bettingOdds;
  if (!bettingOdds?.playerOdds) return [];
  const poolMap: Record<string, string> = { preMatchAnyTimeGoalScorer: 'anytime', preMatchFirstGoalScorer: 'first', preMatchLastGoalScorer: 'last', liveAnyTimeGoalScorer: 'live_anytime' };

  // v6.2: Unicode-normalized scorer map
  const scorerMap = new Map<string, { count: number; minutes: string[]; isFirst: boolean; isLast: boolean }>();
  const sortedGoals = [...goals].sort((a, b) => (parseGoalMinute(a.minute) ?? 999) - (parseGoalMinute(b.minute) ?? 999));
  for (let i = 0; i < sortedGoals.length; i++) {
    const g = sortedGoals[i];
    const rawName = (g.scorer || '').trim();
    if (!rawName) continue;
    const keys = buildNameKeys(rawName);
    const primaryKey = normalizeName(rawName);
    const existing = scorerMap.get(primaryKey) || { count: 0, minutes: [], isFirst: false, isLast: false };
    existing.count++;
    if (g.minute) existing.minutes.push(g.minute);
    if (i === 0) existing.isFirst = true;
    if (i === sortedGoals.length - 1) existing.isLast = true;
    for (const key of keys) scorerMap.set(key, existing);
  }

  const rows: PlayerOddRow[] = []; const now = new Date().toISOString();
  for (const [espnKey, poolName] of Object.entries(poolMap)) {
    const players = bettingOdds.playerOdds[espnKey];
    if (!Array.isArray(players)) continue;
    for (const p of players) {
      const playerName = (p.player || '').trim();
      if (!playerName) continue;
      const fracOdds = p.value || null;
      const decimal = fracToDecimal(fracOdds);
      const impliedProb = decimal && decimal > 0 ? Math.round(1 / decimal * 10000) / 100 : null;
      // v6.2: Multi-key normalized lookup
      const lookupKeys = buildNameKeys(playerName);
      let scorer: { count: number; minutes: string[]; isFirst: boolean; isLast: boolean } | undefined;
      for (const key of lookupKeys) { scorer = scorerMap.get(key); if (scorer) break; }
      let scored = false, goalsScored = 0, isFirst = false, isLast = false;
      let goalMinutes: string[] = [];
      if (scorer) { scored = true; goalsScored = scorer.count; isFirst = scorer.isFirst; isLast = scorer.isLast; goalMinutes = scorer.minutes; }
      let result: string | null = null; let profitDecimal: number | null = null;
      if (poolName === 'anytime' || poolName === 'live_anytime') { result = scored ? 'win' : 'loss'; }
      else if (poolName === 'first') { result = isFirst ? 'win' : 'loss'; }
      else if (poolName === 'last') { result = isLast ? 'win' : 'loss'; }
      if (result === 'win' && decimal) { profitDecimal = Math.round((decimal - 1) * 100) / 100; } else if (result === 'loss') { profitDecimal = -1; }
      rows.push({ id: `${eventId}_${poolName}_${slugify(playerName)}`, match_id: matchId, espn_event_id: eventId, league_id: leagueKey, player_name: playerName, team: null, team_name: null, pool: poolName, odds_fractional: fracOdds || '', odds_decimal: decimal, implied_prob: impliedProb, odd_id: p.oddId || null, scored, goals_scored: goalsScored, first_goal: isFirst, last_goal: isLast, goal_minutes: goalMinutes, result, profit_decimal: profitDecimal, match_date: matchDate, home_team: homeTeam, away_team: awayTeam, captured_at: now, drain_version: DRAIN_VERSION });
    }
  }
  return rows;
}

function extractBet365TeamOdds(bet365: any, eventId: string, matchId: string, leagueKey: string, matchDate: string | null, homeTeam: string | null, awayTeam: string | null, homeScore: number, awayScore: number): any | null {
  const bettingOdds = bet365?.bettingOdds;
  if (!bettingOdds?.teamOdds) return null;
  const to = bettingOdds.teamOdds;
  const totalGoals = homeScore + awayScore;
  const ftResult = homeScore > awayScore ? 'H' : homeScore < awayScore ? 'A' : 'D';
  const getFrac = (key: string): string | null => to[key]?.value || null;
  return { id: `${eventId}_${leagueKey}`, match_id: matchId, espn_event_id: eventId, league_id: leagueKey, b365_home_frac: getFrac('preMatchFullTimeResultHome'), b365_draw_frac: getFrac('preMatchFullTimeResultDraw'), b365_away_frac: getFrac('preMatchFullTimeResultAway'), b365_home_dec: fracToDecimal(getFrac('preMatchFullTimeResultHome')), b365_draw_dec: fracToDecimal(getFrac('preMatchFullTimeResultDraw')), b365_away_dec: fracToDecimal(getFrac('preMatchFullTimeResultAway')), b365_ou_handicap: safeFloat(to.preMatchOverUnderHandicap?.value), b365_over_frac: getFrac('preMatchGoalLineOver'), b365_under_frac: getFrac('preMatchGoalLineUnder'), b365_over_dec: fracToDecimal(getFrac('preMatchGoalLineOver')), b365_under_dec: fracToDecimal(getFrac('preMatchGoalLineUnder')), b365_dc_home_draw_frac: getFrac('preMatchDoubleChanceHomeOrDraw'), b365_dc_draw_away_frac: getFrac('preMatchDoubleChanceDrawOrAway'), b365_dc_home_away_frac: getFrac('preMatchDoubleChanceHomeOrAway'), b365_dc_home_draw_dec: fracToDecimal(getFrac('preMatchDoubleChanceHomeOrDraw')), b365_dc_draw_away_dec: fracToDecimal(getFrac('preMatchDoubleChanceDrawOrAway')), b365_dc_home_away_dec: fracToDecimal(getFrac('preMatchDoubleChanceHomeOrAway')), match_date: matchDate, home_team: homeTeam, away_team: awayTeam, home_score: homeScore, away_score: awayScore, ft_result: ftResult, total_goals: totalGoals, captured_at: new Date().toISOString(), drain_version: DRAIN_VERSION };
}

function buildSnapshot(data: any, eventId: string, leagueKey: string, matchDate: string | null, homeTeam: string | null, awayTeam: string | null, homeScore: number, awayScore: number, matchStatus: string | null): any {
  return { id: `${eventId}_${leagueKey}`, espn_event_id: eventId, league_id: leagueKey, raw_boxscore: data.boxscore || null, raw_pickcenter: data.pickcenter || data.odds || null, raw_keyevents: data.keyEvents || null, raw_commentary: data.commentary || null, raw_rosters: data.rosters || null, raw_standings: data.standings || null, raw_header: data.header || null, raw_plays: data.plays || null, match_date: matchDate, home_team: homeTeam, away_team: awayTeam, final_score: `${homeScore}-${awayScore}`, match_status: matchStatus, payload_size_kb: Math.round(JSON.stringify(data).length / 1024), snapshot_at: new Date().toISOString(), drain_version: DRAIN_VERSION };
}

function extractLineup(rosterData: any): any[] {
  if (!rosterData?.roster) return [];
  return rosterData.roster.map((p: any) => { const stats = p.stats || []; return { name: p.athlete?.displayName || 'Unknown', id: p.athlete?.id || null, position: p.position?.displayName || null, jersey: p.jersey || null, starter: p.starter || false, subbedIn: p.subbedIn || false, subbedOut: p.subbedOut || false, goals: playerStat(stats, 'totalGoals'), assists: playerStat(stats, 'goalAssists'), shots: playerStat(stats, 'totalShots'), shotsOnTarget: playerStat(stats, 'shotsOnTarget'), saves: playerStat(stats, 'saves'), yellowCards: playerStat(stats, 'yellowCards'), redCards: playerStat(stats, 'redCards'), foulsCommitted: playerStat(stats, 'foulsCommitted'), foulsSuffered: playerStat(stats, 'foulsSuffered') }; });
}

function extractPostgame(
  data: any,
  eventId: string,
  leagueKey: string,
  matchId: string,
  startTime: string,
  canonicalGameId: string | null,
): any {
  const boxTeams = data.boxscore?.teams || [];
  const homeTeamData = boxTeams.find((t: any) => t.homeAway === 'home') || boxTeams[0];
  const awayTeamData = boxTeams.find((t: any) => t.homeAway === 'away') || boxTeams[1];
  const homeStats = homeTeamData?.statistics || []; const awayStats = awayTeamData?.statistics || [];
  const hComp = data.header?.competitions?.[0];
  const homeComp = hComp?.competitors?.find((c: any) => c.homeAway === 'home');
  const awayComp = hComp?.competitors?.find((c: any) => c.homeAway === 'away');
  const homeName = homeComp?.team?.displayName || homeTeamData?.team?.displayName;
  const rawEvents = data.keyEvents || [];
  const goals: any[] = []; const cards: any[] = []; const subs: any[] = []; const timeline: any[] = [];
  for (const ev of rawEvents) {
    const typeText = ev.type?.text || ''; const minute = ev.clock?.displayValue || '';
    const team = ev.team?.displayName || null; const p1 = ev.participants?.[0]?.athlete?.displayName || null; const p2 = ev.participants?.[1]?.athlete?.displayName || null; const desc = ev.text || null;
    const side = team === homeName ? 'home' : team ? 'away' : null;
    timeline.push({ minute, type: typeText, team, side, players: [p1, p2].filter(Boolean), description: desc });
    const typeLower = typeText.toLowerCase();
    if (typeLower.includes('goal') && !typeLower.includes('penalty kick') && team) { goals.push({ minute, team, side, scorer: p1, assister: p2, type: typeText, description: desc }); }
    else if (typeLower.includes('card') && team) { cards.push({ minute, team, side, player: p1, card_type: typeLower.includes('red') ? 'red' : 'yellow' }); }
    else if (typeLower.includes('substitution') && team) { subs.push({ minute, team, side, player_in: p1, player_out: p2 }); }
  }
  const scorerStrings = buildScorerStrings(goals);
  const rosters = data.rosters || [];
  const homeRoster = rosters.find((r: any) => r.homeAway === 'home') || rosters[0];
  const awayRoster = rosters.find((r: any) => r.homeAway === 'away') || rosters[1];
  const gi = data.gameInfo || {}; const odds = extractOdds(data);
  const homeScore = homeComp ? parseInt(homeComp.score) || 0 : 0;
  const awayScore = awayComp ? parseInt(awayComp.score) || 0 : 0;
  const penAttempts = (statInt(homeStats, 'penaltyKickShots') || 0) + (statInt(awayStats, 'penaltyKickShots') || 0);
  const flow = extractGameFlow(data, goals, homeScore, awayScore, penAttempts);
  const homeTeam = homeComp?.team?.displayName || homeTeamData?.team?.displayName || null;
  const awayTeam = awayComp?.team?.displayName || awayTeamData?.team?.displayName || null;
  const matchStatus = hComp?.status?.type?.name || null;
  const matchDate = startTime ? startTime.split('T')[0] : null;
  const bet365 = findBet365Provider(data);
  const playerOddsRows = bet365 ? extractPlayerOdds(bet365, eventId, matchId, leagueKey, goals, matchDate, homeTeam, awayTeam) : [];
  const bet365TeamOdds = bet365 ? extractBet365TeamOdds(bet365, eventId, matchId, leagueKey, matchDate, homeTeam, awayTeam, homeScore, awayScore) : null;
  const snapshot = buildSnapshot(data, eventId, leagueKey, matchDate, homeTeam, awayTeam, homeScore, awayScore, matchStatus);
  const postgame = { id: matchId, match_id: matchId, canonical_game_id: canonicalGameId || matchId, espn_event_id: eventId, league_id: leagueKey, home_team: homeTeam, away_team: awayTeam, home_score: homeScore, away_score: awayScore, match_status: matchStatus, start_time: startTime, venue: gi.venue?.fullName || null, attendance: gi.attendance || null, referee: gi.officials?.[0]?.fullName || null, home_possession: statNum(homeStats, 'possessionPct'), away_possession: statNum(awayStats, 'possessionPct'), home_shots: statInt(homeStats, 'totalShots'), away_shots: statInt(awayStats, 'totalShots'), home_shots_on_target: statInt(homeStats, 'shotsOnTarget'), away_shots_on_target: statInt(awayStats, 'shotsOnTarget'), home_shot_accuracy: statNum(homeStats, 'shotPct'), away_shot_accuracy: statNum(awayStats, 'shotPct'), home_passes: statInt(homeStats, 'totalPasses'), away_passes: statInt(awayStats, 'totalPasses'), home_accurate_passes: statInt(homeStats, 'accuratePasses'), away_accurate_passes: statInt(awayStats, 'accuratePasses'), home_pass_pct: statNum(homeStats, 'passPct'), away_pass_pct: statNum(awayStats, 'passPct'), home_crosses: statInt(homeStats, 'totalCrosses'), away_crosses: statInt(awayStats, 'totalCrosses'), home_accurate_crosses: statInt(homeStats, 'accurateCrosses'), away_accurate_crosses: statInt(awayStats, 'accurateCrosses'), home_long_balls: statInt(homeStats, 'totalLongBalls'), away_long_balls: statInt(awayStats, 'totalLongBalls'), home_accurate_long_balls: statInt(homeStats, 'accurateLongBalls'), away_accurate_long_balls: statInt(awayStats, 'accurateLongBalls'), home_corners: statInt(homeStats, 'wonCorners'), away_corners: statInt(awayStats, 'wonCorners'), home_offsides: statInt(homeStats, 'offsides'), away_offsides: statInt(awayStats, 'offsides'), home_penalty_goals: statInt(homeStats, 'penaltyKickGoals'), away_penalty_goals: statInt(awayStats, 'penaltyKickGoals'), home_penalty_attempts: statInt(homeStats, 'penaltyKickShots'), away_penalty_attempts: statInt(awayStats, 'penaltyKickShots'), home_fouls: statInt(homeStats, 'foulsCommitted'), away_fouls: statInt(awayStats, 'foulsCommitted'), home_yellow_cards: statInt(homeStats, 'yellowCards'), away_yellow_cards: statInt(awayStats, 'yellowCards'), home_red_cards: statInt(homeStats, 'redCards'), away_red_cards: statInt(awayStats, 'redCards'), home_saves: statInt(homeStats, 'saves'), away_saves: statInt(awayStats, 'saves'), home_tackles: statInt(homeStats, 'totalTackles'), away_tackles: statInt(awayStats, 'totalTackles'), home_effective_tackles: statInt(homeStats, 'effectiveTackles'), away_effective_tackles: statInt(awayStats, 'effectiveTackles'), home_interceptions: statInt(homeStats, 'interceptions'), away_interceptions: statInt(awayStats, 'interceptions'), home_clearances: statInt(homeStats, 'totalClearance'), away_clearances: statInt(awayStats, 'totalClearance'), home_blocked_shots: statInt(homeStats, 'blockedShots'), away_blocked_shots: statInt(awayStats, 'blockedShots'), ...odds, ...flow, goals, cards, substitutions: subs, timeline, home_scorers: scorerStrings.home, away_scorers: scorerStrings.away, home_lineup: extractLineup(homeRoster), away_lineup: extractLineup(awayRoster), commentary_count: Array.isArray(data.commentary) ? data.commentary.length : 0, drain_version: DRAIN_VERSION, last_drained_at: new Date().toISOString() };
  return { postgame, playerOddsRows, bet365TeamOdds, snapshot };
}

async function processBatch<T, R>(items: T[], batchSize: number, delayMs: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchResults = await Promise.allSettled(batch.map(fn));
    for (const r of batchResults) { if (r.status === 'fulfilled' && r.value) results.push(r.value); }
    if (i + batchSize < items.length) await new Promise(r => setTimeout(r, delayMs));
  }
  return results;
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });
  const supabase = createClient(Deno.env.get('SUPABASE_URL')!, Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!);
  const url = new URL(req.url);
  const leagueParam = url.searchParams.get('league') || DEFAULT_LEAGUES;
  const daysBack = parseInt(url.searchParams.get('days') || '14');
  const dry = url.searchParams.get('dry') === 'true';
  const forceRefresh = url.searchParams.get('force') === 'true';
  const skipSnapshot = url.searchParams.get('skip_snapshot') === 'true';
  const requestedLeagues = leagueParam.split(',').map(l => l.trim().toLowerCase());
  const errors: string[] = [];
  let totalFound = 0, totalDrained = 0, totalPlayerOdds = 0, totalBet365Team = 0, totalSnapshots = 0;
  const leagueStats: any[] = []; const sampleData: any[] = [];

  for (const leagueKey of requestedLeagues) {
    const cfg = LEAGUE_MAP[leagueKey];
    if (!cfg) { errors.push(`Unknown league: ${leagueKey}`); continue; }
    try {
      const cutoff = new Date(); cutoff.setDate(cutoff.getDate() - daysBack);
      const { data: matchRows, error: matchErr } = await supabase
        .from('matches')
        .select('id, canonical_game_id, home_team, away_team, start_time, status, league_id')
        .eq('league_id', cfg.espn)
        .in('status', ['STATUS_FINAL', 'STATUS_FULL_TIME'])
        .gte('start_time', cutoff.toISOString())
        .order('start_time', { ascending: false });
      if (matchErr) { errors.push(`${leagueKey}: query failed: ${matchErr.message}`); continue; }
      if (!matchRows?.length) { leagueStats.push({ league: leagueKey, found: 0, drained: 0, skipped: 0 }); continue; }
      totalFound += matchRows.length;
      let toDrain = matchRows;
      if (!forceRefresh) {
        const ids = matchRows.map((r: any) => r.id);
        const { data: existing } = await supabase.from('soccer_postgame').select('id, drain_version').in('id', ids);
        const existingMap = new Map((existing || []).map((e: any) => [e.id, e.drain_version]));
        toDrain = matchRows.filter((r: any) => { const ver = existingMap.get(r.id); return !ver || ver !== DRAIN_VERSION; });
      }
      const skipped = matchRows.length - toDrain.length;
      if (dry) { leagueStats.push({ league: leagueKey, found: matchRows.length, drained: 0, skipped, wouldDrain: toDrain.length }); continue; }
      let leaguePlayerOdds = 0, leagueBet365Team = 0, leagueSnapshots = 0;
      const drainResults = await processBatch(toDrain, MAX_CONCURRENT, INTER_BATCH_MS, async (row: any) => {
        const eventId = row.id.split('_')[0];
        const summaryUrl = `https://site.api.espn.com/apis/site/v2/sports/soccer/${cfg.espn}/summary?event=${eventId}`;
        try {
          const res = await fetchWithRetry(summaryUrl);
          if (!res.ok) { errors.push(`${leagueKey}/${eventId}: ESPN ${res.status}`); return null; }
          const data = await res.json();
          const hs = data.boxscore?.teams?.[0]?.statistics || [];
          if (!hs.some((s: any) => s.name === 'possessionPct')) return null;
          return extractPostgame(data, eventId, leagueKey, row.id, row.start_time, row.canonical_game_id ?? row.id);
        } catch (e: any) { errors.push(`${leagueKey}/${eventId}: ${e.message}`); return null; }
      });
      const valid = drainResults.filter(Boolean) as any[];
      const postgameRows = valid.map(v => v.postgame);
      const canonicalRows = Array.from(
        new Map(
          postgameRows.map((row: any) => [
            row.canonical_game_id,
            {
              id: row.canonical_game_id,
              league_id: cfg.espn,
              sport: 'soccer',
              home_team_name: row.home_team,
              away_team_name: row.away_team,
              commence_time: row.start_time,
              status: row.match_status || 'STATUS_FINAL',
              game_uuid: crypto.randomUUID(),
            },
          ]),
        ).values(),
      );
      if (canonicalRows.length > 0) {
        for (let i = 0; i < canonicalRows.length; i += 50) {
          const batch = canonicalRows.slice(i, i + 50);
          const { error: canonicalErr } = await supabase
            .from('canonical_games')
            .upsert(batch, { onConflict: 'id', ignoreDuplicates: true });
          if (canonicalErr) {
            errors.push(`${leagueKey}: canonical_games upsert: ${canonicalErr.message}`);
          }
        }
      }
      for (let i = 0; i < postgameRows.length; i += 20) { const batch = postgameRows.slice(i, i + 20); const { error: e } = await supabase.from('soccer_postgame').upsert(batch, { onConflict: 'id' }); if (e) errors.push(`${leagueKey}: postgame upsert: ${e.message}`); }
      // v6.1: Dedup player odds before upsert
      const allPlayerRowsRaw = valid.flatMap(v => v.playerOddsRows || []);
      const playerDedup = new Map<string, any>(); for (const row of allPlayerRowsRaw) playerDedup.set(row.id, row);
      const allPlayerRows = Array.from(playerDedup.values());
      if (allPlayerRows.length > 0) { for (let i = 0; i < allPlayerRows.length; i += 50) { const batch = allPlayerRows.slice(i, i + 50); const { error: e } = await supabase.from('soccer_player_odds').upsert(batch, { onConflict: 'id' }); if (e) errors.push(`${leagueKey}: player_odds upsert: ${e.message}`); } leaguePlayerOdds = allPlayerRows.length; }
      const bet365RowsRaw = valid.map(v => v.bet365TeamOdds).filter(Boolean);
      const bet365Dedup = new Map<string, any>(); for (const row of bet365RowsRaw) bet365Dedup.set(row.id, row);
      const bet365Rows = Array.from(bet365Dedup.values());
      if (bet365Rows.length > 0) { for (let i = 0; i < bet365Rows.length; i += 20) { const batch = bet365Rows.slice(i, i + 20); const { error: e } = await supabase.from('soccer_bet365_team_odds').upsert(batch, { onConflict: 'id' }); if (e) errors.push(`${leagueKey}: bet365_team upsert: ${e.message}`); } leagueBet365Team = bet365Rows.length; }
      if (!skipSnapshot) { const snapshotsRaw = valid.map(v => v.snapshot).filter(Boolean); const snapDedup = new Map<string, any>(); for (const row of snapshotsRaw) snapDedup.set(row.id, row); const snapshots = Array.from(snapDedup.values()); if (snapshots.length > 0) { for (let i = 0; i < snapshots.length; i += 10) { const batch = snapshots.slice(i, i + 10); const { error: e } = await supabase.from('espn_summary_snapshots').upsert(batch, { onConflict: 'id' }); if (e) errors.push(`${leagueKey}: snapshot upsert: ${e.message}`); } leagueSnapshots = snapshots.length; } }
      totalDrained += postgameRows.length; totalPlayerOdds += leaguePlayerOdds; totalBet365Team += leagueBet365Team; totalSnapshots += leagueSnapshots;
      leagueStats.push({ league: leagueKey, found: matchRows.length, drained: postgameRows.length, skipped, odds_coverage: `${postgameRows.filter((v: any) => v.dk_home_ml !== null).length}/${postgameRows.length}`, draw_ml_coverage: `${postgameRows.filter((v: any) => v.dk_draw_ml !== null).length}/${postgameRows.length}`, game_flow_coverage: `${postgameRows.filter((v: any) => v.ht_ft_result !== null).length}/${postgameRows.length}`, player_odds_rows: leaguePlayerOdds, bet365_team_rows: leagueBet365Team, snapshots_stored: leagueSnapshots });
      if (valid.length > 0) { const s = valid[0].postgame; const po = valid[0].playerOddsRows || []; sampleData.push({ match: `${s.home_team} ${s.home_score}-${s.away_score} ${s.away_team}`, dk_3way_ml: `H ${s.dk_home_ml} / D ${s.dk_draw_ml} / A ${s.dk_away_ml}`, ht_ft: s.ht_ft_result, btts: s.btts, first_goal: `${s.first_goal_team} @ ${s.first_goal_minute}'`, half_goals: `1H: ${s.home_goals_1h}-${s.away_goals_1h} | 2H: ${s.home_goals_2h}-${s.away_goals_2h}`, late_goals: s.late_goals, penalty_awarded: s.penalty_awarded, player_odds_count: po.length, player_odds_sample: po.slice(0, 3).map((p: any) => `${p.player_name} ${p.pool} ${p.odds_fractional} \u2192 ${p.result}`) }); }
    } catch (e: any) { errors.push(`${leagueKey}: ${e.message}`); }
  }

  return new Response(JSON.stringify({ success: errors.length === 0, version: DRAIN_VERSION, changes: ['v6.3: Added full-time status support (STATUS_FULL_TIME)', 'v6.3: Default league set now includes 7 newly activated leagues', 'v6.3: Added ESPN retry/backoff for 429/503', 'v6.2: Unicode normalization for name matching', 'v6.2: Multi-key lookup (hyphenated names, middle names, abbreviations)', 'v6.1: Deduplicate rows before upsert', 'v6: Raw ESPN snapshots + Bet365 player/team odds + resolution'], dryRun: dry, leagues: requestedLeagues, totalFound, totalDrained, totalPlayerOdds, totalBet365Team, totalSnapshots, leagueStats, sample: sampleData, errorsCount: errors.length, errors: errors.slice(0, 20) }, null, 2), { headers: { ...CORS, 'Content-Type': 'application/json' } });
});

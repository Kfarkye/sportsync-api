import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

declare const Deno: any;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const LEAGUE_MAP: Record<string, string> = {
  epl: "eng.1", laliga: "esp.1", seriea: "ita.1", bundesliga: "ger.1",
  ligue1: "fra.1", mls: "usa.1", ucl: "uefa.champions", uel: "uefa.europa",
  "eng.1": "eng.1", "esp.1": "esp.1", "ita.1": "ita.1", "ger.1": "ger.1",
  "fra.1": "fra.1", "usa.1": "usa.1", "uefa.champions": "uefa.champions",
  "uefa.europa": "uefa.europa", "fifa.world": "fifa.world",
  "fifa.worldq.uefa": "fifa.worldq.uefa", "fifa.worldq.conmebol": "fifa.worldq.conmebol",
  "fifa.worldq.concacaf": "fifa.worldq.concacaf", "fifa.worldq.afc": "fifa.worldq.afc",
  "fifa.worldq.caf": "fifa.worldq.caf", "arg.1": "arg.1", "bra.1": "bra.1",
  "ned.1": "ned.1", "tur.1": "tur.1", "por.1": "por.1", "bel.1": "bel.1",
  "sco.1": "sco.1", "mex.1": "mex.1",
  "nba": "nba", "nhl": "nhl", "nfl": "nfl", "mlb": "mlb",
  "mens-college-basketball": "mens-college-basketball",
};

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
const MOVEMENT_PROVIDER_IDS = new Set([59, 58, 200, 100, 1004]);

function resolveCoreRoute(leagueId: string): { sport: string; league: string } {
  const league = LEAGUE_MAP[leagueId] ?? leagueId;
  const normalized = String(league ?? "").toLowerCase();

  if (normalized === "nba" || normalized === "mens-college-basketball") {
    return { sport: "basketball", league };
  }
  if (normalized === "nhl") {
    return { sport: "hockey", league };
  }
  if (normalized === "nfl") {
    return { sport: "football", league };
  }
  if (normalized === "mlb") {
    return { sport: "baseball", league };
  }

  return { sport: "soccer", league };
}

function toNum(v: any): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v.replace(/[^\d.\-+]/g, ''));
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function extractML(teamOdds: any): number | null {
  if (!teamOdds) return null;
  let ml = toNum(teamOdds.moneyLine);
  if (ml !== null) return Math.round(ml);
  ml = toNum(teamOdds?.current?.moneyLine);
  if (ml !== null) return Math.round(ml);
  ml = toNum(teamOdds?.close?.moneyLine);
  if (ml !== null) return Math.round(ml);
  ml = toNum(teamOdds?.open?.moneyLine);
  if (ml !== null) return Math.round(ml);
  return null;
}

function extractOpenML(teamOdds: any): number | null {
  if (!teamOdds) return null;
  const ml = toNum(teamOdds?.open?.moneyLine);
  return ml !== null ? Math.round(ml) : null;
}

interface ParsedProvider {
  provider_id: number;
  provider_name: string;
  home_ml: number | null;
  away_ml: number | null;
  draw_ml: number | null;
  total_line: number | null;
  total_over: number | null;
  total_under: number | null;
  spread_line: number | null;
  spread_home: number | null;
  spread_away: number | null;
  open_home_ml: number | null;
  open_away_ml: number | null;
  open_draw_ml: number | null;
  open_total_line: number | null;
  open_spread_line: number | null;
  moneyline_winner: boolean | null;
  spread_winner: boolean | null;
  raw_odds: any;
}

function parseProvider(item: any): ParsedProvider {
  const pid = Number(item?.provider?.id ?? 0);
  const pname = String(item?.provider?.name ?? 'Unknown');
  const total = toNum(item?.overUnder) ?? toNum(item?.close?.overUnder) ?? toNum(item?.current?.overUnder);
  const totalOver = toNum(item?.overOdds) ?? toNum(item?.close?.over?.moneyLine);
  const totalUnder = toNum(item?.underOdds) ?? toNum(item?.close?.under?.moneyLine);
  const openTotal = toNum(item?.open?.overUnder);
  const spread = toNum(item?.spread) ?? toNum(item?.close?.spread) ?? toNum(item?.current?.spread);
  const openSpread = toNum(item?.open?.spread);
  const homeML = extractML(item?.homeTeamOdds);
  const awayML = extractML(item?.awayTeamOdds);
  const drawML = extractML(item?.drawOdds);
  const openHomeML = extractOpenML(item?.homeTeamOdds);
  const openAwayML = extractOpenML(item?.awayTeamOdds);
  const openDrawML = extractOpenML(item?.drawOdds);
  const spreadHome = toNum(item?.homeTeamOdds?.spreadOdds) ?? toNum(item?.homeTeamOdds?.current?.spreadOdds);
  const spreadAway = toNum(item?.awayTeamOdds?.spreadOdds) ?? toNum(item?.awayTeamOdds?.current?.spreadOdds);
  const mlWinner = item?.moneylineWinner ?? item?.homeTeamOdds?.moneyLineWinner ?? null;
  const spreadWin = item?.spreadWinner ?? item?.homeTeamOdds?.spreadWinner ?? null;
  return {
    provider_id: pid, provider_name: pname,
    home_ml: homeML, away_ml: awayML, draw_ml: drawML,
    total_line: total, total_over: totalOver ? Math.round(totalOver) : null, total_under: totalUnder ? Math.round(totalUnder) : null,
    spread_line: spread, spread_home: spreadHome ? Math.round(spreadHome) : null, spread_away: spreadAway ? Math.round(spreadAway) : null,
    open_home_ml: openHomeML, open_away_ml: openAwayML, open_draw_ml: openDrawML,
    open_total_line: openTotal, open_spread_line: openSpread,
    moneyline_winner: typeof mlWinner === 'boolean' ? mlWinner : null,
    spread_winner: typeof spreadWin === 'boolean' ? spreadWin : null,
    raw_odds: item,
  };
}

function hasUsefulData(p: ParsedProvider): boolean {
  return p.total_line !== null || p.home_ml !== null || p.away_ml !== null || p.draw_ml !== null;
}

async function fetchJson(url: string): Promise<any> {
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, { headers: { 'User-Agent': 'drip-backfill/2.0' } });
      if (res.status === 404) return null;
      if (res.status === 429 || res.status === 503) { await sleep(2000 * attempt); continue; }
      if (res.ok) return await res.json();
      return null;
    } catch { if (attempt < 3) await sleep(1000 * attempt); }
  }
  return null;
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: CORS });
  if (req.method !== 'POST') return new Response('Method not allowed', { status: 405, headers: CORS });

  const supabaseUrl = Deno.env.get('SUPABASE_URL') ?? '';
  const serviceRole = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';
  if (!supabaseUrl || !serviceRole) {
    return new Response(JSON.stringify({ error: 'Missing env' }), { status: 500, headers: { ...CORS, 'Content-Type': 'application/json' } });
  }

  const sb = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });
  const body = await req.json().catch(() => ({}));
  const mode = body.mode ?? 'backfill';
  const batchSize = Math.min(body.batch_size ?? 25, 50);
  const leagueFilter = body.league_id ?? body.league ?? null;

  const results: any = { mode, processed: 0, inserted: 0, no_data: 0, errors: 0, probes: [] as any[] };

  let query = sb.from('espn_odds_backfill_log')
    .select('match_id, league_id, espn_event_id')
    .eq('status', 'pending')
    .order('match_id')
    .limit(batchSize);
  if (leagueFilter) query = query.eq('league_id', leagueFilter);

  const { data: pending, error: pendingErr } = await query;
  if (pendingErr) return new Response(JSON.stringify({ error: pendingErr.message }), { status: 500, headers: { ...CORS, 'Content-Type': 'application/json' } });
  if (!pending || pending.length === 0) return new Response(JSON.stringify({ ...results, message: 'No pending matches' }), { headers: { ...CORS, 'Content-Type': 'application/json' } });

  for (const row of pending) {
    const { sport: espnSport, league: espnLeague } = resolveCoreRoute(row.league_id);
    const eventId = row.espn_event_id;
    const baseUrl = `https://sports.core.api.espn.com/v2/sports/${espnSport}/leagues/${espnLeague}/events/${eventId}/competitions/${eventId}`;
    const url = `${baseUrl}/odds?limit=100`;

    await sleep(500);
    const json = await fetchJson(url);
    results.processed++;

    if (!json || !Array.isArray(json?.items) || json.items.length === 0) {
      await sb.from('espn_odds_backfill_log').update({ status: 'no_data', completed_at: new Date().toISOString() }).eq('match_id', row.match_id);
      results.no_data++;
      continue;
    }

    if (mode === 'probe') {
      const parsed = json.items.map((it: any) => {
        const p = parseProvider(it);
        return { provider_id: p.provider_id, provider_name: p.provider_name, total_line: p.total_line, home_ml: p.home_ml, away_ml: p.away_ml, draw_ml: p.draw_ml, spread_line: p.spread_line, open_total_line: p.open_total_line, has_useful: hasUsefulData(p) };
      });
      results.probes.push({ match_id: row.match_id, league_id: row.league_id, event_id: eventId, provider_count: json.items.length, providers: parsed, raw_first_item_keys: Object.keys(json.items[0] ?? {}), raw_home_odds_keys: Object.keys(json.items[0]?.homeTeamOdds ?? {}) });
      continue;
    }

    let providersInserted = 0;
    for (const item of json.items) {
      const p = parseProvider(item);
      if (!hasUsefulData(p)) continue;

      let lineHistory: any[] | null = null;
      if (MOVEMENT_PROVIDER_IDS.has(p.provider_id)) {
        const movementJson = await fetchJson(`${baseUrl}/odds/${p.provider_id}/history/0/movement?limit=100`);
        if (movementJson && Array.isArray(movementJson.items) && movementJson.items.length > 0) {
          lineHistory = movementJson.items;
        }
      }

      const upsertPayload: Record<string, any> = {
        match_id: row.match_id, league_id: row.league_id, espn_event_id: eventId,
        provider_id: p.provider_id, provider_name: p.provider_name,
        home_ml: p.home_ml, away_ml: p.away_ml, draw_ml: p.draw_ml,
        total_line: p.total_line, total_over: p.total_over, total_under: p.total_under,
        spread_line: p.spread_line, spread_home: p.spread_home, spread_away: p.spread_away,
        open_home_ml: p.open_home_ml, open_away_ml: p.open_away_ml, open_draw_ml: p.open_draw_ml,
        open_total_line: p.open_total_line, open_spread_line: p.open_spread_line,
        moneyline_winner: p.moneyline_winner, spread_winner: p.spread_winner,
        raw_odds: p.raw_odds,
      };
      if (lineHistory) upsertPayload.line_history = lineHistory;

      const { error: insertErr } = await sb.from('espn_historical_odds').upsert(upsertPayload, { onConflict: 'match_id,provider_id' });
      if (!insertErr) providersInserted++;
      else results.errors++;
    }

    await sb.from('espn_odds_backfill_log').update({
      status: providersInserted > 0 ? 'success' : 'no_data',
      providers_found: providersInserted,
      completed_at: new Date().toISOString(),
    }).eq('match_id', row.match_id);

    results.inserted += providersInserted;
    if (providersInserted === 0) results.no_data++;
  }

  const { count } = await sb.from('espn_odds_backfill_log').select('*', { count: 'exact', head: true }).eq('status', 'pending');
  results.remaining_pending = count;

  return new Response(JSON.stringify(results), { headers: { ...CORS, 'Content-Type': 'application/json' } });
});

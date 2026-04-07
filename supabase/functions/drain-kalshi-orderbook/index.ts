declare const Deno: any;

import { createClient } from "npm:@supabase/supabase-js@2";
import { createSign } from "node:crypto";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-cron-secret",
};

const KALSHI_BASE_URL = Deno.env.get("KALSHI_BASE_URL") || "https://api.elections.kalshi.com";
const REQUEST_DELAY_MS = 150;
const DEFAULT_MAX_MARKETS = 60;
const ABSOLUTE_MAX_MARKETS = 260;
const DEFAULT_MAX_EVENTS = 24;
const ABSOLUTE_MAX_EVENTS = 120;
const DISCOVERY_PAGE_LIMIT = 200;
const DISCOVERY_MAX_PAGES = 8;
const EXTRA_DISCOVERY_MAX_PAGES = 2;

type Phase = "discover" | "snapshot" | "both";
type SnapshotType = "pregame" | "live" | "settled";
type SnapshotWindow = "default" | "pregame" | "live";

type SportFilter = "all" | "soccer" | "nba" | "ncaamb" | "nfl" | "nhl" | "mlb";

const DISCOVERY_SERIES_BY_FILTER: Record<SportFilter, string[]> = {
  nba: ["KXNBATOTAL", "KXNBASPREAD", "KXNBAGAME"],
  ncaamb: ["KXNCAAMBTOTAL", "KXNCAAMBSPREAD", "KXNCAAMBGAME"],
  nfl: ["KXNFLTOTAL", "KXNFLSPREAD", "KXNFLGAME"],
  nhl: ["KXNHLTOTAL", "KXNHLSPREAD", "KXNHLGAME"],
  mlb: ["KXMLBTOTAL", "KXMLBSPREAD", "KXMLBGAME"],
  soccer: [
    "KXEPLTOTAL", "KXEPLSPREAD", "KXEPLGAME",
    "KXUCLTOTAL", "KXUCLSPREAD", "KXUCLGAME",
    "KXMLSTOTAL", "KXMLSSPREAD", "KXMLSGAME",
    "KXBUNDTOTAL", "KXBUNDSPREAD", "KXBUNDGAME",
    "KXLIGATOTAL", "KXLIGASPREAD", "KXLIGAGAME",
    "KXSERIETOTAL", "KXSERIESPREAD", "KXSERIEGAME",
  ],
  all: [],
};

DISCOVERY_SERIES_BY_FILTER.all = Array.from(
  new Set([
    ...DISCOVERY_SERIES_BY_FILTER.nba,
    ...DISCOVERY_SERIES_BY_FILTER.ncaamb,
    ...DISCOVERY_SERIES_BY_FILTER.nfl,
    ...DISCOVERY_SERIES_BY_FILTER.nhl,
    ...DISCOVERY_SERIES_BY_FILTER.mlb,
    ...DISCOVERY_SERIES_BY_FILTER.soccer,
  ])
);

interface EventRow {
  event_ticker: string;
  sport: string | null;
  league: string | null;
  title: string | null;
  home_team: string | null;
  away_team: string | null;
  game_date: string | null;
  market_count: number;
  market_tickers: string[];
  status: string;
}

interface CandidateMarket {
  eventTicker: string;
  marketTicker: string;
  sport: string | null;
  league: string | null;
  gameDate: string | null;
}

interface MarketIdentity {
  marketType: string;
  marketLabel: string | null;
  lineValue: number | null;
  lineSide: string | null;
  teamName: string | null;
}

interface MatchWindowRow {
  id: string;
  league_id: string | null;
  sport: string | null;
  home_team: string | null;
  away_team: string | null;
  start_time: string | null;
  status: string | null;
  period: number | null;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNum(value: any): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toInt(value: any): number | null {
  const n = toNum(value);
  return n === null ? null : Math.round(n);
}

function normalizeProbPrice(value: any): number | null {
  const n = toNum(value);
  if (n === null) return null;
  if (n > 1.5) return n / 100;
  return n;
}

function getStringField(obj: any, keys: string[]): string | null {
  for (const key of keys) {
    const value = obj?.[key];
    if (value === null || value === undefined) continue;
    const s = String(value).trim();
    if (s) return s;
  }
  return null;
}

function normalizeDateLike(value: any): string | null {
  if (!value) return null;
  const s = String(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function todayUtcDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function shiftUtcDate(yyyyMmDd: string, deltaDays: number): string {
  const d = new Date(`${yyyyMmDd}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + deltaDays);
  return d.toISOString().slice(0, 10);
}

function normalizePem(pem: string): string {
  return pem
    .trim()
    .replace(/^"([\s\S]+)"$/, "$1")
    .replace(/\\n/g, "\n");
}

function isFinalizedStatus(statusValue: string | null): boolean {
  const status = (statusValue || "").toLowerCase();
  return (
    status.includes("final") ||
    status.includes("settl") ||
    status.includes("close") ||
    status.includes("resolv") ||
    status.includes("expire")
  );
}

function isTradableStatus(statusValue: string | null): boolean {
  if (!statusValue) return true;
  const status = statusValue.toLowerCase();
  return status.includes("active") || status.includes("open") || status.includes("trade") || status.includes("live");
}

function inferSnapshotType(statusValue: string | null, gameDate: string | null): SnapshotType {
  const status = (statusValue || "").toLowerCase();
  if (isFinalizedStatus(status)) return "settled";
  if (status.includes("live") || status.includes("in_progress") || status.includes("trading") || status.includes("open")) {
    return "live";
  }
  if (gameDate && gameDate < todayUtcDate()) return "settled";
  return "pregame";
}

function parseTeams(title: string | null): { home: string | null; away: string | null } {
  if (!title) return { home: null, away: null };

  const atMatch = title.match(/^(.+?)\s+at\s+(.+)$/i);
  if (atMatch) {
    return { away: atMatch[1].trim(), home: atMatch[2].trim() };
  }

  const vsMatch = title.match(/^(.+?)\s+vs\.?\s+(.+)$/i);
  if (vsMatch) {
    return { away: vsMatch[1].trim(), home: vsMatch[2].trim() };
  }

  return { home: null, away: null };
}

function parseDateFromEventTicker(eventTicker: string): string | null {
  const m = eventTicker.toUpperCase().match(/(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})/);
  if (!m) return null;

  const monthMap: Record<string, number> = {
    JAN: 0, FEB: 1, MAR: 2, APR: 3, MAY: 4, JUN: 5,
    JUL: 6, AUG: 7, SEP: 8, OCT: 9, NOV: 10, DEC: 11,
  };

  const yy = Number(m[1]);
  const mon = monthMap[m[2]];
  const dd = Number(m[3]);
  if (mon === undefined || !Number.isFinite(yy) || !Number.isFinite(dd)) return null;

  const year = 2000 + yy;
  const d = new Date(Date.UTC(year, mon, dd));
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function extractGameKey(value: string | null): string | null {
  if (!value) return null;
  const m = String(value).toUpperCase().match(/(\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}[A-Z0-9]+)/);
  return m ? m[1] : null;
}

function resolveGameDate(eventTicker: string, ...dateHints: Array<any>): string | null {
  const tickerDate = parseDateFromEventTicker(eventTicker);
  if (tickerDate) return tickerDate;

  for (const hint of dateHints) {
    const embeddedDate = parseDateFromEventTicker(String(hint || ""));
    if (embeddedDate) return embeddedDate;
    const normalized = normalizeDateLike(hint);
    if (normalized) return normalized;
  }
  return null;
}

function inferSportLeague(
  seriesTicker: string | null,
  title: string | null,
  category: string | null = null,
  eventTicker: string | null = null,
  marketTicker: string | null = null
): { sport: string | null; league: string | null } {
  const s = (seriesTicker || "").toUpperCase();
  const e = (eventTicker || "").toUpperCase();
  const m = (marketTicker || "").toUpperCase();
  const t = (title || "").toLowerCase();
  const c = (category || "").toLowerCase();
  const em = `${e} ${m}`;

  if (
    s.includes("KXNCAAMB") ||
    em.includes("KXNCAAMB") ||
    em.includes("CBCHAMPIONSHIP")
  ) {
    return { sport: "basketball", league: "ncaamb" };
  }

  if (s.includes("KXNBA") || em.includes("KXNBA")) return { sport: "basketball", league: "nba" };
  if (s.includes("KXNFL") || em.includes("KXNFL")) return { sport: "football", league: "nfl" };
  if (s.includes("KXNHL") || em.includes("KXNHL")) return { sport: "hockey", league: "nhl" };
  if (s.includes("KXMLB") || em.includes("KXMLB")) return { sport: "baseball", league: "mlb" };

  if (
    s.includes("EPL") ||
    s.includes("UCL") ||
    s.includes("MLS") ||
    s.includes("BUND") ||
    s.includes("LIGA") ||
    s.includes("SERIE") ||
    s.includes("SOCCER") ||
    em.includes("KXSOCCER") ||
    em.includes("KXUCL") ||
    em.includes("KXEPL") ||
    em.includes("KXMLS") ||
    em.includes("KXBUND") ||
    em.includes("KXLIGA") ||
    em.includes("KXSERIE") ||
    t.includes("soccer")
  ) {
    return { sport: "soccer", league: "soccer" };
  }

  if (t.includes("football") && (c.includes("pro football") || c.includes("nfl"))) {
    return { sport: "football", league: "nfl" };
  }

  if (t.includes("basketball") || c.includes("basketball")) return { sport: "basketball", league: null };
  if (t.includes("hockey") || c.includes("hockey")) return { sport: "hockey", league: null };
  if (t.includes("baseball") || c.includes("baseball")) return { sport: "baseball", league: null };
  if (t.includes("soccer") || c.includes("soccer")) return { sport: "soccer", league: null };

  return { sport: null, league: null };
}

function mapSportFilter(value: any): SportFilter {
  const s = String(value || "all").toLowerCase();
  if (s === "basketball") return "nba";
  if (s === "hockey") return "nhl";
  if (s === "baseball") return "mlb";
  if (s === "college-basketball" || s === "mens-college-basketball") return "ncaamb";
  if (s === "ncaamb" || s === "nfl" || s === "soccer" || s === "nba" || s === "nhl" || s === "mlb") return s;
  return "all";
}

function matchesSportFilter(filter: SportFilter, sport: string | null, league: string | null): boolean {
  if (filter === "all") return !!sport && ["soccer", "basketball", "hockey", "baseball", "football"].includes(sport);
  if (filter === "soccer") return sport === "soccer" || (league || "").includes("soccer");
  if (filter === "nba") return league === "nba" || (sport === "basketball" && league === "nba");
  if (filter === "ncaamb") return league === "ncaamb" || league === "mens-college-basketball";
  if (filter === "nfl") return league === "nfl" || sport === "football";
  if (filter === "nhl") return league === "nhl" || sport === "hockey";
  if (filter === "mlb") return league === "mlb" || sport === "baseball";
  return true;
}

function discoverySeriesTickers(filter: SportFilter): string[] {
  return DISCOVERY_SERIES_BY_FILTER[filter] || DISCOVERY_SERIES_BY_FILTER.all;
}

function shouldIncludeSeriesTicker(seriesTicker: string): boolean {
  if (!seriesTicker) return false;
  const upper = seriesTicker.toUpperCase();
  if (!upper.startsWith("KX")) return false;
  return (
    upper.includes("TOTAL") ||
    upper.includes("SPREAD") ||
    upper.includes("GAME") ||
    upper.includes("TEAM") ||
    upper.includes("1H") ||
    upper.includes("HALF") ||
    upper.includes("PROP") ||
    upper.includes("PLAYER") ||
    upper.startsWith("KXMVE")
  );
}

async function discoverExtraSeriesTickers(
  keyId: string | null,
  privateKeyPem: string | null,
  sportFilter: SportFilter,
  baseTickers: string[]
): Promise<string[]> {
  const baseSet = new Set(baseTickers.map((t) => t.toUpperCase()));
  const discovered = new Set<string>();
  let cursor: string | null = null;

  for (let page = 0; page < EXTRA_DISCOVERY_MAX_PAGES; page++) {
    const path =
      `/trade-api/v2/events?status=open&limit=${DISCOVERY_PAGE_LIMIT}` +
      `${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
    const res = await kalshiGetWithRetry(path, keyId, privateKeyPem, 2);
    if (!res.ok) break;

    const events = Array.isArray(res.data?.events) ? res.data.events : [];
    for (const eventRow of events) {
      const seriesTicker = getStringField(eventRow, ["series_ticker", "seriesTicker"]);
      if (!seriesTicker) continue;
      const upper = seriesTicker.toUpperCase();
      if (baseSet.has(upper)) continue;

      if (!shouldIncludeSeriesTicker(seriesTicker)) continue;

      const inferred = inferSportLeague(
        seriesTicker,
        getStringField(eventRow, ["title"]),
        getStringField(eventRow, ["category"]) || getStringField(eventRow?.product_metadata, ["competition"]),
        getStringField(eventRow, ["event_ticker", "eventTicker"]),
        null
      );
      if (!matchesSportFilter(sportFilter, inferred.sport, inferred.league)) continue;

      discovered.add(seriesTicker);
    }

    cursor = getStringField(res.data, ["cursor"]);
    if (!cursor || events.length < DISCOVERY_PAGE_LIMIT) break;
    await sleep(REQUEST_DELAY_MS);
  }

  return Array.from(discovered);
}

const VALID_MARKET_KINDS = new Set([
  "game", "spread", "total", "team_total",
  "1h_game", "1h_spread", "1h_total",
  "player_prop", "prop",
]);

function resolveMarketKind(identity: MarketIdentity): string {
  let kind: string;
  if (identity.marketType === "1h_winner") kind = "1h_game";
  else if (identity.marketType === "moneyline") kind = "game";
  else if (identity.marketType === "spread") kind = "spread";
  else if (identity.marketType === "total") kind = "total";
  else if (identity.marketType === "team_total") kind = "team_total";
  else if (identity.marketType === "1h_total") kind = "1h_total";
  else if (identity.marketType === "1h_spread") kind = "1h_spread";
  else if (identity.marketType === "player_prop") kind = "player_prop";
  else kind = identity.marketType || "prop";

  if (!VALID_MARKET_KINDS.has(kind)) {
    console.error(`[INVARIANT] resolveMarketKind produced unknown kind="${kind}" from marketType="${identity.marketType}". Falling back to "prop".`);
    return "prop";
  }
  return kind;
}

function inferHomeTeamSide(yesLabel: string | null, homeTeam: string | null, awayTeam: string | null): boolean | null {
  const label = normalizeTeamKey(yesLabel);
  if (!label) return null;
  const home = normalizeTeamKey(homeTeam);
  const away = normalizeTeamKey(awayTeam);
  if (home && label.includes(home)) return true;
  if (away && label.includes(away)) return false;
  return null;
}

function mapSnapshotWindow(value: any): SnapshotWindow {
  const normalized = String(value || "").toLowerCase().trim();
  if (normalized === "pregame") return "pregame";
  if (normalized === "live") return "live";
  return "default";
}

function normalizeTeamKey(value: string | null): string {
  if (!value) return "";
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\bsaint\b/g, "st")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstTeamToken(value: string): string {
  return value.split(" ").filter(Boolean)[0] || "";
}

function firstTwoTeamTokens(value: string): string {
  return value.split(" ").filter(Boolean).slice(0, 2).join(" ");
}

function teamMatchScore(eventTeamRaw: string | null, matchTeamRaw: string | null): number {
  const eventTeam = normalizeTeamKey(eventTeamRaw);
  const matchTeam = normalizeTeamKey(matchTeamRaw);
  if (!eventTeam || !matchTeam) return 0;

  if (eventTeam === matchTeam) return 100;
  if (matchTeam.includes(eventTeam) || eventTeam.includes(matchTeam)) return 86;

  const eventTwo = firstTwoTeamTokens(eventTeam);
  const matchTwo = firstTwoTeamTokens(matchTeam);
  if (eventTwo && (matchTeam.includes(eventTwo) || eventTwo === matchTwo)) return 74;
  if (matchTwo && eventTeam.includes(matchTwo)) return 70;

  const eventFirst = firstTeamToken(eventTeam);
  const matchFirst = firstTeamToken(matchTeam);
  if (eventFirst && matchFirst && eventFirst === matchFirst) {
    const eventSecond = eventTeam.split(" ")[1] || "";
    const matchSecond = matchTeam.split(" ")[1] || "";
    if (eventSecond && matchSecond && eventSecond[0] === matchSecond[0]) return 66;
    return 52;
  }

  return 0;
}

function isFinalMatchStatus(statusValue: string | null): boolean {
  const status = (statusValue || "").toLowerCase();
  return (
    status.includes("final") ||
    status.includes("post") ||
    status.includes("complete") ||
    status.includes("settled") ||
    status.includes("closed")
  );
}

function isLiveMatchStatus(statusValue: string | null, periodValue: number | null): boolean {
  if (periodValue !== null && periodValue > 0) return true;
  const status = (statusValue || "").toLowerCase();
  return (
    status.includes("in_progress") ||
    status.includes("in progress") ||
    status.includes("live") ||
    status.includes("halftime") ||
    status.includes("first_half") ||
    status.includes("second_half") ||
    status.includes("quarter")
  );
}

function isPregameMatchStatus(statusValue: string | null): boolean {
  const status = (statusValue || "").toLowerCase();
  if (!status) return true;
  return (
    status.includes("scheduled") ||
    status.includes("pre") ||
    status.includes("not_started") ||
    status.includes("status_created")
  );
}

function normalizeLeagueKey(value: string | null): string {
  return (value || "").toLowerCase().trim();
}

function isLeagueCompatible(eventLeagueRaw: string | null, matchLeagueRaw: string | null): boolean {
  const eventLeague = normalizeLeagueKey(eventLeagueRaw);
  const matchLeague = normalizeLeagueKey(matchLeagueRaw);
  if (!eventLeague || !matchLeague) return false;
  if (eventLeague === matchLeague) return true;
  if (eventLeague === "ncaamb" && matchLeague === "mens-college-basketball") return true;
  if (eventLeague === "mens-college-basketball" && matchLeague === "ncaamb") return true;
  return false;
}

function isLikelyTotalMarketTicker(ticker: string): boolean {
  const upper = ticker.toUpperCase();
  return (
    upper.includes("TOTAL") ||
    upper.includes("TEAM") ||
    upper.includes("1H") ||
    upper.includes("HALF") ||
    upper.includes("PROP") ||
    upper.startsWith("KXMVE")
  );
}

async function filterEventsBySnapshotWindow(
  supabase: any,
  eventRows: any[],
  snapshotWindow: SnapshotWindow
): Promise<{ events: any[]; matchedCount: number; matchRowsScanned: number }> {
  if (snapshotWindow === "default" || eventRows.length === 0) {
    return { events: eventRows, matchedCount: eventRows.length, matchRowsScanned: 0 };
  }

  const now = new Date();
  const fromIso =
    snapshotWindow === "pregame"
      ? new Date(now.getTime() - 10 * 60 * 1000).toISOString()
      : new Date(now.getTime() - 6 * 60 * 60 * 1000).toISOString();
  const toIso =
    snapshotWindow === "pregame"
      ? new Date(now.getTime() + 50 * 60 * 1000).toISOString()
      : new Date(now.getTime() + 20 * 60 * 1000).toISOString();

  const { data: matchRows, error: matchErr } = await supabase
    .from("matches")
    .select("id,league_id,sport,home_team,away_team,start_time,status,period")
    .gte("start_time", fromIso)
    .lte("start_time", toIso)
    .limit(1200);

  if (matchErr) {
    return { events: eventRows, matchedCount: eventRows.length, matchRowsScanned: 0 };
  }

  const candidates: MatchWindowRow[] = (matchRows || []).filter((row: any) => {
    if (!row?.start_time) return false;
    if (!row?.home_team || !row?.away_team) return false;
    if (isFinalMatchStatus(row?.status ? String(row.status) : null)) return false;

    if (snapshotWindow === "pregame") {
      return isPregameMatchStatus(row?.status ? String(row.status) : null);
    }

    return isLiveMatchStatus(row?.status ? String(row.status) : null, toInt(row?.period));
  });

  if (candidates.length === 0) {
    return { events: [], matchedCount: 0, matchRowsScanned: 0 };
  }

  const filtered: any[] = [];
  for (const eventRow of eventRows) {
    const eventTicker = String(eventRow?.event_ticker || "");
    const eventLeague = eventRow?.league ? String(eventRow.league) : null;
    const eventDate = normalizeDateLike(eventRow?.game_date);
    const parsedTeams = parseTeams(eventRow?.title ? String(eventRow.title) : null);
    const eventHome = (eventRow?.home_team ? String(eventRow.home_team) : parsedTeams.home) || null;
    const eventAway = (eventRow?.away_team ? String(eventRow.away_team) : parsedTeams.away) || null;

    if (!eventTicker || !eventHome || !eventAway || !eventDate) continue;

    let bestScore = -1;
    for (const match of candidates) {
      if (!isLeagueCompatible(eventLeague, match.league_id ? String(match.league_id) : null)) continue;
      const matchDate = normalizeDateLike(match.start_time);
      if (!matchDate) continue;

      const dateDiff =
        Math.abs(
          (new Date(`${eventDate}T00:00:00Z`).getTime() - new Date(`${matchDate}T00:00:00Z`).getTime()) /
            (24 * 60 * 60 * 1000)
        ) || 0;
      if (dateDiff > 1.1) continue;

      const homeScore = teamMatchScore(eventHome, match.home_team);
      const awayScore = teamMatchScore(eventAway, match.away_team);
      if (homeScore === 0 || awayScore === 0) continue;

      const orientationScore = homeScore + awayScore;
      const dateScore = dateDiff <= 0.1 ? 16 : 10;
      const totalScore = orientationScore + dateScore;
      if (totalScore > bestScore) bestScore = totalScore;
    }

    if (bestScore >= 110) filtered.push(eventRow);
  }

  return {
    events: filtered,
    matchedCount: filtered.length,
    matchRowsScanned: candidates.length,
  };
}

async function applyPregameClosingPriceBackfill(supabase: any, rows: any[]): Promise<{ updated: number; error: string | null }> {
  const payload = rows
    .filter((row) => row?.snapshot_type === "pregame" && row?.yes_price !== null && row?.yes_price !== undefined)
    .map((row) => ({
      market_ticker: row.market_ticker,
      closing_price: row.yes_price,
      captured_at: row.captured_at,
    }));

  if (payload.length === 0) return { updated: 0, error: null };

  const { data, error } = await supabase.rpc("apply_kalshi_closing_prices_from_snapshots", {
    p_rows: payload,
  });

  if (error) return { updated: 0, error: error.message };
  return { updated: Number(data || 0), error: null };
}

function parseLevel(raw: any): { price: number; qty: number } | null {
  if (Array.isArray(raw)) {
    const price = normalizeProbPrice(raw[0]);
    const qty = toInt(raw[1] ?? raw[2]);
    if (price === null || qty === null) return null;
    return { price, qty };
  }

  const price = normalizeProbPrice(raw?.price ?? raw?.yes_price_dollars ?? raw?.no_price_dollars ?? raw?.bid ?? raw?.px);
  const qty = toInt(raw?.qty ?? raw?.quantity ?? raw?.count ?? raw?.size ?? raw?.volume ?? raw?.contracts);
  if (price === null || qty === null) return null;
  return { price, qty };
}

function parseSideLevels(rawSide: any): Array<{ price: number; qty: number }> {
  if (!Array.isArray(rawSide)) return [];
  return rawSide
    .map(parseLevel)
    .filter((lvl): lvl is { price: number; qty: number } => lvl !== null)
    .sort((a, b) => b.price - a.price)
    .slice(0, 5);
}

function parseOrderbookPayload(payload: any) {
  const root = payload?.orderbook ?? payload?.data?.orderbook ?? payload ?? {};

  const yesRaw =
    root?.yes ??
    root?.yes_bids ??
    root?.yes_levels ??
    root?.bids_yes ??
    root?.orderbook_fp?.yes_dollars ??
    payload?.orderbook_fp?.yes_dollars ??
    [];

  const noRaw =
    root?.no ??
    root?.no_bids ??
    root?.no_levels ??
    root?.bids_no ??
    root?.orderbook_fp?.no_dollars ??
    payload?.orderbook_fp?.no_dollars ??
    [];

  const yesLevels = parseSideLevels(yesRaw);
  const noLevels = parseSideLevels(noRaw);

  const yesTotal = yesLevels.reduce((sum, level) => sum + level.qty, 0);
  const noTotal = noLevels.reduce((sum, level) => sum + level.qty, 0);
  const totalDepth = yesTotal + noTotal;
  const yesBestBid = yesLevels[0]?.price ?? null;
  const noBestBid = noLevels[0]?.price ?? null;

  const midPrice =
    yesBestBid !== null && noBestBid !== null
      ? Number(((yesBestBid + (1 - noBestBid)) / 2).toFixed(6))
      : null;

  const spreadWidth =
    yesBestBid !== null && noBestBid !== null
      ? Number((yesBestBid + noBestBid - 1).toFixed(6))
      : null;

  return {
    yesLevels,
    noLevels,
    yesBestBid,
    yesBestBidQty: yesLevels[0]?.qty ?? null,
    yesTotalBidQty: yesTotal || null,
    noBestBid,
    noBestBidQty: noLevels[0]?.qty ?? null,
    noTotalBidQty: noTotal || null,
    yesNoImbalance: totalDepth > 0 ? Number((yesTotal / totalDepth).toFixed(6)) : null,
    midPrice,
    spreadWidth,
  };
}

function parseTradesPayload(payload: any) {
  const tradesRaw = Array.isArray(payload?.trades)
    ? payload.trades
    : Array.isArray(payload?.data?.trades)
    ? payload.data.trades
    : [];

  const trades = tradesRaw
    .map((trade: any) => {
      const sideRaw = String(trade?.taker_side ?? trade?.side ?? "").toLowerCase();
      const side = sideRaw === "yes" || sideRaw === "no" ? sideRaw : null;
      const qty = toInt(trade?.count_fp ?? trade?.count ?? trade?.quantity ?? trade?.qty) || 0;
      const yesPrice = normalizeProbPrice(trade?.yes_price_dollars ?? trade?.yes_price);
      const noPrice = normalizeProbPrice(trade?.no_price_dollars ?? trade?.no_price);
      const genericPrice = normalizeProbPrice(trade?.price);
      const tradePrice =
        side === "yes"
          ? yesPrice ?? genericPrice
          : side === "no"
          ? noPrice ?? genericPrice
          : genericPrice ?? yesPrice ?? noPrice;
      const createdAt = String(trade?.created_time ?? trade?.created_at ?? "");
      const createdTs = new Date(createdAt).getTime();
      return {
        side,
        qty,
        tradePrice,
        tradeTime: createdAt || null,
        createdTs: Number.isFinite(createdTs) ? createdTs : 0,
      };
    })
    .sort((a, b) => b.createdTs - a.createdTs)
    .slice(0, 50);

  const yesVolume = trades.reduce((sum, t) => sum + (t.side === "yes" ? t.qty : 0), 0);
  const noVolume = trades.reduce((sum, t) => sum + (t.side === "no" ? t.qty : 0), 0);
  const total = yesVolume + noVolume;

  return {
    recentTradeCount: trades.length,
    recentYesVolume: yesVolume,
    recentNoVolume: noVolume,
    recentVolumeImbalance: total > 0 ? Number((yesVolume / total).toFixed(6)) : null,
    lastTradePrice: trades[0]?.tradePrice ?? null,
    lastTradeSide: trades[0]?.side ?? null,
    lastTradeAt: trades[0]?.tradeTime ?? null,
  };
}

function parseLineValue(...texts: Array<string | null>): number | null {
  for (const text of texts) {
    if (!text) continue;
    const m = text.match(/([+-]?\d+(?:\.\d+)?)/);
    if (m) {
      const v = Number(m[1]);
      if (Number.isFinite(v)) return v;
    }
  }
  return null;
}

function parseLineSide(text: string | null): string | null {
  const low = (text || "").toLowerCase();
  if (!low) return null;
  if (low.includes("over")) return "over";
  if (low.includes("under")) return "under";
  if (low.includes("draw") || low.includes("tie")) return "draw";
  if (low.includes("home")) return "home";
  if (low.includes("away")) return "away";
  if (low.includes("yes")) return "yes";
  if (low.includes("no")) return "no";
  return null;
}

function inferTeamNameFromText(text: string, homeTeam: string | null, awayTeam: string | null): string | null {
  const candidates = [homeTeam, awayTeam].filter(Boolean) as string[];
  const hay = String(text || "");
  let bestScore = 0;
  let bestTeam: string | null = null;

  for (const candidate of candidates) {
    const score = teamMatchScore(candidate, hay);
    if (score > bestScore) {
      bestScore = score;
      bestTeam = candidate;
    }
  }

  if (bestTeam && bestScore >= 66) return bestTeam;

  const prefix = hay.match(/^([A-Za-z][A-Za-z .&'-]+?)\s+(scores?|to score|scored)\b/i);
  if (prefix) return prefix[1].trim();

  const byMatch = hay.match(/\b(?:by|for)\s+([A-Za-z][A-Za-z .&'-]+)$/i);
  if (byMatch) return byMatch[1].trim();

  return null;
}

function extractTeamTotalDetails(
  labels: Array<string | null>,
  homeTeam: string | null,
  awayTeam: string | null
): { teamName: string | null; lineValue: number | null; lineSide: string | null } {
  const cleanedLabels = labels.filter(Boolean) as string[];
  let teamName: string | null = null;
  for (const label of cleanedLabels) {
    teamName = inferTeamNameFromText(label, homeTeam, awayTeam);
    if (teamName) break;
  }
  if (!teamName) {
    teamName = inferTeamNameFromText(cleanedLabels.join(" "), homeTeam, awayTeam);
  }
  return {
    teamName,
    lineValue: parseLineValue(...labels),
    lineSide: parseLineSide(cleanedLabels[0] || cleanedLabels[1] || cleanedLabels[2] || null),
  };
}

function classifyMarketIdentity(marketTicker: string, market: any): MarketIdentity {
  const title = getStringField(market, ["title"]);
  const subtitle = getStringField(market, ["subtitle", "yes_sub_title", "no_sub_title"]);
  const yesLabel = getStringField(market, ["yes_sub_title"]);
  const noLabel = getStringField(market, ["no_sub_title"]);

  const text = [marketTicker, title, subtitle, yesLabel, noLabel].filter(Boolean).join(" ").toLowerCase();

  let marketType = "prop";

  const isHalf = text.includes("1h") || text.includes("1st half") || text.includes("first half") || text.includes("halftime");
  const hasOverUnder = text.includes("over") || text.includes("under") || text.includes("total");
  const hasSpread = text.includes("spread") || text.includes(" by ");
  const tickerUpper = marketTicker.toUpperCase();
  const hasTeamTicker = tickerUpper.includes("TEAM") || /KX[A-Z]{2,6}TT/.test(tickerUpper);
  const hasTeamReference =
    hasTeamTicker ||
    text.includes("team total") ||
    text.includes("team points") ||
    text.includes("team runs") ||
    text.includes("team goals") ||
    text.includes("scores") ||
    text.includes("to score") ||
    /\b(points|goals|runs)\s+(by|for|scored by)\b/.test(text);
  const isPlayerProp =
    tickerUpper.includes("KXMVE") ||
    tickerUpper.includes("PROP") ||
    /\b(player|rebounds|assists|strikeouts|passing|rushing|receiving|shots on goal|saves)\b/.test(text);

  if (isPlayerProp) {
    marketType = "player_prop";
  } else if (isHalf && hasSpread) {
    marketType = "1h_spread";
  } else if (isHalf && hasOverUnder) {
    marketType = "1h_total";
  } else if (isHalf) {
    marketType = "1h_winner";
  } else if (text.includes("winner") || text.includes(" to win") || text.includes(" game")) {
    marketType = "moneyline";
  } else if (hasSpread) {
    marketType = "spread";
  } else if (hasOverUnder && hasTeamReference) {
    marketType = "team_total";
  } else if (hasOverUnder) {
    marketType = "total";
  } else if (text.includes("points") || text.includes("reb") || text.includes("ast") || text.includes("player")) {
    marketType = "prop";
  }

  const marketLabel = yesLabel || title || subtitle || marketTicker;
  const lineValue = parseLineValue(yesLabel, noLabel, subtitle, title);
  const lineSide = parseLineSide(yesLabel || subtitle || title);

  return { marketType, marketLabel, lineValue, lineSide, teamName: null };
}

function marketPriorityKey(marketTicker: string): number {
  const t = marketTicker.toUpperCase();
  if (t.includes("GAME")) return 1;
  if (t.includes("TOTAL")) return 2;
  if (t.includes("SPREAD")) return 3;
  if (t.includes("1H")) return 4;
  return 5;
}

async function signKalshiMessage(message: string, privateKeyPem: string): Promise<string> {
  const normalizedPem = normalizePem(privateKeyPem);
  const signer = createSign("RSA-SHA256");
  signer.update(message);
  signer.end();
  return signer.sign(normalizedPem, "base64");
}

async function kalshiGet(
  pathWithQuery: string,
  keyId: string | null,
  privateKeyPem: string | null
): Promise<{ ok: true; data: any } | { ok: false; status: number; error: string }> {
  try {
    const runRequest = async (signed: boolean) => {
      const headers: Record<string, string> = {};
      if (signed && keyId && privateKeyPem) {
        const ts = Date.now().toString();
        const signature = await signKalshiMessage(`${ts}GET${pathWithQuery}`, privateKeyPem);
        headers["KALSHI-ACCESS-KEY"] = keyId;
        headers["KALSHI-ACCESS-TIMESTAMP"] = ts;
        headers["KALSHI-ACCESS-SIGNATURE"] = signature;
      }
      return fetch(`${KALSHI_BASE_URL}${pathWithQuery}`, {
        headers,
        signal: AbortSignal.timeout(12000),
      });
    };

    let res = await runRequest(false);
    if ((res.status === 401 || res.status === 403) && keyId && privateKeyPem) {
      res = await runRequest(true);
    }

    if (!res.ok) {
      return { ok: false, status: res.status, error: await res.text() };
    }

    return { ok: true, data: await res.json() };
  } catch (err: any) {
    return { ok: false, status: 0, error: err?.message || String(err) };
  }
}

async function kalshiGetWithRetry(
  pathWithQuery: string,
  keyId: string | null,
  privateKeyPem: string | null,
  maxRetries = 2
): Promise<{ ok: true; data: any } | { ok: false; status: number; error: string }> {
  let attempt = 0;
  let lastRes: { ok: true; data: any } | { ok: false; status: number; error: string } = {
    ok: false,
    status: 0,
    error: "request_not_attempted",
  };

  while (attempt <= maxRetries) {
    lastRes = await kalshiGet(pathWithQuery, keyId, privateKeyPem);
    if (lastRes.ok) return lastRes;

    const shouldRetry = lastRes.status === 429 || lastRes.status === 503 || lastRes.status === 504;
    if (!shouldRetry || attempt >= maxRetries) return lastRes;

    await sleep(500 * (attempt + 1));
    attempt++;
  }

  return lastRes;
}

async function fetchEventPayload(eventTicker: string, keyId: string | null, privateKeyPem: string | null) {
  return kalshiGetWithRetry(`/trade-api/v2/events/${encodeURIComponent(eventTicker)}`, keyId, privateKeyPem, 3);
}

async function discoverPhase(
  supabase: any,
  keyId: string | null,
  privateKeyPem: string | null,
  sportFilter: SportFilter,
  eventTickersOverride: string[],
  maxEvents: number
) {
  const stats = {
    event_candidates: 0,
    events_processed: 0,
    events_upserted: 0,
    line_markets_upserted: 0,
    events_skipped: 0,
    series_tickers: [] as string[],
    extra_series: [] as string[],
    errors: [] as string[],
  };

  const eventTickers = new Set<string>();
  const today = todayUtcDate();
  const windowStart = shiftUtcDate(today, -1);
  const windowEnd = shiftUtcDate(today, 2);

  if (eventTickersOverride.length > 0) {
    for (const eventTicker of eventTickersOverride) eventTickers.add(eventTicker);
  } else {
    const baseSeriesTickers = discoverySeriesTickers(sportFilter);
    const extraSeriesTickers = await discoverExtraSeriesTickers(keyId, privateKeyPem, sportFilter, baseSeriesTickers);
    const seriesTickers = Array.from(new Set([...baseSeriesTickers, ...extraSeriesTickers]));
    stats.series_tickers = seriesTickers;
    stats.extra_series = extraSeriesTickers;

    for (const seriesTicker of seriesTickers) {
      let cursor: string | null = null;

      for (let page = 0; page < DISCOVERY_MAX_PAGES; page++) {
        const path =
          `/trade-api/v2/events?status=open&series_ticker=${encodeURIComponent(seriesTicker)}&limit=${DISCOVERY_PAGE_LIMIT}` +
          `${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;

        const res = await kalshiGetWithRetry(path, keyId, privateKeyPem, 3);
        if (!res.ok) {
          stats.errors.push(`events_discovery:${seriesTicker}:${res.status}:${res.error.slice(0, 180)}`);
          break;
        }

        const events = Array.isArray(res.data?.events) ? res.data.events : [];
        for (const eventRow of events) {
          const eventTicker = getStringField(eventRow, ["event_ticker", "eventTicker"]);
          if (!eventTicker || !eventTicker.toUpperCase().startsWith("KX")) continue;

          const inferred = inferSportLeague(
            getStringField(eventRow, ["series_ticker", "seriesTicker"]) || seriesTicker,
            getStringField(eventRow, ["title"]),
            getStringField(eventRow, ["category"]) || getStringField(eventRow?.product_metadata, ["competition"]),
            eventTicker,
            null
          );
          if (!matchesSportFilter(sportFilter, inferred.sport, inferred.league)) continue;

          const eventDate = resolveGameDate(
            eventTicker,
            getStringField(eventRow, ["sub_title", "subTitle"]),
            getStringField(eventRow, ["expected_expiration_time", "expiration_time", "close_time", "open_time"])
          );
          if (!eventDate || eventDate < windowStart || eventDate > windowEnd) continue;

          eventTickers.add(eventTicker);
        }

        cursor = getStringField(res.data, ["cursor"]);
        if (!cursor || events.length < DISCOVERY_PAGE_LIMIT) break;
        await sleep(REQUEST_DELAY_MS);
      }
    }
  }

  let discoveryCandidates = Array.from(eventTickers).slice(0, maxEvents);

  if (discoveryCandidates.length === 0 && eventTickersOverride.length === 0) {
    const { data: fallbackRows, error: fallbackErr } = await supabase
      .from("kalshi_line_markets")
      .select("event_ticker,sport,league,game_date,status")
      .order("game_date", { ascending: false })
      .limit(1200);

    if (fallbackErr) {
      stats.errors.push(`fallback_line_markets_lookup:${fallbackErr.message}`);
    } else {
      discoveryCandidates = Array.from(
        new Set(
          (fallbackRows || [])
            .filter((row: any) => {
              const inferredRow = inferSportLeague(null, null, null, String(row?.event_ticker || ""));
              const sport = row?.sport ? String(row.sport).toLowerCase() : inferredRow.sport;
              const league = row?.league ? String(row.league).toLowerCase() : inferredRow.league;
              if (!matchesSportFilter(sportFilter, sport, league)) return false;
              if (isFinalizedStatus(row?.status ? String(row.status) : null)) return false;

              const rowDate = resolveGameDate(
                String(row?.event_ticker || ""),
                row?.game_date
              );
              if (!rowDate) return true;
              return rowDate >= windowStart && rowDate <= windowEnd;
            })
            .map((row: any) => String(row.event_ticker || "").trim())
            .filter(Boolean)
        )
      ).slice(0, maxEvents);
    }
  }

  stats.event_candidates = discoveryCandidates.length;

  const rows: EventRow[] = [];
  const lineMarketRows: any[] = [];

  for (const eventTicker of discoveryCandidates) {
    stats.events_processed++;
    await sleep(REQUEST_DELAY_MS);

    const ev = await fetchEventPayload(eventTicker, keyId, privateKeyPem);
    if (!ev.ok) {
      stats.errors.push(`${eventTicker}:event:${ev.status}:${ev.error.slice(0, 180)}`);
      stats.events_skipped++;
      continue;
    }

    const eventObj = ev.data?.event || {};
    const markets = Array.isArray(ev.data?.markets) ? [...ev.data.markets] : [];
    const seriesTicker = getStringField(eventObj, ["series_ticker", "seriesTicker"]);
    const title = getStringField(eventObj, ["title"]);
    const firstMarket = markets[0] || null;
    const firstMarketTicker = getStringField(firstMarket, ["ticker", "market_ticker"]);

    const inferred = inferSportLeague(
      seriesTicker,
      title,
      getStringField(eventObj, ["category"]) || getStringField(eventObj?.product_metadata, ["competition"]),
      eventTicker,
      firstMarketTicker
    );

    if (!matchesSportFilter(sportFilter, inferred.sport, inferred.league)) {
      stats.events_skipped++;
      continue;
    }

    const { home, away } = parseTeams(title);
    const marketTickers = markets
      .map((m: any) => getStringField(m, ["ticker", "market_ticker"]))
      .filter((t: string | null): t is string => !!t);

    if (marketTickers.length === 0) {
      await sleep(REQUEST_DELAY_MS);
      const lookupRes = await kalshiGetWithRetry(
        `/trade-api/v2/markets?event_ticker=${encodeURIComponent(eventTicker)}&limit=500`,
        keyId,
        privateKeyPem,
        3
      );
      if (lookupRes.ok) {
        const lookupMarkets = Array.isArray(lookupRes.data?.markets) ? lookupRes.data.markets : [];
        for (const m of lookupMarkets) {
          markets.push(m);
          const t = getStringField(m, ["ticker", "market_ticker"]);
          if (t) marketTickers.push(t);
        }
      } else {
        stats.errors.push(`${eventTicker}:markets_lookup:${lookupRes.status}:${lookupRes.error.slice(0, 180)}`);
      }
    }

    const uniqueMarketTickers = Array.from(new Set(marketTickers));
    if (uniqueMarketTickers.length === 0) {
      stats.events_skipped++;
      continue;
    }

    const gameDate = resolveGameDate(
      eventTicker,
      firstMarketTicker,
      getStringField(eventObj, ["expected_expiration_time", "expiration_time", "open_time"]),
      getStringField(firstMarket, ["expected_expiration_time", "expiration_time", "open_time", "close_time"])
    );

    if (!gameDate) {
      stats.events_skipped++;
      continue;
    }

    rows.push({
      event_ticker: eventTicker,
      sport: inferred.sport,
      league: inferred.league,
      title,
      home_team: home,
      away_team: away,
      game_date: gameDate,
      market_count: uniqueMarketTickers.length,
      market_tickers: uniqueMarketTickers,
      status: "active",
    });

    const seenMarketTickers = new Set<string>();
    for (const market of markets) {
      const marketTicker = getStringField(market, ["ticker", "market_ticker"]);
      if (!marketTicker || seenMarketTickers.has(marketTicker)) continue;
      seenMarketTickers.add(marketTicker);

      const identity = classifyMarketIdentity(marketTicker, market);
      const yesLabel = getStringField(market, ["yes_sub_title", "yes_subtitle", "yes_label"]);
      const noLabel = getStringField(market, ["no_sub_title", "no_subtitle", "no_label"]);
      const marketTitle = getStringField(market, ["title"]) || title;
      const marketSubtitle =
        getStringField(market, ["subtitle", "sub_title"]) ||
        getStringField(eventObj, ["sub_title", "subTitle"]);
      const marketGameDate =
        resolveGameDate(
          eventTicker,
          marketTicker,
          getStringField(market, ["expected_expiration_time", "expiration_time", "close_time", "open_time"]),
          gameDate
        ) || gameDate;

      if (!marketGameDate) continue;

      const marketInferred = inferSportLeague(
        getStringField(market, ["series_ticker", "seriesTicker"]) || seriesTicker,
        marketTitle,
        getStringField(market, ["category"]) ||
          getStringField(market?.product_metadata, ["competition"]) ||
          getStringField(eventObj, ["category"]) ||
          getStringField(eventObj?.product_metadata, ["competition"]),
        eventTicker,
        marketTicker
      );

      const settlementPrice = normalizeProbPrice(
        market?.settlement_value_dollars ??
          market?.settlement_price ??
          market?.expiration_value_dollars ??
          market?.last_price_dollars
      );
      const settlementValue = toNum(
        market?.settlement_value ??
          market?.expiration_value ??
          market?.settlement_result ??
          market?.result_value
      );

      const teamTotalDetails = identity.marketType === "team_total"
        ? extractTeamTotalDetails([yesLabel, noLabel, marketSubtitle, marketTitle], home, away)
        : null;

      lineMarketRows.push({
        event_ticker: eventTicker,
        series_ticker: seriesTicker,
        market_ticker: marketTicker,
        sport: marketInferred.sport || inferred.sport,
        league: marketInferred.league || inferred.league,
        market_kind: resolveMarketKind(identity),
        title: marketTitle,
        subtitle: marketSubtitle,
        team_name: teamTotalDetails?.teamName || yesLabel || identity.marketLabel,
        opponent_name: noLabel,
        is_home_team: inferHomeTeamSide(teamTotalDetails?.teamName || yesLabel || identity.marketLabel, home, away),
        line_value: teamTotalDetails?.lineValue ?? identity.lineValue,
        line_side: teamTotalDetails?.lineSide ?? identity.lineSide ?? yesLabel ?? identity.marketLabel,
        game_date: marketGameDate,
        settlement_price: settlementPrice,
        settlement_value: settlementValue,
        result: getStringField(market, ["result", "outcome"]),
        volume: toInt(market?.volume_fp ?? market?.volume),
        open_interest: toInt(market?.open_interest_fp ?? market?.open_interest),
        status: getStringField(market, ["status"]) || getStringField(eventObj, ["status"]) || "open",
        raw_json: {
          event: eventObj,
          market,
        },
      });
    }
  }

  if (rows.length > 0) {
    const { error } = await supabase
      .from("kalshi_events_active")
      .upsert(rows, { onConflict: "event_ticker" });

    if (error) {
      stats.errors.push(`events_upsert:${error.message}`);
    } else {
      stats.events_upserted = rows.length;
    }
  }

  if (lineMarketRows.length > 0) {
    const dedupedLineRows = Array.from(
      lineMarketRows.reduce((acc, row) => {
        const key = String(row.market_ticker || "").trim();
        if (!key) return acc;
        acc.set(key, row);
        return acc;
      }, new Map<string, any>()).values()
    );

    const { error } = await supabase
      .from("kalshi_line_markets")
      .upsert(dedupedLineRows, { onConflict: "market_ticker" });

    if (error) {
      stats.errors.push(`line_markets_upsert:${error.message}`);
    } else {
      stats.line_markets_upserted = dedupedLineRows.length;
    }
  }

  return stats;
}

async function snapshotPhase(
  supabase: any,
  keyId: string | null,
  privateKeyPem: string | null,
  sportFilter: SportFilter,
  eventTickersOverride: string[],
  maxMarkets: number,
  snapshotWindow: SnapshotWindow
) {
  const stats = {
    snapshot_window: snapshotWindow,
    selected_events: 0,
    selected_events_after_window_filter: 0,
    match_rows_scanned: 0,
    selected_markets: 0,
    processed_markets: 0,
    rows_inserted: 0,
    closing_prices_updated: 0,
    skipped_markets: 0,
    errors: [] as string[],
  };

  let eventRows: any[] = [];

  if (eventTickersOverride.length > 0) {
    const { data } = await supabase
      .from("kalshi_events_active")
      .select("event_ticker,sport,league,game_date,market_tickers,status")
      .in("event_ticker", eventTickersOverride);

    eventRows = data || [];

    const missing = eventTickersOverride.filter((et) => !eventRows.some((row) => row.event_ticker === et));
    for (const eventTicker of missing) {
      await sleep(REQUEST_DELAY_MS);
      const ev = await fetchEventPayload(eventTicker, keyId, privateKeyPem);
      if (!ev.ok) {
        stats.errors.push(`${eventTicker}:event_for_snapshot:${ev.status}:${ev.error.slice(0, 180)}`);
        continue;
      }
      const eventObj = ev.data?.event || {};
      const markets = Array.isArray(ev.data?.markets) ? ev.data.markets : [];
      const seriesTicker = getStringField(eventObj, ["series_ticker", "seriesTicker"]);
      const firstMarket = markets[0] || null;
      const inferred = inferSportLeague(
        seriesTicker,
        getStringField(eventObj, ["title"]),
        getStringField(eventObj, ["category"]) || getStringField(eventObj?.product_metadata, ["competition"]),
        eventTicker,
        getStringField(firstMarket, ["ticker", "market_ticker"])
      );
      eventRows.push({
        event_ticker: eventTicker,
        sport: inferred.sport,
        league: inferred.league,
        game_date: resolveGameDate(
          eventTicker,
          getStringField(firstMarket, ["ticker", "market_ticker"]),
          getStringField(eventObj, ["expected_expiration_time", "expiration_time", "open_time"])
        ),
        market_tickers: markets
          .map((m: any) => getStringField(m, ["ticker", "market_ticker"]))
          .filter((t: string | null): t is string => !!t),
        status: "active",
      });
    }
  } else {
    const { data, error } = await supabase
      .from("kalshi_events_active")
      .select("event_ticker,sport,league,game_date,market_tickers,status")
      .eq("status", "active")
      .limit(500);

    if (error) {
      stats.errors.push(`active_events_lookup:${error.message}`);
      return stats;
    }
    eventRows = data || [];
  }

  const today = todayUtcDate();
  const windowStart = shiftUtcDate(today, -1);
  const windowEnd = shiftUtcDate(today, 2);

  const normalizedEvents = eventRows.map((row) => {
    const ticker = String(row?.event_ticker || "");
    const normalizedDate = resolveGameDate(ticker, row?.game_date);
    return {
      ...row,
      event_ticker: ticker,
      game_date: normalizedDate,
    };
  });

  const staleDateFixes = eventRows
    .map((row) => {
      const eventTicker = String(row?.event_ticker || "");
      const inferred = inferSportLeague(null, null, null, eventTicker);
      return {
        eventTicker,
        currentSport: row?.sport ? String(row.sport).toLowerCase() : null,
        currentLeague: row?.league ? String(row.league).toLowerCase() : null,
        currentDate: normalizeDateLike(row?.game_date),
        parsedDate: parseDateFromEventTicker(eventTicker),
        inferredSport: inferred.sport,
        inferredLeague: inferred.league,
      };
    })
    .filter((row) => {
      const dateNeedsFix = !!row.parsedDate && row.parsedDate !== row.currentDate;
      const sportNeedsFix = !row.currentSport && !!row.inferredSport;
      const leagueNeedsFix = !row.currentLeague && !!row.inferredLeague;
      return dateNeedsFix || sportNeedsFix || leagueNeedsFix;
    });

  for (const row of staleDateFixes) {
    const patch: Record<string, any> = {};
    if (row.parsedDate && row.parsedDate !== row.currentDate) patch.game_date = row.parsedDate;
    if (!row.currentSport && row.inferredSport) patch.sport = row.inferredSport;
    if (!row.currentLeague && row.inferredLeague) patch.league = row.inferredLeague;
    if (Object.keys(patch).length === 0) continue;

    await supabase
      .from("kalshi_events_active")
      .update(patch)
      .eq("event_ticker", row.eventTicker);
  }

  const baseFilteredEvents = normalizedEvents.filter((row) => {
    if (!matchesSportFilter(sportFilter, row?.sport || null, row?.league || null)) return false;

    const gameDate = normalizeDateLike(row?.game_date);
    if (!gameDate) return false;
    return gameDate >= windowStart && gameDate <= windowEnd;
  });

  stats.selected_events = baseFilteredEvents.length;

  let filteredEvents = baseFilteredEvents;
  if (snapshotWindow !== "default") {
    const windowResult = await filterEventsBySnapshotWindow(supabase, baseFilteredEvents, snapshotWindow);
    filteredEvents = windowResult.events;
    stats.match_rows_scanned = windowResult.matchRowsScanned;
  }

  stats.selected_events_after_window_filter = filteredEvents.length;

  const marketCandidates: CandidateMarket[] = [];
  const preferTotals = snapshotWindow === "pregame" || snapshotWindow === "live";
  for (const ev of filteredEvents) {
    const tickers = Array.isArray(ev.market_tickers) ? ev.market_tickers : [];
    for (const ticker of tickers) {
      if (!ticker) continue;
      const marketTicker = String(ticker);
      if (preferTotals && !isLikelyTotalMarketTicker(marketTicker)) continue;
      marketCandidates.push({
        eventTicker: String(ev.event_ticker),
        marketTicker,
        sport: ev.sport || null,
        league: ev.league || null,
        gameDate: normalizeDateLike(ev.game_date),
      });
    }
  }

  const uniqueMap = new Map<string, CandidateMarket>();
  for (const m of marketCandidates) {
    if (!uniqueMap.has(m.marketTicker)) uniqueMap.set(m.marketTicker, m);
  }

  const prioritized = Array.from(uniqueMap.values())
    .sort((a, b) => {
      if (a.eventTicker !== b.eventTicker) return a.eventTicker.localeCompare(b.eventTicker);
      return marketPriorityKey(a.marketTicker) - marketPriorityKey(b.marketTicker);
    })
    .slice(0, maxMarkets);

  stats.selected_markets = prioritized.length;

  const rows: any[] = [];
  const touchedEventTickers = new Set<string>();

  for (const candidate of prioritized) {
    stats.processed_markets++;
    const tickerEncoded = encodeURIComponent(candidate.marketTicker);

    await sleep(REQUEST_DELAY_MS);
    const marketRes = await kalshiGetWithRetry(`/trade-api/v2/markets/${tickerEncoded}`, keyId, privateKeyPem, 2);
    if (!marketRes.ok) {
      stats.errors.push(`${candidate.marketTicker}:market:${marketRes.status}:${marketRes.error.slice(0, 180)}`);
      stats.skipped_markets++;
      continue;
    }

    const market = marketRes.data?.market ?? marketRes.data ?? {};
    const marketInferred = inferSportLeague(
      getStringField(market, ["series_ticker", "seriesTicker"]),
      getStringField(market, ["title"]),
      getStringField(market, ["category"]) || getStringField(market?.product_metadata, ["competition"]),
      candidate.eventTicker,
      candidate.marketTicker
    );
    const status = getStringField(market, ["status"]);
    const snapshotType =
      snapshotWindow === "pregame"
        ? "pregame"
        : snapshotWindow === "live"
        ? "live"
        : inferSnapshotType(status, candidate.gameDate);
    const identity = classifyMarketIdentity(candidate.marketTicker, market);

    let ob: any = {
      yesLevels: [],
      noLevels: [],
      yesBestBid: null,
      yesBestBidQty: null,
      yesTotalBidQty: null,
      noBestBid: null,
      noBestBidQty: null,
      noTotalBidQty: null,
      yesNoImbalance: null,
      midPrice: null,
      spreadWidth: null,
    };

    let tr: any = {
      recentTradeCount: 0,
      recentYesVolume: 0,
      recentNoVolume: 0,
      recentVolumeImbalance: null,
      lastTradePrice: null,
      lastTradeSide: null,
      lastTradeAt: null,
    };

    if (!isFinalizedStatus(status)) {
      await sleep(REQUEST_DELAY_MS);
      const orderbookRes = await kalshiGetWithRetry(`/trade-api/v2/markets/${tickerEncoded}/orderbook`, keyId, privateKeyPem, 2);
      if (!orderbookRes.ok) {
        stats.errors.push(`${candidate.marketTicker}:orderbook:${orderbookRes.status}:${orderbookRes.error.slice(0, 180)}`);
        stats.skipped_markets++;
        continue;
      }
      ob = parseOrderbookPayload(orderbookRes.data);

      await sleep(REQUEST_DELAY_MS);
      let tradesRes = await kalshiGetWithRetry(`/trade-api/v2/markets/${tickerEncoded}/trades?limit=50`, keyId, privateKeyPem, 2);
      if (!tradesRes.ok && tradesRes.status === 404) {
        await sleep(REQUEST_DELAY_MS);
        tradesRes = await kalshiGetWithRetry(`/trade-api/v2/markets/trades?ticker=${tickerEncoded}&limit=50`, keyId, privateKeyPem, 2);
      }

      if (tradesRes.ok) {
        tr = parseTradesPayload(tradesRes.data);
      } else {
        stats.errors.push(`${candidate.marketTicker}:trades:${tradesRes.status}:${tradesRes.error.slice(0, 180)}`);
      }
    }

    const yesPrice = normalizeProbPrice(
      market?.last_price_dollars ?? market?.yes_bid_dollars ?? market?.yes_ask_dollars
    );
    let noPrice = normalizeProbPrice(market?.no_bid_dollars ?? market?.no_ask_dollars);
    if (noPrice === null && yesPrice !== null) noPrice = Number((1 - yesPrice).toFixed(6));

    rows.push({
      event_ticker: candidate.eventTicker,
      market_ticker: candidate.marketTicker,
      sport: candidate.sport || marketInferred.sport,
      league: candidate.league || marketInferred.league,

      market_type: identity.marketType,
      market_label: identity.marketLabel,
      line_value: identity.lineValue,
      line_side: identity.lineSide,

      snapshot_type: snapshotType,

      yes_best_bid: ob.yesBestBid,
      yes_best_bid_qty: ob.yesBestBidQty,
      yes_total_bid_qty: ob.yesTotalBidQty,
      yes_depth_levels: ob.yesLevels,

      no_best_bid: ob.noBestBid,
      no_best_bid_qty: ob.noBestBidQty,
      no_total_bid_qty: ob.noTotalBidQty,
      no_depth_levels: ob.noLevels,

      mid_price: ob.midPrice,
      spread_width: ob.spreadWidth,
      yes_no_imbalance: ob.yesNoImbalance,

      recent_trade_count: tr.recentTradeCount,
      recent_yes_volume: tr.recentYesVolume,
      recent_no_volume: tr.recentNoVolume,
      recent_volume_imbalance: tr.recentVolumeImbalance,
      last_trade_price: tr.lastTradePrice,
      last_trade_side: tr.lastTradeSide,
      last_trade_at: tr.lastTradeAt,

      volume: toInt(market?.volume_fp ?? market?.volume),
      open_interest: toInt(market?.open_interest_fp ?? market?.open_interest),
      yes_price: yesPrice,
      no_price: noPrice,

      captured_at: new Date().toISOString(),
    });

    touchedEventTickers.add(candidate.eventTicker);
  }

  if (rows.length > 0) {
    const { error } = await supabase
      .from("kalshi_orderbook_snapshots")
      .insert(rows);

    if (error) {
      stats.errors.push(`snapshot_insert:${error.message}`);
    } else {
      stats.rows_inserted = rows.length;
    }
  }

  if (touchedEventTickers.size > 0) {
    await supabase
      .from("kalshi_events_active")
      .update({ last_snapshot_at: new Date().toISOString() })
      .in("event_ticker", Array.from(touchedEventTickers));
  }

  if (snapshotWindow === "pregame" && rows.length > 0) {
    const closingUpdate = await applyPregameClosingPriceBackfill(supabase, rows);
    if (closingUpdate.error) {
      stats.errors.push(`pregame_closing_price_update:${closingUpdate.error}`);
    } else {
      stats.closing_prices_updated = closingUpdate.updated;
    }
  }

  return stats;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  try {
    const payload = req.method === "POST" ? await req.json().catch(() => ({})) : {};

    const phase: Phase = ["discover", "snapshot", "both"].includes(String(payload?.phase || "").toLowerCase())
      ? (String(payload.phase).toLowerCase() as Phase)
      : "snapshot";

    const sportFilter = mapSportFilter(payload?.sport);
    const eventTickersOverride = Array.isArray(payload?.event_tickers)
      ? payload.event_tickers.map((v: any) => String(v).trim()).filter(Boolean)
      : [];
    const maxMarkets = Math.min(ABSOLUTE_MAX_MARKETS, Math.max(1, toInt(payload?.max_markets) || DEFAULT_MAX_MARKETS));
    const maxEvents = Math.min(ABSOLUTE_MAX_EVENTS, Math.max(1, toInt(payload?.max_events) || DEFAULT_MAX_EVENTS));
    const snapshotWindow = mapSnapshotWindow(payload?.window);

    const keyId = Deno.env.get("KALSHI_API_KEY_ID") || null;
    const privateKeyPem = Deno.env.get("KALSHI_RSA_PRIVATE_KEY") || null;

    const supabase = createClient(
      Deno.env.get("SUPABASE_URL") ?? "",
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "",
      { auth: { persistSession: false } }
    );

    let discovery: any = null;
    let snapshot: any = null;

    if (phase === "discover" || phase === "both") {
      discovery = await discoverPhase(supabase, keyId, privateKeyPem, sportFilter, eventTickersOverride, maxEvents);
    }

    if (phase === "snapshot" || phase === "both") {
      snapshot = await snapshotPhase(
        supabase,
        keyId,
        privateKeyPem,
        sportFilter,
        eventTickersOverride,
        maxMarkets,
        snapshotWindow
      );
    }

    return new Response(
      JSON.stringify({
        status: "ok",
        version: "2026-04-06.v4",
        phase,
        sport: sportFilter,
        window: snapshotWindow,
        max_events: maxEvents,
        max_markets: maxMarkets,
        discovery,
        snapshot,
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  } catch (e: any) {
    return new Response(
      JSON.stringify({ status: "error", error: e?.message || String(e) }),
      { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    );
  }
});

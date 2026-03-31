import { createClient, type SupabaseClient } from "npm:@supabase/supabase-js@2.57.4";

type JsonRecord = Record<string, unknown>;

type KalshiMarket = {
  ticker?: string;
  title?: string;
  subtitle?: string;
  rules_primary?: string;
  no_sub_title?: string;
  custom_strike?: JsonRecord;
  yes_sub_title?: string;
  previous_price_dollars?: string;
  last_price_dollars?: string;
  settlement_value_dollars?: string;
  result?: string;
  volume_fp?: string;
  open_interest_fp?: string;
  status?: string;
  close_time?: string;
  open_time?: string;
};

type KalshiEvent = {
  event_ticker?: string;
  series_ticker?: string;
  title?: string;
  sub_title?: string;
  strike_date?: string;
  markets?: KalshiMarket[];
};

type BackfillMode = "game_winner" | "line_markets";

type RequestParams = {
  mode: BackfillMode;
  targetTable: "kalshi_settlements" | "kalshi_line_markets";
  seriesTicker: string;
  league: string;
  sport: string;
  limit: number;
  maxPages: number;
  startCursor: string;
  resolveClosingPrices: boolean;
  useCandlesticks: boolean;
  seedTeamMap: boolean;
  dryRun: boolean;
};

const KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2";
const DEFAULT_LIMIT = 200;
const DEFAULT_MAX_PAGES = 500;
const PAGE_DELAY_MS = 200;
const FETCH_TIMEOUT_MS = 20_000;
const UPSERT_CHUNK_SIZE = 500;
const EVENT_CONCURRENCY = 2;
const INTERNAL_JOB_SECRET = (Deno.env.get("INTERNAL_JOB_SECRET") ?? "").trim();

const MONTH_MAP: Record<string, string> = {
  JAN: "01",
  FEB: "02",
  MAR: "03",
  APR: "04",
  MAY: "05",
  JUN: "06",
  JUL: "07",
  AUG: "08",
  SEP: "09",
  OCT: "10",
  NOV: "11",
  DEC: "12",
};

const TEAM_NAME_BY_ABBREV: Record<string, Record<string, string>> = {
  nba: {
    ATL: "Atlanta Hawks",
    BKN: "Brooklyn Nets",
    BOS: "Boston Celtics",
    CHA: "Charlotte Hornets",
    CHI: "Chicago Bulls",
    CLE: "Cleveland Cavaliers",
    DAL: "Dallas Mavericks",
    DEN: "Denver Nuggets",
    DET: "Detroit Pistons",
    GSW: "Golden State Warriors",
    HOU: "Houston Rockets",
    IND: "Indiana Pacers",
    LAC: "LA Clippers",
    LAL: "Los Angeles Lakers",
    MEM: "Memphis Grizzlies",
    MIA: "Miami Heat",
    MIL: "Milwaukee Bucks",
    MIN: "Minnesota Timberwolves",
    NOP: "New Orleans Pelicans",
    NYK: "New York Knicks",
    OKC: "Oklahoma City Thunder",
    ORL: "Orlando Magic",
    PHI: "Philadelphia 76ers",
    PHX: "Phoenix Suns",
    POR: "Portland Trail Blazers",
    SAC: "Sacramento Kings",
    SAS: "San Antonio Spurs",
    TOR: "Toronto Raptors",
    UTA: "Utah Jazz",
    WAS: "Washington Wizards",
  },
  nhl: {
    ANA: "Anaheim Ducks",
    BOS: "Boston Bruins",
    BUF: "Buffalo Sabres",
    CAR: "Carolina Hurricanes",
    CBJ: "Columbus Blue Jackets",
    CGY: "Calgary Flames",
    CHI: "Chicago Blackhawks",
    COL: "Colorado Avalanche",
    DAL: "Dallas Stars",
    DET: "Detroit Red Wings",
    EDM: "Edmonton Oilers",
    FLA: "Florida Panthers",
    LA: "Los Angeles Kings",
    MIN: "Minnesota Wild",
    MTL: "Montreal Canadiens",
    NJ: "New Jersey Devils",
    NSH: "Nashville Predators",
    NYI: "New York Islanders",
    NYR: "New York Rangers",
    OTT: "Ottawa Senators",
    PHI: "Philadelphia Flyers",
    PIT: "Pittsburgh Penguins",
    SEA: "Seattle Kraken",
    SJ: "San Jose Sharks",
    STL: "St. Louis Blues",
    TB: "Tampa Bay Lightning",
    TOR: "Toronto Maple Leafs",
    UTA: "Utah Mammoth",
    VAN: "Vancouver Canucks",
    VGK: "Vegas Golden Knights",
    WPG: "Winnipeg Jets",
    WSH: "Washington Capitals",
  },
  nfl: {
    ARI: "Arizona Cardinals",
    ATL: "Atlanta Falcons",
    BAL: "Baltimore Ravens",
    BUF: "Buffalo Bills",
    CAR: "Carolina Panthers",
    CHI: "Chicago Bears",
    CIN: "Cincinnati Bengals",
    CLE: "Cleveland Browns",
    DAL: "Dallas Cowboys",
    DEN: "Denver Broncos",
    DET: "Detroit Lions",
    GB: "Green Bay Packers",
    HOU: "Houston Texans",
    IND: "Indianapolis Colts",
    JAC: "Jacksonville Jaguars",
    KC: "Kansas City Chiefs",
    LA: "Los Angeles Rams",
    LAC: "Los Angeles Chargers",
    LV: "Las Vegas Raiders",
    MIA: "Miami Dolphins",
    MIN: "Minnesota Vikings",
    NE: "New England Patriots",
    NO: "New Orleans Saints",
    NYG: "New York Giants",
    NYJ: "New York Jets",
    PHI: "Philadelphia Eagles",
    PIT: "Pittsburgh Steelers",
    SEA: "Seattle Seahawks",
    SF: "San Francisco 49ers",
    TB: "Tampa Bay Buccaneers",
    TEN: "Tennessee Titans",
    WAS: "Washington Commanders",
  },
  mlb: {
    A: "Athletics",
    ARI: "Arizona Diamondbacks",
    AZ: "Arizona Diamondbacks",
    ATH: "Athletics",
    ATL: "Atlanta Braves",
    BAL: "Baltimore Orioles",
    BOS: "Boston Red Sox",
    CHC: "Chicago Cubs",
    CWS: "Chicago White Sox",
    CIN: "Cincinnati Reds",
    CLE: "Cleveland Guardians",
    COL: "Colorado Rockies",
    DET: "Detroit Tigers",
    FLA: "Miami Marlins",
    HOU: "Houston Astros",
    KAN: "Kansas City Royals",
    KC: "Kansas City Royals",
    LAA: "Los Angeles Angels",
    LAD: "Los Angeles Dodgers",
    MIA: "Miami Marlins",
    MIL: "Milwaukee Brewers",
    MIN: "Minnesota Twins",
    NYM: "New York Mets",
    NYY: "New York Yankees",
    PHI: "Philadelphia Phillies",
    PIT: "Pittsburgh Pirates",
    SD: "San Diego Padres",
    SEA: "Seattle Mariners",
    SF: "San Francisco Giants",
    STL: "St. Louis Cardinals",
    TB: "Tampa Bay Rays",
    TEX: "Texas Rangers",
    TOR: "Toronto Blue Jays",
    WAS: "Washington Nationals",
    WSH: "Washington Nationals",
  },
};

function getRequiredEnv(name: string): string {
  const value = Deno.env.get(name);
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function readRequestSecret(req: Request): string {
  const headerSecret = req.headers.get("x-internal-secret")?.trim();
  if (headerSecret) return headerSecret;

  const authHeader = req.headers.get("authorization") ?? "";
  if (authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim();
  }

  return "";
}

function asString(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length === 0) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return fallback;
}

function clampProbability(value: number | null): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  if (value <= 0 || value >= 1) return null;
  return Math.max(0.0001, Math.min(0.9999, Number(value.toFixed(4))));
}

function parseTickerDate(eventTicker: string | null): string | null {
  if (!eventTicker) return null;
  const match = eventTicker.match(/-(\d{2})([A-Z]{3})(\d{2})/);
  if (!match) return null;

  const year = `20${match[1]}`;
  const month = MONTH_MAP[match[2]];
  const day = match[3];
  if (!month) return null;

  return `${year}-${month}-${day}`;
}

function parseIsoDate(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function parseGameDate(event: KalshiEvent, firstMarket: KalshiMarket | null): string | null {
  const fromStrike = parseIsoDate(asString((event as JsonRecord).strike_date));
  if (fromStrike) return fromStrike;

  const fromTicker = parseTickerDate(asString(event.event_ticker));
  if (fromTicker) return fromTicker;

  const fromCloseTime = parseIsoDate(asString(firstMarket?.close_time));
  if (fromCloseTime) return fromCloseTime;

  return null;
}

function parseHomeAway(title: string | null): { away: string | null; home: string | null } {
  if (!title) return { away: null, home: null };

  const atSplit = title.split(/\s+at\s+/i);
  if (atSplit.length === 2) {
    return { away: atSplit[0].trim() || null, home: atSplit[1].trim() || null };
  }

  const vsSplit = title.split(/\s+vs\.?\s+/i);
  if (vsSplit.length === 2) {
    return { away: vsSplit[0].trim() || null, home: vsSplit[1].trim() || null };
  }

  return { away: null, home: null };
}

function parseMarketAbbrev(marketTicker: string | null): string {
  if (!marketTicker) return "";
  const parts = marketTicker.split("-");
  if (parts.length === 0) return "";
  return parts[parts.length - 1].trim();
}

function inferLineMarketKind(
  seriesTicker: string,
  eventTitle: string | null,
  marketTitle: string | null,
  lineLabel: string | null,
): string {
  const seriesSource = seriesTicker.toUpperCase();
  if (seriesSource.includes("TOTAL") || seriesSource.includes(" OU")) return "total";
  if (seriesSource.includes("SPREAD")) return "spread";

  const source = `${marketTitle ?? ""} ${lineLabel ?? ""} ${eventTitle ?? ""}`.toUpperCase();
  if (source.includes("TOTAL") || source.includes("POINTS")) return "total";
  if (source.includes("SPREAD") || source.includes("WINS BY") || source.includes("MARGIN")) return "spread";
  return "unknown";
}

function deepFindFirstNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = deepFindFirstNumber(item);
      if (found !== null) return found;
    }
    return null;
  }

  if (value && typeof value === "object") {
    for (const inner of Object.values(value as JsonRecord)) {
      const found = deepFindFirstNumber(inner);
      if (found !== null) return found;
    }
  }

  return null;
}

function extractLineValue(market: KalshiMarket, eventTitle: string | null, marketKind: string): number | null {
  const fromStrike = deepFindFirstNumber(market.custom_strike);
  if (fromStrike !== null) return fromStrike;

  const prioritizedSources = [
    market.yes_sub_title ?? "",
    market.no_sub_title ?? "",
    market.title ?? "",
    market.subtitle ?? "",
    market.rules_primary ?? "",
    eventTitle ?? "",
  ].filter((value) => value.trim().length > 0);

  const patterns = marketKind === "total"
    ? [
      /(?:over|under)\s+([+-]?\d+(?:\.\d+)?)/i,
      /([+-]?\d+(?:\.\d+)?)\s+points?/i,
      /total[^0-9]*([+-]?\d+(?:\.\d+)?)/i,
      /(?:set\s+at|is)\s+([+-]?\d+(?:\.\d+)?)/i,
      /([+-]?\d+(?:\.\d+)?)\s*(?:or|and)\s*(?:fewer|less|more)/i,
    ]
    : [
      /wins\s+by\s+over\s+([+-]?\d+(?:\.\d+)?)/i,
      /(?:over|under)\s+([+-]?\d+(?:\.\d+)?)/i,
      /([+-]?\d+(?:\.\d+)?)\s+points?/i,
      /margin[^0-9]*([+-]?\d+(?:\.\d+)?)/i,
    ];

  for (const source of prioritizedSources) {
    for (const pattern of patterns) {
      const match = source.match(pattern);
      if (!match) continue;
      const parsed = Number(match[1]);
      if (Number.isFinite(parsed)) return parsed;
    }
  }

  const marketTicker = asString(market.ticker);
  if (marketTicker) {
    const lastToken = marketTicker.split("-").pop() ?? "";
    const trailingDigits = lastToken.match(/([+-]?\d+(?:\.\d+)?)$/);
    if (trailingDigits) {
      const parsed = Number(trailingDigits[1]);
      if (Number.isFinite(parsed)) return parsed;
    }
  }

  return null;
}

function normalizeLeague(league: string): string {
  const normalized = league.trim().toLowerCase();
  if (normalized === "ncaab" || normalized === "ncaamb") return "ncaab";
  return normalized;
}

function toMatchLeagueId(league: string): string {
  if (league === "ncaab") return "mens-college-basketball";
  return league;
}

function resolveEspnTeamName(league: string, abbrev: string): string | null {
  const perLeague = TEAM_NAME_BY_ABBREV[league];
  if (!perLeague) return null;
  return perLeague[abbrev] ?? null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url: string, label: string): Promise<JsonRecord> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);
  try {
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      const body = await response.text();
      throw new Error(`${label} failed (${response.status}): ${body.slice(0, 200)}`);
    }
    return (await response.json()) as JsonRecord;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchMarketDetail(marketTicker: string): Promise<KalshiMarket | null> {
  const data = await fetchJson(`${KALSHI_BASE_URL}/markets/${marketTicker}`, `market:${marketTicker}`);
  const market = (data.market ?? null) as KalshiMarket | null;
  return market;
}

async function fetchCandlestickClose(
  seriesTicker: string,
  marketTicker: string,
  openTime: string | null,
  closeTime: string | null,
): Promise<number | null> {
  if (!openTime || !closeTime) return null;
  const openMs = Date.parse(openTime);
  const closeMs = Date.parse(closeTime);
  if (Number.isNaN(openMs) || Number.isNaN(closeMs) || closeMs <= openMs) return null;

  const startTs = Math.floor(openMs / 1000);
  const endTs = Math.floor(closeMs / 1000);

  const url = new URL(`${KALSHI_BASE_URL}/series/${seriesTicker}/markets/${marketTicker}/candlesticks`);
  url.searchParams.set("start_ts", String(startTs));
  url.searchParams.set("end_ts", String(endTs));
  url.searchParams.set("period_interval", "60");

  const data = await fetchJson(url.toString(), `candles:${marketTicker}`);
  const candles = ((data.candlesticks ?? []) as JsonRecord[]).filter(Boolean);
  if (candles.length === 0) return null;

  // The final candle can already reflect post-game settlement. Prefer penultimate.
  const preferred = candles.length >= 2 ? candles[candles.length - 2] : candles[candles.length - 1];
  const price = (preferred.price ?? {}) as JsonRecord;
  const closeDollars = clampProbability(asNumber(price.close_dollars));
  return closeDollars;
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let index = 0;

  async function runWorker(): Promise<void> {
    while (index < items.length) {
      const current = index;
      index += 1;
      results[current] = await worker(items[current], current);
    }
  }

  const workers = Array.from({ length: Math.max(1, Math.min(concurrency, items.length || 1)) }, () => runWorker());
  await Promise.all(workers);
  return results;
}

async function parseParams(req: Request): Promise<RequestParams> {
  const url = new URL(req.url);
  const body = req.method === "POST" ? (await req.json().catch(() => ({}))) as JsonRecord : {};

  const pickValue = (name: string): unknown => {
    const queryValue = url.searchParams.get(name);
    if (queryValue !== null && queryValue !== "") return queryValue;
    return body[name];
  };

  const seriesTicker = asString(pickValue("series_ticker"));
  const leagueRaw = asString(pickValue("league"));
  const sport = asString(pickValue("sport"));
  const modeRaw = asString(pickValue("mode")) ?? "game_winner";
  const limit = asNumber(pickValue("limit")) ?? DEFAULT_LIMIT;
  const maxPages = asNumber(pickValue("max_pages")) ?? DEFAULT_MAX_PAGES;
  const startCursor = asString(pickValue("cursor")) ?? "";

  if (!seriesTicker) throw new Error("Missing required param: series_ticker");
  if (!leagueRaw) throw new Error("Missing required param: league");
  if (!sport) throw new Error("Missing required param: sport");
  if (modeRaw !== "game_winner" && modeRaw !== "line_markets") {
    throw new Error("Invalid mode. Use game_winner or line_markets");
  }

  const league = normalizeLeague(leagueRaw);
  const mode = modeRaw as BackfillMode;
  const targetTable = mode === "line_markets" ? "kalshi_line_markets" : "kalshi_settlements";

  return {
    mode,
    targetTable,
    seriesTicker,
    league,
    sport,
    limit: Math.max(1, Math.min(200, Math.trunc(limit))),
    maxPages: Math.max(1, Math.min(1000, Math.trunc(maxPages))),
    startCursor,
    // Line-market runs can include thousands of contracts. Defaulting to detail lookups can exceed
    // edge worker limits; keep it opt-in and rely on nested payload data by default.
    resolveClosingPrices: asBoolean(pickValue("resolve_closing_prices"), mode === "line_markets" ? false : true),
    useCandlesticks: asBoolean(pickValue("use_candlesticks"), false),
    seedTeamMap: mode === "line_markets" ? false : asBoolean(pickValue("seed_team_map"), true),
    dryRun: asBoolean(pickValue("dry_run"), false),
  };
}

async function upsertMarketRows(
  supabase: SupabaseClient,
  tableName: "kalshi_settlements" | "kalshi_line_markets",
  rows: JsonRecord[],
): Promise<{ inserted: number; updated: number }> {
  let inserted = 0;
  let updated = 0;

  for (let start = 0; start < rows.length; start += UPSERT_CHUNK_SIZE) {
    const chunk = rows.slice(start, start + UPSERT_CHUNK_SIZE);
    const tickers = chunk
      .map((row) => asString(row.market_ticker))
      .filter((value): value is string => Boolean(value));

    if (tableName === "kalshi_settlements") {
      const { data: existingRows, error: existingError } = await supabase
        .from(tableName)
        .select("market_ticker")
        .in("market_ticker", tickers);

      if (existingError) {
        throw new Error(`Existing ticker lookup failed: ${existingError.message}`);
      }

      const existingSet = new Set(((existingRows ?? []) as JsonRecord[])
        .map((row) => asString(row.market_ticker))
        .filter((value): value is string => Boolean(value)));

      updated += existingSet.size;
      inserted += Math.max(0, tickers.length - existingSet.size);
    } else {
      // Line market tickers are long and numerous; avoid URL-length issues from large IN(...) lookups.
      inserted += tickers.length;
    }

    const { error: upsertError } = await supabase
      .from(tableName)
      .upsert(chunk, { onConflict: "market_ticker" });

    if (upsertError) {
      throw new Error(`${tableName} upsert failed: ${upsertError.message}`);
    }
  }

  return { inserted, updated };
}

async function upsertTeamMap(supabase: SupabaseClient, rows: JsonRecord[]): Promise<number> {
  if (rows.length === 0) return 0;

  for (let start = 0; start < rows.length; start += UPSERT_CHUNK_SIZE) {
    const chunk = rows.slice(start, start + UPSERT_CHUNK_SIZE);
    const { error } = await supabase
      .from("kalshi_team_map")
      .upsert(chunk, { onConflict: "league,kalshi_name,kalshi_abbrev" });
    if (error) {
      throw new Error(`kalshi_team_map upsert failed: ${error.message}`);
    }
  }

  return rows.length;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", {
      status: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-internal-secret",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      },
    });
  }

  if (req.method !== "GET" && req.method !== "POST") {
    return new Response(JSON.stringify({ code: "METHOD_NOT_ALLOWED", message: "Use GET or POST" }), {
      status: 405,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }

  if (!INTERNAL_JOB_SECRET) {
    return new Response(JSON.stringify({ code: "MISCONFIGURED", message: "Missing INTERNAL_JOB_SECRET" }), {
      status: 500,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }

  if (readRequestSecret(req) !== INTERNAL_JOB_SECRET) {
    return new Response(JSON.stringify({ code: "UNAUTHORIZED", message: "Missing or invalid internal secret" }), {
      status: 401,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  }

  try {
    const params = await parseParams(req);
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "https://qffzvrnbzabcokqqrwbv.supabase.co";
    const serviceRoleKey = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY");
    const supabase = createClient(supabaseUrl, serviceRoleKey, { auth: { persistSession: false } });

    let cursor = params.startCursor;
    let pages = 0;
    let totalEvents = 0;
    let totalMarkets = 0;
    let skippedNoMarkets = 0;
    let skippedNoDate = 0;
    let errors = 0;
    let inserted = 0;
    let updated = 0;
    let teamMapUpserts = 0;

    const unmatchedTeamMap = new Set<string>();

    while (pages < params.maxPages) {
      const eventsUrl = new URL(`${KALSHI_BASE_URL}/events`);
      eventsUrl.searchParams.set("series_ticker", params.seriesTicker);
      eventsUrl.searchParams.set("status", "settled");
      eventsUrl.searchParams.set("limit", String(params.limit));
      eventsUrl.searchParams.set("with_nested_markets", "true");
      if (cursor) eventsUrl.searchParams.set("cursor", cursor);

      const eventsPayload = await fetchJson(eventsUrl.toString(), `events:page:${pages + 1}`);
      const events = ((eventsPayload.events ?? []) as KalshiEvent[]).filter(Boolean);

      if (events.length === 0) break;

      totalEvents += events.length;
      pages += 1;

      const processed = await mapWithConcurrency(events, EVENT_CONCURRENCY, async (event) => {
        const eventTicker = asString(event.event_ticker);
        const title = asString(event.title);
        const subtitle = asString(event.sub_title);
        const markets = (event.markets ?? []).filter(Boolean);

        if (markets.length === 0) {
          return {
            rows: [] as JsonRecord[],
            mapRows: [] as JsonRecord[],
            unmatched: [] as string[],
            skippedNoMarkets: 1,
            skippedNoDate: 0,
            errors: [] as string[],
          };
        }

        const date = parseGameDate(event, markets[0] ?? null);
        if (!date) {
          return {
            rows: [] as JsonRecord[],
            mapRows: [] as JsonRecord[],
            unmatched: [] as string[],
            skippedNoMarkets: 0,
            skippedNoDate: 1,
            errors: [`missing_game_date:${eventTicker ?? "unknown_event"}`],
          };
        }

        const parsedTeams = parseHomeAway(title);
        const winnerMarket = markets.find((market) => asString(market.result)?.toLowerCase() === "yes") ?? null;
        const winnerTicker = asString(winnerMarket?.ticker);

        let winnerClosingPrice: number | null = null;
        const eventErrors: string[] = [];

        if (params.mode === "game_winner" && params.resolveClosingPrices && winnerTicker) {
          try {
            const detail = await fetchMarketDetail(winnerTicker);
            winnerClosingPrice = clampProbability(asNumber(detail?.previous_price_dollars));

            if (winnerClosingPrice === null && params.useCandlesticks) {
              winnerClosingPrice = await fetchCandlestickClose(
                params.seriesTicker,
                winnerTicker,
                asString(detail?.open_time),
                asString(detail?.close_time),
              );
              await sleep(PAGE_DELAY_MS);
            }
          } catch (error) {
            eventErrors.push(`market_detail_error:${winnerTicker}:${error instanceof Error ? error.message : String(error)}`);
            winnerClosingPrice = null;
          }
        }

        const rows: JsonRecord[] = [];
        const mapRows: JsonRecord[] = [];
        const unmatched: string[] = [];

        for (const market of markets) {
          const marketTicker = asString(market.ticker);
          if (!marketTicker || !eventTicker) continue;

          totalMarkets += 1;

          const teamName = asString(market.yes_sub_title);
          const teamAbbrev = parseMarketAbbrev(marketTicker);
          const isHomeTeam = teamName && parsedTeams.home
            ? teamName === parsedTeams.home
            : teamName && parsedTeams.away
              ? teamName !== parsedTeams.away
              : null;
          const opponentName = teamName && parsedTeams.home && parsedTeams.away
            ? teamName === parsedTeams.home
              ? parsedTeams.away
              : teamName === parsedTeams.away
                ? parsedTeams.home
                : null
            : null;

          if (params.mode === "line_markets") {
            let lineClosingPrice: number | null = clampProbability(asNumber(market.previous_price_dollars));
            if (lineClosingPrice === null && params.resolveClosingPrices) {
              try {
                const detail = await fetchMarketDetail(marketTicker);
                lineClosingPrice = clampProbability(asNumber(detail?.previous_price_dollars));
                if (lineClosingPrice === null && params.useCandlesticks) {
                  lineClosingPrice = await fetchCandlestickClose(
                    params.seriesTicker,
                    marketTicker,
                    asString(detail?.open_time),
                    asString(detail?.close_time),
                  );
                  await sleep(PAGE_DELAY_MS);
                }
              } catch (error) {
                eventErrors.push(`line_market_detail_error:${marketTicker}:${error instanceof Error ? error.message : String(error)}`);
                lineClosingPrice = null;
              }
            }

            const lineLabel = asString(market.yes_sub_title);
            const marketTitle = asString(market.title);
            const inferredLineKind = inferLineMarketKind(
              params.seriesTicker,
              title,
              marketTitle,
              lineLabel,
            );

            rows.push({
              event_ticker: eventTicker,
              series_ticker: params.seriesTicker,
              market_ticker: marketTicker,
              sport: params.sport,
              league: params.league,
              market_kind: inferredLineKind,
              title,
              subtitle,
              team_name: teamName,
              opponent_name: opponentName,
              is_home_team: isHomeTeam,
              line_value: extractLineValue(market, title, inferredLineKind),
              line_side: teamName,
              game_date: date,
              closing_price: lineClosingPrice,
              settlement_price: asNumber(market.last_price_dollars),
              settlement_value: asNumber(market.settlement_value_dollars),
              result: asString(market.result)?.toLowerCase() ?? null,
              volume: asNumber(market.volume_fp),
              open_interest: asNumber(market.open_interest_fp),
              status: asString(market.status),
              raw_json: { event, market },
            });
          } else {
            let closingPrice: number | null = null;
            if (winnerClosingPrice !== null && winnerTicker) {
              if (marketTicker === winnerTicker) {
                closingPrice = winnerClosingPrice;
              } else {
                closingPrice = clampProbability(1 - winnerClosingPrice);
              }
            }

            rows.push({
              event_ticker: eventTicker,
              series_ticker: params.seriesTicker,
              market_ticker: marketTicker,
              sport: params.sport,
              league: params.league,
              title,
              subtitle,
              team_name: teamName,
              opponent_name: opponentName,
              is_home_team: isHomeTeam,
              game_date: date,
              closing_price: closingPrice,
              settlement_price: asNumber(market.last_price_dollars),
              settlement_value: asNumber(market.settlement_value_dollars),
              result: asString(market.result)?.toLowerCase() ?? null,
              volume: asNumber(market.volume_fp),
              open_interest: asNumber(market.open_interest_fp),
              status: asString(market.status),
              raw_json: { event, market },
            });
          }

          if (params.seedTeamMap && teamName) {
            const espnName = resolveEspnTeamName(params.league, teamAbbrev);
            if (espnName) {
              mapRows.push({
                kalshi_name: teamName,
                espn_name: espnName,
                league: params.league,
                kalshi_abbrev: teamAbbrev,
                espn_team_id: null,
                updated_at: new Date().toISOString(),
              });
            } else {
              unmatched.push(`${params.league}|${teamName}|${teamAbbrev}`);
            }
          }
        }

        return {
          rows,
          mapRows,
          unmatched,
          skippedNoMarkets: 0,
          skippedNoDate: 0,
          errors: eventErrors,
        };
      });

      const allRows = processed.flatMap((entry) => entry.rows);
      const mapRows = processed.flatMap((entry) => entry.mapRows);
      const allErrors = processed.flatMap((entry) => entry.errors);
      skippedNoMarkets += processed.reduce((sum, entry) => sum + entry.skippedNoMarkets, 0);
      skippedNoDate += processed.reduce((sum, entry) => sum + entry.skippedNoDate, 0);
      errors += allErrors.length;
      for (const key of processed.flatMap((entry) => entry.unmatched)) {
        unmatchedTeamMap.add(key);
      }

      const dedupedRowsMap = new Map<string, JsonRecord>();
      for (const row of allRows) {
        const marketTicker = asString(row.market_ticker);
        if (!marketTicker) continue;
        dedupedRowsMap.set(marketTicker, row);
      }
      const dedupedRows = Array.from(dedupedRowsMap.values());

      const dedupedMapRowsMap = new Map<string, JsonRecord>();
      for (const row of mapRows) {
        const league = asString(row.league);
        const kalshiName = asString(row.kalshi_name);
        const abbrev = asString(row.kalshi_abbrev) ?? "";
        if (!league || !kalshiName) continue;
        dedupedMapRowsMap.set(`${league}::${kalshiName}::${abbrev}`, row);
      }
      const dedupedMapRows = Array.from(dedupedMapRowsMap.values());

      if (!params.dryRun) {
        const settlementStats = await upsertMarketRows(supabase, params.targetTable, dedupedRows);
        inserted += settlementStats.inserted;
        updated += settlementStats.updated;

        if (params.seedTeamMap) {
          teamMapUpserts += await upsertTeamMap(supabase, dedupedMapRows);
        }
      } else {
        inserted += dedupedRows.length;
      }

      cursor = asString(eventsPayload.cursor) ?? "";
      if (!cursor) break;
      await sleep(PAGE_DELAY_MS);
    }

    const response = {
      status: "ok",
      params: {
        ...params,
        matchLeagueId: toMatchLeagueId(params.league),
      },
      stats: {
        pages,
        events: totalEvents,
        markets: totalMarkets,
        inserted,
        updated,
        errors,
        skipped_no_markets: skippedNoMarkets,
        skipped_no_date: skippedNoDate,
        team_map_upserts: teamMapUpserts,
      },
      next_cursor: cursor,
      has_more: Boolean(cursor),
      unmatched_team_map: Array.from(unmatchedTeamMap).slice(0, 100),
    };

    return new Response(JSON.stringify(response), {
      status: 200,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    });
  } catch (error) {
    return new Response(
      JSON.stringify({
        status: "failed",
        code: "KALSHI_BACKFILL_FAILED",
        message: error instanceof Error ? error.message : String(error),
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
      },
    );
  }
});

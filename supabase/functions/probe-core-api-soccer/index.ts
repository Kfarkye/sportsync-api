import "jsr:@supabase/functions-js/edge-runtime.d.ts";

type ProbeResult = {
  status: number;
  has_data: boolean;
  url: string;
  response_size_bytes: number;
  item_count: number;
  provider_ids_found: number[];
  field_names_sample: string[];
  date_range: string | null;
  notes: string;
  provider_attempts?: Array<{
    provider_id: number;
    status: number;
    has_data: boolean;
    item_count: number;
  }>;
};

type LeagueSeed = {
  code: string;
  event_id: string;
  label: string;
};

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

const CORE_BASE = "https://sports.core.api.espn.com/v2/sports/soccer/leagues";
const SITE_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer";
const REQUEST_DELAY_MS = 200;
const REQUEST_BUDGET = 79;

const LEAGUES: LeagueSeed[] = [
  { code: "eng.1", event_id: "740887", label: "EPL" },
  { code: "esp.1", event_id: "748416", label: "La Liga" },
  { code: "ita.1", event_id: "737065", label: "Serie A" },
  { code: "ger.1", event_id: "746943", label: "Bundesliga" },
  { code: "fra.1", event_id: "746644", label: "Ligue 1" },
];

const WC_PROBES: LeagueSeed[] = [
  { code: "fifa.worldq.conmebol", event_id: "684665", label: "WCQ CONMEBOL" },
  { code: "fifa.worldq.uefa", event_id: "761380", label: "WCQ UEFA" },
  { code: "fifa.world", event_id: "633850", label: "World Cup" },
];

const PROVIDER_IDS = [1002, 2000, 100, 200, 58, 45, 1003, 1004];

type FetchState = {
  request_count: number;
  budget_exhausted: boolean;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function toJsonSize(payload: unknown): number {
  try {
    return new TextEncoder().encode(JSON.stringify(payload)).length;
  } catch {
    return 0;
  }
}

function toFieldNamesSample(payload: any): string[] {
  if (!payload || typeof payload !== "object") return [];
  if (Array.isArray(payload)) return Object.keys(payload[0] ?? {}).slice(0, 12);
  if (Array.isArray(payload.items)) return Object.keys(payload.items[0] ?? {}).slice(0, 12);
  return Object.keys(payload).slice(0, 12);
}

function toItemCount(payload: any): number {
  if (!payload) return 0;
  if (Array.isArray(payload)) return payload.length;
  if (typeof payload.count === "number") return payload.count;
  if (Array.isArray(payload.items)) return payload.items.length;
  return 0;
}

function hasData(payload: any): boolean {
  if (payload === null || payload === undefined) return false;
  if (Array.isArray(payload)) return payload.length > 0;
  if (typeof payload !== "object") return false;
  if (typeof payload.count === "number") return payload.count > 0;
  if (Array.isArray(payload.items)) return payload.items.length > 0;
  return Object.keys(payload).length > 0;
}

function parseDate(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function collectDateRange(payload: any): string | null {
  const dates: string[] = [];
  const visit = (node: any, depth: number) => {
    if (depth > 3 || node === null || node === undefined) return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item, depth + 1);
      return;
    }
    if (typeof node !== "object") return;
    for (const [k, v] of Object.entries(node)) {
      if (k.toLowerCase().includes("date")) {
        const parsed = parseDate(v);
        if (parsed) dates.push(parsed);
      }
      if (typeof v === "object" && v !== null) visit(v, depth + 1);
    }
  };
  visit(payload, 0);
  if (dates.length === 0) return null;
  dates.sort();
  return `${dates[0].slice(0, 10)} to ${dates[dates.length - 1].slice(0, 10)}`;
}

function providerIdsFromOddsPayload(payload: any): number[] {
  const set = new Set<number>();
  const items = Array.isArray(payload?.items) ? payload.items : [];
  for (const item of items) {
    const id = Number(item?.provider?.id);
    if (Number.isFinite(id)) set.add(id);
  }
  return Array.from(set).sort((a, b) => a - b);
}

function extractHomeTeamId(competitionPayload: any): string | null {
  const competitors = Array.isArray(competitionPayload?.competitors) ? competitionPayload.competitors : [];
  const home = competitors.find((c: any) => c?.homeAway === "home") ?? competitors[0];
  const teamIdDirect = home?.team?.id;
  if (teamIdDirect) return String(teamIdDirect);
  const ref = String(home?.team?.$ref ?? "");
  const m = ref.match(/\/teams\/(\d+)/);
  return m ? m[1] : null;
}

function extractEventName(competitionPayload: any): string {
  return String(
    competitionPayload?.name ??
      competitionPayload?.shortName ??
      competitionPayload?.competitors?.map((c: any) => c?.team?.displayName).filter(Boolean).join(" vs ") ??
      "Unknown event",
  );
}

async function fetchJson(url: string, state: FetchState): Promise<{ status: number; payload: any; notes: string }> {
  if (state.request_count >= REQUEST_BUDGET) {
    state.budget_exhausted = true;
    return { status: 0, payload: null, notes: "Request budget exhausted" };
  }

  state.request_count += 1;
  await sleep(REQUEST_DELAY_MS);
  try {
    const res = await fetch(url, { headers: { "User-Agent": "sportsync-probe/1.0" } });
    const status = res.status;
    const text = await res.text();
    let payload: any = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch {
      payload = { raw_text: text.slice(0, 300) };
    }
    return { status, payload, notes: status >= 400 ? `HTTP ${status}` : "ok" };
  } catch (err: any) {
    return { status: 0, payload: null, notes: err?.message ?? "network error" };
  }
}

function buildProbeResult(url: string, status: number, payload: any, notes: string): ProbeResult {
  return {
    status,
    has_data: status >= 200 && status < 300 && hasData(payload),
    url,
    response_size_bytes: toJsonSize(payload),
    item_count: toItemCount(payload),
    provider_ids_found: providerIdsFromOddsPayload(payload),
    field_names_sample: toFieldNamesSample(payload),
    date_range: collectDateRange(payload),
    notes,
  };
}

async function resolveOneOddsRef(oddsPayload: any, state: FetchState): Promise<{ fieldNames: string[]; notes: string }> {
  const firstRef = Array.isArray(oddsPayload?.items)
    ? oddsPayload.items.find((it: any) => typeof it?.$ref === "string")?.$ref
    : null;
  if (!firstRef) return { fieldNames: [], notes: "No odds $ref present" };

  const refRes = await fetchJson(String(firstRef), state);
  const refFields = toFieldNamesSample(refRes.payload);
  return {
    fieldNames: refFields,
    notes: refRes.status >= 200 && refRes.status < 300
      ? `Resolved one odds $ref (${refRes.status})`
      : `Odds $ref unresolved (${refRes.status})`,
  };
}

function chooseProviderIds(found: number[]): number[] {
  const prioritized = PROVIDER_IDS.filter((id) => found.includes(id));
  if (prioritized.length > 0) return prioritized.slice(0, 1);
  return PROVIDER_IDS.slice(0, 1);
}

async function probeProviderEndpoint(
  state: FetchState,
  providerIds: number[],
  urlForProvider: (providerId: number) => string,
): Promise<ProbeResult> {
  const attempts: Array<{ provider_id: number; status: number; has_data: boolean; item_count: number }> = [];
  let chosen: ProbeResult | null = null;

  for (const providerId of providerIds) {
    const url = urlForProvider(providerId);
    const { status, payload, notes } = await fetchJson(url, state);
    const result = buildProbeResult(url, status, payload, notes);
    attempts.push({
      provider_id: providerId,
      status: result.status,
      has_data: result.has_data,
      item_count: result.item_count,
    });
    if (result.has_data && !chosen) chosen = result;
  }

  const baseline = chosen ?? buildProbeResult(urlForProvider(providerIds[0]), 404, null, "No provider returned data");
  baseline.provider_attempts = attempts;
  baseline.notes =
    baseline.notes +
    `; providers attempted: ${providerIds.join(",")}`;
  return baseline;
}

function ymd(date: Date): string {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, "0");
  const d = String(date.getUTCDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

async function findFallbackEplEvent(state: FetchState): Promise<string | null> {
  for (let i = 1; i <= 4; i++) {
    const dt = new Date(Date.now() - i * 24 * 60 * 60 * 1000);
    const url = `${SITE_BASE}/eng.1/scoreboard?dates=${ymd(dt)}&limit=100`;
    const { status, payload } = await fetchJson(url, state);
    if (!(status >= 200 && status < 300)) continue;
    const events = Array.isArray(payload?.events) ? payload.events : [];
    const fulltime = events.find((ev: any) =>
      String(ev?.status?.type?.name ?? "").includes("FULL_TIME") ||
      String(ev?.status?.type?.name ?? "").includes("FINAL")
    );
    if (fulltime?.id) return String(fulltime.id);
  }
  return null;
}

async function probeLeague(seed: LeagueSeed, state: FetchState) {
  const coreBase = `${CORE_BASE}/${seed.code}`;
  let eventId = seed.event_id;
  let fallbackUsed = false;

  // Initial odds check for EPL fallback decision.
  const initialOddsUrl = `${coreBase}/events/${eventId}/competitions/${eventId}/odds?limit=200`;
  const initialOddsFetch = await fetchJson(initialOddsUrl, state);
  let oddsPayload = initialOddsFetch.payload;
  let oddsStatus = initialOddsFetch.status;

  if (
    seed.code === "eng.1" &&
    !(oddsStatus >= 200 && oddsStatus < 300 && hasData(oddsPayload))
  ) {
    const fallback = await findFallbackEplEvent(state);
    if (fallback && fallback !== eventId) {
      eventId = fallback;
      fallbackUsed = true;
      const fallbackUrl = `${coreBase}/events/${eventId}/competitions/${eventId}/odds?limit=200`;
      const fallbackFetch = await fetchJson(fallbackUrl, state);
      oddsPayload = fallbackFetch.payload;
      oddsStatus = fallbackFetch.status;
    }
  }

  const compUrl = `${coreBase}/events/${eventId}/competitions/${eventId}`;
  const compFetch = await fetchJson(compUrl, state);
  const compPayload = compFetch.payload ?? {};
  const homeTeamId = extractHomeTeamId(compPayload);
  const eventName = extractEventName(compPayload);

  const oddsResult = buildProbeResult(
    `${coreBase}/events/${eventId}/competitions/${eventId}/odds?limit=200`,
    oddsStatus,
    oddsPayload,
    fallbackUsed ? "Used fallback completed EPL event" : initialOddsFetch.notes,
  );
  const ref = await resolveOneOddsRef(oddsPayload, state);
  oddsResult.field_names_sample = Array.from(new Set([...oddsResult.field_names_sample, ...ref.fieldNames])).slice(0, 16);
  oddsResult.notes = `${oddsResult.notes}; ${ref.notes}`;

  const providersFound = oddsResult.provider_ids_found;
  const providerIdsToTry = chooseProviderIds(providersFound);

  const probabilitiesUrl = `${coreBase}/events/${eventId}/competitions/${eventId}/probabilities?limit=200`;
  const probabilitiesFetch = await fetchJson(probabilitiesUrl, state);
  const probabilitiesResult = buildProbeResult(
    probabilitiesUrl,
    probabilitiesFetch.status,
    probabilitiesFetch.payload,
    probabilitiesFetch.notes,
  );

  const movementResult = await probeProviderEndpoint(
    state,
    providerIdsToTry,
    (providerId) =>
      `${coreBase}/events/${eventId}/competitions/${eventId}/odds/${providerId}/history/0/movement?limit=100`,
  );

  const headToHeadsResult = await probeProviderEndpoint(
    state,
    providerIdsToTry,
    (providerId) =>
      `${coreBase}/events/${eventId}/competitions/${eventId}/odds/${providerId}/head-to-heads`,
  );

  const atsUrl = homeTeamId
    ? `${coreBase}/seasons/2025/types/1/teams/${homeTeamId}/ats`
    : `${coreBase}/seasons/2025/types/1/teams/unknown/ats`;
  const atsFetch = await fetchJson(atsUrl, state);
  const atsResult = buildProbeResult(atsUrl, atsFetch.status, atsFetch.payload, atsFetch.notes);
  if (!homeTeamId) atsResult.notes = "Home team ID unavailable from event payload";

  const oddsRecordsUrl = homeTeamId
    ? `${coreBase}/seasons/2025/types/1/teams/${homeTeamId}/odds-records`
    : `${coreBase}/seasons/2025/types/1/teams/unknown/odds-records`;
  const oddsRecordsFetch = await fetchJson(oddsRecordsUrl, state);
  const oddsRecordsResult = buildProbeResult(
    oddsRecordsUrl,
    oddsRecordsFetch.status,
    oddsRecordsFetch.payload,
    oddsRecordsFetch.notes,
  );
  if (!homeTeamId) oddsRecordsResult.notes = "Home team ID unavailable from event payload";

  const pastPerformancesResult = await probeProviderEndpoint(
    state,
    providerIdsToTry,
    (providerId) =>
      homeTeamId
        ? `${coreBase}/teams/${homeTeamId}/odds/${providerId}/past-performances?limit=200`
        : `${coreBase}/teams/unknown/odds/${providerId}/past-performances?limit=200`,
  );
  if (!homeTeamId) pastPerformancesResult.notes = "Home team ID unavailable from event payload";

  const futuresUrl = `${coreBase}/seasons/2025/futures`;
  const futuresFetch = await fetchJson(futuresUrl, state);
  const futuresResult = buildProbeResult(futuresUrl, futuresFetch.status, futuresFetch.payload, futuresFetch.notes);

  return {
    event_id: eventId,
    event_name: eventName,
    fallback_used: fallbackUsed,
    home_team_id: homeTeamId,
    endpoints: {
      odds: oddsResult,
      probabilities: probabilitiesResult,
      movement: movementResult,
      "head-to-heads": headToHeadsResult,
      ats: atsResult,
      "odds-records": oddsRecordsResult,
      "past-performances": pastPerformancesResult,
      futures: futuresResult,
    },
  };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "GET") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  const startedAt = Date.now();
  const state: FetchState = {
    request_count: 0,
    budget_exhausted: false,
  };

  const resultsByLeague: Record<string, any> = {};

  for (const seed of [...LEAGUES, ...WC_PROBES]) {
    resultsByLeague[seed.code] = await probeLeague(seed, state);
  }

  const leaguesWithOdds = Object.entries(resultsByLeague)
    .filter(([, value]: any) => value?.endpoints?.odds?.has_data)
    .map(([code]) => code);
  const leaguesWithoutOdds = Object.keys(resultsByLeague).filter((code) => !leaguesWithOdds.includes(code));
  const providersAll = new Set<number>();
  for (const value of Object.values(resultsByLeague) as any[]) {
    for (const id of value?.endpoints?.odds?.provider_ids_found ?? []) providersAll.add(Number(id));
  }

  const wcResolved = WC_PROBES
    .map((p) => p.code)
    .filter((code) => Boolean(resultsByLeague[code]?.endpoints?.odds?.has_data));

  const summary = {
    soccer_has_odds_parity_with_nfl:
      leaguesWithOdds.length >= 5 &&
      providersAll.size >= 4 &&
      Object.values(resultsByLeague).every((entry: any) => typeof entry?.endpoints?.odds?.status === "number"),
    leagues_with_odds: leaguesWithOdds,
    leagues_without_odds: leaguesWithoutOdds,
    wc_league_code_found: wcResolved[0] ?? null,
    wc_leagues_with_odds: wcResolved,
    provider_ids_found_across_all: Array.from(providersAll).sort((a, b) => a - b),
    total_requests_sent: state.request_count,
    request_budget: REQUEST_BUDGET,
    budget_exhausted: state.budget_exhausted,
    elapsed_ms: Date.now() - startedAt,
  };

  return new Response(
    JSON.stringify(
      {
        probe_timestamp: new Date().toISOString(),
        results_by_league: resultsByLeague,
        summary,
      },
      null,
      2,
    ),
    {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
    },
  );
});

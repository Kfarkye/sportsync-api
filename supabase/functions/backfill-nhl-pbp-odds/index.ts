import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

declare const Deno: any;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-cron-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const ESPN_BASE = "https://sports.core.api.espn.com/v2/sports/hockey/leagues/nhl";
const RETRYABLE = new Set([429, 503]);
const REQUEST_INTERVAL_MS = 360; // <= ~2.7 req/sec
const MAX_RETRIES = 4;
const BATCH_DEFAULT = 50;

type MatchRow = {
  id: string;
  start_time: string | null;
  status: string | null;
  needs_play?: boolean;
  needs_odds?: boolean;
};

type OddsSnapshot = {
  home_ml: number | null;
  away_ml: number | null;
  home_spread: number | null;
  away_spread: number | null;
  home_spread_odds: number | null;
  away_spread_odds: number | null;
  total: number | null;
  over_odds: number | null;
  under_odds: number | null;
  provider: string | null;
  provider_id: string | null;
};

let lastRequestAt = 0;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function getJwtRole(token: string): string | null {
  try {
    const parts = token.split(".");
    if (parts.length < 2) return null;
    const b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.length % 4 === 0 ? "" : "=".repeat(4 - (b64.length % 4));
    const json = atob(b64 + pad);
    const payload = JSON.parse(json);
    return typeof payload?.role === "string" ? payload.role : null;
  } catch {
    return null;
  }
}

function parseIntSafe(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function parseFloatSafe(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseAmerican(value: any): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.trunc(value);
  if (typeof value === "string") {
    const cleaned = value.trim().replace(/\u2212/g, "-").replace(/[^\d.+-]/g, "");
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? Math.trunc(n) : null;
  }
  if (value && typeof value === "object") {
    const fromAmerican = parseAmerican(value.american ?? value.alternateDisplayValue ?? value.displayValue);
    if (fromAmerican !== null) return fromAmerican;
    const decimal = parseFloatSafe(value.value ?? value.decimal);
    if (decimal !== null && decimal > 1) {
      const profit = decimal - 1;
      if (profit >= 1) return Math.round(profit * 100);
      return Math.round(-100 / profit);
    }
  }
  return null;
}

function normalizeEventType(play: any): string {
  const raw = String(play?.type?.abbreviation ?? play?.type?.text ?? play?.type ?? "play")
    .trim()
    .toLowerCase();
  return raw.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "play";
}

function providerRank(item: any): number {
  const id = String(item?.provider?.id ?? "");
  const name = String(item?.provider?.name ?? "").toLowerCase();
  if (id === "2000" || name.includes("bet365")) return 1;
  if (id === "40" || id === "100" || name.includes("draft")) return 2;
  if (id === "38" || name.includes("caesar")) return 3;
  if (name.includes("fanduel")) return 4;
  if (name.includes("betmgm")) return 5;
  return 9;
}

function toOddsSnapshot(item: any, phase: "open" | "close" | "current"): OddsSnapshot {
  const node = item?.[phase] ?? {};
  const spread = parseFloatSafe(item?.spread ?? node?.spread);
  const homeFav = Boolean(item?.homeTeamOdds?.favorite);
  const awayFav = Boolean(item?.awayTeamOdds?.favorite);
  let homeSpread: number | null = null;
  let awaySpread: number | null = null;
  if (spread !== null) {
    const abs = Math.abs(spread);
    if (homeFav && !awayFav) {
      homeSpread = -abs;
      awaySpread = abs;
    } else if (awayFav && !homeFav) {
      homeSpread = abs;
      awaySpread = -abs;
    } else {
      homeSpread = spread;
      awaySpread = -spread;
    }
  }

  return {
    home_ml:
      parseAmerican(item?.homeTeamOdds?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.[phase]?.moneyLine),
    away_ml:
      parseAmerican(item?.awayTeamOdds?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.[phase]?.moneyLine),
    home_spread: homeSpread,
    away_spread: awaySpread,
    home_spread_odds:
      parseAmerican(item?.homeTeamOdds?.spreadOdds) ??
      parseAmerican(item?.homeTeamOdds?.[phase]?.spread),
    away_spread_odds:
      parseAmerican(item?.awayTeamOdds?.spreadOdds) ??
      parseAmerican(item?.awayTeamOdds?.[phase]?.spread),
    total:
      parseFloatSafe(item?.overUnder) ??
      parseFloatSafe(node?.overUnder) ??
      parseFloatSafe(node?.total?.american) ??
      parseFloatSafe(node?.total?.alternateDisplayValue),
    over_odds:
      parseAmerican(item?.overOdds) ??
      parseAmerican(node?.over),
    under_odds:
      parseAmerican(item?.underOdds) ??
      parseAmerican(node?.under),
    provider: String(item?.provider?.name ?? "ESPN"),
    provider_id: item?.provider?.id ? String(item.provider.id) : null,
  };
}

function hasOdds(snapshot: OddsSnapshot | null): boolean {
  if (!snapshot) return false;
  return [
    snapshot.total,
    snapshot.home_ml,
    snapshot.away_ml,
    snapshot.home_spread,
    snapshot.away_spread,
  ].some((v) => typeof v === "number" && Number.isFinite(v as number));
}

async function fetchJson(url: string): Promise<{ ok: boolean; status: number; data: any; error?: string }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const elapsed = Date.now() - lastRequestAt;
    if (elapsed < REQUEST_INTERVAL_MS) await sleep(REQUEST_INTERVAL_MS - elapsed);
    lastRequestAt = Date.now();
    try {
      const res = await fetch(url, { headers: { "User-Agent": "sportsync-nhl-backfill/1.0" } });
      const status = res.status;
      if (res.ok) {
        const data = await res.json();
        return { ok: true, status, data };
      }
      if (RETRYABLE.has(status) && attempt < MAX_RETRIES) {
        await sleep(Math.min(6000, 300 * 2 ** (attempt - 1)));
        continue;
      }
      return { ok: false, status, data: null, error: `HTTP ${status}` };
    } catch (err: any) {
      if (attempt < MAX_RETRIES) {
        await sleep(Math.min(6000, 300 * 2 ** (attempt - 1)));
        continue;
      }
      return { ok: false, status: 0, data: null, error: err?.message ?? "network error" };
    }
  }
  return { ok: false, status: 0, data: null, error: "exhausted retries" };
}

async function resolveSportLabel(supabase: ReturnType<typeof createClient>): Promise<string> {
  // Force canonical sport label for NHL to match majority existing match-level convention.
  return "hockey";
}

async function fetchMissingMatches(
  supabase: ReturnType<typeof createClient>,
  offset: number,
  limit: number,
): Promise<MatchRow[]> {
  const matchesRes = await supabase
    .from("matches")
    .select("id,start_time,status")
    .eq("league_id", "nhl")
    .in("status", ["STATUS_FULL_TIME", "STATUS_FINAL"])
    .gte("start_time", "2025-10-01T00:00:00Z")
    .order("start_time", { ascending: true });
  if (matchesRes.error) throw new Error(`matches query failed: ${matchesRes.error.message}`);
  const all = (matchesRes.data ?? []) as MatchRow[];
  const targets: MatchRow[] = [];
  const needed = offset + limit;
  for (const m of all) {
    const anyRows = await supabase
      .from("game_events")
      .select("id", { count: "exact", head: true })
      .eq("league_id", "nhl")
      .eq("match_id", m.id);
    if (anyRows.error) throw new Error(`game_events exists check failed for ${m.id}: ${anyRows.error.message}`);
    const hasAny = (anyRows.count ?? 0) > 0;

    let hasOddsSnapshot = false;
    if (hasAny) {
      const oddsRows = await supabase
        .from("game_events")
        .select("id", { count: "exact", head: true })
        .eq("league_id", "nhl")
        .eq("match_id", m.id)
        .eq("event_type", "odds_snapshot");
      if (oddsRows.error) throw new Error(`odds snapshot exists check failed for ${m.id}: ${oddsRows.error.message}`);
      hasOddsSnapshot = (oddsRows.count ?? 0) > 0;
    }

    const needsPlay = !hasAny;
    const needsOdds = !hasOddsSnapshot;
    if (needsPlay || needsOdds) {
      targets.push({
        ...m,
        needs_play: needsPlay,
        needs_odds: needsOdds,
      });
    }
    if (targets.length >= needed) break;
  }
  return targets.slice(offset, offset + limit);
}

async function fetchAllPlays(eventId: string): Promise<any[]> {
  const all: any[] = [];
  let page = 1;
  let count: number | null = null;
  while (page <= 8) {
    const url = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/plays?limit=400&page=${page}`;
    const res = await fetchJson(url);
    if (!res.ok) {
      if (res.status === 404) break;
      throw new Error(`plays fetch failed for ${eventId}: ${res.error}`);
    }
    const items = Array.isArray(res.data?.items) ? res.data.items : [];
    if (typeof res.data?.count === "number") count = res.data.count;
    if (items.length === 0) break;
    all.push(...items);
    if (count !== null && all.length >= count) break;
    if (items.length < 400) break;
    page += 1;
  }
  return all;
}

async function writeClosingLine(
  supabase: ReturnType<typeof createClient>,
  matchId: string,
  seed: OddsSnapshot,
): Promise<boolean> {
  const total = seed.total !== null && seed.total <= 0 ? null : seed.total;
  const payload = {
    match_id: matchId,
    total,
    home_spread: seed.home_spread,
    away_spread: seed.away_spread,
    home_ml: seed.home_ml,
    away_ml: seed.away_ml,
    league_id: "nhl",
    created_at: new Date().toISOString(),
  };

  // closing_lines may not have a unique constraint on match_id, so upsert can fail.
  // Resolve idempotency by updating one existing row for match_id when present, else insert.
  const existing = await supabase
    .from("closing_lines")
    .select("id")
    .eq("match_id", matchId)
    .limit(1)
    .maybeSingle();
  if (existing.error) throw new Error(`closing_lines lookup failed: ${existing.error.message}`);

  if (existing.data?.id) {
    const { error } = await supabase.from("closing_lines").update(payload).eq("id", existing.data.id);
    if (error) throw new Error(`closing_lines update failed: ${error.message}`);
    return true;
  }

  const { error } = await supabase.from("closing_lines").insert(payload);
  if (error) throw new Error(`closing_lines insert failed: ${error.message}`);
  return true;
}

async function backfillClosingFromSnapshots(
  supabase: ReturnType<typeof createClient>,
  limit: number,
  offset: number,
): Promise<{ processed: number; upserted: number; failures: string[] }> {
  const failures: string[] = [];
  const rowsRes = await supabase
    .from("game_events")
    .select("id,match_id,odds_close,odds_live,odds_open")
    .eq("league_id", "nhl")
    .eq("event_type", "odds_snapshot")
    .eq("source", "espn_backfill")
    .order("match_id", { ascending: true })
    .range(offset, offset + limit - 1);
  if (rowsRes.error) throw new Error(`snapshot query failed: ${rowsRes.error.message}`);

  let upserted = 0;
  for (const row of rowsRes.data ?? []) {
    const matchId = String((row as any).match_id ?? "");
    if (!matchId) continue;
    const close = ((row as any).odds_close ?? null) as OddsSnapshot | null;
    const live = ((row as any).odds_live ?? null) as OddsSnapshot | null;
    const open = ((row as any).odds_open ?? null) as OddsSnapshot | null;
    const seed = hasOdds(close) ? close : hasOdds(live) ? live : hasOdds(open) ? open : null;
    if (!seed) continue;
    try {
      const ok = await writeClosingLine(supabase, matchId, seed);
      if (ok) upserted += 1;
    } catch (err: any) {
      failures.push(`${matchId}: ${err?.message ?? String(err)}`);
    }
  }
  return { processed: rowsRes.data?.length ?? 0, upserted, failures };
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function fetchLatestScore(
  supabase: ReturnType<typeof createClient>,
  matchId: string,
): Promise<{ home: number; away: number }> {
  const { data, error } = await supabase
    .from("game_events")
    .select("home_score,away_score,sequence")
    .eq("league_id", "nhl")
    .eq("match_id", matchId)
    .neq("event_type", "odds_snapshot")
    .order("sequence", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error || !data) return { home: 0, away: 0 };
  return {
    home: parseIntSafe((data as any).home_score) ?? 0,
    away: parseIntSafe((data as any).away_score) ?? 0,
  };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ error: "Method not allowed" }), {
      status: 405,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
    const cronSecret = Deno.env.get("CRON_SECRET") ?? "";
    const reqSecret = req.headers.get("x-cron-secret") ?? "";
    const authHeader = req.headers.get("authorization") ?? "";
    const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : "";
    const isServiceRole = getJwtRole(bearer) === "service_role";
    const isCron = reqSecret.length === 32 && timingSafeEqual(reqSecret, cronSecret);
    if (!isServiceRole && !isCron) {
      return new Response(JSON.stringify({ error: "Unauthorized" }), {
        status: 401,
        headers: { ...CORS, "Content-Type": "application/json" },
      });
    }
    if (!supabaseUrl || !serviceRole) throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");

    const body = await req.json().catch(() => ({}));
    const dryRun = Boolean(body.dry_run ?? false);
    const batchSize = Math.max(1, Math.min(100, Number(body.batch_size ?? BATCH_DEFAULT)));
    const offset = Math.max(0, Number(body.offset ?? 0));
    const closingOnly = Boolean(body.closing_only ?? false);

    const supabase = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });
    const sportLabel = await resolveSportLabel(supabase);
    if (!dryRun) {
      // Normalize any prior espn_backfill NHL rows to canonical hockey label.
      await supabase
        .from("game_events")
        .update({ sport: "hockey" })
        .eq("league_id", "nhl")
        .eq("source", "espn_backfill")
        .eq("sport", "icehockey");
    }

    if (closingOnly) {
      const closing = await backfillClosingFromSnapshots(supabase, batchSize, offset);
      return new Response(
        JSON.stringify({
          mode: "closing_only",
          batch_size: batchSize,
          offset,
          processed_rows: closing.processed,
          closing_lines_upserted: closing.upserted,
          failures: closing.failures,
        }),
        {
          status: 200,
          headers: { ...CORS, "Content-Type": "application/json" },
        },
      );
    }

    const targets = await fetchMissingMatches(supabase, offset, batchSize);

    const result: any = {
      batch_size: batchSize,
      offset,
      targets_found: targets.length,
      sport_label: sportLabel,
      processed_games: 0,
      plays_inserted: 0,
      odds_snapshots_inserted: 0,
      closing_lines_upserted: 0,
      failures: [] as string[],
    };

    for (const match of targets) {
      const matchId = match.id;
      const eventId = String(matchId).replace(/_nhl$/i, "");
      if (!/^\d+$/.test(eventId)) {
        result.failures.push(`${matchId}: invalid event id`);
        continue;
      }

      try {
        const needsPlay = match.needs_play !== false;
        const needsOdds = match.needs_odds !== false;
        const plays = needsPlay ? await fetchAllPlays(eventId) : [];
        const oddsUrl = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/odds`;
        const probUrl = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/probabilities?limit=200`;
        const [oddsRes, probRes] = needsOdds
          ? await Promise.all([fetchJson(oddsUrl), fetchJson(probUrl)])
          : [{ ok: false, status: 0, data: null }, { ok: false, status: 0, data: null }];

        const oddsItems = Array.isArray(oddsRes.data?.items)
          ? oddsRes.data.items
          : Array.isArray(oddsRes.data)
          ? oddsRes.data
          : [];

        const selectedOdds = [...oddsItems].sort((a, b) => providerRank(a) - providerRank(b))[0] ?? null;
        const oddsOpen = selectedOdds ? toOddsSnapshot(selectedOdds, "open") : null;
        const oddsClose = selectedOdds ? toOddsSnapshot(selectedOdds, "close") : null;
        const oddsLive = selectedOdds ? toOddsSnapshot(selectedOdds, "current") : null;

        let finalHomeScore = 0;
        let finalAwayScore = 0;
        if (needsPlay && plays.length > 0) {
          const lastPlay = plays[plays.length - 1];
          finalHomeScore = parseIntSafe(lastPlay?.homeScore) ?? 0;
          finalAwayScore = parseIntSafe(lastPlay?.awayScore) ?? 0;
        }

        if (!dryRun && needsPlay && plays.length > 0) {
          const playRows = plays.map((play: any, index: number) => ({
            match_id: matchId,
            league_id: "nhl",
            sport: sportLabel,
            event_type: normalizeEventType(play),
            sequence: parseIntSafe(play?.sequenceNumber) ?? index + 1,
            period: parseIntSafe(play?.period?.number ?? play?.period),
            clock: play?.clock?.displayValue ?? play?.clock?.value ?? null,
            home_score: parseIntSafe(play?.homeScore) ?? 0,
            away_score: parseIntSafe(play?.awayScore) ?? 0,
            play_data: play,
            source: "espn_backfill",
          }));
          for (const c of chunk(playRows, 200)) {
            const { error } = await supabase.from("game_events").upsert(c, {
              onConflict: "match_id,event_type,sequence",
              ignoreDuplicates: true,
            });
            if (error) throw new Error(`plays upsert failed: ${error.message}`);
          }
          result.plays_inserted += playRows.length;
        }

        if (!dryRun && needsOdds) {
          if (!needsPlay) {
            const score = await fetchLatestScore(supabase, matchId);
            finalHomeScore = score.home;
            finalAwayScore = score.away;
          }
          const oddsRow = {
            match_id: matchId,
            league_id: "nhl",
            sport: sportLabel,
            event_type: "odds_snapshot",
            sequence: 0,
            period: null,
            clock: null,
            home_score: finalHomeScore,
            away_score: finalAwayScore,
            odds_snapshot: oddsRes.ok ? oddsRes.data : null,
            odds_open: hasOdds(oddsOpen) ? oddsOpen : null,
            odds_close: hasOdds(oddsClose) ? oddsClose : null,
            odds_live: hasOdds(oddsLive) ? oddsLive : null,
            match_state: {
              probabilities: probRes.ok ? probRes.data : null,
              spreadWinner: selectedOdds?.spreadWinner ?? null,
              moneylineWinner: selectedOdds?.moneylineWinner ?? null,
              provider: selectedOdds?.provider?.name ?? null,
              provider_id: selectedOdds?.provider?.id ?? null,
            },
            source: "espn_backfill",
          };
          const { error: oddsInsertError } = await supabase.from("game_events").upsert(oddsRow, {
            onConflict: "match_id,event_type,sequence",
            ignoreDuplicates: false,
          });
          if (oddsInsertError) throw new Error(`odds snapshot upsert failed: ${oddsInsertError.message}`);
          result.odds_snapshots_inserted += 1;

          const closingSeed = hasOdds(oddsClose) ? oddsClose : hasOdds(oddsLive) ? oddsLive : hasOdds(oddsOpen) ? oddsOpen : null;
          if (closingSeed) {
            const ok = await writeClosingLine(supabase, matchId, closingSeed);
            if (ok) result.closing_lines_upserted += 1;
          }
        }

        result.processed_games += 1;
      } catch (err: any) {
        result.failures.push(`${matchId}: ${err?.message ?? String(err)}`);
      }
    }

    return new Response(JSON.stringify(result), {
      status: 200,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err?.message ?? String(err) }), {
      status: 500,
      headers: { ...CORS, "Content-Type": "application/json" },
    });
  }
});

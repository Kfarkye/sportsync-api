import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";
import { toCanonicalOdds } from "../_shared/odds-contract.ts";

declare const Deno: any;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-cron-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const ESPN_BASE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba";
const RETRYABLE = new Set([429, 503]);
const REQUEST_INTERVAL_MS = 360; // <= ~2.7 req/sec
const MAX_RETRIES = 4;
const BATCH_DEFAULT = 100;

type MatchRow = {
  id: string;
  start_time: string | null;
  status: string | null;
  current_odds: Record<string, unknown> | null;
  odds_total_safe: number | null;
  home_score: number | null;
  away_score: number | null;
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

function providerRank(item: any): number {
  const id = String(item?.provider?.id ?? "");
  const name = String(item?.provider?.name ?? "").toLowerCase();
  if (id === "38" || name.includes("caesars")) return 1;
  if (id === "1004" || name.includes("consensus")) return 2;
  if (id === "40" || name.includes("draft")) return 3;
  if (id === "2000" || name.includes("bet365")) return 4;
  if (name.includes("fanduel")) return 5;
  if (name.includes("betmgm")) return 6;
  return 9;
}

function toSpreadPair(item: any): { homeSpread: number | null; awaySpread: number | null } {
  const spread = parseFloatSafe(item?.spread ?? item?.close?.spread ?? item?.current?.spread ?? item?.open?.spread);
  if (spread === null) return { homeSpread: null, awaySpread: null };
  const abs = Math.abs(spread);
  const homeFav = Boolean(item?.homeTeamOdds?.favorite);
  const awayFav = Boolean(item?.awayTeamOdds?.favorite);
  if (homeFav && !awayFav) return { homeSpread: -abs, awaySpread: abs };
  if (awayFav && !homeFav) return { homeSpread: abs, awaySpread: -abs };
  return { homeSpread: spread, awaySpread: -spread };
}

function toOddsSnapshot(item: any, phase: "open" | "close" | "current"): OddsSnapshot {
  const node = item?.[phase] ?? {};
  const spreadPair = toSpreadPair({
    ...item,
    spread: item?.[phase]?.spread ?? item?.spread,
  });

  return {
    home_ml:
      parseAmerican(item?.homeTeamOdds?.[phase]?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.moneyLine),
    away_ml:
      parseAmerican(item?.awayTeamOdds?.[phase]?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.moneyLine),
    home_spread: spreadPair.homeSpread,
    away_spread: spreadPair.awaySpread,
    home_spread_odds:
      parseAmerican(item?.homeTeamOdds?.[phase]?.spread) ??
      parseAmerican(item?.homeTeamOdds?.spreadOdds),
    away_spread_odds:
      parseAmerican(item?.awayTeamOdds?.[phase]?.spread) ??
      parseAmerican(item?.awayTeamOdds?.spreadOdds),
    total:
      parseFloatSafe(item?.[phase]?.overUnder) ??
      parseFloatSafe(item?.overUnder) ??
      parseFloatSafe(node?.total?.american) ??
      parseFloatSafe(node?.total?.alternateDisplayValue),
    over_odds:
      parseAmerican(item?.[phase]?.over) ??
      parseAmerican(item?.overOdds),
    under_odds:
      parseAmerican(item?.[phase]?.under) ??
      parseAmerican(item?.underOdds),
    provider: String(item?.provider?.name ?? "ESPN"),
    provider_id: item?.provider?.id ? String(item.provider.id) : null,
  };
}

function parseItemToRawOdds(item: any) {
  const spreadPair = toSpreadPair(item);
  return {
    total:
      parseFloatSafe(item?.overUnder) ??
      parseFloatSafe(item?.close?.overUnder) ??
      parseFloatSafe(item?.current?.overUnder) ??
      parseFloatSafe(item?.open?.overUnder) ??
      null,
    overOdds:
      parseAmerican(item?.overOdds) ??
      parseAmerican(item?.close?.over) ??
      parseAmerican(item?.current?.over) ??
      parseAmerican(item?.open?.over),
    underOdds:
      parseAmerican(item?.underOdds) ??
      parseAmerican(item?.close?.under) ??
      parseAmerican(item?.current?.under) ??
      parseAmerican(item?.open?.under),
    homeML:
      parseAmerican(item?.homeTeamOdds?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.close?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.current?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.open?.moneyLine),
    awayML:
      parseAmerican(item?.awayTeamOdds?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.close?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.current?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.open?.moneyLine),
    drawML:
      parseAmerican(item?.drawOdds?.moneyLine) ??
      parseAmerican(item?.drawOdds?.close?.moneyLine) ??
      parseAmerican(item?.drawOdds?.current?.moneyLine) ??
      parseAmerican(item?.drawOdds?.open?.moneyLine),
    homeSpread: spreadPair.homeSpread,
    awaySpread: spreadPair.awaySpread,
    homeSpreadOdds:
      parseAmerican(item?.homeTeamOdds?.spreadOdds) ??
      parseAmerican(item?.homeTeamOdds?.close?.spread) ??
      parseAmerican(item?.homeTeamOdds?.current?.spread) ??
      parseAmerican(item?.homeTeamOdds?.open?.spread),
    awaySpreadOdds:
      parseAmerican(item?.awayTeamOdds?.spreadOdds) ??
      parseAmerican(item?.awayTeamOdds?.close?.spread) ??
      parseAmerican(item?.awayTeamOdds?.current?.spread) ??
      parseAmerican(item?.awayTeamOdds?.open?.spread),
  };
}

function countPopulated(odds: Record<string, unknown>): number {
  return [
    odds.total,
    odds.homeML,
    odds.awayML,
    odds.drawML,
    odds.homeSpread,
    odds.awaySpread,
    odds.overOdds,
    odds.underOdds,
  ].filter((v) => typeof v === "number" && Number.isFinite(v as number)).length;
}

async function fetchJson(url: string): Promise<{ ok: boolean; status: number; data: any; error?: string }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const elapsed = Date.now() - lastRequestAt;
    if (elapsed < REQUEST_INTERVAL_MS) await sleep(REQUEST_INTERVAL_MS - elapsed);
    lastRequestAt = Date.now();

    try {
      const res = await fetch(url, { headers: { "User-Agent": "sportsync-nba-backfill/1.0" } });
      if (res.ok) return { ok: true, status: res.status, data: await res.json() };

      if (RETRYABLE.has(res.status) && attempt < MAX_RETRIES) {
        await sleep(Math.min(8000, 400 * 2 ** (attempt - 1)));
        continue;
      }
      return { ok: false, status: res.status, data: null, error: `HTTP ${res.status}` };
    } catch (err: any) {
      if (attempt < MAX_RETRIES) {
        await sleep(Math.min(8000, 400 * 2 ** (attempt - 1)));
        continue;
      }
      return { ok: false, status: 0, data: null, error: err?.message ?? "network error" };
    }
  }
  return { ok: false, status: 0, data: null, error: "exhausted retries" };
}

async function fetchMissingMatches(
  supabase: ReturnType<typeof createClient>,
  offset: number,
  limit: number,
): Promise<MatchRow[]> {
  const { data, error } = await supabase
    .from("matches")
    .select("id,start_time,status,current_odds,odds_total_safe,home_score,away_score")
    .eq("league_id", "nba")
    .eq("status", "STATUS_FINAL")
    .is("odds_total_safe", null)
    .order("start_time", { ascending: true })
    .range(offset, offset + limit - 1);
  if (error) throw new Error(`matches query failed: ${error.message}`);
  return (data ?? []) as MatchRow[];
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function writeClosingLineIfMissing(
  supabase: ReturnType<typeof createClient>,
  matchId: string,
  seed: OddsSnapshot,
): Promise<boolean> {
  const existing = await supabase
    .from("closing_lines")
    .select("id")
    .eq("match_id", matchId)
    .limit(1)
    .maybeSingle();
  if (existing.error) throw new Error(`closing_lines lookup failed: ${existing.error.message}`);
  if (existing.data?.id) return false;

  const total = seed.total !== null && seed.total <= 0 ? null : seed.total;
  const { error } = await supabase.from("closing_lines").insert({
    match_id: matchId,
    total,
    home_spread: seed.home_spread,
    away_spread: seed.away_spread,
    home_ml: seed.home_ml,
    away_ml: seed.away_ml,
    league_id: "nba",
    created_at: new Date().toISOString(),
  });
  if (error) throw new Error(`closing_lines insert failed: ${error.message}`);
  return true;
}

async function chooseBestOddsItem(oddsJson: any): Promise<any | null> {
  const items = Array.isArray(oddsJson?.items)
    ? oddsJson.items
    : Array.isArray(oddsJson)
    ? oddsJson
    : [];
  if (items.length === 0) return null;

  const ranked = [...items].sort((a, b) => providerRank(a) - providerRank(b));
  for (const item of ranked) {
    const raw = parseItemToRawOdds(item);
    const canonical = toCanonicalOdds(raw, {
      provider: String(item?.provider?.name ?? "ESPN"),
      isLive: false,
      updatedAt: new Date().toISOString(),
    });
    if (canonical.hasOdds) return item;

    const ref = typeof item?.$ref === "string" ? item.$ref : null;
    if (!ref) continue;
    const resolved = await fetchJson(ref);
    if (!resolved.ok || !resolved.data) continue;
    const merged = { ...item, ...resolved.data };
    const rawResolved = parseItemToRawOdds(merged);
    const canonicalResolved = toCanonicalOdds(rawResolved, {
      provider: String(merged?.provider?.name ?? "ESPN"),
      isLive: false,
      updatedAt: new Date().toISOString(),
    });
    if (canonicalResolved.hasOdds) return merged;
  }
  return null;
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

    const supabase = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });
    const targets = await fetchMissingMatches(supabase, offset, batchSize);

    const result: any = {
      batch_size: batchSize,
      offset,
      targets_found: targets.length,
      processed_games: 0,
      odds_found: 0,
      match_updates_queued: 0,
      match_updates_applied: 0,
      odds_snapshot_upserted: 0,
      win_probability_upserted: 0,
      closing_lines_inserted: 0,
      failures: [] as string[],
    };

    const oddsUpdates: any[] = [];

    for (const match of targets) {
      const matchId = match.id;
      const eventId = String(matchId).replace(/_nba$/i, "");
      if (!/^\d+$/.test(eventId)) {
        result.failures.push(`${matchId}: invalid event id`);
        continue;
      }

      try {
        const oddsUrl = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/odds?limit=100`;
        const probUrl = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/probabilities?limit=200`;
        const [oddsRes, probRes] = await Promise.all([fetchJson(oddsUrl), fetchJson(probUrl)]);
        if (!oddsRes.ok) {
          result.failures.push(`${matchId}: odds fetch failed (${oddsRes.error ?? oddsRes.status})`);
          continue;
        }

        const selectedOdds = await chooseBestOddsItem(oddsRes.data);
        if (!selectedOdds) {
          result.failures.push(`${matchId}: no usable odds item`);
          continue;
        }

        const raw = parseItemToRawOdds(selectedOdds);
        const canonical = toCanonicalOdds(raw, {
          provider: String(selectedOdds?.provider?.name ?? "ESPN"),
          isLive: false,
          updatedAt: new Date().toISOString(),
        });
        if (!canonical.hasOdds) {
          result.failures.push(`${matchId}: parsed odds empty`);
          continue;
        }

        result.odds_found += 1;
        oddsUpdates.push({
          id: matchId,
          current_odds: canonical,
          closing_odds: canonical,
          last_odds_update: new Date().toISOString(),
          odds_total_safe: canonical.total ?? null,
          odds_home_spread_safe: canonical.homeSpread ?? null,
          odds_away_spread_safe: canonical.awaySpread ?? null,
          odds_home_ml_safe: canonical.homeML ?? null,
          odds_away_ml_safe: canonical.awayML ?? null,
          odds_api_event_id: `espn_core_${eventId}_${String(selectedOdds?.provider?.id ?? "na")}`,
        });

        const homeScore = parseIntSafe(match.home_score) ?? 0;
        const awayScore = parseIntSafe(match.away_score) ?? 0;
        const oddsOpen = toOddsSnapshot(selectedOdds, "open");
        const oddsClose = toOddsSnapshot(selectedOdds, "close");
        const oddsLive = toOddsSnapshot(selectedOdds, "current");

        if (!dryRun) {
          const oddsRow = {
            match_id: matchId,
            league_id: "nba",
            sport: "basketball",
            event_type: "odds_snapshot",
            sequence: 0,
            period: null,
            clock: null,
            home_score: homeScore,
            away_score: awayScore,
            odds_snapshot: oddsRes.data,
            odds_open: hasOdds(oddsOpen) ? oddsOpen : null,
            odds_close: hasOdds(oddsClose) ? oddsClose : null,
            odds_live: hasOdds(oddsLive) ? oddsLive : null,
            match_state: {
              spreadWinner: selectedOdds?.spreadWinner ?? null,
              moneylineWinner: selectedOdds?.moneylineWinner ?? null,
              provider: selectedOdds?.provider?.name ?? null,
              provider_id: selectedOdds?.provider?.id ?? null,
            },
            source: "espn_backfill",
          };
          const { error: oddsEventErr } = await supabase.from("game_events").upsert(oddsRow, {
            onConflict: "match_id,event_type,sequence",
            ignoreDuplicates: false,
          });
          if (oddsEventErr) throw new Error(`odds_snapshot upsert failed: ${oddsEventErr.message}`);
          result.odds_snapshot_upserted += 1;

          const winProbRow = {
            match_id: matchId,
            league_id: "nba",
            sport: "basketball",
            event_type: "win_probability",
            sequence: 0,
            period: null,
            clock: null,
            home_score: homeScore,
            away_score: awayScore,
            match_state: {
              probabilities: probRes.ok ? probRes.data : null,
            },
            source: "espn_backfill",
          };
          const { error: probErr } = await supabase.from("game_events").upsert(winProbRow, {
            onConflict: "match_id,event_type,sequence",
            ignoreDuplicates: false,
          });
          if (probErr) throw new Error(`win_probability upsert failed: ${probErr.message}`);
          result.win_probability_upserted += 1;

          const closingSeed = hasOdds(oddsClose) ? oddsClose : hasOdds(oddsLive) ? oddsLive : hasOdds(oddsOpen) ? oddsOpen : null;
          if (closingSeed) {
            const inserted = await writeClosingLineIfMissing(supabase, matchId, closingSeed);
            if (inserted) result.closing_lines_inserted += 1;
          }
        }

        result.processed_games += 1;
      } catch (err: any) {
        result.failures.push(`${matchId}: ${err?.message ?? String(err)}`);
      }
    }

    result.match_updates_queued = oddsUpdates.length;
    if (!dryRun && oddsUpdates.length > 0) {
      for (const row of oddsUpdates) {
        const { id, ...patch } = row;
        const { error } = await supabase.from("matches").update(patch).eq("id", id);
        if (error) throw new Error(`matches odds update failed for ${id}: ${error.message}`);
        result.match_updates_applied += 1;
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

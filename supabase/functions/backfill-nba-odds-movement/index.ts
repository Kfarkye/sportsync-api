import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "npm:@supabase/supabase-js@2";

declare const Deno: any;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-cron-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const ESPN_BASE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba";
const PROVIDERS = [59, 58, 200, 100];
const REQUEST_INTERVAL_MS = 360; // <= 3 req/sec
const MAX_RETRIES = 4;
const RETRYABLE = new Set([429, 503]);
const BATCH_DEFAULT = 50;

let lastRequestAt = 0;

type OddsSnapshot = {
  total: number | null;
  home_ml: number | null;
  away_ml: number | null;
  home_spread: number | null;
  away_spread: number | null;
  home_spread_odds: number | null;
  away_spread_odds: number | null;
  over_odds: number | null;
  under_odds: number | null;
  provider: string | null;
  provider_id: string | null;
};

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
    const payload = JSON.parse(atob(b64 + pad));
    return typeof payload?.role === "string" ? payload.role : null;
  } catch {
    return null;
  }
}

function toNum(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toInt(v: unknown): number | null {
  const n = toNum(v);
  return n === null ? null : Math.trunc(n);
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
    const american = parseAmerican(value.american ?? value.alternateDisplayValue ?? value.displayValue);
    if (american !== null) return american;
    const decimal = toNum(value.decimal ?? value.value);
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

function toSpreadPair(item: any): { homeSpread: number | null; awaySpread: number | null } {
  const spread = toNum(item?.spread ?? item?.close?.spread ?? item?.current?.spread ?? item?.open?.spread);
  if (spread === null) return { homeSpread: null, awaySpread: null };
  const abs = Math.abs(spread);
  const homeFav = Boolean(item?.homeTeamOdds?.favorite);
  const awayFav = Boolean(item?.awayTeamOdds?.favorite);
  if (homeFav && !awayFav) return { homeSpread: -abs, awaySpread: abs };
  if (awayFav && !homeFav) return { homeSpread: abs, awaySpread: -abs };
  return { homeSpread: spread, awaySpread: -spread };
}

function snapshotFromItem(item: any): OddsSnapshot {
  const spread = toSpreadPair(item);
  return {
    total:
      toNum(item?.overUnder) ??
      toNum(item?.current?.overUnder) ??
      toNum(item?.close?.overUnder) ??
      toNum(item?.open?.overUnder) ??
      toNum(item?.current?.total?.alternateDisplayValue) ??
      toNum(item?.close?.total?.alternateDisplayValue) ??
      null,
    home_ml:
      parseAmerican(item?.homeTeamOdds?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.current?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.close?.moneyLine) ??
      parseAmerican(item?.homeTeamOdds?.open?.moneyLine),
    away_ml:
      parseAmerican(item?.awayTeamOdds?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.current?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.close?.moneyLine) ??
      parseAmerican(item?.awayTeamOdds?.open?.moneyLine),
    home_spread: spread.homeSpread,
    away_spread: spread.awaySpread,
    home_spread_odds:
      parseAmerican(item?.homeTeamOdds?.spreadOdds) ??
      parseAmerican(item?.homeTeamOdds?.current?.spread) ??
      parseAmerican(item?.homeTeamOdds?.close?.spread) ??
      parseAmerican(item?.homeTeamOdds?.open?.spread),
    away_spread_odds:
      parseAmerican(item?.awayTeamOdds?.spreadOdds) ??
      parseAmerican(item?.awayTeamOdds?.current?.spread) ??
      parseAmerican(item?.awayTeamOdds?.close?.spread) ??
      parseAmerican(item?.awayTeamOdds?.open?.spread),
    over_odds:
      parseAmerican(item?.overOdds) ??
      parseAmerican(item?.current?.over) ??
      parseAmerican(item?.close?.over) ??
      parseAmerican(item?.open?.over),
    under_odds:
      parseAmerican(item?.underOdds) ??
      parseAmerican(item?.current?.under) ??
      parseAmerican(item?.close?.under) ??
      parseAmerican(item?.open?.under),
    provider: String(item?.provider?.name ?? "ESPN"),
    provider_id: item?.provider?.id ? String(item.provider.id) : null,
  };
}

function isoOrNull(v: any): string | null {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function extractCapturedAt(item: any): string | null {
  const direct = [
    item?.timestamp,
    item?.date,
    item?.updated,
    item?.updatedAt,
    item?.created,
    item?.createdAt,
    item?.time,
  ];
  for (const x of direct) {
    const iso = isoOrNull(x);
    if (iso) return iso;
  }
  if (item?.details && typeof item.details === "object") {
    const deep = [
      item.details.timestamp,
      item.details.date,
      item.details.updatedAt,
      item.details.createdAt,
      item.details.time,
    ];
    for (const x of deep) {
      const iso = isoOrNull(x);
      if (iso) return iso;
    }
  }
  return null;
}

function extractPlayTs(playData: any): string | null {
  if (!playData || typeof playData !== "object") return null;
  const candidates = [
    playData?.wallclock,
    playData?.date,
    playData?.time,
    playData?.timestamp,
    playData?.start,
    playData?.end,
    playData?.clock?.timestamp,
    playData?.clock?.wallclock,
  ];
  for (const c of candidates) {
    const iso = isoOrNull(c);
    if (iso) return iso;
  }
  return null;
}

async function fetchJson(url: string): Promise<{ ok: boolean; status: number; data: any; error?: string }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const elapsed = Date.now() - lastRequestAt;
    if (elapsed < REQUEST_INTERVAL_MS) await sleep(REQUEST_INTERVAL_MS - elapsed);
    lastRequestAt = Date.now();

    try {
      const res = await fetch(url, { headers: { "User-Agent": "sportsync-nba-movement/1.0" } });
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

async function fetchTargets(
  supabase: ReturnType<typeof createClient>,
  offset: number,
  limit: number,
): Promise<Array<{ match_id: string; espn_event_id: string }>> {
  const { data, error } = await supabase
    .from("game_events")
    .select("match_id")
    .eq("league_id", "nba")
    .eq("source", "espn_backfill")
    .eq("event_type", "odds_snapshot")
    .order("match_id", { ascending: true })
    .range(offset, offset + limit - 1);
  if (error) throw new Error(`targets query failed: ${error.message}`);
  return (data ?? [])
    .map((r: any) => String(r.match_id ?? ""))
    .filter((id: string) => /_nba$/i.test(id))
    .map((id: string) => ({ match_id: id, espn_event_id: id.replace(/_nba$/i, "") }));
}

async function resolveMovementItems(eventId: string): Promise<{ providerId: number | null; providerName: string | null; items: any[] }> {
  for (const pid of PROVIDERS) {
    const url = `${ESPN_BASE}/events/${eventId}/competitions/${eventId}/odds/${pid}/history/0/movement?limit=100`;
    const res = await fetchJson(url);
    if (!res.ok || !res.data) continue;

    const itemsRaw = Array.isArray(res.data?.items) ? res.data.items : [];
    const resolved: any[] = [];
    for (const item of itemsRaw) {
      if (item && typeof item === "object" && typeof item.$ref === "string") {
        const deref = await fetchJson(item.$ref);
        if (deref.ok && deref.data && typeof deref.data === "object") {
          resolved.push({ ...item, ...deref.data });
        } else {
          resolved.push(item);
        }
      } else {
        resolved.push(item);
      }
    }
    if (resolved.length > 0) {
      const name = String(
        resolved[0]?.provider?.name ??
          (pid === 59
            ? "ESPN Bet - Live Odds"
            : pid === 58
            ? "ESPN BET"
            : pid === 200
            ? "DraftKings - Live Odds"
            : "DraftKings"),
      );
      return { providerId: pid, providerName: name, items: resolved };
    }
  }
  return { providerId: null, providerName: null, items: [] };
}

async function scoreMatchBatch(
  supabase: ReturnType<typeof createClient>,
  batchSize: number,
  offset: number,
): Promise<{ processed: number; updated: number; failures: string[] }> {
  const failures: string[] = [];
  const rowsRes = await supabase
    .from("game_events")
    .select("id,match_id,sequence,odds_live")
    .eq("league_id", "nba")
    .eq("event_type", "odds_movement")
    .eq("source", "espn_movement_backfill")
    .is("home_score", null)
    .order("match_id", { ascending: true })
    .order("sequence", { ascending: true })
    .range(offset, offset + batchSize - 1);
  if (rowsRes.error) throw new Error(`score-match query failed: ${rowsRes.error.message}`);
  const rows = rowsRes.data ?? [];

  let updated = 0;
  for (const row of rows as any[]) {
    const matchId = String(row.match_id ?? "");
    const capturedAt = isoOrNull(row?.odds_live?.captured_at ?? null);
    try {
      const pbpRes = await supabase
        .from("game_events")
        .select("sequence,home_score,away_score,period,clock,play_data")
        .eq("league_id", "nba")
        .eq("match_id", matchId)
        .eq("source", "espn_backfill")
        .neq("event_type", "odds_snapshot")
        .order("sequence", { ascending: true });
      if (pbpRes.error) throw new Error(`pbp query failed: ${pbpRes.error.message}`);
      const pbp = pbpRes.data ?? [];
      if (pbp.length === 0) continue;

      let chosen: any = null;
      if (capturedAt) {
        const capTs = new Date(capturedAt).getTime();
        for (const evt of pbp as any[]) {
          const ts = extractPlayTs(evt.play_data);
          if (!ts) continue;
          const t = new Date(ts).getTime();
          if (t <= capTs) chosen = evt;
          else break;
        }
      }

      if (!chosen) {
        const idx = Math.min(
          pbp.length - 1,
          Math.max(0, Math.floor((toInt(row.sequence) ?? 0) / Math.max(1, pbp.length) * pbp.length)),
        );
        chosen = pbp[idx];
      }
      if (!chosen) continue;

      const patch = {
        home_score: toInt(chosen.home_score),
        away_score: toInt(chosen.away_score),
        period: toInt(chosen.period),
        clock: typeof chosen.clock === "string" ? chosen.clock : null,
      };
      const { error: updErr } = await supabase.from("game_events").update(patch).eq("id", row.id);
      if (updErr) throw new Error(`update failed: ${updErr.message}`);
      updated += 1;
    } catch (err: any) {
      failures.push(`${row.id}: ${err?.message ?? String(err)}`);
    }
  }
  return { processed: rows.length, updated, failures };
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
    const scoreMatchOnly = Boolean(body.score_match_only ?? false);

    const supabase = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });

    if (scoreMatchOnly) {
      const sm = await scoreMatchBatch(supabase, batchSize, offset);
      return new Response(
        JSON.stringify({
          mode: "score_match_only",
          batch_size: batchSize,
          offset,
          processed_rows: sm.processed,
          updated_rows: sm.updated,
          failures: sm.failures,
        }),
        { status: 200, headers: { ...CORS, "Content-Type": "application/json" } },
      );
    }

    const targets = await fetchTargets(supabase, offset, batchSize);
    const result: any = {
      batch_size: batchSize,
      offset,
      targets_found: targets.length,
      processed_games: 0,
      games_with_movement: 0,
      games_without_movement: 0,
      snapshots_inserted: 0,
      providers_used: {} as Record<string, number>,
      failures: [] as string[],
    };

    for (const t of targets) {
      try {
        const movement = await resolveMovementItems(t.espn_event_id);
        result.processed_games += 1;
        if (!movement.providerId || movement.items.length === 0) {
          result.games_without_movement += 1;
          continue;
        }

        const rows = movement.items
          .map((item: any, idx: number) => {
            const snapshot = snapshotFromItem(item);
            if (!hasOdds(snapshot)) return null;
            const capturedAt = extractCapturedAt(item);
            return {
              match_id: t.match_id,
              league_id: "nba",
              sport: "basketball",
              event_type: "odds_movement",
              sequence: idx,
              period: null,
              clock: null,
              home_score: null,
              away_score: null,
              odds_live: {
                total: snapshot.total,
                overOdds: snapshot.over_odds,
                underOdds: snapshot.under_odds,
                homeSpread: snapshot.home_spread,
                awaySpread: snapshot.away_spread,
                homeSpreadOdds: snapshot.home_spread_odds,
                awaySpreadOdds: snapshot.away_spread_odds,
                home_ml: snapshot.home_ml,
                away_ml: snapshot.away_ml,
                provider_id: String(movement.providerId),
                provider: movement.providerName,
                captured_at: capturedAt,
                source: "espn_movement_backfill",
                raw: item,
              },
              source: "espn_movement_backfill",
            };
          })
          .filter((r: any) => !!r);

        if (rows.length === 0) {
          result.games_without_movement += 1;
          continue;
        }

        if (!dryRun) {
          const { error } = await supabase.from("game_events").upsert(rows, {
            onConflict: "match_id,event_type,sequence",
            ignoreDuplicates: false,
          });
          if (error) throw new Error(`movement upsert failed: ${error.message}`);
        }

        result.games_with_movement += 1;
        result.snapshots_inserted += rows.length;
        const pKey = String(movement.providerId);
        result.providers_used[pKey] = (result.providers_used[pKey] ?? 0) + 1;
      } catch (err: any) {
        result.failures.push(`${t.match_id}: ${err?.message ?? String(err)}`);
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

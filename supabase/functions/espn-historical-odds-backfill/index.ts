import { createClient } from "npm:@supabase/supabase-js@2";
import { toCanonicalOdds } from "../_shared/odds-contract.ts";

declare const Deno: any;

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, x-cron-secret",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const DEFAULT_LEAGUES = [
  "arg.1",
  "bra.1",
  "ned.1",
  "tur.1",
  "por.1",
  "bel.1",
  "sco.1",
];

const LEAGUE_MAP: Record<string, string> = {
  epl: "eng.1",
  laliga: "esp.1",
  seriea: "ita.1",
  bundesliga: "ger.1",
  ligue1: "fra.1",
  mls: "usa.1",
  ucl: "uefa.champions",
  uel: "uefa.europa",
  "eng.1": "eng.1",
  "esp.1": "esp.1",
  "ita.1": "ita.1",
  "ger.1": "ger.1",
  "fra.1": "fra.1",
  "usa.1": "usa.1",
  "uefa.champions": "uefa.champions",
  "uefa.europa": "uefa.europa",
  "arg.1": "arg.1",
  "bra.1": "bra.1",
  "ned.1": "ned.1",
  "tur.1": "tur.1",
  "por.1": "por.1",
  "bel.1": "bel.1",
  "sco.1": "sco.1",
};

type MatchRow = {
  id: string;
  league_id: string;
  start_time: string | null;
  status: string | null;
  current_odds: Record<string, unknown> | null;
  odds_total_safe: number | null;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let out = 0;
  for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return out === 0;
}

function isCompleted(status: string | null): boolean {
  const s = String(status ?? "").toUpperCase();
  return s.includes("FULL_TIME") || s.includes("FINAL") || s === "POST";
}

function deriveEventId(match: MatchRow): string | null {
  const fromId = String(match.id ?? "").split("_")[0] ?? "";
  return /^\d+$/.test(fromId) ? fromId : null;
}

function toNum(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const cleaned = value
      .trim()
      .replace(/\u2212/g, "-")
      .replace(/[^\d.+-]/g, "");
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function decimalToAmerican(decimalOdds: number): number | null {
  if (!Number.isFinite(decimalOdds) || decimalOdds <= 1) return null;
  const profit = decimalOdds - 1;
  if (profit >= 1) return Math.round(profit * 100);
  return Math.round(-100 / profit);
}

function extractAmerican(value: any): number | null {
  if (typeof value === "number") return Number.isFinite(value) ? Math.trunc(value) : null;
  if (typeof value === "string") return toNum(value);
  if (!value || typeof value !== "object") return null;

  const american =
    toNum(value.american) ??
    toNum(value.alternateDisplayValue) ??
    toNum(value.displayValue);
  if (american !== null) return Math.trunc(american);

  const decimal = toNum(value.decimal) ?? toNum(value.value);
  if (decimal !== null) return decimalToAmerican(decimal);

  return null;
}

function providerRank(providerName: string): number {
  const p = providerName.toLowerCase();
  if (p.includes("pinnacle")) return 1;
  if (p.includes("bet365") || p.includes("bet 365")) return 2;
  if (p.includes("draftkings")) return 3;
  if (p.includes("caesars")) return 4;
  if (p.includes("fanduel")) return 5;
  if (p.includes("betmgm") || p.includes("mgm")) return 6;
  return 9;
}

function toSpreadPair(item: any): { homeSpread: number | null; awaySpread: number | null } {
  const spread = toNum(item?.spread ?? item?.close?.spread ?? item?.current?.spread ?? item?.open?.spread);
  if (spread === null) return { homeSpread: null, awaySpread: null };
  const abs = Math.abs(spread);
  const homeFav = Boolean(item?.homeTeamOdds?.favorite);
  const awayFav = Boolean(item?.awayTeamOdds?.favorite);
  if (homeFav && !awayFav) return { homeSpread: -abs, awaySpread: abs };
  if (awayFav && !homeFav) return { homeSpread: abs, awaySpread: -abs };
  return { homeSpread: null, awaySpread: null };
}

function parseItemToRawOdds(item: any) {
  const { homeSpread, awaySpread } = toSpreadPair(item);
  return {
    total:
      toNum(item?.overUnder) ??
      toNum(item?.close?.overUnder) ??
      toNum(item?.current?.overUnder) ??
      toNum(item?.open?.overUnder) ??
      null,
    overOdds:
      extractAmerican(item?.overOdds) ??
      extractAmerican(item?.close?.over) ??
      extractAmerican(item?.current?.over) ??
      extractAmerican(item?.open?.over),
    underOdds:
      extractAmerican(item?.underOdds) ??
      extractAmerican(item?.close?.under) ??
      extractAmerican(item?.current?.under) ??
      extractAmerican(item?.open?.under),
    homeML:
      extractAmerican(item?.homeTeamOdds?.moneyLine) ??
      extractAmerican(item?.homeTeamOdds?.close?.moneyLine) ??
      extractAmerican(item?.homeTeamOdds?.current?.moneyLine) ??
      extractAmerican(item?.homeTeamOdds?.open?.moneyLine),
    awayML:
      extractAmerican(item?.awayTeamOdds?.moneyLine) ??
      extractAmerican(item?.awayTeamOdds?.close?.moneyLine) ??
      extractAmerican(item?.awayTeamOdds?.current?.moneyLine) ??
      extractAmerican(item?.awayTeamOdds?.open?.moneyLine),
    drawML:
      extractAmerican(item?.drawOdds?.moneyLine) ??
      extractAmerican(item?.drawOdds?.close?.moneyLine) ??
      extractAmerican(item?.drawOdds?.current?.moneyLine) ??
      extractAmerican(item?.drawOdds?.open?.moneyLine),
    homeSpread,
    awaySpread,
    homeSpreadOdds:
      extractAmerican(item?.homeTeamOdds?.spreadOdds) ??
      extractAmerican(item?.homeTeamOdds?.close?.spread) ??
      extractAmerican(item?.homeTeamOdds?.current?.spread) ??
      extractAmerican(item?.homeTeamOdds?.open?.spread),
    awaySpreadOdds:
      extractAmerican(item?.awayTeamOdds?.spreadOdds) ??
      extractAmerican(item?.awayTeamOdds?.close?.spread) ??
      extractAmerican(item?.awayTeamOdds?.current?.spread) ??
      extractAmerican(item?.awayTeamOdds?.open?.spread),
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

function chooseBestOddsItem(items: any[]): { provider: string; providerId: string; canonical: any } | null {
  let best: { provider: string; providerId: string; canonical: any; score: number } | null = null;

  for (const item of items) {
    const provider = String(item?.provider?.name ?? "ESPN");
    const providerId = String(item?.provider?.id ?? "");
    const rawOdds = parseItemToRawOdds(item);
    const canonical = toCanonicalOdds(rawOdds, {
      provider,
      isLive: false,
      updatedAt: new Date().toISOString(),
    });
    if (!canonical.hasOdds) continue;

    const richness = countPopulated(rawOdds);
    const score = providerRank(provider) * 100 - richness;
    if (!best || score < best.score) {
      best = { provider, providerId, canonical, score };
    }
  }

  return best
    ? { provider: best.provider, providerId: best.providerId, canonical: best.canonical }
    : null;
}

async function fetchJsonWithRetry(url: string, attempts = 4): Promise<any> {
  let lastErr: any = null;
  for (let i = 1; i <= attempts; i++) {
    try {
      const res = await fetch(url, { headers: { "User-Agent": "sportsync-backfill/1.0" } });
      if (res.ok) return await res.json();
      if ((res.status === 429 || res.status === 503) && i < attempts) {
        const waitMs = Math.min(8000, 400 * 2 ** (i - 1));
        await sleep(waitMs);
        continue;
      }
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt.slice(0, 160)}`);
    } catch (err) {
      lastErr = err;
      if (i < attempts) await sleep(Math.min(8000, 400 * 2 ** (i - 1)));
    }
  }
  throw lastErr ?? new Error("fetch failed");
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
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
    const isServiceRole =
      bearer.length > 0 &&
      serviceRole.length > 0 &&
      timingSafeEqual(bearer, serviceRole);
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
    const probeOnly = Boolean(body.probe_only ?? false);
    const limitPerLeague = Math.max(1, Math.min(5000, Number(body.limit_per_league ?? 500)));
    const onlyMissing = body.only_missing !== false;
    const fromDate = body.from_date ? String(body.from_date) : null;
    const toDate = body.to_date ? String(body.to_date) : null;

    const requested = Array.isArray(body.leagues)
      ? body.leagues.map((v: any) => String(v))
      : DEFAULT_LEAGUES;
    const leagues = requested
      .map((l) => String(LEAGUE_MAP[String(l)] ?? l).trim())
      .filter((v, idx, arr) => v && arr.indexOf(v) === idx);

    const supabase = createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });
    const out: any = {
      mode: probeOnly ? "probe" : "backfill",
      dry_run: dryRun,
      leagues,
      per_league: {},
      summary: {
        matches_scanned: 0,
        requests_made: 0,
        odds_found: 0,
        updated: 0,
        skipped_existing: 0,
        failed: 0,
      },
      probe_samples: [] as any[],
    };

    const updates: any[] = [];
    for (const league of leagues) {
      const leagueStats = {
        scanned: 0,
        completed_candidates: 0,
        requests: 0,
        odds_found: 0,
        updates_queued: 0,
        skipped_existing: 0,
        failed: 0,
      };

      let query = supabase
        .from("matches")
        .select(
          "id,league_id,start_time,status,current_odds,odds_total_safe",
        )
        .eq("league_id", league)
        .order("start_time", { ascending: false })
        .limit(limitPerLeague);

      if (fromDate) query = query.gte("start_time", `${fromDate}T00:00:00Z`);
      if (toDate) query = query.lte("start_time", `${toDate}T23:59:59Z`);

      const { data, error } = await query;
      if (error) throw new Error(`matches query failed for ${league}: ${error.message}`);

      const rows = (data ?? []) as MatchRow[];
      leagueStats.scanned = rows.length;
      out.summary.matches_scanned += rows.length;

      for (const match of rows) {
        if (!isCompleted(match.status)) continue;
        leagueStats.completed_candidates++;
        const hasAnyOddsAlready =
          !!match.current_odds ||
          match.odds_total_safe !== null;
        if (onlyMissing && hasAnyOddsAlready) {
          leagueStats.skipped_existing++;
          out.summary.skipped_existing++;
          continue;
        }

        const eventId = deriveEventId(match);
        if (!eventId) {
          leagueStats.failed++;
          out.summary.failed++;
          continue;
        }

        const endpoint =
          `https://sports.core.api.espn.com/v2/sports/soccer/leagues/${league}` +
          `/events/${eventId}/competitions/${eventId}/odds?limit=100`;

        await sleep(220); // ~4.5 req/sec
        leagueStats.requests++;
        out.summary.requests_made++;
        let oddsJson: any;
        try {
          oddsJson = await fetchJsonWithRetry(endpoint, 4);
        } catch (err: any) {
          leagueStats.failed++;
          out.summary.failed++;
          continue;
        }
        const items = Array.isArray(oddsJson?.items) ? oddsJson.items : [];
        const chosen = chooseBestOddsItem(items);

        if (probeOnly && out.probe_samples.length < 12) {
          out.probe_samples.push({
            match_id: match.id,
            event_id: eventId,
            league_id: league,
            provider_count: items.length,
            chosen_provider: chosen?.provider ?? null,
            chosen_provider_id: chosen?.providerId ?? null,
            has_odds: Boolean(chosen?.canonical?.hasOdds),
            total: chosen?.canonical?.total ?? null,
            homeML: chosen?.canonical?.homeML ?? null,
            awayML: chosen?.canonical?.awayML ?? null,
            drawML: chosen?.canonical?.drawML ?? null,
          });
        }

        if (!chosen) continue;
        leagueStats.odds_found++;
        out.summary.odds_found++;

        updates.push({
          id: match.id,
          current_odds: chosen.canonical,
          last_odds_update: new Date().toISOString(),
          odds_api_event_id: `espn_core_${eventId}_${chosen.providerId || "na"}`,
        });
        leagueStats.updates_queued++;
      }

      out.per_league[league] = leagueStats;
    }

    if (!dryRun && !probeOnly && updates.length > 0) {
      for (const batch of chunk(updates, 100)) {
        const { error } = await supabase.rpc("bulk_update_match_odds", { payload: batch });
        if (error) throw new Error(`bulk_update_match_odds failed: ${error.message}`);
        out.summary.updated += batch.length;
      }
    }

    out.summary.updates_queued = updates.length;
    return new Response(JSON.stringify(out), {
      headers: { ...CORS, "Content-Type": "application/json" },
      status: 200,
    });
  } catch (err: any) {
    return new Response(
      JSON.stringify({
        error: err?.message ?? String(err),
      }),
      {
        status: 500,
        headers: { ...CORS, "Content-Type": "application/json" },
      },
    );
  }
});

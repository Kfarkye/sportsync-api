// supabase/functions/espn-enrichment-drain/index.ts
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";
var CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type"
};
var L = {
  info: (e, d = {}) => console.log(JSON.stringify({ level: "INFO", ts: (/* @__PURE__ */ new Date()).toISOString(), fn: "espn-enrichment-drain", event: e, ...d })),
  warn: (e, d = {}) => console.warn(JSON.stringify({ level: "WARN", ts: (/* @__PURE__ */ new Date()).toISOString(), fn: "espn-enrichment-drain", event: e, ...d })),
  error: (e, d = {}) => console.error(JSON.stringify({ level: "ERROR", ts: (/* @__PURE__ */ new Date()).toISOString(), fn: "espn-enrichment-drain", event: e, ...d }))
};
var LEAGUES = {
  nba: { sport: "basketball", espnSlug: "nba", suffix: "_nba", coreLeague: "nba", hasPredictor: true, hasAts: true, spreadK: 6 },
  nfl: { sport: "football", espnSlug: "nfl", suffix: "_nfl", coreLeague: "nfl", hasPredictor: true, hasAts: true, spreadK: 6.8 },
  nhl: { sport: "hockey", espnSlug: "nhl", suffix: "_nhl", coreLeague: "nhl", hasPredictor: true, hasAts: true, spreadK: 2 },
  mlb: { sport: "baseball", espnSlug: "mlb", suffix: "_mlb", coreLeague: "mlb", hasPredictor: true, hasAts: true, spreadK: 3.5 },
  ncaaf: { sport: "football", espnSlug: "college-football", suffix: "_ncaaf", coreLeague: "college-football", hasPredictor: true, hasAts: true, spreadK: 7 },
  ncaab: { sport: "basketball", espnSlug: "mens-college-basketball", suffix: "_ncaab", coreLeague: "mens-college-basketball", hasPredictor: true, hasAts: true, spreadK: 6 },
  epl: { sport: "soccer", espnSlug: "eng.1", suffix: "_epl", coreLeague: "eng.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  laliga: { sport: "soccer", espnSlug: "esp.1", suffix: "_laliga", coreLeague: "esp.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  seriea: { sport: "soccer", espnSlug: "ita.1", suffix: "_seriea", coreLeague: "ita.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  bundesliga: { sport: "soccer", espnSlug: "ger.1", suffix: "_bundesliga", coreLeague: "ger.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  ligue1: { sport: "soccer", espnSlug: "fra.1", suffix: "_ligue1", coreLeague: "fra.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  mls: { sport: "soccer", espnSlug: "usa.1", suffix: "_mls", coreLeague: "usa.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  fifawc: { sport: "soccer", espnSlug: "fifa.world", suffix: "_fifawc", coreLeague: "fifa.world", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  ucl: { sport: "soccer", espnSlug: "uefa.champions", suffix: "_ucl", coreLeague: "uefa.champions", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "ned.1": { sport: "soccer", espnSlug: "ned.1", suffix: "_ned.1", coreLeague: "ned.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "por.1": { sport: "soccer", espnSlug: "por.1", suffix: "_por.1", coreLeague: "por.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "bel.1": { sport: "soccer", espnSlug: "bel.1", suffix: "_bel.1", coreLeague: "bel.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "tur.1": { sport: "soccer", espnSlug: "tur.1", suffix: "_tur.1", coreLeague: "tur.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "bra.1": { sport: "soccer", espnSlug: "bra.1", suffix: "_bra.1", coreLeague: "bra.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "arg.1": { sport: "soccer", espnSlug: "arg.1", suffix: "_arg.1", coreLeague: "arg.1", hasPredictor: true, hasAts: false, spreadK: 1.5 },
  "sco.1": { sport: "soccer", espnSlug: "sco.1", suffix: "_sco.1", coreLeague: "sco.1", hasPredictor: true, hasAts: false, spreadK: 1.5 }
};
var ESPN_SITE = "https://site.api.espn.com/apis/site/v2/sports";
var FETCH_TIMEOUT = 12e3;
var MAX_CONCURRENT = 3;
var INTER_BATCH_DELAY_MS = 300;
var DRAIN_VERSION = "v2.1";
async function safeFetch(url, label) {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT);
    const res = await fetch(url, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) return { data: null, ok: false, error: `${label}: HTTP ${res.status}` };
    const data = await res.json();
    return { data, ok: true };
  } catch (e) {
    return { data: null, ok: false, error: `${label}: ${e.message}` };
  }
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function parallelLimit(tasks, limit, delayMs = INTER_BATCH_DELAY_MS) {
  const results = [];
  for (let i = 0; i < tasks.length; i += limit) {
    const batch = tasks.slice(i, i + limit);
    const batchResults = await Promise.all(batch.map((fn) => fn()));
    for (let j = 0; j < batchResults.length; j++) {
      results[i + j] = batchResults[j];
    }
    if (i + limit < tasks.length && delayMs > 0) await sleep(delayMs);
  }
  return results;
}
function stripLeagueSuffix(value) {
  if (!value) return null;
  return value.replace(/_[a-z0-9.]+$/i, "");
}
function extractSummaryContext(summaryRaw) {
  const competition = summaryRaw?.header?.competitions?.[0];
  const home = competition?.competitors?.find((competitor) => competitor?.homeAway === "home");
  const away = competition?.competitors?.find((competitor) => competitor?.homeAway === "away");
  return {
    homeTeamId: stripLeagueSuffix(home?.team?.id ?? null),
    awayTeamId: stripLeagueSuffix(away?.team?.id ?? null),
    homeTeam: home?.team?.displayName ?? null,
    awayTeam: away?.team?.displayName ?? null,
    startTime: competition?.date ?? summaryRaw?.header?.competitions?.[0]?.date ?? null
  };
}
function getEnrichmentStatus(summaryRaw) {
  return summaryRaw?.header?.competitions?.[0]?.status?.type?.name ?? null;
}
async function loadPostgameSeeds(supabase, leagueKey, league, daysBack, eventIdParam) {
  const errors = [];
  const LOOKUP_BATCH_SIZE = 25;
  const cutoff = /* @__PURE__ */ new Date();
  cutoff.setDate(cutoff.getDate() - daysBack);
  let query = supabase.from("matches").select("id, start_time, home_team, away_team, status").eq("league_id", leagueKey).eq("status", "STATUS_FINAL").gte("start_time", cutoff.toISOString()).order("start_time", { ascending: false });
  if (eventIdParam) {
    const candidateIds = [eventIdParam, `${eventIdParam}${league.suffix}`].filter(Boolean);
    query = query.in("id", candidateIds);
  }
  const { data: matchRows, error: matchError } = await query;
  if (matchError) {
    return { seeds: [], candidateCount: 0, staleCount: 0, skippedFreshCount: 0, errors: [`${leagueKey}: match query failed: ${matchError.message}`] };
  }
  const seeds = [];
  let staleCount = 0;
  let skippedFreshCount = 0;
  for (let index = 0; index < (matchRows || []).length; index += LOOKUP_BATCH_SIZE) {
    const matchBatch = (matchRows || []).slice(index, index + LOOKUP_BATCH_SIZE);
    const existingById = /* @__PURE__ */ new Map();
    const ids = matchBatch.map((row) => row.id);
    if (ids.length > 0) {
      const { data: existingRows, error: enrichmentError } = await supabase.from("espn_enrichment").select("id, summary_raw, home_team_id, away_team_id, home_team, away_team, start_time, home_ats_raw, away_ats_raw, home_stats_raw, away_stats_raw").in("id", ids);
      if (enrichmentError) {
        errors.push(`${leagueKey}: enrichment lookup failed: ${enrichmentError.message}`);
      } else {
        for (const row of existingRows || []) existingById.set(row.id, row);
      }
    }
    for (const matchRow of matchBatch) {
      const existing = existingById.get(matchRow.id);
      const staleStatus = getEnrichmentStatus(existing?.summary_raw);
      const isStale = !existing || staleStatus === "STATUS_SCHEDULED";
      if (!isStale) {
        skippedFreshCount += 1;
        continue;
      }
      const summaryContext = extractSummaryContext(existing?.summary_raw || {});
      const eventId = stripLeagueSuffix(matchRow.id);
      if (!eventId) {
        errors.push(`${leagueKey}: unable to derive event id from ${matchRow.id}`);
        continue;
      }
      staleCount += 1;
      seeds.push({
        id: eventId,
        homeTeamId: stripLeagueSuffix(existing?.home_team_id ?? summaryContext.homeTeamId),
        awayTeamId: stripLeagueSuffix(existing?.away_team_id ?? summaryContext.awayTeamId),
        homeTeam: existing?.home_team ?? summaryContext.homeTeam ?? matchRow.home_team ?? null,
        awayTeam: existing?.away_team ?? summaryContext.awayTeam ?? matchRow.away_team ?? null,
        startTime: existing?.start_time ?? summaryContext.startTime ?? matchRow.start_time ?? null,
        existingRow: existing ?? null,
        source: "postgame"
      });
    }
  }
  return {
    seeds,
    candidateCount: matchRows?.length || 0,
    staleCount,
    skippedFreshCount,
    errors
  };
}
function winProbToImpliedSpread(homeProb, awayProb, k) {
  if (homeProb <= 0 || homeProb >= 1 || awayProb <= 0 || awayProb >= 1) return null;
  const implied = k * Math.log(awayProb / homeProb);
  return Math.round(implied * 10) / 10;
}
function winProbToMoneyline(prob) {
  if (prob <= 0 || prob >= 1) return null;
  if (prob >= 0.5) {
    return Math.round(-100 * prob / (1 - prob));
  } else {
    return Math.round(100 * (1 - prob) / prob);
  }
}
function extractSignals(summary, league) {
  const result = {
    espnWinProb: {},
    espnImpliedSpread: null,
    espnImpliedMoneyline: {},
    marketSpread: null,
    marketTotal: null,
    marketMoneyline: {},
    marketSpreadOdds: {},
    marketOverOdds: null,
    marketUnderOdds: null,
    spreadDivergence: null,
    moneylineDivergence: {},
    provider: null
  };
  if (!summary || typeof summary !== "object") return result;
  const pred = summary.predictor;
  if (pred) {
    const homeProj = parseFloat(pred.homeTeam?.gameProjection || "0");
    const awayProj = parseFloat(pred.awayTeam?.gameProjection || "0");
    if (homeProj > 0 || awayProj > 0) {
      const homeProb = homeProj / 100;
      const awayProb = awayProj / 100;
      result.espnWinProb = { home: homeProb, away: awayProb };
      result.espnImpliedSpread = winProbToImpliedSpread(homeProb, awayProb, league.spreadK);
      result.espnImpliedMoneyline = {
        home: winProbToMoneyline(homeProb),
        away: winProbToMoneyline(awayProb)
      };
    }
  }
  const pc = summary.pickcenter;
  if (Array.isArray(pc) && pc.length > 0) {
    const primary = pc[0];
    result.provider = primary.provider?.name || null;
    if (primary.spread != null) {
      result.marketSpread = parseFloat(primary.spread);
    }
    if (primary.overUnder != null) {
      result.marketTotal = parseFloat(primary.overUnder);
    }
    if (primary.overOdds != null) {
      result.marketOverOdds = parseFloat(primary.overOdds);
    }
    if (primary.underOdds != null) {
      result.marketUnderOdds = parseFloat(primary.underOdds);
    }
    const homeML = primary.homeTeamOdds?.moneyLine;
    const awayML = primary.awayTeamOdds?.moneyLine;
    if (homeML != null || awayML != null) {
      result.marketMoneyline = {
        home: homeML != null ? parseFloat(homeML) : null,
        away: awayML != null ? parseFloat(awayML) : null
      };
    }
    const homeSO = primary.homeTeamOdds?.spreadOdds;
    const awaySO = primary.awayTeamOdds?.spreadOdds;
    if (homeSO != null || awaySO != null) {
      result.marketSpreadOdds = {
        home: homeSO != null ? parseFloat(homeSO) : null,
        away: awaySO != null ? parseFloat(awaySO) : null
      };
    }
  }
  if (result.espnImpliedSpread != null && result.marketSpread != null) {
    result.spreadDivergence = Math.round((result.espnImpliedSpread - result.marketSpread) * 10) / 10;
  }
  if (result.espnImpliedMoneyline?.home != null && result.marketMoneyline?.home != null) {
    result.moneylineDivergence = {
      home: result.espnImpliedMoneyline.home - result.marketMoneyline.home,
      away: (result.espnImpliedMoneyline.away || 0) - (result.marketMoneyline.away || 0)
    };
  }
  return result;
}
async function drainEvent(eventId, league, leagueKey, homeTeamId, awayTeamId, homeTeam, awayTeam, startTime, mode = "upcoming", existingRow = null) {
  const errors = [];
  const endpointsHit = [];
  const t0 = Date.now();
  const { sport, espnSlug } = league;
  const urls = {
    summary: `${ESPN_SITE}/${sport}/${espnSlug}/summary?event=${eventId}`
  };
  if (mode !== "postgame" && homeTeamId) {
    urls.home_stats = `${ESPN_SITE}/${sport}/${espnSlug}/teams/${homeTeamId}/statistics`;
    if (league.hasAts) urls.home_ats = `${ESPN_SITE}/${sport}/${espnSlug}/teams/${homeTeamId}/ats`;
  }
  if (mode !== "postgame" && awayTeamId) {
    urls.away_stats = `${ESPN_SITE}/${sport}/${espnSlug}/teams/${awayTeamId}/statistics`;
    if (league.hasAts) urls.away_ats = `${ESPN_SITE}/${sport}/${espnSlug}/teams/${awayTeamId}/ats`;
  }
  const entries = Object.entries(urls);
  const fetches = entries.map(([key, url]) => async () => {
    const result = await safeFetch(url, key);
    return { key, ...result };
  });
  const results = await parallelLimit(fetches, MAX_CONCURRENT, 100);
  const raw = {};
  for (const r of results) {
    if (r.ok) {
      raw[r.key] = r.data;
      endpointsHit.push(r.key);
    } else if (r.error) {
      errors.push(r.error);
    }
  }
  if (!raw.summary || typeof raw.summary !== "object" || Object.keys(raw.summary).length === 0) {
    errors.push(`summary_missing:${eventId}`);
    return { row: null, errors, endpointsHit };
  }
  const signals = extractSignals(raw.summary || {}, league);
  const durationMs = Date.now() - t0;
  const row = {
    id: `${eventId}${league.suffix}`,
    espn_event_id: eventId,
    league_id: leagueKey,
    sport,
    home_team: homeTeam,
    away_team: awayTeam,
    home_team_id: homeTeamId ? `${homeTeamId}${league.suffix}` : null,
    away_team_id: awayTeamId ? `${awayTeamId}${league.suffix}` : null,
    start_time: startTime,
    // Raw payloads for reprocessing
    summary_raw: raw.summary || {},
    predictor_raw: raw.summary?.predictor || {},
    odds_raw: raw.summary?.pickcenter || {},
    odds_movement_raw: {},
    probabilities_raw: {},
    home_ats_raw: raw.home_ats || existingRow?.home_ats_raw || {},
    away_ats_raw: raw.away_ats || existingRow?.away_ats_raw || {},
    home_stats_raw: raw.home_stats || existingRow?.home_stats_raw || {},
    away_stats_raw: raw.away_stats || existingRow?.away_stats_raw || {},
    home_injuries_raw: raw.summary?.injuries?.[0] || {},
    away_injuries_raw: raw.summary?.injuries?.[1] || {},
    home_records_raw: {},
    away_records_raw: {},
    // ESPN intelligence signals
    espn_win_prob: signals.espnWinProb,
    espn_projected_score: signals.espnImpliedMoneyline,
    // repurposing: stores implied MLs
    espn_power_index: {},
    // Derived spreads
    espn_implied_spread: signals.espnImpliedSpread,
    espn_implied_total: null,
    // ESPN doesn't provide implied totals
    // Market data (from DraftKings via pickcenter)
    market_spread: signals.marketSpread,
    market_total: signals.marketTotal,
    // Divergence: the alpha signal
    spread_divergence: signals.spreadDivergence,
    total_divergence: null,
    // Metadata
    drain_version: DRAIN_VERSION,
    endpoints_hit: endpointsHit,
    last_drained_at: (/* @__PURE__ */ new Date()).toISOString(),
    drain_errors: errors,
    drain_duration_ms: durationMs,
    updated_at: (/* @__PURE__ */ new Date()).toISOString()
  };
  return { row, errors, endpointsHit };
}
Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL"),
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")
  );
  const url = new URL(req.url);
  const leagueParam = url.searchParams.get("league") || "nba";
  const modeParam = (url.searchParams.get("mode") || "upcoming").trim().toLowerCase();
  if (modeParam !== "upcoming" && modeParam !== "postgame") {
    return new Response(JSON.stringify({ error: "Invalid mode. Use upcoming or postgame." }), {
      status: 400,
      headers: { ...CORS, "Content-Type": "application/json" }
    });
  }
  const mode = modeParam;
  const dayWindow = parseInt(url.searchParams.get("days") || (mode === "postgame" ? "5" : "7"));
  const eventIdParam = url.searchParams.get("event");
  const dryRun = url.searchParams.get("dry") === "true";
  const leagueKeys = leagueParam.split(",").map((l) => l.trim().toLowerCase());
  const t0 = Date.now();
  const allRows = [];
  const allErrors = [];
  let totalEndpoints = 0;
  let totalCandidates = 0;
  let totalStale = 0;
  let totalSkippedFresh = 0;
  try {
    for (const leagueKey of leagueKeys) {
      const league = LEAGUES[leagueKey];
      if (!league) {
        allErrors.push(`Unknown league: ${leagueKey}`);
        continue;
      }
      L.info("DRAIN_LEAGUE_START", { league: leagueKey, version: DRAIN_VERSION, mode });
      let seeds = [];
      if (mode === "postgame") {
        const postgameLoad = await loadPostgameSeeds(supabase, leagueKey, league, dayWindow, eventIdParam);
        seeds = postgameLoad.seeds;
        totalCandidates += postgameLoad.candidateCount;
        totalStale += postgameLoad.staleCount;
        totalSkippedFresh += postgameLoad.skippedFreshCount;
        allErrors.push(...postgameLoad.errors);
      } else {
        if (eventIdParam) {
          seeds = [{
            id: eventIdParam,
            homeTeamId: null,
            awayTeamId: null,
            homeTeam: null,
            awayTeam: null,
            startTime: null,
            source: "event"
          }];
        } else {
          const today = /* @__PURE__ */ new Date();
          const end = /* @__PURE__ */ new Date();
          end.setDate(today.getDate() + dayWindow);
          const dateRange = `${fmt(today)}-${fmt(end)}`;
          const groupsParam = leagueKey === "ncaab" ? "&groups=50" : leagueKey === "ncaaf" ? "&groups=80" : "";
          const sbUrl = `${ESPN_SITE}/${league.sport}/${league.espnSlug}/scoreboard?limit=100&dates=${dateRange}${groupsParam}`;
          const sbResult = await safeFetch(sbUrl, "scoreboard");
          if (!sbResult.ok) {
            allErrors.push(`Scoreboard failed for ${leagueKey}: ${sbResult.error}`);
            continue;
          }
          const events = sbResult.data?.events || [];
          seeds = events.map((event) => {
            const comp = event.competitions?.[0];
            const home = comp?.competitors?.find((competitor) => competitor?.homeAway === "home");
            const away = comp?.competitors?.find((competitor) => competitor?.homeAway === "away");
            return {
              id: event.id,
              homeTeamId: home?.team?.id ?? null,
              awayTeamId: away?.team?.id ?? null,
              homeTeam: home?.team?.displayName ?? null,
              awayTeam: away?.team?.displayName ?? null,
              startTime: event.date ?? null,
              source: "scoreboard"
            };
          });
        }
        totalCandidates += seeds.length;
        totalStale += seeds.length;
      }
      L.info("EVENTS_FOUND", { league: leagueKey, count: seeds.length, mode });
      for (const seed of seeds) {
        const { row, errors, endpointsHit } = await drainEvent(
          seed.id,
          league,
          leagueKey,
          seed.homeTeamId,
          seed.awayTeamId,
          seed.homeTeam,
          seed.awayTeam,
          seed.startTime,
          mode,
          seed.existingRow ?? null
        );
        allErrors.push(...errors);
        totalEndpoints += endpointsHit.length;
        if (!row) continue;
        allRows.push(row);
        L.info("EVENT_DRAINED", {
          event: seed.id,
          source: seed.source,
          home: row.home_team,
          away: row.away_team,
          espnSpread: row.espn_implied_spread,
          mktSpread: row.market_spread,
          divergence: row.spread_divergence,
          winProb: row.espn_win_prob
        });
      }
    }
    let upsertCount = 0;
    if (!dryRun && allRows.length > 0) {
      const BATCH = 25;
      for (let i = 0; i < allRows.length; i += BATCH) {
        const batch = allRows.slice(i, i + BATCH);
        const { error } = await supabase.from("espn_enrichment").upsert(batch, { onConflict: "id" });
        if (error) {
          L.error("UPSERT_FAILED", { batch: i / BATCH, error: error.message });
          allErrors.push(`Upsert batch ${i / BATCH}: ${error.message}`);
        } else {
          upsertCount += batch.length;
        }
      }
    }
    const durationMs = Date.now() - t0;
    const status = allErrors.length === 0 ? "success" : allRows.length > 0 ? "partial" : "failure";
    if (!dryRun) {
      await supabase.from("espn_drain_log").insert({
        leagues_queried: leagueKeys,
        events_found: allRows.length,
        events_drained: upsertCount,
        endpoints_total: totalEndpoints,
        errors: allErrors.slice(0, 50),
        duration_ms: durationMs,
        drain_version: DRAIN_VERSION,
        status
      });
    }
    const summary = {
      success: true,
      version: DRAIN_VERSION,
      mode,
      dryRun,
      leagues: leagueKeys,
      candidates: totalCandidates,
      staleCount: mode === "postgame" ? totalStale : null,
      skippedFreshCount: mode === "postgame" ? totalSkippedFresh : null,
      eventsFound: allRows.length,
      eventsDrained: dryRun ? 0 : upsertCount,
      refreshedCount: dryRun ? 0 : upsertCount,
      endpointsTotal: totalEndpoints,
      errorsCount: allErrors.length,
      durationMs,
      status: dryRun ? "dry_run" : status,
      divergences: allRows.filter((r) => r.spread_divergence != null).map((r) => ({
        id: r.id,
        home: r.home_team,
        away: r.away_team,
        espnWinProb: r.espn_win_prob,
        espnImpliedSpread: r.espn_implied_spread,
        marketSpread: r.market_spread,
        marketTotal: r.market_total,
        spreadDivergence: r.spread_divergence
      })).sort((a, b) => Math.abs(b.spreadDivergence) - Math.abs(a.spreadDivergence)),
      errors: allErrors.slice(0, 20)
    };
    return new Response(JSON.stringify(summary, null, 2), {
      headers: { ...CORS, "Content-Type": "application/json" }
    });
  } catch (err) {
    L.error("FATAL", { error: err.message, stack: err.stack?.substring(0, 500) });
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { ...CORS, "Content-Type": "application/json" }
    });
  }
});
function fmt(d) {
  return d.toISOString().split("T")[0].replace(/-/g, "");
}

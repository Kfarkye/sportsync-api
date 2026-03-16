import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";

interface GameState {
  matchId: string;
  leagueId: string;
  espnEventId: string;
  homeTeam: string;
  awayTeam: string;
  homeScore: number;
  awayScore: number;
  matchMinute: number;
  gameClock: string;
  status: "pregame" | "live_1h" | "halftime" | "live_2h" | "fulltime";
  homeRed: number;
  awayRed: number;
}

interface OddsSnapshot {
  source: string;
  liveTotal: number | null;
  liveOverPrice: number | null;
  liveUnderPrice: number | null;
  oddsFormat: string;
  liveHomeMl: number | null;
  liveAwayMl: number | null;
  liveDrawMl: number | null;
  liveSpread: number | null;
  liveHomeSpreadPrice: number | null;
  liveAwaySpreadPrice: number | null;
  altLines: Record<string, number> | null;
  liveBttsYes: number | null;
  liveBttsNo: number | null;
}

interface PriorSnapshot {
  homeScore: number;
  awayScore: number;
  matchMinute: number;
  triggerType: string;
  homeRed: number;
  awayRed: number;
}

interface TriggerResult {
  shouldCapture: boolean;
  triggerType: string;
  triggerDetail: string | null;
}

const LEAGUE_ESPN_MAP: Record<string, { sport: string; league: string }> = {
  epl: { sport: "soccer", league: "eng.1" },
  laliga: { sport: "soccer", league: "esp.1" },
  bundesliga: { sport: "soccer", league: "ger.1" },
  seriea: { sport: "soccer", league: "ita.1" },
  ligue1: { sport: "soccer", league: "fra.1" },
  mls: { sport: "soccer", league: "usa.1" },
  ucl: { sport: "soccer", league: "uefa.champions" },
  uel: { sport: "soccer", league: "uefa.europa" },
  "arg.1": { sport: "soccer", league: "arg.1" },
  "bra.1": { sport: "soccer", league: "bra.1" },
  "ned.1": { sport: "soccer", league: "ned.1" },
  "tur.1": { sport: "soccer", league: "tur.1" },
  "por.1": { sport: "soccer", league: "por.1" },
  "bel.1": { sport: "soccer", league: "bel.1" },
  "sco.1": { sport: "soccer", league: "sco.1" },
};

const ODDS_API_SPORT_MAP: Record<string, string> = {
  epl: "soccer_epl",
  laliga: "soccer_spain_la_liga",
  bundesliga: "soccer_germany_bundesliga",
  seriea: "soccer_italy_serie_a",
  ligue1: "soccer_france_ligue_one",
  mls: "soccer_usa_mls",
  ucl: "soccer_uefa_champs_league",
  uel: "soccer_uefa_europa_league",
  "arg.1": "soccer_argentina_primera",
  "bra.1": "soccer_brazil_serie_a",
  "ned.1": "soccer_netherlands_eredivisie",
  "tur.1": "soccer_turkey_super_league",
  "por.1": "soccer_portugal_primeira",
  "bel.1": "soccer_belgium_first_div",
  "sco.1": "soccer_scotland_premiership",
};

const INTERVAL_MINUTES = [15, 30, 60, 75];
const INTERVAL_TOLERANCE = 3;

function getSupabaseClient() {
  const url = Deno.env.get("SUPABASE_URL");
  const key = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
  if (!url || !key) throw new Error("Missing Supabase credentials");
  return createClient(url, key);
}

async function fetchLiveGameStates(leagueId: string): Promise<GameState[]> {
  const espnConfig = LEAGUE_ESPN_MAP[leagueId];
  if (!espnConfig) return [];

  const url = `https://site.api.espn.com/apis/site/v2/sports/${espnConfig.sport}/${espnConfig.league}/scoreboard`;
  const res = await fetch(url, {
    headers: { "User-Agent": "TheDrip/1.0" },
  });

  if (!res.ok) {
    console.error(`ESPN ${leagueId} fetch failed: ${res.status}`);
    return [];
  }

  const data = await res.json();
  const states: GameState[] = [];

  for (const event of data.events ?? []) {
    const competition = event.competitions?.[0];
    if (!competition) continue;

    const statusDetail = competition.status?.type?.name ?? "STATUS_UNKNOWN";
    const displayClock = competition.status?.displayClock ?? "";
    const period = competition.status?.period ?? 0;
    const minuteRaw = parseInt(displayClock.split(":")[0]) || 0;

    let status: GameState["status"];
    let matchMinute: number;

    if (statusDetail === "STATUS_SCHEDULED" || statusDetail === "STATUS_WARMUP") {
      status = "pregame";
      matchMinute = 0;
    } else if (statusDetail === "STATUS_FIRST_HALF" || (statusDetail === "STATUS_IN_PROGRESS" && period === 1)) {
      status = "live_1h";
      matchMinute = minuteRaw;
    } else if (statusDetail === "STATUS_HALFTIME") {
      status = "halftime";
      matchMinute = 45;
    } else if (statusDetail === "STATUS_SECOND_HALF" || (statusDetail === "STATUS_IN_PROGRESS" && period === 2)) {
      status = "live_2h";
      matchMinute = 45 + minuteRaw;
    } else if (statusDetail === "STATUS_FULL_TIME" || statusDetail === "STATUS_FINAL") {
      status = "fulltime";
      matchMinute = 90;
    } else {
      continue;
    }

    const homeComp = competition.competitors?.find((c: any) => c.homeAway === "home");
    const awayComp = competition.competitors?.find((c: any) => c.homeAway === "away");
    if (!homeComp || !awayComp) continue;

    const extractReds = (comp: any): number => {
      const stats = comp.statistics;
      if (!stats) return 0;
      const redStat = stats.find((s: any) => s.name === "redCards");
      return redStat ? parseInt(redStat.displayValue) || 0 : 0;
    };

    const espnEventId = event.id;
    const matchId = `${espnEventId}_${leagueId}`;

    states.push({
      matchId,
      leagueId,
      espnEventId,
      homeTeam: homeComp.team?.displayName ?? "Unknown",
      awayTeam: awayComp.team?.displayName ?? "Unknown",
      homeScore: parseInt(homeComp.score) || 0,
      awayScore: parseInt(awayComp.score) || 0,
      matchMinute,
      gameClock: displayClock || status,
      status,
      homeRed: extractReds(homeComp),
      awayRed: extractReds(awayComp),
    });
  }

  return states;
}

async function fetchLiveOdds(
  leagueId: string,
  homeTeam: string,
  awayTeam: string
): Promise<OddsSnapshot[]> {
  const apiKey = Deno.env.get("ODDS_API_KEY");
  if (!apiKey) {
    console.warn("No ODDS_API_KEY set — skipping odds fetch");
    return [];
  }

  const sport = ODDS_API_SPORT_MAP[leagueId];
  if (!sport) return [];

  const url = new URL(`https://api.the-odds-api.com/v4/sports/${sport}/odds`);
  url.searchParams.set("apiKey", apiKey);
  url.searchParams.set("regions", "us,uk");
  url.searchParams.set("markets", "h2h,totals,spreads");
  url.searchParams.set("oddsFormat", "american");

  try {
    const res = await fetch(url.toString());
    if (!res.ok) {
      console.error(`Odds API failed: ${res.status}`);
      return [];
    }

    const events = await res.json();
    const snapshots: OddsSnapshot[] = [];

    const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
    const homeNorm = normalize(homeTeam);
    const awayNorm = normalize(awayTeam);

    const matchEvent = events.find((e: any) => {
      const h = normalize(e.home_team ?? "");
      const a = normalize(e.away_team ?? "");
      return (
        (h.includes(homeNorm.slice(0, 6)) || homeNorm.includes(h.slice(0, 6))) &&
        (a.includes(awayNorm.slice(0, 6)) || awayNorm.includes(a.slice(0, 6)))
      );
    });

    if (!matchEvent) return [];

    for (const bookmaker of matchEvent.bookmakers ?? []) {
      const source = bookmaker.key;
      const snapshot: OddsSnapshot = {
        source,
        liveTotal: null,
        liveOverPrice: null,
        liveUnderPrice: null,
        oddsFormat: "american",
        liveHomeMl: null,
        liveAwayMl: null,
        liveDrawMl: null,
        liveSpread: null,
        liveHomeSpreadPrice: null,
        liveAwaySpreadPrice: null,
        altLines: null,
        liveBttsYes: null,
        liveBttsNo: null,
      };

      for (const market of bookmaker.markets ?? []) {
        if (market.key === "h2h") {
          for (const outcome of market.outcomes ?? []) {
            const normName = normalize(outcome.name ?? "");
            if (normName.includes("draw")) {
              snapshot.liveDrawMl = outcome.price;
            } else if (normName.includes(homeNorm.slice(0, 6)) || homeNorm.includes(normName.slice(0, 6))) {
              snapshot.liveHomeMl = outcome.price;
            } else {
              snapshot.liveAwayMl = outcome.price;
            }
          }
        }

        if (market.key === "totals") {
          const altLines: Record<string, number> = {};
          for (const outcome of market.outcomes ?? []) {
            const point = outcome.point;
            if (outcome.name === "Over") {
              if (snapshot.liveTotal === null || Math.abs(point - 2.5) < Math.abs(snapshot.liveTotal - 2.5)) {
                snapshot.liveTotal = point;
                snapshot.liveOverPrice = outcome.price;
              }
              altLines[`o${point}`] = outcome.price;
            }
            if (outcome.name === "Under") {
              if (snapshot.liveTotal !== null && point === snapshot.liveTotal) {
                snapshot.liveUnderPrice = outcome.price;
              }
              altLines[`u${point}`] = outcome.price;
            }
          }
          if (Object.keys(altLines).length > 0) {
            snapshot.altLines = altLines;
          }
        }

        if (market.key === "spreads") {
          for (const outcome of market.outcomes ?? []) {
            const normName = normalize(outcome.name ?? "");
            if (normName.includes(homeNorm.slice(0, 6)) || homeNorm.includes(normName.slice(0, 6))) {
              snapshot.liveSpread = outcome.point;
              snapshot.liveHomeSpreadPrice = outcome.price;
            } else {
              snapshot.liveAwaySpreadPrice = outcome.price;
            }
          }
        }
      }

      if (snapshot.liveTotal !== null || snapshot.liveHomeMl !== null) {
        snapshots.push(snapshot);
      }
    }

    return snapshots;
  } catch (err) {
    console.error("Odds API error:", err);
    return [];
  }
}

function detectTrigger(
  current: GameState,
  prior: PriorSnapshot | null
): TriggerResult {
  const noCapture: TriggerResult = { shouldCapture: false, triggerType: "", triggerDetail: null };

  if (!prior) {
    if (current.status === "pregame") {
      return { shouldCapture: true, triggerType: "pregame", triggerDetail: null };
    }
    if (current.status === "live_1h" && current.matchMinute <= 5) {
      return { shouldCapture: true, triggerType: "kickoff", triggerDetail: null };
    }
    return { shouldCapture: true, triggerType: "interval", triggerDetail: `joined_at_${current.matchMinute}` };
  }

  const currentTotal = current.homeScore + current.awayScore;
  const priorTotal = prior.homeScore + prior.awayScore;
  if (currentTotal > priorTotal) {
    const scorer = current.homeScore > prior.homeScore ? "home" : "away";
    return {
      shouldCapture: true,
      triggerType: "goal",
      triggerDetail: `${scorer}_goal_${current.homeScore}-${current.awayScore}`,
    };
  }

  const currentReds = current.homeRed + current.awayRed;
  const priorReds = prior.homeRed + prior.awayRed;
  if (currentReds > priorReds) {
    const side = current.homeRed > prior.homeRed ? "home" : "away";
    return {
      shouldCapture: true,
      triggerType: "red_card",
      triggerDetail: `${side}_red_${current.matchMinute}`,
    };
  }

  if (current.status === "halftime" && prior.triggerType !== "halftime") {
    return { shouldCapture: true, triggerType: "halftime", triggerDetail: `${current.homeScore}-${current.awayScore}` };
  }

  if (current.status === "fulltime" && prior.triggerType !== "final") {
    return { shouldCapture: true, triggerType: "final", triggerDetail: `${current.homeScore}-${current.awayScore}` };
  }

  for (const target of INTERVAL_MINUTES) {
    const inWindow = Math.abs(current.matchMinute - target) <= INTERVAL_TOLERANCE;
    const priorNotInWindow = Math.abs(prior.matchMinute - target) > INTERVAL_TOLERANCE;
    if (inWindow && priorNotInWindow) {
      return {
        shouldCapture: true,
        triggerType: "interval",
        triggerDetail: `${target}min`,
      };
    }
  }

  return noCapture;
}

async function getPriorSnapshot(
  supabase: ReturnType<typeof createClient>,
  matchId: string
): Promise<PriorSnapshot | null> {
  const { data, error } = await supabase
    .from("soccer_live_odds_snapshots")
    .select("home_score, away_score, match_minute, trigger_type")
    .eq("match_id", matchId)
    .order("captured_at", { ascending: false })
    .limit(1)
    .single();

  if (error || !data) return null;

  return {
    homeScore: data.home_score,
    awayScore: data.away_score,
    matchMinute: data.match_minute,
    triggerType: data.trigger_type,
    homeRed: 0,
    awayRed: 0,
  };
}

async function storeSnapshot(
  supabase: ReturnType<typeof createClient>,
  game: GameState,
  odds: OddsSnapshot,
  trigger: TriggerResult,
  sequence: number
): Promise<boolean> {
  const id = `${game.matchId}_${odds.source}_${trigger.triggerType}_${sequence}`;

  const row = {
    id,
    match_id: game.matchId,
    league_id: game.leagueId,
    source: odds.source,
    captured_at: new Date().toISOString(),
    game_clock: game.gameClock,
    match_minute: game.matchMinute,
    trigger_type: trigger.triggerType,
    trigger_detail: trigger.triggerDetail,
    home_score: game.homeScore,
    away_score: game.awayScore,
    live_total: odds.liveTotal,
    live_over_price: odds.liveOverPrice,
    live_under_price: odds.liveUnderPrice,
    odds_format: odds.oddsFormat,
    live_home_ml: odds.liveHomeMl,
    live_away_ml: odds.liveAwayMl,
    live_draw_ml: odds.liveDrawMl,
    live_spread: odds.liveSpread,
    live_home_spread_price: odds.liveHomeSpreadPrice,
    live_away_spread_price: odds.liveAwaySpreadPrice,
    alt_lines: odds.altLines,
    live_btts_yes: odds.liveBttsYes,
    live_btts_no: odds.liveBttsNo,
    player_props: null,
    drain_version: "v1.0",
  };

  const { error } = await supabase
    .from("soccer_live_odds_snapshots")
    .upsert(row, { onConflict: "id" });

  if (error) {
    console.error(`Store failed for ${id}:`, error.message);
    return false;
  }
  return true;
}

Deno.serve(async (req: Request) => {
  const startTime = Date.now();
  const results = {
    leagues_scanned: 0,
    live_matches_found: 0,
    triggers_fired: 0,
    snapshots_stored: 0,
    errors: [] as string[],
  };

  try {
    const supabase = getSupabaseClient();

    const url = new URL(req.url);
    const leagueParam = url.searchParams.get("leagues");
    const targetLeagues = leagueParam
      ? leagueParam.split(",")
      : Object.keys(LEAGUE_ESPN_MAP);

    for (const leagueId of targetLeagues) {
      results.leagues_scanned++;

      const games = await fetchLiveGameStates(leagueId);

      for (const game of games) {
        if (game.status === "fulltime") {
          const prior = await getPriorSnapshot(supabase, game.matchId);
          if (prior?.triggerType === "final") continue;
        }

        if (game.status === "pregame") continue;

        results.live_matches_found++;

        const prior = await getPriorSnapshot(supabase, game.matchId);
        const trigger = detectTrigger(game, prior);

        if (!trigger.shouldCapture) continue;
        results.triggers_fired++;

        const oddsSnapshots = await fetchLiveOdds(leagueId, game.homeTeam, game.awayTeam);

        if (oddsSnapshots.length === 0) {
          const emptyOdds: OddsSnapshot = {
            source: "espn_state_only",
            liveTotal: null,
            liveOverPrice: null,
            liveUnderPrice: null,
            oddsFormat: "american",
            liveHomeMl: null,
            liveAwayMl: null,
            liveDrawMl: null,
            liveSpread: null,
            liveHomeSpreadPrice: null,
            liveAwaySpreadPrice: null,
            altLines: null,
            liveBttsYes: null,
            liveBttsNo: null,
          };
          const { count } = await supabase
            .from("soccer_live_odds_snapshots")
            .select("id", { count: "exact", head: true })
            .eq("match_id", game.matchId);
          const seq = (count ?? 0) + 1;

          const stored = await storeSnapshot(supabase, game, emptyOdds, trigger, seq);
          if (stored) results.snapshots_stored++;
          continue;
        }

        for (const odds of oddsSnapshots) {
          const { count } = await supabase
            .from("soccer_live_odds_snapshots")
            .select("id", { count: "exact", head: true })
            .eq("match_id", game.matchId)
            .eq("source", odds.source);
          const seq = (count ?? 0) + 1;

          const stored = await storeSnapshot(supabase, game, odds, trigger, seq);
          if (stored) results.snapshots_stored++;
        }
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    results.errors.push(message);
    console.error("Fatal error:", message);
  }

  const elapsed = Date.now() - startTime;

  return new Response(
    JSON.stringify({ ...results, elapsed_ms: elapsed }),
    {
      status: 200,
      headers: { "Content-Type": "application/json", Connection: "keep-alive" },
    }
  );
});

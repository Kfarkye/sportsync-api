// =============================================================================
// CANONICAL GAME STATE ENGINE (v3.4 - INSTITUTIONAL PRODUCTION)
// Deterministic normalization of situational sport data into cross-sport market indices.
// Features: NFL/NCAAF Split, Bayesian PPD Gating, Push Protection, Applied Weather.
// =============================================================================

import {
    Match,
    Sport,
    Team,
    AISignals,
    EdgeEnvironmentTag,
    PregameConstraints,
    OddsSnapshot,
    MarketBlueprint,
    MarketScope,
    SystemState,
} from "./types.ts";
import {
    ExtendedMatch,
    TeamEfficiencyMatrix,
    FairTotalResult,
    FairTotalActive,
    FairTotalNoBet,
    UnifiedStatContainer,
    WeatherInfo,
    TrenchAnalytics
} from "./types.ts";

import { SYSTEM_GATES } from "./gates.ts";
import {
    getElapsedSeconds,
    getRemainingSeconds,
    calculateGameProgress,
    isFinalLikeClock,
    isCollegeBasketball,
    getBaseballInning,
    getBaseballState
} from "./engine/time.ts";
import {
    clamp,
    lerp,
    safeDiv,
    isBasketball,
    isFootball,
    isNCAAF,
    getStatNumber,
    findStatValue,
    parseStatNumber,
    parseMadeAttempt,
    calculateBlowoutState,
    getBasketballPossessions,
    computePitchingWHIP,
    calculateSeasonPhase,
    getMarketEfficiency,
    calculatePatternReinforcement
} from "./engine/utils.ts";

import { calculateHockeyFairTotal, calculateHockeyEfficiency } from "./engine/physics/hockey.ts";
import { calculateBasketballEfficiency, calculateBasketballFairTotal } from "./engine/physics/basketball.ts";
import { calculateFootballEfficiency, calculateFootballFairTotal } from "./engine/physics/football.ts";
import { calculateBaseballEfficiency, calculateBaseballFairTotal } from "./engine/physics/baseball.ts";
import { calculateSoccerEfficiency, calculateSoccerFairTotal } from "./engine/physics/soccer.ts";

import {
    calculateLiabilityInertia,
    calculateEdgeEnvironment,
    getMarketBlueprint,
    calculateNFLTotalOverride,
    getCanonicalOdds,
    getRegimeMultiplier
} from "./engine/market.ts";
import { calculatePregameConstraints } from "./engine/signals/pregame.ts";

// =============================================================================
// 1. STRICT TYPES & INTERFACES
// =============================================================================

// [Removed: Local Types & Interfaces - Now in src/types/engine.ts]

// [Removed: SYSTEM_GATES & REGEX - Now in src/config/gates.ts]

// [Removed: Robust Utilities - Now in src/services/engine/utils.ts]

function getWeather(match: ExtendedMatch): WeatherInfo | undefined {
    return match.weather_info || match.weather_forecast;
}

// [Removed: Sport-Specific State Extractors & Time Progress - Now in src/services/engine/time.ts]

// =============================================================================
// 6. ODDS NORMALIZER
// =============================================================================

// The getCanonicalOdds function is now imported from "./engine/market.ts"
// export function getCanonicalOdds(match: Match): OddsSnapshot {
//     const o = match.opening_odds || {};
//     // v3.6: Prioritize Current Odds (Live) -> Consensus -> Opening
//     const c = (match.current_odds && Object.keys(match.current_odds).length > 0)
//         ? match.current_odds
//         : (match.odds || {});

//     const parse = (val: any, type: "spread" | "price" | "total") => {
//         const v = getOddsValue(val, type);
//         return Number.isFinite(v) ? v : undefined;
//     };

//     return {
//         open: {
//             spread: parse(o.homeSpread ?? o.spread, "spread") ?? 0,
//             total: parse(o.overUnder, "total") ?? 0,
//             mlHome: parse(o.moneylineHome ?? o.homeWin, "price") ?? 0,
//             mlAway: parse(o.moneylineAway ?? o.awayWin, "price") ?? 0,
//             mlDraw: parse(o.moneylineDraw ?? o.draw, "price") ?? 0,
//             homeSpreadPrice: 0, awaySpreadPrice: 0, spreadPrice: 0, overPrice: 0, underPrice: 0, totalPrice: 0
//         },
//         cur: {
//             spread: parse(c.spread ?? c.homeSpread, "spread") ?? 0,
//             total: parse(c.total ?? c.overUnder, "total") ?? 0,
//             mlHome: parse(c.homeWin ?? c.moneylineHome ?? c.home_ml, "price") ?? -110,
//             mlAway: parse(c.awayWin ?? c.moneylineAway ?? c.away_ml, "price") ?? -110,
//             mlDraw: parse(c.draw ?? (c as any).moneylineDraw ?? c.draw_ml, "price") ?? 0,
//             totalPrice: parse(c.overOdds ?? c.over_odds, "price") ?? -110
//         },
//         hasSpread: c.spread !== undefined || c.homeSpread !== undefined,
//         hasTotal: c.total !== undefined || c.overUnder !== undefined,
//         hasML: c.homeWin !== undefined || c.moneylineHome !== undefined || c.home_ml !== undefined
//     };
// }

// =============================================================================
// 7. CORE ANALYTICS: EFFICIENCY MATRIX
// =============================================================================

function calculateEfficiencyMatrix(match: ExtendedMatch, odds: OddsSnapshot): TeamEfficiencyMatrix {
    if (isBasketball(match.sport)) {
        return calculateBasketballEfficiency(match);
    }

    if (match.sport === Sport.HOCKEY) {
        return calculateHockeyEfficiency(match, odds);
    }

    if (isFootball(match.sport)) {
        return calculateFootballEfficiency(match);
    }

    if (match.sport === Sport.BASEBALL) {
        return calculateBaseballEfficiency(match);
    }

    if (match.sport === Sport.SOCCER) {
        return calculateSoccerEfficiency(match);
    }

    return { sport_type: "GENERIC", home: { pace: 0 }, away: { pace: 0 }, context: "STANDARD" };
}

// =============================================================================
// 8. FAIR TOTAL ENGINE
// =============================================================================


function calculateFairTotalBySport(match: ExtendedMatch, odds: OddsSnapshot, efficiency: TeamEfficiencyMatrix, pregame: PregameConstraints): FairTotalResult {
    try {
        const isNCAAB = isCollegeBasketball(match);
        const timeRem = getRemainingSeconds(match);
        const currentPts = (match.homeScore || 0) + (match.awayScore || 0);
        const totalTime = isBasketball(match.sport) ? (isNCAAB ? 2400 : 2880) : 3600;

        const timeFactor = Math.sqrt(Math.max(0.1, timeRem) / totalTime);
        let sd = Math.max(0.15, 1.0 * timeFactor);

        if (isFinalLikeClock(match.displayClock, match.status)) return { status: "NO_BET", reason: "Game Final" };

        // v6.6: Robust Odds Fallback - Use opening total if current total is missing
        // This handles halftime/intermission scenarios where live odds may be unavailable
        const marketTotal = odds.cur.total > 0 ? odds.cur.total : (odds.open.total > 0 ? odds.open.total : 0);
        if (marketTotal <= 0) return { status: "NO_BET", reason: "Critical: Total is Invalid" };

        // Override odds.cur.total with the resolved marketTotal for downstream calculations
        const effectiveOdds: OddsSnapshot = {
            ...odds,
            cur: { ...odds.cur, total: marketTotal }
        };

        let fairTotal = currentPts;
        let regime: FairTotalActive["regime"] = "NORMAL";
        let pushRisk = false;

        if (calculateBlowoutState(match, timeRem)) {
            regime = "BLOWOUT";
            sd = 2.5;
        }

        let varianceFlags: any = undefined;

        if (efficiency.sport_type === "HOCKEY") {
            const hky = calculateHockeyFairTotal(match, efficiency, pregame, timeRem, currentPts, regime === "BLOWOUT" ? "BLOWOUT" : "NORMAL");
            fairTotal = hky.fairTotal;
            // Map strict kernel types back to general engine types
            regime = hky.regime as any;
            sd = hky.sd;
            if (hky.flags) {
                varianceFlags = hky.flags;
            }
            // v6.0 Observability
            if (hky.trace) {
                (varianceFlags as any) = { ...varianceFlags, _trace: hky.trace };
            }
        }
        else if (efficiency.sport_type === "BASKETBALL") {
            const bsk = calculateBasketballFairTotal(match, effectiveOdds, efficiency, timeRem, currentPts);
            fairTotal = bsk.fairTotal;
            regime = bsk.regime as any;

            // Reconstruct Variance Flags & Range Band from Physics Output
            const targetMins = isCollegeBasketball(match) ? 40 : 48;
            const elapsedMins = (targetMins * 60 - timeRem) / 60;
            const rangeWidth = Math.max(4, 9 - (elapsedMins / 10));
            sd = rangeWidth / 1.5; // Map rangeWidth to Standard Deviation proxy

            return {
                status: "ACTIVE",
                fair_total: Number(fairTotal.toFixed(2)),
                p10: Number((fairTotal - rangeWidth).toFixed(2)),
                p90: Number((fairTotal + rangeWidth).toFixed(2)),
                variance_sd: Number(sd.toFixed(2)),
                regime,
                pace_multiplier: regime === "BLOWOUT" ? 0.9 : 1.0,
                range_band: {
                    low: Number((fairTotal - rangeWidth).toFixed(1)),
                    high: Number((fairTotal + rangeWidth).toFixed(1))
                },
                variance_flags: bsk.flags
            };
        }
        else if (efficiency.sport_type === "FOOTBALL") {
            const fb = calculateFootballFairTotal(match, effectiveOdds, efficiency, timeRem, currentPts, timeFactor);
            fairTotal = fb.fairTotal;
            regime = fb.regime as any;
            sd = fb.sd;
            pushRisk = fb.pushRisk;
        }
        else if (efficiency.sport_type === "BASEBALL") {
            const bb = calculateBaseballFairTotal(match, effectiveOdds, efficiency, timeRem, currentPts);
            if (bb.status === "NO_BET") return { status: "NO_BET", reason: (bb.reason as any) || "Game Final" };
            fairTotal = bb.fairTotal;
            regime = bb.regime as any;
            sd = bb.sd;
        }
        else if (efficiency.sport_type === "SOCCER") {
            const sc = calculateSoccerFairTotal(match, effectiveOdds, efficiency, timeRem, currentPts);
            fairTotal = sc.fairTotal;
            regime = sc.regime as any;
            sd = sc.sd;
            pushRisk = sc.pushRisk;
            varianceFlags = sc.flags;
        }
        else {
            const rate = safeDiv(effectiveOdds.cur.total, 3600);
            fairTotal = currentPts + (timeRem * rate);
        }

        return {
            status: "ACTIVE",
            fair_total: Number(fairTotal.toFixed(2)),
            p10: Number((fairTotal - (1.5 * sd)).toFixed(2)),
            p90: Number((fairTotal + (1.5 * sd)).toFixed(2)),
            variance_sd: Number(sd.toFixed(2)),
            regime,
            pace_multiplier: regime === "BLOWOUT" ? 0.9 : 1.0,
            push_risk: pushRisk,
            variance_flags: varianceFlags
        };

    } catch (e) {
        return { status: "NO_BET", reason: "Calculation Error" };
    }
}

function calculatePhase(match: Match): string {
    const clock = (match.displayClock || "").toUpperCase();
    if (clock === "FINAL" || clock === "F" || clock === "FT") return "FINAL";
    if (getElapsedSeconds(match as ExtendedMatch) > 0) return "LIVE";
    if (match.sport === Sport.BASEBALL && (match.period || 0) >= 1) return "LIVE";
    return "PRE";
}

function calculateNewsAdjustment(match: ExtendedMatch): number {
    if (match.venue?.is_indoor) return 0;
    if (!isFootball(match.sport)) return 0;

    const w = getWeather(match);
    if (w && parseStatNumber(w.wind_speed) > SYSTEM_GATES.WIND_THRESHOLD_MPH) return SYSTEM_GATES.WIND_IMPACT_POINTS;
    return 0;
}

// --- META FUNCTIONS ---


// =============================================================================
// 9. MAIN EXPORT
// =============================================================================

export const computeAISignals = (match: Match): AISignals => {
    const extMatch = match as ExtendedMatch;
    const rawOdds = getCanonicalOdds(match);

    // v6.7: GLOBAL ODDS RESOLUTION (The Halftime Guard)
    // Resolve market total at the top level so ALL downstream calculations use consistent data
    const resolvedMarketTotal = rawOdds.cur.total > 0
        ? rawOdds.cur.total
        : (rawOdds.open.total > 0 ? rawOdds.open.total : 0);
    const odds: OddsSnapshot = {
        ...rawOdds,
        cur: { ...rawOdds.cur, total: resolvedMarketTotal }
    };

    const progress = calculateGameProgress(extMatch);
    const phase = calculatePhase(match);

    const efficiency = calculateEfficiencyMatrix(extMatch, odds);
    const pregame = calculatePregameConstraints(extMatch);

    // v5.8: Fair Total now takes pregame constraints (B2B, Elite status) into account
    const fair = calculateFairTotalBySport(extMatch, odds, efficiency, pregame);

    const isFinished = isFinalLikeClock(match.displayClock, match.status);
    const isActive = fair.status === "ACTIVE" && !isFinished;

    const epaSRS = (efficiency.sport_type === "FOOTBALL") ? (efficiency.home.srs || 0) : 0;
    const newsAdjustment = calculateNewsAdjustment(extMatch);
    const edgeEnv = calculateEdgeEnvironment(extMatch, odds, progress);
    const inertia = calculateLiabilityInertia(extMatch, odds);
    const nflOverride = calculateNFLTotalOverride(extMatch, odds, progress, epaSRS);

    // ==========================================================================
    // v5.0: CAL POLY PRECISION PPM ENGINE
    // ==========================================================================
    // INVARIANT: Display_PPM × Game_Minutes ≈ Model_Total (within 1.0)
    // This reverse-calculation ensures visual consistency.
    // ==========================================================================

    const elapsedSecs = getElapsedSeconds(extMatch);
    const elapsedMins = Math.max(1, elapsedSecs / 60);
    const currentTotal = (match.homeScore ?? 0) + (match.awayScore ?? 0);
    const isNCAAB_PPM = isCollegeBasketball(match);
    const gameTotalMins = isBasketball(match.sport) ? (isNCAAB_PPM ? 40 : 48) : 60;

    // RAW OBSERVED PPM (What's happening on the floor)
    // v6.7: Defensive guard - if elapsed is near-zero, use a safe minimum
    const safeElapsedMins = Math.max(1, elapsedMins);
    const rawObsPPM = currentTotal / safeElapsedMins;

    // MODEL PPM (Derived from Model Total for visual invariant)
    // This is the "Implied Pace" - ensures Model PPM × 48 = Model Total
    // v6.7: Use resolved market total as fallback, not raw odds
    const modelTotal = isActive && fair.fair_total > 0
        ? fair.fair_total
        : (resolvedMarketTotal > 0 ? resolvedMarketTotal : 150); // Ultimate fallback
    const modelPPM = modelTotal / gameTotalMins;

    const regimes: string[] = nflOverride.active
        ? [EdgeEnvironmentTag.NFL_TOTAL_FLOOR_OVERSHOOT as any]
        : (edgeEnv.tags as any[]);

    if (isActive) {
        if (fair.regime !== "NORMAL") regimes.push(fair.regime);
        // v5.2: Apply news adjustments BEFORE edge calculation
        fair.fair_total += newsAdjustment;
        if (fair.push_risk) regimes.push("KEY_NUMBER_PUSH_RISK" as any);
    }

    // EDGE POINTS (Absolute magnitude for actionability gate)
    // Now includes news adjustments (Fidelity Correctness)
    // v6.7: Use resolved market total for edge comparison
    const edgePoints = isActive && resolvedMarketTotal > 0
        ? Math.abs(fair.fair_total - resolvedMarketTotal)
        : 0;

    // ==========================================================================
    // EDGE STATE TAXONOMY (Uncertainty-Aware Thresholds)
    // ==========================================================================
    type EdgeState = 'PLAY' | 'LEAN' | 'NEUTRAL';

    // Determine if high uncertainty is active (from variance physics)
    const isHighUncertainty = isActive && fair.variance_flags && (
        fair.variance_flags.blowout || fair.variance_flags.foul_trouble || fair.variance_flags.endgame || fair.variance_flags.power_play_decay
    );

    // Raise thresholds when uncertainty is high (edges less reliable)
    // v5.9: Lowered Hockey Threshold for "Shutdown" Detection
    let actionThreshold = 2.0;
    if (match.sport === Sport.HOCKEY) actionThreshold = 0.65; // Detects 3-goal edges
    else if (isBasketball(match.sport) && !isNCAAB_PPM) actionThreshold = SYSTEM_GATES.NBA.ACTIONABLE_EDGE;

    if (isHighUncertainty) actionThreshold = 6.0;

    const LEAN_THRESHOLD = isHighUncertainty ? 3.0 : 1.0;

    let edgeState: EdgeState = 'NEUTRAL';
    if (edgePoints >= actionThreshold) edgeState = 'PLAY';
    else if (edgePoints >= LEAN_THRESHOLD) edgeState = 'LEAN';
    else edgeState = 'NEUTRAL';

    const baseEdge = isActive ? Math.abs((fair.fair_total - odds.cur.total) / (odds.cur.total || 1)) : 0;

    // v5.0: COMPUTER GROUP PHILOSOPHY (PPM-based trigger)
    const ppmDelta = rawObsPPM - modelPPM;
    const isComputerGroupTrigger = isActive && Math.abs(ppmDelta / (modelPPM || 1)) > 0.12 && progress > 0.2;
    if (isComputerGroupTrigger) {
        regimes.push("COMPUTER_GROUP_REACTIVE" as any);
    }

    // v6.5: DATA INTEGRITY GATE (The Hallucination Guard)
    // Prevents impossible pace (e.g. 58 PPM in basketball) from triggering false buys.
    // v6.7: Now uses configurable thresholds from SYSTEM_GATES
    let isPaceHallucination = false;
    if (isBasketball(match.sport) && rawObsPPM > SYSTEM_GATES.INTEGRITY.MAX_PPM_BASKETBALL) isPaceHallucination = true;
    if (isFootball(match.sport) && rawObsPPM > SYSTEM_GATES.INTEGRITY.MAX_PPM_FOOTBALL) isPaceHallucination = true;
    if (match.sport === Sport.HOCKEY && rawObsPPM > SYSTEM_GATES.INTEGRITY.MAX_PPM_HOCKEY) isPaceHallucination = true;

    // If we detect a hallucination, we force SILENT and mark the reason.
    let finalSystemState: SystemState = (isActive && edgeState !== 'NEUTRAL') ? "ACTIVE" : "SILENT";
    let integrityReason = undefined;

    if (isPaceHallucination) {
        finalSystemState = "SILENT";
        integrityReason = "CRITICAL: Impossible Pace Detected (Clock Error)";
        regimes.push("DATA_INTEGRITY_FAILURE" as any);
    }

    const pCode = (match.sport === Sport.BASEBALL && (match.period || 0) > 9) ? "XTRA" : `P${match.period || 0}`;
    const patternHash = `${String(match.sport).toUpperCase()}:${regimes[0] || "NONE"}:${pCode}`;

    let paceLabel: any = "NORMAL";
    if (efficiency.sport_type === "BASKETBALL") {
        const pace = efficiency.home.pace;
        if (isCollegeBasketball(match)) {
            paceLabel = pace > 74 ? "FAST" : pace < 64 ? "SLOW" : "NORMAL";
        } else {
            paceLabel = pace > 102 ? "FAST" : pace < 94 ? "SLOW" : "NORMAL";
        }
    } else if (efficiency.sport_type === "SOCCER") {
        paceLabel = "TACTICAL";
    }

    const w = getWeather(extMatch);
    const isWindy = w ? parseStatNumber(w.wind_speed) > SYSTEM_GATES.WIND_THRESHOLD_MPH : false;

    // ==========================================================================
    // MARKET LEAN LOGIC (Respects Edge State - No Contradictions)
    // ==========================================================================
    let marketLean: 'OVER' | 'UNDER' | 'NEUTRAL' = 'NEUTRAL';
    let signalLabel = "NEUTRAL READ";

    if (isActive) {
        // v6.7: Use resolvedMarketTotal for delta (consistent with edge calculation)
        const delta = fair.fair_total - resolvedMarketTotal;

        // ONLY assign direction if edge_state is ACTIONABLE (PLAY or LEAN)
        if (edgeState !== 'NEUTRAL' && !isPaceHallucination) {
            if (delta > 0.45) marketLean = 'OVER';
            else if (delta < -0.45) marketLean = 'UNDER';
            else if (delta > 0) marketLean = 'OVER';
            else if (delta < 0) marketLean = 'UNDER';
        }

        // Signal label based on edge magnitude
        if (isPaceHallucination) {
            signalLabel = "DATA INTEGRITY ERROR";
            marketLean = 'NEUTRAL';
        } else if (edgeState === 'PLAY') {
            if (isComputerGroupTrigger) signalLabel = "COMPUTER GROUP ACTION";
            else if (baseEdge > 0.07) signalLabel = "SHARP BUY";
            else signalLabel = "ACTIONABLE PLAY";
        } else if (edgeState === 'LEAN') {
            signalLabel = "OBSERVATIONAL LEAN";
        } else {
            signalLabel = "LIVE READ";
            marketLean = 'NEUTRAL';
        }
    }

    // ==========================================================================
    // CONTEXT DATA (Time + Score for Trust)
    // ==========================================================================
    const remainingSecs = getRemainingSeconds(extMatch);
    const remainingMins = Math.max(0, remainingSecs / 60);


    // v6.0: Structured Observability
    const traceId = typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `trace-${Date.now()}`;
    let traceDump = undefined;

    // Extract trace from variance_flags if present (hacky transport from kernel)
    if (isActive && fair.variance_flags && (fair.variance_flags as any)._trace) {
        traceDump = (fair.variance_flags as any)._trace;
        delete (fair.variance_flags as any)._trace; // Clean up payload
    }

    const signals: AISignals = {
        trace_id: traceId,
        trace_dump: traceDump,
        system_state: finalSystemState,
        dislocation_total_pct: baseEdge,
        market_total: odds.cur.total,
        season_phase: calculateSeasonPhase(extMatch) as any,
        efficiency_matrix: efficiency as any,
        unified_report: { stats: synthesizeTrenchStats(extMatch), efficiency: efficiency as any },
        deterministic_fair_total: isActive ? fair.fair_total : undefined,
        deterministic_regime: isActive ? fair.regime : undefined,
        p10_total: isActive ? fair.p10 : 0,
        p90_total: isActive ? fair.p90 : 0,
        variance_sd: isActive ? fair.variance_sd : 0,
        status_reason: integrityReason || (fair.status === "NO_BET" ? fair.reason : undefined),
        // v5.0: Model Pace (Implied from Total) - NOT raw observed pace
        ppm: {
            observed: Number(rawObsPPM.toFixed(3)),      // Real Pace (Observed)
            projected: Number(modelPPM.toFixed(3)),      // Model Pace
            delta: Number((rawObsPPM - ((odds.cur.total || odds.open.total || 0) / gameTotalMins)).toFixed(3)), // Real vs Market Delta
            implied_total: Number((odds.cur.total || odds.open.total || 0).toFixed(1)) // Market Total (for Implied Pace calc)
        },
        // v5.0: Edge State for UI gating
        edge_state: edgeState,
        edge_points: Number(edgePoints.toFixed(1)),
        // v5.1 Cal Poly Variance Extensions
        variance_flags: (isActive && fair.variance_flags) ? fair.variance_flags : undefined,
        range_band: (isActive && fair.range_band) ? fair.range_band : undefined,
        is_high_uncertainty: isHighUncertainty,
        // v5.0: Context for trust
        context: {
            elapsed_mins: Number(elapsedMins.toFixed(1)),
            remaining_mins: Number(remainingMins.toFixed(1)),
            current_score: `${match.awayScore ?? 0}-${match.homeScore ?? 0}`,
            period: match.period || 1,
            clock: match.displayClock || '—'
        },
        market_bias: "NONE",
        market_efficiency: getMarketEfficiency(extMatch),
        dislocation_side_pct: 0,
        league_intensity: 1.0,
        pregame,
        odds,
        constraints: {
            wind: isWindy && !extMatch.venue?.is_indoor,
            kicker_out: false,
            road_favorite: odds.cur.spread > 0,
            correction_lag: pregame.correction_lag_risk,
            market_shade: pregame.public_bias_expected,
            public_flow_bias: false,
            sharp_resistance: pregame.is_sharp_resistance,
            public_shade: pregame.public_bias_expected,
            is_key_defense: inertia.reason === "KEY_NUMBER_DEFENSE",
            shade_index: 0,
            trap_reason: inertia.reason,
        },
        regimes: regimes as any[],
        regime_multiplier: getRegimeMultiplier(edgeEnv.tags, progress),
        edge_cap: SYSTEM_GATES.MAX_PERMISSIBLE_EDGE,
        evidence_pack: [`FAIR_TOTAL: ${isActive ? fair.fair_total : "N/A"}`, `REGIME: ${isActive ? fair.regime : "N/A"}`],
        sharp_origins: {
            compute: { label: "Compute", status: "NONE", description: "Institutional model reference" },
            data: { label: "Data", status: "NONE", description: "Feed quality and latency" },
            limits: { label: "Limits", status: "NONE", description: "Bookmaker risk tolerance" },
            discipline: { label: "Discipline", status: "NONE", description: "Operational consistency" },
        },
        phase: phase as any,
        risk_flags: nflOverride.active ? ["NFL_OVERRIDE"] : [],
        context_summary: `${match.displayClock} ${match.homeScore}-${match.awayScore} (${phase})`,
        opening_line: String(odds.open.spread),
        current_line: String(odds.cur.spread),
        engine_ref_line: (odds.cur.spread + epaSRS * -4).toFixed(1),
        efficiency_srs: epaSRS,
        news_adjustment: newsAdjustment,
        pattern_hash: patternHash,
        pattern_reinforcement: calculatePatternReinforcement(patternHash),
        persistence: {
            divergence_start_time: undefined,
            sequence_cycles: 0,
            is_historically_validated: false,
            consensus_lag_mins: 0,
        },
        narrative: {
            high_low_state: "STABLE",
            efficiency_trend: "SIDEWAYS",
            pace_context: paceLabel,
            market_response: "ADJUSTING",
            market_lean: marketLean,
            signal_label: signalLabel,
        },
        is_total_override: nflOverride.active,
        override_classification: nflOverride.classification as any,
        override_logs: nflOverride.logs,
    };

    signals.blueprint = getMarketBlueprint(match, signals);
    return signals;
}

function synthesizeTrenchStats(match: ExtendedMatch): TrenchAnalytics {
    if (match.sport === Sport.BASEBALL) {
        return {
            type: "BASEBALL",
            home_pitching: { whip: Number(computePitchingWHIP(match, "HOME").toFixed(2)) },
            away_pitching: { whip: Number(computePitchingWHIP(match, "AWAY").toFixed(2)) },
        };
    }
    if (isFootball(match.sport)) {
        const hRush = getStatNumber(match.homeTeamStats, "rushingyards", "rush_yds");
        const hSRS = parseStatNumber(match.homeTeam.srs ?? 0);

        return {
            type: "FOOTBALL",
            home_ol: {
                rush_grade: hRush > 150 ? 80 : hRush > 100 ? 50 : 20,
                srs_proxy: hSRS
            },
            away_dl: {
                rush_def_grade: 50
            }
        };
    }
    return { type: "GENERIC", context: "Standard" };
}

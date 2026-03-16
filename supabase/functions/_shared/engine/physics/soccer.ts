
import { OddsSnapshot, ExtendedMatch, TeamEfficiencyMatrix, FairTotalActive } from "../../types.ts";
import { SYSTEM_GATES } from "../../gates.ts";
import { getElapsedSeconds } from "../time.ts";
import { clamp, lerp, safeDiv } from "../utils.ts";

export function calculateSoccerEfficiency(match: ExtendedMatch): TeamEfficiencyMatrix {
    // Soccer efficiency is abstract since we don't have possession data usually.
    // We use a "Match Intensity" proxy based on shots/corners if available, or fall back to pace.
    return {
        sport_type: "SOCCER",
        home: { pace: 0, efg: 0 },
        away: { pace: 0, efg: 0 },
        context: "POISSON_DECAY"
    };
}

export function calculateSoccerFairTotal(
    match: ExtendedMatch,
    odds: OddsSnapshot,
    efficiency: any,
    timeRem: number,
    currentPts: number
): { fairTotal: number; regime: string; sd: number; pushRisk: boolean; flags: any } {

    const SOCCER_MATCH_MINS = 90;
    const SOCCER_MATCH_SECS = SOCCER_MATCH_MINS * 60;

    // Normalize Time (Handle 0.5m floor like basketball for singularity protection)
    const elapsedSeconds = getElapsedSeconds(match);
    const elapsedMins = Math.max(elapsedSeconds / 60, 0.5);
    const safeTimeRem = Math.max(120, SOCCER_MATCH_SECS - (elapsedMins * 60));
    const progress = 1 - (safeTimeRem / SOCCER_MATCH_SECS);

    const diff = Math.abs((match.homeScore || 0) - (match.awayScore || 0));
    const marketTotal = odds.cur.total > 0.5 ? odds.cur.total : (odds.open.total || 2.5); // Fallback to 2.5 standard

    // 1. BASELINE: Poisson Decay
    // Goals are not linear. Fatigue + Caution reduces scoring late.
    // MIT TWEAK: Blend observed rate to break the "Parrot" pattern.
    const marketBaselineRate = marketTotal / SOCCER_MATCH_SECS;

    // Observed Rate Calculation (Poisson proxy)
    // In soccer, we trust the market for a long time due to high variance.
    // Reaches 50% trust only by the 70th minute.
    const trustWeight = clamp(progress / 0.8, 0, 0.5);
    const obsRate = safeDiv(currentPts, elapsedSeconds);
    const blendedRate = lerp(marketBaselineRate, obsRate, trustWeight);

    let expectedRem = blendedRate * safeTimeRem;

    // 2. THE "DRAW LOCK" (The Edge)
    // If tied late (>75'), friction increases significantly as teams become risk-averse.
    let regime = "NORMAL";
    let friction = 1.0;

    if (progress > 0.83) { // 75th minute+
        if (diff === 0) {
            regime = "DRAW_LOCK";
            friction = 0.65; // Massive dampening on goal expectancy
        } else if (diff === 1) {
            regime = "PARK_THE_BUS";
            friction = 0.80; // Leading team wastes time
        } else if (diff >= 3) {
            regime = "DEAD_GAME";
            friction = 0.50; // Gentleman's agreement to end it
        }
    }

    let projRemaining = expectedRem * friction;

    // 3. STOPPAGE TIME CHAOS Check (Optional but real)
    // If we receive a "Stoppage Time" signal or are at 90', add small chaotic variance?
    // For now, keep it simple.

    let fairTotal = currentPts + projRemaining;

    // Sanity Cap
    fairTotal = Math.max(currentPts, fairTotal);

    return {
        fairTotal,
        regime,
        sd: -1, // Not used for simple display yet
        pushRisk: false,
        flags: {
            draw_lock: regime === "DRAW_LOCK",
            park_bus: regime === "PARK_THE_BUS"
        }
    };
}

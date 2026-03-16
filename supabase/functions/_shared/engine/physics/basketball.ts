import { Match, Sport, OddsSnapshot } from "../../types.ts";
import { ExtendedMatch, TeamEfficiencyMatrix, FairTotalResult, FairTotalActive } from "../../types.ts";
import { SYSTEM_GATES } from "../../gates.ts";
import { getElapsedSeconds, isCollegeBasketball, getRemainingSeconds } from "../time.ts";
import { clamp, lerp, safeDiv, getBasketballPossessions } from "../utils.ts";

export function calculateBasketballEfficiency(match: ExtendedMatch): TeamEfficiencyMatrix {
    const home = match.homeTeamStats;
    const away = match.awayTeamStats;

    const isNCAAB = isCollegeBasketball(match);
    const targetMins = isNCAAB ? 40 : 48;
    const elapsed = getElapsedSeconds(match) / 60;

    // v4.2: Team-Specific Anchoring (KenPom Awareness)
    let basePace = isNCAAB ? SYSTEM_GATES.NCAAB.BASELINE_PACE : SYSTEM_GATES.NBA.BASELINE_PACE;

    if (isNCAAB) {
        const hPace = (match.homeTeam as any).pace || (match.homeTeam as any).metrics?.pace;
        const aPace = (match.awayTeam as any).pace || (match.awayTeam as any).metrics?.pace;

        if (hPace && aPace) {
            basePace = (hPace + aPace) / 2;
        }
    }

    const trustWeight = clamp(elapsed / (targetMins * 0.25), 0, 1);

    const hPoss = getBasketballPossessions(home);
    const aPoss = getBasketballPossessions(away);

    const minPace = isNCAAB ? SYSTEM_GATES.NCAAB.MIN_PACE : 75;
    const maxPace = isNCAAB ? SYSTEM_GATES.NCAAB.MAX_PACE : 135;

    const rawObsPace = ((hPoss + aPoss) / 2 / Math.max(1, elapsed)) * targetMins;
    const obsPace = clamp(rawObsPace, minPace, maxPace);

    const pace = lerp(basePace, obsPace, trustWeight);
    return {
        sport_type: "BASKETBALL",
        home: { ortg: Number((safeDiv(match.homeScore, hPoss) * 100).toFixed(1)), pace: Number(pace.toFixed(1)), efg: 0.5 },
        away: { ortg: Number((safeDiv(match.awayScore, aPoss) * 100).toFixed(1)), pace: Number(pace.toFixed(1)), efg: 0.5 },
        context: `${pace.toFixed(1)} PACE`
    };
}

export function calculateBasketballFairTotal(
    match: ExtendedMatch,
    odds: OddsSnapshot,
    efficiency: TeamEfficiencyMatrix & { sport_type: "BASKETBALL" },
    timeRem: number,
    currentPts: number
): { fairTotal: number; regime: string; sd: number; pushRisk: boolean; flags: any } {

    const pace = efficiency.home.pace;
    const targetMins = isCollegeBasketball(match) ? 40 : 48;
    const elapsedMins = (targetMins * 60 - timeRem) / 60;
    const possRem = (pace / targetMins) * (timeRem / 60);

    // v5.1 PHYSICS KERNEL: Base Projection
    const marketPPP = safeDiv(odds.cur.total, pace);
    const obsPPP = safeDiv(currentPts, (pace / targetMins) * elapsedMins);
    const trustWeight = clamp(elapsedMins / (targetMins * 0.6), 0, 1); // 30m ramp for NBA
    const blendedPPP = lerp(marketPPP, obsPPP, trustWeight);

    let projRemaining = possRem * blendedPPP;
    let regime: FairTotalActive["regime"] = "NORMAL";

    // v5.1 VARIANCE MODIFIERS (The "Physics")
    const diff = Math.abs(match.homeScore - match.awayScore);
    const flags = {
        blowout: diff > SYSTEM_GATES.NBA.BLOWOUT_DIFF && elapsedMins > (targetMins * 0.6),
        foul_trouble: false, // Ideally needs 'fouls' from stats, defaulting false for now if data missing
        endgame: elapsedMins > SYSTEM_GATES.NBA.ENDGAME_START_MIN && diff <= 6
    };

    // Apply Modifiers
    if (flags.blowout) {
        projRemaining *= SYSTEM_GATES.NBA.BLOWOUT_SCALAR; // Brake Check
        regime = "BLOWOUT";
    }
    if (flags.endgame) {
        projRemaining += SYSTEM_GATES.NBA.ENDGAME_ADDER; // Chaos Lift
        regime = "CHAOS";
    }

    // Invariant: Banked Points
    let rawFair = currentPts + projRemaining;

    // v5.1 DYNAMIC CAP (Safety Ceiling)
    // Prevent runaway projections in early game chaos
    let cap = 300; // Unreachable ceiling base
    const anchorTotal = odds.open.total > 0 ? odds.open.total : (odds.cur.total > 0 ? odds.cur.total : 0);

    if (anchorTotal > 0) {
        if (elapsedMins < 6) cap = anchorTotal + 28;
        else if (elapsedMins < 12) cap = anchorTotal + 35;
    }
    let fairTotal = Math.min(rawFair, cap);

    // Invariant: Final >= Banked Points (Impossible to score negative remaining)
    fairTotal = Math.max(currentPts, fairTotal);

    // We will calculate SD in the caller or return it here. 
    // In the original code, sd was calculated based on timeRem before the switch.
    // However, the original code had `sd = 1.0 * timeFactor`.
    // We'll return just the components.

    return {
        fairTotal,
        regime,
        sd: -1, // Placeholder, calculated by caller usually, but maybe we should override if blowout?
        // Original code: if flags.blowout -> regime="BLOWOUT" and it sets pace_multiplier=0.9
        // But SD logic in main engine: `sd = Math.max(0.15, 1.0 * timeFactor);`
        // THEN `if (calculateBlowoutState(...)) { regime = "BLOWOUT"; sd = 2.5; }`
        // So we need to handle that.
        pushRisk: false, // NBA doesn't really have push risk same as NFL key numbers logic in this code
        flags
    };
}

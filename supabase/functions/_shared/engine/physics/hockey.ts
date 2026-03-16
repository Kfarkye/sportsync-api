
import { PregameConstraints, OddsSnapshot } from "../../types.ts";
import { ExtendedMatch, TeamEfficiencyMatrix } from "../../types.ts";
import { SYSTEM_GATES } from "../../gates.ts";
import { getElapsedSeconds } from "../time.ts";
import { parseStatNumber, clamp, lerp, getStatNumber } from "../utils.ts";

export function calculateHockeyFairTotal(
    match: ExtendedMatch,
    efficiency: TeamEfficiencyMatrix & { sport_type: "HOCKEY" }, // Narrow type
    pregame: PregameConstraints,
    timeRem: number,
    currentPts: number,
    regime: "NORMAL" | "BLOWOUT"
): { fairTotal: number; regime: string; sd: number; flags?: any; trace?: any } {

    if (efficiency.sport_type !== "HOCKEY") {
        throw new Error("Invalid sport type passed to Hockey Kernel");
    }

    const { blended_rate, is_tied_decay, is_en_risk } = efficiency.global;
    let projectedFuture = blended_rate * (timeRem / 60);
    let finalRegime: string = regime;
    const varianceFlags: any = {};
    const elapsedMins = getElapsedSeconds(match) / 60;

    // v5.10 GLOBAL FATIGUE (User Request 524)
    // B2B teams allow more high-danger chances due to "Tired Legs" throughout the game.
    if (pregame.is_back_to_back) {
        projectedFuture *= 1.15;
    }

    const trace = {
        blended_rate,
        projectedFuture_initial: blended_rate * (timeRem / 60),
        regime_in: regime,
        regime_out: finalRegime,
        is_b2b: pregame.is_back_to_back,
        surrenderScalar: undefined as number | undefined,
        p3Inflation: undefined as number | undefined,
        proactive_decay_weight: undefined as number | undefined,
        en_injection: undefined as number | undefined,
        is_tied_decay,
        is_en_risk
    };

    // v5.3: HOCKEY TRANSITION PHYSICS (User Request 339)
    // Fixes "Binary Blindness": Model now anticipates the path to 3-2 / Goalie Pull
    // const elapsedMins = getElapsedSeconds(match) / 60; // Already declared above
    if (elapsedMins >= 40) {
        // 1. 3rd Period Scarcity Correction
        // v5.7 Shutdown Check: If it's a blowout, "Game Theory" says the push dies.
        // v5.8 Fatigue Logic: B2B teams surrender FASTER (0.70x instead of 0.80x)
        if (regime === "BLOWOUT") {
            let surrenderScalar = pregame.is_back_to_back ? 0.65 : 0.70; // v5.9: Tightened

            // v5.9: POWER PLAY VOLATILITY SPIKE (User Request 486)
            // If a Power Play happens in a 4-1 "dead" game, it injects life back into the Over.
            // We reduce the surrender scalar (making it closer to 1.0) to reflect this risk.
            const situation = match.situation as any;
            const situationText = String(situation?.possessionText || "").toLowerCase();
            const REGEX_PP = /\b(pp|power\s*play|man\s*advantage|5\s*v\s*4|5\s*on\s*4|4\s*on\s*3)\b/i;
            const isPowerPlay = !!match.situation?.isPowerPlay || REGEX_PP.test(situationText);

            if (isPowerPlay) {
                surrenderScalar += 0.25; // Bumps 0.70 -> 0.95 (Decay trigger)
                varianceFlags.power_play_decay = true;
            }

            projectedFuture *= surrenderScalar;
            trace.surrenderScalar = surrenderScalar;
        } else {
            const inflation = pregame.is_back_to_back ? 1.35 : SYSTEM_GATES.NHL.P3_INFLATION;
            projectedFuture *= inflation;
            trace.p3Inflation = inflation;

            // 2. Proactive Striking Distance Logic with Time Decay (Theta)
            const diff = Math.abs(match.homeScore - match.awayScore);
            if (diff <= 2 && !is_en_risk) {
                const injection = diff === 1 ? SYSTEM_GATES.NHL.EN_INJECTION_1G : SYSTEM_GATES.NHL.EN_INJECTION_2G;

                // v5.6: TEAM QUALITY SCALAR (User Request 362)
                const hScore = match.homeScore || 0;
                const aScore = match.awayScore || 0;
                const trailingSide = hScore < aScore ? "HOME" : "AWAY";
                const trailingTeam = trailingSide === "HOME" ? match.homeTeam : match.awayTeam;
                const tSRS = parseStatNumber((trailingTeam as any).srs ?? 0);

                const qualityScalar = tSRS > 0.5 ? 1.2 : (tSRS < -0.5 ? 0.8 : 1.0);
                const p3Ratio = clamp(timeRem / 1200, 0, 1);
                const decayWeight = lerp(0.1, 0.6, p3Ratio) * qualityScalar;

                projectedFuture += (injection * decayWeight);
                trace.en_injection = injection;
                trace.proactive_decay_weight = decayWeight;
            }
        }
    }

    if (is_tied_decay) projectedFuture *= SYSTEM_GATES.NHL.TIED_DECAY_MULT;
    if (is_en_risk) {
        const diff = Math.abs(match.homeScore - match.awayScore);
        projectedFuture += (diff === 1 ? SYSTEM_GATES.NHL.EN_INJECTION_1G : SYSTEM_GATES.NHL.EN_INJECTION_2G);
        finalRegime = "CHAOS";
    }

    return {
        fairTotal: currentPts + projectedFuture,
        regime: finalRegime,
        sd: 1.2,
        flags: varianceFlags,
        trace
    };
}

export function calculateHockeyEfficiency(match: ExtendedMatch, odds: OddsSnapshot): TeamEfficiencyMatrix {
    const home = match.homeTeamStats;
    const away = match.awayTeamStats;

    const elapsed = Math.max(0.1, getElapsedSeconds(match) / 60);
    // v5.6 Decoupling: Use Opening Total as the baseline anchor to prevent 
    // the "Fair Value" from being dragged down by the book's live Theta decay.
    const marketTotal = (odds.open.total > 0) ? odds.open.total : (odds.cur.total > 0 ? odds.cur.total : 6.5);
    const marketBaselineRate = marketTotal / 60.0;

    const hSOG = getStatNumber(home, "shotsongoal", "sog");
    const aSOG = getStatNumber(away, "shotsongoal", "sog");
    const totalSOG = hSOG + aSOG;

    const obsXG = totalSOG * SYSTEM_GATES.NHL.SOG_CONVERSION_AVG;
    const obsRate = obsXG / elapsed;

    const weight = clamp(totalSOG / SYSTEM_GATES.NHL.MIN_EVENTS_TRUST, 0.15, 1.0);
    const blendedRate = (obsRate * weight) + (marketBaselineRate * (1.0 - weight));

    const remMins = Math.max(0, 60 - elapsed);
    const diff = Math.abs(match.homeScore - match.awayScore);
    const isTiedDecay = remMins < 8.0 && diff === 0;

    // v5.4 Modern NHL Pull Logic: 4:00 for 2-goal deficit, 3:00 for 1-goal.
    const isEnRisk = (diff === 1 && remMins < 3.0) || (diff === 2 && remMins < 4.5);

    return {
        sport_type: "HOCKEY",
        home: { xg_rate: hSOG * SYSTEM_GATES.NHL.SOG_CONVERSION_AVG, sog: hSOG, projected_contribution: 0 },
        away: { xg_rate: aSOG * SYSTEM_GATES.NHL.SOG_CONVERSION_AVG, sog: aSOG, projected_contribution: 0 },
        global: { market_baseline: marketBaselineRate, blended_rate: blendedRate, is_tied_decay: isTiedDecay, is_en_risk: isEnRisk },
        context: `RATE: ${blendedRate.toFixed(3)} xG/min`
    };
}

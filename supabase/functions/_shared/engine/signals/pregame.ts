import { ExtendedMatch, PregameConstraints } from "../../types.ts";
import { parseStatNumber } from "../utils.ts";
import { getCanonicalOdds } from "../market.ts";

export function calculatePregameConstraints(match: ExtendedMatch): PregameConstraints {
    const odds = getCanonicalOdds(match);
    const spreadMove = odds.cur.spread - odds.open.spread;
    const totalMove = odds.cur.total - odds.open.total;
    const pubBetPct = match.public_betting_pct || 50;

    const isSharpResistance = Math.abs(spreadMove) >= 1.5 && pubBetPct < 50;
    const isPublicBias = pubBetPct > 75;

    // Detect B2B from notes
    const notes = String(match.notes || "").toLowerCase();
    const isB2B = notes.includes("back-to-back") || notes.includes("b2b") || notes.includes("second night");

    // Elite Trailing Check (v5.6 Context)
    const hScore = match.homeScore || 0;
    const aScore = match.awayScore || 0;
    const trailingSide = hScore < aScore ? "HOME" : "AWAY";
    const trailingTeam = trailingSide === "HOME" ? match.homeTeam : match.awayTeam;
    const tSRS = parseStatNumber((trailingTeam as any).srs ?? 0);
    const isEliteTrailing = (match.period ?? 0) >= 3 && tSRS > 0.5;

    return {
        correction_lag_risk: Math.abs(totalMove) > 3.0,
        public_bias_expected: isPublicBias,
        volatility_profile: Math.abs(spreadMove) > 2.0 ? "HIGH" : "LOW",
        regime_likelihood: {},
        alignment_score: isSharpResistance ? 85 : 50,
        is_sharp_resistance: isSharpResistance,
        is_back_to_back: isB2B,
        is_elite_trailing: isEliteTrailing,
    };
}

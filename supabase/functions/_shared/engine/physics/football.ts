import { Sport, OddsSnapshot } from "../../types.ts";
import { ExtendedMatch, TeamEfficiencyMatrix, FairTotalActive } from "../../types.ts";
import { SYSTEM_GATES } from "../../gates.ts";
import { getStatNumber, parseStatNumber, clamp, lerp } from "../utils.ts";

export function calculateFootballEfficiency(match: ExtendedMatch): TeamEfficiencyMatrix {
    const home = match.homeTeamStats;
    const away = match.awayTeamStats;

    const hDrives = Math.max(1, getStatNumber(home, "totaldrives", "drives") || 8);
    const aDrives = Math.max(1, getStatNumber(away, "totaldrives", "drives") || 8);
    const hSRS = parseStatNumber(match.homeTeam.srs ?? 0);
    const aSRS = parseStatNumber(match.awayTeam.srs ?? 0);

    return {
        sport_type: "FOOTBALL",
        home: { ppd: Number((match.homeScore / hDrives).toFixed(2)), pace: 0, srs: hSRS },
        away: { ppd: Number((match.awayScore / aDrives).toFixed(2)), pace: 0, srs: aSRS },
        home_drives: hDrives,
        away_drives: aDrives,
        context: `PPD: ${(match.homeScore / hDrives).toFixed(1)} | ${(match.awayScore / aDrives).toFixed(1)}`
    };
}

export function calculateFootballFairTotal(
    match: ExtendedMatch,
    odds: OddsSnapshot,
    efficiency: TeamEfficiencyMatrix & { sport_type: "FOOTBALL" },
    timeRem: number,
    currentPts: number,
    timeFactor: number
): { fairTotal: number; regime: string; sd: number; pushRisk: boolean } {
    const isNCAAF = match.sport === Sport.COLLEGE_FOOTBALL;
    const config = isNCAAF ? SYSTEM_GATES.NCAAF : SYSTEM_GATES.NFL;

    const diff = Math.abs(match.homeScore - match.awayScore);
    let regime: FairTotalActive["regime"] = "NORMAL";

    if (diff <= 8 && timeRem < 300) {
        regime = "HURRY_UP";
    } else if (diff > 16 && timeRem < 600) {
        regime = "KILL_CLOCK";
    }

    const totalObsDrives = efficiency.home_drives + efficiency.away_drives;
    const minDrives = isNCAAF ? SYSTEM_GATES.NCAAF.MIN_DRIVES_OBSERVED : SYSTEM_GATES.NFL.MIN_DRIVES_OBSERVED;

    const sampleConfidence = totalObsDrives < minDrives ? 0 : 1;
    const timeConfidence = clamp(1.0 - (timeRem / 3600), 0.2, 0.8);

    const finalConfidence = timeConfidence * sampleConfidence;
    const marketPPD = odds.cur.total / config.AVG_DRIVES_PER_GAME;
    const obsAvgPPD = (efficiency.home.ppd + efficiency.away.ppd) / 2;
    const blendedPPD = lerp(marketPPD, obsAvgPPD, finalConfidence);

    const expectedTotalDrives = config.AVG_DRIVES_PER_GAME;
    const remainingDrives = Math.max(2, expectedTotalDrives - totalObsDrives);

    const fairTotal = currentPts + (remainingDrives * blendedPPD);
    const sd = 3.5 * timeFactor;

    let pushRisk = false;
    if (!isNCAAF) {
        const roundedTotal = Math.round(fairTotal);
        if ((SYSTEM_GATES.NFL.KEY_TOTALS as readonly number[]).includes(roundedTotal) && Math.abs(fairTotal - roundedTotal) < 0.3) {
            pushRisk = true;
        }
    }

    return {
        fairTotal,
        regime,
        sd,
        pushRisk
    };
}

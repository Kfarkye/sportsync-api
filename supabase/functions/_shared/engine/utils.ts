
import { Match, Sport } from "../types.ts";
import type { ExtendedMatch } from "../types.ts";
import { SYSTEM_GATES } from "../gates.ts";
import { getRemainingSeconds, getBaseballState } from "./time.ts";

const REGEX = {
    STAT_DASH: /^(\d+(\.\d+)?)\s*[-/]\s*(\d+(\.\d+)?)$/,
    STAT_OF: /^(\d+(\.\d+)?)\s*(of)\s*(\d+(\.\d+)?)$/,
    CLEAN_STAT: /[,%]|mph|km\/h|kts/gi,
};

// Flexible container for raw stats to avoid 'any' while handling dirty feeds
interface UnifiedStatContainer {
    [key: string]: any;
    statistics?: Array<{ name?: string; label?: string; value?: string | number }>;
}

export function clamp(n: number, lo: number, hi: number): number {
    if (Number.isNaN(n)) return lo;
    return Math.max(lo, Math.min(hi, n));
}

export function lerp(a: number, b: number, t: number): number {
    return a + (b - a) * clamp(t, 0, 1);
}

export function safeDiv(num: number, den: number, fallback = 0): number {
    return den === 0 ? fallback : num / den;
}

export function isBasketball(s: Sport): boolean {
    return s === Sport.NBA || s === Sport.BASKETBALL || s === Sport.COLLEGE_BASKETBALL;
}

export function isFootball(s: Sport): boolean {
    return s === Sport.NFL || s === Sport.COLLEGE_FOOTBALL;
}

export function isNCAAF(s: Sport): boolean {
    return s === Sport.COLLEGE_FOOTBALL;
}

export function parseStatNumber(raw: unknown): number {
    if (raw == null) return 0;
    const s = String(raw).trim().replace(REGEX.CLEAN_STAT, "");
    if (!s) return 0;

    const dash = s.match(REGEX.STAT_DASH);
    if (dash) return parseFloat(dash[1]) || 0;

    const of = s.match(REGEX.STAT_OF);
    if (of) return parseFloat(of[1]) || 0;

    const n = parseFloat(s);
    return Number.isFinite(n) ? n : 0;
}

export function parseMadeAttempt(raw: unknown): { made: number; att: number } {
    if (raw == null) return { made: 0, att: 0 };
    const s = String(raw).trim().replace(REGEX.CLEAN_STAT, "");

    const dash = s.match(REGEX.STAT_DASH);
    if (dash) return { made: parseFloat(dash[1]) || 0, att: parseFloat(dash[3]) || 0 };

    const of = s.match(REGEX.STAT_OF);
    if (of) return { made: parseFloat(of[1]) || 0, att: parseFloat(of[4]) || 0 };

    const n = parseFloat(s);
    return { made: Number.isFinite(n) ? n : 0, att: 0 };
}

// Hybrid Stat Accessor (Checks both 'statistics' array and Root keys)
export function findStatValue(statsObj: UnifiedStatContainer | undefined, ...keys: string[]) {
    if (!statsObj) return undefined;

    const want = keys.map(k => k.toLowerCase());

    // 1. Check Root Properties (e.g. { rushing_yards: 100 })
    for (const key of Object.keys(statsObj)) {
        const cleanKey = key.toLowerCase().replace(/[^a-z]/g, "");
        if (want.some(w => cleanKey.includes(w))) {
            if (typeof statsObj[key] === 'number' || typeof statsObj[key] === 'string') {
                return statsObj[key];
            }
        }
    }

    // 2. Check Nested Statistics Array (e.g. { statistics: [{ name: "rushing", value: 100 }] })
    const list = statsObj.statistics;
    if (Array.isArray(list)) {
        const item = list.find((s) => {
            const n = String(s?.name ?? "").toLowerCase();
            const l = String(s?.label ?? "").toLowerCase();
            return want.some(k => n.includes(k) || l.includes(k));
        });
        return item?.value;
    }

    return undefined;
}

export function getStatNumber(statsObj: UnifiedStatContainer | undefined, ...keys: string[]): number {
    return parseStatNumber(findStatValue(statsObj, ...keys));
}

export function calculateBlowoutState(match: ExtendedMatch, timeRem: number): boolean {
    const diff = Math.abs((match.homeScore || 0) - (match.awayScore || 0));

    if (isBasketball(match.sport)) {
        return diff >= SYSTEM_GATES.NBA.BLOWOUT_DIFF && timeRem < 300;
    }
    if (match.sport === Sport.HOCKEY) {
        return diff >= 3 && timeRem < 600;
    }
    if (match.sport === Sport.NFL || match.sport === Sport.COLLEGE_FOOTBALL) {
        return diff >= 24 && timeRem < 600;
    }
    return false;
}

export function computePitchingWHIP(match: ExtendedMatch, side: "HOME" | "AWAY"): number {
    const teamStats = side === "HOME" ? match.homeTeamStats : match.awayTeamStats;
    const oppStats = side === "HOME" ? match.awayTeamStats : match.homeTeamStats;

    const explicitWhip = getStatNumber(teamStats, "whip");
    if (explicitWhip > 0) return explicitWhip;

    const { inning, half, outs } = getBaseballState(match);
    const baseOuts = 3 * (inning - 1);
    const outsPitched = (side === "HOME")
        ? (half === "TOP" ? baseOuts + outs : baseOuts + 3)
        : (half === "TOP" ? baseOuts : baseOuts + outs);

    const ip = Math.max(0.33, outsPitched / 3);
    const hits = getStatNumber(teamStats, "hitsallowed", "hallowed") || getStatNumber(oppStats, "hits", "h");
    const walks = getStatNumber(teamStats, "walksallowed", "bb_allowed") || getStatNumber(oppStats, "walks", "bb");

    return safeDiv(hits + walks, ip);
}

export function getBasketballPossessions(stats: UnifiedStatContainer | undefined): number {
    const fga = getStatNumber(stats, "fga", "fieldgoalsattempted") || parseMadeAttempt(findStatValue(stats, "fg")).att;
    const tov = getStatNumber(stats, "turnovers", "to");
    const orb = getStatNumber(stats, "oreb", "offensiverebounds");
    const fta = getStatNumber(stats, "fta", "freethrowsattempted") || parseMadeAttempt(findStatValue(stats, "ft")).att;
    return Math.max(1, fga - orb + tov + 0.44 * fta);
}

export function calculateSeasonPhase(_match: Match): string { return "MID"; }

export function getMarketEfficiency(match: Match): any {
    return (match as any)?.market_efficiency || "STABLE";
}

export function calculatePatternReinforcement(_hash: string): any { return undefined; }

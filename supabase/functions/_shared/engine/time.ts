
import { Match, Sport } from "../types.ts";
import type { ExtendedMatch } from "../types.ts";
import { SYSTEM_GATES } from "../gates.ts";

// Inlined from utils.ts to break circular dependency (utils → time → utils)
function isBasketball(s: Sport): boolean {
    return s === Sport.NBA || s === Sport.BASKETBALL || s === Sport.COLLEGE_BASKETBALL;
}
function isFootball(s: Sport): boolean {
    return s === Sport.NFL || s === Sport.COLLEGE_FOOTBALL;
}

// Pre-compiled Regex
const REGEX = {
    FINAL: /\b(final|ft|f|full\s*time|ended|postponed|cancelled|canceled|suspended|abandoned|delayed)\b/i,
    SOCCER_HT: /\b(ht|half\s*time|halftime|break)\b/i,
    CLOCK_MMSS: /(\d+)\s*:\s*(\d{1,2})(?!.*\d+\s*:\s*\d{1,2})/,
};

function clamp(n: number, lo: number, hi: number): number {
    if (Number.isNaN(n)) return lo;
    return Math.max(lo, Math.min(hi, n));
}

function parseClockToSeconds(clockRaw: string): number {
    const raw = String(clockRaw || "0:00").trim().toLowerCase();

    if (REGEX.FINAL.test(raw)) return 0;

    if (raw.includes("+") && !raw.includes(":")) {
        const [baseRaw, addedRaw] = raw.split("+");
        return Math.max(0, ((parseFloat(baseRaw) || 0) + (parseFloat(addedRaw) || 0)) * 60);
    }

    const m = raw.match(REGEX.CLOCK_MMSS);
    if (!m) return 0;
    const mins = parseFloat(m[1]) || 0;
    const secs = parseFloat(m[2]) || 0;

    // v3.5: HEURISTIC ELAPSED DETECTION
    // If the clock is in the first few seconds (e.g. 0:05) and it's not a known "zero" state,
    // we assume it's ELAPSED if the sport usually shows remaining (like NFL).
    return Math.max(0, mins * 60 + secs);
}

function parseSoccerElapsedSeconds(period: number, clockRaw: string): number {
    const raw = (clockRaw || "").trim().toLowerCase();
    if (REGEX.SOCCER_HT.test(raw)) return SYSTEM_GATES.SOCCER_HALF_SECONDS;
    if (REGEX.FINAL.test(raw) || /\b(full)\b/.test(raw)) return SYSTEM_GATES.SOCCER_REG_SECONDS;

    const t = parseClockToSeconds(raw);
    if (period >= 2 && t < SYSTEM_GATES.SOCCER_HALF_SECONDS) {
        return clamp(SYSTEM_GATES.SOCCER_HALF_SECONDS + t, 0, SYSTEM_GATES.SOCCER_REG_SECONDS + SYSTEM_GATES.SOCCER_MAX_STOPPAGE);
    }
    return clamp(t, 0, SYSTEM_GATES.SOCCER_REG_SECONDS + SYSTEM_GATES.SOCCER_MAX_STOPPAGE);
}

export function isCollegeBasketball(match: Match | ExtendedMatch): boolean {
    const s = match.sport;
    const lid = match.leagueId?.toLowerCase() || '';
    return s === Sport.COLLEGE_BASKETBALL ||
        lid.includes('college-basketball') ||
        lid.includes('ncaab');
}

export function getBaseballInning(match: ExtendedMatch): number {
    return Math.max(1, Number(match.period) || 1);
}

export function getBaseballState(match: ExtendedMatch) {
    const sit = match.situation || {};
    const txt = String(sit.possessionText || sit.inningHalf || "").toLowerCase();
    return {
        half: (txt.includes("bot") || txt.includes("btm")) ? "BOTTOM" : "TOP",
        outs: clamp(Number(sit.outs) || 0, 0, 3),
        inning: Math.max(1, Number(match.period) || 1)
    };
}

export function getElapsedSeconds(match: ExtendedMatch): number {
    let period = match.period || 1;
    const clock = match.displayClock || "0:00";
    const status = String(match.status || "").toUpperCase();
    const clockUp = clock.toUpperCase();

    // v4.1 BREAK DETECTION (The "Halftime Guard")
    // If the game is at halftime or between periods, the clock often shows 0:00 or X:00
    // but the scores indicate the half is over.
    const isHalftime = status.includes("HALF") || status.includes("HT") ||
        clockUp.includes("HALF") || clockUp.includes("HT");
    const isBetweenPeriods = status.includes("END") || status.includes("BREAK") ||
        clockUp.includes("END") || clockUp.includes("BREAK");

    // v4.0 ELITE PRECISION: Universal Period Detection
    if (period === 1) {
        if (status.includes("Q2") || status.includes("2ND")) period = 2;
        if (status.includes("Q3") || status.includes("3RD")) period = 3;
        if (status.includes("Q4") || status.includes("4TH")) period = 4;
        if (status.includes("P2") || status.includes("2ND PER")) period = 2;
        if (status.includes("P3") || status.includes("3RD PER")) period = 3;
        if (status.includes("OT") || status.includes("OVERTIME") || status.includes("SO")) period = 5;

        if (clockUp.includes("Q4") || clockUp.includes("4TH")) period = 4;
        if (clockUp.includes("OT")) period = 5;
    }

    const sport = match.sport;
    const isBB = isBasketball(sport);
    const isFB = isFootball(sport);
    const isNCAAB = isCollegeBasketball(match);

    // Basketball (Heuristic: NCAAB H1 end = 20:00, NBA Q end = 12:00)
    if (isBB) {
        const t = parseClockToSeconds(clock);
        const secPerPeriod = isNCAAB ? 1200 : 720;
        const regs = isNCAAB ? 2 : 4;

        if (isHalftime && isNCAAB) return 1200;
        if (isHalftime && !isNCAAB) return 1440;

        if (period > regs) return (regs * secPerPeriod) + ((period - regs - 1) * 300) + clamp(300 - t, 0, 300);

        // If clock is 0:00 and we are between periods, return full period
        if ((t === 0 || isBetweenPeriods) && period <= regs) return period * secPerPeriod;

        return ((period - 1) * secPerPeriod) + clamp(secPerPeriod - t, 0, secPerPeriod);
    }

    // Football
    if (isFB) {
        const t = parseClockToSeconds(clock);
        const secPerQ = 900;

        if (isHalftime) return 1800;

        let periodElapsed = 0;
        if (t > 900) {
            return t;
        } else if (t > 720 && period === 1) {
            periodElapsed = Math.max(0, 900 - t);
        } else if (t < 180 && period === 1) {
            periodElapsed = t;
        } else {
            periodElapsed = Math.max(0, 900 - t);
        }

        if (period > 4) return 3600 + clamp(600 - t, 0, 600);

        if (isBetweenPeriods) return period * secPerQ;

        return ((period - 1) * 900) + periodElapsed;
    }

    // Hockey
    if (sport === Sport.HOCKEY) {
        const t = parseClockToSeconds(clock);
        if (isHalftime) return 1200; // End of P1 for hockey? Usually HT is after P2
        if (isBetweenPeriods) return period * 1200;

        if (period > 3) return 3600 + clamp(300 - t, 0, 300);
        return ((period - 1) * 1200) + clamp(1200 - t, 0, 1200);
    }

    // Soccer
    if (sport === Sport.SOCCER) return parseSoccerElapsedSeconds(period, clock);

    // Baseball
    if (sport === Sport.BASEBALL) return (getBaseballInning(match) - 1) * SYSTEM_GATES.MLB.SEC_PER_INNING;

    // Default Fallback
    const t = parseClockToSeconds(clock);
    if (isHalftime) return (period === 1) ? 1800 : 3600; // Vague guess
    return ((period - 1) * 900) + t;
}

export function getRemainingSeconds(match: ExtendedMatch): number {
    const elapsed = getElapsedSeconds(match);
    switch (match.sport) {
        case Sport.NFL: return clamp(3600 - elapsed, 0, 3600);
        case Sport.COLLEGE_FOOTBALL: return clamp(3600 - elapsed, 0, 3600);
        case Sport.NBA:
            return clamp(2880 - elapsed, 0, 2880);
        case Sport.BASKETBALL:
        case Sport.COLLEGE_BASKETBALL:
            return clamp((isCollegeBasketball(match) ? 2400 : 2880) - elapsed, 0, (isCollegeBasketball(match) ? 2400 : 2880));
        case Sport.HOCKEY: return clamp(3600 - elapsed, 0, 3600);
        case Sport.SOCCER:
            return elapsed >= SYSTEM_GATES.SOCCER_REG_SECONDS
                ? clamp(300 - (elapsed - SYSTEM_GATES.SOCCER_REG_SECONDS), 0, 300)
                : SYSTEM_GATES.SOCCER_REG_SECONDS - elapsed;
        case Sport.BASEBALL: {
            const { inning, half, outs } = getBaseballState(match);
            if (inning > 9) return 0;
            const outsElapsed = ((inning - 1) * 6) + (half === "BOTTOM" ? 3 : 0) + outs;
            return Math.max(0, 54 - outsElapsed) * SYSTEM_GATES.MLB.SEC_PER_OUT;
        }
        default: return clamp(3600 - elapsed, 0, 3600);
    }
}

export function calculateGameProgress(match: ExtendedMatch): number {
    if (match.sport === Sport.BASEBALL) {
        const inning = getBaseballInning(match);
        return inning > 9 ? 1.0 : clamp(((inning - 1) * 6) / 54, 0, 1);
    }
    const total = isBasketball(match.sport) ? (match.sport === Sport.COLLEGE_BASKETBALL ? 2400 : 2880)
        : match.sport === Sport.HOCKEY ? 3600
            : match.sport === Sport.SOCCER ? 5400
                : 3600;
    return clamp(getElapsedSeconds(match) / total, 0, 1);
}

export function isFinalLikeClock(clockRaw: unknown, statusRaw?: string): boolean {
    const clock = String(clockRaw ?? "").trim().toLowerCase();
    const status = String(statusRaw ?? "").trim().toLowerCase();
    return REGEX.FINAL.test(clock) || REGEX.FINAL.test(status);
}

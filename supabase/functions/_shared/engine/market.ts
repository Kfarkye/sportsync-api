import { Match, Sport, ExtendedMatch, OddsSnapshot, AISignals, MarketBlueprint, MarketScope, EdgeEnvironmentTag } from "../types.ts";
import { SYSTEM_GATES } from "../gates.ts";
import { isFootball, clamp } from "./utils.ts";
import { getOddsValue } from "../oddsUtils.ts";

export function getCanonicalOdds(match: Match): OddsSnapshot {
    const o = match.opening_odds || {};
    // v3.6: Prioritize Current Odds (Live) -> Consensus -> Opening
    const c = (match.current_odds && Object.keys(match.current_odds).length > 0)
        ? match.current_odds
        : (match.odds || {});

    const parse = (val: any, type: "spread" | "price" | "total") => {
        const v = getOddsValue(val, type);
        return Number.isFinite(v) ? v : undefined;
    };

    return {
        open: {
            spread: parse(o.homeSpread ?? o.spread, "spread") ?? 0,
            total: parse(o.overUnder, "total") ?? 0,
            mlHome: parse(o.moneylineHome ?? o.homeWin, "price") ?? 0,
            mlAway: parse(o.moneylineAway ?? o.awayWin, "price") ?? 0,
            mlDraw: parse(o.moneylineDraw ?? o.draw, "price") ?? 0,
            homeSpreadPrice: 0, awaySpreadPrice: 0, spreadPrice: 0, overPrice: 0, underPrice: 0, totalPrice: 0
        },
        cur: {
            spread: parse(c.spread ?? c.homeSpread, "spread") ?? 0,
            total: parse(c.total ?? c.overUnder, "total") ?? 0,
            mlHome: parse(c.homeWin ?? c.moneylineHome ?? c.home_ml, "price") ?? -110,
            mlAway: parse(c.awayWin ?? c.moneylineAway ?? c.away_ml, "price") ?? -110,
            mlDraw: parse(c.draw ?? (c as any).moneylineDraw ?? c.draw_ml, "price") ?? 0,
            totalPrice: parse(c.overOdds ?? c.over_odds, "price") ?? -110
        },
        hasSpread: c.spread !== undefined || c.homeSpread !== undefined,
        hasTotal: c.total !== undefined || c.overUnder !== undefined,
        hasML: c.homeWin !== undefined || c.moneylineHome !== undefined || c.home_ml !== undefined
    };
}

export function calculateLiabilityInertia(match: ExtendedMatch, odds: OddsSnapshot) {
    const spreadMove = odds.cur.spread - odds.open.spread;
    const isLineFrozen = Math.abs(spreadMove) < 0.5;
    const pubBetPct = match.public_betting_pct || 50;

    if (pubBetPct > SYSTEM_GATES.MARKET.REVERSE_MOVE_PCT && isLineFrozen) {
        return { active: true, reason: "REVERSE_LINE_MOVEMENT" };
    }

    const isContrarianMove = (spreadMove < -1.5 && pubBetPct < SYSTEM_GATES.MARKET.SHARP_STEAM_PCT) ||
        (spreadMove > 1.5 && pubBetPct > (100 - SYSTEM_GATES.MARKET.SHARP_STEAM_PCT));
    if (isContrarianMove) {
        return { active: true, reason: "SHARP_STEAM" };
    }

    const keyNumbers = match.sport === Sport.COLLEGE_FOOTBALL ? SYSTEM_GATES.NCAAF.KEY_NUMBERS : SYSTEM_GATES.NFL.KEY_NUMBERS;
    if (isFootball(match.sport) && (keyNumbers as readonly number[]).includes(Math.abs(odds.cur.spread))) {
        return { active: true, reason: "KEY_NUMBER_DEFENSE" };
    }

    return { active: false, reason: "NONE" };
}

export function calculateEdgeEnvironment(match: Match, odds: OddsSnapshot, progress: number) {
    const tags: EdgeEnvironmentTag[] = [];
    if (progress < 0.3 && Math.abs(odds.cur.total - odds.open.total) > 4) {
        tags.push(EdgeEnvironmentTag.EARLY_MARKET_CORRECTION_LAG);
    }
    return { tags, confidence: tags.length > 0 ? 0.9 : 0 };
}

export function calculateNFLTotalOverride(match: Match, _odds: OddsSnapshot, progress: number, srs: number) {
    if (!isFootball(match.sport)) return { active: false, classification: "NONE", logs: [] };

    const logs: string[] = [];
    if (progress > 0.8 && srs < -4) {
        logs.push("Floor Overshoot Risk: Low efficiency team in garbage time.");
        return { active: true, classification: "UNDER_FLOOR", logs };
    }
    return { active: false, classification: "NONE", logs };
}

export function getRegimeMultiplier(tags: EdgeEnvironmentTag[], progress: number): number {
    if (!tags || tags.length === 0) return 1.0;
    if (tags.includes(EdgeEnvironmentTag.EARLY_MARKET_CORRECTION_LAG)) {
        const start = 0.05, end = 0.35;
        const t = clamp((Math.max(start, Math.min(end, progress)) - start) / (end - start), 0, 1);
        return 1.45 - (t * 0.45);
    }
    return 1.0;
}

export const getMarketBlueprint = (match: Match, signals: Partial<AISignals>): MarketBlueprint => {
    const odds = signals.odds;
    const marketTotal = odds?.cur?.total || 0;
    const fairTotal = signals.deterministic_fair_total || 0;
    const rawPrice = odds?.cur?.totalPrice || -110;
    const priceStr = rawPrice === 100 || rawPrice === -100 ? "EVEN" : (rawPrice > 0 ? `+${rawPrice}` : String(rawPrice));

    const snapshot_id = `${match.id}:${match.fetched_at || Date.now()}`;
    let isValid = true;
    let reasonCode: string | undefined = undefined;

    if (marketTotal <= 0) {
        isValid = false;
        reasonCode = "STALE_PRICE";
    }

    const direction = signals.narrative?.market_lean || 'PASS';
    const line = marketTotal;
    const ticket = (isValid && direction !== 'PASS')
        ? `${direction.charAt(0)}${direction.slice(1).toLowerCase()} ${line.toFixed(1)} (${priceStr})`
        : "Updating...";

    return {
        id: snapshot_id,
        ticket,
        direction: direction as any,
        line,
        price: priceStr,
        market_type: MarketScope.FULL_GAME,
        model_number: fairTotal,
        status: (signals.narrative?.signal_label as any) || 'LIVE_READ',
        is_valid: isValid,
        reason_code: reasonCode
    };
}

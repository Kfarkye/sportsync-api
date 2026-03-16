
import { Match, MatchStatus, Sport } from './types.ts';
import { OddsState, BetResult } from './types.ts';

// ============================================================================
// TYPES
// ============================================================================

export interface BaseAnalysis {
    state: OddsState;
    provider: string;
    isLive: boolean;
}

export interface SpreadAnalysis extends BaseAnalysis {
    line: number | null;
    display: string;
    awayLine: number | null;
    awayDisplay: string;
    odds: string;
    result: BetResult;
    isHomeFav: boolean;
    homeJuice: string;
    awayJuice: string;
    label: string;
}

export interface TotalAnalysis extends BaseAnalysis {
    line: number | null;
    display: string;
    overLine: number | null;
    underLine: number | null;
    overDisplay: string;
    underDisplay: string;
    result: 'OVER' | 'UNDER' | 'PUSH' | null;
    actual: number | null;
    overJuice: string;
    underJuice: string;
    label: string;
    displayLine: string;
}

export interface MoneylineAnalysis extends BaseAnalysis {
    home: string;
    away: string;
    draw: string;
    fav: 'home' | 'away' | null;
    result: 'home' | 'away' | 'draw' | null;
    label: string;
}

interface OddsSource {
    homeSpread: number | null;
    awaySpread: number | null;
    homeSpreadOdds: number | null;
    awaySpreadOdds: number | null;
    total: number | null;
    overOdds: number | null;
    underOdds: number | null;
    homeML: number | null;
    awayML: number | null;
    drawML: number | null;
    overLine: number | null;
    underLine: number | null;
    provider: string;
    isLive: boolean;
}

// ============================================================================
// HELPERS
// ============================================================================

export const isMatchFinal = (status: MatchStatus | string | undefined): boolean => {
    if (!status) return false;
    const s = String(status).toUpperCase();
    return ['FINISHED', 'STATUS_FINAL', 'FINAL', 'STATUS_FINAL_OT', 'POST', 'COMPLETED', 'FT', 'AET', 'PK'].some(k => s.includes(k));
};

export const isMatchLive = (status: MatchStatus | string | undefined): boolean => {
    if (!status) return false;
    const s = String(status).toUpperCase();
    return ['LIVE', 'IN_PROGRESS', 'HALFTIME', 'STATUS_IN_PROGRESS', 'STATUS_HALFTIME', 'FIRST_HALF', 'SECOND_HALF', 'STATUS_FIRST_HALF', 'STATUS_SECOND_HALF'].some(k => s.includes(k));
};

export const getOddsValue = (v: any, type?: 'spread' | 'price' | 'total'): number | null => {
    if (v === undefined || v === null || v === '') return null;

    // 1. Handle actual numbers (already parsed)
    if (typeof v === 'number') {
        if (isNaN(v)) return null;
        // Logic Protection: Spread vs Price validation
        // Spreads practically don't exceed +/- 60 even in extreme cases.
        // If we see +/- 100+, it's almost certainly a Moneyline / Price.
        if (type === 'spread' && Math.abs(v) >= 100) return null;
        return v;
    }

    // 2. String Cleaning
    const s = String(v).toUpperCase().trim();
    if (s === 'PK' || s === 'PICK' || s === 'EV' || s === 'EVEN') return 0;
    if (s === '-' || s === 'N/A' || s === 'NL') return null;

    // Remove team abbreviations/prefix if present (e.g., "EDM -5.5" -> "-5.5")
    // This allows extracting the line even from concatenated provider strings.
    const parts = s.split(/\s+/);
    let target = s;

    // If multiple parts, the one containing a digit and a sign/dot is usually the line
    if (parts.length > 1) {
        const linePart = parts.find(p => p.match(/[-+]?\d/));
        if (linePart) target = linePart;
    }

    // Secondary cleaning: remove non-numeric chars except decimals and signs
    const clean = target
        .replace(/^(O|U|OVER|UNDER)\s*/i, '')           // Strip Total prefixes
        .split('(')[0]                                  // Strip juice in parens
        .replace(/[^\d.-]/g, '');                       // Aggressive non-numeric strip

    const match = clean.match(/([-+]?\d+(\.\d+)?)/);
    if (!match) return null;

    const num = parseFloat(match[1]);
    if (isNaN(num)) return null;

    // Logic Protection: Spread vs Price
    if (type === 'spread' && Math.abs(num) >= 100) return null;

    return num;
};

// ============================================================================
// SYSTEM GUARDS (ANTI-CRASH)
// ============================================================================

/**
 * Safely slices a string or array, preventing "Cannot read properties of undefined (reading 'slice')"
 */
export const safeSlice = (val: any, start: number, end?: number): any => {
    if (!val) return Array.isArray(val) ? [] : '';
    if (typeof val.slice !== 'function') return Array.isArray(val) ? [] : '';
    return val.slice(start, end);
};

/**
 * Safely substrings a string, preventing "Cannot read properties of undefined (reading 'substring')"
 */
export const safeSubstring = (val: any, start: number, end?: number): string => {
    if (typeof val !== 'string') return '';
    return val.substring(start, end);
};

/**
 * Normalizer gate to guarantee data shape for odds objects before processing.
 * Inspired by systematic data-shape failure remediation.
 */
export const normalizeEnhancedOdds = (odds: any): any => {
    if (!odds) return { hasOdds: false, provider: 'UNKNOWN' };

    const safeNum = (v: any) => (typeof v === 'number' && isFinite(v)) ? v : null;
    const safeStr = (v: any, fallback = '') => typeof v === 'string' ? v : fallback;

    return {
        ...odds,
        match_id: odds.match_id || odds.id || null,
        provider: safeStr(odds.provider || odds.source, 'Consensus'),
        hasOdds: !!(odds.hasOdds || odds.homeWin || odds.total || odds.homeSpread),

        // ML Values
        homeML: safeNum(odds.homeML || odds.home_ml),
        awayML: safeNum(odds.awayML || odds.away_ml),
        drawML: safeNum(odds.drawML || odds.draw_ml),

        // Spreads
        homeSpread: safeNum(odds.homeSpread || odds.home_spread),
        awaySpread: safeNum(odds.awaySpread || odds.away_spread),
        homeSpreadOdds: safeNum(odds.homeSpreadOdds || odds.home_spread_odds),
        awaySpreadOdds: safeNum(odds.awaySpreadOdds || odds.away_spread_odds),

        // Totals
        total: safeNum(odds.total || odds.total_line),
        overOdds: safeNum(odds.overOdds || odds.over_odds),
        underOdds: safeNum(odds.underOdds || odds.under_odds),

        // Identifiers & Metadata
        odds_api_event_id: safeStr(odds.odds_api_event_id || odds.eventId, ''),
        updated_at: safeStr(odds.updated_at || odds.lastUpdated, new Date().toISOString()),

        // Ensure no nested slices can fail
        lastUpdated: safeStr(odds.lastUpdated || odds.updated_at, '')
    };
};

const MAX_MONEYLINE = 20000;

const formatOdds = (val: number | null): string => {
    if (val === null) return '-';
    if (val === 100 || val === -100) return 'EVEN';
    if (Math.abs(val) > MAX_MONEYLINE) return '-';
    return val > 0 ? `+${val}` : `${val}`;
};

const getSpreadLabel = (sport: Sport): string => {
    switch (sport) {
        case Sport.BASEBALL: return 'Run Line';
        case Sport.HOCKEY: return 'Puck Line';
        case Sport.SOCCER: return 'Goal Line';
        case Sport.MMA: return 'Point Spread';
        default: return 'Spread';
    }
};

// ============================================================================
// NORMALIZATION
// ============================================================================

export const normalizeOpeningLines = (dbRow: any) => {
    if (!dbRow) return undefined;
    const parseVal = (val: any) => {
        if (val === null || val === undefined || val === '') return undefined;
        const num = Number(val);
        return isNaN(num) ? undefined : num;
    };
    const parseML = (val: any) => {
        if (typeof val === 'number') return val;
        return val ? parseInt(String(val).replace('+', ''), 10) : undefined;
    };

    const isExtremeML = (ml: number | undefined) => {
        if (ml === undefined) return true;
        const absVal = Math.abs(ml);
        return absVal >= 4000;
    };

    const hML = parseML(dbRow.home_ml);
    const aML = parseML(dbRow.away_ml);
    const totalVal = parseVal(dbRow.opening_total || dbRow.total);

    return {
        hasOdds: true,
        homeSpread: parseVal(dbRow.home_spread),
        awaySpread: parseVal(dbRow.away_spread),
        total: (totalVal && totalVal > 1) ? totalVal : undefined,
        overUnder: parseVal(dbRow.total),
        homeML: isExtremeML(hML) ? undefined : hML,
        awayML: isExtremeML(aML) ? undefined : aML,
        drawML: parseML(dbRow.draw_ml),
        provider: dbRow.provider || dbRow.source
    };
};

export const normalizeClosingLines = (dbRow: any) => {
    if (!dbRow) return undefined;
    const parseVal = (val: any) => {
        if (val === null || val === undefined || val === '') return undefined;
        const num = Number(val);
        return isNaN(num) ? undefined : num;
    };
    const parseML = (val: string | number) => {
        if (typeof val === 'number') return val;
        return val ? parseInt(String(val).replace('+', ''), 10) : undefined;
    };
    return {
        hasOdds: true,
        provider: dbRow.provider || 'Closing Consensus',
        homeSpread: parseVal(dbRow.home_spread),
        awaySpread: parseVal(dbRow.away_spread),
        overUnder: parseVal(dbRow.total),
        homeWin: parseML(dbRow.home_ml),
        awayWin: parseML(dbRow.away_ml),
        draw: parseML(dbRow.draw_ml),
        spread: parseVal(dbRow.home_spread),
        total: parseVal(dbRow.total)
    };
};

const normalizeSource = (match: Match): OddsSource => {
    const isFinal = isMatchFinal(match.status);
    // PRIMARY: Check status. FALLBACK: Check current_odds.isLive flag from ingest-odds
    // This handles race conditions where odds arrive before status sync
    const isLive = isMatchLive(match.status) || (match.current_odds as any)?.isLive === true;

    const stack: { data: any, weight: number, label: string }[] = [];

    // Priority 1: Closing Odds (DB) or API Odds (if Final)
    if (isFinal) {
        if (match.closing_odds && Object.keys(match.closing_odds).length > 0) {
            stack.push({ data: match.closing_odds, weight: 100, label: 'Closing' });
        }
        // Fallback to whatever odds we have attached
        if (match.odds && Object.keys(match.odds).length > 0) {
            stack.push({ data: match.odds, weight: 95, label: 'Closing Consensus' });
        }
    }

    // Priority 2: Current Odds (Live)
    // CRITICAL: If LIVE, we ONLY accept Live odds. If Live odds are missing, we DO NOT fallback to pre-game.
    // Displaying "Over 41.5" when the score is 35-38 is catastrophic.
    if (isLive) {
        if (match.current_odds && Object.keys(match.current_odds).length > 0) {
            stack.push({ data: match.current_odds, weight: 100, label: 'Live' });
        }
        // STOIC GUARD: Do not push Consensus (70) or Opening (40) if Live.
        // Falling back to pre-game lines during active play is strictly forbidden in elite apps.
    } else {
        // Normal Priority Stack for Pre-Game / Post-Game
        if (match.current_odds && Object.keys(match.current_odds).length > 0) {
            stack.push({ data: match.current_odds, weight: 80, label: 'Live' });
        }
        if (match.odds && Object.keys(match.odds).length > 0) {
            stack.push({ data: match.odds, weight: 70, label: 'Consensus' });
        }
        if (match.opening_odds && Object.keys(match.opening_odds).length > 0) {
            stack.push({ data: match.opening_odds, weight: 40, label: 'Opening' });
        }
    }

    stack.sort((a, b) => b.weight - a.weight);

    const resolveGroup = (fieldGroups: { keys: string[], type: 'spread' | 'price' | 'total' }[]): (number | null)[] => {
        for (const source of stack) {
            const raw = source.data;
            if (!raw) continue;

            const values: (number | null)[] = fieldGroups.map(() => null);
            let hasAtLeastOne = false;

            fieldGroups.forEach((group, i) => {
                for (const k of group.keys) {
                    let val: any = undefined;
                    if (k.includes('.')) {
                        const parts = k.split('.');
                        let curr = raw;
                        for (const p of parts) {
                            if (curr && typeof curr === 'object') curr = curr[p];
                            else { curr = undefined; break; }
                        }
                        val = curr;
                    } else {
                        val = raw[k];
                    }
                    const parsed = getOddsValue(val, group.type);
                    if (parsed !== null) {
                        values[i] = parsed;
                        hasAtLeastOne = true;
                        break;
                    }
                }
            });

            // CRITICAL: If any part of this market group is found in this source, 
            // return ALL values from this source (even if some are null) to avoid 
            // "frankenstein" mixtures from opening/closing.
            if (hasAtLeastOne) return values;
        }
        return fieldGroups.map(() => null);
    };

    const primarySource = stack[0];
    const provider = primarySource?.data?.provider || primarySource?.label || 'Consensus';
    const isLiveOdds = (primarySource?.label === 'Live' || primarySource?.label === 'Consensus') && isLive;

    // Resolve by Markets
    const spreadRes = resolveGroup([
        { keys: ['homeSpread', 'home_spread', 'spread_home', 'spread_home_value', 'spread.home', 'spread'], type: 'spread' },
        { keys: ['awaySpread', 'away_spread', 'spread_away', 'spread_away_value', 'spread.away'], type: 'spread' },
        { keys: ['homeSpreadOdds', 'home_spread_odds', 'spread_home_odds'], type: 'price' },
        { keys: ['awaySpreadOdds', 'away_spread_odds', 'spread_away_odds'], type: 'price' }
    ]);

    // --- LIVE SOCCER SPREAD CORRECTION (Rest of Game -> Full Game) ---
    // Soccer feeds often send 'Handicaps from Now' while sending 'Full Game Totals'.
    // We use the score context captured at the moment of ingest for deterministic normalization.
    if (match.sport === Sport.SOCCER && isLive && spreadRes[0] !== null) {
        const hScore = primarySource?.data?.homeScoreAtOdds ?? match.homeScore;
        const aScore = primarySource?.data?.awayScoreAtOdds ?? match.awayScore;

        if (typeof hScore === 'number' && typeof aScore === 'number') {
            const scoreDiff = aScore - hScore;
            if (Math.abs(scoreDiff) > 0) {
                // Heuristic: If current line is significantly smaller than the score diff, it's a Rest of Game line.
                if (Math.abs(spreadRes[0]) < Math.abs(scoreDiff)) {
                    spreadRes[0] = spreadRes[0] + scoreDiff;
                    if (spreadRes[1] !== null) spreadRes[1] = spreadRes[1] - scoreDiff;
                }
            }
        }
    }

    const totalRes = resolveGroup([
        { keys: ['overUnder', 'over_under', 'total', 'total_line', 'total.over', 'over', 'totalLine'], type: 'total' },
        { keys: ['overOdds', 'over_odds', 'total_over_odds', 'overPrice', 'total_best.over.price'], type: 'price' },
        { keys: ['underOdds', 'under_odds', 'total_under_odds', 'underPrice', 'total_best.under.price'], type: 'price' },
        { keys: ['overLine', 'over_line'], type: 'total' },
        { keys: ['underLine', 'under_line'], type: 'total' }
    ]);

    const mlRes = resolveGroup([
        { keys: ['homeWin', 'home_ml', 'home_moneyline', 'moneyline.home', 'ml.home', 'moneylineHome'], type: 'price' },
        { keys: ['awayWin', 'away_ml', 'away_moneyline', 'moneyline.away', 'ml.away', 'moneylineAway'], type: 'price' },
        { keys: ['draw', 'draw_ml', 'draw_moneyline', 'moneyline.draw', 'ml.draw'], type: 'price' }
    ]);

    return {
        homeSpread: spreadRes[0],
        awaySpread: spreadRes[1],
        homeSpreadOdds: spreadRes[2],
        awaySpreadOdds: spreadRes[3],
        total: totalRes[0],
        overOdds: totalRes[1],
        underOdds: totalRes[2],
        overLine: totalRes[3],
        underLine: totalRes[4],
        homeML: mlRes[0],
        awayML: mlRes[1],
        drawML: mlRes[2],
        provider,
        isLive: isLiveOdds
    };
};

// ============================================================================
// ANALYZERS
// ============================================================================

export const analyzeSpread = (match: Match): SpreadAnalysis => {
    const source = normalizeSource(match);
    const isFinal = isMatchFinal(match.status);

    let line = source.homeSpread;
    let awayLine = source.awaySpread;
    let display = '-';
    let awayDisplay = '-';

    // 1. Initial Fill (Inversion Fallback)
    if (line === null && awayLine !== null) line = -awayLine;
    else if (awayLine === null && line !== null) awayLine = -line;

    // 2. Market-Aware Correction (Sign Enforcement)
    // Sometimes feeds provide a bare 'spread' that gets assigned to Home, 
    // but it physically represents the Away favorite (or vice versa).
    if (source.homeML !== null && source.awayML !== null) {
        if (line !== null && awayLine !== null) {
            const isHomeFavorite = source.homeML < source.awayML;
            const isAwayFavorite = source.awayML < source.homeML;

            // If Home is the underdog but has a significant negative line, it's flipped.
            if (isAwayFavorite && line < -0.1 && awayLine > 0.1) {
                const tmp = line;
                line = awayLine;
                awayLine = tmp;
            }
            // If Away is the underdog but has a significant negative line, it's flipped.
            else if (isHomeFavorite && awayLine < -0.1 && line > 0.1) {
                const tmp = line;
                line = awayLine;
                awayLine = tmp;
            }
        }
    }

    // 3. Puckline/Runline Defaulting
    if (line === null && (match.sport === Sport.HOCKEY || match.sport === Sport.BASEBALL)) {
        if (source.homeML !== null && source.awayML !== null) {
            if (source.homeML < source.awayML) {
                line = -1.5; awayLine = 1.5;
            } else {
                line = 1.5; awayLine = -1.5;
            }
        } else {
            line = 1.5; awayLine = -1.5;
        }
    }

    if (line !== null) {
        if (line === 0) display = 'PK';
        else display = line > 0 ? `+${line}` : `${line}`;
    }
    if (awayLine !== null) {
        if (awayLine === 0) awayDisplay = 'PK';
        else awayDisplay = awayLine > 0 ? `+${awayLine}` : `${awayLine}`;
    }

    let result: BetResult = null;
    if (isFinal && line !== null && typeof match.homeScore === 'number' && typeof match.awayScore === 'number') {
        const adjustedHome = match.homeScore + line;
        const diff = adjustedHome - match.awayScore;
        if (Math.abs(diff) < 0.1) result = 'push';
        else result = diff > 0 ? 'won' : 'lost';
    }

    return {
        state: isFinal ? 'settled' : (source.isLive ? 'live' : 'open'),
        provider: source.provider,
        isLive: source.isLive,
        line,
        display,
        awayLine,
        awayDisplay,
        odds: formatOdds(source.homeSpreadOdds),
        result,
        isHomeFav: line !== null && line < 0,
        homeJuice: formatOdds(source.homeSpreadOdds),
        awayJuice: formatOdds(source.awaySpreadOdds),
        label: getSpreadLabel(match.sport)
    };
};

export const analyzeTotal = (match: Match): TotalAnalysis => {
    const source = normalizeSource(match);
    const isFinal = isMatchFinal(match.status);

    let line = source.total;
    let display = line !== null ? String(line) : '-';
    let overLine = source.overLine ?? line;
    let underLine = source.underLine ?? line;

    let overDisplay = overLine !== null ? `o${overLine}` : '-';
    let underDisplay = underLine !== null ? `u${underLine}` : '-';

    let result: TotalAnalysis['result'] = null;
    let actual: number | null = null;
    // ... rest of result logic is same ...

    if (isFinal && typeof match.homeScore === 'number' && typeof match.awayScore === 'number') {
        actual = match.homeScore + match.awayScore;
        if (line !== null) {
            if (actual > line) result = 'OVER';
            else if (actual < line) result = 'UNDER';
            else result = 'PUSH';
        }
    }

    return {
        state: isFinal ? 'settled' : (source.isLive ? 'live' : 'open'),
        provider: source.provider,
        isLive: source.isLive,
        line,
        display,
        overLine,
        underLine,
        overDisplay,
        underDisplay,
        displayLine: line !== null ? String(line) : '-',
        result,
        actual,
        overJuice: formatOdds(source.overOdds),
        underJuice: formatOdds(source.underOdds),
        label: 'Total'
    };
};

export const analyzeMoneyline = (match: Match): MoneylineAnalysis => {
    const source = normalizeSource(match);
    const isFinal = isMatchFinal(match.status);

    let fav: 'home' | 'away' | null = null;
    if (source.homeML !== null && source.awayML !== null) {
        // Simple comparison: lower number is favorite (e.g. -150 < +130)
        // Or if both positive, lower is favorite (+110 < +200)
        if (source.homeML < source.awayML) fav = 'home';
        else if (source.awayML < source.homeML) fav = 'away';
    }

    let result: 'home' | 'away' | 'draw' | null = null;
    if (isFinal && typeof match.homeScore === 'number') {
        if (match.homeScore > match.awayScore) result = 'home';
        else if (match.awayScore > match.homeScore) result = 'away';
        else result = 'draw';
    }

    return {
        state: isFinal ? 'settled' : (source.isLive ? 'live' : 'open'),
        provider: source.provider,
        isLive: source.isLive,
        home: formatOdds(source.homeML),
        away: formatOdds(source.awayML),
        draw: formatOdds(source.drawML),
        fav,
        result,
        label: 'Moneyline'
    };
};

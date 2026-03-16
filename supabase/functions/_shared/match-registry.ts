/**
 * Canonical Match Registry (SSOT)
 * Edit in packages/shared/src/match-registry.ts and run `npm run sync:shared`.
 */

/**
 * @fileoverview Canonical Match Registry (Edge-Side)
 * SRE-Hardened source of truth for Sport/League ID mappings and resolution.
 */

type SupabaseClient = any;
// =============================================================================
// 1. CONFIGURATION & CONTRACTS
// =============================================================================

export interface Logger {
    info(message: string, meta?: Record<string, unknown>): void;
    warn(message: string, meta?: Record<string, unknown>): void;
    error(message: string, meta?: Record<string, unknown>): void;
}

const defaultLogger: Logger = {
    info: (msg, meta) => console.log(JSON.stringify({ level: 'INFO', msg, ...meta })),
    warn: (msg, meta) => console.warn(JSON.stringify({ level: 'WARN', msg, ...meta })),
    error: (msg, meta) => console.error(JSON.stringify({ level: 'ERROR', msg, ...meta })),
};

const CONFIG = {
    DB_TIMEOUT_MS: 3000,
    RETRY_ATTEMPTS: 2,
    RETRY_DELAY_MS: 100,
} as const;

export const LEAGUE_SUFFIX_MAP: Readonly<Record<string, string>> = Object.freeze({
    'nfl': '_nfl', 'americanfootball_nfl': '_nfl',
    'college-football': '_ncaaf', 'americanfootball_ncaaf': '_ncaaf',
    'nba': '_nba', 'basketball_nba': '_nba',
    'mens-college-basketball': '_ncaab', 'basketball_ncaab': '_ncaab',
    'wnba': '_wnba', 'basketball_wnba': '_wnba',
    'mlb': '_mlb', 'baseball_mlb': '_mlb',
    'nhl': '_nhl', 'icehockey_nhl': '_nhl',
    'eng.1': '_epl', 'soccer_epl': '_epl',
    'usa.1': '_mls', 'soccer_usa_mls': '_mls',
    'esp.1': '_laliga', 'soccer_spain_la_liga': '_laliga',
    'ger.1': '_bundesliga', 'soccer_germany_bundesliga': '_bundesliga',
    'ita.1': '_seriea', 'soccer_italy_serie_a': '_seriea',
    'fra.1': '_ligue1', 'soccer_france_ligue_one': '_ligue1',
    'uefa.champions': '_ucl', 'soccer_uefa_champions_league': '_ucl',
    'uefa.europa': '_uel', 'soccer_uefa_europa_league': '_uel',
    'ned.1': '_ned.1', 'soccer_ned_1': '_ned.1',
    'por.1': '_por.1', 'soccer_por_1': '_por.1',
    'bel.1': '_bel.1', 'soccer_bel_1': '_bel.1',
    'tur.1': '_tur.1', 'soccer_tur_1': '_tur.1',
    'bra.1': '_bra.1', 'soccer_bra_1': '_bra.1',
    'arg.1': '_arg.1', 'soccer_arg_1': '_arg.1',
    'sco.1': '_sco.1', 'soccer_sco_1': '_sco.1',
    'caf.nations': '_afcon', 'soccer_caf_nations': '_afcon',
    'fifa.world': '_worldcup', 'soccer_fifa_world_cup': '_worldcup',
    'atp': '_tennis', 'wta': '_tennis', 'tennis_atp': '_tennis', 'tennis_wta': '_tennis'
});

export const LEAGUE_ID_MAP: Readonly<Record<string, string>> = Object.freeze({
    'nba': 'nba', 'basketball_nba': 'nba',
    'nfl': 'nfl', 'americanfootball_nfl': 'nfl',
    'ncaaf': 'college-football', 'americanfootball_ncaaf': 'college-football',
    'ncaab': 'mens-college-basketball', 'basketball_ncaab': 'mens-college-basketball',
    'mlb': 'mlb', 'baseball_mlb': 'mlb',
    'nhl': 'nhl', 'icehockey_nhl': 'nhl',
    'epl': 'eng.1', 'soccer_epl': 'eng.1',
    'laliga': 'esp.1', 'soccer_spain_la_liga': 'esp.1',
    'mls': 'usa.1', 'soccer_usa_mls': 'usa.1',
    'bundesliga': 'ger.1', 'soccer_germany_bundesliga': 'ger.1',
    'seriea': 'ita.1', 'soccer_italy_serie_a': 'ita.1',
    'ligue1': 'fra.1', 'soccer_france_ligue_one': 'fra.1',
    'ucl': 'uefa.champions', 'soccer_uefa_champions_league': 'uefa.champions', 'soccer_uefa_champs_league': 'uefa.champions',
    'uel': 'uefa.europa', 'soccer_uefa_europa_league': 'uefa.europa',
    'ned.1': 'ned.1', 'soccer_ned_1': 'ned.1',
    'por.1': 'por.1', 'soccer_por_1': 'por.1',
    'bel.1': 'bel.1', 'soccer_bel_1': 'bel.1',
    'tur.1': 'tur.1', 'soccer_tur_1': 'tur.1',
    'bra.1': 'bra.1', 'soccer_bra_1': 'bra.1',
    'arg.1': 'arg.1', 'soccer_arg_1': 'arg.1',
    'sco.1': 'sco.1', 'soccer_sco_1': 'sco.1',
    'afcon': 'caf.nations', 'soccer_caf_nations': 'caf.nations',
    'worldcup': 'fifa.world', 'soccer_fifa_world_cup': 'fifa.world',
    'wnba': 'wnba', 'basketball_wnba': 'wnba',
    'atp': 'atp', 'wta': 'wta', 'tennis_atp': 'atp', 'tennis_wta': 'wta'
});

export const getCanonicalLeagueId = (rawLeague: string): string => {
    if (!rawLeague) return 'unknown';
    const norm = rawLeague.toLowerCase().trim();
    return LEAGUE_ID_MAP[norm] || norm;
};

// NBA-specific team name aliases (exact case-insensitive match)
const NBA_TEAM_EXACT: Record<string, string> = {
    'la clippers': 'los angeles clippers',
    'l.a. clippers': 'los angeles clippers',
    'la lakers': 'los angeles lakers',
    'l.a. lakers': 'los angeles lakers',
    'okc thunder': 'oklahoma city thunder',
    'okc': 'oklahoma city thunder',
    'philly 76ers': 'philadelphia 76ers',
    '76ers': 'philadelphia 76ers',
    'sixers': 'philadelphia 76ers',
};

// General team name aliases for all sports
const TEAM_ALIASES: Record<string, string> = {
    'la chargers': 'los angeles chargers',
    'la rams': 'los angeles rams',
    'la dodgers': 'los angeles dodgers',
    'la angels': 'los angeles angels',
    'la galaxy': 'los angeles galaxy',
    'la kings': 'los angeles kings',
    'ny knicks': 'new york knicks',
    'ny giants': 'new york giants',
    'ny jets': 'new york jets',
    'ny rangers': 'new york rangers',
    'ny islanders': 'new york islanders',
    'ny mets': 'new york mets',
    'ny yankees': 'new york yankees',
    'nj devils': 'new jersey devils',
    'sf giants': 'san francisco giants',
    'sf 49ers': 'san francisco 49ers',
    'tb buccaneers': 'tampa bay buccaneers',
    'tb rays': 'tampa bay rays',
    'tb lightning': 'tampa bay lightning',
    'kc chiefs': 'kansas city chiefs',
    'kc royals': 'kansas city royals',
    'nola pelicans': 'new orleans pelicans',
    // Serie A Italian teams (Odds API vs ESPN naming)
    'genoa cfc': 'genoa',
    'cagliari calcio': 'cagliari',
    'ac milan': 'milan',
    'inter milan': 'inter',
    'internazionale': 'inter',
    'fc internazionale milano': 'inter',
    'as roma': 'roma',
    'ss lazio': 'lazio',
    'ssc napoli': 'napoli',
    'juventus fc': 'juventus',
    'atalanta bc': 'atalanta',
    'us lecce': 'lecce',
    'acf fiorentina': 'fiorentina',
    'bologna fc': 'bologna',
    'torino fc': 'torino',
    'empoli fc': 'empoli',
    'hellas verona': 'verona',
    'udinese calcio': 'udinese',
    'us sassuolo': 'sassuolo',
    'us salernitana': 'salernitana',
    'parma calcio': 'parma',
    'como 1907': 'como',
    'venezia fc': 'venezia',
    'monza': 'ac monza',
};

export const normalizeTeam = (name: string, leagueId?: string): string => {
    if (!name) return 'unknown';

    // 1. Unicode Normalization & Lowercase
    let clean = name.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim();

    // 2. Quick includes-based aliasing for LA teams (handle BOTH short and full forms)
    if (clean.includes("la clippers") || clean.includes("los angeles clippers")) {
        return "losangelesclippers";
    }
    if (clean.includes("la lakers") || clean.includes("los angeles lakers")) {
        return "losangeleslakers";
    }

    // 3. NBA exact matches (highest priority)
    if (!leagueId || leagueId === 'nba' || leagueId === 'basketball_nba') {
        if (NBA_TEAM_EXACT[clean]) {
            clean = NBA_TEAM_EXACT[clean];
        }
    }

    // 3. General aliases (all sports)
    if (TEAM_ALIASES[clean]) {
        clean = TEAM_ALIASES[clean];
    }

    // 4. Pattern-based prefix expansion (fallback)
    if (clean.startsWith('la ')) clean = `los angeles ${clean.slice(3)}`;
    if (clean.startsWith('l.a. ')) clean = `los angeles ${clean.slice(5)}`;
    if (clean.startsWith('ny ')) clean = `new york ${clean.slice(3)}`;
    if (clean.startsWith('nj ')) clean = `new jersey ${clean.slice(3)}`;
    if (clean.startsWith('sf ')) clean = `san francisco ${clean.slice(3)}`;
    if (clean.startsWith('tb ')) clean = `tampa bay ${clean.slice(3)}`;
    if (clean.startsWith('kc ')) clean = `kansas city ${clean.slice(3)}`;
    if (clean.startsWith('okc ')) clean = `oklahoma city ${clean.slice(4)}`;

    // 5. Additional Abbreviations
    clean = clean
        .replace(/\b(st\.|saint)\b/g, 'st')
        .replace(/\b(utd)\b/g, 'united')
        .replace(/\b(mt\.|mount)\b/g, 'mt');

    // 6. Strip Punctuation
    clean = clean.replace(/['".]/g, '');

    // 7. Remove Noise words
    clean = clean.replace(/\b(the|fc|afc|sc|club|cf|united|city|real|inter|ac)\b/g, '');

    // 8. Final: strip non-alphanumeric
    return clean.replace(/[^a-z0-9]/g, '').trim() || 'unknown';
};

export const generateCanonicalGameId = (
    teamA: string,
    teamB: string,
    commenceTime: string | Date,
    leagueId: string
): string => {
    const dateObj = new Date(commenceTime);
    if (isNaN(dateObj.getTime())) throw new Error(`Invalid commenceTime: ${commenceTime}`);

    const datePart = dateObj.toISOString().split('T')[0].replace(/-/g, '');
    const slugA = normalizeTeam(teamA);
    const slugB = normalizeTeam(teamB);
    const [teamFirst, teamSecond] = [slugA, slugB].sort();

    const normalizedLeague = getCanonicalLeagueId(leagueId);
    const suffix = LEAGUE_SUFFIX_MAP[normalizedLeague] || `_${normalizedLeague}`;
    const cleanSuffix = suffix.startsWith('_') ? suffix.substring(1) : suffix;

    return `${datePart}_${teamFirst}_${teamSecond}_${cleanSuffix}`;
};

/**
 * Normalizes a raw ID (e.g. "401825420") into a canonical DB ID (e.g. "401825420_ncaab").
 */
export const getCanonicalMatchId = (rawId: string, leagueId?: string): string => {
    if (!rawId) return '';
    if (rawId.includes('_')) return rawId;
    const normalizedLeague = getCanonicalLeagueId(leagueId || '');
    const suffix = LEAGUE_SUFFIX_MAP[normalizedLeague] || '';
    return `${rawId}${suffix}`;
};

/**
 * Deterministic ID generation for canonical linking.
 */
export const generateDeterministicId = generateCanonicalGameId;

/**
 * Validates if an ID is canonical (contains an underscore).
 */
export const isCanonicalId = (id: string): boolean => {
    return typeof id === 'string' && id.includes('_');
};

export async function resolveCanonicalVenue(
    supabase: SupabaseClient,
    name: string,
    city: string
): Promise<string | null> {
    if (!name) return null;
    const { data } = await supabase
        .from('canonical_venues')
        .select('id')
        .ilike('name', `%${name}%`)
        .maybeSingle();
    return data?.id || null;
}

export async function resolveCanonicalOfficial(
    supabase: SupabaseClient,
    name: string,
    leagueId: string,
    sport: string
): Promise<string | null> {
    if (!name) return null;
    const { data } = await supabase
        .from('canonical_officials')
        .select('id')
        .ilike('name', `%${name}%`)
        .eq('sport', sport)
        .maybeSingle();
    return data?.id || null;
}

export async function resolveCanonicalMatch(
    supabase: SupabaseClient,
    homeTeam: string,
    awayTeam: string,
    commenceTime: string | Date,
    leagueId: string,
    logger: Logger = defaultLogger
): Promise<string | null> {
    try {
        const date = new Date(commenceTime);
        const windowStart = new Date(date); windowStart.setDate(date.getDate() - 1);
        const windowEnd = new Date(date); windowEnd.setDate(date.getDate() + 1);

        const normalizedHome = normalizeTeam(homeTeam);
        const normalizedAway = normalizeTeam(awayTeam);
        const canonicalLeague = getCanonicalLeagueId(leagueId);

        // DEBUG: Log input
        logger.info("resolveCanonicalMatch INPUT", {
            rawHome: homeTeam,
            rawAway: awayTeam,
            normalizedHome,
            normalizedAway,
            canonicalLeague,
            windowStart: windowStart.toISOString(),
            windowEnd: windowEnd.toISOString()
        });

        // Get the expected suffix for this league
        const LEAGUE_SUFFIX = {
            'nba': '_nba', 'nfl': '_nfl', 'nhl': '_nhl', 'mlb': '_mlb',
            'college-football': '_ncaaf', 'mens-college-basketball': '_ncaab',
            'eng.1': '_epl', 'ita.1': '_seriea', 'esp.1': '_laliga', 'ger.1': '_bundesliga',
            'uefa.champions': '_ucl', 'uefa.europa': '_uel', 'fifa.world': '_worldcup'
        } as Record<string, string>;
        const expectedSuffix = LEAGUE_SUFFIX[canonicalLeague] || `_${canonicalLeague}`;

        const { data: matches, error } = await supabase
            .from("matches")
            .select("id,start_time,home_team,away_team")
            .eq("league_id", canonicalLeague)
            .like("id", `%${expectedSuffix}`)  // Only match IDs ending with correct suffix
            .gte("start_time", windowStart.toISOString())
            .lte("start_time", windowEnd.toISOString());

        if (error) {
            logger.warn("resolveCanonicalMatch query failed", { error });
            return null;
        }

        if (!matches?.length) return null;

        const extractName = (t: unknown): string => {
            if (!t) return "";
            if (typeof t === "string") return t;
            if (typeof t === "object") {
                const o = t as any;
                return o.displayName || o.name || o.shortName || o.abbreviation || "";
            }
            return "";
        };

        // Helper for fuzzy matching (handles LA/Los Angeles, etc.)
        const fuzzyMatch = (a: string, b: string): boolean => {
            if (a === b) return true;
            if (a.includes(b) || b.includes(a)) return true;
            // Extract last token (team name typically) 
            const aLast = a.slice(-7);
            const bLast = b.slice(-7);
            if (aLast.length > 3 && bLast.length > 3 && aLast === bLast) return true;
            return false;
        };

        // Score candidates and pick best (prevents first-hit wrong match)
        type Candidate = { id: string; score: number; dtDiffMs: number };
        const candidates: Candidate[] = [];

        // DEBUG: Log first 3 comparisons
        let comparisonCount = 0;
        for (const m of matches) {
            const rawDbHome = extractName(m.home_team);
            const rawDbAway = extractName(m.away_team);
            const dbHome = normalizeTeam(rawDbHome);
            const dbAway = normalizeTeam(rawDbAway);

            // DEBUG: Log first 3 comparisons
            if (comparisonCount < 3) {
                logger.info("resolveCanonicalMatch COMPARE", {
                    matchId: m.id,
                    dbHome,
                    dbAway,
                    oddsHome: normalizedHome,
                    oddsAway: normalizedAway,
                    exactHomeMatch: dbHome === normalizedHome,
                    exactAwayMatch: dbAway === normalizedAway
                });
                comparisonCount++;
            }

            // Exact match
            const exactSame = dbHome === normalizedHome && dbAway === normalizedAway;
            const exactSwap = dbHome === normalizedAway && dbAway === normalizedHome;
            // Fuzzy fallback
            const fuzzySame = fuzzyMatch(dbHome, normalizedHome) && fuzzyMatch(dbAway, normalizedAway);
            const fuzzySwap = fuzzyMatch(dbHome, normalizedAway) && fuzzyMatch(dbAway, normalizedHome);

            if (!exactSame && !exactSwap && !fuzzySame && !fuzzySwap) continue;

            // DEBUG: Log when a candidate PASSES
            logger.info("resolveCanonicalMatch CANDIDATE_PASSED", {
                matchId: m.id,
                dbHome,
                dbAway,
                oddsHome: normalizedHome,
                oddsAway: normalizedAway,
                exactSame,
                exactSwap,
                fuzzySame,
                fuzzySwap
            });

            const mStart = new Date((m as any).start_time).getTime();
            const dtDiffMs = Math.abs(mStart - date.getTime());

            // Exact match gets bonus
            const exactBonus = (exactSame || exactSwap) ? 500000 : 0;
            const score = 1000000 - dtDiffMs + exactBonus;

            candidates.push({ id: m.id, score, dtDiffMs });
        }

        // DEBUG: Log final candidate selection results
        logger.info("resolveCanonicalMatch SELECTION_RESULT", {
            candidatesFound: candidates.length,
            oddsHome: normalizedHome,
            oddsAway: normalizedAway,
            topCandidates: candidates.slice(0, 3).map(c => ({ id: c.id, score: c.score }))
        });

        if (!candidates.length) {
            logger.warn("resolveCanonicalMatch NO_CANDIDATES", {
                totalMatchesInWindow: matches.length,
                oddsHome: normalizedHome,
                oddsAway: normalizedAway
            });
            return null;
        }

        candidates.sort((a, b) => b.score - a.score);
        return candidates[0].id;
    } catch (e) {
        logger.error("Unexpected error in resolveCanonicalMatch", { error: e });
        return null;
    }
}

/**
 * Extracts the base ID from a canonical match ID (e.g. 401810427_nba -> 401810427).
 */
export const getBaseId = (id: string): string => {
    if (!id) return '';
    return id.split('_')[0];
};

/**
 * US Game Day Translation: Standardizes the "Game Day" for lookups.
 * Uses Pacific Time (America/Los_Angeles) with proper DST handling.
 * Applies "3 AM Rule": games from 12AM-3AM belong to the previous day's betting slate.
 * This MUST match the frontend's getBettingSlateDate() for UI/DB parity.
 */
export const toLocalGameDate = (isoStr: string | Date): string => {
    const d = new Date(typeof isoStr === 'string' ? isoStr.replace(' ', 'T') : isoStr);
    if (isNaN(d.getTime())) return new Date().toISOString().split('T')[0];

    // Use Intl.DateTimeFormat for reliable Pacific Time conversion (handles DST)
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'America/Los_Angeles',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: 'numeric',
        hour12: false
    }).formatToParts(d);

    const find = (t: string) => parts.find(p => p.type === t)?.value;
    const dateStr = `${find('year')}-${find('month')}-${find('day')}`;
    const hour = parseInt(find('hour') || '0', 10);

    // 3 AM Rule: Games played 12AM-3AM Pacific belong to previous day's slate
    if (hour < 3) {
        const adjusted = new Date(dateStr);
        adjusted.setDate(adjusted.getDate() - 1);
        return adjusted.toISOString().split('T')[0];
    }

    return dateStr;
};


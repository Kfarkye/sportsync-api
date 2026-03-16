// Fix: Add Deno global declaration for TypeScript compatibility
declare const Deno: any;

import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'
import { createClient } from 'npm:@supabase/supabase-js@2'
import { getCanonicalLeagueId, resolveCanonicalMatch, normalizeTeam } from '../_shared/match-registry.ts'

const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type, x-cron-secret',
}

// SRE Structured Logger - No Silent Failures
const Logger = {
    info: (event: string, data: Record<string, any> = {}) => console.log(JSON.stringify({ level: 'INFO', ts: new Date().toISOString(), event, ...data })),
    warn: (event: string, data: Record<string, any> = {}) => console.warn(JSON.stringify({ level: 'WARN', ts: new Date().toISOString(), event, ...data })),
    error: (event: string, data: Record<string, any> = {}) => console.error(JSON.stringify({ level: 'ERROR', ts: new Date().toISOString(), event, ...data })),
};

const SOCCER_LEAGUE_AUTO_EXPAND = [
    'epl',
    'laliga',
    'seriea',
    'bundesliga',
    'ligue1',
    'mls',
    'uefa.champions',
    'uefa.europa',
    'arg.1',
    'bra.1',
    'ned.1',
    'tur.1',
    'por.1',
    'bel.1',
    'sco.1',
];

// === INSTRUMENTATION: Diagnose environment/schema split ===
function projectRefFromUrl(url: string): string {
    try {
        const host = new URL(url).host;
        return host.split(".")[0] ?? "unknown";
    } catch { return "unknown"; }
}

async function hash8(input: string): Promise<string> {
    const data = new TextEncoder().encode(input);
    const digest = await crypto.subtle.digest("SHA-256", data);
    const bytes = Array.from(new Uint8Array(digest));
    return bytes.slice(0, 4).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function timingSafeEqual(a: string, b: string): boolean {
    if (a.length !== b.length) return false;
    let out = 0;
    for (let i = 0; i < a.length; i++) out |= a.charCodeAt(i) ^ b.charCodeAt(i);
    return out === 0;
}

serve(async (req) => {
    if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders })

    const debug: any = { timestamp: new Date().toISOString(), steps: [] }

    try {
        const supabaseUrl = Deno.env.get('SUPABASE_URL')
        const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')
        const cronSecret = Deno.env.get('CRON_SECRET') || ''
        const reqSecret = req.headers.get('x-cron-secret') ?? ''

        // Log runtime context once per request to diagnose env/schema split
        const projectRef = projectRefFromUrl(supabaseUrl || '')
        Logger.info('RUNTIME_CONTEXT', {
            supabase_project_ref: projectRef,
            url_present: !!supabaseUrl,
            schema_hint: 'public'
        })

        debug.steps.push({ step: 'init', has_url: !!supabaseUrl, has_key: !!supabaseKey, has_req_secret: !!reqSecret, project_ref: projectRef })

        // Auth: Accept Supabase service role key (same pattern as pregame-intel)
        // The Authorization header is already validated by Supabase's edge runtime
        const authHeader = req.headers.get('authorization') || '';
        const isServiceRole = authHeader.startsWith('Bearer ') && authHeader.length > 50;

        // Also accept x-cron-secret for backwards compatibility (using vars from lines 50-51)
        const isValidCronSecret = reqSecret.length === 32 && timingSafeEqual(reqSecret, cronSecret);

        if (!isServiceRole && !isValidCronSecret) {
            Logger.error('AUTH_FAILURE', {
                endpoint: 'ingest-odds',
                reason: 'Neither service role nor valid cron secret provided',
                has_auth_header: !!authHeader,
                has_cron_secret: !!reqSecret,
                cron_secret_len: reqSecret.length
            });
            return new Response('Unauthorized', { status: 401, headers: corsHeaders });
        }

        const supabase = createClient(supabaseUrl!, supabaseKey!)

        // FIX #2: Diagnostic - Check which matches relations exist
        const { data: matchesRelations, error: diagError } = await supabase.rpc('debug_relation_matches');
        Logger.info('DEBUG_MATCHES_RELATIONS', { data: matchesRelations, error: diagError?.message });

        const { data: leagueConfigs } = await supabase.from('league_config').select('id, odds_api_key').eq('is_active', true);
        const leagueMap = new Map(leagueConfigs?.map(c => [c.id, c.odds_api_key]) || []);
        const activeLeagueIds = (leagueConfigs || []).map((c: any) => String(c.id));
        const activeSoccerLeagueIds = (leagueConfigs || [])
            .filter((c: any) => String(c.odds_api_key || '').startsWith('soccer_'))
            .map((c: any) => String(c.id));

        const body = await req.json().catch(() => ({}))
        const rawRequested = body.leagues || (body.sport_key ? [body.sport_key] : body.sport_keys || []);
        const requestedArray = Array.isArray(rawRequested)
            ? rawRequested.map((v: any) => String(v).trim()).filter(Boolean)
            : String(rawRequested || '').split(',').map((v) => v.trim()).filter(Boolean);

        const requestedSet = new Set<string>(
            requestedArray.length > 0 ? requestedArray : activeLeagueIds
        );

        if (requestedSet.has('all')) {
            activeLeagueIds.forEach((id) => requestedSet.add(id));
            requestedSet.delete('all');
        }

        if (requestedSet.has('soccer')) {
            activeSoccerLeagueIds.forEach((id) => requestedSet.add(id));
            requestedSet.delete('soccer');
        }

        const hasLegacySoccer = ['epl', 'laliga', 'seriea', 'bundesliga', 'ligue1', 'mls', 'uefa.champions', 'uefa.europa']
            .some((id) => requestedSet.has(id));
        if (hasLegacySoccer) {
            for (const leagueId of SOCCER_LEAGUE_AUTO_EXPAND) {
                if (activeLeagueIds.includes(leagueId)) {
                    requestedSet.add(leagueId);
                }
            }
        }

        const leaguesRequested = Array.from(requestedSet);

        Logger.info('INGEST_ODDS_START', {
            leagues: leaguesRequested,
            active_configs: leagueMap.size,
            soccer_auto_expand_applied: hasLegacySoccer
        });

        const apiKey = Deno.env.get('ODDS_API_KEY');
        const results = [];

        for (const rawKey of leaguesRequested) {
            try {
                // Decoupled Mapping: Check league_config table first, fallback to rawKey
                const sportKey = leagueMap.get(rawKey) || rawKey;

                debug.sport_requested = rawKey;
                debug.sport_resolved = sportKey;

                const url = `https://api.the-odds-api.com/v4/sports/${sportKey}/odds/?apiKey=${apiKey}&regions=us&markets=h2h,spreads,totals&oddsFormat=american`;

                debug.steps.push({ step: 'fetch_odds_api', url: url.replace(apiKey || '', 'REDACTED') });

                const resp = await fetch(url);
                if (!resp.ok) {
                    const errText = await resp.text();
                    Logger.warn(`Odds API error for ${sportKey}: ${resp.status}`, { error: errText });
                    continue;
                }

                const data = await resp.json();
                debug.steps.push({ step: 'parse_odds_api', count: data.length });

                if (Array.isArray(data) && data.length > 0) {
                    // Pre-fetch all team mappings for this league to provide consistent identity resolution
                    const { data: leagueMappings } = await supabase.from('team_mappings').select('raw_external_name, canonical_name').eq('league_id', rawKey);
                    const mappingCache = new Map<string, string>(leagueMappings?.map((m: any) => [m.raw_external_name, m.canonical_name]) || []);

                    try {
                        const feedUpserts = data.map((event: any) => {
                            const lines = calculateAnchorLines(event.bookmakers || [], event.home_team, event.away_team, sportKey, true, mappingCache);
                            return {
                                external_id: event.id,
                                sport_key: event.sport_key,
                                home_team: event.home_team,
                                away_team: event.away_team,
                                commence_time: event.commence_time,
                                raw_bookmakers: event.bookmakers,
                                best_spread: lines.spread,
                                best_total: lines.total,
                                best_h2h: lines.h2h,
                                is_live: new Date(event.commence_time) <= new Date(),
                                last_updated: new Date().toISOString()
                            };
                        });

                        await supabase.from('market_feeds').upsert(feedUpserts, { onConflict: 'external_id' });
                    } catch (syncErr: any) {
                        Logger.warn('MARKET_FEED_SYNC_FAILED', { error: syncErr.message });
                    }

                    const updates = await syncToMatches(supabase, data, sportKey, rawKey, mappingCache);
                    debug.steps.push({ step: 'sync_db', sport: rawKey, ...updates });
                    results.push({ sport: rawKey, ...updates });
                }
            } catch (err: any) {
                Logger.error(`Error processing ${rawKey}:`, { error: err.message });
            }
        }

        return new Response(JSON.stringify({
            success: true,
            debug,
            results,
            _trace: results.map((r: any) => r.updatedIds || []).flat().slice(0, 5)
        }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        })

    } catch (e: any) {
        Logger.error('FATAL_INGEST_ODDS', { endpoint: 'ingest-odds', error: e.message, stack: e.stack?.substring(0, 500) })
        return new Response(JSON.stringify({
            error: e.message,
            debug
        }), {
            status: 500,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        })
    }
})

// FIX #3: Per-run dedupe for IDENTITY_GAP
const identityGapSeen = new Set<string>();

function logIdentityGapOnce(key: string, payload: any) {
    if (identityGapSeen.has(key)) return;
    identityGapSeen.add(key);
    Logger.warn('IDENTITY_GAP', payload);
}

/**
 * SRE FIX: Ensure a match ID exists in canonical_games before referencing it.
 * Prevents 23503 Foreign Key Violations in entity_mappings.
 */
async function ensureCanonicalGame(supabase: any, match: any) {
    if (!match.id || !match.id.includes('_')) return;

    // Check if already exists
    const { data: existing } = await supabase
        .from('canonical_games')
        .select('id')
        .eq('id', match.id)
        .maybeSingle();

    if (existing) return;

    // Lazy bootstrap of canonical game record
    Logger.info('LAZY_CANONICAL_BOOTSTRAP', { match_id: match.id });
    const { error } = await supabase.from('canonical_games').insert({
        id: match.id,
        league_id: match.league_id,
        sport: match.league_id.includes('soccer') ? 'soccer' :
            match.league_id.includes('basketball') ? 'basketball' :
                match.league_id.includes('americanfootball') ? 'americanfootball' : 'unknown',
        home_team_name: typeof match.home_team === 'string' ? match.home_team : (match.home_team?.displayName || match.home_team?.name),
        away_team_name: typeof match.away_team === 'string' ? match.away_team : (match.away_team?.displayName || match.away_team?.name),
        commence_time: match.start_time,
        status: match.status || 'scheduled'
    });

    if (error && error.code !== '23505') { // Ignore unique violations
        Logger.error('CANONICAL_BOOTSTRAP_FAILED', { match_id: match.id, error: error.message });
    }
}

async function syncToMatches(supabase: any, oddsData: any[], sportKey: string, rawKey: string, mappingCache: Map<string, string>) {
    const canonicalLeague = getCanonicalLeagueId(sportKey)
    const nowMs = Date.now()
    const winStart = new Date(nowMs - 96 * 3600000).toISOString() // -4 days (covers Thursday -> Monday)
    const winEnd = new Date(nowMs + 168 * 3600000).toISOString() // +7 days

    const killThreshold = new Date(nowMs - 4 * 3600000).toISOString()

    // Make kill threshold non-fatal - if schema issue, log and continue
    try {
        const { data: killedMatches, error: killError, count: killCount } = await supabase.from('matches')
            .update({ status: 'STATUS_FINAL' })
            .eq('league_id', canonicalLeague)
            .in('status', ['STATUS_IN_PROGRESS', 'IN_PROGRESS'])
            .not('last_odds_update', 'is', null)
            .lt('last_odds_update', killThreshold)
            .select('id, home_team, away_team')

        if (killCount && killCount > 0) {
            Logger.warn('KILL_THRESHOLD_APPLIED', { league: canonicalLeague, threshold: killThreshold, count: killCount, matches: killedMatches?.map((m: any) => m.id) })
        }
        if (killError) {
            // If status column is genuinely missing or renamed, this will log but not crash
            Logger.warn('KILL_THRESHOLD_QUERY_ERROR', { league: canonicalLeague, error: killError.message, code: killError.code })
        }
    } catch (err: any) {
        Logger.warn('KILL_THRESHOLD_SKIPPED', { league: canonicalLeague, error: err?.message || String(err), reason: 'Non-fatal, continuing ingestion' })
    }

    let { data: activeMatches, error: activeError } = await supabase
        .from('matches')
        .select('id, canonical_id, home_team, away_team, start_time, league_id, current_odds, opening_odds, is_opening_locked, last_odds_update, status, home_score, away_score')
        .eq('league_id', canonicalLeague)
        .gte('start_time', winStart)
        .lte('start_time', winEnd)
        .not('status', 'in', '("STATUS_FINAL","FINAL")')

    if (activeError) {
        Logger.error('ACTIVE_MATCHES_QUERY_FAILED', { league: canonicalLeague, error: activeError.message, code: activeError.code })
        // Fallback: try querying without status if status is the issue
        if (activeError.message.includes('status')) {
            const { data: fallbackMatches } = await supabase
                .from('matches')
                .select('id, canonical_id, home_team, away_team, start_time, league_id, current_odds, opening_odds, is_opening_locked, last_odds_update')
                .eq('league_id', canonicalLeague)
                .gte('start_time', winStart)
                .lte('start_time', winEnd)
            if (fallbackMatches) {
                Logger.info('ACTIVE_MATCHES_FALLBACK_SUCCESS', { league: canonicalLeague, count: fallbackMatches.length })
                activeMatches = fallbackMatches as any[]
            }
        }
    }

    if (!activeMatches?.length) {
        Logger.info('NO_ACTIVE_MATCHES', { league: canonicalLeague, winStart, winEnd })
        return { resolved: 0, failed: 0 }
    }

    const idMap = new Map<string, any>()
    for (const m of activeMatches) idMap.set(m.id, m)

    const extIds = oddsData.map(u => u.id)
    const { data: mappings } = await supabase.from('entity_mappings')
        .select('external_id, canonical_id')
        .eq('provider', 'THE_ODDS_API').in('external_id', extIds)
    const mapLookup = new Map<string, string>()
    mappings?.forEach((m: any) => mapLookup.set(m.external_id, m.canonical_id))

    const bulkUpdates: any[] = []
    const feedCanonicalUpdates: any[] = []
    let resolvedCount = 0
    let failedCount = 0

    for (const event of oddsData) {
        let matchId: string | null = null
        let canonicalId: string | null = null

        const canId = mapLookup.get(event.id)
        if (canId) {
            const m = activeMatches.find((x: any) => x.canonical_id === canId)
            if (m) {
                matchId = m.id
                canonicalId = canId
            }
        }

        if (!matchId) {
            // 1. Resolve Canonical Names from Registry
            const canonicalHome = mappingCache.get(event.home_team) || event.home_team;
            const canonicalAway = mappingCache.get(event.away_team) || event.away_team;

            // 2. Resolve via Registry (Heuristics)
            const resolved = await resolveCanonicalMatch(supabase, canonicalHome, canonicalAway, event.commence_time, sportKey)

            if (resolved) {
                canonicalId = resolved
                const m = activeMatches.find((x: any) => x.id === resolved || x.canonical_id === resolved)
                if (m) {
                    await ensureCanonicalGame(supabase, m);
                    matchId = m.id;

                    // 3. Persistent Healing: If we matched via fallback, save the alias
                    if (event.home_team !== canonicalHome || event.away_team !== canonicalAway) {
                        await supabase.rpc('heal_team_identity', {
                            p_league_id: rawKey,
                            p_raw_name: event.home_team,
                            p_canonical_name: canonicalHome
                        });
                        await supabase.rpc('heal_team_identity', {
                            p_league_id: rawKey,
                            p_raw_name: event.away_team,
                            p_canonical_name: canonicalAway
                        });
                    }
                }

                await supabase.from('entity_mappings').upsert(
                    { canonical_id: resolved, provider: 'THE_ODDS_API', external_id: event.id, discovery_method: 'auto' },
                    { onConflict: 'provider,external_id' }
                )
            } else {
                // 4. Fuzzy Fallback: Try finding the team via Trigram Similarity if registry fails
                const { data: fuzzyHome } = await supabase.rpc('find_canonical_team', {
                    search_name: event.home_team,
                    search_league_id: rawKey,
                    min_similarity: 0.5
                });

                if (fuzzyHome && fuzzyHome.length > 0) {
                    Logger.info('FUZZY_MATCH_POTENTIAL', { external: event.home_team, canonical: fuzzyHome[0].db_name, score: fuzzyHome[0].score });
                }
            }
        }

        if (!matchId) {
            if (event.bookmakers?.length > 0) {
                failedCount++
                const key = `odds_api:${event.id}`;
                logIdentityGapOnce(key, { endpoint: 'ingest-odds', home: event.home_team, away: event.away_team, odds_api_id: event.id, reason: 'No DB match found' });
            }
            continue
        }
        resolvedCount++

        // SIDE EFFECT: Bridge market_feeds for SRE resolution
        if (canonicalId) {
            feedCanonicalUpdates.push({
                external_id: event.id,
                canonical_id: canonicalId
            })
        }

        const existing = idMap.get(matchId)
        const lines = calculateAnchorLines(event.bookmakers || [], event.home_team, event.away_team, sportKey, true, mappingCache)

        // --- DEEP INSTRUMENTATION: Raw Provider Trace ---
        // If it's a live soccer match with a score, log exactly what the API sent for the spread
        if (sportKey.includes('soccer') && existing.home_score + existing.away_score > 0) {
            Logger.info('LIVE_SOCCER_RAW_TRACE', {
                match_id: matchId,
                score: `${existing.home_score}-${existing.away_score}`,
                raw_spread: lines.spread,
                raw_total: lines.total,
                provider: lines.spread?.bookmaker || 'N/A'
            });
        }

        const newOdds = {
            homeSpread: lines.spread?.home?.point ?? null,
            awaySpread: lines.spread?.away?.point ?? null,
            homeSpreadOdds: lines.spread?.home?.price ?? null,
            awaySpreadOdds: lines.spread?.away?.price ?? null,
            total: lines.total?.over?.point ?? null,
            overOdds: lines.total?.over?.price ?? null,
            underOdds: lines.total?.under?.price ?? null,
            homeWin: lines.h2h?.home?.price ?? null,
            awayWin: lines.h2h?.away?.price ?? null,
            drawWin: lines.h2h?.draw?.price ?? null,
            provider: lines.spread?.home?.bookmaker || 'Consensus',
            lastUpdated: new Date().toISOString(),
            isInstitutional: true,
            isLive: true,
            homeScoreAtOdds: existing.home_score,
            awayScoreAtOdds: existing.away_score,
            _debug_raw: {
                spread_point: lines.spread?.home?.point,
                spread_price: lines.spread?.home?.price,
                total_point: lines.total?.over?.point,
                total_price: lines.total?.over?.price,
                as_of: new Date().toISOString(),
                score_at_time: `${existing.home_score}-${existing.away_score}`
            }
        };

        const prev = existing.current_odds;
        const hasMoved = !prev || prev.homeSpread !== newOdds.homeSpread || prev.total !== newOdds.total;
        const lastUpd = new Date(existing.last_odds_update || 0).getTime();

        if (nowMs - lastUpd < 30000 && !hasMoved) continue;

        bulkUpdates.push({
            id: matchId,
            current_odds: newOdds,
            last_odds_update: new Date().toISOString(),
            odds_api_event_id: event.id
        });
    }

    if (bulkUpdates.length) {
        for (const c of chunk(bulkUpdates, 100)) {
            const { error } = await supabase.rpc('bulk_update_match_odds', { payload: c })
            if (error) {
                Logger.error('BULK_UPDATE_FAILED', { endpoint: 'ingest-odds', rpc: 'bulk_update_match_odds', batch_size: c.length, error: error.message, hint: error.hint })
            } else {
                Logger.info('BULK_UPDATE_SUCCESS', { endpoint: 'ingest-odds', batch_size: c.length })
            }
        }
    }

    if (feedCanonicalUpdates.length) {
        await supabase.from('market_feeds').upsert(feedCanonicalUpdates, { onConflict: 'external_id' })
    }

    return {
        resolved: resolvedCount,
        failed: failedCount,
        updated: bulkUpdates.length,
        updatedIds: bulkUpdates.map(u => u.id)
    }
}

// 6-tier sharp book priority: lower = sharper.
// Pinnacle is the global benchmark, Circa the US sharp benchmark.
const SHARP_BOOK_PRIORITY: Record<string, number> = {
    'pinnacle': 1,
    'circa': 2, 'circa sports': 2,
    'betonlineag': 3, 'betonline.ag': 3, 'betonline': 3,
    'draftkings': 4, 'fanduel': 4,
    'betmgm': 5,
};
const MAX_BOOK_TIER = 6; // everything else

function getBookTier(title: string): number {
    const key = (title || '').toLowerCase().replace(/[^a-z.]/g, '');
    return SHARP_BOOK_PRIORITY[key] ?? MAX_BOOK_TIER;
}

function calculateAnchorLines(bookmakers: any[], homeTeam: string, awayTeam: string, sport: string, isLive: boolean, mappingCache?: Map<string, string>) {
    const best: any = { h2h: null, spread: null, total: null }
    const bestTier: any = { h2h: MAX_BOOK_TIER + 1, spread: MAX_BOOK_TIER + 1, total: MAX_BOOK_TIER + 1 }

    const resolve = (name: string) => mappingCache?.get(name) || name;
    const normHome = normalizeTeam(resolve(homeTeam))
    const normAway = normalizeTeam(resolve(awayTeam))

    for (const book of bookmakers) {
        const tier = getBookTier(book.title)
        for (const market of book.markets) {
            const type = market.key === 'h2h' ? 'h2h' : (market.key === 'spreads' ? 'spread' : (market.key === 'totals' ? 'total' : null))
            if (!type) continue

            // Accept this book only if it's at least as sharp as current best
            if (tier < bestTier[type]) {
                bestTier[type] = tier
                if (type === 'h2h') {
                    const home = market.outcomes.find((o: any) => normalizeTeam(resolve(o.name)) === normHome)
                    const away = market.outcomes.find((o: any) => normalizeTeam(resolve(o.name)) === normAway)
                    const draw = market.outcomes.find((o: any) => o.name === 'Draw' || o.name === 'Tie' || o.name === 'X')
                    best.h2h = { home, away, draw, bookmaker: book.title }
                } else if (type === 'spread') {
                    const home = market.outcomes.find((o: any) => normalizeTeam(resolve(o.name)) === normHome)
                    const away = market.outcomes.find((o: any) => normalizeTeam(resolve(o.name)) === normAway)

                    // Fallback: If name matching fails but there are exactly 2 outcomes, use order
                    let resolvedHome = home;
                    let resolvedAway = away;
                    if (!resolvedHome && !resolvedAway && market.outcomes.length === 2 && !sport.includes('soccer')) {
                        resolvedHome = market.outcomes[0];
                        resolvedAway = market.outcomes[1];
                    }

                    best.spread = { home: resolvedHome, away: resolvedAway, bookmaker: book.title }
                } else if (type === 'total') {
                    const over = market.outcomes.find((o: any) => o.name === 'Over')
                    const under = market.outcomes.find((o: any) => o.name === 'Under')
                    best.total = { over, under, bookmaker: book.title }
                }
            }
        }
    }
    return best
}

function chunk(arr: any[], size: number) {
    const res = []
    for (let i = 0; i < arr.length; i += size) res.push(arr.slice(i, i + size))
    return res
}

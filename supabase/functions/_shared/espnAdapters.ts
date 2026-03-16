
import { Team, Sport, MatchOdds, MatchEvent, StatItem, AdvancedMetrics, TeamPlayerStats, MatchLeader, MomentumPoint, MatchContext, Situation, Drive, LastPlay } from './types.ts';

export const Safe = {
    string: (val: any) => val ? String(val) : undefined,
    number: (val: any, def: number = 0) => {
        const n = parseFloat(val);
        return isNaN(n) ? def : n;
    },
    bool: (val: any) => !!val,
    score: (val: any) => {
        const n = parseInt(val);
        return isNaN(n) ? 0 : n;
    }
};

const asId = (value: any): string => {
    if (value === null || value === undefined) return '';
    return String(value);
};

export const EspnAdapters = {
    Team: (competitor: any, sport: Sport): Team => {
        if (!competitor) return { id: '0', name: 'Unknown', shortName: 'UNK', logo: '', score: 0 } as Team;

        // Tennis uses competitor.athlete (individual), other sports use competitor.team
        const entity = competitor.team || competitor.athlete || {};

        let record = undefined;
        const rawRecords = competitor.records || competitor.record;
        if (Array.isArray(rawRecords) && rawRecords.length > 0) {
            const totalRecord = rawRecords.find((r: any) => r.type === 'total') || rawRecords[0];
            record = totalRecord?.summary;
        }
        return {
            id: Safe.string(entity.id) || '0',
            name: Safe.string(entity.displayName || entity.fullName) || 'Unknown',
            shortName: Safe.string(entity.shortDisplayName || entity.displayName || entity.abbreviation) || 'UNK',
            abbreviation: Safe.string(entity.abbreviation),
            logo: Safe.string(entity.logo || entity.headshot?.href || entity.logos?.[0]?.href) || '',
            color: Safe.string(entity.color),
            record: Safe.string(record),
            rank: Safe.number(competitor.curatedRank?.current, 99) === 99 ? undefined : Safe.number(competitor.curatedRank?.current),
            score: Safe.score(competitor.score),
            // Tennis: player's country flag
            flag: Safe.string(entity.flag?.href),
            linescores: competitor.linescores?.map((ls: any) => {
                const rawVal = ls.value ?? ls.score ?? ls.displayValue;
                return {
                    value: (rawVal !== undefined && rawVal !== null && rawVal !== '') ? Number(rawVal) : undefined,
                    label: Safe.string(ls.label) || '',
                    period: Safe.number(ls.period),
                    // Tennis-specific: tiebreak score and set winner
                    tiebreak: ls.tiebreak !== undefined ? Safe.number(ls.tiebreak) : undefined,
                    winner: ls.winner === true ? true : ls.winner === false ? false : undefined,
                };
            })
        };
    },
    // Enhanced Odds Adapter for Closing Lines
    Odds: (competition: any, pickcenter?: any[]): MatchOdds => {
        let result: MatchOdds = { provider: 'Consensus', hasOdds: false };

        // 1. Try PickCenter (Best for Closing Lines and DraftKings)
        if (pickcenter && pickcenter.length > 0) {
            const PRIORITY = ['draftkings', 'draft kings', 'fanduel', 'william hill', 'williamhill', 'betmgm', 'pinnacle', 'consensus'];

            let primary = null;
            for (const key of PRIORITY) {
                primary = pickcenter.find((p: any) => p.provider?.name?.toLowerCase().includes(key));
                if (primary) break;
            }

            if (!primary) primary = pickcenter[0];

            if (primary) {
                result.hasOdds = true;
                result.provider = primary.provider?.name || 'Consensus';
                result.spread = primary.details; // e.g. "BUF -3.0"
                result.overUnder = primary.overUnder; // e.g. 48.5

                // Extract ML from PickCenter
                if (primary.homeTeamOdds?.moneyLine) result.homeWin = primary.homeTeamOdds.moneyLine;
                if (primary.awayTeamOdds?.moneyLine) result.awayWin = primary.awayTeamOdds.moneyLine;
                if (primary.drawOdds?.moneyLine) result.draw = primary.drawOdds.moneyLine;

                // Extract DK Link if it's available
                const isDK = primary.provider?.name?.toLowerCase().includes('draftkings') || primary.provider?.name?.toLowerCase().includes('draft kings');
                if (isDK && primary.links) {
                    const betLink = primary.links.find((l: any) => l.rel?.includes('bets') || l.href?.includes('draftkings.com'));
                    if (betLink) result.draftkingsLink = betLink.href;
                }
            }
        }

        // 2. Fallback to Competition Odds (Live/Opening)
        if (competition?.odds && competition.odds[0]) {
            const oddsData = competition.odds[0];

            if (!result.hasOdds) {
                result.hasOdds = true;
                result.provider = oddsData.provider?.name || 'Consensus';
                if (oddsData.details) result.spread = oddsData.details;
                if (oddsData.overUnder) result.overUnder = oddsData.overUnder;
            }

            if (!result.homeWin) {
                if (oddsData.moneyline) {
                    result.homeWin = oddsData.moneyline.home?.current?.odds ?? oddsData.moneyline.home?.open?.odds;
                    result.awayWin = oddsData.moneyline.away?.current?.odds ?? oddsData.moneyline.away?.open?.odds;
                    result.draw = oddsData.moneyline.draw?.current?.odds ?? oddsData.moneyline.draw?.open?.odds;
                } else {
                    if (oddsData.homeTeamOdds?.moneyLine) result.homeWin = oddsData.homeTeamOdds.moneyLine;
                    if (oddsData.awayTeamOdds?.moneyLine) result.awayWin = oddsData.awayTeamOdds.moneyLine;
                    if (oddsData.drawOdds?.moneyLine) result.draw = oddsData.drawOdds.moneyLine;
                }
            }
        }

        // Normalize: ensure moneylineHome/moneylineAway are always set alongside homeWin/awayWin
        if (result.homeWin !== undefined && (result as any).moneylineHome === undefined) {
            (result as any).moneylineHome = result.homeWin;
        }
        if (result.awayWin !== undefined && (result as any).moneylineAway === undefined) {
            (result as any).moneylineAway = result.awayWin;
        }

        return result;
    },
    Events: (data: any, sport: Sport): MatchEvent[] => {
        const scoringPlays = data.scoringPlays;
        if (!Array.isArray(scoringPlays)) return [];
        return scoringPlays.map((play: any) => ({
            id: play.id,
            time: play.clock?.displayValue || '',
            period: play.period?.number,
            type: 'score',
            teamId: play.team?.id,
            scoreValue: play.scoringType?.pointValue,
            text: play.text,
            clock: play.clock?.displayValue
        }));
    },
    Stats: (data: any, sport: Sport): StatItem[] => {
        const boxscore = data.boxscore;
        const competitors = data.header?.competitions?.[0]?.competitors;
        if (!boxscore || !boxscore.teams || !competitors) return [];
        const homeComp = competitors.find((c: any) => c.homeAway === 'home');
        const awayComp = competitors.find((c: any) => c.homeAway === 'away');
        if (!homeComp || !awayComp) return [];
        const homeCompId = asId(homeComp.id || homeComp?.team?.id);
        const awayCompId = asId(awayComp.id || awayComp?.team?.id);

        const byHomeAway = (side: 'home' | 'away') =>
            boxscore.teams.find((t: any) => t?.homeAway === side);

        const byCompId = (id: string) =>
            boxscore.teams.find((t: any) => asId(t?.team?.id || t?.id) === id);

        const homeStatsObj = byHomeAway('home') || byCompId(homeCompId);
        const awayStatsObj = byHomeAway('away') || byCompId(awayCompId);
        if (!homeStatsObj?.statistics || !awayStatsObj?.statistics) return [];
        return homeStatsObj.statistics.map((hStat: any) => {
            const aStat = awayStatsObj.statistics.find((s: any) => s.name === hStat.name);
            if (!aStat) return null;
            return {
                label: hStat.label || hStat.name,
                homeValue: hStat.displayValue,
                awayValue: aStat.displayValue
            };
        }).filter((s: any) => s !== null);
    },
    AdvancedMetrics: (data: any, sport: Sport): AdvancedMetrics | undefined => {
        const boxscore = data.boxscore;
        const competitors = data.header?.competitions?.[0]?.competitors;
        if (!boxscore || !boxscore.teams || boxscore.teams.length < 2 || !competitors) return undefined;

        const homeComp = competitors.find((c: any) => c.homeAway === 'home');
        const awayComp = competitors.find((c: any) => c.homeAway === 'away');

        const extract = (teamData: any) => {
            const stats = teamData.statistics || [];
            const metrics: Record<string, number> = {};

            const findStat = (name: string) => stats.find((s: any) => s.name === name)?.displayValue;

            if (sport === Sport.NFL || sport === Sport.COLLEGE_FOOTBALL) {
                metrics.yardsPerPlay = Safe.number(findStat('yardsPerPlay'));
                metrics.yardsPerPass = Safe.number(findStat('yardsPerPass'));
                metrics.yardsPerRush = Safe.number(findStat('yardsPerRushAttempt'));
                const compTotal = findStat('completionAttempts');
                if (compTotal && compTotal.includes('/')) {
                    const [made, att] = compTotal.split('/').map(n => Safe.number(n));
                    metrics.completionPct = (made / Math.max(att, 1)) * 100;
                }
                metrics.thirdDownEff = Safe.number(findStat('thirdDownEff')?.split('/')?.[0]) / Math.max(Safe.number(findStat('thirdDownEff')?.split('/')?.[1]), 1) * 100;
            } else if (sport === Sport.NBA || sport === Sport.COLLEGE_BASKETBALL || sport === Sport.BASKETBALL) {
                metrics.fgPct = Safe.number(findStat('fieldGoalsAttempted')) > 0 ? (Safe.number(findStat('fieldGoalsMade')) / Safe.number(findStat('fieldGoalsAttempted')) * 100) : 0;
                metrics.threePtPct = Safe.number(findStat('threePointFieldGoalsAttempted')) > 0 ? (Safe.number(findStat('threePointFieldGoalsMade')) / Safe.number(findStat('threePointFieldGoalsAttempted')) * 100) : 0;
                metrics.rebounds = Safe.number(findStat('totalRebounds'));
                metrics.turnovers = Safe.number(findStat('turnovers'));
            }

            return metrics;
        };

        return {
            home: extract(boxscore.teams.find((t: any) => t.homeAway === 'home' || (t.team?.id || t.id) === homeComp?.id) || boxscore.teams[0]),
            away: extract(boxscore.teams.find((t: any) => t.homeAway === 'away' || (t.team?.id || t.id) === awayComp?.id) || boxscore.teams[1])
        };
    },
    RecentPlays: (data: any, sport: Sport): any[] => {
        // Football / some baseball feeds: nested on current drive
        const drivePlays = data?.drives?.current?.plays;
        if (Array.isArray(drivePlays) && drivePlays.length > 0) {
            return drivePlays.slice(-5).map((p: any) => ({
                id: Safe.string(p.id),
                clock: Safe.string(p.clock?.displayValue),
                period: Safe.number(p.period?.number ?? p.period),
                text: Safe.string(p.text || p.shortText || p.description),
                type: Safe.string(p.type?.text),
                teamId: Safe.string(p.team?.id)
            }));
        }

        // Basketball/Hockey/Soccer often expose generic plays array
        if (Array.isArray(data?.plays) && data.plays.length > 0) {
            return data.plays.slice(-5).map((p: any) => ({
                id: Safe.string(p.id),
                clock: Safe.string(p.clock?.displayValue || p.clock?.value),
                period: Safe.number(p.period?.number ?? p.period),
                text: Safe.string(p.text || p.shortText || p.description),
                type: Safe.string(p.type?.text),
                teamId: Safe.string(p.team?.id)
            }));
        }

        // Soccer fallback: key events + commentary (when plays are absent)
        if (sport === Sport.SOCCER) {
            const keyEvents = Array.isArray(data?.keyEvents) ? data.keyEvents.slice(-5) : [];
            if (keyEvents.length > 0) {
                return keyEvents.map((e: any) => ({
                    id: Safe.string(e.id),
                    clock: Safe.string(e.clock?.displayValue),
                    period: Safe.number(e.period?.number ?? e.period),
                    text: Safe.string(e.text || e.shortText || e.description),
                    type: Safe.string(e.type?.text),
                    teamId: Safe.string(e.team?.id)
                }));
            }
            if (Array.isArray(data?.commentary) && data.commentary.length > 0) {
                return data.commentary.slice(-5).map((c: any) => ({
                    id: Safe.string(c.id),
                    clock: Safe.string(c.clock?.displayValue),
                    period: Safe.number(c.period?.number ?? c.period),
                    text: Safe.string(c.text || c.shortText || c.description),
                    type: Safe.string(c.type?.text),
                    teamId: Safe.string(c.team?.id)
                }));
            }
        }

        return [];
    },
    PlayerStats: (data: any): TeamPlayerStats[] => {
        const players = data.boxscore?.players;
        if (!Array.isArray(players)) return [];
        return players.map((teamPlayers: any) => ({
            teamId: teamPlayers.team?.id,
            categories: teamPlayers.statistics.map((cat: any) => ({
                name: cat.name,
                displayName: cat.text || cat.label || cat.name,
                labels: cat.labels || cat.descriptions || [],
                athletes: cat.athletes.map((a: any) => ({
                    id: a.athlete?.id,
                    name: a.athlete?.displayName,
                    shortName: a.athlete?.shortName,
                    position: a.athlete?.position?.abbreviation,
                    headshot: a.athlete?.headshot?.href,
                    stats: a.stats
                }))
            }))
        }));
    },
    Leaders: (data: any): MatchLeader[] => {
        const leaders = data.leaders;
        if (!Array.isArray(leaders)) return [];
        return leaders.map((cat: any) => ({
            name: cat.name,
            displayName: cat.displayName,
            leaders: cat.leaders?.map((l: any) => ({
                displayValue: l.displayValue,
                value: l.value,
                athlete: {
                    id: l.athlete?.id,
                    fullName: l.athlete?.fullName,
                    displayName: l.athlete?.displayName,
                    shortName: l.athlete?.shortName,
                    headshot: l.athlete?.headshot?.href,
                    position: { abbreviation: l.athlete?.position?.abbreviation }
                },
                team: { id: l.team?.id }
            })) || []
        }));
    },
    Momentum: (data: any): MomentumPoint[] | undefined => {
        const winProbability = data.winprobability;
        if (!Array.isArray(winProbability)) return undefined;
        return winProbability.map((wp: any, idx: number) => ({
            minute: Safe.number(wp.playOrder || idx),
            value: (Safe.number(wp.homeWinPercentage) * 100) - 50,
            winProb: Safe.number(wp.homeWinPercentage) * 100
        }));
    },
    Context: (data: any): MatchContext => {
        const gameInfo = data.gameInfo;
        const venueData = gameInfo?.venue;
        const weatherData = gameInfo?.weather;
        return {
            weather: weatherData ? {
                temp: Safe.string(weatherData.temperature),
                condition: Safe.string(weatherData.displayValue || weatherData.condition)
            } : undefined,
            venue: venueData ? {
                name: Safe.string(venueData.fullName),
                city: Safe.string(venueData.address?.city),
                state: Safe.string(venueData.address?.state),
                indoor: Safe.bool(venueData.indoor)
            } : undefined,
            attendance: gameInfo?.attendance ? Safe.number(gameInfo.attendance) : undefined,
            broadcasts: data.header?.competitions?.[0]?.broadcasts?.map((b: any) => ({
                market: Safe.string(b.market),
                names: Array.isArray(b.names) ? b.names.map(n => String(n)) : []
            })) || []
        };
    },
    Predictor: (data: any) => {
        const predictor = data.predictor;
        if (!predictor) return undefined;
        return {
            homeTeamChance: Safe.number(predictor.homeTeam?.gameChance || predictor.homeTeam?.chance?.value),
            awayTeamChance: Safe.number(predictor.awayTeam?.gameChance || predictor.awayTeam?.chance?.value),
            homeTeamLine: Safe.string(predictor.homeTeam?.displayLine),
            awayTeamLine: Safe.string(predictor.awayTeam?.displayLine),
        };
    },
    Situation: (data: any): Situation | undefined => {
        // High-Precision State Discovery:
        // ESPN bifurcates game situation data across multiple JSON paths depending on the exact sub-feed (Live vs Static)
        const situationData = data.situation
            || data.header?.competitions?.[0]?.situation
            || data.competitions?.[0]?.situation
            || data.drives?.current?.plays?.slice(-1)[0]?.situation
            || data.drives?.current?.plays?.slice(-1)[0]; // Fallback to raw play if situation is nested flat

        if (!situationData) return undefined;

        // Note: Some ESPN endpoints use 'yardLine', others use 'yardline'.
        // If falling back to a raw Play object, check 'end.yardLine' (spot after play) or 'start.yardLine'
        const rawYard = situationData.yardLine
            ?? situationData.yardline
            ?? situationData.location?.yardLine
            ?? situationData.end?.yardLine
            ?? situationData.end?.yardline;

        return {
            // Football Logic
            down: Safe.number(situationData.down),
            distance: Safe.number(situationData.distance),
            yardLine: Safe.number(rawYard),
            downDistanceText: Safe.string(situationData.downDistanceText || situationData.shortDownDistanceText),
            isRedZone: Safe.bool(situationData.isRedZone || (Safe.number(rawYard) >= 80)), // Auto-detect RedZone if flag is missing

            // Baseball (MLB)
            balls: Safe.number(situationData.balls),
            strikes: Safe.number(situationData.strikes),
            outs: Safe.number(situationData.outs),
            onFirst: Safe.bool(situationData.onFirst),
            onSecond: Safe.bool(situationData.onSecond),
            onThird: Safe.bool(situationData.onThird),

            // NBA / NHL
            isBonus: Safe.bool(situationData.isBonus),
            isPowerPlay: Safe.bool(situationData.isPowerPlay),

            // Global Possession Anchor
            possessionId: Safe.string(situationData.possession || situationData.possessionId || situationData.team?.id),
            possessionText: Safe.string(situationData.possessionText),
        };
    },
    Drive: (data: any): Drive | undefined => {
        const driveData = data.drives?.current;
        if (!driveData) return undefined;

        // DATA HARDENING: ESPN occasionally fails to update the 'plays' or 'yards' counters in the JSON 
        // while the 'description' string (e.g., "4 plays, 24 yards, 1:22") is correct.
        const description = Safe.string(driveData.description) || '';
        let plays = Safe.number(driveData.plays);
        let yards = Safe.number(driveData.yards);

        if (plays === 0 && description.toLowerCase().includes('play')) {
            const match = description.match(/(\d+)\s*play/i);
            if (match) plays = parseInt(match[1]);
        }
        if (yards === 0 && description.toLowerCase().includes('yard')) {
            const match = description.match(/(\d+)\s*yard/i);
            if (match) yards = parseInt(match[1]);
        }

        return {
            description,
            result: Safe.string(driveData.result),
            yards,
            plays,
            timeElapsed: Safe.string(driveData.timeElapsed?.displayValue),
            teamId: Safe.string(driveData.team?.id),
            startYardLine: Safe.number(driveData.start?.yardLine ?? driveData.start?.yardline)
        };
    },
    LastPlay: (data: any): LastPlay | undefined => {
        let lastPlayData = data.drives?.current?.plays?.slice(-1)[0] || data.lastPlay || data.situation?.lastPlay;
        if (!lastPlayData && Array.isArray(data.plays) && data.plays.length > 0) lastPlayData = data.plays[data.plays.length - 1];
        if (!lastPlayData) return undefined;
        return {
            id: Safe.string(lastPlayData.id),
            text: Safe.string(lastPlayData.text),
            clock: Safe.string(lastPlayData.clock?.displayValue),
            type: Safe.string(lastPlayData.type?.text),
            statYardage: Safe.number(lastPlayData.statYardage),
            probability: lastPlayData.probability ? { homeWinPercentage: Safe.number(lastPlayData.probability.homeWinPercentage) } : undefined
        };
    }
};

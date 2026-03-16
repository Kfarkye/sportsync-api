
// =============================================================================
// ENGINE TYPES
// =============================================================================

import type { Match, Sport, Team, MarketEfficiency } from "./index.ts";

export interface WeatherInfo {
    wind_speed: string | number;
    temp?: string | number;
    condition?: string;
}

export interface TeamMetadata {
    srs?: string | number;
    abbreviation?: string;
}

// Flexible container for raw stats to avoid 'any' while handling dirty feeds
export interface UnifiedStatContainer {
    [key: string]: any;
    statistics?: Array<{ name?: string; label?: string; value?: string | number }>;
}


export interface ExtendedMatch extends Match {
    // Dynamic Sport Situation
    situation?: {
        possessionText?: string;
        inningHalf?: string;
        outs?: number;
        balls?: number;
        strikes?: number;
        onFirst?: boolean;
        onSecond?: boolean;
        onThird?: boolean;
        isPowerPlay?: boolean; // Added for Hockey
    };

    // Venue Context
    venue?: {
        name?: string;
        is_indoor?: boolean;
        location?: string;
    };

    // Derived/Augmented Data
    market_efficiency?: MarketEfficiency;
    public_betting_pct?: number;

    // Team Metadata
    homeTeam: Team & TeamMetadata;
    awayTeam: Team & TeamMetadata;

    // Weather
    weather_info?: WeatherInfo;
    weather_forecast?: WeatherInfo;

    // Stats
    homeTeamStats?: UnifiedStatContainer;
    awayTeamStats?: UnifiedStatContainer;
}

export type TeamEfficiencyMatrix =
    | {
        sport_type: "BASKETBALL";
        home: { ortg: number; pace: number; efg: number };
        away: { ortg: number; pace: number; efg: number };
        context: string;
    }
    | {
        sport_type: "SOCCER";
        home: { xg: number; xga: number; ppda: number };
        away: { xg: number; xga: number; ppda: number };
        context: string;
    }
    | {
        sport_type: "HOCKEY";
        home: { xg_rate: number; sog: number; projected_contribution: number };
        away: { xg_rate: number; sog: number; projected_contribution: number };
        global: { market_baseline: number; blended_rate: number; is_tied_decay: boolean; is_en_risk: boolean };
        context: string;
    }
    | {
        sport_type: "FOOTBALL";
        home: { ppd: number; pace: number; srs: number };
        away: { ppd: number; pace: number; srs: number };
        home_drives: number;
        away_drives: number;
        context: string;
    }
    | {
        sport_type: "BASEBALL";
        home: { whip: number; pace: number };
        away: { whip: number; pace: number };
        context: string;
    }
    | {
        sport_type: "GENERIC";
        home: { pace: number };
        away: { pace: number };
        context: string;
    };

export type FairTotalActive = {
    status: "ACTIVE";
    fair_total: number;
    p10: number;
    p90: number;
    variance_sd: number;
    regime: "NORMAL" | "CHAOS" | "BLOWOUT" | "BACKDOOR" | "HURRY_UP" | "KILL_CLOCK";
    pace_multiplier: number;
    push_risk?: boolean;
    // v5.1 Cal Poly Variance Extensions
    variance_flags?: {
        blowout: boolean;
        foul_trouble: boolean;
        endgame: boolean;
        power_play_decay?: boolean; // v5.9 Trace
    };
    range_band?: {
        low: number;
        high: number;
    };
};

export type FairTotalNoBet = {
    status: "NO_BET";
    reason: "Time Invalid or Expired" | "Critical: Total is Invalid" | "Calculation Error" | "Game Final";
};

export type FairTotalResult = FairTotalActive | FairTotalNoBet;

export type TrenchAnalytics =
    | {
        type: "BASEBALL";
        home_pitching: { whip: number };
        away_pitching: { whip: number };
    }
    | {
        type: "FOOTBALL";
        home_ol: { rush_grade: number; srs_proxy: number };
        away_dl: { rush_def_grade: number };
    }
    | {
        type: "GENERIC";
        context: string;
    };

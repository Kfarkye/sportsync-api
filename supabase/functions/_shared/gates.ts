// ============================================================================
// SHARED SYSTEM GATES (SSOT)
// Edit in packages/shared/src/gates.ts and run `npm run sync:shared`.
// ============================================================================

// =============================================================================
// SHARED SYSTEM GATES (Deno-Compatible)
// =============================================================================

export let SYSTEM_GATES = {
    MAX_PERMISSIBLE_EDGE: 0.075,
    EARLY_GAME_THRESHOLD: 0.3,

    // Market Signals
    MARKET: {
        PUBLIC_BIAS_PCT: 75,
        REVERSE_MOVE_PCT: 70, // "Trap" threshold
        SHARP_STEAM_PCT: 40,  // "Steam" threshold (Low public support, high movement)
        DEAD_NUMBERS_NFL: [37, 41, 44], // Low frequency total landing spots
    },

    // Football Baselines (Bifurcated)
    NFL: {
        AVG_DRIVES_PER_GAME: 22.0, // Adjusted to 2024 standards
        SEC_PER_DRIVE_STD: 155,    // Standard Pace
        SEC_PER_DRIVE_HURRY: 100,  // Trailing late (< 1 score)
        SEC_PER_DRIVE_MILK: 180,   // Leading late (> 2 scores)
        KEY_NUMBERS: [3, 7, 10, 14],
        KEY_TOTALS: [37, 41, 43, 44, 47, 51], // Common total landing spots
        GARBAGE_TIME_DIFF: 24,
        MIN_DRIVES_OBSERVED: 4, // Gates PPD logic until sample size exists
    },
    NCAAF: {
        AVG_DRIVES_PER_GAME: 26.5, // Higher tempo / stop clock rules
        SEC_PER_DRIVE_STD: 130,
        SEC_PER_DRIVE_HURRY: 90,
        SEC_PER_DRIVE_MILK: 160,
        KEY_NUMBERS: [3, 7, 10, 14, 17, 21],
        GARBAGE_TIME_DIFF: 28,
        MIN_DRIVES_OBSERVED: 6, // Need more sample in college due to variance
    },

    NBA: {
        BASELINE_PACE: 100.5,       // Updated to 2024 NBA average (~100.5 poss/game)
        BASELINE_PPM: 4.75,         // Modern NBA scoring rate (~228 pts/game)
        CRUNCH_TIME_SEC: 120,
        FOUL_GAME_DIFF: 8,
        BLOWOUT_DIFF: 22,
        MAX_FRICTION_PTS: 4.5,
        // v5.1 Logic Kernel Constants
        ACTIONABLE_EDGE: 3.5, // High fidelity threshold
        BLOWOUT_SCALAR: 0.90, // Brake on remaining points
        FOUL_ADDER: 3.0,      // Tax
        ENDGAME_ADDER: 6.0,   // Lift
        ENDGAME_START_MIN: 42,
    },
    NCAAB: {
        BASELINE_PACE: 68.0,
        MIN_PACE: 55.0,
        MAX_PACE: 85.0,
        BLOWOUT_DIFF: 16,
        BLOWOUT_SCALAR: 0.85,
        ENDGAME_START_MIN: 36,
        ENDGAME_ADDER: 3,
        KEY_TOTALS: [128, 133, 137, 141, 144, 147],
    },
    WIND_THRESHOLD_MPH: 15,
    WIND_IMPACT_POINTS: -1.5,

    // Baseball
    MLB: {
        SEC_PER_INNING: 1020,
        SEC_PER_OUT: 170,
        EXTRA_INNING_RUNS: 1.5, // "Ghost Runner" expectancy per full inning
    },

    // Soccer
    SOCCER_REG_SECONDS: 5400,
    SOCCER_HALF_SECONDS: 2700,
    SOCCER_MAX_STOPPAGE: 720,

    // Hockey Sharp Config
    NHL: {
        SOG_CONVERSION_AVG: 0.096,
        MIN_EVENTS_TRUST: 15,
        TIED_DECAY_MULT: 0.75,
        EN_INJECTION_1G: 0.85,
        EN_INJECTION_2G: 0.70,      // Increased from 0.45: Modern 2-goal deficit pulls are more productive/dangerous
        P3_INFLATION: 1.25,
        PROACTIVE_EN_WEIGHT: 0.45,
    },

    // v6.7: Data Integrity Gates (Hallucination Prevention)
    INTEGRITY: {
        MAX_PPM_BASKETBALL: 10,  // Impossible pace threshold (normal ~5-6)
        MAX_PPM_FOOTBALL: 6,     // Impossible pace threshold (normal ~2-3)
        MAX_PPM_HOCKEY: 4,       // Impossible pace threshold (normal ~1.5-2)
    },
};

export const updateSystemGates = (overrides: any) => {
    if (overrides) {
        SYSTEM_GATES = { ...SYSTEM_GATES, ...overrides };
    }
};

export const REGEX = {
    FINAL: /\b(final|ft|f|full\s*time|ended|postponed|cancelled|canceled|suspended|abandoned|delayed|completed|post|official|terminated)\b/i,
    SOCCER_HT: /\b(ht|half\s*time|halftime|break)\b/i,
    CLOCK_MMSS: /(\d+)\s*:\s*(\d{1,2})(?!.*\d+\s*:\s*\d{1,2})/,
    STAT_DASH: /^(\d+(\.\d+)?)\s*[-/]\s*(\d+(\.\d+)?)$/,
    STAT_OF: /^(\d+(\.\d+)?)\s*(of)\s*(\d+(\.\d+)?)$/,
    CLEAN_STAT: /[,%]|mph|km\/h|kts/gi,
    POWER_PLAY: /\b(pp|power\s*play|man\s*advantage|5\s*v\s*4|5\s*on\s*4|4\s*on\s*3)\b/i
};

// ============================================================================
// SHARED TYPES (SSOT)
// Edit in packages/shared/src/types and run `npm run sync:shared`.
// ============================================================================

export enum Sport {
  NBA = 'NBA',
  NFL = 'NFL',
  BASEBALL = 'BASEBALL',
  HOCKEY = 'HOCKEY',
  SOCCER = 'SOCCER',
  COLLEGE_FOOTBALL = 'COLLEGE_FOOTBALL',
  COLLEGE_BASKETBALL = 'COLLEGE_BASKETBALL',
  WNBA = 'WNBA',
  TENNIS = 'TENNIS',
  GOLF = 'GOLF',
  MMA = 'MMA',
  BASKETBALL = 'BASKETBALL'
}

export enum MatchStatus {
  SCHEDULED = 'SCHEDULED',
  LIVE = 'LIVE',
  FINISHED = 'FINISHED',
  POSTPONED = 'POSTPONED',
  CANCELLED = 'CANCELLED',
  HALFTIME = 'HALFTIME'
}

export interface RankingItem {
  rank: number;
  team: {
    id: string;
    name: string;
    logo: string;
    record?: string;
    color?: string;
  };
  trend: number; // 0 = same, positive = up, negative = down
  points?: number;
  firstPlaceVotes?: number;
}

// --- REF TOOL TYPES ---
export interface RefInfo {
  name: string;
  description?: string;
}

export interface RefGame {
  id: string;
  date: string;
  homeTeam: string;
  awayTeam: string;
  time: string;
  location?: string;
  projectedLine?: string;
  referees: RefInfo[];
  rawSearchData?: string;
}

export interface RefAnalysis {
  crewName: string;
  biasScore: number;
  overUnderTendency: number;
  homeTeamCompatibility: number;
  awayTeamCompatibility: number;
  matchupNotes: string;
  keyInsights: string[];
  recommendation: string;
  confidence: number;
  sources: string[];
}
// ---------------------

export type PropBetType =
  | 'points' | 'rebounds' | 'assists' | 'steals' | 'blocks'
  | 'threes_made' | 'pra' | 'pr' | 'ra' | 'pa'
  | 'passing_yards' | 'rushing_yards' | 'receiving_yards'
  | 'receptions' | 'tackles' | 'sacks' | 'hits'
  | 'shots_on_goal' | 'goals' | 'saves'
  | 'custom';

export type PropResult = 'pending' | 'won' | 'lost' | 'push' | 'void';

export interface PlayerPropBet {
  id: string;
  userId: string;
  matchId?: string; // Optional link to Match
  parlayId?: string; // Optional grouping

  eventDate: string;
  league: string;
  team?: string;
  opponent?: string;
  playerName: string;
  playerId?: string;
  headshotUrl?: string;

  betType: PropBetType;
  marketLabel?: string;
  side: 'over' | 'under' | 'yes' | 'no' | string;
  lineValue: number;

  sportsbook: string;
  oddsAmerican: number;
  oddsDecimal?: number;
  stakeAmount: number;
  potentialPayout?: number;
  impliedProbPct?: number;

  result: PropResult;
  resultValue?: number; // The actual stat value (e.g. 25 pts)
  settledAt?: string;
  settledPnl?: number;

  // Expert / Line Tracking Fields
  openLine?: number;
  currentLine?: number;
  lineMovement?: number;
  impliedProb?: number;
  clv?: number;
  validFrom?: string;
  validTo?: string;
  confidenceScore?: number;
  sourceAttribution?: string;
  provider?: string;

  notes?: string;
  createdAt: string;
  updatedAt: string;
}

export interface Linescore {
  value?: number;
  label?: string;
  period?: number;
  tiebreak?: number;  // Tennis: tiebreak score (e.g., 7 for "7-6(7)")
  winner?: boolean;   // Tennis: whether this set was won
}

export interface RecentFormOpponent {
  id?: string;
  name?: string;
  shortName?: string;
  logo?: string;
  score?: string | number;
}

export interface RecentFormGame {
  id?: string;
  date?: string;
  opponent?: RecentFormOpponent;
  teamScore?: string | number;
  result?: 'W' | 'L' | 'D' | string;
  isHome?: boolean;
}

export interface Team {
  id: string;
  name: string;
  shortName: string;
  abbreviation?: string;
  logo: string;
  color?: string;
  record?: string;
  rank?: number;
  score: number;
  linescores?: Linescore[];
  flag?: string;        // Tennis: player's country flag URL

  stadiumThumb?: string;
  fanArt?: string;
  stadium?: string;
}

export interface MatchOdds {
  provider?: string;
  hasOdds?: boolean;
  fairValue?: number;
  homeWin?: string | number;
  awayWin?: string | number;
  draw?: string | number;
  spread?: string | number;
  homeSpread?: string | number;
  awaySpread?: string | number;
  overUnder?: string | number;
  over?: string | number;
  under?: string | number;
  moneylineHome?: string | number;
  moneylineAway?: string | number;
  home_ml?: string | number;
  away_ml?: string | number;
  draw_ml?: string | number;
  overOdds?: string | number;
  underOdds?: string | number;
  total?: string | number;
  total_line?: string | number;
  totalOver?: string | number;
  homeSpreadOdds?: string | number;
  awaySpreadOdds?: string | number;
  home_spread?: string | number;
  away_spread?: string | number;

  winProbability?: number;
  draftkingsLink?: string;
}

export interface WeatherInfo {
  temp?: number | string;
  condition?: string;
  wind?: string;
  wind_speed?: number | string;
  humidity?: string;
  pressure?: string;
  impact?: string;
}

export type TeamStatLine = {
  name?: string;
  label?: string;
  value?: string | number;
  displayValue?: string | number;
};

export type TeamStatValue = number | string | null | undefined | TeamStatLine[];

export type TeamStats = Record<string, TeamStatValue>;

export interface RosterPlayer {
  id?: string;
  name?: string;
  displayName?: string;
  shortName?: string;
  position?: string | { abbreviation?: string };
  jersey?: string;
  headshot?: string;
  status?: string;
}

export type TraceEntry = JsonRecord;

export interface TopPerformer {
  name?: string;
  team?: string;
  statLine?: string;
  value?: number | string;
  category?: string;
}

export interface OddsSnapshot {
  open: {
    spread: number;
    spreadPrice?: number;
    homeSpreadPrice?: number;
    awaySpreadPrice?: number;
    total: number;
    totalPrice?: number;
    overPrice?: number;
    underPrice?: number;
    mlHome: number;
    mlAway: number;
    mlDraw?: number;
  };
  cur: {
    spread: number;
    spreadPrice?: number;
    homeSpreadPrice?: number;
    awaySpreadPrice?: number;
    total: number;
    totalPrice?: number;
    overPrice?: number;
    underPrice?: number;
    mlHome: number;
    mlAway: number;
    mlDraw?: number;
  };
  hasSpread: boolean;
  hasTotal: boolean;
  hasML: boolean;
}

export interface MatchEvent {
  id: string;
  time: string;
  period?: number;
  type: 'score' | 'foul' | 'highlight' | 'substitution' | 'card' | 'shot' | 'goal';
  teamId?: string;
  playerId?: string;
  detail?: string;
  scoreValue?: number;
  text?: string;
  clock?: string;
  description?: string;
}

export interface StatItem {
  label: string;
  homeValue: string;
  awayValue: string;
  isPercentage?: boolean;
}

export interface AdvancedMetrics {
  home: Record<string, number>;
  away: Record<string, number>;
}

export interface AthleteStats {
  id?: string;
  name: string;
  shortName?: string;
  position?: string;
  stats: string[];
}

export interface PlayerStatCategory {
  name: string;
  displayName: string;
  labels: string[];
  athletes: AthleteStats[];
}

export interface TeamPlayerStats {
  teamId: string;
  categories: PlayerStatCategory[];
}

export interface MomentumPoint {
  minute: number;
  value: number;
  displayClock?: string;
  winProb?: number;
}

export interface MatchContext {
  weather?: {
    temp: string;
    condition: string;
  };
  venue?: {
    name: string;
    city: string;
    state: string;
    indoor: boolean;
  };
  attendance?: number;
  broadcasts?: Array<{
    market: string;
    names: string[];
  }>;
}

export interface Situation {
  // Football
  down?: number;
  distance?: number;
  yardLine?: number;
  downDistanceText?: string;
  isRedZone?: boolean;

  // Baseball
  balls?: number;
  strikes?: number;
  outs?: number;
  onFirst?: boolean;
  onSecond?: boolean;
  onThird?: boolean;

  // NBA / NHL
  isBonus?: boolean;
  isPowerPlay?: boolean;
  ballX?: number;
  ballY?: number;
  playerId?: string;

  // Global
  possessionId?: string;
  possessionText?: string;
}

export interface Drive {
  description: string;
  result?: string;
  yards?: number;
  plays?: number;
  timeElapsed?: string;
  teamId?: string;
  startYardLine?: number;
}

export interface LastPlay {
  id: string;
  text: string;
  clock: string;
  type: string;
  statYardage?: number;
  probability?: {
    homeWinPercentage: number;
  };
}

export interface MatchLeader {
  name: string;
  displayName: string;
  leaders: Array<{
    displayValue: string;
    value: number;
    athlete: {
      id: string;
      fullName: string;
      displayName: string;
      shortName: string;
      headshot?: string;
      position?: { abbreviation: string };
    };
    team?: { id: string };
  }>;
}

export interface Match {
  id: string;
  odds_api_event_id?: string;
  leagueId: string;
  sport: Sport;
  startTime: string | Date;
  status: MatchStatus | string;
  period?: number;
  displayClock?: string;
  minute?: string;
  homeTeam: Team;
  awayTeam: Team;
  homeScore: number;
  awayScore: number;
  odds?: MatchOdds;
  events?: MatchEvent[];
  stats?: StatItem[];
  advancedMetrics?: AdvancedMetrics;
  playerStats?: TeamPlayerStats[];
  leaders?: MatchLeader[];
  momentum?: MomentumPoint[];
  context?: MatchContext;
  situation?: Situation;
  currentDrive?: Drive;
  lastPlay?: LastPlay;
  regulationPeriods?: number;
  win_probability?: { home: number; away: number };
  weather_info?: WeatherInfo;
  current_odds?: MatchOdds;
  opening_odds?: MatchOdds;
  closing_odds?: MatchOdds;
  goalies?: GoalieMatchupData;
  dbProps?: PlayerPropBet[];
  injuries?: InjuryReport[];
  weather_forecast?: {
    wind_speed?: number | string;
    condition?: string;
    temp?: number | string;
  };
  homeTeamStats?: TeamStats;
  awayTeamStats?: TeamStats;
  rosters?: {
    home: RosterPlayer[];
    away: RosterPlayer[];
  };
  // Game Context Fields (for Intel tab)
  seasonType?: number;       // 1=Pre, 2=Regular, 3=Post, 4=Off
  name?: string;             // e.g., "Wild Card Round", "NBA Cup Quarterfinal"
  notes?: string;            // Additional game notes from API
  predictor?: {
    homeTeamChance: number;
    awayTeamChance: number;
    homeTeamLine?: string;
    awayTeamLine?: string;
  };
  ai_signals?: AISignals;
  fetched_at?: number; // timestamp in ms
  canonical_id?: string;
  last_updated?: string;
  home_score?: number;
  away_score?: number;
  ingest_trace?: TraceEntry[];
  logic_trace?: TraceEntry[];
  last_ingest_error?: string;
  // Tennis-specific fields
  round?: string;       // e.g., "Quarterfinal", "Round of 128"
  court?: string;       // e.g., "Rod Laver Arena", "Court 5"
}

export interface GoalieProfile {
  id: string;
  name: string;
  status: 'confirmed' | 'expected' | 'projected' | 'unconfirmed';
  source?: string;
  stats?: {
    gaa: string;
    svPct: string;
    record: string;
    reasoning?: string;
    bettingInsight?: string;
  };
  headshot?: string;
}

export interface GoalieMatchupData {
  home: GoalieProfile;
  away: GoalieProfile;
}

export interface Game extends Match {
  league: string;
  time: string;
  venue: string;
  topPerformers?: TopPerformer[];
}

export interface League {
  id: string;
  name: string;
  sport: Sport;
  apiEndpoint: string;
  oddsKey?: string;
}

export interface Bet {
  id: string;
  matchId: string;
  selection: string;
  odds: string;
  stake: number;
  status: 'PENDING' | 'WON' | 'LOST' | 'PUSH';
  timestamp: number;
  sport?: string;
  marketType?: string;
  analysis?: string;
}

export type ConfidenceTier = 'ELITE' | 'STRONG' | 'LEAN' | 'SPEC' | 'PASS';

export interface UnifiedConfidence {
  score: number;
  tier: ConfidenceTier;
  label: string;
  actionState: 'BUY' | 'LEAN' | 'READ';
}

export interface AnalysisFactor {
  signal: string;
  weight: 'high' | 'medium' | 'low';
  detail: string;
  direction: 'supporting' | 'opposing';
}

export interface RestContext {
  daysRest: number;
  isBackToBack: boolean;
}

export interface DripVolatility {
  volatility_score: number;
  volatility_grade: string;
  projected_swing: string;
  swing_probability: string;
  recommended_side: string;
  reasoning: string[];
  middle_strategy: string;
}

export interface DripLiveMiddle {
  gap_points?: number;
  middle_zone?: string;
  base_hit_rate?: string;
  adjustments?: { factor: string; impact: string }[];
  adjusted_hit_rate?: string;
  ev_per_220?: string;
  quality_grade?: string;
  recommendation?: string;
  reasoning?: string;
  hedge_instruction?: string;
  // Anchored explanation for casual users
  translated_explanation?: TranslatedExplanation;
}

// Anchored Translation Block - provides plain language context without adding certainty or hype
export interface TranslatedExplanation {
  signal: string;           // e.g., "UNDER"
  market_total: string;     // e.g., "61.5"
  time: string;             // e.g., "10:33 Q2"
  score: string;            // e.g., "21–0"
  why_now: string;          // The plain language explanation
}

export interface DripDetection {
  pre_game?: DripVolatility;
  live?: DripLiveMiddle | null;
}

export interface EnhancedEdgeAnalysis {
  pick: string;
  confidence: UnifiedConfidence;
  summary: string;
  tacticalAnalysis?: string;
  factors: AnalysisFactor[];
  counterFactors: AnalysisFactor[];
  lineMovement?: {
    open: string;
    current: string;
    direction: 'sharp' | 'fade' | 'neutral';
  };
  restContext?: {
    home: RestContext;
    away: RestContext;
  };
  sources?: { title: string; url: string }[];
  drip_detection?: DripDetection;
  pregame_scouting?: {
    market_setup: string;
    stress_points: string;
    live_triggers: string;
  };
  unified_report?: UnifiedLiveReport;
  translated_explanation?: TranslatedExplanation | null;
}

export interface UnifiedLiveReport {
  market_reality: {
    fair_value: number;
    market_value: number;
    dislocation_pct: number;
    is_significant: boolean;
  };
  constraints: {
    primary_bottleneck: string;
    details: string;
    cel_tags: string[];
  };
  lenses: {
    quant_lens: string;
    market_behavior_lens: string;
    risk_execution_lens: string;
  };
  kenpom_metrics?: {
    home: { rank: number; adjem: number; adjo: number; adjd: number; adjpt: number };
    away: { rank: number; adjem: number; adjo: number; adjd: number; adjpt: number };
  };
}

export interface MatchIntelligence {
  summary: string;
  tacticalAnalysis: string;
  prediction: {
    pick: string;
    confidence: UnifiedConfidence;
    reasoning: string;
    betType: string;
  };
  context?: string;
  bettingInsight?: string;
  keyMatchup?: string;
  // Audit & Forensic Trace
  integrityScore?: number;
  integrityFindings?: string;
  // Server-generated translation (immutable, locked with signal)
  translated_explanation?: TranslatedExplanation | null;
  thought_trace?: string;
  sources?: { title: string; url: string }[];
}

export interface AIAnalysis {
  summary: string;
  bettingInsight: string;
  keyMatchup: string;
  prediction: string;
}

export interface QuickInsight {
  context: string;
  summary: string;
}

export interface InjuryReport {
  id: string;
  name: string;
  position: string;
  status: string;
  description: string;
  headshot?: string;
  player?: string;
  team?: string;
  impact?: string;
  details?: string;
  returnTimeline?: string;
  analysis?: string;
}

export interface BettingFactor {
  title: string;
  description: string;
  trend: 'HOME_POSITIVE' | 'AWAY_POSITIVE' | 'NEUTRAL';
  confidence?: number;
}

export interface LineMovement {
  opening?: string;
  current?: string;
  direction?: string;
  notes?: string;
}

export interface WeatherCondition {
  temp: string;
  condition: string;
  wind: string;
  humidity: string;
  pressure?: string;
  impact?: string;
}

export interface FatigueMetrics {
  team: string;
  daysRest: number;
  milesTraveled: number;
  timeZonesCrossed: number;
  gamesInLast7Days: number;
  fatigueScore: number;
  note: string;
}

export interface OfficialStats {
  crewName: string;
  referee: string;
  homeWinPct: number;
  overPct: number;
  foulsPerGame: number;
  bias: string;
  keyTendency: string;
}

export interface TeamNews {
  text: string;
  sources?: { title: string; uri: string }[];
}

export interface MatchNews {
  matchId: string;
  report: string;
  keyInjuries: InjuryReport[];
  bettingFactors: BettingFactor[];
  lineMovement?: LineMovement;
  weather?: WeatherCondition;
  fatigue?: { home: FatigueMetrics; away: FatigueMetrics };
  officials?: OfficialStats;
  sources: { title: string; url: string }[];
  status: 'pending' | 'ready' | 'failed' | 'generating';
  sharp_data?: {
    headline: string;
    market_signal: {
      sentiment: 'FADE_PUBLIC' | 'SHARP_BUY' | 'NEUTRAL';
      efficiency_grade: 'A (Strong Value)' | 'B (Good)' | 'C (Fair)' | 'D (Bad Number)' | 'F (Stay Away)';
      is_stale_line: boolean;
      note: string;
    };
    quant_math: {
      fair_win_prob: number;
      user_implied_prob: number;
      edge_percent: number;
      kelly_stake: number;
    };
    analysis: string;
  };
  generatedAt: string;
  expiresAt: string;
}

export type SharpSide = 'PASS' | 'AVOID' | 'OVER' | 'UNDER' | 'HOME' | 'AWAY' | 'DRAW' | string;

export interface SharpRecommendation {
  side?: SharpSide;
  market_type?: 'TOTAL' | 'SPREAD' | 'MONEYLINE' | 'OTHER' | string;
  unit_size?: string;
}

export interface SharpData {
  recommendation?: SharpRecommendation;
  confidence_level?: number;
}

export interface LiveAIAnalysis {
  sharp_data?: SharpData;
  generated_at?: string;
  thought_trace?: string;
  sources?: Array<{ title?: string; url?: string; uri?: string }>;
}

export interface LiveMatchState {
  id: string;
  league_id: string;
  sport: string;
  game_status: string;
  period: number;
  clock: string;
  home_score: number;
  away_score: number;
  situation?: Situation;
  last_play?: LastPlay;
  current_drive?: Drive;
  deterministic_signals?: AISignals;
  ai_analysis?: LiveAIAnalysis;
  opening_odds?: MatchOdds;
  odds?: {
    current?: MatchOdds;
    opening?: MatchOdds;
  };
  updated_at: string;
  created_at?: string;
}

export interface MatchAngle {
  summary: string;
  keyFactors: { title: string; description: string; impact: 'high' | 'medium' | 'low' }[];
  recommendedPlays: {
    label: string;
    odds: string;
    confidence: UnifiedConfidence;
  }[];
  sources?: { title: string; url: string }[];
}

export interface NarrativeIntel {
  headline: string;
  mainRant: string;
  psychologyFactors: { title: string; value: string }[];
  analogies: string[];
  blazingPick: {
    selection: string;
    confidence: UnifiedConfidence;
    reason: string
  };
  sources?: { title: string; url: string }[];
}

export type MarketEfficiency = 'LOW' | 'MEDIUM' | 'HIGH';
export type SystemState = 'ACTIVE' | 'OBSERVE' | 'SILENT';

export interface ShotEvent {
  id: number;
  x: number;
  y: number;
  type: 'goal' | 'shot';
  teamId: 'home' | 'away';
  period: number;
  timeInPeriod: string;
  shooterName: string;
}

export interface HockeyGameData {
  gameId: string;
  shots: ShotEvent[];
  homeTeamAbbrev: string;
  awayTeamAbbrev: string;
}

export interface GameLeader {
  name: string;
  value: string;
  stats: string;
}

// === AI SIGNAL LAYER (DETERMINISTIC) ===

export enum EdgeEnvironmentTag {
  EARLY_MARKET_CORRECTION_LAG = 'EARLY_MARKET_CORRECTION_LAG',
  NBA_LATE_GAME_UNDER_CONTINUATION = 'NBA_LATE_GAME_UNDER_CONTINUATION',
  NBA_MODERN_GARBAGE_KINETICS = 'NBA_MODERN_GARBAGE_KINETICS',
  NFL_TOTAL_FLOOR_OVERSHOOT = 'NFL_TOTAL_FLOOR_OVERSHOOT',
  NFL_LATE_GAME_TD_PRESSURE = 'NFL_LATE_GAME_TD_PRESSURE',
  LOW_MOTIVATION_BOWL = 'LOW_MOTIVATION_BOWL',
  SOCCER_LATE_UNDER_SUPPRESSION = 'SOCCER_LATE_UNDER_SUPPRESSION',
  NHL_EMPTY_NET_INFLATION = 'NHL_EMPTY_NET_INFLATION',
  POSSESSION_ANCHOR_LIMIT = 'POSSESSION_ANCHOR_LIMIT'
}

export interface BookConstraints {
  correction_lag: boolean;     // Early overreaction window
  market_shade: boolean;       // Price deviates vs fair despite stable state
  public_flow_bias: boolean;   // One-sided bias consistent with public behavior
  market_pause: boolean;       // Frozen or delayed updates
}

export interface PregameConstraints {
  public_bias_expected: boolean;
  volatility_profile: 'LOW' | 'MEDIUM' | 'HIGH';
  correction_lag_risk: boolean;
  regime_likelihood: {
    NBA_LATE_GAME_UNDER_CONTINUATION?: number;
    NFL_TOTAL_FLOOR_OVERSHOOT?: number;
    NHL_EMPTY_NET_INFLATION?: number;
    SOCCER_LATE_UNDER_SUPPRESSION?: number;
  };
  alignment_score: number;
  is_sharp_resistance?: boolean;
  is_back_to_back?: boolean;   // Added: Scheduling/Fatigue marker
  is_elite_trailing?: boolean; // Added: v5.6 context
}

export interface EdgeEnvironment {
  tags: EdgeEnvironmentTag[];
  confidence: number; // 0-1
}

export interface SharpOrigin {
  label: string;
  status: 'ACTIVE' | 'POTENTIAL' | 'NONE';
  value?: string;
  description?: string;
}

export interface AISignals {
  // Global Gating
  system_state: SystemState;

  // Market Behavior Indices
  dislocation_side_pct: number;      // Deviation from reference side probability
  dislocation_total_pct: number;     // Deviation from reference total expectation
  market_bias: 'PUBLIC_OVER' | 'PUBLIC_FAV' | 'NONE';
  market_efficiency: MarketEfficiency;
  efficiency_srs?: number;           // Added: YPP-based Power Rating delta
  news_adjustment?: number;          // Added: Information Alpha adjustment
  market_total?: number;

  // Pre-game Context
  season_phase?: string;
  league_intensity?: number;
  pregame?: PregameConstraints;

  // Deterministic Constraints
  constraints: {
    wind?: boolean;
    kicker_out?: boolean;
    road_favorite?: boolean;
    correction_lag: boolean;
    market_shade: boolean;
    public_flow_bias: boolean;
    sharp_resistance?: boolean;   // Added: Reverse Line Movement
    public_shade?: boolean; // Added: Artificial Public Tax
    shade_index?: number; // Point delta between Fair Value and Market
    is_key_defense?: boolean; // Protecting 3, 7, etc.
    trap_reason?: string; // Reason for liability inertia
  };

  odds: OddsSnapshot;

  // Structural Context
  regimes: string[];                 // List of active environment tags
  // v1.6 Deterministic Outputs
  deterministic_fair_total?: number;
  deterministic_regime?: 'NORMAL' | 'CHAOS' | 'BLOWOUT' | 'BACKDOOR' | 'HURRY_UP' | 'KILL_CLOCK';
  p10_total?: number;
  p90_total?: number;
  variance_sd?: number;
  status_reason?: string;
  regime_multiplier?: number;         // Impact on edge quality

  // Persistence Tracking
  edge_cap: number;                  // Maximum permissible edge (default 0.07)

  // Grounding Evidence
  evidence_pack: string[];           // List of strings explaining the signals

  // Sharp Origins (Computer Group Influence)
  sharp_origins?: {
    compute: SharpOrigin;
    data: SharpOrigin;
    limits: SharpOrigin;
    discipline: SharpOrigin;
  };

  // Supplementary (Optional for narration)
  phase?: string;                    // e.g., "Late Q4", "Bottom 9th"
  risk_flags: string[];              // e.g. ["STALE_FEED"]
  context_summary: string;           // Deterministic 1-liner

  // Analytic Values (Lines)
  opening_line?: string;
  current_line?: string;
  engine_ref_line?: string;

  // Pattern Reinforcement (Surgical Confirmation)
  pattern_hash?: string;
  pattern_reinforcement?: {
    score: number;
    status: 'CONFIRMED' | 'STRENGTHENING' | 'NEUTRAL';
    label: string;
  };

  // NFL Total Override (Surgical Gating)
  is_total_override?: boolean;
  override_classification?: 'DELAY' | 'STRUCTURAL' | 'NONE';
  override_logs?: string[];

  // Persistence Validator (North Star Alignment)
  persistence?: {
    divergence_start_time?: string;  // ISO string
    sequence_cycles: number;         // Count of consecutive divergent polling cycles
    is_historically_validated: boolean;
    consensus_lag_mins: number;      // Estimate of market delay
  };

  // Public Narrative (Strategic Pivot)
  narrative?: PublicNarrative;

  // v4.5: UI Data Feeds (Deterministic)
  unified_report?: {
    stats: Record<string, number | string | null | undefined>;
    efficiency: Record<string, number | string | null | undefined>;
  };
  efficiency_matrix?: Record<string, number | string | null | undefined> | null;
  // v5.0: PPM with invariant-safe implied_total
  ppm?: {
    observed: number;        // Raw observed PPM
    projected: number;       // Model Pace (Model Total / Game Minutes)
    delta: number;           // Variance (observed - projected)
    implied_total?: number;  // Visual invariant check: projected × game_mins
  };
  // v5.0: Edge State Taxonomy (Thresholded)
  edge_state?: 'PLAY' | 'LEAN' | 'NEUTRAL';
  edge_points?: number; // Absolute edge magnitude in points
  // v5.0: Context Data (Time + Score for Trust)
  context?: {
    elapsed_mins: number;
    remaining_mins: number;
    current_score: string;  // "AWAY-HOME" format
    period: number;
    clock: string;
  };
  // v5.1: Cal Poly Variance Physics
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
  blueprint?: MarketBlueprint;
  is_high_uncertainty?: boolean;

  // v6.0: Structured Observability
  trace_id?: string;
  trace_dump?: Record<string, unknown>;
}

export interface PublicNarrative {
  high_low_state: 'RISING' | 'FALLING' | 'STABLE' | 'VOLATILE';
  efficiency_trend: 'STRENGTHENING' | 'SOFTENING' | 'SIDEWAYS';
  pace_context: 'FAST' | 'SLOW' | 'NORMAL';
  market_response: 'LAGGING' | 'ADJUSTING' | 'EFFICIENT';
  market_lean?: 'OVER' | 'UNDER' | 'NEUTRAL'; // Added: Explicit directional signal
  signal_label: string; // e.g., "STRUCTURAL REGIME", "MARKET OVERREACTION"
}

export type BetResult = 'won' | 'lost' | 'push' | 'pending' | null;

export interface SpreadAnalysis {
  state: 'open' | 'live' | 'settled';
  provider?: string;
  isLive: boolean;
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

export interface TotalAnalysis {
  state: 'open' | 'live' | 'settled';
  provider?: string;
  isLive: boolean;
  line: number | null;
  display: string;
  overLine: number | null;
  underLine: number | null;
  overDisplay: string;
  underDisplay: string;
  displayLine: string;
  result: 'OVER' | 'UNDER' | 'PUSH' | null;
  actual: number | null;
  overJuice: string;
  underJuice: string;
  label: string;
}

export interface MoneylineAnalysis {
  state: 'open' | 'live' | 'settled';
  provider?: string;
  isLive: boolean;
  home: string;
  away: string;
  draw: string;
  fav: 'home' | 'away' | null;
  result: 'home' | 'away' | 'draw' | null;
  label: string;
}

export interface DeepIntel {
  system_state: 'ACTIVE' | 'OBSERVE' | 'SILENT';
  signals: {
    sharp_resistance: boolean;
    liability_inertia: boolean;
  };
  trench_warfare?: {
    pocket_verdict: 'CLEAN' | 'COLLAPSE' | 'NEUTRAL';
    run_leverage: 'SNOWPLOW' | 'BRICK_WALL' | 'NEUTRAL';
    injury_adjustment: 'CRITICAL' | 'MODERATE' | 'NONE';
  };
  efficiency_matrix?: {
    home: Record<string, number>;
    away: Record<string, number>;
    context: string;
  };
  narrative?: {
    technical_thesis: string;
  };
}

export enum MarketScope {
  FULL_GAME = "FULL_GAME",
  FIRST_HALF = "FIRST_HALF",
  SECOND_HALF = "SECOND_HALF",
  FIRST_QUARTER = "FIRST_QUARTER",
  UNKNOWN = "UNKNOWN"
}

export interface MarketBlueprint {
  id: string;              // snapshot_id (e.g. mat-123:1735345678)
  ticket: string;          // DraftKings: "Under 56.0 (-110)"
  direction: 'OVER' | 'UNDER' | 'HOME' | 'AWAY' | 'PASS';
  line: number;
  price: string;           // "-110" or "EVEN"
  market_type: MarketScope;
  model_number: number;
  status: 'SHARP_BUY' | 'STRONG_VALUE' | 'OBSERVATIONAL_LEAN' | 'LIVE_READ' | 'UPDATING';
  is_valid: boolean;
  reason_code?: string;    // Sentry: "UNIT_MISMATCH", "STALE_PRICE", etc.
}
export interface MatchThesis {
  summary: string;
  keyFactors: { title: string; description: string; impact: 'high' | 'medium' | 'low' }[];
  recommendedPlays: {
    label: string;
    odds: string;
    confidence: UnifiedConfidence;
  }[];
  sources?: { title: string; url: string }[];
}

export interface NarrativeIntel {
  headline: string;
  mainRant: string;
  psychologyFactors: { title: string; value: string }[];
  analogies: string[];
  blazingPick: {
    selection: string;
    confidence: UnifiedConfidence;
    reason: string;
  };
  sources?: { title: string; url: string }[];
}

export * from './odds.ts';
export * from './engine.ts';

import { createClient, type SupabaseClient } from "npm:@supabase/supabase-js@2.57.4";

type JsonRecord = Record<string, unknown>;
type SyncStatus = "completed" | "failed" | "partial";

type SyncExecutionResult = {
  rowsRead: number;
  rowsUpserted: number;
  rowsSkipped?: number;
  partial?: boolean;
  note?: string;
};

type SyncResult = {
  table: string;
  status: SyncStatus;
  didRun: boolean;
  rowsRead: number;
  rowsUpserted: number;
  rowsSkipped: number;
  startedAt: string;
  finishedAt: string;
  watermarkBefore: string | null;
  watermarkAfter: string | null;
  errorMessage: string | null;
};

const BOLTSKS_URL = Deno.env.get("BOLTSKS_URL") ?? "https://qffzvrnbzabcokqqrwbv.supabase.co";
const LOCAL_URL = Deno.env.get("SUPABASE_URL") ?? "https://hylnixnuabtnmjcdnujm.supabase.co";

const PAGE_SIZE = 1000;
const CHUNK_SIZE = 500;
const MAX_PAGES = 200;
const ISO_EPOCH = "1970-01-01T00:00:00.000Z";

const SYNC_INTERVAL_MINUTES: Record<string, number> = {
  trends: 30,
  daily_picks: 30,
  prop_edges: 30,
  mlb_bullpen_kit: 30,
  team_ou_splits: 30,
  team_ats_splits: 30,
  last_game_results: 120,
  opening_lines_backfill: 120,
};

const TARGET_TABLES = Object.keys(SYNC_INTERVAL_MINUTES);

const LEAGUE_ID_CANONICAL_MAP: Record<string, string> = {
  epl: "eng.1",
  "premier league": "eng.1",
  premier_league: "eng.1",
  bundesliga: "ger.1",
  "bundesliga 1": "ger.1",
  bundesliga_1: "ger.1",
  laliga: "esp.1",
  la_liga: "esp.1",
  "la liga": "esp.1",
  seriea: "ita.1",
  serie_a: "ita.1",
  "serie a": "ita.1",
  ligue1: "fra.1",
  ligue_1: "fra.1",
  "ligue 1": "fra.1",
};

function getRequiredEnv(name: string): string {
  const value = Deno.env.get(name);
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function asString(value: unknown): string | null {
  if (typeof value === "string") {
    const normalized = value.trim();
    return normalized.length > 0 ? normalized : null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asInteger(value: unknown): number | null {
  const numeric = asNumber(value);
  return numeric === null ? null : Math.trunc(numeric);
}

function asBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return null;
}

function asIsoTimestamp(value: unknown): string | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString();
  }

  if (typeof value === "string" || typeof value === "number") {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  }

  return null;
}

function asDateString(value: unknown): string | null {
  const iso = asIsoTimestamp(value);
  return iso ? iso.slice(0, 10) : null;
}

function currentUtcDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function getTimestampMs(value: unknown): number {
  const iso = asIsoTimestamp(value);
  if (!iso) return 0;
  const parsed = Date.parse(iso);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function shouldRunByCadence(lastSyncedAt: string | null, intervalMinutes: number): boolean {
  if (!lastSyncedAt) return true;
  const lastSyncedMs = getTimestampMs(lastSyncedAt);
  if (lastSyncedMs <= 0) return true;
  return Date.now() - lastSyncedMs >= intervalMinutes * 60_000;
}

function toErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function normalizeEdgeConfidence(value: unknown): string {
  const normalized = asString(value)?.toUpperCase();
  if (normalized === "A" || normalized === "B" || normalized === "C") return normalized;
  return "C";
}

function normalizeLeagueId(value: unknown): string | null {
  const raw = asString(value);
  if (!raw) return null;
  const normalized = raw.trim().toLowerCase();
  return LEAGUE_ID_CANONICAL_MAP[normalized] ?? normalized;
}

function normalizeTotalLine(value: unknown): number | null {
  const total = asNumber(value);
  if (total === null) return null;
  return total > 0 ? total : null;
}

function normalizeMoneyline(value: unknown): number | null {
  const line = asInteger(value);
  if (line === null || line === 0) return null;
  return line;
}

function lineSnapshotScore(row: JsonRecord): number {
  let score = 0;
  if (asNumber(row.total) !== null) score += 8;
  if (asInteger(row.home_ml) !== null || asInteger(row.away_ml) !== null) score += 4;
  if (asNumber(row.home_spread) !== null || asNumber(row.away_spread) !== null) score += 2;
  if (asString(row.provider)) score += 1;
  return score;
}

async function fetchAllFromTable(
  client: SupabaseClient,
  table: string,
  columns = "*",
): Promise<JsonRecord[]> {
  const rows: JsonRecord[] = [];

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await client.from(table).select(columns).range(from, to);

    if (error) {
      throw new Error(`Source table read failed (${table}): ${error.message}`);
    }

    const pageRows = (data ?? []) as JsonRecord[];
    rows.push(...pageRows);

    if (pageRows.length < PAGE_SIZE) {
      return rows;
    }
  }

  throw new Error(`Pagination guard exceeded for source table ${table}`);
}

async function fetchAllFromRpc(
  client: SupabaseClient,
  rpcName: string,
  args: JsonRecord = {},
): Promise<JsonRecord[]> {
  const rows: JsonRecord[] = [];

  for (let page = 0; page < MAX_PAGES; page += 1) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await client.rpc(rpcName, args).range(from, to);

    if (error) {
      throw new Error(`Source RPC failed (${rpcName}): ${error.message}`);
    }

    const pageRows = (data ?? []) as JsonRecord[];
    rows.push(...pageRows);

    if (pageRows.length < PAGE_SIZE) {
      return rows;
    }
  }

  throw new Error(`Pagination guard exceeded for source RPC ${rpcName}`);
}

async function fullReplace(
  client: SupabaseClient,
  table: string,
  rows: JsonRecord[],
  deleteByColumn: string,
): Promise<void> {
  const { error: deleteError } = await client.from(table).delete().not(deleteByColumn, "is", null);
  if (deleteError) {
    throw new Error(`Delete failed (${table}): ${deleteError.message}`);
  }

  for (let start = 0; start < rows.length; start += CHUNK_SIZE) {
    const chunk = rows.slice(start, start + CHUNK_SIZE);
    if (chunk.length === 0) continue;

    const { error: insertError } = await client.from(table).insert(chunk);
    if (insertError) {
      throw new Error(`Insert failed (${table}): ${insertError.message}`);
    }
  }
}

async function upsertInChunks(
  client: SupabaseClient,
  table: string,
  rows: JsonRecord[],
  onConflict: string,
): Promise<void> {
  for (let start = 0; start < rows.length; start += CHUNK_SIZE) {
    const chunk = rows.slice(start, start + CHUNK_SIZE);
    if (chunk.length === 0) continue;

    const { error } = await client.from(table).upsert(chunk, { onConflict });
    if (error) {
      throw new Error(`Upsert failed (${table}): ${error.message}`);
    }
  }
}

async function ensureWatermarks(client: SupabaseClient, tables: string[]): Promise<void> {
  const payload = tables.map((table) => ({
    source_table: table,
    last_synced_at: ISO_EPOCH,
    updated_at: new Date().toISOString(),
  }));

  const { error } = await client.from("sync_watermarks").upsert(payload, {
    onConflict: "source_table",
    ignoreDuplicates: true,
  });

  if (error) {
    throw new Error(`Failed to ensure sync_watermarks rows: ${error.message}`);
  }
}

async function readWatermarks(client: SupabaseClient, tables: string[]): Promise<Map<string, string>> {
  const { data, error } = await client
    .from("sync_watermarks")
    .select("source_table,last_synced_at")
    .in("source_table", tables);

  if (error) {
    throw new Error(`Failed to read sync_watermarks: ${error.message}`);
  }

  const map = new Map<string, string>();
  for (const row of (data ?? []) as JsonRecord[]) {
    const sourceTable = asString(row.source_table);
    const lastSyncedAt = asIsoTimestamp(row.last_synced_at);
    if (!sourceTable) continue;
    map.set(sourceTable, lastSyncedAt ?? ISO_EPOCH);
  }

  return map;
}

async function persistSyncRun(client: SupabaseClient, result: SyncResult): Promise<string | null> {
  const { data, error } = await client
    .from("sync_runs")
    .insert({
      source_table: result.table,
      target_table: result.table,
      status: result.status,
      rows_read: result.rowsRead,
      rows_upserted: result.rowsUpserted,
      rows_skipped: result.rowsSkipped,
      watermark_before: result.watermarkBefore,
      watermark_after: result.didRun && result.status === "completed" ? result.watermarkAfter : null,
      error_message: result.errorMessage,
      started_at: result.startedAt,
      finished_at: result.finishedAt,
    })
    .select("id")
    .single();

  if (error) {
    console.error(`sync_runs insert failed for ${result.table}:`, error.message);
    return null;
  }

  return asString((data as JsonRecord).id);
}

async function updateWatermark(
  client: SupabaseClient,
  sourceTable: string,
  lastSyncedAt: string,
  runId: string,
): Promise<void> {
  const { error } = await client
    .from("sync_watermarks")
    .update({
      last_synced_at: lastSyncedAt,
      last_run_id: runId,
      updated_at: new Date().toISOString(),
    })
    .eq("source_table", sourceTable);

  if (error) {
    throw new Error(`Failed to update watermark for ${sourceTable}: ${error.message}`);
  }
}

function pickLatestLastGames(rows: JsonRecord[]): JsonRecord[] {
  const latest = new Map<string, JsonRecord>();

  for (const row of rows) {
    const teamName = asString(row.team_name);
    const sport = asString(row.sport);
    if (!teamName || !sport) continue;

    const leagueId = asString(row.league_id) ?? "unknown";
    const key = `${sport}::${leagueId}::${teamName}`;
    const existing = latest.get(key);

    if (!existing) {
      latest.set(key, row);
      continue;
    }

    const incomingMs = getTimestampMs(row.game_date);
    const existingMs = getTimestampMs(existing.game_date);

    if (incomingMs > existingMs) {
      latest.set(key, row);
    }
  }

  return Array.from(latest.values());
}

async function syncTrends(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromRpc(boltsks, "get_all_trends", {
    min_rate: 53,
    signal_mode: "trend",
  });

  const payload: JsonRecord[] = [];
  let skipped = 0;

  for (const row of sourceRows) {
    const visibility = asString(row.visibility)?.toUpperCase();
    if (visibility !== "PUBLIC") {
      skipped += 1;
      continue;
    }

    const layer = asString(row.layer);
    const league = asString(row.league);
    const entity = asString(row.entity);
    const trend = asString(row.trend);

    if (!layer || !league || !entity || !trend) {
      skipped += 1;
      continue;
    }

    payload.push({
      layer,
      league,
      entity,
      trend,
      hit_rate: asNumber(row.hit_rate),
      sample: asInteger(row.sample),
      data_window: asString(row.data_window),
      signal_type: asString(row.signal_type),
    });
  }

  await fullReplace(local, "trends", payload, "layer");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncDailyPicks(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromRpc(boltsks, "get_daily_picks", {});
  const pickDate = currentUtcDate();

  const deduped = new Map<string, JsonRecord>();
  let skipped = 0;

  for (const row of sourceRows) {
    const matchId = asString(row.match_id);
    const play = asString(row.play);

    if (!matchId || !play) {
      skipped += 1;
      continue;
    }

    const key = `${matchId}::${play}::${pickDate}`;
    deduped.set(key, {
      match_id: matchId,
      home_team: asString(row.home_team),
      away_team: asString(row.away_team),
      league_id: normalizeLeagueId(row.league_id),
      start_time: asIsoTimestamp(row.start_time),
      play,
      home_rate: asNumber(row.home_rate),
      home_sample: asInteger(row.home_sample),
      away_rate: asNumber(row.away_rate),
      away_sample: asInteger(row.away_sample),
      avg_rate: asNumber(row.avg_rate),
      pick_type: asString(row.pick_type),
      pick_date: pickDate,
    });
  }

  const payload = Array.from(deduped.values());

  await upsertInChunks(local, "daily_picks", payload, "match_id,play,pick_date");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncPropEdges(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromRpc(boltsks, "get_todays_prop_edges", {});
  const pickDate = currentUtcDate();

  const deduped = new Map<string, JsonRecord>();
  let skipped = 0;

  for (const row of sourceRows) {
    const playerName = asString(row.player_name);
    const betType = asString(row.bet_type);
    const side = asString(row.side);
    const edgeConfidence = normalizeEdgeConfidence(row.edge_confidence);
    const matchId = asString(row.match_id);

    if (!playerName || !betType || !side || !matchId) {
      skipped += 1;
      continue;
    }

    const key = `${playerName}::${betType}::${side}::${matchId}::${pickDate}`;
    deduped.set(key, {
      player_name: playerName,
      league: normalizeLeagueId(row.league),
      bet_type: betType,
      pick_type: asString(row.pick_type),
      side,
      line_value: asNumber(row.line_value),
      odds_american: asInteger(row.odds_american),
      provider: asString(row.provider),
      match_id: matchId,
      team: asString(row.team),
      opponent: asString(row.opponent),
      hist_win_pct: asNumber(row.hist_win_pct),
      hist_total: asInteger(row.hist_total),
      hist_avg_actual: asNumber(row.hist_avg_actual),
      hist_avg_delta: asNumber(row.hist_avg_delta),
      edge_confidence: edgeConfidence,
      pick_date: pickDate,
    });
  }

  const payload = Array.from(deduped.values());

  await upsertInChunks(
    local,
    "prop_edges",
    payload,
    "player_name,bet_type,side,match_id,pick_date",
  );

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncMlbBullpenKit(
  local: SupabaseClient,
  boltsks: SupabaseClient,
): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromTable(boltsks, "mv_mlb_matchup_betting_kit", "*");
  const syncedAt = new Date().toISOString();

  const payload: JsonRecord[] = [];
  let skipped = 0;

  for (const row of sourceRows) {
    const matchId = asString(row.match_id);
    const homeTeam = asString(row.home_team);
    const awayTeam = asString(row.away_team);

    if (!matchId || !homeTeam || !awayTeam) {
      skipped += 1;
      continue;
    }

    payload.push({
      match_id: matchId,
      start_time: asIsoTimestamp(row.start_time),
      home_team: homeTeam,
      away_team: awayTeam,
      home_team_abbr: asString(row.home_team_abbr),
      away_team_abbr: asString(row.away_team_abbr),
      home_probable_starter_name: asString(row.home_probable_starter_name),
      away_probable_starter_name: asString(row.away_probable_starter_name),
      home_starter_last5_era: asNumber(row.home_starter_last5_era),
      away_starter_last5_era: asNumber(row.away_starter_last5_era),
      home_starter_last5_whip: asNumber(row.home_starter_last5_whip),
      away_starter_last5_whip: asNumber(row.away_starter_last5_whip),
      home_starter_last5_avg_ip: asNumber(row.home_starter_last5_avg_ip),
      away_starter_last5_avg_ip: asNumber(row.away_starter_last5_avg_ip),
      home_starter_k_per_9: asNumber(row.home_starter_k_per_9),
      away_starter_k_per_9: asNumber(row.away_starter_k_per_9),
      home_bullpen_stress_score: asNumber(row.home_bullpen_stress_score),
      away_bullpen_stress_score: asNumber(row.away_bullpen_stress_score),
      home_bullpen_era: asNumber(row.home_bullpen_era),
      away_bullpen_era: asNumber(row.away_bullpen_era),
      home_bullpen_rolling10_era: asNumber(row.home_bullpen_rolling10_era),
      away_bullpen_rolling10_era: asNumber(row.away_bullpen_rolling10_era),
      home_bullpen_ip_last3d: asNumber(row.home_bullpen_ip_last3d),
      away_bullpen_ip_last3d: asNumber(row.away_bullpen_ip_last3d),
      home_high_leverage_back_to_back: asInteger(row.home_high_leverage_back_to_back),
      away_high_leverage_back_to_back: asInteger(row.away_high_leverage_back_to_back),
      home_injured_relief_pitchers: asInteger(row.home_injured_relief_pitchers),
      away_injured_relief_pitchers: asInteger(row.away_injured_relief_pitchers),
      home_lead_conversion_after5: asNumber(row.home_lead_conversion_after5),
      away_lead_conversion_after5: asNumber(row.away_lead_conversion_after5),
      home_one_run_hold_rate_after5: asNumber(row.home_one_run_hold_rate_after5),
      away_one_run_hold_rate_after5: asNumber(row.away_one_run_hold_rate_after5),
      home_last5_record: asString(row.home_last5_record),
      away_last5_record: asString(row.away_last5_record),
      home_last5_win_pct: asNumber(row.home_last5_win_pct),
      away_last5_win_pct: asNumber(row.away_last5_win_pct),
      home_ats_summary: asString(row.home_ats_summary),
      away_ats_summary: asString(row.away_ats_summary),
      home_injury_count: asInteger(row.home_injury_count),
      away_injury_count: asInteger(row.away_injury_count),
      home_bullpen_injury_count: asInteger(row.home_bullpen_injury_count),
      away_bullpen_injury_count: asInteger(row.away_bullpen_injury_count),
      series_summary: asString(row.series_summary),
      venue: asString(row.venue),
      weather_temp: asNumber(row.weather_temp),
      weather_gust: asNumber(row.weather_gust),
      dk_home_moneyline: asNumber(row.dk_home_moneyline),
      dk_away_moneyline: asNumber(row.dk_away_moneyline),
      dk_spread: asNumber(row.dk_spread),
      dk_total: asNumber(row.dk_total),
      dk_over_price: asNumber(row.dk_over_price),
      dk_under_price: asNumber(row.dk_under_price),
      bullpen_stress_delta_favors_home: asNumber(row.bullpen_stress_delta_favors_home),
      bullpen_quality_delta_favors_home: asNumber(row.bullpen_quality_delta_favors_home),
      lead_protection_delta_favors_home: asNumber(row.lead_protection_delta_favors_home),
      starter_era_delta_favors_home: asNumber(row.starter_era_delta_favors_home),
      synced_at: syncedAt,
    });
  }

  await fullReplace(local, "mlb_bullpen_kit", payload, "match_id");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncTeamOuSplits(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromTable(boltsks, "mv_team_ou_vs_line_v2", "*");

  const payload: JsonRecord[] = [];
  let skipped = 0;

  for (const row of sourceRows) {
    const teamName = asString(row.team_name);
    const leagueId = normalizeLeagueId(row.league_id);
    const location = asString(row.location);

    if (!teamName || !leagueId || !location) {
      skipped += 1;
      continue;
    }

    payload.push({
      team_name: teamName,
      league_id: leagueId,
      location,
      sport: asString(row.sport),
      games_with_line: asInteger(row.games_with_line),
      over_count: asInteger(row.over_count),
      under_count: asInteger(row.under_count),
      push_count: asInteger(row.push_count),
      over_rate: asNumber(row.over_rate),
      under_rate: asNumber(row.under_rate),
      avg_posted_total: asNumber(row.avg_posted_total),
      avg_actual_total: asNumber(row.avg_actual_total),
      avg_total_delta: asNumber(row.avg_total_delta),
      last_match_date: asIsoTimestamp(row.last_match_date),
    });
  }

  await fullReplace(local, "team_ou_splits", payload, "team_name");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncTeamAtsSplits(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromTable(boltsks, "mv_team_ats_vs_line_v2", "*");

  const payload: JsonRecord[] = [];
  let skipped = 0;

  for (const row of sourceRows) {
    const teamName = asString(row.team_name);
    const leagueId = normalizeLeagueId(row.league_id);
    const location = asString(row.location);

    if (!teamName || !leagueId || !location) {
      skipped += 1;
      continue;
    }

    payload.push({
      team_name: teamName,
      league_id: leagueId,
      location,
      sport: asString(row.sport),
      fav_games: asInteger(row.fav_games),
      fav_covers: asInteger(row.fav_covers),
      fav_cover_rate: asNumber(row.fav_cover_rate),
      dog_games: asInteger(row.dog_games),
      dog_covers: asInteger(row.dog_covers),
      dog_cover_rate: asNumber(row.dog_cover_rate),
      total_games: asInteger(row.total_games),
      avg_spread: asNumber(row.avg_spread),
      last_match_date: asIsoTimestamp(row.last_match_date),
    });
  }

  await fullReplace(local, "team_ats_splits", payload, "team_name");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: skipped,
  };
}

async function syncLastGameResults(local: SupabaseClient, boltsks: SupabaseClient): Promise<SyncExecutionResult> {
  const sourceRows = await fetchAllFromRpc(boltsks, "get_last_game_per_team", {});
  const normalizedRows: JsonRecord[] = [];
  let skipped = 0;

  for (const row of sourceRows) {
    const teamName = asString(row.team_name);
    const leagueId = normalizeLeagueId(row.league_id ?? row.league);
    const sport = asString(row.sport)?.toUpperCase();
    const gameDate = asDateString(row.game_date);

    if (!teamName || !leagueId || !sport || !gameDate) {
      skipped += 1;
      continue;
    }

    normalizedRows.push({
      team_name: teamName,
      league_id: leagueId,
      sport,
      game_date: gameDate,
      opponent: asString(row.opponent),
      btts_1h: asBoolean(row.btts_1h),
      total_corners: asInteger(row.total_corners),
      total_cards: asInteger(row.total_cards),
      goals_2h: asInteger(row.goals_2h),
      late_goals: asInteger(row.late_goals),
      total_goals: asInteger(row.total_goals),
      home_score: asInteger(row.home_score),
      away_score: asInteger(row.away_score),
      was_home: asBoolean(row.was_home),
      form_string: asString(row.form_string),
      wins: asInteger(row.wins),
      draws: asInteger(row.draws),
      losses: asInteger(row.losses),
      matches: asInteger(row.matches),
      btts_rate: asNumber(row.btts_rate),
      over_25_rate: asNumber(row.over_25_rate),
      clean_sheet_rate: asNumber(row.clean_sheet_rate),
    });
  }

  const latestRows = pickLatestLastGames(normalizedRows);
  const payload = latestRows;
  skipped += normalizedRows.length - latestRows.length;

  await fullReplace(local, "last_game_results", payload, "team_name");

  return {
    rowsRead: sourceRows.length,
    rowsUpserted: payload.length,
    rowsSkipped: Math.max(0, skipped),
  };
}

async function syncOpeningLinesBackfill(
  local: SupabaseClient,
  boltsks: SupabaseClient,
): Promise<SyncExecutionResult> {
  const sourceOpeningRows = await fetchAllFromTable(
    boltsks,
    "opening_lines",
    "match_id,home_spread,away_spread,total,home_ml,away_ml,provider,created_at",
  );
  const sourceClosingRows = await fetchAllFromTable(
    boltsks,
    "closing_lines",
    "match_id,home_spread,away_spread,total,home_ml,away_ml,league_id,created_at",
  );
  const sourceMarketRows = await fetchAllFromTable(
    boltsks,
    "market_history",
    "match_id,total_line,home_ml,away_ml,home_spread,away_spread,provider,source,ts,is_live",
  );
  const localRows = await fetchAllFromTable(
    local,
    "opening_lines",
    "match_id,total,home_ml,away_ml,home_spread,away_spread,league_id,provider,source",
  );

  const localByMatch = new Map<string, JsonRecord>();
  for (const row of localRows) {
    const matchId = asString(row.match_id);
    if (!matchId) continue;
    if (!localByMatch.has(matchId)) {
      localByMatch.set(matchId, row);
    }
  }

  const openingByMatch = new Map<string, JsonRecord>();
  const closingByMatch = new Map<string, JsonRecord>();
  const marketByMatch = new Map<string, JsonRecord>();

  const chooseBest = (targetMap: Map<string, JsonRecord>, matchId: string, candidate: JsonRecord): void => {
    const existing = targetMap.get(matchId);
    if (!existing) {
      targetMap.set(matchId, candidate);
      return;
    }

    const existingScore = lineSnapshotScore(existing);
    const incomingScore = lineSnapshotScore(candidate);

    if (incomingScore > existingScore) {
      targetMap.set(matchId, candidate);
      return;
    }

    if (incomingScore === existingScore) {
      const existingMs = getTimestampMs(existing.created_at);
      const incomingMs = getTimestampMs(candidate.created_at);
      if (incomingMs > existingMs) {
        targetMap.set(matchId, candidate);
      }
    }
  };

  for (const row of sourceClosingRows) {
    const matchId = asString(row.match_id);
    if (!matchId) continue;

    chooseBest(closingByMatch, matchId, {
      match_id: matchId,
      home_spread: asNumber(row.home_spread),
      away_spread: asNumber(row.away_spread),
      total: normalizeTotalLine(row.total),
      home_ml: normalizeMoneyline(row.home_ml),
      away_ml: normalizeMoneyline(row.away_ml),
      league_id: normalizeLeagueId(row.league_id),
      provider: "kalshi",
      source: "Boltsks ClosingLines Backfill",
      created_at: asIsoTimestamp(row.created_at) ?? new Date().toISOString(),
    });
  }

  for (const row of sourceOpeningRows) {
    const matchId = asString(row.match_id);
    if (!matchId) continue;

    chooseBest(openingByMatch, matchId, {
      match_id: matchId,
      home_spread: asNumber(row.home_spread),
      away_spread: asNumber(row.away_spread),
      total: normalizeTotalLine(row.total),
      home_ml: normalizeMoneyline(row.home_ml),
      away_ml: normalizeMoneyline(row.away_ml),
      league_id: null,
      provider: asString(row.provider),
      source: "Boltsks OpeningLines Backfill",
      created_at: asIsoTimestamp(row.created_at) ?? new Date().toISOString(),
    });
  }

  for (const row of sourceMarketRows) {
    if (asBoolean(row.is_live) === true) continue;

    const matchId = asString(row.match_id);
    if (!matchId) continue;

    chooseBest(marketByMatch, matchId, {
      match_id: matchId,
      home_spread: asNumber(row.home_spread),
      away_spread: asNumber(row.away_spread),
      total: normalizeTotalLine(row.total_line),
      home_ml: normalizeMoneyline(row.home_ml),
      away_ml: normalizeMoneyline(row.away_ml),
      league_id: null,
      provider: asString(row.provider) ?? asString(row.source),
      source: "Boltsks MarketHistory Backfill",
      created_at: asIsoTimestamp(row.ts) ?? new Date().toISOString(),
    });
  }

  const mergedByMatch = new Map<string, JsonRecord>();
  let mergedCount = 0;
  let mergedFromMarketCount = 0;

  const mergeMissingFields = (
    base: JsonRecord,
    fallback: JsonRecord,
  ): { row: JsonRecord; filled: number } => {
    const merged: JsonRecord = {
      match_id: asString(base.match_id) ?? asString(fallback.match_id),
      total: normalizeTotalLine(base.total),
      home_spread: asNumber(base.home_spread),
      away_spread: asNumber(base.away_spread),
      home_ml: normalizeMoneyline(base.home_ml),
      away_ml: normalizeMoneyline(base.away_ml),
      league_id: normalizeLeagueId(base.league_id) ?? normalizeLeagueId(fallback.league_id),
      provider: asString(base.provider) ?? asString(fallback.provider),
      source: asString(base.source) ?? asString(fallback.source),
      created_at: asIsoTimestamp(base.created_at) ?? asIsoTimestamp(fallback.created_at) ?? new Date().toISOString(),
    };

    let filled = 0;
    const fallbackTotal = normalizeTotalLine(fallback.total);
    const fallbackHomeSpread = asNumber(fallback.home_spread);
    const fallbackAwaySpread = asNumber(fallback.away_spread);
    const fallbackHomeMl = normalizeMoneyline(fallback.home_ml);
    const fallbackAwayMl = normalizeMoneyline(fallback.away_ml);

    if (asNumber(merged.home_spread) === null && fallbackHomeSpread !== null) {
      merged.home_spread = fallbackHomeSpread;
      filled += 1;
    }
    if (asNumber(merged.away_spread) === null && fallbackAwaySpread !== null) {
      merged.away_spread = fallbackAwaySpread;
      filled += 1;
    }
    if (normalizeTotalLine(merged.total) === null && fallbackTotal !== null) {
      merged.total = fallbackTotal;
      filled += 1;
    }
    if (normalizeMoneyline(merged.home_ml) === null && fallbackHomeMl !== null) {
      merged.home_ml = fallbackHomeMl;
      filled += 1;
    }
    if (normalizeMoneyline(merged.away_ml) === null && fallbackAwayMl !== null) {
      merged.away_ml = fallbackAwayMl;
      filled += 1;
    }

    return { row: merged, filled };
  };

  for (const [matchId, closing] of closingByMatch.entries()) {
    mergedByMatch.set(matchId, closing);
  }

  for (const [matchId, opening] of openingByMatch.entries()) {
    const closing = mergedByMatch.get(matchId);
    if (!closing) {
      mergedByMatch.set(matchId, {
        ...opening,
        provider: asString(opening.provider) ?? "espn",
      });
      continue;
    }

    const merged = mergeMissingFields(opening, closing);
    if (merged.filled > 0) {
      mergedCount += 1;
      if (!asString(opening.provider)) {
        merged.row.provider = "espn+kalshi";
      }
    }

    mergedByMatch.set(matchId, merged.row);
  }

  for (const [matchId, market] of marketByMatch.entries()) {
    const existing = mergedByMatch.get(matchId);
    if (!existing) {
      mergedByMatch.set(matchId, market);
      continue;
    }

    const merged = mergeMissingFields(existing, market);
    if (merged.filled > 0) {
      mergedFromMarketCount += 1;
    }
    mergedByMatch.set(matchId, merged.row);
  }

  const inserts: JsonRecord[] = [];
  const updates: Array<{ matchId: string; patch: JsonRecord }> = [];
  let skipped = 0;

  for (const [matchId, candidate] of mergedByMatch.entries()) {
    const candidateTotal = normalizeTotalLine(candidate.total);
    const candidateHomeMl = normalizeMoneyline(candidate.home_ml);
    const candidateAwayMl = normalizeMoneyline(candidate.away_ml);
    const candidateHomeSpread = asNumber(candidate.home_spread);
    const candidateAwaySpread = asNumber(candidate.away_spread);

    if (
      candidateTotal === null &&
      candidateHomeMl === null &&
      candidateAwayMl === null &&
      candidateHomeSpread === null &&
      candidateAwaySpread === null
    ) {
      skipped += 1;
      continue;
    }

    const existing = localByMatch.get(matchId);

    if (!existing) {
      inserts.push({
        match_id: matchId,
        total: candidateTotal,
        home_ml: candidateHomeMl,
        away_ml: candidateAwayMl,
        home_spread: candidateHomeSpread,
        away_spread: candidateAwaySpread,
        league_id: normalizeLeagueId(candidate.league_id),
        provider: asString(candidate.provider),
        source: asString(candidate.source),
        created_at: asIsoTimestamp(candidate.created_at) ?? new Date().toISOString(),
      });
      continue;
    }

    const patch: JsonRecord = {};
    const existingTotal = normalizeTotalLine(existing.total);
    const existingHomeMl = normalizeMoneyline(existing.home_ml);
    const existingAwayMl = normalizeMoneyline(existing.away_ml);
    const existingHomeSpread = asNumber(existing.home_spread);
    const existingAwaySpread = asNumber(existing.away_spread);
    const existingLeagueId = normalizeLeagueId(existing.league_id);
    const existingProvider = asString(existing.provider);
    const existingSource = asString(existing.source);
    const candidateLeagueId = normalizeLeagueId(candidate.league_id);
    const candidateProvider = asString(candidate.provider);
    const candidateSource = asString(candidate.source);

    if (existingTotal === null && candidateTotal !== null) patch.total = candidateTotal;
    if (existingHomeMl === null && candidateHomeMl !== null) patch.home_ml = candidateHomeMl;
    if (existingAwayMl === null && candidateAwayMl !== null) patch.away_ml = candidateAwayMl;
    if (existingHomeSpread === null && candidateHomeSpread !== null) patch.home_spread = candidateHomeSpread;
    if (existingAwaySpread === null && candidateAwaySpread !== null) patch.away_spread = candidateAwaySpread;
    if (!existingLeagueId && candidateLeagueId) patch.league_id = candidateLeagueId;
    if (!existingProvider && candidateProvider) patch.provider = candidateProvider;
    if (!existingSource && candidateSource) patch.source = candidateSource;

    if (Object.keys(patch).length > 0) {
      updates.push({ matchId, patch });
    } else {
      skipped += 1;
    }
  }

  for (let start = 0; start < inserts.length; start += CHUNK_SIZE) {
    const chunk = inserts.slice(start, start + CHUNK_SIZE);
    if (chunk.length === 0) continue;
    const { error } = await local.from("opening_lines").insert(chunk);
    if (error) {
      throw new Error(`Insert failed (opening_lines backfill): ${error.message}`);
    }
  }

  for (const update of updates) {
    const { error } = await local.from("opening_lines").update(update.patch).eq("match_id", update.matchId);
    if (error) {
      throw new Error(`Update failed (opening_lines backfill for ${update.matchId}): ${error.message}`);
    }
  }

  const { count: rowsSanitizedCount, error: sanitizeCountError } = await local
    .from("opening_lines")
    .select("id", { count: "exact", head: true })
    .not("total", "is", null)
    .lte("total", 0);

  if (sanitizeCountError) {
    throw new Error(`Sanitizer pre-count failed (opening_lines): ${sanitizeCountError.message}`);
  }

  const { error: cleanError } = await local
    .from("opening_lines")
    .update({ total: null })
    .not("total", "is", null)
    .lte("total", 0);

  if (cleanError) {
    throw new Error(`Zero-line cleanup failed (opening_lines): ${cleanError.message}`);
  }

  const rowsProcessed = mergedByMatch.size;
  const rowsSanitized = rowsSanitizedCount ?? 0;
  const sanitizerRate = rowsProcessed > 0
    ? Number((rowsSanitized / rowsProcessed).toFixed(4))
    : 0;

  const { error: sanitizerLogError } = await local
    .from("kalshi_sync_log")
    .insert({
      league: "all",
      rows_processed: rowsProcessed,
      rows_sanitized: rowsSanitized,
      sanitizer_rate: sanitizerRate,
      notes: `opening_lines_backfill total<=0 sanitizer; inserted=${inserts.length}; updated=${updates.length}`,
    });

  const sanitizerLogMissingTable = sanitizerLogError
    && (
      sanitizerLogError.code === "42P01"
      || sanitizerLogError.message.toLowerCase().includes("kalshi_sync_log")
    );

  if (sanitizerLogError && !sanitizerLogMissingTable) {
    throw new Error(`Sanitizer log insert failed: ${sanitizerLogError.message}`);
  }

  const sanitizerLogState = sanitizerLogError ? "skipped" : "logged";

  return {
    rowsRead: sourceOpeningRows.length + sourceClosingRows.length + sourceMarketRows.length,
    rowsUpserted: inserts.length + updates.length,
    rowsSkipped: skipped,
    note:
      `opening_lines_backfill from_opening=${openingByMatch.size}, ` +
      `from_closing=${closingByMatch.size}, merged=${mergedCount}, ` +
      `from_market=${marketByMatch.size}, merged_market=${mergedFromMarketCount}, ` +
      `inserted=${inserts.length}, updated=${updates.length}, ` +
      `sanitized=${rowsSanitized}, sanitizer_rate=${sanitizerRate}, ` +
      `sanitizer_log=${sanitizerLogState}`,
  };
}

async function runSyncTask(
  table: string,
  intervalMinutes: number,
  watermarkMap: Map<string, string>,
  run: () => Promise<SyncExecutionResult>,
): Promise<SyncResult> {
  const startedAt = new Date().toISOString();
  const watermarkBefore = watermarkMap.get(table) ?? ISO_EPOCH;

  if (!shouldRunByCadence(watermarkBefore, intervalMinutes)) {
    return {
      table,
      status: "completed",
      didRun: false,
      rowsRead: 0,
      rowsUpserted: 0,
      rowsSkipped: 1,
      startedAt,
      finishedAt: new Date().toISOString(),
      watermarkBefore,
      watermarkAfter: watermarkBefore,
      errorMessage: `Skipped by cadence (${intervalMinutes} minute interval)`,
    };
  }

  try {
    const output = await run();
    const finishedAt = new Date().toISOString();

    return {
      table,
      status: output.partial ? "partial" : "completed",
      didRun: true,
      rowsRead: output.rowsRead,
      rowsUpserted: output.rowsUpserted,
      rowsSkipped: output.rowsSkipped ?? 0,
      startedAt,
      finishedAt,
      watermarkBefore,
      watermarkAfter: finishedAt,
      errorMessage: output.note ?? null,
    };
  } catch (error) {
    return {
      table,
      status: "failed",
      didRun: true,
      rowsRead: 0,
      rowsUpserted: 0,
      rowsSkipped: 0,
      startedAt,
      finishedAt: new Date().toISOString(),
      watermarkBefore,
      watermarkAfter: null,
      errorMessage: toErrorMessage(error),
    };
  }
}

Deno.serve(async (req) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ code: "METHOD_NOT_ALLOWED", message: "Use POST" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const syncSecret = getRequiredEnv("SYNC_SECRET");
    const incomingSecret = req.headers.get("x-sync-secret");

    if (!incomingSecret || incomingSecret !== syncSecret) {
      return new Response(JSON.stringify({ code: "UNAUTHORIZED", message: "Invalid sync secret" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    }

    const boltsksServiceRoleKey = getRequiredEnv("BOLTSKS_SERVICE_ROLE_KEY");
    const localServiceRoleKey = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY");

    const boltsks = createClient(BOLTSKS_URL, boltsksServiceRoleKey, {
      auth: { persistSession: false },
    });

    const local = createClient(LOCAL_URL, localServiceRoleKey, {
      auth: { persistSession: false },
    });

    await ensureWatermarks(local, TARGET_TABLES);
    const watermarkMap = await readWatermarks(local, TARGET_TABLES);

    const taskOrder: Array<{ table: string; run: () => Promise<SyncExecutionResult> }> = [
      { table: "trends", run: () => syncTrends(local, boltsks) },
      { table: "daily_picks", run: () => syncDailyPicks(local, boltsks) },
      { table: "prop_edges", run: () => syncPropEdges(local, boltsks) },
      { table: "mlb_bullpen_kit", run: () => syncMlbBullpenKit(local, boltsks) },
      { table: "team_ou_splits", run: () => syncTeamOuSplits(local, boltsks) },
      { table: "team_ats_splits", run: () => syncTeamAtsSplits(local, boltsks) },
      { table: "last_game_results", run: () => syncLastGameResults(local, boltsks) },
      { table: "opening_lines_backfill", run: () => syncOpeningLinesBackfill(local, boltsks) },
    ];

    const results: SyncResult[] = [];

    for (const task of taskOrder) {
      const intervalMinutes = SYNC_INTERVAL_MINUTES[task.table] ?? 30;
      const result = await runSyncTask(task.table, intervalMinutes, watermarkMap, task.run);
      results.push(result);

      const runId = await persistSyncRun(local, result);
      if (runId && result.status === "completed" && result.didRun && result.watermarkAfter) {
        await updateWatermark(local, task.table, result.watermarkAfter, runId);
        watermarkMap.set(task.table, result.watermarkAfter);
      }
    }

    const hasFailed = results.some((result) => result.status === "failed");
    const hasPartial = results.some((result) => result.status === "partial");

    const status = hasFailed ? "failed" : hasPartial ? "partial" : "completed";

    return new Response(
      JSON.stringify({
        status,
        project_ref: "hylnixnuabtnmjcdnujm",
        source_project_ref: "qffzvrnbzabcokqqrwbv",
        results,
      }),
      {
        status: hasFailed ? 500 : 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  } catch (error) {
    return new Response(
      JSON.stringify({
        status: "failed",
        code: "SYNC_CONSUMER_DATA_FATAL",
        message: toErrorMessage(error),
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      },
    );
  }
});

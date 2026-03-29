const { CACHE, handleOptions, sendError, sendJson, setApiHeaders } = require("./_lib/http");

const SUPABASE_URL =
  process.env.SUPABASE_URL || "https://hylnixnuabtnmjcdnujm.supabase.co";
const SUPABASE_KEY =
  process.env.SUPABASE_ANON_KEY ||
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
  process.env.VITE_SUPABASE_ANON_KEY ||
  process.env.SUPABASE_SERVICE_ROLE_KEY ||
  process.env.SUPABASE_API_KEY ||
  "";
const REQUEST_TIMEOUT_MS = 12000;

const DEFAULT_LIMIT = 250;
const DEFAULT_MIN_RATE = 80;
const MAX_LIMIT = 1000;

function clampNumber(value, fallback, min, max) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function normalizeLayer(value) {
  const raw = String(value || "").trim();
  return raw.length === 0 || raw.toLowerCase() === "all" ? null : raw;
}

function readString(row, keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed.length > 0) return trimmed;
    }
    if (typeof value === "number" && Number.isFinite(value)) return String(value);
  }
  return null;
}

function readNumber(row, keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string") {
      const parsed = Number.parseFloat(value);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return null;
}

function readBoolean(row, keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value === 1;
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (["true", "1", "y", "yes", "on"].includes(normalized)) return true;
      if (["false", "0", "n", "no", "off"].includes(normalized)) return false;
    }
  }
  return false;
}

function normalizeLeagueName(value) {
  const raw = String(value || "").trim();
  if (!raw) return "global";
  if (/^[a-z]{2,6}\.\d+$/.test(raw.toLowerCase())) return raw.toUpperCase();
  if (raw.length <= 3) return raw.toUpperCase();

  return raw
    .split(/[-_./\s]+/)
    .filter(Boolean)
    .map((part) => {
      const upper = part.toUpperCase();
      if (upper.length <= 3) return upper;
      return `${upper[0]}${upper.slice(1).toLowerCase()}`;
    })
    .join(" ");
}

function normalizeSignal(value) {
  if (!value) return "neutral";
  const normalized = String(value).trim().toLowerCase();
  if (normalized.startsWith("fade") || normalized === "negative") return "fade";
  if (normalized.startsWith("trend") || normalized === "positive") return "trend";
  return normalized;
}

function normalizeDirection(signalType, trendText) {
  if (signalType === "fade") return "FADE";
  if (signalType === "trend") return "TREND";

  const candidate = `${trendText}`.toLowerCase();
  if (/\b(fade|under|down|away|sell|against|lay|negative|off)\b/.test(candidate)) return "FADE";
  if (/\b(trend|over|up|home|positive|buy|for|long)\b/.test(candidate)) return "TREND";
  return "NEUTRAL";
}

function normalizeTeamKey(league, team) {
  return `${normalizeLeagueName(league)}::${String(team || "")
    .normalize("NFKD")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()}`;
}

function sanitizeRow(row) {
  const section = readString(row, ["section"]) ?? null;
  const team = readString(row, ["team", "entity", "team_name"]) ?? "Unknown";
  const league = normalizeLeagueName(readString(row, ["league", "league_id"]) ?? "global");
  const trend = readString(row, ["trend", "signal_text"]) ?? "No trend";
  const record = readString(row, ["record", "record_line"]) ?? "0-0";
  const layer = readString(row, ["layer", "signal_layer"]) ?? "Unspecified";
  const hitRate = readNumber(row, ["hit_rate", "hitrate"]) ?? 0;
  const sample = Math.max(0, Math.round(readNumber(row, ["sample", "sample_size", "games"]) || 0));
  const signalType = normalizeSignal(readString(row, ["signal_type", "signalType"]));
  const direction = normalizeDirection(signalType, trend);
  const lastHeld = readBoolean(row, ["last_held", "lastHeld"]);

  return {
    id: `${league}|${team}|${layer}|${trend}`.toLowerCase(),
    section,
    team,
    league,
    trend,
    record,
    layer,
    hit_rate: Number(Math.max(0, Math.min(100, hitRate)).toFixed(1)),
    sample,
    last_held: Boolean(lastHeld),
    signal_type: signalType,
    direction,
    updated_at: readString(row, ["updated_at", "updatedAt"]) ?? null,
    logo_url: null,
  };
}

function sortByLeagueAndLayer(a, b) {
  const layerCompare = String(a.layer || "").localeCompare(String(b.layer || ""));
  if (layerCompare !== 0) return layerCompare;
  return String(a.team || "").localeCompare(String(b.team || ""));
}

function uniqueSorted(values) {
  return [...new Set(values.filter(Boolean))].sort((a, b) => a.localeCompare(b));
}

function round1(value) {
  if (!Number.isFinite(value ?? NaN)) return null;
  return Math.round(value * 10) / 10;
}

function average(values) {
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function addTotalFromPair(row, keysLeft, keysRight, out) {
  const left = readNumber(row, keysLeft);
  const right = readNumber(row, keysRight);
  if (!Number.isFinite(left ?? NaN) && !Number.isFinite(right ?? NaN)) {
    return;
  }

  out.push((left ?? 0) + (right ?? 0));
}

function addAverageFromPair(row, keysLeft, keysRight, out) {
  const left = readNumber(row, keysLeft);
  const right = readNumber(row, keysRight);
  if (!Number.isFinite(left ?? NaN) && !Number.isFinite(right ?? NaN)) {
    return;
  }

  const count = (Number.isFinite(left) ? 1 : 0) + (Number.isFinite(right) ? 1 : 0);
  if (count > 0) {
    out.push(((left ?? 0) + (right ?? 0)) / count);
  }
}

function normalizePercent(value) {
  if (!Number.isFinite(value ?? NaN)) return null;
  if (Math.abs(value) <= 1.05 && Math.abs(value) >= 0) return value * 100;
  return value;
}

function addToSeries(series, value) {
  if (Number.isFinite(value ?? NaN)) series.push(value);
}

function latestUpdatedAt(rows) {
  const timestamps = rows
    .map((row) => row.updated_at)
    .filter((value) => typeof value === "string" && value.length > 0)
    .map((value) => new Date(value).getTime())
    .filter((value) => Number.isFinite(value));

  if (!timestamps.length) return new Date().toISOString();
  const latest = Math.max(...timestamps);
  return new Date(latest).toISOString();
}

async function fetchJson(url, init) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    const text = await response.text();
    let payload = null;

    try {
      payload = text ? JSON.parse(text) : null;
    } catch {
      payload = text;
    }

    return {
      ok: response.ok,
      status: response.status,
      payload,
    };
  } catch (error) {
    if (error?.name === "AbortError") {
      return { ok: false, status: 504, payload: { error: "Request timed out" } };
    }
    throw error;
  } finally {
    clearTimeout(timeoutId);
  }
}

function buildHeaders() {
  return {
    apikey: SUPABASE_KEY,
    Authorization: `Bearer ${SUPABASE_KEY}`,
    "Content-Type": "application/json",
  };
}

async function loadTrends(pLeague, pLayer, pMinRate, pLimit) {
  const rpcUrl = `${SUPABASE_URL}/rest/v1/rpc/get_trends`;
  const body = {
    ...(pLeague ? { p_league: pLeague } : {}),
    ...(pLayer ? { p_layer: pLayer } : {}),
    p_min_rate: pMinRate,
    p_limit: pLimit,
  };

  const rpc = await fetchJson(rpcUrl, {
    method: "POST",
    headers: buildHeaders(),
    body: JSON.stringify(body),
  });

  if (!rpc.ok) {
    throw new Error(
      `get_trends RPC failed (${rpc.status}): ${typeof rpc.payload === "string" ? rpc.payload : JSON.stringify(rpc.payload)}`,
    );
  }

  const rows = Array.isArray(rpc.payload) ? rpc.payload.map(sanitizeRow) : [];
  const deduped = Array.from(
    new Map(
      rows.map((row) => [
        `${row.team.toLowerCase()}|${row.league.toLowerCase()}|${row.layer}|${row.trend}`,
        row,
      ]),
    ).values(),
  );
  deduped.sort(sortByLeagueAndLayer);
  return deduped;
}

async function loadMatchFeedSummary() {
  const feedUrl = `${SUPABASE_URL}/rest/v1/match_feed?status=eq.finished&select=*`;
  const feed = await fetchJson(feedUrl, {
    method: "GET",
    headers: buildHeaders(),
  });

  if (!feed.ok || !Array.isArray(feed.payload) || !feed.payload.length) {
    return {
      avgGoals: null,
      avgCorners: null,
      avgCards: null,
      avgPassPct: null,
      avgShotAccuracy: null,
      overRoi: null,
      homeAtsRoi: null,
    };
  }

  const goals = [];
  const corners = [];
  const cards = [];
  const pass = [];
  const shot = [];
  const over = [];
  const ats = [];

  for (const row of feed.payload) {
    addTotalFromPair(row, ["home_score"], ["away_score"], goals);
    addTotalFromPair(row, ["home_goals"], ["away_goals"], goals);
    addTotalFromPair(row, ["home_team_score"], ["away_team_score"], goals);
    addTotalFromPair(row, ["goals_scored"], ["goals_allowed"], goals);

    addTotalFromPair(row, ["home_corners"], ["away_corners"], corners);
    addTotalFromPair(row, ["corners"], ["total_corners"], corners);

    addTotalFromPair(row, ["home_cards"], ["away_cards"], cards);
    addTotalFromPair(row, ["home_yellow_cards"], ["away_yellow_cards"], cards);
    addTotalFromPair(row, ["team_cards"], ["opponent_cards"], cards);

    addAverageFromPair(row, ["home_pass_pct"], ["away_pass_pct"], pass);
    addAverageFromPair(row, ["home_pass_percentage"], ["away_pass_percentage"], pass);

    addAverageFromPair(row, ["home_shot_accuracy"], ["away_shot_accuracy"], shot);
    addAverageFromPair(row, ["home_shots_accuracy"], ["away_shots_accuracy"], shot);

    addToSeries(over, normalizePercent(readNumber(row, ["over_roi", "over_roi_pct", "over_roi_percentage"])));
    addToSeries(ats, normalizePercent(readNumber(row, ["home_ats_roi", "home_ats_roi_pct", "home_ats_roi_percentage"])));
  }

  return {
    avgGoals: round1(average(goals)),
    avgCorners: round1(average(corners)),
    avgCards: round1(average(cards)),
    avgPassPct: round1(average(pass)),
    avgShotAccuracy: round1(average(shot)),
    overRoi: round1(average(over)),
    homeAtsRoi: round1(average(ats)),
  };
}

async function loadTeamLogos(rows) {
  const logoUrl = `${SUPABASE_URL}/rest/v1/team_logos?select=team_name,league_id,logo_url`;
  const logosResp = await fetchJson(logoUrl, {
    method: "GET",
    headers: buildHeaders(),
  });

  if (!logosResp.ok || !Array.isArray(logosResp.payload)) return rows;

  const logoMap = new Map();
  for (const logo of logosResp.payload) {
    const team = readString(logo, ["team_name"]);
    const league = readString(logo, ["league_id"]);
    const url = readString(logo, ["logo_url"]);
    if (!team || !league || !url) continue;
    logoMap.set(normalizeTeamKey(league, team), url);
  }

  for (const row of rows) {
    row.logo_url = logoMap.get(normalizeTeamKey(row.league, row.team)) || null;
  }

  return rows;
}

module.exports = async function handler(req, res) {
  if (handleOptions(req, res, CACHE.TRENDS)) {
    return;
  }

  setApiHeaders(res, CACHE.TRENDS);

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return sendError(res, 405, "METHOD_NOT_ALLOWED", "Use GET for this endpoint.", CACHE.TRENDS);
  }

  if (!SUPABASE_KEY || !SUPABASE_URL) {
    return sendError(
      res,
      503,
      "TREND_API_ERROR",
      "SUPABASE credentials are missing. Set SUPABASE_URL and one of SUPABASE_ANON_KEY or SUPABASE_SERVICE_ROLE_KEY.",
      CACHE.NO_STORE,
    );
  }

  const query = req.query || {};
  const minRate = clampNumber(query.p_min_rate || query.minRate, DEFAULT_MIN_RATE, 50, 100);
  const limit = clampNumber(query.p_limit || query.limit, DEFAULT_LIMIT, 25, MAX_LIMIT);
  const layer = normalizeLayer(query.p_layer || query.layer);
  const league = normalizeLayer(query.p_league || query.league);

  try {
    const rows = await loadTrends(league, layer, minRate, limit);
    const rowsWithLogos = await loadTeamLogos(rows);
    const metrics = await loadMatchFeedSummary();

    return sendJson(
      res,
      200,
      {
        updatedAt: latestUpdatedAt(rowsWithLogos),
        sourceLabel: rowsWithLogos.length > 0 ? "Powered by get_trends RPC" : "Trend feed unavailable",
        metrics,
        rows: rowsWithLogos,
        layers: uniqueSorted(rowsWithLogos.map((row) => row.layer)),
        leagues: uniqueSorted(rowsWithLogos.map((row) => row.league)),
      },
      CACHE.TRENDS,
    );
  } catch (error) {
    return sendError(res, 502, "TREND_API_ERROR", error?.message || "Trend service error", CACHE.NO_STORE);
  }
};

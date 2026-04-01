#!/usr/bin/env node

/**
 * WC26 ESPN drain pipeline for qffzvrnbzabcokqqrwbv.
 *
 * WORKING ESPN ENDPOINTS (verified 2026-04-01):
 * - https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard
 * - https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams
 * - https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/164
 * - https://site.api.espn.com/apis/v2/sports/soccer/fifa.world/standings
 * - https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard?dates=20260611-20260719
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/1/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/2/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/3/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/4/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/5/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/6/events?limit=200
 * - https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world/seasons/2026/types/7/events?limit=200
 *
 * Known non-working endpoint (kept for probe logging):
 * - https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/futures (404 at verification time)
 */

import { execSync } from "node:child_process";

const PROJECT_REF = "qffzvrnbzabcokqqrwbv";
const SUPABASE_URL =
  process.env.SUPABASE_URL?.trim() || `https://${PROJECT_REF}.supabase.co`;
const REST_BASE = `${SUPABASE_URL}/rest/v1`;
const ESPN_SITE_BASE = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world";
const ESPN_CORE_BASE = "https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.world";
const QUALIFY_STORY_URL =
  "https://www.espn.com/soccer/story/_/id/40297462/2026-world-cup-how-nations-world-qualify";

const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2_000;
const CONCURRENCY = 8;

const STAGE_BY_TYPE = {
  "1": "group",
  "2": "r32",
  "3": "r16",
  "4": "qf",
  "5": "sf",
  "6": "third_place",
  "7": "final",
};

const VENUE_ALIASES = {
  "Estadio Banorte": "Estadio Azteca",
};

const COACH_KEY_PATTERN = /(coach|manager|staff)/i;
const BOOKMAKER_ALIASES = {
  betmgm: "BetMGM",
  caesars: "Caesars",
  draftkings: "DraftKings",
  espn: "ESPN",
  fanduel: "FanDuel",
  kalshi: "Kalshi",
  polymarket: "Polymarket",
};

const PLACEHOLDER_REPLACEMENTS = [
  {
    previous_slug: "uefa-playoff-a",
    slug: "bih",
    name: "Bosnia and Herzegovina",
    fifa_code: "BIH",
    confederation: "UEFA",
  },
  {
    previous_slug: "uefa-playoff-b",
    slug: "swe",
    name: "Sweden",
    fifa_code: "SWE",
    confederation: "UEFA",
  },
  {
    previous_slug: "uefa-playoff-c",
    slug: "tur",
    name: "Türkiye",
    fifa_code: "TUR",
    confederation: "UEFA",
  },
  {
    previous_slug: "uefa-playoff-d",
    slug: "cze",
    name: "Czechia",
    fifa_code: "CZE",
    confederation: "UEFA",
  },
  {
    previous_slug: "intercon-playoff-1",
    slug: "cod",
    name: "DR Congo",
    fifa_code: "COD",
    confederation: "CAF",
  },
  {
    previous_slug: "intercon-playoff-2",
    slug: "irq",
    name: "Iraq",
    fifa_code: "IRQ",
    confederation: "AFC",
  },
];

const ET_FORMATTER = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function toEtIso(utcIso) {
  const date = new Date(utcIso);
  const parts = ET_FORMATTER.formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const year = Number(byType.year);
  const month = Number(byType.month);
  const day = Number(byType.day);
  const hour = Number(byType.hour);
  const minute = Number(byType.minute);
  const second = Number(byType.second);

  const etAsUtcMs = Date.UTC(year, month - 1, day, hour, minute, second);
  const offsetMinutes = Math.round((etAsUtcMs - date.getTime()) / 60_000);
  const sign = offsetMinutes <= 0 ? "-" : "+";
  const absOffset = Math.abs(offsetMinutes);
  const offsetHours = pad2(Math.floor(absOffset / 60));
  const offsetMins = pad2(absOffset % 60);

  return `${year}-${pad2(month)}-${pad2(day)}T${pad2(hour)}:${pad2(minute)}:${pad2(second)}${sign}${offsetHours}:${offsetMins}`;
}

function parseJsonOrNull(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function normalizeForLookup(value) {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function normalizeEspnRefUrl(value) {
  if (typeof value !== "string") {
    return null;
  }
  return value.replace(/^http:\/\//i, "https://");
}

function parseLeagueFromEspnUrl(url) {
  if (typeof url !== "string") {
    return null;
  }
  const match = url.match(/\/league\/([A-Z0-9._-]+)/i);
  return match ? match[1].toLowerCase() : null;
}

function parseLeaguesDropdownFromSquadHtml(html) {
  if (typeof html !== "string") {
    return [];
  }
  const match = html.match(/"leaguesDropdown":(\[[\s\S]*?\]),"sbpg"/);
  if (!match) {
    return [];
  }
  const parsed = parseJsonOrNull(match[1]);
  if (!Array.isArray(parsed)) {
    return [];
  }
  const leagues = parsed
    .map((entry) => parseLeagueFromEspnUrl(entry?.url) || String(entry?.value || "").toLowerCase())
    .filter(Boolean);
  return [...new Set(leagues)];
}

function extractNameParts(entry) {
  if (!entry || typeof entry !== "object") {
    return null;
  }
  const first = typeof entry.firstName === "string" ? entry.firstName.trim() : "";
  const last = typeof entry.lastName === "string" ? entry.lastName.trim() : "";
  const byParts = [first, last].filter(Boolean).join(" ").trim();
  if (byParts) {
    return byParts;
  }
  for (const key of ["displayName", "fullName", "name", "shortName"]) {
    const value = entry[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function flattenCoachLikeEntries(value) {
  if (!value) {
    return [];
  }
  if (Array.isArray(value)) {
    return value;
  }
  if (typeof value === "object") {
    if (Array.isArray(value.items)) {
      return value.items;
    }
    return [value];
  }
  return [];
}

function collectCoachLikePaths(payload, prefix = "$", out = new Set()) {
  if (payload === null || payload === undefined) {
    return out;
  }
  if (Array.isArray(payload)) {
    for (let index = 0; index < payload.length; index += 1) {
      collectCoachLikePaths(payload[index], `${prefix}[${index}]`, out);
    }
    return out;
  }
  if (typeof payload === "object") {
    for (const [key, value] of Object.entries(payload)) {
      const path = `${prefix}.${key}`;
      if (COACH_KEY_PATTERN.test(key)) {
        out.add(path);
      }
      collectCoachLikePaths(value, path, out);
    }
  }
  return out;
}

function collectCoachNames(payload, prefix = "$", result = []) {
  if (payload === null || payload === undefined) {
    return result;
  }
  if (Array.isArray(payload)) {
    for (let index = 0; index < payload.length; index += 1) {
      collectCoachNames(payload[index], `${prefix}[${index}]`, result);
    }
    return result;
  }
  if (typeof payload !== "object") {
    return result;
  }

  for (const [key, value] of Object.entries(payload)) {
    const path = `${prefix}.${key}`;
    if (COACH_KEY_PATTERN.test(key)) {
      const entries = flattenCoachLikeEntries(value);
      for (const entry of entries) {
        const name = extractNameParts(entry);
        if (name) {
          result.push({ name, path });
        }
      }
    }
    collectCoachNames(value, path, result);
  }
  return result;
}

function parseSlotLabel(displayName) {
  const normalized = String(displayName || "").trim();
  if (!normalized) {
    return null;
  }

  const groupWinner = normalized.match(/^Group\s+([A-L])\s+Winner$/i);
  if (groupWinner) {
    return `${groupWinner[1].toUpperCase()}1`;
  }

  const groupSecond = normalized.match(/^Group\s+([A-L])\s+2nd\s+Place$/i);
  if (groupSecond) {
    return `${groupSecond[1].toUpperCase()}2`;
  }

  const thirdPlace = normalized.match(/^Third\s+Place\s+Group\s+([A-L](?:\/[A-L])*)$/i);
  if (thirdPlace) {
    return `3${thirdPlace[1].toUpperCase()}`;
  }

  const round32Winner = normalized.match(/^Round\s+of\s+32\s+(\d+)\s+Winner$/i);
  if (round32Winner) {
    return `Winner R32-${round32Winner[1]}`;
  }

  const round16Winner = normalized.match(/^Round\s+of\s+16\s+(\d+)\s+Winner$/i);
  if (round16Winner) {
    return `Winner R16-${round16Winner[1]}`;
  }

  const quarterfinalWinner = normalized.match(/^Quarterfinal\s+(\d+)\s+Winner$/i);
  if (quarterfinalWinner) {
    return `Winner QF-${quarterfinalWinner[1]}`;
  }

  const semifinalOutcome = normalized.match(/^Semifinal\s+(\d+)\s+(Winner|Loser)$/i);
  if (semifinalOutcome) {
    const side = semifinalOutcome[2].toLowerCase() === "winner" ? "Winner" : "Loser";
    return `${side} SF-${semifinalOutcome[1]}`;
  }

  return null;
}

function inferWcFuturesMarket(value) {
  const normalized = normalizeForLookup(value);
  if (!normalized) {
    return null;
  }
  if (normalized.includes("group") && normalized.includes("winner")) {
    return "group_winner";
  }
  if (normalized.includes("outright") || normalized.includes("winner")) {
    return "outright_winner";
  }
  return null;
}

function normalizeBookmaker(value) {
  const normalized = normalizeForLookup(value);
  if (!normalized) {
    return null;
  }
  return BOOKMAKER_ALIASES[normalized] || value;
}

function normalizeImpliedProbability(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return null;
  }
  if (numeric > 0 && numeric < 1) {
    return Number(numeric.toFixed(6));
  }
  if (numeric >= 1 && numeric <= 100) {
    return Number((numeric / 100).toFixed(6));
  }
  return null;
}

function resolveServiceRoleKey() {
  const envKey =
    process.env.SUPABASE_SERVICE_ROLE_KEY?.trim() ||
    process.env.SUPABASE_SERVICE_KEY?.trim();
  if (envKey) {
    return envKey;
  }

  const output = execSync(
    `supabase projects api-keys --project-ref ${PROJECT_REF} -o json`,
    { encoding: "utf8" },
  );
  const parsed = JSON.parse(output);
  const found = parsed.find(
    (entry) => entry?.name === "service_role" && typeof entry.api_key === "string",
  );
  if (found?.api_key) {
    return found.api_key;
  }

  throw new Error(
    "Missing SUPABASE service role key. Set SUPABASE_SERVICE_ROLE_KEY or login to Supabase CLI.",
  );
}

async function requestWithRetry(url, options = {}, label = url, allowStatuses = []) {
  let lastStatus = null;
  let lastBody = "";
  let lastError = null;

  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(url, options);
      const body = await response.text();
      const parsed = parseJsonOrNull(body);
      const allowed = allowStatuses.includes(response.status);
      if (response.ok || allowed) {
        return {
          ok: response.ok,
          status: response.status,
          body,
          json: parsed,
        };
      }

      lastStatus = response.status;
      lastBody = body;
      if (attempt < RETRY_ATTEMPTS) {
        console.warn(
          `[retry ${attempt}/${RETRY_ATTEMPTS}] ${label} failed with HTTP ${response.status}`,
        );
        await sleep(RETRY_DELAY_MS);
      }
    } catch (error) {
      lastError = error;
      if (attempt < RETRY_ATTEMPTS) {
        console.warn(
          `[retry ${attempt}/${RETRY_ATTEMPTS}] ${label} threw ${error?.message || String(error)}`,
        );
        await sleep(RETRY_DELAY_MS);
      }
    }
  }

  if (lastError) {
    throw new Error(`${label} failed after ${RETRY_ATTEMPTS} attempts: ${lastError.message}`);
  }

  throw new Error(
    `${label} failed after ${RETRY_ATTEMPTS} attempts: HTTP ${lastStatus} ${lastBody.slice(0, 240)}`,
  );
}

function authHeaders(serviceKey, extra = {}) {
  return {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    ...extra,
  };
}

async function restSelect(serviceKey, table, query) {
  const url = `${REST_BASE}/${table}?${query}`;
  const response = await requestWithRetry(url, { headers: authHeaders(serviceKey) }, `SELECT ${table}`);
  if (!Array.isArray(response.json)) {
    throw new Error(`Unexpected SELECT response for ${table}: ${response.body.slice(0, 240)}`);
  }
  return response.json;
}

async function restPatch(serviceKey, table, filterQuery, payload) {
  const url = `${REST_BASE}/${table}?${filterQuery}`;
  const response = await requestWithRetry(
    url,
    {
      method: "PATCH",
      headers: authHeaders(serviceKey, {
        "Content-Type": "application/json",
        Prefer: "return=representation",
      }),
      body: JSON.stringify(payload),
    },
    `PATCH ${table}`,
  );
  if (!Array.isArray(response.json)) {
    throw new Error(`Unexpected PATCH response for ${table}: ${response.body.slice(0, 240)}`);
  }
  return response.json;
}

async function restUpsert(serviceKey, table, rows, onConflict) {
  if (!rows.length) {
    return [];
  }
  const conflict = onConflict ? `?on_conflict=${encodeURIComponent(onConflict)}` : "";
  const url = `${REST_BASE}/${table}${conflict}`;
  const response = await requestWithRetry(
    url,
    {
      method: "POST",
      headers: authHeaders(serviceKey, {
        "Content-Type": "application/json",
        Prefer: "resolution=merge-duplicates,return=representation",
      }),
      body: JSON.stringify(rows),
    },
    `UPSERT ${table}`,
  );
  if (!Array.isArray(response.json)) {
    throw new Error(`Unexpected UPSERT response for ${table}: ${response.body.slice(0, 240)}`);
  }
  return response.json;
}

async function restInsert(serviceKey, table, rows) {
  if (!rows.length) {
    return [];
  }
  const url = `${REST_BASE}/${table}`;
  const response = await requestWithRetry(
    url,
    {
      method: "POST",
      headers: authHeaders(serviceKey, {
        "Content-Type": "application/json",
        Prefer: "return=representation",
      }),
      body: JSON.stringify(rows),
    },
    `INSERT ${table}`,
  );
  if (!Array.isArray(response.json)) {
    throw new Error(`Unexpected INSERT response for ${table}: ${response.body.slice(0, 240)}`);
  }
  return response.json;
}

async function restDelete(serviceKey, table, filterQuery) {
  const url = `${REST_BASE}/${table}?${filterQuery}`;
  await requestWithRetry(
    url,
    {
      method: "DELETE",
      headers: authHeaders(serviceKey, {
        Prefer: "return=minimal",
      }),
    },
    `DELETE ${table}`,
  );
}

function parseTeamIdFromRef(ref) {
  if (typeof ref !== "string") {
    return null;
  }
  const match = ref.match(/\/teams\/(\d+)/);
  return match ? Number(match[1]) : null;
}

function parseEventIdFromRef(ref) {
  if (typeof ref !== "string") {
    return null;
  }
  const match = ref.match(/\/events\/(\d+)/);
  return match ? Number(match[1]) : null;
}

function numericRankOrMax(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric > 0) {
    return numeric;
  }
  return 99_999;
}

function compareSlugsByStrength(rankBySlug, left, right) {
  const leftRank = numericRankOrMax(rankBySlug.get(left));
  const rightRank = numericRankOrMax(rankBySlug.get(right));
  if (leftRank !== rightRank) {
    return leftRank - rightRank;
  }
  return String(left || "").localeCompare(String(right || ""));
}

function pickBestSlug(rankBySlug, slugs) {
  const unique = [...new Set(slugs.filter(Boolean))];
  if (!unique.length) {
    return null;
  }
  unique.sort((left, right) => compareSlugsByStrength(rankBySlug, left, right));
  return unique[0];
}

function buildGroupSeeds(wc26Teams) {
  const grouped = new Map();
  for (const team of wc26Teams) {
    const groupLetter = String(team.group_letter || "").toUpperCase();
    if (!groupLetter) {
      continue;
    }
    if (!grouped.has(groupLetter)) {
      grouped.set(groupLetter, []);
    }
    grouped.get(groupLetter).push({
      slug: team.slug,
      fifa_rank: team.fifa_rank,
    });
  }

  const seeds = new Map();
  for (const [groupLetter, teams] of grouped.entries()) {
    const sorted = [...teams].sort((left, right) => {
      const rankDelta =
        numericRankOrMax(left.fifa_rank) - numericRankOrMax(right.fifa_rank);
      if (rankDelta !== 0) {
        return rankDelta;
      }
      return String(left.slug || "").localeCompare(String(right.slug || ""));
    });

    const fallback = sorted[0]?.slug || null;
    seeds.set(groupLetter, {
      winner: sorted[0]?.slug || fallback,
      second: sorted[1]?.slug || fallback,
      third: sorted[2]?.slug || fallback,
    });
  }

  return seeds;
}

function resolvePlaceholderDisplayToSlug(displayName, groupSeeds, rankBySlug, outcomesByMatchNumber) {
  const normalized = String(displayName || "").trim();
  if (!normalized) {
    return null;
  }

  const groupWinner = normalized.match(/^Group\s+([A-L])\s+Winner$/i);
  if (groupWinner) {
    return groupSeeds.get(groupWinner[1].toUpperCase())?.winner || null;
  }

  const groupSecond = normalized.match(/^Group\s+([A-L])\s+2nd\s+Place$/i);
  if (groupSecond) {
    return groupSeeds.get(groupSecond[1].toUpperCase())?.second || null;
  }

  const thirdPlace = normalized.match(/^Third\s+Place\s+Group\s+([A-L](?:\/[A-L])*)$/i);
  if (thirdPlace) {
    const groups = thirdPlace[1].split("/").map((value) => value.toUpperCase());
    const candidates = groups
      .map((groupLetter) => groupSeeds.get(groupLetter)?.third || null)
      .filter(Boolean);
    return pickBestSlug(rankBySlug, candidates);
  }

  const round32Winner = normalized.match(/^Round\s+of\s+32\s+(\d+)\s+Winner$/i);
  if (round32Winner) {
    const matchNumber = 72 + Number(round32Winner[1]);
    return outcomesByMatchNumber.get(matchNumber)?.winner || null;
  }

  const round16Winner = normalized.match(/^Round\s+of\s+16\s+(\d+)\s+Winner$/i);
  if (round16Winner) {
    const matchNumber = 88 + Number(round16Winner[1]);
    return outcomesByMatchNumber.get(matchNumber)?.winner || null;
  }

  const quarterfinalWinner = normalized.match(/^Quarterfinal\s+(\d+)\s+Winner$/i);
  if (quarterfinalWinner) {
    const matchNumber = 96 + Number(quarterfinalWinner[1]);
    return outcomesByMatchNumber.get(matchNumber)?.winner || null;
  }

  const semifinalOutcome = normalized.match(/^Semifinal\s+(\d+)\s+(Winner|Loser)$/i);
  if (semifinalOutcome) {
    const matchNumber = 100 + Number(semifinalOutcome[1]);
    const outcome = outcomesByMatchNumber.get(matchNumber) || null;
    if (!outcome) {
      return null;
    }
    return semifinalOutcome[2].toLowerCase() === "winner"
      ? outcome.winner
      : outcome.loser;
  }

  return null;
}

function findNumericRank(teamDetail) {
  const stats = teamDetail?.team?.record?.items?.[0]?.stats;
  if (!Array.isArray(stats)) {
    return null;
  }
  const rankStat = stats.find((stat) => stat?.name === "rank");
  if (!rankStat) {
    return null;
  }
  const value = Number(rankStat.value);
  return Number.isFinite(value) ? Math.trunc(value) : null;
}

function pickCoachName(candidates) {
  const unique = [...new Set(candidates.map((candidate) => candidate?.name).filter(Boolean))];
  return unique[0] || null;
}

function buildTeamSlugLookup(wc26Teams) {
  const lookup = new Map();
  for (const team of wc26Teams) {
    lookup.set(normalizeForLookup(team.slug), team.slug);
    lookup.set(normalizeForLookup(team.fifa_code), team.slug);
    lookup.set(normalizeForLookup(team.name), team.slug);
  }

  const aliases = {
    "congodr": "cod",
    "drcongo": "cod",
    "ivorycoast": "civ",
    "cotedivoire": "civ",
    "iriran": "irn",
    "southkorea": "kor",
    "korearepublic": "kor",
    "unitedstates": "usa",
    usa: "usa",
    turkey: "tur",
    czechrepublic: "cze",
  };

  for (const [key, slug] of Object.entries(aliases)) {
    if (!lookup.has(key)) {
      lookup.set(key, slug);
    }
  }

  return lookup;
}

function maybeMapFutureTeamToSlug(teamLabel, lookup) {
  const normalized = normalizeForLookup(teamLabel);
  if (!normalized) {
    return null;
  }
  return lookup.get(normalized) || null;
}

function isWorldCupLeagueValue(value) {
  const normalized = normalizeForLookup(value);
  if (!normalized) {
    return false;
  }
  return normalized.includes("fifaworld") || normalized.includes("worldcup");
}

function collectCoachAudit(payloadMap) {
  const names = [];
  const paths = new Set();

  for (const [source, payload] of payloadMap.entries()) {
    if (!payload) {
      continue;
    }
    const sourcePaths = collectCoachLikePaths(payload);
    for (const path of sourcePaths) {
      paths.add(`${source}:${path}`);
    }
    const sourceNames = collectCoachNames(payload);
    for (const entry of sourceNames) {
      names.push({
        name: entry.name,
        path: `${source}:${entry.path}`,
      });
    }
  }

  return {
    coach_name: pickCoachName(names),
    coach_paths: [...paths].sort(),
    coach_name_paths: names.map((entry) => `${entry.path}=${entry.name}`),
  };
}

async function fetchCoachPayloadFromCoreTeam(coreTeamPayload) {
  const ref = normalizeEspnRefUrl(coreTeamPayload?.coaches?.$ref);
  if (!ref) {
    return null;
  }
  const coachesListResponse = await requestWithRetry(
    ref,
    {},
    `fetch coaches list ${ref}`,
    [404, 500],
  );
  if (!coachesListResponse.ok) {
    return { list: coachesListResponse.json || null, details: [] };
  }

  const detailRefs = Array.isArray(coachesListResponse.json?.items)
    ? coachesListResponse.json.items
        .map((item) => normalizeEspnRefUrl(item?.$ref))
        .filter(Boolean)
    : [];
  const detailPayloads = await mapWithConcurrency(detailRefs, async (detailRef) => {
    const detailResponse = await requestWithRetry(
      detailRef,
      {},
      `fetch coach detail ${detailRef}`,
      [404, 500],
    );
    return detailResponse.ok ? detailResponse.json : null;
  });

  return {
    list: coachesListResponse.json || null,
    details: detailPayloads.filter(Boolean),
  };
}

async function fetchAlternateLeagueCoachPayloads(detailPayload, teamId) {
  const squadLink = Array.isArray(detailPayload?.team?.links)
    ? detailPayload.team.links.find((link) => Array.isArray(link?.rel) && link.rel.includes("squad"))
    : null;
  const squadUrl = squadLink?.href || `https://www.espn.com/soccer/team/squad/_/id/${teamId}`;
  const squadHtml = await requestWithRetry(
    squadUrl,
    {},
    `fetch squad html ${teamId}`,
    [404],
  );
  if (!squadHtml.ok || typeof squadHtml.body !== "string") {
    return [];
  }

  const leagues = parseLeaguesDropdownFromSquadHtml(squadHtml.body)
    .filter((league) => league !== "fifa.world");
  if (!leagues.length) {
    return [];
  }

  const payloads = await mapWithConcurrency(leagues, async (league) => {
    const teamResponse = await requestWithRetry(
      `https://sports.core.api.espn.com/v2/sports/soccer/leagues/${league}/teams/${teamId}`,
      {},
      `fetch core team alt league ${league}/${teamId}`,
      [404],
    );
    if (!teamResponse.ok) {
      return null;
    }
    const coachPayload = await fetchCoachPayloadFromCoreTeam(teamResponse.json);
    if (!coachPayload) {
      return {
        league,
        core_team: teamResponse.json,
      };
    }
    return {
      league,
      core_team: teamResponse.json,
      coaches_list: coachPayload.list,
      coaches_details: coachPayload.details,
    };
  });

  return payloads.filter(Boolean);
}

function toRosterJson(rosterPayload) {
  const athletes = Array.isArray(rosterPayload?.athletes) ? rosterPayload.athletes : [];
  const players = athletes
    .map((athlete) => {
      const espnId = Number(athlete?.id);
      return {
        name:
          (typeof athlete?.displayName === "string" && athlete.displayName) ||
          (typeof athlete?.fullName === "string" && athlete.fullName) ||
          null,
        position:
          (typeof athlete?.position?.abbreviation === "string" &&
            athlete.position.abbreviation) ||
          null,
        club:
          (typeof athlete?.defaultTeam?.displayName === "string" &&
            athlete.defaultTeam.displayName) ||
          (typeof athlete?.defaultTeam?.name === "string" && athlete.defaultTeam.name) ||
          null,
        espn_id: Number.isFinite(espnId) ? espnId : null,
      };
    })
    .filter((player) => player.name);
  return { players };
}

async function mapWithConcurrency(items, mapper) {
  const results = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await mapper(items[index], index);
    }
  }

  const workers = Array.from({ length: Math.min(CONCURRENCY, items.length) }, () =>
    worker(),
  );
  await Promise.all(workers);
  return results;
}

async function probeEspnEndpoints() {
  const endpoints = [
    `${ESPN_SITE_BASE}/scoreboard`,
    `${ESPN_SITE_BASE}/teams`,
    `${ESPN_SITE_BASE}/teams/164`,
    "https://site.api.espn.com/apis/v2/sports/soccer/fifa.world/standings",
    `${ESPN_SITE_BASE}/scoreboard?dates=20260611-20260719`,
    `${ESPN_SITE_BASE}/futures`,
    `${ESPN_CORE_BASE}/seasons/2026/types/1/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/2/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/3/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/4/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/5/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/6/events?limit=200`,
    `${ESPN_CORE_BASE}/seasons/2026/types/7/events?limit=200`,
  ];

  console.log("\n[step 1] probing ESPN endpoints");
  const statuses = [];
  for (const url of endpoints) {
    const response = await requestWithRetry(
      url,
      {},
      `probe ${url}`,
      [404],
    );
    statuses.push({
      url,
      status: response.status,
      ok: response.ok,
    });
  }

  for (const status of statuses) {
    const marker = status.ok ? "ok" : "fail";
    console.log(`  [${marker}] ${status.status} ${status.url}`);
  }
}

async function verifyInterconWinners() {
  console.log("\n[step 0] verifying intercontinental playoff winners from ESPN story");
  const article = await requestWithRetry(QUALIFY_STORY_URL, {}, "fetch qualification story");
  const lower = article.body.toLowerCase();
  const hasDrCongo = lower.includes("dr congo") && lower.includes("interconfederation playoffs");
  const hasIraq = lower.includes("iraq") && lower.includes("interconfederation playoffs");
  if (!hasDrCongo || !hasIraq) {
    throw new Error(
      "Could not verify DR Congo and Iraq from ESPN qualifying story content.",
    );
  }
  console.log("  verified mentions for DR Congo and Iraq in interconfederation playoff path");
}

async function replacePlaceholderTeams(serviceKey) {
  console.log("\n[step 0] replacing placeholder WC26 teams");
  for (const replacement of PLACEHOLDER_REPLACEMENTS) {
    const flagCode = replacement.fifa_code.toLowerCase();
    const payload = {
      slug: replacement.slug,
      name: replacement.name,
      fifa_code: replacement.fifa_code,
      flag_uri: `https://a.espncdn.com/i/teamlogos/countries/500/${flagCode}.png`,
      confederation: replacement.confederation,
      is_playoff_pending: false,
    };
    const result = await restPatch(
      serviceKey,
      "wc26_teams",
      `slug=eq.${encodeURIComponent(replacement.previous_slug)}`,
      payload,
    );
    if (result.length) {
      console.log(
        `  updated ${replacement.previous_slug} -> ${replacement.slug} (${replacement.name})`,
      );
      continue;
    }

    const alreadyUpdated = await restSelect(
      serviceKey,
      "wc26_teams",
      `select=slug,name,fifa_code&slug=eq.${encodeURIComponent(replacement.slug)}&limit=1`,
    );
    if (alreadyUpdated.length) {
      console.log(
        `  already updated: ${replacement.previous_slug} -> ${replacement.slug}`,
      );
      continue;
    }

    throw new Error(
      `No row found for replacement ${replacement.previous_slug} and target ${replacement.slug} missing`,
    );
  }
}

async function fetchEspnTeams() {
  const teamsResponse = await requestWithRetry(
    `${ESPN_SITE_BASE}/teams`,
    {},
    "fetch ESPN teams",
  );
  const teams =
    teamsResponse.json?.sports?.[0]?.leagues?.[0]?.teams?.map((entry) => entry.team) || [];
  if (teams.length !== 48) {
    throw new Error(`Expected 48 ESPN teams, got ${teams.length}`);
  }
  return teams;
}

async function fetchAllEspnEvents() {
  const allRefs = [];
  for (const typeId of ["1", "2", "3", "4", "5", "6", "7"]) {
    const listResponse = await requestWithRetry(
      `${ESPN_CORE_BASE}/seasons/2026/types/${typeId}/events?limit=200`,
      {},
      `fetch type ${typeId} events`,
    );
    const refs = Array.isArray(listResponse.json?.items)
      ? listResponse.json.items.map((item) => item?.$ref).filter(Boolean)
      : [];
    allRefs.push(...refs);
  }
  const uniqueEventIds = [...new Set(allRefs.map(parseEventIdFromRef).filter(Number.isFinite))].sort(
    (a, b) => a - b,
  );
  if (uniqueEventIds.length !== 104) {
    throw new Error(`Expected 104 ESPN event ids, got ${uniqueEventIds.length}`);
  }
  return uniqueEventIds;
}

async function fetchOpenApiSpec(serviceKey) {
  const response = await requestWithRetry(
    `${REST_BASE}/`,
    { headers: authHeaders(serviceKey) },
    "fetch rest openapi",
  );
  return response.json || {};
}

async function upsertFixtureSlotLabelsIfAvailable(serviceKey, slotRows, openapi) {
  if (!slotRows.length) {
    return;
  }
  const hasSlotsTable = Boolean(openapi?.definitions?.wc26_fixture_slots?.properties);
  if (!hasSlotsTable) {
    console.log(
      "  wc26_fixture_slots table not found (schema locked). slot labels logged for migration readiness.",
    );
    const preview = slotRows.slice(0, 10).map((row) => ({
      fixture_id: row.fixture_id,
      home_slot_label: row.home_slot_label,
      away_slot_label: row.away_slot_label,
    }));
    console.log(`  slot-label preview: ${JSON.stringify(preview)}`);
    return;
  }

  const filteredRows = slotRows.filter((row) => row.home_slot_label || row.away_slot_label);
  if (!filteredRows.length) {
    return;
  }

  await restUpsert(serviceKey, "wc26_fixture_slots", filteredRows, "fixture_id");
  console.log(`  upserted ${filteredRows.length} fixture slot-label rows`);
}

async function drainFixtures(serviceKey, wc26Teams, espnTeams, openapi) {
  console.log("\n[step 2] draining fixtures");

  const teamsByCode = new Map(
    wc26Teams.map((team) => [String(team.fifa_code || "").toUpperCase(), team.slug]),
  );
  const rankBySlug = new Map(
    wc26Teams.map((team) => [team.slug, team.fifa_rank]),
  );
  const groupSeeds = buildGroupSeeds(wc26Teams);
  const groupBySlug = new Map(
    wc26Teams.map((team) => [team.slug, team.group_letter]),
  );
  const teamIdToSlug = new Map();
  for (const team of espnTeams) {
    const code = String(team.abbreviation || "").toUpperCase();
    const slug = teamsByCode.get(code) || null;
    teamIdToSlug.set(Number(team.id), slug);
  }

  const venueRows = await restSelect(
    serviceKey,
    "wc26_venues",
    "select=stadium_name,city",
  );
  const cityByVenue = new Map(
    venueRows.map((venue) => [venue.stadium_name, venue.city]),
  );

  const placeholderDisplayByTeamId = new Map();
  async function resolvePlaceholderDisplayName(teamId) {
    if (!Number.isFinite(teamId)) {
      return null;
    }
    if (placeholderDisplayByTeamId.has(teamId)) {
      return placeholderDisplayByTeamId.get(teamId);
    }
    const response = await requestWithRetry(
      `${ESPN_CORE_BASE}/teams/${teamId}`,
      {},
      `fetch placeholder team ${teamId}`,
    );
    const displayName =
      (typeof response.json?.displayName === "string" && response.json.displayName.trim()) ||
      (typeof response.json?.name === "string" && response.json.name.trim()) ||
      null;
    placeholderDisplayByTeamId.set(teamId, displayName);
    return displayName;
  }

  const eventIds = await fetchAllEspnEvents();
  const events = await mapWithConcurrency(eventIds, async (eventId) => {
    const eventResponse = await requestWithRetry(
      `${ESPN_CORE_BASE}/events/${eventId}`,
      {},
      `fetch event ${eventId}`,
    );
    return eventResponse.json;
  });

  const fixturesRaw = events.map((event) => {
    const typeId = String(event?.seasonType?.$ref?.match(/types\/(\d+)/)?.[1] || "");
    const stage = STAGE_BY_TYPE[typeId];
    if (!stage) {
      throw new Error(`Unknown stage type for event ${event?.id}: ${typeId}`);
    }
    const competition = event.competitions?.[0];
    if (!competition) {
      throw new Error(`Missing competition for event ${event?.id}`);
    }
    const home = competition.competitors?.find((item) => item.homeAway === "home");
    const away = competition.competitors?.find((item) => item.homeAway === "away");
    const homeTeamId = parseTeamIdFromRef(home?.team?.$ref);
    const awayTeamId = parseTeamIdFromRef(away?.team?.$ref);

    const rawVenue = competition.venue?.fullName || null;
    const normalizedVenue = rawVenue ? VENUE_ALIASES[rawVenue] || rawVenue : null;
    const normalizedCity = normalizedVenue ? cityByVenue.get(normalizedVenue) || null : null;
    if (!normalizedVenue || !normalizedCity) {
      throw new Error(
        `Missing venue mapping for event ${event?.id} venue=${rawVenue} mapped=${normalizedVenue}`,
      );
    }

    const kickoffUtc = String(event.date);
    const kickoffMs = Date.parse(kickoffUtc);
    return {
      event_id: Number(event.id),
      kickoff_ms: kickoffMs,
      kickoff: toEtIso(kickoffUtc),
      stage,
      home_team_id: homeTeamId,
      away_team_id: awayTeamId,
      venue: normalizedVenue,
      city: normalizedCity,
    };
  });

  fixturesRaw.sort((a, b) => a.kickoff_ms - b.kickoff_ms || a.event_id - b.event_id);
  const outcomesByMatchNumber = new Map();
  const fixtures = [];
  const fixtureSlots = [];
  for (let index = 0; index < fixturesRaw.length; index += 1) {
    const row = fixturesRaw[index];
    const matchNumber = index + 1;
    const isGroup = row.stage === "group";

    let homeSlug =
      Number.isFinite(row.home_team_id) ? teamIdToSlug.get(row.home_team_id) || null : null;
    let awaySlug =
      Number.isFinite(row.away_team_id) ? teamIdToSlug.get(row.away_team_id) || null : null;
    let homeSlotLabel = null;
    let awaySlotLabel = null;

    if (!homeSlug && Number.isFinite(row.home_team_id)) {
      const displayName = await resolvePlaceholderDisplayName(row.home_team_id);
      homeSlotLabel = parseSlotLabel(displayName);
      homeSlug = resolvePlaceholderDisplayToSlug(
        displayName,
        groupSeeds,
        rankBySlug,
        outcomesByMatchNumber,
      );
    }
    if (!awaySlug && Number.isFinite(row.away_team_id)) {
      const displayName = await resolvePlaceholderDisplayName(row.away_team_id);
      awaySlotLabel = parseSlotLabel(displayName);
      awaySlug = resolvePlaceholderDisplayToSlug(
        displayName,
        groupSeeds,
        rankBySlug,
        outcomesByMatchNumber,
      );
    }

    if (!homeSlug || !awaySlug) {
      throw new Error(
        `Could not resolve slugs for event ${row.event_id} stage=${row.stage} home=${row.home_team_id} away=${row.away_team_id}`,
      );
    }

    const groupLetter = isGroup ? groupBySlug.get(homeSlug) || null : null;
    if (isGroup && !groupLetter) {
      throw new Error(
        `Missing group letter for group-stage match ${row.event_id} home_slug=${homeSlug}`,
      );
    }

    let winnerSlug = homeSlug;
    let loserSlug = awaySlug;
    if (compareSlugsByStrength(rankBySlug, awaySlug, homeSlug) < 0) {
      winnerSlug = awaySlug;
      loserSlug = homeSlug;
    }
    outcomesByMatchNumber.set(matchNumber, {
      winner: winnerSlug,
      loser: loserSlug,
    });

    fixtures.push({
      fixture_id: `wc26_${String(matchNumber).padStart(3, "0")}`,
      match_number: matchNumber,
      home_slug: homeSlug,
      away_slug: awaySlug,
      stage: row.stage,
      group_letter: groupLetter,
      venue: row.venue,
      city: row.city,
      kickoff: row.kickoff,
    });

    if (!isGroup) {
      fixtureSlots.push({
        fixture_id: `wc26_${String(matchNumber).padStart(3, "0")}`,
        home_slot_label: homeSlotLabel,
        away_slot_label: awaySlotLabel,
      });
    }
  }

  if (fixtures.length !== 104) {
    throw new Error(`Expected 104 fixture rows, got ${fixtures.length}`);
  }

  await restUpsert(serviceKey, "wc26_fixtures", fixtures, "fixture_id");
  console.log(`  upserted ${fixtures.length} fixtures`);
  await upsertFixtureSlotLabelsIfAvailable(serviceKey, fixtureSlots, openapi);
}

async function drainTeamEnrichment(serviceKey, wc26Teams, espnTeams) {
  console.log("\n[step 3] draining team enrichment");
  const slugByCode = new Map(
    wc26Teams.map((team) => [String(team.fifa_code || "").toUpperCase(), team.slug]),
  );
  const existingBySlug = new Map(wc26Teams.map((team) => [team.slug, team]));

  const teamUpdates = await mapWithConcurrency(espnTeams, async (espnTeam) => {
    const espnId = Number(espnTeam.id);
    const code = String(espnTeam.abbreviation || "").toUpperCase();
    const slug = slugByCode.get(code) || null;
    if (!slug) {
      throw new Error(`No wc26 slug for ESPN team code ${code} (${espnId})`);
    }

    const detailResponse = await requestWithRetry(
      `${ESPN_SITE_BASE}/teams/${espnId}`,
      {},
      `fetch team detail ${espnId}`,
    );
    const rosterResponse = await requestWithRetry(
      `${ESPN_SITE_BASE}/teams/${espnId}/roster`,
      {},
      `fetch team roster ${espnId}`,
    );
    const coreTeamResponse = await requestWithRetry(
      `${ESPN_CORE_BASE}/teams/${espnId}`,
      {},
      `fetch core team ${espnId}`,
      [404],
    );

    const coachPayload = coreTeamResponse.ok
      ? await fetchCoachPayloadFromCoreTeam(coreTeamResponse.json)
      : null;

    const payloadMap = new Map();
    payloadMap.set("site_detail", detailResponse.json || null);
    payloadMap.set("site_roster", rosterResponse.json || null);
    payloadMap.set("core_team", coreTeamResponse.ok ? coreTeamResponse.json : null);
    payloadMap.set("core_coaches_list", coachPayload?.list || null);
    if (Array.isArray(coachPayload?.details)) {
      for (let idx = 0; idx < coachPayload.details.length; idx += 1) {
        payloadMap.set(`core_coach_detail_${idx + 1}`, coachPayload.details[idx]);
      }
    }

    const fifaRank = findNumericRank(detailResponse.json);
    let coachAudit = collectCoachAudit(payloadMap);
    let alternateLeaguePayloads = [];
    if (!coachAudit.coach_name) {
      alternateLeaguePayloads = await fetchAlternateLeagueCoachPayloads(detailResponse.json, espnId);
      for (let idx = 0; idx < alternateLeaguePayloads.length; idx += 1) {
        const payload = alternateLeaguePayloads[idx];
        const prefix = `alt_league_${payload.league || idx + 1}`;
        payloadMap.set(`${prefix}_team`, payload.core_team || null);
        payloadMap.set(`${prefix}_coaches_list`, payload.coaches_list || null);
        if (Array.isArray(payload.coaches_details)) {
          for (let detailIndex = 0; detailIndex < payload.coaches_details.length; detailIndex += 1) {
            payloadMap.set(
              `${prefix}_coach_detail_${detailIndex + 1}`,
              payload.coaches_details[detailIndex],
            );
          }
        }
      }
      coachAudit = collectCoachAudit(payloadMap);
    }
    const rosterJson = toRosterJson(rosterResponse.json);
    const existing = existingBySlug.get(slug) || {};

    return {
      slug,
      fifa_rank: fifaRank ?? existing.fifa_rank ?? null,
      head_coach: coachAudit.coach_name ?? existing.head_coach ?? null,
      roster_json:
        rosterJson.players.length > 0
          ? rosterJson
          : existing.roster_json || { players: [] },
      _audit: {
        slug,
        code,
        team_name: espnTeam.displayName || espnTeam.name || slug,
        coach_name: coachAudit.coach_name,
        coach_paths: coachAudit.coach_paths,
        coach_name_paths: coachAudit.coach_name_paths,
        alt_leagues_checked: alternateLeaguePayloads.map((payload) => payload.league).filter(Boolean),
      },
    };
  });

  await mapWithConcurrency(teamUpdates, async (update) => {
    const { slug, _audit, ...payload } = update;
    const updated = await restPatch(
      serviceKey,
      "wc26_teams",
      `slug=eq.${encodeURIComponent(slug)}`,
      payload,
    );
    if (!updated.length) {
      throw new Error(`No wc26_teams row found for enrichment slug=${slug}`);
    }
    return updated[0];
  });
  console.log(`  updated enrichment fields for ${teamUpdates.length} teams`);

  const audits = teamUpdates.map((update) => update._audit);
  const sourceGaps = audits.filter(
    (audit) =>
      !audit.coach_name &&
      (audit.coach_paths.length === 0 ||
        audit.coach_paths.every((path) => /coaches?\.\$ref$/i.test(path))),
  );
  const parserGaps = audits.filter(
    (audit) =>
      !audit.coach_name &&
      !sourceGaps.some((sourceGap) => sourceGap.slug === audit.slug),
  );

  const pathFrequency = new Map();
  for (const audit of audits) {
    for (const path of audit.coach_paths) {
      pathFrequency.set(path, (pathFrequency.get(path) || 0) + 1);
    }
  }
  const topPaths = [...pathFrequency.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, 30)
    .map(([path, count]) => `${count} ${path}`);

  console.log(
    `  coach extraction audit: parsed=${audits.length - sourceGaps.length - parserGaps.length}, source_gaps=${sourceGaps.length}, parser_gaps=${parserGaps.length}`,
  );
  if (topPaths.length) {
    console.log(`  coach-like keys/paths: ${topPaths.join(" | ")}`);
  }
  if (sourceGaps.length) {
    const summary = sourceGaps
      .map((audit) => `${audit.code}(${audit.team_name})`)
      .join(", ");
    console.log(`  ESPN source gaps (no coach payload): ${summary}`);
  }
  if (parserGaps.length) {
    const summary = parserGaps
      .map((audit) => `${audit.code}(${audit.team_name})`)
      .join(", ");
    console.log(`  parser gaps (coach-like payload but no parsed name): ${summary}`);
  }
}

function americanToDecimal(americanOdds) {
  if (!Number.isFinite(americanOdds) || americanOdds === 0) {
    return null;
  }
  if (americanOdds > 0) {
    return 1 + americanOdds / 100;
  }
  return 1 + 100 / Math.abs(americanOdds);
}

async function drainOddsIfAvailable(serviceKey, wc26Teams) {
  console.log("\n[step 4] draining futures odds from existing odds pipeline");

  const futuresRows = await restSelect(
    serviceKey,
    "futures_odds",
    "select=league,season,market,team,odds_american,implied_prob,bookmaker,snapshot_date,created_at,updated_at,notes&limit=10000",
  );

  const wcFuturesRows = futuresRows.filter((row) => {
    const leagueFlag = isWorldCupLeagueValue(row.league);
    const marketFlag = isWorldCupLeagueValue(row.market) || inferWcFuturesMarket(row.market);
    const notesFlag = isWorldCupLeagueValue(row.notes);
    return Boolean(leagueFlag || marketFlag || notesFlag);
  });

  if (!wcFuturesRows.length) {
    console.log(
      "  no World Cup rows found in futures_odds (existing odds pipeline currently not mapped for WC26), skipping insert",
    );
    return 0;
  }

  const teamLookup = buildTeamSlugLookup(wc26Teams);
  const rows = [];
  for (const futures of wcFuturesRows) {
    const market = inferWcFuturesMarket(futures.market) || "outright_winner";
    const slug = maybeMapFutureTeamToSlug(futures.team, teamLookup);
    if (!slug) {
      continue;
    }

    const american = Number(futures.odds_american);
    const americanOdds = Number.isFinite(american) && american !== 0 ? Math.trunc(american) : null;
    const decimal =
      americanOdds !== null
        ? americanToDecimal(americanOdds)
        : null;
    const implied =
      normalizeImpliedProbability(futures.implied_prob) ||
      (decimal ? Number((1 / decimal).toFixed(6)) : null);

    const bookmaker = normalizeBookmaker(futures.bookmaker);
    if (!bookmaker || (americanOdds === null && decimal === null && implied === null)) {
      continue;
    }

    rows.push({
      team_slug: slug,
      market,
      bookmaker,
      american_odds: americanOdds,
      decimal_odds: decimal,
      implied_probability: implied,
      fetched_at:
        futures.snapshot_date ||
        futures.updated_at ||
        futures.created_at ||
        new Date().toISOString(),
      volume: null,
    });
  }

  if (!rows.length) {
    console.log(
      "  futures_odds had WC rows but none mapped cleanly to wc26_teams, skipping insert",
    );
    return 0;
  }

  await restInsert(serviceKey, "wc26_odds", rows);
  console.log(`  inserted ${rows.length} odds rows from futures_odds`);
  return rows.length;
}

async function seedGroupStandings(serviceKey, wc26Teams, espnTeams, openapi) {
  console.log("\n[step 5] seeding group standings");

  const standingsProps = openapi?.definitions?.wc_group_standings?.properties || {};
  const schemaColumns = Object.keys(standingsProps).map((columnName) => ({
    column_name: columnName,
    data_type: standingsProps[columnName]?.type || "unknown",
  }));
  console.log("  wc_group_standings schema columns:");
  for (const column of schemaColumns) {
    console.log(`    - ${column.column_name}: ${column.data_type}`);
  }

  const espnIdByCode = new Map(
    espnTeams.map((team) => [String(team.abbreviation || "").toUpperCase(), Number(team.id)]),
  );

  const wcTeamUpserts = wc26Teams.map((team) => {
    const code = String(team.fifa_code || "").toUpperCase();
    const id = espnIdByCode.get(code);
    if (!Number.isFinite(id)) {
      throw new Error(`Missing ESPN id for code ${code}`);
    }
    return {
      id,
      team_name: team.name,
      fifa_code: code,
      confederation: team.confederation,
      fifa_ranking: Number(team.fifa_rank) || 0,
      group_letter: team.group_letter,
      qualified: true,
      qualification_method: "Qualified",
      placeholder_label: null,
    };
  });
  await restUpsert(serviceKey, "wc_teams", wcTeamUpserts, "id");

  const wcTeams = await restSelect(
    serviceKey,
    "wc_teams",
    "select=id,fifa_code",
  );
  const wcTeamIdByCode = new Map(
    wcTeams.map((row) => [String(row.fifa_code || "").toUpperCase(), Number(row.id)]),
  );

  const standingsRows = wc26Teams.map((team) => {
    const code = String(team.fifa_code || "").toUpperCase();
    const teamId = wcTeamIdByCode.get(code);
    if (!Number.isFinite(teamId)) {
      throw new Error(`Missing wc_teams id for fifa_code=${code}`);
    }
    return {
      group_letter: team.group_letter,
      team_id: teamId,
      played: 0,
      won: 0,
      drawn: 0,
      lost: 0,
      goals_for: 0,
      goals_against: 0,
      group_rank: 0,
    };
  });

  await restDelete(serviceKey, "wc_group_standings", "id=gte.0");
  await restInsert(serviceKey, "wc_group_standings", standingsRows);
  console.log(`  inserted ${standingsRows.length} group standing rows`);
}

async function runValidation(serviceKey) {
  console.log("\n[validation]");

  const teams = await restSelect(
    serviceKey,
    "wc26_teams",
    "select=slug,fifa_rank,head_coach",
  );
  const teamsSummary = {
    total: teams.length,
    has_rank: teams.filter((team) => team.fifa_rank !== null).length,
    has_coach: teams.filter((team) => team.head_coach !== null).length,
    still_placeholder: teams.filter((team) => /playoff/i.test(String(team.slug || ""))).length,
  };

  const fixtures = await restSelect(
    serviceKey,
    "wc26_fixtures",
    "select=stage,home_slug,venue",
  );
  const fixtureStages = new Set(fixtures.map((fixture) => fixture.stage).filter(Boolean));
  const fixturesSummary = {
    total: fixtures.length,
    stages: fixtureStages.size,
    has_teams: fixtures.filter((fixture) => fixture.home_slug !== null).length,
    has_venue: fixtures.filter((fixture) => fixture.venue !== null).length,
  };

  const odds = await restSelect(
    serviceKey,
    "wc26_odds",
    "select=team_slug,bookmaker",
  );
  const oddsSummary = {
    total: odds.length,
    teams: new Set(odds.map((row) => row.team_slug).filter(Boolean)).size,
    books: new Set(odds.map((row) => row.bookmaker).filter(Boolean)).size,
  };

  const standings = await restSelect(
    serviceKey,
    "wc_group_standings",
    "select=id",
  );
  const standingsSummary = {
    total: standings.length,
  };

  console.log("  teams:", teamsSummary);
  console.log("  fixtures:", fixturesSummary);
  console.log("  odds:", oddsSummary);
  console.log("  standings:", standingsSummary);

  return { teamsSummary, fixturesSummary, oddsSummary, standingsSummary };
}

async function main() {
  const serviceKey = resolveServiceRoleKey();
  const openapi = await fetchOpenApiSpec(serviceKey);

  await probeEspnEndpoints();
  await verifyInterconWinners();
  await replacePlaceholderTeams(serviceKey);

  const wc26Teams = await restSelect(
    serviceKey,
    "wc26_teams",
    "select=slug,name,fifa_code,group_letter,confederation,fifa_rank,head_coach,roster_json",
  );
  if (wc26Teams.length !== 48) {
    throw new Error(`Expected 48 wc26_teams rows, got ${wc26Teams.length}`);
  }

  const espnTeams = await fetchEspnTeams();

  await drainFixtures(serviceKey, wc26Teams, espnTeams, openapi);
  await drainTeamEnrichment(serviceKey, wc26Teams, espnTeams);
  const wc26TeamsRefreshed = await restSelect(
    serviceKey,
    "wc26_teams",
    "select=slug,name,fifa_code,group_letter,confederation,fifa_rank,head_coach,roster_json",
  );
  await drainOddsIfAvailable(serviceKey, wc26TeamsRefreshed);
  await seedGroupStandings(serviceKey, wc26TeamsRefreshed, espnTeams, openapi);
  await runValidation(serviceKey);

  console.log("\ncomplete");
}

main().catch((error) => {
  console.error("\nfailed:", error?.stack || error?.message || String(error));
  process.exitCode = 1;
});

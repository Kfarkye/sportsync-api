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

function extractCoachName(rosterPayload) {
  const coachBlock = rosterPayload?.coach;
  const coachEntry = Array.isArray(coachBlock) ? coachBlock[0] : coachBlock;
  if (!coachEntry || typeof coachEntry !== "object") {
    return null;
  }
  const fromParts = [coachEntry.firstName, coachEntry.lastName]
    .filter((value) => typeof value === "string" && value.trim().length > 0)
    .join(" ")
    .trim();
  if (fromParts) {
    return fromParts;
  }
  const fallback =
    (typeof coachEntry.displayName === "string" && coachEntry.displayName.trim()) ||
    (typeof coachEntry.name === "string" && coachEntry.name.trim()) ||
    "";
  return fallback || null;
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

async function drainFixtures(serviceKey, wc26Teams, espnTeams) {
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
  for (let index = 0; index < fixturesRaw.length; index += 1) {
    const row = fixturesRaw[index];
    const matchNumber = index + 1;
    const isGroup = row.stage === "group";

    let homeSlug =
      Number.isFinite(row.home_team_id) ? teamIdToSlug.get(row.home_team_id) || null : null;
    let awaySlug =
      Number.isFinite(row.away_team_id) ? teamIdToSlug.get(row.away_team_id) || null : null;

    if (!homeSlug && Number.isFinite(row.home_team_id)) {
      const displayName = await resolvePlaceholderDisplayName(row.home_team_id);
      homeSlug = resolvePlaceholderDisplayToSlug(
        displayName,
        groupSeeds,
        rankBySlug,
        outcomesByMatchNumber,
      );
    }
    if (!awaySlug && Number.isFinite(row.away_team_id)) {
      const displayName = await resolvePlaceholderDisplayName(row.away_team_id);
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
  }

  if (fixtures.length !== 104) {
    throw new Error(`Expected 104 fixture rows, got ${fixtures.length}`);
  }

  await restUpsert(serviceKey, "wc26_fixtures", fixtures, "fixture_id");
  console.log(`  upserted ${fixtures.length} fixtures`);
}

async function drainTeamEnrichment(serviceKey, wc26Teams, espnTeams) {
  console.log("\n[step 3] draining team enrichment");
  const slugByCode = new Map(
    wc26Teams.map((team) => [String(team.fifa_code || "").toUpperCase(), team.slug]),
  );

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

    const fifaRank = findNumericRank(detailResponse.json);
    const coachName = extractCoachName(rosterResponse.json);
    const rosterJson = toRosterJson(rosterResponse.json);

    return {
      slug,
      fifa_rank: fifaRank,
      head_coach: coachName,
      roster_json: rosterJson,
    };
  });

  await mapWithConcurrency(teamUpdates, async (update) => {
    const { slug, ...payload } = update;
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
  console.log("\n[step 4] draining futures odds");
  const response = await requestWithRetry(
    `${ESPN_SITE_BASE}/futures`,
    {},
    "fetch futures endpoint",
    [404],
  );

  if (response.status === 404) {
    console.log("  futures endpoint unavailable on ESPN (404), skipping odds drain");
    return 0;
  }

  const slugByCode = new Map(
    wc26Teams.map((team) => [String(team.fifa_code || "").toUpperCase(), team.slug]),
  );

  const rows = [];
  const fetchedAt = new Date().toISOString();
  const payload = response.json;
  const eventList = Array.isArray(payload?.events) ? payload.events : [];

  for (const event of eventList) {
    const competitors = event?.competitions?.[0]?.competitors || [];
    for (const competitor of competitors) {
      const code = String(competitor?.team?.abbreviation || "").toUpperCase();
      const slug = slugByCode.get(code) || null;
      const american = Number(competitor?.odds?.american || competitor?.odds?.moneyLine);
      if (!slug || !Number.isFinite(american)) {
        continue;
      }
      const decimal = americanToDecimal(american);
      rows.push({
        team_slug: slug,
        market: "outright_winner",
        bookmaker: "ESPN",
        american_odds: Math.trunc(american),
        decimal_odds: decimal,
        implied_probability: decimal ? Number((1 / decimal).toFixed(6)) : null,
        fetched_at: fetchedAt,
        volume: null,
      });
    }
  }

  if (!rows.length) {
    console.log("  futures endpoint returned no usable outright odds rows");
    return 0;
  }

  await restInsert(serviceKey, "wc26_odds", rows);
  console.log(`  inserted ${rows.length} futures odds rows`);
  return rows.length;
}

async function seedGroupStandings(serviceKey, wc26Teams, espnTeams) {
  console.log("\n[step 5] seeding group standings");

  const openapi = await requestWithRetry(`${REST_BASE}/`, {
    headers: authHeaders(serviceKey),
  }, "fetch rest openapi");
  const standingsProps = openapi.json?.definitions?.wc_group_standings?.properties || {};
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

  await probeEspnEndpoints();
  await verifyInterconWinners();
  await replacePlaceholderTeams(serviceKey);

  const wc26Teams = await restSelect(
    serviceKey,
    "wc26_teams",
    "select=slug,name,fifa_code,group_letter,confederation,fifa_rank,head_coach",
  );
  if (wc26Teams.length !== 48) {
    throw new Error(`Expected 48 wc26_teams rows, got ${wc26Teams.length}`);
  }

  const espnTeams = await fetchEspnTeams();

  await drainFixtures(serviceKey, wc26Teams, espnTeams);
  await drainTeamEnrichment(serviceKey, wc26Teams, espnTeams);
  const wc26TeamsRefreshed = await restSelect(
    serviceKey,
    "wc26_teams",
    "select=slug,name,fifa_code,group_letter,confederation,fifa_rank,head_coach",
  );
  await drainOddsIfAvailable(serviceKey, wc26TeamsRefreshed);
  await seedGroupStandings(serviceKey, wc26TeamsRefreshed, espnTeams);
  await runValidation(serviceKey);

  console.log("\ncomplete");
}

main().catch((error) => {
  console.error("\nfailed:", error?.stack || error?.message || String(error));
  process.exitCode = 1;
});

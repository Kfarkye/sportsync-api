const { CACHE, handleOptions, sendError, sendJson, setApiHeaders } = require("./_lib/http");

const MATCH_CONTEXT_ENDPOINT =
  process.env.MATCH_CONTEXT_ENDPOINT ||
  "https://hylnixnuabtnmjcdnujm.supabase.co/functions/v1/match-context";

const MATCH_CONTEXT_API_KEY =
  process.env.MATCH_CONTEXT_API_KEY ||
  process.env.SPORTSYNC_SANDBOX_API_KEY ||
  process.env.SPORTSYNC_API_KEY ||
  process.env.API_KEY ||
  "";

const REQUEST_TIMEOUT_MS = Number.parseInt(process.env.MATCH_CONTEXT_TIMEOUT_MS || "12000", 10);

const DEFAULT_ALLOWED_IDS = [
  "b193b51f-cbed-4398-a297-237dd3322607",
  "b42fe447-b2b1-485f-ae6d-1559ee2b57c7",
  "d6742e61-2457-43fd-aa3f-e61f6a76c7af",
  "c94d7e01-333d-41cd-a67d-cc0285fa7f28",
];

function parseAllowedIds() {
  const raw = process.env.DEMO_MATCH_IDS || DEFAULT_ALLOWED_IDS.join(",");
  return new Set(
    String(raw)
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean),
  );
}

function toJsonPayload(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

module.exports = async function handler(req, res) {
  if (handleOptions(req, res, CACHE.NO_STORE)) {
    return;
  }

  setApiHeaders(res, CACHE.NO_STORE);

  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return sendError(res, 405, "METHOD_NOT_ALLOWED", "Use GET for this endpoint.");
  }

  if (!MATCH_CONTEXT_API_KEY) {
    return sendError(
      res,
      500,
      "MISSING_API_KEY_CONFIG",
      "Set MATCH_CONTEXT_API_KEY for proxy calls.",
    );
  }

  const matchId = String(req.query?.match_id || "").trim();
  if (!matchId) {
    return sendError(res, 400, "MISSING_MATCH_ID", "Required query param: match_id");
  }

  const allowedIds = parseAllowedIds();
  if (!allowedIds.has(matchId)) {
    return sendError(
      res,
      403,
      "MATCH_NOT_ALLOWED",
      "This demo proxy only serves published sample IDs.",
    );
  }

  const targetUrl = new URL(MATCH_CONTEXT_ENDPOINT);
  targetUrl.searchParams.set("match_id", matchId);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), Number.isFinite(REQUEST_TIMEOUT_MS) ? REQUEST_TIMEOUT_MS : 12000);

  try {
    const upstream = await fetch(targetUrl.toString(), {
      method: "GET",
      headers: { "x-api-key": MATCH_CONTEXT_API_KEY },
      signal: controller.signal,
    });

    const bodyText = await upstream.text();
    const payload = toJsonPayload(bodyText);
    clearTimeout(timeoutId);

    if (!upstream.ok) {
      return sendJson(
        res,
        upstream.status,
        payload || {
          error: { code: "UPSTREAM_ERROR", message: `match-context returned HTTP ${upstream.status}` },
        },
      );
    }

    return sendJson(res, 200, payload);
  } catch (error) {
    clearTimeout(timeoutId);
    if (error?.name === "AbortError") {
      return sendError(res, 504, "UPSTREAM_TIMEOUT", "match-context request timed out.");
    }

    return sendError(
      res,
      502,
      "UPSTREAM_REQUEST_FAILED",
      error?.message || "Failed to call match-context endpoint.",
    );
  }
};

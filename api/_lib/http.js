const DEFAULT_API_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-api-key",
  "Access-Control-Max-Age": "86400",
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Permissions-Policy": "camera=(), microphone=(), geolocation=()",
};

const CACHE = {
  NO_STORE: "no-store, max-age=0",
  DEMO: "public, max-age=60, s-maxage=300",
  TRENDS: "public, max-age=30, s-maxage=60, stale-while-revalidate=120",
};

function setApiHeaders(res, cacheControl = CACHE.NO_STORE) {
  for (const [key, value] of Object.entries(DEFAULT_API_HEADERS)) {
    res.setHeader(key, value);
  }

  if (cacheControl) {
    res.setHeader("Cache-Control", cacheControl);
  }
}

function sendJson(res, statusCode, payload, cacheControl = CACHE.NO_STORE) {
  setApiHeaders(res, cacheControl);
  return res.status(statusCode).json(payload);
}

function sendError(res, statusCode, code, message, cacheControl = CACHE.NO_STORE) {
  return sendJson(
    res,
    statusCode,
    {
      error: {
        code,
        message,
      },
    },
    cacheControl,
  );
}

function handleOptions(req, res, cacheControl = CACHE.NO_STORE) {
  if (req.method !== "OPTIONS") {
    return false;
  }

  setApiHeaders(res, cacheControl);
  res.status(204).end();
  return true;
}

module.exports = {
  CACHE,
  handleOptions,
  sendError,
  sendJson,
  setApiHeaders,
};

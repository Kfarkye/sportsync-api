const DEFAULT_API_HEADERS = {
  "Access-Control-Allow-Origin": "*",
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

function normalizeHeaderOptions(cacheOrOptions) {
  if (typeof cacheOrOptions === "string" || cacheOrOptions === undefined || cacheOrOptions === null) {
    return {
      cacheControl: cacheOrOptions ?? CACHE.NO_STORE,
      allowMethods: "GET, OPTIONS",
      allowHeaders: "Content-Type, x-api-key",
    };
  }

  return {
    cacheControl: cacheOrOptions.cacheControl ?? CACHE.NO_STORE,
    allowMethods: cacheOrOptions.allowMethods ?? "GET, OPTIONS",
    allowHeaders: cacheOrOptions.allowHeaders ?? "Content-Type, x-api-key",
  };
}

function setApiHeaders(res, cacheOrOptions = CACHE.NO_STORE) {
  const options = normalizeHeaderOptions(cacheOrOptions);

  for (const [key, value] of Object.entries(DEFAULT_API_HEADERS)) {
    res.setHeader(key, value);
  }

  res.setHeader("Access-Control-Allow-Methods", options.allowMethods);
  res.setHeader("Access-Control-Allow-Headers", options.allowHeaders);

  if (options.cacheControl) {
    res.setHeader("Cache-Control", options.cacheControl);
  }
}

function sendJson(res, statusCode, payload, cacheOrOptions = CACHE.NO_STORE) {
  setApiHeaders(res, cacheOrOptions);
  return res.status(statusCode).json(payload);
}

function sendError(res, statusCode, code, message, cacheOrOptions = CACHE.NO_STORE) {
  return sendJson(
    res,
    statusCode,
    {
      error: {
        code,
        message,
      },
    },
    cacheOrOptions,
  );
}

function handleOptions(req, res, cacheOrOptions = CACHE.NO_STORE) {
  if (req.method !== "OPTIONS") {
    return false;
  }

  setApiHeaders(res, cacheOrOptions);
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

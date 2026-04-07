const { CACHE, handleOptions, sendError, sendJson, setApiHeaders } = require("./_lib/http");

const STRIPE_API_BASE = "https://api.stripe.com/v1";
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || "";
const PUBLIC_APP_URL = process.env.PUBLIC_APP_URL || "https://sportsync-api.vercel.app";

const PLAN_CONFIG = {
  trader: {
    mode: "subscription",
    price: process.env.STRIPE_PRICE_TRADER_MONTHLY || process.env.STRIPE_PRICE_TRADER || "",
  },
  desk: {
    mode: "subscription",
    price: process.env.STRIPE_PRICE_DESK_MONTHLY || process.env.STRIPE_PRICE_DESK || "",
  },
};

function parseJsonBody(req) {
  if (!req.body) return {};
  if (typeof req.body === "object") return req.body;

  if (typeof req.body === "string") {
    try {
      return JSON.parse(req.body);
    } catch {
      return {};
    }
  }

  if (Buffer.isBuffer(req.body)) {
    try {
      return JSON.parse(req.body.toString("utf8"));
    } catch {
      return {};
    }
  }

  return {};
}

function safeBaseUrl(req, requestedOrigin) {
  let fallback;
  try {
    fallback = new URL(PUBLIC_APP_URL);
  } catch {
    fallback = new URL("https://sportsync-api.vercel.app");
  }

  const hostCandidates = new Set([fallback.host]);
  if (typeof req.headers?.host === "string" && req.headers.host.trim().length > 0) {
    hostCandidates.add(req.headers.host.trim());
  }

  const candidate = typeof requestedOrigin === "string" ? requestedOrigin.trim() : "";
  if (candidate.length > 0) {
    try {
      const parsed = new URL(candidate);
      if ((parsed.protocol === "https:" || parsed.hostname === "localhost") && hostCandidates.has(parsed.host)) {
        return parsed.origin;
      }
    } catch {
      // ignore invalid user-provided origin and use fallback
    }
  }

  return fallback.origin;
}

function readStripeError(payload, statusCode) {
  const code = payload?.error?.code;
  const message = payload?.error?.message;
  if (typeof message === "string" && message.trim().length > 0) {
    if (/api key/i.test(message)) {
      return `Stripe authentication failed. Verify STRIPE_SECRET_KEY. (stripe_http_${statusCode}${code ? `:${code}` : ""})`;
    }

    const sanitized = message
      .trim()
      .replace(/(sk|rk)_(test|live)_[A-Za-z0-9_*]+/g, "[REDACTED_KEY]")
      .replace(/\s+/g, " ");
    return `${sanitized} (stripe_http_${statusCode}${code ? `:${code}` : ""})`;
  }

  return `Stripe request failed (HTTP ${statusCode}).`;
}

module.exports = async function handler(req, res) {
  const corsOptions = {
    cacheControl: CACHE.NO_STORE,
    allowMethods: "POST, OPTIONS",
  };

  if (handleOptions(req, res, corsOptions)) {
    return;
  }

  setApiHeaders(res, corsOptions);

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return sendError(res, 405, "METHOD_NOT_ALLOWED", "Use POST for this endpoint.", corsOptions);
  }

  if (!STRIPE_SECRET_KEY) {
    return sendError(
      res,
      503,
      "MISSING_STRIPE_CONFIG",
      "Stripe is not configured. Set STRIPE_SECRET_KEY and plan price IDs.",
      corsOptions,
    );
  }

  const body = parseJsonBody(req);
  const plan = String(body?.plan || "").trim().toLowerCase();

  if (!plan || !Object.prototype.hasOwnProperty.call(PLAN_CONFIG, plan)) {
    return sendError(res, 400, "INVALID_PLAN", "Plan must be one of: trader, desk.", corsOptions);
  }

  const planConfig = PLAN_CONFIG[plan];
  if (!planConfig.price) {
    return sendError(
      res,
      503,
      "PLAN_NOT_CONFIGURED",
      `Stripe price ID is missing for plan '${plan}'.`,
      corsOptions,
    );
  }

  const baseUrl = safeBaseUrl(req, body?.origin);
  const params = new URLSearchParams();
  params.set("mode", planConfig.mode);
  params.set("line_items[0][price]", planConfig.price);
  params.set("line_items[0][quantity]", "1");
  params.set("success_url", `${baseUrl}/success.html?session_id={CHECKOUT_SESSION_ID}&plan=${plan}`);
  params.set("cancel_url", `${baseUrl}/cancelled.html?plan=${plan}`);
  params.set("allow_promotion_codes", "true");
  params.set("metadata[plan]", plan);

  const email = String(body?.email || "").trim();
  if (email.includes("@") && email.length <= 254) {
    params.set("customer_email", email);
  }

  try {
    const stripeResponse = await fetch(`${STRIPE_API_BASE}/checkout/sessions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${STRIPE_SECRET_KEY}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: params.toString(),
    });

    const payload = await stripeResponse.json().catch(() => ({}));

    if (!stripeResponse.ok) {
      return sendError(
        res,
        502,
        "STRIPE_API_ERROR",
        readStripeError(payload, stripeResponse.status),
        corsOptions,
      );
    }

    const checkoutUrl = payload?.url;
    if (typeof checkoutUrl !== "string" || checkoutUrl.trim().length === 0) {
      return sendError(
        res,
        502,
        "STRIPE_INVALID_RESPONSE",
        "Stripe did not return a checkout URL.",
        corsOptions,
      );
    }

    return sendJson(
      res,
      200,
      {
        plan,
        checkout_url: checkoutUrl,
        checkout_session_id: payload?.id || null,
      },
      corsOptions,
    );
  } catch (error) {
    return sendError(
      res,
      502,
      "STRIPE_REQUEST_FAILED",
      String(error?.message || "Unable to create checkout session.").replace(
        /(sk|rk)_(test|live)_[A-Za-z0-9_*]+/g,
        "[REDACTED_KEY]",
      ),
      corsOptions,
    );
  }
};

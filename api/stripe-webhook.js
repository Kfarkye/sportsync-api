const crypto = require("crypto");
const { CACHE, handleOptions, sendError, sendJson, setApiHeaders } = require("./_lib/http");

const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || "";
const FULFILLMENT_WEBHOOK_URL = process.env.FULFILLMENT_WEBHOOK_URL || "";
const STRIPE_NOTIFY_WEBHOOK_URL = process.env.STRIPE_NOTIFY_WEBHOOK_URL || "";

function getRawBody(req) {
  if (typeof req.body === "string") return req.body;
  if (Buffer.isBuffer(req.body)) return req.body.toString("utf8");
  return null;
}

function parseStripeSignature(header) {
  if (typeof header !== "string") return null;
  const parts = header.split(",").map((part) => part.trim());
  const timestamp = parts.find((part) => part.startsWith("t="))?.slice(2);
  const signatures = parts
    .filter((part) => part.startsWith("v1="))
    .map((part) => part.slice(3))
    .filter(Boolean);

  if (!timestamp || signatures.length === 0) return null;
  return { timestamp, signatures };
}

function computeSignature(payload, secret, timestamp) {
  return crypto.createHmac("sha256", secret).update(`${timestamp}.${payload}`, "utf8").digest("hex");
}

function safeCompare(a, b) {
  const aBuf = Buffer.from(a);
  const bBuf = Buffer.from(b);
  if (aBuf.length !== bBuf.length) return false;
  return crypto.timingSafeEqual(aBuf, bBuf);
}

async function postJson(url, payload) {
  if (!url) return { ok: true };

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    return { ok: false, status: response.status };
  }

  return { ok: true };
}

async function notifyFulfillment(payload) {
  if (!FULFILLMENT_WEBHOOK_URL) return { ok: true };

  return postJson(FULFILLMENT_WEBHOOK_URL, payload);
}

async function notifyChannel(payload) {
  if (!STRIPE_NOTIFY_WEBHOOK_URL) return { ok: true };

  const amount = payload.amount_total != null ? payload.amount_total / 100 : null;
  const currency = payload.currency ? payload.currency.toUpperCase() : null;
  const amountLabel = amount != null && currency ? `${amount.toFixed(2)} ${currency}` : "unknown amount";
  const text = `New ${payload.plan || "unknown"} checkout: ${amountLabel} · session ${payload.session_id || "unknown"} · ${payload.customer_email || "no email"}`;

  return postJson(STRIPE_NOTIFY_WEBHOOK_URL, {
    text,
    event: payload,
  });
}

module.exports = async function handler(req, res) {
  const corsOptions = {
    cacheControl: CACHE.NO_STORE,
    allowMethods: "POST, OPTIONS",
    allowHeaders: "Content-Type, Stripe-Signature",
  };

  if (handleOptions(req, res, corsOptions)) {
    return;
  }

  setApiHeaders(res, corsOptions);

  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return sendError(res, 405, "METHOD_NOT_ALLOWED", "Use POST for this endpoint.", corsOptions);
  }

  if (!STRIPE_WEBHOOK_SECRET) {
    return sendError(
      res,
      503,
      "MISSING_STRIPE_CONFIG",
      "Stripe webhook secret is not configured.",
      corsOptions,
    );
  }

  const signatureHeader = req.headers?.["stripe-signature"] || req.headers?.["Stripe-Signature"];
  const signature = parseStripeSignature(signatureHeader);
  if (!signature) {
    return sendError(res, 400, "INVALID_SIGNATURE", "Missing or invalid Stripe signature.", corsOptions);
  }

  const rawBody = getRawBody(req);
  if (!rawBody) {
    return sendError(res, 400, "INVALID_PAYLOAD", "Unable to read raw request body.", corsOptions);
  }

  const expected = computeSignature(rawBody, STRIPE_WEBHOOK_SECRET, signature.timestamp);
  const valid = signature.signatures.some((sig) => safeCompare(sig, expected));
  if (!valid) {
    return sendError(res, 400, "INVALID_SIGNATURE", "Stripe signature verification failed.", corsOptions);
  }

  let event;
  try {
    event = JSON.parse(rawBody);
  } catch {
    return sendError(res, 400, "INVALID_PAYLOAD", "Payload is not valid JSON.", corsOptions);
  }

  if (event?.type === "checkout.session.completed") {
    const session = event?.data?.object || {};
    const payload = {
      event_id: event.id,
      type: event.type,
      session_id: session.id || null,
      customer_email: session.customer_details?.email || session.customer_email || null,
      plan: session.metadata?.plan || null,
      amount_total: session.amount_total ?? null,
      currency: session.currency || null,
      created: session.created || null,
    };

    const notifyResult = await notifyFulfillment(payload);
    if (!notifyResult.ok) {
      return sendError(
        res,
        502,
        "FULFILLMENT_FAILED",
        "Payment received but fulfillment hook failed.",
        corsOptions,
      );
    }

    const notifyChannelResult = await notifyChannel(payload);
    if (!notifyChannelResult.ok) {
      return sendError(
        res,
        502,
        "NOTIFY_FAILED",
        "Payment received but notification hook failed.",
        corsOptions,
      );
    }
  }

  return sendJson(res, 200, { received: true }, corsOptions);
};

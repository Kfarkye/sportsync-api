import {
  CORS_HEADERS,
  errorResponse,
  formUrlEncode,
  getRequiredEnv,
  jsonResponse,
  normalizeTier,
  parseJsonBody,
  stripeRequest,
} from "../_shared/billing.ts";

type CheckoutPayload = {
  email?: string;
  tier?: string;
  success_url?: string;
  cancel_url?: string;
};

function getPriceIdForTier(tier: "builder" | "pro"): string {
  if (tier === "builder") return getRequiredEnv("STRIPE_PRICE_BUILDER");
  return getRequiredEnv("STRIPE_PRICE_PRO");
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });
  if (req.method !== "POST") return errorResponse(405, "METHOD_NOT_ALLOWED", "Use POST.");

  try {
    const stripeSecret = getRequiredEnv("STRIPE_SECRET_KEY");
    const defaultAppUrl = Deno.env.get("PUBLIC_APP_URL") ?? "https://sportsync.com";

    const payload = parseJsonBody<CheckoutPayload>(await req.text());
    const email = payload.email?.trim().toLowerCase();
    const tier = normalizeTier(payload.tier);
    const successUrl = payload.success_url?.trim() || `${defaultAppUrl}/portal/index.html?checkout=success`;
    const cancelUrl = payload.cancel_url?.trim() || `${defaultAppUrl}/portal/index.html?checkout=cancelled`;

    if (!email) return errorResponse(400, "MISSING_EMAIL", "email is required.");
    if (!tier) return errorResponse(400, "INVALID_TIER", "tier must be builder or pro.");
    if (tier === "free") {
      return errorResponse(400, "FREE_TIER_NOT_CHECKOUT", "Free tier does not use Stripe checkout.");
    }

    const priceId = getPriceIdForTier(tier);
    const body = formUrlEncode({
      mode: "subscription",
      customer_email: email,
      success_url: successUrl,
      cancel_url: cancelUrl,
      "line_items[0][price]": priceId,
      "line_items[0][quantity]": 1,
      "metadata[tier]": tier,
      allow_promotion_codes: true,
    });

    const session = await stripeRequest("/checkout/sessions", "POST", stripeSecret, body);
    return jsonResponse({
      session_id: session.id ?? null,
      checkout_url: session.url ?? null,
      tier,
      email,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return errorResponse(500, "CHECKOUT_CREATE_FAILED", message);
  }
});

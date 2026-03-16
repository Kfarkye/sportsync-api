import {
  CORS_HEADERS,
  createServiceRoleClient,
  errorResponse,
  formUrlEncode,
  getRequiredEnv,
  jsonResponse,
  parseJsonBody,
  requirePortalToken,
  stripeRequest,
} from "../_shared/billing.ts";

type PortalPayload = {
  customer_id?: string;
  email?: string;
  return_url?: string;
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });
  if (req.method !== "POST") return errorResponse(405, "METHOD_NOT_ALLOWED", "Use POST.");

  try {
    await requirePortalToken(req);

    const supabase = createServiceRoleClient();
    const stripeSecret = getRequiredEnv("STRIPE_SECRET_KEY");
    const defaultReturn = `${Deno.env.get("PUBLIC_APP_URL") ?? "https://sportsync.com"}/portal/index.html`;

    const payload = parseJsonBody<PortalPayload>(await req.text());
    const customerId = payload.customer_id?.trim() || null;
    const email = payload.email?.trim().toLowerCase() || null;
    const returnUrl = payload.return_url?.trim() || defaultReturn;

    if (!customerId && !email) {
      return errorResponse(400, "MISSING_CUSTOMER", "customer_id or email is required.");
    }

    let query = supabase
      .from("customers")
      .select("id, email, stripe_customer_id")
      .limit(1);

    if (customerId) {
      query = query.eq("id", customerId);
    } else {
      query = query.eq("email", email);
    }

    const { data: customer, error: customerError } = await query.maybeSingle();
    if (customerError) {
      return errorResponse(500, "CUSTOMER_LOOKUP_FAILED", customerError.message);
    }
    if (!customer?.stripe_customer_id) {
      return errorResponse(404, "STRIPE_CUSTOMER_NOT_FOUND", "No Stripe customer found for this account.");
    }

    const body = formUrlEncode({
      customer: customer.stripe_customer_id,
      return_url: returnUrl,
    });
    const session = await stripeRequest("/billing_portal/sessions", "POST", stripeSecret, body);
    return jsonResponse({
      customer_id: customer.id,
      stripe_customer_id: customer.stripe_customer_id,
      portal_url: session.url ?? null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("Unauthorized portal token")) {
      return errorResponse(401, "UNAUTHORIZED", "Invalid portal token.");
    }
    return errorResponse(500, "BILLING_PORTAL_FAILED", message);
  }
});

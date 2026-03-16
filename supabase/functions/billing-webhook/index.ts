import {
  CORS_HEADERS,
  createServiceRoleClient,
  errorResponse,
  fetchPlanLimits,
  formUrlEncode,
  getRequiredEnv,
  jsonResponse,
  normalizeTier,
  randomApiKey,
  resolveTierOrDefault,
  sha256Hex,
  stripeRequest,
  verifyStripeSignature,
} from "../_shared/billing.ts";

type StripeEvent = {
  id?: string;
  type?: string;
  data?: { object?: Record<string, unknown> };
};

function asRecord(value: unknown): Record<string, unknown> | null {
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asString(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return null;
}

async function upsertCustomer(
  supabase: ReturnType<typeof createServiceRoleClient>,
  stripeCustomerId: string,
  email: string | null,
  tier: string,
  status: string,
): Promise<Record<string, unknown>> {
  const { data: existing, error: existingError } = await supabase
    .from("customers")
    .select("id, plan, status")
    .eq("stripe_customer_id", stripeCustomerId)
    .maybeSingle();
  if (existingError) throw new Error(`Customer lookup failed: ${existingError.message}`);

  const payload = {
    stripe_customer_id: stripeCustomerId,
    email,
    plan: tier,
    status,
  };

  if (existing) {
    const { data, error } = await supabase
      .from("customers")
      .update(payload)
      .eq("id", existing.id)
      .select("*")
      .single();
    if (error) throw new Error(`Customer update failed: ${error.message}`);
    return data as Record<string, unknown>;
  }

  const { data, error } = await supabase
    .from("customers")
    .insert(payload)
    .select("*")
    .single();
  if (error) throw new Error(`Customer create failed: ${error.message}`);
  return data as Record<string, unknown>;
}

async function syncCustomerKeysToTier(
  supabase: ReturnType<typeof createServiceRoleClient>,
  customerId: string,
  tierInput: string,
): Promise<void> {
  const tier = resolveTierOrDefault(tierInput);
  const limits = await fetchPlanLimits(supabase, tier);

  const { error } = await supabase
    .from("api_keys")
    .update({
      tier,
      rate_limit_per_minute: limits.rpm,
      rate_limit_per_day: limits.day,
      monthly_request_cap: limits.month,
    })
    .eq("customer_id", customerId)
    .eq("active", true);

  if (error) throw new Error(`Key tier sync failed: ${error.message}`);
}

async function ensureDefaultKey(
  supabase: ReturnType<typeof createServiceRoleClient>,
  customerId: string,
  tierInput: string,
): Promise<void> {
  const { data: existing, error: existingError } = await supabase
    .from("api_keys")
    .select("id")
    .eq("customer_id", customerId)
    .eq("active", true)
    .is("revoked_at", null)
    .limit(1);
  if (existingError) throw new Error(`Default key check failed: ${existingError.message}`);
  if ((existing ?? []).length > 0) return;

  const tier = resolveTierOrDefault(tierInput);
  const limits = await fetchPlanLimits(supabase, tier);

  const raw = randomApiKey("sk_live");
  const hash = await sha256Hex(raw);
  const prefix = raw.slice(0, 12);
  const { error } = await supabase
    .from("api_keys")
    .insert({
      customer_id: customerId,
      key_hash: hash,
      key_prefix: prefix,
      name: "Primary key",
      tier,
      active: true,
      rate_limit_per_minute: limits.rpm,
      rate_limit_per_day: limits.day,
      monthly_request_cap: limits.month,
    });
  if (error) throw new Error(`Default key create failed: ${error.message}`);
}

function tierFromPriceId(priceId: string | null): "free" | "builder" | "pro" | null {
  if (!priceId) return null;
  if (priceId === Deno.env.get("STRIPE_PRICE_BUILDER")) return "builder";
  if (priceId === Deno.env.get("STRIPE_PRICE_PRO")) return "pro";
  return null;
}

function graceHours(): number {
  const envValue = asNumber(Deno.env.get("PAYMENT_GRACE_HOURS"));
  if (envValue === null || envValue <= 0) return 72;
  return Math.trunc(envValue);
}

function unixToIso(value: unknown): string | null {
  const unix = asNumber(value);
  if (unix === null) return null;
  return new Date(unix * 1000).toISOString();
}

async function fetchStripeSubscription(
  stripeSecret: string,
  subscriptionId: string,
): Promise<Record<string, unknown>> {
  const query = formUrlEncode({ "expand[]": "items.data.price" });
  return await stripeRequest(`/subscriptions/${subscriptionId}?${query}`, "GET", stripeSecret);
}

function extractFirstPriceId(source: Record<string, unknown>): string | null {
  const items = asRecord(source.items);
  const data = items?.data;
  if (!Array.isArray(data) || data.length === 0) return null;
  const first = asRecord(data[0]);
  const price = asRecord(first?.price);
  return asString(price?.id);
}

async function applySubscriptionState(
  supabase: ReturnType<typeof createServiceRoleClient>,
  customer: Record<string, unknown>,
  subscriptionId: string,
  tier: string,
  status: string,
  periodEndIso: string | null,
  graceEndsIso: string | null,
) {
  const customerId = asString(customer.id);
  if (!customerId) throw new Error("Customer id missing.");

  const payload: Record<string, unknown> = {
    stripe_subscription_id: subscriptionId,
    customer_id: customerId,
    tier,
    status,
    current_period_end: periodEndIso,
    grace_ends_at: graceEndsIso,
  };

  const { error: subError } = await supabase
    .from("subscriptions")
    .upsert(payload, { onConflict: "stripe_subscription_id" });
  if (subError) throw new Error(`Subscription upsert failed: ${subError.message}`);

  const { error: customerError } = await supabase
    .from("customers")
    .update({ plan: tier, status })
    .eq("id", customerId);
  if (customerError) throw new Error(`Customer tier update failed: ${customerError.message}`);

  await syncCustomerKeysToTier(supabase, customerId, tier);
}

async function handleEvent(
  supabase: ReturnType<typeof createServiceRoleClient>,
  stripeSecret: string,
  event: StripeEvent,
): Promise<void> {
  const eventType = event.type ?? "";
  const object = asRecord(event.data?.object) ?? {};
  const metadata = asRecord(object.metadata);
  const customerDetails = asRecord(object.customer_details);

  if (eventType === "checkout.session.completed") {
    const stripeCustomerId = asString(object.customer);
    const subscriptionId = asString(object.subscription);
    const email = asString(customerDetails?.email ?? object.customer_email);
    const tierFromMetadata = normalizeTier(metadata?.tier);
    if (!stripeCustomerId || !subscriptionId) return;

    const subscription = await fetchStripeSubscription(stripeSecret, subscriptionId);
    const priceId = extractFirstPriceId(subscription);
    const tier = tierFromPriceId(priceId) ?? tierFromMetadata ?? "builder";
    const status = asString(subscription.status) ?? "active";
    const periodEndIso = unixToIso(subscription.current_period_end);

    const customer = await upsertCustomer(supabase, stripeCustomerId, email, tier, status);
    await applySubscriptionState(supabase, customer, subscriptionId, tier, status, periodEndIso, null);
    const customerId = asString(customer.id);
    if (customerId) await ensureDefaultKey(supabase, customerId, tier);
    return;
  }

  if (eventType === "customer.subscription.updated" || eventType === "customer.subscription.deleted") {
    const stripeCustomerId = asString(object.customer);
    const subscriptionId = asString(object.id);
    const status = asString(object.status) ?? (eventType === "customer.subscription.deleted" ? "canceled" : "active");
    const periodEndIso = unixToIso(object.current_period_end);
    const priceId = extractFirstPriceId(object);
    const tier = eventType === "customer.subscription.deleted"
      ? "free"
      : (tierFromPriceId(priceId) ?? "builder");

    if (!stripeCustomerId || !subscriptionId) return;

    const customer = await upsertCustomer(supabase, stripeCustomerId, null, tier, status);
    await applySubscriptionState(supabase, customer, subscriptionId, tier, status, periodEndIso, null);
    return;
  }

  if (eventType === "invoice.payment_failed") {
    const stripeCustomerId = asString(object.customer);
    const subscriptionId = asString(object.subscription);
    if (!stripeCustomerId || !subscriptionId) return;

    const customer = await upsertCustomer(supabase, stripeCustomerId, null, "builder", "past_due");
    const graceIso = new Date(Date.now() + graceHours() * 60 * 60 * 1000).toISOString();
    const periodEndIso = unixToIso(object.period_end);
    const customerPlan = asString(customer.plan) ?? "builder";
    await applySubscriptionState(supabase, customer, subscriptionId, resolveTierOrDefault(customerPlan), "past_due", periodEndIso, graceIso);
    return;
  }

  if (eventType === "invoice.paid") {
    const stripeCustomerId = asString(object.customer);
    const subscriptionId = asString(object.subscription);
    if (!stripeCustomerId || !subscriptionId) return;
    const subscription = await fetchStripeSubscription(stripeSecret, subscriptionId);
    const priceId = extractFirstPriceId(subscription);
    const tier = tierFromPriceId(priceId) ?? "builder";
    const status = asString(subscription.status) ?? "active";
    const periodEndIso = unixToIso(subscription.current_period_end);
    const customer = await upsertCustomer(supabase, stripeCustomerId, null, tier, status);
    await applySubscriptionState(supabase, customer, subscriptionId, tier, status, periodEndIso, null);
    return;
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });
  if (req.method !== "POST") return errorResponse(405, "METHOD_NOT_ALLOWED", "Use POST.");

  try {
    const stripeSecret = getRequiredEnv("STRIPE_SECRET_KEY");
    const webhookSecret = getRequiredEnv("STRIPE_WEBHOOK_SECRET");
    const signature = req.headers.get("stripe-signature") ?? "";
    const rawBody = await req.text();

    if (!signature) return errorResponse(400, "MISSING_SIGNATURE", "stripe-signature header is required.");
    const validSignature = await verifyStripeSignature(rawBody, signature, webhookSecret);
    if (!validSignature) return errorResponse(400, "INVALID_SIGNATURE", "Stripe signature verification failed.");

    const event = JSON.parse(rawBody) as StripeEvent;
    const eventId = asString(event.id);
    const eventType = asString(event.type) ?? "unknown";
    if (!eventId) return errorResponse(400, "INVALID_EVENT", "Stripe event id is missing.");

    const supabase = createServiceRoleClient();

    const { data: existingEvent, error: existingError } = await supabase
      .from("stripe_webhook_events")
      .select("id,status")
      .eq("stripe_event_id", eventId)
      .maybeSingle();

    if (existingError) {
      return errorResponse(500, "WEBHOOK_LOG_LOOKUP_FAILED", existingError.message);
    }
    if (existingEvent) {
      return jsonResponse({ ok: true, duplicate: true, event_id: eventId, status: existingEvent.status });
    }

    const { error: insertError } = await supabase
      .from("stripe_webhook_events")
      .insert({
        stripe_event_id: eventId,
        event_type: eventType,
        payload: event as unknown as Record<string, unknown>,
        status: "processing",
        processed_at: new Date().toISOString(),
      });
    if (insertError) return errorResponse(500, "WEBHOOK_LOG_INSERT_FAILED", insertError.message);

    try {
      await handleEvent(supabase, stripeSecret, event);
      await supabase
        .from("stripe_webhook_events")
        .update({ status: "processed", error_message: null, processed_at: new Date().toISOString() })
        .eq("stripe_event_id", eventId);
      return jsonResponse({ ok: true, event_id: eventId, event_type: eventType });
    } catch (handlerError) {
      const message = handlerError instanceof Error ? handlerError.message : String(handlerError);
      await supabase
        .from("stripe_webhook_events")
        .update({ status: "failed", error_message: message, processed_at: new Date().toISOString() })
        .eq("stripe_event_id", eventId);
      return errorResponse(500, "WEBHOOK_HANDLER_FAILED", message);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return errorResponse(500, "WEBHOOK_FAILED", message);
  }
});

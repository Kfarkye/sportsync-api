import {
  CORS_HEADERS,
  createServiceRoleClient,
  errorResponse,
  fetchPlanLimits,
  jsonResponse,
  parseJsonBody,
  randomApiKey,
  requirePortalToken,
  resolveTierOrDefault,
  sha256Hex,
} from "../_shared/billing.ts";

type KeyPayload = {
  customer_id?: string;
  name?: string;
  tier?: string;
  key_id?: string;
};

function extractSubpath(url: URL): string {
  const marker = "/functions/v1/api-keys";
  const index = url.pathname.indexOf(marker);
  if (index < 0) return "";
  return url.pathname.slice(index + marker.length).replace(/^\/+/, "");
}

async function buildCustomerSnapshot(supabase: ReturnType<typeof createServiceRoleClient>, customerId: string) {
  const { data: customer, error: customerError } = await supabase
    .from("customers")
    .select("id,email,plan,status,stripe_customer_id")
    .eq("id", customerId)
    .maybeSingle();

  if (customerError) throw new Error(`Customer lookup failed: ${customerError.message}`);
  if (!customer) throw new Error("Customer not found.");

  const { data: keys, error: keyError } = await supabase
    .from("api_keys")
    .select("id,name,key_prefix,tier,active,rate_limit_per_minute,rate_limit_per_day,monthly_request_cap,created_at,last_used_at,revoked_at")
    .eq("customer_id", customerId)
    .order("created_at", { ascending: false });
  if (keyError) throw new Error(`Key lookup failed: ${keyError.message}`);

  const now = new Date();
  const startOfDay = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())).toISOString();
  const startOfMonth = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1)).toISOString();
  const end = now.toISOString();

  const { count: dayCount, error: dayError } = await supabase
    .from("api_request_logs")
    .select("id", { count: "exact", head: true })
    .eq("api_key_id", (keys ?? []).find((row) => row.active)?.id ?? "00000000-0000-0000-0000-000000000000")
    .gte("created_at", startOfDay)
    .lte("created_at", end);
  if (dayError) throw new Error(`Usage lookup failed: ${dayError.message}`);

  const { count: monthCount, error: monthError } = await supabase
    .from("api_request_logs")
    .select("id", { count: "exact", head: true })
    .eq("api_key_id", (keys ?? []).find((row) => row.active)?.id ?? "00000000-0000-0000-0000-000000000000")
    .gte("created_at", startOfMonth)
    .lte("created_at", end);
  if (monthError) throw new Error(`Usage lookup failed: ${monthError.message}`);

  const primaryTier = resolveTierOrDefault(customer.plan ?? (keys ?? []).find((row) => row.active)?.tier ?? "free");
  const limits = await fetchPlanLimits(supabase, primaryTier);

  return {
    customer,
    plan: {
      tier: primaryTier,
      limits: {
        per_minute: limits.rpm,
        per_day: limits.day,
        per_month: limits.month,
      },
    },
    usage: {
      today: dayCount ?? 0,
      month: monthCount ?? 0,
    },
    keys: keys ?? [],
  };
}

async function createKey(
  supabase: ReturnType<typeof createServiceRoleClient>,
  customerId: string,
  name: string,
  tierInput: string | undefined,
  rotatedFrom: string | null,
) {
  const tier = resolveTierOrDefault(tierInput);
  const limits = await fetchPlanLimits(supabase, tier);

  const rawKey = randomApiKey("sk_live");
  const keyHash = await sha256Hex(rawKey);
  const keyPrefix = rawKey.slice(0, 12);

  const insertPayload: Record<string, unknown> = {
    customer_id: customerId,
    key_hash: keyHash,
    key_prefix: keyPrefix,
    name,
    tier,
    active: true,
    rate_limit_per_minute: limits.rpm,
    rate_limit_per_day: limits.day,
    monthly_request_cap: limits.month,
  };
  if (rotatedFrom) insertPayload.rotated_from_key_id = rotatedFrom;

  const { data, error } = await supabase
    .from("api_keys")
    .insert(insertPayload)
    .select("id,name,key_prefix,tier,active,rate_limit_per_minute,rate_limit_per_day,monthly_request_cap,created_at,last_used_at,revoked_at")
    .single();
  if (error) throw new Error(`Key create failed: ${error.message}`);

  return { key: data, raw_key: rawKey };
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });

  try {
    await requirePortalToken(req);
    const supabase = createServiceRoleClient();

    const url = new URL(req.url);
    const subpath = extractSubpath(url);

    if (req.method === "GET" && subpath.length === 0) {
      const customerId = url.searchParams.get("customer_id")?.trim();
      if (!customerId) return errorResponse(400, "MISSING_CUSTOMER_ID", "customer_id is required.");
      const snapshot = await buildCustomerSnapshot(supabase, customerId);
      return jsonResponse(snapshot);
    }

    if (req.method === "POST" && subpath === "create") {
      const payload = parseJsonBody<KeyPayload>(await req.text());
      const customerId = payload.customer_id?.trim();
      if (!customerId) return errorResponse(400, "MISSING_CUSTOMER_ID", "customer_id is required.");

      const created = await createKey(
        supabase,
        customerId,
        payload.name?.trim() || "Primary key",
        payload.tier,
        null,
      );
      return jsonResponse({
        message: "API key created. Save the raw key now; it will not be shown again.",
        ...created,
      });
    }

    const rotateMatch = subpath.match(/^([0-9a-f-]{36})\/rotate$/i);
    if (req.method === "POST" && rotateMatch) {
      const keyId = rotateMatch[1];
      const payload = parseJsonBody<KeyPayload>(await req.text());
      const customerId = payload.customer_id?.trim();
      if (!customerId) return errorResponse(400, "MISSING_CUSTOMER_ID", "customer_id is required.");

      const { data: oldKey, error: oldKeyError } = await supabase
        .from("api_keys")
        .select("id,customer_id,tier,name,active")
        .eq("id", keyId)
        .eq("customer_id", customerId)
        .maybeSingle();
      if (oldKeyError) return errorResponse(500, "KEY_LOOKUP_FAILED", oldKeyError.message);
      if (!oldKey) return errorResponse(404, "KEY_NOT_FOUND", "API key not found.");

      const created = await createKey(
        supabase,
        customerId,
        payload.name?.trim() || oldKey.name || "Rotated key",
        oldKey.tier ?? payload.tier,
        oldKey.id,
      );

      const { error: revokeError } = await supabase
        .from("api_keys")
        .update({ active: false, revoked_at: new Date().toISOString() })
        .eq("id", oldKey.id);
      if (revokeError) return errorResponse(500, "KEY_REVOKE_FAILED", revokeError.message);

      return jsonResponse({
        message: "API key rotated. Old key is revoked.",
        ...created,
      });
    }

    const revokeMatch = subpath.match(/^([0-9a-f-]{36})\/revoke$/i);
    if (req.method === "POST" && revokeMatch) {
      const keyId = revokeMatch[1];
      const payload = parseJsonBody<KeyPayload>(await req.text());
      const customerId = payload.customer_id?.trim();
      if (!customerId) return errorResponse(400, "MISSING_CUSTOMER_ID", "customer_id is required.");

      const { error } = await supabase
        .from("api_keys")
        .update({ active: false, revoked_at: new Date().toISOString() })
        .eq("id", keyId)
        .eq("customer_id", customerId);

      if (error) return errorResponse(500, "KEY_REVOKE_FAILED", error.message);
      return jsonResponse({ message: "API key revoked.", key_id: keyId });
    }

    return errorResponse(404, "NOT_FOUND", "Unknown API key action.");
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes("Unauthorized portal token")) {
      return errorResponse(401, "UNAUTHORIZED", "Invalid portal token.");
    }
    return errorResponse(500, "API_KEYS_FAILED", message);
  }
});

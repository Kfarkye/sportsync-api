import { createClient, type SupabaseClient } from "npm:@supabase/supabase-js@2.57.4";

export type Tier = "free" | "builder" | "pro";

export const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, stripe-signature, x-portal-token, x-worker-secret",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
};

const LEGACY_TIER_ALIAS: Record<string, Tier> = {
  sandbox: "free",
  production: "builder",
  enterprise: "pro",
};

export function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
  });
}

export function errorResponse(status: number, code: string, message: string): Response {
  return jsonResponse({ error: { code, message } }, status);
}

export function getRequiredEnv(name: string): string {
  const value = Deno.env.get(name);
  if (!value || value.trim().length === 0) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

export function createServiceRoleClient(): SupabaseClient {
  const supabaseUrl = getRequiredEnv("SUPABASE_URL");
  const serviceRole = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY");
  return createClient(supabaseUrl, serviceRole, { auth: { persistSession: false } });
}

export function normalizeTier(value: unknown): Tier | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (normalized === "free" || normalized === "builder" || normalized === "pro") {
    return normalized;
  }
  return LEGACY_TIER_ALIAS[normalized] ?? null;
}

export function resolveTierOrDefault(value: unknown, fallback: Tier = "free"): Tier {
  return normalizeTier(value) ?? fallback;
}

export async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function randomApiKey(prefix = "sk_live"): string {
  const bytes = new Uint8Array(24);
  crypto.getRandomValues(bytes);
  const raw = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${prefix}_${raw}`;
}

export function toIsoOrNull(value: unknown): string | null {
  if (!value) return null;
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

export function parseJsonBody<T = Record<string, unknown>>(raw: string): T {
  if (!raw || raw.trim().length === 0) return {} as T;
  return JSON.parse(raw) as T;
}

export function formUrlEncode(payload: Record<string, string | number | boolean>): string {
  const body = new URLSearchParams();
  for (const [key, value] of Object.entries(payload)) {
    body.append(key, String(value));
  }
  return body.toString();
}

export async function stripeRequest(
  path: string,
  method: "GET" | "POST",
  secretKey: string,
  body?: string,
): Promise<Record<string, unknown>> {
  const response = await fetch(`https://api.stripe.com/v1${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${secretKey}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body ?? undefined,
  });

  const payload = await response.json() as Record<string, unknown>;
  if (!response.ok) {
    const errorObject = payload.error as Record<string, unknown> | undefined;
    const errorMessage = typeof errorObject?.message === "string"
      ? errorObject.message
      : `Stripe request failed: ${response.status}`;
    throw new Error(errorMessage);
  }

  return payload;
}

export function timingSafeEqualHex(left: string, right: string): boolean {
  if (left.length !== right.length) return false;
  let mismatch = 0;
  for (let i = 0; i < left.length; i += 1) {
    mismatch |= left.charCodeAt(i) ^ right.charCodeAt(i);
  }
  return mismatch === 0;
}

export async function verifyStripeSignature(rawBody: string, signatureHeader: string, webhookSecret: string): Promise<boolean> {
  const pieces = signatureHeader.split(",").map((part) => part.trim());
  const timestampPart = pieces.find((piece) => piece.startsWith("t="));
  const signaturePart = pieces.find((piece) => piece.startsWith("v1="));
  if (!timestampPart || !signaturePart) return false;

  const timestamp = timestampPart.slice(2);
  const expected = signaturePart.slice(3);
  const payload = `${timestamp}.${rawBody}`;

  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(webhookSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );

  const signature = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  const actual = [...new Uint8Array(signature)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return timingSafeEqualHex(actual, expected);
}

export async function fetchPlanLimits(
  client: SupabaseClient,
  tier: Tier,
): Promise<{ rpm: number; day: number; month: number | null }> {
  let row: Record<string, unknown> | null = null;

  const tryPlanColumn = await client
    .from("plan_entitlements")
    .select("*")
    .eq("plan", tier)
    .maybeSingle();

  if (!tryPlanColumn.error && tryPlanColumn.data) {
    row = tryPlanColumn.data as Record<string, unknown>;
  } else {
    const tryTierColumn = await client
      .from("plan_entitlements")
      .select("*")
      .eq("tier", tier)
      .maybeSingle();
    if (!tryTierColumn.error && tryTierColumn.data) {
      row = tryTierColumn.data as Record<string, unknown>;
    }
  }

  const asNumber = (value: unknown): number | null => {
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }
    return null;
  };

  const rpm = asNumber(row?.rate_limit_per_minute ?? row?.requests_per_minute ?? row?.rpm) ?? 10;
  const day = asNumber(row?.rate_limit_per_day ?? row?.requests_per_day ?? row?.rpd) ?? 500;
  const month = asNumber(row?.monthly_request_cap ?? row?.requests_per_month ?? row?.rpm_monthly);

  return {
    rpm: Math.trunc(rpm),
    day: Math.trunc(day),
    month: month === null ? null : Math.trunc(month),
  };
}

export async function requirePortalToken(req: Request): Promise<void> {
  const required = getRequiredEnv("PORTAL_API_TOKEN");
  const provided = req.headers.get("x-portal-token")?.trim();
  if (!provided || provided !== required) {
    throw new Error("Unauthorized portal token");
  }
}

import {
  CORS_HEADERS,
  createServiceRoleClient,
  errorResponse,
  jsonResponse,
} from "../_shared/billing.ts";

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS_HEADERS });
  if (req.method !== "POST") return errorResponse(405, "METHOD_NOT_ALLOWED", "Use POST.");

  try {
    const requiredSecret = Deno.env.get("WORKER_SECRET");
    const providedSecret = req.headers.get("x-worker-secret")?.trim();
    if (requiredSecret && providedSecret !== requiredSecret) {
      return errorResponse(401, "UNAUTHORIZED", "Invalid worker secret.");
    }

    const supabase = createServiceRoleClient();
    const { data, error } = await supabase.rpc("run_billing_downgrade_worker");
    if (error) return errorResponse(500, "WORKER_RPC_FAILED", error.message);
    return jsonResponse({ ok: true, result: data });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return errorResponse(500, "WORKER_FAILED", message);
  }
});

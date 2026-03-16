declare const Deno: any;

const encoder = new TextEncoder();

export type TimingMetric = {
  name: string;
  dur: number;
  desc?: string;
};

export function getRequestId(req: Request): string {
  return (
    req.headers.get("x-request-id") ||
    req.headers.get("x-trace-id") ||
    crypto.randomUUID()
  );
}

export function payloadBytes(value: unknown): number {
  if (typeof value === "string") return encoder.encode(value).length;
  return encoder.encode(JSON.stringify(value ?? null)).length;
}

export function buildServerTiming(metrics: TimingMetric[]): string {
  return metrics
    .filter((m) => Number.isFinite(m.dur) && m.dur >= 0)
    .map((m) => {
      const safeName = (m.name || "step").replace(/[^a-zA-Z0-9_-]/g, "_");
      const base = `${safeName};dur=${m.dur.toFixed(1)}`;
      return m.desc ? `${base};desc="${m.desc.replace(/"/g, "")}"` : base;
    })
    .join(", ");
}

export async function safeJsonBody<T = Record<string, unknown>>(
  req: Request,
  maxBytes = 64 * 1024
): Promise<{ ok: true; value: T } | { ok: false; error: string }> {
  const text = await req.text();
  if (!text.trim()) return { ok: true, value: {} as T };

  const size = encoder.encode(text).length;
  if (size > maxBytes) {
    return { ok: false, error: `Request body too large (${size} bytes)` };
  }

  try {
    return { ok: true, value: JSON.parse(text) as T };
  } catch {
    return { ok: false, error: "Invalid JSON body" };
  }
}

type JsonInit = {
  status?: number;
  cors?: Record<string, string>;
  requestId?: string;
  cacheControl?: string;
  timings?: TimingMetric[];
  extraHeaders?: Record<string, string>;
};

export function jsonResponse(payload: unknown, init: JsonInit = {}): Response {
  const body = typeof payload === "string" ? payload : JSON.stringify(payload);
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(init.cors || {}),
    ...(init.extraHeaders || {}),
  };

  if (init.requestId) headers["X-Request-Id"] = init.requestId;
  if (init.cacheControl) headers["Cache-Control"] = init.cacheControl;
  if (init.timings?.length) headers["Server-Timing"] = buildServerTiming(init.timings);

  headers["X-Payload-Bytes"] = String(payloadBytes(body));

  return new Response(body, {
    status: init.status ?? 200,
    headers,
  });
}

export function weakEtag(seed: string): string {
  const input = encoder.encode(seed);
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input[i];
    hash = Math.imul(hash, 16777619);
  }
  return `W/"${(hash >>> 0).toString(16)}"`;
}


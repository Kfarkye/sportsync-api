export type CanonicalOdds = {
    homeSpread: number | null;
    awaySpread: number | null;
    homeSpreadOdds: number | null;
    awaySpreadOdds: number | null;
    total: number | null;
    overOdds: number | null;
    underOdds: number | null;
    homeML: number | null;
    awayML: number | null;
    drawML: number | null;
    provider: string;
    isLive: boolean;
    updatedAt: string; // ISO
    hasOdds: boolean;
};

type UnknownRecord = Record<string, unknown>;

const isRecord = (v: unknown): v is UnknownRecord =>
    typeof v === "object" && v !== null && !Array.isArray(v);

const toNullNumber = (v: unknown): number | null => {
    if (v === null || v === undefined) return null;
    if (typeof v === "number") return Number.isFinite(v) ? v : null;
    if (typeof v === "string") {
        const s = v.trim();
        if (!s) return null;
        // Normalize unicode minus and remove odds formatting noise
        const cleaned = s
            .replace(/\u2212/g, "-") // unicode minus
            .replace(/[^\d\.\-+]/g, ""); // keep digits, dot, sign
        if (!cleaned || cleaned === "-" || cleaned === "+" || cleaned === ".") return null;
        const n = Number(cleaned);
        return Number.isFinite(n) ? n : null;
    }
    return null;
};

const toBool = (v: unknown, fallback = false): boolean => {
    if (typeof v === "boolean") return v;
    if (typeof v === "number") return v !== 0;
    if (typeof v === "string") {
        const s = v.trim().toLowerCase();
        if (["true", "1", "yes", "y"].includes(s)) return true;
        if (["false", "0", "no", "n"].includes(s)) return false;
    }
    return fallback;
};

const toISO = (v: unknown): string => {
    if (typeof v === "string") {
        const d = new Date(v);
        if (!Number.isNaN(d.getTime())) return d.toISOString();
    }
    if (v instanceof Date && !Number.isNaN(v.getTime())) return v.toISOString();
    return new Date().toISOString();
};

const toStringSafe = (v: unknown, fallback = ""): string =>
    typeof v === "string" ? v : fallback;

const hasAnyOdds = (o: Omit<CanonicalOdds, "hasOdds">): boolean => {
    const nums = [
        o.homeSpread,
        o.awaySpread,
        o.homeSpreadOdds,
        o.awaySpreadOdds,
        o.total,
        o.overOdds,
        o.underOdds,
        o.homeML,
        o.awayML,
        o.drawML,
    ];
    return nums.some((n) => typeof n === "number" && Number.isFinite(n));
};

// Extractors for common upstream shapes (keep this small + deterministic)
const get = (obj: UnknownRecord, path: string): unknown => {
    const parts = path.split(".");
    let cur: unknown = obj;
    for (const p of parts) {
        if (!isRecord(cur)) return undefined;
        cur = cur[p];
    }
    return cur;
};

// This is the ONLY function writers should call.
export const toCanonicalOdds = (
    input: unknown,
    meta: { provider: string; isLive: boolean; updatedAt?: unknown }
): CanonicalOdds => {
    const rec = isRecord(input) ? input : {};

    // Spreads: collapse spread_home/spread_home_value/homeSpread etc.
    const homeSpread =
        toNullNumber(rec.homeSpread) ??
        toNullNumber(rec.spread_home) ??
        toNullNumber(rec.spread_home_value) ??
        toNullNumber(get(rec, "spread_best.home.line")) ??
        null;

    const awaySpread =
        toNullNumber(rec.awaySpread) ??
        toNullNumber(rec.spread_away) ??
        toNullNumber(rec.spread_away_value) ??
        toNullNumber(get(rec, "spread_best.away.line")) ??
        (typeof homeSpread === "number" ? -homeSpread : null);

    const homeSpreadOdds =
        toNullNumber(rec.homeSpreadOdds) ??
        toNullNumber(rec.spread_home_odds) ??
        toNullNumber(get(rec, "spread_best.home.price")) ??
        null;

    const awaySpreadOdds =
        toNullNumber(rec.awaySpreadOdds) ??
        toNullNumber(rec.spread_away_odds) ??
        toNullNumber(get(rec, "spread_best.away.price")) ??
        null;

    const total =
        toNullNumber(rec.total) ??
        toNullNumber(rec.total_points) ??
        toNullNumber(get(rec, "total_best.total")) ??
        toNullNumber(get(rec, "total_best.line")) ??
        null;

    const overOdds =
        toNullNumber(rec.overOdds) ??
        toNullNumber(get(rec, "total_best.over.price")) ??
        toNullNumber(get(rec, "totals_best.over.price")) ??
        null;

    const underOdds =
        toNullNumber(rec.underOdds) ??
        toNullNumber(get(rec, "total_best.under.price")) ??
        toNullNumber(get(rec, "totals_best.under.price")) ??
        null;

    const homeML =
        toNullNumber(rec.homeML) ??
        toNullNumber(rec.home_ml) ??
        toNullNumber(get(rec, "h2h_best.home.price")) ??
        null;

    const awayML =
        toNullNumber(rec.awayML) ??
        toNullNumber(rec.away_ml) ??
        toNullNumber(get(rec, "h2h_best.away.price")) ??
        null;

    const drawML =
        toNullNumber(rec.drawML) ??
        toNullNumber(rec.draw_ml) ??
        toNullNumber(get(rec, "h2h_best.draw.price")) ??
        null;

    const updatedAt = toISO(meta.updatedAt ?? rec.updatedAt ?? rec.updated_at);
    const provider = meta.provider;
    const isLive = meta.isLive;

    const base: Omit<CanonicalOdds, "hasOdds"> = {
        homeSpread,
        awaySpread,
        homeSpreadOdds,
        awaySpreadOdds,
        total,
        overOdds,
        underOdds,
        homeML,
        awayML,
        drawML,
        provider,
        isLive,
        updatedAt,
    };

    return { ...base, hasOdds: hasAnyOdds(base) };
};

// Hard gate: use this right before DB write.
// If it throws, the writer must not write.
export function assertCanonicalOdds(o: unknown): asserts o is CanonicalOdds {
    if (!isRecord(o)) throw new Error("CanonicalOdds: not an object");

    const keys = [
        "homeSpread",
        "awaySpread",
        "homeSpreadOdds",
        "awaySpreadOdds",
        "total",
        "overOdds",
        "underOdds",
        "homeML",
        "awayML",
        "drawML",
        "provider",
        "isLive",
        "updatedAt",
        "hasOdds",
    ] as const;

    // No extra keys allowed (prevents drift)
    for (const k of Object.keys(o)) {
        if (!keys.includes(k as any)) throw new Error(`CanonicalOdds: extra key "${k}"`);
    }
    for (const k of keys) {
        if (!(k in o)) throw new Error(`CanonicalOdds: missing key "${k}"`);
    }

    const numOrNull = (v: unknown) =>
        v === null || (typeof v === "number" && Number.isFinite(v));

    const numFields = [
        "homeSpread",
        "awaySpread",
        "homeSpreadOdds",
        "awaySpreadOdds",
        "total",
        "overOdds",
        "underOdds",
        "homeML",
        "awayML",
        "drawML",
    ] as const;

    for (const f of numFields) {
        if (!numOrNull(o[f])) throw new Error(`CanonicalOdds: invalid ${f}`);
    }

    if (typeof o.provider !== "string" || !o.provider.trim())
        throw new Error("CanonicalOdds: invalid provider");

    if (typeof o.isLive !== "boolean") throw new Error("CanonicalOdds: invalid isLive");
    if (typeof o.hasOdds !== "boolean") throw new Error("CanonicalOdds: invalid hasOdds");

    if (typeof o.updatedAt !== "string") throw new Error("CanonicalOdds: invalid updatedAt");
    const d = new Date(o.updatedAt);
    if (Number.isNaN(d.getTime())) throw new Error("CanonicalOdds: updatedAt not ISO");
};

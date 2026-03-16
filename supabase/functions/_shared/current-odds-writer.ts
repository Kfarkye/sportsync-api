import { toCanonicalOdds, assertCanonicalOdds, CanonicalOdds } from "./odds-contract.ts";

type SupabaseLike = {
    from: (table: string) => {
        upsert: (values: any, opts?: any) => Promise<{ data?: any; error?: any }>;
    };
};

export async function writeCurrentOdds(params: {
    supabase: SupabaseLike;
    matchId: string;
    rawOdds: unknown;
    provider: string;
    isLive: boolean;
    updatedAt?: unknown;
}) {
    const canonical: CanonicalOdds = toCanonicalOdds(params.rawOdds, {
        provider: params.provider,
        isLive: params.isLive,
        updatedAt: params.updatedAt,
    });

    assertCanonicalOdds(canonical);

    const payload = {
        id: params.matchId,
        current_odds: canonical, // JSONB
        last_odds_update: canonical.updatedAt,
    };

    const { error } = await params.supabase
        .from("matches")
        .upsert(payload, { onConflict: "id" });

    if (error) throw new Error(`writeCurrentOdds failed: ${error.message ?? String(error)}`);
}

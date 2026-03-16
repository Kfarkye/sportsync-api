// Backward-compatible shim for legacy exports referenced by _shared/index.ts.
// Canonical odds logic now lives in odds-contract.ts.
export { toCanonicalOdds, assertCanonicalOdds, type CanonicalOdds } from './odds-contract.ts';

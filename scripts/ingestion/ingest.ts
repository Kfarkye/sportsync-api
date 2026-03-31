import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { StatePayload } from './types';

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

export async function ingestState(payload: StatePayload) {
  const isoDate = new Date().toISOString().split('T')[0];
  console.log(`[ingest] Starting: ${payload.state_code} (${payload.state_name})`);

  // ── Step 1: Upsert State ──────────────────────────────────────────
  const { error: stateErr } = await supabase.from('states').upsert({
    state_code: payload.state_code,
    state_name: payload.state_name,
    state_slug: payload.state_slug,
    hero_image_url: payload.hero_image_url,
  });
  if (stateErr) throw new Error(`[states] ${stateErr.message}`);
  console.log(`  ✓ states`);

  // ── Step 2: Upsert Sources & Build Memory Map ─────────────────────
  const sourceMap = new Map<string, number>();
  for (const src of payload.sources) {
    const { data, error } = await supabase
      .from('rx_sources')
      .upsert({ ...src, last_accessed: isoDate }, { onConflict: 'source_key' })
      .select('id')
      .single();
    if (error) throw new Error(`[rx_sources] ${src.source_key}: ${error.message}`);
    sourceMap.set(src.source_key, data.id);
  }
  console.log(`  ✓ rx_sources (${sourceMap.size} mapped)`);

  // ── Step 3: Upsert Overrides ──────────────────────────────────────
  const { error: ovrErr } = await supabase.from('state_rx_overrides').upsert(
    { state_code: payload.state_code, ...payload.overrides, last_verified: isoDate },
    { onConflict: 'state_code' }
  );
  if (ovrErr) throw new Error(`[state_rx_overrides] ${ovrErr.message}`);
  console.log(`  ✓ state_rx_overrides`);

  // ── Step 4: Upsert Access Facts ───────────────────────────────────
  const { error: accErr } = await supabase.from('state_access_facts').upsert(
    { state_code: payload.state_code, ...payload.access_facts, last_verified: isoDate },
    { onConflict: 'state_code' }
  );
  if (accErr) throw new Error(`[state_access_facts] ${accErr.message}`);
  console.log(`  ✓ state_access_facts`);

  // ── Step 5: Replace Rules ─────────────────────────────────────────
  await supabase.from('state_rx_rule_details').delete().eq('state_code', payload.state_code);
  if (payload.rules.length > 0) {
    const { error: rulesErr } = await supabase.from('state_rx_rule_details').insert(
      payload.rules.map((r) => ({ state_code: payload.state_code, ...r, last_verified: isoDate }))
    );
    if (rulesErr) throw new Error(`[state_rx_rule_details] ${rulesErr.message}`);
  }
  console.log(`  ✓ state_rx_rule_details (${payload.rules.length} rows)`);

  // ── Step 6: Replace Restrictions ──────────────────────────────────
  await supabase.from('state_rx_restrictions').delete().eq('state_code', payload.state_code);
  if (payload.restrictions.length > 0) {
    const { error: restErr } = await supabase.from('state_rx_restrictions').insert(
      payload.restrictions.map((r) => ({ state_code: payload.state_code, ...r, last_verified: isoDate }))
    );
    if (restErr) throw new Error(`[state_rx_restrictions] ${restErr.message}`);
  }
  console.log(`  ✓ state_rx_restrictions (${payload.restrictions.length} rows)`);

  // ── Step 7: Replace Segment Guidance ──────────────────────────────
  // Must delete items first (FK dependency), then guidance rows
  await supabase.from('segment_guidance_items').delete().eq('state_code', payload.state_code);
  await supabase.from('segment_state_guidance').delete().eq('state_code', payload.state_code);
  for (const seg of payload.segments) {
    const { error: sgErr } = await supabase.from('segment_state_guidance').insert({
      segment_slug: seg.segment_slug,
      state_code: payload.state_code,
      segment_data: seg.segment_data,
      last_verified: isoDate,
    });
    if (sgErr) throw new Error(`[segment_state_guidance] ${seg.segment_slug}: ${sgErr.message}`);

    if (seg.items.length > 0) {
      const { error: itemErr } = await supabase.from('segment_guidance_items').insert(
        seg.items.map((item) => ({
          segment_slug: seg.segment_slug,
          state_code: payload.state_code,
          ...item,
        }))
      );
      if (itemErr) throw new Error(`[segment_guidance_items] ${seg.segment_slug}: ${itemErr.message}`);
    }
  }
  console.log(`  ✓ segment_state_guidance (${payload.segments.length} segments)`);

  // ── Step 8: Replace Claims (Trust Layer) ──────────────────────────
  await supabase.from('rx_claims').delete().eq('state_code', payload.state_code);
  if (payload.claims.length > 0) {
    const claimRows = payload.claims.map((c) => {
      const sourceId = sourceMap.get(c.source_key);
      if (!sourceId) throw new Error(`[rx_claims] No source_id found for key: ${c.source_key}`);
      return {
        claim_text: c.claim_text,
        source_id: sourceId,
        evidence_type: c.evidence_type,
        state_code: payload.state_code,
        medication_slug: c.medication_slug,
        segment_slug: c.segment_slug,
        target_table: c.target_table,
        target_field: c.target_field,
        target_key: c.target_key,
        verified_by: 'ts_pipeline',
        last_verified: isoDate,
      };
    });
    const { error: claimErr } = await supabase.from('rx_claims').insert(claimRows);
    if (claimErr) throw new Error(`[rx_claims] ${claimErr.message}`);
  }
  console.log(`  ✓ rx_claims (${payload.claims.length} claims)`);

  console.log(`[ingest] ✅ ${payload.state_code} complete.`);
}

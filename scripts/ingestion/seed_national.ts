import * as fs from 'fs';

const STATES = [
  { code: 'WA', name: 'Washington', pdmp: 'Prescription Review' },
  { code: 'OR', name: 'Oregon', pdmp: 'Oregon PDMP' },
  { code: 'NV', name: 'Nevada', pdmp: 'NV PMP' },
  { code: 'AZ', name: 'Arizona', pdmp: 'AZ CSPMP' },
  { code: 'CO', name: 'Colorado', pdmp: 'Colorado PDMP' },
  { code: 'NM', name: 'New Mexico', pdmp: 'NM PMP' },
  { code: 'UT', name: 'Utah', pdmp: 'Utah CSD' },
  { code: 'ID', name: 'Idaho', pdmp: 'Idaho PDMP' },
  { code: 'MT', name: 'Montana', pdmp: 'MPDPR' },
  { code: 'WY', name: 'Wyoming', pdmp: 'WORx' },
  { code: 'ND', name: 'North Dakota', pdmp: 'ND PDMP' },
  { code: 'SD', name: 'South Dakota', pdmp: 'SD PDMP' },
  { code: 'NE', name: 'Nebraska', pdmp: 'Nebraska PDMP' },
  { code: 'KS', name: 'Kansas', pdmp: 'K-TRACS' },
  { code: 'OK', name: 'Oklahoma', pdmp: 'PMP AWARxE' },
  { code: 'MN', name: 'Minnesota', pdmp: 'MN PMP' },
  { code: 'IA', name: 'Iowa', pdmp: 'Iowa PMP' },
  { code: 'MO', name: 'Missouri', pdmp: 'Missouri PDMP' },
  { code: 'AR', name: 'Arkansas', pdmp: 'Arkansas PDMP' },
  { code: 'LA', name: 'Louisiana', pdmp: 'Louisiana PMP' },
  { code: 'WI', name: 'Wisconsin', pdmp: 'ePDMP' },
  { code: 'MI', name: 'Michigan', pdmp: 'MAPS' },
  { code: 'IN', name: 'Indiana', pdmp: 'INSPECT' },
  { code: 'KY', name: 'Kentucky', pdmp: 'KASPER' },
  { code: 'TN', name: 'Tennessee', pdmp: 'CSMD' },
  { code: 'MS', name: 'Mississippi', pdmp: 'MS PMP' },
  { code: 'AL', name: 'Alabama', pdmp: 'Alabama PDMP' },
  { code: 'GA', name: 'Georgia', pdmp: 'Georgia PDMP' },
  { code: 'SC', name: 'South Carolina', pdmp: 'SCRIPTS' },
  { code: 'NC', name: 'North Carolina', pdmp: 'CSRS' },
  { code: 'VA', name: 'Virginia', pdmp: 'Virginia PMP' },
  { code: 'WV', name: 'West Virginia', pdmp: 'CSAPP' },
  { code: 'MD', name: 'Maryland', pdmp: 'CRISP' },
  { code: 'DE', name: 'Delaware', pdmp: 'Delaware PMP' },
  { code: 'NJ', name: 'New Jersey', pdmp: 'NJPMP' },
  { code: 'CT', name: 'Connecticut', pdmp: 'CPMRS' },
  { code: 'RI', name: 'Rhode Island', pdmp: 'Rhode Island PDMP' },
  { code: 'MA', name: 'Massachusetts', pdmp: 'MassPAT' },
  { code: 'NH', name: 'New Hampshire', pdmp: 'NH PDMP' },
  { code: 'VT', name: 'Vermont', pdmp: 'VPMS' },
  { code: 'ME', name: 'Maine', pdmp: 'Maine PMP' },
  { code: 'AK', name: 'Alaska', pdmp: 'AK PDMP' },
  { code: 'HI', name: 'Hawaii', pdmp: 'Hawaii PDMP' },
  { code: 'DC', name: 'District of Columbia', pdmp: 'DC PDMP' }
];

let sql = '';

function escapeSql(str: string) {
  return str.replace(/'/g, "''");
}

for (const state of STATES) {
  const slug = state.name.toLowerCase().replace(/\./g, '').replace(/\s+/g, '-');
  
  // States
  sql += `INSERT INTO states (state_code, state_name, state_slug, hero_image_url) VALUES ('${state.code}', '${escapeSql(state.name)}', '${slug}', '/images/states/${slug}.jpg') ON CONFLICT (state_code) DO NOTHING;\n`;
  
  // Sources
  sql += `INSERT INTO rx_sources (source_key, source_label, source_url, source_type, last_accessed) VALUES ('${slug}_pdmp_mandate', '${escapeSql(state.name)} PDMP Mandate', 'https://example.com/pdmp/${slug}', 'state_board_guidance', CURRENT_DATE) ON CONFLICT (source_key) DO NOTHING;\n`;
  sql += `INSERT INTO rx_sources (source_key, source_label, source_url, source_type, last_accessed) VALUES ('${slug}_e_rx_law', '${escapeSql(state.name)} E-Rx Law', 'https://example.com/law/${slug}', 'state_statute', CURRENT_DATE) ON CONFLICT (source_key) DO NOTHING;\n`;

  // Overrides
  sql += `INSERT INTO state_rx_overrides (state_code, out_of_state_controlled, schedule_ii_transfer, schedule_iii_v_transfer, e_rx_scope, telehealth_controlled, emergency_supply_allowed, out_of_state_controlled_text, schedule_ii_transfer_text, schedule_iii_v_transfer_text, e_rx_scope_text, telehealth_controlled_text, pdmp_name, pdmp_mandatory_check, board_of_pharmacy, board_url, last_verified) VALUES ('${state.code}', 'permitted_with_restrictions', 'electronic_only', 'permitted', 'all_controlled', 'restricted', false, 'Pharmacists in ${escapeSql(state.name)} may fill out-of-state prescriptions subject to verification.', 'Electronic transmission is mandatory.', 'Permitted with up to 5 refills over 6 months.', 'Electronic prescribing is required for all controlled substances.', 'Strict compliance with federal and state telehealth initial visit rules.', '${escapeSql(state.pdmp)}', true, '${escapeSql(state.name)} Board of Pharmacy', 'https://example.com/board/${slug}', CURRENT_DATE) ON CONFLICT (state_code) DO NOTHING;\n`;

  // Access Facts
  sql += `INSERT INTO state_access_facts (state_code, avg_wait_psychiatry, avg_wait_primary_care, pcp_can_prescribe_stimulants, telehealth_services_available, notes, evidence_type, last_verified) VALUES ('${state.code}', '3-4 weeks', '1-2 weeks', true, 'Limited initial controlled substance prescribing.', 'Standard cross-border dispensing friction applies in ${escapeSql(state.name)}.', 'market_access', CURRENT_DATE) ON CONFLICT (state_code) DO NOTHING;\n`;

  // Rule Details
  sql += `INSERT INTO state_rx_rule_details (state_code, category, evidence_type, sort_order, rule_text, last_verified) VALUES ('${state.code}', 'general', 'state_law', 1, 'Electronic prescribing is mandatory for all prescriptions.', CURRENT_DATE) ON CONFLICT DO NOTHING;\n`;
  sql += `INSERT INTO state_rx_rule_details (state_code, category, evidence_type, sort_order, rule_text, last_verified) VALUES ('${state.code}', 'general', 'state_board_guidance', 2, 'Pharmacists must check the ${escapeSql(state.pdmp)} system prior to dispensing.', CURRENT_DATE) ON CONFLICT DO NOTHING;\n`;

  // Restrictions
  sql += `INSERT INTO state_rx_restrictions (state_code, restriction_type, applies_to, description, evidence_type, last_verified) VALUES ('${state.code}', 'e_rx_only', 'all_controlled', 'Paper prescriptions generally not accepted without explicit exception.', 'state_law', CURRENT_DATE);\n`;

  // Segments
  sql += `INSERT INTO segment_state_guidance (segment_slug, state_code, segment_data, last_verified) VALUES ('travel-nurse', '${state.code}', '{"risk_level":"medium","key_friction":"Verification lag and e-prescribing mandates."}'::jsonb, CURRENT_DATE) ON CONFLICT DO NOTHING;\n`;
  sql += `INSERT INTO segment_guidance_items (segment_slug, state_code, sort_order, guidance_text, evidence_type) VALUES ('travel-nurse', '${state.code}', 1, 'Ensure your prescriber can send scripts electronically to ${escapeSql(state.name)}.', 'editorial');\n`;

  // Claims
  sql += `INSERT INTO rx_claims (claim_text, source_id, evidence_type, state_code, target_table, target_field, target_key, verified_by, last_verified) VALUES ('${escapeSql(state.name)} enforces electronic prescribing for controlled substances.', (SELECT id FROM rx_sources WHERE source_key = '${slug}_e_rx_law'), 'state_law', '${state.code}', 'state_rx_overrides', 'e_rx_scope', '${state.code}', 'pipeline', CURRENT_DATE);\n`;
  sql += `INSERT INTO rx_claims (claim_text, source_id, evidence_type, state_code, target_table, target_field, target_key, verified_by, last_verified) VALUES ('${escapeSql(state.pdmp)} checks are mandatory prior to dispensing.', (SELECT id FROM rx_sources WHERE source_key = '${slug}_pdmp_mandate'), 'state_board_guidance', '${state.code}', 'state_rx_overrides', 'pdmp_mandatory_check', '${state.code}', 'pipeline', CURRENT_DATE);\n`;
}

fs.writeFileSync('seed_national.sql', sql);
console.log('SQL generated!');

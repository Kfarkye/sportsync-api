export interface RxSource {
  source_key: string;
  source_label: string;
  source_url: string;
  source_type: 'federal_register' | 'state_statute' | 'state_board_guidance' | 'cfr' | 'pharmacy_policy' | 'market_data' | 'news';
}

export interface StateRxOverrides {
  out_of_state_controlled: 'permitted' | 'permitted_with_restrictions' | 'electronic_only' | 'not_permitted' | 'varies';
  schedule_ii_transfer: 'permitted' | 'permitted_with_restrictions' | 'electronic_only' | 'not_permitted' | 'varies';
  schedule_iii_v_transfer: 'permitted' | 'permitted_with_restrictions' | 'electronic_only' | 'not_permitted' | 'varies';
  e_rx_scope: 'all_controlled' | 'schedule_ii_only' | 'not_required' | 'recommended';
  telehealth_controlled: 'allowed' | 'allowed_no_initial' | 'restricted' | 'not_permitted';
  emergency_supply_allowed: boolean;
  out_of_state_controlled_text: string;
  schedule_ii_transfer_text: string;
  schedule_iii_v_transfer_text: string;
  e_rx_scope_text: string;
  telehealth_controlled_text: string;
  emergency_supply_text: string | null;
  pdmp_name: string | null;
  pdmp_url: string | null;
  pdmp_mandatory_check: boolean;
  board_of_pharmacy: string;
  board_url: string;
  board_phone: string | null;
  special_restrictions: string | null;
}

export interface StateAccessFacts {
  avg_wait_psychiatry: string | null;
  avg_wait_primary_care: string | null;
  pcp_can_prescribe_stimulants: boolean;
  telehealth_services_available: string | null;
  notes: string | null;
  evidence_type: 'market_access';
  source_url: string | null;
}

export interface StateRxRuleDetail {
  category: 'schedule_ii' | 'schedule_iii_v' | 'non_controlled' | 'general' | 'telehealth';
  evidence_type: 'federal_law' | 'state_law' | 'state_board_guidance' | 'pharmacy_operational' | 'market_access' | 'editorial' | 'educational';
  sort_order: number;
  rule_text: string;
  source_url: string | null;
}

export interface StateRxRestriction {
  restriction_type: 'refuses_out_of_state_cii' | 'e_rx_only' | 'in_state_prescriber_required' | 'id_required' | 'prior_auth_required' | 'other';
  applies_to: 'all_controlled' | 'schedule_ii' | 'schedule_iii_v' | 'opioids_only' | 'stimulants_only' | 'buprenorphine_only' | 'all';
  description: string;
  workaround: string | null;
  evidence_type: 'federal_law' | 'state_law' | 'state_board_guidance' | 'pharmacy_operational' | 'market_access' | 'editorial' | 'educational';
  source_url: string | null;
}

export interface SegmentDataPayload {
  risk_level: 'low' | 'medium' | 'high';
  key_friction: string;
}

export interface SegmentGuidanceItem {
  sort_order: number;
  guidance_text: string;
  evidence_type: 'federal_law' | 'state_law' | 'state_board_guidance' | 'pharmacy_operational' | 'market_access' | 'editorial' | 'educational';
  source_url: string | null;
}

export interface SegmentGuidance {
  segment_slug: string;
  segment_data: SegmentDataPayload;
  items: SegmentGuidanceItem[];
}

export interface RxClaim {
  claim_text: string;
  source_key: string;
  evidence_type: 'federal_law' | 'state_law' | 'state_board_guidance' | 'pharmacy_operational' | 'market_access' | 'editorial' | 'educational';
  medication_slug: string | null;
  segment_slug: string | null;
  target_table: string | null;
  target_field: string | null;
  target_key: string | null;
}

export interface StatePayload {
  state_code: string;
  state_name: string;
  state_slug: string;
  hero_image_url: string | null;
  sources: RxSource[];
  overrides: StateRxOverrides;
  access_facts: StateAccessFacts;
  rules: StateRxRuleDetail[];
  restrictions: StateRxRestriction[];
  segments: SegmentGuidance[];
  claims: RxClaim[];
}

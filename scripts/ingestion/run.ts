import 'dotenv/config';
import { ingestState } from './ingest';
import { StatePayload } from './types';
import * as fs from 'fs';
import * as path from 'path';

async function run() {
  // Accept a filename argument, default to batch_1.json
  const file = process.argv[2] || 'payloads/batch_1.json';
  const filePath = path.resolve(__dirname, file);

  console.log(`\n📂 Loading: ${filePath}\n`);
  const raw = fs.readFileSync(filePath, 'utf-8');
  const parsed = JSON.parse(raw);

  // Handle both single objects and arrays
  const payloads: StatePayload[] = Array.isArray(parsed) ? parsed : [parsed];

  console.log(`🏭 Processing ${payloads.length} state(s)...\n`);

  let success = 0;
  let failed = 0;

  for (const state of payloads) {
    try {
      await ingestState(state);
      console.log(`✅ ${state.state_code} ingested successfully\n`);
      success++;
    } catch (e) {
      console.error(`❌ Failed to ingest ${state.state_code}:`, e, '\n');
      failed++;
    }
  }

  console.log(`\n🎯 Done. ${success} succeeded, ${failed} failed.`);
}

run();

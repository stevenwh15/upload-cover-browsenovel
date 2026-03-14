require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const axios = require('axios');
const fs = require('fs');
const path = require('path');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID;
const CF_API_TOKEN = process.env.CF_API_TOKEN;
const BASE_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/images/v1`;

const DATA_FILE = path.join(__dirname, '../../data/nu-cover.parquet');
const RESULTS_FILE = path.join(__dirname, '../upload-results.csv');
const FAILED_FILE = path.join(__dirname, '../upload-failed.csv');

// ---------------------------------------------------------------------------
// Readers
// ---------------------------------------------------------------------------

async function readParquet(filePath) {
  const parquet = require('@dsnp/parquetjs');
  const reader = await parquet.ParquetReader.openFile(filePath);
  const cursor = reader.getCursor(['bn_id', 'id', 'cover_url']);
  const rows = [];
  let record;
  while ((record = await cursor.next()) !== null) {
    rows.push(record);
  }
  await reader.close();
  return rows;
}

/** Returns Map<bn_id, { id, cover_url, cloudflare_id }> */
function loadResultsCsv() {
  const map = new Map();
  if (!fs.existsSync(RESULTS_FILE)) return map;

  const lines = fs.readFileSync(RESULTS_FILE, 'utf-8').split('\n');
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    // Simple CSV split (no quoted commas in these values)
    const [bn_id, id, cover_url, cloudflare_id] = line.split(',').map((v) => v.replace(/^"|"$/g, ''));
    if (bn_id) map.set(bn_id, { id, cover_url, cloudflare_id });
  }
  return map;
}

/** Returns Set<bn_id> */
function loadFailedCsv() {
  const set = new Set();
  if (!fs.existsSync(FAILED_FILE)) return set;

  const lines = fs.readFileSync(FAILED_FILE, 'utf-8').split('\n');
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const bn_id = line.split(',')[0].replace(/^"|"$/g, '');
    if (bn_id) set.add(bn_id);
  }
  return set;
}

// ---------------------------------------------------------------------------
// Live Cloudflare check
// ---------------------------------------------------------------------------

async function fetchCloudflareImageIds() {
  if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
    throw new Error('Missing CF_ACCOUNT_ID or CF_API_TOKEN');
  }

  let page = 1;
  const ids = new Set();
  console.log('Fetching live image IDs from Cloudflare...');

  while (true) {
    const response = await axios.get(BASE_URL, {
      params: { page, per_page: 100 },
      headers: { Authorization: `Bearer ${CF_API_TOKEN}` },
    });

    const images = response.data?.result?.images ?? [];
    if (images.length === 0) break;

    images.forEach((img) => ids.add(img.id));
    process.stdout.write(`\r  Fetched ${ids.size} IDs (page ${page})...`);
    page++;
  }

  console.log();
  return ids;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const isLive = process.argv.includes('--live');

  console.log('Reading parquet data...');
  const allRows = (await readParquet(DATA_FILE)).filter((r) => r.cover_url && r.bn_id);
  console.log(`Total rows with cover_url: ${allRows.length}`);

  const uploadedMap = loadResultsCsv();
  const failedSet = loadFailedCsv();

  const missing = [];
  const notStarted = [];

  for (const row of allRows) {
    if (uploadedMap.has(row.bn_id)) continue;
    if (failedSet.has(row.bn_id)) continue;
    notStarted.push(row);
  }

  console.log('\n=== Local Verification ===');
  console.log(`Total in parquet:       ${allRows.length}`);
  console.log(`Uploaded (results CSV): ${uploadedMap.size}`);
  console.log(`Failed (failed CSV):    ${failedSet.size}`);
  console.log(`Not yet attempted:      ${notStarted.length}`);

  if (notStarted.length > 0) {
    console.log('\nNot yet attempted (first 10):');
    notStarted.slice(0, 10).forEach((r) => console.log(`  ${r.bn_id}  ${r.id}`));
    if (notStarted.length > 10) console.log(`  ... and ${notStarted.length - 10} more`);
  }

  // --live: check each cloudflare_id still exists on Cloudflare
  if (isLive) {
    console.log('\n=== Live Cloudflare Check ===');
    const liveIds = await fetchCloudflareImageIds();
    console.log(`Total images on Cloudflare: ${liveIds.size}`);

    let presentCount = 0;
    const ghostEntries = []; // in results CSV but not on Cloudflare

    for (const [bn_id, { id, cloudflare_id }] of uploadedMap) {
      if (liveIds.has(cloudflare_id)) {
        presentCount++;
      } else {
        ghostEntries.push({ bn_id, id, cloudflare_id });
        missing.push({ bn_id, id, cloudflare_id });
      }
    }

    console.log(`Present on Cloudflare:      ${presentCount}/${uploadedMap.size}`);
    if (ghostEntries.length > 0) {
      console.log(`\nIn results CSV but NOT on Cloudflare (${ghostEntries.length}):`);
      ghostEntries.forEach((e) => console.log(`  ${e.bn_id}  ${e.id}  ${e.cloudflare_id}`));
    } else {
      console.log('All uploaded IDs confirmed present on Cloudflare.');
    }
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

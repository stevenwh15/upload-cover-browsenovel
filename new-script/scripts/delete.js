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

const CONCURRENCY = 32;
const RETRY_LIMIT = 3;
const INITIAL_BACKOFF_MS = 1000;

const RESULTS_FILE = path.join(__dirname, '../upload-results.csv');

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

/** Returns array of cloudflare_ids from upload-results.csv */
function loadCloudflareIds() {
  if (!fs.existsSync(RESULTS_FILE)) {
    console.error(`Results file not found: ${RESULTS_FILE}`);
    process.exit(1);
  }

  const lines = fs.readFileSync(RESULTS_FILE, 'utf-8').split('\n');
  const ids = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const parts = line.split(',');
    const cfId = parts[3]?.replace(/^"|"$/g, '');
    if (cfId) ids.push(cfId);
  }
  return ids;
}

async function deleteImage(imageId) {
  for (let attempt = 0; attempt < RETRY_LIMIT; attempt++) {
    try {
      await axios.delete(`${BASE_URL}/${imageId}`, {
        headers: { Authorization: `Bearer ${CF_API_TOKEN}` },
      });
      return true;
    } catch (error) {
      const isRateLimit = error.response?.data?.errors?.some((e) => e.code === 971);
      if (isRateLimit && attempt < RETRY_LIMIT - 1) {
        const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
        console.warn(`  [rate-limit] ${imageId} — retrying in ${backoff}ms`);
        await new Promise((r) => setTimeout(r, backoff));
      } else {
        const msg = error.response ? JSON.stringify(error.response.data) : error.message;
        console.error(`  [error] deleting ${imageId}: ${msg}`);
        return false;
      }
    }
  }
  return false;
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

async function deleteAll(ids) {
  const queue = [...ids];
  let deleted = 0;
  const failed = [];

  async function worker() {
    while (queue.length > 0) {
      const id = queue.shift();
      if (!id) continue;

      if (await deleteImage(id)) {
        deleted++;
        process.stdout.write(`\r  Deleted ${deleted}/${ids.length}...`);
      } else {
        failed.push(id);
      }
    }
  }

  const workers = Array(CONCURRENCY).fill(null).map(() => worker());
  await Promise.all(workers);
  console.log(); // newline after progress

  return { deleted, failed };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
    console.error('Missing CF_ACCOUNT_ID or CF_API_TOKEN in .env');
    process.exit(1);
  }

  const toDelete = loadCloudflareIds();
  console.log(`IDs loaded from upload-results.csv: ${toDelete.length}`);

  if (toDelete.length === 0) {
    console.log('Nothing to delete.');
    return;
  }

  console.log('\nDeleting...');
  const { deleted, failed } = await deleteAll(toDelete);

  console.log('\n=== Deletion Complete ===');
  console.log(`Deleted: ${deleted}/${toDelete.length}`);
  if (failed.length > 0) {
    console.log(`Failed IDs (${failed.length}):`);
    failed.forEach((id) => console.log(`  ${id}`));
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID;
const CF_API_TOKEN = process.env.CF_API_TOKEN;
const UPLOAD_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/images/v1`;

const METADATA = { source: 'nu-scrape', type: 'novel-cover' };

// Rate limit: 1200 req / 5 min  →  we use 1000 per window to stay safe
const CHUNK_SIZE = 1000;
const WINDOW_MS = 5 * 60 * 1000; // 5 minutes
const CONCURRENCY = 32;
const RETRY_LIMIT = 3;
const INITIAL_BACKOFF_MS = 1500;

const SAMPLE_SIZE = 100; // default run processes this many rows

const DATA_FILE = path.join(__dirname, '../../data/nu-cover.parquet');
const RESULTS_FILE = path.join(__dirname, '../upload-results.csv');
const FAILED_FILE = path.join(__dirname, '../upload-failed-updated.csv');

// ---------------------------------------------------------------------------
// CSV helpers (append-only, no external dep)
// ---------------------------------------------------------------------------

const RESULTS_HEADER = 'bn_id,id,cover_url,cloudflare_id\n';
const FAILED_HEADER = 'bn_id,id,cover_url\n';

function ensureCsv(filePath, header) {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, header, 'utf-8');
  }
}

function appendResultRow(bn_id, id, cover_url, cloudflare_id) {
  const line = `${csvEscape(bn_id)},${csvEscape(id)},${csvEscape(cover_url)},${csvEscape(cloudflare_id)}\n`;
  fs.appendFileSync(RESULTS_FILE, line, 'utf-8');
}

const failedIds = new Set();

function appendFailedRow(bn_id, id, cover_url) {
  if (failedIds.has(bn_id)) return;
  failedIds.add(bn_id);
  const line = `${csvEscape(bn_id)},${csvEscape(id)},${csvEscape(cover_url)}\n`;
  fs.appendFileSync(FAILED_FILE, line, 'utf-8');
}

function parseCsvLine(line) {
  const fields = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
      else if (ch === '"') { inQuotes = false; }
      else { cur += ch; }
    } else {
      if (ch === '"') { inQuotes = true; }
      else if (ch === ',') { fields.push(cur); cur = ''; }
      else { cur += ch; }
    }
  }
  fields.push(cur);
  return fields;
}

function csvEscape(val) {
  const s = String(val ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

/** Returns a Set of bn_ids already present in upload-results.csv */
function loadUploadedIds() {
  const uploaded = new Set();
  if (!fs.existsSync(RESULTS_FILE)) return uploaded;

  const lines = fs.readFileSync(RESULTS_FILE, 'utf-8').split('\n');
  // skip header (line 0) and empty trailing line
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const bn_id = parseCsvLine(line)[0];
    if (bn_id) uploaded.add(bn_id);
  }
  return uploaded;
}

/** Returns array of unique { bn_id, id, cover_url } from upload-failed.csv, and rewrites the file deduped */
function loadFailedRows() {
  if (!fs.existsSync(FAILED_FILE)) return [];

  const lines = fs.readFileSync(FAILED_FILE, 'utf-8').split('\n');
  const seen = new Set();
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const [bn_id, id, cover_url] = parseCsvLine(line);
    if (bn_id && !seen.has(bn_id)) {
      seen.add(bn_id);
      rows.push({ bn_id, id, cover_url });
    }
  }

  // Rewrite deduped
  const content = FAILED_HEADER + rows.map((r) => `${csvEscape(r.bn_id)},${csvEscape(r.id)},${csvEscape(r.cover_url)}`).join('\n') + (rows.length ? '\n' : '');
  fs.writeFileSync(FAILED_FILE, content, 'utf-8');

  return rows;
}

// ---------------------------------------------------------------------------
// Parquet reader (parquetjs)
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

// ---------------------------------------------------------------------------
// Cloudflare upload
// ---------------------------------------------------------------------------

async function uploadFromUrl(bn_id, id, cover_url) {
  for (let attempt = 0; attempt < RETRY_LIMIT; attempt++) {
    try {
      const form = new FormData();
      form.append('url', cover_url);
      form.append('metadata', JSON.stringify(METADATA));

      const cfResponse = await axios.post(UPLOAD_URL, form, {
        headers: {
          ...form.getHeaders(),
          Authorization: `Bearer ${CF_API_TOKEN}`,
        },
      });

      if (cfResponse.data?.success) {
        return cfResponse.data.result.id;
      }
      throw new Error(`Unexpected response: ${JSON.stringify(cfResponse.data)}`);
    } catch (error) {
      const msg = error.response ? JSON.stringify(error.response.data) : error.message;
      if (attempt < RETRY_LIMIT - 1) {
        const backoff = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
        console.warn(`  [retry ${attempt + 1}/${RETRY_LIMIT - 1}] ${id} — ${msg} — retrying in ${backoff}ms`);
        await sleep(backoff);
      } else {
        console.error(`  [error] ${id}: ${msg}`);
        return null;
      }
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Concurrency worker pool
// ---------------------------------------------------------------------------

async function processChunk(chunk, startIdx, total) {
  const queue = [...chunk];
  let successCount = 0;
  let failCount = 0;

  async function worker() {
    while (queue.length > 0) {
      const row = queue.shift();
      if (!row) continue;

      const { bn_id, id, cover_url } = row;
      const cfId = await uploadFromUrl(bn_id, id, cover_url);

      if (cfId) {
        appendResultRow(bn_id, id, cover_url, cfId);
        successCount++;
        console.log(`  [ok] (${startIdx + successCount + failCount}/${total}) ${id} → ${cfId}`);
      } else {
        appendFailedRow(bn_id, id, cover_url);
        failCount++;
        console.log(`  [fail] (${startIdx + successCount + failCount}/${total}) ${id}`);
      }
    }
  }

  const workers = Array(CONCURRENCY).fill(null).map(() => worker());
  await Promise.all(workers);

  return { successCount, failCount };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatMs(ms) {
  const m = Math.floor(ms / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  return `${m}m ${s}s`;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
    console.error('Missing CF_ACCOUNT_ID or CF_API_TOKEN in .env');
    process.exit(1);
  }

  const isFullRun = process.argv.includes('--all');
  const isRetryFailed = process.argv.includes('--retry-failed');

  ensureCsv(RESULTS_FILE, RESULTS_HEADER);
  ensureCsv(FAILED_FILE, FAILED_HEADER);

  // Deduplicate failed file on every run and seed failedIds
  loadFailedRows().forEach((r) => failedIds.add(r.bn_id));

  let toUpload;

  if (isRetryFailed) {
    console.log('Mode: RETRY FAILED');
    const failedRows = loadFailedRows();
    if (failedRows.length === 0) {
      console.log('No failed rows to retry.');
      return;
    }
    // Skip any that have since succeeded
    const alreadyUploaded = loadUploadedIds();
    toUpload = failedRows.filter((r) => !alreadyUploaded.has(r.bn_id));
    console.log(`Failed rows: ${failedRows.length} | Already succeeded: ${failedRows.length - toUpload.length} | Retrying: ${toUpload.length}\n`);
    // Clear the failed file so it only contains the ones that fail again
    fs.writeFileSync(FAILED_FILE, FAILED_HEADER, 'utf-8');
    failedIds.clear();
  } else {
    console.log(`Mode: ${isFullRun ? 'FULL RUN (--all)' : `SAMPLE (first ${SAMPLE_SIZE} rows)`}`);

    console.log('Reading parquet data...');
    let allRows = await readParquet(DATA_FILE);
    allRows = allRows.filter((r) => r.cover_url && r.bn_id);

    if (!isFullRun) {
      allRows = allRows.slice(0, SAMPLE_SIZE);
    }

    console.log(`Total rows to consider: ${allRows.length}`);

    const alreadyUploaded = loadUploadedIds();
    console.log(`Already uploaded (skipping): ${alreadyUploaded.size}`);

    toUpload = allRows.filter((r) => !alreadyUploaded.has(r.bn_id));
    console.log(`Remaining to upload: ${toUpload.length}\n`);
  }

  if (toUpload.length === 0) {
    console.log('Nothing to upload.');
    return;
  }

  let totalSuccess = 0;
  let totalFail = 0;
  const chunkCount = Math.ceil(toUpload.length / CHUNK_SIZE);

  for (let i = 0; i < toUpload.length; i += CHUNK_SIZE) {
    const chunkIndex = Math.floor(i / CHUNK_SIZE) + 1;
    const chunk = toUpload.slice(i, i + CHUNK_SIZE);
    const chunkStart = Date.now();

    console.log(`--- Chunk ${chunkIndex}/${chunkCount}: ${chunk.length} items ---`);

    const { successCount, failCount } = await processChunk(chunk, i, toUpload.length);
    totalSuccess += successCount;
    totalFail += failCount;

    const elapsed = Date.now() - chunkStart;
    console.log(`--- Chunk done: ${successCount} ok, ${failCount} failed (${formatMs(elapsed)}) ---`);

    // If there are more chunks, wait for the rest of the 5-minute window
    if (i + CHUNK_SIZE < toUpload.length) {
      const remaining = WINDOW_MS - elapsed;
      if (remaining > 0) {
        console.log(`Waiting ${formatMs(remaining)} before next chunk (rate limit)...`);
        await sleep(remaining);
      }
    }
  }

  console.log('\n=== Upload Complete ===');
  console.log(`Successful: ${totalSuccess}`);
  console.log(`Failed:     ${totalFail}`);
  console.log(`Results:    ${RESULTS_FILE}`);
  if (totalFail > 0) {
    console.log(`Failed CSV: ${FAILED_FILE}`);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

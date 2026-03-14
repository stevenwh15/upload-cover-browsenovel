require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const axios = require('axios');
const fs = require('fs');
const path = require('path');

const INPUT_CSV = path.join(__dirname, '../upload-failed.csv');
const OUTPUT_CSV = path.join(__dirname, '../upload-failed-updated.csv');
const OUTPUT_HEADER = 'bn_id,id,cover_url\n';
const DELAY = 500; // ms between requests

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

function csvEscape(val) {
  const s = String(val ?? '');
  if (s.includes(',') || s.includes('"') || s.includes('\n')) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function ensureCsv(filePath, header) {
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, header, 'utf-8');
  }
}

function appendRow(bn_id, id, cover_url) {
  const line = `${csvEscape(bn_id)},${csvEscape(id)},${csvEscape(cover_url)}\n`;
  fs.appendFileSync(OUTPUT_CSV, line, 'utf-8');
}

/** Returns a Set of bn_ids already present in the output CSV */
function loadScrapedIds() {
  const done = new Set();
  if (!fs.existsSync(OUTPUT_CSV)) return done;
  const lines = fs.readFileSync(OUTPUT_CSV, 'utf-8').split('\n');
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const bn_id = line.split(',')[0].replace(/^"|"$/g, '');
    if (bn_id) done.add(bn_id);
  }
  return done;
}

function parseInputCSV(text) {
  const lines = text.trim().split('\n');
  return lines.slice(1).map(line => {
    const parts = line.split(',');
    return { bn_id: parts[0], id: parts[1], cover_url: parts[2] };
  });
}

// ---------------------------------------------------------------------------
// Scraper
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const fetchWithRetries = async (url, retries = 5) => {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await axios.get(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; cover-scraper/1.0)' },
        timeout: 15000,
      });
      return res.data;
    } catch (err) {
      if (err.response && err.response.status === 429) {
        const wait = 10000 * (attempt + 1);
        console.warn(`Rate limited. Waiting ${wait}ms before retry ${attempt + 1}/${retries}`);
        await sleep(wait);
        continue;
      }
      if (attempt === retries) throw err;
      await sleep(3000);
    }
  }
};

const scrapeCover = async (novelId) => {
  const url = `https://www.novelupdates.com/series/${novelId}/`;
  const html = await fetchWithRetries(url);
  const match = html.match(/<div[^>]*class="seriesimg"[^>]*>[\s\S]*?<img[^>]*src="([^"]+)"/);
  return match ? match[1] : null;
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  ensureCsv(OUTPUT_CSV, OUTPUT_HEADER);

  const raw = fs.readFileSync(INPUT_CSV, 'utf-8');
  const rows = parseInputCSV(raw);

  const done = loadScrapedIds();
  const remaining = rows.filter(r => !done.has(r.bn_id));

  console.log(`Total: ${rows.length} | Already scraped: ${done.size} | Remaining: ${remaining.length}\n`);

  if (remaining.length === 0) {
    console.log('All done, nothing left to scrape.');
    return;
  }

  let success = 0;
  let failed = 0;

  for (let i = 0; i < remaining.length; i++) {
    const { bn_id, id, cover_url } = remaining[i];
    process.stdout.write(`[${i + 1}/${remaining.length}] ${id}... `);
    try {
      const newCover = await scrapeCover(id);
      if (newCover) {
        appendRow(bn_id, id, newCover);
        console.log(`OK -> ${newCover}`);
        success++;
      } else {
        appendRow(bn_id, id, cover_url);
        console.log(`NO COVER FOUND, keeping original`);
        failed++;
      }
    } catch (err) {
      appendRow(bn_id, id, cover_url);
      console.log(`ERROR: ${err.message}, keeping original`);
      failed++;
    }
    await sleep(DELAY);
  }

  console.log(`\nDone! Success: ${success} | Kept original: ${failed}`);
  console.log(`Output: ${OUTPUT_CSV}`);
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});

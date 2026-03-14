require('dotenv').config();
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const FormData = require('form-data');

// Rate limit is 1200 request per 5 minutes.

const CF_ACCOUNT_ID = "68e2367375f633f4fb9cc933be821680";
const CF_API_TOKEN = "LyysWwvlMiub5Ujc03_R2JiChc4mBqLATCoNXz4q";

const UPLOAD_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/images/v1`;
const CONCURRENCY_LIMIT = 32; 
const RETRY_LIMIT = 3;
const INITIAL_BACKOFF_MS = 1200;

const METADATA = {
  source: 'qidiantu-scrape',
  type: 'novel-cover'
};

async function uploadImage(filePath) {
  const form = new FormData();
  form.append('file', fs.createReadStream(filePath));
  form.append('metadata', JSON.stringify(METADATA));

  for (let attempt = 0; attempt < RETRY_LIMIT; attempt++) {
    try {
      const response = await axios.post(UPLOAD_URL, form, {
        headers: {
          ...form.getHeaders(),
          'Authorization': `Bearer ${CF_API_TOKEN}`,
        },
      });
      return response.data;
    } catch (error) {
      const isRateLimitError = error.response?.data?.errors?.some(e => e.code === 971);
      if (isRateLimitError && attempt < RETRY_LIMIT - 1) {
        const backoffTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
        console.warn(`Rate limit hit for ${path.basename(filePath)}. Retrying in ${backoffTime}ms...`);
        await new Promise(resolve => setTimeout(resolve, backoffTime));
      } else {
        console.error(`Error uploading ${path.basename(filePath)}:`, error.response ? error.response.data : error.message);
        return null;
      }
    }
  }
  return null;
}

async function main() {
  if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
    console.error('Please set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN environment variables.');
    return;
  }

  const imageDir = path.join(__dirname, 'cover-image');
  const allFiles = fs.readdirSync(imageDir).filter(f => f.endsWith('.jpeg') || f.endsWith('.jpg') || f.endsWith('.png'));

  const resultsFilePath = 'cloudflare-image-ids.json';
  let results = {};
  if (fs.existsSync(resultsFilePath)) {
      try {
          results = JSON.parse(fs.readFileSync(resultsFilePath, 'utf-8'));
          console.log(`Loaded ${Object.keys(results).length} existing image IDs.`);
      } catch (e) {
          console.error('Could not parse existing results file. Starting fresh.', e);
          results = {};
      }
  }

  const uploadedFileNames = new Set(Object.keys(results));
  const filesToUpload = allFiles.filter(file => !uploadedFileNames.has(path.parse(file).name));
  
  console.log(`Found ${allFiles.length} total files. ${filesToUpload.length} files need to be uploaded.`);

  if (filesToUpload.length === 0) {
      console.log('All files already uploaded.');
      return;
  }

  let successCount = 0;
  const failedFiles = [];

  const CHUNK_SIZE = 1000;
  const SLEEP_DURATION_MS = 5 * 60 * 1000;
  const filesToProcessQueue = [...filesToUpload];

  while (filesToProcessQueue.length > 0) {
    const chunkToProcess = filesToProcessQueue.splice(0, CHUNK_SIZE);
    console.log(`\n--- Processing a new chunk of ${chunkToProcess.length} files ---`);
    
    const fileQueue = [...chunkToProcess];

    async function worker() {
      while (fileQueue.length > 0) {
        const file = fileQueue.shift();
        if (!file) continue;

        const filePath = path.join(imageDir, file);
        const result = await uploadImage(filePath);

        if (result && result.success) {
          const fileNameWithoutExt = path.parse(file).name;
          results[fileNameWithoutExt] = result.result.id;
          successCount++;
          // Write to file immediately on success to save progress
          fs.writeFileSync(resultsFilePath, JSON.stringify(results, null, 2));
          console.log(`(${successCount}/${filesToUpload.length}) Uploaded ${file}: ${result.result.id} (progress saved)`);
        } else {
          failedFiles.push(file);
        }
      }
    }

    const workers = Array(CONCURRENCY_LIMIT).fill(null).map(() => worker());
    await Promise.all(workers);

    console.log(`--- Finished processing chunk ---`);
    if (filesToProcessQueue.length > 0) {
      console.log(`Pausing for 5 minutes before the next chunk...`);
      await new Promise(resolve => setTimeout(resolve, SLEEP_DURATION_MS));
      console.log('Resuming uploads...');
    }
  }

  // Final write to ensure everything is saved.
  fs.writeFileSync(resultsFilePath, JSON.stringify(results, null, 2));
  console.log(`Finished. Saved all image IDs to ${resultsFilePath}. Total entries: ${Object.keys(results).length}`);

  console.log(`\n--- Upload Summary for this run ---`);
  console.log(`Total files to process: ${filesToUpload.length}`);
  console.log(`Successful uploads: ${successCount}`);
  console.log(`Failed uploads: ${failedFiles.length}`);
  if (failedFiles.length > 0) {
    console.log('Failed files:', failedFiles);
  }
  console.log(`----------------------`);
}

main();

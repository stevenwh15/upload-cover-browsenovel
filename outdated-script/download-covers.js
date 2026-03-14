const fs = require('fs');
const path = require('path');
const axios = require('axios');
const csv = require('csv-parser');

const csvFilePath = 'novel.csv';
const outputDir = 'cover-image';
const concurrencyLimit = 10; // Number of concurrent downloads

// --- PROXY CONFIGURATION ---
// If you're using a proxy, set useProxy to true and fill in your proxy details.
const useProxy = false; // Set to true to use a proxy
const proxy = {
  protocol: 'http',
  host: '127.0.0.1',
  port: 8080,
  // auth: { // Uncomment and fill in if your proxy requires authentication
  //   username: 'your-username',
  //   password: 'your-password',
  // },
};
// --- END PROXY CONFIGURATION ---

// Create output directory if it doesn't exist
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir);
}

const downloadImage = async (url, filepath) => {
  try {
    const options = {
      url,
      method: 'GET',
      responseType: 'stream',
    };

    if (useProxy) {
      options.proxy = proxy;
    }

    const response = await axios(options);

    return new Promise((resolve, reject) => {
      const writer = fs.createWriteStream(filepath);
      response.data
        .pipe(writer)
        .on('finish', () => resolve())
        .on('error', e => reject(e));
    });
  } catch (error) {
    console.error(`Failed to download ${url}: ${error.message}`);
  }
};

const rows = [];
fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on('data', row => {
    rows.push(row);
  })
  .on('end', async () => {
    console.log('CSV file successfully processed. Starting downloads...');
    let successCount = 0;
    let failureCount = 0;
    const failedDownloads = [];

    for (let i = 0; i < rows.length; i += concurrencyLimit) {
      const chunk = rows.slice(i, i + concurrencyLimit);
      await Promise.all(
        chunk.map(async row => {
          const { slug, cover_url } = row;
          if (slug && cover_url) {
            try {
              const filename = `${slug}.jpeg`;
              const filepath = path.join(outputDir, filename);
              await downloadImage(cover_url, filepath);
              console.log(`Downloaded ${filename}`);
              successCount++;
            } catch (error) {
              console.error(`Failed to process ${slug}: ${error.message}`);
              failureCount++;
              failedDownloads.push({ slug, cover_url, error: error.message });
            }
          }
        }),
      );
    }

    console.log('\n--- Download Statistics ---');
    console.log(`Successful downloads: ${successCount}`);
    console.log(`Failed downloads: ${failureCount}`);
    if (failedDownloads.length > 0) {
      console.log('\nFailed downloads details:');
      failedDownloads.forEach(item => {
        console.log(`- Slug: ${item.slug}, URL: ${item.cover_url}, Error: ${item.error}`);
      });
    }
  });

require('dotenv').config();
const axios = require('axios');

const CF_ACCOUNT_ID = "68e2367375f633f4fb9cc933be821680";
const CF_API_TOKEN = "LyysWwvlMiub5Ujc03_R2JiChc4mBqLATCoNXz4q";

const BASE_URL = `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/images/v1`;
const CONCURRENCY_LIMIT = 32;
const RETRY_LIMIT = 3;
const INITIAL_BACKOFF_MS = 1000;

const METADATA_TO_MATCH = {
  source: 'qidiantu-scrape',
  type: 'novel-cover'
};

async function listAllImages() {
  let page = 1;
  const perPage = 100;
  let allImages = [];

  while (true) {
    try {
      const response = await axios.get(BASE_URL, {
        params: { page, per_page: perPage },
        headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` },
      });

      const { images } = response.data.result;
      if (images.length === 0) {
        break; 
      }
      allImages = allImages.concat(images);
      page++;
    } catch (error) {
      console.error('Error fetching images:', error.response ? error.response.data : error.message);
      return [];
    }
  }
  return allImages;
}

function imageHasMatchingMetadata(image) {
    if (!image.meta) return false;
    return Object.entries(METADATA_TO_MATCH).every(([key, value]) => image.meta[key] === value);
}

async function deleteImage(imageId) {
    for (let attempt = 0; attempt < RETRY_LIMIT; attempt++) {
        try {
            await axios.delete(`${BASE_URL}/${imageId}`, {
                headers: { 'Authorization': `Bearer ${CF_API_TOKEN}` },
            });
            console.log(`Successfully deleted image ${imageId}`);
            return true;
        } catch (error) {
            const isRateLimitError = error.response?.data?.errors?.some(e => e.code === 971);
            if (isRateLimitError && attempt < RETRY_LIMIT - 1) {
                const backoffTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
                console.warn(`Rate limit hit for deleting ${imageId}. Retrying in ${backoffTime}ms...`);
                await new Promise(resolve => setTimeout(resolve, backoffTime));
            } else {
                console.error(`Error deleting image ${imageId}:`, error.response ? error.response.data : error.message);
                return false;
            }
        }
    }
    return false;
}


async function main() {
  if (!CF_ACCOUNT_ID || !CF_API_TOKEN) {
    console.error('Please set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN environment variables.');
    return;
  }

  console.log('Fetching all images...');
  const allImages = await listAllImages();
  console.log(`Found ${allImages.length} total images.`);

  const imagesToDelete = allImages.filter(imageHasMatchingMetadata);
  console.log(`Found ${imagesToDelete.length} images to delete.`);

  if (imagesToDelete.length === 0) {
    console.log('No images to delete.');
    return;
  }
  
  let deletedCount = 0;
  const failedDeletions = [];
  const imageQueue = [...imagesToDelete];

  async function worker() {
    while (imageQueue.length > 0) {
      const image = imageQueue.shift();
      if (!image) continue;

      if (await deleteImage(image.id)) {
        deletedCount++;
      } else {
        failedDeletions.push(image.id);
      }
    }
  }

  const workers = Array(CONCURRENCY_LIMIT).fill(null).map(() => worker());
  await Promise.all(workers);

  console.log(`\n--- Deletion Summary ---`);
  console.log(`Successfully deleted ${deletedCount} of ${imagesToDelete.length} targeted images.`);
  if (failedDeletions.length > 0) {
    console.log(`Failed to delete ${failedDeletions.length} images:`, failedDeletions);
  }
  console.log(`------------------------`);
}

main();

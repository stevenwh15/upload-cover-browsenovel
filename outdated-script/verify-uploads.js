const fs = require('fs');
const path = require('path');

function main() {
    const imageDir = path.join(__dirname, 'cover-image');
    const resultsFilePath = 'cloudflare-image-ids.json';

    // 1. Get all image filenames from the cover-image directory
    const imageFiles = fs.readdirSync(imageDir)
        .filter(f => f.endsWith('.jpeg') || f.endsWith('.jpg') || f.endsWith('.png'))
        .map(file => path.parse(file).name); // Get filename without extension

    console.log(`Found ${imageFiles.length} image files in the 'cover-image' directory.`);

    // 2. Load the uploaded image data
    let uploadedImages = {};
    if (fs.existsSync(resultsFilePath)) {
        try {
            uploadedImages = JSON.parse(fs.readFileSync(resultsFilePath, 'utf-8'));
            console.log(`Loaded ${Object.keys(uploadedImages).length} entries from 'cloudflare-image-ids.json'.`);
        } catch (e) {
            console.error('Could not parse cloudflare-image-ids.json. Please ensure it is a valid JSON file.');
            return;
        }
    } else {
        console.error('cloudflare-image-ids.json not found. Cannot verify uploads.');
        return;
    }
    
    const uploadedImageNames = new Set(Object.keys(uploadedImages));

    // 3. Find the missing files
    const missingFiles = imageFiles.filter(fileName => !uploadedImageNames.has(fileName));

    // 4. Report the results
    console.log('\n--- Verification Report ---');
    if (missingFiles.length === 0) {
        console.log('Success! All images from the cover-image directory have been uploaded.');
    } else {
        console.log(`Found ${missingFiles.length} missing image(s):`);
        missingFiles.forEach(file => console.log(`- ${file}`));
    }
    console.log('---------------------------');
}

main();


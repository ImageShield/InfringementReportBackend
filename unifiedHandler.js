const { S3, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');
const { RekognitionClient, CompareFacesCommand } = require('@aws-sdk/client-rekognition');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const { getSecrets, log } = require('./utils');
const FormData = require('form-data');
const Jimp = require('jimp');
const fs = require('fs');
// Initialize AWS clients
const s3 = new S3();
const dynamoClient = new DynamoDB();
const dynamoDB = DynamoDBDocument.from(dynamoClient);
const rekognition = new RekognitionClient();

const API_KEY = process.env.Is_api_key;
const IMAGE_UPDATES_URL = process.env.image_update_endpoint;
const CLEARED_IMAGES_URL = process.env.cleared_image_endpoint;


exports.unifiedHandler = async (event) => {
    log('INFO', 'Processing Lambda Invoked', { total_images: event.length });
  
    const clearedImageIds = [];
    const processingPromises = event.map(async ({ original_id, url: imageUrl }) => {
    try {
      // --- 1. Download and process the source image ---
      const sourceImageBuffer = await processImageUrl(imageUrl);
      if (!sourceImageBuffer) {
        log('ERROR', 'Source image processing failed', { original_id });
        clearedImageIds.push(original_id);
        return;
      }
  
      // --- 2. Perform Bing Visual Search ---
      let searchResults = [];
      try {
        searchResults = await performBingSearchWithPagination(sourceImageBuffer);
        log('INFO', 'Bing search completed', { original_id, resultsCount: searchResults.length });
      } catch (error) {
        log('ERROR', 'Bing search failed', { original_id, error: error.message });
        clearedImageIds.push(original_id);
        return;
      }

  const comparePromises = searchResults.map(async (result) => {
    if (!isValidUrl(result.targetUrl)) {
      log('WARN', 'Skipping result with invalid target URL', { targetUrl: result.targetUrl });
      return null;
    }
    try {
      const targetImageBuffer = await processImageUrl(result.targetUrl);
      if (!targetImageBuffer) {
        log('WARN', 'Target image processing failed, skipping', {
          url: result.hostPageUrl,
          targetUrl: result.targetUrl
        });
        return null;
      }
      // Use Rekognition to compare the input (sourceImageBuffer) with the target image
      const compareResponse = await rekognition.send(new CompareFacesCommand({
        SourceImage: { Bytes: sourceImageBuffer },
        TargetImage: { Bytes: targetImageBuffer },
        SimilarityThreshold: 90
      }));
      if (compareResponse.FaceMatches && compareResponse.FaceMatches.length > 0) {
        log('INFO', 'Face match found', {
          url: result.hostPageUrl,
          similarity: compareResponse.FaceMatches[0].Similarity
        });
        return {
          url: result.hostPageUrl,
          malicious: false,
          false_positive_eligible: false
        };
      } else {
        return null;
      }
    } catch (error) {
      log('WARN', 'Failed to process search result', {
        url: result.hostPageUrl,
        targetUrl: result.targetUrl,
        error: error.message
      });
      return null;
    }
  });

  const compareResults = await Promise.allSettled(comparePromises);
  const matches = compareResults
    .filter(res => res.status === 'fulfilled' && res.value)
    .map(res => res.value);

  // --- 4. Notify the external system based on match results ---
  if (matches.length > 0) {
    await notifyImageUpdates(original_id, matches);
  } else {
    clearedImageIds.push(original_id);
  }

} catch (error) {
  log('ERROR', 'Processing error', { original_id, error: error.message, stack: error.stack });
  clearedImageIds.push(original_id);
}
return;
});
await Promise.all(processingPromises);
if (clearedImageIds.length > 0) {
  await notifyCleared(clearedImageIds);
}
};

  function isValidUrl(url) {
    try {
      // new URL() will throw if the URL is invalid.
      new URL(url);
      return true;
    } catch (err) {
      return false;
    }
  }

  async function processImageUrl(url) {
    // Validate URL before making request
    if (!isValidUrl(url)) {
      log('WARN', 'Image processing encountered an error', { url, error: 'Invalid URL' });
      return null;
    }
    
    try {
      const response = await axios.get(url, {
        responseType: 'arraybuffer',
        timeout: 15000,
        maxContentLength: 10 * 1024 * 1024,
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Accept': 'image/jpeg,image/png,*/*'
        }
      });
      
      if (response.status !== 200) {
        log('WARN', 'HTTP error fetching image', { status: response.status, url });
        return null;
      }
      
      const imageData = Buffer.from(response.data);
      let image;
      try {
        image = await Jimp.read(imageData);
      } catch (firstError) {
        try {
          image = new Jimp(1920, 1080);
          await image.bitmap.data.set(imageData);
        } catch (secondError) {
          log('WARN', 'Image processing failed after retries', {
            firstError: firstError.message,
            secondError: secondError.message,
            url
          });
          return null;
        }
      }
      
      if (image.hasAlpha()) {
        image.background(0xFFFFFFFF);
      }
      
      if (image.bitmap.width > 1920 || image.bitmap.height > 1080) {
        image.scaleToFit(1920, 1080);
      }
      
      return await image.quality(85).getBufferAsync(Jimp.MIME_JPEG);
      
    } catch (error) {
      log('WARN', 'Image processing encountered an error', { url, error: error.message });
      return null;
    }
  }

  async function performBingSearchWithPagination(imageBuffer) {
    const secrets = await getSecrets();
    const bingUrl = secrets.BING_API_URL;
    const bingKey = secrets.BING_API_KEY;
    const formData = new FormData();
    formData.append('image', imageBuffer, {
      filename: 'image.jpg',
      contentType: 'image/jpeg'
    });
  
    let allResults = [];
    let offset = 0;
    const count = 100;
    let hasMoreResults = true;
  
    while (hasMoreResults) {
      log('INFO', 'Sending Bing request', { offset, count });
      const pagedUrl = `${bingUrl}?offset=${offset}&count=${count}`;
      const response = await axios.post(pagedUrl, formData, {
        headers: {
          ...formData.getHeaders(),
          'Ocp-Apim-Subscription-Key': bingKey,
        },
        maxContentLength: Infinity,
        maxBodyLength: Infinity
      });
      if (!response.data.tags) {
        throw new Error('Unexpected Bing API response structure.');
      }
      // Process results as before (using similar parsing logic)
      const results = [];
      response.data.tags.forEach((tag) => {
        if (tag.actions) {
          tag.actions.forEach((action) => {
            if (action.data && Array.isArray(action.data.value)) {
              action.data.value.forEach((item) => {
                if (
                  item.contentUrl &&
                  isValidUrl(item.contentUrl) &&
                  (item.contentUrl.toLowerCase().endsWith('.jpg') ||
                   item.contentUrl.toLowerCase().endsWith('.jpeg') ||
                   item.contentUrl.toLowerCase().endsWith('.png'))
                ) {
                  results.push({
                    resultId: uuidv4(),
                    hostPageUrl: item.hostPageUrl,
                    targetUrl: item.contentUrl,
                    timestamp: new Date().toISOString(),
                  });
                }
              });
            }
          });
        }
      });
      allResults = allResults.concat(results);
      // If fewer results than count are returned, no more pages are available.
      if (results.length < count) {
        hasMoreResults = false;
      } else {
        offset += count;
      }
    }
    return allResults;
  }
  


  // Notify ImageShield of found infringements.
  async function notifyImageUpdates(original_id, matches) {
    // Filter matches to include only those with valid URLs
    const validMatches = matches.filter(match => isValidUrl(match.url));
    const payload = {
      id: original_id,
      img_data: validMatches
    };
    log('INFO', 'Notifying cleared_images', { IMAGE_UPDATES_URL, payload });
    try {
      const response = await axios.post(
        IMAGE_UPDATES_URL,
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': API_KEY
          }
        }
      );
      log('INFO', 'Successfully notified image_updates', { response: response.data });
    } catch (error) {
      log('ERROR', 'Failed to notify image_updates', { error: error.message });
    }
  }
  // Notify ImageShield of a cleared image (no infringements found).
  async function notifyCleared(original_ids) {
    try {
      const payload = {cleared_image_ids: original_ids};
      log('INFO', 'Notifying cleared_images', { CLEARED_IMAGES_URL, payload, count: original_ids.length });
      const response = await axios.post(
        CLEARED_IMAGES_URL,
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            'x-api-key': API_KEY
          }
        }
      );
      log('INFO', 'Successfully notified cleared_images', { response: response.data, count: original_ids.length });
    } catch (error) {
      log('ERROR', 'Failed to notify cleared_images', { error: error.message, count: original_ids.length });
    }
  }

  
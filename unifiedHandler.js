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

exports.unifiedHandler = async (event) => {
    const { original_id, imageUrl } = event;
    log('INFO', 'Processing Lambda Invoked', { original_id, imageUrl });

    try {
        // Download and process source image
        const sourceImageBuffer = await processImageUrl(imageUrl);
        if (sourceImageBuffer === null) {
            log('ERROR', 'Source image processing failed', { original_id });
            await updateFailedStatus(original_id);
            return {
                id: original_id,
                img_data: []
            };
        }
        await updateProgress(original_id, 20);

        // Perform Bing search
        let searchResults = [];
        try {
            searchResults = await performBingSearch(sourceImageBuffer);
            await updateProgress(original_id, 40);
            
            log('INFO', 'Bing search completed', { 
                original_id, 
                resultsCount: searchResults.length 
            });
        } catch (error) {
            log('ERROR', 'Bing search failed', {
                original_id,
                error: error.message
            });
            await updateFailedStatus(original_id);
            return {
                id: original_id,
                img_data: []
            };
        }

        // Process results with face comparison
        const matches = [];
        const total = searchResults.length;
        let processed = 0;

        for (const result of searchResults) {
            try {
                const targetImageBuffer = await processImageUrl(result.targetUrl);
                if (targetImageBuffer === null) {
                    log('WARN', 'Target image processing failed, skipping', {
                        url: result.hostPageUrl,
                        contentUrl: result.contentUrl
                    });
                    continue;  // Skip to next image
                }

                const compareResponse = await rekognition.send(new CompareFacesCommand({
                    SourceImage: { Bytes: sourceImageBuffer },
                    TargetImage: { Bytes: targetImageBuffer },
                    SimilarityThreshold: 90
                }));

                if (compareResponse.FaceMatches && compareResponse.FaceMatches.length > 0) {
                    matches.push({
                        url: result.hostPageUrl,
                        malicious: false,
                        false_positive_eligible: false
                    });
                    log('INFO', 'Face match found', {
                        url: result.hostPageUrl,
                        contentUrl: result.contentUrl,
                        similarity: compareResponse.FaceMatches[0].Similarity
                    });
                }
            } catch (error) {
                log('WARN', 'Failed to process result', {
                    url: result.hostPageUrl,
                    Contenturl: result.contentUrl,
                    error: error.message
                });
                // Continue processing other results
            }

            processed++;
            const progress = Math.round(40 + (processed / total * 60));
            await updateProgress(original_id, progress);
        }

        // Store final results
        await dynamoDB.put({
            TableName: 'ImageSearchResults',
            Item: {
                original_id: original_id,
                status: 'completed',
                progress: 100,
                matches: matches,
                timestamp: new Date().toISOString()
            }
        });

        log('INFO', 'Processing completed', { 
            original_id, 
            matches_found: matches.length 
        });

        return {
            id: original_id,
            img_data: matches
        };

    } catch (error) {
        log('ERROR', 'Processing error', { error: error.message, original_id });
        await updateFailedStatus(original_id);
        return {
            id: original_id,
            img_data: []
        };
    }
};

// exports.unifiedHandler = async (event) => {
//     const { original_id, imageUrl } = event;
//     log('INFO', 'Processing Lambda Invoked', { original_id, imageUrl });

//     try {
//         // Download and process source image
//         let sourceImageBuffer;
//         try {
//             sourceImageBuffer = await processImageUrl(imageUrl);
//             await updateProgress(original_id, 20);
//         } catch (error) {
//             log('ERROR', 'Failed to process source image', { 
//                 original_id, 
//                 error: error.message 
//             });
//             throw error;
//         }

//         // Perform Bing search
//         const searchResults = await performBingSearch(sourceImageBuffer);
//         await updateProgress(original_id, 40);
        
//         log('INFO', 'Bing search completed', { 
//             original_id, 
//             resultsCount: searchResults.length 
//         });

//         // Process results with face comparison
//         const matches = [];
//         const total = searchResults.length;
//         let processed = 0;

//         for (const result of searchResults) {
//             let retries = 0;
//             while (retries <= MAX_RETRIES) {
//                 try {
//                     // Get image URL from the result
//                     const imageUrl = result.targetUrl;
//                     let targetImageBuffer;
                    
//                     try {
//                         targetImageBuffer = await processImageUrl(imageUrl);
//                     } catch (imageError) {
//                         log('WARN', 'Failed to process image', {
//                             url: result.hostPageUrl,
//                             imageUrl: imageUrl,
//                             error: imageError.message
//                         });
//                         break; // Skip retries for image processing errors
//                     }

//                     // Compare faces
//                     const compareResponse = await rekognition.send(new CompareFacesCommand({
//                         SourceImage: { Bytes: sourceImageBuffer },
//                         TargetImage: { Bytes: targetImageBuffer },
//                         SimilarityThreshold: 90
//                     }));

//                     if (compareResponse.FaceMatches && compareResponse.FaceMatches.length > 0) {
//                         matches.push({
//                             url: result.hostPageUrl,
//                             malicious: false,
//                             false_positive_eligible: false
//                         });
//                         log('INFO', 'Face match found', {
//                             url: result.hostPageUrl,
//                             similarity: compareResponse.FaceMatches[0].Similarity
//                         });
//                     }
//                     break; // Success, exit retry loop

//                 } catch (processError) {
//                     if (processError.name === 'InvalidImageFormatException' || 
//                         processError.message.includes('Invalid image format')) {
//                         log('WARN', 'Invalid image format', {
//                             url: result.hostPageUrl,
//                             error: processError.message
//                         });
//                         break; // Don't retry format errors
//                     }
                    
//                     retries++;
//                     if (retries <= MAX_RETRIES) {
//                         await new Promise(resolve => setTimeout(resolve, 1000 * retries));
//                         continue;
//                     }
                    
//                     log('WARN', 'Failed to process result after retries', {
//                         url: result.hostPageUrl,
//                         error: processError.message
//                     });
//                 }
//             }

//             processed++;
//             const progress = Math.round(40 + (processed / total * 60));
//             await updateProgress(original_id, progress);
//         }

//         // Store final results
//         await dynamoDB.put({
//             TableName: 'ImageSearchResults',
//             Item: {
//                 original_id: original_id,
//                 status: 'completed',
//                 progress: 100,
//                 matches: matches,
//                 timestamp: new Date().toISOString()
//             }
//         }, {
//             removeUndefinedValues: true
//         });

//         log('INFO', 'Processing completed', { 
//             original_id, 
//             matches_found: matches.length 
//         });

//         return {
//             id: original_id,
//             img_data: matches
//         };

//     } catch (error) {
//         log('ERROR', 'Processing error', { error: error.message, original_id });
//         await updateFailedStatus(original_id);
//         return {
//             id: original_id,
//             img_data: []
//         };
//     }
// };

// async function processImageUrl(url) {
//     try {
//         // Download image with increased timeout and proper headers
//         const response = await axios.get(url, {
//             responseType: 'arraybuffer',
//             timeout: 10000,
//             maxContentLength: 10 * 1024 * 1024, // 10MB limit
//             headers: {
//                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
//                 'Accept': '*/*'  // Accept all content types
//             },
//             validateStatus: (status) => status < 500 // Accept any status < 500
//         });

//         if (response.status !== 200) {
//             throw new Error(`HTTP status ${response.status}`);
//         }

//         // Log content type for debugging
//         log('INFO', 'Image response received', {
//             contentType: response.headers['content-type'],
//             url: url,
//             size: response.data.length
//         });

//         try {
//             // Try to read the buffer directly with Jimp
//             const image = await Jimp.read(Buffer.from(response.data));
            
//             // Log original image details
//             log('INFO', 'Image processed', {
//                 originalMime: image.getMIME(),
//                 width: image.getWidth(),
//                 height: image.getHeight()
//             });
            
//             // Resize if too large (Rekognition has limits)
//             if (image.bitmap.width > 1920 || image.bitmap.height > 1080) {
//                 image.scaleToFit(1920, 1080);
//             }

//             // Force convert to JPEG with specific quality
//             const buffer = await image.quality(85).getBufferAsync(Jimp.MIME_JPEG);
            
//             // Verify the processed buffer
//             if (!buffer || buffer.length === 0) {
//                 throw new Error('Processed image buffer is empty');
//             }

//             return buffer;
//         } catch (jimpError) {
//             log('ERROR', 'Image processing error', { 
//                 error: jimpError.message,
//                 contentType: response.headers['content-type'],
//                 url: url
//             });
//             throw new Error(`Image processing failed: ${jimpError.message}`);
//         }

//     } catch (error) {
//         // Enhanced error handling with specific cases
//         if (error.response?.status === 403) {
//             throw new Error('Access forbidden');
//         } else if (error.response?.status === 503) {
//             throw new Error('Service unavailable');
//         } else if (error.code === 'ENOTFOUND') {
//             throw new Error('Domain not found');
//         } else if (error.code === 'ETIMEDOUT') {
//             throw new Error('Request timed out');
//         } else if (error.response?.status === 404) {
//             throw new Error('Image not found');
//         } else if (error.message.includes('maxContentLength')) {
//             throw new Error('Image too large');
//         }
        
//         // If none of the above, throw the original error with additional context
//         throw new Error(`Failed to process image: ${error.message}`);
//     }
// }

// async function processImageUrl(url) {
//     try {
//         // Download image with increased timeout and proper headers
//         const response = await axios.get(url, {
//             responseType: 'arraybuffer',
//             timeout: 10000,
//             maxContentLength: 10 * 1024 * 1024, // 10MB limit
//             headers: {
//                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
//                 'Accept': '*/*'
//             },
//             validateStatus: (status) => status < 500
//         });

//         if (response.status !== 200) {
//             throw new Error(`HTTP status ${response.status}`);
//         }

//         log('INFO', 'Image response received', {
//             contentType: response.headers['content-type'],
//             url: url,
//             size: response.data.length
//         });

//         try {
//             // Try to read the buffer with Jimp with additional error handling
//             const image = await Jimp.read(Buffer.from(response.data));
            
//             // Log original image details for debugging
//             log('INFO', 'Image loaded', {
//                 originalMime: image.getMIME(),
//                 width: image.getWidth(),
//                 height: image.getHeight()
//             });
            
//             // Resize if too large
//             if (image.bitmap.width > 1920 || image.bitmap.height > 1080) {
//                 image.scaleToFit(1920, 1080);
//             }

//             // Always convert to JPEG with optimal quality
//             image.quality(85);
            
//             // If image has transparency, set white background
//             if (image.hasAlpha()) {
//                 image.background(0xFFFFFFFF);
//             }

//             const buffer = await image.getBufferAsync(Jimp.MIME_JPEG);
            
//             if (!buffer || buffer.length === 0) {
//                 throw new Error('Processed image buffer is empty');
//             }

//             log('INFO', 'Image processed successfully', {
//                 finalSize: buffer.length,
//                 width: image.getWidth(),
//                 height: image.getHeight()
//             });

//             return buffer;

//         } catch (jimpError) {
//             log('ERROR', 'Jimp processing error', {
//                 error: jimpError.message,
//                 contentType: response.headers['content-type'],
//                 url: url
//             });
//             throw new Error(`Image processing failed: ${jimpError.message}`);
//         }

//     } catch (error) {
//         if (error.response?.status === 403) {
//             throw new Error('Access forbidden');
//         } else if (error.response?.status === 503) {
//             throw new Error('Service unavailable');
//         } else if (error.code === 'ENOTFOUND') {
//             throw new Error('Domain not found');
//         } else if (error.code === 'ETIMEDOUT') {
//             throw new Error('Request timed out');
//         }
        
//         throw new Error(`Failed to process image: ${error.message}`);
//     }
// }

async function processImageUrl(url) {
    try {
        // Try different Accept headers to handle WebP
        const response = await axios.get(url, {
            responseType: 'arraybuffer',
            timeout: 10000,
            maxContentLength: 10 * 1024 * 1024,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'image/jpeg,image/png,*/*'
            }
        });

        if (response.status !== 200) {
            log('WARN', 'HTTP error', { status: response.status, url });
            return null;
        }

        const imageData = Buffer.from(response.data);
        
        // Try different approaches to read the image
        let image;
        try {
            // First try: direct read
            image = await Jimp.read(imageData);
        } catch (firstError) {
            try {
                // Second try: create new image and load data
                image = new Jimp(1920, 1080);  // Create blank image
                await image.bitmap.data.set(imageData);  // Load data directly
            } catch (secondError) {
                log('WARN', 'Image processing failed after retries', {
                    firstError: firstError.message,
                    secondError: secondError.message,
                    url: url
                });
                return null;
            }
        }

        // Process the image
        if (image.hasAlpha()) {
            image.background(0xFFFFFFFF);
        }
        
        if (image.bitmap.width > 1920 || image.bitmap.height > 1080) {
            image.scaleToFit(1920, 1080);
        }

        const buffer = await image.quality(85).getBufferAsync(Jimp.MIME_JPEG);
        
        log('INFO', 'Image processed successfully', {
            finalSize: buffer.length,
            width: image.getWidth(),
            height: image.getHeight()
        });

        return buffer;

    } catch (error) {
        log('WARN', 'Image processing failed', {
            url: url,
            error: error.message
        });
        return null;
    }
}


async function performBingSearch(imageBuffer) {
    const secrets = await getSecrets();
    const formData = new FormData();
    formData.append('image', imageBuffer, {
        filename: 'image.jpg',
        contentType: 'image/jpeg'
    });

    log('INFO', 'Sending request to Bing Visual Search API');

    const searchResponse = await axios.post(
        secrets.BING_API_URL,
        formData,
        {
            headers: {
                ...formData.getHeaders(),
                'Ocp-Apim-Subscription-Key': secrets.BING_API_KEY,
            },
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        }
    );

    log('INFO', 'Bing API Response received', { 
        status: searchResponse.status,
        hasTags: !!searchResponse.data.tags,
        tagCount: searchResponse.data.tags?.length || 0
    });

    return searchResponse.data.tags
        ? searchResponse.data.tags.flatMap((tag) =>
            tag.actions?.flatMap((action) =>
                action.data?.value?.map((item) => ({
                    resultId: uuidv4(),
                    hostPageUrl: item.hostPageUrl,
                    targetUrl: item.contentUrl,
                    timestamp: new Date().toISOString(),
                })) || []
            ) || []
        )
        : [];
}

async function updateProgress(original_id, progress) {
    try {
        await dynamoDB.update({
            TableName: 'ImageSearchResults',
            Key: { original_id },
            UpdateExpression: 'SET progress = :progress',
            ExpressionAttributeValues: { ':progress': progress }
        });
    } catch (error) {
        log('WARN', 'Failed to update progress', { error: error.message });
    }
}

async function updateFailedStatus(original_id) {
    try {
        await dynamoDB.put({
            TableName: 'ImageSearchResults',
            Item: {
                original_id,
                status: 'failed',
                progress: 0,
                matches: [],
                timestamp: new Date().toISOString()
            }
        });
    } catch (error) {
        log('WARN', 'Failed to update failed status', { error: error.message });
    }
}
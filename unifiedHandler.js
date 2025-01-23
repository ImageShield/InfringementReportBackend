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

// Constants
const BUCKET_NAME = 'infringementuploads';
const SEARCH_METADATA_TABLE = 'SearchMetadata';
const SEARCH_RESULTS_TABLE = 'SearchResults';
const CONCURRENT_PROCESSING = 50; // Process 50 images at a time

exports.unifiedHandler = async (event) => {
    const { searchId, imageId, s3Key, imageBase64, fullName, location, employer } = event;
    log('INFO', 'Processing Lambda Invoked', { searchId, imageId, s3Key });

    try {
        await processSearch(searchId, imageId, s3Key, imageBase64, fullName, location, employer);
    } catch (error) {
        log('ERROR', 'Processing error', { error: error.message, searchId });
        await updateSearchStatus(searchId, 'failed', 0);
    }
};

async function processSearch(searchId, imageId, s3Key, imageBase64, fullName, location, employer) {
    try {
        const base64Data = imageBase64.replace(/^data:image\/\w+;base64,/, "");
        const imageBuffer = Buffer.from(base64Data, "base64");

        // Run Bing and Google searches in parallel
        log('INFO', 'Starting parallel searches');
        const [bingResults, googleResults] = await Promise.all([
            performBingSearch(imageBuffer),
            performGoogleSearch(fullName, location, employer)
        ]);

        // Combine all results while preserving source information
        const searchResults = [
            ...bingResults,
            ...googleResults
        ];

        log('INFO', 'Combined search results', { 
            totalResults: searchResults.length,
            bingResults: bingResults.length,
            googleResults: googleResults.length
        });

        // Process images with controlled concurrency
        const matches = {
            bing: [],
            google: []
        };
        let processed = 0;

        for (let i = 0; i < searchResults.length; i += CONCURRENT_PROCESSING) {
            const batch = searchResults.slice(i, i + CONCURRENT_PROCESSING);
            const batchPromises = batch.map(result => processImage(result, s3Key));
            
            const batchResults = await Promise.all(batchPromises);
            const validResults = batchResults.filter(result => result !== null);

            // Separate results by source
            validResults.forEach(result => {
                if (result.source === 'bing') {
                    matches.bing.push(result);
                } else {
                    matches.google.push(result);
                }
            });
            
            processed += batch.length;
            const progress = Math.round((processed / searchResults.length) * 100);
            
            await updateSearchStatus(searchId, 'processing', progress, matches, {
                totalProcessed: processed,
                totalAvailable: searchResults.length,
                bingMatches: matches.bing.length,
                googleMatches: matches.google.length
            });

            log('INFO', 'Batch processed', { 
                batchNumber: Math.floor(i/CONCURRENT_PROCESSING) + 1,
                totalBatches: Math.ceil(searchResults.length/CONCURRENT_PROCESSING),
                bingMatches: matches.bing.length,
                googleMatches: matches.google.length,
                totalProcessed: processed
            });
        }

        await updateSearchStatus(searchId, 'completed', 100, matches, {
            totalProcessed: processed,
            totalAvailable: searchResults.length,
            bingMatches: matches.bing.length,
            googleMatches: matches.google.length
        });

    } catch (error) {
        log('ERROR', 'Search processing error', { error: error.message });
        await updateSearchStatus(searchId, 'failed', 0);
        throw error;
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
                    source: 'bing',
                    hostPageUrl: item.hostPageUrl,
                    targetUrl: item.contentUrl,
                    timestamp: new Date().toISOString(),
                })) || []
            ) || []
        )
        : [];
}

async function performGoogleSearch(fullName, location, employer) {
    const secrets = await getSecrets();
    const searchTerms = [
        fullName,
        `${fullName} ${location}`,
        `${fullName} ${employer}`,
        `${fullName} ${location} ${employer}`
    ];
    
    log('INFO', 'Starting Google Search with terms', { searchTerms });

    const searchPromises = searchTerms.map(async (term) => {
        try {
            const response = await axios.get(secrets.GOOGLE_API_URL, {
                params: {
                    key: secrets.GOOGLE_API_KEY,
                    cx: secrets.GOOGLE_CX,
                    q: term,
                    searchType: 'image',
                    num: 10
                },
            });

            return response.data.items?.map(item => ({
                resultId: uuidv4(),
                source: 'google',
                hostPageUrl: item.image.contextLink,
                targetUrl: item.link,
                thumbnailUrl: item.image.thumbnailLink,
                imageHeight: item.image.height,
                imageWidth: item.image.width,
                searchTerm: term,
                timestamp: new Date().toISOString(),
            })) || [];
        } catch (error) {
            log('WARN', 'Google Search API Error', { 
                error: error.message,
                searchTerm: term 
            });
            return [];
        }
    });

    const results = await Promise.all(searchPromises);
    const flatResults = results.flat();
    
    const uniqueResults = Object.values(
        flatResults.reduce((acc, result) => {
            if (!acc[result.targetUrl]) {
                // First occurrence of this URL
                acc[result.targetUrl] = result;
            } else {
                // URL exists, combine search terms
                acc[result.targetUrl].searchTerm = `${acc[result.targetUrl].searchTerm}, ${result.searchTerm}`;
            }
            return acc;
        }, {})
    );
    
    log('INFO', 'Google Search completed', { 
        totalResults: flatResults.length,
        uniqueResults: uniqueResults.length,
        resultsPerTerm: results.map(r => r.length)
    });

    return uniqueResults;
}

async function processImage(result, sourceS3Key) {
    const processId = uuidv4();
    let targetKey = `temp-results/${processId}.jpg`;
    
    try {
        // Fetch image with increased timeout
        const response = await axios.get(result.targetUrl, {
            responseType: 'arraybuffer',
            timeout: 30000,
            validateStatus: status => status === 200,
            maxContentLength: 10 * 1024 * 1024,
            maxBodyLength: 10 * 1024 * 1024,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        });

        // Verify we have valid image data
        if (!response.data || response.data.length === 0) {
            throw new Error('Empty image data received');
        }

        let image;
        try {
            // Use Buffer.from for the image data
            const imageBuffer = Buffer.from(response.data);
            
            // Attempt to read the image directly
            image = await Jimp.read(imageBuffer);
            
            // If we get here, image reading was successful
            log('INFO', 'Successfully read image', { 
                processId,
                mime: image.getMIME(),
                width: image.getWidth(),
                height: image.getHeight()
            });

        } catch (jimpError) {
            log('WARN', 'Failed to process image', {
                processId,
                error: jimpError.message
            });
            return null;
        }

        // Process image with memory-efficient settings
        const processedImage = image
            .scaleToFit(800, 800)
            .quality(80);

        if (processedImage.hasAlpha()) {
            processedImage.background(0xFFFFFFFF);
        }

        // Ensure JPEG format
        const processedBuffer = await processedImage.getBufferAsync(Jimp.MIME_JPEG);

        // Upload to S3
        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: targetKey,
            Body: processedBuffer,
            ContentType: 'image/jpeg'
        }));

        // Small delay for S3 consistency
        await new Promise(resolve => setTimeout(resolve, 1000));

        log('INFO', 'Starting face comparison', { processId });

        try {
            const compareFacesResponse = await rekognition.send(new CompareFacesCommand({
                SourceImage: { S3Object: { Bucket: BUCKET_NAME, Name: sourceS3Key } },
                TargetImage: { S3Object: { Bucket: BUCKET_NAME, Name: targetKey } },
                SimilarityThreshold: 90
            }));

            log('INFO', 'Face comparison completed', { 
                processId,
                matchCount: compareFacesResponse.FaceMatches?.length || 0,
                similarity: compareFacesResponse.FaceMatches?.[0]?.Similarity
            });

            if (compareFacesResponse.FaceMatches?.length > 0) {
                const match = {
                    ...result,
                    resultId: uuidv4(),
                    similarity: compareFacesResponse.FaceMatches[0].Similarity,
                    timestamp: new Date().toISOString()
                };

                await dynamoDB.put({
                    TableName: SEARCH_RESULTS_TABLE,
                    Item: match
                });
                
                log('INFO', 'Match saved to SearchResults table', { 
                    processId,
                    resultId: match.resultId,
                    similarity: match.similarity,
                    source: result.source
                });

                return match;
            }
        } catch (rekognitionError) {
            log('ERROR', 'Rekognition error', {
                processId,
                error: rekognitionError.message,
                code: rekognitionError.code
            });
        }
    } catch (error) {
        log('WARN', 'Error processing image', { 
            processId,
            url: result.targetUrl, 
            error: error.message,
            source: result.source
        });
    } finally {
        try {
            await s3.send(new DeleteObjectCommand({
                Bucket: BUCKET_NAME,
                Key: targetKey
            }));
            log('INFO', 'Temporary file cleaned up', { processId });
        } catch (cleanupError) {
            log('WARN', 'Failed to delete temporary file', { 
                processId,
                error: cleanupError.message 
            });
        }
    }
    return null;
}
async function updateSearchStatus(searchId, status, progress, results = {}, additional = {}) {
    const updateExpression = ['SET #status = :status, progress = :progress'];
    const expressionAttributeNames = { '#status': 'status' };
    const expressionAttributeValues = {
        ':status': status,
        ':progress': progress
    };

    if (Object.keys(results).length > 0) {
        updateExpression.push('results = :results');
        expressionAttributeValues[':results'] = results;
    }

    Object.entries(additional).forEach(([key, value]) => {
        updateExpression.push(`#${key} = :${key}`);
        expressionAttributeNames[`#${key}`] = key;
        expressionAttributeValues[`:${key}`] = value;
    });

    await dynamoDB.update({
        TableName: SEARCH_METADATA_TABLE,
        Key: { searchId },
        UpdateExpression: updateExpression.join(', '),
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: expressionAttributeValues
    });
}

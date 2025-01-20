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
    const { searchId, imageId, s3Key, imageBase64 } = event;
    log('INFO', 'Processing Lambda Invoked', { searchId, imageId, s3Key });

    try {
        await processSearch(searchId, imageId, s3Key, imageBase64);
    } catch (error) {
        log('ERROR', 'Processing error', { error: error.message, searchId });
        await updateSearchStatus(searchId, 'failed', 0);
    }
};

async function processSearch(searchId, imageId, s3Key, imageBase64) {
    try {
        log('INFO', 'Starting Bing search');

        const base64Data = imageBase64.replace(/^data:image\/\w+;base64,/, "");
        const imageBuffer = Buffer.from(base64Data, "base64");
        
        // Perform visual search
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

        // Process search results
        const searchResults = searchResponse.data.tags
            ? searchResponse.data.tags.flatMap((tag) =>
                tag.actions?.flatMap((action) =>
                    action.data?.value?.map((item) => ({
                        resultId: uuidv4(),
                        imageId,
                        hostPageUrl: item.hostPageUrl,
                        targetUrl: item.contentUrl,
                        timestamp: new Date().toISOString(),
                    })) || []
                ) || []
            )
            : [];

        log('INFO', 'Search results found', { count: searchResults.length });

        // Process images with controlled concurrency
        const matches = [];
        let processed = 0;

        for (let i = 0; i < searchResults.length; i += CONCURRENT_PROCESSING) {
            const batch = searchResults.slice(i, i + CONCURRENT_PROCESSING);
            const batchPromises = batch.map(result => processImage(result, s3Key));
            
            const batchResults = await Promise.all(batchPromises);
            const validResults = batchResults.filter(result => result !== null);
            matches.push(...validResults);
            
            processed += batch.length;
            const progress = Math.round((processed / searchResults.length) * 100);
            
            // Update progress
            await updateSearchStatus(searchId, 'processing', progress, matches, {
                totalProcessed: processed,
                totalAvailable: searchResults.length
            });

            log('INFO', 'Batch processed', { 
                batchNumber: Math.floor(i/CONCURRENT_PROCESSING) + 1,
                totalBatches: Math.ceil(searchResults.length/CONCURRENT_PROCESSING),
                matchesFound: matches.length,
                totalProcessed: processed
            });
        }

        // Update final status
        await updateSearchStatus(searchId, 'completed', 100, matches, {
            totalProcessed: processed,
            totalAvailable: searchResults.length
        });

        log('INFO', 'Processing completed', { 
            searchId, 
            totalProcessed: processed, 
            totalMatches: matches.length 
        });

    } catch (error) {
        log('ERROR', 'Search processing error', { 
            error: error.message,
            stack: error.stack
        });
        await updateSearchStatus(searchId, 'failed', 0);
        throw error;
    }
}

async function processImage(result, sourceS3Key) {
    const processId = uuidv4();
    try {
        const response = await axios.get(result.targetUrl, {
            responseType: 'arraybuffer',
            timeout: 10000, // Increased timeout to 10 seconds
            validateStatus: status => status === 200,
            maxContentLength: 10 * 1024 * 1024 // 10MB max
        });

        const image = await Jimp.read(Buffer.from(response.data));
        const processedImage = image
            .scaleToFit(1920, 1080)
            .quality(85);

        if (processedImage.hasAlpha()) {
            processedImage.background(0xFFFFFFFF);
        }

        const processedBuffer = await processedImage.getBufferAsync(Jimp.MIME_JPEG);
        const targetKey = `temp-results/${processId}.jpg`;

        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: targetKey,
            Body: processedBuffer,
            ContentType: 'image/jpeg'
        }));

        log('INFO', 'Comparing faces');

        const compareFacesResponse = await rekognition.send(new CompareFacesCommand({
            SourceImage: { S3Object: { Bucket: BUCKET_NAME, Name: sourceS3Key } },
            TargetImage: { S3Object: { Bucket: BUCKET_NAME, Name: targetKey } },
            SimilarityThreshold: 98
        }));

        log('INFO', 'Face comparison completed', { 
            processId,
            matchCount: compareFacesResponse.FaceMatches?.length || 0 
        });

        await s3.send(new DeleteObjectCommand({
            Bucket: BUCKET_NAME,
            Key: targetKey
        }));

        if (compareFacesResponse.FaceMatches?.length > 0) {
            const match = {
                resultId: uuidv4(),
                imageId: result.imageId,
                hostPageUrl: result.hostPageUrl,
                targetUrl: result.targetUrl,
                similarity: compareFacesResponse.FaceMatches[0].Similarity,
                timestamp: new Date().toISOString()
            };

            // Save match to SearchResults table
            try {
                await dynamoDB.put({
                    TableName: SEARCH_RESULTS_TABLE,
                    Item: match
                });
                
                log('INFO', 'Match saved to SearchResults table', { 
                    processId,
                    resultId: match.resultId,
                    similarity: match.similarity 
                });
            } catch (dbError) {
                log('ERROR', 'Failed to save match to SearchResults', {
                    processId,
                    error: dbError.message
                });
            }

            return match;
        }
    } catch (error) {
        log('WARN', 'Error processing image', { 
            processId,
            url: result.targetUrl, 
            error: error.message 
        });
    } finally {

        try {
            const targetKey = `temp-results/${processId}.jpg`;
            await s3.send(new DeleteObjectCommand({
                Bucket: BUCKET_NAME,
                Key: targetKey,
            }));
            log('INFO', 'Temporary target image deleted from S3', { 
                processId,
                targetKey 
            });
        }catch (cleanupError) {
            log('WARN', 'Failed to delete temporary S3 file', { 
                processId,
                error: cleanupError.message 
            });
        }

    }
    return null;
}

async function updateSearchStatus(searchId, status, progress, results = [], additional = {}) {
    const updateExpression = ['SET #status = :status, progress = :progress'];
    const expressionAttributeNames = { '#status': 'status' };
    const expressionAttributeValues = {
        ':status': status,
        ':progress': progress
    };

    if (results.length > 0) {
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

const { S3, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');
const { RekognitionClient, CompareFacesCommand } = require('@aws-sdk/client-rekognition');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');
const { getSecrets, log } = require('./utils.js');
const fs = require('fs');
const FormData = require('form-data');
const Jimp = require('jimp');

const s3 = new S3();
const dynamoClient = new DynamoDB();
const dynamoDB = DynamoDBDocument.from(dynamoClient);
const rekognition = new RekognitionClient();

const BUCKET_NAME = 'infringementuploads';
const SEARCH_METADATA_TABLE = 'SearchMetadata';
const SEARCH_RESULTS_TABLE = 'SearchResults';

const MAX_SEARCH_RESULTS = 10;

const MAX_PARALLEL_PROCESSING = 5

let searchResults = [];

exports.unifiedHandler = async (event) => {
    log('INFO', 'unifiedHandler Invoked');

    try {
        const body = JSON.parse(event.body);
        const { base64Image } = body;

        if (!base64Image) {
            throw new Error('Missing required parameter: base64Image');
        }

        // Step 1: Upload Source Image to S3
        const base64Data = base64Image.replace(/^data:image\/\w+;base64,/, "");
        const imageBuffer = Buffer.from(base64Data, "base64");
        const imageId = uuidv4();
        const tempFilePath = `/tmp/${imageId}.jpg`;
        fs.writeFileSync(tempFilePath, imageBuffer);
        const s3Key = `uploads/${imageId}.jpg`;

        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: s3Key,
            Body: imageBuffer,
            ContentType: 'image/jpeg',
        }));

        log('INFO', 'Source image uploaded to S3', { imageId, s3Key });

        // Step 2: Save Metadata to DynamoDB
        const timestamp = new Date().toISOString();
        await dynamoDB.put({
            TableName: SEARCH_METADATA_TABLE,
            Item: {
                imageId,
                s3Key,
                timestamp,
                status: 'Uploaded',
            },
        });

        log('INFO', 'Metadata saved to DynamoDB', { imageId, s3Key });

        // Step 3: Perform Google Search
        // const secrets = await getSecrets();
        // const imageUrl = `https://${BUCKET_NAME}.s3.amazonaws.com/${s3Key}`;

        // log('INFO', 'Searching for image:', {imageUrl});

        // try {
        //     const googleResponse = await axios.get(secrets.GOOGLE_API_URL, {
        //         params: {
        //             key: secrets.GOOGLE_API_KEY,
        //             cx: secrets.GOOGLE_CX,
        //             q: imageUrl,
        //             searchType: 'image',
        //         },
        //     });

        //     log('DEBUG', 'Google Search API Request', {
        //         apiUrl: secrets.GOOGLE_API_URL,
        //         params: {
        //             key: secrets.GOOGLE_API_KEY,
        //             cx: secrets.GOOGLE_CX,
        //             q: imageUrl,
        //             searchType: 'image',
        //         },
        //     });            

        //     log('INFO', 'Google search completed');


        //     if (googleResponse.data.items && Array.isArray(googleResponse.data.items)) {
        //         searchResults = googleResponse.data.items.map((item) => ({
        //             resultId: uuidv4(),
        //             imageId,
        //             targetUrl: item.link,
        //             timestamp: new Date().toISOString(),
        //         }));
        //     } else {
        //         log('INFO', 'No search results returned from Google API', { imageUrl });
        //     }

        //     log('INFO', 'Search results processed', { searchResultsCount: searchResults.length });
        // } catch (error) {
        //     log('ERROR', 'Google Search API Error', { error: error.message });
        //     throw new Error('Google Search API failed');
        // }

        const secrets = await getSecrets();
        const imageUrl = `https://${BUCKET_NAME}.s3.amazonaws.com/${s3Key}`;
        const formData = new FormData();
        formData.append('image', fs.createReadStream(tempFilePath));

        log('INFO', 'Searching for image:', {imageUrl});

        log('INFO', 'Initiating Bing Visual Search');

        log('INFO', 'Bing Visual Search Request', {
            url: secrets.BING_API_URL,
            headers: {
                'Ocp-Apim-Subscription-Key': secrets.BING_API_KEY },
            body: { imageInfo: { url: imageUrl } },
        });

        try {
            const payload = {
                knowledgeRequest: {
                    imageInfo: {
                        url: imageUrl, // Ensure this is properly encoded
                    },
                },
            };


            const bingResponse = await axios.post(
                secrets.BING_API_URL,
                formData,
                {
                    headers: {
                        ...formData.getHeaders(),
                        'Ocp-Apim-Subscription-Key': secrets.BING_API_KEY, // Add API key
                    },
                }
            );

            log('INFO', 'Bing Visual Search completed',  { response: bingResponse.data });
            if (bingResponse.data.tags && bingResponse.data.tags.length > 0) {
                searchResults = bingResponse.data.tags.flatMap((tag) =>
                    tag.actions.flatMap((action) =>
                        action.data?.value?.map((item) => ({
                            resultId: uuidv4(),
                            imageId,
                            hostPageUrl: item.hostPageUrl,
                            targetUrl: item.contentUrl,
                            timestamp: new Date().toISOString(),
                        })) || []
                    )
                ).slice(0, MAX_PARALLEL_PROCESSING);
            } else {
                log('INFO', 'No search results returned from Bing Visual Search', { imageUrl });
            }

            log('INFO', 'Search results processed', { searchResultsCount: searchResults.length });
        } catch (error) {
            log('ERROR', 'Bing Visual Search API Error', { error: error.response?.data || error.message });
            throw new Error('Bing Visual Search API failed');
        } finally {
                // Clean up temporary file
                fs.unlinkSync(tempFilePath);
        }


        // Step 4: Process each search result
        const matches = [];
        // for (const result of searchResults) {
        //     try {
        //         log('INFO', 'Processing target image', { targetUrl: result.targetUrl });
                
        //         // Step 4.1: Download Target Image
        //         const response = await axios.get(result.targetUrl, { responseType: 'arraybuffer' });
        //         const imageBuffer = Buffer.from(response.data);
        
        //         // Read and process image
        //         const image = await Jimp.read(imageBuffer);
                
        //         // Process image to meet Rekognition requirements
        //         const processedImage = image
        //             .scaleToFit(1920, 1080)    // Limit maximum dimensions
        //             .quality(85);               // Set quality
                
        //         // Handle alpha channel if present
        //         if (processedImage.hasAlpha()) {
        //             processedImage.background(0xFFFFFFFF);  // White background
        //         }
                
        //         // Get buffer in JPEG format
        //         let processedBuffer = await processedImage.getBufferAsync(Jimp.MIME_JPEG);
                
        //         // Check buffer size and reduce if needed
        //         if (processedBuffer.length > 5 * 1024 * 1024) { // 5MB limit
        //             const reprocessedImage = await Jimp.read(processedBuffer);
        //             processedBuffer = await reprocessedImage
        //                 .quality(60)
        //                 .getBufferAsync(Jimp.MIME_JPEG);
        //         }
        
        //         const targetKey = `temp-results/${uuidv4()}.jpg`;
        
        //         // Upload to S3 with proper headers
        //         await s3.send(
        //             new PutObjectCommand({
        //                 Bucket: BUCKET_NAME,
        //                 Key: targetKey,
        //                 Body: processedBuffer,
        //                 ContentType: 'image/jpeg',
        //                 Metadata: {
        //                     'original-url': result.targetUrl
        //                 }
        //             })
        //         );
        //         log('INFO', 'Processed image uploaded to S3', { 
        //             targetKey, 
        //             size: processedBuffer.length 
        //         });
        
        //         // Wait for S3 consistency
        //         await new Promise(resolve => setTimeout(resolve, 2000));
         
        //         // Step 4.3: Perform Face Comparison
        //         const compareFacesResponse = await rekognition.send(new CompareFacesCommand({
        //             SourceImage: {
        //                 S3Object: {
        //                     Bucket: BUCKET_NAME,
        //                     Name: s3Key,
        //                 },
        //             },
        //             TargetImage: {
        //                 S3Object: {
        //                     Bucket: BUCKET_NAME,
        //                     Name: targetKey,
        //                 },
        //             },
        //             SimilarityThreshold: 98,
        //         }));

        //         // log('INFO', 'Comparing faces s3Key and targetKey', {s3Key, targetKey})

        //         if (compareFacesResponse.FaceMatches && compareFacesResponse.FaceMatches.length > 0) {
        //             const match = {
        //                 resultId: uuidv4(),
        //                 imageId,
        //                 targetUrl: result.targetUrl,
        //                 similarity: compareFacesResponse.FaceMatches[0].Similarity,
        //                 timestamp: new Date().toISOString(),
        //             };

        //             matches.push(match);

        //             // Save Match to DynamoDB
        //             await dynamoDB.put({
        //                 TableName: SEARCH_RESULTS_TABLE,
        //                 Item: match,
        //             });
        //         }

        //         // Step 4.4: Delete Target Image from S3
        //         await s3.send(new DeleteObjectCommand({
        //             Bucket: BUCKET_NAME,
        //             Key: targetKey,
        //         }));

        //         log('INFO', 'Temporary target image deleted from S3', { targetkey });
        //     } catch (error) {
        //         log('WARN', 'Error processing target image', { error: error.message, targetUrl: result.targetUrl });
        //     }
        // }

        // log('INFO', 'Face comparison completed', { matchCount: matches.length });

        for (const result of searchResults) {
            const processId = uuidv4();
            try {
                log('INFO', 'Processing target image', { 
                    processId,
                    targetUrl: result.targetUrl 
                });
                
                // Step 4.1: Download and validate target image
                const response = await axios.get(result.targetUrl, { 
                    responseType: 'arraybuffer',
                    validateStatus: status => status === 200, // Only accept 200 status
                    timeout: 5000 // 5 second timeout
                });
        
                // Validate content type
                const contentType = response.headers['content-type'];
                if (!contentType || !contentType.startsWith('image/')) {
                    throw new Error(`Invalid content type: ${contentType}`);
                }
        
                const imageBuffer = Buffer.from(response.data);
                
                // Read and process image with explicit error handling
                let image;
                try {
                    image = await Jimp.read(imageBuffer);
                } catch (readError) {
                    throw new Error(`Failed to read image: ${readError.message}`);
                }
                
                // Process image to meet Rekognition requirements
                const processedImage = image
                    .scaleToFit(1920, 1080)    // Limit maximum dimensions
                    .quality(85);               // Set quality
                
                // Handle alpha channel if present
                if (processedImage.hasAlpha()) {
                    processedImage.background(0xFFFFFFFF);  // White background
                    log('INFO', 'Applied white background to image with alpha channel', { processId });
                }
                
                // Get buffer in JPEG format with explicit error handling
                let processedBuffer;
                try {
                    processedBuffer = await processedImage.getBufferAsync(Jimp.MIME_JPEG);
                } catch (bufferError) {
                    throw new Error(`Failed to get JPEG buffer: ${bufferError.message}`);
                }
                
                // Validate processed image size
                if (processedBuffer.length > 5 * 1024 * 1024) {
                    log('INFO', 'Image exceeds 5MB, reducing quality', { 
                        processId,
                        originalSize: processedBuffer.length 
                    });
                    
                    const reprocessedImage = await Jimp.read(processedBuffer);
                    processedBuffer = await reprocessedImage
                        .quality(60)
                        .getBufferAsync(Jimp.MIME_JPEG);
                        
                    if (processedBuffer.length > 5 * 1024 * 1024) {
                        throw new Error('Image still exceeds 5MB after compression');
                    }
                }
        
                const targetKey = `temp-results/${processId}.jpg`;
        
                // Upload to S3 with proper headers and explicit error handling
                try {
                    await s3.send(
                        new PutObjectCommand({
                            Bucket: BUCKET_NAME,
                            Key: targetKey,
                            Body: processedBuffer,
                            ContentType: 'image/jpeg',
                            Metadata: {
                                'original-url': result.targetUrl,
                                'process-id': processId
                            }
                        })
                    );
                    
                    log('INFO', 'Processed image uploaded to S3', { 
                        processId,
                        targetKey, 
                        size: processedBuffer.length 
                    });
                } catch (s3Error) {
                    throw new Error(`Failed to upload to S3: ${s3Error.message}`);
                }
        
                // Wait for S3 consistency
                await new Promise(resolve => setTimeout(resolve, 2000));

                log('INFO', 'Comparing faces in:', { s3Key, targetKey});
                
                // Step 4.3: Perform Face Comparison with explicit error handling
                try {
                    const compareFacesResponse = await rekognition.send(new CompareFacesCommand({
                        SourceImage: {
                            S3Object: {
                                Bucket: BUCKET_NAME,
                                Name: s3Key,
                            },
                        },
                        TargetImage: {
                            S3Object: {
                                Bucket: BUCKET_NAME,
                                Name: targetKey,
                            },
                        },
                        SimilarityThreshold: 98,
                    }));
        
                    log('INFO', 'Face comparison completed successfully', { 
                        processId,
                        matchCount: compareFacesResponse.FaceMatches?.length || 0 
                    });
        
                    if (compareFacesResponse.FaceMatches && compareFacesResponse.FaceMatches.length > 0) {
                        const match = {
                            resultId: uuidv4(),
                            imageId,
                            hostPageUrl: result.hostPageUrl,
                            targetUrl: result.targetUrl,
                            similarity: compareFacesResponse.FaceMatches[0].Similarity,
                            timestamp: new Date().toISOString(),
                        };
        
                        matches.push(match);
        
                        // Save Match to DynamoDB with explicit error handling
                        try {
                            await dynamoDB.put({
                                TableName: SEARCH_RESULTS_TABLE,
                                Item: match,
                            });
                            
                            log('INFO', 'Match saved to DynamoDB', { 
                                processId,
                                resultId: match.resultId,
                                similarity: match.similarity 
                            });
                        } catch (dbError) {
                            log('ERROR', 'Failed to save match to DynamoDB', {
                                processId,
                                error: dbError.message
                            });
                            // Continue processing even if DB save fails
                        }
                    }
                } catch (rekognitionError) {
                    throw new Error(`Rekognition error: ${rekognitionError.message}`);
                }
        
            } catch (error) {
                log('WARN', 'Error processing target image', { 
                    processId,
                    error: error.message, 
                    targetUrl: result.targetUrl,
                    stage: error.stage || 'unknown'
                });
            } finally {
                // Always attempt to clean up temporary S3 file
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
                } catch (cleanupError) {
                    log('WARN', 'Failed to delete temporary S3 file', { 
                        processId,
                        error: cleanupError.message 
                    });
                }
            }
        }

        // Step 5: Return Matched Results
        return {
            statusCode: 200,
            body: JSON.stringify({ 
                results: matches.map(match => ({
                    title: `Match (${Math.round(match.similarity)}% similar)`,
                    url: match.hostPageUrl,
                    thumbnailUrl: match.targetUrl
                }))
            }),
        };
    } catch (error) {
        log('ERROR', 'Error in unifiedHandler', { error: error.message });
        return {
            statusCode: 500,
            body: JSON.stringify({ message: error.message }),
        };
    }
};
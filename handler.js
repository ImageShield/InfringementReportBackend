const { S3, PutObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDB } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocument } = require('@aws-sdk/lib-dynamodb');
const { Lambda } = require('@aws-sdk/client-lambda');
const { v4: uuidv4 } = require('uuid');
const { log } = require('./utils');
const fs = require('fs');

// Initialize AWS clients
const s3 = new S3();
const dynamoClient = new DynamoDB();
const dynamoDB = DynamoDBDocument.from(dynamoClient);
const lambda = new Lambda();

// Constants
const BUCKET_NAME = 'infringementuploads';
const SEARCH_METADATA_TABLE = 'SearchMetadata';
const PROCESSOR_LAMBDA = 'PROCESSOR_LAMBDA';

exports.handler = async (event) => {
    log('INFO', 'API Handler Invoked', { path: event.routeKey });

    if (event.routeKey === 'POST /upload') {
        return await handleUpload(event);
    } else if (event.routeKey === 'GET /status/{original_id}') {
        return await handleGetStatus(event);
    }

    return createResponse(404, { message: 'Not Found' });
};

async function handleUpload(event) {
    try {
        const images = JSON.parse(event.body);
        
        if (!Array.isArray(images)) {
            return createResponse(400, { message: 'Payload must be an array' });
        }

        // Validate each image entry
        for (const image of images) {
            if (!image.original_id && image.original_id !== 0) {
                return createResponse(400, { message: 'Missing original_id' });
            }
            if (!image.url) {
                return createResponse(400, { message: 'Missing url' });
            }

            await dynamoDB.put({
                TableName: 'ImageSearchResults',
                Item: {
                    original_id: image.original_id,
                    status: 'processing',
                    progress: 0,
                    matches: [],
                    timestamp: new Date().toISOString()
                }
            });
        }

        // Process each image asynchronously
        const processingPromises = images.map(image => 
            lambda.invoke({
                FunctionName: PROCESSOR_LAMBDA,
                InvocationType: 'Event',
                Payload: JSON.stringify({
                    original_id: image.original_id,
                    imageUrl: image.url,
                    priority: image.priority || 'high'
                })
            })
        );

        await Promise.all(processingPromises);

        return createResponse(202, {
            message: 'Processing started',
            total_images: images.length
        });

    } catch (error) {
        log('ERROR', 'Upload handler error', { error: error.message });
        return createResponse(500, { message: error.message });
    }
}

async function handleGetStatus(event) {
    try {
        const originalId = event.pathParameters?.original_id;
        
        if (!originalId) {
            return createResponse(400, { message: 'Original ID is required' });
        }
        const idForQuery = Number(originalId) || originalId;

        try {
            const { Item: result } = await dynamoDB.get({
                TableName: 'ImageSearchResults',
                Key: { original_id: idForQuery }
            });

            if (!result) {
                return createResponse(404, { 
                    id: originalId,
                    img_data: []
                });
            }

            if (result.status === 'processing') {
                return createResponse(200, {
                    id: originalId,
                    status: 'processing',
                    progress: result.progress || 0
                });
            } else if (result.status === 'failed') {
                return createResponse(200, {
                    id: originalId,
                    img_data: []
                });
            }

            // Format matches array if exists
            const matches = Array.isArray(result.matches) ? result.matches : [];
            
            return createResponse(200, {
                id: originalId,
                img_data: matches.map(match => ({
                    url: match.url,
                    malicious: match.malicious || false,
                    false_positive_eligible: match.false_positive_eligible || false
                }))
            });

        } catch (dbError) {
            log('ERROR', 'DynamoDB error', { 
                error: dbError.message,
                originalId 
            });
            
            return createResponse(500, {
                message: 'Database error occurred',
                id: originalId
            });
        }

    } catch (error) {
        log('ERROR', 'Status handler error', { error: error.message });
        return createResponse(500, {
            message: 'Internal server error',
            id: originalId
        });
    }
}

function createResponse(statusCode, body) {
    return {
        statusCode,
        headers: {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        body: JSON.stringify(body)
    };
}
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

    if (event.routeKey === 'POST /initiateSearch') {
        return await handleInitiateSearch(event);
    } else if (event.routeKey === 'GET /status/{searchId}') {
        return await handleGetStatus(event);
    }

    return createResponse(404, { message: 'Not Found' });
};

async function handleInitiateSearch(event) {
    try {
        const body = JSON.parse(event.body);
        const { base64Image, fullName, location, employer } = body;

        const requiredFields = { base64Image, fullName, location, employer };
        for (const [field, value] of Object.entries(requiredFields)) {
            if (!value) {
                return createResponse(400, { message: `Missing required parameter: ${field}` });
            }
        }

        const searchId = uuidv4();
        const imageId = uuidv4();
        const s3Key = `uploads/${imageId}.jpg`;

        // Upload image to S3
        const base64Data = base64Image.replace(/^data:image\/\w+;base64,/, "");
        const imageBuffer = Buffer.from(base64Data, "base64");


        await s3.send(new PutObjectCommand({
            Bucket: BUCKET_NAME,
            Key: s3Key,
            Body: imageBuffer,
            ContentType: 'image/jpeg',
        }));

        log('INFO', 'Image uploaded to S3', { searchId, imageId, s3Key });

        // Create initial metadata
        await dynamoDB.put({
            TableName: SEARCH_METADATA_TABLE,
            Item: {
                searchId,
                imageId,
                s3Key,
                fullName,
                location,
                employer,
                status: 'processing',
                timestamp: new Date().toISOString(),
                progress: 0,
                results: []
            }
        });

        log('INFO', 'Initial metadata saved', { searchId });

        // Invoke processing Lambda asynchronously
        await lambda.invoke({
            FunctionName: PROCESSOR_LAMBDA,
            InvocationType: 'Event',
            Payload: JSON.stringify({
                searchId,
                imageId,
                s3Key,
                imageBase64: base64Image,
                fullName,
                location,
                employer
            })
        });

        log('INFO', 'Processing Lambda invoked', { searchId });

        return {
            statusCode: 202,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify({
                searchId,
                status: 'processing',
                message: 'Processing started'
            })
        };

    } catch (error) {
        log('ERROR', 'Search initiation error', { error: error.message });
        return createResponse(500, { message: error.message });
    }
}

// async function handleGetStatus(event) {
//     try {
//         const searchId = event.pathParameters?.searchId;
        
//         if (!searchId) {
//             return createResponse(400, { message: 'Search ID is required' });
//         }

//         const { Item: search } = await dynamoDB.get({
//             TableName: SEARCH_METADATA_TABLE,
//             Key: { searchId }
//         });

//         if (!search) {
//             return createResponse(404, { message: 'Search not found' });
//         }

//         return createResponse(200, {
//             status: search.status,
//             progress: search.progress,
//             results: {
//                 bing: search.results.bing?.map(match => ({
//                     title: `Match (${Math.round(match.similarity)}% similar)`,
//                     url: match.hostPageUrl,
//                     thumbnailUrl: match.targetUrl,
//                     source: 'bing'
//                 })) || [],
//                 google: search.results.google?.map(match => ({
//                     title: `Match (${Math.round(match.similarity)}% similar)`,
//                     url: match.hostPageUrl,
//                     thumbnailUrl: match.thumbnailUrl || match.targetUrl,
//                     source: 'google'
//                 })) || []
//             },
//             totalProcessed: search.totalProcessed,
//             totalAvailable: search.totalAvailable
//         });
//     } catch (error) {
//         log('ERROR', 'Error fetching status', { error: error.message });
//         return createResponse(500, { message: error.message });
//     }
// }


async function handleGetStatus(event) {
    try {
        const searchId = event.pathParameters?.searchId;
        
        if (!searchId) {
            return createResponse(400, { message: 'Search ID is required' });
        }

        const { Item: search } = await dynamoDB.get({
            TableName: SEARCH_METADATA_TABLE,
            Key: { searchId }
        });

        if (!search) {
            return createResponse(404, { message: 'Search not found' });
        }

        // When search is completed, format and return the results
        const formattedResults = {
            bing: search.results.bing?.map(match => ({
                title: `Match (${Math.round(match.similarity)}% similar)`,
                url: match.hostPageUrl,
                thumbnailUrl: match.targetUrl,
                source: 'bing'
            })) || [],
            google: search.results.google?.map(match => ({
                title: `Match (${Math.round(match.similarity)}% similar)`,
                url: match.hostPageUrl,
                thumbnailUrl: match.thumbnailUrl || match.targetUrl,
                source: 'google',
                searchTerm: match.searchTerm
            })) || []
        };

        return createResponse(200, {
            status: search.status,
            progress: search.progress,
            results: formattedResults,
            totalProcessed: search.totalProcessed,
            totalAvailable: search.totalAvailable
        });
    } catch (error) {
        log('ERROR', 'Error fetching status', { error: error.message });
        return createResponse(500, { message: error.message });
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
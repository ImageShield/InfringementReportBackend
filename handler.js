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
const PROCESSOR_LAMBDA = process.env.Processor_Lambda

exports.handler = async (event) => {
    log('INFO', 'API Handler Invoked', { path: event.routeKey });


        // === 1. Authorization Check ===
    const authHeader = event.headers?.Authorization || event.headers?.authorization;
    const EXPECTED_TOKEN = process.env.AUTH_TOKEN || 'YourExpectedAuthToken';
    if (!authHeader || authHeader !== EXPECTED_TOKEN) {
        return createResponse(401, { message: 'Unauthorized' });
    }
    
    // === 2. Only Accept POST /upload ===
    if (event.routeKey === 'POST /upload') {
        return await handleUpload(event);
    }
    
    return createResponse(404, { message: 'Not Found' });
    };

    async function handleUpload(event) {
        try {
          const images = JSON.parse(event.body);
          
          if (!Array.isArray(images)) {
            return createResponse(400, { message: 'Payload must be an array' });
          }
          
          // Here you could also add additional validation per image (e.g. ensure original_id and url exist)
          for (const image of images) {
            if (typeof image.original_id !== 'number' || !image.url) {
              return createResponse(400, { message: 'Each image must have an original_id (number) and a url' });
            }
          }
          
          // === 3. Dispatch Processing Asynchronously ===
          const processingPromises = images.map((image) =>
            lambda.invoke({
              FunctionName: PROCESSOR_LAMBDA,
              InvocationType: 'Event', // asynchronous invocation
              Payload: JSON.stringify({
                original_id: image.original_id,
                imageUrl: image.url,
                // if priority isn’t provided, default to "high"
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
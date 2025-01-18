const { SecretsManager, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const { v4: uuidv4 } = require('uuid');
const axios = require('axios');

const secretsManager = new SecretsManager();

let cachedSecrets = null;

const getSecrets = async () => {
    if (cachedSecrets) {
        return cachedSecrets;
    }

    const secretName = 'Infringement-secrets';

    try {
        const response = await secretsManager.send(new GetSecretValueCommand({
            SecretId: secretName,
        }));

        if ('SecretString' in response) {
            cachedSecrets = JSON.parse(response.SecretString);
            return cachedSecrets;
        }
        throw new Error('SecretString not found in Secrets Manager response');
    } catch (error) {
        console.error('Failed to fetch secrets from Secrets Manager:', error);
        throw error;
    }
};

const log = (level, message, context = {}) => {
    console.log(JSON.stringify({
        timestamp: new Date().toISOString(),
        level,
        message,
        ...context,
    }));
};

// Export only what's needed
module.exports = {
    getSecrets,
    log,
};
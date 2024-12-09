import { Env } from './types';
import {
	SecretsManagerClient,
	GetSecretValueCommand,
	GetSecretValueCommandOutput,
} from '@aws-sdk/client-secrets-manager';


export class SecretsManagerService {
	private client: SecretsManagerClient;

	constructor(env: Env, region: string = 'us-east-1') {
		// Initialize the AWS Secrets Manager client
		this.client = new SecretsManagerClient({
			region,
			credentials: {
				accessKeyId: env.AWS_ACCESS_KEY_ID,
				secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
			},
		});
	}

	// Method to fetch the secret value
	public async getSecret(secretName: string): Promise<string | null> {
		try {
			const command = new GetSecretValueCommand({ SecretId: secretName });
			const secretData: GetSecretValueCommandOutput = await this.client.send(command);

			// Return secret string if available
			if ('SecretString' in secretData) {
				return secretData.SecretString || null;
			} else {
				return null;
			}
		} catch (err) {
			console.error(`Error retrieving secret: ${(err as Error).message}`);
			return null;
		}
	}
}


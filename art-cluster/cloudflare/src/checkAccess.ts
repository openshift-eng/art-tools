import { Env } from './types';
import { SecretsManagerService } from './secretsManager';
import { createHash, timingSafeEqual } from 'crypto';

let ENTERPRISE_SERVICE_ACCOUNTS: { [key: string]: string } | null = null;
let POCKETS_SERVICE_ACCOUNTS: { [key: string]: string } | null = null;

export class AccessChecker {
    private env: Env;

    constructor(env: Env) {
        this.env = env;
    }

    public static async create(env: Env): Promise<AccessChecker> {
        const accessChecker = new AccessChecker(env);
        accessChecker.load();
        return accessChecker;
    }

    // Static async method to initialize the class with async operations
    private async load() {
        if (!ENTERPRISE_SERVICE_ACCOUNTS) {
            if (this.env.ENTERPRISE_SERVICE_ACCOUNTS) {
                try {
                    ENTERPRISE_SERVICE_ACCOUNTS = JSON.parse(this.env.ENTERPRISE_SERVICE_ACCOUNTS)
                } catch (e) {
                    console.error('Failed to parse ENTERPRISE_SERVICE_ACCOUNTS:', e);
                }
                console.info('loaded ENTERPRISE_SERVICE_ACCOUNTS from evironment variables')
            } else {
                const secretsManager = new SecretsManagerService(this.env);
                const secretName = 'art_srv_request_basic_auth/ENTERPRISE_SERVICE_ACCOUNTS';

                const secret = await secretsManager.getSecret(secretName);
                if (secret) {
                    try {
                        // Parse the secret string as a JSON object
                        ENTERPRISE_SERVICE_ACCOUNTS = JSON.parse(secret);
                    } catch (e) {
                        console.error('Failed to parse ENTERPRISE_SERVICE_ACCOUNTS:', e);
                        throw new Error('Secret parsing failed');
                    }
                }
            }
        }

        if (!POCKETS_SERVICE_ACCOUNTS) {
            if (this.env.POCKETS_SERVICE_ACCOUNTS) {
                try {
                    POCKETS_SERVICE_ACCOUNTS = JSON.parse(this.env.POCKETS_SERVICE_ACCOUNTS)
                } catch (e) {
                    console.error('Failed to parse POCKETS_SERVICE_ACCOUNTS:', e);
                }
                console.info('loaded POCKETS_SERVICE_ACCOUNTS from evironment variables')
            } else {
                const secretsManager = new SecretsManagerService(this.env);
                const secretName = 'art_srv_request_basic_auth/POCKETS_SERVICE_ACCOUNTS';

                const secret = await secretsManager.getSecret(secretName);
                if (secret) {
                    try {
                        // Parse the secret string as a JSON object
                        POCKETS_SERVICE_ACCOUNTS = JSON.parse(secret);
                    } catch (e) {
                        console.error('Failed to parse POCKETS_SERVICE_ACCOUNTS:', e);
                        throw new Error('Secret parsing failed');
                    }
                }
            }
        }
    }

    // Check enterprise access
    public async checkEnterpriseAccess(username: string, password: string): Promise<boolean> {
        // Check if the username exists in the parsed object
        if (ENTERPRISE_SERVICE_ACCOUNTS && username in ENTERPRISE_SERVICE_ACCOUNTS) {
            const storedPassword = ENTERPRISE_SERVICE_ACCOUNTS[username];
            const passwordHash = createHash('sha256').update(password).digest('hex');

            const passwordBuffer = Buffer.from(passwordHash);
            const storedPasswordBuffer = Buffer.from(storedPassword);

            // Use crypto's timing-safe comparison
            if (timingSafeEqual(passwordBuffer, storedPasswordBuffer)) {
                return true; // Authorized
            }
        }

        return false; // Not authorized
    }

    // Check pocket access
    public async checkPocketAccess(username: string, password: string): Promise<boolean> {
        // Check if the username exists in the parsed object
        if (POCKETS_SERVICE_ACCOUNTS && username in POCKETS_SERVICE_ACCOUNTS) {
            const storedPassword = POCKETS_SERVICE_ACCOUNTS[username];
            const passwordHash = createHash('sha256').update(password).digest('hex');

            const passwordBuffer = Buffer.from(passwordHash);
            const storedPasswordBuffer = Buffer.from(storedPassword);

            // Use crypto's timing-safe comparison
            if (timingSafeEqual(passwordBuffer, storedPasswordBuffer)) {
                return true; // Authorized
            }
        }

        return false; // Not authorized
    }
}


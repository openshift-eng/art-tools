export interface Env {
    AWS_ACCESS_KEY_ID: string;
    AWS_SECRET_ACCESS_KEY: string;
    BUCKET_bucketname: string;
    ENTERPRISE_SERVICE_ACCOUNTS: string;
    POCKETS_SERVICE_ACCOUNTS: string;
};

export interface SiteConfig {
    name: string;
    bucket: R2Bucket;
    public: string[];
    cgw: {[location: string]: string};
    replace: {[prefix: string]: string;};
    limit: number;
    desp: {[path: string]: string;};
    decodeURI?: boolean;
    legalInfo?: string;
    showPoweredBy?: boolean;
    favicon?: string;
    dangerousOverwriteZeroByteObject?: boolean;
}

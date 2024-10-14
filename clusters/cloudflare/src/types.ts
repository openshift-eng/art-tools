export type Env = {
    [K in `BUCKET_${string}`]: R2Bucket;
};

export interface SiteConfig {
    name: string;
    bucket: R2Bucket;
    public: string[];
    replace: {[prefix: string]: string;};
    desp: {[path: string]: string;};
    decodeURI?: boolean;
    legalInfo?: string;
    showPoweredBy?: boolean;
    favicon?: string;
    dangerousOverwriteZeroByteObject?: boolean;
}

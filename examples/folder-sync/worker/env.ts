import type { DurableObjectNamespace, Fetcher, R2Bucket } from "@cloudflare/workers-types";

export interface Env {
  readonly FOLDERS: DurableObjectNamespace;
  readonly INDEX: DurableObjectNamespace;
  readonly BLOBS: R2Bucket;
  readonly ASSETS: Fetcher;
  readonly WRITER_TOKEN: string;
}

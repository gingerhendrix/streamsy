import type { DurableObjectNamespace, Fetcher } from "@cloudflare/workers-types";

export interface Env {
  readonly DOCUMENTS: DurableObjectNamespace;
  readonly ASSETS: Fetcher;
  readonly COMPACTION_THRESHOLD: string;
}

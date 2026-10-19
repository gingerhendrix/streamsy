/** Private pure-policy input, promoted to protocol options in Batch 3. */
export interface CreateOptions {
  readonly contentType?: string;
  readonly ttlSeconds?: number;
  readonly expiresAt?: string;
  readonly closed?: boolean;
  readonly forkedFrom?: string;
  readonly forkOffset?: string;
  readonly forkSubOffset?: number;
  readonly initialData?: Uint8Array;
}

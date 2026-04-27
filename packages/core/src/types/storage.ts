/**
 * Storage Layer Types
 *
 * Types for the persistence layer that handles offset generation,
 * message storage, and TTL management
 */
export interface StoredMessage {
  data: Uint8Array;
  offset: string;
  timestamp: number;
}

export interface StreamMetadata {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  createdAt: number;
  lastSeq?: string;

  // Fork relationship — set when this stream was created as a fork.
  forkedFrom?: string;
  forkOffset?: string;

  // Reference count: number of forks pointing at this stream as their source.
  refCount?: number;

  // Soft-delete: stream is logically deleted but data is retained for fork
  // readers. Direct client operations against this stream return 410 Gone.
  softDeleted?: boolean;
}

export interface CreateStreamOptions {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  initialData?: Uint8Array[];

  // Fork creation — when set, the stream is created as a fork of `forkedFrom`
  // at `forkOffset`. The storage initializes its counter/currentOffset so that
  // future appends produce offsets greater than `forkOffset`.
  forkedFrom?: string;
  forkOffset?: string;
}

export interface StorageReadResult {
  messages: StoredMessage[];
  nextOffset: string;
  upToDate: boolean;
}

export interface StorageReadLiveResult {
  messages: StoredMessage[];
  nextOffset: string;
  timedOut: boolean;
}

/**
 * Storage Layer Interface
 *
 */
export interface StreamStorage {
  // Lifecycle
  createStream(options: CreateStreamOptions): Promise<string>;
  deleteAll(): Promise<void>;

  // Metadata
  getMetadata(): Promise<StreamMetadata | null>;
  getCurrentOffset(): Promise<string>;

  // Messages — operates only on this stream's own data (does not walk fork chains)
  append(messages: Uint8Array[], seq?: string): Promise<string>;
  read(afterOffset?: string): Promise<StorageReadResult>;

  // Live reads (waits for new messages)
  readLive(
    afterOffset: string,
    signal?: AbortSignal,
  ): Promise<StorageReadLiveResult>;

  // Fork lifecycle primitives
  setRefCount(value: number): Promise<void>;
  setSoftDeleted(value: boolean): Promise<void>;
}

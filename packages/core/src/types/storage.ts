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
  closed?: boolean;
  closedAt?: number;
}

export interface CreateStreamOptions {
  contentType: string;
  ttlSeconds?: number;
  expiresAt?: string;
  initialData?: Uint8Array[];
  closed?: boolean;
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

  // Messages
  append(messages: Uint8Array[], seq?: string): Promise<string>;
  read(afterOffset?: string): Promise<StorageReadResult>;

  // Closure
  close(messages?: Uint8Array[], seq?: string): Promise<string>;

  // Live reads (waits for new messages)
  readLive(
    afterOffset: string,
    signal?: AbortSignal,
  ): Promise<StorageReadLiveResult>;
}

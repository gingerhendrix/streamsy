/**
 * In-Memory Storage Implementation
 *
 * Implements the StreamStorage interface using in-memory data structures.
 * Suitable for development, testing, and ephemeral use cases.
 *
 * Ports the waiter pattern from DurableObjectStreamStorage for live reads.
 */

import type {
  StreamStorage,
  StreamMetadata,
  CreateStreamOptions,
  StorageReadResult,
  StorageReadLiveResult,
  StoredMessage,
} from "@streamsy/core";

const ZERO_OFFSET = `${"0".repeat(16)}_${"0".repeat(16)}`;

export interface MemoryStreamStorageOptions {
  // Called when this stream's TTL fires and the stream is fully purged
  // (refCount == 0). The registry uses this to drop its map entry and
  // cascade GC up the fork chain.
  onPurge?: () => void | Promise<void>;
}

export class MemoryStreamStorage implements StreamStorage {
  private metadata: StreamMetadata | null = null;
  private messages: StoredMessage[] = [];
  private counter: number = 0;
  private currentOffset: string = ZERO_OFFSET;
  private waiters: Set<() => void> = new Set();
  private ttlTimer: ReturnType<typeof setTimeout> | null = null;
  private onPurge?: () => void | Promise<void>;

  constructor(options: MemoryStreamStorageOptions = {}) {
    this.onPurge = options.onPurge;
  }

  async createStream(options: CreateStreamOptions): Promise<string> {
    const metadata: StreamMetadata = {
      contentType: options.contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
      forkedFrom: options.forkedFrom,
      forkOffset: options.forkOffset,
      refCount: 0,
    };

    this.metadata = metadata;
    this.messages = [];

    if (options.forkedFrom && options.forkOffset) {
      // Initialize counter from the fork offset so that subsequent appends
      // produce strictly greater offsets and lex-ordering is preserved.
      this.counter = parseCounter(options.forkOffset);
      this.currentOffset = options.forkOffset;
    } else {
      this.counter = 0;
      this.currentOffset = ZERO_OFFSET;
    }

    this.scheduleTtl(options.ttlSeconds, options.expiresAt);

    if (options.initialData?.length) {
      return await this.append(options.initialData);
    }

    return this.currentOffset;
  }

  async deleteAll(): Promise<void> {
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }

    for (const waiter of this.waiters) {
      waiter();
    }
    this.waiters.clear();

    this.metadata = null;
    this.messages = [];
    this.counter = 0;
    this.currentOffset = ZERO_OFFSET;
  }

  async getMetadata(): Promise<StreamMetadata | null> {
    return this.metadata;
  }

  async getCurrentOffset(): Promise<string> {
    return this.currentOffset;
  }

  async append(messages: Uint8Array[], seq?: string): Promise<string> {
    let lastOffset = this.currentOffset;

    for (const data of messages) {
      this.counter++;
      const offset = formatOffset(this.counter);
      lastOffset = offset;

      this.messages.push({
        data,
        offset,
        timestamp: Date.now(),
      });
    }

    this.currentOffset = lastOffset;

    if (seq && this.metadata) {
      this.metadata = { ...this.metadata, lastSeq: seq };
    }

    this.notifyWaiters();

    return lastOffset;
  }

  async read(afterOffset?: string): Promise<StorageReadResult> {
    let messages: StoredMessage[];

    if (afterOffset) {
      messages = this.messages.filter((msg) => msg.offset > afterOffset);
    } else {
      messages = [...this.messages];
    }

    const nextOffset =
      messages.length > 0
        ? messages[messages.length - 1]!.offset
        : this.currentOffset;

    return {
      messages,
      nextOffset,
      upToDate: nextOffset === this.currentOffset,
    };
  }

  async readLive(
    afterOffset: string,
    signal?: AbortSignal,
  ): Promise<StorageReadLiveResult> {
    const result = await this.read(afterOffset);
    if (result.messages.length > 0) {
      return { ...result, timedOut: false };
    }

    const timeout = 30_000;

    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        this.waiters.delete(notify);
        resolve({
          messages: [],
          nextOffset: result.nextOffset,
          timedOut: true,
        });
      }, timeout);

      const notify = async () => {
        clearTimeout(timeoutId);
        this.waiters.delete(notify);
        const r = await this.read(afterOffset);
        resolve({ ...r, timedOut: false });
      };

      this.waiters.add(notify);

      signal?.addEventListener("abort", () => {
        clearTimeout(timeoutId);
        this.waiters.delete(notify);
        resolve({
          messages: [],
          nextOffset: result.nextOffset,
          timedOut: true,
        });
      });
    });
  }

  async setRefCount(value: number): Promise<void> {
    if (!this.metadata) return;
    this.metadata = { ...this.metadata, refCount: value };
  }

  async setSoftDeleted(value: boolean): Promise<void> {
    if (!this.metadata) return;
    this.metadata = { ...this.metadata, softDeleted: value };
    if (value) {
      // Wake live waiters; they should observe the soft-delete and exit.
      this.notifyWaiters();
    }
  }

  private scheduleTtl(ttlSeconds?: number, expiresAt?: string): void {
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }

    let delay: number | null = null;
    if (ttlSeconds !== undefined) {
      delay = ttlSeconds * 1000;
    } else if (expiresAt) {
      delay = new Date(expiresAt).getTime() - Date.now();
    }

    if (delay !== null && delay >= 0) {
      this.ttlTimer = setTimeout(() => {
        void this.handleTtlFired();
      }, delay);
    }
  }

  private async handleTtlFired(): Promise<void> {
    this.ttlTimer = null;
    if (!this.metadata) return;

    const refCount = this.metadata.refCount ?? 0;
    if (refCount > 0) {
      // Forks still reference this stream — soft-delete only.
      await this.setSoftDeleted(true);
      return;
    }

    // No active forks — fully purge.
    await this.deleteAll();
    await this.onPurge?.();
  }

  private notifyWaiters(): void {
    for (const waiter of this.waiters) {
      waiter();
    }
  }
}

function formatOffset(counter: number): string {
  const counterStr = String(counter).padStart(16, "0");
  const byteOffset = "0".repeat(16);
  return `${counterStr}_${byteOffset}`;
}

function parseCounter(offset: string): number {
  const [counterStr] = offset.split("_");
  return parseInt(counterStr ?? "0", 10);
}

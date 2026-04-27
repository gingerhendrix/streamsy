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

export class MemoryStreamStorage implements StreamStorage {
  private metadata: StreamMetadata | null = null;
  private messages: StoredMessage[] = [];
  private counter: number = 0;
  private currentOffset: string = MemoryStreamStorage.formatOffset(0);
  private waiters: Set<() => void> = new Set();
  private ttlTimer: ReturnType<typeof setTimeout> | null = null;

  async createStream(options: CreateStreamOptions): Promise<string> {
    const metadata: StreamMetadata = {
      contentType: options.contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
      ...(options.closed ? { closed: true, closedAt: Date.now() } : {}),
    };

    this.metadata = metadata;
    this.messages = [];
    this.counter = 0;
    this.currentOffset = MemoryStreamStorage.formatOffset(0);

    // Set TTL timer if configured
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }

    if (options.ttlSeconds) {
      this.ttlTimer = setTimeout(() => {
        void this.deleteAll();
      }, options.ttlSeconds * 1000);
    } else if (options.expiresAt) {
      const delay = new Date(options.expiresAt).getTime() - Date.now();
      if (delay > 0) {
        this.ttlTimer = setTimeout(() => {
          void this.deleteAll();
        }, delay);
      }
    }

    // Handle initial data
    if (options.initialData?.length) {
      return await this.append(options.initialData);
    }

    return this.currentOffset;
  }

  async deleteAll(): Promise<void> {
    // Clear TTL timer
    if (this.ttlTimer) {
      clearTimeout(this.ttlTimer);
      this.ttlTimer = null;
    }

    // Notify and clear all waiters
    for (const waiter of this.waiters) {
      waiter();
    }
    this.waiters.clear();

    // Reset state
    this.metadata = null;
    this.messages = [];
    this.counter = 0;
    this.currentOffset = MemoryStreamStorage.formatOffset(0);
  }

  async getMetadata(): Promise<StreamMetadata | null> {
    return this.metadata;
  }

  async getCurrentOffset(): Promise<string> {
    return this.currentOffset;
  }

  async append(messages: Uint8Array[], seq?: string): Promise<string> {
    let lastOffset = "";

    for (const data of messages) {
      this.counter++;
      const offset = MemoryStreamStorage.formatOffset(this.counter);
      lastOffset = offset;

      this.messages.push({
        data,
        offset,
        timestamp: Date.now(),
      });
    }

    if (lastOffset !== "") {
      this.currentOffset = lastOffset;
    }

    if (seq && this.metadata) {
      this.metadata = { ...this.metadata, lastSeq: seq };
    }

    // Notify waiters (ported from DO storage)
    this.notifyWaiters();

    return this.currentOffset;
  }

  async close(messages?: Uint8Array[], seq?: string): Promise<string> {
    if (messages && messages.length > 0) {
      await this.append(messages, seq);
    } else if (seq && this.metadata) {
      this.metadata = { ...this.metadata, lastSeq: seq };
    }

    if (this.metadata) {
      this.metadata = { ...this.metadata, closed: true, closedAt: Date.now() };
    }

    // Wake any long-poll waiters so they observe the closed state.
    this.notifyWaiters();

    return this.currentOffset;
  }

  async read(afterOffset?: string): Promise<StorageReadResult> {
    let messages: StoredMessage[];

    if (afterOffset) {
      // Find messages after the given offset using string comparison
      // (offsets are zero-padded so lexicographic order = numeric order)
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
    // Check for existing messages first
    const result = await this.read(afterOffset);
    if (result.messages.length > 0) {
      return { ...result, timedOut: false };
    }

    // Wait for new messages or timeout (waiter pattern from DO storage)
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

  private notifyWaiters(): void {
    for (const waiter of this.waiters) {
      waiter();
    }
  }

  private static formatOffset(counter: number): string {
    const counterStr = String(counter).padStart(16, "0");
    const byteOffset = "0".repeat(16);
    return `${counterStr}_${byteOffset}`;
  }
}

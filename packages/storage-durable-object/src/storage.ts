/**
 * Storage Layer Implementation - Cloudflare Durable Object
 *
 * SQLite-backed Durable Object storage.
 * Extends DurableObject for RPC access from the protocol layer.
 * Uses the synchronous KV storage API for performance.
 */

import { DurableObject } from "cloudflare:workers";
import type {
  StreamStorage as StreamStorageInterface,
  StreamMetadata,
  CreateStreamOptions,
  StorageReadResult,
  StorageReadLiveResult,
  StoredMessage,
} from "@streamsy/core";

export class DurableObjectStreamStorage
  extends DurableObject
  implements StreamStorageInterface
{
  private waiters: Set<() => void> = new Set();
  private metadata: StreamMetadata | null = null;
  private counter: number = 0;
  private currentOffset: string;

  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env);
    this.metadata = ctx.storage.kv.get("metadata") ?? null;
    this.counter = ctx.storage.kv.get("counter") ?? 0;
    this.currentOffset =
      ctx.storage.kv.get("currentOffset") ?? this.formatOffset(0);
  }

  /**
   * TTL alarm handler - deletes expired streams
   */
  override async alarm(): Promise<void> {
    await this.deleteAll();
  }

  async createStream(options: CreateStreamOptions): Promise<string> {
    const metadata: StreamMetadata = {
      contentType: options.contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
    };

    this.ctx.storage.kv.put("metadata", metadata);
    this.ctx.storage.kv.put("counter", 0);
    const initialOffset = this.formatOffset(0);
    this.ctx.storage.kv.put("currentOffset", initialOffset);

    // Update memoized state
    this.metadata = metadata;
    this.counter = 0;
    this.currentOffset = initialOffset;

    // Set alarm for TTL if configured
    if (options.ttlSeconds) {
      await this.ctx.storage.setAlarm(Date.now() + options.ttlSeconds * 1000);
    } else if (options.expiresAt) {
      await this.ctx.storage.setAlarm(new Date(options.expiresAt).getTime());
    }

    // Handle initial data
    if (options.initialData?.length) {
      return await this.append(options.initialData);
    }

    return initialOffset;
  }

  async deleteAll(): Promise<void> {
    // Clear all waiters
    for (const waiter of this.waiters) {
      waiter();
    }
    this.waiters.clear();

    // Clear memoized state
    this.metadata = null;
    this.counter = 0;
    this.currentOffset = this.formatOffset(0);

    // Delete all storage
    await this.ctx.storage.deleteAll();
  }

  async getMetadata(): Promise<StreamMetadata | null> {
    return this.metadata;
  }

  async getCurrentOffset(): Promise<string> {
    return this.currentOffset;
  }

  async append(messages: Uint8Array[], seq?: string): Promise<string> {
    await this.resetTtlIfApplicable();

    let lastOffset = "";

    for (const data of messages) {
      this.counter++;
      const offset = this.formatOffset(this.counter);
      lastOffset = offset;

      this.ctx.storage.kv.put(`message:${offset}`, {
        data,
        offset,
        timestamp: Date.now(),
      } satisfies StoredMessage);
    }

    this.ctx.storage.kv.put("counter", this.counter);
    this.ctx.storage.kv.put("currentOffset", lastOffset);
    this.currentOffset = lastOffset;

    if (seq && this.metadata) {
      const updatedMeta = { ...this.metadata, lastSeq: seq };
      this.ctx.storage.kv.put("metadata", updatedMeta);
      this.metadata = updatedMeta;
    }

    // Notify waiters
    this.notifyWaiters();

    return lastOffset;
  }

  async read(afterOffset?: string): Promise<StorageReadResult> {
    await this.resetTtlIfApplicable();

    const listOptions: DurableObjectListOptions = {
      prefix: "message:",
    };

    if (afterOffset) {
      listOptions.startAfter = `message:${afterOffset}`;
    }

    const entries = this.ctx.storage.kv.list<StoredMessage>(listOptions);
    const messages: StoredMessage[] = [];

    for (const [_, value] of entries) {
      messages.push(value);
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
    // read() resets TTL; no separate call needed here.
    const result = await this.read(afterOffset);
    if (result.messages.length > 0) {
      return { ...result, timedOut: false };
    }

    // Wait for new messages or timeout
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

  private notifyWaiters() {
    for (const waiter of this.waiters) {
      waiter();
    }
  }

  // Per PROTOCOL.md §5.1: Stream-TTL is a sliding window. Stream-Expires-At
  // is an absolute deadline and is not reset.
  private async resetTtlIfApplicable(): Promise<void> {
    const ttlSeconds = this.metadata?.ttlSeconds;
    if (!ttlSeconds) return;
    await this.ctx.storage.setAlarm(Date.now() + ttlSeconds * 1000);
  }

  private formatOffset(counter: number): string {
    const counterStr = String(counter).padStart(16, "0");
    const byteOffset = "0".repeat(16);
    return `${counterStr}_${byteOffset}`;
  }
}

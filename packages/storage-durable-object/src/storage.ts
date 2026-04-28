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
  ProducerState,
} from "@streamsy/core";

const ZERO_OFFSET = `${"0".repeat(16)}_${"0".repeat(16)}`;

export class DurableObjectStreamStorage
  extends DurableObject
  implements StreamStorageInterface
{
  private waiters: Set<() => void> = new Set();
  private metadata: StreamMetadata | null = null;
  private counter: number = 0;
  private currentOffset: string;
  private producerLocks: Map<string, Promise<void>> = new Map();

  constructor(ctx: DurableObjectState, env: object) {
    super(ctx, env);
    this.metadata = ctx.storage.kv.get("metadata") ?? null;
    this.counter = ctx.storage.kv.get("counter") ?? 0;
    this.currentOffset = ctx.storage.kv.get("currentOffset") ?? ZERO_OFFSET;
  }

  /**
   * TTL alarm handler.
   * If forks still reference this stream (refCount > 0), soft-delete instead of purging.
   */
  override async alarm(): Promise<void> {
    if (!this.metadata) return;
    const refCount = this.metadata.refCount ?? 0;
    if (refCount > 0) {
      await this.setSoftDeleted(true);
      return;
    }
    await this.deleteAll();
  }

  async createStream(options: CreateStreamOptions): Promise<string> {
    const metadata: StreamMetadata = {
      contentType: options.contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: Date.now(),
      ...(options.closed ? { closed: true, closedAt: Date.now() } : {}),
      forkedFrom: options.forkedFrom,
      forkOffset: options.forkOffset,
      refCount: 0,
    };

    this.ctx.storage.kv.put("metadata", metadata);

    let initialOffset: string;
    let initialCounter: number;
    if (options.forkedFrom && options.forkOffset) {
      initialOffset = options.forkOffset;
      initialCounter = parseCounter(options.forkOffset);
    } else {
      initialOffset = ZERO_OFFSET;
      initialCounter = 0;
    }

    this.ctx.storage.kv.put("counter", initialCounter);
    this.ctx.storage.kv.put("currentOffset", initialOffset);

    this.metadata = metadata;
    this.counter = initialCounter;
    this.currentOffset = initialOffset;

    if (options.ttlSeconds) {
      await this.ctx.storage.setAlarm(Date.now() + options.ttlSeconds * 1000);
    } else if (options.expiresAt) {
      await this.ctx.storage.setAlarm(new Date(options.expiresAt).getTime());
    }

    if (options.initialData?.length) {
      return await this.append(options.initialData);
    }

    return initialOffset;
  }

  async deleteAll(): Promise<void> {
    for (const waiter of this.waiters) {
      waiter();
    }
    this.waiters.clear();

    this.metadata = null;
    this.counter = 0;
    this.currentOffset = ZERO_OFFSET;

    // Delete all storage (also clears producer:* entries)
    await this.ctx.storage.deleteAll();
  }

  async getProducerState(
    producerId: string,
  ): Promise<ProducerState | undefined> {
    return this.ctx.storage.kv.get<ProducerState>(`producer:${producerId}`);
  }

  async setProducerState(
    producerId: string,
    state: ProducerState,
  ): Promise<void> {
    this.ctx.storage.kv.put(`producer:${producerId}`, state);
  }

  async acquireProducerLock(producerId: string): Promise<() => void> {
    while (this.producerLocks.has(producerId)) {
      await this.producerLocks.get(producerId);
    }

    let release!: () => void;
    const lock = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.producerLocks.set(producerId, lock);

    return () => {
      this.producerLocks.delete(producerId);
      release();
    };
  }

  async getMetadata(): Promise<StreamMetadata | null> {
    return this.metadata;
  }

  async getCurrentOffset(): Promise<string> {
    return this.currentOffset;
  }

  async append(messages: Uint8Array[], seq?: string): Promise<string> {
    await this.resetTtlIfApplicable();

    let lastOffset = this.currentOffset;

    for (const data of messages) {
      this.counter++;
      const offset = formatOffset(this.counter);
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

    this.notifyWaiters();

    return this.currentOffset;
  }

  async close(messages?: Uint8Array[], seq?: string): Promise<string> {
    if (messages && messages.length > 0) {
      await this.append(messages, seq);
    } else {
      await this.resetTtlIfApplicable();
      if (seq && this.metadata) {
        const updatedMeta = { ...this.metadata, lastSeq: seq };
        this.ctx.storage.kv.put("metadata", updatedMeta);
        this.metadata = updatedMeta;
      }
    }

    if (this.metadata) {
      const closedMeta = {
        ...this.metadata,
        closed: true,
        closedAt: Date.now(),
      };
      this.ctx.storage.kv.put("metadata", closedMeta);
      this.metadata = closedMeta;
    }

    // Wake any long-poll waiters so they observe the closed state.
    this.notifyWaiters();

    return this.currentOffset;
  }

  async read(afterOffset?: string): Promise<StorageReadResult> {
    await this.resetTtlIfApplicable();
    return this.readNoTouch(afterOffset);
  }

  async readNoTouch(afterOffset?: string): Promise<StorageReadResult> {
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
    const updated = { ...this.metadata, refCount: value };
    this.ctx.storage.kv.put("metadata", updated);
    this.metadata = updated;
  }

  async setSoftDeleted(value: boolean): Promise<void> {
    if (!this.metadata) return;
    const updated = { ...this.metadata, softDeleted: value };
    this.ctx.storage.kv.put("metadata", updated);
    this.metadata = updated;
    if (value) {
      this.notifyWaiters();
    }
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

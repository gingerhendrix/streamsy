/** Core-owned durable streams protocol and lifecycle policy. */

import type {
  StreamProtocolInterface,
  CreateOptions,
  CreateResult,
  AppendOptions,
  AppendResult,
  ReadOptions,
  ReadResult,
  ReadLiveOptions,
  ReadLiveResult,
  MetadataResult,
  DeleteResult,
} from "./types/protocol.ts";
import type { Clock, StreamStoreAdapter } from "./types/storage.ts";
import { systemClock } from "./protocol/helpers/clock.ts";
import { ProducerIdempotencyService } from "./protocol/helpers/producer-idempotency-service.ts";
import { AppendService } from "./protocol/append-service.ts";
import { ReadService } from "./protocol/read-service.ts";
import { LiveReadService } from "./protocol/live-read-service.ts";
import { ExpiryPolicy } from "./protocol/helpers/expiry-policy.ts";
import { CreateStreamService } from "./protocol/create-stream-service.ts";
import { ForkService } from "./protocol/helpers/fork-service.ts";
import { StreamGcService } from "./protocol/helpers/stream-gc-service.ts";
import { InProcessLockProvider } from "./protocol/storage/in-process-lock-provider.ts";
import { StreamMessageReader } from "./protocol/storage/stream-message-reader.ts";
import { StreamMessageWriter } from "./protocol/storage/stream-message-writer.ts";
import { StreamRecordFactory } from "./protocol/storage/stream-record-factory.ts";

export { ZERO_OFFSET } from "./protocol/helpers/offset-generator.ts";

const LONG_POLL_TIMEOUT_MS = 1_500;

export interface StreamProtocolOptions {
  clock?: Clock;
  longPollTimeoutMs?: number;
}

export class StreamProtocol implements StreamProtocolInterface {
  private clock: Clock;
  private longPollTimeoutMs: number;
  private locks = new InProcessLockProvider();
  private producerIdempotency: ProducerIdempotencyService;
  private appendService: AppendService;
  private readService: ReadService;
  private liveReadService: LiveReadService;
  private expiryPolicy: ExpiryPolicy;
  private createStreamService: CreateStreamService;
  private forkService: ForkService;
  private gcService: StreamGcService;
  private recordFactory: StreamRecordFactory;
  private messageWriter: StreamMessageWriter;
  private messageReader: StreamMessageReader;

  constructor(
    private store: StreamStoreAdapter,
    options: StreamProtocolOptions = {},
  ) {
    this.clock = options.clock ?? systemClock;
    this.longPollTimeoutMs = options.longPollTimeoutMs ?? LONG_POLL_TIMEOUT_MS;
    this.expiryPolicy = new ExpiryPolicy(store, this.clock, (id) => this.handleScheduledExpiry(id));
    this.gcService = new StreamGcService(store, {
      isExpired: (record) => this.expiryPolicy.isExpired(record),
    });
    this.producerIdempotency = new ProducerIdempotencyService(store);
    this.recordFactory = new StreamRecordFactory(this.clock, this.expiryPolicy);
    this.messageWriter = new StreamMessageWriter(store, this.clock, this.expiryPolicy);
    this.messageReader = new StreamMessageReader(store, this.expiryPolicy);
    this.appendService = new AppendService(store, this.producerIdempotency, {
      appendMessages: (id, record, data, seq) =>
        this.messageWriter.appendMessages(id, record, data, seq),
      closeRecord: (id, record, data, seq) => this.messageWriter.closeRecord(id, record, data, seq),
    });
    this.readService = new ReadService(store, (id, record, afterOffset) =>
      this.messageReader.readChain(id, record, afterOffset, undefined, true),
    );
    this.liveReadService = new LiveReadService(store, this.clock, this.longPollTimeoutMs, {
      readChain: (id, record, afterOffset) =>
        this.messageReader.readChain(id, record, afterOffset, undefined, true),
      readOwn: (id, after) => this.messageReader.readOwn(id, after),
      touch: (id, record) => this.expiryPolicy.touch(id, record, "live-read"),
    });
    this.forkService = new ForkService(store, {
      expireIfNeeded: (id) => this.expiryPolicy.expireIfNeeded(id),
      newRecord: (id, contentType, opts, fork) =>
        this.recordFactory.newRecord(id, contentType, opts, fork),
      scheduleExpiry: (record) => this.expiryPolicy.scheduleExpiry(record),
      appendMessages: (id, record, data) => this.messageWriter.appendMessages(id, record, data),
    });
    this.createStreamService = new CreateStreamService(store, {
      newRecord: (id, contentType, opts) =>
        this.recordFactory.newRecord(id, contentType, opts, undefined),
      scheduleExpiry: (record) => this.expiryPolicy.scheduleExpiry(record),
      appendMessages: (id, record, data) => this.messageWriter.appendMessages(id, record, data),
      closeRecord: (id, record, data) => this.messageWriter.closeRecord(id, record, data),
      createFork: (id, opts) => this.forkService.execute(id, opts),
    });
  }

  async create(streamId: string, options: CreateOptions): Promise<CreateResult> {
    return this.withStreamMutationLock(streamId, async () => {
      await this.expiryPolicy.expireIfNeeded(streamId);
      return this.createStreamService.execute(streamId, options);
    });
  }

  async append(streamId: string, options: AppendOptions): Promise<AppendResult> {
    return this.withStreamMutationLock(streamId, async () => {
      await this.expiryPolicy.expireIfNeeded(streamId);
      return this.appendService.execute(streamId, options);
    });
  }

  async read(streamId: string, options: ReadOptions): Promise<ReadResult> {
    await this.expiryPolicy.expireIfNeeded(streamId);
    return this.readService.execute(streamId, options);
  }

  async readLive(streamId: string, options: ReadLiveOptions): Promise<ReadLiveResult> {
    await this.expiryPolicy.expireIfNeeded(streamId);
    return this.liveReadService.execute(streamId, options);
  }

  async metadata(streamId: string): Promise<MetadataResult> {
    await this.expiryPolicy.expireIfNeeded(streamId);
    const record = await this.store.get(streamId);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    return {
      status: "ok",
      contentType: record.config.contentType,
      nextOffset: record.currentOffset,
      ttlSeconds: record.config.ttlSeconds,
      expiresAt: record.config.expiresAt,
      closed: record.lifecycle.closed === true,
    };
  }

  async delete(streamId: string): Promise<DeleteResult> {
    await this.expiryPolicy.expireIfNeeded(streamId);
    return this.gcService.delete(streamId);
  }

  /** Called by adapter schedulers/alarms; core decides soft-delete vs purge. */
  async handleScheduledExpiry(streamId: string): Promise<void> {
    return this.gcService.handleScheduledExpiry(streamId);
  }

  private withStreamMutationLock<T>(streamId: string, fn: () => Promise<T>): Promise<T> {
    // Offset allocation, initial-data writes, producer state, and lifecycle mutation must be
    // serialized per stream. Producer-keyed locks allow different producers to read the same
    // tail/counter concurrently and allocate duplicate offsets before either update persists.
    const lockKey = `stream:${streamId}`;
    return this.store.withLock
      ? this.store.withLock(lockKey, fn)
      : this.locks.withLock(lockKey, fn);
  }
}

/** Core-owned durable streams protocol and lifecycle policy. */

import type {
  StreamProtocolFactory,
  ProtocolStream,
  ProtocolGetResult,
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
import type { Stream, StreamFactory } from "./types/factory.ts";
import type { Clock } from "./types/storage.ts";
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

export interface StreamProtocolDeps extends StreamProtocolOptions {
  storage: {
    factory: StreamFactory;
  };
}

export function createStreamProtocol(deps: StreamProtocolDeps): StreamProtocolFactory {
  return new StreamProtocol(deps);
}

export class StreamProtocol implements StreamProtocolFactory {
  private clock: Clock;
  private longPollTimeoutMs: number;
  private locks = new InProcessLockProvider();
  private expiryPolicy: ExpiryPolicy;
  private gcService: StreamGcService;
  private recordFactory: StreamRecordFactory;

  constructor(private deps: StreamProtocolDeps) {
    this.clock = deps.clock ?? systemClock;
    this.longPollTimeoutMs = deps.longPollTimeoutMs ?? LONG_POLL_TIMEOUT_MS;
    this.expiryPolicy = new ExpiryPolicy(
      (id) => this.storageStream(id),
      this.clock,
      (id) => this.handleScheduledExpiry(id),
    );
    this.gcService = new StreamGcService((id) => this.storageStream(id), {
      isExpired: (record) => this.expiryPolicy.isExpired(record),
    });
    this.recordFactory = new StreamRecordFactory(this.clock, this.expiryPolicy);
  }

  async create(streamId: string, options: CreateOptions): Promise<CreateResult> {
    const storage = await this.storageStream(streamId);
    const result = await this.withStreamMutationLock(streamId, storage, async () => {
      const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
      return this.servicesFor(storage).create.execute(streamId, record, options);
    });
    if (result.status === "created" || result.status === "exists") {
      return { ...result, stream: this.boundProtocolStream(streamId) };
    }
    return result;
  }

  async get(streamId: string): Promise<ProtocolGetResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    return { status: "ok", stream: this.boundProtocolStream(streamId) };
  }

  /** Called by adapter schedulers/alarms; core decides soft-delete vs purge. */
  async handleScheduledExpiry(streamId: string): Promise<void> {
    return this.gcService.handleScheduledExpiry(streamId);
  }

  private boundProtocolStream(streamId: string): ProtocolStream {
    return {
      id: streamId,
      append: (options) => this.appendBound(streamId, options),
      read: (options) => this.readBound(streamId, options),
      readLive: (options) => this.readLiveBound(streamId, options),
      metadata: () => this.metadataBound(streamId),
      delete: () => this.deleteBound(streamId),
    };
  }

  private async appendBound(streamId: string, options: AppendOptions): Promise<AppendResult> {
    const storage = await this.storageStream(streamId);
    return this.withStreamMutationLock(streamId, storage, async () => {
      const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
      return this.servicesFor(storage).append.execute(streamId, record, options);
    });
  }

  private async readBound(streamId: string, options: ReadOptions): Promise<ReadResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
    return this.servicesFor(storage).read.execute(streamId, record, options);
  }

  private async readLiveBound(streamId: string, options: ReadLiveOptions): Promise<ReadLiveResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
    return this.servicesFor(storage).liveRead.execute(streamId, record, options);
  }

  private async metadataBound(streamId: string): Promise<MetadataResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
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

  private async deleteBound(streamId: string): Promise<DeleteResult> {
    await this.expiryPolicy.expireIfNeeded(streamId);
    return this.gcService.deleteStream(streamId);
  }

  private async storageStream(streamId: string): Promise<Stream> {
    return this.deps.storage.factory.getStream(streamId);
  }

  private servicesFor(storage: Stream): {
    append: AppendService;
    read: ReadService;
    liveRead: LiveReadService;
    create: CreateStreamService;
  } {
    const producerIdempotency = new ProducerIdempotencyService(storage.producers);
    const messageWriter = new StreamMessageWriter(storage, this.clock, this.expiryPolicy);
    const messageReader = new StreamMessageReader(
      (id) => this.storageStream(id),
      this.expiryPolicy,
    );
    const append = new AppendService(producerIdempotency, {
      appendMessages: (id, record, data, seq) =>
        messageWriter.appendMessages(id, record, data, seq),
      closeRecord: (id, record, data, seq) => messageWriter.closeRecord(id, record, data, seq),
    });
    const read = new ReadService((id, record, afterOffset) =>
      messageReader.readChain(id, record, afterOffset, undefined, true),
    );
    const liveRead = new LiveReadService(storage, this.clock, this.longPollTimeoutMs, {
      readChain: (id, record, afterOffset) =>
        messageReader.readChain(id, record, afterOffset, undefined, true),
      readOwn: (id, after) => messageReader.readOwn(id, after),
      touch: (id, record) => this.expiryPolicy.touch(id, record, "live-read"),
    });
    const forkService = new ForkService((id) => this.storageStream(id), {
      expireIfNeeded: async (id) => {
        await this.expiryPolicy.expireIfNeeded(id);
      },
      newRecord: (id, contentType, opts, fork) =>
        this.recordFactory.newRecord(id, contentType, opts, fork),
      scheduleExpiry: (record) => this.expiryPolicy.scheduleExpiry(record),
      appendMessages: (id, record, data) => messageWriter.appendMessages(id, record, data),
    });
    const create = new CreateStreamService(storage, {
      newRecord: (id, contentType, opts) =>
        this.recordFactory.newRecord(id, contentType, opts, undefined),
      scheduleExpiry: (record) => this.expiryPolicy.scheduleExpiry(record),
      appendMessages: (id, record, data) => messageWriter.appendMessages(id, record, data),
      closeRecord: (id, record, data) => messageWriter.closeRecord(id, record, data),
      createFork: (id, opts) => forkService.execute(id, opts),
    });
    return { append, read, liveRead, create };
  }

  private async withStreamMutationLock<T>(
    streamId: string,
    storage: Stream,
    fn: () => Promise<T>,
  ): Promise<T> {
    return storage.mutations
      ? storage.mutations.withMutationLock(fn)
      : this.locks.withLock(`stream:${streamId}`, fn);
  }
}

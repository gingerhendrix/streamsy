/** Core-owned durable streams protocol and lifecycle policy. */

import type {
  StreamProtocolFactory,
  ProtocolStream as ProtocolStreamApi,
  ProtocolGetResult,
  CreateOptions,
  CreateResult,
  CreateOutcome,
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

interface ProtocolStreamDeps {
  id: string;
  storage: Stream;
  clock: Clock;
  longPollTimeoutMs: number;
  expiryPolicy: ExpiryPolicy;
  gcService: StreamGcService;
  recordFactory: StreamRecordFactory;
  resolveStorageStream: (streamId: string) => Promise<Stream>;
  withStreamMutationLock: <T>(
    streamId: string,
    storage: Stream,
    fn: () => Promise<T>,
  ) => Promise<T>;
}

export class ProtocolStream implements ProtocolStreamApi {
  readonly id: string;
  private appendService: AppendService;
  private readService: ReadService;
  private liveReadService: LiveReadService;
  private createService: CreateStreamService;

  constructor(private deps: ProtocolStreamDeps) {
    this.id = deps.id;
    const producerIdempotency = new ProducerIdempotencyService({ store: deps.storage.producers });
    const messageWriter = new StreamMessageWriter({
      stream: deps.storage,
      clock: deps.clock,
      expiryPolicy: deps.expiryPolicy,
    });
    const messageReader = new StreamMessageReader({
      resolve: deps.resolveStorageStream,
      expiryPolicy: deps.expiryPolicy,
    });
    this.appendService = new AppendService({
      producerIdempotency,
      mutators: {
        appendMessages: (id, record, data, seq) =>
          messageWriter.appendMessages(id, record, data, seq),
        closeRecord: (id, record, data, seq) => messageWriter.closeRecord(id, record, data, seq),
      },
    });
    this.readService = new ReadService({
      readChain: (id, record, afterOffset) =>
        messageReader.readChain(id, record, afterOffset, undefined, true),
    });
    this.liveReadService = new LiveReadService({
      store: deps.storage,
      clock: deps.clock,
      longPollTimeoutMs: deps.longPollTimeoutMs,
      readChain: (id, record, afterOffset) =>
        messageReader.readChain(id, record, afterOffset, undefined, true),
      readOwn: (id, after) => messageReader.readOwn(id, after),
      touch: (id, record) => deps.expiryPolicy.touch(id, record, "live-read"),
    });
    const forkService = new ForkService({
      resolve: deps.resolveStorageStream,
      mutators: {
        expireIfNeeded: async (id) => {
          await deps.expiryPolicy.expireIfNeeded(id);
        },
        newRecord: (id, contentType, opts, fork) =>
          deps.recordFactory.newRecord(id, contentType, opts, fork),
        scheduleExpiry: (record) => deps.expiryPolicy.scheduleExpiry(record),
        appendMessages: (id, record, data) => messageWriter.appendMessages(id, record, data),
      },
    });
    this.createService = new CreateStreamService({
      store: deps.storage,
      mutators: {
        newRecord: (id, contentType, opts) =>
          deps.recordFactory.newRecord(id, contentType, opts, undefined),
        scheduleExpiry: (record) => deps.expiryPolicy.scheduleExpiry(record),
        appendMessages: (id, record, data) => messageWriter.appendMessages(id, record, data),
        closeRecord: (id, record, data) => messageWriter.closeRecord(id, record, data),
        createFork: (id, opts) => forkService.execute(id, opts),
      },
    });
  }

  async create(options: CreateOptions): Promise<CreateOutcome> {
    return this.deps.withStreamMutationLock(this.id, this.deps.storage, async () => {
      const record = await this.deps.expiryPolicy.expireIfNeeded(this.id, this.deps.storage);
      return this.createService.execute(this.id, record, options);
    });
  }

  async append(options: AppendOptions): Promise<AppendResult> {
    return this.deps.withStreamMutationLock(this.id, this.deps.storage, async () => {
      const record = await this.deps.expiryPolicy.expireIfNeeded(this.id, this.deps.storage);
      return this.appendService.execute(this.id, record, options);
    });
  }

  async read(options: ReadOptions): Promise<ReadResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.id, this.deps.storage);
    return this.readService.execute(this.id, record, options);
  }

  async readLive(options: ReadLiveOptions): Promise<ReadLiveResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.id, this.deps.storage);
    return this.liveReadService.execute(this.id, record, options);
  }

  async metadata(): Promise<MetadataResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.id, this.deps.storage);
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

  async delete(): Promise<DeleteResult> {
    await this.deps.expiryPolicy.expireIfNeeded(this.id);
    return this.deps.gcService.deleteStream(this.id);
  }
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
    this.expiryPolicy = new ExpiryPolicy({
      resolve: (id) => this.storageStream(id),
      clock: this.clock,
      onScheduledExpiry: (id) => this.handleScheduledExpiry(id),
    });
    this.gcService = new StreamGcService({
      resolve: (id) => this.storageStream(id),
      mutators: {
        isExpired: (record) => this.expiryPolicy.isExpired(record),
      },
    });
    this.recordFactory = new StreamRecordFactory({
      clock: this.clock,
      expiryPolicy: this.expiryPolicy,
    });
  }

  async create(streamId: string, options: CreateOptions): Promise<CreateResult> {
    const storage = await this.storageStream(streamId);
    const stream = this.boundProtocolStream(streamId, storage);
    const result = await stream.create(options);
    if (result.status === "created" || result.status === "exists") {
      return { ...result, stream };
    }
    return result;
  }

  async get(streamId: string): Promise<ProtocolGetResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(streamId, storage);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    return { status: "ok", stream: this.boundProtocolStream(streamId, storage) };
  }

  /** Called by adapter schedulers/alarms; core decides soft-delete vs purge. */
  async handleScheduledExpiry(streamId: string): Promise<void> {
    return this.gcService.handleScheduledExpiry(streamId);
  }

  private boundProtocolStream(streamId: string, storage: Stream): ProtocolStream {
    return new ProtocolStream({
      id: streamId,
      storage,
      clock: this.clock,
      longPollTimeoutMs: this.longPollTimeoutMs,
      expiryPolicy: this.expiryPolicy,
      gcService: this.gcService,
      recordFactory: this.recordFactory,
      resolveStorageStream: (id) => this.storageStream(id),
      withStreamMutationLock: (id, stream, fn) => this.withStreamMutationLock(id, stream, fn),
    });
  }

  private async storageStream(streamId: string): Promise<Stream> {
    return this.deps.storage.factory.getStream(streamId);
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

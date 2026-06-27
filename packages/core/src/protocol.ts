/** Core-owned durable streams protocol and lifecycle policy. */

import type {
  AppendOptions,
  AppendResult,
  CreateOptions,
  CreateOutcome,
  CreateResult,
  DeleteResult,
  MetadataResult,
  ProtocolGetResult,
  ProtocolStream as ProtocolStreamApi,
  ReadLiveOptions,
  ReadLiveResult,
  ReadOptions,
  ReadResult,
  StreamProtocolFactory,
} from "./types/protocol.ts";
import type { Stream, StreamFactory } from "./types/factory.ts";
import { notSupported } from "./types/factory.ts";
import type { Clock, StoredMessage, StreamRecord } from "./types/storage.ts";
import { systemClock } from "./protocol/helpers/clock.ts";
import { AppendService } from "./protocol/append-service.ts";
import { ReadService } from "./protocol/read-service.ts";
import { LiveReadService } from "./protocol/live-read-service.ts";
import { ExpiryPolicy } from "./protocol/helpers/expiry-policy.ts";
import { CreateStreamService } from "./protocol/create-stream-service.ts";
import { ForkPlanBuilder } from "./protocol/helpers/fork-plan-builder.ts";
import { StreamMessageReader } from "./protocol/storage/stream-message-reader.ts";
import { StreamRecordFactory } from "./protocol/storage/stream-record-factory.ts";
import { compareOffsets, isValidOffset, ZERO_OFFSET } from "./protocol/helpers/offset-generator.ts";
import { runAfterCommit } from "./protocol/helpers/after-commit-effects.ts";

export { ZERO_OFFSET } from "./protocol/helpers/offset-generator.ts";

const LONG_POLL_TIMEOUT_MS = 1_500;
const MAX_COMMIT_ATTEMPTS = 8;
const MAX_NO_PROGRESS_ATTEMPTS = 8;
type DeleteStatus = "purged" | "retained-soft-deleted" | "not-found" | "gone";

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

async function runDeleteEffects(status: DeleteStatus, storage: Stream): Promise<void> {
  if (status === "purged") await runAfterCommit({ cancelExpiry: true, notify: "deleted" }, storage);
  else if (status === "retained-soft-deleted")
    await runAfterCommit({ notify: "soft-deleted" }, storage);
}

function mapDelete(status: DeleteStatus): DeleteResult {
  if (status === "not-found") return { status: "not-found" };
  if (status === "gone") return { status: "gone" };
  return { status: "ok" };
}

interface ProtocolStreamDeps {
  storage: Stream;
  factory: StreamFactory;
  clock: Clock;
  longPollTimeoutMs: number;
  expiryPolicy: ExpiryPolicy;
  recordFactory: StreamRecordFactory;
  resolveStorageStream: (streamId: string) => Promise<Stream>;
}

export class ProtocolStream implements ProtocolStreamApi {
  readonly id: string;
  private appendService: AppendService;
  private readService: ReadService;
  private liveReadService: LiveReadService;
  private createService: CreateStreamService;
  private forkPlanBuilder: ForkPlanBuilder;

  constructor(private deps: ProtocolStreamDeps) {
    this.id = deps.storage.id;
    const messageReader = new StreamMessageReader({
      stream: deps.storage,
      resolve: deps.resolveStorageStream,
      expiryPolicy: deps.expiryPolicy,
    });
    this.appendService = new AppendService({ clock: deps.clock });
    this.readService = new ReadService({
      readChain: (record, afterOffset) => messageReader.readChain(record, afterOffset),
    });
    this.liveReadService = new LiveReadService({
      store: deps.storage,
      clock: deps.clock,
      longPollTimeoutMs: deps.longPollTimeoutMs,
      readChain: (record, afterOffset) => messageReader.readChain(record, afterOffset),
      readOwn: (after) => messageReader.readOwn(after),
    });
    this.createService = new CreateStreamService({
      clock: deps.clock,
      newRecord: (contentType, opts) =>
        deps.recordFactory.newRecord(deps.storage.id, contentType, opts, undefined),
    });
    this.forkPlanBuilder = new ForkPlanBuilder({
      clock: deps.clock,
      newRecord: (streamId, contentType, opts, fork) =>
        deps.recordFactory.newRecord(streamId, contentType, opts, fork),
    });
  }

  async create(options: CreateOptions): Promise<CreateOutcome> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    const decision = this.createService.plan(record, options);
    if (decision.kind === "terminal") return decision.result;

    if (decision.kind === "fork") return this.createFork(options);

    const commit = await this.deps.factory.create(decision.plan);
    if (commit.status === "exists")
      return this.createService.resultForExisting(commit.record, options);

    await runAfterCommit(decision.plan.afterCommit, this.deps.storage);
    return decision.toResult(commit.record);
  }

  async append(options: AppendOptions): Promise<AppendResult> {
    let lastRecord = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    let noProgressAttempts = 0;
    let attempt = 0;
    const hasUserExpectedOffset = options.expectedOffset !== undefined;

    while (true) {
      const producerState = options.producer
        ? await this.deps.storage.getProducerState(options.producer.producerId)
        : undefined;
      const decision = this.appendService.plan(lastRecord, options, producerState);
      if (decision.kind === "terminal") return decision.result;

      const out = await this.deps.storage.commit(decision.plan);
      if (out.status === "committed") {
        await runAfterCommit(decision.plan.afterCommit, this.deps.storage);
        return decision.toResult(out.record);
      }

      lastRecord = out.record;
      if (lastRecord === null) return { status: "not-found" };

      if (hasUserExpectedOffset) {
        attempt++;
        if (attempt >= MAX_COMMIT_ATTEMPTS) {
          return {
            status: "conflict",
            conflictReason: "expected-offset",
            offset: lastRecord.currentOffset,
          };
        }
      } else {
        const previousOffset = decision.plan.preconditions.expectedOffset ?? ZERO_OFFSET;
        const madeProgress = compareOffsets(lastRecord.currentOffset, previousOffset) > 0;
        noProgressAttempts = madeProgress ? 0 : noProgressAttempts + 1;
        if (noProgressAttempts >= MAX_NO_PROGRESS_ATTEMPTS) return { status: "busy" };
      }

      await Promise.resolve();
    }
  }

  async read(options: ReadOptions): Promise<ReadResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    return this.readService.execute(record, options);
  }

  async readLive(options: ReadLiveOptions): Promise<ReadLiveResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    return this.liveReadService.execute(record, options);
  }

  async metadata(): Promise<MetadataResult> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
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
    await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    const commit = await this.deps.factory.delete({ streamId: this.id, reason: "delete" });
    await runDeleteEffects(commit.status, this.deps.storage);
    return mapDelete(commit.status);
  }

  /**
   * Read the source messages immediately after `forkOffset`, composed through
   * the source's own fork chain, so a sub-offset fork can materialize a
   * partial-message prefix (and chained forks compose). Only needed when a
   * positive `forkSubOffset` is requested; validation of the offset/sub-offset
   * combination stays in the fork plan builder.
   */
  private async readSourceTailForSubOffset(
    sourceStream: Stream,
    source: StreamRecord | null,
    options: CreateOptions,
  ): Promise<StoredMessage[] | undefined> {
    const subOffset = options.forkSubOffset;
    if (subOffset === undefined || subOffset <= 0 || !source) return undefined;
    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValidOffset(forkOffset)) return undefined;
    const reader = new StreamMessageReader({
      stream: sourceStream,
      resolve: this.deps.resolveStorageStream,
      expiryPolicy: this.deps.expiryPolicy,
    });
    return reader.readChain(source, forkOffset, undefined, false);
  }

  private async createFork(options: CreateOptions): Promise<CreateOutcome> {
    const fork = this.deps.factory.fork;
    if (!fork) return notSupported("fork");

    const sourceId = options.forkedFrom!;
    const sourceStream = await this.deps.resolveStorageStream(sourceId);
    const source = await this.deps.expiryPolicy.expireIfNeeded(sourceStream);
    const sourceTail = await this.readSourceTailForSubOffset(sourceStream, source, options);
    const decision = this.forkPlanBuilder.build(this.id, sourceId, source, options, sourceTail);
    if (decision.kind === "terminal") return decision.result;

    const commit = await fork.call(this.deps.factory, decision.plan);
    if (commit.status === "exists") {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "config-mismatch",
        errorMessage: `Stream already exists: ${this.id}`,
      };
    }
    if (commit.status === "fork-source-gone") {
      return {
        status: "not-found",
        nextOffset: "",
        contentType: "",
        errorMessage: `Source stream not found: ${sourceId}`,
      };
    }

    await runAfterCommit(decision.plan.afterCommit, this.deps.storage);
    return decision.toResult(commit.record);
  }
}

export class StreamProtocol implements StreamProtocolFactory {
  private clock: Clock;
  private longPollTimeoutMs: number;
  private expiryPolicy: ExpiryPolicy;
  private recordFactory: StreamRecordFactory;

  constructor(private deps: StreamProtocolDeps) {
    this.clock = deps.clock ?? systemClock;
    this.longPollTimeoutMs = deps.longPollTimeoutMs ?? LONG_POLL_TIMEOUT_MS;
    this.expiryPolicy = new ExpiryPolicy({
      resolve: (id) => this.storageStream(id),
      clock: this.clock,
      onScheduledExpiry: (id) => this.handleScheduledExpiry(id),
    });
    this.recordFactory = new StreamRecordFactory({
      clock: this.clock,
      expiryPolicy: this.expiryPolicy,
    });
  }

  async create(streamId: string, options: CreateOptions): Promise<CreateResult> {
    const storage = await this.storageStream(streamId);
    const stream = this.boundProtocolStream(storage);
    const result = await stream.create(options);
    if (result.status === "created" || result.status === "exists") {
      return { ...result, stream };
    }
    return result;
  }

  async get(streamId: string): Promise<ProtocolGetResult> {
    const storage = await this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(storage);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    return { status: "ok", stream: this.boundProtocolStream(storage) };
  }

  /** Called by adapter schedulers/alarms; core decides whether expiry applies. */
  async handleScheduledExpiry(streamId: string): Promise<void> {
    const storage = await this.storageStream(streamId);
    const record = await storage.getRecord();
    if (!record) return;
    if (!this.expiryPolicy.isExpired(record)) {
      // The deadline slid forward (e.g. a GET/POST renewed a sliding TTL) after
      // this alarm was scheduled. A fired one-shot alarm is consumed, so
      // re-arm it against the latest stored `expiresAtMs` instead of dropping
      // expiry and relying solely on lazy checks.
      await this.expiryPolicy.scheduleExpiry(record);
      return;
    }
    const commit = await this.deps.storage.factory.delete({ streamId, reason: "expiry" });
    await runDeleteEffects(commit.status, storage);
  }

  private boundProtocolStream(storage: Stream): ProtocolStream {
    return new ProtocolStream({
      storage,
      factory: this.deps.storage.factory,
      clock: this.clock,
      longPollTimeoutMs: this.longPollTimeoutMs,
      expiryPolicy: this.expiryPolicy,
      recordFactory: this.recordFactory,
      resolveStorageStream: (id) => this.storageStream(id),
    });
  }

  private async storageStream(streamId: string): Promise<Stream> {
    return this.deps.storage.factory.getStream(streamId);
  }
}

/** Core-owned durable streams protocol and lifecycle policy. */

import type {
  AfterCommitHook,
  AppendOptions,
  AppendResult,
  CommitEvent,
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
import type { StorageAdapter } from "./types/storage-adapter.ts";
import { notSupported } from "./types/storage-adapter.ts";
import type { Clock, StoredMessage, StreamRecord } from "./types/storage.ts";
import { systemClock } from "./protocol/helpers/clock.ts";
import { AppendService } from "./protocol/append-service.ts";
import { ReadService } from "./protocol/read-service.ts";
import { LiveReadService, type LiveReadStore } from "./protocol/live-read-service.ts";
import { bindStream, type BoundStream } from "./protocol/helpers/bind-stream.ts";
import { ExpiryPolicy } from "./protocol/helpers/expiry-policy.ts";
import { CreateStreamService } from "./protocol/create-stream-service.ts";
import { ForkPlanBuilder } from "./protocol/helpers/fork-plan-builder.ts";
import { StreamMessageReader } from "./protocol/storage/stream-message-reader.ts";
import { StreamRecordFactory } from "./protocol/storage/stream-record-factory.ts";
import {
  assertValidOffsetGenerator,
  compareOffsets,
  defaultOffsetGenerator,
  isValidOffset,
  type OffsetGenerator,
} from "./protocol/helpers/offset-generator.ts";
import { runAfterCommit } from "./protocol/helpers/after-commit-effects.ts";

export { ZERO_OFFSET } from "./protocol/helpers/offset-generator.ts";

const LONG_POLL_TIMEOUT_MS = 1_500;
const MAX_COMMIT_ATTEMPTS = 8;
const MAX_NO_PROGRESS_ATTEMPTS = 8;
type DeleteStatus = "purged" | "retained-soft-deleted" | "not-found" | "gone";

export interface StreamProtocolOptions {
  clock?: Clock;
  longPollTimeoutMs?: number;
  /** Opaque offset scheme; defaults to Streamsy's fixed-width counter format. */
  offsetGenerator?: OffsetGenerator;
}

export interface StreamProtocolDeps extends StreamProtocolOptions {
  storage: {
    adapter: StorageAdapter;
  };
}

export function createStreamProtocol(deps: StreamProtocolDeps): StreamProtocolFactory {
  return new StreamProtocol(deps);
}

async function runDeleteEffects(status: DeleteStatus, storage: BoundStream): Promise<void> {
  // Only expiry cancellation crosses as an after-commit effect now. Waking live
  // readers on a purge / soft-delete is the adapter's own concern, fired from
  // inside the delete; `awaitChange` re-reads durable state and observes the
  // `!present` / `softDeleted` transition regardless.
  if (status === "purged") await runAfterCommit({ cancelExpiry: true }, storage);
}

function mapDelete(status: DeleteStatus): DeleteResult {
  if (status === "not-found") return { status: "not-found" };
  if (status === "gone") return { status: "gone" };
  return { status: "ok" };
}

/**
 * Adapt a storage `Stream` to the live-read store. Every adapter implements
 * `awaitChange` (a backend that cannot wake cheaply polls inside its own
 * implementation), so core wires it straight through with no fallback.
 */
function liveReadStore(storage: BoundStream): LiveReadStore {
  return {
    getRecord: () => storage.getRecord(),
    awaitChange: (options) => storage.awaitChange(options),
  };
}

interface ProtocolStreamDeps {
  storage: BoundStream;
  adapter: StorageAdapter;
  clock: Clock;
  longPollTimeoutMs: number;
  expiryPolicy: ExpiryPolicy;
  recordFactory: StreamRecordFactory;
  resolveStorageStream: (streamId: string) => BoundStream;
  emitAfterCommit: (record: StreamRecord) => void;
  offsets: OffsetGenerator;
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
    this.appendService = new AppendService({ clock: deps.clock, offsets: deps.offsets });
    this.readService = new ReadService({
      readChain: (record, afterOffset) => messageReader.readChain(record, afterOffset),
    });
    this.liveReadService = new LiveReadService({
      store: liveReadStore(deps.storage),
      clock: deps.clock,
      longPollTimeoutMs: deps.longPollTimeoutMs,
      offsets: deps.offsets,
      readChain: (record, afterOffset) => messageReader.readChain(record, afterOffset),
      readOwn: (after) => messageReader.readOwn(after),
    });
    this.createService = new CreateStreamService({
      clock: deps.clock,
      offsets: deps.offsets,
      newRecord: (contentType, opts) =>
        deps.recordFactory.newRecord(deps.storage.id, contentType, opts, undefined),
    });
    this.forkPlanBuilder = new ForkPlanBuilder({
      clock: deps.clock,
      offsets: deps.offsets,
      newRecord: (streamId, contentType, opts, fork) =>
        deps.recordFactory.newRecord(streamId, contentType, opts, fork),
    });
  }

  async create(options: CreateOptions): Promise<CreateOutcome> {
    const record = await this.deps.expiryPolicy.expireIfNeeded(this.deps.storage);
    const decision = this.createService.plan(record, options);
    if (decision.kind === "terminal") return decision.result;

    if (decision.kind === "fork") return this.createFork(options);

    const commit = await this.deps.adapter.create(decision.plan);
    if (commit.status === "exists")
      return this.createService.resultForExisting(commit.record, options);

    await runAfterCommit(decision.afterCommit, this.deps.storage);
    this.deps.emitAfterCommit(commit.record);
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

      const out = await this.deps.storage.append(decision.plan);
      if (out.status === "appended") {
        await runAfterCommit(decision.afterCommit, this.deps.storage);
        this.deps.emitAfterCommit(out.record);
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
        const previousOffset =
          decision.plan.preconditions.expectedOffset ?? this.deps.offsets.initialOffset;
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
    const commit = await this.deps.adapter.delete({ streamId: this.id, reason: "delete" });
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
    sourceStream: BoundStream,
    source: StreamRecord | null,
    options: CreateOptions,
  ): Promise<StoredMessage[] | undefined> {
    const subOffset = options.forkSubOffset;
    if (subOffset === undefined || subOffset <= 0 || !source) return undefined;
    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValidOffset(this.deps.offsets, forkOffset)) return undefined;
    const reader = new StreamMessageReader({
      stream: sourceStream,
      resolve: this.deps.resolveStorageStream,
      expiryPolicy: this.deps.expiryPolicy,
    });
    return reader.readChain(source, forkOffset, undefined, false);
  }

  private async createFork(options: CreateOptions): Promise<CreateOutcome> {
    const fork = this.deps.adapter.fork;
    if (!fork) return notSupported("fork");

    const sourceId = options.forkedFrom!;
    const sourceStream = this.deps.resolveStorageStream(sourceId);
    const source = await this.deps.expiryPolicy.expireIfNeeded(sourceStream);
    const sourceTail = await this.readSourceTailForSubOffset(sourceStream, source, options);
    const decision = this.forkPlanBuilder.build(this.id, sourceId, source, options, sourceTail);
    if (decision.kind === "terminal") return decision.result;

    const commit = await fork.call(this.deps.adapter, decision.plan);
    if (commit.status === "exists") {
      // Same idempotency as create: a racing byte-identical fork resolves as
      // `exists` success; a genuinely different child is a config conflict.
      return this.createService.resultForExisting(commit.record, options);
    }
    if (commit.status === "fork-source-gone") {
      return {
        status: "not-found",
        nextOffset: "",
        contentType: "",
        errorMessage: `Source stream not found: ${sourceId}`,
      };
    }

    await runAfterCommit(decision.afterCommit, this.deps.storage);
    this.deps.emitAfterCommit(commit.record);
    return decision.toResult(commit.record);
  }
}

export class StreamProtocol implements StreamProtocolFactory {
  private clock: Clock;
  private longPollTimeoutMs: number;
  private expiryPolicy: ExpiryPolicy;
  private recordFactory: StreamRecordFactory;
  readonly offsetGenerator: OffsetGenerator;
  private hooks = new Set<AfterCommitHook>();

  constructor(private deps: StreamProtocolDeps) {
    this.clock = deps.clock ?? systemClock;
    this.offsetGenerator = deps.offsetGenerator ?? defaultOffsetGenerator;
    assertValidOffsetGenerator(this.offsetGenerator);
    this.longPollTimeoutMs = deps.longPollTimeoutMs ?? LONG_POLL_TIMEOUT_MS;
    this.expiryPolicy = new ExpiryPolicy({
      resolve: (id) => this.storageStream(id),
      clock: this.clock,
      onScheduledExpiry: (id) => this.handleScheduledExpiry(id),
    });
    this.recordFactory = new StreamRecordFactory({
      clock: this.clock,
      expiryPolicy: this.expiryPolicy,
      offsets: this.offsetGenerator,
    });
  }

  isValidOffset(offset: string): boolean {
    return isValidOffset(this.offsetGenerator, offset);
  }

  async create(streamId: string, options: CreateOptions): Promise<CreateResult> {
    const storage = this.storageStream(streamId);
    const stream = this.boundProtocolStream(storage);
    const result = await stream.create(options);
    if (result.status === "created" || result.status === "exists") {
      return { ...result, stream };
    }
    return result;
  }

  async get(streamId: string): Promise<ProtocolGetResult> {
    const storage = this.storageStream(streamId);
    const record = await this.expiryPolicy.expireIfNeeded(storage);
    if (!record) return { status: "not-found" };
    if (record.lifecycle.softDeleted) return { status: "gone" };
    return { status: "ok", stream: this.boundProtocolStream(storage) };
  }

  onAfterCommit(hook: AfterCommitHook): () => void {
    this.hooks.add(hook);
    return () => {
      this.hooks.delete(hook);
    };
  }

  /** Called by adapter schedulers/alarms; core decides whether expiry applies. */
  async handleScheduledExpiry(streamId: string): Promise<void> {
    const storage = this.storageStream(streamId);
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
    const commit = await this.deps.storage.adapter.delete({ streamId, reason: "expiry" });
    await runDeleteEffects(commit.status, storage);
  }

  private boundProtocolStream(storage: BoundStream): ProtocolStream {
    return new ProtocolStream({
      storage,
      adapter: this.deps.storage.adapter,
      clock: this.clock,
      longPollTimeoutMs: this.longPollTimeoutMs,
      expiryPolicy: this.expiryPolicy,
      recordFactory: this.recordFactory,
      resolveStorageStream: (id) => this.storageStream(id),
      emitAfterCommit: (record) => this.emitAfterCommit(record),
      offsets: this.offsetGenerator,
    });
  }

  /** Bind the flat adapter to one id for ergonomic core-side per-stream calls. */
  private storageStream(streamId: string): BoundStream {
    return bindStream(this.deps.storage.adapter, streamId);
  }

  private emitAfterCommit(record: StreamRecord): void {
    const event: CommitEvent = {
      streamId: record.id,
      offset: record.currentOffset,
      closed: record.lifecycle.closed === true,
      softDeleted: record.lifecycle.softDeleted === true,
    };
    for (const hook of this.hooks) {
      try {
        hook(event);
      } catch (error) {
        console.warn("after-commit hook failed", error);
      }
    }
  }
}

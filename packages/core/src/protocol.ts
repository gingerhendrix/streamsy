/**
 * Protocol Layer Implementation
 *
 * Handles validation, JSON mode processing, cursor generation,
 * and orchestration between HTTP and storage layers.
 */

import type {
  StreamProtocolInterface,
  StorageFactory,
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
import type {
  ProducerState,
  StreamMetadata,
  StreamStorage,
  StoredMessage,
} from "./types/storage.ts";

type ProducerValidation =
  | { kind: "accepted"; proposedState: ProducerState }
  | { kind: "duplicate"; lastSeq: number; epoch: number }
  | { kind: "stale-epoch"; currentEpoch: number }
  | { kind: "gap"; expectedSeq: number; receivedSeq: number }
  | { kind: "invalid-epoch-seq" };
const CURSOR_EPOCH = new Date("2024-10-09T00:00:00.000Z").getTime();
const CURSOR_INTERVAL_MS = 20_000;
const ZERO_OFFSET = `${"0".repeat(16)}_${"0".repeat(16)}`;
const OFFSET_REGEX = /^\d{1,16}_\d{1,16}$/;

export class StreamProtocol implements StreamProtocolInterface {
  constructor(private getStorage: StorageFactory) {}

  async create(
    streamId: string,
    options: CreateOptions
  ): Promise<CreateResult> {
    const storage = this.getStorage(streamId);
    const existing = await storage.getMetadata();

    if (existing) {
      if (existing.softDeleted) {
        // Soft-deleted streams block path re-creation.
        return {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "soft-deleted",
        };
      }

      if (!this.configMatches(existing, options)) {
        return {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "config-mismatch",
        };
      }
      const offset = await storage.getCurrentOffset();
      return {
        status: "exists",
        nextOffset: offset,
        contentType: existing.contentType,
        closed: existing.closed === true,
      };
    }

    if (options.forkedFrom) {
      return await this.createFork(streamId, options);
    }

    const contentType = options.contentType ?? "application/octet-stream";
    const wantClosed = options.closed === true;

    const initialMessages = options.initialData
      ? this.processData(options.initialData, contentType)
      : undefined;

    const nextOffset = await storage.createStream({
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      initialData: wantClosed ? undefined : initialMessages,
      closed: wantClosed,
    });

    let finalOffset = nextOffset;
    if (wantClosed) {
      finalOffset = await storage.close(initialMessages);
    }

    return {
      status: "created",
      nextOffset: finalOffset,
      contentType,
      closed: wantClosed,
    };
  }

  private async createFork(
    streamId: string,
    options: CreateOptions
  ): Promise<CreateResult> {
    const sourcePath = options.forkedFrom!;
    const sourceStorage = this.getStorage(sourcePath);
    const sourceMeta = await sourceStorage.getMetadata();

    if (!sourceMeta) {
      return {
        status: "not-found",
        nextOffset: "",
        contentType: "",
        errorMessage: `Source stream not found: ${sourcePath}`,
      };
    }

    if (sourceMeta.softDeleted) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-source-soft-deleted",
        errorMessage: `Source stream is soft-deleted: ${sourcePath}`,
      };
    }

    const sourceTail = await sourceStorage.getCurrentOffset();
    let forkOffset = options.forkOffset;
    if (forkOffset === undefined) {
      forkOffset = sourceTail;
    } else {
      if (!OFFSET_REGEX.test(forkOffset)) {
        return {
          status: "bad-request",
          nextOffset: "",
          contentType: "",
          errorMessage: "Invalid Stream-Fork-Offset format",
        };
      }
      if (forkOffset < ZERO_OFFSET || forkOffset > sourceTail) {
        return {
          status: "bad-request",
          nextOffset: "",
          contentType: "",
          errorMessage: "Stream-Fork-Offset exceeds source tail",
        };
      }
    }

    let contentType = options.contentType;
    if (!contentType || contentType.trim() === "") {
      contentType = sourceMeta.contentType;
    } else if (
      !this.contentTypeMatches(contentType, sourceMeta.contentType)
    ) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-content-type",
        errorMessage: "Fork Content-Type does not match source",
      };
    }

    // Resolve fork TTL/Expiry per spec table.
    const { ttlSeconds, expiresAt } = this.resolveForkExpiry(
      options,
      sourceMeta
    );

    const storage = this.getStorage(streamId);
    const nextOffset = await storage.createStream({
      contentType,
      ttlSeconds,
      expiresAt,
      forkedFrom: sourcePath,
      forkOffset,
      initialData: options.initialData
        ? this.processData(options.initialData, contentType)
        : undefined,
    });

    // Increment source refCount.
    const newRefCount = (sourceMeta.refCount ?? 0) + 1;
    await sourceStorage.setRefCount(newRefCount);

    return { status: "created", nextOffset, contentType };
  }

  async append(
    streamId: string,
    options: AppendOptions
  ): Promise<AppendResult> {
    const storage = this.getStorage(streamId);

    if (options.producer) {
      const release = await storage.acquireProducerLock(
        options.producer.producerId
      );
      try {
        return await this.appendInner(storage, options);
      } finally {
        release();
      }
    }

    return this.appendInner(storage, options);
  }

  private async appendInner(
    storage: StreamStorage,
    options: AppendOptions
  ): Promise<AppendResult> {
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return { status: "not-found" };
    }

    if (metadata.softDeleted) {
      return { status: "gone" };
    }

    const isClosed = metadata.closed === true;
    const wantClose = options.close === true;
    const hasBody = options.data.byteLength > 0;

    // Producer validation must run before closed-stream conflict handling so
    // retries of the producer tuple that closed the stream dedupe to 204 rather
    // than becoming 409 STREAM_CLOSED. Accepted producer tuples still cannot
    // append/close an already-closed stream.
    let producerValidation: ProducerValidation | undefined;
    if (options.producer) {
      const state = await storage.getProducerState(options.producer.producerId);
      producerValidation = this.validateProducer(
        state,
        options.producer.producerEpoch,
        options.producer.producerSeq
      );

      switch (producerValidation.kind) {
        case "duplicate":
          return {
            status: "duplicate",
            nextOffset: await storage.getCurrentOffset(),
            producerEpoch: producerValidation.epoch,
            producerSeq: producerValidation.lastSeq,
            closed: isClosed,
          };
        case "stale-epoch":
          return {
            status: "stale-epoch",
            currentEpoch: producerValidation.currentEpoch,
          };
        case "gap":
          return {
            status: "producer-gap",
            expectedSeq: producerValidation.expectedSeq,
            receivedSeq: producerValidation.receivedSeq,
          };
        case "invalid-epoch-seq":
          return { status: "invalid-epoch-seq" };
      }
    }

    // Close-only request: empty body + Stream-Closed: true
    if (wantClose && !hasBody) {
      // Idempotent non-producer close-only against an already closed stream.
      if (isClosed) {
        return {
          status: "appended",
          nextOffset: await storage.getCurrentOffset(),
          closed: true,
        };
      }

      // Close without appending data; ignore content-type for empty body.
      const offset = await storage.close(undefined, options.seq);
      if (producerValidation && producerValidation.kind === "accepted") {
        await storage.setProducerState(
          options.producer!.producerId,
          producerValidation.proposedState
        );
        return {
          status: "appended",
          nextOffset: offset,
          producerEpoch: producerValidation.proposedState.epoch,
          producerSeq: producerValidation.proposedState.lastSeq,
          closed: true,
        };
      }
      return { status: "appended", nextOffset: offset, closed: true };
    }

    // Stream is already closed and we have a body, or an accepted non-duplicate
    // producer tuple: closed wins over content-type and sequence.
    if (isClosed) {
      return {
        status: "conflict",
        conflictReason: "closed",
        closed: true,
        nextOffset: await storage.getCurrentOffset(),
      };
    }

    if (!this.contentTypeMatches(metadata.contentType, options.contentType)) {
      return { status: "conflict", conflictReason: "content-type" };
    }

    if (options.seq && metadata.lastSeq && options.seq <= metadata.lastSeq) {
      return { status: "conflict", conflictReason: "sequence" };
    }

    const processed = this.processData(options.data, metadata.contentType);

    if (wantClose) {
      const nextOffset = await storage.close(processed, options.seq);
      if (producerValidation && producerValidation.kind === "accepted") {
        await storage.setProducerState(
          options.producer!.producerId,
          producerValidation.proposedState
        );
        return {
          status: "appended",
          nextOffset,
          producerEpoch: producerValidation.proposedState.epoch,
          producerSeq: producerValidation.proposedState.lastSeq,
          closed: true,
        };
      }
      return { status: "appended", nextOffset, closed: true };
    }

    const nextOffset = await storage.append(processed, options.seq);
    if (producerValidation && producerValidation.kind === "accepted") {
      await storage.setProducerState(
        options.producer!.producerId,
        producerValidation.proposedState
      );
      return {
        status: "appended",
        nextOffset,
        producerEpoch: producerValidation.proposedState.epoch,
        producerSeq: producerValidation.proposedState.lastSeq,
      };
    }

    return { status: "appended", nextOffset };
  }

  private validateProducer(
    state: ProducerState | undefined,
    epoch: number,
    seq: number
  ): ProducerValidation {
    if (!state) {
      // First time we see this producer. Treat as a fresh epoch session.
      if (seq !== 0) {
        return { kind: "gap", expectedSeq: 0, receivedSeq: seq };
      }
      return { kind: "accepted", proposedState: { epoch, lastSeq: 0 } };
    }

    if (epoch < state.epoch) {
      return { kind: "stale-epoch", currentEpoch: state.epoch };
    }

    if (epoch > state.epoch) {
      if (seq !== 0) {
        return { kind: "invalid-epoch-seq" };
      }
      return { kind: "accepted", proposedState: { epoch, lastSeq: 0 } };
    }

    // Same epoch
    if (seq <= state.lastSeq) {
      return { kind: "duplicate", lastSeq: state.lastSeq, epoch: state.epoch };
    }
    if (seq === state.lastSeq + 1) {
      return {
        kind: "accepted",
        proposedState: { epoch, lastSeq: seq },
      };
    }
    return {
      kind: "gap",
      expectedSeq: state.lastSeq + 1,
      receivedSeq: seq,
    };
  }

  async read(streamId: string, options: ReadOptions): Promise<ReadResult> {
    const storage = this.getStorage(streamId);
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return {
        status: "not-found",
        messages: [],
        nextOffset: "",
        upToDate: false,
      };
    }

    if (metadata.softDeleted) {
      return {
        status: "gone",
        messages: [],
        nextOffset: "",
        upToDate: false,
      };
    }

    const requestedOffset = this.normalizeOffset(options.offset);
    const tail = await storage.getCurrentOffset();
    const messages = await this.readChain(
      streamId,
      metadata,
      requestedOffset,
      undefined
    );

    const lastOffset =
      messages.length > 0 ? messages[messages.length - 1]!.offset : tail;
    const nextOffset = lastOffset > tail ? lastOffset : tail;
    const upToDate = nextOffset === tail;
    const isClosed = metadata.closed === true;

    return {
      status: "ok",
      messages,
      nextOffset,
      upToDate,
      // Only signal closed when caller has reached the tail. Partial reads
      // (more data exists between response and final offset) MUST NOT carry
      // Stream-Closed.
      closed: isClosed && upToDate,
    };
  }

  async readLive(
    streamId: string,
    options: ReadLiveOptions
  ): Promise<ReadLiveResult> {
    const storage = this.getStorage(streamId);
    const metadata = await storage.getMetadata();

    if (!metadata) {
      return {
        status: "not-found",
        messages: [],
        nextOffset: "",
        upToDate: false,
        cursor: "",
      };
    }

    if (metadata.softDeleted) {
      return {
        status: "gone",
        messages: [],
        nextOffset: "",
        upToDate: false,
        cursor: "",
      };
    }

    const isClosed = metadata.closed === true;

    // Closed stream: short-circuit to avoid waiting. Include inherited fork data
    // if the caller has not caught up to the fork boundary.
    if (isClosed) {
      const messages = await this.readChain(
        streamId,
        metadata,
        options.offset,
        undefined
      );
      const tail = await storage.getCurrentOffset();
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : tail;
      const nextOffset = lastOffset > tail ? lastOffset : tail;
      const upToDate = nextOffset === tail;
      return {
        status: messages.length > 0 ? "ok" : "timeout",
        messages,
        nextOffset,
        upToDate: true,
        cursor: this.generateCursor(options.cursor),
        closed: upToDate,
      };
    }

    // For forks: if the requested offset is in the inherited range, serve the
    // inherited data immediately without long-polling. The wait only applies
    // when the client has caught up to the fork's own tail.
    const forkOffset = metadata.forkOffset;
    if (
      metadata.forkedFrom &&
      forkOffset !== undefined &&
      options.offset < forkOffset
    ) {
      const messages = await this.readChain(
        streamId,
        metadata,
        options.offset,
        undefined
      );
      const tail = await storage.getCurrentOffset();
      const lastOffset =
        messages.length > 0 ? messages[messages.length - 1]!.offset : tail;
      return {
        status: "ok",
        messages,
        nextOffset: lastOffset > tail ? lastOffset : tail,
        upToDate: true,
        cursor: this.generateCursor(options.cursor),
      };
    }

    const { messages, nextOffset, timedOut } = await storage.readLive(
      options.offset,
      options.signal
    );

    // The stream may have closed while we were waiting.
    const finalMeta = await storage.getMetadata();
    const finalClosed = finalMeta?.closed === true;
    const reachedTail = nextOffset === (await storage.getCurrentOffset());

    return {
      status: timedOut ? "timeout" : "ok",
      messages,
      nextOffset,
      upToDate: true,
      cursor: this.generateCursor(options.cursor),
      closed: finalClosed && reachedTail,
    };
  }

  async metadata(streamId: string): Promise<MetadataResult> {
    const storage = this.getStorage(streamId);
    const meta = await storage.getMetadata();
    if (!meta) return { status: "not-found" };

    if (meta.softDeleted) {
      return { status: "gone" };
    }

    return {
      status: "ok",
      contentType: meta.contentType,
      nextOffset: await storage.getCurrentOffset(),
      ttlSeconds: meta.ttlSeconds,
      expiresAt: meta.expiresAt,
      closed: meta.closed === true,
    };
  }

  async delete(streamId: string): Promise<DeleteResult> {
    const storage = this.getStorage(streamId);
    const meta = await storage.getMetadata();
    if (!meta) return { status: "not-found" };

    // Soft-deleted streams reject all direct operations with 410 Gone (section 4.2).
    if (meta.softDeleted) {
      return { status: "gone" };
    }

    if ((meta.refCount ?? 0) > 0) {
      // Active forks reference this stream — soft-delete instead of purging.
      await storage.setSoftDeleted(true);
      return { status: "ok" };
    }

    // Full delete: cascade refcount decrement up the chain.
    await this.purgeWithCascade(streamId, meta);
    return { status: "ok" };
  }

  // === Private helpers ===

  private async purgeWithCascade(
    streamId: string,
    meta: StreamMetadata
  ): Promise<void> {
    const storage = this.getStorage(streamId);
    const forkedFrom = meta.forkedFrom;
    await storage.deleteAll();

    if (!forkedFrom) return;

    const parentStorage = this.getStorage(forkedFrom);
    const parentMeta = await parentStorage.getMetadata();
    if (!parentMeta) return;

    const newRefCount = Math.max(0, (parentMeta.refCount ?? 0) - 1);
    await parentStorage.setRefCount(newRefCount);

    if (newRefCount === 0 && parentMeta.softDeleted) {
      await this.purgeWithCascade(forkedFrom, parentMeta);
    }
  }

  private async readChain(
    streamId: string,
    metadata: StreamMetadata,
    afterOffset: string | undefined,
    capOffset: string | undefined,
    touchOwnTtl = true
  ): Promise<StoredMessage[]> {
    const storage = this.getStorage(streamId);
    const out: StoredMessage[] = [];

    if (metadata.forkedFrom && metadata.forkOffset !== undefined) {
      const myForkOffset = metadata.forkOffset;
      // Need inherited data if either:
      //   - no afterOffset (read from beginning), OR
      //   - afterOffset is below this stream's forkOffset
      if (afterOffset === undefined || afterOffset < myForkOffset) {
        const upstreamCap =
          capOffset && capOffset < myForkOffset ? capOffset : myForkOffset;
        const sourceMeta = await this.getStorage(
          metadata.forkedFrom
        ).getMetadata();
        if (sourceMeta) {
          const inherited = await this.readChain(
            metadata.forkedFrom,
            sourceMeta,
            afterOffset,
            upstreamCap,
            false
          );
          out.push(...inherited);
        }
      }
    }

    // Add this stream's own messages after `afterOffset`, capped at `capOffset`.
    const ownStart =
      metadata.forkOffset !== undefined &&
      (afterOffset === undefined || afterOffset < metadata.forkOffset)
        ? metadata.forkOffset
        : afterOffset;
    const r = touchOwnTtl || !storage.readNoTouch
      ? await storage.read(ownStart)
      : await storage.readNoTouch(ownStart);
    for (const msg of r.messages) {
      if (capOffset !== undefined && msg.offset > capOffset) break;
      out.push(msg);
    }

    return out;
  }

  private resolveForkExpiry(
    opts: CreateOptions,
    sourceMeta: StreamMetadata
  ): { ttlSeconds?: number; expiresAt?: string } {
    if (opts.ttlSeconds !== undefined) {
      return { ttlSeconds: opts.ttlSeconds };
    }
    if (opts.expiresAt) {
      return { expiresAt: opts.expiresAt };
    }
    if (sourceMeta.ttlSeconds !== undefined) {
      return { ttlSeconds: sourceMeta.ttlSeconds };
    }
    if (sourceMeta.expiresAt) {
      return { expiresAt: sourceMeta.expiresAt };
    }
    return {};
  }

  private processData(data: Uint8Array, contentType: string): Uint8Array[] {
    if (!contentType.toLowerCase().startsWith("application/json")) {
      return [data];
    }

    const text = new TextDecoder().decode(data);
    const parsed = JSON.parse(text);

    if (Array.isArray(parsed)) {
      if (parsed.length === 0) {
        return [];
      }
      return parsed.map((item) =>
        new TextEncoder().encode(JSON.stringify(item))
      );
    }

    return [new TextEncoder().encode(JSON.stringify(parsed))];
  }

  private contentTypeMatches(expected: string, actual: string): boolean {
    const normalize = (ct: string) => ct.toLowerCase().split(";")[0]!.trim();
    return normalize(expected) === normalize(actual);
  }

  private configMatches(
    existing: StreamMetadata,
    options: CreateOptions
  ): boolean {
    const contentType =
      options.contentType ?? existing.contentType ?? "application/octet-stream";
    if (!this.contentTypeMatches(existing.contentType, contentType)) {
      return false;
    }

    // Idempotent fork creation: forkedFrom must match. forkOffset only
    // compared when explicitly supplied (when omitted, server defaulted).
    if ((options.forkedFrom ?? undefined) !== existing.forkedFrom) {
      return false;
    }
    if (
      options.forkOffset !== undefined &&
      options.forkOffset !== existing.forkOffset
    ) {
      return false;
    }

    if (existing.ttlSeconds !== options.ttlSeconds) {
      // For forks, omitted TTL/Expires inherits — accept inheritance match.
      if (
        options.forkedFrom &&
        options.ttlSeconds === undefined &&
        options.expiresAt === undefined
      ) {
        // skip strict TTL check on idempotent fork PUT with no expiry overrides
      } else {
        return false;
      }
    }

    if (existing.expiresAt !== options.expiresAt) {
      if (
        options.forkedFrom &&
        options.ttlSeconds === undefined &&
        options.expiresAt === undefined
      ) {
        // accept inheritance
      } else {
        return false;
      }
    }

    // Closure status is part of the configuration: PUT must match.
    const existingClosed = existing.closed === true;
    const wantClosed = options.closed === true;
    if (existingClosed !== wantClosed) {
      return false;
    }

    return true;
  }

  private normalizeOffset(offset?: string): string | undefined {
    if (!offset || offset === "-1") return undefined;
    return offset;
  }

  private generateCursor(previous?: string): string {
    const now = Date.now();
    const currentInterval = Math.floor(
      (now - CURSOR_EPOCH) / CURSOR_INTERVAL_MS
    );

    if (!previous) {
      return String(currentInterval);
    }

    const previousInterval = parseInt(previous, 10);
    if (previousInterval < currentInterval) {
      return String(currentInterval);
    }

    const jitterIntervals = Math.max(1, Math.floor(Math.random() * 180));
    return String(previousInterval + jitterIntervals);
  }
}

/**
 * Fork-side orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. source lazy expiry via injected `expireIfNeeded(sourcePath)`
 *   2. source load and lifecycle gates:
 *      - missing -> not-found shape with `errorMessage`
 *      - soft-deleted -> conflict/fork-source-soft-deleted shape with `errorMessage`
 *   3. fork-offset resolution and validation:
 *      - omitted -> defaults to `source.currentOffset`
 *      - invalid format -> bad-request
 *      - below ZERO_OFFSET or above source tail -> bad-request
 *   4. content-type resolution:
 *      - omitted/blank -> inherits `source.config.contentType`
 *      - non-blank incompatible -> conflict/fork-content-type
 *   5. expiry resolution: explicit ttlSeconds, explicit expiresAt, then
 *      inherit source ttlSeconds, then inherit source expiresAt; otherwise
 *      no expiry.
 *   6. record construction via injected `newRecord(streamId, contentType,
 *      optionsWithResolvedExpiry, { forkedFrom, forkOffset })`.
 *   7. transaction boundary: `create(record)` and, when it reports
 *      `created`, `incrementChildRefCount(sourcePath)` in the same
 *      `inTransaction(...)`.
 *   8. initial expiry scheduling via injected `scheduleExpiry(record)`.
 *   9. initial-data framing via `frameMessages` and append via injected
 *      `appendMessages(...)` only when framing produced at least one message.
 *  10. result shape: `{ status: "created", nextOffset, contentType }` with no
 *      `closed` field. Fork creation does not currently process
 *      `options.closed`.
 *
 * Used by `CreateStreamService` when `CreateOptions.forkedFrom` is present.
 * The lazy-expiry prelude for the create target is owned by
 * `StreamProtocol.create`; this service runs source-side expiry for
 * `forkedFrom` before reading the source record.
 *
 * Not exported from `packages/core/src/index.ts`; service-level tests import
 * directly from this module.
 */

import type { CreateOptions, CreateResult } from "../../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../../types/storage.ts";
import { ZERO_OFFSET, compareOffsets, isValidOffset } from "./offset-generator.ts";
import { contentTypeMatches } from "./content-type-matcher.ts";
import { frameMessages } from "./message-framer.ts";

export interface ForkDescriptor {
  forkedFrom: StreamId;
  forkOffset: string;
}

export interface ForkServiceMutators {
  expireIfNeeded(streamId: StreamId): Promise<void>;
  newRecord(
    streamId: StreamId,
    contentType: string,
    options: CreateOptions,
    fork: ForkDescriptor,
  ): StreamRecord;
  scheduleExpiry(record: StreamRecord): Promise<void>;
  appendMessages(streamId: StreamId, record: StreamRecord, data: Uint8Array[]): Promise<string>;
}

export function resolveForkExpiry(
  opts: CreateOptions,
  source: StreamRecord,
): { ttlSeconds?: number; expiresAt?: string } {
  if (opts.ttlSeconds !== undefined) return { ttlSeconds: opts.ttlSeconds };
  if (opts.expiresAt) return { expiresAt: opts.expiresAt };
  if (source.config.ttlSeconds !== undefined) return { ttlSeconds: source.config.ttlSeconds };
  if (source.config.expiresAt) return { expiresAt: source.config.expiresAt };
  return {};
}

export class ForkService {
  constructor(
    private store: StreamStoreAdapter,
    private mutators: ForkServiceMutators,
  ) {}

  async execute(streamId: StreamId, options: CreateOptions): Promise<CreateResult> {
    const sourcePath = options.forkedFrom!;
    await this.mutators.expireIfNeeded(sourcePath);
    const source = await this.store.get(sourcePath);
    if (!source)
      return {
        status: "not-found",
        nextOffset: "",
        contentType: "",
        errorMessage: `Source stream not found: ${sourcePath}`,
      };
    if (source.lifecycle.softDeleted) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-source-soft-deleted",
        errorMessage: `Source stream is soft-deleted: ${sourcePath}`,
      };
    }

    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValidOffset(forkOffset)) {
      return {
        status: "bad-request",
        nextOffset: "",
        contentType: "",
        errorMessage: "Invalid Stream-Fork-Offset format",
      };
    }
    if (
      compareOffsets(forkOffset, ZERO_OFFSET) < 0 ||
      compareOffsets(forkOffset, source.currentOffset) > 0
    ) {
      return {
        status: "bad-request",
        nextOffset: "",
        contentType: "",
        errorMessage: "Stream-Fork-Offset exceeds source tail",
      };
    }

    let contentType = options.contentType;
    if (!contentType || contentType.trim() === "") contentType = source.config.contentType;
    else if (!contentTypeMatches(contentType, source.config.contentType)) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "fork-content-type",
        errorMessage: "Fork Content-Type does not match source",
      };
    }

    const expiry = resolveForkExpiry(options, source);
    const record = this.mutators.newRecord(
      streamId,
      contentType,
      { ...options, ...expiry },
      { forkedFrom: sourcePath, forkOffset },
    );
    const initialMessages = options.initialData
      ? frameMessages(options.initialData, contentType)
      : [];

    const createResult = await this.inTransaction(async (tx) => {
      const result = await tx.create(record);
      if (result.status === "created") await tx.incrementChildRefCount(sourcePath);
      return result;
    });
    if (createResult.status === "exists") {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "config-mismatch",
        errorMessage: `Stream already exists: ${streamId}`,
      };
    }
    await this.mutators.scheduleExpiry(record);
    let final = record.currentOffset;
    if (initialMessages.length > 0)
      final = await this.mutators.appendMessages(streamId, record, initialMessages);
    return { status: "created", nextOffset: final, contentType };
  }

  private async inTransaction<T>(fn: (tx: StreamStoreAdapter) => Promise<T>): Promise<T> {
    return this.store.transaction ? this.store.transaction(fn) : fn(this.store);
  }
}

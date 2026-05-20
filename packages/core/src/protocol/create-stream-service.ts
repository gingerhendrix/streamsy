/**
 * Create-side orchestration for the durable streams protocol.
 *
 * Responsibilities:
 *
 *   1. existing record load (lifecycle gate / idempotency)
 *      - soft-deleted -> conflict/soft-deleted
 *      - config mismatch -> conflict/config-mismatch
 *      - compatible -> exists with current offset/content type/closed
 *   2. fork delegation when no existing record and `options.forkedFrom` is set
 *   3. non-fork creation:
 *      - default `contentType` to "application/octet-stream"
 *      - frame `initialData` via `frameMessages`
 *      - construct the record via injected `newRecord`
 *      - persist via `store.create`
 *      - schedule initial expiry via `ExpiryPolicy`
 *      - append framed initial messages when present
 *      - close the record when `options.closed === true`
 *      - shape `CreateResult` with `status: "created"`
 *
 * Used by `StreamProtocol.create`. The lazy-expiry prelude
 * (`expireIfNeeded`) for the create target stays in `StreamProtocol.create`.
 * Fork creation, soft-delete/purge GC, and the shared mutators (`newRecord`,
 * `appendStoredMessages`, `closeRecord`) are injected callbacks used by both
 * create/fork and append flows.
 *
 * Not exported from `packages/core/src/index.ts`; service-level tests import
 * directly from this module.
 */

import type { CreateOptions, CreateResult } from "../types/protocol.ts";
import type { StreamId, StreamRecord, StreamStoreAdapter } from "../types/storage.ts";
import { configMatches } from "./helpers/create-config-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";

export interface CreateStreamMutators {
  newRecord(streamId: StreamId, contentType: string, options: CreateOptions): StreamRecord;
  scheduleExpiry(record: StreamRecord): Promise<void>;
  appendMessages(
    streamId: StreamId,
    record: StreamRecord,
    data: Uint8Array[],
  ): Promise<string>;
  closeRecord(
    streamId: StreamId,
    record: StreamRecord,
    data: Uint8Array[],
  ): Promise<string>;
  createFork(streamId: StreamId, options: CreateOptions): Promise<CreateResult>;
}

export class CreateStreamService {
  constructor(
    private store: StreamStoreAdapter,
    private mutators: CreateStreamMutators,
  ) {}

  async execute(streamId: StreamId, options: CreateOptions): Promise<CreateResult> {
    const existing = await this.store.get(streamId);

    if (existing) {
      if (existing.lifecycle.softDeleted) {
        return {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "soft-deleted",
        };
      }
      if (!configMatches(existing, options)) {
        return {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "config-mismatch",
        };
      }
      return this.resultForExisting(existing, options);
    }

    if (options.forkedFrom) return this.mutators.createFork(streamId, options);

    const contentType = options.contentType ?? "application/octet-stream";
    const initialMessages = options.initialData
      ? frameMessages(options.initialData, contentType)
      : [];
    const wantClosed = options.closed === true;

    const record = this.mutators.newRecord(streamId, contentType, options);
    const createResult = await this.store.create(record);
    if (createResult.status === "exists") return this.resultForExisting(createResult.record, options);
    await this.mutators.scheduleExpiry(record);

    let final = record.currentOffset;
    if (initialMessages.length > 0) {
      final = await this.mutators.appendMessages(streamId, record, initialMessages);
    }
    if (wantClosed) {
      const latest = (await this.store.get(streamId)) ?? record;
      final = await this.mutators.closeRecord(streamId, latest, []);
    }

    return { status: "created", nextOffset: final, contentType, closed: wantClosed };
  }

  private resultForExisting(existing: StreamRecord, options: CreateOptions): CreateResult {
    if (existing.lifecycle.softDeleted) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "soft-deleted",
      };
    }
    if (!configMatches(existing, options)) {
      return {
        status: "conflict",
        nextOffset: "",
        contentType: "",
        conflictReason: "config-mismatch",
      };
    }
    return {
      status: "exists",
      nextOffset: existing.currentOffset,
      contentType: existing.config.contentType,
      closed: existing.lifecycle.closed === true,
    };
  }
}

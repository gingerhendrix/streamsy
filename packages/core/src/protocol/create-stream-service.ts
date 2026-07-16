/** Create-side plan building for one storage-bound stream snapshot. */

import type { CreatePlan } from "../types/storage-adapter.ts";
import type { CreateOptions, CreateOutcome } from "../types/protocol.ts";
import type { Clock, StoredMessage, StreamRecord } from "../types/storage.ts";
import type { AfterCommitEffects } from "./helpers/after-commit-effects.ts";
import { configMatches } from "./helpers/create-config-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";
import { allocate as allocateOffsets, type OffsetGenerator } from "./helpers/offset-generator.ts";

export type CreateDecision =
  | { kind: "terminal"; result: CreateOutcome }
  | {
      kind: "create";
      plan: CreatePlan;
      /** Core-side effects run by core after the adapter commit; never on the seam. */
      afterCommit: AfterCommitEffects;
      toResult: (record: StreamRecord) => CreateOutcome;
    }
  | { kind: "fork" };

export interface CreateStreamServiceDeps {
  clock: Clock;
  newRecord(contentType: string, options: CreateOptions): StreamRecord;
  offsets: OffsetGenerator;
}

export class CreateStreamService {
  constructor(private deps: CreateStreamServiceDeps) {}

  plan(record: StreamRecord | null, options: CreateOptions): CreateDecision {
    if (record) return { kind: "terminal", result: this.resultForExisting(record, options) };
    if (options.forkedFrom) return { kind: "fork" };

    const contentType = options.contentType ?? "application/octet-stream";
    const initialMessages = this.initialMessages(
      options.initialData,
      contentType,
      this.deps.offsets.initialOffset,
      0,
    );
    const wantClosed = options.closed === true;
    const newRecord = this.deps.newRecord(contentType, options);
    const finalRecord = this.withInitialTail(newRecord, initialMessages, wantClosed);
    // `finalRecord` is the single source of truth — a created-closed stream is
    // pre-folded into `record.lifecycle` (no separate close-after step).
    const plan: CreatePlan = {
      record: finalRecord,
      initialMessages,
    };

    return {
      kind: "create",
      plan,
      afterCommit:
        finalRecord.lifecycle.expiresAtMs !== undefined
          ? { scheduleExpiryAt: finalRecord.lifecycle.expiresAtMs }
          : {},
      toResult: (created) => ({
        status: "created",
        nextOffset: created.currentOffset,
        contentType,
        closed: wantClosed,
      }),
    };
  }

  resultForExisting(existing: StreamRecord, options: CreateOptions): CreateOutcome {
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

  private initialMessages(
    initialData: Uint8Array | undefined,
    contentType: string,
    previousOffset: string,
    startCounter: number,
  ): StoredMessage[] {
    const framed = initialData ? frameMessages(initialData, contentType) : [];
    const allocation = allocateOffsets(
      this.deps.offsets,
      previousOffset,
      startCounter,
      framed.length,
    );
    const now = this.deps.clock.now();
    return framed.map((data, i) => ({
      data,
      offset: allocation.offsets[i]!,
      timestamp: now,
    }));
  }

  private withInitialTail(
    record: StreamRecord,
    initialMessages: StoredMessage[],
    closed: boolean,
  ): StreamRecord {
    const lastMessage = initialMessages[initialMessages.length - 1];
    return {
      ...record,
      currentOffset: lastMessage?.offset ?? record.currentOffset,
      counter: record.counter + initialMessages.length,
      lifecycle: {
        ...record.lifecycle,
        ...(closed ? { closed: true, closedAt: this.deps.clock.now() } : {}),
      },
    };
  }
}

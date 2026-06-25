/** Create-side plan building for one storage-bound stream snapshot. */

import type { CreatePlan } from "../types/factory.ts";
import type { CreateOptions, CreateOutcome } from "../types/protocol.ts";
import type { Clock, StoredMessage, StreamRecord } from "../types/storage.ts";
import { configMatches } from "./helpers/create-config-matcher.ts";
import { frameMessages } from "./helpers/message-framer.ts";
import { allocate as allocateOffsets } from "./helpers/offset-generator.ts";

export type CreateDecision =
  | { kind: "terminal"; result: CreateOutcome }
  | { kind: "create"; plan: CreatePlan; toResult: (record: StreamRecord) => CreateOutcome }
  | { kind: "fork" };

export interface CreateStreamServiceDeps {
  clock: Clock;
  newRecord(contentType: string, options: CreateOptions): StreamRecord;
}

export class CreateStreamService {
  constructor(private deps: CreateStreamServiceDeps) {}

  plan(record: StreamRecord | null, options: CreateOptions): CreateDecision {
    if (record) return { kind: "terminal", result: this.resultForExisting(record, options) };
    if (options.forkedFrom) return { kind: "fork" };

    const contentType = options.contentType ?? "application/octet-stream";
    const initialMessages = this.initialMessages(options.initialData, contentType, 0);
    const wantClosed = options.closed === true;
    const newRecord = this.deps.newRecord(contentType, options);
    const finalRecord = this.withInitialTail(newRecord, initialMessages, wantClosed);
    const plan: CreatePlan = {
      record: finalRecord,
      initialMessages,
      closeAfter: wantClosed,
      afterCommit: {
        ...(finalRecord.lifecycle.expiresAtMs !== undefined
          ? { scheduleExpiryAt: finalRecord.lifecycle.expiresAtMs }
          : {}),
        ...(wantClosed
          ? { notify: "closed" as const }
          : initialMessages.length > 0
            ? { notify: "message" as const }
            : {}),
      },
    };

    return {
      kind: "create",
      plan,
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
    startCounter: number,
  ): StoredMessage[] {
    const framed = initialData ? frameMessages(initialData, contentType) : [];
    const allocation = allocateOffsets(startCounter, framed.length);
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
    const allocation = allocateOffsets(record.counter, initialMessages.length);
    return {
      ...record,
      currentOffset: allocation.nextOffset,
      counter: allocation.endCounter,
      lifecycle: {
        ...record.lifecycle,
        ...(closed ? { closed: true, closedAt: this.deps.clock.now() } : {}),
      },
    };
  }
}

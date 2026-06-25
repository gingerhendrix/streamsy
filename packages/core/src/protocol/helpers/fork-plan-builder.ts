/** Fork-side validation and plan building. */

import type { ForkPlan } from "../../types/factory.ts";
import type { CreateOptions, CreateOutcome } from "../../types/protocol.ts";
import type { Clock, StoredMessage, StreamId, StreamRecord } from "../../types/storage.ts";
import { contentTypeMatches } from "./content-type-matcher.ts";
import { frameMessages } from "./message-framer.ts";
import {
  allocate as allocateOffsets,
  compareOffsets,
  isValidOffset,
  ZERO_OFFSET,
} from "./offset-generator.ts";

export interface ForkDescriptor {
  forkedFrom: StreamId;
  forkOffset: string;
}

export type ForkBuildResult =
  | { kind: "terminal"; result: CreateOutcome }
  | { kind: "fork"; plan: ForkPlan; toResult: (record: StreamRecord) => CreateOutcome };

export interface ForkPlanBuilderDeps {
  clock: Clock;
  newRecord(
    streamId: StreamId,
    contentType: string,
    options: CreateOptions,
    fork: ForkDescriptor,
  ): StreamRecord;
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

export class ForkPlanBuilder {
  constructor(private deps: ForkPlanBuilderDeps) {}

  build(
    targetId: StreamId,
    sourcePath: StreamId,
    source: StreamRecord | null,
    options: CreateOptions,
  ): ForkBuildResult {
    if (!source)
      return {
        kind: "terminal",
        result: {
          status: "not-found",
          nextOffset: "",
          contentType: "",
          errorMessage: `Source stream not found: ${sourcePath}`,
        },
      };
    if (source.lifecycle.softDeleted) {
      return {
        kind: "terminal",
        result: {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "fork-source-soft-deleted",
          errorMessage: `Source stream is soft-deleted: ${sourcePath}`,
        },
      };
    }

    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValidOffset(forkOffset)) {
      return {
        kind: "terminal",
        result: {
          status: "bad-request",
          nextOffset: "",
          contentType: "",
          errorMessage: "Invalid Stream-Fork-Offset format",
        },
      };
    }
    if (
      compareOffsets(forkOffset, ZERO_OFFSET) < 0 ||
      compareOffsets(forkOffset, source.currentOffset) > 0
    ) {
      return {
        kind: "terminal",
        result: {
          status: "bad-request",
          nextOffset: "",
          contentType: "",
          errorMessage: "Stream-Fork-Offset exceeds source tail",
        },
      };
    }

    let contentType = options.contentType;
    if (!contentType || contentType.trim() === "") contentType = source.config.contentType;
    else if (!contentTypeMatches(contentType, source.config.contentType)) {
      return {
        kind: "terminal",
        result: {
          status: "conflict",
          nextOffset: "",
          contentType: "",
          conflictReason: "fork-content-type",
          errorMessage: "Fork Content-Type does not match source",
        },
      };
    }

    const expiry = resolveForkExpiry(options, source);
    const baseRecord = this.deps.newRecord(
      targetId,
      contentType,
      { ...options, ...expiry },
      {
        forkedFrom: sourcePath,
        forkOffset,
      },
    );
    const initialMessages = this.initialMessages(
      options.initialData,
      contentType,
      baseRecord.counter,
    );
    const child = this.withInitialTail(baseRecord, initialMessages);
    const plan: ForkPlan = {
      child,
      sourceId: sourcePath,
      initialMessages,
      precondition: { sourceLiveAtOffset: forkOffset },
      afterCommit: {
        ...(child.lifecycle.expiresAtMs !== undefined
          ? { scheduleExpiryAt: child.lifecycle.expiresAtMs }
          : {}),
        ...(initialMessages.length > 0 ? { notify: "message" as const } : {}),
      },
    };

    return {
      kind: "fork",
      plan,
      toResult: (record) => ({
        status: "created",
        nextOffset: record.currentOffset,
        contentType,
      }),
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

  private withInitialTail(record: StreamRecord, initialMessages: StoredMessage[]): StreamRecord {
    const allocation = allocateOffsets(record.counter, initialMessages.length);
    return {
      ...record,
      currentOffset: allocation.nextOffset,
      counter: allocation.endCounter,
    };
  }
}

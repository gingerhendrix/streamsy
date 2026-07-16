/** Fork-side validation and plan building. */

import type { ForkPlan } from "../../types/storage-adapter.ts";
import type { CreateOptions, CreateOutcome } from "../../types/protocol.ts";
import type { Clock, StoredMessage, StreamId, StreamRecord } from "../../types/storage.ts";
import type { AfterCommitEffects } from "./after-commit-effects.ts";
import { contentTypeMatches } from "./content-type-matcher.ts";
import { frameMessages } from "./message-framer.ts";
import {
  allocate as allocateOffsets,
  compareOffsets,
  isValidOffset,
  type OffsetGenerator,
} from "./offset-generator.ts";

export interface ForkDescriptor {
  forkedFrom: StreamId;
  forkOffset: string;
  forkSubOffset?: number;
}

export type ForkBuildResult =
  | { kind: "terminal"; result: CreateOutcome }
  | {
      kind: "fork";
      plan: ForkPlan;
      /** Core-side effects run by core after the adapter commit; never on the seam. */
      afterCommit: AfterCommitEffects;
      toResult: (record: StreamRecord) => CreateOutcome;
    };

export interface ForkPlanBuilderDeps {
  clock: Clock;
  offsets: OffsetGenerator;
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
    /**
     * Source messages strictly after `forkOffset`, in offset order. Required to
     * materialize a sub-offset prefix; the caller reads them through the fork
     * chain so chained forks compose. Omit when no sub-offset is requested.
     */
    sourceTail?: StoredMessage[],
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
    if (!isValidOffset(this.deps.offsets, forkOffset)) {
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
      compareOffsets(forkOffset, this.deps.offsets.initialOffset) < 0 ||
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

    const subOffset = options.forkSubOffset;
    const prefix = this.materializePrefix(subOffset, contentType, sourceTail);
    if (!prefix.ok) {
      return {
        kind: "terminal",
        result: {
          status: "bad-request",
          nextOffset: "",
          contentType: "",
          errorMessage: prefix.errorMessage,
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
        ...(subOffset !== undefined && subOffset > 0 ? { forkSubOffset: subOffset } : {}),
      },
    );
    const initialMessages = this.initialMessages(
      prefix.messages,
      options.initialData,
      contentType,
      baseRecord.currentOffset,
      baseRecord.counter,
    );
    const child = this.withInitialTail(baseRecord, initialMessages);
    const plan: ForkPlan = {
      child,
      sourceId: sourcePath,
      initialMessages,
      precondition: { sourceLiveAtOffset: forkOffset },
    };

    return {
      kind: "fork",
      plan,
      afterCommit:
        child.lifecycle.expiresAtMs !== undefined
          ? { scheduleExpiryAt: child.lifecycle.expiresAtMs }
          : {},
      toResult: (record) => ({
        status: "created",
        nextOffset: record.currentOffset,
        contentType,
      }),
    };
  }

  /**
   * Materialize the partial-message prefix addressed by `forkSubOffset` from the
   * source messages that follow `forkOffset`. `0`/absent yields no prefix. For
   * JSON the sub-offset counts whole flattened messages; for binary/text it
   * counts bytes within the single next source message. Overshoot or an empty
   * source is a 400.
   */
  private materializePrefix(
    subOffset: number | undefined,
    contentType: string,
    sourceTail: StoredMessage[] | undefined,
  ): { ok: true; messages: Uint8Array[] } | { ok: false; errorMessage: string } {
    if (subOffset === undefined || subOffset === 0) return { ok: true, messages: [] };
    const tail = sourceTail ?? [];
    if (contentType.toLowerCase().startsWith("application/json")) {
      if (subOffset > tail.length)
        return { ok: false, errorMessage: "Stream-Fork-Sub-Offset exceeds source message count" };
      return { ok: true, messages: tail.slice(0, subOffset).map((m) => m.data) };
    }
    const first = tail[0];
    if (!first)
      return { ok: false, errorMessage: "Stream-Fork-Sub-Offset has no source message to fork" };
    if (subOffset > first.data.byteLength)
      return { ok: false, errorMessage: "Stream-Fork-Sub-Offset exceeds source message length" };
    return { ok: true, messages: [first.data.subarray(0, subOffset)] };
  }

  private initialMessages(
    prefix: Uint8Array[],
    initialData: Uint8Array | undefined,
    contentType: string,
    previousOffset: string,
    startCounter: number,
  ): StoredMessage[] {
    const framed = [...prefix, ...(initialData ? frameMessages(initialData, contentType) : [])];
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

  private withInitialTail(record: StreamRecord, initialMessages: StoredMessage[]): StreamRecord {
    const lastMessage = initialMessages[initialMessages.length - 1];
    return {
      ...record,
      currentOffset: lastMessage?.offset ?? record.currentOffset,
      counter: record.counter + initialMessages.length,
    };
  }
}

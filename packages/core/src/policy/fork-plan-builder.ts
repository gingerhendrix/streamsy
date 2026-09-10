import { Predicate } from "effect";
/** Fork-side validation and plan building. */

import type { Operation } from "../storage/mutation.ts";
import type { CreateOptions } from "./options.ts";
import {
  ForkSourceNotFound,
  CreateConflict,
  InvalidForkRequest,
  type CreateError,
} from "../protocol/errors.ts";
import type { CreateResult } from "../protocol/results.ts";
import { Offset, type StoredMessage, type StreamId, type StreamRecord } from "../schema/index.ts";
interface Clock {
  now(): number;
}

import { contentTypeMatches } from "./content-type-matcher.ts";
import { frameMessages } from "./message-framer.ts";
import { next, compare as compareOffsets, isValid, ZERO_OFFSET } from "../offset/index.ts";

export interface ForkDescriptor {
  forkedFrom: StreamId;
  forkOffset: Offset;
  forkSubOffset?: number;
}

export interface ForkExpiryOptions {
  ttlSeconds?: number;
  expiresAt?: string;
}

export type ForkBuildResult =
  | { _tag: "Rejected"; error: CreateError }
  | {
      _tag: "Fork";
      plan: Extract<Operation, { _tag: "Create" }>;
      toResult: (record: StreamRecord) => CreateResult;
    };

export interface ForkPlanBuilderDeps {
  clock: Clock;
  newRecord(
    streamId: StreamId,
    contentType: string,
    options: CreateOptions,
    fork: ForkDescriptor,
  ): StreamRecord;
}

export function resolveForkExpiry(opts: CreateOptions, source: StreamRecord): ForkExpiryOptions {
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
        _tag: "Rejected",
        error: new ForkSourceNotFound({
          id: targetId,
          source: sourcePath,
          message: `Source stream not found: ${sourcePath}`,
        }),
      };
    if (source.lifecycle.softDeleted) {
      return {
        _tag: "Rejected",
        error: new CreateConflict({
          id: targetId,
          reason: "fork-source-soft-deleted",
          message: `Source stream is soft-deleted: ${sourcePath}`,
        }),
      };
    }

    const forkOffset = options.forkOffset ?? source.currentOffset;
    if (!isValid(forkOffset)) {
      return {
        _tag: "Rejected",
        error: new InvalidForkRequest({
          id: targetId,
          message: "Invalid Stream-Fork-Offset format",
        }),
      };
    }
    if (
      compareOffsets(forkOffset, ZERO_OFFSET) < 0 ||
      compareOffsets(forkOffset, source.currentOffset) > 0
    ) {
      return {
        _tag: "Rejected",
        error: new InvalidForkRequest({
          id: targetId,
          message: "Stream-Fork-Offset exceeds source tail",
        }),
      };
    }

    let contentType = options.contentType;
    if (!contentType || contentType.trim() === "") contentType = source.config.contentType;
    else if (!contentTypeMatches(contentType, source.config.contentType)) {
      return {
        _tag: "Rejected",
        error: new CreateConflict({
          id: targetId,
          reason: "fork-content-type",
          message: "Fork Content-Type does not match source",
        }),
      };
    }

    const subOffset = options.forkSubOffset;
    const prefix = this.materializePrefix(subOffset, contentType, sourceTail);
    if (Predicate.isTagged(prefix, "Invalid")) {
      return {
        _tag: "Rejected",
        error: new InvalidForkRequest({ id: targetId, message: prefix.errorMessage }),
      };
    }

    const expiry = resolveForkExpiry(options, source);
    const fork: ForkDescriptor = {
      forkedFrom: sourcePath,
      forkOffset: Offset.make(forkOffset),
    };
    if (subOffset !== undefined && subOffset > 0) fork.forkSubOffset = subOffset;
    const baseRecord = this.deps.newRecord(targetId, contentType, { ...options, ...expiry }, fork);
    const initialMessages = this.initialMessages(
      prefix.messages,
      options.initialData,
      contentType,
      baseRecord.currentOffset,
    );
    const child = this.withInitialTail(baseRecord, initialMessages);
    const plan: Extract<Operation, { _tag: "Create" }> = {
      _tag: "Create",
      record: child,
      initialMessages,
      forkSource: { id: sourcePath, liveAtOffset: Offset.make(forkOffset) },
    };

    return {
      _tag: "Fork",
      plan,
      toResult: (record) => ({
        _tag: "Created",
        nextOffset: record.currentOffset,
        contentType,
        closed: record.lifecycle.closed,
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
  ): { _tag: "Prefix"; messages: Uint8Array[] } | { _tag: "Invalid"; errorMessage: string } {
    if (subOffset === undefined || subOffset === 0) return { _tag: "Prefix", messages: [] };
    const tail = sourceTail ?? [];
    if (contentType.toLowerCase().startsWith("application/json")) {
      if (subOffset > tail.length)
        return {
          _tag: "Invalid",
          errorMessage: "Stream-Fork-Sub-Offset exceeds source message count",
        };
      return { _tag: "Prefix", messages: tail.slice(0, subOffset).map((m) => m.data) };
    }
    const first = tail[0];
    if (!first)
      return {
        _tag: "Invalid",
        errorMessage: "Stream-Fork-Sub-Offset has no source message to fork",
      };
    if (subOffset > first.data.byteLength)
      return {
        _tag: "Invalid",
        errorMessage: "Stream-Fork-Sub-Offset exceeds source message length",
      };
    return { _tag: "Prefix", messages: [first.data.subarray(0, subOffset)] };
  }

  private initialMessages(
    prefix: Uint8Array[],
    initialData: Uint8Array | undefined,
    contentType: string,
    previousOffset: Offset,
  ): StoredMessage[] {
    const framed = [...prefix, ...(initialData ? frameMessages(initialData, contentType) : [])];
    let offset = previousOffset;
    const now = this.deps.clock.now();
    return framed.map((data) => {
      offset = next(offset);
      return { data, offset, timestamp: now };
    });
  }

  private withInitialTail(record: StreamRecord, initialMessages: StoredMessage[]): StreamRecord {
    const lastMessage = initialMessages[initialMessages.length - 1];
    return {
      id: record.id,
      config: record.config,
      lifecycle: record.lifecycle,
      currentOffset: lastMessage?.offset ?? record.currentOffset,
    };
  }
}

import { Clock, Effect, Predicate, Option } from "effect";
import type { StorageFault } from "../fault.ts";
import type { Storage } from "../storage/storage.ts";
import { StreamId, Offset, type StreamConfig, type StreamRecord } from "../schema/index.ts";
import { ZERO_OFFSET, next, isValid } from "../offset/index.ts";
import { configMatches } from "../policy/create-config-matcher.ts";
import { computeExpiresAtMs } from "../policy/expiry-policy.ts";
import { ForkPlanBuilder, type ForkDescriptor } from "../policy/fork-plan-builder.ts";
import { frameMessages } from "../policy/message-framer.ts";
import type { CreateOptions } from "./options.ts";
import type { CreateOutcome } from "./outcomes.ts";
import { expireIfNeeded } from "./expiry.ts";

type WritableConfig = { -readonly [K in keyof StreamConfig]: StreamConfig[K] };

function newRecord(
  id: StreamId,
  contentType: string,
  options: CreateOptions,
  now: number,
  fork?: ForkDescriptor,
): StreamRecord {
  const at = computeExpiresAtMs(options, now);
  const config: WritableConfig = { contentType, createdAt: now };
  if (options.ttlSeconds !== undefined) config.ttlSeconds = options.ttlSeconds;
  if (options.expiresAt !== undefined) config.expiresAt = options.expiresAt;
  const lifecycle: {
    closed: boolean;
    softDeleted: boolean;
    closedAt?: number;
    expiresAtMs?: number;
  } & Partial<ForkDescriptor> = { closed: options.closed === true, softDeleted: false, ...fork };
  if (options.closed) lifecycle.closedAt = now;
  if (at !== undefined) lifecycle.expiresAtMs = at;
  return { id, currentOffset: fork?.forkOffset ?? ZERO_OFFSET, config, lifecycle };
}
function existingResult(record: StreamRecord, options: CreateOptions): CreateOutcome {
  if (record.lifecycle.softDeleted)
    return { status: "conflict", nextOffset: "", contentType: "", conflictReason: "soft-deleted" };
  if (!configMatches(record, options))
    return {
      status: "conflict",
      nextOffset: "",
      contentType: "",
      conflictReason: "config-mismatch",
    };
  return {
    status: "exists",
    nextOffset: record.currentOffset,
    contentType: record.config.contentType,
    closed: record.lifecycle.closed,
  };
}
export const create = Effect.fn("Protocol.create")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: CreateOptions = {},
): Effect.fn.Return<CreateOutcome, StorageFault> {
  // Capability refusal must not acquire records, read messages or attempt writes.
  if (options.forkedFrom !== undefined && storage.capabilities.fork === "none")
    return { status: "not-supported", feature: "fork" };
  const existing = yield* expireIfNeeded(storage, id);
  if (Option.isSome(existing)) return existingResult(existing.value, options);
  const now = yield* Clock.currentTimeMillis;
  let plan: Extract<import("../storage/mutation.ts").Operation, { _tag: "Create" }>;
  if (options.forkedFrom !== undefined) {
    const sourceId = StreamId.make(options.forkedFrom);
    const source = yield* expireIfNeeded(storage, sourceId);
    const after = options.forkOffset;
    const tail =
      options.forkSubOffset && after && isValid(after)
        ? [...(yield* storage.messages(sourceId, { after: Offset.make(after) }))]
        : undefined;
    const builder = new ForkPlanBuilder({
      clock: { now: () => now },
      newRecord: (target, type, opts, fork) => newRecord(target, type, opts, now, fork),
    });
    const built = builder.build(id, sourceId, Option.getOrNull(source), options, tail);
    if (Predicate.isTagged(built, "Terminal")) return built.result;
    plan = built.plan;
    if (storage.capabilities.fork === "copy") {
      const prefix = yield* storage.messages(sourceId, { until: plan.record.lifecycle.forkOffset });
      // The child keeps provenance for config idempotency; copy storage has no lineage edge.
      plan = {
        _tag: "Create",
        record: plan.record,
        initialMessages: [...prefix, ...plan.initialMessages],
      };
    }
  } else {
    const type = options.contentType ?? "application/octet-stream";
    const record = newRecord(id, type, options, now);
    let offset = record.currentOffset;
    const initialMessages = (
      options.initialData ? frameMessages(options.initialData, type) : []
    ).map((data) => ({ data, offset: (offset = next(offset)), timestamp: now }));
    plan = {
      _tag: "Create",
      record: {
        id: record.id,
        config: record.config,
        lifecycle: record.lifecycle,
        currentOffset: offset,
      },
      initialMessages,
    };
  }
  const result = yield* storage.mutate({ operations: [plan] }).pipe(Effect.uninterruptible);
  if (Predicate.isTagged(result, "Applied")) {
    const record = result.results[0].record;
    return {
      status: "created",
      nextOffset: record.currentOffset,
      contentType: record.config.contentType,
      closed: record.lifecycle.closed,
    };
  }
  if (result.reason === "exists" && Option.isSome(result.record))
    return existingResult(result.record.value, options);
  return {
    status: "not-found",
    nextOffset: "",
    contentType: "",
    errorMessage: "Source stream disappeared during fork",
  };
});

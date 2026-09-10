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
import { CreateConflict, ForkSourceNotFound, NotSupported, type CreateError } from "./errors.ts";
import type { CreateResult } from "./results.ts";
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
const existingResult = Effect.fn("Protocol.existingResult")(function* (
  record: StreamRecord,
  options: CreateOptions,
): Effect.fn.Return<CreateResult, CreateConflict> {
  if (record.lifecycle.softDeleted)
    return yield* new CreateConflict({
      id: record.id,
      reason: "soft-deleted",
      message: "Stream exists with different configuration",
    });
  if (!configMatches(record, options))
    return yield* new CreateConflict({
      id: record.id,
      reason: "config-mismatch",
      message: "Stream exists with different configuration",
    });
  return {
    _tag: "Exists",
    nextOffset: record.currentOffset,
    contentType: record.config.contentType,
    closed: record.lifecycle.closed,
  };
});
export const create = Effect.fn("Protocol.create")(function* (
  storage: typeof Storage.Service,
  id: StreamId,
  options: CreateOptions = {},
): Effect.fn.Return<CreateResult, CreateError | StorageFault> {
  // Capability refusal must not acquire records, read messages or attempt writes.
  if (options.forkedFrom !== undefined && storage.capabilities.fork === "none")
    return yield* new NotSupported({ id, feature: "fork" });
  const existing = yield* expireIfNeeded(storage, id);
  if (Option.isSome(existing)) return yield* existingResult(existing.value, options);
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
    if (Predicate.isTagged(built, "Rejected")) return yield* built.error;
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
  return yield* storage.mutate({ operations: [plan] }).pipe(
    Effect.uninterruptible,
    Effect.map((result): CreateResult => {
      const record = result.results[0].record;
      return {
        _tag: "Created",
        nextOffset: record.currentOffset,
        contentType: record.config.contentType,
        closed: record.lifecycle.closed,
      };
    }),
    Effect.catchTag("MutationRejected", (rejection): Effect.Effect<CreateResult, CreateError> => {
      if (rejection.reason === "exists" && Option.isSome(rejection.record))
        return existingResult(rejection.record.value, options);
      return Effect.fail(
        new ForkSourceNotFound({
          id,
          source: StreamId.make(options.forkedFrom ?? id),
          message: "Source stream disappeared during fork",
        }),
      );
    }),
  );
});

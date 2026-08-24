import type { JsonValue, StreamBatch } from "@streamsy/core";
import { Effect, Schema } from "effect";
import type { StreamBinding } from "../binding.ts";
import { sourceAck, streamIdentityEquals, type SourceAck } from "../causal.ts";
import {
  MalformedSourceBoundary,
  ProjectionPoison,
  type MeshOperationalError,
} from "../effect/errors.ts";
import { AppendStreams, ReadStreams, type EffectReadSession } from "../effect/streams.ts";
import {
  appendDerivedStateBatch,
  DerivedRecovery,
  type AppendDerivedStateResult,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import type { ProducerLane } from "./lane.ts";
import { CatchUpLimits as CatchUpLimitsSchema } from "./schemas.ts";

export interface CatchUpLimits {
  readonly maxItems: number;
  readonly maxPages: number;
  readonly maxBatches: number;
  readonly maxBytes: number;
}

export interface ProjectionBoundary {
  readonly source: SourceAck;
  readonly page: number;
  readonly bytes: number;
}

export interface CatchUpOptions<Input> {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  readonly decode: (batch: StreamBatch, boundary: ProjectionBoundary) => Iterable<Input>;
  readonly reduce: (items: readonly Input[], boundary: ProjectionBoundary) => Iterable<JsonValue>;
}

export interface CatchUpProgress {
  readonly checkpoint: RecoveredDerivedState;
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export type CatchUpResult =
  | ({ readonly status: "caught-up" } & CatchUpProgress)
  | ({ readonly status: "limit-reached"; readonly limit: keyof CatchUpLimits } & CatchUpProgress)
  | ({
      readonly status: "boundary-too-large";
      readonly limit: "maxItems" | "maxBytes";
      readonly source: SourceAck;
      readonly actual: number;
      readonly maximum: number;
    } & CatchUpProgress)
  | ({
      readonly status: "missing" | "gone";
      readonly stream: "source" | "target";
    } & Partial<CatchUpProgress>)
  | ({
      readonly status: "output-conflict";
      readonly reason: string;
      readonly offset?: string;
    } & CatchUpProgress)
  | (Extract<
      AppendDerivedStateResult,
      { readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }
    > &
      CatchUpProgress);

/**
 * Bounded recover → pull → pure step → commit workflow.
 *
 * Commits remain explicit and sequential. Interruption cancels the current read
 * session and is never translated into a routine result. Interruption during a
 * remote append therefore leaves durability unknown until the next recovery.
 */
export const catchUp = Effect.fn("catchUp")(<Input>(options: CatchUpOptions<Input>) =>
  Effect.gen(function* () {
    validateOptions(options);
    const recovery = yield* DerivedRecovery;
    const reads = yield* ReadStreams;

    const recovered = yield* recovery.recover(options.target, options.lane);
    if (recovered.status !== "ready") {
      return {
        status: recovered.status === "not-found" ? ("missing" as const) : ("gone" as const),
        stream: "target" as const,
      };
    }
    const initial: CatchUpProgress = {
      checkpoint: recovered,
      pages: 0,
      batches: 0,
      items: 0,
      bytes: 0,
    };
    const readOptions =
      recovered.sourceThrough === undefined
        ? { live: false as const }
        : { offset: recovered.sourceThrough, live: false as const };
    const opened = yield* reads.open(options.source, readOptions);
    if (opened.status !== "ok")
      return {
        status: opened.status === "not-found" ? ("missing" as const) : ("gone" as const),
        stream: "source" as const,
        ...initial,
      };

    return yield* pullBoundary(options, opened.session, initial);
  }).pipe(Effect.scoped),
);

const pullBoundary = <Input>(
  options: CatchUpOptions<Input>,
  session: EffectReadSession,
  progress: CatchUpProgress,
): Effect.Effect<CatchUpResult, MeshOperationalError, AppendStreams | DerivedRecovery> =>
  Effect.gen(function* () {
    const next = yield* session.next;
    if (next.done) {
      const ended = yield* session.done;
      if (ended.status === "cancelled") return yield* Effect.interrupt;
      return { status: "caught-up" as const, ...progress };
    }
    const batch = next.value;
    if (!hasSourcePayload(batch)) return yield* pullBoundary(options, session, progress);
    if (progress.pages >= options.limits.maxPages)
      return { status: "limit-reached" as const, limit: "maxPages" as const, ...progress };
    if (progress.batches >= options.limits.maxBatches)
      return { status: "limit-reached" as const, limit: "maxBatches" as const, ...progress };

    const ack = yield* Effect.try({
      try: () => sourceAck(options.source.identity, batch.offset),
      catch: (cause) => new MalformedSourceBoundary({ offset: batch.offset, cause }),
    });
    const bytes = encodedBatchBytes(batch);
    if (bytes > options.limits.maxBytes)
      return {
        status: "boundary-too-large" as const,
        limit: "maxBytes" as const,
        source: ack,
        actual: bytes,
        maximum: options.limits.maxBytes,
        ...progress,
      };
    if (progress.bytes + bytes > options.limits.maxBytes)
      return { status: "limit-reached" as const, limit: "maxBytes" as const, ...progress };
    const boundary: ProjectionBoundary = { source: ack, page: progress.pages + 1, bytes };

    const items = yield* Effect.try({
      try: () => Array.from(options.decode(batch, boundary)),
      catch: (cause) =>
        new ProjectionPoison({ phase: "decode", sourcePosition: ack.position, cause }),
    });
    if (items.length > options.limits.maxItems)
      return {
        status: "boundary-too-large" as const,
        limit: "maxItems" as const,
        source: ack,
        actual: items.length,
        maximum: options.limits.maxItems,
        ...progress,
      };
    if (progress.items + items.length > options.limits.maxItems)
      return { status: "limit-reached" as const, limit: "maxItems" as const, ...progress };
    const facts = yield* Effect.try({
      try: () => Array.from(options.reduce(items, boundary)),
      catch: (cause) =>
        new ProjectionPoison({ phase: "reduce", sourcePosition: ack.position, cause }),
    });

    const appended = yield* appendDerivedStateBatch({
      target: options.target,
      lane: options.lane,
      previous: progress.checkpoint,
      sourceThrough: ack.position,
      facts,
    });
    switch (appended.status) {
      case "not-found":
        return { status: "missing" as const, stream: "target" as const, ...progress };
      case "gone":
        return { status: "gone" as const, stream: "target" as const, ...progress };
      case "output-conflict":
      case "stale-epoch":
      case "producer-gap":
      case "invalid-epoch-seq":
        return { ...appended, ...progress };
    }
    return yield* pullBoundary(options, session, {
      checkpoint: appended.checkpoint,
      pages: progress.pages + 1,
      batches: progress.batches + 1,
      items: progress.items + items.length,
      bytes: progress.bytes + bytes,
    });
  });

function validateOptions<Input>(options: CatchUpOptions<Input>): void {
  if (!streamIdentityEquals(options.source.identity, options.lane.source))
    throw new TypeError("Source binding identity does not match the producer lane");
  if (!streamIdentityEquals(options.target.identity, options.lane.target))
    throw new TypeError("Target binding identity does not match the producer lane");
  Schema.decodeUnknownSync(CatchUpLimitsSchema)(options.limits);
}

function hasSourcePayload(batch: StreamBatch): boolean {
  if (batch.kind === "json") return batch.items.length > 0;
  if (batch.kind === "text") return batch.text.length > 0;
  return batch.data.byteLength > 0;
}

function encodedBatchBytes(batch: StreamBatch): number {
  if (batch.kind === "json")
    return new TextEncoder().encode(JSON.stringify(batch.items)).byteLength;
  if (batch.kind === "text") return new TextEncoder().encode(batch.text).byteLength;
  return batch.data.byteLength;
}

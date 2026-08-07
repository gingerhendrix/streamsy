import type { JsonValue, StreamBatch } from "@streamsy/core";
import { Effect } from "effect";
import type { StreamBinding } from "../binding.ts";
import { sourceAck, streamIdentityEquals } from "../causal.ts";
import {
  MalformedSourceBoundary,
  ProjectionPoison,
  StateRestorePoison,
  type MeshOperationalError,
} from "../effect/errors.ts";
import { AppendStreams, ReadStreams, type EffectReadSession } from "../effect/streams.ts";
import {
  appendDerivedStateBatch,
  DerivedRecovery,
  DerivedStateHistory,
  type AppendDerivedStateResult,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import type { ProducerLane } from "./lane.ts";
import type { CatchUpLimits, ProjectionBoundary } from "./projection.ts";

/** Versioned recovery law implemented by {@link catchUpState}. */
export const SINGLE_SOURCE_STATE_KIND = "single-source-state/v1";

export interface StateStepResult<State> {
  readonly state: State;
  readonly facts: readonly JsonValue[];
}

export interface CatchUpStateOptions<State, Input> {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  /** State before any durable target history is applied. */
  readonly initial: State;
  /** Fold complete durable target facts back into typed application state. */
  readonly restore: (initial: State, events: readonly JsonValue[]) => State;
  readonly decode: (batch: StreamBatch, boundary: ProjectionBoundary) => Iterable<Input>;
  /** One complete source delivery boundary becomes one target transaction. */
  readonly step: (
    state: State,
    input: readonly Input[],
    boundary: ProjectionBoundary,
  ) => StateStepResult<State>;
}

export interface CatchUpStateProgress<State> {
  readonly checkpoint: RecoveredDerivedState;
  readonly state: State;
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export type CatchUpStateResult<State> =
  | ({ readonly status: "caught-up" } & CatchUpStateProgress<State>)
  | ({
      readonly status: "limit-reached";
      readonly limit: keyof CatchUpLimits;
    } & CatchUpStateProgress<State>)
  | ({
      readonly status: "boundary-too-large";
      readonly limit: "maxItems" | "maxBytes";
      readonly actual: number;
      readonly maximum: number;
    } & CatchUpStateProgress<State>)
  | {
      readonly status: "missing" | "gone";
      readonly stream: "source" | "target";
    }
  | ({
      readonly status: "output-conflict";
      readonly reason: string;
      readonly offset?: string;
    } & CatchUpStateProgress<State>)
  | (Extract<
      AppendDerivedStateResult,
      { readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }
    > &
      CatchUpStateProgress<State>);

/**
 * Bounded `recover → restore → pull → pure step → commit` workflow for one
 * ordered source and one State target.
 *
 * Restoration scans complete durable target history, so the recovery cost is
 * O(history). Snapshots are deliberately deferred. Every accepted boundary
 * commits its application facts and exactly one lineage row in one append.
 * Interruption remains interruption and never becomes a routine result.
 */
export const catchUpState = Effect.fn("catchUpState")(
  <State, Input>(options: CatchUpStateOptions<State, Input>) =>
    Effect.gen(function* () {
      validateStateOptions(options);
      const history = yield* DerivedStateHistory;
      const reads = yield* ReadStreams;

      const recovered = yield* history.recoverHistory(options.target, options.lane);
      if (recovered.status !== "ready") {
        return {
          status: recovered.status === "not-found" ? ("missing" as const) : ("gone" as const),
          stream: "target" as const,
        };
      }
      const state = yield* Effect.try({
        try: () => options.restore(options.initial, recovered.facts),
        catch: (cause) =>
          new StateRestorePoison({ targetOffset: recovered.checkpoint.targetOffset, cause }),
      });
      const initial: CatchUpStateProgress<State> = {
        checkpoint: recovered.checkpoint,
        state,
        pages: 0,
        batches: 0,
        items: 0,
        bytes: 0,
      };
      const opened = yield* reads.open(options.source, {
        ...(recovered.checkpoint.sourceThrough === undefined
          ? {}
          : { offset: recovered.checkpoint.sourceThrough }),
        live: false,
      });
      if (opened.status !== "ok") {
        return {
          status: opened.status === "not-found" ? ("missing" as const) : ("gone" as const),
          stream: "source" as const,
        };
      }
      return yield* pullStateBoundary(options, opened.session, initial);
    }).pipe(Effect.scoped),
);

const pullStateBoundary = <State, Input>(
  options: CatchUpStateOptions<State, Input>,
  session: EffectReadSession,
  progress: CatchUpStateProgress<State>,
): Effect.Effect<
  CatchUpStateResult<State>,
  MeshOperationalError,
  AppendStreams | DerivedRecovery
> =>
  Effect.gen(function* () {
    const next = yield* session.next;
    if (next.done) {
      const ended = yield* session.done;
      if (ended.status === "cancelled") return yield* Effect.interrupt;
      return { status: "caught-up" as const, ...progress };
    }
    const batch = next.value;
    if (!hasSourcePayload(batch)) return yield* pullStateBoundary(options, session, progress);
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
        actual: items.length,
        maximum: options.limits.maxItems,
        ...progress,
      };
    if (progress.items + items.length > options.limits.maxItems)
      return { status: "limit-reached" as const, limit: "maxItems" as const, ...progress };

    const stepped = yield* Effect.try({
      try: () => options.step(progress.state, items, boundary),
      catch: (cause) =>
        new ProjectionPoison({ phase: "step", sourcePosition: ack.position, cause }),
    });

    const appended = yield* appendDerivedStateBatch({
      target: options.target,
      lane: options.lane,
      previous: progress.checkpoint,
      sourceThrough: ack.position,
      facts: stepped.facts,
    });
    switch (appended.status) {
      case "not-found":
        return { status: "missing" as const, stream: "target" as const };
      case "gone":
        return { status: "gone" as const, stream: "target" as const };
      case "output-conflict":
      case "stale-epoch":
      case "producer-gap":
      case "invalid-epoch-seq":
        return { ...appended, ...progress };
    }
    return yield* pullStateBoundary(options, session, {
      checkpoint: appended.checkpoint,
      state: stepped.state,
      pages: progress.pages + 1,
      batches: progress.batches + 1,
      items: progress.items + items.length,
      bytes: progress.bytes + bytes,
    });
  });

function validateStateOptions<State, Input>(options: CatchUpStateOptions<State, Input>): void {
  if (!streamIdentityEquals(options.source.identity, options.lane.source))
    throw new TypeError("Source binding identity does not match the producer lane");
  if (!streamIdentityEquals(options.target.identity, options.lane.target))
    throw new TypeError("Target binding identity does not match the producer lane");
  for (const [name, value] of Object.entries(options.limits)) {
    if (!Number.isSafeInteger(value) || value <= 0)
      throw new TypeError(`${name} must be a positive safe integer`);
  }
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

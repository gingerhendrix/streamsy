import { Effect, Option, Schema } from "effect";
import { Commit, type CommitApi } from "./commit.ts";
import { DeriveFault } from "./fault.ts";
import { sameIdentity, type Identity } from "./identity.ts";
import type { Checkpoint } from "./stores.ts";
import type { Source } from "./source.ts";
import type { Sink } from "./sink.ts";

export interface StepContext {
  readonly identity: Identity;
  readonly sourcePosition: string;
}
export interface Definition<I, O, S> {
  readonly id: string;
  readonly version: string | number;
  readonly source: Source<I>;
  readonly sink: Sink<O>;
  readonly initial: S;
  readonly stateSchema: Schema.Codec<S, string>;
  readonly step: (
    state: S,
    input: I,
    context: StepContext,
  ) => { readonly state: S; readonly outputs: ReadonlyArray<O> };
}
export interface Projection<I, O, S> extends Definition<I, O, S> {
  readonly identity: Identity;
}
export const make = <I, O, S>(definition: Definition<I, O, S>): Projection<I, O, S> => ({
  ...definition,
  identity: {
    id: definition.id,
    version: String(definition.version),
    generation: `v${definition.version}`,
    source: definition.source.identity,
    sink: definition.sink.identity,
  },
});
export interface Limits {
  readonly boundaries?: number;
  readonly items?: number;
  readonly bytes?: number;
}
export interface Progress {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly boundaries: number;
  readonly items: number;
  readonly bytes: number;
  readonly checkpoint: Checkpoint;
}
const invalidState = (message: string) => new DeriveFault({ reason: "invalid-state", message });
const restore = Effect.fn("Derive.restore")(function* <I, O, S>(
  projection: Projection<I, O, S>,
  owner: CommitApi,
) {
  const checkpoint = yield* owner.checkpoints.load(projection.id);
  const state = yield* owner.states.load(projection.id);
  if (Option.isNone(checkpoint) && Option.isNone(state))
    return {
      checkpoint: {
        identity: projection.identity,
        sourcePosition: projection.source.initialPosition,
        sinkPosition: projection.sink.initialPosition,
        revision: 0,
      } satisfies Checkpoint,
      state: projection.initial,
    };
  if (Option.isNone(checkpoint) || Option.isNone(state))
    return yield* invalidState("Checkpoint and state must both exist");
  if (!sameIdentity(checkpoint.value.identity, projection.identity))
    return yield* new DeriveFault({
      reason: "identity-mismatch",
      message: `Stored identity differs for ${projection.id}`,
    });
  if (checkpoint.value.revision !== state.value.revision)
    return yield* invalidState("Checkpoint and state revisions differ");
  return {
    checkpoint: checkpoint.value,
    state: yield* Schema.decodeEffect(projection.stateSchema)(state.value.encoded).pipe(
      Effect.mapError(() => invalidState("Cannot decode current state")),
    ),
  };
});
const validateLimits = (limits: Limits) =>
  Effect.suspend(() => {
    const values = [
      limits.boundaries ?? 100,
      limits.items ?? 1000,
      ...(limits.bytes === undefined ? [] : [limits.bytes]),
    ];
    return values.every((value) => Number.isSafeInteger(value) && value > 0)
      ? Effect.void
      : Effect.fail(
          new DeriveFault({
            reason: "invalid-limits",
            message: "Limits must be positive safe integers",
          }),
        );
  });

/** One directly testable restore → pull → pure step → fused commit pass. */
export const pass = Effect.fn("Derive.Projection.pass")(function* <I, O, S>(
  projection: Projection<I, O, S>,
  limits: Limits = {},
): Effect.fn.Return<Progress, DeriveFault, Commit> {
  yield* validateLimits(limits);
  const owner = yield* Commit;
  if (owner !== projection.sink.owner)
    return yield* new DeriveFault({
      reason: "unsupported-composition",
      message: "Sink and stores require the exact same Commit owner",
    });
  const restored = yield* restore(projection, owner);
  const before = restored.checkpoint;
  const empty = { boundaries: 0, items: 0, bytes: 0, checkpoint: before };
  const boundary = yield* projection.source.pull(before.sourcePosition, {
    items: limits.items ?? 1000,
    bytes: limits.bytes,
  });
  if (boundary.status === "history-unavailable")
    return yield* new DeriveFault({
      reason: "history-unavailable",
      message: `Required source history unavailable for ${projection.id}`,
    });
  if (boundary.status === "limit-reached") return { ...empty, status: "limit-reached" };
  if (
    boundary.items.length > (limits.items ?? 1000) ||
    (limits.bytes !== undefined && (boundary.bytes ?? 0) > limits.bytes)
  )
    return { ...empty, status: "limit-reached" };
  if (boundary.bytes !== undefined && (!Number.isSafeInteger(boundary.bytes) || boundary.bytes < 0))
    return yield* new DeriveFault({
      reason: "invalid-source",
      message: "Source byte count must be a nonnegative safe integer",
    });
  if (boundary.endPosition === before.sourcePosition) {
    if (boundary.items.length > 0 || !boundary.upToDate)
      return yield* new DeriveFault({
        reason: "invalid-source",
        message: "Source did not advance its boundary",
      });
    return { ...empty, status: boundary.closed ? "source-closed" : "caught-up" };
  }
  let state = restored.state;
  const outputs: Array<O> = [];
  for (const item of boundary.items) {
    const next = projection.step(state, item, {
      identity: projection.identity,
      sourcePosition: boundary.endPosition,
    });
    state = next.state;
    outputs.push(...next.outputs);
  }
  const encoded = yield* Schema.encodeEffect(projection.stateSchema)(state).pipe(
    Effect.mapError(() => invalidState("Cannot encode current state")),
  );
  if (!Number.isSafeInteger(before.revision + 1))
    return yield* invalidState("State revision exhausted");
  const checkpoint = yield* owner.withTransaction(
    Effect.gen(function* () {
      // Detect accidental overlapping calls before sink output. This is not distributed fencing.
      const current = yield* restore(projection, owner);
      if (
        current.checkpoint.revision !== before.revision ||
        current.checkpoint.sourcePosition !== before.sourcePosition ||
        current.checkpoint.sinkPosition !== before.sinkPosition
      )
        return yield* new DeriveFault({
          reason: "sink-conflict",
          message: "Projection checkpoint changed during the pass",
        });
      const sinkPosition =
        outputs.length === 0
          ? before.sinkPosition
          : yield* projection.sink.write(outputs, before.sinkPosition);
      const next = {
        identity: projection.identity,
        sourcePosition: boundary.endPosition,
        sinkPosition,
        revision: before.revision + 1,
      };
      yield* owner.states.save(projection.id, { revision: next.revision, encoded });
      yield* owner.checkpoints.save(projection.id, next);
      return next;
    }),
  );
  return {
    status:
      boundary.closed && boundary.upToDate
        ? "source-closed"
        : boundary.upToDate
          ? "caught-up"
          : "progress",
    boundaries: 1,
    items: boundary.items.length,
    bytes: boundary.bytes ?? 0,
    checkpoint,
  };
});

export const catchUp = Effect.fn("Derive.Projection.catchUp")(function* <I, O, S>(
  projection: Projection<I, O, S>,
  limits: Limits = {},
): Effect.fn.Return<Progress, DeriveFault, Commit> {
  yield* validateLimits(limits);
  const maxBoundaries = limits.boundaries ?? 100;
  const maxItems = limits.items ?? 1000;
  let boundaries = 0;
  let items = 0;
  let bytes = 0;
  let result: Progress;
  do {
    result = yield* pass(projection, {
      items: maxItems - items,
      bytes: limits.bytes === undefined ? undefined : limits.bytes - bytes,
    });
    boundaries += result.boundaries;
    items += result.items;
    bytes += result.bytes;
    if (result.status !== "progress") return { ...result, boundaries, items, bytes };
  } while (
    boundaries < maxBoundaries &&
    items < maxItems &&
    (limits.bytes === undefined || bytes < limits.bytes)
  );
  return { ...result, status: "limit-reached", boundaries, items, bytes };
});

/** Scope owns the fiber and every wait; joining exposes terminal close or a typed failure. */
export const follow = Effect.fn("Derive.Projection.follow")(function* <I, O, S>(
  projection: Projection<I, O, S>,
  options: Limits & { readonly repairIntervalMs?: number } = {},
) {
  yield* validateLimits(options);
  const interval = options.repairIntervalMs ?? 1000;
  if (!Number.isFinite(interval) || interval <= 0)
    return yield* new DeriveFault({
      reason: "invalid-limits",
      message: "repairIntervalMs must be positive",
    });
  const cycle = catchUp(projection, options).pipe(
    Effect.tap((result) =>
      result.status === "limit-reached" && result.boundaries === 0
        ? Effect.sleep(interval)
        : result.status === "caught-up"
          ? projection.source
              .wait(result.checkpoint.sourcePosition)
              .pipe(Effect.timeoutOption(interval), Effect.asVoid)
          : Effect.yieldNow,
    ),
  );
  return yield* cycle.pipe(
    Effect.repeat({ while: (result) => result.status !== "source-closed" }),
    Effect.forkScoped,
  );
});

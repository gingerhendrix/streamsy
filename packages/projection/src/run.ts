import { withPermit } from "./serialize.ts";
import type { Outputs } from "./outputs.ts";
import { CurrentOwner, isForeignOwner } from "@streamsy/core/internal/memory";
import { Effect } from "effect";
import type { InputServices, InputMap, Slice } from "./batch.ts";
import { Checkpoints, advance, rangesOf, restore, type CheckpointRecord } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import type { Atomic, Projection } from "./projection.ts";
import { readInputs, validateOptions, type RunOptions } from "./read.ts";
import { passStream } from "./stream-output.ts";
import { unitOf } from "./unit.ts";

export interface Progress {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly units: number;
  readonly items: number;
  readonly record: Pick<CheckpointRecord, "identity" | "inputs">;
}

/** One atomic unit: restore, read every input, then process and checkpoint in the owner transaction. */
export const passAtomic = Effect.fn("Projection.passAtomic")(function* <
  Inputs extends InputMap,
  E,
  R,
>(
  projection: Atomic<Inputs, E, R>,
  options: RunOptions = {},
): Effect.fn.Return<Progress, E | ProjectionFault, R | Checkpoints | InputServices<Inputs>> {
  yield* validateOptions(options);
  const owner = yield* Checkpoints;
  const before = yield* restore(projection, owner);
  const empty = { units: 0, items: 0, record: before.record };
  const read = yield* readInputs(projection.inputs, before.record.inputs);
  const slices = Object.values<Slice<unknown>>(read.slices);
  const closed = slices.every((slice) => slice.closed);
  if (read.items === 0) return { ...empty, status: closed ? "source-closed" : "caught-up" };
  const unit = unitOf(
    projection.id,
    projection.version,
    projection.generation,
    projection.params,
    rangesOf(read.slices),
  );
  const record = yield* owner.withTransaction(
    Effect.gen(function* () {
      const again = yield* owner.load(projection);
      if (again.token !== before.token)
        return yield* new ProjectionFault({
          phase: "checkpoint",
          reason: "token-conflict",
          message: `Record of ${projection.id} changed while the unit was read`,
        });
      yield* projection.process(read.slices, unit).pipe(
        Effect.provideService(CurrentOwner, owner.owner),
        Effect.catchDefect((defect) =>
          isForeignOwner(defect)
            ? Effect.fail(
                new ProjectionFault({
                  phase: "process",
                  reason: "unsupported-composition",
                  message: defect.message,
                }),
              )
            : Effect.die(defect),
        ),
      );
      const inputs = advance(before.record.inputs, read.slices);
      const { identity, adapters, pending } = before.record;
      const next: CheckpointRecord =
        pending === undefined
          ? { identity, inputs, adapters }
          : { identity, inputs, pending, adapters };
      yield* owner.save(projection, next, before.token);
      return next;
    }),
  );
  // A non-empty pass is progress; only an empty pass is authoritative for caught-up.
  return {
    status: "progress",
    units: 1,
    items: read.items,
    record,
  };
});

/** One unit under the strategy the declaration carries. */
export const pass = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: RunOptions = {},
): Effect.Effect<Progress, E | ProjectionFault, R | OR | Checkpoints | InputServices<Inputs>> =>
  projection._tag === "Atomic" ? passAtomic(projection, options) : passStream(projection, options);

/**
 * Internal kernel without a permit. Call only while holding the permit or simulating
 * a competing process in tests. Repeats `pass` until caught-up, every input is closed, or
 * the limit is reached. The trailing empty pass adds nothing to the totals.
 */
export const runKernel = Effect.fn("Projection.runKernel")(function* <
  Inputs extends InputMap,
  E,
  R,
  OR = never,
>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: RunOptions = {},
): Effect.fn.Return<Progress, E | ProjectionFault, R | OR | Checkpoints | InputServices<Inputs>> {
  yield* validateOptions(options);
  const cap = options.limit ?? Number.POSITIVE_INFINITY;
  let units = 0;
  let items = 0;
  let result: Progress;
  do {
    result = yield* pass(projection);
    units += result.units;
    items += result.items;
    if (result.status !== "progress") return { ...result, units, items };
  } while (units < cap);
  return { ...result, status: "limit-reached", units, items };
});
export type { Projection };

/** Runs one projection at a time for this canonical key across hosts in the current process. */
export const run = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: RunOptions = {},
): Effect.Effect<Progress, E | ProjectionFault, R | OR | Checkpoints | InputServices<Inputs>> =>
  withPermit(projection, runKernel(projection, options));

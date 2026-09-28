import { Transaction } from "./transaction.ts";
import { withPermit } from "./serialize.ts";
import type { Outputs } from "./outputs.ts";
import { CurrentOwner, isForeignOwner } from "@streamsy/core/internal/memory";
import { Effect, Predicate } from "effect";
import type { InputServices, InputMap, Slice } from "./batch.ts";
import {
  CheckpointStore,
  advance,
  rangesOf,
  restore,
  type CheckpointRecord,
} from "./checkpoint-store.ts";
import { ProjectionFault } from "./fault.ts";
import type { Atomic } from "./atomic.ts";
import { readInputs, validateOptions, type RunOptions } from "./read.ts";
import { passOutputs } from "./outputs-pass.ts";
import { offsetsOf, type Offsets } from "./snapshot.ts";
import { unitOf } from "./unit.ts";

export interface Progress<Inputs extends InputMap = InputMap> {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly units: number;
  readonly items: number;
  /** The accepted offset of each input after this call. */
  readonly offsets: Offsets<Inputs>;
}

/** One atomic unit: restore, read every input, then process and checkpoint in the owner transaction. */
export const passAtomic = Effect.fn("Projection.passAtomic")(function* <
  Inputs extends InputMap,
  E,
  R,
>(
  projection: Atomic<Inputs, E, R>,
): Effect.fn.Return<
  Progress<Inputs>,
  E | ProjectionFault,
  R | Transaction | CheckpointStore | InputServices<Inputs>
> {
  const owner = yield* CheckpointStore;
  const transaction = yield* Transaction;
  const before = yield* restore(projection, owner);
  const empty = { units: 0, items: 0, offsets: offsetsOf<Inputs>(before.record) };
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
  const record = yield* transaction.withTransaction(
    Effect.gen(function* () {
      const again = yield* owner.load(projection);
      if (again.token !== before.token)
        return yield* new ProjectionFault({
          phase: "checkpoint",
          reason: "token-conflict",
          message: `Record of ${projection.id} changed while the unit was read`,
        });
      const state = yield* projection.process(read.slices, unit, before.record.state).pipe(
        Effect.provideService(CurrentOwner, transaction.owner),
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
          : { identity, inputs, adapters, pending };
      const encoded = projection.state === undefined ? before.record.state : state;
      if (encoded !== undefined) Object.assign(next, { state: encoded });
      yield* owner.compareAndSet(projection, before.token, next);
      return next;
    }),
  );
  // A non-empty pass is progress; only an empty pass is authoritative for caught-up.
  return {
    status: "progress",
    units: 1,
    items: read.items,
    offsets: offsetsOf<Inputs>(record),
  };
});

/** One unit under the strategy the declaration carries. */
export const pass = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
): Effect.Effect<
  Progress<Inputs>,
  E | ProjectionFault,
  R | OR | Transaction | CheckpointStore | InputServices<Inputs>
> => (Predicate.isTagged(projection, "Atomic") ? passAtomic(projection) : passOutputs(projection));

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
): Effect.fn.Return<
  Progress<Inputs>,
  E | ProjectionFault,
  R | OR | Transaction | CheckpointStore | InputServices<Inputs>
> {
  yield* validateOptions(options);
  const cap = options.limit ?? Number.POSITIVE_INFINITY;
  let units = 0;
  let items = 0;
  let result: Progress<Inputs>;
  do {
    result = yield* pass(projection);
    units += result.units;
    items += result.items;
    if (result.status !== "progress") return { ...result, units, items };
  } while (units < cap);
  return { ...result, status: "limit-reached", units, items };
});

/** Runs one projection at a time for this canonical key across hosts in the current process. */
export const run = <Inputs extends InputMap, E, R, OR = never>(
  projection: Atomic<Inputs, E, R> | Outputs<Inputs, E, R, OR>,
  options: RunOptions = {},
): Effect.Effect<
  Progress<Inputs>,
  E | ProjectionFault,
  R | OR | Transaction | CheckpointStore | InputServices<Inputs>
> => withPermit(projection, runKernel(projection, options));

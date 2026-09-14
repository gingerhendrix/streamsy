import { Effect } from "effect";
import type { StreamsReader, StreamsWriter } from "@streamsy/core";
import type { InputMap, Slice } from "./batch.ts";
import { Checkpoints, advance, rangesOf, restore, type CheckpointRecord } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import type { Fused, Projection, Stream } from "./projection.ts";
import { DEFAULT_ITEMS, DEFAULT_UNITS, readInputs, validateBudget, type Budget } from "./read.ts";
import { passStream } from "./stream-output.ts";
import { unitOf } from "./unit.ts";

export interface Progress {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly units: number;
  readonly items: number;
  readonly bytes: number;
  readonly record: CheckpointRecord;
}

/** The host services every strategy is given; a protocol Layer always carries both stream services. */
export type Host = Checkpoints | StreamsReader | StreamsWriter;

/** One fused unit: restore, read every input, then process and checkpoint in the owner transaction. */
export const passFused = Effect.fn("Projection.passFused")(function* <
  Inputs extends InputMap,
  E,
  R,
>(
  projection: Fused<Inputs, E, R>,
  budget: Budget = {},
): Effect.fn.Return<Progress, E | ProjectionFault, R | Checkpoints | StreamsReader> {
  yield* validateBudget(budget);
  const owner = yield* Checkpoints;
  const before = yield* restore(projection, owner);
  const empty = { units: 0, items: 0, bytes: 0, record: before.record };
  const read = yield* readInputs(projection.inputs, before.record.inputs, {
    items: budget.items ?? DEFAULT_ITEMS,
    bytes: budget.bytes,
  });
  if (read.refused) return { ...empty, status: "limit-reached" };
  const slices = Object.values<Slice<unknown>>(read.slices);
  const closed = slices.every((slice) => slice.closed);
  if (read.items === 0) return { ...empty, status: closed ? "source-closed" : "caught-up" };
  const unit = unitOf(
    projection.id,
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
      yield* projection.process(read.slices, unit);
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
    status: closed ? "source-closed" : "progress",
    units: 1,
    items: read.items,
    bytes: read.bytes,
    record,
  };
});

/** One unit under the strategy the declaration carries. */
export const pass = <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Stream<Inputs, O, E, R>,
  budget: Budget = {},
): Effect.Effect<Progress, E | ProjectionFault, R | Host> =>
  projection._tag === "Fused" ? passFused(projection, budget) : passStream(projection, budget);

/**
 * Repeats `pass` until an empty pass reports caught-up, every input is closed, or
 * the budget is spent. The trailing empty pass adds nothing to the totals.
 */
export const run = Effect.fn("Projection.run")(function* <Inputs extends InputMap, O, E, R>(
  projection: Fused<Inputs, E, R> | Stream<Inputs, O, E, R>,
  budget: Budget = {},
): Effect.fn.Return<Progress, E | ProjectionFault, R | Host> {
  yield* validateBudget(budget);
  const maxUnits = budget.units ?? DEFAULT_UNITS;
  const maxItems = budget.items ?? DEFAULT_ITEMS;
  let units = 0;
  let items = 0;
  let bytes = 0;
  let result: Progress;
  do {
    result = yield* pass(projection, {
      items: maxItems - items,
      bytes: budget.bytes === undefined ? undefined : budget.bytes - bytes,
    });
    units += result.units;
    items += result.items;
    bytes += result.bytes;
    if (result.status !== "progress") return { ...result, units, items, bytes };
  } while (
    units < maxUnits &&
    items < maxItems &&
    (budget.bytes === undefined || bytes < budget.bytes)
  );
  return { ...result, status: "limit-reached", units, items, bytes };
});
export type { Projection };

import { Effect, Option } from "effect";
import { ZERO_OFFSET, type StreamsReader } from "@streamsy/core";
import type { InputMap, Slice } from "./batch.ts";
import { Checkpoints, type CheckpointRecord, type CheckpointsApi } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import type { Identity, Projection } from "./projection.ts";
import { DEFAULT_ITEMS, DEFAULT_UNITS, readInputs, validateBudget, type Budget } from "./read.ts";
import { encodeKey, unitOf, type Range } from "./unit.ts";

export interface Progress {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly units: number;
  readonly items: number;
  readonly bytes: number;
  readonly record: CheckpointRecord;
}

const identityOf = (inputs: InputMap): Record<string, string> =>
  Object.fromEntries(
    Object.entries(inputs).map(([name, ref]) => [name, encodeKey([ref.id, ref.contentType])]),
  );
const sameIdentity = (a: Record<string, string>, b: Record<string, string>): boolean => {
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((k) => a[k] === b[k]);
};

/** The stored record, or the unstored initial record for a fresh key. */
const restore = Effect.fn("Projection.restore")(function* (
  projection: Identity,
  owner: CheckpointsApi,
) {
  const loaded = yield* owner.load(projection);
  const identity = { inputs: identityOf(projection.inputs) };
  if (Option.isNone(loaded.record)) {
    const record: CheckpointRecord = {
      identity,
      inputs: Object.fromEntries(Object.keys(projection.inputs).map((name) => [name, ZERO_OFFSET])),
      adapters: {},
    };
    return { record, token: loaded.token };
  }
  if (!sameIdentity(loaded.record.value.identity.inputs, identity.inputs))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "identity-mismatch",
      message: `Stored inputs differ from the declaration of ${projection.id}`,
    });
  for (const name of Object.keys(projection.inputs)) {
    if (loaded.record.value.inputs[name] === undefined)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "invalid-record",
        input: name,
        message: `Stored record of ${projection.id} has no offset for ${name}`,
      });
  }
  return { record: loaded.record.value, token: loaded.token };
});

const advance = (
  inputs: Record<string, string>,
  slices: Record<string, Slice<unknown>>,
): Record<string, string> =>
  Object.fromEntries(
    Object.entries(inputs).map(([name, offset]) => [name, slices[name]?.nextOffset ?? offset]),
  );
const rangesOf = (slices: Record<string, Slice<unknown>>): Record<string, Range> =>
  Object.fromEntries(
    Object.entries(slices)
      .filter(([, slice]) => slice.items.length > 0)
      .map(([name, slice]) => [name, { from: slice.from, nextOffset: slice.nextOffset }]),
  );

/** One unit: restore, read every input, then process and checkpoint in the owner transaction. */
export const pass = Effect.fn("Projection.pass")(function* <Inputs extends InputMap, E, R>(
  projection: Projection<Inputs, E, R>,
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

/**
 * Repeats `pass` until an empty pass reports caught-up, every input is closed, or
 * the budget is spent. The trailing empty pass adds nothing to the totals.
 */
export const run = Effect.fn("Projection.run")(function* <Inputs extends InputMap, E, R>(
  projection: Projection<Inputs, E, R>,
  budget: Budget = {},
): Effect.fn.Return<Progress, E | ProjectionFault, R | Checkpoints | StreamsReader> {
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

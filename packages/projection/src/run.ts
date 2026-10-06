import { Effect } from "effect";
import {
  inputEntries,
  emptySlice,
  type InputServices,
  type InputMap,
  type Slice,
  type Slices,
} from "./batch.ts";
import { advance, rangesOf, restore } from "./checkpoint-store.ts";
import { HandlerFailed, type Pending } from "./commit.ts";
import { isPinned, setDetails, type Details } from "./commit/internal.ts";
import { ProjectionFault } from "./fault.ts";
import type { Executable } from "./executable.ts";
import { readInputs, reproduce, validateOptions, type RunOptions } from "./read.ts";
import { offsetsOf, type Offsets } from "./snapshot.ts";
import { unitOf } from "./unit.ts";
import { withPermit } from "./serialize.ts";

export interface Progress<Inputs extends InputMap = InputMap> {
  readonly status: "progress" | "caught-up" | "source-closed" | "limit-reached";
  readonly units: number;
  readonly items: number;
  readonly offsets: Offsets<Inputs>;
}

/** Restore and adapt one input unit; the Commit owns every write. */
export const pass = Effect.fn("Projection.pass")(function* <
  Inputs extends InputMap,
  E,
  R,
  Q,
  Result,
  P,
  OR = never,
>(
  projection: Executable<Inputs, E, R, Q, Result, P, unknown, OR>,
): Effect.fn.Return<
  Progress<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> {
  const before = yield* restore(projection, projection.commit);
  const pending = before.record.pending;

  if (pending !== undefined && !isPinned(projection.commit))
    return yield* new ProjectionFault({
      phase: "load",
      reason: "invalid-record",
      message: `Commit ${projection.commit.name} cannot resume a pinned unit`,
    });
  let batch: Slices<Inputs>;
  let count = 0;

  if (pending !== undefined) {
    const slices: Record<string, Slice<unknown>> = {};

    for (const [name, address] of inputEntries(projection.inputs)) {
      const range = pending.ranges[name];

      if (range === undefined) slices[name] = emptySlice(before.record.inputs[name] ?? "");
      else {
        const read = yield* reproduce(name, address, range);
        slices[name] = read.slice;
        count += read.slice.items.length;
      }
    }

    // SAFETY: each slice is reproduced with its declared input codec.
    batch = slices as Slices<Inputs>;
  } else {
    const read = yield* readInputs(projection.inputs, before.record.inputs);
    batch = read.slices;
    count = read.items;

    if (count === 0)
      return {
        status: Object.values<Slice<unknown>>(batch).every((slice) => slice.closed)
          ? "source-closed"
          : "caught-up",
        units: 0,
        items: 0,
        offsets: offsetsOf<Inputs>(before.record),
      };
  }

  const ranges = pending?.ranges ?? rangesOf(batch);

  const unit = unitOf(
    projection.id,
    projection.version,
    projection.generation,
    projection.params,
    ranges,
  );

  const { pending: _pending, ...record } = before.record;
  const next = { ...record, inputs: advance(record.inputs, ranges) };
  const outputs: Record<string, Details["outputs"][string]> = {};

  for (const [name, address] of Object.entries(projection.streams ?? {})) {
    const service = yield* address.stream;
    outputs[name] = { address, service };
  }

  let prepared: unknown;

  const processed = Effect.suspend(() =>
    projection.process(batch, unit, before.record.state, prepared),
  ).pipe(Effect.mapError((error) => new HandlerFailed({ error })));

  // SAFETY: the runner requires Exclude<R, P> from its caller; apply supplies P in the same Effect context.
  // oxlint-disable-next-line effecttsgo/unsafe-effect-type-assertion -- TypeScript cannot prove that apply supplies P for Exclude<R, P>; see SAFETY.
  const run = processed as Effect.Effect<Result, HandlerFailed, P>;

  const preparing =
    projection.prepare === undefined
      ? undefined
      : Effect.suspend(() => projection.prepare!(batch, unit)).pipe(
          Effect.tap((value) =>
            Effect.sync(() => {
              prepared = value;
            }),
          ),
          Effect.asVoid,
          Effect.mapError((error) => new HandlerFailed({ error })),
        );

  // SAFETY: Q includes prepare requirements and the runner requires Q from its caller.
  // oxlint-disable-next-line effecttsgo/unsafe-effect-type-assertion -- The runner's caller supplies the prepare requirements in Q; see SAFETY.
  const prepare = preparing as Effect.Effect<void, HandlerFailed> | undefined;

  const request: Pending<Result, P> = setDetails(
    { key: projection, unit, loaded: before.loaded, next, run },
    {
      outputs,
      prepare,
      ranges: Object.fromEntries(
        Object.entries(ranges).map(([name, range]) => [
          name,
          { from: range.from, nextOffset: range.nextOffset, count: batch[name]?.items.length ?? 0 },
        ]),
      ),
    },
  );

  yield* projection.commit.apply(request).pipe(
    Effect.catchTag("HandlerFailed", (failure) =>
      // SAFETY: this wrapper was made from this declaration's handler error immediately above.
      Effect.fail(failure.error as E),
    ),
  );

  return { status: "progress", units: 1, items: count, offsets: offsetsOf<Inputs>(next) };
});

/**
 * Internal kernel without a permit. Call only while holding the permit or simulating
 * a competing process in tests. Repeats `pass` until caught-up, every input is closed, or
 * the limit is reached. The trailing empty pass adds nothing to the totals.
 */
export const runKernel = Effect.fn("Projection.runKernel")(function* <
  Inputs extends InputMap,
  E,
  R,
  Q,
  Result,
  P,
  OR = never,
>(
  projection: Executable<Inputs, E, R, Q, Result, P, unknown, OR>,
  options: RunOptions = {},
): Effect.fn.Return<
  Progress<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
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
export const run = <Inputs extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Executable<Inputs, E, R, Q, Result, P, unknown, OR>,
  options: RunOptions = {},
): Effect.Effect<
  Progress<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> => withPermit(projection, runKernel(projection, options));

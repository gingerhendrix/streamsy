/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord and Range are Schema.Struct plain data records. */
import { Array, Effect, Option, Record } from "effect";
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
import {
  isPinned,
  setDetails,
  withState as settled,
  type Details,
  type Result as Settled,
} from "./commit/internal.ts";
import { forget } from "./forget.ts";
import { ProjectionFault } from "./fault.ts";
import type { Executable } from "./executable.ts";
import { readInputs, reproduce, validateOptions, type RunOptions } from "./read.ts";
import {
  offsetsOf,
  stateOf,
  type Decodable,
  type Offsets,
  type Snapshot,
  type StateValue,
} from "./snapshot.ts";
import { unitOf } from "./unit.ts";

/** The result of one `pass`: one committed unit (`progress`), or no input to commit. */
export interface PassResult<Inputs extends InputMap = InputMap> {
  readonly status: "progress" | "caught-up" | "source-closed";
  readonly units: number;
  readonly items: number;
  readonly offsets: Offsets<Inputs>;
}

/** One runner cycle: the committed state and offsets, plus the work that the cycle did. */
export interface Progress<S, Inputs extends InputMap = InputMap> extends Snapshot<S, Inputs> {
  readonly status: "caught-up" | "limit-reached" | "source-closed";
  /** Units that this cycle committed. */
  readonly units: number;
  /** Input items in those units. */
  readonly items: number;
}

/** Internal: a pass result plus the encoded state of the record that the pass left. */
interface Step<Inputs extends InputMap = InputMap> extends PassResult<Inputs> {
  readonly encoded: string | undefined;
}

/**
 * Internal: a cycle before its state is decoded. The offsets and the encoded state come
 * from the last pass, so they always match.
 */
export type Cycle<Inputs extends InputMap = InputMap> = Omit<Progress<unknown, Inputs>, "state"> & {
  readonly encoded: string | undefined;
};

/**
 * Internal: one pass that also returns the encoded state it committed, or the stored
 * state when it found no input.
 */
const step = Effect.fn("Projection.pass")(function* <
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
  Step<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> {
  const before = yield* restore(projection, projection.commit);

  if (before.record.ended) {
    yield* forget(projection);

    // The last unit already committed this state. Report it with its own offsets.
    return {
      status: "source-closed",
      units: 0,
      items: 0,
      offsets: offsetsOf<Inputs>(before.record),
      encoded: before.record.state,
    };
  }

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
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: Each slice is reproduced through its declared codec; TS cannot infer the mapped keys from a string record.
    batch = slices as Slices<Inputs>;
  } else {
    const read = yield* readInputs(
      projection.inputs,
      before.record.inputs,
      Option.isSome(before.loaded.record),
    );

    batch = read.slices;
    count = read.items;

    const terminal = Array.some(
      Record.toEntries<string, Slice<unknown>>(batch),
      ([name, slice]) =>
        slice.gone === true || (slice.closed && slice.upToDate && !before.record.closed?.[name]),
    );

    if (count === 0 && !terminal)
      return {
        status: Array.every(Record.values<string, Slice<unknown>>(batch), (slice) => slice.closed)
          ? "source-closed"
          : "caught-up",
        units: 0,
        items: 0,
        offsets: offsetsOf<Inputs>(before.record),
        encoded: before.record.state,
      };
  }

  const gone = Array.some(
    Record.values<string, Slice<unknown>>(batch),
    (slice) => slice.gone === true,
  );

  const ranges = pending?.ranges ?? rangesOf(batch);

  const unit = unitOf(
    projection.id,
    projection.version,
    projection.generation,
    projection.params,
    ranges,
  );

  const { pending: _pending, ...record } = before.record;
  const closed = { ...record.closed };

  for (const [name, slice] of Record.toEntries<string, Slice<unknown>>(batch)) {
    if (slice.closed && slice.upToDate) closed[name] = true;
  }

  const next = {
    ...record,
    inputs: advance(record.inputs, ranges),
    closed,
  };

  if (gone) Object.assign(next, { ended: true });
  const outputs: Record<string, Details["outputs"][string]> = {};

  for (const [name, address] of Record.toEntries(projection.streams ?? {})) {
    const service = yield* address.stream;
    outputs[name] = { address, service };
  }

  let prepared: unknown;
  // The last result of `run`. The Commit stores the state of the run that it commits.
  let result: Result | undefined;

  const processed = Effect.suspend(() =>
    projection.process(batch, unit, before.record.state, prepared),
  ).pipe(
    Effect.tap((value) =>
      Effect.sync(() => {
        result = value;
      }),
    ),
    Effect.mapError((error) => new HandlerFailed({ error })),
  );

  // SAFETY: the runner requires Exclude<R, P> from its caller; apply supplies P in the same Effect context.
  // oxlint-disable-next-line effecttsgo/unsafe-effect-type-assertion, typescript/no-unsafe-type-assertion -- TypeScript cannot prove that apply supplies P for Exclude<R, P>; see SAFETY.
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
  // oxlint-disable-next-line effecttsgo/unsafe-effect-type-assertion, typescript/no-unsafe-type-assertion -- The runner's caller supplies the prepare requirements in Q; see SAFETY.
  const prepare = preparing as Effect.Effect<void, HandlerFailed> | undefined;

  const request: Pending<Result, P> = setDetails(
    { key: projection, unit, loaded: before.loaded, next, run },
    {
      outputs,
      prepare,
      ranges: Record.fromEntries(
        Array.map(Record.toEntries(ranges), ([name, range]) => [
          name,
          {
            ...range,
            count: batch[name]?.items.length ?? 0,
            closed: batch[name]?.closed ?? false,
            upToDate: batch[name]?.upToDate ?? false,
          },
        ]),
      ),
    },
  );

  yield* projection.commit.apply(request).pipe(
    Effect.catchTag("HandlerFailed", (failure) =>
      // SAFETY: this wrapper was made from this declaration's handler error immediately above.
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: HandlerFailed was built from this declaration's error E above; the shared Commit wrapper erases E.
      Effect.fail(failure.error as E),
    ),
  );

  // SAFETY: every Commit settles a Result of the shared union; settled() reads only `encoded`.
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- SAFETY: Declarations produce the Commit Result union; the generic Result erases it.
  const stored = settled(next, result as Settled);

  // After a gone input, report the state that the final unit committed, even though forget deletes the record.
  if (gone) yield* forget(projection);

  return {
    status: gone ? "source-closed" : "progress",
    units: 1,
    items: count,
    offsets: offsetsOf<Inputs>(next),
    encoded: stored.state,
  };
});

/** Restore and adapt one input unit; the Commit owns every write. */
export const pass = <Inputs extends InputMap, E, R, Q, Result, P, OR = never>(
  projection: Executable<Inputs, E, R, Q, Result, P, unknown, OR>,
): Effect.Effect<
  PassResult<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> => Effect.map(step(projection), ({ encoded: _encoded, ...result }) => result);

/** Internal: repeats pass until caught up, closed, or limited. The caller validates options. */
export const catchUp = Effect.fnUntraced(function* <
  Inputs extends InputMap,
  E,
  R,
  Q,
  Result,
  P,
  OR = never,
>(
  projection: Executable<Inputs, E, R, Q, Result, P, unknown, OR>,
  options: RunOptions,
): Effect.fn.Return<
  Cycle<Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> {
  const cap = options.limit ?? Number.POSITIVE_INFINITY;
  let units = 0;
  let items = 0;
  let offsets: Offsets<Inputs>;
  let encoded: string | undefined;

  do {
    const result = yield* step(projection);
    units += result.units;
    items += result.items;
    offsets = result.offsets;
    encoded = result.encoded;

    if (result.status !== "progress")
      return { status: result.status, units, items, offsets, encoded };
  } while (units < cap);

  return { status: "limit-reached", units, items, offsets, encoded };
});

/** Internal: a cycle with its state decoded once. It reads no checkpoint. */
export const progressOf = <Inputs extends InputMap, S>(
  projection: Decodable<S>,
  { encoded, ...cycle }: Cycle<Inputs>,
): Effect.Effect<Progress<StateValue<S>, Inputs>, ProjectionFault> =>
  Effect.map(stateOf(projection, encoded), (state) => ({ ...cycle, state }));

/**
 * Repeats pass until caught up, closed, or limited, then decodes the state of the last
 * pass once. After a gone input, the result holds the state that the final unit
 * committed, although the kernel then deletes the record. Use one writer per key.
 */
export const run = Effect.fn("Projection.run")(function* <
  Inputs extends InputMap,
  E,
  R,
  Q,
  Result,
  P,
  S = never,
  OR = never,
>(
  projection: Executable<Inputs, E, R, Q, Result, P, S, OR>,
  options: RunOptions = {},
): Effect.fn.Return<
  Progress<StateValue<S>, Inputs>,
  E | ProjectionFault,
  Exclude<R, P> | Q | OR | InputServices<Inputs>
> {
  yield* validateOptions(options);

  return yield* progressOf(projection, yield* catchUp(projection, options));
});

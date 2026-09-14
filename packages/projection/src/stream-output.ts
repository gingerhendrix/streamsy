import { Effect } from "effect";
import { Producer, StreamsReader, StreamsWriter, type StreamRef } from "@streamsy/core";
import { emptySlice, type InputMap, type Slice, type Slices } from "./batch.ts";
import { Checkpoints, advance, rangesOf, restore, type CheckpointRecord } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import type { Stream } from "./projection.ts";
import { DEFAULT_ITEMS, readInputs, reproduce, validateBudget, type Budget } from "./read.ts";
import type { Progress } from "./run.ts";
import { canonicalParams, unitOf, type PendingUnit } from "./unit.ts";

/** The pinned tuple identity: the params join the id so two parameterisations never share a sequence. */
export const producerId = (id: string, params: Record<string, string>): string =>
  Object.keys(params).length === 0 ? id : `${id}/${canonicalParams(params)}`;

/** Append one unit under its pinned tuple; appended and duplicate are one outcome. */
const appendPinned = Effect.fn("Projection.appendPinned")(function* <O>(
  output: StreamRef.StreamRef<O>,
  items: ReadonlyArray<O>,
  position: Producer.Position,
) {
  const pin = (reason: ProjectionFault["reason"], message: string) =>
    new ProjectionFault({ phase: "pin", reason, message });
  return yield* Producer.append(output, items, position).pipe(
    Effect.catchTags({
      EncodeFault: () =>
        new ProjectionFault({
          phase: "process",
          reason: "invalid-output",
          message: `Cannot encode output for ${output.id}`,
        }),
      StaleEpoch: (error) =>
        pin(
          "stale-epoch",
          `Epoch ${position.epoch} of ${output.id} is behind ${error.currentEpoch}`,
        ),
      ProducerGap: (error) =>
        pin(
          "invalid-record",
          `Sequence ${error.receivedSeq} of ${output.id} does not follow ${error.expectedSeq}`,
        ),
      InvalidEpochSeq: () =>
        pin("invalid-record", `Epoch ${position.epoch} of ${output.id} must start at seq 0`),
      NotSupported: (error) =>
        pin("unsupported-composition", `${output.id} does not support ${error.feature}`),
      StorageFault: () => pin("storage-failure", `Cannot append to ${output.id}`),
      TransportFault: () => pin("storage-failure", `Cannot append to ${output.id}`),
      StreamNotFound: () => pin("invalid-output", `Output ${output.id} does not exist`),
      StreamGone: () => pin("invalid-output", `Output ${output.id} is gone`),
      StreamClosed: () => pin("invalid-output", `Output ${output.id} is closed`),
      OffsetMismatch: () => pin("invalid-output", `Output ${output.id} refused the append`),
      AppendConflict: () => pin("invalid-output", `Output ${output.id} refused the append`),
      StreamBusy: () => pin("invalid-output", `Output ${output.id} is busy`),
      InvalidAppendRequest: (error) => pin("invalid-output", error.message),
    }),
  );
});

/** The pin write is the last save before the append, so its conflict is a pin failure. */
const asPin = (fault: ProjectionFault): ProjectionFault =>
  fault.phase === "checkpoint" && fault.reason === "token-conflict"
    ? new ProjectionFault({ phase: "pin", reason: fault.reason, message: fault.message })
    : fault;

/**
 * One stream-output unit. A pinned unit from an earlier pass settles first: its
 * ranges are reproduced and sent under the pinned tuple, and both `Appended` and
 * `Duplicate` advance the record. Only then does the pass read new items.
 */
export const passStream = Effect.fn("Projection.passStream")(function* <
  Inputs extends InputMap,
  O,
  E,
  R,
>(
  projection: Stream<Inputs, O, E, R>,
  budget: Budget = {},
): Effect.fn.Return<
  Progress,
  E | ProjectionFault,
  R | Checkpoints | StreamsReader | StreamsWriter
> {
  yield* validateBudget(budget);
  const owner = yield* Checkpoints;
  const before = yield* restore(projection, owner);
  const { identity, adapters } = before.record;
  const stream = adapters.stream ?? { epoch: projection.generation, nextSeq: 0 };
  if (stream.epoch !== projection.generation)
    return yield* new ProjectionFault({
      phase: "load",
      reason: "stale-epoch",
      message: `Stored epoch ${stream.epoch} of ${projection.id} is not generation ${projection.generation}`,
    });
  const position = (seq: number): Producer.Position => ({
    producerId: producerId(projection.id, projection.params),
    epoch: stream.epoch,
    seq,
  });
  const settled = (inputs: Record<string, string>, nextSeq: number): CheckpointRecord => ({
    identity,
    inputs,
    adapters: { ...adapters, stream: { epoch: stream.epoch, nextSeq } },
  });

  if (before.record.pending !== undefined) {
    const pending: PendingUnit = before.record.pending;
    const slices: Record<string, Slice<unknown>> = {};
    let items = 0;
    let bytes = 0;
    for (const [name, ref] of Object.entries(projection.inputs)) {
      const range = pending.ranges[name];
      if (range === undefined) {
        slices[name] = emptySlice(before.record.inputs[name] ?? "");
        continue;
      }
      const read = yield* reproduce(name, ref, range);
      slices[name] = read.slice;
      items += read.slice.items.length;
      bytes += read.bytes;
    }
    const unit = unitOf(projection.id, projection.generation, projection.params, pending.ranges);
    // SAFETY: `slices` has exactly the keys of `inputs`, each reproduced through that input's codec.
    const outputs = yield* projection.process(slices as Slices<Inputs>, unit);
    yield* appendPinned(projection.output, outputs, position(pending.seq));
    const record = settled(advance(before.record.inputs, pending.ranges), pending.seq + 1);
    yield* owner.save(projection, record, before.token);
    const closed = Object.values(slices).every((slice) => slice.closed);
    return { status: closed ? "source-closed" : "progress", units: 1, items, bytes, record };
  }

  const empty = { units: 0, items: 0, bytes: 0, record: before.record };
  const read = yield* readInputs(projection.inputs, before.record.inputs, {
    items: budget.items ?? DEFAULT_ITEMS,
    bytes: budget.bytes,
  });
  if (read.refused) return { ...empty, status: "limit-reached" };
  const slices = Object.values<Slice<unknown>>(read.slices);
  const closed = slices.every((slice) => slice.closed);
  if (read.items === 0) return { ...empty, status: closed ? "source-closed" : "caught-up" };
  const ranges = rangesOf(read.slices);
  const unit = unitOf(projection.id, projection.generation, projection.params, ranges);
  const outputs = yield* projection.process(read.slices, unit);
  const inputs = advance(before.record.inputs, read.slices);
  let record: CheckpointRecord;
  if (outputs.length === 0) {
    // Nothing to append: no pin, no sequence consumed, and the stream entry is left as loaded.
    record = { identity, inputs, adapters };
    yield* owner.save(projection, record, before.token);
  } else {
    const pinned: CheckpointRecord = {
      identity,
      inputs: before.record.inputs,
      pending: {
        ranges: Object.fromEntries(
          Object.entries(ranges).map(([name, range]) => [
            name,
            {
              from: range.from,
              nextOffset: range.nextOffset,
              count: read.slices[name]?.items.length ?? 0,
            },
          ]),
        ),
        seq: stream.nextSeq,
      },
      adapters: { ...adapters, stream },
    };
    const pinToken = yield* owner
      .save(projection, pinned, before.token)
      .pipe(Effect.mapError(asPin));
    yield* appendPinned(projection.output, outputs, position(stream.nextSeq));
    record = settled(inputs, stream.nextSeq + 1);
    yield* owner.save(projection, record, pinToken);
  }
  // A non-empty pass is progress; only an empty pass is authoritative for caught-up.
  return {
    status: closed ? "source-closed" : "progress",
    units: 1,
    items: read.items,
    bytes: read.bytes,
    record,
  };
});

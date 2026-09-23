import { Effect } from "effect";
import { Producer, Streams, StreamsReader, StreamsWriter, type StreamRef } from "@streamsy/core";
import { emptySlice, type InputMap, type Slice, type Slices } from "./batch.ts";
import { Checkpoints, advance, rangesOf, restore, type CheckpointRecord } from "./checkpoint.ts";
import { ProjectionFault } from "./fault.ts";
import type { Named } from "./outputs.ts";
import type { Processed } from "./output.ts";
import type { Pinned } from "./projection.ts";
import { readInputs, reproduce, validateOptions, type RunOptions } from "./read.ts";
import type { Progress } from "./run.ts";
import { canonicalParams, unitOf } from "./unit.ts";

/** The pinned tuple identity: the params join the id so two parameterisations never share a sequence. */
export const producerId = (id: string, version: number, params: Record<string, string>): string =>
  `${id}/v${version}${Object.keys(params).length === 0 ? "" : `/${canonicalParams(params)}`}`;

/** Append one unit under its pinned tuple; appended and duplicate are one outcome. */
const appendPinned = Effect.fn("Projection.appendPinned")(function* <O>(
  output: StreamRef.StreamRef<O>,
  items: ReadonlyArray<O>,
  position: Producer.Position,
) {
  const pin = (reason: ProjectionFault["reason"], message: string, cause?: unknown) =>
    new ProjectionFault({ phase: "pin", reason, message, cause });
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
          `Epoch ${position.producerEpoch} of ${output.id} is behind ${error.currentEpoch}`,
        ),
      ProducerGap: (error) =>
        pin(
          "invalid-record",
          `Sequence ${error.receivedSeq} of ${output.id} does not follow ${error.expectedSeq}`,
        ),
      InvalidEpochSeq: () =>
        pin(
          "invalid-record",
          `Epoch ${position.producerEpoch} of ${output.id} must start at seq 0`,
        ),
      NotSupported: (error) =>
        pin("unsupported-composition", `${output.id} does not support ${error.feature}`),
      StorageFault: (cause) => pin("storage-failure", `Cannot append to ${output.id}`, cause),
      TransportFault: (cause) => pin("storage-failure", `Cannot append to ${output.id}`, cause),
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

/** One pinned unit, with independent tuple sequences and one atomic settle save. */
export const passStream = Effect.fn("Projection.passStream")(function* <
  Inputs extends InputMap,
  O,
  E,
  R,
>(
  projection: Pinned<Inputs, O, E, R> | Named<Inputs, E, R>,
  options: RunOptions = {},
): Effect.fn.Return<
  Progress,
  E | ProjectionFault,
  R | Checkpoints | StreamsReader | StreamsWriter
> {
  yield* validateOptions(options);
  const owner = yield* Checkpoints;
  const before = yield* restore(projection, owner);
  const { identity, adapters } = before.record;
  const streams: Readonly<Record<string, StreamRef.StreamRef<unknown>>> =
    projection._tag === "Stream" ? { stream: projection.output } : projection.streams;
  const positions = { ...adapters.outputs };
  for (const [name, position] of Object.entries(positions)) {
    if (position.epoch !== projection.generation)
      return yield* new ProjectionFault({
        phase: "load",
        reason: "stale-epoch",
        message: `Stored epoch ${position.epoch} of ${projection.id}/${name} is not generation ${projection.generation}`,
      });
  }
  const pending = before.record.pending;
  let batch: Slices<Inputs>;
  let count = 0;
  if (pending !== undefined) {
    const slices: Record<string, Slice<unknown>> = {};
    for (const [name, ref] of Object.entries(projection.inputs)) {
      const range = pending.ranges[name];
      if (range === undefined) slices[name] = emptySlice(before.record.inputs[name] ?? "");
      else {
        const read = yield* reproduce(name, ref, range);
        slices[name] = read.slice;
        count += read.slice.items.length;
      }
    }
    // SAFETY: each slice is reproduced through the codec of its declared input.
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
        record: before.record,
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
  const processed: Processed =
    projection._tag === "Stream"
      ? { items: { stream: yield* projection.process(batch, unit) } }
      : yield* projection.process(batch, unit);
  const seqs: Record<string, number> =
    pending === undefined
      ? Object.fromEntries(
          Object.keys(streams)
            .filter((name) => (processed.items[name]?.length ?? 0) > 0)
            .map((name) => [name, positions[name]?.nextSeq ?? 0]),
        )
      : pending.seqs;
  // A changed handler may not silently omit a previously pinned output.
  for (const name of Object.keys(seqs)) {
    if (streams[name] === undefined || (processed.items[name]?.length ?? 0) === 0)
      return yield* new ProjectionFault({
        phase: "process",
        reason: "invalid-output",
        message: `Cannot reproduce pinned output ${name}`,
      });
  }
  let token = before.token;
  if (pending === undefined && Object.keys(seqs).length > 0) {
    const reader = yield* StreamsReader;
    for (const [name, output] of Object.entries(streams)) {
      if (seqs[name] === undefined) continue;
      const head = yield* reader.head(output.id).pipe(
        Effect.catchTag("StreamNotFound", () =>
          Streams.create(output).pipe(Effect.andThen(reader.head(output.id))),
        ),
        Effect.mapError(
          (cause) =>
            new ProjectionFault({
              phase: "pin",
              reason:
                cause._tag === "StorageFault" || cause._tag === "TransportFault"
                  ? "storage-failure"
                  : "invalid-output",
              message: `Cannot prepare output ${output.id}`,
              cause,
            }),
        ),
      );
      if (head.closed)
        return yield* new ProjectionFault({
          phase: "pin",
          reason: "invalid-output",
          message: `Output ${output.id} is closed`,
        });
      positions[name] = positions[name] ?? { epoch: projection.generation, nextSeq: 0 };
    }
    const pinned: CheckpointRecord = {
      identity,
      inputs: before.record.inputs,
      adapters: { outputs: positions },
      pending: {
        seqs,
        ranges: Object.fromEntries(
          Object.entries(ranges).map(([name, range]) => [
            name,
            {
              from: range.from,
              nextOffset: range.nextOffset,
              count: batch[name]?.items.length ?? 0,
            },
          ]),
        ),
      },
    };
    token = yield* owner.save(projection, pinned, before.token).pipe(Effect.mapError(asPin));
  }
  for (const [name, output] of Object.entries(streams)) {
    const seq = seqs[name];
    if (seq === undefined) continue;
    yield* appendPinned(output, processed.items[name] ?? [], {
      producerId: producerId(projection.id, projection.version, projection.params),
      producerEpoch: projection.generation,
      producerSeq: seq,
    });
    positions[name] = { epoch: projection.generation, nextSeq: seq + 1 };
  }
  const record: CheckpointRecord = {
    identity,
    inputs: advance(before.record.inputs, ranges),
    adapters: Object.keys(positions).length === 0 ? adapters : { outputs: positions },
  };
  if (processed.saveState === undefined) yield* owner.save(projection, record, token);
  else
    yield* owner.withTransaction(
      Effect.gen(function* () {
        yield* owner.save(projection, record, token);
        if (processed.saveState !== undefined) yield* processed.saveState;
      }),
    );
  return { status: "progress", units: 1, items: count, record };
});

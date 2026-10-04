import { appendPinned } from "../outputs.ts";
/* oxlint-disable typescript/no-misused-spread -- CheckpointRecord is a Schema.Struct plain data record. */
import { Array, Effect, Option } from "effect";
import { isStreamsFault } from "@streamsy/core";
import { ownerOf } from "@streamsy/core/internal";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import { CheckpointStore, type CheckpointRecord } from "../checkpoint-store.ts";
import { ProjectionFault } from "../fault.ts";
import { make, type Emitted, type Accumulated, type OutputNames } from "./model.ts";
import { canonicalParams } from "../unit.ts";
import { beforeRecord, detailsOf, load, remove, pin, withState } from "./internal.ts";
/** The pinned tuple identity: the params join the id so two parameterisations never share a sequence. */
export const producerId = (id: string, version: number, params: Record<string, string>): string =>
  `${id}/v${version}${Object.keys(params).length === 0 ? "" : `/${canonicalParams(params)}`}`;

/** The pin write is the last save before the append, so its conflict is a pin failure. */
const asPin = (fault: ProjectionFault): ProjectionFault =>
  fault.phase === "checkpoint" && fault.reason === "token-conflict"
    ? new ProjectionFault({ phase: "pin", reason: fault.reason, message: fault.message })
    : fault;

export const producer = pin(
  make<Emitted<OutputNames> | Accumulated<unknown, OutputNames>, never, CheckpointStore>({
    name: "producer",
    guarantee: "effectively-once",
    remove,
    load: (key) =>
      Effect.gen(function* () {
        const loaded = yield* load(key);
        if (Option.isSome(loaded.record)) {
          if (
            loaded.record.value.pending !== undefined &&
            loaded.record.value.pending.seqs === undefined
          )
            return yield* new ProjectionFault({
              phase: "load",
              reason: "invalid-record",
              message: "A producer pin must include output sequences",
            });
          for (const [name, position] of Object.entries(
            loaded.record.value.adapters.outputs ?? {},
          )) {
            if (position.epoch !== key.generation)
              return yield* new ProjectionFault({
                phase: "load",
                reason: "stale-epoch",
                message: `Stored epoch ${position.epoch} of ${key.id}/${name} is not generation ${key.generation}`,
              });
          }
        }
        return loaded;
      }),
    apply: (request) =>
      Effect.gen(function* () {
        const owner = yield* CheckpointStore;
        const projection = request.key;
        const before = { record: beforeRecord(request), token: request.loaded.token };
        const { identity, adapters } = before.record;
        const streams = detailsOf(request).outputs;
        const ranges = request.unit.ranges;
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
        const result = yield* request.run;
        const processed = {
          items: Object.fromEntries(
            Object.entries(result.outputs).map(([name, output]) => [name, output.items]),
          ),
        };
        const seqs: Record<string, number> =
          pending === undefined
            ? Object.fromEntries(
                Object.keys(streams)
                  .filter((name) => (processed.items[name]?.length ?? 0) > 0)
                  .map((name) => [name, positions[name]?.nextSeq ?? 0]),
              )
            : (pending.seqs ?? {});
        // A changed handler may not silently omit a previously pinned output.
        for (const name of Object.keys(seqs)) {
          if (streams[name] === undefined || (processed.items[name]?.length ?? 0) === 0)
            return yield* new ProjectionFault({
              phase: "process",
              reason: "invalid-output",
              message: `Cannot reproduce pinned output ${name}`,
            });
        }
        if (pending !== undefined) {
          for (const name of Object.keys(streams)) {
            if ((processed.items[name]?.length ?? 0) > 0 && seqs[name] === undefined)
              return yield* new ProjectionFault({
                phase: "process",
                reason: "invalid-output",
                message: `Cannot reproduce pinned unit: output ${name} has items that were not pinned`,
              });
          }
        }
        let token = before.token;
        if (pending === undefined && Object.keys(seqs).length > 0) {
          for (const [name, output] of Object.entries(streams)) {
            if (seqs[name] === undefined) continue;
            const { wire, owner: outputOwner } = output.service;
            const head = yield* wire.head(output.address.id).pipe(
              Effect.catchTag("StreamNotFound", (cause) =>
                outputOwner !== ownerOf(owner.load)
                  ? Effect.fail(cause)
                  : Streams.create(output.address).pipe(
                      Effect.provideService(StreamsWriter, wire),
                      Effect.andThen(wire.head(output.address.id)),
                    ),
              ),
              Effect.mapError(
                (cause) =>
                  new ProjectionFault({
                    phase: "pin",
                    reason: isStreamsFault(cause) ? "storage-failure" : "invalid-output",
                    message: `Cannot prepare output ${output.address.id}`,
                    cause,
                  }),
              ),
            );
            if (head.closed)
              return yield* new ProjectionFault({
                phase: "pin",
                reason: "invalid-output",
                message: `Output ${output.address.id} is closed`,
              });
            positions[name] = positions[name] ?? { epoch: projection.generation, nextSeq: 0 };
          }
          const pinned: CheckpointRecord = {
            identity,
            inputs: before.record.inputs,
            adapters: { outputs: { ...positions } },
            pending: {
              seqs,
              ranges: Object.fromEntries(
                Object.entries(ranges).map(([name, range]) => [
                  name,
                  {
                    from: range.from,
                    nextOffset: range.nextOffset,
                    count: detailsOf(request).ranges[name]?.count ?? 0,
                  },
                ]),
              ),
            },
          };
          if (before.record.state !== undefined)
            Object.assign(pinned, { state: before.record.state });
          token = yield* owner
            .compareAndSet(projection, before.token, pinned)
            .pipe(Effect.mapError(asPin));
        }
        for (const [name, output] of Object.entries(streams)) {
          const seq = seqs[name];
          const items = processed.items[name] ?? [];
          // Every pinned output has items: the check above fails the unit otherwise.
          if (seq === undefined || !Array.isReadonlyArrayNonEmpty(items)) continue;
          yield* appendPinned(output.address, output.service.wire, items, {
            producerId: producerId(projection.id, projection.version, projection.params),
            producerEpoch: projection.generation,
            producerSeq: seq,
          });
          positions[name] = { epoch: projection.generation, nextSeq: seq + 1 };
        }
        const record: CheckpointRecord = {
          identity,
          inputs: request.next.inputs,
          adapters: Object.keys(positions).length === 0 ? adapters : { outputs: positions },
        };
        yield* owner.compareAndSet(
          projection,
          token,
          withState({ ...request.next, ...record }, result),
        );
      }),
  }),
);

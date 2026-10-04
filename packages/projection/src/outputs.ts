/* oxlint-disable typescript/no-explicit-any -- Route maps retain exact public types until the checked runtime adapter boundary. */
import { Effect, Predicate, type Schema } from "effect";
import type { NonEmptyReadonlyArray } from "effect/Array";
import { DurableState, type Producer, type StreamAddress, type Streamsy } from "@streamsy/core";
import { addressesOf, type AddressOf, type Inputs, type DecodedParams } from "./declaration.ts";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import type { ResolvedOutput } from "./commit/internal.ts";
import type { InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";

export type RoutedMap = Inputs;
export type Item<T> =
  AddressOf<T> extends { readonly collections: infer C extends StreamAddress.Collections }
    ? DurableState.Change<C>
    : AddressOf<T> extends { readonly codec: Schema.Codec<infer A, any, any, any> }
      ? A
      : never;
export type Services<O extends RoutedMap> = {
  [K in keyof O]: AddressOf<O[K]> extends Streamsy.Address<any, infer R> ? R : never;
}[keyof O];

/** Resolve routes just like inputs, retaining the route codec and collection names. */
export const resolveOutputs = (outputs: RoutedMap, params: DecodedParams, inputs: InputMap) => {
  const streams = addressesOf(outputs, params);
  const ids = Object.values(streams).map((address) => address.id);
  if (new Set(ids).size !== ids.length)
    throw new RangeError("Two outputs may not name the same stream");
  const inputIds = new Set(Object.values(inputs).map((address) => address.id));
  if (ids.some((id) => inputIds.has(id)))
    throw new RangeError("An output may not name an input stream");
  if (Object.values(streams).some((address) => !Predicate.isTagged(address, "Json")))
    throw new RangeError("Projection outputs require JSON or state routes");
  return streams;
};

export const collect = (
  outputs: Readonly<Record<string, Streamsy.Address<unknown, any>>>,
  buffers: Record<string, unknown[]>,
  emitted: Readonly<Record<string, ReadonlyArray<any>>>,
) =>
  Effect.gen(function* () {
    for (const [name, entries] of Object.entries(emitted)) {
      const output = outputs[name];
      if (!Object.hasOwn(outputs, name) || output === undefined || !Array.isArray(entries))
        return yield* new ProjectionFault({
          phase: "process",
          reason: "invalid-output",
          message: `Invalid output array ${name}`,
        });
      const values = yield* Effect.try({
        try: () =>
          "collections" in output
            ? // SAFETY: state addresses carry their collections; constructor signatures check each change against them.
              DurableState.changes(
                // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- The address's state marker establishes this collection shape.
                output as StreamAddress.StateAddress<StreamAddress.Collections, any, any>,
                {},
                entries,
              )
            : entries,
        catch: (cause) =>
          new ProjectionFault({
            phase: "process",
            reason: "invalid-output",
            message: `Invalid row change for ${name}`,
            cause,
          }),
      });
      (buffers[name] ??= []).push(...values);
    }
  });

/** Append one unit under its pinned tuple; appended and duplicate are one outcome. */
export const appendPinned = Effect.fn("Projection.appendPinned")(function* <O>(
  output: StreamAddress.StreamAddress<O>,
  wire: ResolvedOutput["service"]["wire"],
  items: NonEmptyReadonlyArray<O>,
  position: Producer.Position,
) {
  const pin = (reason: ProjectionFault["reason"], message: string, cause?: unknown) =>
    new ProjectionFault({ phase: "pin", reason, message, cause });
  return yield* Streams.append(output, items, { producer: position }).pipe(
    Effect.provideService(StreamsWriter, wire),
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
      NotSupported: (error) =>
        pin("unsupported-composition", `${output.id} does not support ${error.feature}`),
      StorageFault: (cause) => pin("storage-failure", `Cannot append to ${output.id}`, cause),
      TransportFault: (cause) => pin("storage-failure", `Cannot append to ${output.id}`, cause),
      StreamNotFound: () => pin("invalid-output", `Output ${output.id} does not exist`),
      StreamGone: () => pin("invalid-output", `Output ${output.id} is gone`),
      StreamClosed: () => pin("invalid-output", `Output ${output.id} is closed`),
      AppendConflict: () => pin("invalid-output", `Output ${output.id} refused the append`),
      StreamBusy: () => pin("invalid-output", `Output ${output.id} is busy`),
    }),
  );
});

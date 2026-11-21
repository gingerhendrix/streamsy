/* oxlint-disable typescript/no-explicit-any -- Route maps retain exact public types until the checked runtime adapter boundary. */
import { Array, Effect, Predicate, Record, type Schema } from "effect";
import {
  DurableState,
  isStreamsFault,
  type Producer,
  type StreamAddress,
  type Streamsy,
} from "@streamsy/core";
import {
  isDeclaration,
  type AddressOf,
  type DecodedParams,
  type Input,
  type StreamDeclaration,
} from "./declaration.ts";
import { Streams, StreamsWriter } from "@streamsy/core/protocol";
import type { ResolvedOutput } from "./commit/internal.ts";
import type { InputMap } from "./batch.ts";
import { ProjectionFault } from "./fault.ts";
import { acceptedKey, type AcceptedOutput } from "./accepted.ts";
import {
  GenerationOutputs,
  generationBinding,
  isPerGeneration,
  type GenerationOutputBindings,
  type PerGeneration,
  type RoutedOutput,
} from "./per-generation.ts";
import {
  routedHandle,
  routedDefinition,
  type RoutedDefinition,
  type RoutedHandle,
  type RoutedOutputBindings,
} from "./routed-outputs.ts";
import type { ProjectionKey } from "./checkpoint-store.ts";

export type RoutedMap = Readonly<Record<string, RoutedOutput>>;

export type StaticOutputMap = Readonly<
  Record<string, Input | PerGeneration<StreamDeclaration, string>>
>;

export type Item<T> =
  AddressOf<T> extends { readonly collections: infer C extends StreamAddress.Collections }
    ? DurableState.Entry<C>
    : AddressOf<T> extends { readonly codec: Schema.Codec<infer A, any, any, any> }
      ? A
      : never;

export type Services<O extends RoutedMap> = {
  [K in keyof O]: AddressOf<O[K]> extends Streamsy.Address<any, infer R> ? R : never;
}[keyof O];

/** Resolve ordinary and accepted outputs with the same parameter sources. */
type Address = Streamsy.Address<any, any>;

interface ResolvedBindings {
  readonly streams: Record<string, Address>;
  readonly accepted: Address | undefined;
  readonly generationOutputs: ReadonlyMap<string, { readonly shared: boolean }>;
  readonly routed: ReadonlyMap<string, RoutedDefinition<Streamsy.Any["Identifier"]>>;
  readonly handlerOutputs: Record<
    string,
    Address | RoutedHandle<Address, DecodedParams, Streamsy.Any["Identifier"]>
  >;
}

export const resolveOutputs = (
  outputs: RoutedMap,
  acceptedOutput: AcceptedOutput | undefined,
  params: DecodedParams,
  key: ProjectionKey,
  inputs: InputMap,
): ResolvedBindings => {
  const generationOutputs = new Map<string, { readonly shared: boolean }>();

  const resolve = (name: string, output: RoutedOutput): Streamsy.Address<any, any> => {
    if (isPerGeneration(output)) {
      const { route, param, shared } = generationBinding(output);

      generationOutputs.set(name, { shared });

      return route.at({ ...params, [param]: key.generation });
    }

    return isDeclaration(output) ? output.at(params) : output;
  };

  const routed = new Map<string, ReturnType<typeof routedDefinition>>();

  const streams = Record.fromEntries(
    Array.flatMap(Record.toEntries(outputs), ([name, output]) => {
      if (isPerGeneration(output) && generationBinding(output).routeBy !== undefined) {
        routed.set(name, routedDefinition(generationBinding(output).route));

        return [];
      }

      return [[name, resolve(name, output)] as const];
    }),
  );

  const accepted = acceptedOutput === undefined ? undefined : resolve(acceptedKey, acceptedOutput);

  const addresses =
    accepted === undefined ? Record.values(streams) : [...Record.values(streams), accepted];

  const ids = Array.map(addresses, (address) => address.id);

  if (new Set(ids).size !== ids.length)
    throw new RangeError("Two outputs may not name the same stream");
  const inputIds = new Set(Array.map(Record.values(inputs), (address) => address.id));

  if (Array.some(ids, (id) => inputIds.has(id)))
    throw new RangeError("An output may not name an input stream");

  if (Array.some(addresses, (address) => !Predicate.isTagged(address, "Json")))
    throw new RangeError("Projection outputs require JSON or state routes");

  const occupied = new Set([...inputIds, ...ids]);

  const handles = Record.fromEntries(
    Array.flatMap(Record.toEntries(outputs), ([name, output]) => {
      if (!isPerGeneration(output)) return [];
      const { route, param, routeBy } = generationBinding(output);

      return routeBy === undefined
        ? []
        : [
            [
              name,
              routedHandle(
                name,
                route,
                routeBy,
                { ...params, [param]: key.generation },
                key,
                occupied,
                routed,
              ),
            ] as const,
          ];
    }),
  );

  return {
    streams,
    accepted,
    generationOutputs,
    routed,
    handlerOutputs: routed.size === 0 ? streams : { ...streams, ...handles },
  };
};

export const createOutput = (
  address: StreamAddress.StreamAddress<unknown>,
  wire: ResolvedOutput["service"]["wire"],
) =>
  Streams.create(address).pipe(
    Effect.provideService(StreamsWriter, wire),
    Effect.mapError(
      (cause) =>
        new ProjectionFault({
          phase: "process",
          reason: isStreamsFault(cause) ? "storage-failure" : "invalid-output",
          message: `Cannot create output ${address.id}`,
          cause,
        }),
    ),
  );

export interface BoundOutputs<R> extends GenerationOutputBindings, RoutedOutputBindings<R> {
  readonly streams?: Readonly<Record<string, Streamsy.Address<unknown, R>>>;
  readonly accepted?: Streamsy.Address<Streamsy.WakeEvent, R>;
}

export const boundOutputs = <R>(bound: BoundOutputs<R>) =>
  bound.accepted === undefined
    ? (bound.streams ?? {})
    : { ...bound.streams, [acceptedKey]: bound.accepted };

/** Create the generation-bound outputs of one binding, using each output's owner. */
export const createOutputs = <R>(bound: BoundOutputs<R>): Effect.Effect<void, ProjectionFault, R> =>
  Effect.gen(function* () {
    const bindings = bound[GenerationOutputs];

    if (bindings === undefined) return;

    for (const [name, address] of Record.toEntries(boundOutputs(bound))) {
      if (!bindings.has(name)) continue;
      const { wire } = yield* address.stream;

      yield* createOutput(address, wire);
    }
  });

export const collect = (
  outputs: Readonly<Record<string, Streamsy.Address<unknown, any>>>,
  buffers: Record<string, unknown[]>,
  emitted: Readonly<Record<string, ReadonlyArray<any>>>,
) =>
  Effect.gen(function* () {
    for (const [name, entries] of Record.toEntries(emitted)) {
      const output = outputs[name];

      if (
        !Record.has(outputs, name) ||
        output === undefined ||
        !Array.isArray<typeof entries>(entries)
      )
        return yield* new ProjectionFault({
          phase: "process",
          reason: "invalid-output",
          message: `Invalid output array ${name}`,
        });

      const values = yield* Effect.try({
        try: () =>
          "collections" in output
            ? // SAFETY: state addresses carry their collections; constructor signatures check each change against them. Controls pass through.
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

    return yield* Effect.void;
  });

const pin = (reason: ProjectionFault["reason"], message: string, cause?: unknown) =>
  new ProjectionFault({ phase: "pin", reason, message, cause });

/** Append one unit under its pinned tuple; appended and duplicate are one outcome. */
export const appendPinned = Effect.fn("Projection.appendPinned")(function* <O>(
  output: StreamAddress.StreamAddress<O>,
  wire: ResolvedOutput["service"]["wire"],
  items: Array.NonEmptyReadonlyArray<O>,
  position: Producer.Position,
) {
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
      StreamBusy: () => pin("output-busy", `Output ${output.id} is busy`),
    }),
  );
});

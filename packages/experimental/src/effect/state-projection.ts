import type { JsonValue, StreamBatch, StreamProtocolClient } from "@streamsy/core";
import { Context, Effect, Layer, Schema } from "effect";
import { bindStream } from "../binding.ts";
import { streamIdentity, type SourceAck, type StreamIdentity } from "../causal.ts";
import { AppendStreamsLive, ReadStreamsLive } from "../effect/streams.ts";
import { DerivedRecoveryLive } from "../ivm-mesh/derived-append.ts";
import { canonicalLaneInput, deriveProducerLane } from "../ivm-mesh/lane.ts";
import { NonNegativeInt, NormalizedRequiredText, PositiveInt } from "../ivm-mesh/schemas.ts";
import {
  catchUp as catchUpInternal,
  type CatchUpProgress as InternalProgress,
  type CatchUpResult as InternalResult,
  type ProjectionBoundary,
} from "../ivm-mesh/projection.ts";

export interface Definition<Input> {
  readonly id: string;
  readonly version: number;
  readonly input: Schema.Decoder<Input>;
  readonly project: (context: {
    readonly value: Input;
    readonly source: SourceAck;
    readonly index: number;
  }) => Iterable<JsonValue>;
}

export interface MakeOptions<Input> extends Definition<Input> {}

/** Transport-free identity and application id for one durable stream. */
export interface StreamResource {
  readonly identity: StreamIdentity;
  readonly streamId: string;
}

export interface StreamResourceOptions extends StreamResource {}

export interface Instance<Input> {
  readonly definition: Definition<Input>;
  readonly source: StreamResource;
  readonly target: StreamResource;
  readonly generation: string;
  readonly producerEpoch: number;
}

export interface InstanceOptions {
  readonly source: StreamResource;
  readonly target: StreamResource;
  readonly generation: string;
  readonly producerEpoch: number;
}

export interface Limits {
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export interface CatchUpOptions {
  readonly limits: Limits;
}

export interface CatchUpProgress {
  readonly sourceThrough?: string;
  readonly targetOffset: string;
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export type CatchUpOutcome =
  | { readonly status: "caught-up"; readonly progress: CatchUpProgress }
  | {
      readonly status: "limit-reached";
      readonly limit: keyof Limits;
      readonly progress: CatchUpProgress;
    }
  | {
      readonly status: "boundary-too-large";
      readonly limit: "items" | "bytes";
      readonly source: SourceAck;
      readonly actual: number;
      readonly maximum: number;
      readonly progress: CatchUpProgress;
    }
  | {
      readonly status: "missing" | "gone";
      readonly stream: "source" | "target";
      readonly progress?: CatchUpProgress;
    }
  | {
      readonly status: "output-conflict";
      readonly reason: string;
      readonly offset?: string;
      readonly progress: CatchUpProgress;
    }
  | {
      readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq";
      readonly progress: CatchUpProgress;
    };

interface ClientService {
  readonly client: StreamProtocolClient;
}

class Client extends Context.Service<Client, ClientService>()(
  "@streamsy/experimental/StateProjection/Client",
) {}

/** Declare stable projection identity and pure JSON-item-to-State logic. */
export function make<Input>(options: MakeOptions<Input>): Definition<Input> {
  const decodeText = Schema.decodeUnknownSync(NormalizedRequiredText);
  const decodePositiveInt = Schema.decodeUnknownSync(PositiveInt);
  const definition = {
    ...options,
    id: decodeText(options.id),
    version: decodePositiveInt(options.version),
  };
  return Object.freeze(definition);
}

/** Construct an inert stream resource without capturing transport authority. */
export function resource(options: StreamResourceOptions): StreamResource {
  return Object.freeze({
    identity: streamIdentity(options.identity.name),
    streamId: Schema.decodeUnknownSync(NormalizedRequiredText)(options.streamId),
  });
}

/** Supply the fixed client authority used to resolve resources during a run. */
export function layerClient(client: StreamProtocolClient): Layer.Layer<Client> {
  return Layer.succeed(Client, Client.of({ client }));
}

const StateProjectionLive = DerivedRecoveryLive.pipe(
  Layer.provide(ReadStreamsLive),
  Layer.merge(ReadStreamsLive),
  Layer.merge(AppendStreamsLive),
);

/** Bind a declaration to one immutable source, target, and output generation. */
export function instance<Input>(
  definition: Definition<Input>,
  options: InstanceOptions,
): Instance<Input> {
  const source = resource(options.source);
  const target = resource(options.target);
  const generation = Schema.decodeUnknownSync(NormalizedRequiredText)(options.generation);
  const producerEpoch = Schema.decodeUnknownSync(NonNegativeInt)(options.producerEpoch);

  // Validate the complete lane configuration synchronously while keeping the
  // derived producer id out of application-visible state.
  canonicalLaneInput({
    processorId: definition.id,
    processorVersion: String(definition.version),
    outputGeneration: generation,
    source: source.identity,
    target: target.identity,
    producerEpoch,
  });

  return Object.freeze({ definition, source, target, generation, producerEpoch });
}

/**
 * Run one bounded recovery and catch-up pass.
 *
 * Recovery currently scans complete target history, so its cost is O(history).
 * The public outcome intentionally omits producer and recovered-checkpoint data.
 */
export const catchUp = Effect.fn("StateProjection.catchUp")(
  <Input>(projection: Instance<Input>, options: CatchUpOptions) =>
    Effect.gen(function* () {
      Schema.decodeUnknownSync(
        Schema.Struct({
          pages: PositiveInt,
          batches: PositiveInt,
          items: PositiveInt,
          bytes: PositiveInt,
        }),
      )(options.limits);
      const { client } = yield* Client;
      const source = bindStream({ ...projection.source, client });
      const target = bindStream({ ...projection.target, client });
      const lane = yield* Effect.promise(() =>
        deriveProducerLane({
          processorId: projection.definition.id,
          processorVersion: String(projection.definition.version),
          outputGeneration: projection.generation,
          source: source.identity,
          target: target.identity,
          producerEpoch: projection.producerEpoch,
        }),
      );

      const outcome = yield* catchUpInternal({
        source,
        target,
        lane,
        limits: {
          maxPages: options.limits.pages,
          maxBatches: options.limits.batches,
          maxItems: options.limits.items,
          maxBytes: options.limits.bytes,
        },
        decode: (batch) => decodeJsonItems(projection.definition.input, batch),
        reduce: (items, boundary) => projectItems(projection.definition, items, boundary),
      });

      return toPublicOutcome(outcome);
    }).pipe(
      // @effect-diagnostics-next-line strictEffectProvide:off -- StateProjection.catchUp is the documented application facade that owns its fixed-client Live adapters.
      Effect.provide(StateProjectionLive),
    ),
);

function decodeJsonItems<Input>(
  schema: Schema.Decoder<Input>,
  batch: StreamBatch,
): readonly Input[] {
  if (batch.kind !== "json") throw new TypeError("StateProjection sources must contain JSON");
  const decode = Schema.decodeUnknownSync(schema);
  return batch.items.map((item) => decode(item));
}

function projectItems<Input>(
  definition: Definition<Input>,
  items: readonly Input[],
  boundary: ProjectionBoundary,
): readonly JsonValue[] {
  return items.flatMap((value, index) =>
    Array.from(definition.project({ value, source: boundary.source, index })),
  );
}

function toPublicOutcome(outcome: InternalResult): CatchUpOutcome {
  const progress = hasProgress(outcome) ? publicProgress(outcome) : undefined;

  switch (outcome.status) {
    case "caught-up":
      return { status: outcome.status, progress: publicProgress(outcome) };
    case "limit-reached":
      return {
        status: outcome.status,
        limit: publicLimit(outcome.limit),
        progress: publicProgress(outcome),
      };
    case "boundary-too-large":
      return {
        status: outcome.status,
        limit: outcome.limit === "maxItems" ? "items" : "bytes",
        source: outcome.source,
        actual: outcome.actual,
        maximum: outcome.maximum,
        progress: publicProgress(outcome),
      };
    case "missing":
    case "gone":
      return {
        status: outcome.status,
        stream: outcome.stream,
        ...(progress === undefined ? {} : { progress }),
      };
    case "output-conflict":
      return {
        status: outcome.status,
        reason: outcome.reason,
        ...(outcome.offset === undefined ? {} : { offset: outcome.offset }),
        progress: publicProgress(outcome),
      };
    case "stale-epoch":
    case "producer-gap":
    case "invalid-epoch-seq":
      return { status: outcome.status, progress: publicProgress(outcome) };
  }
  return exhaustive(outcome);
}

function hasProgress(outcome: InternalResult): outcome is InternalResult & InternalProgress {
  return "checkpoint" in outcome;
}

function publicProgress(progress: InternalProgress): CatchUpProgress {
  return {
    ...(progress.checkpoint.sourceThrough === undefined
      ? {}
      : { sourceThrough: progress.checkpoint.sourceThrough }),
    targetOffset: progress.checkpoint.targetOffset,
    pages: progress.pages,
    batches: progress.batches,
    items: progress.items,
    bytes: progress.bytes,
  };
}

function publicLimit(limit: keyof import("../ivm-mesh/projection.ts").CatchUpLimits): keyof Limits {
  switch (limit) {
    case "maxPages":
      return "pages";
    case "maxBatches":
      return "batches";
    case "maxItems":
      return "items";
    case "maxBytes":
      return "bytes";
  }
  return exhaustive(limit);
}

function exhaustive(value: never): never {
  throw new TypeError(`Unexpected StateProjection variant: ${String(value)}`);
}

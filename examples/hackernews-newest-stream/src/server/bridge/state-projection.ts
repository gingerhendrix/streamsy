// STEP 1 BRIDGE: private, example-local, delete in Step 5.
import {
  StreamRef,
  Streams,
  StreamsReader,
  StreamsWriter,
  ZERO_OFFSET,
  isValid,
} from "@streamsy/core-next";
import { Effect, Schema, Stream } from "effect";
const Positive = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThan(0));
const NonNegative = Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0));
export interface SourceAck {
  readonly position: string;
}
export type ProjectionServices = StreamsReader | StreamsWriter;
export interface Definition<Input> {
  readonly id: string;
  readonly version: number;
  readonly input: Schema.Decoder<Input>;
  readonly project: (context: {
    readonly value: Input;
    readonly source: SourceAck;
    readonly index: number;
  }) => Iterable<Schema.Json>;
}

export interface MakeOptions<Input> extends Definition<Input> {}

/** Transport-free identity and application id for one durable stream. */
export interface StreamResource {
  readonly ref: StreamRef.StreamRef<unknown>;
  readonly streamId: string;
}

export interface StreamResourceOptions {
  readonly streamId: string;
}

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

const LimitsSchema = Schema.Struct({
  pages: Positive,
  batches: Positive,
  items: Positive,
  bytes: Positive,
});

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

export const make = <Input>(options: Definition<Input>): Definition<Input> =>
  Object.freeze({
    ...options,
    id: Schema.decodeSync(Schema.NonEmptyString)(options.id.trim()),
    version: Schema.decodeSync(Positive)(options.version),
  });
export const resource = (options: StreamResourceOptions): StreamResource => {
  const streamId = Schema.decodeSync(Schema.NonEmptyString)(options.streamId.trim());
  return Object.freeze({ streamId, ref: StreamRef.json(streamId, { schema: Schema.Unknown }) });
};
export const instance = <Input>(
  definition: Definition<Input>,
  options: InstanceOptions,
): Instance<Input> =>
  Object.freeze({
    definition,
    source: resource(options.source),
    target: resource(options.target),
    generation: Schema.decodeSync(Schema.NonEmptyString)(options.generation.trim()),
    producerEpoch: Schema.decodeSync(NonNegative)(options.producerEpoch),
  });
class BridgeFault extends Schema.TaggedError<BridgeFault>()("BridgeFault", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
const FactPosition = Schema.Struct({ headers: Schema.Struct({ offset: Schema.String }) });

/** Whole target recovery, then whole source read pages. Limits count source pages only. */
export const catchUp = Effect.fn("StateProjection.catchUp")(function* <Input>(
  projection: Instance<Input>,
  options: CatchUpOptions,
): Effect.fn.Return<
  CatchUpOutcome,
  | import("@streamsy/core-next").StorageFault
  | import("@streamsy/core-next").EncodeFault
  | import("@streamsy/core-next").DecodeFault
  | import("@streamsy/core-next").StreamUnavailable
  | Schema.SchemaError
  | BridgeFault,
  ProjectionServices
> {
  const limits = yield* Schema.decodeEffect(LimitsSchema)(options.limits);
  const reader = yield* StreamsReader;
  const target = yield* reader.head(projection.target.ref.id);
  if (target.status !== "ok")
    return { status: target.status === "not-found" ? "missing" : "gone", stream: "target" };
  const recovery = yield* Stream.runCollect(Streams.read(projection.target.ref));
  let targetOffset: string = ZERO_OFFSET;
  let sourceThrough: string | undefined;
  for (const batch of recovery) {
    targetOffset = batch.nextOffset;
    for (const fact of batch.items) {
      const position = yield* Schema.decodeUnknownEffect(FactPosition)(fact);
      if (!isValid(position.headers.offset))
        return yield* new BridgeFault({ message: "Invalid recovered source offset" });
      sourceThrough = position.headers.offset;
    }
  }
  let progress: CatchUpProgress = {
    targetOffset,
    sourceThrough,
    pages: 0,
    batches: 0,
    items: 0,
    bytes: 0,
  };
  for (;;) {
    const read = yield* reader.read(projection.source.ref.id, { offset: progress.sourceThrough });
    if (read.status !== "ok")
      return {
        status: read.status === "not-found" ? "missing" : "gone",
        stream: "source",
        progress,
      };
    if (read.messages.length === 0) return { status: "caught-up", progress };
    for (const limit of ["pages", "batches"] as const)
      if (progress[limit] >= limits[limit]) return { status: "limit-reached", limit, progress };
    const source = { position: read.nextOffset };
    const values = yield* Effect.forEach(read.messages, (message) =>
      Schema.decodeEffect(projection.source.ref.codec)(new TextDecoder().decode(message.data)),
    );
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.Unknown)))(
      values,
    );
    const bytes = new TextEncoder().encode(encoded).byteLength;
    if (bytes > limits.bytes)
      return {
        status: "boundary-too-large",
        limit: "bytes",
        source,
        actual: bytes,
        maximum: limits.bytes,
        progress,
      };
    if (progress.bytes + bytes > limits.bytes)
      return { status: "limit-reached", limit: "bytes", progress };
    const items = yield* Effect.forEach(values, (value) =>
      Schema.decodeUnknownEffect(projection.definition.input)(value),
    );
    if (items.length > limits.items)
      return {
        status: "boundary-too-large",
        limit: "items",
        source,
        actual: items.length,
        maximum: limits.items,
        progress,
      };
    if (progress.items + items.length > limits.items)
      return { status: "limit-reached", limit: "items", progress };
    const facts = yield* Effect.try({
      try: () =>
        items.flatMap((value, index) =>
          Array.from(projection.definition.project({ value, source, index })),
        ),
      catch: (cause) => new BridgeFault({ message: "Projection failed", cause }),
    });
    // This private bridge has no lineage/checkpoint rows. A nonempty fact batch
    // with the correct boundary is necessary to recover progress on the next run.
    if (facts.length === 0)
      return yield* new BridgeFault({
        message: "Bridge requires at least one fact per source boundary",
      });
    for (const fact of facts) {
      const position = yield* Schema.decodeUnknownEffect(FactPosition)(fact);
      if (position.headers.offset !== source.position)
        return yield* new BridgeFault({ message: "Fact offset must match source boundary" });
    }
    const appended = yield* Streams.append(projection.target.ref, facts, {
      expectedOffset: progress.targetOffset,
    });
    if (appended.status === "not-found" || appended.status === "gone")
      return {
        status: appended.status === "not-found" ? "missing" : "gone",
        stream: "target",
        progress,
      };
    if (appended.status !== "appended") {
      const conflict = {
        status: "output-conflict" as const,
        reason: appended.status === "conflict" ? appended.conflictReason : appended.status,
        progress,
      };
      return "offset" in appended ? { ...conflict, offset: appended.offset } : conflict;
    }
    progress = {
      targetOffset: appended.offset,
      sourceThrough: source.position,
      pages: progress.pages + 1,
      batches: progress.batches + 1,
      items: progress.items + items.length,
      bytes: progress.bytes + bytes,
    };
    if (read.upToDate) return { status: "caught-up", progress };
  }
});

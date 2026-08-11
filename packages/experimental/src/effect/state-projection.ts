import type { JsonValue, StreamBatch } from "@streamsy/core";
import { Effect, Schema } from "effect";
import type { StreamBinding } from "../binding.ts";
import type { SourceAck } from "../causal.ts";
import { DerivedRecoveryLive } from "../ivm-mesh/derived-append.ts";
import { canonicalLaneInput, deriveProducerLane } from "../ivm-mesh/lane.ts";
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

export interface Instance<Input> {
  readonly definition: Definition<Input>;
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly generation: string;
  readonly producerEpoch: number;
}

export interface InstanceOptions {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
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

/** Declare stable projection identity and pure JSON-item-to-State logic. */
export function make<Input>(options: MakeOptions<Input>): Definition<Input> {
  const definition = {
    ...options,
    id: requiredText(options.id, "id"),
    version: positiveSafeInteger(options.version, "version"),
  };
  return Object.freeze(definition);
}

/** Bind a declaration to one immutable source, target, and output generation. */
export function instance<Input>(
  definition: Definition<Input>,
  options: InstanceOptions,
): Instance<Input> {
  const generation = requiredText(options.generation, "generation");
  const producerEpoch = nonNegativeSafeInteger(options.producerEpoch, "producerEpoch");

  // Validate the complete lane configuration synchronously while keeping the
  // derived producer id out of application-visible state.
  canonicalLaneInput({
    processorId: definition.id,
    processorVersion: String(definition.version),
    outputGeneration: generation,
    source: options.source.identity,
    target: options.target.identity,
    producerEpoch,
  });

  return Object.freeze({ ...options, definition, generation, producerEpoch });
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
      validateLimits(options.limits);
      const lane = yield* Effect.promise(() =>
        deriveProducerLane({
          processorId: projection.definition.id,
          processorVersion: String(projection.definition.version),
          outputGeneration: projection.generation,
          source: projection.source.identity,
          target: projection.target.identity,
          producerEpoch: projection.producerEpoch,
        }),
      );

      const outcome = yield* catchUpInternal({
        source: projection.source,
        target: projection.target,
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
    }).pipe(Effect.provide(DerivedRecoveryLive)),
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
}

function validateLimits(limits: Limits): void {
  positiveSafeInteger(limits.pages, "limits.pages");
  positiveSafeInteger(limits.batches, "limits.batches");
  positiveSafeInteger(limits.items, "limits.items");
  positiveSafeInteger(limits.bytes, "limits.bytes");
}

function requiredText(value: string, name: string): string {
  if (typeof value !== "string" || value.length === 0) throw new TypeError(`${name} is required`);
  if (value.length > 512) throw new TypeError(`${name} must not exceed 512 code units`);
  return value.normalize("NFC");
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function nonNegativeSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  return value;
}

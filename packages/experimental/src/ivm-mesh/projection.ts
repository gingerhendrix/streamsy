import type { ClientFailure, JsonValue, StreamBatch } from "@streamsy/core";
import type { StreamBinding } from "../binding.ts";
import { sourceAck, streamIdentityEquals, type SourceAck } from "../causal.ts";
import {
  appendDerivedStateBatch,
  recoverDerivedState,
  type AppendDerivedStateResult,
  type RecoveredDerivedState,
} from "./derived-append.ts";
import type { ProducerLane } from "./lane.ts";

export interface CatchUpLimits {
  /** Maximum decoded source items incorporated by this invocation. */
  readonly maxItems: number;
  /** Maximum source delivery pages incorporated by this invocation. */
  readonly maxPages: number;
  /** Maximum derived State transactions committed by this invocation. */
  readonly maxBatches: number;
  /** Maximum encoded source bytes incorporated by this invocation. */
  readonly maxBytes: number;
}

export interface ProjectionBoundary {
  readonly source: SourceAck;
  readonly page: number;
  readonly bytes: number;
}

export interface CatchUpOptions<Input> {
  readonly source: StreamBinding;
  readonly target: StreamBinding;
  readonly lane: ProducerLane;
  readonly limits: CatchUpLimits;
  readonly decode: (batch: StreamBatch, boundary: ProjectionBoundary) => Iterable<Input>;
  readonly reduce: (items: readonly Input[], boundary: ProjectionBoundary) => Iterable<JsonValue>;
  readonly signal?: AbortSignal;
}

export interface CatchUpProgress {
  readonly checkpoint: RecoveredDerivedState;
  readonly pages: number;
  readonly batches: number;
  readonly items: number;
  readonly bytes: number;
}

export type CatchUpResult =
  | ({ readonly status: "caught-up" } & CatchUpProgress)
  | ({
      readonly status: "limit-reached";
      readonly limit: keyof CatchUpLimits;
    } & CatchUpProgress)
  | ({
      /** This boundary cannot fit even in a fresh invocation with the configured limit. */
      readonly status: "boundary-too-large";
      readonly limit: "maxItems" | "maxBytes";
      readonly source: SourceAck;
      readonly actual: number;
      readonly maximum: number;
    } & CatchUpProgress)
  | ({
      readonly status: "cancelled";
      readonly phase: "recovery" | "read" | "decode" | "reduce" | "append" | "after-commit";
      readonly durableProgress: "none" | "committed" | "unknown";
    } & Partial<CatchUpProgress>)
  | ({
      readonly status: "poison";
      readonly phase: "decode" | "reduce";
      readonly source: SourceAck;
      readonly cause: unknown;
    } & CatchUpProgress)
  | ({
      readonly status: "malformed-input";
      readonly stream: "source";
      readonly offset: string;
      readonly cause: unknown;
    } & CatchUpProgress)
  | ({
      readonly status: "missing";
      readonly stream: "source" | "target";
    } & Partial<CatchUpProgress>)
  | ({ readonly status: "gone"; readonly stream: "source" | "target" } & Partial<CatchUpProgress>)
  | ({
      readonly status: "retryable";
      readonly phase: "recovery" | "read" | "append";
      readonly failure: ClientFailure;
    } & Partial<CatchUpProgress>)
  | ({
      readonly status: "failed";
      readonly phase: "recovery" | "read" | "append";
      readonly failure: ClientFailure;
    } & Partial<CatchUpProgress>)
  | ({
      readonly status: "output-conflict";
      readonly reason: string;
      readonly offset?: string;
    } & CatchUpProgress)
  | (Extract<
      AppendDerivedStateResult,
      { readonly status: "stale-epoch" | "producer-gap" | "invalid-epoch-seq" }
    > &
      CatchUpProgress)
  | ({
      readonly status: "incompatible-output" | "malformed-output";
      readonly message: string;
    } & Partial<CatchUpProgress>);

/**
 * Incorporate complete source delivery boundaries into a derived State stream.
 *
 * Recovery is intentionally O(target history). Reads resume after the durable
 * source checkpoint, and every processed delivery boundary produces exactly
 * one State transaction, including filtered boundaries with no fact changes.
 */
export async function catchUp<Input>(options: CatchUpOptions<Input>): Promise<CatchUpResult> {
  validateOptions(options);
  const signal = options.signal;
  if (signal?.aborted) return cancelledWithoutCheckpoint("recovery");

  const recovered = await recoverDerivedState(options.target, options.lane, signal);
  if (recovered.status !== "ready") return recoveryFailure(recovered);
  let progress: CatchUpProgress = {
    checkpoint: recovered,
    pages: 0,
    batches: 0,
    items: 0,
    bytes: 0,
  };
  if (signal?.aborted) return cancelled(progress, "read", "none");

  const read = await options.source.client.stream(options.source.streamId).read({
    ...(recovered.sourceThrough === undefined ? {} : { offset: recovered.sourceThrough }),
    live: false,
    signal,
  });
  if (read.status === "not-found") return { status: "missing", stream: "source", ...progress };
  if (read.status === "gone") return { status: "gone", stream: "source", ...progress };
  if (read.status === "error") return clientFailure(read, "read", progress);

  for await (const batch of read.session) {
    if (signal?.aborted) {
      read.session.cancel(signal.reason);
      return cancelled(progress, "read", "none");
    }
    if (!hasSourcePayload(batch)) continue;
    if (progress.pages >= options.limits.maxPages) {
      read.session.cancel("projection page limit reached");
      return { status: "limit-reached", limit: "maxPages", ...progress };
    }
    if (progress.batches >= options.limits.maxBatches) {
      read.session.cancel("projection batch limit reached");
      return { status: "limit-reached", limit: "maxBatches", ...progress };
    }

    let ack: SourceAck;
    try {
      ack = sourceAck(options.source.identity, batch.offset);
    } catch (cause) {
      read.session.cancel(cause);
      return malformedSourceBoundary(progress, batch.offset, cause);
    }
    const bytes = encodedBatchBytes(batch);
    if (bytes > options.limits.maxBytes) {
      read.session.cancel("source boundary exceeds projection byte limit");
      return {
        status: "boundary-too-large",
        limit: "maxBytes",
        source: ack,
        actual: bytes,
        maximum: options.limits.maxBytes,
        ...progress,
      };
    }
    if (progress.bytes + bytes > options.limits.maxBytes) {
      read.session.cancel("projection byte limit reached");
      return { status: "limit-reached", limit: "maxBytes", ...progress };
    }
    const boundary: ProjectionBoundary = { source: ack, page: progress.pages + 1, bytes };

    let items: readonly Input[];
    try {
      items = Array.from(options.decode(batch, boundary));
    } catch (cause) {
      read.session.cancel(cause);
      return { status: "poison", phase: "decode", source: ack, cause, ...progress };
    }
    if (signal?.aborted) {
      read.session.cancel(signal.reason);
      return cancelled(progress, "decode", "none");
    }
    if (items.length > options.limits.maxItems) {
      read.session.cancel("source boundary exceeds projection item limit");
      return {
        status: "boundary-too-large",
        limit: "maxItems",
        source: ack,
        actual: items.length,
        maximum: options.limits.maxItems,
        ...progress,
      };
    }
    if (progress.items + items.length > options.limits.maxItems) {
      read.session.cancel("projection item limit reached");
      return { status: "limit-reached", limit: "maxItems", ...progress };
    }

    let facts: readonly JsonValue[];
    try {
      facts = Array.from(options.reduce(items, boundary));
    } catch (cause) {
      read.session.cancel(cause);
      return { status: "poison", phase: "reduce", source: ack, cause, ...progress };
    }
    if (signal?.aborted) {
      read.session.cancel(signal.reason);
      return cancelled(progress, "append", "none");
    }

    const appended = await appendDerivedStateBatch({
      target: options.target,
      lane: options.lane,
      previous: progress.checkpoint,
      sourceThrough: ack.position,
      facts,
      signal,
    });
    if (appended.status !== "appended" && appended.status !== "sequence-already-accepted") {
      read.session.cancel("projection output did not commit");
      return appendFailure(appended, progress, signal);
    }
    progress = {
      checkpoint: appended.checkpoint,
      pages: progress.pages + 1,
      batches: progress.batches + 1,
      items: progress.items + items.length,
      bytes: progress.bytes + bytes,
    };
    if (signal?.aborted) {
      read.session.cancel(signal.reason);
      return cancelled(progress, "after-commit", "committed");
    }
  }

  const ended = await read.session.done;
  if (ended.status === "done") return { status: "caught-up", ...progress };
  if (ended.status === "cancelled") return cancelled(progress, "read", "none");
  return clientFailure(ended, "read", progress);
}

function validateOptions<Input>(options: CatchUpOptions<Input>): void {
  if (!streamIdentityEquals(options.source.identity, options.lane.source)) {
    throw new TypeError("Source binding identity does not match the producer lane");
  }
  if (!streamIdentityEquals(options.target.identity, options.lane.target)) {
    throw new TypeError("Target binding identity does not match the producer lane");
  }
  for (const [name, value] of Object.entries(options.limits)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new TypeError(`${name} must be a positive safe integer`);
    }
  }
}

function hasSourcePayload(batch: StreamBatch): boolean {
  if (batch.kind === "json") return batch.items.length > 0;
  if (batch.kind === "text") return batch.text.length > 0;
  return batch.data.byteLength > 0;
}

function encodedBatchBytes(batch: StreamBatch): number {
  if (batch.kind === "json")
    return new TextEncoder().encode(JSON.stringify(batch.items)).byteLength;
  if (batch.kind === "text") return new TextEncoder().encode(batch.text).byteLength;
  return batch.data.byteLength;
}

function cancelled(
  progress: CatchUpProgress,
  phase: Extract<CatchUpResult, { status: "cancelled" }>["phase"],
  durableProgress: Extract<CatchUpResult, { status: "cancelled" }>["durableProgress"],
): CatchUpResult {
  return {
    status: "cancelled",
    phase,
    durableProgress:
      durableProgress === "none" && progress.batches > 0 ? "committed" : durableProgress,
    ...progress,
  };
}

function cancelledWithoutCheckpoint(
  phase: "recovery",
): Extract<CatchUpResult, { status: "cancelled" }> {
  return {
    status: "cancelled",
    phase,
    durableProgress: "none",
  };
}

function recoveryFailure(
  result: Exclude<Awaited<ReturnType<typeof recoverDerivedState>>, RecoveredDerivedState>,
): CatchUpResult {
  if (result.status === "not-found") return { status: "missing", stream: "target" };
  if (result.status === "gone") return { status: "gone", stream: "target" };
  if (result.status === "cancelled") return cancelledWithoutCheckpoint("recovery");
  if (result.status === "error") return clientFailure(result, "recovery");
  if ("message" in result) return { status: result.status, message: result.message };
  throw new TypeError(`Unhandled recovery result: ${result.status}`);
}

function appendFailure(
  result: AppendDerivedStateResult,
  progress: CatchUpProgress,
  signal?: AbortSignal,
): CatchUpResult {
  if (result.status === "appended" || result.status === "sequence-already-accepted") {
    throw new TypeError("appendFailure received a successful append result");
  }
  if (result.status === "not-found") return { status: "missing", stream: "target", ...progress };
  if (result.status === "gone") return { status: "gone", stream: "target", ...progress };
  if (result.status === "cancelled") return cancelled(progress, "append", "unknown");
  if (result.status === "error") {
    if (result.code === "aborted" || signal?.aborted) {
      return cancelled(progress, "append", "unknown");
    }
    return clientFailure(result, "append", progress);
  }
  if (result.status === "output-conflict") return { ...result, ...progress };
  if (result.status === "malformed-output" || result.status === "incompatible-output") {
    return { ...result, ...progress };
  }
  if (
    result.status === "stale-epoch" ||
    result.status === "producer-gap" ||
    result.status === "invalid-epoch-seq"
  ) {
    return { ...result, ...progress };
  }
  return {
    status: "output-conflict",
    reason: result.status,
    ...progress,
  };
}

function clientFailure(
  failure: ClientFailure,
  phase: "recovery" | "read" | "append",
  progress?: CatchUpProgress,
): CatchUpResult {
  return {
    status: failure.retryable ? "retryable" : "failed",
    phase,
    failure,
    ...progress,
  };
}

function malformedSourceBoundary(
  progress: CatchUpProgress,
  offset: string,
  cause: unknown,
): CatchUpResult {
  return {
    status: "malformed-input",
    stream: "source",
    offset,
    cause,
    ...progress,
  };
}

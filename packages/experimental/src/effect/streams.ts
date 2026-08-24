import type {
  AppendJsonBatchOptions,
  AppendStreamOptions,
  ClientAppendResult,
  ClientCreateResult,
  ClientReadResult,
  CreateStreamOptions,
  JsonValue,
  ReadEndResult,
  ReadStreamOptions,
  StreamBatch,
} from "@streamsy/core";
import { Context, Effect, Layer, Schema, type Scope } from "effect";
import type { StreamBinding } from "../binding.ts";
import { StreamAppendError, StreamCreateError, StreamReadError } from "./errors.ts";

export type StreamCancellationReason = Schema.Schema.Type<ReturnType<typeof Schema.Defect>>;

export interface EffectReadSession<T extends JsonValue = JsonValue> {
  readonly contentType?: string;
  readonly startOffset?: string;
  readonly next: Effect.Effect<IteratorResult<StreamBatch<T>>, StreamReadError>;
  readonly done: Effect.Effect<Exclude<ReadEndResult, { status: "error" }>, StreamReadError>;
  readonly cancel: (reason?: StreamCancellationReason) => Effect.Effect<void>;
}

export type ReadOpenResult<T extends JsonValue = JsonValue> =
  | { readonly status: "ok"; readonly session: EffectReadSession<T> }
  | { readonly status: "not-found" | "gone" };
export type CreateOutcome = Exclude<ClientCreateResult, { status: "error" }>;
export type AppendOutcome = Exclude<ClientAppendResult, { status: "error" }>;

export interface CreateStreamsService {
  readonly create: (
    binding: StreamBinding,
    options?: CreateStreamOptions,
  ) => Effect.Effect<CreateOutcome, StreamCreateError>;
}

export class CreateStreams extends Context.Service<CreateStreams, CreateStreamsService>()(
  "@streamsy/experimental/CreateStreams",
) {}

export interface ReadStreamsService {
  /**
   * Acquire a finite read session in the current Scope.
   *
   * Every successful `ok` acquisition registers exactly one session cancel
   * finalizer before the result becomes visible to the caller. Callers own the
   * surrounding workflow with `Effect.scoped`; they never manually cancel.
   */
  readonly open: (
    binding: StreamBinding,
    options?: ReadStreamOptions,
  ) => Effect.Effect<ReadOpenResult, StreamReadError, Scope.Scope>;
}

export class ReadStreams extends Context.Service<ReadStreams, ReadStreamsService>()(
  "@streamsy/experimental/ReadStreams",
) {}

export interface AppendStreamsService {
  readonly append: (
    binding: StreamBinding,
    data: Uint8Array | string,
    options?: AppendStreamOptions,
  ) => Effect.Effect<AppendOutcome, StreamAppendError>;
  readonly appendJsonBatch: (
    binding: StreamBinding,
    items: readonly JsonValue[],
    options?: AppendJsonBatchOptions,
  ) => Effect.Effect<AppendOutcome, StreamAppendError>;
}

export class AppendStreams extends Context.Service<AppendStreams, AppendStreamsService>()(
  "@streamsy/experimental/AppendStreams",
) {}

function createPromise<A>(operation: string, run: (signal: AbortSignal) => Promise<A>) {
  return Effect.tryPromise({
    try: run,
    catch: (cause) => StreamCreateError.from(operation, cause),
  });
}

function readPromise<A>(operation: string, run: (signal: AbortSignal) => Promise<A>) {
  return Effect.tryPromise({ try: run, catch: (cause) => StreamReadError.from(operation, cause) });
}

function appendPromise<A>(operation: string, run: (signal: AbortSignal) => Promise<A>) {
  return Effect.tryPromise({
    try: run,
    catch: (cause) => StreamAppendError.from(operation, cause),
  });
}

/** Register ownership atomically for every successful session acquisition. */
function scopedReadOpen(
  acquire: Effect.Effect<ReadOpenResult, StreamReadError>,
): Effect.Effect<ReadOpenResult, StreamReadError, Scope.Scope> {
  return Effect.acquireRelease(
    acquire,
    (opened) =>
      opened.status === "ok"
        ? opened.session.cancel("read scope closed").pipe(Effect.ignoreCause)
        : Effect.void,
    { interruptible: true },
  );
}

export const CreateStreamsLive = Layer.succeed(
  CreateStreams,
  CreateStreams.of({
    create: Effect.fn("CreateStreams.create")((binding, options) =>
      createPromise("create", (signal) =>
        binding.client.stream(binding.streamId).create({ ...options, signal }),
      ).pipe(
        Effect.flatMap((result: ClientCreateResult) =>
          result.status === "error"
            ? Effect.fail(StreamCreateError.from("create", result))
            : Effect.succeed(result),
        ),
      ),
    ),
  }),
);

export const ReadStreamsLive = Layer.succeed(
  ReadStreams,
  ReadStreams.of({
    open: Effect.fn("ReadStreams.open")((binding, options) =>
      scopedReadOpen(
        Effect.gen(function* () {
          const result: ClientReadResult = yield* readPromise("open", (signal) =>
            binding.client.stream(binding.streamId).read({ ...options, signal }),
          );
          if (result.status === "error") return yield* StreamReadError.from("open", result);
          if (result.status !== "ok") return result;
          const iterator = result.session[Symbol.asyncIterator]();
          return {
            status: "ok" as const,
            session: {
              contentType: result.session.contentType,
              startOffset: result.session.startOffset,
              next: readPromise("next", () => iterator.next()),
              done: readPromise("done", () => result.session.done).pipe(
                Effect.flatMap((ended: ReadEndResult) =>
                  ended.status === "error"
                    ? Effect.fail(StreamReadError.from("done", ended))
                    : Effect.succeed(ended),
                ),
              ),
              cancel: (reason?: StreamCancellationReason) =>
                Effect.sync(() => result.session.cancel(reason)),
            },
          };
        }),
      ),
    ),
  }),
);

export const AppendStreamsLive = Layer.succeed(
  AppendStreams,
  AppendStreams.of({
    append: Effect.fn("AppendStreams.append")((binding, data, options) =>
      appendPromise("append", (signal) =>
        binding.client.stream(binding.streamId).append(data, { ...options, signal }),
      ).pipe(
        Effect.flatMap((result: ClientAppendResult) =>
          result.status === "error"
            ? Effect.fail(StreamAppendError.from("append", result))
            : Effect.succeed(result),
        ),
      ),
    ),
    appendJsonBatch: Effect.fn("AppendStreams.appendJsonBatch")((binding, items, options) =>
      appendPromise("appendJsonBatch", (signal) =>
        binding.client.stream(binding.streamId).appendJsonBatch(items, { ...options, signal }),
      ).pipe(
        Effect.flatMap((result: ClientAppendResult) =>
          result.status === "error"
            ? Effect.fail(StreamAppendError.from("appendJsonBatch", result))
            : Effect.succeed(result),
        ),
      ),
    ),
  }),
);

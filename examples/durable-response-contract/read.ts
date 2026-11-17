import { stream } from "@durable-streams/client";
import { Array, Effect, Queue, Schema, Stream } from "effect";
import { ResponseEntry } from "./entry-schema.ts";
import { foldResponse, type ReadBatch, type ResumeSnapshot } from "./fold.ts";
import { responsePath } from "./api.ts";

/** Decode every part before advancing the offset. Bad wire data never silently loses bytes. */
export const applyBatch = (state: ResumeSnapshot, batch: ReadBatch): ResumeSnapshot => {
  if (state.closed) return state;

  const parts =
    batch.offset === state.offset
      ? []
      : Array.map(batch.items, (part) => Schema.decodeUnknownSync(ResponseEntry)(part));

  const view = Array.reduce(parts, state.view, foldResponse);

  return {
    offset: batch.offset,
    closed: batch.closed,
    view:
      batch.closed && !view.terminal
        ? { ...view, terminal: true, status: "Error: stream closed without a terminal record" }
        : view,
  };
};

/** The compatibility edge owns the Durable Streams client's native fetch and cancellation. */
export const streamReader = (
  id: string,
  offset: string,
  origin = window.location.origin,
): Stream.Stream<ReadBatch, string> =>
  Stream.callback<ReadBatch, string>((queue) =>
    Effect.gen(function* () {
      const opened = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: (signal) =>
            stream({
              url: new URL(responsePath(id), origin),
              offset,
              live: true,
              json: true,
              signal,
              warnOnHttp: false,
            }),
          catch: () => "Unable to open response",
        }),
        (response) => Effect.sync(() => response.cancel()),
      );

      opened.subscribeJson((batch) => {
        Queue.offerUnsafe(queue, {
          items: batch.items,
          offset: batch.offset,
          closed: batch.streamClosed,
        });

        if (batch.streamClosed) Queue.endUnsafe(queue);
      });
      yield* Effect.tryPromise({ try: () => opened.closed, catch: () => "Response read failed" });

      // The client can resolve `closed` before its final response body is consumed.
      // Only the decoded closed batch ends our queue; keep the body alive until then.
      return yield* Effect.never;
    }).pipe(Effect.catch((error) => Queue.fail(queue, error))),
  );

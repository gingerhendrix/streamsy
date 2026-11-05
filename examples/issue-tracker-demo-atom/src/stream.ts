import { stream, type JsonBatch } from "@durable-streams/client";
import { Array, Effect, Queue, Schema, Stream } from "effect";
import { StateEvent, type JsonValue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { trackConnection } from "./connection.ts";
import { ReadEvent } from "./domain/workspace.ts";

const decodeChange = Schema.decodeUnknownResult(StateEvent);

/**
 * The changes in one read batch. Items that are not changes, such as State
 * controls, are skipped: this server never writes them.
 */
export const batchEvent = (batch: JsonBatch<JsonValue>): ReadEvent =>
  ReadEvent.Batch({
    changes: Array.filterMap(batch.items, (item) => decodeChange(item)),
    isUpToDate: batch.upToDate,
  });

/** Whether the stream reader reaches the server, for the "Offline" badge. */
export const connection = trackConnection();

/**
 * Read the workspace's State stream from the start and stay live. Closing the
 * Stream cancels the read. The reader retries a network failure with backoff
 * and never gives up, so `Failed` means that the server refused the read.
 */
export const streamReader = (workspaceId: string): Stream.Stream<ReadEvent> =>
  Stream.callback<ReadEvent>((queue) =>
    Effect.gen(function* () {
      const url = yield* Effect.sync(
        () =>
          new URL(`/streams/workspace/${encodeURIComponent(workspaceId)}`, window.location.origin),
      );

      const response = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: (signal) =>
            stream<JsonValue>({
              url,
              live: true,
              json: true,
              signal,
              warnOnHttp: false,
              fetch: connection.fetch,
            }),
          catch: () => "Unable to open the workspace stream",
        }),
        (opened) => Effect.sync(() => opened.cancel()),
      );

      response.subscribeJson<JsonValue>((batch) => {
        Queue.offerUnsafe(queue, batchEvent(batch));
      });

      yield* Effect.tryPromise({
        try: () => response.closed,
        catch: () => "The workspace stream stopped",
      });
    }).pipe(Effect.catch((error) => Queue.offer(queue, ReadEvent.Failed({ error })))),
  );

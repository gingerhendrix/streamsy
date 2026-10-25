import { stream, type JsonBatch } from "@durable-streams/client";
import { Array, Effect, Queue, Schema, Stream } from "effect";
import { Subscription } from "foldkit";
import { StateEvent, type JsonValue } from "@streamsy/issue-tracker-demo-contract/state-schema";
import { Message } from "./message.ts";
import type { Model } from "./model.ts";

const decodeChange = Schema.decodeUnknownResult(StateEvent);

/**
 * The changes in one read batch. Items that are not changes, such as State
 * controls, are skipped: this server never writes them.
 */
export const batchMessage = (batch: JsonBatch<JsonValue>): Message =>
  Message.ReceivedStateBatch({
    changes: Array.filterMap(batch.items, (item) => decodeChange(item)),
    isUpToDate: batch.upToDate,
  });

/**
 * A fetch for the stream reader that reports when the server stops or starts
 * answering. Any HTTP answer, even an error status, means the server is
 * reachable. A rejected fetch is a network failure, unless the reader aborted
 * it on close. The reader retries with backoff and never gives up.
 */
const trackedFetch = (report: (isOnline: boolean) => void): typeof globalThis.fetch => {
  let isOnline = true;

  const set = (next: boolean) => {
    if (next === isOnline) return;
    isOnline = next;
    report(next);
  };

  const tracked = (input: RequestInfo | URL, init?: RequestInit) =>
    // oxlint-disable-next-line effecttsgo/global-fetch -- The stream client requires native Response and AbortSignal behavior from its fetch adapter.
    globalThis.fetch(input, init).then(
      (response) => {
        set(true);

        return response;
      },
      (error) => {
        if (!init?.signal?.aborted) set(false);
        throw error;
      },
    );

  return Object.assign(tracked, { preconnect: globalThis.fetch.preconnect });
};

/**
 * Read the workspace's State stream from the start and stay live. Each batch
 * becomes one `ReceivedStateBatch`. Closing the subscription cancels the read.
 */
const workspaceMessages = (workspaceId: string) =>
  Stream.callback<Message>((queue) => {
    const offer = (message: Message) => {
      Queue.offerUnsafe(queue, message);
    };

    return Effect.gen(function* () {
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
              fetch: trackedFetch((isOnline) => offer(Message.ChangedConnection({ isOnline }))),
            }),
          catch: () => "Unable to open the workspace stream",
        }),
        (opened) => Effect.sync(() => opened.cancel()),
      );

      response.subscribeJson<JsonValue>((batch) => offer(batchMessage(batch)));

      yield* Effect.tryPromise({
        try: () => response.closed,
        catch: () => "The workspace stream stopped",
      });
    }).pipe(Effect.catch((error) => Queue.offer(queue, Message.FailedReadWorkspace({ error }))));
  });

export const subscriptions = Subscription.make<Model, Message>()((entry) => ({
  workspaceStream: entry(
    { workspaceId: Schema.String },
    {
      modelToDependencies: (model) => ({ workspaceId: model.workspaceId }),
      dependenciesToStream: ({ workspaceId }) => workspaceMessages(workspaceId),
    },
  ),
}));

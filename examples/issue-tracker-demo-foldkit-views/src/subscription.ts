/* oxlint-disable effecttsgo/strict-effect-provide -- Each subscription owns its remote client and heap. */
import { Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Array, Effect, Layer, Queue, Record, Schema, Stream, Struct } from "effect";
import { FetchHttpClient } from "effect/http";
import { Subscription } from "foldkit";
import { Message } from "./message.ts";
import type { Model } from "./model.ts";
import { Workspaces, workspaceViews } from "./workspace-views.ts";

const unavailableStatuses = new Set([500, 502, 503, 504]);

/**
 * A fetch for the stream reader that reports when the server stops or starts
 * answering. The Vite proxy can return 500 when the upstream is down.
 * Ignore aborts when the subscription closes.
 */
export const trackedFetch = (report: (isOnline: boolean) => void): typeof globalThis.fetch => {
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
        set(!unavailableStatuses.has(response.status));

        return response;
      },
      (error) => {
        if (!init?.signal?.aborted) set(false);
        throw error;
      },
    );

  // oxlint-disable-next-line effecttsgo/global-fetch -- Preserve runtime-specific properties of the wrapped fetch.
  return Object.assign(tracked, globalThis.fetch);
};

const byId = <A extends { readonly id: string }>(rows: ReadonlyArray<A>) =>
  Record.fromEntries(Array.map(rows, (row) => [row.id, row] as const));

/** The subscription's scope owns the workspace reader. */
export const workspaceMessages = (workspaceId: string, baseUrl: string) =>
  Stream.callback<Message>((queue) => {
    const offer = (message: Message) => Queue.offer(queue, message);

    const fetch = trackedFetch((isOnline) => {
      Queue.offerUnsafe(queue, Message.ChangedConnection({ isOnline }));
    });

    const inputs = RemoteStreams.layer(Streamsy.group(Workspaces), { baseUrl }).pipe(
      Layer.provide(FetchHttpClient.layer),
    );

    return Effect.gen(function* () {
      const live = yield* workspaceViews.at({ workspaceId });

      yield* live.snapshots.pipe(
        Stream.map(({ rows, offsets, caughtUp }) =>
          Message.ReceivedViews({
            caughtUp,
            offset: offsets[Workspaces.key] ?? ZERO_OFFSET,
            rows: {
              projects: byId(rows.projects),
              issues: byId(
                Array.map(rows.visibleIssues, (issue) => Struct.omit(issue, ["project"])),
              ),
              comments: byId(rows.commentsOn),
            },
            counts: {
              issueCountByProject: Record.fromEntries(
                Array.map(rows.issueCountByProject, ({ projectId, count }) => [projectId, count]),
              ),
              statusCounts: rows.statusCounts,
            },
          }),
        ),
        Stream.runForEach(offer),
      );
      yield* offer(Message.FailedReadWorkspace({ error: "The workspace stream closed" }));
    }).pipe(
      Effect.provide(inputs),
      Effect.provideService(FetchHttpClient.Fetch, fetch),
      Effect.catch((error) => offer(Message.FailedReadWorkspace({ error: error.message }))),
      Effect.andThen(Queue.end(queue)),
    );
  });

export const subscriptions = Subscription.make<Model, Message>()((entry) => ({
  workspaceStream: entry(
    { workspaceId: Schema.String },
    {
      modelToDependencies: (model) => ({ workspaceId: model.workspaceId }),
      dependenciesToStream: ({ workspaceId }) =>
        workspaceMessages(workspaceId, new URL("/streams", window.location.origin).href),
    },
  ),
}));

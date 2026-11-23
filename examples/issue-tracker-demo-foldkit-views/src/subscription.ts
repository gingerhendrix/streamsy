/* oxlint-disable effecttsgo/strict-effect-provide -- Each subscription owns its remote client and heap. */
import { StreamAddress, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { ViewBackend } from "@streamsy/views";
import { Array, Effect, Layer, Queue, Record, Result, Schema, Stream, Struct } from "effect";
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

/** The subscription's scope owns the binding and both readers for this page. */
export const workspaceMessages = (workspaceId: string, baseUrl: string) =>
  Stream.callback<Message>((queue) => {
    const offer = (message: Message) => Queue.offer(queue, message);

    const fetch = trackedFetch((isOnline) => {
      Queue.offerUnsafe(queue, Message.ChangedConnection({ isOnline }));
    });

    const host = Layer.merge(
      ViewBackend.layerMemoryHost,
      RemoteStreams.layer(Streamsy.group(Workspaces), { baseUrl }).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
    );

    return Effect.gen(function* () {
      const params = { workspaceId };
      const live = yield* workspaceViews.at(params);
      const workspaces = yield* Workspaces;

      // Use changes as a notification, then read every output together. Updating
      // each output in its own message can render counts and lists from different units.
      const snapshots = Stream.mergeAll(
        [
          live.changes("projects").pipe(Stream.map(() => undefined)),
          live.changes("visibleIssues").pipe(Stream.map(() => undefined)),
          live.changes("commentsOn").pipe(Stream.map(() => undefined)),
          live.changes("issueCountByProject").pipe(Stream.map(() => undefined)),
          live.changes("statusCounts").pipe(Stream.map(() => undefined)),
        ],
        { concurrency: "unbounded" },
      ).pipe(
        Stream.mapEffect(() =>
          Effect.all({
            projects: live.rows("projects"),
            issues: live.rows("visibleIssues"),
            comments: live.rows("commentsOn"),
            projectCounts: live.rows("issueCountByProject"),
            statusCounts: live.rows("statusCounts"),
          }),
        ),
        Stream.changesWith(
          (previous, current) =>
            previous.projects === current.projects &&
            previous.issues === current.issues &&
            previous.comments === current.comments &&
            previous.projectCounts === current.projectCounts &&
            previous.statusCounts === current.statusCounts,
        ),
        Stream.map(({ projects, issues, comments, projectCounts, statusCounts }) =>
          Message.ReceivedViews({
            caughtUp: projects.caughtUp,
            rows: {
              projects: byId(projects.rows),
              issues: byId(Array.map(issues.rows, (issue) => Struct.omit(issue, ["project"]))),
              comments: byId(comments.rows),
            },
            counts: {
              issueCountByProject: Record.fromEntries(
                Array.map(projectCounts.rows, ({ projectId, count }) => [projectId, count]),
              ),
              statusCounts: statusCounts.rows,
            },
          }),
        ),
      );

      // Views expose row values, while the write confirmation is in State event
      // headers. A terminal failure here disables pending indicators while
      // the Views follower and the HTTP write commands keep running.
      const confirmations = workspaces.follow(params).pipe(
        Stream.filterMap((batch) => {
          const txids = Array.dedupe(
            Array.flatMap(batch.items, (item) =>
              StreamAddress.isControl(item) || item.headers.txid === undefined
                ? []
                : [item.headers.txid],
            ),
          );

          return txids.length === 0
            ? Result.failVoid
            : Result.succeed(Message.ConfirmedWrites({ txids }));
        }),
        Stream.catch(() => Stream.empty),
        Stream.concat(Stream.succeed(Message.StoppedConfirmingWrites())),
      );

      // A failed startup HEAD can fail caughtUp without ending changes.
      const startup = Stream.fromEffect(live.caughtUp).pipe(Stream.drain);
      yield* Stream.merge(snapshots, confirmations).pipe(
        Stream.merge(startup),
        Stream.runForEach(offer),
      );
      yield* offer(Message.FailedReadWorkspace({ error: "The workspace stream closed" }));
    }).pipe(
      Effect.provide(host),
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

/** Framework-neutral checked sink handling adapted to the local gateway. */
import { handleStateSink, StateSinkSourceFailure } from "@streamsy/state-sink/effect";
import {
  DocumentSinkSourceFailure,
  handleDocumentSink,
  handleStreamSink,
  StreamSinkSourceFailure,
} from "@streamsy/sinks/effect";
import { Effect } from "effect";
import {
  boardIssues,
  boardLabelCounts,
  issueTransitions,
  streamNames,
  workspaceSummary,
} from "../../domain/declaration.ts";
import { StreamGateway } from "../transport/gateway.ts";
import { advance } from "../application/maintenance.ts";
import { IssueStore } from "../persistence/store.ts";
import { ensureWorkspace, Streams } from "../transport/streams.ts";
import { buildWorkspaceSummary } from "./summary.ts";
import { readTransitions } from "./transitions.ts";

export const matchBoardSink = (pathname: string) => boardIssues.compiledRoute.match(pathname);

export const handleSinkRequest = (request: Request) =>
  handleStateSink(boardIssues, request, {
    snapshot: Effect.fn("IssueTracker.sinkSnapshot")(function* ({ workspaceId }) {
      const streams = yield* Streams;
      yield* advance(workspaceId).pipe(
        Effect.mapError(
          (error) => new StateSinkSourceFailure({ phase: "snapshot", detail: String(error) }),
        ),
      );
      const store = yield* IssueStore;
      const rows = yield* store
        .boardRows(workspaceId)
        .pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "snapshot", detail: String(error) }),
          ),
        );
      const head = yield* Effect.promise((signal) =>
        streams.client.stream(streamNames.boardState(workspaceId)).head({ signal }),
      );
      if (head.status !== "ok") {
        return yield* new StateSinkSourceFailure({
          phase: "snapshot",
          detail: `head returned ${head.status}`,
        });
      }
      return { rows, offset: head.offset ?? "-1" };
    }),
    suffix: Effect.fn("IssueTracker.sinkSuffix")(function* (incoming, { workspaceId }) {
      const gateway = yield* StreamGateway;
      if (new URL(incoming.url).searchParams.get("live") === null) {
        yield* ensureWorkspace(workspaceId).pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "suffix", detail: String(error) }),
          ),
        );
        yield* advance(workspaceId).pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "suffix", detail: String(error) }),
          ),
        );
      }
      const target = new URL(incoming.url);
      target.pathname = `${gateway.prefix}/${streamNames.boardState(workspaceId)}`;
      return yield* gateway.fetch(
        new Request(target, { method: incoming.method, headers: incoming.headers }),
      );
    }),
  });

export const matchLabelCountSink = (pathname: string) =>
  boardLabelCounts.compiledRoute.match(pathname);

/**
 * Serve the label-count product through its own checked State sink.
 *
 * It is the same two capabilities the board sink has and for the same reasons:
 * a snapshot of the durable rows with the State stream's current offset, and a
 * suffix proxied to the gateway so resume is the transport's own. What differs
 * is only which relation and which stream.
 */
export const handleLabelCountSinkRequest = (request: Request) =>
  handleStateSink(boardLabelCounts, request, {
    snapshot: Effect.fn("IssueTracker.labelCountSnapshot")(function* ({ workspaceId }) {
      const streams = yield* Streams;
      yield* advance(workspaceId).pipe(
        Effect.mapError(
          (error) => new StateSinkSourceFailure({ phase: "snapshot", detail: String(error) }),
        ),
      );
      const store = yield* IssueStore;
      const rows = yield* store
        .labelCountRows(workspaceId)
        .pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "snapshot", detail: String(error) }),
          ),
        );
      const head = yield* Effect.promise((signal) =>
        streams.client.stream(streamNames.labelCountState(workspaceId)).head({ signal }),
      );
      if (head.status !== "ok") {
        return yield* new StateSinkSourceFailure({
          phase: "snapshot",
          detail: `head returned ${head.status}`,
        });
      }
      return { rows, offset: head.offset ?? "-1" };
    }),
    suffix: Effect.fn("IssueTracker.labelCountSuffix")(function* (incoming, { workspaceId }) {
      const gateway = yield* StreamGateway;
      if (new URL(incoming.url).searchParams.get("live") === null) {
        yield* ensureWorkspace(workspaceId).pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "suffix", detail: String(error) }),
          ),
        );
        yield* advance(workspaceId).pipe(
          Effect.mapError(
            (error) => new StateSinkSourceFailure({ phase: "suffix", detail: String(error) }),
          ),
        );
      }
      const target = new URL(incoming.url);
      target.pathname = `${gateway.prefix}/${streamNames.labelCountState(workspaceId)}`;
      return yield* gateway.fetch(
        new Request(target, { method: incoming.method, headers: incoming.headers }),
      );
    }),
  });

export const matchTransitionSink = (pathname: string) =>
  issueTransitions.compiledRoute.match(pathname);

/**
 * Serve the declared transition feed.
 *
 * The workspace is brought up to its durable tail first, so a consumer polling
 * the feed sees the transitions of facts that were appended by anything —
 * including another process — and not only those a command in this process
 * happened to publish. Nothing between the durable feed and the response
 * reorders it.
 */
export const handleTransitionFeedRequest = (request: Request) =>
  handleStreamSink(issueTransitions, request, {
    read: Effect.fn("IssueTracker.transitionFeed")(function* ({ workspaceId }, offset) {
      yield* ensureWorkspace(workspaceId).pipe(Effect.mapError(feedUnavailable));
      yield* advance(workspaceId).pipe(Effect.mapError(feedUnavailable));
      return yield* readTransitions(workspaceId, offset).pipe(
        Effect.mapError((error) => {
          const { _tag: tag } = error;
          return tag === "TransitionReadFailure"
            ? new StreamSinkSourceFailure({ reason: error.reason, detail: error.detail })
            : feedUnavailable(error);
        }),
      );
    }),
  });

export const matchSummarySink = (pathname: string) =>
  workspaceSummary.compiledRoute.match(pathname);

export const handleWorkspaceSummaryRequest = (request: Request) =>
  handleDocumentSink(workspaceSummary, request, {
    document: Effect.fn("IssueTracker.workspaceSummary")(function* ({ workspaceId }) {
      return yield* buildWorkspaceSummary(workspaceId).pipe(
        Effect.mapError((error) => new DocumentSinkSourceFailure({ detail: String(error) })),
      );
    }),
  });

/**
 * Every application failure below the feed is reported as an unavailable feed.
 *
 * That includes a poisoned feed message: the handler's public error union has
 * no separate tag for it, and reporting it as unavailable keeps the failure
 * fail-stop and visible rather than serving a shorter page.
 */
function feedUnavailable(error: { readonly _tag: string }): StreamSinkSourceFailure {
  const { _tag: tag } = error;
  return new StreamSinkSourceFailure({ reason: "unavailable", detail: tag });
}

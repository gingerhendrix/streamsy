/** Framework-neutral checked state-sink handling adapted to the local gateway. */
import {
  authorizerLayer,
  handleStateSink,
  SinkAuthorizationDenied,
  StateSinkAuthorizer,
  StateSinkSourceFailure,
} from "@streamsy/state-sink/effect";
import { Effect, Layer } from "effect";
import { boardIssues, streamNames } from "../domain/declaration.ts";
import { StreamGateway } from "./gateway.ts";
import { advance } from "./maintenance.ts";
import { IssueStore } from "./store.ts";
import { ensureWorkspace, Streams } from "./streams.ts";

export const SCOPE_HEADER = "x-streamsy-scope";
export const LOCAL_AUTHORIZATION_GENERATION = "local-v1";

export const localSinkAuthorizerLayer: Layer.Layer<StateSinkAuthorizer> = authorizerLayer(
  Effect.fn("IssueTracker.authorizeSink")(function* ({ request, sink }) {
    if (request.headers.get(SCOPE_HEADER) !== sink.auth.required) {
      return yield* new SinkAuthorizationDenied({ required: sink.auth.required });
    }
    return { generation: LOCAL_AUTHORIZATION_GENERATION, subject: "local-example" };
  }),
);

export const matchBoardSink = (pathname: string) => boardIssues.compiledRoute.match(pathname);

export const authorizeBoardSink = Effect.fn("IssueTracker.authorizeBoardSink")(function* (
  request: Request,
  workspaceId: string,
) {
  const authorizer = yield* StateSinkAuthorizer;
  return yield* authorizer.authorize({ request, sink: boardIssues, params: { workspaceId } });
});

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

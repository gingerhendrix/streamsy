import { Streamsy } from "@streamsy/core";
import { Http } from "@streamsy/core";
import { Projection } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { SqlClient } from "effect/sql";
import { CommandRequest } from "../shared/api.ts";
import { issueRows } from "./projection.ts";
import { WorkspaceSummary } from "../domain/outputs.ts";
import { tracker, outputStreams, Board, LabelCounts, Transitions } from "./outputs.ts";
import { routes } from "./streams.ts";
import { transact } from "./state.ts";

export const workspaceSummary = Effect.gen(function* () {
  const params = yield* Serve.params(Board);

  return yield* Serve.value(
    WorkspaceSummary,
    Projection.read(tracker.at(params)).pipe(
      Effect.map(({ state }) => ({
        workspaceId: state.workspaceId,
        issueCount: state.issueCount,
        doneCount: state.doneCount,
        labelCount: state.labels.length,
        projectCount: state.projects.length,
      })),
    ),
    { contract: "compact-summary-v1" },
  );
});

export const outputRoutes = Layer.mergeAll(
  Serve.mount(Board, "/state/workspaces/:workspaceId/issues"),
  Serve.mount(LabelCounts, "/state/workspaces/:workspaceId/label-counts"),
  Serve.mount(Transitions, "/feed/workspaces/:workspaceId/issue-transitions"),
  Serve.route("/workspaces/:workspaceId/summary", workspaceSummary),
);

/** The route names a workspace that this server does not run. */
class UnknownWorkspace extends Schema.TaggedError<
  UnknownWorkspace,
  { readonly brand: unique symbol }
>()("UnknownWorkspace", {}) {}

/** The command body does not decode. */
class InvalidCommand extends Schema.TaggedError<
  InvalidCommand,
  { readonly brand: unique symbol }
>()("InvalidCommand", {}) {}

const json = HttpServerResponse.jsonUnsafe;

const workspace = (workspaces: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const { workspaceId } = yield* HttpRouter.params;

    if (workspaceId === undefined || !workspaces.includes(workspaceId))
      return yield* new UnknownWorkspace();

    return workspaceId;
  });

/** Answer 404 for an unknown workspace. Log any other failure and answer 500. */
const api = <E, R>(
  handler: Effect.Effect<HttpServerResponse.HttpServerResponse, E | UnknownWorkspace, R>,
) =>
  handler.pipe(
    Effect.catchTag("UnknownWorkspace", () =>
      Effect.succeed(json({ error: "Unknown workspace" }, { status: 404 })),
    ),
    Effect.catch((error) =>
      Effect.logError(error).pipe(
        Effect.as(json({ error: "Internal server error" }, { status: 500 })),
      ),
    ),
  );

const commands = (workspaces: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const workspaceId = yield* workspace(workspaces);
    const request = yield* HttpServerRequest.HttpServerRequest;

    const command = yield* request.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(CommandRequest)),
      Effect.mapError(() => new InvalidCommand()),
    );

    const { event, offset } = yield* transact(workspaceId, command);

    return json({ commandId: command.commandId, event, offset });
  }).pipe(
    Effect.catchTags({
      InvalidCommand: () => Effect.succeed(json({ error: "Invalid command" }, { status: 400 })),
      UnknownIssue: () => Effect.succeed(json({ error: "Unknown issue" }, { status: 400 })),
      OffsetMismatch: () =>
        Effect.succeed(json({ error: "Concurrent command conflict" }, { status: 409 })),
    }),
    api,
  );

const drafts = (workspaces: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const workspaceId = yield* workspace(workspaces);
    const sql = yield* SqlClient.SqlClient;

    const rows =
      yield* sql`SELECT * FROM notification_drafts WHERE workspace_id = ${workspaceId} ORDER BY event_id`;

    return json({ workspaceId, rows });
  }).pipe(api);

const status = (workspaces: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const workspaceId = yield* workspace(workspaces);
    const { offsets } = yield* Projection.read(issueRows.at({ workspaceId }));

    const { offsets: outputOffsets } = yield* Projection.read(tracker.at({ workspaceId }));

    return json({ workspaceId, offsets, outputOffsets });
  }).pipe(api);

export const app = (workspaces: ReadonlyArray<string>) =>
  Layer.mergeAll(
    Http.routes(Streamsy.group(...Object.values(routes), ...outputStreams), {
      prefix: "/streams",
      access: "read",
    }),
    outputRoutes,
    HttpRouter.add("POST", "/api/workspaces/:workspaceId/commands", commands(workspaces)),
    Serve.route("/api/workspaces/:workspaceId/status", status(workspaces)),
    Serve.route("/api/workspaces/:workspaceId/drafts", drafts(workspaces)),
    HttpRouter.add("GET", "/", json({ service: "bugsy", workspaces })),
  );

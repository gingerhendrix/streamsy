/**
 * The application's durable-stream capability, as a service.
 *
 * A host resolves storage into one `StreamProtocolClient` and provides this
 * layer once. Application code never receives a client as an argument and never
 * touches the client's Promise API: it asks for `Streams`, which owns the only
 * transport adapter boundary in the server.
 *
 * Bindings stay inert — a mesh identity, the fixed client handle, and a stream
 * id. Only `ensure` performs work.
 */
import type { StreamProtocolClient } from "@streamsy/core";
import { bindStream, type StreamBinding } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { Context, Effect, Layer } from "effect";
import { streamNames } from "../../domain/declaration.ts";
import { StreamUnavailable } from "../errors.ts";

export interface WorkspaceBindings {
  /** The canonical append-only source of issue facts for one workspace. */
  readonly issueEvents: (workspaceId: string) => StreamBinding;
  /** The canonical append-only source of issue-label membership facts. */
  readonly issueLabelEvents: (workspaceId: string) => StreamBinding;
  /** The Durable State stream the `stateSink` publishes. */
  readonly boardState: (workspaceId: string) => StreamBinding;
  /** The Durable State stream the label-count `stateSink` publishes. */
  readonly labelCountState: (workspaceId: string) => StreamBinding;
  readonly projects: (workspaceId: string) => StreamBinding;
  readonly users: (workspaceId: string) => StreamBinding;
  readonly labels: (workspaceId: string) => StreamBinding;
  readonly metadata: (workspaceId: string) => StreamBinding;
  /** The append-only feed the `streamSink` publishes issue transitions to. */
  readonly issueTransitions: (workspaceId: string) => StreamBinding;
}

export interface WorkspaceStreams {
  /**
   * The resolved protocol client. Exposed for the host's stream routes and for
   * tests — not for application logic, which uses `bindings`.
   */
  readonly client: StreamProtocolClient;
  readonly bindings: WorkspaceBindings;
  /** Create a JSON stream when it does not exist yet. Creation is idempotent. */
  readonly ensure: (streamId: string) => Effect.Effect<void, StreamUnavailable>;
}

export class Streams extends Context.Service<Streams, WorkspaceStreams>()(
  "issue-tracker/Streams",
) {}

export function bindByName(client: StreamProtocolClient, name: string): StreamBinding {
  return bindStream({ identity: streamIdentity(name), client, streamId: name });
}

export function workspaceBindings(client: StreamProtocolClient): WorkspaceBindings {
  return {
    issueEvents: (workspaceId) => bindByName(client, streamNames.issueEvents(workspaceId)),
    issueLabelEvents: (workspaceId) =>
      bindByName(client, streamNames.issueLabelEvents(workspaceId)),
    boardState: (workspaceId) => bindByName(client, streamNames.boardState(workspaceId)),
    labelCountState: (workspaceId) => bindByName(client, streamNames.labelCountState(workspaceId)),
    projects: (workspaceId) => bindByName(client, streamNames.projects(workspaceId)),
    users: (workspaceId) => bindByName(client, streamNames.users(workspaceId)),
    labels: (workspaceId) => bindByName(client, streamNames.labels(workspaceId)),
    metadata: (workspaceId) => bindByName(client, streamNames.metadata(workspaceId)),
    issueTransitions: (workspaceId) =>
      bindByName(client, streamNames.issueTransitions(workspaceId)),
  };
}

export const layer = (client: StreamProtocolClient): Layer.Layer<Streams> =>
  Layer.succeed(
    Streams,
    Streams.of({
      client,
      bindings: workspaceBindings(client),
      ensure: Effect.fn("Streams.ensure")(function* (streamId: string) {
        const created = yield* Effect.tryPromise({
          try: (signal) =>
            client.stream(streamId).create({ contentType: "application/json", signal }),
          catch: (cause) =>
            new StreamUnavailable({ streamId, status: `transport: ${String(cause)}` }),
        });
        // `conflict` means the stream already exists, which is the normal path.
        if (created.status === "created" || created.status === "conflict") return undefined;
        return yield* new StreamUnavailable({ streamId, status: created.status });
      }),
    }),
  );

/** Create every stream one workspace needs, in one named operation. */
export const ensureWorkspace = Effect.fn("Streams.ensureWorkspace")(function* (
  workspaceId: string,
) {
  const streams = yield* Streams;
  yield* streams.ensure(streamNames.issueEvents(workspaceId));
  yield* streams.ensure(streamNames.issueLabelEvents(workspaceId));
  yield* streams.ensure(streamNames.boardState(workspaceId));
  yield* streams.ensure(streamNames.labelCountState(workspaceId));
  yield* streams.ensure(streamNames.projects(workspaceId));
  yield* streams.ensure(streamNames.users(workspaceId));
  yield* streams.ensure(streamNames.labels(workspaceId));
  yield* streams.ensure(streamNames.metadata(workspaceId));
  yield* streams.ensure(streamNames.issueTransitions(workspaceId));
});

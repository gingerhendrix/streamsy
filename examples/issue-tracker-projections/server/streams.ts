/**
 * The application's durable-stream capability, as a service.
 *
 * A host resolves storage into one `StreamProtocolClient` and provides this
 * layer once. Application code never receives a client as an argument and never
 * calls the client's Promise API directly: it asks for `Streams`, which owns
 * the only adapter boundary in the server.
 *
 * Bindings themselves stay inert values — a mesh identity, the fixed client
 * handle, and an application stream id. Nothing here performs transport work
 * except `ensure`.
 */
import type { StreamProtocolClient } from "@streamsy/core";
import { bindStream, type StreamBinding } from "@streamsy/experimental/binding";
import { streamIdentity } from "@streamsy/experimental/causal";
import { Context, Effect, Layer } from "effect";
import { streamNames } from "../shared/domain.ts";
import { StreamUnavailable } from "./errors.ts";

/** Bounded work limits for one projection pass. */
export const PROJECTION_LIMITS = {
  maxItems: 500,
  maxPages: 200,
  maxBatches: 200,
  maxBytes: 1_000_000,
} as const;

export interface WorkspaceBindings {
  readonly projects: (workspaceId: string) => StreamBinding;
  readonly issueEvents: (workspaceId: string, issueId: string) => StreamBinding;
  readonly issueDetail: (workspaceId: string, issueId: string) => StreamBinding;
  readonly membership: (workspaceId: string, projectId: string) => StreamBinding;
  readonly board: (workspaceId: string, projectId: string) => StreamBinding;
}

export interface StreamsShape {
  /**
   * The resolved protocol client. Exposed for the few callers that legitimately
   * need the handle itself — the browser-facing HTTP routes and tests — not for
   * application logic, which uses `bindings` and the mesh services instead.
   */
  readonly client: StreamProtocolClient;
  readonly bindings: WorkspaceBindings;
  /** Create a JSON stream when it does not exist yet. Creation is idempotent. */
  readonly ensure: (streamId: string) => Effect.Effect<void, StreamUnavailable>;
}

export class Streams extends Context.Service<Streams, StreamsShape>()(
  "issue-tracker-projections/Streams",
) {}

export function bindByName(client: StreamProtocolClient, name: string): StreamBinding {
  return bindStream({ identity: streamIdentity(name), client, streamId: name });
}

export function workspaceBindings(client: StreamProtocolClient): WorkspaceBindings {
  return {
    projects: (workspaceId) => bindByName(client, streamNames.projects(workspaceId)),
    issueEvents: (workspaceId, issueId) =>
      bindByName(client, streamNames.issueEvents(workspaceId, issueId)),
    issueDetail: (workspaceId, issueId) =>
      bindByName(client, streamNames.issueDetail(workspaceId, issueId)),
    membership: (workspaceId, projectId) =>
      bindByName(client, streamNames.membership(workspaceId, projectId)),
    board: (workspaceId, projectId) =>
      bindByName(client, streamNames.board(workspaceId, projectId)),
  };
}

/** Provide the streams capability over one resolved protocol client. */
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
        if (created.status === "created" || created.status === "conflict") return;
        return yield* Effect.fail(new StreamUnavailable({ streamId, status: created.status }));
      }),
    }),
  );

/** Create every stream a workspace command touches, in one named operation. */
export const ensureAll = Effect.fn("Streams.ensureAll")(function* (streamIds: readonly string[]) {
  const streams = yield* Streams;
  for (const streamId of streamIds) yield* streams.ensure(streamId);
});

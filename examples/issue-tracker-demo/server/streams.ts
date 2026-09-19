import { StreamRef, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer, Schema } from "effect";
import {
  CommentCodec,
  IssueCodec,
  ProjectCodec,
  type Comment,
  type Issue,
  type Project,
  type StateEvent,
} from "../shared/state-schema.ts";
import { databasePath, streamPrefix, workspaceStreamId } from "./config.ts";

const projectChange = StreamRef.stateChange({ schema: ProjectCodec, type: "project" });
const issueChange = StreamRef.stateChange({ schema: IssueCodec, type: "issue" });
const commentChange = StreamRef.stateChange({ schema: CommentCodec, type: "comment" });

/** One multi-type reader for the workspace stream. */
export const workspaceEvents = (workspaceId: string) =>
  StreamRef.json(workspaceStreamId(workspaceId), {
    schema: Schema.Union([projectChange, issueChange, commentChange]),
  });

/** Per-type refs write the same stream while retaining the entity codec. */
export const projectEvents = (workspaceId: string) =>
  StreamRef.state(workspaceStreamId(workspaceId), {
    schema: ProjectCodec,
    type: "project",
    key: "id",
  });
export const issueEvents = (workspaceId: string) =>
  StreamRef.state(workspaceStreamId(workspaceId), {
    schema: IssueCodec,
    type: "issue",
    key: "id",
  });
export const commentEvents = (workspaceId: string) =>
  StreamRef.state(workspaceStreamId(workspaceId), {
    schema: CommentCodec,
    type: "comment",
    key: "id",
  });

export function appendWorkspaceEvent(
  workspaceId: string,
  event: StateEvent,
  expectedOffset?: string,
) {
  const options = expectedOffset === undefined ? {} : { expectedOffset };
  // `stateChange` currently widens `type` to string, so the runtime
  // discriminator needs these local casts to recover the entity type.
  switch (event.type) {
    case "project":
      // SAFETY: this branch checked the public event discriminator; the cast
      // restores the literal type that `stateChange` currently widens.
      return Streams.append(
        projectEvents(workspaceId),
        [event as StreamRef.StateChange<Project>],
        options,
      );
    case "issue":
      // SAFETY: this branch checked the public event discriminator; the cast
      // restores the literal type that `stateChange` currently widens.
      return Streams.append(
        issueEvents(workspaceId),
        [event as StreamRef.StateChange<Issue>],
        options,
      );
    case "comment":
      // SAFETY: this branch checked the public event discriminator; the cast
      // restores the literal type that `stateChange` currently widens.
      return Streams.append(
        commentEvents(workspaceId),
        [event as StreamRef.StateChange<Comment>],
        options,
      );
    default:
      return Effect.die(new TypeError(`Unknown workspace event type: ${event.type}`));
  }
}

/** Minimal runtime surface used by the Promise-native Bun route handlers. */
export interface DemoRuntime {
  runPromise<A, E>(effect: Effect.Effect<A, E, StreamsReader | StreamsWriter>): Promise<A>;
}

/** The HTTP conversion shares the runtime's already-acquired protocol services. */
export class DemoStreams extends Context.Service<
  DemoStreams,
  { readonly fetch: (request: Request) => Promise<Response> }
>()("IssueTracker/DemoStreams") {}

export const demoStreamsLayer = Layer.effect(
  DemoStreams,
  Effect.gen(function* () {
    const context = yield* Effect.context<StreamsReader | StreamsWriter>();
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() => Http.makeEdge({ pathPrefix: streamPrefix }, Layer.succeedContext(context))),
      (acquired) => Effect.promise(() => acquired.dispose()),
    );
    return DemoStreams.of({ fetch: (request) => edge.handler(request) });
  }),
);

const runtimeServices = Layer.effectContext(
  Effect.context<DemoStreams | StreamsReader | StreamsWriter>(),
);
const memoryApplicationLayer = runtimeServices.pipe(
  Layer.provide(demoStreamsLayer.pipe(Layer.provideMerge(Streams.layerMemory()))),
);
const sqliteApplicationLayer = (filename: string) =>
  runtimeServices.pipe(
    Layer.provide(
      demoStreamsLayer.pipe(Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } }))),
    ),
  );

export const applicationLayer =
  databasePath === undefined ? memoryApplicationLayer : sqliteApplicationLayer(databasePath);

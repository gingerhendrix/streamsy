import { StreamRef, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { CommentCodec, IssueCodec, ProjectCodec } from "../shared/state-schema.ts";
import { databasePath, streamPrefix, workspaceStreamId } from "./config.ts";

/** One multi-collection ref for reading and writing a workspace stream. */
export const workspaceEvents = (workspaceId: string) =>
  StreamRef.state(workspaceStreamId(workspaceId), {
    collections: {
      project: { schema: ProjectCodec, key: "id" },
      issue: { schema: IssueCodec, key: "id" },
      comment: { schema: CommentCodec, key: "id" },
    },
  });

export type WorkspaceEvent = StreamRef.CollectionsChange<
  ReturnType<typeof workspaceEvents>["collections"]
>;

export function appendWorkspaceEvent(
  workspaceId: string,
  event: WorkspaceEvent,
  expectedOffset?: string,
) {
  const options = expectedOffset === undefined ? {} : { expectedOffset };
  return Streams.append(workspaceEvents(workspaceId), [event], options);
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

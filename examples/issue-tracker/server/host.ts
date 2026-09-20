import { Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import * as Http from "@streamsy/core/http";
import type { Host } from "@streamsy/projection";
import * as ProjectionSqlite from "@streamsy/projection/sqlite";
import * as BunStorage from "@streamsy/storage/bun";
import { Context, Effect, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Projection } from "@streamsy/projection";
import { issueRows } from "./projection.ts";
import { prepareSchema } from "./schema.ts";
import { refs } from "./streams.ts";

export class StreamHttp extends Context.Service<
  StreamHttp,
  { readonly fetch: (request: Request) => Promise<Response> }
>()("IssueTracker/StreamHttp") {}

const streamHttpLayer = Layer.effect(
  StreamHttp,
  Effect.gen(function* () {
    const context = yield* Effect.context<StreamsReader | StreamsWriter>();
    const edge = yield* Effect.acquireRelease(
      Effect.sync(() => Http.makeEdge({ pathPrefix: "/streams" }, Layer.succeedContext(context))),
      (value) => Effect.promise(() => value.dispose()),
    );
    return StreamHttp.of({ fetch: edge.handler });
  }),
);

export const hostLayer = (filename: string) =>
  ProjectionSqlite.layer.pipe(
    Layer.provideMerge(BunStorage.layerProtocol({ client: { filename } })),
  );

export class ApplicationReady extends Context.Service<ApplicationReady, true>()(
  "IssueTracker/ApplicationReady",
) {}

export const applicationLayer = (filename: string, workspaces: ReadonlyArray<string> = []) => {
  const base = streamHttpLayer.pipe(Layer.provideMerge(hostLayer(filename)));
  const ready = Layer.effect(
    ApplicationReady,
    Effect.gen(function* () {
      yield* prepareSchema;
      yield* createInputs(workspaces.flatMap(refs));
      if (workspaces.length > 0)
        yield* Projection.onChange(
          issueRows,
          workspaces.map((workspaceId) => ({ workspaceId })),
        );
      return true as const;
    }),
  ).pipe(Layer.provide(base));
  return Layer.effectContext(
    Effect.context<Host | SqlClient.SqlClient | StreamHttp | ApplicationReady>(),
  ).pipe(Layer.provide(ready.pipe(Layer.provideMerge(base))));
};

export const createInputs = (inputs: ReadonlyArray<Parameters<typeof Streams.create>[0]>) =>
  Effect.forEach(inputs, (ref) => Streams.create(ref).pipe(Effect.ignore), { discard: true });

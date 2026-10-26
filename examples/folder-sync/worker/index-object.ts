import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault, Streamsy } from "@streamsy/core";
import { Projection, ProjectionWakeup } from "@streamsy/projection";
import { Serve } from "@streamsy/serve";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { HealthLive } from "../server/api.ts";
import { CreateFolder } from "../server/domain.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { registerFolder } from "../server/folder-registry.ts";
import { createFailures, FolderCreateFailed, folderRoutes } from "../server/folder-routes.ts";
import { WriterTokenLive } from "../server/http.ts";
import { IndexRequestLive } from "../server/index-api.ts";
import { initialiseSearch } from "../server/search-index.ts";
import { Files, Folders, FolderWake } from "../server/streams.ts";
import type { Env } from "./env.ts";
import { wakeSearch } from "./search.ts";
import { filesLink, wakeLink } from "./links.ts";
import { folderObject } from "./placement.ts";
import { objectStorage } from "./storage.ts";

/** Creates a registered folder's `Files` stream in its folder object. */
class FolderStreams extends Context.Service<
  FolderStreams,
  {
    readonly create: (input: {
      readonly folderId: string;
      readonly title: string;
      readonly txid: string;
    }) => Effect.Effect<void, FolderCreateFailed>;
  }
>()("FolderSync/FolderStreams") {}

const folderStreams = (env: Env) =>
  Layer.succeed(
    FolderStreams,
    FolderStreams.of({
      create: (input) =>
        Schema.encodeEffect(Schema.fromJsonString(CreateFolder))(input).pipe(
          Effect.catchTag("SchemaError", (cause) => Effect.fail(new FolderCreateFailed({ cause }))),
          Effect.flatMap((body) =>
            Effect.tryPromise({
              try: (signal) =>
                folderObject(env, input.folderId)
                  .fetch("https://internal/__create", {
                    method: "POST",
                    headers: { "content-type": "application/json" },
                    body,
                    signal,
                  })
                  .then((response) => {
                    if (!response.ok) throw new Error(`Folder create answered ${response.status}`);

                    return response.text();
                  }),
              catch: (cause) => new FolderCreateFailed({ cause }),
            }),
          ),
          Effect.asVoid,
        ),
    }),
  );

// The Worker edge checked the token, and this route checks it again.
const IndexInternal =
  // The stream comes first, so a reader that finds the folder in the registry can read it.
  // A crash between the two leaves an unlisted empty stream. A retry creates it again
  // without change and then registers the folder.
  HttpRouter.addAll(
    folderRoutes((input) =>
      createFailures(
        Effect.flatMap(FolderStreams, (streams) => streams.create(input)).pipe(
          Effect.andThen(registerFolder(input)),
        ),
      ),
    ),
  );

const indexInitialise = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    yield* (yield* Folders).create({});
    // Create the empty wake stream, so that a reader gets an empty stream and not 404.
    yield* (yield* FolderWake).create({});
  }),
);

// Acquisition replays the wake rows after the durable `wakeSearch` cursor before
// the host admits requests or arms its scoped wakeup.
const indexBootstrap = Layer.effectDiscard(Projection.run(wakeSearch.at({}))).pipe(
  Layer.provide(indexInitialise),
);

const indexLayer = (state: DurableObjectState, env: Env) =>
  Layer.mergeAll(FileIndexLive, folderStreams(env), WriterTokenLive).pipe(
    Layer.provideMerge(indexBootstrap),
    Layer.provideMerge(objectStorage(state, env, Streamsy.group(Folders, FolderWake))),
    Layer.provideMerge(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({ FOLDER_SYNC_WRITER_TOKEN: env.WRITER_TOKEN }),
      ),
    ),
    Layer.catchCause((cause) =>
      Layer.effectContext(
        Effect.fail(
          new StorageFault({
            operation: "folder-sync.index.acquire",
            message: Cause.pretty(cause),
            retryable: true,
          }),
        ),
      ),
    ),
  );

const IndexApp = Layer.mergeAll(
  IndexRequestLive.pipe(Layer.provide(HttpServer.layerServices)),
  HealthLive,
  IndexInternal,
  Serve.mount(Folders, "/streams/folders"),
  Serve.mount(FolderWake, "/streams/wake"),
  // Folder objects append wake rows here through the object stub. The Worker
  // edge answers 404 for every `/__` path, so no client reaches this mount.
  wakeLink.routes,
);

export class IndexObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof indexLayer>>,
  Files
>({
  app: IndexApp,
  layer: indexLayer,
  links: (env) => [filesLink.client(env.FOLDERS)],
  wakeups: [ProjectionWakeup.attach(wakeSearch)],
}) {}

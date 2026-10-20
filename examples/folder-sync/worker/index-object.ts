import type { DurableObjectState } from "@cloudflare/workers-types";
import { StorageFault, Streamsy } from "@streamsy/core";
import { RemoteStreams } from "@streamsy/core/remote";
import { Serve } from "@streamsy/serve";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { Cause, ConfigProvider, Context, Effect, Layer, Option, Schema } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpRouter,
  HttpServer,
  HttpServerResponse,
} from "effect/http";
import { HealthLive } from "../server/api.ts";
import { CreateFolder, FolderId } from "../server/domain.ts";
import { FileIndexLive } from "../server/file-index.ts";
import { registerFolder } from "../server/folder-registry.ts";
import { createFailures, FolderCreateFailed, folderRoutes } from "../server/folder-routes.ts";
import { jsonBody, WriterTokenLive } from "../server/http.ts";
import { IndexRequestLive } from "../server/index-api.ts";
import { initialiseSearch } from "../server/search-index.ts";
import { Files, FolderKey, Folders } from "../server/streams.ts";
import type { Env } from "./env.ts";
import { IndexRunner, IndexRunnerLive } from "./index-runner.ts";
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

/** An HTTP client whose stream reads go to the folder object that owns each stream. */
const folderObjectClient = (env: Env) =>
  Layer.effect(
    HttpClient.HttpClient,
    Effect.map(HttpClient.HttpClient, (client) =>
      HttpClient.transform(client, (response) =>
        Effect.provideService(response, FetchHttpClient.Fetch, (input, init) => {
          const request = new Request(input, init);
          const match = /^\/streams\/folders\/([^/]+)\/files$/.exec(new URL(request.url).pathname);

          return Option.match(Schema.decodeOption(FolderKey)(match?.[1] ?? ""), {
            onNone: () => Promise.resolve(new Response(null, { status: 404 })),
            onSome: (folderId) => folderObject(env, folderId).fetch(request),
          });
        }),
      ),
    ),
  ).pipe(Layer.provide(FetchHttpClient.layer));

/** `Files` on the index object reads every folder through its owner. */
const remoteFiles = (env: Env) =>
  RemoteStreams.layer(Streamsy.group(Files), { baseUrl: "https://internal/streams" }).pipe(
    Layer.provide(folderObjectClient(env)),
  );

const IndexInternal = Layer.mergeAll(
  // The Worker edge checked the token, and this route checks it again.
  // Registration comes first: a crash leaves a discoverable folder that a retry can finish.
  HttpRouter.addAll(
    folderRoutes((input) =>
      createFailures(
        registerFolder(input).pipe(
          Effect.tap(() => Effect.flatMap(FolderStreams, (streams) => streams.create(input))),
        ),
      ),
    ),
  ),
  HttpRouter.add(
    "POST",
    "/__moved",
    Effect.gen(function* () {
      const input = yield* jsonBody(Schema.Struct({ folderId: FolderId }));
      yield* (yield* IndexRunner).moved(input.folderId);

      return HttpServerResponse.text("indexed");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "POST",
    "/__catchup",
    Effect.gen(function* () {
      yield* (yield* IndexRunner).catchup;

      return HttpServerResponse.text("indexed");
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("Index catch-up deferred", cause).pipe(
          Effect.as(
            HttpServerResponse.jsonUnsafe({ error: "Index catch-up deferred" }, { status: 503 }),
          ),
        ),
      ),
    ),
  ),
);

const indexInitialise = Layer.effectDiscard(
  Effect.gen(function* () {
    yield* initialiseSearch;
    yield* (yield* Folders).create({});
  }),
);

const indexLayer = (state: DurableObjectState, env: Env) =>
  Layer.mergeAll(FileIndexLive, IndexRunnerLive, folderStreams(env), WriterTokenLive).pipe(
    Layer.provideMerge(indexInitialise),
    Layer.provideMerge(remoteFiles(env)),
    Layer.provideMerge(objectStorage(state, env, Streamsy.group(Folders))),
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
);

export class IndexObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof indexLayer>>
>({
  app: IndexApp,
  layer: indexLayer,
}) {}

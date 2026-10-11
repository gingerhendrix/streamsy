import { Effect, Layer, Match, Schema } from "effect";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiMiddleware,
} from "effect/http-api";
import { FileIndex, SearchResult } from "./file-index.ts";
import { ManifestFile } from "./streams.ts";

class IndexBadRequest extends Schema.TaggedError<IndexBadRequest>()(
  "IndexBadRequest",
  {
    error: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

class IndexFailure extends Schema.TaggedError<IndexFailure>()(
  "IndexFailure",
  {
    error: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

class IndexRequestErrors extends HttpApiMiddleware.Service<IndexRequestErrors>()(
  "FolderSync/IndexRequestErrors",
  {
    error: IndexBadRequest,
  },
) {}

class IndexEndpoints extends HttpApiGroup.make("index").add(
  HttpApiEndpoint.get("list", "/ls", {
    query: {
      folder: Schema.optionalKey(Schema.String),
      prefix: Schema.optionalKey(Schema.String),
      glob: Schema.optionalKey(Schema.String),
    },
    success: Schema.Array(ManifestFile),
    error: [IndexBadRequest, IndexFailure],
  }),
  HttpApiEndpoint.get("search", "/search", {
    query: { q: Schema.String, folder: Schema.optionalKey(Schema.String) },
    success: Schema.Array(SearchResult),
    error: [IndexBadRequest, IndexFailure],
  }),
) {}

class IndexApi extends HttpApi.make("FolderSyncIndexApi")
  .add(IndexEndpoints)
  .middleware(IndexRequestErrors)
  .prefix("/api") {}

type IndexError =
  | Effect.Error<ReturnType<typeof FileIndex.Service.list>>
  | Effect.Error<ReturnType<typeof FileIndex.Service.search>>;

const indexErrors = <A, R>(self: Effect.Effect<A, IndexError, R>) =>
  self.pipe(
    Effect.catch((error) =>
      Match.value(error).pipe(
        Match.tag("InvalidFile", ({ message }) =>
          Effect.fail(new IndexBadRequest({ error: message })),
        ),
        Match.orElse((cause) =>
          Effect.logError(cause).pipe(
            Effect.andThen(Effect.fail(new IndexFailure({ error: "Index storage failure" }))),
          ),
        ),
      ),
    ),
  );

const HandlersLive = HttpApiBuilder.group(IndexApi, "index", (handlers) =>
  Effect.gen(function* () {
    const index = yield* FileIndex;

    return handlers
      .handle("list", ({ query }) => index.list(query).pipe(indexErrors))
      .handle("search", ({ query }) => index.search(query.q, query.folder).pipe(indexErrors));
  }),
);

export const IndexApiLive = HttpApiBuilder.layer(IndexApi).pipe(
  Layer.provide(HandlersLive),
  Layer.provide(
    HttpApiMiddleware.layerSchemaErrorTransform(IndexRequestErrors, (error) =>
      Effect.fail(new IndexBadRequest({ error: `Invalid query: ${error.cause.message}` })),
    ),
  ),
);

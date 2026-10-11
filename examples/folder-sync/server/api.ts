import { Effect, Layer, Match, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiMiddleware,
  HttpApiSchema,
} from "effect/http-api";
import { BlobStore } from "./blob-store.ts";
import { FILE_LIMIT, FileOp, FileTooLarge, Hash, Identifier, Version } from "./domain.ts";
import { FolderStore } from "./folder-store.ts";
import { Folder } from "./streams.ts";

class BadRequest extends Schema.TaggedError<BadRequest>()(
  "BadRequest",
  { error: Schema.String },
  { httpApiStatus: 400 },
) {}

class NotFound extends Schema.TaggedError<NotFound>()(
  "NotFound",
  { error: Schema.String },
  { httpApiStatus: 404 },
) {}

class TooLarge extends Schema.TaggedError<TooLarge>()(
  "TooLarge",
  { error: Schema.String },
  { httpApiStatus: 413 },
) {}

class Conflict extends Schema.TaggedError<Conflict>()(
  "Conflict",
  { error: Schema.String },
  { httpApiStatus: 409 },
) {}

class Unavailable extends Schema.TaggedError<Unavailable>()(
  "Unavailable",
  { error: Schema.String },
  { httpApiStatus: 503 },
) {}

class InternalError extends Schema.TaggedError<InternalError>()(
  "InternalError",
  { error: Schema.String },
  { httpApiStatus: 500 },
) {}

class RequestErrors extends HttpApiMiddleware.Service<RequestErrors>()("FolderSync/RequestErrors", {
  error: BadRequest,
}) {}

const errors = [BadRequest, NotFound, TooLarge, Conflict, Unavailable, InternalError];

const fileParams = { folderId: Identifier, "*": Schema.String };

/** HttpApi decodes these URI components before passing metadata to a handler. */
const HeaderIdentifier = Schema.String.check(Schema.isPattern(/^[!-~]+$/)).pipe(
  Schema.decodeTo(Schema.StringFromUriComponent),
  Schema.decodeTo(Identifier),
);

const metadataHeaders = {
  "x-file-base": Schema.optionalKey(Version),
  "x-file-op-id": HeaderIdentifier,
  "x-file-author": HeaderIdentifier,
  "x-file-origin": HeaderIdentifier,
};

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

class FilesApi extends HttpApiGroup.make("files").add(
  HttpApiEndpoint.post("create", "/folders", {
    payload: Schema.Struct({ folderId: Identifier, title: Identifier }),
    success: Folder,
    error: errors,
  }),
  HttpApiEndpoint.put("put", "/folders/:folderId/files/*", {
    params: fileParams,
    headers: metadataHeaders,
    success: Written,
    error: errors,
  }),
  HttpApiEndpoint.delete("delete", "/folders/:folderId/files/*", {
    params: fileParams,
    headers: metadataHeaders,
    success: Written,
    error: errors,
  }),
  HttpApiEndpoint.get("read", "/folders/:folderId/files/*", {
    params: fileParams,
    query: { at: Schema.optionalKey(Version), minVersion: Schema.optionalKey(Version) },
    success: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
    error: errors,
  }),
  HttpApiEndpoint.get("history", "/folders/:folderId/history/*", {
    params: fileParams,
    success: History,
    error: errors,
  }),
  HttpApiEndpoint.get("blob", "/blobs/:hash", {
    params: { hash: Hash },
    success: Schema.Uint8Array.pipe(HttpApiSchema.asUint8Array()),
    error: errors,
  }),
) {}

export class FolderSyncApi extends HttpApi.make("FolderSyncApi")
  .add(FilesApi)
  .middleware(RequestErrors)
  .prefix("/api") {}

type StoreError =
  | Effect.Error<ReturnType<typeof FolderStore.Service.write>>
  | Effect.Error<ReturnType<typeof FolderStore.Service.create>>
  | Effect.Error<ReturnType<typeof FolderStore.Service.read>>
  | Effect.Error<ReturnType<typeof FolderStore.Service.history>>
  | Effect.Error<ReturnType<typeof BlobStore.Service.get>>;

const apiErrors = <A, R>(self: Effect.Effect<A, StoreError, R>) =>
  self.pipe(
    Effect.catch((error) =>
      Match.value(error).pipe(
        Match.tag("InvalidFile", ({ message }) => Effect.fail(new BadRequest({ error: message }))),
        Match.tag("MissingFile", ({ message }) => Effect.fail(new NotFound({ error: message }))),
        Match.tag("FileTooLarge", () =>
          Effect.fail(new TooLarge({ error: "Files must be at most 8 MiB" })),
        ),
        Match.tag("OffsetMismatch", () =>
          Effect.fail(new Conflict({ error: "Concurrent write, retry with the same opId" })),
        ),
        Match.tag("VersionUnavailable", () =>
          Effect.fail(
            new Unavailable({ error: "Folder did not reach minVersion within 10 seconds" }),
          ),
        ),
        Match.tag("WriteHookFailure", ({ cause }) =>
          Effect.logError(cause).pipe(
            Effect.andThen(Effect.fail(new InternalError({ error: "Storage failure" }))),
          ),
        ),
        Match.orElse((cause) =>
          Effect.logError(cause).pipe(
            Effect.andThen(Effect.fail(new InternalError({ error: "Storage failure" }))),
          ),
        ),
      ),
    ),
  );

/** Bound the streamed body before allocating the complete file. Chunked requests have the same limit. */
const requestBytes = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const chunks: Uint8Array[] = [];
  let size = 0;
  yield* request.stream.pipe(
    Stream.runForEach((chunk) => {
      size += chunk.length;

      if (size > FILE_LIMIT) return Effect.fail(new FileTooLarge({ size }));
      chunks.push(chunk);

      return Effect.void;
    }),
    Effect.catchTag("HttpServerError", () => new BadRequest({ error: "Cannot read request body" })),
  );
  const bytes = new Uint8Array(size);
  let offset = 0;

  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }

  return bytes;
});

const HandlersLive = HttpApiBuilder.group(FolderSyncApi, "files", (handlers) =>
  Effect.gen(function* () {
    const store = yield* FolderStore;
    const blobs = yield* BlobStore;

    return handlers
      .handle("create", ({ payload }) =>
        store.create(payload.folderId, payload.title).pipe(apiErrors),
      )
      .handleRaw("put", ({ params, headers }) =>
        Effect.gen(function* () {
          const bytes = yield* requestBytes.pipe(
            Effect.catchTag(
              "FileTooLarge",
              () => new TooLarge({ error: "Files must be at most 8 MiB" }),
            ),
          );

          return yield* store
            .write(
              params.folderId,
              params["*"],
              {
                base: headers["x-file-base"] ?? null,
                opId: headers["x-file-op-id"],
                author: headers["x-file-author"],
                origin: headers["x-file-origin"],
              },
              bytes,
            )
            .pipe(apiErrors);
        }),
      )
      .handle("delete", ({ params, headers }) =>
        store
          .write(params.folderId, params["*"], {
            base: headers["x-file-base"] ?? null,
            opId: headers["x-file-op-id"],
            author: headers["x-file-author"],
            origin: headers["x-file-origin"],
          })
          .pipe(apiErrors),
      )
      .handle("read", ({ params, query }) =>
        store.read(params.folderId, params["*"], query).pipe(
          apiErrors,
          Effect.map(({ bytes, version, operation }) =>
            HttpServerResponse.uint8Array(bytes, {
              headers: { "x-file-version": version, etag: `"${operation.hash}"` },
            }),
          ),
        ),
      )
      .handle("history", ({ params }) =>
        store.history(params.folderId, params["*"]).pipe(apiErrors),
      )
      .handle("blob", ({ params }) =>
        blobs.get(params.hash).pipe(
          apiErrors,
          Effect.flatMap((bytes) =>
            bytes === undefined
              ? Effect.fail(new NotFound({ error: "Blob not found" }))
              : Effect.succeed(
                  HttpServerResponse.uint8Array(bytes, {
                    headers: {
                      "cache-control": "public, max-age=31536000, immutable",
                      etag: `"${params.hash}"`,
                    },
                  }),
                ),
          ),
        ),
      );
  }),
);

const RequestErrorsLive = HttpApiMiddleware.layerSchemaErrorTransform(RequestErrors, (error) =>
  Effect.fail(
    new BadRequest({
      error: `Invalid request ${error.kind.toLowerCase()}: ${error.cause.message}`,
    }),
  ),
);

export const ApiLive = HttpApiBuilder.layer(FolderSyncApi).pipe(
  Layer.provide(HandlersLive),
  Layer.provide(RequestErrorsLive),
);

export const HealthLive = HttpRouter.add("GET", "/", HttpServerResponse.text("Folder sync"));

export const NotFoundLive = HttpRouter.add(
  "*",
  "/api/*",
  HttpServerResponse.jsonUnsafe({ error: "Not found" }, { status: 404 }),
);

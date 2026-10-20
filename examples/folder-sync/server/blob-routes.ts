import { Effect, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { BlobFailure, BlobStore } from "./blob-store.ts";
import { FILE_LIMIT, Hash } from "./domain.ts";
import { authorize, json, requestFailures, storageFailure } from "./http.ts";

/** Content-addressed blobs never change, so any cache may keep them. */
const immutable = (hash: string) => ({
  "cache-control": "public, max-age=31536000, immutable",
  etag: `"${hash}"`,
});

/** `PUT`, `HEAD` and `GET /api/blobs/:hash` on the Bun host. The Worker serves blobs from R2. */
export const blobRoutes = [
  HttpRouter.route(
    "*",
    "/api/blobs/:hash",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const blobs = yield* BlobStore;

      if (request.method === "PUT") yield* authorize();
      const { hash } = yield* HttpRouter.schemaPathParams(Schema.Struct({ hash: Hash }));

      if (request.method === "PUT") {
        const length = request.headers["content-length"];

        if (length !== undefined && Number(length) > FILE_LIMIT)
          return json({ error: "Files must be at most 64 MiB" }, { status: 413 });
        yield* blobs.put(
          hash,
          request.stream.pipe(
            Stream.mapError((cause) => new BlobFailure({ message: String(cause) })),
          ),
        );

        return json({ hash });
      }

      if (request.method === "HEAD") {
        const size = yield* blobs.head(hash);

        return size === undefined
          ? json({ error: "Blob not found" }, { status: 404 })
          : HttpServerResponse.empty({
              status: 200,
              headers: { ...immutable(hash), "content-length": String(size) },
            });
      }

      if (request.method === "GET") {
        const content = yield* blobs.get(hash);

        return content === undefined
          ? json({ error: "Blob not found" }, { status: 404 })
          : HttpServerResponse.stream(content.body, {
              headers: { ...immutable(hash), "content-length": String(content.size) },
            });
      }

      return json({ error: "Method not allowed" }, { status: 405 });
    }).pipe(
      Effect.catchTags(
        {
          Unauthorized: requestFailures.Unauthorized,
          SchemaError: requestFailures.SchemaError,
          InvalidFile: ({ message }) => Effect.succeed(json({ error: message }, { status: 400 })),
          FileTooLarge: () =>
            Effect.succeed(json({ error: "Files must be at most 64 MiB" }, { status: 413 })),
        },
        storageFailure,
      ),
    ),
  ),
];

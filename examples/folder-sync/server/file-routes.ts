import { Array, Effect } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { BlobStore } from "./blob-store.ts";
import { CommitFile, DeleteFile, InvalidFile } from "./domain.ts";
import { FolderStore } from "./folder-store.ts";
import { authorize, json, jsonBody, requestFailures, storageFailure } from "./http.ts";

/**
 * Writes carry their parent in the body and have no precondition. A request that
 * still sends one expects a compare-and-set that the server does not give, so it
 * gets `400` and not a silent last-writer-wins.
 */
const refusePreconditions = (headers: Readonly<Record<string, string | undefined>>) =>
  headers["if-match"] === undefined && headers["if-none-match"] === undefined
    ? Effect.void
    : Effect.fail(
        new InvalidFile({
          message: "Preconditions are not supported. Send the parent version in the body.",
        }),
      );

const decodeSegment = (segment: string) =>
  Effect.try({
    try: () => decodeURIComponent(segment),
    catch: () => new InvalidFile({ message: "Invalid URL" }),
  });

/**
 * `PUT`, `DELETE` and `GET /api/folders/:folderId/files/:path`. A wildcard
 * route reads both segments, because an encoded folder id can pass the router's
 * named-parameter limit and a path holds slashes. Each method has its own route,
 * so the wildcard does not also claim `POST /api/folders`.
 */
const fileHandler = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const store = yield* FolderStore;
  const pathname = new URL(request.url, "http://localhost").pathname;
  const route = /^\/api\/folders\/([^/]+)\/files\/(.+)$/.exec(pathname);

  if (request.method !== "GET") yield* authorize();

  if (route === null) return json({ error: "Not found" }, { status: 404 });
  const folderId = yield* decodeSegment(route[1] ?? "");
  const path = yield* decodeSegment(route[2] ?? "");

  if (request.method === "PUT") {
    yield* refusePreconditions(request.headers);
    const head = yield* store.commit(folderId, path, yield* jsonBody(CommitFile));

    return json(head, { headers: { etag: `"${head.version}"` } });
  }

  if (request.method === "DELETE") {
    yield* refusePreconditions(request.headers);

    return json(yield* store.remove(folderId, path, yield* jsonBody(DeleteFile)));
  }

  // The view names the content by hash. The bytes come from the blob store.
  const { head, value } = yield* store.read(folderId, path);
  const content = yield* (yield* BlobStore).get(value.hash);

  return content === undefined
    ? json({ error: "Blob not found" }, { status: 404 })
    : HttpServerResponse.stream(content.body, {
        headers: { etag: `"${head.version}"`, "content-length": String(content.size) },
      });
}).pipe(
  Effect.catchTags(
    {
      ...requestFailures,
      InvalidFile: ({ message }) => Effect.succeed(json({ error: message }, { status: 400 })),
      MissingFile: ({ message }) => Effect.succeed(json({ error: message }, { status: 404 })),
      BlobMissing: ({ hash }) =>
        Effect.succeed(json({ error: "blob-missing", hash }, { status: 409 })),
      FileTooLarge: () =>
        Effect.succeed(json({ error: "Files must be at most 64 MiB" }, { status: 413 })),
      // The last change of the path is a tombstone. Its version is the parent of a new write.
      DeletedFile: ({ version }) =>
        Effect.succeed(json({ error: "File not found", deleted: version }, { status: 404 })),
      WriteConflict: () =>
        Effect.succeed(
          json(
            { error: "Other writes kept landing first; retry" },
            { status: 503, headers: { "retry-after": "1" } },
          ),
        ),
    },
    storageFailure,
  ),
);

export const fileRoutes = Array.map(["GET", "PUT", "DELETE"] as const, (method) =>
  HttpRouter.route(method, "/api/folders/*", fileHandler),
);

/** Any other `/api` path. A write still needs the token, so a 401 never reveals a route. */
export const fallbackRoutes = [
  HttpRouter.route(
    "*",
    "/api/*",
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;

      if (["PUT", "POST", "DELETE"].includes(request.method)) yield* authorize();

      return json({ error: "Not found" }, { status: 404 });
    }).pipe(Effect.catchTags({ Unauthorized: requestFailures.Unauthorized })),
  ),
];

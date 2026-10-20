import { Effect, Schema } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { BlobStore } from "./blob-store.ts";
import { CommitFile, DeleteFile, InvalidFile, Precondition, VersionFromString } from "./domain.ts";
import { FolderStore } from "./folder-store.ts";
import { authorize, json, jsonBody, requestFailures, storageFailure } from "./http.ts";

/** Read `If-Match: "<version>"` or `If-None-Match: *`. A write may send one of them. */
const precondition = Effect.fn("Api.precondition")(function* (
  headers: Readonly<Record<string, string | undefined>>,
) {
  const match = headers["if-match"];
  const absent = headers["if-none-match"];

  if (match !== undefined && absent !== undefined)
    return yield* new InvalidFile({ message: "Conflicting preconditions" });

  if (absent !== undefined) {
    if (absent !== "*") return yield* new InvalidFile({ message: "Expected If-None-Match: *" });

    return Precondition.IfAbsent();
  }

  if (match === undefined) return Precondition.Unconditional();

  if (!/^"[1-9]\d*"$/.test(match))
    return yield* new InvalidFile({ message: "Expected a quoted version in If-Match" });

  const version = yield* Schema.decodeEffect(VersionFromString)(match.slice(1, -1)).pipe(
    Effect.mapError(() => new InvalidFile({ message: "Invalid version" })),
  );

  return Precondition.IfMatch({ version });
});

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
    const condition = yield* precondition(request.headers);
    const head = yield* store.commit(folderId, path, yield* jsonBody(CommitFile), condition);

    return json(head, { headers: { etag: `"${head.version}"` } });
  }

  if (request.method === "DELETE") {
    const condition = yield* precondition(request.headers);

    return json(yield* store.remove(folderId, path, yield* jsonBody(DeleteFile), condition));
  }

  const { head, value } = yield* store.read(folderId, path);
  const headers = { etag: `"${head.version}"` };

  if (value.text !== undefined)
    return HttpServerResponse.uint8Array(new TextEncoder().encode(value.text), { headers });
  const content = yield* (yield* BlobStore).get(value.hash);

  return content === undefined
    ? json({ error: "Blob not found" }, { status: 404 })
    : HttpServerResponse.stream(content.body, {
        headers: { ...headers, "content-length": String(content.size) },
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
      PreconditionFailed: ({ head }) =>
        Effect.succeed(
          json(head ?? { head: null }, {
            status: 412,
            headers: head === null ? {} : { etag: `"${head.version}"` },
          }),
        ),
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

export const fileRoutes = (["GET", "PUT", "DELETE"] as const).map((method) =>
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

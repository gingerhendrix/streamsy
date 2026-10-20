import { Config, Effect, Layer, Match, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { BlobFailure, BlobStore } from "./blob-store.ts";
import {
  CommitFile,
  CreateFolder,
  DeleteFile,
  FILE_LIMIT,
  Hash,
  InvalidFile,
  VersionFromString,
  type Precondition,
} from "./domain.ts";
import { FolderStore } from "./folder-store.ts";

const json = HttpServerResponse.jsonUnsafe;
export const JSON_BODY_LIMIT = 512 * 1024;

class JsonBodyTooLarge extends Schema.TaggedError<JsonBodyTooLarge>()("JsonBodyTooLarge", {}) {}

const jsonBody = Effect.fn("Api.jsonBody")(function* (
  request: HttpServerRequest.HttpServerRequest,
) {
  const length = request.headers["content-length"];
  if (length !== undefined && Number(length) > JSON_BODY_LIMIT)
    return yield* new JsonBodyTooLarge();
  let size = 0;
  const chunks = yield* request.stream.pipe(
    Stream.tap((chunk) => {
      size += chunk.length;

      return size > JSON_BODY_LIMIT ? Effect.fail(new JsonBodyTooLarge()) : Effect.void;
    }),
    Stream.runCollect,
  );
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }

  return yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
    new TextDecoder().decode(bytes),
  );
});

const precondition = Effect.fn("Api.precondition")(function* (
  headers: Readonly<Record<string, string | undefined>>,
) {
  const match = headers["if-match"];
  const absent = headers["if-none-match"];

  if (match !== undefined && absent !== undefined)
    return yield* new InvalidFile({ message: "Conflicting preconditions" });

  if (absent !== undefined) {
    if (absent !== "*") return yield* new InvalidFile({ message: "Expected If-None-Match: *" });
    const result: Precondition = { absent: true };

    return result;
  }

  if (match === undefined) return undefined;

  if (!/^"[1-9]\d*"$/.test(match))
    return yield* new InvalidFile({ message: "Expected a quoted version in If-Match" });

  const version = yield* Schema.decodeEffect(VersionFromString)(match.slice(1, -1)).pipe(
    Effect.mapError(() => new InvalidFile({ message: "Invalid version" })),
  );

  return { version };
});

const make = Effect.gen(function* () {
  const store = yield* FolderStore;
  const blobs = yield* BlobStore;
  const token = yield* Config.String("FOLDER_SYNC_WRITER_TOKEN").pipe(Config.withDefault(""));

  const handle = Effect.fn("Api.handle")(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const pathname = new URL(request.url, "http://localhost").pathname;
    const method = request.method;

    if (
      ["PUT", "POST", "DELETE"].includes(method) &&
      (token === "" || request.headers.authorization !== `Bearer ${token}`)
    )
      return json({ error: "Unauthorized" }, { status: 401 });

    if (pathname === "/api/folders" && method === "POST") {
      const body = yield* Schema.decodeUnknownEffect(CreateFolder)(yield* jsonBody(request));

      return json(yield* store.create(body.folderId, body.title, body.txid));
    }

    const blob = /^\/api\/blobs\/([^/]+)$/.exec(pathname);

    if (blob !== null) {
      const hash = yield* Schema.decodeEffect(Hash)(blob[1] ?? "");
      const headers = { "cache-control": "public, max-age=31536000, immutable", etag: `"${hash}"` };

      if (method === "PUT") {
        const length = request.headers["content-length"];

        if (length !== undefined && Number(length) > FILE_LIMIT)
          return json({ error: "File too large" }, { status: 413 });
        yield* blobs.put(
          hash,
          request.stream.pipe(
            Stream.mapError((cause) => new BlobFailure({ message: String(cause) })),
          ),
        );

        return json({ hash });
      }

      if (method === "HEAD") {
        const size = yield* blobs.head(hash);

        return size === undefined
          ? json({ error: "Blob not found" }, { status: 404 })
          : HttpServerResponse.empty({
              status: 200,
              headers: { ...headers, "content-length": String(size) },
            });
      }

      if (method === "GET") {
        const content = yield* blobs.get(hash);

        return content === undefined
          ? json({ error: "Blob not found" }, { status: 404 })
          : HttpServerResponse.stream(content.body, {
              headers: { ...headers, "content-length": String(content.size) },
            });
      }
    }

    const file = /^\/api\/folders\/([^/]+)\/files\/(.+)$/.exec(pathname);

    if (file !== null) {
      const folderId = yield* Effect.try({
        try: () => decodeURIComponent(file[1] ?? ""),
        catch: () => new InvalidFile({ message: "Invalid URL" }),
      });

      const path = yield* Effect.try({
        try: () => decodeURIComponent(file[2] ?? ""),
        catch: () => new InvalidFile({ message: "Invalid URL" }),
      });

      if (method === "PUT") {
        const condition = yield* precondition(request.headers);
        const body = yield* Schema.decodeUnknownEffect(CommitFile)(yield* jsonBody(request));
        const head = yield* store.commit(folderId, path, body, condition);

        return json(head, { headers: { etag: `"${head.version}"` } });
      }

      if (method === "DELETE") {
        const condition = yield* precondition(request.headers);
        const body = yield* Schema.decodeUnknownEffect(DeleteFile)(yield* jsonBody(request));

        return json(yield* store.remove(folderId, path, body, condition));
      }

      if (method === "GET") {
        const { head, value } = yield* store.read(folderId, path);
        const headers = { etag: `"${head.version}"` };

        if (value.text !== undefined)
          return HttpServerResponse.uint8Array(new TextEncoder().encode(value.text), { headers });
        const content = yield* blobs.get(value.hash);

        return content === undefined
          ? json({ error: "Blob not found" }, { status: 404 })
          : HttpServerResponse.stream(content.body, {
              headers: { ...headers, "content-length": String(content.size) },
            });
      }
    }

    return json({ error: "Not found" }, { status: 404 });
  });

  return yield* handle().pipe(
    Effect.catch((error) =>
      Match.value(error).pipe(
        Match.tag("JsonBodyTooLarge", () =>
          Effect.succeed(
            json({ error: "JSON write bodies must be at most 512 KiB" }, { status: 413 }),
          ),
        ),
        Match.tag("InvalidFile", ({ message }) =>
          Effect.succeed(json({ error: message }, { status: 400 })),
        ),
        Match.tag("SchemaError", () =>
          Effect.succeed(json({ error: "Invalid request" }, { status: 400 })),
        ),
        Match.tag("HttpServerError", () =>
          Effect.succeed(json({ error: "Invalid body" }, { status: 400 })),
        ),
        Match.tag("MissingFile", ({ message }) =>
          Effect.succeed(json({ error: message }, { status: 404 })),
        ),
        Match.tag("FolderConflict", ({ message }) =>
          Effect.succeed(json({ error: message }, { status: 409 })),
        ),
        Match.tag("BlobMissing", ({ hash }) =>
          Effect.succeed(json({ error: "blob-missing", hash }, { status: 409 })),
        ),
        Match.tag("FileTooLarge", () =>
          Effect.succeed(json({ error: "Files must be at most 64 MiB" }, { status: 413 })),
        ),
        Match.tag("PreconditionFailed", ({ head }) =>
          Effect.succeed(
            json(head ?? { head: null }, {
              status: 412,
              headers: head === null ? {} : { etag: `"${head.version}"` },
            }),
          ),
        ),
        Match.orElse((cause) =>
          Effect.logError(cause).pipe(
            Effect.as(json({ error: "Storage failure" }, { status: 500 })),
          ),
        ),
      ),
    ),
  );
});

export const ApiRequestLive = HttpRouter.add("*", "/api/*", make);

export const ApiLive = ApiRequestLive.pipe(
  HttpRouter.provideRequest(Layer.effectContext(Effect.context<FolderStore | BlobStore>())),
);

export const HealthLive = HttpRouter.add("GET", "/health", HttpServerResponse.text("Folder sync"));

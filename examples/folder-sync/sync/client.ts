import { Offset } from "@streamsy/core";
import { Context, Effect, FileSystem, Layer, Option, Schema, Stream } from "effect";
import { HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { File, Head, INLINE_LIMIT, inlineText, VersionFromString } from "../server/domain.ts";
import { Folder, FolderKey } from "../server/streams.ts";
import { encodeFilePath } from "./decisions.ts";
import type { Pending } from "./state.ts";

export class RemoteFailure extends Schema.TaggedError<RemoteFailure>()("RemoteFailure", {
  message: Schema.String,
  retryable: Schema.Boolean,
  status: Schema.Int,
}) {}

export class StaleFile extends Schema.TaggedError<StaleFile>()("StaleFile", {
  head: Schema.NullOr(Head),
}) {}

export class BlobMissing extends Schema.TaggedError<BlobMissing>()("SyncBlobMissing", {}) {}

const headers = Schema.Struct({
  operation: Schema.Literals(["upsert", "delete"]),
  txid: Schema.String,
});

export const FileMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("file"),
    key: Schema.String,
    value: Schema.optionalKey(File),
    headers,
  }),
  Schema.Struct({ headers: Schema.Struct({ control: Schema.Literal("reset") }) }),
]);

export type FileMessage = typeof FileMessage.Type;

const FolderMessage = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("folder"),
    key: Schema.String,
    value: Schema.optionalKey(Folder),
    headers,
  }),
  Schema.Struct({ headers: Schema.Struct({ control: Schema.Literal("reset") }) }),
]);

const receipt = Schema.Struct({
  version: Schema.optionalKey(File.fields.version),
  txid: Schema.optionalKey(Schema.String),
  hash: Schema.optionalKey(Head.fields.hash),
});

const reject = (status: number, message = `Server answered ${status}`) =>
  new RemoteFailure({
    status,
    message,
    retryable: status === 0 || status >= 500 || [408, 429].includes(status),
  });

const cursor = (values: Readonly<Record<string, string>>) =>
  Schema.decodeUnknownEffect(Offset)(values["stream-next-offset"]);

const makeRemote = (server: string, token?: string) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const fs = yield* FileSystem.FileSystem;
    const root = server.replace(/\/$/, "");

    const fileUrl = (folderId: string, path: string) =>
      `${root}/api/folders/${encodeURIComponent(folderId)}/files/${encodeFilePath(path)}`;

    const streamUrl = (folderId: string) =>
      `${root}/streams/folders/${Schema.encodeSync(FolderKey)(folderId)}/files`;

    const request = Effect.fn("SyncRemote.request")(function* (
      req: HttpClientRequest.HttpClientRequest,
    ) {
      return yield* client
        .execute(token === undefined ? req : HttpClientRequest.bearerToken(req, token))
        .pipe(
          Effect.mapError(
            (cause) => new RemoteFailure({ message: String(cause), retryable: true, status: 0 }),
          ),
        );
    });

    const pageAt = Effect.fn("SyncRemote.pageAt")(function* <
      S extends Schema.ConstraintDecoder<unknown>,
    >(url: string, schema: S, offset: string, live: boolean) {
      const response = yield* request(
        HttpClientRequest.get(
          `${url}?offset=${encodeURIComponent(offset)}${live ? "&live=long-poll" : ""}`,
        ),
      );

      if (response.status === 410) return { rows: [], cursor: "-1", upToDate: false, reset: true };

      if (![200, 204].includes(response.status)) return yield* reject(response.status);

      return {
        rows:
          response.status === 204
            ? []
            : yield* Schema.decodeUnknownEffect(Schema.Array(schema))(yield* response.json),
        cursor: yield* cursor(response.headers),
        upToDate: response.headers["stream-up-to-date"] === "true",
        reset: false,
      };
    });

    const folders = (offset: string, live: boolean) =>
      pageAt(`${root}/streams/folders`, FolderMessage, offset, live);

    const page = (folderId: string, offset: string, live: boolean) =>
      pageAt(streamUrl(folderId), FileMessage, offset, live);

    const tail = Effect.fn("SyncRemote.tail")(function* (folderId: string) {
      const response = yield* request(HttpClientRequest.head(streamUrl(folderId)));

      if (response.status !== 200) return yield* reject(response.status);

      return yield* cursor(response.headers);
    });

    const body = Effect.fn("SyncRemote.body")(function* (hash: string) {
      const response = yield* request(HttpClientRequest.get(`${root}/api/blobs/${hash}`));

      if (response.status !== 200) return yield* reject(response.status);

      return response.stream.pipe(Stream.mapError((cause) => reject(0, String(cause))));
    });

    const head = Effect.fn("SyncRemote.head")(function* (folderId: string, path: string) {
      const response = yield* request(HttpClientRequest.get(fileUrl(folderId, path)));

      if (response.status === 404) return null;

      if (response.status !== 200) return yield* reject(response.status);

      const version = yield* Schema.decodeUnknownEffect(VersionFromString)(
        response.headers.etag?.replace(/^"|"$/g, ""),
      );

      return {
        version,
        body: response.stream.pipe(Stream.mapError((cause) => reject(0, String(cause)))),
      };
    });

    const upload = Effect.fn("SyncRemote.upload")(function* (operation: Pending) {
      if (operation.source === null || operation.hash === null) return yield* Effect.void;

      const response = yield* request(
        HttpClientRequest.put(`${root}/api/blobs/${operation.hash}`, {
          // FetchHttpClient drops explicit Content-Length on Effect streams.
          // Bun's file Blob streams the immutable source and supplies its known length.
          body: HttpBody.raw(Bun.file(operation.source), {
            contentType: "application/octet-stream",
            contentLength: operation.size,
          }),
        }),
      );

      if (response.status !== 200) return yield* reject(response.status);
      yield* response.text;

      return yield* Effect.void;
    });

    const push = Effect.fn("SyncRemote.push")(function* (operation: Pending) {
      // Defence for callers outside the daemon: an unseen file must never be deleted.
      if (operation.source === null && operation.base === null)
        return yield* Schema.decodeEffect(receipt)({});

      let text: string | undefined;

      if (operation.source !== null && operation.size <= INLINE_LIMIT)
        text = inlineText(yield* fs.readFile(operation.source));

      const content = {
        txid: operation.txid,
        author: "sync CLI",
        hash: operation.hash,
        size: operation.size,
      };

      const payload =
        operation.source === null
          ? { txid: operation.txid, author: "sync CLI" }
          : text === undefined
            ? content
            : { ...content, text };

      const condition =
        operation.base === null ? { "if-none-match": "*" } : { "if-match": `"${operation.base}"` };

      const req =
        operation.source === null
          ? HttpClientRequest.delete(fileUrl(operation.folderId, operation.path))
          : HttpClientRequest.put(fileUrl(operation.folderId, operation.path));

      const response = yield* request(
        req.pipe(
          HttpClientRequest.setHeaders(condition),
          HttpClientRequest.bodyJsonUnsafe(payload),
        ),
      );

      if (response.status === 412) {
        const raw = yield* response.json;
        const missing = Schema.decodeUnknownOption(Schema.Struct({ head: Schema.Null }))(raw);

        if (Option.isSome(missing)) return yield* new StaleFile({ head: null });

        return yield* new StaleFile({ head: yield* Schema.decodeUnknownEffect(Head)(raw) });
      }

      if (response.status === 409) {
        const error = yield* Schema.decodeUnknownEffect(Schema.Struct({ error: Schema.String }))(
          yield* response.json,
        );

        if (error.error === "blob-missing") return yield* new BlobMissing();

        return yield* reject(409, error.error);
      }

      if (response.status !== 200) return yield* reject(response.status);

      return yield* Schema.decodeUnknownEffect(receipt)(yield* response.json);
    });

    return { folders, page, tail, body, head, upload, push };
  });

export class SyncRemote extends Context.Service<
  SyncRemote,
  Effect.Success<ReturnType<typeof makeRemote>>
>()("FolderSync/SyncRemote") {}

export const remoteLayer = (server: string, token?: string) =>
  Layer.effect(SyncRemote, makeRemote(server, token));

import { Offset, Streamsy } from "@streamsy/core";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";
import { HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { Head, Version, VersionFromString } from "../server/domain.ts";
import { FileMessage, FolderMessage } from "../server/messages.ts";
import {
  blobUrl,
  fileUrl,
  folderStreamUrl,
  FOLDERS_STREAM_URL,
  streamRead,
  WAKE_STREAM_URL,
} from "../server/routes.ts";
import type { Pending } from "./state.ts";

export class RemoteFailure extends Schema.TaggedError<
  RemoteFailure,
  { readonly brand: unique symbol }
>()("RemoteFailure", {
  message: Schema.String,
  retryable: Schema.Boolean,
  status: Schema.Int,
}) {}

export class SyncBlobMissing extends Schema.TaggedError<
  SyncBlobMissing,
  { readonly brand: unique symbol }
>()("SyncBlobMissing", {}) {}

/** A commit answer, a delete answer, or `{}` for a delete of a path with no live file. */
const receipt = Schema.Struct({
  version: Schema.optionalKey(Head.fields.version),
  parent: Schema.optionalKey(Head.fields.parent),
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
    const root = server.replace(/\/$/, "");

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
        HttpClientRequest.get(`${root}${streamRead(url, offset, live)}`),
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
      pageAt(FOLDERS_STREAM_URL, FolderMessage, offset, live);

    const page = (folderId: string, offset: string, live: boolean) =>
      pageAt(folderStreamUrl(folderId), FileMessage, offset, live);

    /** One page of the wake stream: one row for each folder stream that moved. */
    const wake = (offset: string, live: boolean) =>
      pageAt(WAKE_STREAM_URL, Streamsy.WakeMessage, offset, live);

    const tail = Effect.fn("SyncRemote.tail")(function* (folderId: string) {
      const response = yield* request(
        HttpClientRequest.head(`${root}${folderStreamUrl(folderId)}`),
      );

      if (response.status !== 200) return yield* reject(response.status);

      return yield* cursor(response.headers);
    });

    const body = Effect.fn("SyncRemote.body")(function* (hash: string) {
      const response = yield* request(HttpClientRequest.get(`${root}${blobUrl(hash)}`));

      if (response.status !== 200) return yield* reject(response.status);

      return response.stream.pipe(Stream.mapError((cause) => reject(0, String(cause))));
    });

    const head = Effect.fn("SyncRemote.head")(function* (folderId: string, path: string) {
      const response = yield* request(HttpClientRequest.get(`${root}${fileUrl(folderId, path)}`));

      // A deleted path answers 404 with the version of its tombstone.
      if (response.status === 404) {
        const missing = Schema.decodeUnknownOption(Schema.Struct({ deleted: Version }))(
          yield* response.json.pipe(Effect.orElseSucceed(() => ({}))),
        );

        return Option.getOrNull(missing);
      }

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
        HttpClientRequest.put(`${root}${blobUrl(operation.hash)}`, {
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

      // The parent is the version that the local edit started from. There is no precondition.
      const payload =
        operation.source === null
          ? { txid: operation.txid, author: "sync CLI", parent: operation.base }
          : {
              txid: operation.txid,
              author: "sync CLI",
              parent: operation.base,
              hash: operation.hash,
              size: operation.size,
            };

      const url = `${root}${fileUrl(operation.folderId, operation.path)}`;

      const req =
        operation.source === null ? HttpClientRequest.delete(url) : HttpClientRequest.put(url);

      const response = yield* request(req.pipe(HttpClientRequest.bodyJsonUnsafe(payload)));

      if (response.status === 409) {
        const error = yield* Schema.decodeUnknownEffect(Schema.Struct({ error: Schema.String }))(
          yield* response.json,
        );

        if (error.error === "blob-missing") return yield* new SyncBlobMissing();

        return yield* reject(409, error.error);
      }

      if (response.status !== 200) return yield* reject(response.status);

      return yield* Schema.decodeUnknownEffect(receipt)(yield* response.json);
    });

    return { folders, page, wake, tail, body, head, upload, push };
  });

export class SyncRemote extends Context.Service<
  SyncRemote,
  Effect.Success<ReturnType<typeof makeRemote>>
>()("FolderSync/SyncRemote") {}

export const remoteLayer = (server: string, token?: string) =>
  Layer.effect(SyncRemote, makeRemote(server, token));

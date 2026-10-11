import type { HttpClientError } from "effect/http/HttpClientError";
import { Context, Effect, Layer, Schema } from "effect";
import { HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { FileOp, Hash, Version } from "../server/domain.ts";
import { Change, ManifestFile } from "../server/streams.ts";
import { encodeFilePath } from "./decisions.ts";
import type { Pending } from "./state.ts";

export class RemoteFailure extends Schema.TaggedError<RemoteFailure>()("RemoteFailure", {
  message: Schema.String,
  retryable: Schema.Boolean,
  status: Schema.Int,
}) {}

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

export interface RemoteHead {
  readonly version: string | null;
  readonly hash: string | null;
  readonly bytes: Uint8Array | null;
}

const reject = (status: number) =>
  new RemoteFailure({ message: `Server answered ${status}`, retryable: false, status });

const cursor = (headers: Readonly<Record<string, string>>) =>
  Schema.decodeUnknownEffect(Version)(headers["stream-next-offset"]);

export const remoteLayer = (server: string) =>
  Layer.effect(
    SyncRemote,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const root = server.replace(/\/$/, "");

      const fileUrl = (folderId: string, path: string) =>
        `${root}/api/folders/${encodeURIComponent(folderId)}/files/${encodeFilePath(path)}`;

      const request = Effect.fn("SyncRemote.request")(function* (
        req: HttpClientRequest.HttpClientRequest,
      ) {
        const response = yield* client
          .execute(req)
          .pipe(
            Effect.mapError(
              (cause) => new RemoteFailure({ message: String(cause), retryable: true, status: 0 }),
            ),
          );

        if (response.status >= 500 || response.status === 409 || response.status === 429)
          return yield* new RemoteFailure({
            message: `Server answered ${response.status}`,
            retryable: true,
            status: response.status,
          });

        return response;
      });

      const snapshot = Effect.gen(function* () {
        const response = yield* request(HttpClientRequest.get(`${root}/api/ls`));

        if (response.status !== 200) return yield* reject(response.status);

        return {
          files: yield* Schema.decodeUnknownEffect(Schema.Array(ManifestFile))(
            yield* response.json,
          ),
          cursor: yield* cursor(response.headers),
        };
      });

      const tail = Effect.gen(function* () {
        const response = yield* request(HttpClientRequest.head(`${root}/streams/changes`));

        if (response.status !== 200) return yield* reject(response.status);

        return yield* cursor(response.headers);
      });

      const page = Effect.fn("SyncRemote.page")(function* (offset: string, live: boolean) {
        const response = yield* request(
          HttpClientRequest.get(
            `${root}/streams/changes?offset=${encodeURIComponent(offset)}${live ? "&live=long-poll" : ""}`,
          ),
        );

        if (response.status !== 200 && response.status !== 204)
          return yield* reject(response.status);

        return {
          rows:
            response.status === 204
              ? []
              : yield* Schema.decodeUnknownEffect(Schema.Array(Change))(yield* response.json),
          cursor: yield* cursor(response.headers),
          upToDate: response.headers["stream-up-to-date"] === "true",
        };
      });

      const head = Effect.fn("SyncRemote.head")(function* (
        folderId: string,
        path: string,
        minVersion?: string,
      ): Effect.fn.Return<RemoteHead, RemoteFailure | RemoteDecodeError> {
        const response = yield* request(
          HttpClientRequest.get(
            `${fileUrl(folderId, path)}${minVersion === undefined ? "" : `?minVersion=${encodeURIComponent(minVersion)}`}`,
          ),
        );

        if (response.status === 200) {
          const version = yield* Schema.decodeUnknownEffect(Version)(
            response.headers["x-file-version"],
          );

          const hash = yield* Schema.decodeUnknownEffect(Hash)(
            response.headers.etag?.replace(/^"|"$/g, ""),
          );

          return { version, hash, bytes: new Uint8Array(yield* response.arrayBuffer) };
        }

        if (response.status !== 404) return yield* reject(response.status);
        const historyUrl = `${root}/api/folders/${encodeURIComponent(folderId)}/history/${encodeFilePath(path)}`;
        const history = yield* request(HttpClientRequest.get(historyUrl));

        if (history.status === 404) return { version: null, hash: null, bytes: null };

        if (history.status !== 200) return yield* reject(history.status);
        const last = (yield* Schema.decodeUnknownEffect(History)(yield* history.json)).at(-1);

        // A recreation raced the 404. Retry the head rather than treating a live operation as a delete.
        if (last?.operation.type === "put") return yield* head(folderId, path, last.version);

        return { version: last?.version ?? null, hash: null, bytes: null };
      });

      const push = Effect.fn("SyncRemote.push")(function* (operation: Pending, origin: string) {
        const common = {
          "x-file-op-id": encodeURIComponent(operation.opId),
          "x-file-author": encodeURIComponent("sync CLI"),
          "x-file-origin": encodeURIComponent(origin),
        };

        const headers =
          operation.base === null ? common : { ...common, "x-file-base": operation.base };

        const url = fileUrl(operation.folderId, operation.path);

        const req =
          operation.bytes === null
            ? HttpClientRequest.delete(url, { headers })
            : HttpClientRequest.put(url, { headers, body: HttpBody.uint8Array(operation.bytes) });

        const response = yield* request(req);

        // A new local delete can race a remote delete. There is already no live head to delete.
        if (response.status === 404 && operation.bytes === null) return { version: null };

        if (response.status !== 200) return yield* reject(response.status);

        return yield* Schema.decodeUnknownEffect(Written)(yield* response.json);
      });

      return SyncRemote.of({ snapshot, tail, page, head, push });
    }),
  );

type RemoteDecodeError = HttpClientError | Schema.SchemaError;

export class SyncRemote extends Context.Service<
  SyncRemote,
  {
    readonly snapshot: Effect.Effect<
      { files: ReadonlyArray<ManifestFile>; cursor: string },
      RemoteFailure | RemoteDecodeError
    >;
    readonly tail: Effect.Effect<string, RemoteFailure | RemoteDecodeError>;
    readonly page: (
      offset: string,
      live: boolean,
    ) => Effect.Effect<
      { rows: ReadonlyArray<Change>; cursor: string; upToDate: boolean },
      RemoteFailure | RemoteDecodeError
    >;
    readonly head: (
      folderId: string,
      path: string,
      minVersion?: string,
    ) => Effect.Effect<RemoteHead, RemoteFailure | RemoteDecodeError>;
    readonly push: (
      operation: Pending,
      origin: string,
    ) => Effect.Effect<
      { version: string | null; overwrote?: string },
      RemoteFailure | RemoteDecodeError
    >;
  }
>()("FolderSync/SyncRemote") {}

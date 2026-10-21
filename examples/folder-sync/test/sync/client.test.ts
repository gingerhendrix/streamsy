import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Result, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { remoteLayer, SyncBlobMissing, SyncRemote } from "../../sync/client.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { captureLocal } from "../../sync/local.ts";
import { FileSystem } from "effect";

const encodeBody = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const decodeBody = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const cursor = "0000000000000001_0000000000000000";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "folder URLs, reset/410, parents, receipts and write statuses follow the protocol",
    () =>
      Effect.gen(function* () {
        let status = 204;

        type MockBody =
          | ReadonlyArray<{ headers: { control: "reset" } }>
          | {
              version?: number;
              parent?: number | null;
              txid?: string;
              hash?: string;
              error?: string;
              deleted?: number;
            };

        let responseBody: MockBody = {};

        const requests: Array<{
          url: string;
          headers: Readonly<Record<string, string>>;
          method: string;
          body: unknown;
        }> = [];

        const http = HttpClient.make((request, url) =>
          Effect.sync(() => {
            requests.push({
              url: url.href,
              headers: request.headers,
              method: request.method,
              body:
                request.body._tag === "Uint8Array"
                  ? decodeBody(new TextDecoder().decode(request.body.body))
                  : undefined,
            });

            return HttpClientResponse.fromWeb(
              request,
              new Response(status === 204 ? null : encodeBody(responseBody), {
                status,
                headers: { "stream-next-offset": cursor, "stream-up-to-date": "true" },
              }),
            );
          }),
        );

        const context = yield* Layer.build(
          remoteLayer("http://sync.test", "token").pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
          ),
        );

        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        expect(yield* remote.page("daily/demo", cursor, true)).toEqual({
          rows: [],
          cursor,
          upToDate: true,
          reset: false,
        });
        expect(requests.at(-1)?.url).toContain("/streams/folders/6461696c792f64656d6f/files?");
        status = 410;
        expect((yield* remote.page("daily/demo", cursor, false)).reset).toBe(true);
        status = 200;
        responseBody = [{ headers: { control: "reset" } }];
        expect((yield* remote.page("daily/demo", cursor, false)).rows).toEqual([
          { headers: { control: "reset" } },
        ]);
        const directory = yield* testDirectory("client-source-");
        const fs = yield* FileSystem.FileSystem;
        yield* fs.makeDirectory(`${directory}/.folder-sync`, { recursive: true });
        yield* fs.writeFileString(`${directory}/a`, "text");
        const content = yield* captureLocal(directory, "a");

        if (content === null) return yield* Effect.die("Missing source");

        const op = {
          seq: 1,
          folderId: "daily/demo",
          path: "a%2Fb.md",
          txid: "my-txid",
          base: 4,
          ...content,
          uploaded: 0,
        };

        responseBody = { version: 5, parent: 4, txid: "first-txid", hash: content.hash };
        expect(yield* remote.push(op)).toEqual({
          version: 5,
          parent: 4,
          txid: "first-txid",
          hash: content.hash,
        });
        expect(requests.at(-1)).toMatchObject({
          method: "PUT",
          headers: { authorization: "Bearer token", "content-type": "application/json" },
          body: { txid: "my-txid", parent: 4, hash: content.hash, size: content.size },
        });
        // The parent is in the body. No precondition header is sent.
        expect(requests.at(-1)?.headers).not.toHaveProperty("if-match");
        expect(requests.at(-1)?.url).toContain("/folders/daily%2Fdemo/files/a%252Fb.md");
        yield* remote.push({ ...op, base: null });
        expect(requests.at(-1)?.body).toMatchObject({ parent: null });
        expect(requests.at(-1)?.headers).not.toHaveProperty("if-none-match");
        const count = requests.length;
        expect(
          yield* remote.push({ ...op, base: null, source: null, hash: null, size: 0 }),
        ).toEqual({});
        expect(requests).toHaveLength(count);
        responseBody = {};
        expect(yield* remote.push({ ...op, source: null, hash: null, size: 0 })).toEqual({});
        expect(requests.at(-1)).toMatchObject({
          method: "DELETE",
          body: { txid: "my-txid", author: "sync CLI", parent: 4 },
        });
        // A deleted path reads as its tombstone version. A path with no change reads as null.
        status = 404;
        responseBody = { error: "File not found", deleted: 6 };
        expect(yield* remote.head("daily/demo", "a.md")).toEqual({ deleted: 6 });
        responseBody = { error: "File not found" };
        expect(yield* remote.head("daily/demo", "a.md")).toBeNull();
        status = 409;
        responseBody = { error: "blob-missing", hash: content.hash };
        expect(Schema.is(SyncBlobMissing)(yield* Effect.flip(remote.push(op)))).toBe(true);

        for (const [code, retryable] of [
          [401, false],
          [403, false],
          [400, false],
          [413, false],
          [500, true],
          [503, true],
          [429, true],
        ] as const) {
          status = code;
          const result = yield* remote.push(op).pipe(Effect.result);
          expect(Result.isFailure(result)).toBe(true);

          if (Result.isFailure(result))
            expect(result.failure).toMatchObject({ status: code, retryable });
        }

        status = 200;
        responseBody = { version: 6, parent: 4, txid: "deleted" };
        expect(yield* remote.push({ ...op, source: null, hash: null, size: 0 })).toEqual({
          version: 6,
          parent: 4,
          txid: "deleted",
        });
        expect(requests.at(-1)?.method).toBe("DELETE");

        return yield* Effect.void;
      }),
  );
});

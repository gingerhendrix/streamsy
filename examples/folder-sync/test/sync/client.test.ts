import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Result, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { BlobMissing, remoteLayer, StaleFile, SyncRemote } from "../../sync/client.ts";
import { testDirectory } from "../../scripts/test-directory.ts";
import { captureLocal } from "../../sync/local.ts";
import { FileSystem } from "effect";

const encodeBody = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const cursor = "0000000000000001_0000000000000000";

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect(
    "folder URLs, reset/410, preconditions, receipts and write statuses follow the protocol",
    () =>
      Effect.gen(function* () {
        let status = 204;

        type MockBody =
          | ReadonlyArray<{ headers: { control: "reset" } }>
          | { version?: number; txid?: string; hash?: string; head?: null; error?: string };

        let responseBody: MockBody = {};

        const requests: Array<{
          url: string;
          headers: Readonly<Record<string, string>>;
          method: string;
        }> = [];

        const http = HttpClient.make((request, url) =>
          Effect.sync(() => {
            requests.push({ url: url.href, headers: request.headers, method: request.method });

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

        responseBody = { version: 5, txid: "first-txid", hash: content.hash };
        expect((yield* remote.push(op)).txid).toBe("first-txid");
        expect(requests.at(-1)).toMatchObject({
          method: "PUT",
          headers: {
            "if-match": '"4"',
            authorization: "Bearer token",
            "content-type": "application/json",
          },
        });
        expect(requests.at(-1)?.url).toContain("/folders/daily%2Fdemo/files/a%252Fb.md");
        yield* remote.push({ ...op, base: null });
        expect(requests.at(-1)?.headers["if-none-match"]).toBe("*");
        const count = requests.length;
        expect(
          yield* remote.push({ ...op, base: null, source: null, hash: null, size: 0 }),
        ).toEqual({});
        expect(requests).toHaveLength(count);
        responseBody = {};
        expect(yield* remote.push({ ...op, source: null, hash: null, size: 0 })).toEqual({});
        expect(requests.at(-1)).toMatchObject({ method: "DELETE", headers: { "if-match": '"4"' } });
        status = 412;
        responseBody = { head: null };
        const stale = yield* Effect.flip(remote.push(op));
        expect(Schema.is(StaleFile)(stale)).toBe(true);
        status = 409;
        responseBody = { error: "blob-missing", hash: content.hash };
        expect(Schema.is(BlobMissing)(yield* Effect.flip(remote.push(op)))).toBe(true);

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
        responseBody = { txid: "deleted" };
        expect(yield* remote.push({ ...op, source: null, hash: null, size: 0 })).toEqual({
          txid: "deleted",
        });
        expect(requests.at(-1)?.method).toBe("DELETE");

        return yield* Effect.void;
      }),
  );
});

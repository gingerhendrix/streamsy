import { expect, layer } from "@streamsy/effect-test";
import { Effect, Layer, Result } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { RemoteFailure, SyncRemote, remoteLayer } from "../../sync/client.ts";

const version = "0000000000000001_0000000000000000";

layer(Layer.empty, { excludeTestServices: true })((it) => {
  it.effect(
    "204 retains the cursor; 5xx and minVersion 503 remain retryable; URLs and metadata are encoded",
    () =>
      Effect.gen(function* () {
        let status = 204;
        let seenUrl = "";
        let seenBase: string | undefined;
        let seenOrigin: string | undefined;

        const http = HttpClient.make((request, url) =>
          Effect.sync(() => {
            seenUrl = url.href;
            seenBase = request.headers["x-file-base"];
            seenOrigin = request.headers["x-file-origin"];

            return HttpClientResponse.fromWeb(
              request,
              new Response(status === 200 ? `{"version":"${version}"}` : null, {
                status,
                headers: { "stream-next-offset": version, "stream-up-to-date": "true" },
              }),
            );
          }),
        );

        const context = yield* Layer.build(
          remoteLayer("http://sync.test").pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
          ),
        );

        const remote = yield* SyncRemote.pipe(Effect.provideContext(context));
        const empty = yield* remote.page(version, true);
        expect(empty).toEqual({ rows: [], cursor: version, upToDate: true });
        expect(seenUrl).toContain("live=long-poll");

        for (const code of [500, 503]) {
          status = code;

          const result = yield* remote
            .head("daily/demo", "docs/雪 100%.md", version)
            .pipe(Effect.result);

          expect(Result.isFailure(result)).toBe(true);

          if (Result.isFailure(result)) {
            expect(result.failure).toBeInstanceOf(RemoteFailure);
            expect(result.failure).toMatchObject({ retryable: true, status: code });
          }

          expect(seenUrl).toContain(
            "/folders/daily%2Fdemo/files/docs/%E9%9B%AA%20100%25.md?minVersion=",
          );
        }

        status = 200;
        yield* remote.push(
          {
            seq: 1,
            folderId: "daily/demo",
            path: "a%2Fb.md",
            base: version,
            opId: "op/雪",
            bytes: new TextEncoder().encode("text"),
            hash: "a".repeat(64),
          },
          "client/雪",
        );
        expect(seenUrl).toContain("/files/a%252Fb.md");
        expect(seenBase).toBe(version);
        expect(seenOrigin).toBe(encodeURIComponent("client/雪"));
        status = 400;

        const invalid = yield* remote
          .push(
            {
              seq: 2,
              folderId: "docs",
              path: "a",
              base: null,
              opId: "invalid",
              bytes: new Uint8Array(),
              hash: "a".repeat(64),
            },
            "test",
          )
          .pipe(Effect.result);

        if (Result.isFailure(invalid))
          expect(invalid.failure).toMatchObject({ retryable: false, status: 400 });
        else throw new Error("Expected a rejected write");
      }),
  );
});

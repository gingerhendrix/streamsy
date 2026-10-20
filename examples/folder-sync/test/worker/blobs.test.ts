/* oxlint-disable effecttsgo/async-function -- Local workerd/R2 integration test boundary. */
import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { Effect, FileSystem, Schema } from "effect";
import { Miniflare } from "miniflare";
import { expect } from "vitest";
import { FILE_LIMIT } from "../../server/domain.ts";
import { testDirectory } from "../../scripts/test-directory.ts";

const digest = (bytes: Uint8Array) => new Bun.CryptoHasher("sha256").update(bytes).digest("hex");

/** Isolate the stateless production handler, including synthetic bodies HTTP would reject first. */
const worker = Effect.gen(function* () {
  const directory = yield* testDirectory("worker-r2-");
  const fs = yield* FileSystem.FileSystem;
  const entrypoint = `${directory}/entry.ts`;

  const handlerPath = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(
    new URL("../../worker/blob-api.ts", import.meta.url).pathname,
  );

  yield* fs.writeFileString(
    entrypoint,
    `import { handleBlobRequest } from ${handlerPath};
export default { async fetch(request, env) {
  const url = new URL(request.url);
  if (url.pathname === '/storage-failure') {
    request = new Request('http://localhost/api/blobs/' + '0'.repeat(64), {
      method: 'PUT', headers: {'content-length': '65536'},
      body: new ReadableStream({pull(controller) {controller.enqueue(new Uint8Array(65536));}})
    });
    env = {...env, BLOBS: {head: async () => null, put: async () => {throw new Error('injected storage failure');}}};
  }
  if (url.pathname === '/declared') {
    request = new Request('http://localhost/api/blobs/' + '0'.repeat(64), {
      method: 'PUT', headers: {'content-length': '${FILE_LIMIT + 1}'}, body: 'x'
    });
  }
  if (url.pathname === '/oversized') {
    request = new Request('http://localhost/api/blobs/' + '0'.repeat(64), {
      method: 'PUT', headers: {'content-length': '1'},
      body: new ReadableStream({start(controller) {
        controller.enqueue(new Uint8Array(${FILE_LIMIT + 1})); controller.close();
      }})
    });
  }
  if (url.pathname === '/duplicate') {
    request = new Request('http://localhost/api/blobs/' + url.searchParams.get('hash'), {
      method: 'PUT', headers: {'content-length': '1'},
      body: new ReadableStream({pull() {throw new Error('Duplicate upload consumed its body');}})
    });
  }
  return handleBlobRequest(request, env);
}};`,
  );

  const built = yield* Effect.promise(() =>
    Bun.build({ entrypoints: [entrypoint], target: "browser", format: "esm" }),
  );

  if (!built.success) return yield* Effect.die(new AggregateError(built.logs));
  const output = built.outputs[0];

  if (output === undefined) return yield* Effect.die("Missing Worker bundle");
  const script = yield* Effect.promise(() => output.text());

  return yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new Miniflare({
          modules: true,
          compatibilityDate: "2026-08-06",
          script,
          cf: false,
          r2Buckets: ["BLOBS"],
          r2Persist: `${directory}/r2`,
        }),
    ),
    (mf) => Effect.promise(() => mf.dispose()),
  );
});

layer(BunServices.layer, { excludeTestServices: true })((it) => {
  it.effect("local R2 verifies streamed SHA-256 and stores no mismatched upload", () =>
    Effect.gen(function* () {
      const mf = yield* worker;
      const bytes = new Uint8Array([0, 1, 255]);
      const hash = digest(bytes);
      const wrong = "0".repeat(64);

      const put = (key: string) =>
        Effect.promise(() =>
          mf.dispatchFetch(`http://localhost/api/blobs/${key}`, {
            method: "PUT",
            headers: { "content-length": String(bytes.length) },
            body: new ReadableStream({
              start(controller) {
                controller.enqueue(bytes);
                controller.close();
              },
            }),
          }),
        );

      const rejected = yield* put(wrong);
      expect(rejected.status).toBe(400);
      expect(yield* Effect.promise(() => rejected.text())).toContain("hash mismatch");

      const missing = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/api/blobs/${wrong}`, { method: "HEAD" }),
      );

      expect(missing.status).toBe(404);
      expect((yield* put(hash)).status).toBe(200);

      const head = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/api/blobs/${hash}`, { method: "HEAD" }),
      );

      expect(head.status).toBe(200);
      expect(head.headers.get("content-length")).toBe("3");

      const get = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/api/blobs/${hash}`),
      );

      expect(get.headers.get("cache-control")).toContain("immutable");
      expect(new Uint8Array(yield* Effect.promise(() => get.arrayBuffer()))).toEqual(bytes);
      const before = yield* Effect.promise(() => mf.getR2Bucket("BLOBS"));
      const original = yield* Effect.promise(() => before.head(hash));

      const duplicate = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/duplicate?hash=${hash}`),
      );

      expect(duplicate.status).toBe(200);
      expect((yield* Effect.promise(() => before.head(hash)))?.uploaded).toEqual(
        original?.uploaded,
      );
    }),
  );

  it.effect("local R2 accepts empty blobs and bounds declared and streamed sizes", () =>
    Effect.gen(function* () {
      const mf = yield* worker;
      const empty = digest(new Uint8Array());

      const put = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/api/blobs/${empty}`, {
          method: "PUT",
          headers: { "content-length": "0" },
        }),
      );

      expect(put.status).toBe(200);

      const head = yield* Effect.promise(() =>
        mf.dispatchFetch(`http://localhost/api/blobs/${empty}`, { method: "HEAD" }),
      );

      expect(head.headers.get("content-length")).toBe("0");
      expect(head.headers.get("cache-control")).toContain("immutable");
      const failed = yield* Effect.promise(() =>
        mf.dispatchFetch("http://localhost/storage-failure"),
      );
      expect(failed.status).toBe(500);

      const oversized = yield* Effect.promise(() => mf.dispatchFetch("http://localhost/declared"));

      expect(oversized.status).toBe(413);
      const streamed = yield* Effect.promise(() => mf.dispatchFetch("http://localhost/oversized"));
      expect(streamed.status).toBe(413);
      const bucket = yield* Effect.promise(() => mf.getR2Bucket("BLOBS"));
      expect(yield* Effect.promise(() => bucket.head("0".repeat(64)))).toBeNull();
    }),
  );
});

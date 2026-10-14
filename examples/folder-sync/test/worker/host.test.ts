import { BunServices } from "@effect/platform-bun";
import { expect, layer } from "@streamsy/effect-test";
import { Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";
import { ManifestFile, Change, Folder } from "../../server/streams.ts";
import { SearchResult } from "../../server/search-result.ts";
import { CreateFolder, Version } from "../../server/domain.ts";

const token = "worker-test-token";

const headers = { authorization: `Bearer ${token}` };

class Waiting extends Schema.TaggedError<Waiting>()("WorkerTestWaiting", {}) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 100 })),
    }),
  );

const worker = Effect.gen(function* () {
  const directory = yield* testDirectory("worker-host-");

  return yield* Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, token, 0, true)),
    (runtime) => Effect.promise(runtime.stop),
  );
});

layer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect(
      "workerd deduplicates an outbox replay after intake persisted but its response failed",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "lost-response", title: "Replay" }),
          });

          yield* create.text;

          const configured = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false, afterPersist: true }),
          });

          yield* configured.text;

          const put = yield* HttpClient.put(`${host.url}/api/folders/lost-response/files/a.md`, {
            headers: {
              ...headers,
              "x-file-op-id": "lost",
              "x-file-author": "test",
              "x-file-origin": "test",
            },
            body: HttpBody.text("dedupneedle"),
          });

          expect(put.status).toBe(200);
          yield* put.text;

          const pending = yield* HttpClient.get(
            `${host.url}/api/admin/pending?folder=lost-response`,
            { headers },
          );

          expect(yield* pending.json).toEqual([expect.objectContaining({ state: "pending" })]);
          const feed = yield* HttpClient.get(`${host.url}/streams/changes?offset=-1`);
          const before = yield* feed.json;
          expect(before).toHaveLength(1);

          const restored = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false }),
          });

          yield* restored.text;
          // The folder alarm retries the notice. Nothing here calls the scheduled handler.
          yield* until(
            Effect.gen(function* () {
              const response = yield* HttpClient.get(
                `${host.url}/api/admin/pending?folder=lost-response`,
                { headers },
              );

              return yield* Schema.decodeUnknownEffect(
                Schema.Array(Schema.Struct({ state: Schema.String })),
              )(yield* response.json);
            }),
            (entries) => entries.every((row) => row.state !== "pending"),
          );
          const replayed = yield* HttpClient.get(`${host.url}/streams/changes?offset=-1`);
          expect(yield* replayed.json).toEqual(before);
          expect(replayed.headers["stream-up-to-date"]).toBe("true");
          expect(replayed.headers["stream-next-offset"]).toBe(feed.headers["stream-next-offset"]);
          const hits = yield* HttpClient.get(`${host.url}/api/search?q=dedupneedle`);
          expect(yield* hits.json).toHaveLength(1);
        }),
      { timeout: 15000 },
    );
    it.effect(
      "workerd persists notices and wakes folders while projection reads fail",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "fault", title: "Fault" }),
          });

          expect(create.status).toBe(200);
          yield* create.text;

          const configured = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false, projection: true }),
          });

          yield* configured.text;

          const put = yield* HttpClient.put(`${host.url}/api/folders/fault/files/a.md`, {
            headers: {
              ...headers,
              "x-file-op-id": "fault",
              "x-file-author": "test",
              "x-file-origin": "test",
            },
            body: HttpBody.text("projectionneedle"),
          });

          expect(put.status).toBe(200);
          yield* put.text;

          // The folder alarm delivers the notice. Intake persists it while the projection fails.
          yield* until(
            Effect.gen(function* () {
              const pending = yield* HttpClient.get(`${host.url}/api/admin/pending?folder=fault`, {
                headers,
              });

              return yield* Schema.decodeUnknownEffect(
                Schema.Array(Schema.Struct({ state: Schema.String })),
              )(yield* pending.json);
            }),
            (entries) => entries.every((row) => row.state !== "pending"),
          );
          const wake = yield* HttpClient.post(`${host.url}/api/admin/wake`, { headers });
          expect(wake.status).toBe(200);
          yield* wake.text;
          const stalled = yield* HttpClient.get(`${host.url}/api/ls`);
          expect(stalled.status).toBe(503);
          expect(stalled.headers["retry-after"]).toBe("1");
          yield* stalled.text;

          const restored = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false }),
          });

          yield* restored.text;
          const rows = yield* HttpClient.get(`${host.url}/api/ls`);
          expect(yield* rows.json).toHaveLength(1);
          const hits = yield* HttpClient.get(`${host.url}/api/search?q=projectionneedle`);
          expect(yield* hits.json).toHaveLength(1);

          for (let attempt = 0; attempt < 2; attempt++) {
            const feed = yield* HttpClient.get(`${host.url}/streams/changes?offset=-1`);
            expect(yield* feed.json).toHaveLength(1);
            expect(feed.headers["stream-up-to-date"]).toBe("true");
          }
        }),
      { timeout: 15000 },
    );
    it.effect(
      "workerd validates folder limits and content type before registry changes",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          for (const folderId of ["a".repeat(101), "雪".repeat(84)]) {
            const response = yield* HttpClient.post(`${host.url}/api/folders`, {
              headers,
              body: yield* HttpBody.json({ folderId, title: "Rejected" }),
            });

            expect(response.status).toBe(400);
            expect(yield* response.text).toContain(
              "100 decoded UTF-16 code units and 249 UTF-8 bytes",
            );
          }

          const unsupported = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: HttpBody.text(
              yield* Schema.encodeEffect(Schema.fromJsonString(CreateFolder))({
                folderId: "ghost",
                title: "Ghost",
              }),
            ),
          });

          expect(unsupported.status).toBe(415);
          yield* unsupported.text;

          const malformed = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers: { ...headers, "content-type": "application/json" },
            body: HttpBody.text("{", "application/json"),
          });

          expect(malformed.status).toBe(400);
          yield* malformed.text;

          const invalidTitle = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "invalid-title", title: "" }),
          });

          expect(invalidTitle.status).toBe(400);
          yield* invalidTitle.text;
          const namespace = yield* Effect.promise(() => host.mf.getDurableObjectNamespace("INDEX"));

          const registry = yield* Effect.promise(() =>
            namespace.get(namespace.idFromName("index")).fetch("https://internal/__registry"),
          );

          expect(
            yield* Schema.decodeUnknownEffect(Schema.Array(Folder))(
              yield* Effect.promise(() => registry.json()),
            ),
          ).toEqual([]);

          for (const folderId of [
            "a".repeat(100),
            "/".repeat(100),
            "%".repeat(100),
            "雪".repeat(83),
            "😀".repeat(50),
          ]) {
            const created = yield* HttpClient.post(`${host.url}/api/folders`, {
              headers,
              body: yield* HttpBody.json({ folderId, title: "Boundary" }),
            });

            expect(created.status).toBe(200);
            yield* created.text;
            const url = `${host.url}/api/folders/${encodeURIComponent(folderId)}/files/a`;

            const put = yield* HttpClient.put(url, {
              headers: {
                ...headers,
                "x-file-op-id": "boundary",
                "x-file-author": "test",
                "x-file-origin": "test",
              },
              body: HttpBody.text(folderId),
            });

            expect(put.status).toBe(200);
            yield* put.text;
            const read = yield* HttpClient.get(url);
            expect(read.status).toBe(200);
            expect(yield* read.text).toBe(folderId);

            const history = yield* HttpClient.get(
              `${host.url}/api/folders/${encodeURIComponent(folderId)}/history/a`,
            );

            expect(history.status).toBe(200);
            yield* history.text;

            const deletion = yield* HttpClient.del(url, {
              headers: {
                ...headers,
                "x-file-op-id": "delete",
                "x-file-author": "test",
                "x-file-origin": "test",
              },
            });

            expect(deletion.status).toBe(200);
            yield* deletion.text;
          }
        }),
      { timeout: 15000 },
    );
    it.effect(
      "workerd recovers durable notices through the folder alarm exactly once",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;
          const folderId = "2026-10-10/most-of-the-durable-filesystem-sync-search";

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId, title: "Long opaque id" }),
          });

          expect(create.status).toBe(200);
          yield* create.text;

          const blocked = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: true }),
          });

          expect(blocked.status).toBe(200);
          yield* blocked.text;
          const file = `${host.url}/api/folders/${encodeURIComponent(folderId)}/files/a.md`;

          const receipt = yield* HttpClient.put(file, {
            headers: {
              ...headers,
              "x-file-op-id": "recovery",
              "x-file-author": "test",
              "x-file-origin": "test",
            },
            body: HttpBody.text("recoverneedle"),
          });

          expect(receipt.status).toBe(200);

          const written = yield* Schema.decodeUnknownEffect(Schema.Struct({ version: Version }))(
            yield* receipt.json,
          );

          const pending = yield* HttpClient.get(
            `${host.url}/api/admin/pending?folder=${encodeURIComponent(folderId)}`,
            { headers },
          );

          const entries = yield* Schema.decodeUnknownEffect(
            Schema.Array(Schema.Struct({ state: Schema.String, idempotencyKey: Schema.String })),
          )(yield* pending.json);

          expect(entries).toEqual([
            { state: "pending", idempotencyKey: `${folderId}/${written.version}` },
          ]);
          const before = yield* HttpClient.get(`${host.url}/api/ls`);
          expect(yield* before.json).toEqual([]);

          const restored = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false }),
          });

          yield* restored.text;

          // The folder alarm fires at the outbox's durable backoff instant. No folder request or cron drives recovery.
          const rows = yield* until(
            Effect.gen(function* () {
              const response = yield* HttpClient.get(`${host.url}/api/ls`);

              return yield* Schema.decodeUnknownEffect(Schema.Array(ManifestFile))(
                yield* response.json,
              );
            }),
            (found) => found.length === 1,
          );

          expect(rows[0]?.version).toBe(written.version);
          // The hourly backstop wakes the folder again and must not deliver the notice twice.
          const runtime = yield* Effect.promise(() => host.mf.getWorker());
          yield* Effect.promise(() => runtime.scheduled({ cron: "0 * * * *" }));
          const changes = yield* HttpClient.get(`${host.url}/streams/changes?offset=-1`);
          const feed = yield* Schema.decodeUnknownEffect(Schema.Array(Change))(yield* changes.json);
          expect(feed).toHaveLength(1);
          expect(feed[0]?.version).toBe(written.version);
          expect(changes.headers["stream-up-to-date"]).toBe("true");
          const search = yield* HttpClient.get(`${host.url}/api/search?q=recoverneedle`);
          expect(
            (yield* Schema.decodeUnknownEffect(Schema.Array(SearchResult))(yield* search.json))[0]
              ?.version,
          ).toBe(written.version);
          const ls = yield* HttpClient.get(`${host.url}/api/ls`);
          yield* ls.text;
          expect(ls.headers["stream-next-offset"]).toBe(changes.headers["stream-next-offset"]);
          expect(ls.headers["stream-epoch"]).toBe(changes.headers["stream-epoch"]);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd advances the folder head checkpoint from the folder alarm",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "head", title: "Head" }),
          });

          expect(create.status).toBe(200);
          yield* create.text;

          const head = HttpClient.get(`${host.url}/api/admin/head?folder=head`, { headers }).pipe(
            Effect.flatMap((response) => response.json),
            Effect.flatMap(
              Schema.decodeUnknownEffect(Schema.Struct({ seq: Schema.Int, offset: Schema.String })),
            ),
          );

          const empty = yield* head;
          expect(empty.seq).toBe(0);

          for (const [index, path] of ["a.md", "b.md", "c.md"].entries()) {
            const put = yield* HttpClient.put(`${host.url}/api/folders/head/files/${path}`, {
              headers: {
                ...headers,
                "x-file-op-id": `head-${index}`,
                "x-file-author": "test",
                "x-file-origin": "test",
              },
              body: HttpBody.text(`head ${path}`),
            });

            expect(put.status).toBe(200);
            yield* put.text;
          }

          // No read or cron runs the projection. Only the head wakeup writes this checkpoint.
          const advanced = yield* until(head, (stored) => stored.seq === 3);
          expect(advanced.offset > empty.offset).toBe(true);

          const read = yield* HttpClient.get(`${host.url}/api/folders/head/files/b.md`);
          expect(yield* read.text).toBe("head b.md");
          expect(read.headers["x-file-version"]).toBe("2");
          expect(yield* head).toEqual(advanced);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd enforces write auth and supports SQLite ranking, safe snippets, bad-query errors and portable globs",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          const denied = yield* HttpClient.post(`${host.url}/api/folders`, {
            body: yield* HttpBody.json({ folderId: "docs", title: "Docs" }),
          });

          expect(denied.status).toBe(401);
          yield* denied.text;

          for (const authorization of [
            "Bearer worker-test-tokem",
            "Bearer short",
            "Bearer worker-test-token-extra",
            "Bearer",
            "Bearer WORKER-TEST-TOKEN",
          ]) {
            const wrong = yield* HttpClient.post(`${host.url}/api/folders`, {
              headers: { authorization },
              body: yield* HttpBody.json({ folderId: "denied", title: "Denied" }),
            });

            expect(wrong.status).toBe(401);
            expect(yield* wrong.text).toContain("Writer authorization required");
          }

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "docs", title: "Docs" }),
          });

          expect(create.status).toBe(200);
          yield* create.text;

          for (const [path, text] of [
            ["a.md", "needle " + "filler ".repeat(100)],
            ["z.md", "<img src=x> needle needle needle"],
          ] as const) {
            const response = yield* HttpClient.put(`${host.url}/api/folders/docs/files/${path}`, {
              headers: {
                ...headers,
                "x-file-op-id": path,
                "x-file-author": "test",
                "x-file-origin": "test",
              },
              body: HttpBody.text(text),
            });

            expect(response.status).toBe(200);
            yield* response.text;
          }

          const response = yield* HttpClient.get(`${host.url}/api/search?q=needle`);

          const hits = yield* Schema.decodeUnknownEffect(Schema.Array(SearchResult))(
            yield* response.json,
          );

          expect(hits.map((hit) => hit.path)).toEqual(["z.md", "a.md"]);
          expect(hits[0]?.snippet.map((part) => part.text).join("")).toContain("<img");
          expect(hits[0]?.snippet.some((part) => part.match)).toBe(true);

          for (const query of ['"unterminated', "foo:bar", "AND", "NEAR(", "a OR"]) {
            const invalid = yield* HttpClient.get(
              `${host.url}/api/search?q=${encodeURIComponent(query)}`,
            );

            expect(invalid.status).toBe(400);
            yield* invalid.text;
          }

          const listed = yield* HttpClient.get(`${host.url}/api/ls?glob=**%2Fz.*&folder=docs`);

          const rows = yield* Schema.decodeUnknownEffect(Schema.Array(ManifestFile))(
            yield* listed.json,
          );

          expect(rows.map((row) => row.path)).toEqual(["z.md"]);
          const html = yield* HttpClient.get(host.url);
          expect(html.status).toBe(200);
          expect(yield* html.text).toContain("/assets/");

          const broken = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false, projection: true }),
          });

          yield* broken.text;
          const stalled = yield* HttpClient.get(`${host.url}/api/ls`);
          expect(stalled.status).toBe(503);
          expect(stalled.headers["retry-after"]).toBe("1");
          yield* stalled.text;

          const repair = yield* HttpClient.post(`${host.url}/api/admin/test-intake`, {
            headers,
            body: yield* HttpBody.json({ fail: false }),
          });

          yield* repair.text;
          const healthy = yield* HttpClient.get(`${host.url}/api/ls`);
          expect(healthy.status).toBe(200);
          yield* healthy.text;
        }),
      { timeout: 15000 },
    );
    it.effect(
      "workerd restarts with folder receipts, binary R2 bytes, the registry and index epoch intact",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-restart-");

          const host = yield* Effect.acquireRelease(
            Effect.promise(() => startWorker(directory, token)),
            (runtime) => Effect.promise(runtime.stop),
          );

          const create = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: yield* HttpBody.json({ folderId: "daily/demo", title: "Docs" }),
          });

          yield* create.text;
          const file = "/api/folders/daily%2Fdemo/files/binary";
          const bytes = new Uint8Array([0, 255, 1]);

          const metadata = {
            ...headers,
            "x-file-op-id": "binary",
            "x-file-author": "test",
            "x-file-origin": "test",
          };

          const put = yield* HttpClient.put(`${host.url}${file}`, {
            headers: metadata,
            body: HttpBody.uint8Array(bytes),
          });

          const receipt = yield* Schema.decodeUnknownEffect(Schema.Struct({ version: Version }))(
            yield* put.json,
          );

          const before = yield* HttpClient.get(`${host.url}/api/ls`);
          yield* before.text;
          const epoch = before.headers["stream-epoch"];
          yield* Effect.promise(host.stop);

          const restarted = yield* Effect.acquireRelease(
            Effect.promise(() => startWorker(directory, token)),
            (runtime) => Effect.promise(runtime.stop),
          );

          const head = yield* HttpClient.get(`${restarted.url}${file}`);
          expect(new Uint8Array(yield* head.arrayBuffer)).toEqual(bytes);
          expect(head.headers["x-file-version"]).toBe(String(receipt.version));

          const replay = yield* HttpClient.put(`${restarted.url}${file}`, {
            headers: metadata,
            body: HttpBody.uint8Array(bytes),
          });

          expect(yield* replay.json).toEqual(receipt);
          const listed = yield* HttpClient.get(`${restarted.url}/api/ls`);
          expect(listed.headers["stream-epoch"]).toBe(epoch);
          expect(yield* listed.json).toHaveLength(1);
          const wake = yield* HttpClient.post(`${restarted.url}/api/admin/wake`, { headers });
          expect(wake.status).toBe(200);
          yield* wake.text;
          const changes = yield* HttpClient.get(`${restarted.url}/streams/changes?offset=-1`);
          expect(yield* changes.json).toHaveLength(1);
          expect(changes.headers["stream-up-to-date"]).toBe("true");
        }),
      { timeout: 15000 },
    );
  },
);

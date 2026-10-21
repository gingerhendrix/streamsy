import { Database } from "bun:sqlite";
import { StreamAddress } from "@streamsy/core";
import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientResponse } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";
import { FolderKey } from "../../server/streams.ts";
import { ListedFile, SearchResult } from "../../server/index-rows.ts";
import { CreateFolder, File, Head } from "../../server/domain.ts";
import { CHECKPOINT_EVERY } from "../../server/folder-store.ts";

const token = "worker-test-token";

const headers = { authorization: `Bearer ${token}` };

const digest = (text: string) => new Bun.CryptoHasher("sha256").update(text).digest("hex");

const content = (text: string, txid: string) => ({
  txid,
  author: "test",
  hash: digest(text),
  size: new TextEncoder().encode(text).length,
});

const fileUrl = (url: string, folderId: string, path = "a.md") =>
  `${url}/api/folders/${encodeURIComponent(folderId)}/files/${path}`;

const streamUrl = (url: string, folderId: string) =>
  `${url}/streams/folders/${Schema.encodeSync(FolderKey)(folderId)}/files`;

const jsonPost = (url: string, body: typeof CreateFolder.Type | { dropPings: boolean }) =>
  HttpClient.post(url, { headers, body: HttpBody.jsonUnsafe(body) });

const create = (url: string, folderId: string, title = folderId) =>
  jsonPost(`${url}/api/folders`, { txid: `create:${folderId}`, folderId, title });

/** Every commit names a blob, so put the text in the blob store first. */
const uploadText = Effect.fn("WorkerTest.upload")(function* (url: string, text: string) {
  const response = yield* HttpClient.put(`${url}/api/blobs/${digest(text)}`, {
    headers,
    body: HttpBody.text(text, "application/octet-stream"),
  });

  expect(response.status).toBe(200);
  yield* response.text;
});

const commit = (url: string, folderId: string, text: string, txid: string, version?: number) =>
  uploadText(url, text).pipe(
    Effect.andThen(
      HttpClient.put(fileUrl(url, folderId), {
        headers: version === undefined ? headers : { ...headers, "if-match": `"${version}"` },
        body: HttpBody.jsonUnsafe(content(text, txid)),
      }),
    ),
  );

const remove = (url: string, folderId: string, version: number) =>
  HttpClient.del(fileUrl(url, folderId), {
    headers: { ...headers, "if-match": `"${version}"` },
    body: HttpBody.jsonUnsafe({ txid: `delete:${version}`, author: "test" }),
  });

const head = (response: HttpClientResponse.HttpClientResponse) =>
  response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Head)));

class Waiting extends Schema.TaggedError<Waiting>()("WorkerTestWaiting", {}) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 200 })),
    }),
  );

const launch = (directory: string) =>
  Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, token, 0, true)),
    (runtime) => Effect.promise(runtime.stop),
  );

const worker = testDirectory("worker-host-").pipe(Effect.flatMap(launch));

/** About 60 KB. Quotes, backslashes and newlines grow each time the text is JSON encoded. */
const largeText = (index: number) =>
  `line "${index}" with "quotes" and \\ a backslash\n`.repeat(1500).slice(0, 60_000);

/** The stored folder view record: JSON state inside a JSON envelope. */
const SavedView = Schema.fromJsonString(
  Schema.Struct({
    record: Schema.Struct({
      state: Schema.fromJsonString(
        Schema.Struct({ last: Schema.Finite, files: Schema.Array(Schema.Unknown) }),
      ),
    }),
  }),
);

/** Read the folder view checkpoints straight from the Durable Object SQLite files. */
const savedViews = (directory: string, folderKey: string) =>
  Array.from(new Bun.Glob("objects/folder-sync-FolderObject/*.sqlite").scanSync(directory))
    .filter((path) => !path.endsWith("/metadata.sqlite"))
    .flatMap((path) => {
      const db = new Database(`${directory}/${path}`, { readonly: true });

      try {
        return db
          .query<{ owner_value: string }, [string]>(
            "SELECT owner_value FROM streamsy_owner_records WHERE owner_key LIKE ?",
          )
          .all(`%folder-sync-view%${folderKey}%`)
          .map((row) => row.owner_value);
      } finally {
        db.close();
      }
    });

const listed = (url: string) =>
  HttpClient.get(`${url}/api/ls`).pipe(
    Effect.flatMap((response) => response.json),
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(ListedFile))),
  );

const changes = (url: string, folderId: string) =>
  HttpClient.get(`${streamUrl(url, folderId)}?offset=-1`).pipe(
    Effect.tap((response) => Effect.sync(() => expect(response.status, folderId).toBe(200))),
    Effect.flatMap((response) => response.json),
    Effect.flatMap(
      Schema.decodeUnknownEffect(
        Schema.Array(StreamAddress.stateChange({ type: "file", schema: File })),
      ),
    ),
  );

layer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect(
      "workerd serves State streams read-only and retries the current hash without appending",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;
          expect((yield* create(host.url, "docs")).status).toBe(200);
          const written = yield* head(yield* commit(host.url, "docs", "one", "first"));
          expect(written).toEqual({ version: 1, hash: digest("one"), txid: "first" });
          const before = yield* changes(host.url, "docs");
          expect(before).toHaveLength(1);
          expect(before[0]).toMatchObject({
            type: "file",
            key: "a.md",
            value: { version: 1, hash: digest("one") },
            headers: { operation: "upsert", txid: "first" },
          });
          expect(yield* head(yield* commit(host.url, "docs", "one", "retry", 99))).toEqual(written);
          expect(yield* changes(host.url, "docs")).toEqual(before);
          const read = yield* HttpClient.get(fileUrl(host.url, "docs"));
          expect(read.headers.etag).toBe('"1"');
          expect(yield* read.text).toBe("one");

          for (const url of [`${host.url}/streams/folders`, streamUrl(host.url, "docs")]) {
            for (const request of [HttpClient.post, HttpClient.put, HttpClient.del]) {
              const response = yield* request(url, { body: HttpBody.text("invalid") });
              expect(response.status).toBe(405);
              yield* response.text;
            }
          }

          expect((yield* commit(host.url, "docs", "two", "stale", 99)).status).toBe(412);
          expect((yield* remove(host.url, "docs", 1)).status).toBe(200);
          const deleted = yield* changes(host.url, "docs");
          expect(deleted.at(-1)).toMatchObject({
            type: "file",
            key: "a.md",
            headers: { operation: "delete" },
          });
          expect(deleted.at(-1)).not.toHaveProperty("value");
          const absent = yield* remove(host.url, "docs", 1);
          expect(yield* absent.json).toEqual({});
          expect(yield* changes(host.url, "docs")).toEqual(deleted);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd derives heads from a checkpoint and the stream tail",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;
          yield* create(host.url, "derived");
          // Past one checkpoint interval, so reads fold a saved checkpoint plus a tail.
          const writes = CHECKPOINT_EVERY + 3;

          for (let version = 1; version <= writes; version++) {
            const written = yield* head(
              yield* commit(
                host.url,
                "derived",
                `text ${version}`,
                `t${version}`,
                version === 1 ? undefined : version - 1,
              ),
            );

            expect(written).toMatchObject({ version, txid: `t${version}` });
          }

          const read = yield* HttpClient.get(fileUrl(host.url, "derived"));
          expect(read.headers.etag).toBe(`"${writes}"`);
          expect(yield* read.text).toBe(`text ${writes}`);
          const stale = yield* commit(host.url, "derived", "stale", "stale", writes - 1);
          expect(stale.status).toBe(412);
          expect(yield* head(stale)).toMatchObject({ version: writes, txid: `t${writes}` });
          expect(yield* changes(host.url, "derived")).toHaveLength(writes);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd saves the view checkpoint of a folder with more than 2 MB of content",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-large-");
          const host = yield* launch(directory);
          yield* create(host.url, "large");
          const writes = 40;

          for (let index = 1; index <= writes; index++) {
            yield* uploadText(host.url, largeText(index));

            const written = yield* HttpClient.put(fileUrl(host.url, "large", `f${index}.md`), {
              headers: { ...headers, "if-none-match": "*" },
              body: HttpBody.jsonUnsafe(content(largeText(index), `t${index}`)),
            });

            expect(written.status, `write ${index}`).toBe(200);
          }

          const rows = savedViews(directory, yield* Schema.encodeEffect(FolderKey)("large"));
          expect(rows).toHaveLength(1);
          const row = rows[0] ?? "";
          const saved = yield* Schema.decodeEffect(SavedView)(row);

          // The first write saves, and write CHECKPOINT_EVERY + 1 saves again.
          expect(saved.record.state.last).toBe(CHECKPOINT_EVERY + 1);
          expect(saved.record.state.files).toHaveLength(CHECKPOINT_EVERY + 1);
          // Metadata only: far below the 2 MB row limit of a SQLite-backed Durable Object.
          expect(row.length).toBeLessThan(64 * 1024);

          const first = yield* HttpClient.get(fileUrl(host.url, "large", "f1.md"));
          expect(first.headers.etag).toBe(`"1"`);
          expect(yield* first.text).toBe(largeText(1));
          const last = yield* HttpClient.get(fileUrl(host.url, "large", `f${writes}.md`));
          expect(yield* last.text).toBe(largeText(writes));
        }),
      { timeout: 30000 },
    );

    it.effect(
      "the hourly cron catches up source cursors after lost pings, including deletes",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;
          const folderId = "daily/opaque-雪";
          yield* create(host.url, folderId);
          const disabled = yield* jsonPost(`${host.url}/api/admin/test-index`, { dropPings: true });
          expect(disabled.status).toBe(200);
          yield* disabled.text;
          yield* head(yield* commit(host.url, folderId, "cronneedle", "lost-ping"));
          expect(yield* listed(host.url)).toEqual([]);
          const before = yield* HttpClient.get(`${host.url}/api/search?q=cronneedle`);
          expect(yield* before.json).toEqual([]);
          const runtime = yield* Effect.promise(() => host.mf.getWorker());
          yield* Effect.promise(() => runtime.scheduled({ cron: "0 * * * *" }));
          expect(yield* listed(host.url)).toMatchObject([{ folderId, path: "a.md", version: 1 }]);
          const hits = yield* HttpClient.get(`${host.url}/api/search?q=cronneedle`);
          expect(yield* hits.json).toHaveLength(1);
          yield* remove(host.url, folderId, 1);
          expect(yield* listed(host.url)).toHaveLength(1);
          yield* Effect.promise(() => runtime.scheduled({ cron: "0 * * * *" }));
          expect(yield* listed(host.url)).toEqual([]);
          const removed = yield* HttpClient.get(`${host.url}/api/search?q=cronneedle`);
          expect(yield* removed.json).toEqual([]);
          yield* Effect.promise(() => runtime.scheduled({ cron: "0 * * * *" }));
          expect(yield* changes(host.url, folderId)).toHaveLength(2);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd validates folder boundaries and preserves slash ids in the registry",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          for (const folderId of ["a".repeat(101), "雪".repeat(84)])
            expect((yield* create(host.url, folderId)).status).toBe(400);
          expect(
            (yield* jsonPost(`${host.url}/api/folders`, {
              txid: "invalid",
              folderId: "ghost",
              title: "",
            })).status,
          ).toBe(400);

          const malformed = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: HttpBody.text("{", "application/json"),
          });

          expect(malformed.status).toBe(400);
          yield* malformed.text;

          const unsupported = yield* HttpClient.post(`${host.url}/api/folders`, {
            headers,
            body: HttpBody.text(
              yield* Schema.encodeEffect(Schema.fromJsonString(CreateFolder))({
                txid: "text",
                folderId: "ghost",
                title: "Ghost",
              }),
            ),
          });

          expect(unsupported.status).toBe(415);
          yield* unsupported.text;
          const empty = yield* HttpClient.get(`${host.url}/streams/folders?offset=-1`);
          expect(yield* empty.json).toEqual([]);

          for (const folderId of [
            "a".repeat(100),
            "/".repeat(100),
            "%".repeat(100),
            "雪".repeat(83),
            "😀".repeat(50),
          ]) {
            expect((yield* create(host.url, folderId, "Boundary")).status).toBe(200);
            expect((yield* create(host.url, folderId, "Boundary")).status).toBe(200);
            expect((yield* create(host.url, folderId, "Different")).status).toBe(409);
            expect((yield* commit(host.url, folderId, folderId, "boundary")).status).toBe(200);
            const read = yield* HttpClient.get(fileUrl(host.url, folderId));
            expect(yield* read.text).toBe(folderId);
            expect(yield* changes(host.url, folderId)).toHaveLength(1);
          }

          const registry = yield* HttpClient.get(`${host.url}/streams/folders?offset=-1`);
          expect(yield* registry.json).toHaveLength(5);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "workerd enforces auth and indexes pinged writes with ranking, safe snippets and portable globs",
      () =>
        Effect.gen(function* () {
          const host = yield* worker;

          for (const authorization of [
            undefined,
            "Bearer wrong",
            "Bearer short",
            `Bearer ${token}-extra`,
            "Bearer",
            `Bearer ${token.toUpperCase()}`,
          ]) {
            const denied = yield* HttpClient.post(`${host.url}/api/folders`, {
              headers: authorization === undefined ? {} : { authorization },
              body: HttpBody.jsonUnsafe({ txid: "denied", folderId: "denied", title: "Denied" }),
            });

            expect(denied.status).toBe(401);
            yield* denied.text;
          }

          yield* create(host.url, "docs");

          for (const [path, text] of [
            ["a.md", "needle " + "filler ".repeat(100)],
            ["z.md", "<img src=x> needle needle needle"],
          ] as const) {
            yield* uploadText(host.url, text);
            expect(
              (yield* HttpClient.put(fileUrl(host.url, "docs", path), {
                headers,
                body: HttpBody.jsonUnsafe(content(text, path)),
              })).status,
            ).toBe(200);
          }

          yield* until(listed(host.url), (rows) => rows.length === 2);
          const response = yield* HttpClient.get(`${host.url}/api/search?q=needle`);

          const hits = yield* Schema.decodeUnknownEffect(Schema.Array(SearchResult))(
            yield* response.json,
          );

          expect(hits.map((hit) => hit.path)).toEqual(["z.md", "a.md"]);
          expect(hits[0]?.snippet.map((part) => part.text).join("")).toContain("<img");
          expect(hits[0]?.snippet.some((part) => part.match)).toBe(true);

          for (const query of ['"unterminated', "foo:bar", "AND", "NEAR(", "a OR"])
            expect(
              (yield* HttpClient.get(`${host.url}/api/search?q=${encodeURIComponent(query)}`))
                .status,
            ).toBe(400);

          for (const glob of ["**/z.*", "[!a]*.md"]) {
            const filtered = yield* HttpClient.get(
              `${host.url}/api/ls?glob=${encodeURIComponent(glob)}&folder=docs`,
            );

            const rows = yield* Schema.decodeUnknownEffect(Schema.Array(ListedFile))(
              yield* filtered.json,
            );

            expect(rows.map((row) => row.path)).toEqual(["z.md"]);
          }

          expect((yield* HttpClient.get(`${host.url}/api/ls?glob=%5B`)).status).toBe(400);
          const html = yield* HttpClient.get(host.url);
          expect(yield* html.text).toContain("/assets/");
          yield* remove(host.url, "docs", 1);
          yield* until(listed(host.url), (rows) => rows.length === 1);
        }),
      { timeout: 15000 },
    );

    it.effect(
      "Worker indexing caps blob text, skips binary and missing blobs, and resumes persisted cursors",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-index-restart-");
          const host = yield* launch(directory);
          yield* create(host.url, "blobs");
          yield* jsonPost(`${host.url}/api/admin/test-index`, { dropPings: true });
          const prefix = "capneedle ";
          const large = prefix + "x".repeat(1024 * 1024 - prefix.length - 1) + "☃ beyondcapneedle";

          const entries = [
            { path: "large", bytes: new TextEncoder().encode(large) },
            { path: "binary", bytes: new TextEncoder().encode("binaryneedle\0") },
            { path: "missing", bytes: new TextEncoder().encode("missingneedle\0") },
          ];

          const hashes: string[] = [];

          for (const { path, bytes } of entries) {
            const hash = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
            hashes.push(hash);

            const uploaded = yield* HttpClient.put(`${host.url}/api/blobs/${hash}`, {
              headers,
              body: HttpBody.uint8Array(bytes),
            });

            expect(uploaded.status).toBe(200);
            yield* uploaded.text;

            const committed = yield* HttpClient.put(fileUrl(host.url, "blobs", path), {
              headers,
              body: HttpBody.jsonUnsafe({ txid: path, author: "test", hash, size: bytes.length }),
            });

            expect(committed.status).toBe(200);
            yield* committed.text;
          }

          const bucket = yield* Effect.promise(() => host.mf.getR2Bucket("BLOBS"));
          const missingHash = hashes[2];
          const largeHash = hashes[0];

          if (missingHash === undefined || largeHash === undefined)
            return yield* Effect.die("Missing test blob hash");
          yield* Effect.promise(() => bucket.delete(missingHash));
          const runtime = yield* Effect.promise(() => host.mf.getWorker());
          yield* Effect.promise(() => runtime.scheduled({ cron: "0 * * * *" }));
          expect((yield* listed(host.url)).map((row) => row.path)).toEqual([
            "binary",
            "large",
            "missing",
          ]);

          for (const [query, count] of [
            ["capneedle", 1],
            ["beyondcapneedle", 0],
            ["binaryneedle", 0],
            ["missingneedle", 0],
          ] as const) {
            const response = yield* HttpClient.get(`${host.url}/api/search?q=${query}`);
            expect(yield* response.json, query).toHaveLength(count);
          }

          // Removing an already indexed blob exposes an accidental replay after restart:
          // replay would replace its searchable row with metadata only.
          yield* Effect.promise(() => bucket.delete(largeHash));
          yield* Effect.promise(host.stop);
          const restarted = yield* launch(directory);
          yield* head(yield* commit(restarted.url, "blobs", "freshneedle", "after-restart"));
          const wake = yield* HttpClient.post(`${restarted.url}/api/admin/wake`, { headers });
          expect(wake.status).toBe(200);
          yield* wake.text;
          expect(yield* listed(restarted.url)).toHaveLength(4);

          for (const query of ["capneedle", "freshneedle"]) {
            const response = yield* HttpClient.get(`${restarted.url}/api/search?q=${query}`);
            expect(yield* response.json, query).toHaveLength(1);
          }

          return yield* Effect.void;
        }),
      { timeout: 20000 },
    );

    it.effect(
      "a binary R2 blob and its file version survive a Worker restart",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-binary-restart-");
          const host = yield* launch(directory);
          yield* create(host.url, "binary");
          const bytes = new Uint8Array([0, 255, 1, 128, 0]);
          const hash = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");

          const upload = yield* HttpClient.put(`${host.url}/api/blobs/${hash}`, {
            headers,
            body: HttpBody.uint8Array(bytes),
          });

          expect(upload.status).toBe(200);
          yield* upload.text;

          const written = yield* head(
            yield* HttpClient.put(fileUrl(host.url, "binary"), {
              headers,
              body: HttpBody.jsonUnsafe({
                txid: "binary",
                author: "test",
                hash,
                size: bytes.length,
              }),
            }),
          );

          expect(written.version).toBe(1);
          yield* Effect.promise(host.stop);
          const restarted = yield* launch(directory);
          const file = yield* HttpClient.get(fileUrl(restarted.url, "binary"));
          expect(file.status).toBe(200);
          expect(file.headers.etag).toBe('"1"');
          expect(new Uint8Array(yield* file.arrayBuffer)).toEqual(bytes);
          const blob = yield* HttpClient.get(`${restarted.url}/api/blobs/${hash}`);
          expect(blob.status).toBe(200);
          expect(new Uint8Array(yield* blob.arrayBuffer)).toEqual(bytes);
        }),
      { timeout: 20000 },
    );

    it.effect(
      "DO restarts preserve the counter after deletion and reject stale pre-recreation versions",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-restart-");
          const host = yield* launch(directory);
          yield* create(host.url, "daily/demo", "Docs");
          expect(
            yield* head(yield* commit(host.url, "daily/demo", "first", "first")),
          ).toMatchObject({ version: 1 });
          yield* remove(host.url, "daily/demo", 1);
          yield* Effect.promise(host.stop);
          const restarted = yield* launch(directory);
          expect((yield* HttpClient.get(fileUrl(restarted.url, "daily/demo"))).status).toBe(404);

          const second = yield* head(
            yield* commit(restarted.url, "daily/demo", "second", "second"),
          );

          expect(second.version).toBe(2);
          expect((yield* commit(restarted.url, "daily/demo", "stale", "stale", 1)).status).toBe(
            412,
          );
          expect((yield* remove(restarted.url, "daily/demo", 1)).status).toBe(412);
          expect(
            yield* head(yield* commit(restarted.url, "daily/demo", "second", "retry", 1)),
          ).toEqual(second);
          yield* Effect.promise(restarted.stop);
          const again = yield* launch(directory);
          expect(
            yield* head(yield* commit(again.url, "daily/demo", "third", "third", 2)),
          ).toMatchObject({ version: 3 });
          const retained = yield* changes(again.url, "daily/demo");
          expect(
            retained.map((change) => ("value" in change ? change.value.version : null)),
          ).toEqual([1, null, 2, 3]);
          const wake = yield* HttpClient.post(`${again.url}/api/admin/wake`, { headers });
          expect(wake.status).toBe(200);
          yield* wake.text;
          expect(yield* listed(again.url)).toMatchObject([{ folderId: "daily/demo", version: 3 }]);
          const registry = yield* HttpClient.get(`${again.url}/streams/folders?offset=-1`);
          expect(yield* registry.json).toHaveLength(1);
        }),
      { timeout: 20000 },
    );
  },
);

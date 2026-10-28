import { Database } from "bun:sqlite";
import { BunServices } from "@effect/platform-bun";
import { layer } from "@effect/vitest";
import { expect } from "vitest";
import { Array, Effect, Layer, Record, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientResponse } from "effect/http";
import { testDirectory } from "../../scripts/test-directory.ts";
import { startWorker } from "../../scripts/worker-harness.ts";
import { Files, FolderKey } from "../../server/streams.ts";

const token = "worker-wake-token";

const headers = { authorization: `Bearer ${token}` };

const digest = (text: string) => new Bun.CryptoHasher("sha256").update(text).digest("hex");

class Waiting extends Schema.TaggedError<Waiting, { readonly brand: unique symbol }>()(
  "WorkerWakeWaiting",
  {},
) {}

const until = <A, E, R>(effect: Effect.Effect<A, E, R>, ready: (value: A) => boolean) =>
  effect.pipe(
    Effect.filterOrFail(ready, () => new Waiting()),
    Effect.retry({
      while: Schema.is(Waiting),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 250 })),
    }),
  );

const launch = (directory: string) =>
  Effect.acquireRelease(
    Effect.promise(() => startWorker(directory, token, 0, true)),
    (runtime) => Effect.promise(runtime.stop),
  );

const create = Effect.fn("WorkerWake.create")(function* (url: string, folderId: string) {
  const response = yield* HttpClient.post(`${url}/api/folders`, {
    headers,
    body: HttpBody.jsonUnsafe({ txid: `create:${folderId}`, folderId, title: folderId }),
  });

  expect(response.status, folderId).toBe(200);
  yield* response.text;
});

/** Upload the text, commit it at `path`, and answer the folder's stream tail. */
const write = Effect.fn("WorkerWake.write")(function* (
  url: string,
  folderId: string,
  path: string,
) {
  const text = `${folderId}/${path}`;

  const uploaded = yield* HttpClient.put(`${url}/api/blobs/${digest(text)}`, {
    headers,
    body: HttpBody.text(text, "application/octet-stream"),
  });

  expect(uploaded.status).toBe(200);
  yield* uploaded.text;

  const committed = yield* HttpClient.put(
    `${url}/api/folders/${encodeURIComponent(folderId)}/files/${path}`,
    {
      headers,
      body: HttpBody.jsonUnsafe({
        txid: text,
        parent: null,
        author: "test",
        hash: digest(text),
        size: new TextEncoder().encode(text).length,
      }),
    },
  );

  expect(committed.status).toBe(200);
  yield* committed.text;

  const key = yield* Schema.encodeEffect(FolderKey)(folderId);
  const tail = yield* HttpClient.head(`${url}/streams/folders/${key}/files`);

  return tail.headers["stream-next-offset"];
});

const WakeEvent = Schema.Struct({
  key: Schema.String,
  value: Schema.optionalKey(Schema.Struct({ offset: Schema.String })),
});

/** The public wake stream, folded to `stream id -> offset`. */
const rows = (url: string) =>
  HttpClient.get(`${url}/streams/wake?offset=-1`).pipe(
    Effect.tap((response) => Effect.sync(() => expect(response.status).toBe(200))),
    Effect.flatMap((response) => response.json),
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(WakeEvent))),
    Effect.map((events) =>
      Array.reduce(events, Record.empty<string, string>(), (folded, event) =>
        event.value === undefined
          ? Record.remove(folded, event.key)
          : Record.set(folded, event.key, event.value.offset),
      ),
    ),
  );

const listed = (url: string) =>
  HttpClient.get(`${url}/api/ls`).pipe(
    Effect.flatMap((response) => response.json),
    Effect.flatMap(
      Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ folderId: Schema.String }))),
    ),
  );

/** The status of one response, after its body is read. */
const status = <E, R>(request: Effect.Effect<HttpClientResponse.HttpClientResponse, E, R>) =>
  request.pipe(
    Effect.tap((response) => response.text),
    Effect.map((response) => response.status),
  );

layer(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer), { excludeTestServices: true })(
  (it) => {
    it.effect(
      "a folder write reaches the wake stream on the index object, which stays read-only",
      () =>
        Effect.gen(function* () {
          const host = yield* testDirectory("worker-wake-").pipe(Effect.flatMap(launch));

          // The index object creates the stream at start: no 404 before the first write.
          expect(yield* rows(host.url)).toEqual({});

          yield* create(host.url, "docs");
          const first = yield* write(host.url, "docs", "a.md");
          const docs = Files.at({ folderId: "docs" }).id;
          yield* until(rows(host.url), (current) => current[docs] === first);

          yield* create(host.url, "notes/2026");
          const notes = yield* write(host.url, "notes/2026", "b.md");
          const second = yield* write(host.url, "docs", "c.md");
          const other = Files.at({ folderId: "notes/2026" }).id;

          const both = yield* until(
            rows(host.url),
            (current) => current[other] === notes && current[docs] === second,
          );

          expect(both).toEqual({ [docs]: second, [other]: notes });

          for (const method of [HttpClient.post, HttpClient.put, HttpClient.del])
            expect(
              yield* status(
                method(`${host.url}/streams/wake`, { headers, body: HttpBody.text("[]") }),
              ),
            ).toBe(405);

          // The internal mount is reachable only through object stubs.
          for (const path of ["/__wake/wake", "/__wake/wake?offset=-1"]) {
            expect(
              yield* status(
                HttpClient.post(`${host.url}${path}`, {
                  headers: { ...headers, "content-type": "application/json" },
                  body: HttpBody.text("[]", "application/json"),
                }),
              ),
            ).toBe(404);
            expect(yield* status(HttpClient.get(`${host.url}${path}`, { headers }))).toBe(404);
          }

          expect(yield* rows(host.url)).toEqual(both);
        }),
      { timeout: 30000 },
    );

    it.effect(
      "a durable wake-search mark indexes a dirty wake row after restart without a new write, and folder deletion clears search",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-dirty-restart-");
          const first = yield* launch(directory);
          yield* create(first.url, "dirty");

          const pause = yield* HttpClient.post(`${first.url}/api/admin/test-index`, {
            headers,
            body: HttpBody.jsonUnsafe({ pauseIndex: true }),
          });

          expect(pause.status).toBe(200);
          yield* pause.text;
          const tail = yield* write(first.url, "dirty", "a.md");
          const id = Files.at({ folderId: "dirty" }).id;
          yield* until(rows(first.url), (current) => current[id] === tail);
          expect(yield* listed(first.url)).toEqual([]);
          yield* Effect.promise(first.stop);

          const host = yield* launch(directory);

          // Unpause before any index request. The paused test alarm returns without
          // re-arming, so an acquire while paused would swallow the mark's alarm.
          const unpause = yield* HttpClient.post(`${host.url}/api/admin/test-index`, {
            headers,
            body: HttpBody.jsonUnsafe({ pauseIndex: false }),
          });

          yield* unpause.text;
          // Acquire finds the durable `wakeSearch` mark from before the stop and arms the
          // alarm, which indexes the folder with no new write.
          yield* until(listed(host.url), (current) => current.length === 1);
          expect(yield* listed(host.url)).toEqual([{ folderId: "dirty" }]);

          const removed = yield* HttpClient.post(`${host.url}/api/admin/test-remove-folder`, {
            headers,
            body: HttpBody.jsonUnsafe({ folderId: "dirty" }),
          });

          expect(removed.status).toBe(204);
          yield* removed.text;
          yield* until(rows(host.url), (current) => current[id] === undefined);
          yield* until(listed(host.url), (current) => current.length === 0);
          yield* Effect.promise(host.stop);

          for (const path of new Bun.Glob("objects/folder-sync-IndexObject/*.sqlite").scanSync(
            directory,
          )) {
            if (path.endsWith("/metadata.sqlite")) continue;
            const db = new Database(`${directory}/${path}`, { readonly: true });

            try {
              expect(
                db
                  .query(
                    "SELECT owner_key FROM streamsy_owner_records WHERE owner_key LIKE '%folder-index%'",
                  )
                  .all(),
              ).toEqual([]);
            } finally {
              db.close();
            }
          }

          const again = yield* launch(directory);
          expect(yield* listed(again.url)).toEqual([]);
        }),
      { timeout: 30000 },
    );

    it.effect(
      "a folder object stopped before its wake alarm produces the row after a restart, with no new write",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-wake-mark-");
          const first = yield* launch(directory);
          yield* create(first.url, "stalled");

          const pause = yield* HttpClient.post(`${first.url}/api/admin/test-folder`, {
            headers,
            body: HttpBody.jsonUnsafe({ folderId: "stalled", pauseWake: true }),
          });

          expect(pause.status).toBe(200);
          yield* pause.text;
          const tail = yield* write(first.url, "stalled", "a.md");
          const id = Files.at({ folderId: "stalled" }).id;
          expect((yield* rows(first.url))[id]).not.toBe(tail);
          yield* Effect.promise(first.stop);

          // The restart drops the in-memory set. Only the pending mark records the write.
          const host = yield* launch(directory);

          const resume = yield* HttpClient.post(`${host.url}/api/admin/test-folder`, {
            headers,
            body: HttpBody.jsonUnsafe({ folderId: "stalled", pauseWake: false }),
          });

          expect(resume.status).toBe(200);
          yield* resume.text;

          // A read acquires the folder object, which finds the mark and arms its alarm.
          const key = yield* Schema.encodeEffect(FolderKey)("stalled");
          expect(yield* status(HttpClient.head(`${host.url}/streams/folders/${key}/files`))).toBe(
            200,
          );
          yield* until(rows(host.url), (current) => current[id] === tail);
        }),
      { timeout: 30000 },
    );

    it.effect(
      "after a restart, a folder object's next write starts its rows with no saved id",
      () =>
        Effect.gen(function* () {
          const directory = yield* testDirectory("worker-wake-restart-");
          const first = yield* launch(directory);
          yield* create(first.url, "later");
          yield* Effect.promise(first.stop);

          const host = yield* launch(directory);
          const tail = yield* write(host.url, "later", "a.md");
          const later = Files.at({ folderId: "later" }).id;
          yield* until(rows(host.url), (current) => current[later] === tail);
        }),
      { timeout: 30000 },
    );
  },
);

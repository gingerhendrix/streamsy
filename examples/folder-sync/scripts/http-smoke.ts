import { StreamAddress } from "@streamsy/core";
import { BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import { Console, Effect, Fiber, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { testDirectory } from "./test-directory.ts";
import { Change, ManifestFile } from "../server/streams.ts";
import { SearchResult } from "../server/file-index.ts";
import { FILE_LIMIT, FileOp, Version } from "../server/domain.ts";

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

const metadata = (opId: string, base?: string) => {
  const common = {
    "x-file-op-id": encodeURIComponent(opId),
    "x-file-author": encodeURIComponent("Zoë"),
    "x-file-origin": encodeURIComponent("smoke/雪"),
  };

  return base === undefined ? common : { ...common, "x-file-base": base };
};

const text = (value: string) => new TextEncoder().encode(value);

const written = (url: string, opId: string, body: Uint8Array, base?: string) =>
  Effect.gen(function* () {
    const response = yield* HttpClient.put(url, {
      headers: metadata(opId, base),
      body: HttpBody.uint8Array(body),
    });

    yield* check(response.status === 200, `PUT ${url}: ${response.status} ${yield* response.text}`);

    return yield* Schema.decodeUnknownEffect(Written)(yield* response.json);
  });

class NotIndexed extends Schema.TaggedError<NotIndexed>()("NotIndexed", {}) {}

const searchUntil = (url: string, q: string, version: string) =>
  Effect.gen(function* () {
    const response = yield* HttpClient.get(`${url}/api/search?q=${encodeURIComponent(q)}`);
    yield* check(response.status === 200, `search status ${response.status}`);

    const rows = yield* Schema.decodeUnknownEffect(Schema.Array(SearchResult))(
      yield* response.json,
    );

    if (!rows.some((row) => row.version === version)) return yield* new NotIndexed();

    return rows;
  }).pipe(
    Effect.retry({
      while: Schema.is(NotIndexed),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 150 })),
    }),
  );

const listUntil = (url: string, query: string, version: string) =>
  Effect.gen(function* () {
    const response = yield* HttpClient.get(`${url}/api/ls?${query}`);
    yield* check(response.status === 200, `list status ${response.status}`);
    yield* check(
      Schema.is(Version)(response.headers["stream-next-offset"]),
      "ls includes its consumed feed cursor",
    );

    const rows = yield* Schema.decodeUnknownEffect(Schema.Array(ManifestFile))(
      yield* response.json,
    );

    if (!rows.some((row) => row.version === version)) return yield* new NotIndexed();

    return rows;
  }).pipe(
    Effect.retry({
      while: Schema.is(NotIndexed),
      schedule: Schedule.spaced("20 millis").pipe(Schedule.upTo({ times: 150 })),
    }),
  );

const readChanges = Effect.fn("Smoke.readChanges")(function* (url: string) {
  let offset = "-1";
  const rows: Change[] = [];

  while (true) {
    const response = yield* HttpClient.get(`${url}/streams/changes?offset=${offset}`);
    yield* check(response.status === 200, `changes status ${response.status}`);
    rows.push(...(yield* Schema.decodeUnknownEffect(Schema.Array(Change))(yield* response.json)));
    const next = response.headers["stream-next-offset"];

    if (next === undefined) return yield* new SmokeFailure({ message: "missing changes cursor" });
    offset = next;

    if (response.headers["stream-up-to-date"] === "true") return { rows, offset };
  }
});

const readManifest = Effect.fn("Smoke.readManifest")(function* (url: string) {
  const rows = new Map<string, ManifestFile>();
  const Events = Schema.Array(StreamAddress.stateChange({ type: "file", schema: ManifestFile }));
  let offset = "-1";

  while (true) {
    const response = yield* HttpClient.get(`${url}/streams/manifest?offset=${offset}`);
    yield* check(
      response.status === 200 && response.headers["x-streamsy-state-version"] === "1",
      "manifest serves Durable State contract",
    );
    const events = yield* Schema.decodeUnknownEffect(Events)(yield* response.json);

    for (const event of events) {
      if ("value" in event) rows.set(event.key, event.value);
      else rows.delete(event.key);
    }

    const next = response.headers["stream-next-offset"];

    if (next === undefined) return yield* new SmokeFailure({ message: "missing manifest cursor" });
    offset = next;

    if (response.headers["stream-up-to-date"] === "true") return [...rows.values()];
  }
});

const orderedManifest = (entries: readonly ManifestFile[]) =>
  entries.toSorted((a, b) => a.id.localeCompare(b.id));

const smoke = Effect.gen(function* () {
  const directory = yield* testDirectory("http-smoke-");

  for (const mode of ["transient", "sqlite"] as const) {
    const options = {
      cwd: `${import.meta.dir}/..`,
      entry: "server/index.ts",
      isReady: (status: number) => status === 200,
      env:
        mode === "sqlite"
          ? { FOLDER_SYNC_DB: `${directory}/files.sqlite` }
          : { FOLDER_SYNC_DB: "" },
    };

    const server = yield* spawnServer(options);
    const folderId = "2026-10-10/demo with spaces";

    const create = yield* HttpClient.post(`${server.url}/api/folders`, {
      body: yield* HttpBody.json({ folderId, title: "Smoke folder" }),
    });

    yield* check(create.status === 200, `create folder: ${create.status} ${yield* create.text}`);
    const folderUrl = `${server.url}/api/folders/${encodeURIComponent(folderId)}`;
    const url = `${folderUrl}/files/docs/hello%20world.md`;
    const first = yield* written(url, "first", text("first version"));
    const second = yield* written(url, "second", text("second version"), first.version);
    const third = yield* written(url, "third", text("stale edit"), first.version);
    yield* check(
      third.overwrote === second.version,
      "stale base must name the overwritten version",
    );
    const replay = yield* written(url, "third", text("stale edit"), first.version);
    yield* check(replay.version === third.version, "replay must reuse the version");
    const head = yield* HttpClient.get(`${url}?minVersion=${third.version}`);
    yield* check((yield* head.text) === "stale edit", "head bytes");
    yield* check(head.headers["x-file-version"] === third.version, "head version");
    const old = yield* HttpClient.get(`${url}?at=${first.version}`);
    yield* check((yield* old.text) === "first version", "historical bytes");
    const historyResponse = yield* HttpClient.get(`${folderUrl}/history/docs/hello%20world.md`);
    const history = yield* Schema.decodeUnknownEffect(History)(yield* historyResponse.json);
    yield* check(history.length === 3, "history contains exactly three writes");
    yield* check(
      history.every(
        ({ operation }) => operation.author === "Zoë" && operation.origin === "smoke/雪",
      ),
      "Unicode metadata survives the HTTP headers",
    );

    const invalidMetadata = yield* HttpClient.put(`${folderUrl}/files/invalid-metadata`, {
      headers: { ...metadata("invalid"), "x-file-author": "%GG" },
      body: HttpBody.uint8Array(text("invalid")),
    });

    yield* check(invalidMetadata.status === 400, "invalid percent-encoding answers 400");
    yield* check(history[2]?.operation.overwrote === second.version, "history overwrite mark");
    // A1 succeeds, B1 moves the head, then a lost-response outbox replays A1.
    const replayUrl = `${folderUrl}/files/replay.md`;
    const a1 = yield* written(replayUrl, "A1", text("from A"));
    const b1 = yield* written(replayUrl, "B1", text("from B"), a1.version);
    const a1Replay = yield* written(replayUrl, "A1", text("from A"));
    yield* check(
      a1Replay.version === a1.version && a1Replay.overwrote === a1.overwrote,
      "intervening-write replay returns the original receipt",
    );
    const replayHead = yield* HttpClient.get(replayUrl);
    yield* check(
      (yield* replayHead.text) === "from B" && replayHead.headers["x-file-version"] === b1.version,
      "replay preserves B's head",
    );
    const replayHistory = yield* HttpClient.get(`${folderUrl}/history/replay.md`);
    yield* check(
      (yield* Schema.decodeUnknownEffect(History)(yield* replayHistory.json)).length === 2,
      "replay after B appends nothing",
    );

    const reused = yield* HttpClient.put(`${folderUrl}/files/other.md`, {
      headers: metadata("A1"),
      body: HttpBody.uint8Array(text("bad")),
    });

    yield* check(
      reused.status === 400 && (yield* reused.text).includes("opId already used"),
      "opId cannot move to another path",
    );
    const binary = new Uint8Array([0, 255, 1, 2]);
    const blobUrl = `${folderUrl}/files/binary.bin`;
    yield* written(blobUrl, "binary", binary);
    const binaryHistory = yield* HttpClient.get(`${folderUrl}/history/binary.bin`);
    const rows = yield* Schema.decodeUnknownEffect(History)(yield* binaryHistory.json);
    const op = rows[0]?.operation;

    if (op === undefined || op.type !== "put" || !("blob" in op))
      return yield* new SmokeFailure({ message: "binary must use a blob" });
    const blob = yield* HttpClient.get(`${server.url}/api/blobs/${op.blob}`);
    yield* check(
      new Uint8Array(yield* blob.arrayBuffer).join(",") === binary.join(","),
      "blob bytes",
    );
    yield* check(
      blob.headers["cache-control"]?.includes("immutable") === true,
      "blob cache headers",
    );

    const oversized = yield* HttpClient.put(`${folderUrl}/files/oversized.bin`, {
      headers: metadata("oversized"),
      body: HttpBody.uint8Array(new Uint8Array(FILE_LIMIT + 1)),
    });

    yield* check(oversized.status === 413, `oversized body: ${oversized.status}`);

    const missingDelete = yield* HttpClient.del(`${folderUrl}/files/never-written`, {
      headers: metadata("missing-delete"),
    });

    yield* check(missingDelete.status === 404, "delete of a never-written path answers 404");
    const missingHistory = yield* HttpClient.get(`${folderUrl}/history/never-written`);
    yield* check(
      (yield* Schema.decodeUnknownEffect(History)(yield* missingHistory.json)).length === 0,
      "missing delete appends nothing",
    );
    const deleted = yield* HttpClient.del(url, { headers: metadata("delete", third.version) });
    yield* check(deleted.status === 200, "delete status");
    const deletion = yield* Schema.decodeUnknownEffect(Written)(yield* deleted.json);

    const repeatedDelete = yield* HttpClient.del(url, {
      headers: metadata("new-delete", deletion.version),
    });

    yield* check(repeatedDelete.status === 404, "a new delete of a tombstone answers 404");

    const replayedDelete = yield* HttpClient.del(url, {
      headers: metadata("delete", third.version),
    });

    const deleteReplay = yield* Schema.decodeUnknownEffect(Written)(yield* replayedDelete.json);
    yield* check(
      replayedDelete.status === 200 && deleteReplay.version === deletion.version,
      "known delete replay reuses its original version",
    );
    const deletedHistory = yield* HttpClient.get(`${folderUrl}/history/docs/hello%20world.md`);
    yield* check(
      (yield* Schema.decodeUnknownEffect(History)(yield* deletedHistory.json)).length === 4,
      "failed and replayed deletes append nothing",
    );
    const deletedHead = yield* HttpClient.get(url);
    yield* check(deletedHead.status === 404, "deleted head status");
    const retained = yield* HttpClient.get(`${url}?at=${third.version}`);
    yield* check((yield* retained.text) === "stale edit", "delete retains history");

    const indexed = yield* written(
      `${folderUrl}/files/docs/index.md`,
      "index",
      text("durable search corpus"),
    );

    yield* searchUntil(server.url, "durable", indexed.version);

    const listed = yield* listUntil(
      server.url,
      `folder=${encodeURIComponent(folderId)}&prefix=docs%2F&glob=**%2F*.md`,
      indexed.version,
    );

    yield* check(
      listed.length === 1 && listed[0]?.path === "docs/index.md",
      "ls prefix and glob exclude deleted files",
    );
    const invalidSearch = yield* HttpClient.get(`${server.url}/api/search?q=%22unterminated`);
    yield* check(invalidSearch.status === 400, "invalid FTS query answers 400");
    const feed = yield* readChanges(server.url);
    yield* check(
      feed.rows.some((row) => row.version === third.version && row.overwrote === second.version),
      "feed preserves overwrite marks",
    );
    yield* check(
      feed.rows.some((row) => row.deleted && row.version === deletion.version),
      "feed contains tombstones",
    );
    yield* check(
      feed.rows.every((row) => row.folderId === folderId),
      "feed preserves opaque slash folder id",
    );

    const tail = yield* HttpClient.get(
      `${server.url}/streams/changes?offset=${feed.offset}&live=long-poll`,
    ).pipe(Effect.forkScoped);

    const tailed = yield* written(`${folderUrl}/files/tail.md`, "tail", text("tailneedle"));
    const reply = yield* Fiber.join(tail);
    yield* check(reply.status === 200, "change long-poll wakes on a commit");
    const tailRows = yield* Schema.decodeUnknownEffect(Schema.Array(Change))(yield* reply.json);
    yield* check(
      tailRows.length === 1 && tailRows[0]?.version === tailed.version,
      "tail resumes after cursor without duplicates",
    );
    const current = yield* listUntil(server.url, "", tailed.version);
    const manifest = yield* readManifest(server.url);
    const encodeManifest = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(ManifestFile)));
    const fromStream = yield* encodeManifest(orderedManifest(manifest));
    const fromList = yield* encodeManifest(orderedManifest(current));
    yield* check(
      fromStream === fromList,
      "Durable State replay equals ls after updates and deletes",
    );
    yield* server.stop;

    if (mode === "sqlite") {
      const restart = yield* spawnServer({ ...options, port: server.port });
      const persisted = yield* HttpClient.get(`${restart.url}/api/blobs/${op.blob}`);
      yield* check(
        new Uint8Array(yield* persisted.arrayBuffer).join(",") === binary.join(","),
        "blob survives restart",
      );

      const persistedHistory = yield* HttpClient.get(
        `${restart.url}/api/folders/${encodeURIComponent(folderId)}/history/docs/hello%20world.md`,
      );

      yield* check(
        (yield* Schema.decodeUnknownEffect(History)(yield* persistedHistory.json)).length === 4,
        "log survives restart",
      );
      const restartedUrl = `${restart.url}/api/folders/${encodeURIComponent(folderId)}`;

      const afterRestart = yield* written(
        `${restartedUrl}/files/restart.md`,
        "restart-index",
        text("restartneedle"),
      );

      yield* searchUntil(restart.url, "restartneedle", afterRestart.version);
      yield* listUntil(restart.url, `folder=${encodeURIComponent(folderId)}`, afterRestart.version);
      const restartedFeed = yield* readChanges(restart.url);
      yield* check(
        restartedFeed.rows.length === feed.rows.length + 2,
        "restart restores old folder followers without duplicate changes",
      );
      yield* check(
        restartedFeed.rows.some(
          (row) => row.path === "restart.md" && row.version === afterRestart.version,
        ),
        "old folder feeds changes after restart",
      );
      yield* restart.stop;
    }

    yield* Console.log(`folder-sync HTTP smoke passed (${mode})`);
  }

  return undefined;
});

BunRuntime.runMain(
  smoke.pipe(
    Effect.scoped,
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This smoke script is an executable runtime boundary.
    Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  ),
);

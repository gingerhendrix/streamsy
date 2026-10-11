import { BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import { Console, Effect, Layer, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient } from "effect/http";
import { testDirectory } from "./test-directory.ts";
import { FILE_LIMIT, FileOp, Version } from "../server/domain.ts";

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const Written = Schema.Struct({ version: Version, overwrote: Schema.optionalKey(Version) });

const History = Schema.Array(Schema.Struct({ version: Version, operation: FileOp }));

const metadata = (opId: string, base?: string) => {
  const common = { "x-file-op-id": opId, "x-file-author": "smoke", "x-file-origin": "smoke" };

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

const smoke = Effect.gen(function* () {
  const directory = yield* testDirectory("http-smoke-");

  for (const mode of ["memory", "sqlite"] as const) {
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
    const deleted = yield* HttpClient.del(url, { headers: metadata("delete", third.version) });
    yield* check(deleted.status === 200, "delete status");
    const deletedHead = yield* HttpClient.get(url);
    yield* check(deletedHead.status === 404, "deleted head status");
    const retained = yield* HttpClient.get(`${url}?at=${third.version}`);
    yield* check((yield* retained.text) === "stale edit", "delete retains history");
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

import { BunCrypto, BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import { Array, Config, Console, Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { Deleted, prepareContent, Head } from "../server/domain.ts";
import { FolderKey } from "../server/streams.ts";
import { ListedFile, SearchResult } from "../server/index-rows.ts";
import { testDirectory } from "./test-directory.ts";

class SmokeFailure extends Schema.TaggedError<SmokeFailure, { readonly brand: unique symbol }>()(
  "SmokeFailure",
  {
    message: Schema.String,
  },
) {}

class Waiting extends Schema.TaggedError<Waiting, { readonly brand: unique symbol }>()(
  "Waiting",
  {},
) {}

const check = (ok: boolean, message: string) =>
  ok ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const text = (value: string) => new TextEncoder().encode(value);

const smoke = Effect.gen(function* () {
  const directory = yield* testDirectory("http-smoke-");
  const target = yield* Config.String("FOLDER_SYNC_URL").pipe(Config.withDefault(""));

  const configuredToken = yield* Config.String("FOLDER_SYNC_WRITER_TOKEN").pipe(
    Config.withDefault(""),
  );

  const token = configuredToken || "local-http-smoke-token";
  const rawClient = yield* HttpClient.HttpClient;
  const client = HttpClient.mapRequest(rawClient, HttpClientRequest.bearerToken(token));
  const modes: readonly string[] = target ? ["external"] : ["transient", "sqlite"];

  for (const mode of modes) {
    const options = {
      cwd: `${import.meta.dirname}/..`,
      entry: "server/index.ts",
      isReady: (status: number) => status === 200,
      env: {
        FOLDER_SYNC_WRITER_TOKEN: token,
        FOLDER_SYNC_DB: mode === "sqlite" ? `${directory}/files.sqlite` : "",
      },
    };

    const server = target
      ? { url: target, port: Number(new URL(target).port), stop: Effect.void }
      : yield* spawnServer(options);

    const folderId = "2026/demo with spaces/雪";
    const folderUrl = `${server.url}/api/folders/${encodeURIComponent(folderId)}`;
    const streamUrl = `${server.url}/streams/folders/${yield* Schema.encodeEffect(FolderKey)(folderId)}/files`;

    const create = yield* client.post(`${server.url}/api/folders`, {
      body: HttpBody.jsonUnsafe({ txid: "folder", folderId, title: "Smoke folder" }),
    });

    yield* check(create.status === 200, `Create answered ${create.status}: ${yield* create.text}`);

    const put = Effect.fn("Smoke.put")(function* (
      path: string,
      bytes: Uint8Array,
      txid: string,
      parent: number | null = null,
    ) {
      const content = yield* prepareContent(bytes);

      const upload = yield* client.put(`${server.url}/api/blobs/${content.hash}`, {
        body: HttpBody.uint8Array(bytes),
      });

      yield* check(upload.status === 200, "blob upload");

      const response = yield* client.put(`${folderUrl}/files/${path}`, {
        body: HttpBody.jsonUnsafe({ ...content, parent, author: "Zoë", txid }),
      });

      yield* check(
        response.status === 200,
        `Commit ${path}: ${response.status} ${yield* response.text}`,
      );

      return yield* Schema.decodeUnknownEffect(Head)(yield* response.json);
    });

    const one = yield* put("a.md", text("first"), "first");
    const two = yield* put("a.md", text("second"), "second", one.version);
    const same = yield* put("a.md", text("second"), "noop", one.version);
    yield* check(
      same.txid === "second" && same.version === two.version,
      "same-hash no-op returns current txid",
    );

    const retry = yield* put("a.md", text("first"), "first");
    yield* check(
      retry.version === one.version && retry.parent === null,
      "late retry gets its first answer",
    );

    const fork = yield* put("a.md", text("fork"), "fork", one.version);
    yield* check(
      fork.version === 3 && fork.parent === one.version,
      "two writes from one parent both land, and the second is the head",
    );

    const reused = yield* client.put(`${folderUrl}/files/b.md`, {
      body: HttpBody.jsonUnsafe({
        ...(yield* prepareContent(text("first"))),
        parent: null,
        txid: "first",
        author: "test",
      }),
    });

    yield* check(reused.status === 400, "a txid used for another path gets 400");

    const conditional = yield* client.put(`${folderUrl}/files/a.md`, {
      headers: { "if-match": `"${fork.version}"` },
      body: HttpBody.jsonUnsafe({
        ...(yield* prepareContent(text("conditional"))),
        parent: fork.version,
        txid: "conditional",
        author: "test",
      }),
    });

    yield* check(conditional.status === 400, "If-Match gets 400");
    const read = yield* client.get(`${folderUrl}/files/a.md`);
    yield* check(
      read.headers.etag === `"${fork.version}"` && (yield* read.text) === "fork",
      "version ETag and content",
    );

    const deleted = yield* client.del(`${folderUrl}/files/a.md`, {
      body: HttpBody.jsonUnsafe({ txid: "delete", author: "test", parent: fork.version }),
    });

    yield* check(deleted.status === 200, "delete");
    const tombstone = yield* Schema.decodeUnknownEffect(Deleted)(yield* deleted.json);
    yield* check(
      tombstone.version === 4 && tombstone.parent === fork.version,
      "delete takes a version",
    );
    const gone = yield* client.get(`${folderUrl}/files/a.md`);

    const missing = yield* Schema.decodeUnknownEffect(Schema.Struct({ deleted: Schema.Int }))(
      yield* gone.json,
    );

    yield* check(
      gone.status === 404 && missing.deleted === tombstone.version,
      "deleted file names its tombstone",
    );
    const three = yield* put("a.md", text("durable searchneedle"), "third", tombstone.version);
    yield* check(three.version === 5 && three.parent === 4, "a new file names the delete");
    const binary = yield* put("binary", new Uint8Array([0, 255, 1, 2]), "binary");
    const blob = yield* client.get(`${server.url}/api/blobs/${binary.hash}`);
    yield* check(
      new Uint8Array(yield* blob.arrayBuffer).join(",") === "0,255,1,2" &&
        blob.headers["cache-control"]?.includes("immutable") === true,
      "binary bytes and cache headers",
    );
    const blobHead = yield* client.head(`${server.url}/api/blobs/${binary.hash}`);
    yield* check(
      blobHead.status === 200 && blobHead.headers["content-length"] === "4",
      "blob HEAD",
    );

    for (const url of [`${server.url}/streams/folders`, streamUrl])
      for (const method of ["PUT", "POST", "DELETE"] as const) {
        const response = yield* client.execute(HttpClientRequest.make(method)(url));
        yield* check(response.status === 405, "streams are read-only");
      }

    const stream = yield* client.get(`${streamUrl}?offset=-1`);
    const changes = yield* stream.json;
    yield* check(
      Array.isArray(changes) && changes.length === 6,
      "source history contains one change per write",
    );
    yield* Effect.gen(function* () {
      const result = yield* client.get(`${server.url}/api/search?q=searchneedle`);

      const rows = yield* Schema.decodeUnknownEffect(Schema.Array(SearchResult))(
        yield* result.json,
      );

      if (rows.length !== 1) return yield* new Waiting();

      return undefined;
    }).pipe(
      Effect.retry({
        while: Schema.is(Waiting),
        schedule: Schedule.spaced("10 millis").pipe(Schedule.upTo({ times: 200 })),
      }),
    );
    const listing = yield* client.get(`${server.url}/api/ls`);
    const rows = yield* Schema.decodeUnknownEffect(Schema.Array(ListedFile))(yield* listing.json);
    yield* check(rows.length === 2, "ls reflects current files");
    yield* server.stop;

    if (mode === "sqlite") {
      const restart = yield* spawnServer({ ...options, port: server.port });
      const restartContent = yield* prepareContent(text("restart"));

      const restartUpload = yield* client.put(`${restart.url}/api/blobs/${restartContent.hash}`, {
        body: HttpBody.uint8Array(text("restart")),
      });

      yield* check(restartUpload.status === 200, "blob upload after restart");

      const response = yield* client.put(
        `${restart.url}/api/folders/${encodeURIComponent(folderId)}/files/after-restart`,
        {
          body: HttpBody.jsonUnsafe({
            ...restartContent,
            parent: null,
            txid: "restart",
            author: "test",
          }),
        },
      );

      const head = yield* Schema.decodeUnknownEffect(Head)(yield* response.json);
      yield* check(head.version === 7, "counter survives restart");
      const retained = yield* client.get(`${restart.url}/api/blobs/${binary.hash}`);
      yield* check(
        new Uint8Array(yield* retained.arrayBuffer).join(",") === "0,255,1,2",
        "disk blob survives restart",
      );
      yield* restart.stop;
    }

    yield* Console.log(`folder-sync HTTP smoke passed (${mode})`);
  }
});

BunRuntime.runMain(
  smoke.pipe(
    Effect.scoped,
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- Executable smoke owns the scoped HTTP and Bun runtime.
    Effect.provide(Layer.mergeAll(BunServices.layer, BunCrypto.layer, FetchHttpClient.layer)),
  ),
);

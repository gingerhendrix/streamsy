import { BunCrypto, BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import { Config, Console, Effect, Layer, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpBody, HttpClient, HttpClientRequest } from "effect/http";
import { prepareContent, Head } from "../server/domain.ts";
import { FolderKey, ListedFile } from "../server/streams.ts";
import { SearchResult } from "../server/search-result.ts";
import { testDirectory } from "./test-directory.ts";

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

class Waiting extends Schema.TaggedError<Waiting>()("Waiting", {}) {}

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
      base?: number,
    ) {
      const content = yield* prepareContent(bytes);

      if (content.text === undefined) {
        const upload = yield* client.put(`${server.url}/api/blobs/${content.hash}`, {
          body: HttpBody.uint8Array(bytes),
        });

        yield* check(upload.status === 200, "blob upload");
      }

      const response = yield* client.put(`${folderUrl}/files/${path}`, {
        headers: base === undefined ? {} : { "if-match": `"${base}"` },
        body: HttpBody.jsonUnsafe({ ...content, author: "Zoë", txid }),
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

    const stale = yield* client.put(`${folderUrl}/files/a.md`, {
      headers: { "if-match": `"${one.version}"` },
      body: HttpBody.jsonUnsafe({
        ...(yield* prepareContent(text("first"))),
        txid: "first",
        author: "test",
      }),
    });

    yield* check(stale.status === 412, "moved retry gets 412");
    const read = yield* client.get(`${folderUrl}/files/a.md`);
    yield* check(
      read.headers.etag === `"${two.version}"` && (yield* read.text) === "second",
      "version ETag and content",
    );

    const deleted = yield* client.del(`${folderUrl}/files/a.md`, {
      headers: { "if-match": `"${two.version}"` },
      body: HttpBody.jsonUnsafe({ txid: "delete", author: "test" }),
    });

    yield* check(deleted.status === 200, "delete");
    yield* check((yield* client.get(`${folderUrl}/files/a.md`)).status === 404, "deleted file");
    const three = yield* put("a.md", text("durable searchneedle"), "third");
    yield* check(three.version === 3, "delete consumes no version");
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
      Array.isArray(changes) && changes.length === 5,
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

      const response = yield* client.put(
        `${restart.url}/api/folders/${encodeURIComponent(folderId)}/files/after-restart`,
        {
          body: HttpBody.jsonUnsafe({
            ...(yield* prepareContent(text("restart"))),
            txid: "restart",
            author: "test",
          }),
        },
      );

      const head = yield* Schema.decodeUnknownEffect(Head)(yield* response.json);
      yield* check(head.version === 5, "counter survives restart");
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

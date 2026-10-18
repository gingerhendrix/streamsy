import { BunRuntime, BunServices } from "@effect/platform-bun";
import { spawnServer } from "@streamsy/test-support/server";
import {
  Clock,
  Config,
  Console,
  Crypto,
  Effect,
  Fiber,
  Layer,
  Option,
  Path,
  Schema,
  Stream,
} from "effect";
import {
  FetchHttpClient,
  Headers,
  HttpBody,
  HttpClient,
  type HttpClientResponse,
} from "effect/http";

class SmokeFailure extends Schema.TaggedError<SmokeFailure>()("SmokeFailure", {
  message: Schema.String,
}) {}

const check = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new SmokeFailure({ message }));

const requiredHeader = (response: HttpClientResponse.HttpClientResponse, name: string) =>
  Headers.get(response.headers, name).pipe(
    Option.filter((value) => value.length > 0),
    Effect.fromOption,
    Effect.mapError(() => new SmokeFailure({ message: `Expected response header ${name}` })),
  );

const readSseUntilControl = (response: HttpClientResponse.HttpClientResponse) =>
  response.stream.pipe(
    Stream.decodeText,
    Stream.scan(
      () => "",
      (output, chunk) => output + chunk,
    ),
    Stream.takeUntil(
      (output) => output.includes("event: data") && output.includes("event: control"),
    ),
    Stream.runLast,
    Effect.map(Option.getOrElse(() => "")),
    Effect.timeoutOrElse({
      duration: "5 seconds",
      orElse: () =>
        Effect.fail(new SmokeFailure({ message: "Timed out waiting for SSE data/control events" })),
    }),
  );

/** The protocol checks against one stream URL. Returns the tail after the second append. */
const protocolChecks = Effect.fn("protocolChecks")(function* (streamUrl: string) {
  const createResponse = yield* HttpClient.put(streamUrl, {
    headers: { "content-type": "application/json" },
  });

  yield* check(createResponse.status === 201, `create status ${createResponse.status}`);
  yield* requiredHeader(createResponse, "stream-next-offset");

  const appendResponse = yield* HttpClient.post(streamUrl, {
    body: HttpBody.jsonUnsafe({ type: "hello", n: 1 }),
  });

  yield* check(appendResponse.status === 204, `append status ${appendResponse.status}`);
  const firstOffset = yield* requiredHeader(appendResponse, "stream-next-offset");

  const readResponse = yield* HttpClient.get(`${streamUrl}?offset=-1`);
  yield* check(readResponse.status === 200, `read status ${readResponse.status}`);
  yield* check(readResponse.headers["stream-up-to-date"] === "true", "read should be up to date");
  const readOffset = yield* requiredHeader(readResponse, "stream-next-offset");
  yield* check(readOffset === firstOffset, "read next offset should match append offset");
  const etag = yield* requiredHeader(readResponse, "etag");
  const readBody = yield* readResponse.text;
  yield* check(readBody === '[{"type":"hello","n":1}]', `unexpected read body ${readBody}`);

  const notModifiedResponse = yield* HttpClient.get(`${streamUrl}?offset=-1`, {
    headers: { "if-none-match": etag },
  });

  yield* check(
    notModifiedResponse.status === 304,
    `If-None-Match status ${notModifiedResponse.status}`,
  );

  const longPoll = yield* Effect.forkScoped(
    HttpClient.get(`${streamUrl}?offset=${firstOffset}&live=long-poll`),
  );

  yield* Effect.sleep("100 millis");

  const secondAppendResponse = yield* HttpClient.post(streamUrl, {
    body: HttpBody.jsonUnsafe({ type: "hello", n: 2 }),
  });

  yield* check(
    secondAppendResponse.status === 204,
    `second append status ${secondAppendResponse.status}`,
  );

  const longPollResponse = yield* Fiber.join(longPoll);
  yield* check(
    longPollResponse.status === 200,
    `long-poll success status ${longPollResponse.status}`,
  );
  yield* requiredHeader(longPollResponse, "stream-cursor");
  const secondOffset = yield* requiredHeader(longPollResponse, "stream-next-offset");
  const longPollBody = yield* longPollResponse.text;
  yield* check(
    longPollBody === '[{"type":"hello","n":2}]',
    `unexpected long-poll body ${longPollBody}`,
  );

  const sseResponse = yield* HttpClient.get(`${streamUrl}?offset=-1&live=sse`);
  yield* check(sseResponse.status === 200, `SSE status ${sseResponse.status}`);
  const contentType = sseResponse.headers["content-type"];
  yield* check(
    contentType?.startsWith("text/event-stream") === true,
    `unexpected SSE content-type ${contentType}`,
  );
  const sseText = yield* readSseUntilControl(sseResponse);
  yield* check(sseText.includes("event: data"), "SSE should include a data event");
  yield* check(sseText.includes("event: control"), "SSE should include a control event");
  yield* check(
    sseText.includes(`"streamNextOffset":"${secondOffset}"`),
    "SSE control should include the current next offset",
  );
  yield* check(sseText.includes('"upToDate":true'), "SSE control should mark upToDate");

  return secondOffset;
});

/**
 * Spawns `src/index.ts` and runs the protocol checks, plus the checks that
 * need a server of its own: both loopback addresses, the drain on stop, and a
 * fresh store after a restart.
 */
const spawned = Effect.fn("spawned")(function* (streamId: string) {
  const path = yield* Path.Path;
  const cwd = path.resolve(import.meta.dir, "..");

  // The protocol answers 400 at the root once the routes are mounted.
  const serverOptions = { cwd, entry: "src/index.ts", isReady: (status: number) => status === 400 };
  const server = yield* spawnServer(serverOptions);
  const streamUrl = `${server.url}/${streamId}`;

  for (const host of ["127.0.0.1", "[::1]"]) {
    const response = yield* HttpClient.get(`http://${host}:${server.port}/`);
    yield* check(response.status === 400, `${host} should reach the protocol listener`);
  }

  const secondOffset = yield* protocolChecks(streamUrl);

  const parked = yield* Effect.forkScoped(HttpClient.get(`${streamUrl}?offset=now&live=long-poll`));
  yield* Effect.sleep("250 millis");
  yield* check(parked.pollUnsafe() === undefined, "shutdown probe should have a parked long poll");
  yield* server.stop;
  // Serve.layer answers the parked poll like a long-poll timeout, so the
  // client reconnects from the same offset instead of seeing an error.
  const drained = yield* Fiber.join(parked);
  yield* check(drained.status === 204, `shutdown long-poll status ${drained.status}`);
  yield* check(
    (yield* requiredHeader(drained, "stream-next-offset")) === secondOffset,
    "shutdown long-poll should return the current tail",
  );
  yield* check(
    drained.headers["stream-up-to-date"] === "true",
    "shutdown long-poll should be up to date",
  );
  yield* requiredHeader(drained, "stream-cursor");

  const restarted = yield* spawnServer({ ...serverOptions, port: server.port });
  const afterRestartResponse = yield* HttpClient.get(`${restarted.url}/${streamId}?offset=-1`);
  yield* check(
    afterRestartResponse.status === 404,
    `read after restart status ${afterRestartResponse.status}`,
  );

  yield* Console.log(
    `memory-server HTTP smoke passed for ${streamId}, including fresh-store restart`,
  );
});

/**
 * With `BASE_URL` set, the smoke runs the protocol checks against that running
 * server and does not spawn one. The base URL may hold a path prefix.
 */
const smoke = Effect.gen(function* () {
  const baseUrl = yield* Config.String("BASE_URL").pipe(Config.option);
  const streamId = `smoke/${yield* Clock.currentTimeMillis}-${yield* (yield* Crypto.Crypto).randomUUIDv4()}`;
  yield* Option.match(baseUrl, {
    onNone: () => spawned(streamId),
    onSome: (url) =>
      protocolChecks(`${url.replace(/\/$/, "")}/${streamId}`).pipe(
        Effect.andThen(Console.log(`memory-server HTTP smoke passed for ${url}/${streamId}`)),
      ),
  });
});

smoke.pipe(
  Effect.scoped,
  // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This script is the entry point.
  Effect.provide(Layer.mergeAll(BunServices.layer, FetchHttpClient.layer)),
  BunRuntime.runMain,
);

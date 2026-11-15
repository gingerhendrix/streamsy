// oxlint-disable effecttsgo/async-function -- vitest drives these Effect programs through Promise boundaries.
import { describe, expect, it } from "vitest";
import { Context, Effect, Layer, Random } from "effect";
import { TestClock } from "effect/testing";
import {
  LIVE_SOCKET_PROTOCOL,
  LiveCloseCode,
  closeCodeForStatus,
  controlFrame,
  isLiveUpgrade,
  nextCursor,
  openFrame,
  upgradeRequired,
} from "../../src/http/live-frames.ts";
import { read } from "../../src/http/read.ts";
import { readQueryParser } from "../../src/http/read-query-parser.ts";
import { StreamsReader, StreamsWriter } from "../../src/protocol/tags.ts";
import { layerTest } from "../../src/testing/streams-test.ts";
import { StreamId } from "../../src/schema/index.ts";
import { CURSOR_EPOCH_MS, CURSOR_INTERVAL_MS } from "../../src/policy/cursor-generator.ts";

const parse = (query: string) => readQueryParser(() => true).parse(new URL(`http://x/s?${query}`));

describe("live=websocket query", () => {
  it("parses websocket as a live mode", () => {
    expect(parse("offset=now&live=websocket")).toEqual({
      ok: true,
      offset: "now",
      live: "websocket",
      cursor: undefined,
    });
  });

  it("ignores an unknown live mode as before", () => {
    expect(parse("offset=now&live=socket")).toMatchObject({ ok: true, live: undefined });
  });
});

describe("live frames", () => {
  it("open carries the protocol, content type and offset", () => {
    expect(JSON.parse(openFrame("text/plain", "1_0"))).toEqual({
      type: "open",
      protocol: LIVE_SOCKET_PROTOCOL,
      contentType: "text/plain",
      streamNextOffset: "1_0",
    });
  });

  it("control follows the SSE control rules", () => {
    expect(
      JSON.parse(controlFrame({ nextOffset: "2_0", cursor: "9", upToDate: true, closed: false })),
    ).toEqual({ type: "control", streamNextOffset: "2_0", streamCursor: "9", upToDate: true });

    expect(
      JSON.parse(controlFrame({ nextOffset: "2_0", cursor: "9", upToDate: false, closed: false })),
    ).toEqual({ type: "control", streamNextOffset: "2_0", streamCursor: "9" });

    // A closed stream has no cursor and no upToDate, as in SSE.
    expect(
      JSON.parse(controlFrame({ nextOffset: "3_0", cursor: "9", upToDate: true, closed: true })),
    ).toEqual({ type: "control", streamNextOffset: "3_0", streamClosed: true });
  });

  it("nextCursor advances like a long-poll cursor", async () => {
    const at = CURSOR_EPOCH_MS + 5 * CURSOR_INTERVAL_MS;

    const [fresh, repeat] = await Effect.runPromise(
      Effect.gen(function* () {
        yield* TestClock.setTime(at);

        return [yield* nextCursor(undefined), yield* nextCursor("5")] as const;
      }).pipe(
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- This test program is its own entry point.
        Effect.provide(TestClock.layer()),
        Effect.provideService(Random.Random, {
          nextIntUnsafe: () => 0,
          nextDoubleUnsafe: () => 0.5,
        }),
      ),
    );

    expect(fresh).toBe("5");
    expect(repeat).toBe("95");
  });
});

describe("live close codes", () => {
  it("maps read statuses to close codes", () => {
    expect(closeCodeForStatus(404)).toBe(LiveCloseCode.NotFound);
    expect(closeCodeForStatus(410)).toBe(LiveCloseCode.Gone);
    expect(closeCodeForStatus(500)).toBe(LiveCloseCode.InternalError);
    expect(closeCodeForStatus(401)).toBe(LiveCloseCode.InternalError);
  });

  it("keeps the protocol's close code values", () => {
    expect(LiveCloseCode).toEqual({
      StreamClosed: 1000,
      InternalError: 1011,
      Restart: 1012,
      CatchUpRequired: 4100,
      BadMessage: 4400,
      NotFound: 4404,
      Gone: 4410,
    });
  });
});

const request = (query: string, init?: RequestInit) => new Request(`http://x/s?${query}`, init);

describe("live upgrade requests", () => {
  it("needs GET, the upgrade header and live=websocket", () => {
    const upgrade = { headers: { upgrade: "WebSocket" } };
    expect(isLiveUpgrade(request("offset=now&live=websocket", upgrade))).toBe(true);
    expect(isLiveUpgrade(request("offset=now&live=websocket"))).toBe(false);
    expect(isLiveUpgrade(request("offset=now&live=sse", upgrade))).toBe(false);
    expect(
      isLiveUpgrade(request("offset=now&live=websocket", { ...upgrade, method: "POST" })),
    ).toBe(false);
  });

  it("answers 426 with the upgrade header", async () => {
    const response = upgradeRequired();
    expect(response.status).toBe(426);
    expect(response.headers.get("upgrade")).toBe("websocket");
    expect(await response.text()).toBe("WebSocket upgrade required");
  });
});

describe("live=websocket on a plain HTTP read", () => {
  const id = StreamId.make("plain");

  const readQuery = (query: string) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(layerTest());
          const writer = Context.get(context, StreamsWriter);
          const reader = Context.get(context, StreamsReader);
          yield* writer.create(id, { contentType: "application/json" });

          const response = yield* read(
            reader,
            id,
            new URL(`http://x/plain?${query}`),
            new Headers(),
            "no-store",
          );

          return response.status;
        }),
      ),
    );

  it("answers 426, so the client falls back to long-poll", async () => {
    expect(await readQuery("offset=now&live=websocket")).toBe(426);
  });

  it("keeps the HTTP errors that come first", async () => {
    expect(await readQuery("live=websocket")).toBe(400);
    expect(await readQuery("offset=bad&live=websocket")).toBe(400);
  });
});

// oxlint-disable effecttsgo/async-function -- These tests exercise the named Web conversion boundary and consume actual response bytes.
import { expect, it } from "bun:test";
import { Clock, Effect, Layer } from "effect";
import { Streams, StreamsReader, ZERO_OFFSET } from "@streamsy/core";
import fixtures from "../../src/http/fixtures/wire.json";
import { makeEdge } from "../../src/http/edge.ts";

interface WireFixture {
  readonly label: string;
  readonly status: number;
  readonly statusText: string;
  readonly headers: Readonly<Record<string, string | undefined>>;
  readonly bytes: ReadonlyArray<number>;
}
const expiryFixtures: Readonly<Record<string, ReadonlyArray<WireFixture>>> = fixtures;

async function assertWire(response: Response, expected: WireFixture, bytes?: Uint8Array) {
  expect(response.status).toBe(expected.status);
  expect(response.statusText).toBe(expected.statusText);
  expect(expected.headers).toStrictEqual(Object.fromEntries(response.headers));
  expect(Array.from(bytes ?? new Uint8Array(await response.arrayBuffer()))).toEqual(
    Array.from(expected.bytes),
  );
}

const fixedTime = 1_780_000_000_000;
const clockLayer = Layer.effect(
  Clock.Clock,
  Effect.gen(function* () {
    const live = yield* Clock.Clock;
    return {
      currentTimeMillis: Effect.succeed(fixedTime),
      currentTimeMillisUnsafe: () => fixedTime,
      currentTimeNanos: live.currentTimeNanos,
      currentTimeNanosUnsafe: () => live.currentTimeNanosUnsafe(),
      monotonicTimeNanos: live.monotonicTimeNanos,
      monotonicTimeNanosUnsafe: () => live.monotonicTimeNanosUnsafe(),
      sleep: (duration) => live.sleep(duration),
    };
  }),
);

for (const visibility of ["private", "public"] as const) {
  it(`frozen ${visibility} status, status text, all headers and body bytes agree`, async () => {
    const layer = Streams.layerMemory({ longPollTimeoutMs: 5 }).pipe(
      Layer.provideMerge(clockLayer),
    );
    const edge = makeEdge(
      { pathPrefix: "/api.v1", cacheVisibility: visibility, maxMessageSize: 64 },
      layer,
    );
    let position = 0;
    const compare = async (path: string, init?: RequestInit) => {
      const expected = fixtures[visibility][position++];
      if (expected === undefined) throw new Error("Missing frozen HTTP response");
      expect(expected.label).toBe(`${init?.method ?? "GET"} ${path}`);
      const right = await edge.handler(new Request(`http://example.test${path}`, init));
      await assertWire(right, expected);
      return right;
    };
    try {
      await compare("/wrong");
      await compare("/api.v1/");
      await compare("/api.v1/s", { method: "OPTIONS" });
      await compare("/api.v1/s", { method: "PATCH" });
      await compare("/api.v1/missing");
      await compare("/api.v1/s", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: '[{"a":1},{"b":2}]',
      });
      await compare("/api.v1/s", {
        method: "PUT",
        headers: { "content-type": "application/json" },
      });
      await compare("/api.v1/s", { method: "PUT", headers: { "content-type": "text/plain" } });
      await compare("/api.v1/s", { method: "HEAD" });
      const catchUp = await compare("/api.v1/s?offset=-1");
      const etag = catchUp.headers.get("etag");
      if (!etag) throw new Error("Expected catch-up ETag");
      await compare("/api.v1/s?offset=-1", { headers: { "if-none-match": etag } });
      await compare("/api.v1/s?offset=now");
      await compare("/api.v1/s?offset=bad");
      for (const body of ["", "[]", "{", "x".repeat(65)])
        await compare("/api.v1/s", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body,
        });
      await compare("/api.v1/s", { method: "POST", headers: { "producer-id": "p" }, body: "x" });
      await compare("/api.v1/s", {
        method: "POST",
        headers: { "stream-expected-offset": "bad" },
        body: "x",
      });
      await compare("/api.v1/s", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "producer-id": "p",
          "producer-epoch": "0",
          "producer-seq": "0",
        },
        body: '{"c":3}',
      });
      await compare("/api.v1/s", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "producer-id": "p",
          "producer-epoch": "0",
          "producer-seq": "0",
        },
        body: '{"c":3}',
      });
      const staleOffset = await compare("/api.v1/s", {
        method: "POST",
        headers: { "content-type": "application/json", "stream-expected-offset": ZERO_OFFSET },
        body: "1",
      });
      expect(staleOffset.status).toBe(409);
      expect(staleOffset.headers.get("stream-next-offset")).toBe(
        "0000000000000003_0000000000000000",
      );
      const mismatch = await compare("/api.v1/s", {
        method: "POST",
        headers: { "content-type": "text/plain" },
        body: "x",
      });
      expect(mismatch.status).toBe(409);
      await compare("/api.v1/s", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "stream-seq": "z",
          "producer-id": "p",
          "producer-epoch": "1",
          "producer-seq": "0",
        },
        body: "1",
      });
      const sequence = await compare("/api.v1/s", {
        method: "POST",
        headers: { "content-type": "application/json", "stream-seq": "a" },
        body: "1",
      });
      expect(sequence.status).toBe(409);
      for (const [epoch, seq, status, expectedHeaders] of [
        ["0", "1", 403, { "producer-epoch": "1" }],
        ["1", "2", 409, { "producer-expected-seq": "1", "producer-received-seq": "2" }],
        ["2", "1", 400, {}],
      ] as const) {
        const conflict = await compare("/api.v1/s", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "producer-id": "p",
            "producer-epoch": epoch,
            "producer-seq": seq,
          },
          body: "1",
        });
        expect(conflict.status).toBe(status);
        expect(Object.fromEntries(conflict.headers)).toMatchObject(expectedHeaders);
      }
      await compare("/api.v1/s?offset=-1&live=long-poll");
      // Fixed wall time makes timeout cursor bytes reproducible; timers remain live.
      await compare("/api.v1/s?offset=now&live=long-poll");
      await compare("/api.v1/s", { method: "POST", headers: { "stream-closed": "true" } });
      const closed = await compare("/api.v1/s", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "1",
      });
      expect(closed.status).toBe(409);
      expect(closed.headers.get("stream-next-offset")).toBe("0000000000000004_0000000000000000");
      expect(closed.headers.get("stream-closed")).toBe("true");
      await compare("/api.v1/s?offset=-1&live=sse");
      await compare("/api.v1/s?offset=now&live=long-poll");
      for (const type of ["text/plain", "application/octet-stream"]) {
        const path = `/api.v1/${type.split("/")[0]}`;
        await compare(path, {
          method: "PUT",
          headers: { "content-type": type, "stream-closed": "true" },
          body: new Uint8Array([65, 13, 10, 66, 0, 255]),
        });
        await compare(path);
        await compare(`${path}?offset=-1&live=sse`);
      }
      await compare("/api.v1/child", {
        method: "PUT",
        headers: { "stream-forked-from": "/api.v1/s" },
      });
      await compare("/api.v1/s", { method: "DELETE" });
      await compare("/api.v1/s");
      await compare("/api.v1/child", { method: "DELETE" });
      await compare("/api.v1/child");
      expect(position).toBe(fixtures[visibility].length);
    } finally {
      await edge.dispose();
    }
  });
}

const expiryCases: ReadonlyArray<{
  name: string;
  headers: Record<string, string>;
  put: number;
  head: number;
  effectRejects?: true;
}> = [
  { name: "positive TTL", headers: { "stream-ttl": "60" }, put: 201, head: 200 },
  { name: "zero TTL", headers: { "stream-ttl": "0" }, put: 201, head: 404 },
  { name: "negative TTL", headers: { "stream-ttl": "-1" }, put: 400, head: 404 },
  { name: "padded TTL", headers: { "stream-ttl": "01" }, put: 400, head: 404 },
  { name: "fractional TTL", headers: { "stream-ttl": "1.5" }, put: 400, head: 404 },
  {
    name: "TTL and expiry",
    headers: { "stream-ttl": "60", "stream-expires-at": "2028-01-01T00:00:00Z" },
    put: 400,
    head: 404,
  },
  {
    name: "ISO UTC",
    headers: { "stream-expires-at": "2028-01-01T00:00:00Z" },
    put: 201,
    head: 200,
  },
  {
    name: "ISO offset",
    headers: { "stream-expires-at": "2028-01-01T01:00:00+01:00" },
    put: 201,
    head: 200,
  },
  {
    name: "ISO no zone",
    headers: { "stream-expires-at": "2028-01-01T00:00:00" },
    put: 201,
    head: 200,
  },
  { name: "numeric year", headers: { "stream-expires-at": "2028" }, put: 201, head: 200 },
  { name: "numeric zero date", headers: { "stream-expires-at": "0" }, put: 201, head: 404 },
  {
    name: "numeric milliseconds rejected",
    headers: { "stream-expires-at": "1780000000000" },
    put: 400,
    head: 404,
  },
  { name: "invalid date", headers: { "stream-expires-at": "not-a-date" }, put: 400, head: 404 },
  {
    name: "lowercase ISO zone",
    headers: { "stream-expires-at": "2028-01-01t00:00:00z" },
    put: 201,
    head: 200,
    effectRejects: true,
  },
  {
    name: "RFC UTC zone",
    headers: { "stream-expires-at": "Sat, 01 Jan 2028 00:00:00 UTC" },
    put: 201,
    head: 200,
    effectRejects: true,
  },
];

for (const fixture of expiryCases) {
  it(`frozen expiry parsing and HEAD: ${fixture.name}${fixture.effectRejects ? " (intentional Effect rejection)" : ""}`, async () => {
    const edge = makeEdge({}, Streams.layerMemory().pipe(Layer.provideMerge(clockLayer)));
    try {
      for (const method of ["PUT", "HEAD"] as const) {
        const init = { method, headers: fixture.headers };
        const right = await edge.handler(new Request("http://example.test/expiry", init));
        const expected = expiryFixtures["expiry: " + fixture.name]?.[method === "PUT" ? 0 : 1];
        if (expected === undefined) throw new Error("Missing frozen expiry response");
        expect(expected.label).toBe(method);
        expect(right.status).toBe(
          fixture.effectRejects
            ? method === "PUT"
              ? 400
              : 404
            : method === "PUT"
              ? fixture.put
              : fixture.head,
        );
        await assertWire(right, expected);
        if (method === "HEAD" && fixture.head === 200 && !fixture.effectRejects) {
          const expectedHeader =
            fixture.headers["stream-ttl"] ?? fixture.headers["stream-expires-at"];
          if (expectedHeader === undefined) throw new Error("Missing fixture expiry header");
          expect(
            right.headers.get(fixture.headers["stream-ttl"] ? "stream-ttl" : "stream-expires-at"),
          ).toBe(expectedHeader);
        }
      }
    } finally {
      await edge.dispose();
    }
  });
}

// Compare complete event bytes, independent of transport chunk boundaries, then cancel.
async function firstEvents(response: Response, count: number): Promise<Uint8Array> {
  if (!response.body) throw new Error("Expected SSE body");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) throw new Error("SSE ended before expected events");
      text += decoder.decode(next.value, { stream: true });
      let end = 0;
      let events = 0;
      while (events < count) {
        const boundary = text.indexOf("\n\n", end);
        if (boundary < 0) break;
        end = boundary + 2;
        events++;
      }
      if (events === count) return new TextEncoder().encode(text.slice(0, end));
    }
  } finally {
    await reader.cancel();
  }
}

for (const contentType of ["application/json", "application/octet-stream"]) {
  it(`open SSE initial and real timeout frames match for ${contentType}`, async () => {
    const data =
      contentType === "application/json"
        ? new TextEncoder().encode('[{"x":1},2]')
        : new Uint8Array([0, 255, 10, 128]);
    // Put the timeout cursor in the next interval to avoid random collision jitter on either stack.
    const readers = Layer.effect(
      StreamsReader,
      Effect.gen(function* () {
        const reader = yield* StreamsReader;
        const clock = yield* Clock.Clock;
        const timeoutClock = {
          currentTimeNanos: clock.currentTimeNanos,
          currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
          monotonicTimeNanos: clock.monotonicTimeNanos,
          monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
          sleep: (duration: import("effect").Duration.Duration) => clock.sleep(duration),
          currentTimeMillis: Effect.succeed(fixedTime + 20_000),
          currentTimeMillisUnsafe: () => fixedTime + 20_000,
        };
        return StreamsReader.of({
          ...reader,
          readNext: (id, options) =>
            reader.readNext(id, options).pipe(Effect.provideService(Clock.Clock, timeoutClock)),
        });
      }),
    ).pipe(
      Layer.provideMerge(
        Streams.layerMemory({ longPollTimeoutMs: 5 }).pipe(Layer.provideMerge(clockLayer)),
      ),
    );
    const edge = makeEdge({}, readers);
    let right: Response | undefined;
    try {
      await edge.handler(
        new Request("http://example.test/open", {
          method: "PUT",
          headers: { "content-type": contentType },
          body: data,
        }),
      );
      right = await edge.handler(new Request("http://example.test/open?offset=-1&live=sse"));
      const expected = expiryFixtures["sse: " + contentType]?.[0];
      if (expected === undefined) throw new Error("Missing frozen SSE response");
      const newBytes = await firstEvents(right, 3);
      await assertWire(right, expected, newBytes);
      const events = new TextDecoder().decode(newBytes).split("\n\n");
      expect(events[0]).toContain("event: data");
      expect(events[1]).toContain('"streamCursor":"2578400"');
      expect(events[2]).toContain('"streamCursor":"2578401"');
    } finally {
      if (right?.body && !right.body.locked) await right.body.cancel();
      await edge.dispose();
    }
  });
}

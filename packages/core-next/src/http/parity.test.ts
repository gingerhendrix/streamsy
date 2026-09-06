// oxlint-disable effecttsgo/global-date -- The frozen legacy clock contract explicitly requires a Date-returning function.
// oxlint-disable effecttsgo/async-function -- These tests exercise the named Web conversion boundary and consume actual response bytes.
import { expect, it } from "bun:test";
import { Clock, Effect, Layer } from "effect";
import { StreamProtocol } from "../../../core/src/protocol.ts";
import { createHttpHandler } from "../../../core/src/http.ts";
import { createMemoryStorageAdapter } from "../../../core/src/storage/memory/adapter.ts";
import { Streams } from "../index.ts";
import { makeEdge } from "./edge.ts";

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
  it(`old/new ${visibility} status, status text, all headers and body bytes agree`, async () => {
    const old = createHttpHandler({
      protocol: new StreamProtocol({
        storage: { adapter: createMemoryStorageAdapter() },
        longPollTimeoutMs: 5,
        clock: { now: () => fixedTime, date: (value) => new Date(value ?? fixedTime) },
      }),
      pathPrefix: "/api.v1",
      cacheVisibility: visibility,
      maxMessageSize: 64,
    });
    const layer = Streams.layerMemory({ longPollTimeoutMs: 5 }).pipe(
      Layer.provideMerge(clockLayer),
    );
    const edge = makeEdge(
      { pathPrefix: "/api.v1", cacheVisibility: visibility, maxMessageSize: 64 },
      layer,
    );
    const compare = async (path: string, init?: RequestInit) => {
      const url = `http://example.test${path}`;
      const left = await old.fetch(new Request(url, init));
      const right = await edge.handler(new Request(url, init));
      expect(right.status).toBe(left.status);
      expect(right.statusText).toBe(left.statusText);
      expect(Object.fromEntries(right.headers)).toEqual(Object.fromEntries(left.headers));
      expect(new Uint8Array(await right.arrayBuffer())).toEqual(
        new Uint8Array(await left.arrayBuffer()),
      );
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
      await compare("/api.v1/s?offset=-1&batch_size=1");
      await compare("/api.v1/s?offset=now");
      await compare("/api.v1/s?offset=bad");
      for (const batch of ["0", "-1", "1.5", "10001", "NaN", "Infinity"])
        await compare(`/api.v1/s?batch_size=${batch}`);
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
      await compare("/api.v1/s?offset=-1&live=long-poll");
      await compare("/api.v1/s?offset=-1&live=long-poll&batch_size=1");
      // Fixed wall time makes timeout cursor bytes reproducible; timers remain live.
      await compare("/api.v1/s?offset=now&live=long-poll");
      await compare("/api.v1/s", { method: "POST", headers: { "stream-closed": "true" } });
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
    } finally {
      await edge.dispose();
    }
  });
}

// oxlint-disable effecttsgo/async-function -- Web boundary tests use requests and response body promises.
import { expect, it } from "vitest";
import { Effect, Fiber, Layer } from "effect";
import { HttpRouter } from "effect/http";
import { MemoryStorage } from "../../src/index.ts";
import { routes as protocolRoutes } from "../../src/protocol/index.ts";
import {
  ShutdownSignal,
  makeShutdownSignal,
  type ShutdownSignalService,
} from "../../src/http/shutdown-signal.ts";

const decoder = new TextDecoder();

/** Routes over memory storage with a signal the test can drain. */
const drainableEdge = () => {
  const signal = Effect.runSync(makeShutdownSignal);

  const edge = HttpRouter.toWebHandler(
    protocolRoutes({}).pipe(
      Layer.provideMerge(MemoryStorage.layerProtocol({ longPollTimeoutMs: 30_000 })),
      Layer.provideMerge(Layer.succeed(ShutdownSignal, signal)),
    ),
    { disableLogger: true },
  );

  return { edge, signal };
};

/** Wait until `n` live reads are running, so the drain has something to end. */
const liveReads = async (signal: ShutdownSignalService, n: number) => {
  const deadline = performance.now() + 2000;

  while ((await Effect.runPromise(signal.active)) !== n) {
    if (performance.now() > deadline) throw new Error(`Expected ${n} live reads`);
    await Effect.runPromise(Effect.sleep("5 millis"));
  }
};

const seed = async (edge: ReturnType<typeof drainableEdge>["edge"]) => {
  const created = await edge.handler(
    new Request("http://x/s", {
      method: "PUT",
      headers: { "content-type": "text/plain" },
      body: "seed",
    }),
  );

  const tail = created.headers.get("stream-next-offset");

  if (!tail) throw new Error("Expected current tail");

  return tail;
};

it("a drain answers a parked long poll with a 204 that looks like a timeout", async () => {
  const { edge, signal } = drainableEdge();

  try {
    const tail = await seed(edge);

    const pending = edge.handler(
      new Request(`http://x/s?offset=${tail}&live=long-poll&cursor=1000`),
    );

    await liveReads(signal, 1);
    await Effect.runPromise(signal.drain(1000));
    const response = await pending;
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-next-offset")).toBe(tail);
    expect(response.headers.get("stream-up-to-date")).toBe("true");
    expect(response.headers.get("stream-cursor")).toMatch(/^\d+$/);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(await Effect.runPromise(signal.active)).toBe(0);
  } finally {
    await edge.dispose();
  }
});

it("a drain answers offset=now with the current tail", async () => {
  const { edge, signal } = drainableEdge();

  try {
    const tail = await seed(edge);
    const pending = edge.handler(new Request("http://x/s?offset=now&live=long-poll"));
    await liveReads(signal, 1);
    await Effect.runPromise(signal.drain(1000));
    const response = await pending;
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-next-offset")).toBe(tail);
  } finally {
    await edge.dispose();
  }
});

it("a drain ends an open SSE body after a whole batch and its control event", async () => {
  const { edge, signal } = drainableEdge();

  try {
    await seed(edge);
    const response = await edge.handler(new Request("http://x/s?offset=-1&live=sse"));
    expect(response.status).toBe(200);

    if (!response.body) throw new Error("Expected SSE body");
    const reader = response.body.getReader();
    const first = await reader.read();
    expect(decoder.decode(first.value)).toContain("event: control");
    await liveReads(signal, 1);
    await Effect.runPromise(signal.drain(1000));
    let text = decoder.decode(first.value);

    for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read())
      text += decoder.decode(chunk.value);
    expect(await reader.closed).toBeUndefined();
    expect(text).toMatch(/event: control\ndata:\{[^\n]*\}\n\n$/);
    expect(await Effect.runPromise(signal.active)).toBe(0);
  } finally {
    await edge.dispose();
  }
});

it("a long poll that arrives after the drain started gets the same 204 at once", async () => {
  const { edge, signal } = drainableEdge();

  try {
    const tail = await seed(edge);
    await Effect.runPromise(signal.drain(1000));
    const startedAt = performance.now();
    const response = await edge.handler(new Request(`http://x/s?offset=${tail}&live=long-poll`));
    expect(performance.now() - startedAt).toBeLessThan(1000);
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-next-offset")).toBe(tail);
    expect(response.headers.get("stream-cursor")).toMatch(/^\d+$/);
  } finally {
    await edge.dispose();
  }
});

it("drain stops waiting at its timeout when a live read does not end", async () => {
  const signal = Effect.runSync(makeShutdownSignal);
  const stuck = signal.track(Effect.never);
  const fiber = Effect.runFork(stuck);

  try {
    await liveReads(signal, 1);
    const startedAt = performance.now();
    await Effect.runPromise(signal.drain(100));
    const elapsed = performance.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(90);
    expect(elapsed).toBeLessThan(1000);
  } finally {
    await Effect.runPromise(Fiber.interrupt(fiber));
  }
});

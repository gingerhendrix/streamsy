// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real HTTP host lifecycle test.
import { describe, expect, it } from "bun:test";
import { fileURLToPath } from "node:url";
import { Cause, Context, Deferred, Duration, Effect, Exit, Layer, Schema, Scope } from "effect";
import { HttpRouter, HttpServer } from "effect/http";
import { Streamsy, MemoryStorage } from "@streamsy/core";
import { ShutdownSignal } from "@streamsy/core/http";
import {
  Protocol,
  routes as protocolRoutes,
  StreamsReader,
  StreamsWriter,
} from "@streamsy/core/protocol";
import { Serve } from "../src/index.ts";
import { memoryLayer, parkableLayer } from "./support/parkable.ts";
import { listener } from "./support/runtime.ts";
import { testHost, type TestHostOptions } from "./support/scoped-host.ts";

const drainingHost = async (layer: TestHostOptions["layer"]) => {
  const host = await Effect.runPromise(
    testHost({ layer, port: 0, drain: { drainTimeout: Duration.seconds(2) } }),
  );
  const created = await fetch(new URL("s", host.url), {
    method: "PUT",
    headers: { "content-type": "text/plain" },
    body: "seed",
  });
  const tail = created.headers.get("stream-next-offset");
  if (!tail) throw new Error("Expected current tail");
  return { host, tail };
};

it("a draining host answers a parked long poll with 204, offset and cursor", async () => {
  const readStarted = Deferred.makeUnsafe<void>();
  const hold = Deferred.makeUnsafe<void>();
  const { host, tail } = await drainingHost(parkableLayer(readStarted, hold));
  try {
    const url = new URL("s", host.url);
    url.search = `offset=${tail}&live=long-poll`;
    const pending = fetch(url);
    await Effect.runPromise(Deferred.await(readStarted).pipe(Effect.timeout(2000)));
    const startedAt = performance.now();
    await Effect.runPromise(host.stop);
    expect(performance.now() - startedAt).toBeLessThan(1000);
    const response = await pending;
    expect(response.status).toBe(204);
    expect(response.headers.get("stream-next-offset")).toBe(tail);
    expect(response.headers.get("stream-up-to-date")).toBe("true");
    expect(response.headers.get("stream-cursor")).toMatch(/^\d+$/);
  } finally {
    Deferred.doneUnsafe(hold, Exit.void);
    await Effect.runPromise(host.stop);
  }
});

it("a draining host ends an open SSE body cleanly after a control event", async () => {
  const { host } = await drainingHost(memoryLayer);
  try {
    const url = new URL("s", host.url);
    url.search = "offset=-1&live=sse";
    const response = await fetch(url);
    if (!response.body) throw new Error("Expected SSE response body");
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let text = decoder.decode((await reader.read()).value);
    expect(text).toContain("event: control");
    await Effect.runPromise(host.stop);
    for (let chunk = await reader.read(); !chunk.done; chunk = await reader.read())
      text += decoder.decode(chunk.value);
    expect(await reader.closed).toBeUndefined();
    expect(text).toMatch(/event: control\ndata:\{[^\n]*"streamNextOffset"[^\n]*\}\n\n$/);
  } finally {
    await Effect.runPromise(host.stop);
  }
});

it("without a drain a parked long poll does not get the drain 204", async () => {
  const readStarted = Deferred.makeUnsafe<void>();
  const hold = Deferred.makeUnsafe<void>();
  const host = await Effect.runPromise(
    testHost({ layer: parkableLayer(readStarted, hold), port: 0 }),
  );
  try {
    const created = await fetch(new URL("s", host.url), {
      method: "PUT",
      headers: { "content-type": "text/plain" },
      body: "seed",
    });
    const url = new URL("s", host.url);
    url.search = `offset=${created.headers.get("stream-next-offset")}&live=long-poll`;
    const outcome = fetch(url).then(
      (response) => response.status,
      () => "error",
    );
    await Effect.runPromise(Deferred.await(readStarted).pipe(Effect.timeout(2000)));
    await Effect.runPromise(host.stop);
    // Today's abrupt shutdown: an empty 503 or a reset connection.
    expect(await outcome).not.toBe(204);
  } finally {
    Deferred.doneUnsafe(hold, Exit.void);
    await Effect.runPromise(host.stop);
  }
});

it("a client abort of a long poll on a Serve.mount route interrupts the read", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finished = yield* Deferred.make<boolean>();
      const protocol = Layer.effectContext(
        Effect.gen(function* () {
          const reader = yield* StreamsReader;
          const writer = yield* StreamsWriter;
          return Context.make(StreamsWriter, writer).pipe(
            Context.add(
              StreamsReader,
              StreamsReader.of({
                ...reader,
                readNext: (id, options) =>
                  Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(reader.readNext(id, options)),
                    Effect.onExit((exit) =>
                      Deferred.succeed(
                        finished,
                        Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause),
                      ),
                    ),
                  ),
              }),
            ),
          );
        }),
      ).pipe(Layer.provide(MemoryStorage.layerProtocol({ longPollTimeoutMs: 30_000 })));
      class Events extends Streamsy.json<Events>()(
        "serve/serve-layer.test/Events",
        "events/:seat",
        {
          params: { seat: Schema.String },
          schema: Schema.String,
        },
      ) {}
      const context = yield* Layer.build(
        Serve.layer(Layer.mergeAll(protocolRoutes(), Serve.mount(Events, "/feed/:seat")), {
          disableLogger: true,
          disableListenLog: true,
        }).pipe(
          Layer.provide(
            Protocol.bind(protocol, Streamsy.group(Events)).pipe(Layer.provideMerge(protocol)),
          ),
          Layer.provideMerge(listener({ port: 0 })),
        ),
      );
      const base = HttpServer.formatAddress(Context.get(context, HttpServer.HttpServer).address);
      const created = yield* Effect.promise(() =>
        fetch(`${base}/events/a`, {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: '"seed"',
        }),
      );
      expect(created.status).toBe(201);
      const tail = created.headers.get("stream-next-offset");
      expect(tail).not.toBeNull();
      const abort = new AbortController();
      yield* Effect.addFinalizer(() => Effect.sync(() => abort.abort()));
      const pending = fetch(`${base}/feed/a?offset=${tail}&live=long-poll`, {
        signal: abort.signal,
      }).then(
        () => undefined,
        () => undefined,
      );
      yield* Deferred.await(started).pipe(Effect.timeout(2000));
      abort.abort();
      yield* Effect.promise(() => pending);
      expect(yield* Deferred.await(finished).pipe(Effect.timeout(2000))).toBe(true);
    }).pipe(Effect.scoped),
  ));

/**
 * Serve one route that counts as a live read and never ends, so the drain can
 * only stop at its bound. `close` reports how long the host took to stop.
 */
const stuckReadHost = (drainTimeout: Duration.Input | undefined) =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const Stuck = HttpRouter.add(
      "GET",
      "/stuck",
      Effect.gen(function* () {
        const signal = yield* ShutdownSignal;
        return yield* signal.track(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
        );
      }),
    );
    const scope = yield* Scope.make();
    const context = yield* Layer.buildWithScope(
      Serve.layer(Stuck, { disableLogger: true, disableListenLog: true, drainTimeout }).pipe(
        Layer.provideMerge(listener({ port: 0 })),
      ),
      scope,
    );
    const server = Context.get(context, HttpServer.HttpServer);
    return {
      url: HttpServer.formatAddress(server.address),
      started: Deferred.await(started),
      close: Scope.close(scope, Exit.void).pipe(
        Effect.timed,
        Effect.map(([elapsed]) => Duration.toMillis(elapsed)),
      ),
    };
  });

describe("the drain bound", () => {
  const stopStuck = async (drainTimeout: Duration.Input | undefined) => {
    const host = await Effect.runPromise(stuckReadHost(drainTimeout));
    const outcome = fetch(new URL("stuck", host.url)).then(
      (response) => response.status,
      () => "error",
    );
    await Effect.runPromise(host.started.pipe(Effect.timeout(2000)));
    const elapsed = await Effect.runPromise(host.close);
    // The serve layer interrupts the stuck handler after the drain gives up.
    expect(await outcome).not.toBe(200);
    return elapsed;
  };

  it("drainTimeout bounds the wait for a live read that does not end", async () => {
    const elapsed = await stopStuck(Duration.millis(300));
    expect(elapsed).toBeGreaterThanOrEqual(250);
    expect(elapsed).toBeLessThan(1500);
  });
});

const exitCode = (exit: Exit.Exit<unknown, unknown>) => {
  let result: number | undefined;
  Serve.teardown(exit, (value) => {
    result = value;
  });
  return result;
};

describe("Serve.teardown", () => {
  it("maps an interrupt-only exit to 0", () => {
    expect(exitCode(Exit.interrupt(1))).toBe(0);
  });

  it("maps a failure to 1", () => {
    expect(exitCode(Exit.fail("boom"))).toBe(1);
    expect(exitCode(Exit.die(new Error("boom")))).toBe(1);
  });

  it("maps a success to 0", () => {
    expect(exitCode(Exit.succeed("done"))).toBe(0);
  });
});

/** A port that was free a moment ago. */
const freePort = () => {
  const probe = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = probe.port;
  void probe.stop(true);
  if (port === undefined) throw new Error("Expected a bound port");
  return port;
};

const waitFor = async (check: () => Promise<boolean>, what: string) => {
  const deadline = performance.now() + 10_000;
  while (performance.now() < deadline) {
    if (await check().catch(() => false)) return;
    await Bun.sleep(25);
  }
  throw new Error(`Timed out waiting for ${what}`);
};

describe("a launched Serve.layer host", () => {
  const main = fileURLToPath(new URL("./support/serve-layer-main.ts", import.meta.url));

  const spawn = (port: number) =>
    Bun.spawn(["bun", main], {
      env: { ...process.env, PORT: String(port) },
      stdout: "pipe",
      stderr: "pipe",
    });

  it("drains a parked long poll on SIGTERM and exits 0 with empty stderr", async () => {
    const port = freePort();
    const child = spawn(port);
    try {
      const base = `http://127.0.0.1:${port}`;
      const active = async () => Number(await (await fetch(`${base}/active`)).text());
      await waitFor(async () => (await fetch(`${base}/active`)).ok, "the server to answer");
      const created = await fetch(`${base}/streams/s`, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "seed",
      });
      expect(created.status).toBe(201);
      const tail = created.headers.get("stream-next-offset");
      if (!tail) throw new Error("Expected current tail");
      const poll = fetch(`${base}/streams/s?offset=${tail}&live=long-poll`);
      await waitFor(async () => (await active()) === 1, "the long poll to park");
      child.kill("SIGTERM");
      const response = await poll;
      expect(response.status).toBe(204);
      expect(response.headers.get("stream-next-offset")).toBe(tail);
      expect(await child.exited).toBe(0);
      expect(await new Response(child.stderr).text()).toBe("");
    } finally {
      child.kill("SIGKILL");
    }
  }, 20_000);

  it("exits 1 when the port is already in use", async () => {
    const taken = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
    try {
      const port = taken.port;
      if (port === undefined) throw new Error("Expected a bound port");
      const child = spawn(port);
      try {
        expect(await child.exited).toBe(1);
      } finally {
        child.kill("SIGKILL");
      }
    } finally {
      await taken.stop(true);
    }
  }, 20_000);
});

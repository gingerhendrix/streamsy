// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real Bun/Web executable edge lifecycle test.
import { expect, it } from "bun:test";
import { Cause, Context, Deferred, Effect, Exit, Layer, Stream } from "effect";
import {
  Memory,
  Protocol,
  Storage,
  Streams,
  StreamsReader,
  StreamsWriter,
} from "@streamsy/core-next";
import { serve } from "./bun.ts";

for (const shutdown of ["abort", "stop"] as const) {
  it(`${shutdown} releases the SSE producing read and changes subscription; stop permits same-port rebind`, async () => {
    const subscribed = Deferred.makeUnsafe<void>();
    const released = Deferred.makeUnsafe<void>();
    const finished = Deferred.makeUnsafe<void>();
    let subscribers = 0;
    let activeReads = 0;
    let interrupted = false;
    let ownerClosed = false;
    const observedStorage = Layer.effect(
      Storage,
      Effect.gen(function* () {
        const storage = yield* Storage;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            ownerClosed = true;
          }),
        );
        return Storage.of({
          ...storage,
          changes: (id) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Effect.acquireRelease(
                  Effect.sync(() => {
                    subscribers++;
                  }),
                  () =>
                    Effect.sync(() => {
                      subscribers--;
                    }).pipe(Effect.andThen(Deferred.succeed(released, undefined))),
                );
                return storage
                  .changes(id)
                  .pipe(Stream.tap(() => Deferred.succeed(subscribed, undefined)));
              }),
            ),
        });
      }),
    ).pipe(Layer.provide(Memory.layer()));
    const observedProtocol = Layer.effectContext(
      Effect.gen(function* () {
        const reader = yield* StreamsReader;
        const writer = yield* StreamsWriter;
        return Context.make(StreamsWriter, writer).pipe(
          Context.add(
            StreamsReader,
            StreamsReader.of({
              ...reader,
              readNext: (id, options) =>
                Effect.suspend(() => {
                  activeReads++;
                  return reader.readNext(id, options).pipe(
                    Effect.onExit((exit) =>
                      Effect.sync(() => {
                        activeReads--;
                        interrupted = Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause);
                      }).pipe(Effect.andThen(Deferred.succeed(finished, undefined))),
                    ),
                  );
                }),
            }),
          ),
        );
      }),
    ).pipe(
      Layer.provide(
        Protocol.layer({ longPollTimeoutMs: 30_000 }).pipe(Layer.provide(observedStorage)),
      ),
    );
    const host = await serve({ layer: observedProtocol, port: 0 });
    const abort = new AbortController();
    try {
      const url = new URL("s", host.url);
      expect(
        (await fetch(url, { method: "PUT", headers: { "content-type": "text/plain" } })).status,
      ).toBe(201);
      url.search = "offset=-1&live=sse";
      const response = await fetch(url, { signal: abort.signal });
      if (!response.body) throw new Error("Expected SSE response body");
      const reader = response.body.getReader();
      const first = await reader.read();
      expect(new TextDecoder().decode(first.value)).toContain("event: control");
      await Effect.runPromise(Deferred.await(subscribed).pipe(Effect.timeout(2000)));
      expect(subscribers).toBe(1);
      expect(activeReads).toBe(1);
      const pending = reader.read().catch(() => ({ done: true }));
      if (shutdown === "abort") abort.abort();
      else await host.stop();
      await pending;
      await Effect.runPromise(Deferred.await(released).pipe(Effect.timeout(2000)));
      await Effect.runPromise(Deferred.await(finished).pipe(Effect.timeout(2000)));
      expect(subscribers).toBe(0);
      expect(activeReads).toBe(0);
      expect(interrupted).toBe(true);
      expect(ownerClosed).toBe(shutdown === "stop");
      await host.stop();
      expect(ownerClosed).toBe(true);
      const rebound = await serve({ layer: Streams.layerMemory(), port: host.port });
      try {
        expect(rebound.port).toBe(host.port);
        expect((await fetch(new URL("s", rebound.url), { method: "PUT" })).status).toBe(201);
      } finally {
        await rebound.stop();
      }
      await host.stop();
    } finally {
      abort.abort();
      await host.stop();
    }
  });
}

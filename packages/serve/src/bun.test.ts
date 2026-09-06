// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This is the real Bun/Web executable edge lifecycle test.
import { expect, it, spyOn } from "bun:test";
import { Cause, Context, Deferred, Effect, Exit, Layer, Stream } from "effect";
import { Memory, Protocol, Storage, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import { serve } from "./bun.ts";

for (const shutdown of ["abort", "stop", "long-poll-abort"] as const) {
  it(`${shutdown} releases the live producing read and changes subscription; stop permits same-port rebind`, async () => {
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
    const errors = spyOn(console, "error");
    const warnings = spyOn(console, "warn");
    const logs = spyOn(console, "log");
    const stderr = spyOn(process.stderr, "write");
    const host = await serve({ layer: observedProtocol, port: 0 });
    const abort = new AbortController();
    try {
      const url = new URL("s", host.url);
      const created = await fetch(url, {
        method: "PUT",
        headers: { "content-type": "text/plain" },
        body: "seed",
      });
      expect(created.status).toBe(201);
      const tail = created.headers.get("stream-next-offset");
      if (!tail) throw new Error("Expected current tail");
      url.search = `offset=${tail}&live=${shutdown === "long-poll-abort" ? "long-poll" : "sse"}`;
      let pending: Promise<unknown>;
      if (shutdown === "long-poll-abort") {
        // Start the request without awaiting headers: it must remain parked for 30 seconds unless interrupted.
        pending = fetch(url, { signal: abort.signal }).catch((error) => {
          if (error instanceof Error) return error;
          throw error;
        });
      } else {
        const response = await fetch(url, { signal: abort.signal });
        if (!response.body) throw new Error("Expected SSE response body");
        const reader = response.body.getReader();
        const first = await reader.read();
        expect(new TextDecoder().decode(first.value)).toContain("event: control");
        pending = reader.read().catch(() => ({ done: true }));
      }
      await Effect.runPromise(
        Deferred.await(subscribed).pipe(
          Effect.timeout(2000),
          Effect.catch(() =>
            Effect.die(
              new Error(
                "subscription not established; subscribers=" +
                  subscribers +
                  ", activeReads=" +
                  activeReads +
                  ", interrupted=" +
                  interrupted,
              ),
            ),
          ),
        ),
      );
      expect(subscribers).toBe(1);
      expect(activeReads).toBe(1);
      if (shutdown === "stop") await host.stop();
      else abort.abort();
      const completed = await pending;
      if (shutdown === "long-poll-abort") expect(completed).toMatchObject({ name: "AbortError" });
      await Effect.runPromise(
        Deferred.await(released).pipe(
          Effect.timeout(2000),
          Effect.catch(() =>
            Effect.die(
              new Error(
                "changes subscription not released after client abort; subscribers=" +
                  subscribers +
                  ", activeReads=" +
                  activeReads +
                  ", interrupted=" +
                  interrupted,
              ),
            ),
          ),
        ),
      );
      await Effect.runPromise(
        Deferred.await(finished).pipe(
          Effect.timeout(2000),
          Effect.catch(() =>
            Effect.die(
              new Error(
                "readNext fiber not interrupted after client abort; subscribers=" +
                  subscribers +
                  ", activeReads=" +
                  activeReads +
                  ", interrupted=" +
                  interrupted,
              ),
            ),
          ),
        ),
      );
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
      try {
        await host.stop();
        expect(errors).not.toHaveBeenCalled();
        expect(warnings).not.toHaveBeenCalled();
        expect(logs).not.toHaveBeenCalled();
        expect(stderr).not.toHaveBeenCalled();
      } finally {
        errors.mockRestore();
        warnings.mockRestore();
        logs.mockRestore();
        stderr.mockRestore();
      }
    }
  });
}

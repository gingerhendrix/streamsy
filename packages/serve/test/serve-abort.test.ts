// oxlint-disable effecttsgo/async-function, effecttsgo/global-fetch -- This test owns a real Bun listener and its Web client boundary.
import { expect, test } from "bun:test";
import { Cause, Context, Deferred, Effect, Exit, Layer, Schema } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { NetAddress } from "effect/unstable/net";
import { Http, StreamRoute, Streams, StreamsReader, StreamsWriter } from "@streamsy/core";
import { Serve } from "../src/index.ts";
import { listener } from "../src/bun.ts";

// Adapted from the B review's bun-abort.ts; the uninterruptible control proves
// that completing the client request alone does not imply release of the read.
test.each([false, true])(
  "Serve long-poll abort releases the read (uninterruptible control: %s)",
  async (control) => {
    await Effect.runPromise(
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
        ).pipe(Layer.provide(Streams.layerMemory({ longPollTimeoutMs: 500 })));
        const family = StreamRoute.json("events/:seat", {
          params: { seat: Schema.String },
          schema: Schema.String,
        });
        const route = control
          ? HttpRouter.add("GET", "/feed/:seat", Http.read(family.ref({ seat: "a" }).id), {
              uninterruptible: true,
            })
          : Serve.stream(family, "/feed/:seat");
        const context = yield* Layer.build(
          HttpRouter.serve(Layer.mergeAll(Http.routes(), route), {
            disableLogger: true,
            disableListenLog: true,
          }).pipe(Layer.provide(protocol), Layer.provideMerge(listener({ port: 0 }))),
        );
        const server = Context.get(context, HttpServer.HttpServer);
        if (!NetAddress.isInetAddress(server.address))
          return yield* Effect.die(new Error("Expected TCP listener"));
        const base = `http://127.0.0.1:${server.address.port}`;
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
        const interrupted = yield* Deferred.await(finished).pipe(Effect.timeout(2000));
        expect(interrupted).toBe(!control);
        return undefined;
      }).pipe(Effect.scoped),
    );
  },
);

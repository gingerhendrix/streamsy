/* oxlint-disable effecttsgo/async-function, effecttsgo/abort-controller-in-effect -- Own native AI SDK controllers and Promise tasks at the scoped framework adapter boundary. */
import { Http, Streamsy } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";
import { toDurableStreamResponse } from "@durable-streams/aisdk-transport";
import { streamText } from "ai";
import { Array, DateTime, Effect, Layer, Predicate, Schema, Semaphore } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { ChatRequest, Id, StopRequest, responsePath, type Stopped } from "./contract.ts";
import { scriptedModel } from "./model.ts";

class Responses extends Streamsy.json<Responses>()("AI SDK/Responses", "responses/:id", {
  params: { id: Id },
  schema: Schema.Json,
}) {}

const group = Streamsy.group(Responses);

const json = HttpServerResponse.jsonUnsafe;

interface Running {
  controller: AbortController;
  task: Promise<unknown>;
  finished: boolean;
  stopped?: Stopped;
}

const body = <A, I>(schema: Schema.Codec<A, I>) =>
  Effect.gen(function* () {
    return yield* (yield* HttpServerRequest.HttpServerRequest).json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    );
  });

const routeId = Effect.gen(function* () {
  return yield* Schema.decodeUnknownEffect(Id)((yield* HttpRouter.RouteContext).params["id"]);
});

const badRequest = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.catchIf(Predicate.or(Schema.isSchemaError, Predicate.isTagged("HttpServerError")), () =>
      Effect.succeed(json({ error: "Invalid request" }, { status: 400 })),
    ),
  );

const nativeRuns = (paceMs: number) => ({
  ...scriptedModel(paceMs),
  running: new Map<string, Running>(),
});

const routes = (options: { port: number; owner: ReturnType<typeof nativeRuns> }) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const responses = yield* Responses;
      // Own only in-flight runs. Recorded output and closure live in SQLite.
      const { model, stats, running } = options.owner;
      const base = `http://127.0.0.1:${options.port}`;
      const stopGate = yield* Semaphore.make(1);

      const head = (id: string) =>
        responses.head({ id }).pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

      const probe = (replay: boolean) =>
        badRequest(
          Effect.gen(function* () {
            const id = yield* routeId;
            const stored = yield* head(id);

            if (!stored || (!replay && (stored.closed || !running.has(id))))
              return HttpServerResponse.empty({ status: 204 });

            return json(
              { streamUrl: responsePath(id) },
              { headers: { location: responsePath(id), "cache-control": "no-store" } },
            );
          }),
        );

      const start = badRequest(
        Effect.gen(function* () {
          const input = yield* body(ChatRequest);

          const prompt = Array.join(
            Array.flatMap(input.messages.at(-1)?.parts ?? [], (part) =>
              part.type === "text" && part.text ? [part.text] : [],
            ),
            "",
          );

          if (!prompt.trim() || prompt.length > 2000)
            return json({ error: "A text prompt is required" }, { status: 400 });

          // Synchronous reservation precedes the first await so concurrent POSTs cannot call twice.
          if (running.has(input.id))
            return json({ error: "Response already exists" }, { status: 409 });

          const run: Running = {
            controller: new AbortController(),
            task: Promise.resolve(),
            finished: false,
          };

          running.set(input.id, run);
          const stored = yield* head(input.id);

          if (stored) {
            running.delete(input.id);

            return json({ error: "Response already exists" }, { status: 409 });
          }

          const source = streamText({
            model,
            prompt,
            abortSignal: run.controller.signal,
          }).toUIMessageStream({
            generateMessageId: () => `answer-${input.id}`,
            messageMetadata: ({ part }) => {
              if (part.type === "start") return { prompt };

              if (part.type === "finish" || part.type === "error") run.finished = true;

              if (part.type === "abort" && run.stopped) return { stopped: run.stopped };

              return undefined;
            },
          });

          const response = yield* Effect.tryPromise(() =>
            toDurableStreamResponse({
              source,
              stream: {
                writeUrl: `${base}/streams/responses/${input.id}`,
                readUrl: responsePath(input.id),
              },
              waitUntil: (task) => {
                run.task = task.finally(() => {
                  if (running.get(input.id) === run) running.delete(input.id);
                });
              },
            }),
          ).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                run.controller.abort();
                running.delete(input.id);
              }),
            ),
          );

          return HttpServerResponse.fromWeb(response);
        }),
      ).pipe(Effect.uninterruptible);

      const stop = stopGate
        .withPermit(
          badRequest(
            Effect.gen(function* () {
              const id = yield* routeId;
              const input = yield* body(StopRequest);
              const stored = yield* head(id);

              if (!stored) return json({ error: "Response not found" }, { status: 404 });
              const run = running.get(id);

              if (stored.closed || run?.finished) {
                if (run) yield* Effect.promise(() => run.task);

                return json({ alreadyClosed: true });
              }

              if (!run)
                return json(
                  {
                    error:
                      "Provider is unavailable after host restart; recorded output can still be replayed",
                  },
                  { status: 409 },
                );
              const alreadyClosed = run.stopped !== undefined;

              if (!alreadyClosed) {
                run.stopped = { ...input, at: DateTime.formatIso(yield* DateTime.now) };
                run.controller.abort();
              }

              // Await the provider's observed abort, queued appends, and durable close before replying.
              yield* Effect.promise(() => run.task);

              return json({ alreadyClosed });
            }),
          ),
        )
        .pipe(Effect.uninterruptible);

      return Layer.mergeAll(
        HttpRouter.add("POST", "/api/chat", start),
        HttpRouter.add("POST", "/api/chat/:id/stop", stop),
        HttpRouter.add("GET", "/api/chat/:id/stream", probe(false)),
        // Explicit replay probe: the unchanged transport always replays from -1, even after closure.
        HttpRouter.add("GET", "/api/chat/:id/replay", probe(true)),
        HttpRouter.add(
          "GET",
          "/api/chat/:id/status",
          badRequest(
            Effect.gen(function* () {
              const stored = yield* head(yield* routeId);

              return json({ exists: stored !== undefined, closed: stored?.closed ?? false });
            }),
          ),
        ),
        HttpRouter.add(
          "GET",
          "/debug/model-calls",
          Effect.sync(() => json(stats)),
        ),
        Serve.mount(Responses, "/responses/:id"),
        Http.routes(group, { prefix: "/streams", access: "full" }),
      );
    }),
  );

/** Each instance owns a SQLite Streamsy host and native framework generation tasks. */
export const server = (options: { database: string; port: number; paceMs?: number }) =>
  Layer.unwrap(
    Effect.sync(() => {
      const owner = nativeRuns(options.paceMs ?? 250);

      const http = Serve.layer(routes({ port: options.port, owner })).pipe(
        Layer.provide(listener({ port: options.port, hostname: "127.0.0.1" })),
        Layer.provide(BunStorage.layer(group, { client: { filename: options.database } })),
      );

      // Acquire after HTTP so this finalizer runs while the published writer's routes still exist.
      const shutdown = Layer.effectDiscard(
        Effect.addFinalizer(() =>
          Effect.gen(function* () {
            const at = DateTime.formatIso(yield* DateTime.now);
            yield* Effect.promise(async () => {
              const tasks = [...owner.running.values()];

              for (const run of tasks) {
                if (!run.finished) {
                  run.stopped ??= { by: "server", reason: "Host shutting down", at };
                  run.controller.abort();
                }
              }

              await Promise.allSettled(Array.map(tasks, (run) => run.task));
            });
          }),
        ),
      );

      return shutdown.pipe(Layer.provide(http));
    }),
  );

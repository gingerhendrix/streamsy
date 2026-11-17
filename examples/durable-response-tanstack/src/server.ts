/* oxlint-disable effecttsgo/async-function, effecttsgo/abort-controller-in-effect -- Scope owns native TanStack controllers and Promise writer tasks at the framework adapter boundary. */
import { Http, Streamsy, ZERO_OFFSET } from "@streamsy/core";
import { Serve } from "@streamsy/serve";
import { listener } from "@streamsy/serve/bun";
import { BunStorage } from "@streamsy/storage/bun";
import { toDurableStreamResponse } from "@durable-streams/tanstack-ai-transport";
import { RUN_CANCEL_REASON } from "@tanstack/ai";
import { Array, DateTime, Effect, Layer, Predicate, Schema, Semaphore, Scope } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { ChatRequest, Id, StopRequest, responsePath } from "./contract.ts";
import { scriptedModel, type Running } from "./model.ts";

class Responses extends Streamsy.json<Responses>()("TanStack/Responses", "responses/:id", {
  params: { id: Id },
  schema: Schema.Json,
}) {}

const group = Streamsy.group(Responses);

const json = HttpServerResponse.jsonUnsafe;

const routeId = Effect.gen(function* () {
  return yield* Schema.decodeUnknownEffect(Id)((yield* HttpRouter.RouteContext).params["id"]);
});

const body = <A, I>(schema: Schema.Codec<A, I>) =>
  Effect.gen(function* () {
    return yield* (yield* HttpServerRequest.HttpServerRequest).json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    );
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

const routes = (port: number, owner: ReturnType<typeof nativeRuns>) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const responses = yield* Responses;
      const scope = yield* Scope.Scope;
      const setupGate = yield* Semaphore.make(1);
      const stopGate = yield* Semaphore.make(1);

      const head = (id: string) =>
        responses.head({ id }).pipe(Effect.catchTag("StreamNotFound", () => Effect.void));

      const probe = (replay: boolean) =>
        badRequest(
          Effect.gen(function* () {
            const id = yield* routeId;
            const stored = yield* head(id);

            if (!stored || (!replay && (stored.closed || !owner.running.has(id))))
              return HttpServerResponse.empty({ status: 204 });

            return json(
              { streamUrl: responsePath(id) },
              { headers: { location: responsePath(id), "cache-control": "no-store" } },
            );
          }),
        );

      const prepare = setupGate
        .withPermit(
          badRequest(
            Effect.gen(function* () {
              const id = yield* routeId;

              if (yield* head(id))
                return json({ error: "Response already exists" }, { status: 409 });
              yield* responses.create({ id });

              return json(
                { streamUrl: responsePath(id) },
                { status: 201, headers: { location: responsePath(id) } },
              );
            }),
          ),
        )
        .pipe(Effect.uninterruptible);

      const start = setupGate
        .withPermit(
          badRequest(
            Effect.gen(function* () {
              const id = yield* routeId;
              const input = yield* body(ChatRequest);

              const prompt = Array.join(
                Array.flatMap(input.messages.at(-1)?.parts ?? [], (part) =>
                  part.type === "text" && part.content ? [part.content] : [],
                ),
                "",
              );

              if (!prompt.trim() || prompt.length > 2000)
                return json({ error: "A text prompt is required" }, { status: 400 });
              const stored = yield* head(id);

              if (!stored)
                return json({ error: "Prepare the response before sending" }, { status: 404 });

              if (owner.running.has(id) || stored.closed || stored.nextOffset !== ZERO_OFFSET)
                return json({ error: "Response already started" }, { status: 409 });

              const run: Running = {
                controller: new AbortController(),
                task: Promise.resolve(),
                decision: "running",
              };

              owner.running.set(id, run);
              // Await mode exposes writer failures to the owning task. The HTTP start itself still returns immediately.
              run.task = toDurableStreamResponse(owner.source(id, prompt, run), {
                mode: "await",
                stream: {
                  writeUrl: `http://127.0.0.1:${port}/streams/responses/${id}`,
                  readUrl: responsePath(id),
                  createIfMissing: false,
                },
              })
                .then(() => undefined)
                .finally(() => {
                  if (owner.running.get(id) === run) owner.running.delete(id);
                });
              // Supervise a detached native task without creating an unhandled rejection. Stop awaits the original task.
              yield* Effect.forkIn(scope)(
                Effect.tryPromise(() => run.task).pipe(
                  Effect.catchTag("UnknownError", (error) => Effect.logError(error)),
                ),
              );

              return json(
                { streamUrl: responsePath(id) },
                {
                  status: 201,
                  headers: { location: responsePath(id), "cache-control": "no-store" },
                },
              );
            }),
          ),
        )
        .pipe(Effect.uninterruptible);

      const stop = stopGate
        .withPermit(
          badRequest(
            Effect.gen(function* () {
              const id = yield* routeId;
              const input = yield* body(StopRequest);
              const stored = yield* head(id);

              if (!stored) return json({ error: "Response not found" }, { status: 404 });
              const at = DateTime.formatIso(yield* DateTime.now);
              const run = owner.running.get(id);

              if (stored.closed || run?.decision === "finished" || run?.decision === "error") {
                if (run) yield* Effect.tryPromise(() => run.task);

                return json({ alreadyClosed: true });
              }

              if (!run)
                return json(
                  {
                    error:
                      "No active provider: this is a prepared response or an open log left by a host crash",
                  },
                  { status: 409 },
                );
              const alreadyClosed = run.decision === "stopped";

              if (!alreadyClosed) {
                run.stopped = { ...input, at };
                run.decision = "stopped";
                run.controller.abort(RUN_CANCEL_REASON);
              }

              yield* Effect.tryPromise(() => run.task);
              const closed = yield* responses.head({ id });

              if (!closed.closed)
                return json(
                  { error: "Writer did not close the durable response" },
                  { status: 503 },
                );

              return json({ alreadyClosed });
            }),
          ),
        )
        .pipe(Effect.uninterruptible);

      return Layer.mergeAll(
        HttpRouter.add("POST", "/api/chat/:id/prepare", prepare),
        HttpRouter.add("POST", "/api/chat/:id", start),
        HttpRouter.add("POST", "/api/chat/:id/stop", stop),
        HttpRouter.add("GET", "/api/chat/:id/stream", probe(false)),
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
          Effect.sync(() => json(owner.stats())),
        ),
        Serve.mount(Responses, "/responses/:id"),
        Http.routes(group, { prefix: "/streams", access: "full" }),
      );
    }),
  );

export const server = (options: { database: string; port: number; paceMs?: number }) =>
  Layer.unwrap(
    Effect.sync(() => {
      const owner = nativeRuns(options.paceMs ?? 150);

      const http = Serve.layer(routes(options.port, owner)).pipe(
        Layer.provide(listener({ port: options.port, hostname: "127.0.0.1" })),
        Layer.provide(BunStorage.layer(group, { client: { filename: options.database } })),
      );

      // Cleanup acquires after HTTP, so native writers can append their final events before routes disappear.
      return Layer.effectDiscard(
        Effect.addFinalizer(() =>
          Effect.gen(function* () {
            const at = DateTime.formatIso(yield* DateTime.now);
            yield* Effect.promise(async () => {
              const tasks = [...owner.running.values()];

              for (const run of tasks) {
                if (run.decision === "running") {
                  run.stopped = { by: "server", reason: "Host shutting down", at };
                  run.decision = "stopped";
                  run.controller.abort(RUN_CANCEL_REASON);
                }
              }

              await Promise.allSettled(Array.map(tasks, (run) => run.task));
            });
          }),
        ),
      ).pipe(Layer.provide(http));
    }),
  );

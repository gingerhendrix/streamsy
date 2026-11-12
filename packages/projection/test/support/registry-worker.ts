/* oxlint-disable effecttsgo/async-function -- Workers and storage alarms use native promises at the host boundary. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  Request as WorkerRequest,
  Response as WorkerResponse,
} from "@cloudflare/workers-types";
import {
  Array,
  Clock,
  Context,
  Effect,
  Layer,
  Match,
  Option,
  Schedule,
  Schema,
  Stream,
} from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { StorageFault, Streamsy } from "@streamsy/core";
import { ProjectionMarks } from "@streamsy/core/internal";
import { CheckpointStore, Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { StreamsyObject, Wakeups } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";

class Source extends Streamsy.json<Source>()("Registry/WorkerdSource", "source/:id", {
  params: { id: Schema.String },
  schema: Schema.String,
}) {}

class Control extends Context.Service<
  Control,
  {
    mode: "ok" | "binding" | "storage";
    runs: number;
    readonly seen: Array<string>;
    attempts: number | null;
    readonly alarm: Effect.Effect<number | null>;
  }
>()("Registry/Control") {}

const declaration = Projection.process({
  id: "workerd-registry",
  params: { id: Schema.String },
  inputs: { source: Source },
  commit: Commit.atLeastOnce,
  process: (_batch, unit) =>
    Effect.gen(function* () {
      const control = yield* Control;
      control.runs += 1;
      control.seen.push(unit.params.id);
      const marks = yield* (yield* ProjectionMarks).read("workerd-registry", ['{"id":"a"}']);
      control.attempts = marks.get('{"id":"a"}')?.attempts ?? null;

      if (control.mode === "binding" && unit.params.id === "a")
        return yield* Effect.fail("offline");

      if (control.mode === "storage")
        return yield* new StorageFault({
          operation: "test.storage",
          message: "offline",
          retryable: true,
        });

      return undefined;
    }),
});

const layer = (state: DurableObjectState) =>
  Layer.mergeAll(
    CheckpointStore.layer,
    Projection.Registry.layer,
    Layer.succeed(Control, {
      mode: "ok",
      runs: 0,
      seen: [],
      attempts: null,
      alarm: Effect.promise(() => state.storage.getAlarm()),
    }),
  ).pipe(
    Layer.provideMerge(
      DurableObjectStorage.layer(Streamsy.group(Source), { client: { storage: state.storage } }),
    ),
  );

const app = Layer.mergeAll(
  HttpRouter.add(
    "GET",
    "/event",
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      const pull = yield* Stream.toPull(registry.events);
      yield* registry.schedule(declaration.at({ id: "a" }));

      const event = yield* Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.runHead,
        Effect.timeout("2 seconds"),
      );

      return HttpServerResponse.jsonUnsafe(Option.getOrThrow(event));
    }).pipe(Effect.orDie),
  ),
  // GET mutations intentionally skip the host's ordinary HTTP mutation reconciliation.
  HttpRouter.add(
    "GET",
    "/setup",
    Effect.gen(function* () {
      const source = yield* Source;
      yield* source.create({ id: "a" });
      yield* source.append({ id: "a" }, ["hello"]);

      return HttpServerResponse.text("ready");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/schedule",
    Effect.gen(function* () {
      yield* (yield* Projection.Registry).schedule(declaration.at({ id: "a" }));

      const status = (yield* (yield* Projection.Registry).status)[0]!;

      return HttpServerResponse.jsonUnsafe({ status, alarm: yield* (yield* Control).alarm });
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/schedule-good",
    Effect.gen(function* () {
      const source = yield* Source;
      yield* source.create({ id: "good" });
      yield* source.append({ id: "good" }, ["hello"]);
      yield* (yield* Projection.Registry).schedule(declaration.at({ id: "good" }));

      const status = (yield* (yield* Projection.Registry).status)[0]!;

      return HttpServerResponse.jsonUnsafe({ status, alarm: yield* (yield* Control).alarm });
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/retry",
    Effect.gen(function* () {
      const count = yield* (yield* Projection.Registry).retry(declaration);

      const status = (yield* (yield* Projection.Registry).status)[0]!;

      return HttpServerResponse.jsonUnsafe({ count, status, alarm: yield* (yield* Control).alarm });
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/defer",
    Effect.gen(function* () {
      yield* (yield* ProjectionMarks).defer(
        declaration.id,
        '{"id":"a"}',
        4,
        (yield* Clock.currentTimeMillis) + 60_000,
      );
      yield* (yield* Wakeups).reconcile;

      return HttpServerResponse.text("deferred");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/mode/:mode",
    Effect.gen(function* () {
      const params = yield* HttpRouter.params;
      const control = yield* Control;
      control.mode = Match.value(params.mode).pipe(
        Match.when("binding", () => "binding" as const),
        Match.when("storage", () => "storage" as const),
        Match.orElse(() => "ok" as const),
      );

      return HttpServerResponse.text(control.mode);
    }),
  ),
  HttpRouter.add(
    "GET",
    "/status",
    Effect.gen(function* () {
      const control = yield* Control;
      const registry = yield* Projection.Registry;
      const status = (yield* registry.status)[0]!;
      const marks = yield* (yield* ProjectionMarks).read(declaration.id, ['{"id":"a"}']);

      return HttpServerResponse.jsonUnsafe({
        runs: control.runs,
        seen: control.seen,
        attempts: control.attempts,
        alarm: yield* control.alarm,
        status,
        mark: marks.get('{"id":"a"}'),
        host: yield* (yield* Wakeups).status,
      });
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/wait",
    Effect.gen(function* () {
      const registry = yield* Projection.Registry;
      yield* registry.schedule(declaration.at({ id: "a" }));

      const observed = yield* registry.changes.pipe(
        Stream.filter((statuses) => Option.isSome(statuses[0]!.lastPassAt)),
        Stream.runHead,
        Effect.timeoutOption("2 seconds"),
      );

      return HttpServerResponse.jsonUnsafe({ observed: Option.isSome(observed) });
    }).pipe(Effect.orDie),
  ),
);

interface Env {
  readonly DURABLE: DurableObjectNamespace;
  readonly MEMORY: DurableObjectNamespace;
  readonly EXHAUSTED: DurableObjectNamespace;
}

export class DurableRegistryObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  layer,
  app,
  wakeups: [ProjectionWakeup.attach(declaration, { retry: Schedule.spaced("1 hour") })],
}) {}

export class MemoryRegistryObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  layer,
  app,
  wakeups: [
    ProjectionWakeup.attach(declaration, { durable: false, retry: Schedule.spaced("1 hour") }),
  ],
}) {}

export class ExhaustedRegistryObject extends StreamsyObject.make<
  Env,
  Layer.Success<ReturnType<typeof layer>>
>({
  layer,
  app,
  wakeups: [ProjectionWakeup.attach(declaration, { durable: false, retry: Schedule.recurs(0) })],
}) {}

export default {
  async fetch(request: WorkerRequest, env: Env): Promise<WorkerResponse> {
    const url = new URL(request.url);
    const [, kind, name, ...path] = url.pathname.split("/");

    const ns = Match.value(kind).pipe(
      Match.when("durable", () => env.DURABLE),
      Match.when("exhausted", () => env.EXHAUSTED),
      Match.orElse(() => env.MEMORY),
    );

    url.pathname = `/${Array.join(path, "/")}`;

    return ns.get(ns.idFromName(name ?? "test")).fetch(url.toString());
  },
};

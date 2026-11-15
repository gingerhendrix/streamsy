/* oxlint-disable effecttsgo/async-function -- The worker exposes manual alarm delivery and a persisted wall clock for deterministic tests. */
import type {
  DurableObjectNamespace,
  DurableObjectState,
  Request as WorkerRequest,
  Response as WorkerResponse,
} from "@cloudflare/workers-types";
import { Clock, Context, Effect, Layer, Option, Schema } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import { Streamsy } from "@streamsy/core";
import { CheckpointStore, Commit, Projection, ProjectionWakeup } from "@streamsy/projection";
import { StreamsyObject } from "@streamsy/serve/cloudflare";
import { DurableObjectStorage } from "@streamsy/storage/durable-object";

class Input extends Streamsy.json<Input>()("TimerWorker/Input", "input", {
  params: {},
  schema: Schema.Finite,
}) {}

class Control extends Context.Service<
  Control,
  {
    now: number;
    readonly wakes: number[];
    readonly state: DurableObjectState;
  }
>()("TimerWorker/Control") {}

const timer = Projection.process({
  id: "workerd-timer",
  params: {},
  inputs: { input: Input },
  commit: Commit.atLeastOnce,
  process: (batch, unit) =>
    Effect.gen(function* () {
      const control = yield* Control;

      if (Option.isSome(unit.due)) {
        control.wakes.push(unit.due.value);
        yield* Effect.promise(() => control.state.storage.put("wakes", control.wakes));
      }

      const at = batch.input.items.at(-1);

      return at === undefined || at === 0 ? undefined : Projection.wakeAt(at);
    }),
});

const layer = (state: DurableObjectState) => {
  const controlLayer = Layer.effect(
    Control,
    Effect.gen(function* () {
      const now = yield* Effect.promise(() => state.storage.get<number>("now"));
      const wakes = yield* Effect.promise(() => state.storage.get<number[]>("wakes"));

      return { now: now ?? Date.UTC(2099, 0, 1), wakes: wakes ?? [], state };
    }),
  );

  const clock = Layer.effect(
    Clock.Clock,
    Effect.gen(function* () {
      const base = yield* Clock.Clock;
      const control = yield* Control;

      return Clock.Clock.of({
        monotonicTimeNanos: base.monotonicTimeNanos,
        monotonicTimeNanosUnsafe: () => base.monotonicTimeNanosUnsafe(),
        currentTimeNanos: base.currentTimeNanos,
        currentTimeNanosUnsafe: () => base.currentTimeNanosUnsafe(),
        sleep: (duration) => base.sleep(duration),
        currentTimeMillisUnsafe: () => control.now,
        currentTimeMillis: Effect.sync(() => control.now),
      });
    }),
  ).pipe(Layer.provideMerge(controlLayer));

  return Layer.mergeAll(CheckpointStore.layer, Projection.Registry.layer).pipe(
    Layer.provideMerge(
      DurableObjectStorage.layer(Streamsy.group(Input), { client: { storage: state.storage } }),
    ),
    Layer.provideMerge(clock),
  );
};

const app = Layer.mergeAll(
  HttpRouter.add(
    "GET",
    "/time/:at",
    Effect.gen(function* () {
      const at = Number((yield* HttpRouter.params).at);
      const control = yield* Control;
      control.now = at;
      yield* Effect.promise(() => control.state.storage.put("now", at));

      return HttpServerResponse.text("clock set");
    }),
  ),
  HttpRouter.add(
    "GET",
    "/snooze/:at",
    Effect.gen(function* () {
      const at = Number((yield* HttpRouter.params).at);
      const input = yield* Input;
      yield* input.create({});
      yield* input.append({}, [at]);
      yield* (yield* Projection.Registry).schedule(timer.at({}));

      return HttpServerResponse.text("scheduled");
    }).pipe(Effect.orDie),
  ),
  HttpRouter.add(
    "GET",
    "/status",
    Effect.gen(function* () {
      const control = yield* Control;

      return HttpServerResponse.jsonUnsafe({
        now: control.now,
        wakes: control.wakes,
        alarm: yield* Effect.promise(() => control.state.storage.getAlarm()),
        status: (yield* (yield* Projection.Registry).status)[0],
      });
    }).pipe(Effect.orDie),
  ),
);

interface Env {
  readonly TIMERS: DurableObjectNamespace;
}

export class TimerObject extends StreamsyObject.make<Env, Layer.Success<ReturnType<typeof layer>>>({
  layer,
  app,
  wakeups: [ProjectionWakeup.attach(timer)],
}) {
  override async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/tick") {
      await super.alarm();

      return new Response("ticked");
    }

    return super.fetch(request);
  }
}

export default {
  async fetch(request: WorkerRequest, env: Env): Promise<WorkerResponse> {
    return env.TIMERS.get(env.TIMERS.idFromName("timer")).fetch(request);
  },
};

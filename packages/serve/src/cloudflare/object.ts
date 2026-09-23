import type { AlarmInvocationInfo, DurableObjectState } from "@cloudflare/workers-types";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, ManagedRuntime, Option } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/unstable/http";
import type { Storage, StorageFault, StreamsReader, StreamsWriter } from "@streamsy/core";
import { alarmLayer } from "./alarm.ts";
import { alarm } from "./host-program.ts";
import { acquireObject, providedApp, unavailable, type ObjectApp } from "./object-runtime.ts";

export interface ObjectConfiguration<Env, R = never> {
  readonly app: ObjectApp<R>;
  readonly layer: (
    state: DurableObjectState,
    env: Env,
  ) => Layer.Layer<StreamsReader | StreamsWriter | Storage | R, StorageFault>;
}
export interface ObjectInstance {
  fetch(request: Request): Promise<Response>;
  alarm(info?: AlarmInvocationInfo): Promise<void>;
}
interface Compiled {
  readonly handler: (request: Request) => Promise<Response>;
  readonly alarm: Effect.Effect<void, StorageFault>;
}
class ObjectOwner extends Context.Service<
  ObjectOwner,
  { readonly get: Effect.Effect<Compiled, StorageFault> }
>()("streamsy/ObjectOwner") {}

class ObjectHost<Env, R> extends DurableObject<Env> {
  readonly #runtime: ManagedRuntime.ManagedRuntime<ObjectOwner, never>;
  constructor(state: DurableObjectState, env: Env, configuration: ObjectConfiguration<Env, R>) {
    super(state, env);
    this.#runtime = ManagedRuntime.make(
      Layer.effect(
        ObjectOwner,
        Effect.map(
          acquireObject(
            Layer.suspend(() =>
              Layer.merge(configuration.layer(state, env), alarmLayer(state.storage)),
            ),
            (context) =>
              Effect.gen(function* () {
                const web = HttpRouter.toWebHandler(providedApp(configuration.app, context), {
                  disableLogger: true,
                });
                yield* Effect.addFinalizer(() => Effect.promise(web.dispose));
                return { handler: web.handler, alarm: alarm.pipe(Effect.provide(context)) };
              }),
          ),
          (get) => ({ get }),
        ),
      ),
    );
  }
  override fetch(request: Request): Promise<Response> {
    return this.#runtime.runPromise(
      Effect.gen(function* () {
        const owner = yield* ObjectOwner;
        const compiled = yield* owner.get.pipe(
          Effect.map(Option.some),
          Effect.catch(() => Effect.succeed(Option.none())),
          Effect.catchDefect(() => Effect.succeed(Option.none())),
        );
        if (Option.isNone(compiled)) return HttpServerResponse.toWeb(unavailable());
        return yield* Effect.promise(() => compiled.value.handler(request));
      }),
    );
  }
  override alarm(_info?: AlarmInvocationInfo): Promise<void> {
    return this.#runtime.runPromise(
      Effect.gen(function* () {
        const owner = yield* ObjectOwner;
        const compiled = yield* owner.get;
        yield* compiled.alarm;
      }),
    );
  }
}
export const StreamsyObject = {
  make: <Env = unknown, R = never>(
    configuration: ObjectConfiguration<Env, R>,
  ): new (state: DurableObjectState, env: Env) => ObjectHost<Env, R> =>
    class extends ObjectHost<Env, R> {
      constructor(state: DurableObjectState, env: Env) {
        super(state, env, configuration);
      }
    },
};

import type { AlarmInvocationInfo, DurableObjectState } from "@cloudflare/workers-types";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, ManagedRuntime, Option } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import type { Storage, StorageFault } from "@streamsy/core";
import { alarmSlotLayer } from "./alarm.ts";
import type { WakeupSource } from "./wakeup.ts";
import { withLinks } from "./links.ts";
import type { LinkClient } from "./object-link.ts";
import { acquireObject, providedApp, unavailable, type ObjectApp } from "./object-runtime.ts";

export interface ObjectConfiguration<Env, R = never, L = never> {
  readonly app: ObjectApp<R | L>;
  readonly wakeups?: ReadonlyArray<WakeupSource<Storage | R | L>>;
  readonly layer: (
    state: DurableObjectState,
    env: Env,
  ) => Layer.Layer<Storage | R, StorageFault, L>;
  /**
   * Streams that live on other objects. Their services come from these links
   * and never from `layer`. An object whose `layer` also provides a linked
   * stream fails to start.
   */
  readonly links?: (env: Env) => ReadonlyArray<LinkClient<L>>;
}

export interface ObjectInstance {
  fetch(request: Request): Promise<Response>;
  alarm(info?: AlarmInvocationInfo): Promise<void>;
}

interface Compiled {
  readonly handler: (request: Request) => Promise<Response>;
  readonly alarm: Effect.Effect<void>;
}

class ObjectOwner extends Context.Service<
  ObjectOwner,
  { readonly get: Effect.Effect<Compiled, StorageFault> }
>()("streamsy/ObjectOwner") {}

class ObjectHost<Env, R, L> extends DurableObject<Env> {
  readonly #runtime: ManagedRuntime.ManagedRuntime<ObjectOwner, never>;
  constructor(state: DurableObjectState, env: Env, configuration: ObjectConfiguration<Env, R, L>) {
    super(state, env);
    this.#runtime = ManagedRuntime.make(
      Layer.effect(
        ObjectOwner,
        Effect.map(
          acquireObject(
            Layer.suspend(() =>
              Layer.merge(
                withLinks(configuration.layer(state, env), configuration.links?.(env)),
                alarmSlotLayer(state.storage),
              ),
            ),
            (context, alarm) =>
              Effect.gen(function* () {
                const web = HttpRouter.toWebHandler(providedApp(configuration.app, context), {
                  disableLogger: true,
                });

                yield* Effect.addFinalizer(() => Effect.promise(web.dispose));

                return { handler: web.handler, alarm: alarm.pipe(Effect.provide(context)) };
              }),
            configuration.wakeups,
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
          Effect.asSome,
          Effect.catch(() => Effect.succeedNone),
          Effect.catchDefect(() => Effect.succeedNone),
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
  make: <Env = unknown, R = never, L = never>(
    configuration: ObjectConfiguration<Env, R, L>,
  ): new (state: DurableObjectState, env: Env) => ObjectHost<Env, R, L> =>
    class extends ObjectHost<Env, R, L> {
      constructor(state: DurableObjectState, env: Env) {
        super(state, env, configuration);
      }
    },
};

import type { AlarmInvocationInfo, DurableObjectState } from "@cloudflare/workers-types";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, ManagedRuntime } from "effect";
import { HttpEffect } from "effect/unstable/http";
import type { Storage, StorageFault, StreamsReader, StreamsWriter } from "@streamsy/core";
import type { HttpOptions } from "@streamsy/core/http";
import { Alarm, alarmLayer } from "./alarm.ts";
import { fetch, alarm } from "./host-program.ts";
import { ObjectOptions } from "./object-options.ts";

const unavailable = (): Response =>
  new Response("Storage unavailable", {
    status: 503,
    headers: {
      "retry-after": "1",
      "x-content-type-options": "nosniff",
      "cross-origin-resource-policy": "cross-origin",
    },
  });

type Runtime = ManagedRuntime.ManagedRuntime<
  StreamsReader | StreamsWriter | Storage | Alarm | ObjectOptions,
  StorageFault
>;

interface Configuration<Env> {
  readonly options: HttpOptions;
  readonly layer: (
    state: DurableObjectState,
    env: Env,
  ) => Layer.Layer<StreamsReader | StreamsWriter | Storage, StorageFault>;
}

class ObjectHost<Env> extends DurableObject<Env> {
  readonly #configuration: Configuration<Env>;
  constructor(state: DurableObjectState, env: Env, configuration: Configuration<Env>) {
    super(state, env);
    this.#configuration = configuration;
  }
  #runtime: Runtime | undefined;

  #getRuntime(): Runtime {
    return (this.#runtime ??= ManagedRuntime.make(
      Layer.mergeAll(
        this.#configuration.layer(this.ctx, this.env),
        alarmLayer(this.ctx.storage),
        Layer.succeed(ObjectOptions, this.#configuration.options),
      ),
    ));
  }

  #recover(runtime: Runtime): Promise<void> {
    if (this.#runtime !== runtime) return Promise.resolve();
    this.#runtime = undefined;
    return runtime.dispose().catch(() => undefined);
  }

  override fetch(request: Request): Promise<Response> {
    let runtime: Runtime;
    try {
      runtime = this.#getRuntime();
    } catch {
      return Promise.resolve(unavailable());
    }
    return runtime.context().then(
      (context) =>
        HttpEffect.toWebHandler(fetch.pipe(Effect.provide(context), Effect.interruptible))(request),
      () => this.#recover(runtime).then(unavailable),
    );
  }

  override alarm(_info?: AlarmInvocationInfo): Promise<void> {
    const runtime = this.#getRuntime();
    // Acquisition failures discard the cached runtime; sweep failures keep it.
    return runtime.context().then(
      () => runtime.runPromise(alarm),
      (error) =>
        this.#recover(runtime).then(() => {
          throw error;
        }),
    );
  }
}

/** The wrangler/Miniflare boundary. Each instance owns one lazy runtime. */
const make = <Env = unknown>(
  configuration: Configuration<Env>,
): new (state: DurableObjectState, env: Env) => ObjectHost<Env> =>
  class extends ObjectHost<Env> {
    constructor(state: DurableObjectState, env: Env) {
      super(state, env, configuration);
    }
  };

export const StreamsyObject = { make };

import type { AlarmInvocationInfo, DurableObjectState, WebSocket } from "@cloudflare/workers-types";
import { DurableObject } from "cloudflare:workers";
import { Context, Effect, Layer, ManagedRuntime, Option } from "effect";
import { HttpRouter, HttpServerResponse } from "effect/http";
import type { Storage, StorageFault } from "@streamsy/core";
import { LIVE_PING, LIVE_PONG, LiveCloseCode } from "@streamsy/core/http";
import { alarmSlotLayer } from "./alarm.ts";
import type { WakeupSource } from "./wakeup.ts";
import { withLinks } from "./links.ts";
import type { LinkClient } from "./object-link.ts";
import {
  liveSettings,
  // oxlint-disable-next-line anti-slop-effect/no-service-constructor-imports -- This private composition root builds the live sockets with the object context, as it does the wakeups.
  makeLiveSockets,
  type LiveConfiguration,
  type LiveSockets,
} from "./live-socket.ts";
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
  /**
   * Live reads over hibernatable WebSockets (`live=websocket`), a Streamsy
   * extension. Off by default. Without it the object answers `426` to
   * `live=websocket`, and clients fall back to long-poll.
   */
  readonly live?: LiveConfiguration;
}

export interface ObjectInstance {
  fetch(request: Request): Promise<Response>;
  alarm(info?: AlarmInvocationInfo): Promise<void>;
  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void>;
  webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void>;
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The platform handler signature passes an unknown error.
  webSocketError(ws: WebSocket, error: unknown): Promise<void>;
}

interface Compiled {
  readonly handler: (request: Request) => Promise<Response>;
  readonly alarm: Effect.Effect<void>;
  readonly live: Option.Option<LiveSockets>;
}

class ObjectOwner extends Context.Service<
  ObjectOwner,
  { readonly get: Effect.Effect<Compiled, StorageFault> }
>()("streamsy/ObjectOwner") {}

class ObjectHost<Env, R, L> extends DurableObject<Env> {
  readonly #runtime: ManagedRuntime.ManagedRuntime<ObjectOwner, never>;
  constructor(state: DurableObjectState, env: Env, configuration: ObjectConfiguration<Env, R, L>) {
    super(state, env);
    const live = Option.map(Option.fromUndefinedOr(configuration.live), liveSettings);

    // The runtime answers the client heartbeat without waking the object.
    if (Option.isSome(live))
      state.setWebSocketAutoResponse(new WebSocketRequestResponsePair(LIVE_PING, LIVE_PONG));

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
                const web = HttpRouter.toWebHandler(providedApp(configuration.app, context));

                yield* Effect.addFinalizer(() => Effect.promise(web.dispose));

                const sockets = Option.isSome(live)
                  ? Option.some(
                      yield* makeLiveSockets(state, web.handler, live.value).pipe(
                        Effect.provide(context),
                      ),
                    )
                  : Option.none<LiveSockets>();

                return {
                  handler: web.handler,
                  alarm: alarm.pipe(Effect.provide(context)),
                  live: sockets,
                };
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
        const { handler, live } = compiled.value;

        if (Option.isNone(live)) return yield* Effect.promise(() => handler(request));

        const response = live.value.accepts(request)
          ? yield* live.value.connect(request)
          : yield* Effect.promise(() => handler(request));

        // Pushes finish inside the event that committed, so nothing waits after it.
        yield* live.value.settle;

        return response;
      }),
    );
  }
  override alarm(_info?: AlarmInvocationInfo): Promise<void> {
    return this.#runtime.runPromise(
      Effect.gen(function* () {
        const owner = yield* ObjectOwner;
        const compiled = yield* owner.get;
        yield* compiled.alarm;

        if (Option.isSome(compiled.live)) yield* compiled.live.value.settle;
      }),
    );
  }
  /** Reached only for a message other than the `ping` that the runtime answers. */
  override webSocketMessage(ws: WebSocket, _message: string | ArrayBuffer): Promise<void> {
    ws.close(LiveCloseCode.BadMessage, "Unexpected client message");

    return Promise.resolve();
  }
  /** The runtime answers the close frame. The client keeps its own offset. */
  override webSocketClose(
    _ws: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    return Promise.resolve();
  }
  // oxlint-disable-next-line anti-slop/no-unknown-parameters -- The platform handler signature passes an unknown error.
  override webSocketError(_ws: WebSocket, error: unknown): Promise<void> {
    return Effect.runPromise(Effect.logWarning("Live socket error", error));
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

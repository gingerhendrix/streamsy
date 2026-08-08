/**
 * Best-effort projection wakes, as a service.
 *
 * A wake only affects latency: every message runs the same bounded repair the
 * explicit endpoint runs, so a lost or duplicated wake still converges. That is
 * why the service's failure channel is `never` — a send that fails is logged and
 * swallowed here rather than turning an accepted, durable command into an error
 * at the edge.
 *
 * The local host provides `layerDisabled`; the Worker provides `layerQueue`
 * over its Cloudflare queue binding.
 */
import { Context, Effect, Layer } from "effect";

export interface WakeMessage {
  readonly workspaceId: string;
  readonly projectId: string;
  readonly issueId?: string;
}

export interface WakeShape {
  readonly wake: (message: WakeMessage) => Effect.Effect<void>;
}

export class Wake extends Context.Service<Wake, WakeShape>()("issue-tracker-projections/Wake") {}

/** No background lane. Repair and the next request are the only convergence. */
export const layerDisabled: Layer.Layer<Wake> = Layer.succeed(
  Wake,
  Wake.of({ wake: () => Effect.void }),
);

/** Send wakes through a queue-like sender, absorbing delivery failures. */
export const layerQueue = (send: (message: WakeMessage) => Promise<void>): Layer.Layer<Wake> =>
  Layer.succeed(
    Wake,
    Wake.of({
      wake: Effect.fn("Wake.send")(
        function* (message: WakeMessage) {
          yield* Effect.promise(() => send(message));
        },
        (effect, message) =>
          effect.pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("projection wake could not be delivered", {
                workspaceId: message.workspaceId,
                projectId: message.projectId,
                cause,
              }),
            ),
          ),
      ),
    }),
  );

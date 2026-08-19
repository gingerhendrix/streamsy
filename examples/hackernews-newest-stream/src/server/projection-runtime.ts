import type { StreamProtocolClient } from "@streamsy/core";
import {
  StateProjection,
  type CatchUpOutcome,
  type StateProjectionLimits,
} from "@streamsy/experimental/effect/state-projection";
import { ManagedRuntime } from "effect";
import { hackerNewsStoryIndex } from "./story-index-projection.ts";
import { hackerNewsSource, hackerNewsTarget } from "./streams.ts";

export type ProjectionStatus = {
  running: boolean;
  lastAttemptStartedAt?: string;
  lastAttemptCompletedAt?: string;
  lastError?: string;
  lastOutcome?: CatchUpOutcome;
};

export function createStoryProjectionRuntime(
  client: StreamProtocolClient,
  limits: StateProjectionLimits,
) {
  const runtime = ManagedRuntime.make(StateProjection.layerClient(client));
  const projection = StateProjection.instance(hackerNewsStoryIndex, {
    source: hackerNewsSource,
    target: hackerNewsTarget,
    generation: "v1",
    producerEpoch: 0,
  });
  let running = false;
  let lastAttemptStartedAt: string | undefined;
  let lastAttemptCompletedAt: string | undefined;
  let lastError: string | undefined;
  let lastOutcome: CatchUpOutcome | undefined;

  async function catchUp(): Promise<CatchUpOutcome | undefined> {
    running = true;
    lastAttemptStartedAt = new Date().toISOString();
    lastError = undefined;
    try {
      lastOutcome = await runtime.runPromise(StateProjection.catchUp(projection, { limits }));
      return lastOutcome;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
      return undefined;
    } finally {
      running = false;
      lastAttemptCompletedAt = new Date().toISOString();
    }
  }

  function status(): ProjectionStatus {
    return {
      running,
      lastAttemptStartedAt,
      lastAttemptCompletedAt,
      lastError,
      lastOutcome,
    };
  }

  return {
    catchUp,
    status,
    dispose: () => runtime.dispose(),
  };
}

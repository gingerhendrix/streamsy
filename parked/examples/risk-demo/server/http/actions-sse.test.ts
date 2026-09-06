/* oxlint-disable effecttsgo/async-function -- Vitest owns these Promise-native test callbacks; application workflows are exercised through their existing Effect runtimes or Promise facades. */
/* oxlint-disable effecttsgo/global-timers, effecttsgo/new-promise -- These Vitest cases directly coordinate the Web ReadableStream Promise/timer contract under test. */
/**
 * The connection bound, isolated from the game.
 *
 * A healthy stream closes because its blocking read expires and the loop
 * re-checks the clock. That is the easy case, and it is not the one the bound
 * exists for: a read that never settles — wedged storage, a live read that lost
 * its wakeup — would hold the response open forever if the deadline were only
 * ever consulted between reads. So the bound is a real timer, and these tests
 * drive it with reads that never resolve.
 */

import { describe, expect, it } from "vitest";

import { actionsStreamResponse, type ActionsPage } from "./actions-sse.ts";
import { createActionsDecoder } from "../../src/application/actions-stream.ts";

const never = new Promise<ActionsPage>(() => {});

/** Read a response to its end, returning how long that took and what arrived. */
async function drain(response: Response): Promise<{ elapsedMs: number; body: string }> {
  const started = performance.now();
  const body = await response.text();
  return { elapsedMs: performance.now() - started, body };
}

describe("actions stream bound", () => {
  it("closes on the wall clock even if the first read never settles", async () => {
    // The bound's clock starts when the response is constructed, so the elapsed
    // time is measured from here rather than from `drain`. Measuring from the
    // drain subtracts however long the scheduler took to get there, which under
    // a loaded test run is most of the 60ms budget — the source of a flake that
    // had nothing to do with the behaviour under test.
    const startedAt = performance.now();
    const response = actionsStreamResponse({
      read: () => never,
      signal: new AbortController().signal,
      timeoutMs: 60,
    });

    const { body } = await drain(response);
    const elapsedMs = performance.now() - startedAt;
    // It ended by itself, at its bound, having written nothing — rather than
    // holding a client on a connection that was never going to speak.
    expect(elapsedMs).toBeGreaterThanOrEqual(40);
    expect(elapsedMs).toBeLessThan(3_000);
    expect(body).toBe("");
  });

  it("closes on the wall clock when a later read hangs after a good batch", async () => {
    let call = 0;
    const response = actionsStreamResponse({
      read: async () => {
        call += 1;
        if (call > 1) return never;
        return { messages: [], nextOffset: "off-1", upToDate: true };
      },
      signal: new AbortController().signal,
      timeoutMs: 60,
    });

    const { elapsedMs, body } = await drain(response);
    expect(elapsedMs).toBeLessThan(3_000);
    // The batch it did produce is intact, and the cursor is re-stated before the
    // connection ends, so the client still knows where to reconnect.
    expect(createActionsDecoder().push(body)).toEqual([
      { messages: [], nextOffset: "off-1", upToDate: true, closed: false },
    ]);
    expect(call).toBe(2);
  });

  it("aborts the in-flight read when the bound expires", async () => {
    let seen: AbortSignal | undefined;
    const response = actionsStreamResponse({
      read: (_cursor, _waitMs, signal) => {
        seen = signal;
        return never;
      },
      signal: new AbortController().signal,
      timeoutMs: 60,
    });

    await drain(response);
    // The read is told to stop too: a bound that closed only the response would
    // leave the storage read running behind it.
    expect(seen?.aborted).toBe(true);
  });

  it("stops when the client disconnects before the bound", async () => {
    const client = new AbortController();
    let seen: AbortSignal | undefined;
    const response = actionsStreamResponse({
      read: (_cursor, _waitMs, signal) => {
        seen = signal;
        return never;
      },
      signal: client.signal,
      timeoutMs: 10_000,
    });

    const drained = drain(response);
    setTimeout(() => client.abort(), 25);
    const { elapsedMs } = await drained;
    expect(elapsedMs).toBeLessThan(3_000);
    expect(seen?.aborted).toBe(true);
  });
});

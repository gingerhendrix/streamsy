import { describe, expect, it } from "vitest";
import type { AwaitChangeResult, StreamChangeSnapshot } from "../../types/storage.ts";
import { raceAbortAwaitChange } from "./race-abort.ts";
import { formatCounter, ZERO_OFFSET } from "./offset-generator.ts";

const fallback: StreamChangeSnapshot = {
  present: true,
  currentOffset: ZERO_OFFSET,
  closed: false,
  softDeleted: false,
};

const changed: AwaitChangeResult = {
  status: "changed",
  snapshot: { present: true, currentOffset: formatCounter(1), closed: false, softDeleted: false },
};

describe("raceAbortAwaitChange", () => {
  it("passes the wait through unchanged when there is no signal", async () => {
    const wait = Promise.resolve(changed);
    const raced = raceAbortAwaitChange(wait, fallback);
    expect(raced).toBe(wait);
    expect(await raced).toBe(changed);
  });

  it("resolves immediately to a timeout-shaped fallback when already aborted", async () => {
    const controller = new AbortController();
    controller.abort();
    const neverSettles = new Promise<AwaitChangeResult>(() => {});

    const result = await raceAbortAwaitChange(neverSettles, fallback, controller.signal);
    expect(result.status).toBe("timeout");
    expect(result.snapshot).toBe(fallback);
  });

  it("resolves timeout-shaped on abort and leaves the underlying wait to settle", async () => {
    const controller = new AbortController();
    let settleWait!: (value: AwaitChangeResult) => void;
    const wait = new Promise<AwaitChangeResult>((resolve) => {
      settleWait = resolve;
    });

    const raced = raceAbortAwaitChange(wait, fallback, controller.signal);
    controller.abort();
    const result = await raced;
    expect(result.status).toBe("timeout");
    expect(result.snapshot).toBe(fallback);

    // The underlying wait is never rejected — it settles by its own timeout later.
    settleWait(changed);
    expect(await wait).toBe(changed);
  });

  it("returns the wait result when the wait wins the race", async () => {
    const controller = new AbortController();
    const wait = Promise.resolve(changed);
    const result = await raceAbortAwaitChange(wait, fallback, controller.signal);
    expect(result).toBe(changed);
  });

  it("propagates a rejection from the underlying wait", async () => {
    const controller = new AbortController();
    let rejectWait!: (reason: unknown) => void;
    const wait = new Promise<AwaitChangeResult>((_, reject) => {
      rejectWait = reject;
    });

    const raced = raceAbortAwaitChange(wait, fallback, controller.signal);
    rejectWait(new Error("boom"));
    await expect(raced).rejects.toThrow("boom");
  });
});

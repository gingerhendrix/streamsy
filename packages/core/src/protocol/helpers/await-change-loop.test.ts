import { describe, expect, it } from "vitest";
import type { StreamRecord } from "../../types/storage.ts";
import { runAwaitChangeLoop } from "./await-change-loop.ts";

function record(currentOffset: string): StreamRecord {
  return {
    id: "s",
    config: { contentType: "application/octet-stream", createdAt: 0 },
    lifecycle: {},
    currentOffset,
    counter: 0,
  };
}

const failOnPark = (): Promise<void> => {
  throw new Error("should not park when state already advanced");
};

describe("runAwaitChangeLoop", () => {
  it("caps the total wait budget at totalCapMs even when timeoutMs is much larger", async () => {
    // Deterministic mock clock: each park advances the clock by its full timeout
    // (no real sleep), so we can assert exactly how long the loop would block.
    let clock = 0;
    const parks: number[] = [];
    const now = () => clock;
    const waitForWake = (ms: number) => {
      parks.push(ms);
      clock += ms;
      return Promise.resolve();
    };

    const result = await runAwaitChangeLoop(
      { readRecord: () => record("1_0"), waitForWake, totalCapMs: 1500, now },
      { fromOffset: "1_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("timeout");
    // Returned at the cap (1500), NOT the 10_000ms caller budget.
    expect(clock).toBe(1500);
    // No single park ever exceeded the cap either.
    expect(Math.max(...parks)).toBeLessThanOrEqual(1500);
  });

  it("uses the caller budget when no totalCapMs is given", async () => {
    let clock = 0;
    const waitForWake = (ms: number) => {
      clock += ms;
      return Promise.resolve();
    };

    const result = await runAwaitChangeLoop(
      { readRecord: () => record("1_0"), waitForWake, now: () => clock },
      { fromOffset: "1_0", timeoutMs: 250 },
    );

    expect(result.status).toBe("timeout");
    expect(clock).toBe(250);
  });

  it("caps each individual park at parkCapMs", async () => {
    let clock = 0;
    const parks: number[] = [];
    const waitForWake = (ms: number) => {
      parks.push(ms);
      clock += ms;
      return Promise.resolve();
    };

    const result = await runAwaitChangeLoop(
      { readRecord: () => record("1_0"), waitForWake, parkCapMs: 100, now: () => clock },
      { fromOffset: "1_0", timeoutMs: 350 },
    );

    expect(result.status).toBe("timeout");
    expect(parks).toEqual([100, 100, 100, 50]);
  });

  it("returns changed immediately when state already advanced past fromOffset", async () => {
    const result = await runAwaitChangeLoop(
      { readRecord: () => record("2_0"), waitForWake: failOnPark, now: () => 0 },
      { fromOffset: "1_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("changed");
    if (result.status !== "changed") throw new Error("expected changed");
    expect(result.snapshot).toMatchObject({ present: true, currentOffset: "2_0" });
  });

  it("returns changed on an offset regression (purge → re-create while parked)", async () => {
    const result = await runAwaitChangeLoop(
      // Re-created stream restarts below the parked position; inequality (not
      // just advance) must trigger `changed`.
      { readRecord: () => record("0_0"), waitForWake: failOnPark, now: () => 0 },
      { fromOffset: "3_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("changed");
  });

  it("supports an async readRecord", async () => {
    const result = await runAwaitChangeLoop(
      { readRecord: () => Promise.resolve(record("2_0")), waitForWake: failOnPark, now: () => 0 },
      { fromOffset: "1_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("changed");
  });

  it("wakes and returns changed when a park resolves after state advances", async () => {
    let advanced = false;
    const result = await runAwaitChangeLoop(
      {
        readRecord: () => record(advanced ? "2_0" : "1_0"),
        // Simulate a wake: the underlying state advances, then the park resolves.
        waitForWake: () => {
          advanced = true;
          return Promise.resolve();
        },
        now: () => 0,
      },
      { fromOffset: "1_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("changed");
  });

  it("catches a mutation whose wake was lost mid-registration via the parkCapMs re-read", async () => {
    // The lost-notify interleaving the entry re-read alone cannot cover: the
    // mutation commits AFTER the loop's re-read but its wake is dropped before
    // the park registers (async read→register window). With a per-park cap the
    // next re-read repairs the miss within the cap instead of stalling to the
    // full caller timeout.
    let clock = 0;
    let offset = "1_0";
    const parks: number[] = [];
    const result = await runAwaitChangeLoop(
      {
        readRecord: () => {
          const snapshot = record(offset);
          // Mutation lands immediately after this read — its wake is lost.
          offset = "2_0";
          return snapshot;
        },
        waitForWake: (ms) => {
          // The wake never arrives; the park runs to its (capped) timeout.
          parks.push(ms);
          clock += ms;
          return Promise.resolve();
        },
        parkCapMs: 50,
        now: () => clock,
      },
      { fromOffset: "1_0", timeoutMs: 10_000 },
    );

    expect(result.status).toBe("changed");
    // Repaired within one capped park, not the 10s budget.
    expect(parks).toEqual([50]);
    expect(clock).toBe(50);
  });

  it("re-parks within the remaining budget after a non-advancing wake", async () => {
    let clock = 0;
    const parks: number[] = [];
    const waitForWake = (ms: number) => {
      parks.push(ms);
      clock += 100; // a spurious wake arrives 100ms in
      return Promise.resolve();
    };

    const result = await runAwaitChangeLoop(
      { readRecord: () => record("1_0"), waitForWake, now: () => clock },
      { fromOffset: "1_0", timeoutMs: 250 },
    );

    expect(result.status).toBe("timeout");
    // First park gets the full budget; subsequent parks shrink to the remainder.
    expect(parks).toEqual([250, 150, 50]);
  });
});

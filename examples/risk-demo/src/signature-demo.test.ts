import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";

import { createInMemoryStores } from "../server/stores.ts";
import { runSignatureDemo } from "../server/signature-demo.ts";

function run(seed: number) {
  const protocol = createStreamProtocol({ storage: { adapter: createMemoryStorageAdapter() } });
  const stores = createInMemoryStores();
  return runSignatureDemo({ protocol, stores, seed });
}

type Summary = Awaited<ReturnType<typeof run>>["summary"];
const winnerName = (s: Summary): string | null =>
  s.players.find((p) => p.id === s.winnerId)?.name ?? null;

describe("signature demo scenario", () => {
  it("demonstrates the full signature sequence deterministically", async () => {
    const { summary, trace } = await run(1234);

    // 1 + 9: an agent-only game finished with a winner and a final watermark.
    expect(summary.winnerId).not.toBeNull();
    expect(summary.finalWatermark).not.toBeNull();
    expect(summary.turnsPlayed).toBeGreaterThan(0);

    // 2: an agent's notification cursor was persisted, reloaded, and resumed.
    expect(summary.cursorRestart?.resumed).toBe(true);

    // 3: an accepted attack was retained with recorded dice + canonical offset.
    expect(summary.attackRecorded).not.toBeNull();
    expect(summary.attackRecorded!.attackerRolls.length).toBeGreaterThan(0);
    expect(summary.attackRecorded!.sourceOffset).toBeTruthy();

    // 4: causal syncedThrough resolved only once the board incorporated the ack.
    expect(summary.causalWait?.synced).toBe(true);

    // 5: an idempotent retry returned the original ack as a duplicate.
    expect(summary.idempotentRetry?.duplicate).toBe(true);
    expect(summary.idempotentRetry?.sameOffset).toBe(true);

    // 6: a stale/racing command was safely rejected.
    expect(summary.staleCommand?.rejected).toBe(true);

    // 7: crash immediately after output commit did not double-apply.
    expect(summary.crashRecovery.doubleApplied).toBe(false);
    expect(summary.crashRecovery.boardEqual).toBe(true);
    expect(summary.crashRecovery.committedAfterRecovery).toBeGreaterThanOrEqual(
      summary.crashRecovery.committedBeforeCrash,
    );

    // 8: a fresh generation rebuilt equivalently, cut over, old one retained.
    expect(summary.rebuild.boardEqual).toBe(true);
    expect(summary.rebuild.watermarkEqual).toBe(true);
    expect(summary.rebuild.activeGeneration).toBe(summary.rebuild.toGeneration);
    expect(summary.rebuild.retained).toContain(summary.rebuild.fromGeneration);

    // The trace is ordered machine-readable evidence.
    expect(trace[0]!.seq).toBe(1);
    expect(trace.at(-1)!.step).toBe("summary");
    expect(trace.map((t) => t.step)).toContain("crash-recovery");
    expect(trace.map((t) => t.step)).toContain("generation-rebuild");
  });

  it("is deterministic across runs with the same seed", async () => {
    // Game identities (gameId/playerId) are random per run, but the seeded game
    // logic is deterministic — so the winning ROLE, dice, offsets, and structure
    // are stable across runs.
    const a = await run(1234);
    const b = await run(1234);
    expect(winnerName(b.summary)).toBe(winnerName(a.summary));
    expect(b.summary.finalWatermark).toBe(a.summary.finalWatermark);
    expect(b.summary.turnsPlayed).toBe(a.summary.turnsPlayed);
    expect(b.summary.attackRecorded).toEqual(a.summary.attackRecorded);
    expect(b.summary.crashRecovery).toEqual(a.summary.crashRecovery);
    expect(b.summary.rebuild.toGeneration).toBe(a.summary.rebuild.toGeneration);
    expect(b.summary.rebuild.retained).toEqual(a.summary.rebuild.retained);
  });
});

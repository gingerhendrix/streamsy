import { describe, expect, it } from "vitest";
import { createMemoryStorageAdapter, createStreamProtocol } from "@streamsy/core";
import type { StreamProtocolFactory } from "@streamsy/core";
import { ProjectionRuntime } from "@streamsy/experimental/projection";

import { createInMemoryStores } from "../server/stores.ts";
import {
  analyzeProjectionOutput,
  detectDoubleApply,
  runSignatureDemo,
} from "../server/signature-demo.ts";
import {
  createBoardProjectionAdapter,
  writeCanonicalEvents,
} from "./materializer/board-projection.ts";
import { recordFullGameEvents } from "./testkit.ts";

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

    // 7: crash immediately after output commit did not double-apply. Proven by
    // exactly one transition per canonical event, no repeated source ordinal,
    // and an exact match against a clean control build of the same log.
    const crash = summary.crashRecovery;
    expect(crash.doubleApplied).toBe(false);
    expect(crash.boardEqual).toBe(true);
    expect(crash.watermarkEqual).toBe(true);
    expect(crash.duplicateSourceSeqs).toEqual([]);
    expect(crash.canonicalEvents).toBeGreaterThan(0);
    expect(crash.actualTransitions).toBe(crash.canonicalEvents);
    expect(crash.actualTransitions).toBe(crash.expectedTransitions);
    expect(crash.actualOutputMessages).toBe(crash.expectedOutputMessages);
    // The crash really did happen mid-build (not after everything was written).
    expect(crash.committedAtCrash).toBeGreaterThan(0);
    expect(crash.committedAtCrash).toBeLessThan(crash.actualOutputMessages);

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

/** Materialize a canonical log into `generation` and return the analysis inputs. */
async function buildProjection(
  protocol: StreamProtocolFactory,
  source: string,
  generation: string,
) {
  const streamId = `games/neg/projections/board/${generation}`;
  const runtime = new ProjectionRuntime({
    protocol,
    adapter: createBoardProjectionAdapter({
      gameId: "neg",
      sourceStreamId: source,
      outputStreamId: streamId,
      generation,
    }),
  });
  await runtime.catchUp();
  return streamId;
}

describe("no-double-apply detector", () => {
  it("passes a clean build and FAILS a stream with a duplicated transition", async () => {
    const protocol = createStreamProtocol({
      storage: { adapter: createMemoryStorageAdapter() },
    });
    const source = "games/neg/events";
    const events = recordFullGameEvents(1234);
    await writeCanonicalEvents(protocol, source, events);

    const controlId = await buildProjection(protocol, source, "control");
    const subjectId = await buildProjection(protocol, source, "subject");
    const control = await analyzeProjectionOutput(protocol, controlId);
    const clean = await analyzeProjectionOutput(protocol, subjectId);

    // Sanity: one transition per canonical event, ordinals 0..N-1 exactly once.
    expect(control.transitions).toBe(events.length);
    expect(control.sourceSeqs).toEqual(events.map((_, i) => i));
    expect(control.duplicateSourceSeqs).toEqual([]);

    // The production detector accepts an honest build...
    expect(detectDoubleApply(clean, control)).toBe(false);

    // ...and now genuinely double-apply one transition by re-appending an
    // already-committed `projectionMeta` row to the subject stream.
    const got = await protocol.get(subjectId);
    if (got.status !== "ok") throw new Error("subject stream missing");
    const read = await got.stream.read({});
    if (read.status !== "ok") throw new Error("cannot read subject stream");
    const decoder = new TextDecoder();
    const duplicated = read.messages
      .map((m) => JSON.parse(decoder.decode(m.data)) as { type?: string; value?: unknown })
      .find((row) => row.type === "projectionMeta");
    expect(duplicated).toBeDefined();
    const appended = await got.stream.append({
      data: new TextEncoder().encode(JSON.stringify([duplicated])),
      contentType: "application/json",
    });
    expect(appended.status).toBe("appended");

    // The detector must now flag it — this is what makes `doubleApplied=false`
    // in the signature proof a real claim rather than a tautology.
    const tampered = await analyzeProjectionOutput(protocol, subjectId);
    expect(tampered.duplicateSourceSeqs.length).toBeGreaterThan(0);
    expect(tampered.transitions).toBe(control.transitions + 1);
    expect(detectDoubleApply(tampered, control)).toBe(true);
  });
});

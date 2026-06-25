import { describe, expect, it } from "vitest";
import type { CreateOptions } from "../../types/protocol.ts";
import type { StreamRecord } from "../../types/storage.ts";
import { ForkPlanBuilder } from "./fork-plan-builder.ts";
import { ZERO_OFFSET, parseCounter } from "./offset-generator.ts";

const clock = { now: () => 1_000, date: (value?: number | string) => new Date(value ?? 1_000) };

function source(overrides: Partial<StreamRecord> = {}): StreamRecord {
  return {
    id: "source",
    config: { contentType: "text/plain", createdAt: 1, ttlSeconds: 20 },
    lifecycle: { expiresAtMs: 21_000 },
    currentOffset: "0000000000000002_0000000000000000",
    counter: 2,
    ...overrides,
  };
}

function newRecord(
  streamId: string,
  contentType: string,
  options: CreateOptions,
  fork: { forkedFrom: string; forkOffset: string },
): StreamRecord {
  return {
    id: streamId,
    config: {
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: 1_000,
    },
    lifecycle: {
      forkedFrom: fork.forkedFrom,
      forkOffset: fork.forkOffset,
      expiresAtMs: options.ttlSeconds === undefined ? undefined : 11_000,
    },
    currentOffset: fork.forkOffset,
    counter: parseCounter(fork.forkOffset),
  };
}

describe("ForkPlanBuilder", () => {
  it("returns source validation failures without a plan", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    expect(builder.build("child", "source", null, {})).toMatchObject({
      kind: "terminal",
      result: { status: "not-found" },
    });
    expect(
      builder.build("child", "source", source({ lifecycle: { softDeleted: true } }), {}),
    ).toMatchObject({
      kind: "terminal",
      result: { status: "conflict", conflictReason: "fork-source-soft-deleted" },
    });
  });

  it("builds a fork plan with inherited expiry, source liveness precondition, and initial messages", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });
    const decision = builder.build("child", "source", source(), {
      initialData: new TextEncoder().encode("child"),
    });
    if (decision.kind !== "fork") throw new Error("expected fork plan");

    expect(decision.plan.sourceId).toBe("source");
    expect(decision.plan.precondition).toEqual({
      sourceLiveAtOffset: "0000000000000002_0000000000000000",
    });
    expect(decision.plan.child.id).toBe("child");
    expect(decision.plan.child.lifecycle.forkedFrom).toBe("source");
    expect(decision.plan.initialMessages?.map((m) => m.offset)).toEqual([
      "0000000000000003_0000000000000000",
    ]);
    expect(decision.plan.afterCommit).toEqual({
      scheduleExpiryAt: 11_000,
      notify: "message",
    });
  });

  it("rejects fork offsets beyond the source tail", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    expect(
      builder.build("child", "source", source(), {
        forkOffset: "0000000000000003_0000000000000000",
      }),
    ).toMatchObject({
      kind: "terminal",
      result: { status: "bad-request", errorMessage: "Stream-Fork-Offset exceeds source tail" },
    });
    expect(builder.build("child", "source", source(), { forkOffset: ZERO_OFFSET })).toMatchObject({
      kind: "fork",
    });
  });
});

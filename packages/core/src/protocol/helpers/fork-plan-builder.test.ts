import { describe, expect, it } from "vitest";
import type { CreateOptions } from "../../types/protocol.ts";
import type { StoredMessage, StreamRecord } from "../../types/storage.ts";
import { ForkPlanBuilder } from "./fork-plan-builder.ts";
import { ZERO_OFFSET, formatCounter, parseCounter } from "./offset-generator.ts";

const clock = { now: () => 1_000, date: (value?: number | string) => new Date(value ?? 1_000) };

const enc = (value: string) => new TextEncoder().encode(value);
const dec = (value: Uint8Array) => new TextDecoder().decode(value);

function tailMessages(items: string[]): StoredMessage[] {
  return items.map((value, i) => ({
    data: enc(value),
    offset: formatCounter(i + 1),
    timestamp: 1,
  }));
}

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
  fork: { forkedFrom: string; forkOffset: string; forkSubOffset?: number },
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
      ...(fork.forkSubOffset !== undefined && fork.forkSubOffset > 0
        ? { forkSubOffset: fork.forkSubOffset }
        : {}),
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

  describe("sub-offset materialization", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    it("materializes a binary sub-offset prefix as the child's own first message", () => {
      const decision = builder.build(
        "child",
        "source",
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 3 },
        tailMessages(["hello"]),
      );
      if (decision.kind !== "fork") throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hel"]);
      expect(decision.plan.child.lifecycle.forkSubOffset).toBe(3);
      expect(decision.plan.initialMessages?.map((m) => m.offset)).toEqual([formatCounter(1)]);
    });

    it("materializes whole JSON messages by flattened count", () => {
      const decision = builder.build(
        "child",
        "source",
        source({ config: { contentType: "application/json", createdAt: 1 } }),
        { contentType: "application/json", forkOffset: ZERO_OFFSET, forkSubOffset: 2 },
        tailMessages(['{"a":1}', '{"b":2}', '{"c":3}']),
      );
      if (decision.kind !== "fork") throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual([
        '{"a":1}',
        '{"b":2}',
      ]);
    });

    it("frames the initial body after the materialized prefix", () => {
      const decision = builder.build(
        "child",
        "source",
        source(),
        {
          contentType: "text/plain",
          forkOffset: ZERO_OFFSET,
          forkSubOffset: 3,
          initialData: enc("XY"),
        },
        tailMessages(["hello"]),
      );
      if (decision.kind !== "fork") throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hel", "XY"]);
    });

    it("treats sub-offset 0 as absent (no prefix, not recorded)", () => {
      const decision = builder.build(
        "child",
        "source",
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 0 },
        undefined,
      );
      if (decision.kind !== "fork") throw new Error("expected fork plan");
      expect(decision.plan.initialMessages ?? []).toEqual([]);
      expect(decision.plan.child.lifecycle.forkSubOffset).toBeUndefined();
    });

    it("rejects a binary sub-offset that overshoots the message length", () => {
      expect(
        builder.build(
          "child",
          "source",
          source(),
          { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 5 },
          tailMessages(["hi"]),
        ),
      ).toMatchObject({ kind: "terminal", result: { status: "bad-request" } });
    });

    it("rejects a JSON sub-offset that overshoots the message count", () => {
      expect(
        builder.build(
          "child",
          "source",
          source({ config: { contentType: "application/json", createdAt: 1 } }),
          { contentType: "application/json", forkOffset: ZERO_OFFSET, forkSubOffset: 4 },
          tailMessages(['{"a":1}', '{"b":2}', '{"c":3}']),
        ),
      ).toMatchObject({ kind: "terminal", result: { status: "bad-request" } });
    });

    it("rejects a positive sub-offset with no source message to fork", () => {
      expect(
        builder.build(
          "child",
          "source",
          source(),
          { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 1 },
          [],
        ),
      ).toMatchObject({ kind: "terminal", result: { status: "bad-request" } });
    });

    it("accepts a binary sub-offset equal to the message length", () => {
      const decision = builder.build(
        "child",
        "source",
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 5 },
        tailMessages(["hello"]),
      );
      if (decision.kind !== "fork") throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hello"]);
    });
  });
});

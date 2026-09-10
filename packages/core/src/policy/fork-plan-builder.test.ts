import { Predicate } from "effect";
import { describe, expect, it } from "bun:test";
import type { CreateOptions } from "./options.ts";
import { Offset, StreamId, type StoredMessage, type StreamRecord } from "../schema/index.ts";
import { ForkPlanBuilder } from "./fork-plan-builder.ts";
import { ZERO_OFFSET } from "../offset/index.ts";
const formatCounter = (n: number) =>
  Offset.make(`${String(n).padStart(16, "0")}_${"0".repeat(16)}`);

const clock = { now: () => 1_000 };

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
    id: StreamId.make("source"),
    config: { contentType: "text/plain", createdAt: 1, ttlSeconds: 20 },
    lifecycle: { closed: false, softDeleted: false, expiresAtMs: 21_000 },
    currentOffset: formatCounter(2),
    ...overrides,
  };
}

function newRecord(
  streamId: StreamId,
  contentType: string,
  options: CreateOptions,
  fork: { forkedFrom: StreamId; forkOffset: Offset; forkSubOffset?: number },
): StreamRecord {
  const lifecycle: StreamRecord["lifecycle"] = {
    closed: false,
    softDeleted: false,
    forkedFrom: fork.forkedFrom,
    forkOffset: fork.forkOffset,
    expiresAtMs: options.ttlSeconds === undefined ? undefined : 11_000,
  };
  if (fork.forkSubOffset !== undefined && fork.forkSubOffset > 0) {
    return {
      id: streamId,
      config: { contentType, createdAt: 1_000 },
      lifecycle: { ...lifecycle, forkSubOffset: fork.forkSubOffset },
      currentOffset: fork.forkOffset,
    };
  }
  return {
    id: streamId,
    config: {
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: 1_000,
    },
    lifecycle,
    currentOffset: fork.forkOffset,
  };
}

describe("ForkPlanBuilder", () => {
  it("returns source validation failures without a plan", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    expect(builder.build(StreamId.make("child"), StreamId.make("source"), null, {})).toMatchObject({
      _tag: "Rejected",
      error: { _tag: "ForkSourceNotFound" },
    });
    expect(
      builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source({ lifecycle: { closed: false, softDeleted: true } }),
        {},
      ),
    ).toMatchObject({
      _tag: "Rejected",
      error: { _tag: "CreateConflict", reason: "fork-source-soft-deleted" },
    });
  });

  it("builds a fork plan with inherited expiry, source liveness precondition, and initial messages", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });
    const decision = builder.build(StreamId.make("child"), StreamId.make("source"), source(), {
      initialData: new TextEncoder().encode("child"),
    });
    if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");

    expect(decision.plan.forkSource?.id).toBe(StreamId.make("source"));
    expect(decision.plan.forkSource).toEqual({
      id: StreamId.make("source"),
      liveAtOffset: formatCounter(2),
    });
    expect(decision.plan.record.id).toBe(StreamId.make("child"));
    expect(decision.plan.record.lifecycle.forkedFrom).toBe(StreamId.make("source"));
    expect(decision.plan.initialMessages?.map((m) => m.offset)).toEqual([formatCounter(3)]);
    expect(decision.plan.record.lifecycle.expiresAtMs).toBe(11_000);
  });

  it("rejects fork offsets beyond the source tail", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    expect(
      builder.build(StreamId.make("child"), StreamId.make("source"), source(), {
        forkOffset: "0000000000000003_0000000000000000",
      }),
    ).toMatchObject({
      _tag: "Rejected",
      error: { _tag: "InvalidForkRequest", message: "Stream-Fork-Offset exceeds source tail" },
    });
    expect(
      builder.build(StreamId.make("child"), StreamId.make("source"), source(), {
        forkOffset: ZERO_OFFSET,
      }),
    ).toMatchObject({
      _tag: "Fork",
    });
  });

  describe("sub-offset materialization", () => {
    const builder = new ForkPlanBuilder({ clock, newRecord });

    it("materializes a binary sub-offset prefix as the child's own first message", () => {
      const decision = builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 3 },
        tailMessages(["hello"]),
      );
      if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hel"]);
      expect(decision.plan.record.lifecycle.forkSubOffset).toBe(3);
      expect(decision.plan.initialMessages?.map((m) => m.offset)).toEqual([formatCounter(1)]);
    });

    it("materializes whole JSON messages by flattened count", () => {
      const decision = builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source({ config: { contentType: "application/json", createdAt: 1 } }),
        { contentType: "application/json", forkOffset: ZERO_OFFSET, forkSubOffset: 2 },
        tailMessages(['{"a":1}', '{"b":2}', '{"c":3}']),
      );
      if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual([
        '{"a":1}',
        '{"b":2}',
      ]);
    });

    it("frames the initial body after the materialized prefix", () => {
      const decision = builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source(),
        {
          contentType: "text/plain",
          forkOffset: ZERO_OFFSET,
          forkSubOffset: 3,
          initialData: enc("XY"),
        },
        tailMessages(["hello"]),
      );
      if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hel", "XY"]);
    });

    it("treats sub-offset 0 as absent (no prefix, not recorded)", () => {
      const decision = builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 0 },
        undefined,
      );
      if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");
      expect(decision.plan.initialMessages ?? []).toEqual([]);
      expect(decision.plan.record.lifecycle.forkSubOffset).toBeUndefined();
    });

    it("rejects a binary sub-offset that overshoots the message length", () => {
      expect(
        builder.build(
          StreamId.make("child"),
          StreamId.make("source"),
          source(),
          { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 5 },
          tailMessages(["hi"]),
        ),
      ).toMatchObject({ _tag: "Rejected", error: { _tag: "InvalidForkRequest" } });
    });

    it("rejects a JSON sub-offset that overshoots the message count", () => {
      expect(
        builder.build(
          StreamId.make("child"),
          StreamId.make("source"),
          source({ config: { contentType: "application/json", createdAt: 1 } }),
          { contentType: "application/json", forkOffset: ZERO_OFFSET, forkSubOffset: 4 },
          tailMessages(['{"a":1}', '{"b":2}', '{"c":3}']),
        ),
      ).toMatchObject({ _tag: "Rejected", error: { _tag: "InvalidForkRequest" } });
    });

    it("rejects a positive sub-offset with no source message to fork", () => {
      expect(
        builder.build(
          StreamId.make("child"),
          StreamId.make("source"),
          source(),
          { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 1 },
          [],
        ),
      ).toMatchObject({ _tag: "Rejected", error: { _tag: "InvalidForkRequest" } });
    });

    it("accepts a binary sub-offset equal to the message length", () => {
      const decision = builder.build(
        StreamId.make("child"),
        StreamId.make("source"),
        source(),
        { contentType: "text/plain", forkOffset: ZERO_OFFSET, forkSubOffset: 5 },
        tailMessages(["hello"]),
      );
      if (!Predicate.isTagged(decision, "Fork")) throw new Error("expected fork plan");
      expect(decision.plan.initialMessages?.map((m) => dec(m.data))).toEqual(["hello"]);
    });
  });
});

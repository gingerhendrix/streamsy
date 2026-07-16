import { describe, expect, it } from "vitest";
import type { AppendOptions } from "../types/protocol.ts";
import type { StreamRecord } from "../types/storage.ts";
import { AppendService } from "./append-service.ts";
import { defaultOffsetGenerator, ZERO_OFFSET } from "./helpers/offset-generator.ts";

const encode = (s: string) => new TextEncoder().encode(s);
const clock = { now: () => 1_000, date: (value?: number | string) => new Date(value ?? 1_000) };

function record(overrides: Partial<StreamRecord> = {}): StreamRecord {
  return {
    id: "s",
    config: { contentType: "text/plain", createdAt: 1, ttlSeconds: 60 },
    lifecycle: {},
    currentOffset: ZERO_OFFSET,
    counter: 0,
    ...overrides,
  };
}

function append(overrides: Partial<AppendOptions> = {}): AppendOptions {
  return {
    contentType: "text/plain",
    data: encode("a"),
    ...overrides,
  };
}

describe("AppendService.plan", () => {
  it("returns terminal not-found and gone results without a write plan", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });

    expect(service.plan(null, append(), undefined)).toEqual({
      kind: "terminal",
      result: { status: "not-found" },
    });
    expect(service.plan(record({ lifecycle: { softDeleted: true } }), append(), undefined)).toEqual(
      {
        kind: "terminal",
        result: { status: "gone" },
      },
    );
  });

  it("keeps close-only on an already closed stream idempotent regardless of expectedOffset", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });
    const decision = service.plan(
      record({ currentOffset: "0000000000000001_0000000000000000", lifecycle: { closed: true } }),
      append({ data: new Uint8Array(), close: true, expectedOffset: ZERO_OFFSET }),
      undefined,
    );

    expect(decision).toEqual({
      kind: "terminal",
      result: {
        status: "appended",
        offset: "0000000000000001_0000000000000000",
        closed: true,
      },
    });
  });

  it("reports content-type and closed conflicts before expected-offset conflicts", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });

    expect(
      service.plan(
        record({ currentOffset: "0000000000000001_0000000000000000" }),
        append({ contentType: "application/json", expectedOffset: ZERO_OFFSET }),
        undefined,
      ),
    ).toEqual({
      kind: "terminal",
      result: { status: "conflict", conflictReason: "content-type" },
    });

    expect(
      service.plan(
        record({
          currentOffset: "0000000000000001_0000000000000000",
          lifecycle: { closed: true },
        }),
        append({ expectedOffset: ZERO_OFFSET }),
        undefined,
      ),
    ).toEqual({
      kind: "terminal",
      result: {
        status: "conflict",
        conflictReason: "closed",
        closed: true,
        offset: "0000000000000001_0000000000000000",
      },
    });
  });

  it("builds an append plan with allocated messages, CAS, and TTL touch", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });
    const decision = service.plan(record(), append(), undefined);
    if (decision.kind !== "append") throw new Error("expected append plan");

    expect(decision.plan.preconditions).toEqual({
      expectedOffset: ZERO_OFFSET,
      expectedClosed: false,
    });
    expect(decision.plan.messages).toHaveLength(1);
    expect(decision.plan.messages?.[0]?.offset).toBe("0000000000000001_0000000000000000");
    expect(decision.plan.recordPatch).toEqual({
      currentOffset: "0000000000000001_0000000000000000",
      counter: 1,
      lifecycle: { expiresAtMs: 61_000 },
    });
    expect(decision.afterCommit).toEqual({
      scheduleExpiryAt: 61_000,
    });
  });

  it("encodes accepted producer state as an atomic precondition", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });
    const decision = service.plan(
      record(),
      append({ producer: { producerId: "p", producerEpoch: 1, producerSeq: 1 } }),
      { epoch: 1, lastSeq: 0 },
    );
    if (decision.kind !== "append") throw new Error("expected append plan");

    expect(decision.plan.preconditions.producer).toEqual({
      producerId: "p",
      expected: { epoch: 1, lastSeq: 0 },
      next: { epoch: 1, lastSeq: 1 },
    });
    expect(decision.toResult(record({ currentOffset: "ignored" }))).toMatchObject({
      status: "appended",
      producerEpoch: 1,
      producerSeq: 1,
    });
  });

  it("returns duplicate producer acknowledgements without a write plan", () => {
    const service = new AppendService({ clock, offsets: defaultOffsetGenerator });
    const decision = service.plan(
      record({ currentOffset: "0000000000000001_0000000000000000" }),
      append({ producer: { producerId: "p", producerEpoch: 1, producerSeq: 0 } }),
      { epoch: 1, lastSeq: 0 },
    );

    expect(decision).toEqual({
      kind: "terminal",
      result: {
        status: "duplicate",
        offset: "0000000000000001_0000000000000000",
        producerEpoch: 1,
        producerSeq: 0,
        closed: false,
      },
    });
  });
});

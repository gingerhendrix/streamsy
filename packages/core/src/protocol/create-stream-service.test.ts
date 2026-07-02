import { describe, expect, it } from "vitest";
import type { CreateOptions } from "../types/protocol.ts";
import type { StreamRecord } from "../types/storage.ts";
import { CreateStreamService } from "./create-stream-service.ts";
import { ZERO_OFFSET } from "./helpers/offset-generator.ts";

const clock = { now: () => 1_000, date: (value?: number | string) => new Date(value ?? 1_000) };

function newRecord(contentType: string, options: CreateOptions): StreamRecord {
  return {
    id: "s",
    config: {
      contentType,
      ttlSeconds: options.ttlSeconds,
      expiresAt: options.expiresAt,
      createdAt: 1_000,
    },
    lifecycle:
      options.ttlSeconds === undefined ? {} : { expiresAtMs: 1_000 + options.ttlSeconds * 1_000 },
    currentOffset: ZERO_OFFSET,
    counter: 0,
  };
}

describe("CreateStreamService.plan", () => {
  it("builds a CreatePlan with framed initial data, final tail, and after-commit effects", () => {
    const service = new CreateStreamService({ clock, newRecord });
    const decision = service.plan(null, {
      contentType: "text/plain",
      ttlSeconds: 10,
      initialData: new TextEncoder().encode("hello"),
      closed: true,
    });
    if (decision.kind !== "create") throw new Error("expected create plan");

    expect(decision.plan.record.currentOffset).toBe("0000000000000001_0000000000000000");
    expect(decision.plan.record.counter).toBe(1);
    expect(decision.plan.record.lifecycle.closed).toBe(true);
    expect(decision.plan.initialMessages?.map((m) => m.offset)).toEqual([
      "0000000000000001_0000000000000000",
    ]);
    expect(decision.afterCommit).toEqual({
      scheduleExpiryAt: 11_000,
    });
  });

  it("maps an existing compatible record to exists", () => {
    const service = new CreateStreamService({ clock, newRecord });
    const existing = newRecord("text/plain", { contentType: "text/plain" });

    expect(service.plan(existing, { contentType: "text/plain" })).toEqual({
      kind: "terminal",
      result: {
        status: "exists",
        nextOffset: ZERO_OFFSET,
        contentType: "text/plain",
        closed: false,
      },
    });
  });

  it("returns a fork decision for absent fork creates", () => {
    const service = new CreateStreamService({ clock, newRecord });

    expect(service.plan(null, { forkedFrom: "source" })).toEqual({ kind: "fork" });
  });
});

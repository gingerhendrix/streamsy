import { describe, expect, it } from "vitest";
import { validateProducer } from "../../src/policy/producer-idempotency-service.ts";

describe("producer idempotency decisions", () => {
  it("starts an absent producer at sequence zero and rejects an initial gap", () => {
    expect(validateProducer(undefined, 4, 0)).toMatchObject({
      _tag: "Accepted",
      proposedState: { epoch: 4, lastSeq: 0 },
    });
    expect(validateProducer(undefined, 4, 2)).toMatchObject({
      _tag: "Gap",
      expectedSeq: 0,
      receivedSeq: 2,
    });
  });
  it("fences an older epoch even when its sequence would otherwise advance", () => {
    expect(validateProducer({ epoch: 4, lastSeq: 2 }, 3, 3)).toMatchObject({
      _tag: "StaleEpoch",
      currentEpoch: 4,
    });
  });
  it("requires a new epoch to restart at sequence zero", () => {
    const state = { epoch: 4, lastSeq: 2 };
    expect(validateProducer(state, 5, 0)).toMatchObject({
      _tag: "Accepted",
      proposedState: { epoch: 5, lastSeq: 0 },
    });
    expect(validateProducer(state, 5, 3)).toMatchObject({ _tag: "InvalidEpochSeq" });
    expect(state).toEqual({ epoch: 4, lastSeq: 2 });
  });
  it("recognizes both the last acknowledged tuple and older sequences as duplicates", () => {
    for (const seq of [1, 2]) {
      expect(validateProducer({ epoch: 4, lastSeq: 2 }, 4, seq)).toMatchObject({
        _tag: "Duplicate",
        epoch: 4,
        lastSeq: 2,
      });
    }
  });
  it("accepts exactly the next sequence without mutating acknowledged state", () => {
    const state = { epoch: 4, lastSeq: 2 };
    expect(validateProducer(state, 4, 3)).toMatchObject({
      _tag: "Accepted",
      proposedState: { epoch: 4, lastSeq: 3 },
    });
    expect(state).toEqual({ epoch: 4, lastSeq: 2 });
  });
  it("reports the expected sequence for a gap within an epoch", () => {
    expect(validateProducer({ epoch: 4, lastSeq: 2 }, 4, 5)).toMatchObject({
      _tag: "Gap",
      expectedSeq: 3,
      receivedSeq: 5,
    });
  });
});

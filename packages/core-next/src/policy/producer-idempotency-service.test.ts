import { describe, expect, it } from "bun:test";
import { validateProducer, rejectionToAppendResult } from "./producer-idempotency-service.ts";
import { ZERO_OFFSET } from "../offset/index.ts";

describe("producer idempotency decisions", () => {
  it("starts an absent producer at sequence zero and rejects an initial gap", () => {
    expect(validateProducer(undefined, 4, 0)).toEqual({
      _tag: "Accepted",
      proposedState: { epoch: 4, lastSeq: 0 },
    });
    expect(validateProducer(undefined, 4, 2)).toEqual({
      _tag: "Gap",
      expectedSeq: 0,
      receivedSeq: 2,
    });
  });
  it("fences an older epoch even when its sequence would otherwise advance", () => {
    expect(validateProducer({ epoch: 4, lastSeq: 2 }, 3, 3)).toEqual({
      _tag: "StaleEpoch",
      currentEpoch: 4,
    });
  });
  it("requires a new epoch to restart at sequence zero", () => {
    const state = { epoch: 4, lastSeq: 2 };
    expect(validateProducer(state, 5, 0)).toEqual({
      _tag: "Accepted",
      proposedState: { epoch: 5, lastSeq: 0 },
    });
    expect(validateProducer(state, 5, 3)).toEqual({ _tag: "InvalidEpochSeq" });
    expect(state).toEqual({ epoch: 4, lastSeq: 2 });
  });
  it("recognizes both the last acknowledged tuple and older sequences as duplicates", () => {
    for (const seq of [1, 2]) {
      expect(validateProducer({ epoch: 4, lastSeq: 2 }, 4, seq)).toEqual({
        _tag: "Duplicate",
        epoch: 4,
        lastSeq: 2,
      });
    }
  });
  it("accepts exactly the next sequence without mutating acknowledged state", () => {
    const state = { epoch: 4, lastSeq: 2 };
    expect(validateProducer(state, 4, 3)).toEqual({
      _tag: "Accepted",
      proposedState: { epoch: 4, lastSeq: 3 },
    });
    expect(state).toEqual({ epoch: 4, lastSeq: 2 });
  });
  it("reports the expected sequence for a gap within an epoch", () => {
    expect(validateProducer({ epoch: 4, lastSeq: 2 }, 4, 5)).toEqual({
      _tag: "Gap",
      expectedSeq: 3,
      receivedSeq: 5,
    });
  });
  it("maps duplicate metadata and the current closed flag to the append result", () => {
    for (const closed of [false, true]) {
      expect(
        rejectionToAppendResult({ _tag: "Duplicate", epoch: 4, lastSeq: 2 }, ZERO_OFFSET, closed),
      ).toEqual({
        status: "duplicate",
        offset: ZERO_OFFSET,
        producerEpoch: 4,
        producerSeq: 2,
        closed,
      });
    }
  });
  it("maps each non-duplicate rejection without losing its details", () => {
    expect(
      rejectionToAppendResult({ _tag: "StaleEpoch", currentEpoch: 4 }, ZERO_OFFSET, false),
    ).toEqual({ status: "stale-epoch", currentEpoch: 4 });
    expect(
      rejectionToAppendResult({ _tag: "Gap", expectedSeq: 3, receivedSeq: 5 }, ZERO_OFFSET, false),
    ).toEqual({ status: "producer-gap", expectedSeq: 3, receivedSeq: 5 });
    expect(rejectionToAppendResult({ _tag: "InvalidEpochSeq" }, ZERO_OFFSET, false)).toEqual({
      status: "invalid-epoch-seq",
    });
  });
});

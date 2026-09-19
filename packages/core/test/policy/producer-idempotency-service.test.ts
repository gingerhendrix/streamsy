import { describe, expect, it } from "bun:test";
import {
  validateProducer,
  rejectionToAppendError,
} from "../../src/policy/producer-idempotency-service.ts";
import { StreamId } from "../../src/schema/index.ts";
const id = StreamId.make("s");

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
  it("maps each non-duplicate rejection without losing its details", () => {
    expect(rejectionToAppendError({ _tag: "StaleEpoch", currentEpoch: 4 }, id)).toMatchObject({
      _tag: "StaleEpoch",
      currentEpoch: 4,
    });
    expect(
      rejectionToAppendError({ _tag: "Gap", expectedSeq: 3, receivedSeq: 5 }, id),
    ).toMatchObject({ _tag: "ProducerGap", expectedSeq: 3, receivedSeq: 5 });
    expect(rejectionToAppendError({ _tag: "InvalidEpochSeq" }, id)).toMatchObject({
      _tag: "InvalidEpochSeq",
    });
  });
});

import { describe, expect, it } from "bun:test";
import { Option, Schema } from "effect";
import { ZERO_OFFSET } from "../offset/index.ts";
import { MutationOutcome, Operation } from "../storage/mutation.ts";
import {
  MessageWindow,
  ProducerState,
  RecordPatch,
  StoredMessage,
  StreamConfig,
  StreamId,
  StreamLifecycle,
} from "./index.ts";

interface FiniteFieldCase {
  readonly name: string;
  readonly decode: (value: number) => object;
  readonly make: (value: number) => object;
  readonly decodeOmitted?: () => object;
  readonly makeOmitted?: () => object;
}

const id = StreamId.make("s");
const bytes = new Uint8Array([1]);

const cases: ReadonlyArray<FiniteFieldCase> = [
  {
    name: "StreamConfig.ttlSeconds",
    decode: (ttlSeconds) =>
      Schema.decodeSync(StreamConfig)({
        contentType: "text/plain",
        ttlSeconds,
        createdAt: 0,
      }),
    make: (ttlSeconds) =>
      StreamConfig.make({ contentType: "text/plain", ttlSeconds, createdAt: 0 }),
    decodeOmitted: () =>
      Schema.decodeSync(StreamConfig)({ contentType: "text/plain", createdAt: 0 }),
    makeOmitted: () => StreamConfig.make({ contentType: "text/plain", createdAt: 0 }),
  },
  {
    name: "StreamConfig.createdAt",
    decode: (createdAt) =>
      Schema.decodeSync(StreamConfig)({ contentType: "text/plain", createdAt }),
    make: (createdAt) => StreamConfig.make({ contentType: "text/plain", createdAt }),
  },
  {
    name: "StreamLifecycle.closedAt",
    decode: (closedAt) =>
      Schema.decodeSync(StreamLifecycle)({ closed: true, closedAt, softDeleted: false }),
    make: (closedAt) => StreamLifecycle.make({ closed: true, closedAt, softDeleted: false }),
    decodeOmitted: () => Schema.decodeSync(StreamLifecycle)({ closed: false, softDeleted: false }),
    makeOmitted: () => StreamLifecycle.make({ closed: false, softDeleted: false }),
  },
  {
    name: "StreamLifecycle.forkSubOffset",
    decode: (forkSubOffset) =>
      Schema.decodeSync(StreamLifecycle)({
        closed: false,
        forkSubOffset,
        softDeleted: false,
      }),
    make: (forkSubOffset) =>
      StreamLifecycle.make({ closed: false, forkSubOffset, softDeleted: false }),
    decodeOmitted: () => Schema.decodeSync(StreamLifecycle)({ closed: false, softDeleted: false }),
    makeOmitted: () => StreamLifecycle.make({ closed: false, softDeleted: false }),
  },
  {
    name: "StreamLifecycle.expiresAtMs",
    decode: (expiresAtMs) =>
      Schema.decodeSync(StreamLifecycle)({ closed: false, expiresAtMs, softDeleted: false }),
    make: (expiresAtMs) => StreamLifecycle.make({ closed: false, expiresAtMs, softDeleted: false }),
    decodeOmitted: () => Schema.decodeSync(StreamLifecycle)({ closed: false, softDeleted: false }),
    makeOmitted: () => StreamLifecycle.make({ closed: false, softDeleted: false }),
  },
  {
    name: "StoredMessage.timestamp",
    decode: (timestamp) =>
      Schema.decodeSync(StoredMessage)({ offset: ZERO_OFFSET, timestamp, data: bytes }),
    make: (timestamp) => StoredMessage.make({ offset: ZERO_OFFSET, timestamp, data: bytes }),
  },
  {
    name: "ProducerState.epoch",
    decode: (epoch) => Schema.decodeSync(ProducerState)({ epoch, lastSeq: 0 }),
    make: (epoch) => ProducerState.make({ epoch, lastSeq: 0 }),
  },
  {
    name: "ProducerState.lastSeq",
    decode: (lastSeq) => Schema.decodeSync(ProducerState)({ epoch: 0, lastSeq }),
    make: (lastSeq) => ProducerState.make({ epoch: 0, lastSeq }),
  },
  {
    name: "MessageWindow.limit",
    decode: (limit) => Schema.decodeSync(MessageWindow)({ limit }),
    make: (limit) => MessageWindow.make({ limit }),
    decodeOmitted: () => Schema.decodeSync(MessageWindow)({}),
    makeOmitted: () => MessageWindow.make({}),
  },
  {
    name: "RecordPatch.config.ttlSeconds",
    decode: (ttlSeconds) => Schema.decodeSync(RecordPatch)({ config: { ttlSeconds } }),
    make: (ttlSeconds) => RecordPatch.make({ config: { ttlSeconds } }),
    decodeOmitted: () => Schema.decodeSync(RecordPatch)({ config: {} }),
    makeOmitted: () => RecordPatch.make({ config: {} }),
  },
  {
    name: "Delete.expectedExpiresAtMs",
    decode: (expectedExpiresAtMs) =>
      Schema.decodeSync(Operation)({
        _tag: "Delete",
        streamId: id,
        reason: "expiry",
        expectedExpiresAtMs,
      }),
    make: (expectedExpiresAtMs) =>
      Operation.make({ _tag: "Delete", streamId: id, reason: "expiry", expectedExpiresAtMs }),
    decodeOmitted: () =>
      Schema.decodeSync(Operation)({ _tag: "Delete", streamId: id, reason: "delete" }),
    makeOmitted: () => Operation.make({ _tag: "Delete", streamId: id, reason: "delete" }),
  },
  {
    name: "Rejected.index",
    decode: (index) =>
      Schema.decodeSync(MutationOutcome)({
        _tag: "Rejected",
        index,
        reason: "not-found",
        record: Option.none(),
      }),
    make: (index) =>
      MutationOutcome.make({
        _tag: "Rejected",
        index,
        reason: "not-found",
        record: Option.none(),
      }),
  },
];

const nonFiniteValues = [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY];
const finiteValues = [-Number.MAX_VALUE, -1.5, -0, 0, 1.5, Number.MAX_VALUE];

for (const finiteCase of cases) {
  describe(finiteCase.name, () => {
    it("rejects NaN and both infinities during decoding and checked construction", () => {
      for (const value of nonFiniteValues) {
        expect(() => finiteCase.decode(value)).toThrow();
        expect(() => finiteCase.make(value)).toThrow();
      }
    });

    it("accepts finite values without adding sign, integer, or range restrictions", () => {
      for (const value of finiteValues) {
        expect(() => finiteCase.decode(value)).not.toThrow();
        expect(() => finiteCase.make(value)).not.toThrow();
      }
    });

    if (finiteCase.decodeOmitted !== undefined && finiteCase.makeOmitted !== undefined) {
      it("continues to allow the optional field to be omitted", () => {
        expect(finiteCase.decodeOmitted).not.toThrow();
        expect(finiteCase.makeOmitted).not.toThrow();
      });
    }
  });
}

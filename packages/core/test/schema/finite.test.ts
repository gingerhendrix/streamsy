import { describe, expect, it } from "vitest";
import { Option, Schema } from "effect";
import { ZERO_OFFSET } from "../../src/offset/index.ts";
import { MutationRejected, Operation } from "../../src/storage/mutation.ts";
import {
  MessageWindow,
  ProducerState,
  RecordPatch,
  StoredMessage,
  StreamConfig,
  StreamId,
  StreamLifecycle,
} from "../../src/schema/index.ts";

interface FiniteFieldCase {
  readonly name: string;
  readonly decode: (value: number) => object;
  readonly make: (value: number) => object;
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
  },
  {
    name: "StreamLifecycle.expiresAtMs",
    decode: (expiresAtMs) =>
      Schema.decodeSync(StreamLifecycle)({ closed: false, expiresAtMs, softDeleted: false }),
    make: (expiresAtMs) => StreamLifecycle.make({ closed: false, expiresAtMs, softDeleted: false }),
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
  },
  {
    name: "RecordPatch.config.ttlSeconds",
    decode: (ttlSeconds) => Schema.decodeSync(RecordPatch)({ config: { ttlSeconds } }),
    make: (ttlSeconds) => RecordPatch.make({ config: { ttlSeconds } }),
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
  },
  {
    name: "MutationRejected.index",
    decode: (index) =>
      Schema.decodeSync(MutationRejected)({
        _tag: "MutationRejected",
        index,
        reason: "not-found",
        record: Option.none(),
      }),
    make: (index) =>
      new MutationRejected({
        index,
        reason: "not-found",
        record: Option.none(),
      }),
  },
];

const nonFiniteValues = [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY];

for (const finiteCase of cases) {
  describe(finiteCase.name, () => {
    it("rejects NaN and both infinities during decoding and checked construction", () => {
      for (const value of nonFiniteValues) {
        expect(() => {
          finiteCase.decode(value);
        }).toThrow();
        expect(() => {
          finiteCase.make(value);
        }).toThrow();
      }
    });
  });
}

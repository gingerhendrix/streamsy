import { expect, test } from "bun:test";
import { Schema } from "effect";
import { StreamId } from "../../src/schema/index.ts";
import {
  StreamNotFound,
  StreamGone,
  StreamClosed,
  OffsetMismatch,
  StreamBusy,
  StaleEpoch,
  ProducerGap,
  InvalidEpochSeq,
  ForkSourceNotFound,
  NotSupported,
} from "../../src/protocol/errors.ts";

const id = StreamId.make("events");

const source = StreamId.make("source");

const check = <A extends Error, I>(schema: Schema.Codec<A, I>, error: A, message: string) => {
  test(`${error.name} has a message without adding a wire field`, () => {
    expect(error.message).toBe(message);
    expect(Schema.encodeSync(schema)(error)).not.toHaveProperty("message");
  });
};

check(StreamNotFound, new StreamNotFound({ id }), "Stream events was not found");

check(StreamGone, new StreamGone({ id }), "Stream events was deleted");

check(StreamClosed, new StreamClosed({ id, offset: "tail" }), "Stream events is closed at tail");

check(
  OffsetMismatch,
  new OffsetMismatch({ id, actual: "tail", expected: "old" }),
  "Stream events is at tail, not at the expected old",
);

check(StreamBusy, new StreamBusy({ id }), "Stream events is busy; try again");

check(
  StaleEpoch,
  new StaleEpoch({ id, currentEpoch: 2 }),
  "Producer epoch is stale on events; the current epoch is 2",
);

check(
  ProducerGap,
  new ProducerGap({ id, expectedSeq: 2, receivedSeq: 4 }),
  "Producer sequence gap on events: expected 2, received 4",
);

check(InvalidEpochSeq, new InvalidEpochSeq({ id }), "Invalid producer epoch or sequence on events");

check(
  ForkSourceNotFound,
  new ForkSourceNotFound({ id, source }),
  "Fork source source of events was not found",
);

check(
  NotSupported,
  new NotSupported({ id, feature: "fork" }),
  "Stream events does not support fork",
);

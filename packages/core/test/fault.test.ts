import { expect, test } from "bun:test";
import { Schema } from "effect";
import {
  StorageFault,
  StreamsFault,
  TransportFault,
  isStreamsFault,
  isRetryableFault,
} from "../src/fault.ts";
import { StreamNotFound } from "../src/protocol/errors.ts";
import { StreamId } from "../src/schema/index.ts";

for (const reason of ["request", "response", "decode", "configuration"] as const) {
  for (const status of [undefined, 400, 429, 500, 503]) {
    test(`TransportFault retryable: ${reason}, ${status ?? "no status"}`, () => {
      const fields = { operation: "read", reason, message: "failed" };
      const fault =
        status === undefined
          ? new TransportFault(fields)
          : new TransportFault({ ...fields, status });
      const retryable = reason === "request" || status === 429 || status === 500 || status === 503;
      expect(fault.retryable).toBe(retryable);
      expect(isRetryableFault(fault)).toBe(retryable);
      expect(Schema.encodeSync(TransportFault)(fault)).not.toHaveProperty("retryable");
    });
  }
}

test("StreamsFault guards accept both faults and reject protocol errors", () => {
  const storage = new StorageFault({ operation: "read", message: "busy", retryable: true });
  const transport = new TransportFault({
    operation: "read",
    reason: "configuration",
    message: "bad URL",
  });
  const protocol = new StreamNotFound({ id: StreamId.make("events") });
  for (const fault of [storage, transport]) {
    expect(isStreamsFault(fault)).toBe(true);
    expect(Schema.is(StreamsFault)(fault)).toBe(true);
  }
  expect(isRetryableFault(storage)).toBe(true);
  expect(
    isRetryableFault(new StorageFault({ operation: "read", message: "closed", retryable: false })),
  ).toBe(false);
  expect(isRetryableFault(transport)).toBe(false);
  expect(isStreamsFault(protocol)).toBe(false);
  expect(isRetryableFault(protocol)).toBe(false);
  expect(isStreamsFault(undefined)).toBe(false);
});

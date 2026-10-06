import { expect, test } from "bun:test";
import { StorageFault, TransportFault } from "@streamsy/core";
import { ProjectionFault } from "../src/fault.ts";

test("ProjectionFault retries only storage failures with a retryable or unknown cause", () => {
  const causes = [
    { cause: undefined, retryable: true },
    { cause: new Error("store unavailable"), retryable: true },
    {
      cause: new StorageFault({ operation: "load", message: "busy", retryable: true }),
      retryable: true,
    },
    {
      cause: new StorageFault({ operation: "load", message: "closed", retryable: false }),
      retryable: false,
    },
    {
      cause: new TransportFault({ operation: "read", reason: "request", message: "offline" }),
      retryable: true,
    },
    {
      cause: new TransportFault({ operation: "read", reason: "configuration", message: "bad URL" }),
      retryable: false,
    },
  ];

  for (const { cause, retryable } of causes) {
    expect(
      new ProjectionFault({ phase: "load", reason: "storage-failure", message: "failed", cause })
        .retryable,
    ).toBe(retryable);
    expect(
      new ProjectionFault({ phase: "load", reason: "invalid-record", message: "invalid", cause })
        .retryable,
    ).toBe(false);
  }
});

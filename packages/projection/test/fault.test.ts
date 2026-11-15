import { expect, test } from "vitest";
import { StorageFault, TransportFault } from "@streamsy/core";
import { ProjectionFault } from "../src/fault.ts";

test("ProjectionFault storage failures preserve the cause retryability", () => {
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

for (const reason of [
  "history-unavailable",
  "identity-mismatch",
  "invalid-record",
  "invalid-source",
  "invalid-options",
  "invalid-output",
  "token-conflict",
  "output-busy",
  "range-unreproducible",
  "stale-epoch",
  "unsupported-composition",
] as const) {
  test(`ProjectionFault classifies ${reason}`, () => {
    expect(new ProjectionFault({ phase: "load", reason, message: "failed" }).retryable).toBe(
      reason === "token-conflict" || reason === "output-busy",
    );
  });
}

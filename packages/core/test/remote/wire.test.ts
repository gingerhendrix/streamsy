import { expect, test } from "vitest";
import { isStreamsFault } from "../../src/fault.ts";
import { failure, unexpectedWith } from "../../src/remote/wire.ts";

for (const status of [400, 429, 500, 503]) {
  test(`unexpected HTTP ${status} retains status and detail`, () => {
    const fault = unexpectedWith("read", status, " busy ");
    expect(fault.status).toBe(status);
    expect(fault.message).toBe(`Unexpected HTTP ${status}: busy`);
    expect(fault.retryable).toBe(status !== 400);
    expect(unexpectedWith("read", status, " ").message).toBe(`Unexpected HTTP ${status}`);
  });
}

test("request faults omit an absent status and pass the public guard", () => {
  const fault = failure("read", "request", "offline");
  expect(isStreamsFault(fault)).toBe(true);
  expect(fault.retryable).toBe(true);
  expect(fault).not.toHaveProperty("status");
});
